package sync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

func newDiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestBatch(batchID, nodeID string, seq int64, collectedAt time.Time) *types.TelemetryBatch {
	return &types.TelemetryBatch{
		BatchID:        batchID,
		NodeID:         nodeID,
		SequenceNumber: seq,
		CollectedAt:    collectedAt,
		Metrics: []types.MetricSample{
			{Name: "cpu_usage", Value: 42.0, Timestamp: collectedAt},
		},
	}
}

// TestSyncer_IdempotentDuplicateSuccess verifies Amendment 4:
// When control plane returns 200 "already_accepted" for a PENDING batch,
// the edge marks that batch as SYNCED.
func TestSyncer_IdempotentDuplicateSuccess(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "idempotent_sync.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	batch := newTestBatch("batch-dup-test", "node-1", 1, now)
	if err := store.PersistBatch(ctx, batch); err != nil {
		t.Fatalf("PersistBatch failed: %v", err)
	}

	// Server simulates already having accepted this batch
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK) // 200 OK
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":   "already_accepted",
			"batch_id": "batch-dup-test",
			"node_id":  "node-1",
		})
	}))
	defer ts.Close()

	client := NewHTTPClient(ts.URL, WithLogger(newDiscardLogger()))
	syncer := NewSyncer(store, client, newDiscardLogger())

	stats, err := syncer.SyncPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("SyncPendingBatches failed: %v", err)
	}
	if stats.TotalSynced != 1 {
		t.Fatalf("expected 1 synced batch, got %d", stats.TotalSynced)
	}

	rec, err := store.GetBatch(ctx, "batch-dup-test")
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if rec.SyncStatus != storage.SyncStatusSynced {
		t.Errorf("expected status SYNCED on duplicate acceptance, got %s", rec.SyncStatus)
	}
	if rec.SentAt == nil {
		t.Errorf("expected sent_at to be populated")
	}
}

// TestSyncer_RetryAttemptSemantics verifies Amendment 3:
// Multiple internal HTTP retries within a failed sync cycle increment
// the persisted database attempt counter ONCE, not per HTTP retry.
func TestSyncer_RetryAttemptSemantics(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "retry_semantics.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	batch := newTestBatch("batch-retry-test", "node-1", 1, now)
	if err := store.PersistBatch(ctx, batch); err != nil {
		t.Fatalf("PersistBatch failed: %v", err)
	}

	// Server returns 503 for all attempts
	var httpAttempts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpAttempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	// Configure client with 3 retries and minimal backoff
	client := NewHTTPClient(ts.URL,
		WithMaxRetries(3),
		WithBaseBackoff(1*time.Millisecond),
		WithLogger(newDiscardLogger()),
	)
	syncer := NewSyncer(store, client, newDiscardLogger())

	// Run first sync cycle
	stats, err := syncer.SyncPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("SyncPendingBatches failed: %v", err)
	}
	if stats.TotalFailed != 1 {
		t.Fatalf("expected 1 failed batch, got %d", stats.TotalFailed)
	}

	// Internal HTTP retries should be 3
	if httpAttempts.Load() != 3 {
		t.Errorf("expected 3 internal HTTP attempts, got %d", httpAttempts.Load())
	}

	// Persisted database attempt counter must be EXACTLY 1 (one sync cycle failed)
	rec, err := store.GetBatch(ctx, "batch-retry-test")
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if rec.Attempt != 1 {
		t.Errorf("expected persisted attempt counter to be 1, got %d", rec.Attempt)
	}
	if rec.SyncStatus != storage.SyncStatusPending {
		t.Errorf("expected status PENDING after failure, got %s", rec.SyncStatus)
	}

	// Run second sync cycle
	_, _ = syncer.SyncPendingBatches(ctx, 10)
	rec, err = store.GetBatch(ctx, "batch-retry-test")
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if rec.Attempt != 2 {
		t.Errorf("expected persisted attempt counter to be 2 after 2 cycles, got %d", rec.Attempt)
	}
}

// TestSyncer_NodeFailureIsolation verifies Amendments 1 & 2:
// If Node A sequence N fails:
// - Node A sequence N+1 is halted (preserving sequence order).
// - Node B is NOT blocked and continues synchronizing independently.
func TestSyncer_NodeFailureIsolation(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "node_isolation.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	// Node A batches: seq 1 and seq 2
	_ = store.PersistBatch(ctx, newTestBatch("batch-a-1", "node-A", 1, now))
	_ = store.PersistBatch(ctx, newTestBatch("batch-a-2", "node-A", 2, now.Add(1*time.Second)))

	// Node B batches: seq 1 and seq 2
	_ = store.PersistBatch(ctx, newTestBatch("batch-b-1", "node-B", 1, now))
	_ = store.PersistBatch(ctx, newTestBatch("batch-b-2", "node-B", 2, now.Add(1*time.Second)))

	// Server fails all batches for Node A, but succeeds for Node B
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b types.TelemetryBatch
		_ = json.NewDecoder(r.Body).Decode(&b)

		if b.NodeID == "node-A" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":   "accepted",
			"batch_id": b.BatchID,
			"node_id":  b.NodeID,
		})
	}))
	defer ts.Close()

	client := NewHTTPClient(ts.URL,
		WithMaxRetries(2),
		WithBaseBackoff(1*time.Millisecond),
		WithLogger(newDiscardLogger()),
	)
	syncer := NewSyncer(store, client, newDiscardLogger())

	stats, err := syncer.SyncPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("SyncPendingBatches failed: %v", err)
	}

	// Node B succeeded 2 batches, Node A failed 1 batch (seq 1)
	if stats.NodeSuccesses["node-B"] != 2 {
		t.Errorf("expected 2 successes for Node B, got %d", stats.NodeSuccesses["node-B"])
	}
	if stats.TotalFailed != 1 {
		t.Errorf("expected 1 failure (Node A seq 1), got %d", stats.TotalFailed)
	}

	// Verify Node A states in SQLite:
	// batch-a-1 should be PENDING with attempt=1
	recA1, _ := store.GetBatch(ctx, "batch-a-1")
	if recA1.SyncStatus != storage.SyncStatusPending || recA1.Attempt != 1 {
		t.Errorf("expected batch-a-1 to be PENDING with attempt 1, got status %s, attempt %d", recA1.SyncStatus, recA1.Attempt)
	}

	// batch-a-2 must NOT have been attempted (attempt=0) because seq 1 failed
	recA2, _ := store.GetBatch(ctx, "batch-a-2")
	if recA2.SyncStatus != storage.SyncStatusPending || recA2.Attempt != 0 {
		t.Errorf("expected batch-a-2 to remain PENDING with attempt 0, got status %s, attempt %d", recA2.SyncStatus, recA2.Attempt)
	}

	// Verify Node B states in SQLite:
	// both batch-b-1 and batch-b-2 should be SYNCED
	recB1, _ := store.GetBatch(ctx, "batch-b-1")
	recB2, _ := store.GetBatch(ctx, "batch-b-2")
	if recB1.SyncStatus != storage.SyncStatusSynced || recB2.SyncStatus != storage.SyncStatusSynced {
		t.Errorf("expected Node B batches to be SYNCED, got B1=%s, B2=%s", recB1.SyncStatus, recB2.SyncStatus)
	}
}

// TestSyncer_PerNodeSequencePreservation verifies that within a single node,
// batches are sent and accepted strictly in (NodeID, SequenceNumber) order.
func TestSyncer_PerNodeSequencePreservation(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "seq_order.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	// Persist 4 batches for node-seq
	for seq := int64(1); seq <= 4; seq++ {
		batch := newTestBatch(filepath.Join("b", string(rune('0'+seq))), "node-seq", seq, now.Add(time.Duration(seq)*time.Second))
		_ = store.PersistBatch(ctx, batch)
	}

	var mu sync.Mutex
	var receivedSequences []int64

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b types.TelemetryBatch
		_ = json.NewDecoder(r.Body).Decode(&b)

		mu.Lock()
		receivedSequences = append(receivedSequences, b.SequenceNumber)
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":   "accepted",
			"batch_id": b.BatchID,
			"node_id":  b.NodeID,
		})
	}))
	defer ts.Close()

	client := NewHTTPClient(ts.URL, WithLogger(newDiscardLogger()))
	syncer := NewSyncer(store, client, newDiscardLogger())

	stats, err := syncer.SyncPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("SyncPendingBatches failed: %v", err)
	}
	if stats.TotalSynced != 4 {
		t.Fatalf("expected 4 synced batches, got %d", stats.TotalSynced)
	}

	mu.Lock()
	defer mu.Unlock()
	for i, seq := range receivedSequences {
		expected := int64(i + 1)
		if seq != expected {
			t.Errorf("sequence order violated: index %d was %d, expected %d", i, seq, expected)
		}
	}
}

// TestSyncer_OfflineDurabilityThenReconnect verifies the core Phase 3 distributed flow:
// 1. Edge operates offline and persists batches locally
// 2. Control plane comes online
// 3. Edge reconnects and synchronizes all pending batches
func TestSyncer_OfflineDurabilityThenReconnect(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "offline_then_online.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	// 1. Edge generates and stores 3 batches locally while offline
	for i := int64(1); i <= 3; i++ {
		batch := newTestBatch(filepath.Join("off", string(rune('0'+i))), "edge-node", i, now.Add(time.Duration(i)*time.Second))
		if err := store.PersistBatch(ctx, batch); err != nil {
			t.Fatalf("PersistBatch %d failed: %v", i, err)
		}
	}

	// Attempt sync to non-existent server (offline)
	deadClient := NewHTTPClient("http://127.0.0.1:59999",
		WithMaxRetries(1),
		WithBaseBackoff(1*time.Millisecond),
		WithLogger(newDiscardLogger()),
	)
	offlineSyncer := NewSyncer(store, deadClient, newDiscardLogger())

	stats, _ := offlineSyncer.SyncPendingBatches(ctx, 10)
	if stats.TotalSynced != 0 {
		t.Errorf("expected 0 synced while offline, got %d", stats.TotalSynced)
	}

	// Confirm all 3 batches are STILL in SQLite with status PENDING (no data loss!)
	pending, err := store.GetPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("GetPendingBatches failed: %v", err)
	}
	if len(pending) != 3 {
		t.Fatalf("expected 3 pending batches in store, got %d", len(pending))
	}

	// 2. Control plane comes online
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b types.TelemetryBatch
		_ = json.NewDecoder(r.Body).Decode(&b)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":   "accepted",
			"batch_id": b.BatchID,
			"node_id":  b.NodeID,
		})
	}))
	defer ts.Close()

	// 3. Edge connects and synchronizes
	liveClient := NewHTTPClient(ts.URL, WithLogger(newDiscardLogger()))
	liveSyncer := NewSyncer(store, liveClient, newDiscardLogger())

	liveStats, err := liveSyncer.SyncPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("live sync failed: %v", err)
	}
	if liveStats.TotalSynced != 3 {
		t.Fatalf("expected 3 synced batches, got %d", liveStats.TotalSynced)
	}

	// Verify all batches in store are now SYNCED
	remainingPending, _ := store.GetPendingBatches(ctx, 10)
	if len(remainingPending) != 0 {
		t.Errorf("expected 0 pending batches remaining, got %d", len(remainingPending))
	}
}

// TestSyncer_TimestampSemantics verifies Amendment 6:
// collected_at remains unchanged, sent_at records edge activity, and ingested_at is generated by server.
func TestSyncer_TimestampSemantics(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "timestamps.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	originalCollectedAt := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	batch := newTestBatch("batch-ts-1", "node-ts", 1, originalCollectedAt)
	if err := store.PersistBatch(ctx, batch); err != nil {
		t.Fatalf("PersistBatch failed: %v", err)
	}

	var receivedBatch types.TelemetryBatch
	serverIngestedAt := time.Date(2026, 9, 20, 12, 5, 0, 0, time.UTC)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&receivedBatch)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":      "accepted",
			"batch_id":    receivedBatch.BatchID,
			"node_id":     receivedBatch.NodeID,
			"ingested_at": serverIngestedAt,
		})
	}))
	defer ts.Close()

	client := NewHTTPClient(ts.URL, WithLogger(newDiscardLogger()))
	syncer := NewSyncer(store, client, newDiscardLogger())

	_, err = syncer.SyncPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("SyncPendingBatches failed: %v", err)
	}

	// 1. Verify collected_at was NOT modified by sync or serialization
	if !receivedBatch.CollectedAt.Equal(originalCollectedAt) {
		t.Errorf("collected_at was modified: want %v, got %v", originalCollectedAt, receivedBatch.CollectedAt)
	}

	// 2. Verify sent_at was attached by edge syncer
	if receivedBatch.SentAt == nil || receivedBatch.SentAt.IsZero() {
		t.Errorf("sent_at was not populated during transmission")
	}

	// 3. Verify SQLite record has sent_at recorded
	rec, _ := store.GetBatch(ctx, "batch-ts-1")
	if rec.SentAt == nil || !rec.SentAt.Equal(*receivedBatch.SentAt) {
		t.Errorf("persisted sent_at mismatch")
	}
	if !rec.CollectedAt.Equal(originalCollectedAt) {
		t.Errorf("persisted collected_at was corrupted")
	}
}

// TestSyncer_FailureSafetyMatrix tests failure behavior across failure modes:
// HTTP timeout → PENDING (attempt incremented)
// connection refused → PENDING (attempt incremented)
// HTTP 5xx → PENDING (attempt incremented)
// HTTP 4xx validation failure → FAILED (quarantined, attempt preserved, never deleted)
// Confirming that no failed sync deletes the batch or marks it SYNCED.
func TestSyncer_FailureSafetyMatrix(t *testing.T) {
	testCases := []struct {
		name               string
		setupMock          func() (string, func())
		clientOpts         []HTTPClientOption
		expectedStatus     storage.SyncStatus
		expectedAttempt    int
		expectedFailed     int
		expectedQuarantine int
	}{
		{
			name: "ConnectionRefused",
			setupMock: func() (string, func()) {
				// Port 59998 is not listening
				return "http://127.0.0.1:59998", func() {}
			},
			clientOpts: []HTTPClientOption{
				WithMaxRetries(1),
				WithBaseBackoff(1 * time.Millisecond),
			},
			expectedStatus:     storage.SyncStatusPending,
			expectedAttempt:    1,
			expectedFailed:     1,
			expectedQuarantine: 0,
		},
		{
			name: "HTTPTimeout",
			setupMock: func() (string, func()) {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					time.Sleep(100 * time.Millisecond)
				}))
				return ts.URL, ts.Close
			},
			clientOpts: []HTTPClientOption{
				WithHTTPTimeout(10 * time.Millisecond),
				WithMaxRetries(1),
				WithBaseBackoff(1 * time.Millisecond),
			},
			expectedStatus:     storage.SyncStatusPending,
			expectedAttempt:    1,
			expectedFailed:     1,
			expectedQuarantine: 0,
		},
		{
			name: "HTTP500InternalServerError",
			setupMock: func() (string, func()) {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
				}))
				return ts.URL, ts.Close
			},
			clientOpts: []HTTPClientOption{
				WithMaxRetries(2),
				WithBaseBackoff(1 * time.Millisecond),
			},
			expectedStatus:     storage.SyncStatusPending,
			expectedAttempt:    1,
			expectedFailed:     1,
			expectedQuarantine: 0,
		},
		{
			name: "HTTP400BadRequest",
			setupMock: func() (string, func()) {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusBadRequest)
					_, _ = w.Write([]byte(`{"error":"invalid_batch"}`))
				}))
				return ts.URL, ts.Close
			},
			clientOpts: []HTTPClientOption{
				WithMaxRetries(1),
				WithBaseBackoff(1 * time.Millisecond),
			},
			expectedStatus:     storage.SyncStatusFailed,
			expectedAttempt:    0,
			expectedFailed:     0,
			expectedQuarantine: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dbPath := filepath.Join(t.TempDir(), tc.name+".db")
			store, err := storage.OpenSQLite(dbPath)
			if err != nil {
				t.Fatalf("OpenSQLite failed: %v", err)
			}
			defer store.Close()

			now := time.Now().UTC()
			batchID := "batch-" + tc.name
			batch := newTestBatch(batchID, "node-fail-test", 1, now)
			if err := store.PersistBatch(ctx, batch); err != nil {
				t.Fatalf("PersistBatch failed: %v", err)
			}

			url, cleanup := tc.setupMock()
			defer cleanup()

			opts := append([]HTTPClientOption{WithLogger(newDiscardLogger())}, tc.clientOpts...)
			client := NewHTTPClient(url, opts...)
			syncer := NewSyncer(store, client, newDiscardLogger())

			stats, _ := syncer.SyncPendingBatches(ctx, 10)
			if stats.TotalSynced != 0 {
				t.Errorf("expected 0 synced, got %d", stats.TotalSynced)
			}
			if stats.TotalFailed != tc.expectedFailed {
				t.Errorf("expected %d failed batch, got %d", tc.expectedFailed, stats.TotalFailed)
			}
			if stats.TotalQuarantined != tc.expectedQuarantine {
				t.Errorf("expected %d quarantined batch, got %d", tc.expectedQuarantine, stats.TotalQuarantined)
			}

			// Invariant 1: Local batch MUST NOT be deleted (count remains 1)
			count, err := store.CountBatches(ctx)
			if err != nil {
				t.Fatalf("CountBatches failed: %v", err)
			}
			if count != 1 {
				t.Errorf("expected count to remain 1, got %d", count)
			}

			// Invariant 2: Local batch status matches expectedStatus (FAILED for 400, PENDING for transient)
			rec, err := store.GetBatch(ctx, batchID)
			if err != nil {
				t.Fatalf("GetBatch failed: %v", err)
			}
			if rec.SyncStatus != tc.expectedStatus {
				t.Errorf("expected status %s after failure, got %s", tc.expectedStatus, rec.SyncStatus)
			}

			// Invariant 3: Persisted attempt counter matches expectedAttempt
			if rec.Attempt != tc.expectedAttempt {
				t.Errorf("expected attempt %d, got %d", tc.expectedAttempt, rec.Attempt)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Phase 6.28 Stage 2: Poison-Batch Quarantine and HOL Blocking Prevention Tests
// ---------------------------------------------------------------------------

type failingQuarantineStore struct {
	storage.Store
}

func (f *failingQuarantineStore) MarkBatchQuarantined(ctx context.Context, batchID string, reason string, failedAt time.Time) error {
	return errors.New("simulated storage failure during quarantine")
}

type mockPublisher struct {
	publishFunc func(ctx context.Context, batch *types.TelemetryBatch) error
}

func (m *mockPublisher) PublishBatch(ctx context.Context, batch *types.TelemetryBatch) error {
	if m.publishFunc != nil {
		return m.publishFunc(ctx, batch)
	}
	return nil
}

func (m *mockPublisher) Close() error {
	return nil
}

// 1. Permanent HTTP 400 validation failure is quarantined.
func TestSyncer_Quarantine_HTTP400Validation(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "q_http400.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	batch := newTestBatch("batch-400", "node-1", 1, now)
	if err := store.PersistBatch(ctx, batch); err != nil {
		t.Fatalf("PersistBatch failed: %v", err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"malformed_telemetry_schema"}`))
	}))
	defer ts.Close()

	client := NewHTTPClient(ts.URL, WithLogger(newDiscardLogger()))
	syncer := NewSyncer(store, client, newDiscardLogger())

	stats, err := syncer.SyncPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("SyncPendingBatches failed: %v", err)
	}

	if stats.TotalSynced != 0 {
		t.Errorf("expected 0 synced, got %d", stats.TotalSynced)
	}
	if stats.TotalQuarantined != 1 {
		t.Errorf("expected 1 quarantined batch, got %d", stats.TotalQuarantined)
	}
	if stats.TotalFailed != 0 {
		t.Errorf("expected 0 failed (halted) batches, got %d", stats.TotalFailed)
	}

	rec, err := store.GetBatch(ctx, "batch-400")
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if rec.SyncStatus != storage.SyncStatusFailed {
		t.Errorf("expected status FAILED, got %s", rec.SyncStatus)
	}
	if rec.FailedAt == nil {
		t.Fatal("expected failed_at to be populated")
	}
	if !strings.Contains(rec.ErrorReason, "400") {
		t.Errorf("expected error_reason to mention 400, got %q", rec.ErrorReason)
	}

	// Verify not in pending batches
	pending, err := store.GetPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("GetPendingBatches failed: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("expected 0 pending batches, got %d", len(pending))
	}
}

// 2. HTTP 413 is quarantined when the existing client reports it as permanent.
func TestSyncer_Quarantine_HTTP413EntityTooLarge(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "q_http413.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	batch := newTestBatch("batch-413", "node-1", 1, now)
	if err := store.PersistBatch(ctx, batch); err != nil {
		t.Fatalf("PersistBatch failed: %v", err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		_, _ = w.Write([]byte(`{"error":"payload_exceeds_max_bytes"}`))
	}))
	defer ts.Close()

	client := NewHTTPClient(ts.URL, WithLogger(newDiscardLogger()))
	syncer := NewSyncer(store, client, newDiscardLogger())

	stats, err := syncer.SyncPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("SyncPendingBatches failed: %v", err)
	}

	if stats.TotalQuarantined != 1 {
		t.Errorf("expected 1 quarantined batch, got %d", stats.TotalQuarantined)
	}

	rec, err := store.GetBatch(ctx, "batch-413")
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if rec.SyncStatus != storage.SyncStatusFailed {
		t.Errorf("expected status FAILED, got %s", rec.SyncStatus)
	}
	if !strings.Contains(rec.ErrorReason, "413") {
		t.Errorf("expected error_reason to mention 413, got %q", rec.ErrorReason)
	}
}

// 3. A permanently invalid batch is quarantined and a later valid batch for the same node can sync in the same cycle.
func TestSyncer_HeadOfLineBlocking_QuarantineAllowsSubsequentSync(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "hol_quarantine.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	// Batch 1 (seq 1): poison batch
	b1 := newTestBatch("batch-poison-seq1", "node-hol", 1, now)
	if err := store.PersistBatch(ctx, b1); err != nil {
		t.Fatalf("PersistBatch b1 failed: %v", err)
	}
	// Batch 2 (seq 2): healthy batch
	b2 := newTestBatch("batch-valid-seq2", "node-hol", 2, now)
	if err := store.PersistBatch(ctx, b2); err != nil {
		t.Fatalf("PersistBatch b2 failed: %v", err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b types.TelemetryBatch
		_ = json.NewDecoder(r.Body).Decode(&b)
		if b.BatchID == "batch-poison-seq1" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"poison_payload"}`))
			return
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
	}))
	defer ts.Close()

	client := NewHTTPClient(ts.URL, WithLogger(newDiscardLogger()))
	syncer := NewSyncer(store, client, newDiscardLogger())

	stats, err := syncer.SyncPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("SyncPendingBatches failed: %v", err)
	}

	if stats.TotalQuarantined != 1 {
		t.Errorf("expected 1 quarantined batch, got %d", stats.TotalQuarantined)
	}
	if stats.TotalSynced != 1 {
		t.Errorf("expected 1 synced batch, got %d", stats.TotalSynced)
	}
	if stats.TotalFailed != 0 {
		t.Errorf("expected 0 failed batches, got %d", stats.TotalFailed)
	}

	// Batch 1 must be FAILED
	rec1, err := store.GetBatch(ctx, "batch-poison-seq1")
	if err != nil {
		t.Fatalf("GetBatch b1 failed: %v", err)
	}
	if rec1.SyncStatus != storage.SyncStatusFailed {
		t.Errorf("expected b1 to be FAILED, got %s", rec1.SyncStatus)
	}

	// Batch 2 must be SYNCED
	rec2, err := store.GetBatch(ctx, "batch-valid-seq2")
	if err != nil {
		t.Fatalf("GetBatch b2 failed: %v", err)
	}
	if rec2.SyncStatus != storage.SyncStatusSynced {
		t.Errorf("expected b2 to be SYNCED, got %s", rec2.SyncStatus)
	}
}

// 4. Repeated transient 503/network failures remain PENDING below the retry threshold.
func TestSyncer_TransientFailure_BelowThresholdRemainsPending(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "transient_below_thresh.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	b := newTestBatch("batch-503-transient", "node-1", 1, now)
	if err := store.PersistBatch(ctx, b); err != nil {
		t.Fatalf("PersistBatch failed: %v", err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	client := NewHTTPClient(ts.URL, WithMaxRetries(1), WithBaseBackoff(1*time.Millisecond), WithLogger(newDiscardLogger()))
	syncer := NewSyncer(store, client, newDiscardLogger(), WithMaxSyncAttempts(5))

	// Run 3 cycles (less than threshold 5)
	for i := 1; i <= 3; i++ {
		stats, err := syncer.SyncPendingBatches(ctx, 10)
		if err != nil {
			t.Fatalf("cycle %d failed: %v", i, err)
		}
		if stats.TotalQuarantined != 0 {
			t.Errorf("cycle %d: expected 0 quarantined, got %d", i, stats.TotalQuarantined)
		}
		if stats.TotalFailed != 1 {
			t.Errorf("cycle %d: expected 1 failed, got %d", i, stats.TotalFailed)
		}
	}

	rec, err := store.GetBatch(ctx, "batch-503-transient")
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if rec.SyncStatus != storage.SyncStatusPending {
		t.Errorf("expected status PENDING below threshold, got %s", rec.SyncStatus)
	}
	if rec.Attempt != 3 {
		t.Errorf("expected attempt 3, got %d", rec.Attempt)
	}
}

// 5. A transient failure reaching the threshold becomes FAILED.
func TestSyncer_TransientFailure_RetryExhaustionQuarantined(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "transient_exhaustion.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	b1 := newTestBatch("batch-exhaust", "node-1", 1, now)
	if err := store.PersistBatch(ctx, b1); err != nil {
		t.Fatalf("PersistBatch b1 failed: %v", err)
	}
	b2 := newTestBatch("batch-after-exhaust", "node-1", 2, now)
	if err := store.PersistBatch(ctx, b2); err != nil {
		t.Fatalf("PersistBatch b2 failed: %v", err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b types.TelemetryBatch
		_ = json.NewDecoder(r.Body).Decode(&b)
		if b.BatchID == "batch-exhaust" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
	}))
	defer ts.Close()

	client := NewHTTPClient(ts.URL, WithMaxRetries(1), WithBaseBackoff(1*time.Millisecond), WithLogger(newDiscardLogger()))
	syncer := NewSyncer(store, client, newDiscardLogger(), WithMaxSyncAttempts(3))

	// Cycle 1: fails, attempt -> 1, PENDING
	stats1, _ := syncer.SyncPendingBatches(ctx, 10)
	if stats1.TotalQuarantined != 0 || stats1.TotalFailed != 1 {
		t.Fatalf("cycle 1: expected 0 quarantined, 1 failed, got %+v", stats1)
	}

	// Cycle 2: fails, attempt -> 2, PENDING
	stats2, _ := syncer.SyncPendingBatches(ctx, 10)
	if stats2.TotalQuarantined != 0 || stats2.TotalFailed != 1 {
		t.Fatalf("cycle 2: expected 0 quarantined, 1 failed, got %+v", stats2)
	}

	// Cycle 3: attempt 2 + 1 >= 3 -> QUARANTINED!
	// And b2 should sync in the same cycle now that b1 is quarantined!
	stats3, _ := syncer.SyncPendingBatches(ctx, 10)
	if stats3.TotalQuarantined != 1 {
		t.Errorf("cycle 3: expected 1 quarantined, got %d", stats3.TotalQuarantined)
	}
	if stats3.TotalSynced != 1 {
		t.Errorf("cycle 3: expected 1 synced (b2 unblocked), got %d", stats3.TotalSynced)
	}

	rec1, _ := store.GetBatch(ctx, "batch-exhaust")
	if rec1.SyncStatus != storage.SyncStatusFailed {
		t.Errorf("expected b1 FAILED after exhaustion, got %s", rec1.SyncStatus)
	}
	if !strings.Contains(rec1.ErrorReason, "exhausted") {
		t.Errorf("expected reason to contain 'exhausted', got %q", rec1.ErrorReason)
	}

	rec2, _ := store.GetBatch(ctx, "batch-after-exhaust")
	if rec2.SyncStatus != storage.SyncStatusSynced {
		t.Errorf("expected b2 SYNCED after b1 unblocked, got %s", rec2.SyncStatus)
	}
}

// 6. The attempt threshold counts logical sync cycles rather than internal HTTP attempts.
func TestSyncer_AttemptCounting_LogicalCyclesNotHTTPAttempts(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "logical_attempt_count.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	b := newTestBatch("batch-attempts-test", "node-1", 1, now)
	if err := store.PersistBatch(ctx, b); err != nil {
		t.Fatalf("PersistBatch failed: %v", err)
	}

	httpTries := int32(0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&httpTries, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	// Configure HTTP client with maxRetries = 3 (will attempt 3 HTTP requests per cycle)
	client := NewHTTPClient(ts.URL, WithMaxRetries(3), WithBaseBackoff(1*time.Millisecond), WithLogger(newDiscardLogger()))
	syncer := NewSyncer(store, client, newDiscardLogger(), WithMaxSyncAttempts(10))

	// Execute ONE logical sync cycle
	stats, err := syncer.SyncPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("SyncPendingBatches failed: %v", err)
	}
	if stats.TotalFailed != 1 {
		t.Errorf("expected 1 failed cycle, got %d", stats.TotalFailed)
	}

	// 3 HTTP tries should have been executed by HTTPClient
	if atomic.LoadInt32(&httpTries) != 3 {
		t.Errorf("expected 3 internal HTTP attempts, got %d", atomic.LoadInt32(&httpTries))
	}

	// SQLite record attempt counter MUST be 1 (counting logical cycles, not internal HTTP tries)
	rec, err := store.GetBatch(ctx, "batch-attempts-test")
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if rec.Attempt != 1 {
		t.Errorf("expected persisted attempt counter to be 1, got %d", rec.Attempt)
	}
}

// 7. Failure to persist quarantine stops subsequent processing safely and returns a meaningful error.
func TestSyncer_QuarantinePersistenceFailure_StopsNodeSafely(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "q_persist_fail.db")
	realStore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer realStore.Close()

	now := time.Now().UTC()
	b1 := newTestBatch("batch-qfail-1", "node-1", 1, now)
	if err := realStore.PersistBatch(ctx, b1); err != nil {
		t.Fatalf("PersistBatch b1 failed: %v", err)
	}
	b2 := newTestBatch("batch-qfail-2", "node-1", 2, now)
	if err := realStore.PersistBatch(ctx, b2); err != nil {
		t.Fatalf("PersistBatch b2 failed: %v", err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest) // triggers permanent quarantine path
	}))
	defer ts.Close()

	client := NewHTTPClient(ts.URL, WithLogger(newDiscardLogger()))
	failingStore := &failingQuarantineStore{Store: realStore}
	syncer := NewSyncer(failingStore, client, newDiscardLogger())

	stats, err := syncer.SyncPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("SyncPendingBatches returned unexpected error: %v", err)
	}

	if stats.TotalQuarantined != 0 {
		t.Errorf("expected 0 quarantined due to persistence failure, got %d", stats.TotalQuarantined)
	}
	if stats.TotalFailed != 1 {
		t.Errorf("expected 1 failed (halted) batch, got %d", stats.TotalFailed)
	}
	if stats.TotalSynced != 0 {
		t.Errorf("expected 0 synced, got %d", stats.TotalSynced)
	}
	if stats.NodeFailures["node-1"] == nil || !strings.Contains(stats.NodeFailures["node-1"].Error(), "quarantine persistence failed") {
		t.Errorf("expected quarantine persistence failure recorded in NodeFailures, got %v", stats.NodeFailures["node-1"])
	}

	// Batch 2 must remain PENDING and untouched (attempt 0)
	rec2, err := realStore.GetBatch(ctx, "batch-qfail-2")
	if err != nil {
		t.Fatalf("GetBatch b2 failed: %v", err)
	}
	if rec2.SyncStatus != storage.SyncStatusPending || rec2.Attempt != 0 {
		t.Errorf("expected b2 to remain PENDING with attempt 0, got status=%s attempt=%d", rec2.SyncStatus, rec2.Attempt)
	}
}

// 8. Parent context cancellation does not record an attempt with a cancelled context and does not quarantine the batch.
func TestSyncer_Cancellation_DoesNotRecordAttemptOrQuarantine(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dbPath := filepath.Join(t.TempDir(), "cancel_test.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	b := newTestBatch("batch-cancel", "node-1", 1, now)
	if err := store.PersistBatch(context.Background(), b); err != nil {
		t.Fatalf("PersistBatch failed: %v", err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Cancel parent context before responding
		cancel()
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer ts.Close()

	client := NewHTTPClient(ts.URL, WithLogger(newDiscardLogger()))
	syncer := NewSyncer(store, client, newDiscardLogger())

	stats, err := syncer.SyncPendingBatches(ctx, 10)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got %v", err)
	}
	if stats.TotalQuarantined != 0 {
		t.Errorf("expected 0 quarantined on cancellation, got %d", stats.TotalQuarantined)
	}

	// Batch must remain PENDING with attempt 0 (no attempt recorded with cancelled context)
	rec, err := store.GetBatch(context.Background(), "batch-cancel")
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if rec.SyncStatus != storage.SyncStatusPending {
		t.Errorf("expected status PENDING after cancellation, got %s", rec.SyncStatus)
	}
	if rec.Attempt != 0 {
		t.Errorf("expected attempt 0, got %d", rec.Attempt)
	}
}

// 9. Internal request timeout is not incorrectly treated as parent shutdown.
func TestSyncer_InternalTimeout_NotParentCancellation(t *testing.T) {
	// Generous 5-second parent context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dbPath := filepath.Join(t.TempDir(), "internal_timeout.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	b := newTestBatch("batch-timeout", "node-1", 1, now)
	if err := store.PersistBatch(ctx, b); err != nil {
		t.Fatalf("PersistBatch failed: %v", err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond) // exceeds client timeout
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	// Short internal client timeout (10ms)
	client := NewHTTPClient(ts.URL, WithHTTPTimeout(10*time.Millisecond), WithMaxRetries(1), WithBaseBackoff(1*time.Millisecond), WithLogger(newDiscardLogger()))
	syncer := NewSyncer(store, client, newDiscardLogger(), WithMaxSyncAttempts(5))

	stats, err := syncer.SyncPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("SyncPendingBatches failed: %v", err)
	}

	// Handled as a transient cycle failure (NOT parent shutdown)
	if stats.TotalFailed != 1 {
		t.Errorf("expected 1 failed batch, got %d", stats.TotalFailed)
	}
	if stats.TotalQuarantined != 0 {
		t.Errorf("expected 0 quarantined, got %d", stats.TotalQuarantined)
	}

	// Batch remains PENDING with attempt incremented to 1
	rec, err := store.GetBatch(ctx, "batch-timeout")
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if rec.SyncStatus != storage.SyncStatusPending {
		t.Errorf("expected status PENDING after internal timeout, got %s", rec.SyncStatus)
	}
	if rec.Attempt != 1 {
		t.Errorf("expected attempt 1, got %d", rec.Attempt)
	}
}

// 10. Successful recovery after a transient failure still uses the existing successful status transition.
func TestSyncer_TransientRecovery_MarksSynced(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "recovery_synced.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	b := newTestBatch("batch-recover", "node-1", 1, now)
	if err := store.PersistBatch(ctx, b); err != nil {
		t.Fatalf("PersistBatch failed: %v", err)
	}

	shouldFail := true
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if shouldFail {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
	}))
	defer ts.Close()

	client := NewHTTPClient(ts.URL, WithMaxRetries(1), WithBaseBackoff(1*time.Millisecond), WithLogger(newDiscardLogger()))
	syncer := NewSyncer(store, client, newDiscardLogger(), WithMaxSyncAttempts(5))

	// Cycle 1: fails
	stats1, _ := syncer.SyncPendingBatches(ctx, 10)
	if stats1.TotalFailed != 1 || stats1.TotalSynced != 0 {
		t.Fatalf("cycle 1: expected failure, got %+v", stats1)
	}

	// Server recovers
	shouldFail = false

	// Cycle 2: recovers
	stats2, err := syncer.SyncPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("cycle 2 failed: %v", err)
	}
	if stats2.TotalSynced != 1 {
		t.Errorf("expected 1 synced batch after recovery, got %d", stats2.TotalSynced)
	}

	rec, err := store.GetBatch(ctx, "batch-recover")
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if rec.SyncStatus != storage.SyncStatusSynced {
		t.Errorf("expected status SYNCED, got %s", rec.SyncStatus)
	}
	if rec.SentAt == nil {
		t.Errorf("expected sent_at to be populated")
	}
}

// 11. Sequence numbers and payload identity remain unchanged when a batch is quarantined.
func TestSyncer_Quarantine_PreservesSequenceAndPayloadIdentity(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "preserve_identity.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	b := &types.TelemetryBatch{
		BatchID:        "batch-exact-identity",
		NodeID:         "node-exact",
		SequenceNumber: 77,
		CollectedAt:    now,
		Metrics: []types.MetricSample{
			{Name: "cpu_usage_percent", Value: 88.5, Unit: "percent", Timestamp: now},
			{Name: "disk_iops", Value: 1200.0, Unit: "ops", Timestamp: now},
		},
	}
	if err := store.PersistBatch(ctx, b); err != nil {
		t.Fatalf("PersistBatch failed: %v", err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"bad_schema"}`))
	}))
	defer ts.Close()

	client := NewHTTPClient(ts.URL, WithLogger(newDiscardLogger()))
	syncer := NewSyncer(store, client, newDiscardLogger())

	stats, err := syncer.SyncPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("SyncPendingBatches failed: %v", err)
	}
	if stats.TotalQuarantined != 1 {
		t.Errorf("expected 1 quarantined, got %d", stats.TotalQuarantined)
	}

	rec, err := store.GetBatch(ctx, "batch-exact-identity")
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}

	if rec.BatchID != "batch-exact-identity" {
		t.Errorf("batch_id modified: %s", rec.BatchID)
	}
	if rec.NodeID != "node-exact" {
		t.Errorf("node_id modified: %s", rec.NodeID)
	}
	if rec.SequenceNumber != 77 {
		t.Errorf("sequence_number modified: %d", rec.SequenceNumber)
	}
	if !rec.CollectedAt.Equal(now) {
		t.Errorf("collected_at modified: %v vs %v", rec.CollectedAt, now)
	}
	if len(rec.Payload.Metrics) != 2 || rec.Payload.Metrics[0].Name != "cpu_usage_percent" {
		t.Errorf("payload corrupted: %+v", rec.Payload)
	}
}

// 12. Audit events are emitted for quarantine and requeue at the appropriate integration points.
func TestSyncer_Quarantine_AuditEventsEmitted(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "audit_quarantine.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	b := newTestBatch("batch-audit-test", "node-aud", 1, now)
	if err := store.PersistBatch(ctx, b); err != nil {
		t.Fatalf("PersistBatch failed: %v", err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"bad_signature"}`))
	}))
	defer ts.Close()

	client := NewHTTPClient(ts.URL, WithLogger(newDiscardLogger()))
	syncer := NewSyncer(store, client, newDiscardLogger(), WithAuditStore(store))

	// Quarantine
	stats, err := syncer.SyncPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("SyncPendingBatches failed: %v", err)
	}
	if stats.TotalQuarantined != 1 {
		t.Fatalf("expected 1 quarantined, got %d", stats.TotalQuarantined)
	}

	// Verify quarantine audit event was recorded in AuditStore
	events, err := store.ListAuditEvents(ctx, storage.AuditFilter{NodeID: "node-aud"})
	if err != nil {
		t.Fatalf("ListAuditEvents failed: %v", err)
	}
	if len(events) < 1 {
		t.Fatalf("expected at least 1 audit event, got %d", len(events))
	}

	var qEvt *storage.StoredAuditEvent
	for _, e := range events {
		if e.EventType == "TELEMETRY_BATCH_QUARANTINED" {
			qEvt = e
			break
		}
	}
	if qEvt == nil {
		t.Fatal("expected TELEMETRY_BATCH_QUARANTINED audit event to be recorded")
	}
	if qEvt.NodeID != "node-aud" {
		t.Errorf("expected node_id node-aud, got %s", qEvt.NodeID)
	}
	if qEvt.Metadata["batch_id"] != "batch-audit-test" {
		t.Errorf("expected metadata batch_id batch-audit-test, got %s", qEvt.Metadata["batch_id"])
	}

	// Requeue batch
	if err := syncer.RequeueBatch(ctx, "batch-audit-test"); err != nil {
		t.Fatalf("RequeueBatch failed: %v", err)
	}

	// Verify requeue audit event was recorded in AuditStore
	eventsAfter, err := store.ListAuditEvents(ctx, storage.AuditFilter{NodeID: "node-aud"})
	if err != nil {
		t.Fatalf("ListAuditEvents after requeue failed: %v", err)
	}

	var rqEvt *storage.StoredAuditEvent
	for _, e := range eventsAfter {
		if e.EventType == "TELEMETRY_BATCH_REQUEUED" {
			rqEvt = e
			break
		}
	}
	if rqEvt == nil {
		t.Fatal("expected TELEMETRY_BATCH_REQUEUED audit event to be recorded")
	}
	if rqEvt.Metadata["batch_id"] != "batch-audit-test" {
		t.Errorf("expected metadata batch_id batch-audit-test in requeue event, got %s", rqEvt.Metadata["batch_id"])
	}
}

// 13. If NATS uses a separate dispatch path, test invalid-subject quarantine there.
func TestSyncer_NATS_InvalidSubjectQuarantined(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "nats_invalid_subj.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	b := newTestBatch("batch-nats-badnode", "node.bad*id", 1, now)
	if err := store.PersistBatch(ctx, b); err != nil {
		t.Fatalf("PersistBatch failed: %v", err)
	}

	publisher := &mockPublisher{
		publishFunc: func(ctx context.Context, batch *types.TelemetryBatch) error {
			// buildSubject fails with ErrInvalidNodeID for nodes containing '.' or '*'
			return fmt.Errorf("%w: prohibited characters", ErrInvalidNodeID)
		},
	}

	syncer := NewSyncerWithPublisher(store, publisher, newDiscardLogger())

	stats, err := syncer.SyncPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("SyncPendingBatches failed: %v", err)
	}

	if stats.TotalQuarantined != 1 {
		t.Errorf("expected 1 quarantined batch for invalid NATS subject, got %d", stats.TotalQuarantined)
	}

	rec, err := store.GetBatch(ctx, "batch-nats-badnode")
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if rec.SyncStatus != storage.SyncStatusFailed {
		t.Errorf("expected status FAILED, got %s", rec.SyncStatus)
	}
	if !strings.Contains(rec.ErrorReason, "permanent sync failure") {
		t.Errorf("expected permanent failure reason, got %q", rec.ErrorReason)
	}
}

// 14. NATS transient failure below threshold remains PENDING.
func TestSyncer_NATS_TransientFailureBelowThreshold(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "nats_transient.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	b := newTestBatch("batch-nats-transient", "node-nats", 1, now)
	if err := store.PersistBatch(ctx, b); err != nil {
		t.Fatalf("PersistBatch failed: %v", err)
	}

	publisher := &mockPublisher{
		publishFunc: func(ctx context.Context, batch *types.TelemetryBatch) error {
			return ErrPublishNack
		},
	}

	syncer := NewSyncerWithPublisher(store, publisher, newDiscardLogger(), WithMaxSyncAttempts(5))

	// Cycle 1: fails transiently
	stats1, err := syncer.SyncPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("SyncPendingBatches cycle 1 failed: %v", err)
	}
	if stats1.TotalQuarantined != 0 || stats1.TotalFailed != 1 {
		t.Errorf("cycle 1: expected 0 quarantined, 1 failed, got %+v", stats1)
	}

	rec, err := store.GetBatch(ctx, "batch-nats-transient")
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if rec.SyncStatus != storage.SyncStatusPending {
		t.Errorf("expected status PENDING, got %s", rec.SyncStatus)
	}
	if rec.Attempt != 1 {
		t.Errorf("expected attempt 1, got %d", rec.Attempt)
	}
}

type failingAuditStore struct {
	storage.AuditStore
	err error
}

func (f *failingAuditStore) RecordAuditEvent(ctx context.Context, event *storage.StoredAuditEvent) error {
	if f.err != nil {
		return f.err
	}
	return f.AuditStore.RecordAuditEvent(ctx, event)
}

// 15. Audit-store failure does not undo quarantine or block subsequent batches.
func TestSyncer_Quarantine_AuditStoreFailureDoesNotUndoQuarantine(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "audit_fail_quarantine.db")
	realStore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer realStore.Close()

	now := time.Now().UTC()
	// Batch 1 (seq 1): poison batch (HTTP 400)
	b1 := &types.TelemetryBatch{
		BatchID:        "batch-auditfail-poison-1",
		NodeID:         "node-auditfail",
		SequenceNumber: 1,
		CollectedAt:    now,
		Metrics: []types.MetricSample{
			{Name: "cpu_usage", Value: 99.9, Unit: "percent", Timestamp: now},
		},
	}
	if err := realStore.PersistBatch(ctx, b1); err != nil {
		t.Fatalf("PersistBatch b1 failed: %v", err)
	}

	// Batch 2 (seq 2): healthy batch
	b2 := &types.TelemetryBatch{
		BatchID:        "batch-auditfail-valid-2",
		NodeID:         "node-auditfail",
		SequenceNumber: 2,
		CollectedAt:    now.Add(1 * time.Second),
		Metrics: []types.MetricSample{
			{Name: "cpu_usage", Value: 25.0, Unit: "percent", Timestamp: now.Add(1 * time.Second)},
		},
	}
	if err := realStore.PersistBatch(ctx, b2); err != nil {
		t.Fatalf("PersistBatch b2 failed: %v", err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b types.TelemetryBatch
		_ = json.NewDecoder(r.Body).Decode(&b)
		if b.BatchID == "batch-auditfail-poison-1" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"bad_schema"}`))
			return
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
	}))
	defer ts.Close()

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))

	sentinelAuditErr := errors.New("sentinel: disk full during audit event write")
	mockAudit := &failingAuditStore{
		AuditStore: realStore,
		err:        sentinelAuditErr,
	}

	client := NewHTTPClient(ts.URL, WithLogger(newDiscardLogger()))
	syncer := NewSyncer(realStore, client, logger, WithAuditStore(mockAudit))

	stats, err := syncer.SyncPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("SyncPendingBatches returned unexpected error: %v", err)
	}

	// Quarantine statistics must reflect successful quarantine, NOT audit write failure
	if stats.TotalQuarantined != 1 {
		t.Errorf("expected TotalQuarantined == 1, got %d", stats.TotalQuarantined)
	}
	if stats.TotalSynced != 1 {
		t.Errorf("expected TotalSynced == 1 (later valid batch synced), got %d", stats.TotalSynced)
	}
	if stats.TotalFailed != 0 {
		t.Errorf("expected TotalFailed == 0, got %d", stats.TotalFailed)
	}

	// Audit error must be observable through the established logger
	loggedOutput := logBuf.String()
	if !strings.Contains(loggedOutput, "failed to record audit event for quarantined batch") {
		t.Errorf("expected log to contain audit failure message, got:\n%s", loggedOutput)
	}
	if !strings.Contains(loggedOutput, sentinelAuditErr.Error()) {
		t.Errorf("expected log to contain sentinel audit error %q, got:\n%s", sentinelAuditErr.Error(), loggedOutput)
	}

	// Batch 1 must be durably quarantined in SQLite as FAILED (not rolled back)
	rec1, err := realStore.GetBatch(ctx, "batch-auditfail-poison-1")
	if err != nil {
		t.Fatalf("GetBatch b1 failed: %v", err)
	}
	if rec1.SyncStatus != storage.SyncStatusFailed {
		t.Errorf("expected batch 1 to be FAILED in storage, got %s", rec1.SyncStatus)
	}
	if rec1.FailedAt == nil {
		t.Error("expected failed_at to be populated")
	}
	if rec1.BatchID != "batch-auditfail-poison-1" || rec1.NodeID != "node-auditfail" || rec1.SequenceNumber != 1 {
		t.Errorf("batch 1 metadata corrupted: id=%s node=%s seq=%d", rec1.BatchID, rec1.NodeID, rec1.SequenceNumber)
	}
	if len(rec1.Payload.Metrics) != 1 || rec1.Payload.Metrics[0].Value != 99.9 {
		t.Errorf("batch 1 payload corrupted: %+v", rec1.Payload)
	}

	// Batch 2 must be SYNCED (head-of-line blocking prevented even with audit failure)
	rec2, err := realStore.GetBatch(ctx, "batch-auditfail-valid-2")
	if err != nil {
		t.Fatalf("GetBatch b2 failed: %v", err)
	}
	if rec2.SyncStatus != storage.SyncStatusSynced {
		t.Errorf("expected batch 2 to be SYNCED, got %s", rec2.SyncStatus)
	}
}

// 16. RequeueBatch error propagation and preservation of identity.
func TestSyncer_RequeueBatch_ErrorPropagationAndStateTransitions(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "requeue_errors.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	client := NewHTTPClient("http://127.0.0.1:59999", WithLogger(newDiscardLogger()))
	syncer := NewSyncer(store, client, newDiscardLogger(), WithAuditStore(store))

	// 1. Nonexistent batch returns ErrBatchNotFound via errors.Is
	errNonexistent := syncer.RequeueBatch(ctx, "nonexistent-batch-id")
	if errNonexistent == nil {
		t.Fatal("expected error requeuing nonexistent batch, got nil")
	}
	if !errors.Is(errNonexistent, storage.ErrBatchNotFound) {
		t.Errorf("expected errors.Is(err, storage.ErrBatchNotFound), got: %v", errNonexistent)
	}

	now := time.Now().UTC()
	bPending := &types.TelemetryBatch{
		BatchID:        "batch-requeue-pending",
		NodeID:         "node-rq",
		SequenceNumber: 10,
		CollectedAt:    now,
		Metrics: []types.MetricSample{
			{Name: "mem_used", Value: 4096.0, Unit: "bytes", Timestamp: now},
		},
	}
	if err := store.PersistBatch(ctx, bPending); err != nil {
		t.Fatalf("PersistBatch bPending failed: %v", err)
	}

	// 2. Existing batch in PENDING status returns ErrInvalidBatchTransition via errors.Is
	errPending := syncer.RequeueBatch(ctx, "batch-requeue-pending")
	if errPending == nil {
		t.Fatal("expected error requeuing PENDING batch, got nil")
	}
	if !errors.Is(errPending, storage.ErrInvalidBatchTransition) {
		t.Errorf("expected errors.Is(err, storage.ErrInvalidBatchTransition), got: %v", errPending)
	}

	// 3. Existing batch in SYNCED status returns ErrInvalidBatchTransition via errors.Is
	if err := store.MarkBatchSynced(ctx, "batch-requeue-pending", now); err != nil {
		t.Fatalf("MarkBatchSynced failed: %v", err)
	}
	errSynced := syncer.RequeueBatch(ctx, "batch-requeue-pending")
	if errSynced == nil {
		t.Fatal("expected error requeuing SYNCED batch, got nil")
	}
	if !errors.Is(errSynced, storage.ErrInvalidBatchTransition) {
		t.Errorf("expected errors.Is(err, storage.ErrInvalidBatchTransition), got: %v", errSynced)
	}

	// 4. Batch in FAILED status can be successfully requeued
	bFailed := &types.TelemetryBatch{
		BatchID:        "batch-requeue-failed",
		NodeID:         "node-rq",
		SequenceNumber: 11,
		CollectedAt:    now,
		Metrics: []types.MetricSample{
			{Name: "voltage", Value: 3.3, Unit: "volts", Timestamp: now},
			{Name: "temp_c", Value: 42.1, Unit: "celsius", Timestamp: now},
		},
	}
	if err := store.PersistBatch(ctx, bFailed); err != nil {
		t.Fatalf("PersistBatch bFailed failed: %v", err)
	}
	if err := store.MarkBatchQuarantined(ctx, "batch-requeue-failed", "permanent sync failure", now); err != nil {
		t.Fatalf("MarkBatchQuarantined failed: %v", err)
	}

	// Verify it is currently FAILED
	recBefore, err := store.GetBatch(ctx, "batch-requeue-failed")
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if recBefore.SyncStatus != storage.SyncStatusFailed {
		t.Fatalf("expected status FAILED, got %s", recBefore.SyncStatus)
	}

	// Requeue
	if err := syncer.RequeueBatch(ctx, "batch-requeue-failed"); err != nil {
		t.Fatalf("RequeueBatch failed on FAILED batch: %v", err)
	}

	// Verify post-requeue state and identity preservation
	recAfter, err := store.GetBatch(ctx, "batch-requeue-failed")
	if err != nil {
		t.Fatalf("GetBatch after requeue failed: %v", err)
	}
	if recAfter.SyncStatus != storage.SyncStatusPending {
		t.Errorf("expected status PENDING after requeue, got %s", recAfter.SyncStatus)
	}
	if recAfter.Attempt != 0 {
		t.Errorf("expected attempt 0 after requeue, got %d", recAfter.Attempt)
	}
	if recAfter.FailedAt != nil {
		t.Errorf("expected failed_at nil after requeue, got %v", recAfter.FailedAt)
	}
	if recAfter.ErrorReason != "" {
		t.Errorf("expected error_reason empty after requeue, got %q", recAfter.ErrorReason)
	}

	// Identity preservation
	if recAfter.BatchID != bFailed.BatchID {
		t.Errorf("batch_id modified: %s vs %s", recAfter.BatchID, bFailed.BatchID)
	}
	if recAfter.NodeID != bFailed.NodeID {
		t.Errorf("node_id modified: %s vs %s", recAfter.NodeID, bFailed.NodeID)
	}
	if recAfter.SequenceNumber != bFailed.SequenceNumber {
		t.Errorf("sequence_number modified: %d vs %d", recAfter.SequenceNumber, bFailed.SequenceNumber)
	}
	if !recAfter.CollectedAt.Equal(bFailed.CollectedAt) {
		t.Errorf("collected_at modified: %v vs %v", recAfter.CollectedAt, bFailed.CollectedAt)
	}
	if len(recAfter.Payload.Metrics) != 2 || recAfter.Payload.Metrics[0].Name != "voltage" || recAfter.Payload.Metrics[1].Name != "temp_c" {
		t.Errorf("payload modified after requeue: %+v", recAfter.Payload)
	}
}

// 17. Oversized NATS batch is quarantined immediately on attempt 1, allowing subsequent valid batch to sync.
func TestSyncer_NATS_OversizedBatchQuarantinedImmediately(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "nats_oversized.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	// Batch 1 (seq 1): oversized batch (publisher returns ErrMessageTooLarge)
	b1 := &types.TelemetryBatch{
		BatchID:        "batch-nats-oversized",
		NodeID:         "node-nats-size",
		SequenceNumber: 1,
		CollectedAt:    now,
		Metrics: []types.MetricSample{
			{Name: "sensor_burst", Value: 999.9, Unit: "raw", Timestamp: now},
		},
	}
	if err := store.PersistBatch(ctx, b1); err != nil {
		t.Fatalf("PersistBatch b1 failed: %v", err)
	}

	// Batch 2 (seq 2): healthy normal batch
	b2 := &types.TelemetryBatch{
		BatchID:        "batch-nats-valid-subsequent",
		NodeID:         "node-nats-size",
		SequenceNumber: 2,
		CollectedAt:    now.Add(1 * time.Second),
		Metrics: []types.MetricSample{
			{Name: "sensor_burst", Value: 12.3, Unit: "raw", Timestamp: now.Add(1 * time.Second)},
		},
	}
	if err := store.PersistBatch(ctx, b2); err != nil {
		t.Fatalf("PersistBatch b2 failed: %v", err)
	}

	publisher := &mockPublisher{
		publishFunc: func(ctx context.Context, batch *types.TelemetryBatch) error {
			if batch.BatchID == "batch-nats-oversized" {
				return fmt.Errorf("%w: payload size 1500000 exceeds limit 1048576", ErrMessageTooLarge)
			}
			return nil
		},
	}

	syncer := NewSyncerWithPublisher(store, publisher, newDiscardLogger(), WithMaxSyncAttempts(10))

	// Execute ONE sync cycle
	stats, err := syncer.SyncPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("SyncPendingBatches failed: %v", err)
	}

	// Batch 1 must be quarantined immediately on attempt 1 (TotalQuarantined = 1)
	if stats.TotalQuarantined != 1 {
		t.Errorf("expected 1 quarantined batch, got %d", stats.TotalQuarantined)
	}
	// Batch 2 must be published successfully in the same cycle (TotalSynced = 1, unblocked)
	if stats.TotalSynced != 1 {
		t.Errorf("expected 1 synced batch (subsequent unblocked), got %d", stats.TotalSynced)
	}
	if stats.TotalFailed != 0 {
		t.Errorf("expected 0 failed batches, got %d", stats.TotalFailed)
	}

	// Verify Batch 1 in storage
	rec1, err := store.GetBatch(ctx, "batch-nats-oversized")
	if err != nil {
		t.Fatalf("GetBatch b1 failed: %v", err)
	}
	if rec1.SyncStatus != storage.SyncStatusFailed {
		t.Errorf("expected b1 to be FAILED, got %s", rec1.SyncStatus)
	}
	if rec1.Attempt != 0 {
		t.Errorf("expected b1 attempt to remain 0 on immediate permanent quarantine, got %d", rec1.Attempt)
	}
	if !strings.Contains(rec1.ErrorReason, "exceeds limit") {
		t.Errorf("expected error_reason to mention size limit, got %q", rec1.ErrorReason)
	}
	if rec1.SequenceNumber != 1 || rec1.NodeID != "node-nats-size" {
		t.Errorf("b1 metadata corrupted: seq=%d node=%s", rec1.SequenceNumber, rec1.NodeID)
	}
	if len(rec1.Payload.Metrics) != 1 || rec1.Payload.Metrics[0].Value != 999.9 {
		t.Errorf("b1 payload corrupted: %+v", rec1.Payload)
	}

	// Verify Batch 2 in storage
	rec2, err := store.GetBatch(ctx, "batch-nats-valid-subsequent")
	if err != nil {
		t.Fatalf("GetBatch b2 failed: %v", err)
	}
	if rec2.SyncStatus != storage.SyncStatusPublished {
		t.Errorf("expected b2 to be PUBLISHED, got %s", rec2.SyncStatus)
	}
}

// 18. Audit correlation by batch ID: verifies that both quarantine and requeue events
// share CorrelationID = batchID, queryable via ListAuditEventsByCorrelation.
func TestSyncer_AuditCorrelation_BatchID(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "audit_corr.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	targetBatchID := "batch-corr-lifecycle-99"
	b := newTestBatch(targetBatchID, "node-corr", 1, now)
	if err := store.PersistBatch(ctx, b); err != nil {
		t.Fatalf("PersistBatch failed: %v", err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"permanent_schema_failure"}`))
	}))
	defer ts.Close()

	client := NewHTTPClient(ts.URL, WithLogger(newDiscardLogger()))
	syncer := NewSyncer(store, client, newDiscardLogger(), WithAuditStore(store))

	// 1. Trigger Quarantine
	stats, err := syncer.SyncPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("SyncPendingBatches failed: %v", err)
	}
	if stats.TotalQuarantined != 1 {
		t.Fatalf("expected 1 quarantined, got %d", stats.TotalQuarantined)
	}

	// 2. Trigger Requeue
	if err := syncer.RequeueBatch(ctx, targetBatchID); err != nil {
		t.Fatalf("RequeueBatch failed: %v", err)
	}

	// 3. Query audit events using ListAuditEventsByCorrelation(targetBatchID)
	corrEvents, err := store.ListAuditEventsByCorrelation(ctx, targetBatchID, 50)
	if err != nil {
		t.Fatalf("ListAuditEventsByCorrelation failed: %v", err)
	}
	if len(corrEvents) != 2 {
		t.Fatalf("expected exactly 2 correlated audit events for %s, got %d", targetBatchID, len(corrEvents))
	}

	// Event 1 must be TELEMETRY_BATCH_QUARANTINED
	evt1 := corrEvents[0]
	if evt1.EventType != "TELEMETRY_BATCH_QUARANTINED" {
		t.Errorf("expected event 1 type TELEMETRY_BATCH_QUARANTINED, got %s", evt1.EventType)
	}
	if evt1.CorrelationID != targetBatchID {
		t.Errorf("expected event 1 CorrelationID %s, got %s", targetBatchID, evt1.CorrelationID)
	}
	if evt1.IncidentID != fmt.Sprintf("quarantine-%s", targetBatchID) {
		t.Errorf("expected event 1 IncidentID quarantine-%s, got %s", targetBatchID, evt1.IncidentID)
	}

	// Event 2 must be TELEMETRY_BATCH_REQUEUED
	evt2 := corrEvents[1]
	if evt2.EventType != "TELEMETRY_BATCH_REQUEUED" {
		t.Errorf("expected event 2 type TELEMETRY_BATCH_REQUEUED, got %s", evt2.EventType)
	}
	if evt2.CorrelationID != targetBatchID {
		t.Errorf("expected event 2 CorrelationID %s, got %s", targetBatchID, evt2.CorrelationID)
	}
	if evt2.IncidentID != fmt.Sprintf("requeue-%s", targetBatchID) {
		t.Errorf("expected event 2 IncidentID requeue-%s, got %s", targetBatchID, evt2.IncidentID)
	}
}

// 19. truncateReason bounds, ASCII, multibyte UTF-8, and emoji safety near boundary.
func TestTruncateReason_BoundsAndUnicodeSafety(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		maxBytes int
		wantMax  int
		wantEnd  string
	}{
		{
			name:     "short ascii within limit",
			input:    "simple error",
			maxBytes: 100,
			wantMax:  100,
			wantEnd:  "simple error",
		},
		{
			name:     "exact limit ascii",
			input:    "12345",
			maxBytes: 5,
			wantMax:  5,
			wantEnd:  "12345",
		},
		{
			name:     "long ascii exceeds limit",
			input:    strings.Repeat("a", 1500),
			maxBytes: 1024,
			wantMax:  1024,
			wantEnd:  "...",
		},
		{
			name:     "zero limit returns empty",
			input:    "some text",
			maxBytes: 0,
			wantMax:  0,
			wantEnd:  "",
		},
		{
			name:     "negative limit returns empty",
			input:    "some text",
			maxBytes: -5,
			wantMax:  0,
			wantEnd:  "",
		},
		{
			name:     "small limit below suffix length",
			input:    "abcdefgh",
			maxBytes: 2,
			wantMax:  2,
			wantEnd:  "ab",
		},
		{
			name: "japanese multibyte near boundary does not split rune",
			// "エラー詳細：" (each kanji/fullwidth character is 3 bytes)
			// 'エ'(3), 'ラ'(3), 'ー'(3), '詳'(3), '細'(3), '：'(3) = 18 bytes
			input:    "エラー詳細：スキーマ不適合による恒久的是正が必要です",
			maxBytes: 10, // limit = 10 - 3 = 7 -> fits 2 runes (6 bytes) + "..." = 9 bytes
			wantMax:  10,
			wantEnd:  "...",
		},
		{
			name: "emoji 4-byte near boundary does not split rune",
			// "Alert: 🚨🚨🚨" -> '🚨' is 4 bytes
			input:    "Alert: 🚨🚨🚨",
			maxBytes: 12, // limit = 12 - 3 = 9. "Alert: " is 7 bytes. 7 + 4 = 11 > 9, so stops at "Alert: " + "..." = 10 bytes
			wantMax:  12,
			wantEnd:  "...",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := truncateReason(tc.input, tc.maxBytes)
			if len(got) > tc.wantMax {
				t.Errorf("len(got) = %d exceeds maxBytes %d", len(got), tc.wantMax)
			}
			if !utf8.ValidString(got) {
				t.Errorf("got invalid UTF-8 string: %q", got)
			}
			if tc.wantEnd != "" && !strings.HasSuffix(got, tc.wantEnd) {
				t.Errorf("expected suffix %q, got %q", tc.wantEnd, got)
			}
		})
	}
}

// 20. End-to-end quarantine with 2500-byte error reason:
// reason is safely bounded to 1024 bytes and stored in AuditStore without validation failure.
func TestSyncer_Quarantine_LongReasonBoundedAndPersisted(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "audit_long_reason.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	batchID := "batch-long-reason-01"
	b := newTestBatch(batchID, "node-long-reason", 1, now)
	if err := store.PersistBatch(ctx, b); err != nil {
		t.Fatalf("PersistBatch failed: %v", err)
	}

	// 2500-byte error message with Unicode characters
	longErrMsg := "proxy upstream gateway rejection: " + strings.Repeat("詳細エラー項目_", 200)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(longErrMsg))
	}))
	defer ts.Close()

	client := NewHTTPClient(ts.URL, WithLogger(newDiscardLogger()))
	syncer := NewSyncer(store, client, newDiscardLogger(), WithAuditStore(store))

	stats, err := syncer.SyncPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("SyncPendingBatches failed: %v", err)
	}
	if stats.TotalQuarantined != 1 {
		t.Fatalf("expected 1 quarantined, got %d", stats.TotalQuarantined)
	}

	// Audit event must be recorded successfully (not dropped due to length validation)
	events, err := store.ListAuditEventsByCorrelation(ctx, batchID, 10)
	if err != nil {
		t.Fatalf("ListAuditEventsByCorrelation failed: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 audit event for %s, got %d", batchID, len(events))
	}

	evt := events[0]
	if len(evt.Reason) > storage.MaxAuditReasonLength {
		t.Errorf("audit event reason length %d exceeds MaxAuditReasonLength %d", len(evt.Reason), storage.MaxAuditReasonLength)
	}
	if !utf8.ValidString(evt.Reason) {
		t.Errorf("audit event reason is not valid UTF-8: %q", evt.Reason)
	}
	if !strings.HasSuffix(evt.Reason, "...") {
		t.Errorf("expected truncated reason to end with '...', got: %s", evt.Reason)
	}
}

// 21. Requeue audit-store failure is non-blocking and does not undo the requeue transition.
func TestSyncer_Requeue_AuditStoreFailure_DoesNotUndoRequeue(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "requeue_audit_fail.db")
	realStore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer realStore.Close()

	now := time.Now().UTC()
	b := newTestBatch("batch-rq-audit-fail", "node-rq-fail", 1, now)
	if err := realStore.PersistBatch(ctx, b); err != nil {
		t.Fatalf("PersistBatch failed: %v", err)
	}
	if err := realStore.MarkBatchQuarantined(ctx, "batch-rq-audit-fail", "initial failure", now); err != nil {
		t.Fatalf("MarkBatchQuarantined failed: %v", err)
	}

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))

	sentinelAuditErr := errors.New("audit store disk I/O error during requeue")
	mockAudit := &failingAuditStore{
		AuditStore: realStore,
		err:        sentinelAuditErr,
	}

	client := NewHTTPClient("http://127.0.0.1:59999", WithLogger(newDiscardLogger()))
	syncer := NewSyncer(realStore, client, logger, WithAuditStore(mockAudit))

	// Requeue should return nil (success in SQLite), despite audit store failure
	if err := syncer.RequeueBatch(ctx, "batch-rq-audit-fail"); err != nil {
		t.Fatalf("RequeueBatch failed unexpectedly: %v", err)
	}

	// Verify batch is in PENDING status in SQLite
	rec, err := realStore.GetBatch(ctx, "batch-rq-audit-fail")
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if rec.SyncStatus != storage.SyncStatusPending {
		t.Errorf("expected batch status PENDING, got %s", rec.SyncStatus)
	}
	if rec.Attempt != 0 {
		t.Errorf("expected attempt 0, got %d", rec.Attempt)
	}

	// Verify error was logged
	logged := logBuf.String()
	if !strings.Contains(logged, "failed to record audit event for requeued batch") {
		t.Errorf("expected log to contain audit error warning, got:\n%s", logged)
	}
}

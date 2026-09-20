package sync

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// TestSyncer_FailureSafetyMatrix explicitly tests item 4 of the audit:
// HTTP timeout → PENDING
// connection refused → PENDING
// HTTP 5xx → PENDING
// HTTP 4xx validation failure → PENDING
// Confirming that no failed sync deletes the batch or marks it SYNCED.
func TestSyncer_FailureSafetyMatrix(t *testing.T) {
	testCases := []struct {
		name       string
		setupMock  func() (string, func())
		clientOpts []HTTPClientOption
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
			if stats.TotalFailed != 1 {
				t.Errorf("expected 1 failed batch, got %d", stats.TotalFailed)
			}

			// Invariant 1: Local batch MUST NOT be deleted (count remains 1)
			count, err := store.CountBatches(ctx)
			if err != nil {
				t.Fatalf("CountBatches failed: %v", err)
			}
			if count != 1 {
				t.Errorf("expected count to remain 1, got %d", count)
			}

			// Invariant 2: Local batch MUST NOT be marked SYNCED (remains PENDING)
			rec, err := store.GetBatch(ctx, batchID)
			if err != nil {
				t.Fatalf("GetBatch failed: %v", err)
			}
			if rec.SyncStatus != storage.SyncStatusPending {
				t.Errorf("expected status PENDING after failure, got %s", rec.SyncStatus)
			}

			// Invariant 3: Persisted attempt counter incremented to 1
			if rec.Attempt != 1 {
				t.Errorf("expected attempt 1, got %d", rec.Attempt)
			}
		})
	}
}

package sync_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	edgesync "github.com/Chamalka-heshi/AegisEdge/edge/agent/sync"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

func newTestTelemetryBatch(batchID, nodeID string, seq int64, t time.Time) *types.TelemetryBatch {
	return &types.TelemetryBatch{
		BatchID:        batchID,
		NodeID:         nodeID,
		SequenceNumber: seq,
		CollectedAt:    t,
		Metrics: []types.MetricSample{
			{Name: "cpu_usage", Value: 25.5, Timestamp: t},
			{Name: "memory_usage", Value: 50.0, Timestamp: t},
		},
	}
}

// TestOfflineFirst_AuthAndAvailabilityFailuresPreserveLocalStorage verifies Section 7 invariants:
// When control-plane authentication fails (HTTP 401/403) or server is unreachable:
// 1. Telemetry in SQLite WAL is NEVER deleted or corrupted.
// 2. Pending batches remain PENDING.
// 3. Retry attempts are recorded in SQLite WAL.
// 4. Per-node sequence order is preserved without dropping gaps.
func TestOfflineFirst_AuthAndAvailabilityFailuresPreserveLocalStorage(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name       string
		statusCode int
		handler    http.HandlerFunc
	}{
		{
			name:       "AuthFailure_401_Unauthorized",
			statusCode: http.StatusUnauthorized,
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
			},
		},
		{
			name:       "AuthFailure_403_Forbidden",
			statusCode: http.StatusForbidden,
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusForbidden)
			},
		},
		{
			name:       "ControlPlane_503_Unavailable",
			statusCode: http.StatusServiceUnavailable,
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusServiceUnavailable)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), tc.name+".db")
			store, err := storage.OpenSQLite(dbPath)
			if err != nil {
				t.Fatalf("OpenSQLite failed: %v", err)
			}
			defer store.Close()

			now := time.Now().UTC()
			batch1 := newTestTelemetryBatch("batch-offline-1", "node-edge-1", 1, now)
			batch2 := newTestTelemetryBatch("batch-offline-2", "node-edge-1", 2, now.Add(1*time.Second))

			if err := store.PersistBatch(ctx, batch1); err != nil {
				t.Fatalf("PersistBatch 1 failed: %v", err)
			}
			if err := store.PersistBatch(ctx, batch2); err != nil {
				t.Fatalf("PersistBatch 2 failed: %v", err)
			}

			server := httptest.NewServer(tc.handler)
			defer server.Close()

			client := edgesync.NewHTTPClient(server.URL,
				edgesync.WithMaxRetries(1),
				edgesync.WithBaseBackoff(1*time.Millisecond),
			)
			syncer := edgesync.NewSyncer(store, client, nil)

			stats, err := syncer.SyncPendingBatches(ctx, 10)
			if err != nil {
				t.Fatalf("SyncPendingBatches returned unexpected error: %v", err)
			}

			if stats.TotalSynced != 0 {
				t.Errorf("expected 0 synced batches during failure, got %d", stats.TotalSynced)
			}
			if stats.TotalFailed != 1 {
				t.Errorf("expected 1 failed batch (halted after seq 1 failure), got %d", stats.TotalFailed)
			}

			// Invariant 1: Local SQLite record for batch 1 is intact, marked PENDING, attempt=1
			rec1, err := store.GetBatch(ctx, "batch-offline-1")
			if err != nil {
				t.Fatalf("GetBatch 1 failed: %v", err)
			}
			if rec1.SyncStatus != storage.SyncStatusPending {
				t.Errorf("expected batch 1 to remain PENDING, got %v", rec1.SyncStatus)
			}
			if rec1.Attempt != 1 {
				t.Errorf("expected attempt 1 for batch 1, got %d", rec1.Attempt)
			}

			// Invariant 2: Local SQLite record for batch 2 was NOT attempted (attempt=0) to prevent sequence gap
			rec2, err := store.GetBatch(ctx, "batch-offline-2")
			if err != nil {
				t.Fatalf("GetBatch 2 failed: %v", err)
			}
			if rec2.SyncStatus != storage.SyncStatusPending {
				t.Errorf("expected batch 2 to remain PENDING, got %v", rec2.SyncStatus)
			}
			if rec2.Attempt != 0 {
				t.Errorf("expected attempt 0 for batch 2 (sequence order preserved), got %d", rec2.Attempt)
			}

			// Invariant 3: Total batch count in SQLite remains 2 (zero data loss)
			totalCount, err := store.CountBatches(ctx)
			if err != nil {
				t.Fatalf("CountBatches failed: %v", err)
			}
			if totalCount != 2 {
				t.Errorf("CRITICAL: batches were dropped or deleted; expected count 2, got %d", totalCount)
			}
		})
	}
}

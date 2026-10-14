package storage

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

func newTestBatch(batchID string, nodeID string, seq int64, now time.Time) *types.TelemetryBatch {
	return &types.TelemetryBatch{
		BatchID:        batchID,
		NodeID:         nodeID,
		SequenceNumber: seq,
		CollectedAt:    now,
		Metrics: []types.MetricSample{
			{Name: "cpu_usage_percent", Value: 45.2, Unit: "percent", Timestamp: now},
			{Name: "memory_usage_percent", Value: 68.1, Unit: "percent", Timestamp: now},
		},
	}
}

// TestSQLite_Idempotency verifies that attempting to persist the same BatchID twice
// returns ErrDuplicateBatch and does NOT create duplicate logical records.
func TestSQLite_Idempotency(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "idempotency_test.db")

	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	batch := newTestBatch("batch-uuid-unique-1", "node-1", 1, now)

	// First insert: must succeed
	if err := store.PersistBatch(ctx, batch); err != nil {
		t.Fatalf("first PersistBatch failed: %v", err)
	}

	// Verify total count is 1
	count, err := store.CountBatches(ctx)
	if err != nil {
		t.Fatalf("CountBatches failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 batch, got %d", count)
	}

	// Second insert with identical BatchID: must fail with ErrDuplicateBatch
	err = store.PersistBatch(ctx, batch)
	if !errors.Is(err, ErrDuplicateBatch) {
		t.Fatalf("expected ErrDuplicateBatch, got %v", err)
	}

	// Verify count remains strictly 1 (no duplicate record created)
	count, err = store.CountBatches(ctx)
	if err != nil {
		t.Fatalf("CountBatches failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected count to remain 1, got %d", count)
	}
}

// TestSQLite_OfflineFirstDurabilityAndReopening demonstrates that the edge operates
// completely independently of the control plane, storing multiple batches locally in WAL mode
// and verifying that all data survives a complete application/connection restart.
// Note: This test verifies SQLite persistence across application/process restarts; it does
// not simulate physical power loss or OS crash scenarios.
func TestSQLite_OfflineFirstDurabilityAndReopening(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "offline_durability.db")
	now := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
	nodeID := "edge-gateway-alpha"

	// 1. Start edge storage (Control plane is completely offline/non-existent)
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}

	// 2. Persist 5 sequential batches locally
	const batchCount = 5
	for i := int64(1); i <= batchCount; i++ {
		batch := newTestBatch(
			filepath.Join("batch", string(rune('A'+i))),
			nodeID,
			i,
			now.Add(time.Duration(i)*time.Second),
		)
		if err := store.PersistBatch(ctx, batch); err != nil {
			t.Fatalf("PersistBatch failed on batch %d: %v", i, err)
		}
	}

	// 3. Confirm all 5 batches are present with status PENDING
	pending, err := store.GetPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("GetPendingBatches failed: %v", err)
	}
	if len(pending) != batchCount {
		t.Fatalf("expected %d pending batches, got %d", batchCount, len(pending))
	}
	for i, rec := range pending {
		expectedSeq := int64(i + 1)
		if rec.SequenceNumber != expectedSeq {
			t.Errorf("batch %d sequence mismatch: got %d, want %d", i, rec.SequenceNumber, expectedSeq)
		}
		if rec.SyncStatus != SyncStatusPending {
			t.Errorf("batch %d status mismatch: got %s, want PENDING", i, rec.SyncStatus)
		}
	}

	// 4. Simulate application termination / restart: close the store
	if err := store.Close(); err != nil {
		t.Fatalf("store.Close failed: %v", err)
	}

	// 5. Reopen storage from the exact same database file
	reopenedStore, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("reopening store failed: %v", err)
	}
	defer reopenedStore.Close()

	// 6. Verify data integrity across restart
	count, err := reopenedStore.CountBatches(ctx)
	if err != nil {
		t.Fatalf("reopened CountBatches failed: %v", err)
	}
	if count != batchCount {
		t.Fatalf("reopened count mismatch: expected %d, got %d", batchCount, count)
	}

	// 7. Verify latest sequence number restored accurately
	latestSeq, err := reopenedStore.GetLatestSequenceNumber(ctx, nodeID)
	if err != nil {
		t.Fatalf("reopened GetLatestSequenceNumber failed: %v", err)
	}
	if latestSeq != batchCount {
		t.Fatalf("expected latest sequence %d, got %d", batchCount, latestSeq)
	}

	// 8. Retrieve specific record to ensure payload unmarshaling succeeds
	rec, err := reopenedStore.GetBatch(ctx, filepath.Join("batch", string(rune('A'+1))))
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if rec.NodeID != nodeID || rec.SequenceNumber != 1 {
		t.Errorf("record mismatch: %+v", rec)
	}
	if len(rec.Payload.Metrics) != 2 {
		t.Errorf("expected 2 metrics in payload, got %d", len(rec.Payload.Metrics))
	}
}

// TestSQLite_FailureCases covers invalid input, missing records, closed store errors, and empty store.
func TestSQLite_FailureCases(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "failures_test.db")

	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}

	// 1. Query non-existent batch
	_, err = store.GetBatch(ctx, "non-existent-batch-id")
	if !errors.Is(err, ErrBatchNotFound) {
		t.Fatalf("expected ErrBatchNotFound, got %v", err)
	}

	// 2. Query empty database for latest sequence
	latestSeq, err := store.GetLatestSequenceNumber(ctx, "unknown-node")
	if err != nil {
		t.Fatalf("GetLatestSequenceNumber failed: %v", err)
	}
	if latestSeq != -1 {
		t.Fatalf("expected sequence -1 for empty node, got %d", latestSeq)
	}

	// 3. Persist nil batch
	if err := store.PersistBatch(ctx, nil); !errors.Is(err, ErrInvalidBatch) {
		t.Fatalf("expected ErrInvalidBatch for nil batch, got %v", err)
	}

	// 4. Persist batch with invalid domain data (empty BatchID)
	invalidBatch := &types.TelemetryBatch{
		BatchID:        "",
		NodeID:         "node-1",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics: []types.MetricSample{
			{Name: "cpu", Value: 10, Timestamp: time.Now().UTC()},
		},
	}
	if err := store.PersistBatch(ctx, invalidBatch); !errors.Is(err, ErrInvalidBatch) {
		t.Fatalf("expected ErrInvalidBatch for empty BatchID, got %v", err)
	}

	// 5. Operations after store is closed must return ErrStoreClosed
	if err := store.Close(); err != nil {
		t.Fatalf("store.Close failed: %v", err)
	}

	validBatch := newTestBatch("batch-post-close", "node-1", 1, time.Now().UTC())
	if err := store.PersistBatch(ctx, validBatch); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("expected ErrStoreClosed, got %v", err)
	}
	if _, err := store.GetBatch(ctx, "any"); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("expected ErrStoreClosed on GetBatch, got %v", err)
	}
	if _, err := store.GetPendingBatches(ctx, 10); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("expected ErrStoreClosed on GetPendingBatches, got %v", err)
	}
	if _, err := store.GetLatestSequenceNumber(ctx, "node-1"); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("expected ErrStoreClosed on GetLatestSequenceNumber, got %v", err)
	}
	if _, err := store.CountBatches(ctx); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("expected ErrStoreClosed on CountBatches, got %v", err)
	}
}

// TestSQLite_PragmasConfigured verifies that the SQLite connection has WAL mode,
// synchronous=NORMAL (value 1), busy_timeout=5000, and foreign_keys=ON (value 1) actively configured.
func TestSQLite_PragmasConfigured(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "pragma_check.db")
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	// 1. Check journal_mode
	var journalMode string
	if err := store.db.QueryRow("PRAGMA journal_mode;").Scan(&journalMode); err != nil {
		t.Fatalf("query journal_mode failed: %v", err)
	}
	if strings.ToLower(journalMode) != "wal" {
		t.Errorf("expected journal_mode 'wal', got %q", journalMode)
	}

	// 2. Check synchronous (0 = OFF, 1 = NORMAL, 2 = FULL, 3 = EXTRA)
	var syncMode int
	if err := store.db.QueryRow("PRAGMA synchronous;").Scan(&syncMode); err != nil {
		t.Fatalf("query synchronous failed: %v", err)
	}
	if syncMode != 1 {
		t.Errorf("expected synchronous mode 1 (NORMAL), got %d", syncMode)
	}

	// 3. Check busy_timeout
	var busyTimeout int
	if err := store.db.QueryRow("PRAGMA busy_timeout;").Scan(&busyTimeout); err != nil {
		t.Fatalf("query busy_timeout failed: %v", err)
	}
	if busyTimeout != 5000 {
		t.Errorf("expected busy_timeout 5000, got %d", busyTimeout)
	}

	// 4. Check foreign_keys
	var foreignKeys int
	if err := store.db.QueryRow("PRAGMA foreign_keys;").Scan(&foreignKeys); err != nil {
		t.Fatalf("query foreign_keys failed: %v", err)
	}
	if foreignKeys != 1 {
		t.Errorf("expected foreign_keys 1 (ON), got %d", foreignKeys)
	}
}

// TestSQLite_MarkBatchSynced verifies that MarkBatchSynced updates status to SYNCED and captures sent_at.
func TestSQLite_MarkBatchSynced(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "sync_test.db")
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	batch := newTestBatch("batch-sync-1", "node-1", 1, now)
	if err := store.PersistBatch(ctx, batch); err != nil {
		t.Fatalf("PersistBatch failed: %v", err)
	}

	sentAt := now.Add(500 * time.Millisecond)
	if err := store.MarkBatchSynced(ctx, "batch-sync-1", sentAt); err != nil {
		t.Fatalf("MarkBatchSynced failed: %v", err)
	}

	rec, err := store.GetBatch(ctx, "batch-sync-1")
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if rec.SyncStatus != SyncStatusSynced {
		t.Errorf("expected SYNCED, got %s", rec.SyncStatus)
	}
	if rec.SentAt == nil || !rec.SentAt.Equal(sentAt) {
		t.Errorf("expected sent_at %v, got %v", sentAt, rec.SentAt)
	}

	// Should not appear in pending batches anymore
	pending, err := store.GetPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("GetPendingBatches failed: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("expected 0 pending batches, got %d", len(pending))
	}

	// Non-existent batch should return ErrBatchNotFound
	if err := store.MarkBatchSynced(ctx, "non-existent", sentAt); !errors.Is(err, ErrBatchNotFound) {
		t.Errorf("expected ErrBatchNotFound, got %v", err)
	}
}

// TestSQLite_RecordSyncAttempt verifies that RecordSyncAttempt increments attempt count and leaves status as PENDING.
func TestSQLite_RecordSyncAttempt(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "attempt_test.db")
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	batch := newTestBatch("batch-attempt-1", "node-1", 1, now)
	if err := store.PersistBatch(ctx, batch); err != nil {
		t.Fatalf("PersistBatch failed: %v", err)
	}

	sentAt1 := now.Add(1 * time.Second)
	if err := store.RecordSyncAttempt(ctx, "batch-attempt-1", sentAt1); err != nil {
		t.Fatalf("RecordSyncAttempt 1 failed: %v", err)
	}

	rec, err := store.GetBatch(ctx, "batch-attempt-1")
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if rec.Attempt != 1 {
		t.Errorf("expected attempt 1, got %d", rec.Attempt)
	}
	if rec.SyncStatus != SyncStatusPending {
		t.Errorf("expected status PENDING after failed attempt, got %s", rec.SyncStatus)
	}

	// Second failed sync cycle
	sentAt2 := now.Add(2 * time.Second)
	if err := store.RecordSyncAttempt(ctx, "batch-attempt-1", sentAt2); err != nil {
		t.Fatalf("RecordSyncAttempt 2 failed: %v", err)
	}

	rec, err = store.GetBatch(ctx, "batch-attempt-1")
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if rec.Attempt != 2 {
		t.Errorf("expected attempt 2, got %d", rec.Attempt)
	}
	if rec.SyncStatus != SyncStatusPending {
		t.Errorf("expected status PENDING, got %s", rec.SyncStatus)
	}

	// Non-existent batch should return ErrBatchNotFound
	if err := store.RecordSyncAttempt(ctx, "non-existent", sentAt2); !errors.Is(err, ErrBatchNotFound) {
		t.Errorf("expected ErrBatchNotFound, got %v", err)
	}
}

// TestSQLite_PerNodePendingQueries verifies distinct node retrieval and per-node sequence ordering.
func TestSQLite_PerNodePendingQueries(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "per_node_test.db")
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	// Insert out-of-order across nodes
	_ = store.PersistBatch(ctx, newTestBatch("batch-b-1", "node-B", 1, now.Add(1*time.Second)))
	_ = store.PersistBatch(ctx, newTestBatch("batch-a-2", "node-A", 2, now.Add(2*time.Second)))
	_ = store.PersistBatch(ctx, newTestBatch("batch-a-1", "node-A", 1, now))
	_ = store.PersistBatch(ctx, newTestBatch("batch-b-2", "node-B", 2, now.Add(3*time.Second)))

	nodes, err := store.GetPendingNodes(ctx)
	if err != nil {
		t.Fatalf("GetPendingNodes failed: %v", err)
	}
	if len(nodes) != 2 || nodes[0] != "node-A" || nodes[1] != "node-B" {
		t.Fatalf("expected ['node-A', 'node-B'], got %v", nodes)
	}

	// Get Node-A pending batches: must be strictly ordered by sequence (1 then 2)
	pendingA, err := store.GetPendingBatchesByNode(ctx, "node-A", 10)
	if err != nil {
		t.Fatalf("GetPendingBatchesByNode A failed: %v", err)
	}
	if len(pendingA) != 2 {
		t.Fatalf("expected 2 pending batches for Node A, got %d", len(pendingA))
	}
	if pendingA[0].SequenceNumber != 1 || pendingA[1].SequenceNumber != 2 {
		t.Errorf("Node A ordering incorrect: seq %d, then seq %d", pendingA[0].SequenceNumber, pendingA[1].SequenceNumber)
	}

	// Get Node-B pending batches
	pendingB, err := store.GetPendingBatchesByNode(ctx, "node-B", 10)
	if err != nil {
		t.Fatalf("GetPendingBatchesByNode B failed: %v", err)
	}
	if len(pendingB) != 2 {
		t.Fatalf("expected 2 pending batches for Node B, got %d", len(pendingB))
	}
	if pendingB[0].SequenceNumber != 1 || pendingB[1].SequenceNumber != 2 {
		t.Errorf("Node B ordering incorrect: seq %d, then seq %d", pendingB[0].SequenceNumber, pendingB[1].SequenceNumber)
	}
}

// TestSQLite_MarkBatchPublished verifies the PUBLISHED sync status for NATS JetStream.
func TestSQLite_MarkBatchPublished(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "published.db")
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	batch := newTestBatch("batch-pub-1", "node-pub", 1, now)
	if err := store.PersistBatch(ctx, batch); err != nil {
		t.Fatalf("PersistBatch failed: %v", err)
	}

	// Verify initial status is PENDING
	rec, err := store.GetBatch(ctx, "batch-pub-1")
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if rec.SyncStatus != SyncStatusPending {
		t.Fatalf("expected PENDING, got %s", rec.SyncStatus)
	}

	// Mark as PUBLISHED
	publishedAt := now.Add(10 * time.Second)
	if err := store.MarkBatchPublished(ctx, "batch-pub-1", publishedAt); err != nil {
		t.Fatalf("MarkBatchPublished failed: %v", err)
	}

	// Verify status is now PUBLISHED
	rec, err = store.GetBatch(ctx, "batch-pub-1")
	if err != nil {
		t.Fatalf("GetBatch after publish failed: %v", err)
	}
	if rec.SyncStatus != SyncStatusPublished {
		t.Errorf("expected PUBLISHED, got %s", rec.SyncStatus)
	}
}

// TestSQLite_PublishedExcludedFromPending verifies that PUBLISHED batches
// are not returned by GetPendingBatches or GetPendingBatchesByNode.
func TestSQLite_PublishedExcludedFromPending(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "published_excluded.db")
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()

	// Persist 3 batches
	for i := int64(1); i <= 3; i++ {
		batch := newTestBatch("batch-excl-"+string(rune('0'+i)), "node-excl", i, now.Add(time.Duration(i)*time.Second))
		_ = store.PersistBatch(ctx, batch)
	}

	// Mark batch 2 as PUBLISHED
	_ = store.MarkBatchPublished(ctx, "batch-excl-1", now)

	// Mark batch 3 as SYNCED (HTTP path)
	_ = store.MarkBatchSynced(ctx, "batch-excl-2", now)

	// Only batch 3 (seq=3) should be pending
	pending, err := store.GetPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("GetPendingBatches failed: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending, got %d", len(pending))
	}
	if pending[0].BatchID != "batch-excl-3" {
		t.Errorf("expected batch-excl-3 as pending, got %s", pending[0].BatchID)
	}

	// GetPendingBatchesByNode should also exclude PUBLISHED
	pendingByNode, err := store.GetPendingBatchesByNode(ctx, "node-excl", 10)
	if err != nil {
		t.Fatalf("GetPendingBatchesByNode failed: %v", err)
	}
	if len(pendingByNode) != 1 {
		t.Fatalf("expected 1 pending by node, got %d", len(pendingByNode))
	}

	// GetPendingNodes should still return node-excl (because batch 3 is still pending)
	nodes, err := store.GetPendingNodes(ctx)
	if err != nil {
		t.Fatalf("GetPendingNodes failed: %v", err)
	}
	if len(nodes) != 1 || nodes[0] != "node-excl" {
		t.Errorf("expected ['node-excl'], got %v", nodes)
	}
}

// TestSQLite_MarkBatchPublished_NotFound verifies error for non-existent batch.
func TestSQLite_MarkBatchPublished_NotFound(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "pub_notfound.db")
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	err = store.MarkBatchPublished(ctx, "nonexistent-batch", time.Now().UTC())
	if !errors.Is(err, ErrBatchNotFound) {
		t.Errorf("expected ErrBatchNotFound, got %v", err)
	}
}

// TestSQLite_MigrationV2_PublishedAt verifies that migration v2 adds the published_at column.
func TestSQLite_MigrationV2_PublishedAt(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "migration_v2.db")
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	// Verify schema_migrations has version 2
	var maxVersion int
	err = store.db.QueryRowContext(ctx, "SELECT MAX(version) FROM schema_migrations;").Scan(&maxVersion)
	if err != nil {
		t.Fatalf("failed to query schema version: %v", err)
	}
	if maxVersion < 2 {
		t.Errorf("expected schema version >= 2, got %d", maxVersion)
	}

	// Verify published_at column exists by querying it
	_, err = store.db.ExecContext(ctx, "SELECT published_at FROM telemetry_batches LIMIT 1;")
	if err != nil {
		t.Errorf("published_at column query failed: %v", err)
	}
}

// TestSQLite_PruneCompletedBatches verifies all retention and sequence-preservation contracts:
// 1. Old SYNCED records are pruned.
// 2. Old PUBLISHED records are pruned.
// 3. Recent completed records are retained.
// 4. Old PENDING records are retained.
// 5. Old FAILED records are retained.
// 6. Prune limit is respected.
// 7. Latest sequence number for each node is permanently preserved.
// 8. Monotonic sequence continuity survives close, reopen, and restart.
func TestSQLite_PruneCompletedBatches(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "prune_test.db")
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	oldTime := now.Add(-48 * time.Hour)
	recentTime := now.Add(-10 * time.Minute)
	cutoff := now.Add(-24 * time.Hour)

	// Node 1:
	// batch-1: old SYNCED (seq 1) -> eligible for prune
	// batch-2: old SYNCED (seq 2) -> eligible for prune
	// batch-3: old PUBLISHED (seq 3) -> eligible for prune
	// batch-4: recent SYNCED (seq 4) -> NOT eligible (recent)
	// batch-5: old PENDING (seq 5) -> NOT eligible (pending)
	// batch-6: old FAILED (seq 6) -> NOT eligible (failed)
	// batch-7: old SYNCED (seq 7, latest) -> NOT eligible (highest sequence for node-1)
	b1 := newTestBatch("batch-1", "node-1", 1, oldTime)
	b2 := newTestBatch("batch-2", "node-1", 2, oldTime)
	b3 := newTestBatch("batch-3", "node-1", 3, oldTime)
	b4 := newTestBatch("batch-4", "node-1", 4, recentTime)
	b5 := newTestBatch("batch-5", "node-1", 5, oldTime)
	b6 := newTestBatch("batch-6", "node-1", 6, oldTime)
	b7 := newTestBatch("batch-7", "node-1", 7, oldTime)

	// Node 2:
	// batch-n2-1: old SYNCED (seq 10) -> eligible for prune
	// batch-n2-2: old PUBLISHED (seq 20, latest) -> NOT eligible (highest sequence for node-2)
	bn2_1 := newTestBatch("batch-n2-1", "node-2", 10, oldTime)
	bn2_2 := newTestBatch("batch-n2-2", "node-2", 20, oldTime)

	for _, b := range []*types.TelemetryBatch{b1, b2, b3, b4, b5, b6, b7, bn2_1, bn2_2} {
		if err := store.PersistBatch(ctx, b); err != nil {
			t.Fatalf("PersistBatch %s failed: %v", b.BatchID, err)
		}
	}

	// Transition statuses:
	if err := store.MarkBatchSynced(ctx, "batch-1", oldTime); err != nil {
		t.Fatalf("MarkBatchSynced batch-1 failed: %v", err)
	}
	if err := store.MarkBatchSynced(ctx, "batch-2", oldTime); err != nil {
		t.Fatalf("MarkBatchSynced batch-2 failed: %v", err)
	}
	if err := store.MarkBatchPublished(ctx, "batch-3", oldTime); err != nil {
		t.Fatalf("MarkBatchPublished batch-3 failed: %v", err)
	}
	if err := store.MarkBatchSynced(ctx, "batch-4", recentTime); err != nil {
		t.Fatalf("MarkBatchSynced batch-4 failed: %v", err)
	}
	// batch-5 left as PENDING
	// batch-6 set to FAILED via direct query:
	_, err = store.db.ExecContext(ctx, "UPDATE telemetry_batches SET sync_status = 'FAILED' WHERE batch_id = 'batch-6';")
	if err != nil {
		t.Fatalf("setting batch-6 to FAILED failed: %v", err)
	}
	if err := store.MarkBatchSynced(ctx, "batch-7", oldTime); err != nil {
		t.Fatalf("MarkBatchSynced batch-7 failed: %v", err)
	}
	if err := store.MarkBatchSynced(ctx, "batch-n2-1", oldTime); err != nil {
		t.Fatalf("MarkBatchSynced batch-n2-1 failed: %v", err)
	}
	if err := store.MarkBatchPublished(ctx, "batch-n2-2", oldTime); err != nil {
		t.Fatalf("MarkBatchPublished batch-n2-2 failed: %v", err)
	}

	// 1. Test limit bounding: prune with limit = 2
	pruned, err := store.PruneCompletedBatches(ctx, cutoff, 2)
	if err != nil {
		t.Fatalf("PruneCompletedBatches with limit 2 failed: %v", err)
	}
	if pruned != 2 {
		t.Errorf("expected 2 pruned rows with limit=2, got %d", pruned)
	}

	// 2. Prune remaining eligible batches with limit = 100
	prunedRemaining, err := store.PruneCompletedBatches(ctx, cutoff, 100)
	if err != nil {
		t.Fatalf("PruneCompletedBatches remaining failed: %v", err)
	}
	// Eligible were: batch-1, batch-2, batch-3 from node-1 (total 3), and batch-n2-1 from node-2 (total 1). Total eligible = 4.
	// Since 2 were already pruned, remaining should be 2.
	if prunedRemaining != 2 {
		t.Errorf("expected 2 remaining pruned rows, got %d", prunedRemaining)
	}

	// 3. Verify batch retention
	// Must be deleted:
	for _, id := range []string{"batch-1", "batch-2", "batch-3", "batch-n2-1"} {
		if _, err := store.GetBatch(ctx, id); !errors.Is(err, ErrBatchNotFound) {
			t.Errorf("expected batch %s to be deleted, got %v", id, err)
		}
	}
	// Must be retained:
	// batch-4 (recent SYNCED)
	if _, err := store.GetBatch(ctx, "batch-4"); err != nil {
		t.Errorf("expected batch-4 to be retained: %v", err)
	}
	// batch-5 (old PENDING)
	if _, err := store.GetBatch(ctx, "batch-5"); err != nil {
		t.Errorf("expected batch-5 to be retained: %v", err)
	}
	// batch-6 (old FAILED)
	if _, err := store.GetBatch(ctx, "batch-6"); err != nil {
		t.Errorf("expected batch-6 to be retained: %v", err)
	}
	// batch-7 (latest sequence for node-1, preserved despite being old SYNCED)
	if _, err := store.GetBatch(ctx, "batch-7"); err != nil {
		t.Errorf("expected batch-7 (highest seq node-1) to be retained: %v", err)
	}
	// batch-n2-2 (latest sequence for node-2, preserved despite being old PUBLISHED)
	if _, err := store.GetBatch(ctx, "batch-n2-2"); err != nil {
		t.Errorf("expected batch-n2-2 (highest seq node-2) to be retained: %v", err)
	}

	// 4. Verify sequence continuity across close and reopen
	seq1, err := store.GetLatestSequenceNumber(ctx, "node-1")
	if err != nil {
		t.Fatalf("GetLatestSequenceNumber node-1 failed: %v", err)
	}
	if seq1 != 7 {
		t.Errorf("expected node-1 latest sequence to remain 7, got %d", seq1)
	}
	seq2, err := store.GetLatestSequenceNumber(ctx, "node-2")
	if err != nil {
		t.Fatalf("GetLatestSequenceNumber node-2 failed: %v", err)
	}
	if seq2 != 20 {
		t.Errorf("expected node-2 latest sequence to remain 20, got %d", seq2)
	}

	// Close store and reopen to test restart recovery
	if err := store.Close(); err != nil {
		t.Fatalf("Close store failed: %v", err)
	}

	reopenedStore, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("reopened OpenSQLite failed: %v", err)
	}
	defer reopenedStore.Close()

	reopenedSeq1, err := reopenedStore.GetLatestSequenceNumber(ctx, "node-1")
	if err != nil {
		t.Fatalf("reopened GetLatestSequenceNumber node-1 failed: %v", err)
	}
	if reopenedSeq1 != 7 {
		t.Errorf("expected reopened node-1 latest sequence 7, got %d", reopenedSeq1)
	}

	reopenedSeq2, err := reopenedStore.GetLatestSequenceNumber(ctx, "node-2")
	if err != nil {
		t.Fatalf("reopened GetLatestSequenceNumber node-2 failed: %v", err)
	}
	if reopenedSeq2 != 20 {
		t.Errorf("expected reopened node-2 latest sequence 20, got %d", reopenedSeq2)
	}
}

// TestSQLite_PruneCompletedBatches_ConcurrentPersistence ensures concurrent writes
// and pruning do not cause lock contention failures or corrupt sequence numbers.
func TestSQLite_PruneCompletedBatches_ConcurrentPersistence(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "concurrent_prune.db")
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	oldTime := now.Add(-48 * time.Hour)
	cutoff := now.Add(-24 * time.Hour)

	// Insert 10 old synced batches
	for i := int64(1); i <= 10; i++ {
		b := newTestBatch(strings.Repeat("a", 8)+string(rune('0'+i)), "node-conc", i, oldTime)
		if err := store.PersistBatch(ctx, b); err != nil {
			t.Fatalf("PersistBatch %d failed: %v", i, err)
		}
		if err := store.MarkBatchSynced(ctx, b.BatchID, oldTime); err != nil {
			t.Fatalf("MarkBatchSynced %d failed: %v", i, err)
		}
	}

	// Concurrently prune and insert new batches
	errCh := make(chan error, 2)

	go func() {
		// Insert 5 new batches
		for i := int64(11); i <= 15; i++ {
			b := newTestBatch(strings.Repeat("b", 8)+string(rune('0'+i)), "node-conc", i, time.Now().UTC())
			if err := store.PersistBatch(ctx, b); err != nil {
				errCh <- err
				return
			}
		}
		errCh <- nil
	}()

	go func() {
		// Prune
		_, err := store.PruneCompletedBatches(ctx, cutoff, 5)
		errCh <- err
	}()

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent operation failed: %v", err)
		}
	}

	latestSeq, err := store.GetLatestSequenceNumber(ctx, "node-conc")
	if err != nil {
		t.Fatalf("GetLatestSequenceNumber failed: %v", err)
	}
	if latestSeq < 10 {
		t.Errorf("expected latest sequence >= 10, got %d", latestSeq)
	}
}

// TestSQLite_PruneCompletedBatches_StatusSpecificTimestamps verifies that:
// 1. An old SYNCED row with sent_at is eligible.
// 2. An old PUBLISHED row with published_at is eligible.
// 3. A SYNCED row with sent_at NULL is retained even if published_at is populated.
// 4. A PUBLISHED row with published_at NULL is retained even if sent_at is populated.
func TestSQLite_PruneCompletedBatches_StatusSpecificTimestamps(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "status_timestamps_test.db")
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	oldTimeStr := now.Add(-48 * time.Hour).Format(time.RFC3339Nano)
	cutoff := now.Add(-24 * time.Hour)

	// Insert batches manually to test precise column populated states
	// b1: SYNCED with sent_at populated, published_at NULL (eligible)
	// b2: PUBLISHED with published_at populated, sent_at NULL (eligible)
	// b3: SYNCED with sent_at NULL, published_at populated (must NOT be pruned)
	// b4: PUBLISHED with published_at NULL, sent_at populated (must NOT be pruned)
	// b5: SYNCED with sent_at populated (highest sequence 99 -> must NOT be pruned)

	insertQuery := `
	INSERT INTO telemetry_batches (batch_id, node_id, sequence_number, collected_at, sent_at, published_at, payload, created_at, sync_status)
	VALUES (?, 'node-status', ?, ?, ?, ?, '{}', ?, ?);
	`

	if _, err := store.db.ExecContext(ctx, insertQuery, "b1-synced-valid", 1, oldTimeStr, oldTimeStr, nil, oldTimeStr, "SYNCED"); err != nil {
		t.Fatalf("insert b1 failed: %v", err)
	}
	if _, err := store.db.ExecContext(ctx, insertQuery, "b2-pub-valid", 2, oldTimeStr, nil, oldTimeStr, oldTimeStr, "PUBLISHED"); err != nil {
		t.Fatalf("insert b2 failed: %v", err)
	}
	if _, err := store.db.ExecContext(ctx, insertQuery, "b3-synced-nullsent", 3, oldTimeStr, nil, oldTimeStr, oldTimeStr, "SYNCED"); err != nil {
		t.Fatalf("insert b3 failed: %v", err)
	}
	if _, err := store.db.ExecContext(ctx, insertQuery, "b4-pub-nullpub", 4, oldTimeStr, oldTimeStr, nil, oldTimeStr, "PUBLISHED"); err != nil {
		t.Fatalf("insert b4 failed: %v", err)
	}
	if _, err := store.db.ExecContext(ctx, insertQuery, "b5-highest-seq", 99, oldTimeStr, oldTimeStr, nil, oldTimeStr, "SYNCED"); err != nil {
		t.Fatalf("insert b5 failed: %v", err)
	}

	pruned, err := store.PruneCompletedBatches(ctx, cutoff, 100)
	if err != nil {
		t.Fatalf("PruneCompletedBatches failed: %v", err)
	}
	if pruned != 2 {
		t.Errorf("expected exactly 2 pruned rows (b1 and b2), got %d", pruned)
	}

	// b1 and b2 must be deleted
	if _, err := store.GetBatch(ctx, "b1-synced-valid"); !errors.Is(err, ErrBatchNotFound) {
		t.Errorf("expected b1-synced-valid to be pruned, got %v", err)
	}
	if _, err := store.GetBatch(ctx, "b2-pub-valid"); !errors.Is(err, ErrBatchNotFound) {
		t.Errorf("expected b2-pub-valid to be pruned, got %v", err)
	}

	// b3 (SYNCED with NULL sent_at) must be retained
	if _, err := store.GetBatch(ctx, "b3-synced-nullsent"); err != nil {
		t.Errorf("expected b3-synced-nullsent to be retained: %v", err)
	}

	// b4 (PUBLISHED with NULL published_at) must be retained
	if _, err := store.GetBatch(ctx, "b4-pub-nullpub"); err != nil {
		t.Errorf("expected b4-pub-nullpub to be retained: %v", err)
	}

	// b5 (highest sequence) must be retained
	if _, err := store.GetBatch(ctx, "b5-highest-seq"); err != nil {
		t.Errorf("expected b5-highest-seq to be retained: %v", err)
	}
}

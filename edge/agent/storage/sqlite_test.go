package storage

import (
	"context"
	"database/sql"
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

// ---------------------------------------------------------------------------
// Phase 6.28 Stage 1: Durable Telemetry Quarantine Storage Tests
// ---------------------------------------------------------------------------

// 1. TestSQLite_MarkBatchQuarantined_Valid: PENDING batch transitions to FAILED,
// error_reason and failed_at are persisted and recoverable via GetBatch.
func TestSQLite_MarkBatchQuarantined_Valid(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "quarantine_valid.db")
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	failedAt := now.Add(2 * time.Second)
	batch := newTestBatch("batch-q1", "node-q", 10, now)
	if err := store.PersistBatch(ctx, batch); err != nil {
		t.Fatalf("PersistBatch failed: %v", err)
	}

	reason := "schema violation: payload metric value out of bounds"
	if err := store.MarkBatchQuarantined(ctx, "batch-q1", reason, failedAt); err != nil {
		t.Fatalf("MarkBatchQuarantined failed: %v", err)
	}

	rec, err := store.GetBatch(ctx, "batch-q1")
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}

	if rec.SyncStatus != SyncStatusFailed {
		t.Errorf("expected SyncStatus %s, got %s", SyncStatusFailed, rec.SyncStatus)
	}
	if rec.ErrorReason != reason {
		t.Errorf("expected ErrorReason %q, got %q", reason, rec.ErrorReason)
	}
	if rec.FailedAt == nil {
		t.Fatal("expected FailedAt to be populated, got nil")
	}
	if !rec.FailedAt.Equal(failedAt) {
		t.Errorf("expected FailedAt %v, got %v", failedAt, *rec.FailedAt)
	}
	if rec.SequenceNumber != 10 {
		t.Errorf("expected SequenceNumber 10, got %d", rec.SequenceNumber)
	}
	if rec.NodeID != "node-q" {
		t.Errorf("expected NodeID node-q, got %s", rec.NodeID)
	}

	// Must no longer appear in pending queries
	pending, err := store.GetPendingBatches(ctx, 100)
	if err != nil {
		t.Fatalf("GetPendingBatches failed: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("expected 0 pending batches, got %d", len(pending))
	}
}

// 2. TestSQLite_MarkBatchQuarantined_NotFound: non-existent batch returns ErrBatchNotFound.
func TestSQLite_MarkBatchQuarantined_NotFound(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "quarantine_notfound.db")
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	err = store.MarkBatchQuarantined(ctx, "nonexistent-batch", "unrecognized schema", time.Now().UTC())
	if err == nil {
		t.Fatal("expected error for non-existent batch, got nil")
	}
	if !errors.Is(err, ErrBatchNotFound) {
		t.Errorf("expected ErrBatchNotFound, got %v", err)
	}
}

// 3. TestSQLite_MarkBatchQuarantined_InvalidStatus: batch in SYNCED, PUBLISHED, or FAILED status cannot be quarantined; returns ErrInvalidBatchTransition.
func TestSQLite_MarkBatchQuarantined_InvalidStatus(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "quarantine_invalid_status.db")
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()

	// 1. SYNCED batch
	bSynced := newTestBatch("batch-synced", "node-1", 1, now)
	if err := store.PersistBatch(ctx, bSynced); err != nil {
		t.Fatalf("PersistBatch bSynced failed: %v", err)
	}
	if err := store.MarkBatchSynced(ctx, "batch-synced", now); err != nil {
		t.Fatalf("MarkBatchSynced failed: %v", err)
	}

	// 2. PUBLISHED batch
	bPub := newTestBatch("batch-pub", "node-1", 2, now)
	if err := store.PersistBatch(ctx, bPub); err != nil {
		t.Fatalf("PersistBatch bPub failed: %v", err)
	}
	if err := store.MarkBatchPublished(ctx, "batch-pub", now); err != nil {
		t.Fatalf("MarkBatchPublished failed: %v", err)
	}

	// 3. Already FAILED batch
	bFailed := newTestBatch("batch-failed", "node-1", 3, now)
	if err := store.PersistBatch(ctx, bFailed); err != nil {
		t.Fatalf("PersistBatch bFailed failed: %v", err)
	}
	if err := store.MarkBatchQuarantined(ctx, "batch-failed", "first error", now); err != nil {
		t.Fatalf("MarkBatchQuarantined bFailed failed: %v", err)
	}

	// Attempt to quarantine SYNCED
	err = store.MarkBatchQuarantined(ctx, "batch-synced", "fail", now)
	if !errors.Is(err, ErrInvalidBatchTransition) {
		t.Errorf("expected ErrInvalidBatchTransition for SYNCED batch, got %v", err)
	}

	// Attempt to quarantine PUBLISHED
	err = store.MarkBatchQuarantined(ctx, "batch-pub", "fail", now)
	if !errors.Is(err, ErrInvalidBatchTransition) {
		t.Errorf("expected ErrInvalidBatchTransition for PUBLISHED batch, got %v", err)
	}

	// Attempt to quarantine already FAILED
	err = store.MarkBatchQuarantined(ctx, "batch-failed", "second error", now)
	if !errors.Is(err, ErrInvalidBatchTransition) {
		t.Errorf("expected ErrInvalidBatchTransition for already FAILED batch, got %v", err)
	}
}

// 4. TestSQLite_GetFailedBatches_Empty: returns empty slice, no error when no failed batches exist.
func TestSQLite_GetFailedBatches_Empty(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "getfailed_empty.db")
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	failed, err := store.GetFailedBatches(ctx, 100)
	if err != nil {
		t.Fatalf("GetFailedBatches failed: %v", err)
	}
	if failed == nil || len(failed) != 0 {
		t.Errorf("expected non-nil empty slice, got %+v", failed)
	}
}

// 5. TestSQLite_GetFailedBatches_Ordering: returns failed batches ordered by failed_at ASC, sequence_number ASC, batch_id ASC.
func TestSQLite_GetFailedBatches_Ordering(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "getfailed_ordering.db")
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	baseTime := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	t1 := baseTime.Add(1 * time.Minute)
	t2 := baseTime.Add(2 * time.Minute)

	// Create batches with specific ordering criteria
	// Expected ordering: failed_at ASC, sequence_number ASC, batch_id ASC
	// 1: t1, seq 1, "batch-aa"
	// 2: t1, seq 1, "batch-c"
	// 3: t1, seq 1, "batch-d"
	// 4: t1, seq 2, "batch-b"
	// 5: t2, seq 5, "batch-a"
	batches := []struct {
		id       string
		seq      int64
		failedAt time.Time
	}{
		{"batch-a", 5, t2},
		{"batch-b", 2, t1},
		{"batch-d", 1, t1},
		{"batch-c", 1, t1},
		{"batch-aa", 1, t1},
	}

	for _, b := range batches {
		tb := newTestBatch(b.id, "node-1", b.seq, baseTime)
		if err := store.PersistBatch(ctx, tb); err != nil {
			t.Fatalf("PersistBatch %s failed: %v", b.id, err)
		}
		if err := store.MarkBatchQuarantined(ctx, b.id, "quarantine reason", b.failedAt); err != nil {
			t.Fatalf("MarkBatchQuarantined %s failed: %v", b.id, err)
		}
	}

	failed, err := store.GetFailedBatches(ctx, 100)
	if err != nil {
		t.Fatalf("GetFailedBatches failed: %v", err)
	}

	expectedIDs := []string{"batch-aa", "batch-c", "batch-d", "batch-b", "batch-a"}
	if len(failed) != len(expectedIDs) {
		t.Fatalf("expected %d failed batches, got %d", len(expectedIDs), len(failed))
	}

	for i, expID := range expectedIDs {
		if failed[i].BatchID != expID {
			t.Errorf("at index %d: expected %s, got %s", i, expID, failed[i].BatchID)
		}
	}
}

// 6. TestSQLite_GetFailedBatches_Limit: respects limit parameter; default limit (100) when limit <= 0.
func TestSQLite_GetFailedBatches_Limit(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "getfailed_limit.db")
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	for i := 1; i <= 5; i++ {
		bID := "batch-" + string(rune('0'+i))
		tb := newTestBatch(bID, "node-1", int64(i), now)
		if err := store.PersistBatch(ctx, tb); err != nil {
			t.Fatalf("PersistBatch %s failed: %v", bID, err)
		}
		if err := store.MarkBatchQuarantined(ctx, bID, "fail", now.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatalf("MarkBatchQuarantined %s failed: %v", bID, err)
		}
	}

	// Limit = 2
	failed2, err := store.GetFailedBatches(ctx, 2)
	if err != nil {
		t.Fatalf("GetFailedBatches with limit 2 failed: %v", err)
	}
	if len(failed2) != 2 {
		t.Errorf("expected 2 batches, got %d", len(failed2))
	}

	// Limit <= 0 defaults to 100
	failedDefault, err := store.GetFailedBatches(ctx, 0)
	if err != nil {
		t.Fatalf("GetFailedBatches with limit 0 failed: %v", err)
	}
	if len(failedDefault) != 5 {
		t.Errorf("expected all 5 batches with limit 0 default, got %d", len(failedDefault))
	}

	failedNeg, err := store.GetFailedBatches(ctx, -5)
	if err != nil {
		t.Fatalf("GetFailedBatches with negative limit failed: %v", err)
	}
	if len(failedNeg) != 5 {
		t.Errorf("expected all 5 batches with negative limit default, got %d", len(failedNeg))
	}
}

// 7. TestSQLite_GetFailedBatches_Isolation: does not return PENDING, SYNCED, or PUBLISHED batches.
func TestSQLite_GetFailedBatches_Isolation(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "getfailed_isolation.db")
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()

	// 1. PENDING
	bPending := newTestBatch("b-pending", "node-1", 1, now)
	if err := store.PersistBatch(ctx, bPending); err != nil {
		t.Fatalf("PersistBatch bPending failed: %v", err)
	}

	// 2. SYNCED
	bSynced := newTestBatch("b-synced", "node-1", 2, now)
	if err := store.PersistBatch(ctx, bSynced); err != nil {
		t.Fatalf("PersistBatch bSynced failed: %v", err)
	}
	if err := store.MarkBatchSynced(ctx, "b-synced", now); err != nil {
		t.Fatalf("MarkBatchSynced failed: %v", err)
	}

	// 3. PUBLISHED
	bPub := newTestBatch("b-pub", "node-1", 3, now)
	if err := store.PersistBatch(ctx, bPub); err != nil {
		t.Fatalf("PersistBatch bPub failed: %v", err)
	}
	if err := store.MarkBatchPublished(ctx, "b-pub", now); err != nil {
		t.Fatalf("MarkBatchPublished failed: %v", err)
	}

	// 4. FAILED
	bFailed := newTestBatch("b-failed", "node-1", 4, now)
	if err := store.PersistBatch(ctx, bFailed); err != nil {
		t.Fatalf("PersistBatch bFailed failed: %v", err)
	}
	if err := store.MarkBatchQuarantined(ctx, "b-failed", "quarantine reason", now); err != nil {
		t.Fatalf("MarkBatchQuarantined failed: %v", err)
	}

	// GetFailedBatches must return ONLY b-failed
	failed, err := store.GetFailedBatches(ctx, 100)
	if err != nil {
		t.Fatalf("GetFailedBatches failed: %v", err)
	}
	if len(failed) != 1 {
		t.Fatalf("expected 1 failed batch, got %d", len(failed))
	}
	if failed[0].BatchID != "b-failed" {
		t.Errorf("expected b-failed, got %s", failed[0].BatchID)
	}

	// GetPendingBatches must return ONLY b-pending
	pending, err := store.GetPendingBatches(ctx, 100)
	if err != nil {
		t.Fatalf("GetPendingBatches failed: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending batch, got %d", len(pending))
	}
	if pending[0].BatchID != "b-pending" {
		t.Errorf("expected b-pending, got %s", pending[0].BatchID)
	}
}

// 8. TestSQLite_RequeueQuarantinedBatch_Valid: FAILED batch transitions to PENDING, attempt reset to 0,
// error_reason and failed_at cleared to NULL/empty; verify recoverable via GetBatch and GetPendingBatches.
func TestSQLite_RequeueQuarantinedBatch_Valid(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "requeue_valid.db")
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	b := newTestBatch("batch-rq1", "node-rq", 42, now)
	if err := store.PersistBatch(ctx, b); err != nil {
		t.Fatalf("PersistBatch failed: %v", err)
	}

	// Simulate 3 failed sync attempts
	for i := 0; i < 3; i++ {
		if err := store.RecordSyncAttempt(ctx, "batch-rq1", now); err != nil {
			t.Fatalf("RecordSyncAttempt %d failed: %v", i, err)
		}
	}

	// Quarantine the batch
	failedAt := now.Add(5 * time.Second)
	reason := "unrecoverable transport poison message: invalid schema version"
	if err := store.MarkBatchQuarantined(ctx, "batch-rq1", reason, failedAt); err != nil {
		t.Fatalf("MarkBatchQuarantined failed: %v", err)
	}

	// Verify quarantined state before requeue
	recBefore, err := store.GetBatch(ctx, "batch-rq1")
	if err != nil {
		t.Fatalf("GetBatch before requeue failed: %v", err)
	}
	if recBefore.SyncStatus != SyncStatusFailed || recBefore.Attempt != 3 || recBefore.ErrorReason != reason {
		t.Fatalf("unexpected state before requeue: status=%s, attempt=%d, reason=%q", recBefore.SyncStatus, recBefore.Attempt, recBefore.ErrorReason)
	}

	// Requeue the batch
	if err := store.RequeueQuarantinedBatch(ctx, "batch-rq1"); err != nil {
		t.Fatalf("RequeueQuarantinedBatch failed: %v", err)
	}

	// Verify batch is restored to PENDING with attempt reset and reason/failed_at cleared
	recAfter, err := store.GetBatch(ctx, "batch-rq1")
	if err != nil {
		t.Fatalf("GetBatch after requeue failed: %v", err)
	}
	if recAfter.SyncStatus != SyncStatusPending {
		t.Errorf("expected SyncStatus %s, got %s", SyncStatusPending, recAfter.SyncStatus)
	}
	if recAfter.Attempt != 0 {
		t.Errorf("expected Attempt 0, got %d", recAfter.Attempt)
	}
	if recAfter.ErrorReason != "" {
		t.Errorf("expected empty ErrorReason, got %q", recAfter.ErrorReason)
	}
	if recAfter.FailedAt != nil {
		t.Errorf("expected nil FailedAt, got %v", recAfter.FailedAt)
	}

	// Verify batch appears in GetPendingBatches
	pending, err := store.GetPendingBatches(ctx, 100)
	if err != nil {
		t.Fatalf("GetPendingBatches failed: %v", err)
	}
	if len(pending) != 1 || pending[0].BatchID != "batch-rq1" {
		t.Errorf("expected batch-rq1 in pending batches, got %+v", pending)
	}

	// Verify GetFailedBatches is empty
	failed, err := store.GetFailedBatches(ctx, 100)
	if err != nil {
		t.Fatalf("GetFailedBatches failed: %v", err)
	}
	if len(failed) != 0 {
		t.Errorf("expected 0 failed batches after requeue, got %d", len(failed))
	}
}

// 9. TestSQLite_RequeueQuarantinedBatch_NotFound: non-existent batch returns ErrBatchNotFound.
func TestSQLite_RequeueQuarantinedBatch_NotFound(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "requeue_notfound.db")
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	err = store.RequeueQuarantinedBatch(ctx, "nonexistent-batch")
	if err == nil {
		t.Fatal("expected error for nonexistent batch, got nil")
	}
	if !errors.Is(err, ErrBatchNotFound) {
		t.Errorf("expected ErrBatchNotFound, got %v", err)
	}
}

// 10. TestSQLite_RequeueQuarantinedBatch_InvalidStatus: batch in PENDING, SYNCED, or PUBLISHED status cannot be requeued; returns ErrInvalidBatchTransition.
func TestSQLite_RequeueQuarantinedBatch_InvalidStatus(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "requeue_invalid_status.db")
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()

	// 1. PENDING batch cannot be requeued (already pending)
	bPending := newTestBatch("batch-p", "node-1", 1, now)
	if err := store.PersistBatch(ctx, bPending); err != nil {
		t.Fatalf("PersistBatch bPending failed: %v", err)
	}
	err = store.RequeueQuarantinedBatch(ctx, "batch-p")
	if !errors.Is(err, ErrInvalidBatchTransition) {
		t.Errorf("expected ErrInvalidBatchTransition for PENDING batch, got %v", err)
	}

	// 2. SYNCED batch cannot be requeued
	bSynced := newTestBatch("batch-s", "node-1", 2, now)
	if err := store.PersistBatch(ctx, bSynced); err != nil {
		t.Fatalf("PersistBatch bSynced failed: %v", err)
	}
	if err := store.MarkBatchSynced(ctx, "batch-s", now); err != nil {
		t.Fatalf("MarkBatchSynced failed: %v", err)
	}
	err = store.RequeueQuarantinedBatch(ctx, "batch-s")
	if !errors.Is(err, ErrInvalidBatchTransition) {
		t.Errorf("expected ErrInvalidBatchTransition for SYNCED batch, got %v", err)
	}

	// 3. PUBLISHED batch cannot be requeued
	bPub := newTestBatch("batch-pub", "node-1", 3, now)
	if err := store.PersistBatch(ctx, bPub); err != nil {
		t.Fatalf("PersistBatch bPub failed: %v", err)
	}
	if err := store.MarkBatchPublished(ctx, "batch-pub", now); err != nil {
		t.Fatalf("MarkBatchPublished failed: %v", err)
	}
	err = store.RequeueQuarantinedBatch(ctx, "batch-pub")
	if !errors.Is(err, ErrInvalidBatchTransition) {
		t.Errorf("expected ErrInvalidBatchTransition for PUBLISHED batch, got %v", err)
	}
}

// 11. TestSQLite_QuarantinedBatch_NotPruned: FAILED batch with old timestamp is NEVER deleted by PruneCompletedBatches.
func TestSQLite_QuarantinedBatch_NotPruned(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "quarantine_prune.db")
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	oldTime := now.Add(-72 * time.Hour)
	cutoff := now.Add(-24 * time.Hour)

	// b1: SYNCED with old completion timestamp (seq 1) -> eligible for prune
	b1 := newTestBatch("b1-synced", "node-1", 1, oldTime)
	if err := store.PersistBatch(ctx, b1); err != nil {
		t.Fatalf("PersistBatch b1 failed: %v", err)
	}
	if err := store.MarkBatchSynced(ctx, "b1-synced", oldTime); err != nil {
		t.Fatalf("MarkBatchSynced b1 failed: %v", err)
	}

	// b2: FAILED with old failed_at timestamp (seq 2) -> NEVER eligible for prune
	b2 := newTestBatch("b2-quarantined", "node-1", 2, oldTime)
	if err := store.PersistBatch(ctx, b2); err != nil {
		t.Fatalf("PersistBatch b2 failed: %v", err)
	}
	if err := store.MarkBatchQuarantined(ctx, "b2-quarantined", "fatal parsing bug", oldTime); err != nil {
		t.Fatalf("MarkBatchQuarantined b2 failed: %v", err)
	}

	// b3: SYNCED with old completion timestamp (seq 3, highest sequence) -> NOT pruned due to high-water mark invariant
	b3 := newTestBatch("b3-synced-latest", "node-1", 3, oldTime)
	if err := store.PersistBatch(ctx, b3); err != nil {
		t.Fatalf("PersistBatch b3 failed: %v", err)
	}
	if err := store.MarkBatchSynced(ctx, "b3-synced-latest", oldTime); err != nil {
		t.Fatalf("MarkBatchSynced b3 failed: %v", err)
	}

	// Prune completed batches
	pruned, err := store.PruneCompletedBatches(ctx, cutoff, 100)
	if err != nil {
		t.Fatalf("PruneCompletedBatches failed: %v", err)
	}
	if pruned != 1 {
		t.Errorf("expected exactly 1 pruned row (b1), got %d", pruned)
	}

	// b1 must be deleted
	if _, err := store.GetBatch(ctx, "b1-synced"); !errors.Is(err, ErrBatchNotFound) {
		t.Errorf("expected b1-synced to be deleted, got %v", err)
	}

	// b2 (FAILED) must be retained
	rec2, err := store.GetBatch(ctx, "b2-quarantined")
	if err != nil {
		t.Fatalf("expected b2-quarantined to be retained, got err: %v", err)
	}
	if rec2.SyncStatus != SyncStatusFailed {
		t.Errorf("expected SyncStatus %s, got %s", SyncStatusFailed, rec2.SyncStatus)
	}

	// b3 (latest sequence) must be retained
	if _, err := store.GetBatch(ctx, "b3-synced-latest"); err != nil {
		t.Fatalf("expected b3-synced-latest to be retained, got err: %v", err)
	}
}

// 12. TestSQLite_QuarantinedBatch_LatestSequenceNumber: FAILED batch contributes to GetLatestSequenceNumber (monotonicity preserved).
func TestSQLite_QuarantinedBatch_LatestSequenceNumber(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "quarantine_seq.db")
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()

	// Seq 1: SYNCED
	b1 := newTestBatch("seq-1", "node-alpha", 1, now)
	if err := store.PersistBatch(ctx, b1); err != nil {
		t.Fatalf("PersistBatch b1 failed: %v", err)
	}
	if err := store.MarkBatchSynced(ctx, "seq-1", now); err != nil {
		t.Fatalf("MarkBatchSynced b1 failed: %v", err)
	}

	// Seq 2: PUBLISHED
	b2 := newTestBatch("seq-2", "node-alpha", 2, now)
	if err := store.PersistBatch(ctx, b2); err != nil {
		t.Fatalf("PersistBatch b2 failed: %v", err)
	}
	if err := store.MarkBatchPublished(ctx, "seq-2", now); err != nil {
		t.Fatalf("MarkBatchPublished b2 failed: %v", err)
	}

	// Seq 3: FAILED (Quarantined)
	b3 := newTestBatch("seq-3", "node-alpha", 3, now)
	if err := store.PersistBatch(ctx, b3); err != nil {
		t.Fatalf("PersistBatch b3 failed: %v", err)
	}
	if err := store.MarkBatchQuarantined(ctx, "seq-3", "corrupt schema", now); err != nil {
		t.Fatalf("MarkBatchQuarantined b3 failed: %v", err)
	}

	// Latest sequence number must be 3
	latestSeq, err := store.GetLatestSequenceNumber(ctx, "node-alpha")
	if err != nil {
		t.Fatalf("GetLatestSequenceNumber failed: %v", err)
	}
	if latestSeq != 3 {
		t.Errorf("expected latest sequence number 3, got %d", latestSeq)
	}

	// Seq 4: PENDING
	b4 := newTestBatch("seq-4", "node-alpha", 4, now)
	if err := store.PersistBatch(ctx, b4); err != nil {
		t.Fatalf("PersistBatch b4 failed: %v", err)
	}

	latestSeq, err = store.GetLatestSequenceNumber(ctx, "node-alpha")
	if err != nil {
		t.Fatalf("GetLatestSequenceNumber after b4 failed: %v", err)
	}
	if latestSeq != 4 {
		t.Errorf("expected latest sequence number 4, got %d", latestSeq)
	}
}

// 13. TestSQLite_Quarantine_PersistenceAcrossRestart: quarantine state (FAILED status, error_reason, failed_at) survives database close and reopen.
func TestSQLite_Quarantine_PersistenceAcrossRestart(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "quarantine_restart.db")

	store1, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite 1 failed: %v", err)
	}

	now := time.Now().UTC()
	failedAt := now.Add(10 * time.Second)
	reason := "fatal schema incompatibility detected at edge"
	b := newTestBatch("batch-restart", "node-rst", 100, now)
	if err := store1.PersistBatch(ctx, b); err != nil {
		t.Fatalf("PersistBatch failed: %v", err)
	}
	if err := store1.MarkBatchQuarantined(ctx, "batch-restart", reason, failedAt); err != nil {
		t.Fatalf("MarkBatchQuarantined failed: %v", err)
	}

	// Close store 1 cleanly
	if err := store1.Close(); err != nil {
		t.Fatalf("store1.Close failed: %v", err)
	}

	// Reopen store 2
	store2, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite 2 failed: %v", err)
	}
	defer store2.Close()

	rec, err := store2.GetBatch(ctx, "batch-restart")
	if err != nil {
		t.Fatalf("GetBatch from reopened store failed: %v", err)
	}
	if rec.SyncStatus != SyncStatusFailed {
		t.Errorf("expected SyncStatus %s, got %s", SyncStatusFailed, rec.SyncStatus)
	}
	if rec.ErrorReason != reason {
		t.Errorf("expected ErrorReason %q, got %q", reason, rec.ErrorReason)
	}
	if rec.FailedAt == nil || !rec.FailedAt.Equal(failedAt) {
		t.Errorf("expected FailedAt %v, got %v", failedAt, rec.FailedAt)
	}

	failedBatches, err := store2.GetFailedBatches(ctx, 10)
	if err != nil {
		t.Fatalf("GetFailedBatches from reopened store failed: %v", err)
	}
	if len(failedBatches) != 1 || failedBatches[0].BatchID != "batch-restart" {
		t.Errorf("expected batch-restart in GetFailedBatches, got %+v", failedBatches)
	}

	// Requeue in reopened store
	if err := store2.RequeueQuarantinedBatch(ctx, "batch-restart"); err != nil {
		t.Fatalf("RequeueQuarantinedBatch in reopened store failed: %v", err)
	}

	recRequeued, err := store2.GetBatch(ctx, "batch-restart")
	if err != nil {
		t.Fatalf("GetBatch after requeue failed: %v", err)
	}
	if recRequeued.SyncStatus != SyncStatusPending {
		t.Errorf("expected SyncStatus %s, got %s", SyncStatusPending, recRequeued.SyncStatus)
	}
	if recRequeued.ErrorReason != "" {
		t.Errorf("expected empty ErrorReason, got %q", recRequeued.ErrorReason)
	}
	if recRequeued.FailedAt != nil {
		t.Errorf("expected nil FailedAt, got %v", recRequeued.FailedAt)
	}
}

// 14. TestSQLite_Migration_v11: verify schema version is 11, columns error_reason and failed_at exist, index exists, upgrade from a pre-v11 database succeeds and preserves existing data.
func TestSQLite_Migration_v11(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "migration_v11.db")

	// 1. Manually construct a v10 database (without error_reason and failed_at)
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}

	v10SQL := `
	CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
	INSERT INTO schema_migrations VALUES (1, '2026-09-20T00:00:00Z');
	INSERT INTO schema_migrations VALUES (2, '2026-09-22T00:00:00Z');
	INSERT INTO schema_migrations VALUES (3, '2026-09-24T00:00:00Z');
	INSERT INTO schema_migrations VALUES (4, '2026-10-01T00:00:00Z');
	INSERT INTO schema_migrations VALUES (5, '2026-10-02T00:00:00Z');
	INSERT INTO schema_migrations VALUES (6, '2026-10-04T00:00:00Z');
	INSERT INTO schema_migrations VALUES (7, '2026-10-06T00:00:00Z');
	INSERT INTO schema_migrations VALUES (8, '2026-10-08T00:00:00Z');
	INSERT INTO schema_migrations VALUES (9, '2026-10-10T00:00:00Z');
	INSERT INTO schema_migrations VALUES (10, '2026-10-12T00:00:00Z');

	CREATE TABLE telemetry_batches (
		batch_id TEXT PRIMARY KEY,
		node_id TEXT NOT NULL,
		sequence_number INTEGER NOT NULL,
		collected_at TEXT NOT NULL,
		sent_at TEXT,
		attempt INTEGER NOT NULL DEFAULT 0,
		payload TEXT NOT NULL,
		created_at TEXT NOT NULL,
		sync_status TEXT NOT NULL DEFAULT 'PENDING',
		published_at TEXT
	);

	INSERT INTO telemetry_batches (batch_id, node_id, sequence_number, collected_at, payload, created_at, sync_status)
	VALUES ('b-pre-v11-pending', 'node-legacy', 1, '2026-10-15T00:00:00Z', '{"batch_id":"b-pre-v11-pending","node_id":"node-legacy","sequence_number":1,"collected_at":"2026-10-15T00:00:00Z","metrics":[{"name":"cpu","value":50,"unit":"percent","timestamp":"2026-10-15T00:00:00Z"}]}', '2026-10-15T00:00:00Z', 'PENDING');

	INSERT INTO telemetry_batches (batch_id, node_id, sequence_number, collected_at, payload, created_at, sync_status, sent_at)
	VALUES ('b-pre-v11-synced', 'node-legacy', 2, '2026-10-15T01:00:00Z', '{"batch_id":"b-pre-v11-synced","node_id":"node-legacy","sequence_number":2,"collected_at":"2026-10-15T01:00:00Z","metrics":[{"name":"cpu","value":55,"unit":"percent","timestamp":"2026-10-15T01:00:00Z"}]}', '2026-10-15T01:00:00Z', 'SYNCED', '2026-10-15T01:05:00Z');
	`
	if _, err := db.Exec(v10SQL); err != nil {
		db.Close()
		t.Fatalf("failed to setup v10 database: %v", err)
	}
	db.Close()

	// 2. Open using OpenSQLite, triggering migration v11
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed to upgrade v10 database: %v", err)
	}
	defer store.Close()

	// 3. Verify schema_migrations has version 11
	var maxVersion int
	err = store.db.QueryRowContext(ctx, "SELECT MAX(version) FROM schema_migrations;").Scan(&maxVersion)
	if err != nil {
		t.Fatalf("failed to query schema version: %v", err)
	}
	if maxVersion != 11 {
		t.Errorf("expected schema version 11, got %d", maxVersion)
	}

	// 4. Verify error_reason and failed_at columns exist
	_, err = store.db.ExecContext(ctx, "SELECT error_reason, failed_at FROM telemetry_batches LIMIT 1;")
	if err != nil {
		t.Errorf("error_reason and failed_at column query failed: %v", err)
	}

	// 5. Verify index idx_telemetry_batches_failed exists
	var indexCount int
	err = store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_telemetry_batches_failed';").Scan(&indexCount)
	if err != nil {
		t.Fatalf("querying index existence failed: %v", err)
	}
	if indexCount != 1 {
		t.Errorf("expected index idx_telemetry_batches_failed to exist, count=%d", indexCount)
	}

	// 6. Verify pre-existing data survived migration
	recPending, err := store.GetBatch(ctx, "b-pre-v11-pending")
	if err != nil {
		t.Fatalf("failed to get legacy pending batch: %v", err)
	}
	if recPending.SyncStatus != SyncStatusPending || recPending.ErrorReason != "" || recPending.FailedAt != nil {
		t.Errorf("unexpected legacy pending batch state: %+v", recPending)
	}

	recSynced, err := store.GetBatch(ctx, "b-pre-v11-synced")
	if err != nil {
		t.Fatalf("failed to get legacy synced batch: %v", err)
	}
	if recSynced.SyncStatus != SyncStatusSynced {
		t.Errorf("expected legacy batch to remain SYNCED, got %s", recSynced.SyncStatus)
	}

	// 7. Verify quarantine operation works on the migrated database
	failedAt := time.Now().UTC()
	err = store.MarkBatchQuarantined(ctx, "b-pre-v11-pending", "migrated quarantine test", failedAt)
	if err != nil {
		t.Fatalf("MarkBatchQuarantined on upgraded DB failed: %v", err)
	}

	failedBatches, err := store.GetFailedBatches(ctx, 10)
	if err != nil {
		t.Fatalf("GetFailedBatches on upgraded DB failed: %v", err)
	}
	if len(failedBatches) != 1 || failedBatches[0].BatchID != "b-pre-v11-pending" {
		t.Errorf("expected b-pre-v11-pending in GetFailedBatches, got %+v", failedBatches)
	}
}

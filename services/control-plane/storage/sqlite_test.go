package storage

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

func sampleBatch(nodeID, batchID string, seq int64) *types.TelemetryBatch {
	now := time.Now().UTC()
	return &types.TelemetryBatch{
		NodeID:         nodeID,
		BatchID:        batchID,
		SequenceNumber: seq,
		CollectedAt:    now.Add(-5 * time.Second),
		Metrics: []types.MetricSample{
			{
				Name:      "cpu_usage",
				Value:     42.5,
				Timestamp: now.Add(-5 * time.Second),
			},
			{
				Name:      "memory_mb",
				Value:     512.0,
				Timestamp: now.Add(-5 * time.Second),
			},
		},
	}
}

func TestSQLiteStore_RecordAndHasBatch(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	batch := sampleBatch("node-1", "batch-alpha", 1)
	ingestedAt := time.Now().UTC()

	// Before recording
	has, err := store.HasBatch(ctx, batch.NodeID, batch.BatchID)
	if err != nil {
		t.Fatalf("HasBatch failed: %v", err)
	}
	if has {
		t.Fatalf("expected HasBatch to be false before recording")
	}

	// First recording: newly accepted
	isNew, err := store.RecordBatch(ctx, batch, ingestedAt)
	if err != nil {
		t.Fatalf("RecordBatch failed: %v", err)
	}
	if !isNew {
		t.Fatalf("expected isNew=true on first recording")
	}

	// After recording: HasBatch is true
	has, err = store.HasBatch(ctx, batch.NodeID, batch.BatchID)
	if err != nil {
		t.Fatalf("HasBatch failed: %v", err)
	}
	if !has {
		t.Fatalf("expected HasBatch to be true after recording")
	}

	// Verify complete payload is persisted and retrievable
	payload, err := store.GetPayload(ctx, batch.NodeID, batch.BatchID)
	if err != nil {
		t.Fatalf("GetPayload failed: %v", err)
	}
	if payload.BatchID != batch.BatchID || payload.NodeID != batch.NodeID {
		t.Errorf("retrieved payload mismatch: got {%s, %s}", payload.NodeID, payload.BatchID)
	}
	if len(payload.Metrics) != 2 {
		t.Errorf("expected 2 metric samples in retrieved payload, got %d", len(payload.Metrics))
	}

	// Count is 1
	count, err := store.Count(ctx)
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected count 1, got %d", count)
	}

	// Second recording of the same key: duplicate detected
	isNew2, err := store.RecordBatch(ctx, batch, ingestedAt)
	if err != nil {
		t.Fatalf("RecordBatch duplicate failed with error: %v", err)
	}
	if isNew2 {
		t.Fatalf("expected isNew2=false on duplicate recording")
	}

	// Count remains 1
	count, _ = store.Count(ctx)
	if count != 1 {
		t.Errorf("expected count to remain 1, got %d", count)
	}
}

func TestSQLiteStore_CrossNodeIsolation(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	sharedBatchID := "shared-uuid-001"
	batchA := sampleBatch("node-A", sharedBatchID, 1)
	batchB := sampleBatch("node-B", sharedBatchID, 1)
	ingestedAt := time.Now().UTC()

	isNewA, errA := store.RecordBatch(ctx, batchA, ingestedAt)
	if errA != nil || !isNewA {
		t.Fatalf("expected node-A insert to succeed as new: isNew=%v, err=%v", isNewA, errA)
	}

	isNewB, errB := store.RecordBatch(ctx, batchB, ingestedAt)
	if errB != nil || !isNewB {
		t.Fatalf("expected node-B insert to succeed independently as new: isNew=%v, err=%v", isNewB, errB)
	}

	count, _ := store.Count(ctx)
	if count != 2 {
		t.Errorf("expected count 2 for distinct nodes with shared batch ID, got %d", count)
	}

	payloadA, errA := store.GetPayload(ctx, "node-A", sharedBatchID)
	payloadB, errB := store.GetPayload(ctx, "node-B", sharedBatchID)
	if errA != nil || errB != nil {
		t.Fatalf("failed to retrieve payloads: errA=%v, errB=%v", errA, errB)
	}
	if payloadA.NodeID != "node-A" || payloadB.NodeID != "node-B" {
		t.Errorf("node isolation violated in payload retrieval")
	}
}

func TestSQLiteStore_RestartRecovery(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "restart_test.db")

	batch := sampleBatch("node-restart", "batch-persist-01", 1)
	ingestedAt := time.Now().UTC()

	// Session 1: open, insert, close
	{
		store1, err := OpenSQLite(dbPath)
		if err != nil {
			t.Fatalf("failed to open store1: %v", err)
		}
		isNew, err := store1.RecordBatch(ctx, batch, ingestedAt)
		if err != nil || !isNew {
			t.Fatalf("session 1 record failed: isNew=%v, err=%v", isNew, err)
		}
		if err := store1.Close(); err != nil {
			t.Fatalf("failed to close store1: %v", err)
		}
	}

	// Session 2: reopen same file, verify persistence of both metadata and payload
	{
		store2, err := OpenSQLite(dbPath)
		if err != nil {
			t.Fatalf("failed to reopen store2: %v", err)
		}
		defer store2.Close()

		has, err := store2.HasBatch(ctx, batch.NodeID, batch.BatchID)
		if err != nil {
			t.Fatalf("HasBatch failed on store2: %v", err)
		}
		if !has {
			t.Errorf("expected batch to be present in store2 after reopen")
		}

		// Retrieve complete payload
		payload, err := store2.GetPayload(ctx, batch.NodeID, batch.BatchID)
		if err != nil {
			t.Fatalf("GetPayload failed on store2 after reopen: %v", err)
		}
		if payload.BatchID != batch.BatchID {
			t.Errorf("expected payload batch ID %q, got %q", batch.BatchID, payload.BatchID)
		}
		if len(payload.Metrics) != 2 {
			t.Errorf("expected 2 metric samples after reopen, got %d", len(payload.Metrics))
		}

		count, err := store2.Count(ctx)
		if err != nil {
			t.Fatalf("Count failed on store2: %v", err)
		}
		if count != 1 {
			t.Errorf("expected count 1 in store2, got %d", count)
		}

		// Re-recording the same batch must be recognized as duplicate
		isNew, err := store2.RecordBatch(ctx, batch, ingestedAt)
		if err != nil {
			t.Fatalf("RecordBatch duplicate failed with error: %v", err)
		}
		if isNew {
			t.Errorf("expected isNew=false on store2 duplicate replay, got true")
		}
	}
}

func TestSQLiteStore_AtomicTransactionRollback(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	// Inject error after metadata insert to simulate a failure/crash before payload insert completes
	store.HookAfterMetaInsert = func() error {
		return errors.New("simulated I/O failure during batch transaction")
	}

	batch := sampleBatch("node-rollback", "batch-fail-01", 1)
	isNew, err := store.RecordBatch(ctx, batch, time.Now().UTC())

	if err == nil {
		t.Fatalf("expected error from injected rollback failure, got nil")
	}
	if isNew {
		t.Errorf("expected isNew=false on rolled back transaction")
	}

	// Crucial: metadata record must NOT be committed when transaction rolls back!
	has, err := store.HasBatch(ctx, batch.NodeID, batch.BatchID)
	if err != nil {
		t.Fatalf("HasBatch failed: %v", err)
	}
	if has {
		t.Errorf("atomicity violation: batch was marked present in metadata despite transaction rollback!")
	}

	// Payload must also not exist
	_, err = store.GetPayload(ctx, batch.NodeID, batch.BatchID)
	if !errors.Is(err, ErrBatchNotFound) {
		t.Errorf("expected ErrBatchNotFound for rolled back batch, got %v", err)
	}

	count, _ := store.Count(ctx)
	if count != 0 {
		t.Errorf("expected count 0 after rollback, got %d", count)
	}
}

func TestSQLiteStore_GetRecentBatches(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	const total = 5

	for i := 1; i <= total; i++ {
		batch := sampleBatch("node-recent", fmt.Sprintf("b-%d", i), int64(i))
		ingestedAt := now.Add(time.Duration(i) * time.Millisecond)
		isNew, err := store.RecordBatch(ctx, batch, ingestedAt)
		if err != nil || !isNew {
			t.Fatalf("failed to record batch %d: %v", i, err)
		}
	}

	// Fetch recent 3 batches -> should return b-3, b-4, b-5 in chronological order
	recent, err := store.GetRecentBatches(ctx, 3)
	if err != nil {
		t.Fatalf("GetRecentBatches failed: %v", err)
	}
	if len(recent) != 3 {
		t.Fatalf("expected 3 recent batches, got %d", len(recent))
	}
	if recent[0].Batch.BatchID != "b-3" || recent[1].Batch.BatchID != "b-4" || recent[2].Batch.BatchID != "b-5" {
		t.Errorf("unexpected ordering in GetRecentBatches: {%s, %s, %s}",
			recent[0].Batch.BatchID, recent[1].Batch.BatchID, recent[2].Batch.BatchID)
	}
}

func TestSQLiteStore_ConcurrentDuplicateInsertion(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "concurrent_dedup.db")

	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	batch := sampleBatch("node-concurrent", "batch-race-01", 10)
	ingestedAt := time.Now().UTC()
	workers := 25

	var startBarrier sync.WaitGroup
	startBarrier.Add(1)

	var doneBarrier sync.WaitGroup
	doneBarrier.Add(workers)

	var newCount atomic.Int32
	var dupCount atomic.Int32
	var errCount atomic.Int32

	for i := 0; i < workers; i++ {
		go func() {
			defer doneBarrier.Done()
			startBarrier.Wait()

			isNew, err := store.RecordBatch(ctx, batch, ingestedAt)
			if err != nil {
				errCount.Add(1)
			} else if isNew {
				newCount.Add(1)
			} else {
				dupCount.Add(1)
			}
		}()
	}

	// Release all workers simultaneously
	startBarrier.Done()
	doneBarrier.Wait()

	if errCount.Load() > 0 {
		t.Errorf("expected 0 errors, got %d", errCount.Load())
	}
	if newCount.Load() != 1 {
		t.Errorf("expected exactly 1 newly recorded batch, got %d", newCount.Load())
	}
	if dupCount.Load() != int32(workers-1) {
		t.Errorf("expected exactly %d duplicates, got %d", workers-1, dupCount.Load())
	}

	count, _ := store.Count(ctx)
	if count != 1 {
		t.Errorf("expected database count 1, got %d", count)
	}

	payload, err := store.GetPayload(ctx, batch.NodeID, batch.BatchID)
	if err != nil {
		t.Fatalf("GetPayload failed after concurrent insertion: %v", err)
	}
	if payload.BatchID != batch.BatchID {
		t.Errorf("payload corrupted after concurrent insertion")
	}
}

func TestSQLiteStore_ValidationAndClosedErrors(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}

	// Nil batch
	if _, err := store.RecordBatch(ctx, nil, time.Now().UTC()); !errors.Is(err, ErrInvalidMeta) {
		t.Errorf("expected ErrInvalidMeta on nil batch, got %v", err)
	}

	// Zero ingestedAt
	batch := sampleBatch("node-1", "b-1", 1)
	if _, err := store.RecordBatch(ctx, batch, time.Time{}); !errors.Is(err, ErrInvalidMeta) {
		t.Errorf("expected ErrInvalidMeta on zero ingestedAt, got %v", err)
	}

	// Close store
	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Operations on closed store must fail with ErrStoreClosed
	if _, err := store.RecordBatch(ctx, batch, time.Now().UTC()); !errors.Is(err, ErrStoreClosed) {
		t.Errorf("expected ErrStoreClosed on RecordBatch, got %v", err)
	}
	if _, err := store.HasBatch(ctx, "node-1", "b-1"); !errors.Is(err, ErrStoreClosed) {
		t.Errorf("expected ErrStoreClosed on HasBatch, got %v", err)
	}
	if _, err := store.GetPayload(ctx, "node-1", "b-1"); !errors.Is(err, ErrStoreClosed) {
		t.Errorf("expected ErrStoreClosed on GetPayload, got %v", err)
	}
	if _, err := store.GetRecentBatches(ctx, 10); !errors.Is(err, ErrStoreClosed) {
		t.Errorf("expected ErrStoreClosed on GetRecentBatches, got %v", err)
	}
	if _, err := store.Count(ctx); !errors.Is(err, ErrStoreClosed) {
		t.Errorf("expected ErrStoreClosed on Count, got %v", err)
	}

	// Repeated close is safe no-op
	if err := store.Close(); err != nil {
		t.Errorf("expected repeated Close to be nil, got %v", err)
	}
}

func TestSQLiteStore_AutoCreateDirectory(t *testing.T) {
	tempDir := t.TempDir()
	nestedPath := filepath.Join(tempDir, "deep", "nested", "storage", "cp.db")

	store, err := OpenSQLite(nestedPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed on nested path: %v", err)
	}
	defer store.Close()

	batch := sampleBatch("node-dir", "batch-dir", 1)
	isNew, err := store.RecordBatch(context.Background(), batch, time.Now().UTC())
	if err != nil || !isNew {
		t.Errorf("failed to record batch in nested db: isNew=%v, err=%v", isNew, err)
	}
}

func TestSQLiteStore_InvalidPath(t *testing.T) {
	if _, err := OpenSQLite(""); err == nil {
		t.Errorf("expected error for empty db path, got nil")
	}
}

// TestSQLiteStore_DuplicateKeyNarrowDetection verifies that isDuplicateKeyError strictly
// distinguishes primary-key and unique constraint violations from other constraint failures
// (e.g., check constraints, foreign-key failures, NOT NULL violations) and generic errors.
func TestSQLiteStore_DuplicateKeyNarrowDetection(t *testing.T) {
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	// 1. Genuine primary-key duplicate on cp_ingested_batches
	query := `INSERT INTO cp_ingested_batches (node_id, batch_id, sequence_number, collected_at, ingested_at) VALUES ('n1', 'b1', 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');`
	if _, err := store.db.Exec(query); err != nil {
		t.Fatalf("initial insert failed: %v", err)
	}
	_, errDup := store.db.Exec(query)
	if errDup == nil {
		t.Fatalf("expected error on duplicate insert, got nil")
	}
	if !isDuplicateKeyError(errDup) {
		t.Errorf("expected isDuplicateKeyError=true for primary-key violation, got false (%v)", errDup)
	}

	// 2. Setup a helper table with various non-uniqueness constraints to test error discrimination
	if _, err := store.db.Exec(`
		CREATE TABLE test_constraint_discrimination (
			id TEXT PRIMARY KEY,
			val INT CHECK (val > 0),
			parent_id TEXT REFERENCES cp_ingested_batches(batch_id)
		);
	`); err != nil {
		t.Fatalf("creating test constraint table failed: %v", err)
	}

	// 2a. CHECK constraint failure
	_, errCheck := store.db.Exec(`INSERT INTO test_constraint_discrimination VALUES ('t1', -1, NULL);`)
	if errCheck == nil {
		t.Fatalf("expected CHECK constraint failure, got nil")
	}
	if isDuplicateKeyError(errCheck) {
		t.Errorf("false positive: isDuplicateKeyError treated CHECK constraint error as duplicate: %v", errCheck)
	}

	// 2b. FOREIGN KEY constraint failure
	_, errFK := store.db.Exec(`INSERT INTO test_constraint_discrimination VALUES ('t2', 10, 'nonexistent_batch');`)
	if errFK == nil {
		t.Fatalf("expected FOREIGN KEY constraint failure, got nil")
	}
	if isDuplicateKeyError(errFK) {
		t.Errorf("false positive: isDuplicateKeyError treated FOREIGN KEY error as duplicate: %v", errFK)
	}

	// 2c. NOT NULL constraint failure
	_, errNotNull := store.db.Exec(`INSERT INTO test_constraint_discrimination VALUES (NULL, 10, NULL);`)
	if errNotNull == nil {
		t.Fatalf("expected NOT NULL constraint failure, got nil")
	}
	if isDuplicateKeyError(errNotNull) {
		t.Errorf("false positive: isDuplicateKeyError treated NOT NULL error as duplicate: %v", errNotNull)
	}

	// 2d. Generic non-database error
	genericErr := errors.New("network socket timeout during query")
	if isDuplicateKeyError(genericErr) {
		t.Errorf("false positive: isDuplicateKeyError treated generic error as duplicate: %v", genericErr)
	}

	// 2e. ErrDuplicateBatch sentinel
	if !isDuplicateKeyError(ErrDuplicateBatch) {
		t.Errorf("expected isDuplicateKeyError=true for ErrDuplicateBatch sentinel")
	}
}

// TestSQLiteStore_PragmasReadback verifies that all durability, concurrency, and integrity
// PRAGMAs are active on the configured single-connection pool.
func TestSQLiteStore_PragmasReadback(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "pragmas_verify.db")

	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	// 1. journal_mode must be WAL
	var journalMode string
	if err := store.db.QueryRow("PRAGMA journal_mode;").Scan(&journalMode); err != nil {
		t.Fatalf("failed to query journal_mode: %v", err)
	}
	if journalMode != "wal" {
		t.Errorf("expected journal_mode 'wal', got %q", journalMode)
	}

	// 2. synchronous must be FULL (2)
	var synchronous int
	if err := store.db.QueryRow("PRAGMA synchronous;").Scan(&synchronous); err != nil {
		t.Fatalf("failed to query synchronous: %v", err)
	}
	if synchronous != 2 {
		t.Errorf("expected synchronous=2 (FULL), got %d", synchronous)
	}

	// 3. foreign_keys must be ON (1)
	var foreignKeys int
	if err := store.db.QueryRow("PRAGMA foreign_keys;").Scan(&foreignKeys); err != nil {
		t.Fatalf("failed to query foreign_keys: %v", err)
	}
	if foreignKeys != 1 {
		t.Errorf("expected foreign_keys=1 (ON), got %d", foreignKeys)
	}

	// 4. busy_timeout must be 5000ms
	var busyTimeout int
	if err := store.db.QueryRow("PRAGMA busy_timeout;").Scan(&busyTimeout); err != nil {
		t.Fatalf("failed to query busy_timeout: %v", err)
	}
	if busyTimeout != 5000 {
		t.Errorf("expected busy_timeout=5000, got %d", busyTimeout)
	}
}

// TestSQLiteStore_CorruptPayload_GetPayload asserts that GetPayload on a corrupted row
// returns an error that unwraps to ErrCorruptPayload and contains contextual node and batch IDs.
func TestSQLiteStore_CorruptPayload_GetPayload(t *testing.T) {
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	batch := sampleBatch("node-c1", "batch-c1", 1)
	if _, err := store.RecordBatch(ctx, batch, time.Now().UTC()); err != nil {
		t.Fatalf("RecordBatch failed: %v", err)
	}

	// Manually corrupt the payload JSON in the database
	if _, err := store.db.Exec(`UPDATE cp_telemetry_payloads SET payload_json = '{"metrics": [corrupted...' WHERE node_id = 'node-c1' AND batch_id = 'batch-c1'`); err != nil {
		t.Fatalf("failed to inject payload corruption: %v", err)
	}

	payload, err := store.GetPayload(ctx, "node-c1", "batch-c1")
	if payload != nil {
		t.Errorf("expected nil payload on corruption, got %+v", payload)
	}
	if err == nil {
		t.Fatalf("expected error on corrupt payload, got nil")
	}

	if !errors.Is(err, ErrCorruptPayload) {
		t.Errorf("expected errors.Is(err, ErrCorruptPayload) to be true, got %v", err)
	}

	var corruptErr *CorruptPayloadError
	if !errors.As(err, &corruptErr) {
		t.Fatalf("expected error to cast to *CorruptPayloadError, got %T", err)
	}
	if corruptErr.NodeID != "node-c1" || corruptErr.BatchID != "batch-c1" {
		t.Errorf("unexpected CorruptPayloadError fields: node=%q, batch=%q", corruptErr.NodeID, corruptErr.BatchID)
	}
	if !strings.Contains(corruptErr.Error(), "node-c1") || !strings.Contains(corruptErr.Error(), "batch-c1") {
		t.Errorf("expected error message to contain node and batch IDs, got: %s", corruptErr.Error())
	}
}

// TestSQLiteStore_CorruptPayload_GetRecentBatches_TolerantRecovery asserts that GetRecentBatches
// recovers valid batches in chronological order even when some rows have corrupt JSON, while
// returning a joined error surfacing all corrupt rows.
func TestSQLiteStore_CorruptPayload_GetRecentBatches_TolerantRecovery(t *testing.T) {
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	baseTime := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)

	// Ingest 5 batches chronologically
	for i := 1; i <= 5; i++ {
		batch := sampleBatch("node-mix", fmt.Sprintf("batch-%02d", i), int64(i))
		ingestedAt := baseTime.Add(time.Duration(i) * time.Minute)
		if _, err := store.RecordBatch(ctx, batch, ingestedAt); err != nil {
			t.Fatalf("RecordBatch %d failed: %v", i, err)
		}
	}

	// Corrupt batches 2 and 4
	corruptQuery := `UPDATE cp_telemetry_payloads SET payload_json = '{invalid-json' WHERE batch_id IN ('batch-02', 'batch-04')`
	if _, err := store.db.Exec(corruptQuery); err != nil {
		t.Fatalf("failed to inject corruption: %v", err)
	}

	// Retrieve recent batches
	recent, err := store.GetRecentBatches(ctx, 10)
	if err == nil {
		t.Fatalf("expected error containing corruption diagnostics, got nil")
	}

	if !errors.Is(err, ErrCorruptPayload) {
		t.Errorf("expected errors.Is(err, ErrCorruptPayload) to be true, got %v", err)
	}

	// Verify error mentions both corrupted batches
	errStr := err.Error()
	if !strings.Contains(errStr, "batch-02") || !strings.Contains(errStr, "batch-04") {
		t.Errorf("expected joined error to name both corrupt batches, got: %s", errStr)
	}

	// Exactly 3 valid batches should be recovered
	if len(recent) != 3 {
		t.Fatalf("expected 3 valid batches recovered, got %d", len(recent))
	}

	// Chronological order: batch-01, batch-03, batch-05
	expectedIDs := []string{"batch-01", "batch-03", "batch-05"}
	for i, expectedID := range expectedIDs {
		if recent[i].Batch.BatchID != expectedID {
			t.Errorf("at index %d: expected batch %q, got %q", i, expectedID, recent[i].Batch.BatchID)
		}
	}

	// Verify that corrupted rows were NOT deleted or altered in the database (no auto-repair/deletion)
	var rowCount int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM cp_telemetry_payloads WHERE batch_id IN ('batch-02', 'batch-04')`).Scan(&rowCount); err != nil {
		t.Fatalf("failed to count corrupt rows: %v", err)
	}
	if rowCount != 2 {
		t.Errorf("corrupted rows must remain in the database for post-mortem forensics, expected 2 rows, found %d", rowCount)
	}
}

// TestSQLiteStore_CorruptPayload_AllCorrupt asserts that when all rows are corrupted,
// GetRecentBatches returns an empty non-nil slice and the joined error.
func TestSQLiteStore_CorruptPayload_AllCorrupt(t *testing.T) {
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	for i := 1; i <= 2; i++ {
		batch := sampleBatch("node-all-c", fmt.Sprintf("batch-%d", i), int64(i))
		if _, err := store.RecordBatch(ctx, batch, time.Now().UTC()); err != nil {
			t.Fatalf("RecordBatch failed: %v", err)
		}
	}

	if _, err := store.db.Exec(`UPDATE cp_telemetry_payloads SET payload_json = 'corrupt'`); err != nil {
		t.Fatalf("failed to corrupt all rows: %v", err)
	}

	recent, err := store.GetRecentBatches(ctx, 10)
	if err == nil {
		t.Fatalf("expected error on all corrupt, got nil")
	}
	if !errors.Is(err, ErrCorruptPayload) {
		t.Errorf("expected ErrCorruptPayload, got %v", err)
	}
	if recent == nil {
		t.Fatalf("expected non-nil empty slice, got nil")
	}
	if len(recent) != 0 {
		t.Errorf("expected 0 recovered batches, got %d", len(recent))
	}
}

// TestSQLiteStore_QueryFailure_DistinguishedFromCorruption asserts that database query/connection
// errors (closed store, canceled context) are properly differentiated from payload corruption.
func TestSQLiteStore_QueryFailure_DistinguishedFromCorruption(t *testing.T) {
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}

	ctx := context.Background()
	batch := sampleBatch("node-qf", "batch-qf", 1)
	if _, err := store.RecordBatch(ctx, batch, time.Now().UTC()); err != nil {
		t.Fatalf("RecordBatch failed: %v", err)
	}

	// 1. Canceled context during query
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()

	_, errCanceled := store.GetRecentBatches(canceledCtx, 5)
	if errCanceled == nil {
		t.Fatalf("expected error on canceled context, got nil")
	}
	if errors.Is(errCanceled, ErrCorruptPayload) {
		t.Errorf("canceled context error must not be classified as ErrCorruptPayload: %v", errCanceled)
	}

	// 2. Closed store
	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	_, errClosedRecent := store.GetRecentBatches(ctx, 5)
	if !errors.Is(errClosedRecent, ErrStoreClosed) {
		t.Errorf("expected ErrStoreClosed, got %v", errClosedRecent)
	}
	if errors.Is(errClosedRecent, ErrCorruptPayload) {
		t.Errorf("closed store error must not be classified as ErrCorruptPayload")
	}

	_, errClosedPayload := store.GetPayload(ctx, "node-qf", "batch-qf")
	if !errors.Is(errClosedPayload, ErrStoreClosed) {
		t.Errorf("expected ErrStoreClosed, got %v", errClosedPayload)
	}
	if errors.Is(errClosedPayload, ErrCorruptPayload) {
		t.Errorf("closed store error must not be classified as ErrCorruptPayload")
	}
}

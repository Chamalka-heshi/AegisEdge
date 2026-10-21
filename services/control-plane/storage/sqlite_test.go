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

func TestSQLiteStore_Node_RegisterAndGet(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC().Truncate(time.Millisecond)
	record := &NodeRecord{
		NodeID:          "node-reg-1",
		Status:          types.NodeStatusHealthy,
		LastSeen:        now,
		RegisteredAt:    now,
		AgentVersion:    "v1.0.0",
		ProtocolVersion: "v1",
		Hostname:        "worker-alpha",
		OS:              "linux",
		Architecture:    "amd64",
		Capabilities:    []string{"gpu", "tpu"},
		Labels:          map[string]string{"zone": "us-east-1", "tier": "edge"},
		SequenceNumber:  0,
	}

	isNew, err := store.RegisterNode(ctx, record)
	if err != nil {
		t.Fatalf("RegisterNode failed: %v", err)
	}
	if !isNew {
		t.Fatalf("expected isNew=true on first registration")
	}

	retrieved, err := store.GetNode(ctx, "node-reg-1")
	if err != nil {
		t.Fatalf("GetNode failed: %v", err)
	}

	if retrieved.NodeID != record.NodeID {
		t.Errorf("expected NodeID %q, got %q", record.NodeID, retrieved.NodeID)
	}
	if retrieved.Status != types.NodeStatusHealthy {
		t.Errorf("expected Status %q, got %q", types.NodeStatusHealthy, retrieved.Status)
	}
	if retrieved.Hostname != "worker-alpha" {
		t.Errorf("expected Hostname 'worker-alpha', got %q", retrieved.Hostname)
	}
	if retrieved.OS != "linux" || retrieved.Architecture != "amd64" {
		t.Errorf("expected linux/amd64, got %s/%s", retrieved.OS, retrieved.Architecture)
	}
	if len(retrieved.Capabilities) != 2 || retrieved.Capabilities[0] != "gpu" || retrieved.Capabilities[1] != "tpu" {
		t.Errorf("unexpected capabilities: %v", retrieved.Capabilities)
	}
	if retrieved.Labels["zone"] != "us-east-1" || retrieved.Labels["tier"] != "edge" {
		t.Errorf("unexpected labels: %v", retrieved.Labels)
	}
	if !retrieved.RegisteredAt.Equal(now) {
		t.Errorf("expected RegisteredAt %v, got %v", now, retrieved.RegisteredAt)
	}
}

func TestSQLiteStore_Node_DuplicateRegistration_Semantics(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	t0 := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Millisecond)
	initial := &NodeRecord{
		NodeID:          "node-dup-1",
		Status:          types.NodeStatusHealthy,
		LastSeen:        t0,
		RegisteredAt:    t0,
		AgentVersion:    "v1.0.0",
		ProtocolVersion: "v1",
		Hostname:        "orig-host",
		OS:              "linux",
		Architecture:    "arm64",
		Capabilities:    []string{"camera"},
		Labels:          map[string]string{"rack": "r1"},
		SequenceNumber:  50,
	}

	isNew, err := store.RegisterNode(ctx, initial)
	if err != nil || !isNew {
		t.Fatalf("initial register failed: isNew=%v, err=%v", isNew, err)
	}

	// Change status via heartbeat to DEGRADED
	t1 := t0.Add(10 * time.Minute)
	if err := store.UpdateNodeHeartbeat(ctx, "node-dup-1", types.NodeStatusDegraded, 75, t1); err != nil {
		t.Fatalf("UpdateNodeHeartbeat failed: %v", err)
	}

	// Re-register with empty optional fields
	t2 := time.Now().UTC().Truncate(time.Millisecond)
	dupEmpty := &NodeRecord{
		NodeID:          "node-dup-1",
		Status:          types.NodeStatusHealthy, // request says healthy, but existing is degraded
		LastSeen:        t2,
		RegisteredAt:    t2,  // new timestamp in payload, but must NOT overwrite original
		AgentVersion:    "",  // empty: must not erase "v1.0.0"
		ProtocolVersion: "",  // empty: must not erase "v1"
		Hostname:        "",  // empty: must not erase "orig-host"
		OS:              "",  // empty: must not erase "linux"
		Architecture:    "",  // empty: must not erase "arm64"
		Capabilities:    nil, // nil: must not erase ["camera"]
		Labels:          nil, // nil: must not erase {"rack": "r1"}
		SequenceNumber:  10,  // lower sequence: must not regress 75
	}

	isNewDup, err := store.RegisterNode(ctx, dupEmpty)
	if err != nil {
		t.Fatalf("RegisterNode duplicate failed: %v", err)
	}
	if isNewDup {
		t.Fatalf("expected isNewDup=false on duplicate registration")
	}

	// Verify existing metadata preserved
	afterDup, err := store.GetNode(ctx, "node-dup-1")
	if err != nil {
		t.Fatalf("GetNode failed: %v", err)
	}
	if !afterDup.RegisteredAt.Equal(t0) {
		t.Errorf("RegisteredAt was mutated: expected %v, got %v", t0, afterDup.RegisteredAt)
	}
	if afterDup.Status != types.NodeStatusDegraded {
		t.Errorf("Status was reset: expected DEGRADED, got %v", afterDup.Status)
	}
	if afterDup.SequenceNumber != 75 {
		t.Errorf("SequenceNumber regressed: expected 75, got %d", afterDup.SequenceNumber)
	}
	if afterDup.Hostname != "orig-host" {
		t.Errorf("Hostname erased: expected 'orig-host', got %q", afterDup.Hostname)
	}
	if afterDup.AgentVersion != "v1.0.0" {
		t.Errorf("AgentVersion erased: expected 'v1.0.0', got %q", afterDup.AgentVersion)
	}
	if len(afterDup.Capabilities) != 1 || afterDup.Capabilities[0] != "camera" {
		t.Errorf("Capabilities erased: got %v", afterDup.Capabilities)
	}
	if afterDup.Labels["rack"] != "r1" {
		t.Errorf("Labels erased: got %v", afterDup.Labels)
	}
	if !afterDup.LastSeen.Equal(t2) {
		t.Errorf("LastSeen not updated: expected %v, got %v", t2, afterDup.LastSeen)
	}

	// Re-register with explicit new non-empty metadata
	t3 := t2.Add(5 * time.Minute)
	dupUpdated := &NodeRecord{
		NodeID:         "node-dup-1",
		LastSeen:       t3,
		RegisteredAt:   t3,
		Hostname:       "new-host",
		AgentVersion:   "v2.0.0",
		Capabilities:   []string{"camera", "radar"},
		Labels:         map[string]string{"rack": "r2", "env": "prod"},
		SequenceNumber: 80,
	}
	isNewDup2, err := store.RegisterNode(ctx, dupUpdated)
	if err != nil || isNewDup2 {
		t.Fatalf("re-register with new metadata failed: isNew=%v, err=%v", isNewDup2, err)
	}

	afterDup2, _ := store.GetNode(ctx, "node-dup-1")
	if afterDup2.Hostname != "new-host" {
		t.Errorf("expected updated Hostname 'new-host', got %q", afterDup2.Hostname)
	}
	if afterDup2.AgentVersion != "v2.0.0" {
		t.Errorf("expected updated AgentVersion 'v2.0.0', got %q", afterDup2.AgentVersion)
	}
	if len(afterDup2.Capabilities) != 2 || afterDup2.Capabilities[1] != "radar" {
		t.Errorf("expected updated Capabilities, got %v", afterDup2.Capabilities)
	}
	if afterDup2.Labels["env"] != "prod" {
		t.Errorf("expected updated Labels, got %v", afterDup2.Labels)
	}
	if afterDup2.SequenceNumber != 80 {
		t.Errorf("expected SequenceNumber 80, got %d", afterDup2.SequenceNumber)
	}
	if !afterDup2.RegisteredAt.Equal(t0) {
		t.Errorf("RegisteredAt must still be original t0: expected %v, got %v", t0, afterDup2.RegisteredAt)
	}
}

func TestSQLiteStore_Node_List_DeterministicOrdering(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	t0 := time.Now().UTC().Truncate(time.Millisecond)
	nodes := []*NodeRecord{
		{NodeID: "node-c", RegisteredAt: t0.Add(20 * time.Second), LastSeen: t0},
		{NodeID: "node-a", RegisteredAt: t0.Add(10 * time.Second), LastSeen: t0},
		{NodeID: "node-b", RegisteredAt: t0.Add(30 * time.Second), LastSeen: t0},
		// Same timestamp to test tie-breaker by node_id ASC
		{NodeID: "node-z", RegisteredAt: t0, LastSeen: t0},
		{NodeID: "node-m", RegisteredAt: t0, LastSeen: t0},
	}

	for _, n := range nodes {
		if _, err := store.RegisterNode(ctx, n); err != nil {
			t.Fatalf("RegisterNode %s failed: %v", n.NodeID, err)
		}
	}

	list, err := store.ListNodes(ctx)
	if err != nil {
		t.Fatalf("ListNodes failed: %v", err)
	}
	if len(list) != 5 {
		t.Fatalf("expected 5 nodes, got %d", len(list))
	}

	// Expected order:
	// 1. t0: node-m (alphabetical tie-breaker)
	// 2. t0: node-z
	// 3. t0+10s: node-a
	// 4. t0+20s: node-c
	// 5. t0+30s: node-b
	expectedOrder := []string{"node-m", "node-z", "node-a", "node-c", "node-b"}
	for i, exp := range expectedOrder {
		if list[i].NodeID != exp {
			t.Errorf("position %d: expected %q, got %q", i, exp, list[i].NodeID)
		}
	}
}

func TestSQLiteStore_Node_Heartbeat_SuccessAndUnchanged(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC().Truncate(time.Millisecond)
	_, err = store.RegisterNode(ctx, &NodeRecord{
		NodeID:       "node-hb-1",
		Status:       types.NodeStatusHealthy,
		RegisteredAt: now,
		LastSeen:     now,
	})
	if err != nil {
		t.Fatalf("RegisterNode failed: %v", err)
	}

	// 1. Successful heartbeat
	hbTime1 := now.Add(5 * time.Second)
	err = store.UpdateNodeHeartbeat(ctx, "node-hb-1", types.NodeStatusHealthy, 10, hbTime1)
	if err != nil {
		t.Fatalf("UpdateNodeHeartbeat failed: %v", err)
	}

	rec, _ := store.GetNode(ctx, "node-hb-1")
	if rec.SequenceNumber != 10 || !rec.LastSeen.Equal(hbTime1) {
		t.Errorf("heartbeat 1 verification failed: seq=%d, lastSeen=%v", rec.SequenceNumber, rec.LastSeen)
	}

	// 2. Unchanged heartbeat: same status and sequence number!
	// Must NOT return ErrNodeNotFound (protects against RowsAffected() == 0 bug)
	hbTime2 := hbTime1.Add(5 * time.Second)
	err = store.UpdateNodeHeartbeat(ctx, "node-hb-1", types.NodeStatusHealthy, 10, hbTime2)
	if err != nil {
		t.Fatalf("identical heartbeat failed: %v", err)
	}

	rec2, _ := store.GetNode(ctx, "node-hb-1")
	if !rec2.LastSeen.Equal(hbTime2) {
		t.Errorf("expected LastSeen %v on unchanged heartbeat, got %v", hbTime2, rec2.LastSeen)
	}
}

func TestSQLiteStore_Node_Heartbeat_MissingNode(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	err = store.UpdateNodeHeartbeat(ctx, "missing-node", types.NodeStatusHealthy, 1, time.Now().UTC())
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}
}

func TestSQLiteStore_Node_Heartbeat_MonotonicSequence(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC().Truncate(time.Millisecond)
	_, _ = store.RegisterNode(ctx, &NodeRecord{
		NodeID:         "node-hb-mono",
		RegisteredAt:   now,
		SequenceNumber: 100,
	})

	// Delayed heartbeat with lower sequence number (50 < 100)
	later := now.Add(10 * time.Second)
	err = store.UpdateNodeHeartbeat(ctx, "node-hb-mono", types.NodeStatusHealthy, 50, later)
	if err != nil {
		t.Fatalf("UpdateNodeHeartbeat failed: %v", err)
	}

	rec, _ := store.GetNode(ctx, "node-hb-mono")
	if rec.SequenceNumber != 100 {
		t.Errorf("SequenceNumber regressed: expected 100, got %d", rec.SequenceNumber)
	}
	if !rec.LastSeen.Equal(later) {
		t.Errorf("LastSeen must advance to %v, got %v", later, rec.LastSeen)
	}
}

func TestSQLiteStore_Node_TelemetryActivity_AtomicWithBatch(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	regTime := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	_, err = store.RegisterNode(ctx, &NodeRecord{
		NodeID:         "node-act-atomic",
		RegisteredAt:   regTime,
		LastSeen:       regTime,
		SequenceNumber: 10,
	})
	if err != nil {
		t.Fatalf("RegisterNode failed: %v", err)
	}

	// 1. Ingest new batch
	batch := sampleBatch("node-act-atomic", "b-atom-1", 50)
	ingestedAt := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	isNew, err := store.RecordBatch(ctx, batch, ingestedAt)
	if err != nil || !isNew {
		t.Fatalf("RecordBatch failed: isNew=%v, err=%v", isNew, err)
	}

	rec, _ := store.GetNode(ctx, "node-act-atomic")
	if rec.SequenceNumber != 50 {
		t.Errorf("expected sequence number 50 in cp_nodes, got %d", rec.SequenceNumber)
	}
	if !rec.LastSeen.Equal(ingestedAt) {
		t.Errorf("expected LastSeen %v, got %v", ingestedAt, rec.LastSeen)
	}

	// 2. Duplicate batch: must NOT update LastSeen or sequence number
	ingestedAtDup := ingestedAt.Add(1 * time.Hour)
	isNewDup, err := store.RecordBatch(ctx, batch, ingestedAtDup)
	if err != nil {
		t.Fatalf("duplicate RecordBatch failed: %v", err)
	}
	if isNewDup {
		t.Fatalf("expected isNewDup=false for duplicate batch")
	}

	recDup, _ := store.GetNode(ctx, "node-act-atomic")
	if recDup.SequenceNumber != 50 {
		t.Errorf("duplicate batch modified sequence number: expected 50, got %d", recDup.SequenceNumber)
	}
	if !recDup.LastSeen.Equal(ingestedAt) {
		t.Errorf("duplicate batch modified LastSeen: expected %v, got %v", ingestedAt, recDup.LastSeen)
	}
}

func TestSQLiteStore_Node_TelemetryActivity_UnknownNode_NoFabrication(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	// Ingest batch for unregistered node
	batch := sampleBatch("ghost-node", "b-ghost-1", 100)
	ingestedAt := time.Now().UTC()
	isNew, err := store.RecordBatch(ctx, batch, ingestedAt)
	if err != nil || !isNew {
		t.Fatalf("RecordBatch failed for unknown node: isNew=%v, err=%v", isNew, err)
	}

	// Batch must be safely persisted
	has, err := store.HasBatch(ctx, "ghost-node", "b-ghost-1")
	if err != nil || !has {
		t.Fatalf("expected batch to be persisted: has=%v, err=%v", has, err)
	}

	// But no synthetic node should exist in cp_nodes
	_, err = store.GetNode(ctx, "ghost-node")
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("expected ErrNodeNotFound for unknown node, got %v", err)
	}

	list, err := store.ListNodes(ctx)
	if err != nil {
		t.Fatalf("ListNodes failed: %v", err)
	}
	if len(list) != 0 {
		t.Errorf("expected 0 nodes in registry, got %d", len(list))
	}
}

func TestSQLiteStore_Node_TelemetryActivity_RollbackOnFailure(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	_, _ = store.RegisterNode(ctx, &NodeRecord{
		NodeID:         "node-rollback-1",
		RegisteredAt:   time.Now().UTC(),
		SequenceNumber: 5,
	})

	// Inject error after payload insert
	store.HookAfterPayloadInsert = func() error {
		return errors.New("simulated disk error before commit")
	}

	batch := sampleBatch("node-rollback-1", "b-rollback-1", 99)
	isNew, err := store.RecordBatch(ctx, batch, time.Now().UTC())
	if err == nil {
		t.Fatalf("expected RecordBatch to fail on injected hook, got nil error")
	}
	if isNew {
		t.Fatalf("expected isNew=false on failure")
	}

	// Verify complete transaction rollback:
	// 1. Batch metadata NOT stored
	has, _ := store.HasBatch(ctx, "node-rollback-1", "b-rollback-1")
	if has {
		t.Errorf("batch metadata was persisted despite rollback")
	}

	// 2. Payload NOT stored
	_, err = store.GetPayload(ctx, "node-rollback-1", "b-rollback-1")
	if !errors.Is(err, ErrBatchNotFound) {
		t.Errorf("payload was persisted despite rollback")
	}

	// 3. Node activity NOT mutated
	rec, _ := store.GetNode(ctx, "node-rollback-1")
	if rec.SequenceNumber != 5 {
		t.Errorf("node sequence was mutated despite rollback: expected 5, got %d", rec.SequenceNumber)
	}
}

func TestSQLiteStore_Node_PersistenceReopen(t *testing.T) {
	ctx := context.Background()
	dbDir := t.TempDir()
	dbPath := filepath.Join(dbDir, "node_persistence.db")

	store1, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite 1 failed: %v", err)
	}

	t0 := time.Date(2026, 9, 15, 8, 30, 0, 0, time.UTC)
	_, err = store1.RegisterNode(ctx, &NodeRecord{
		NodeID:          "node-disk-1",
		Status:          types.NodeStatusHealthy,
		RegisteredAt:    t0,
		LastSeen:        t0,
		Hostname:        "disk-host-alpha",
		AgentVersion:    "v1.2.3",
		ProtocolVersion: "v1",
		OS:              "linux",
		Architecture:    "amd64",
		Capabilities:    []string{"nvme", "gpu"},
		Labels:          map[string]string{"env": "staging"},
		SequenceNumber:  42,
	})
	if err != nil {
		t.Fatalf("RegisterNode on store1 failed: %v", err)
	}

	if err := store1.Close(); err != nil {
		t.Fatalf("store1 Close failed: %v", err)
	}

	// Reopen database from same file path
	store2, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite 2 failed: %v", err)
	}
	defer store2.Close()

	rec, err := store2.GetNode(ctx, "node-disk-1")
	if err != nil {
		t.Fatalf("GetNode on store2 failed: %v", err)
	}

	if rec.NodeID != "node-disk-1" {
		t.Errorf("expected node-disk-1, got %q", rec.NodeID)
	}
	if rec.Hostname != "disk-host-alpha" || rec.AgentVersion != "v1.2.3" {
		t.Errorf("unexpected metadata after reopen: %s / %s", rec.Hostname, rec.AgentVersion)
	}
	if rec.SequenceNumber != 42 {
		t.Errorf("expected sequence 42 after reopen, got %d", rec.SequenceNumber)
	}
	if len(rec.Capabilities) != 2 || rec.Capabilities[0] != "nvme" {
		t.Errorf("unexpected capabilities after reopen: %v", rec.Capabilities)
	}
	if rec.Labels["env"] != "staging" {
		t.Errorf("unexpected labels after reopen: %v", rec.Labels)
	}
	if !rec.RegisteredAt.Equal(t0) {
		t.Errorf("expected RegisteredAt %v after reopen, got %v", t0, rec.RegisteredAt)
	}

	list, err := store2.ListNodes(ctx)
	if err != nil || len(list) != 1 {
		t.Fatalf("ListNodes on store2 failed: count=%d, err=%v", len(list), err)
	}
}

func TestSQLiteStore_Node_CorruptJSON_Handling(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	_, _ = store.RegisterNode(ctx, &NodeRecord{
		NodeID:       "node-corrupt-1",
		RegisteredAt: now,
		LastSeen:     now,
	})

	// Manually corrupt capabilities_json in database
	_, err = store.db.Exec(`UPDATE cp_nodes SET capabilities_json = 'not-valid-json' WHERE node_id = 'node-corrupt-1';`)
	if err != nil {
		t.Fatalf("raw DB corruption failed: %v", err)
	}

	// 1. GetNode on corrupted record
	rec, err := store.GetNode(ctx, "node-corrupt-1")
	if err == nil {
		t.Fatalf("expected error on corrupt node record, got nil")
	}
	if !errors.Is(err, ErrCorruptNodeRecord) {
		t.Errorf("expected error to unwrap to ErrCorruptNodeRecord, got %v", err)
	}
	if rec == nil || rec.NodeID != "node-corrupt-1" {
		t.Errorf("expected partially recovered record, got %v", rec)
	}

	// 2. ListNodes on table containing corrupted record
	list, err := store.ListNodes(ctx)
	if err == nil {
		t.Fatalf("expected ListNodes to return error on corrupt JSON, got nil")
	}
	if !errors.Is(err, ErrCorruptNodeRecord) {
		t.Errorf("expected error to unwrap to ErrCorruptNodeRecord, got %v", err)
	}
	if len(list) != 1 || list[0].NodeID != "node-corrupt-1" {
		t.Errorf("expected ListNodes to return the node despite corrupt JSON, got %v", list)
	}
}

func TestSQLiteStore_Node_ConcurrentUpdates(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	_, err = store.RegisterNode(ctx, &NodeRecord{
		NodeID:         "node-conc-1",
		RegisteredAt:   now,
		LastSeen:       now,
		SequenceNumber: 0,
	})
	if err != nil {
		t.Fatalf("RegisterNode failed: %v", err)
	}

	const goroutines = 25
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// Goroutines updating heartbeat
	for i := 0; i < goroutines; i++ {
		seq := int64((i + 1) * 2)
		go func(s int64) {
			defer wg.Done()
			_ = store.UpdateNodeHeartbeat(ctx, "node-conc-1", types.NodeStatusHealthy, s, time.Now().UTC())
		}(seq)
	}

	// Goroutines updating telemetry activity
	for i := 0; i < goroutines; i++ {
		seq := int64((i + 1) * 3)
		go func(s int64) {
			defer wg.Done()
			_, _ = store.UpdateNodeTelemetryActivity(ctx, "node-conc-1", s, time.Now().UTC())
		}(seq)
	}

	wg.Wait()

	rec, err := store.GetNode(ctx, "node-conc-1")
	if err != nil {
		t.Fatalf("GetNode failed: %v", err)
	}

	// Max sequence sent is goroutines * 3 = 75
	expectedMax := int64(goroutines * 3)
	if rec.SequenceNumber != expectedMax {
		t.Errorf("expected sequence number to reach %d under concurrent monotonic updates, got %d", expectedMax, rec.SequenceNumber)
	}
}

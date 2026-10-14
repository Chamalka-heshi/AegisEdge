package storage

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
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

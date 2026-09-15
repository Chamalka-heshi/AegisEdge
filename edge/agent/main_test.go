package main

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/telemetry"
)

func newDiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestRunCollectionStep_SuccessAndConfirmation(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "step_test.db")
	logger := newDiscardLogger()

	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	nodeID := "node-step-test"
	gen, err := telemetry.NewSimulatedGenerator(nodeID, 0)
	if err != nil {
		t.Fatalf("NewSimulatedGenerator failed: %v", err)
	}

	// Execute step 1
	b1, err := runCollectionStep(ctx, gen, store, logger)
	if err != nil {
		t.Fatalf("runCollectionStep 1 failed: %v", err)
	}
	if b1.SequenceNumber != 0 {
		t.Errorf("expected sequence 0, got %d", b1.SequenceNumber)
	}

	// Verify it was persisted and can be queried directly from store
	rec1, err := store.GetBatch(ctx, b1.BatchID)
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if rec1.SyncStatus != storage.SyncStatusPending {
		t.Errorf("expected status PENDING, got %s", rec1.SyncStatus)
	}

	// Execute step 2
	b2, err := runCollectionStep(ctx, gen, store, logger)
	if err != nil {
		t.Fatalf("runCollectionStep 2 failed: %v", err)
	}
	if b2.SequenceNumber != 1 {
		t.Errorf("expected sequence 1, got %d", b2.SequenceNumber)
	}

	// Verify total count in database is 2
	count, err := store.CountBatches(ctx)
	if err != nil {
		t.Fatalf("CountBatches failed: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 batches stored, got %d", count)
	}
}

func TestRunCollectionStep_PersistenceFailure(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "failure_step.db")
	logger := newDiscardLogger()

	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}

	// Close store prematurely to induce persistence failure
	_ = store.Close()

	gen, _ := telemetry.NewSimulatedGenerator("node-err", 0)

	// Collection step must return an error and not pretend it was accepted
	_, err = runCollectionStep(ctx, gen, store, logger)
	if err == nil {
		t.Fatal("expected error on closed store, got nil")
	}
}

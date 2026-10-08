package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

func newTestCheckpointStore(t *testing.T) (*SQLiteStore, string) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "checkpoint_test.db")
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("failed to create test SQLite store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store, dbPath
}

func TestCheckpointStore_MigrationV9_FreshDatabase(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestCheckpointStore(t)

	// Verify table exists by saving and retrieving a checkpoint
	cp := &StoredCheckpoint{
		CheckpointID:          "cp-fresh-1",
		NodeID:                "node-fresh-1",
		State:                 "RECOVERY_NOT_REQUIRED",
		StartedAt:             time.Now().UTC(),
		RecoveryReason:        "clean startup",
		LastReconciledAttempt: 0,
		SchemaVersion:         9,
		CreatedAt:             time.Now().UTC(),
		UpdatedAt:             time.Now().UTC(),
	}

	if err := store.SaveCheckpoint(ctx, cp); err != nil {
		t.Fatalf("SaveCheckpoint failed on fresh db: %v", err)
	}

	latest, err := store.GetLatestCheckpoint(ctx, "node-fresh-1")
	if err != nil {
		t.Fatalf("GetLatestCheckpoint failed: %v", err)
	}
	if latest == nil {
		t.Fatal("expected latest checkpoint, got nil")
	}
	if latest.CheckpointID != "cp-fresh-1" {
		t.Errorf("expected checkpoint_id cp-fresh-1, got %s", latest.CheckpointID)
	}
	if latest.State != "RECOVERY_NOT_REQUIRED" {
		t.Errorf("expected state RECOVERY_NOT_REQUIRED, got %s", latest.State)
	}
}

func TestCheckpointStore_RoundTripAndUpdates(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestCheckpointStore(t)
	nodeID := "node-roundtrip-1"

	// 1. None exists initially
	initial, err := store.GetLatestCheckpoint(ctx, nodeID)
	if err != nil {
		t.Fatalf("GetLatestCheckpoint error: %v", err)
	}
	if initial != nil {
		t.Fatalf("expected nil checkpoint, got %+v", initial)
	}

	// 2. Save first checkpoint
	t1 := time.Now().UTC().Truncate(time.Millisecond)
	cp1 := &StoredCheckpoint{
		CheckpointID:          "cp-100",
		NodeID:                nodeID,
		State:                 "RECOVERY_LOADING",
		StartedAt:             t1,
		RecoveryReason:        "startup begun",
		LastReconciledAttempt: 0,
		SchemaVersion:         9,
		CreatedAt:             t1,
		UpdatedAt:             t1,
	}
	if err := store.SaveCheckpoint(ctx, cp1); err != nil {
		t.Fatalf("SaveCheckpoint failed: %v", err)
	}

	got1, err := store.GetLatestCheckpoint(ctx, nodeID)
	if err != nil {
		t.Fatalf("GetLatestCheckpoint failed: %v", err)
	}
	if got1.State != "RECOVERY_LOADING" {
		t.Errorf("expected state RECOVERY_LOADING, got %s", got1.State)
	}

	// 3. Update existing checkpoint (UPSERT on conflict)
	t2 := t1.Add(time.Second)
	cp1.State = "RECOVERY_COMPLETE"
	cp1.CompletedAt = &t2
	cp1.RecoveryReason = "all items reconciled cleanly"
	cp1.UpdatedAt = t2
	if err := store.SaveCheckpoint(ctx, cp1); err != nil {
		t.Fatalf("SaveCheckpoint upsert failed: %v", err)
	}

	gotUpdated, err := store.GetLatestCheckpoint(ctx, nodeID)
	if err != nil {
		t.Fatalf("GetLatestCheckpoint failed: %v", err)
	}
	if gotUpdated.State != "RECOVERY_COMPLETE" {
		t.Errorf("expected state RECOVERY_COMPLETE, got %s", gotUpdated.State)
	}
	if gotUpdated.CompletedAt == nil {
		t.Fatal("expected completed_at to be non-nil")
	}

	// 4. Save second checkpoint
	t3 := t2.Add(time.Minute)
	cp2 := &StoredCheckpoint{
		CheckpointID:          "cp-200",
		NodeID:                nodeID,
		State:                 "RECOVERY_BLOCKED",
		StartedAt:             t3,
		RecoveryReason:        "unknown mitigation detected",
		LastReconciledAttempt: 1,
		SchemaVersion:         9,
		CreatedAt:             t3,
		UpdatedAt:             t3,
	}
	if err := store.SaveCheckpoint(ctx, cp2); err != nil {
		t.Fatalf("SaveCheckpoint cp2 failed: %v", err)
	}

	latest, err := store.GetLatestCheckpoint(ctx, nodeID)
	if err != nil {
		t.Fatalf("GetLatestCheckpoint failed: %v", err)
	}
	if latest.CheckpointID != "cp-200" {
		t.Errorf("expected cp-200 to be latest, got %s", latest.CheckpointID)
	}
	if latest.State != "RECOVERY_BLOCKED" {
		t.Errorf("expected RECOVERY_BLOCKED, got %s", latest.State)
	}

	// 5. List checkpoints
	list, err := store.ListCheckpoints(ctx, nodeID, 10)
	if err != nil {
		t.Fatalf("ListCheckpoints failed: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 checkpoints, got %d", len(list))
	}
	if list[0].CheckpointID != "cp-200" || list[1].CheckpointID != "cp-100" {
		t.Errorf("expected descending chronological order, got %s, %s", list[0].CheckpointID, list[1].CheckpointID)
	}
}

func TestCheckpointStore_ValidationSecurityGuards(t *testing.T) {
	// Nil checkpoint
	var nilCP *StoredCheckpoint
	if err := nilCP.Validate(); err == nil {
		t.Error("expected error for nil checkpoint")
	}

	// Empty ID
	cp := &StoredCheckpoint{
		NodeID:        "node-1",
		State:         "RECOVERY_LOADING",
		StartedAt:     time.Now().UTC(),
		SchemaVersion: 9,
	}
	if err := cp.Validate(); err == nil {
		t.Error("expected error for empty CheckpointID")
	}

	// Empty NodeID
	cp.CheckpointID = "cp-1"
	cp.NodeID = ""
	if err := cp.Validate(); err == nil {
		t.Error("expected error for empty NodeID")
	}

	// Empty State
	cp.NodeID = "node-1"
	cp.State = ""
	if err := cp.Validate(); err == nil {
		t.Error("expected error for empty State")
	}

	// Zero StartedAt
	cp.State = "RECOVERY_LOADING"
	cp.StartedAt = time.Time{}
	if err := cp.Validate(); err == nil {
		t.Error("expected error for zero StartedAt")
	}

	// Invalid schema version
	cp.StartedAt = time.Now().UTC()
	cp.SchemaVersion = 0
	if err := cp.Validate(); err == nil {
		t.Error("expected error for SchemaVersion <= 0")
	}

	// Security: credential pattern in reason
	cp.SchemaVersion = 9
	cp.RecoveryReason = "failed due to token=secret123"
	if err := cp.Validate(); err == nil {
		t.Error("expected error for credential pattern in recovery reason")
	}

	// Security: executable command in reason
	cp.RecoveryReason = "run /bin/sh -c reboot"
	if err := cp.Validate(); err == nil {
		t.Error("expected error for shell command in recovery reason")
	}

	// Valid checkpoint
	cp.RecoveryReason = "clean recovery completed safely"
	if err := cp.Validate(); err != nil {
		t.Errorf("expected valid checkpoint to pass, got: %v", err)
	}
}

func TestCheckpointStore_ListMitigationsByStatus(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestCheckpointStore(t)

	now := time.Now().UTC()
	m1 := &StoredMitigation{
		ActionID:   "act-1",
		DecisionID: "dec-1",
		IncidentID: "inc-1",
		NodeID:     "node-1",
		ActionType: types.ActionSimulatedThrottle,
		Target:     "service-a",
		Status:     types.MitigationStatusPending,
		StartedAt:  now,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	m2 := &StoredMitigation{
		ActionID:   "act-2",
		DecisionID: "dec-2",
		IncidentID: "inc-2",
		NodeID:     "node-1",
		ActionType: types.ActionSimulatedRestart,
		Target:     "service-b",
		Status:     types.MitigationStatusExecuting,
		StartedAt:  now.Add(time.Second),
		CreatedAt:  now.Add(time.Second),
		UpdatedAt:  now.Add(time.Second),
	}
	m3 := &StoredMitigation{
		ActionID:   "act-3",
		DecisionID: "dec-3",
		IncidentID: "inc-3",
		NodeID:     "node-1",
		ActionType: types.ActionSimulatedAlert,
		Target:     "ops",
		Status:     types.MitigationStatusPending,
		StartedAt:  now.Add(2 * time.Second),
		CreatedAt:  now.Add(2 * time.Second),
		UpdatedAt:  now.Add(2 * time.Second),
	}

	_ = store.RecordMitigation(ctx, m1)
	_ = store.RecordMitigation(ctx, m2)
	_ = store.RecordMitigation(ctx, m3)

	pendings, err := store.ListMitigationsByStatus(ctx, types.MitigationStatusPending, 10)
	if err != nil {
		t.Fatalf("ListMitigationsByStatus failed: %v", err)
	}
	if len(pendings) != 2 {
		t.Fatalf("expected 2 pending mitigations, got %d", len(pendings))
	}
	if pendings[0].ActionID != "act-1" || pendings[1].ActionID != "act-3" {
		t.Errorf("expected act-1 and act-3 in order, got %s, %s", pendings[0].ActionID, pendings[1].ActionID)
	}

	executings, err := store.ListMitigationsByStatus(ctx, types.MitigationStatusExecuting, 10)
	if err != nil {
		t.Fatalf("ListMitigationsByStatus failed: %v", err)
	}
	if len(executings) != 1 || executings[0].ActionID != "act-2" {
		t.Fatalf("expected act-2 executing, got %+v", executings)
	}
}

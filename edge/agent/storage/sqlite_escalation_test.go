package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

func TestSQLiteStore_EscalationLifecycle(t *testing.T) {
	ctx := context.Background()
	dbDir := t.TempDir()
	dbPath := filepath.Join(dbDir, "escalation_test.db")

	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC().Truncate(time.Millisecond)

	esc := &StoredEscalation{
		EscalationID:          "esc-001",
		IncidentID:            "inc-100",
		NodeID:                "node-alpha",
		ActionID:              "act-001",
		PolicyVersion:         "v1.0.0",
		FailureClassification: types.FailureMitigationFailed,
		Status:                types.EscalationStatusPending,
		Attempt:               1,
		MaxAttempts:           3,
		Reason:                "action simulation failed",
		Evidence:              map[string]string{"exit_code": "1"},
		CreatedAt:             now,
		UpdatedAt:             now,
	}

	// 1. RecordEscalation
	if err := store.RecordEscalation(ctx, esc); err != nil {
		t.Fatalf("RecordEscalation failed: %v", err)
	}

	// 2. Duplicate escalation rejected
	if err := store.RecordEscalation(ctx, esc); err != ErrDuplicateEscalation {
		t.Fatalf("expected ErrDuplicateEscalation, got %v", err)
	}

	// 3. GetEscalation
	retrieved, err := store.GetEscalation(ctx, "esc-001")
	if err != nil {
		t.Fatalf("GetEscalation failed: %v", err)
	}
	if retrieved.EscalationID != esc.EscalationID || retrieved.FailureClassification != types.FailureMitigationFailed {
		t.Fatalf("retrieved escalation mismatch: %+v", retrieved)
	}

	// 4. CountEscalations
	count, err := store.CountEscalations(ctx)
	if err != nil {
		t.Fatalf("CountEscalations failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected count 1, got %d", count)
	}

	// 5. UpdateEscalationStatus
	if err := store.UpdateEscalationStatus(ctx, "esc-001", types.EscalationStatusEscalated, nil, "escalating to operator", now.Add(time.Second)); err != nil {
		t.Fatalf("UpdateEscalationStatus failed: %v", err)
	}

	updated, err := store.GetEscalation(ctx, "esc-001")
	if err != nil {
		t.Fatalf("GetEscalation after update failed: %v", err)
	}
	if updated.Status != types.EscalationStatusEscalated {
		t.Fatalf("expected status ESCALATED, got %s", updated.Status)
	}

	// 6. CountFailuresInWindow
	since := now.Add(-10 * time.Minute)
	failures, err := store.CountFailuresInWindow(ctx, "node-alpha", "inc-100", since)
	if err != nil {
		t.Fatalf("CountFailuresInWindow failed: %v", err)
	}
	if failures != 1 {
		t.Fatalf("expected 1 failure in window, got %d", failures)
	}

	// 7. GetLatestEscalation
	latest, err := store.GetLatestEscalation(ctx, "node-alpha", "inc-100")
	if err != nil {
		t.Fatalf("GetLatestEscalation failed: %v", err)
	}
	if latest == nil || latest.EscalationID != "esc-001" {
		t.Fatalf("expected latest esc-001, got %+v", latest)
	}

	// 8. Circuit Breaker State
	cbState, err := store.GetCircuitState(ctx, "node-alpha", "inc-100")
	if err != nil {
		t.Fatalf("GetCircuitState failed: %v", err)
	}
	if cbState != nil {
		t.Fatalf("expected nil circuit state before record, got %+v", cbState)
	}

	tripTime := now.Add(2 * time.Minute)
	newState := &StoredCircuitState{
		NodeID:       "node-alpha",
		IncidentID:   "inc-100",
		State:        types.CircuitOpen,
		FailureCount: 3,
		TrippedAt:    &tripTime,
		UpdatedAt:    tripTime,
	}
	if err := store.SetCircuitState(ctx, newState); err != nil {
		t.Fatalf("SetCircuitState failed: %v", err)
	}

	cbState, err = store.GetCircuitState(ctx, "node-alpha", "inc-100")
	if err != nil {
		t.Fatalf("GetCircuitState failed: %v", err)
	}
	if cbState == nil || cbState.State != types.CircuitOpen || cbState.FailureCount != 3 {
		t.Fatalf("unexpected circuit state: %+v", cbState)
	}

	// 9. Reopen DB to verify persistence across restarts
	store.Close()

	store2, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("reopening store failed: %v", err)
	}
	defer store2.Close()

	reloadedEsc, err := store2.GetEscalation(ctx, "esc-001")
	if err != nil {
		t.Fatalf("GetEscalation on reloaded store failed: %v", err)
	}
	if reloadedEsc.EscalationID != "esc-001" || reloadedEsc.Status != types.EscalationStatusEscalated {
		t.Fatalf("reloaded escalation mismatch: %+v", reloadedEsc)
	}

	reloadedCB, err := store2.GetCircuitState(ctx, "node-alpha", "inc-100")
	if err != nil {
		t.Fatalf("GetCircuitState on reloaded store failed: %v", err)
	}
	if reloadedCB == nil || reloadedCB.State != types.CircuitOpen {
		t.Fatalf("reloaded circuit breaker mismatch: %+v", reloadedCB)
	}
}

func TestSQLiteStore_EscalationValidation(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	// Nil escalation
	if err := store.RecordEscalation(ctx, nil); err != ErrInvalidEscalation {
		t.Fatalf("expected ErrInvalidEscalation for nil, got %v", err)
	}

	// Invalid classification
	esc := &StoredEscalation{
		EscalationID:          "e-1",
		IncidentID:            "i-1",
		NodeID:                "n-1",
		FailureClassification: types.FailureClassification("UNKNOWN"),
		Status:                types.EscalationStatusPending,
		CreatedAt:             time.Now().UTC(),
	}
	if err := store.RecordEscalation(ctx, esc); err == nil {
		t.Fatalf("expected error for invalid classification, got nil")
	}
}

package storage_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
	_ "modernc.org/sqlite"
)

func makeTestMitigation(actionID, decisionID, incidentID, nodeID string, status types.MitigationStatus) *storage.StoredMitigation {
	now := time.Now().UTC()
	completed := now.Add(100 * time.Millisecond)
	return &storage.StoredMitigation{
		ActionID:    actionID,
		DecisionID:  decisionID,
		IncidentID:  incidentID,
		NodeID:      nodeID,
		ActionType:  types.ActionSimulatedThrottle,
		Target:      "telemetry_generator",
		Status:      status,
		Mode:        "AUTO_EXECUTE",
		Message:     "simulated mitigation action",
		ErrorCode:   "",
		Parameters:  map[string]string{"throttle_percent": "50", "duration_sec": "60"},
		StartedAt:   now,
		CompletedAt: &completed,
		DurationMs:  100,
		Simulated:   true,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

// TEST: Migration v4 initializes correctly on a fresh database
func TestSQLite_MigrationV4_FreshDatabase(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "fresh_v4.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	// Verify schema_migrations has version 4
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	defer db.Close()

	var version int
	err = db.QueryRow("SELECT MAX(version) FROM schema_migrations;").Scan(&version)
	if err != nil {
		t.Fatalf("failed to query schema version: %v", err)
	}
	if version < 4 {
		t.Fatalf("expected schema version >= 4, got: %d", version)
	}

	// Verify mitigation_records table exists
	var name string
	err = db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='mitigation_records';").Scan(&name)
	if err != nil {
		t.Fatalf("mitigation_records table was not created by migration v4: %v", err)
	}

	// Verify indexes exist
	expectedIndexes := []string{
		"idx_mitigation_records_incident_id",
		"idx_mitigation_records_node_id",
		"idx_mitigation_records_status",
	}
	for _, idx := range expectedIndexes {
		var idxName string
		err := db.QueryRow("SELECT name FROM sqlite_master WHERE type='index' AND name=?;", idx).Scan(&idxName)
		if err != nil {
			t.Errorf("expected index %s to exist: %v", idx, err)
		}
	}
}

// TEST: Migration v4 upgrades an existing Phase 5.6 (v3) database correctly
func TestSQLite_MigrationV4_UpgradeFromV3(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "upgrade_v3_to_v4.db")

	// 1. Manually construct a v3 database
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}

	v3SQL := `
	CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
	INSERT INTO schema_migrations VALUES (1, '2026-09-20T00:00:00Z');
	INSERT INTO schema_migrations VALUES (2, '2026-09-22T00:00:00Z');
	INSERT INTO schema_migrations VALUES (3, '2026-09-27T00:00:00Z');

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

	CREATE TABLE incident_records (
		incident_id TEXT PRIMARY KEY,
		node_id TEXT NOT NULL,
		metric_name TEXT NOT NULL,
		rule_name TEXT NOT NULL,
		severity TEXT NOT NULL,
		status TEXT NOT NULL,
		description TEXT NOT NULL,
		trigger_metric TEXT NOT NULL,
		trigger_value REAL NOT NULL,
		threshold REAL NOT NULL,
		evidence TEXT NOT NULL,
		triggered_at TEXT NOT NULL,
		updated_at TEXT NOT NULL,
		resolved_at TEXT,
		created_at TEXT NOT NULL
	);

	CREATE TABLE incident_observations (
		anomaly_id TEXT PRIMARY KEY,
		incident_id TEXT,
		node_id TEXT NOT NULL,
		metric_name TEXT NOT NULL,
		detected_at TEXT NOT NULL,
		observed_value REAL NOT NULL,
		anomaly_score REAL NOT NULL,
		detection_method TEXT NOT NULL,
		evidence TEXT NOT NULL,
		created_at TEXT NOT NULL
	);

	INSERT INTO telemetry_batches (batch_id, node_id, sequence_number, collected_at, payload, created_at, sync_status)
	VALUES ('b-pre-v4', 'node-legacy', 1, '2026-09-25T00:00:00Z', '{}', '2026-09-25T00:00:00Z', 'PENDING');

	INSERT INTO incident_records (incident_id, node_id, metric_name, rule_name, severity, status, description, trigger_metric, trigger_value, threshold, evidence, triggered_at, updated_at, created_at)
	VALUES ('inc-pre-v4', 'node-legacy', 'cpu_usage_percent', 'high_cpu', 'HIGH', 'ANOMALY_DETECTED', 'pre-v4 incident', 'cpu_usage_percent', 95.0, 90.0, '{}', '2026-09-27T00:00:00Z', '2026-09-27T00:00:00Z', '2026-09-27T00:00:00Z');
	`
	if _, err := db.Exec(v3SQL); err != nil {
		db.Close()
		t.Fatalf("failed to set up v3 database: %v", err)
	}
	db.Close()

	// 2. Open with OpenSQLite (triggers migration v4)
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed to upgrade v3 database: %v", err)
	}
	defer store.Close()

	// Verify legacy records survived
	ctx := context.Background()
	rec, err := store.GetBatch(ctx, "b-pre-v4")
	if err != nil || rec.NodeID != "node-legacy" {
		t.Fatalf("failed to retrieve pre-v4 telemetry batch: %v", err)
	}
	inc, err := store.GetIncident(ctx, "inc-pre-v4")
	if err != nil || inc.IncidentID != "inc-pre-v4" {
		t.Fatalf("failed to retrieve pre-v4 incident: %v", err)
	}

	// Verify migration version is now 4
	db2, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open reopened failed: %v", err)
	}
	defer db2.Close()

	var version int
	if err := db2.QueryRow("SELECT MAX(version) FROM schema_migrations;").Scan(&version); err != nil || version < 4 {
		t.Fatalf("expected version >= 4 after upgrade, got: %d, err: %v", version, err)
	}

	// Verify mitigation_records table is usable
	count, err := store.CountMitigations(ctx)
	if err != nil || count != 0 {
		t.Fatalf("expected 0 mitigations on fresh table, got: %d, err: %v", count, err)
	}
}

// TEST 9: Idempotent restart test (Section 9 of specification)
// 1. Create mitigation.
// 2. Persist it.
// 3. Close SQLite.
// 4. Reopen SQLite.
// 5. Retrieve mitigation.
// 6. Verify ActionID / DecisionID unchanged.
// 7. Verify status unchanged.
// 8. Persist the same logical mitigation again.
// 9. Verify no duplicate logical record is created.
func TestSQLite_IdempotentRestartTest(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "idempotent_restart.db")

	// 1. Create mitigation
	mit := makeTestMitigation("act-idem-01", "dec-idem-01", "inc-idem-01", "node-idem-01", types.MitigationStatusExecuted)

	// 2. Persist it
	store1, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite store1 failed: %v", err)
	}
	if err := store1.RecordMitigation(ctx, mit); err != nil {
		t.Fatalf("RecordMitigation failed: %v", err)
	}

	// 3. Close SQLite
	if err := store1.Close(); err != nil {
		t.Fatalf("store1.Close failed: %v", err)
	}

	// 4. Reopen SQLite
	store2, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite store2 failed: %v", err)
	}
	defer store2.Close()

	// 5. Retrieve mitigation
	retrieved, err := store2.GetMitigation(ctx, "act-idem-01")
	if err != nil {
		t.Fatalf("GetMitigation failed: %v", err)
	}

	// 6. Verify ActionID / DecisionID unchanged
	if retrieved.ActionID != mit.ActionID {
		t.Errorf("ActionID changed across restart: got %s, want %s", retrieved.ActionID, mit.ActionID)
	}
	if retrieved.DecisionID != mit.DecisionID {
		t.Errorf("DecisionID changed across restart: got %s, want %s", retrieved.DecisionID, mit.DecisionID)
	}

	// 7. Verify status unchanged
	if retrieved.Status != types.MitigationStatusExecuted {
		t.Errorf("Status changed across restart: got %s, want %s", retrieved.Status, types.MitigationStatusExecuted)
	}

	// 8. Persist the same logical mitigation again
	err = store2.RecordMitigation(ctx, mit)
	if !errors.Is(err, storage.ErrDuplicateMitigation) {
		t.Errorf("expected ErrDuplicateMitigation on duplicate record, got %v", err)
	}

	// 9. Verify no duplicate logical record is created
	count, err := store2.CountMitigations(ctx)
	if err != nil {
		t.Fatalf("CountMitigations failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("IDEMPOTENCY VIOLATION: expected count to remain 1, got %d", count)
	}
}

// TEST 10: EXECUTING -> UNKNOWN recovery test (Section 10 of specification)
// 1. Persist mitigation as EXECUTING.
// 2. Close database.
// 3. Reopen database.
// 4. Run startup recovery.
// 5. Verify status becomes: UNKNOWN_RECONCILIATION_REQUIRED
// 6. Run recovery again.
// 7. Verify state does not change again.
func TestSQLite_ExecutingToUnknownRecovery(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "recovery_test.db")

	// 1. Persist mitigation as EXECUTING
	mit := makeTestMitigation("act-crash-01", "dec-crash-01", "inc-crash-01", "node-crash-01", types.MitigationStatusExecuting)
	mit.CompletedAt = nil // In-flight action has no completion timestamp

	store1, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite store1 failed: %v", err)
	}
	if err := store1.RecordMitigation(ctx, mit); err != nil {
		t.Fatalf("RecordMitigation failed: %v", err)
	}

	// 2. Close database (simulating unexpected process shutdown while EXECUTING)
	if err := store1.Close(); err != nil {
		t.Fatalf("store1.Close failed: %v", err)
	}

	// 3. Reopen database (agent startup)
	store2, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite store2 failed: %v", err)
	}
	defer store2.Close()

	// 4. Run startup recovery
	recoveredCount, err := store2.RecoverInFlightMitigations(ctx)
	if err != nil {
		t.Fatalf("RecoverInFlightMitigations failed: %v", err)
	}
	if recoveredCount != 1 {
		t.Fatalf("expected 1 recovered in-flight record, got %d", recoveredCount)
	}

	// 5. Verify status becomes: UNKNOWN_RECONCILIATION_REQUIRED
	rec, err := store2.GetMitigation(ctx, "act-crash-01")
	if err != nil {
		t.Fatalf("GetMitigation failed: %v", err)
	}
	if rec.Status != types.MitigationStatusUnknownReconciliationRequired {
		t.Fatalf("CRITICAL SAFETY FAILURE: expected status UNKNOWN_RECONCILIATION_REQUIRED, got %s", rec.Status)
	}
	if rec.ErrorCode != "RESTART_RECONCILIATION_REQUIRED" {
		t.Errorf("expected error_code RESTART_RECONCILIATION_REQUIRED, got %s", rec.ErrorCode)
	}

	// 6. Run recovery again (idempotency check)
	secondRecoveredCount, err := store2.RecoverInFlightMitigations(ctx)
	if err != nil {
		t.Fatalf("second RecoverInFlightMitigations failed: %v", err)
	}
	if secondRecoveredCount != 0 {
		t.Errorf("expected 0 recovered on second run, got %d", secondRecoveredCount)
	}

	// 7. Verify state does not change again (still UNKNOWN, not converted to EXECUTED)
	rec2, err := store2.GetMitigation(ctx, "act-crash-01")
	if err != nil {
		t.Fatalf("GetMitigation failed: %v", err)
	}
	if rec2.Status != types.MitigationStatusUnknownReconciliationRequired {
		t.Fatalf("expected status to remain UNKNOWN_RECONCILIATION_REQUIRED, got %s", rec2.Status)
	}
}

// TEST 11: Transaction failure tests (Section 11 of specification)
func TestSQLite_TransactionFailuresAndValidation(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "tx_failures.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	// 1. Nil mitigation
	if err := store.RecordMitigation(ctx, nil); !errors.Is(err, storage.ErrInvalidMitigation) {
		t.Errorf("expected ErrInvalidMitigation for nil record, got %v", err)
	}

	// 2. Empty action_id
	mBad := makeTestMitigation("", "dec-1", "inc-1", "node-1", types.MitigationStatusExecuted)
	if err := store.RecordMitigation(ctx, mBad); !errors.Is(err, storage.ErrInvalidMitigation) {
		t.Errorf("expected ErrInvalidMitigation for empty action_id, got %v", err)
	}

	// 3. Empty decision_id
	mBad = makeTestMitigation("act-1", "", "inc-1", "node-1", types.MitigationStatusExecuted)
	if err := store.RecordMitigation(ctx, mBad); !errors.Is(err, storage.ErrInvalidMitigation) {
		t.Errorf("expected ErrInvalidMitigation for empty decision_id, got %v", err)
	}

	// 4. Empty incident_id
	mBad = makeTestMitigation("act-1", "dec-1", "", "node-1", types.MitigationStatusExecuted)
	if err := store.RecordMitigation(ctx, mBad); !errors.Is(err, storage.ErrInvalidMitigation) {
		t.Errorf("expected ErrInvalidMitigation for empty incident_id, got %v", err)
	}

	// 5. Empty node_id
	mBad = makeTestMitigation("act-1", "dec-1", "inc-1", "", types.MitigationStatusExecuted)
	if err := store.RecordMitigation(ctx, mBad); !errors.Is(err, storage.ErrInvalidMitigation) {
		t.Errorf("expected ErrInvalidMitigation for empty node_id, got %v", err)
	}

	// 6. Invalid status
	mBad = makeTestMitigation("act-1", "dec-1", "inc-1", "node-1", types.MitigationStatus("INVALID_STATUS"))
	if err := store.RecordMitigation(ctx, mBad); !errors.Is(err, storage.ErrInvalidMitigation) {
		t.Errorf("expected ErrInvalidMitigation for invalid status, got %v", err)
	}

	// 7. Invalid action type
	mBad = makeTestMitigation("act-1", "dec-1", "inc-1", "node-1", types.MitigationStatusExecuted)
	mBad.ActionType = types.MitigationActionType("ARBITRARY_SHELL_COMMAND")
	if err := store.RecordMitigation(ctx, mBad); !errors.Is(err, storage.ErrInvalidMitigation) {
		t.Errorf("expected ErrInvalidMitigation for invalid action_type, got %v", err)
	}

	// 8. Duplicate ActionID with different DecisionID
	m1 := makeTestMitigation("act-dup-act", "dec-dup-1", "inc-1", "node-1", types.MitigationStatusExecuted)
	if err := store.RecordMitigation(ctx, m1); err != nil {
		t.Fatalf("first RecordMitigation failed: %v", err)
	}
	m2 := makeTestMitigation("act-dup-act", "dec-dup-2", "inc-1", "node-1", types.MitigationStatusExecuted)
	if err := store.RecordMitigation(ctx, m2); !errors.Is(err, storage.ErrDuplicateMitigation) {
		t.Errorf("expected ErrDuplicateMitigation for duplicate ActionID, got %v", err)
	}

	// 9. Duplicate DecisionID with different ActionID
	m3 := makeTestMitigation("act-dup-3", "dec-dup-common", "inc-1", "node-1", types.MitigationStatusExecuted)
	if err := store.RecordMitigation(ctx, m3); err != nil {
		t.Fatalf("RecordMitigation m3 failed: %v", err)
	}
	m4 := makeTestMitigation("act-dup-4", "dec-dup-common", "inc-1", "node-1", types.MitigationStatusExecuted)
	if err := store.RecordMitigation(ctx, m4); !errors.Is(err, storage.ErrDuplicateMitigation) {
		t.Errorf("expected ErrDuplicateMitigation for duplicate DecisionID, got %v", err)
	}

	// 10. Non-existent mitigation retrieval
	if _, err := store.GetMitigation(ctx, "nonexistent-action"); !errors.Is(err, storage.ErrMitigationNotFound) {
		t.Errorf("expected ErrMitigationNotFound, got %v", err)
	}
	if _, err := store.GetMitigationByDecisionID(ctx, "nonexistent-decision"); !errors.Is(err, storage.ErrMitigationNotFound) {
		t.Errorf("expected ErrMitigationNotFound for decision, got %v", err)
	}

	// 11. Forbidden transition: UNKNOWN cannot transition to EXECUTED
	mUnk := makeTestMitigation("act-unk-01", "dec-unk-01", "inc-1", "node-1", types.MitigationStatusUnknownReconciliationRequired)
	if err := store.RecordMitigation(ctx, mUnk); err != nil {
		t.Fatalf("RecordMitigation mUnk failed: %v", err)
	}
	err = store.MarkMitigationExecuted(ctx, "act-unk-01", "should fail", time.Now().UTC(), 10)
	if !errors.Is(err, storage.ErrInvalidMitigationTransition) {
		t.Errorf("CRITICAL: expected ErrInvalidMitigationTransition from UNKNOWN to EXECUTED, got %v", err)
	}

	// 12. Forbidden transition: EXECUTED cannot transition back to EXECUTING
	err = store.MarkMitigationExecuting(ctx, m1.ActionID, time.Now().UTC())
	if !errors.Is(err, storage.ErrInvalidMitigationTransition) {
		t.Errorf("expected ErrInvalidMitigationTransition from EXECUTED to EXECUTING, got %v", err)
	}

	// 13. Context cancellation handling
	ctxCancelled, cancel := context.WithCancel(context.Background())
	cancel()
	mCancel := makeTestMitigation("act-cancel-01", "dec-cancel-01", "inc-1", "node-1", types.MitigationStatusExecuted)
	if err := store.RecordMitigation(ctxCancelled, mCancel); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}

	// 14. Closed database handling
	if err := store.Close(); err != nil {
		t.Fatalf("store.Close failed: %v", err)
	}
	if err := store.RecordMitigation(ctx, m1); !errors.Is(err, storage.ErrStoreClosed) {
		t.Errorf("expected ErrStoreClosed, got %v", err)
	}
	if _, err := store.GetMitigation(ctx, "act-dup-act"); !errors.Is(err, storage.ErrStoreClosed) {
		t.Errorf("expected ErrStoreClosed for GetMitigation, got %v", err)
	}
}

// TEST 13: Concurrency test - concurrent duplicate persistence (Section 13 of specification)
func TestSQLite_ConcurrentDuplicatePersistence(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "concurrent_mit.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	mit := makeTestMitigation("act-conc-01", "dec-conc-01", "inc-conc-01", "node-conc-01", types.MitigationStatusExecuted)

	const goroutines = 50
	var wg sync.WaitGroup
	var successCount int64
	var duplicateCount int64

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := store.RecordMitigation(ctx, mit)
			if err == nil {
				// Success
				atomic.AddInt64(&successCount, 1)
			} else if errors.Is(err, storage.ErrDuplicateMitigation) {
				// Expected duplicate rejection
				atomic.AddInt64(&duplicateCount, 1)
			} else {
				t.Errorf("unexpected error on concurrent insert: %v", err)
			}
		}()
	}

	wg.Wait()

	if successCount != 1 {
		t.Fatalf("expected exactly 1 insert to succeed, got %d", successCount)
	}
	if duplicateCount != int64(goroutines-1) {
		t.Fatalf("expected %d duplicate rejections, got %d", goroutines-1, duplicateCount)
	}

	// Verify database record count is strictly 1
	count, err := store.CountMitigations(ctx)
	if err != nil {
		t.Fatalf("CountMitigations failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected count 1, got %d", count)
	}
}

// TEST 14: Incident relationship test (Section 14 of specification)
func TestSQLite_IncidentMitigationRelationship(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "incident_mit_rel.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	incidentID := "inc-target-01"
	otherIncidentID := "inc-other-02"

	// Record 3 mitigations for target incident
	for i := 1; i <= 3; i++ {
		actID := fmt.Sprintf("act-target-%d", i)
		decID := fmt.Sprintf("dec-target-%d", i)
		mit := makeTestMitigation(actID, decID, incidentID, "node-1", types.MitigationStatusExecuted)
		mit.StartedAt = time.Now().UTC().Add(time.Duration(i) * time.Second)
		completed := mit.StartedAt.Add(100 * time.Millisecond)
		mit.CompletedAt = &completed
		if err := store.RecordMitigation(ctx, mit); err != nil {
			t.Fatalf("RecordMitigation %d failed: %v", i, err)
		}
	}

	// Record 1 mitigation for other incident
	otherMit := makeTestMitigation("act-other-01", "dec-other-01", otherIncidentID, "node-1", types.MitigationStatusExecuted)
	if err := store.RecordMitigation(ctx, otherMit); err != nil {
		t.Fatalf("RecordMitigation other failed: %v", err)
	}

	// List mitigations for incidentID
	targetList, err := store.ListMitigations(ctx, incidentID, 10)
	if err != nil {
		t.Fatalf("ListMitigations failed: %v", err)
	}
	if len(targetList) != 3 {
		t.Fatalf("expected 3 mitigations for target incident, got %d", len(targetList))
	}
	for _, m := range targetList {
		if m.IncidentID != incidentID {
			t.Errorf("expected incident_id %s, got %s", incidentID, m.IncidentID)
		}
	}

	// List all mitigations
	allList, err := store.ListMitigations(ctx, "", 10)
	if err != nil {
		t.Fatalf("ListMitigations (all) failed: %v", err)
	}
	if len(allList) != 4 {
		t.Fatalf("expected 4 total mitigations, got %d", len(allList))
	}

	// Verify ToMitigationAction conversion
	mitAction := targetList[0].ToMitigationAction()
	if mitAction.ActionID != targetList[0].ActionID {
		t.Errorf("ToMitigationAction ActionID mismatch: got %s, want %s", mitAction.ActionID, targetList[0].ActionID)
	}
	if mitAction.IncidentID != targetList[0].IncidentID {
		t.Errorf("ToMitigationAction IncidentID mismatch: got %s, want %s", mitAction.IncidentID, targetList[0].IncidentID)
	}
	if mitAction.Status != types.MitigationStatusExecuted {
		t.Errorf("ToMitigationAction Status mismatch: got %s, want EXECUTED", mitAction.Status)
	}
}

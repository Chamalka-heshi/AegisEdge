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

func makeTestApproval(approvalID, decisionID, incidentID, nodeID string, status types.ApprovalStatus, ttl time.Duration) *storage.StoredApproval {
	now := time.Now().UTC()
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	return &storage.StoredApproval{
		ApprovalID:          approvalID,
		DecisionID:          decisionID,
		IncidentID:          incidentID,
		ActionID:            fmt.Sprintf("act-%s", decisionID[4:]),
		NodeID:              nodeID,
		ActionType:          types.ActionSimulatedThrottle,
		Target:              "telemetry_generator",
		PolicyVersion:       "1.0.0",
		DecisionFingerprint: fmt.Sprintf("fp-%s-valid", decisionID),
		Status:              status,
		RequestedAt:         now,
		ExpiresAt:           now.Add(ttl),
		RequestedBy:         "operator_test_agent",
		ApprovedBy:          "",
		RejectedBy:          "",
		Reason:              "test mitigation approval request",
		CreatedAt:           now,
		UpdatedAt:           now,
	}
}

// TEST: Migration v5 initializes correctly on a fresh database
func TestSQLite_MigrationV5_FreshDatabase(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "fresh_v5.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Direct verification via SQL connection
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open raw sqlite: %v", err)
	}
	defer db.Close()

	// Verify schema_migrations has version 5
	var version int
	err = db.QueryRowContext(ctx, "SELECT MAX(version) FROM schema_migrations;").Scan(&version)
	if err != nil {
		t.Fatalf("failed to query schema_migrations: %v", err)
	}
	if version < 5 {
		t.Fatalf("expected migration version >= 5, got %d", version)
	}

	// Verify approval_records table exists
	var tableName string
	err = db.QueryRowContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name='approval_records';").Scan(&tableName)
	if err != nil {
		t.Fatalf("table approval_records does not exist: %v", err)
	}

	// Verify required indexes exist
	expectedIndexes := []string{
		"idx_approval_records_incident",
		"idx_approval_records_node",
		"idx_approval_records_status",
		"idx_approval_records_action",
	}
	for _, idx := range expectedIndexes {
		var idxName string
		err = db.QueryRowContext(ctx, "SELECT name FROM sqlite_master WHERE type='index' AND name=?;", idx).Scan(&idxName)
		if err != nil {
			t.Errorf("expected index %s was not found: %v", idx, err)
		}
	}
}

// TEST: Migration v4 to v5 upgrade
func TestSQLite_MigrationV5_UpgradeFromV4(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "upgrade_v4_v5.db")

	// 1. Create a simulated v4 database manually
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to create sqlite file: %v", err)
	}

	v4Setup := `
	CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
	INSERT INTO schema_migrations VALUES (1, '2026-09-20T00:00:00Z');
	INSERT INTO schema_migrations VALUES (2, '2026-09-22T00:00:00Z');
	INSERT INTO schema_migrations VALUES (3, '2026-09-24T00:00:00Z');
	INSERT INTO schema_migrations VALUES (4, '2026-10-01T00:00:00Z');

	CREATE TABLE telemetry_batches (batch_id TEXT PRIMARY KEY, node_id TEXT, sequence_number INTEGER, collected_at TEXT, sent_at TEXT, attempt INTEGER, payload TEXT, created_at TEXT, sync_status TEXT);
	CREATE TABLE incident_records (incident_id TEXT PRIMARY KEY, node_id TEXT, rule_name TEXT, severity TEXT, status TEXT, description TEXT, trigger_metric TEXT, trigger_value REAL, threshold REAL, evidence TEXT, triggered_at TEXT, updated_at TEXT, resolved_at TEXT);
	CREATE TABLE incident_observations (anomaly_id TEXT PRIMARY KEY, incident_id TEXT, node_id TEXT, metric_name TEXT, detected_at TEXT, observed_value REAL, anomaly_score REAL, detection_method TEXT, evidence TEXT, created_at TEXT);
	CREATE TABLE mitigation_records (action_id TEXT PRIMARY KEY, decision_id TEXT NOT NULL UNIQUE, incident_id TEXT NOT NULL, node_id TEXT NOT NULL, action_type TEXT NOT NULL, target TEXT NOT NULL, status TEXT NOT NULL, mode TEXT NOT NULL, message TEXT NOT NULL, error_code TEXT, parameters TEXT, started_at TEXT NOT NULL, completed_at TEXT, duration_ms INTEGER NOT NULL DEFAULT 0, simulated INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
	`
	if _, err := db.Exec(v4Setup); err != nil {
		t.Fatalf("failed to setup v4 schema: %v", err)
	}
	db.Close()

	// 2. Open using AegisEdge OpenSQLite, triggering migration v5
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed on v4 DB: %v", err)
	}
	defer store.Close()

	// 3. Verify upgraded version
	db2, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open raw upgraded DB: %v", err)
	}
	defer db2.Close()

	var version int
	if err := db2.QueryRow("SELECT MAX(version) FROM schema_migrations;").Scan(&version); err != nil || version < 5 {
		t.Fatalf("expected migration version >= 5, got %d (err: %v)", version, err)
	}
}

// TEST: RecordApproval, GetApproval, GetApprovalByDecisionID, and duplicate suppression
func TestSQLite_RecordAndGetApproval(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "record_approval.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	app := makeTestApproval("app-dec-1001", "dec-1001", "inc-1001", "node-01", types.ApprovalStatusPending, 15*time.Minute)

	// Record approval
	if err := store.RecordApproval(ctx, app); err != nil {
		t.Fatalf("RecordApproval failed: %v", err)
	}

	// Duplicate RecordApproval with same decision_id must fail
	duplicate := makeTestApproval("app-dec-diff", "dec-1001", "inc-1001", "node-01", types.ApprovalStatusPending, 15*time.Minute)
	if err := store.RecordApproval(ctx, duplicate); !errors.Is(err, storage.ErrDuplicateApproval) {
		t.Fatalf("expected ErrDuplicateApproval for duplicate decision_id, got %v", err)
	}

	// Duplicate RecordApproval with same approval_id must fail
	duplicateAppID := makeTestApproval("app-dec-1001", "dec-diff", "inc-1001", "node-01", types.ApprovalStatusPending, 15*time.Minute)
	if err := store.RecordApproval(ctx, duplicateAppID); !errors.Is(err, storage.ErrDuplicateApproval) {
		t.Fatalf("expected ErrDuplicateApproval for duplicate approval_id, got %v", err)
	}

	// Retrieve by ApprovalID
	got, err := store.GetApproval(ctx, app.ApprovalID)
	if err != nil {
		t.Fatalf("GetApproval failed: %v", err)
	}
	if got.ApprovalID != app.ApprovalID || got.DecisionID != app.DecisionID || got.Status != types.ApprovalStatusPending {
		t.Fatalf("retrieved approval does not match: got %+v, want %+v", got, app)
	}

	// Retrieve by DecisionID
	gotByDec, err := store.GetApprovalByDecisionID(ctx, app.DecisionID)
	if err != nil {
		t.Fatalf("GetApprovalByDecisionID failed: %v", err)
	}
	if gotByDec.ApprovalID != app.ApprovalID {
		t.Fatalf("mismatched approval retrieved by decision_id: %s", gotByDec.ApprovalID)
	}

	// Non-existent lookups
	if _, err := store.GetApproval(ctx, "nonexistent"); !errors.Is(err, storage.ErrApprovalNotFound) {
		t.Fatalf("expected ErrApprovalNotFound for nonexistent approval, got %v", err)
	}
	if _, err := store.GetApprovalByDecisionID(ctx, "nonexistent"); !errors.Is(err, storage.ErrApprovalNotFound) {
		t.Fatalf("expected ErrApprovalNotFound for nonexistent decision, got %v", err)
	}
}

// TEST: ApproveApproval lifecycle and idempotency
func TestSQLite_ApproveApproval_LifecycleAndIdempotency(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "approve_lifecycle.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	now := time.Now().UTC()
	app := makeTestApproval("app-dec-2001", "dec-2001", "inc-2001", "node-01", types.ApprovalStatusPending, 10*time.Minute)

	if err := store.RecordApproval(ctx, app); err != nil {
		t.Fatalf("RecordApproval failed: %v", err)
	}

	// Approve the pending approval
	if err := store.ApproveApproval(ctx, app.ApprovalID, "operator_alice", now); err != nil {
		t.Fatalf("ApproveApproval failed: %v", err)
	}

	got, err := store.GetApproval(ctx, app.ApprovalID)
	if err != nil {
		t.Fatalf("GetApproval failed: %v", err)
	}
	if got.Status != types.ApprovalStatusApproved || got.ApprovedBy != "operator_alice" {
		t.Fatalf("unexpected approval state: status=%s, approvedBy=%s", got.Status, got.ApprovedBy)
	}

	// Idempotent re-approval: must succeed without error
	if err := store.ApproveApproval(ctx, app.ApprovalID, "operator_alice", now.Add(time.Second)); err != nil {
		t.Fatalf("idempotent re-approval failed: %v", err)
	}

	// Rejecting an APPROVED approval MUST FAIL (Section 7 rule)
	if err := store.RejectApproval(ctx, app.ApprovalID, "operator_bob", "changed mind", now); !errors.Is(err, storage.ErrCannotRejectApproved) {
		t.Fatalf("expected ErrCannotRejectApproved, got %v", err)
	}
}

// TEST: Expiration prevents approval and transitions to EXPIRED
func TestSQLite_ApproveApproval_Expiration(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "expire_approval.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	now := time.Now().UTC()
	// TTL of 1 second
	app := makeTestApproval("app-dec-exp", "dec-exp", "inc-exp", "node-01", types.ApprovalStatusPending, 1*time.Second)

	if err := store.RecordApproval(ctx, app); err != nil {
		t.Fatalf("RecordApproval failed: %v", err)
	}

	// Attempting to approve 5 seconds later (past expiration)
	futureTime := now.Add(5 * time.Second)
	if err := store.ApproveApproval(ctx, app.ApprovalID, "operator_alice", futureTime); !errors.Is(err, storage.ErrApprovalExpired) {
		t.Fatalf("expected ErrApprovalExpired, got %v", err)
	}

	// Record should now be marked EXPIRED in store
	got, err := store.GetApproval(ctx, app.ApprovalID)
	if err != nil {
		t.Fatalf("GetApproval failed: %v", err)
	}
	if got.Status != types.ApprovalStatusExpired {
		t.Fatalf("expected status EXPIRED, got %s", got.Status)
	}
}

// TEST: RejectApproval lifecycle
func TestSQLite_RejectApproval_Lifecycle(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "reject_lifecycle.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	now := time.Now().UTC()
	app := makeTestApproval("app-dec-rej", "dec-rej", "inc-rej", "node-01", types.ApprovalStatusPending, 10*time.Minute)

	if err := store.RecordApproval(ctx, app); err != nil {
		t.Fatalf("RecordApproval failed: %v", err)
	}

	// Reject
	if err := store.RejectApproval(ctx, app.ApprovalID, "operator_charlie", "unsafe target", now); err != nil {
		t.Fatalf("RejectApproval failed: %v", err)
	}

	got, err := store.GetApproval(ctx, app.ApprovalID)
	if err != nil {
		t.Fatalf("GetApproval failed: %v", err)
	}
	if got.Status != types.ApprovalStatusRejected || got.RejectedBy != "operator_charlie" {
		t.Fatalf("unexpected state: status=%s, rejectedBy=%s", got.Status, got.RejectedBy)
	}

	// Idempotent re-rejection
	if err := store.RejectApproval(ctx, app.ApprovalID, "operator_charlie", "unsafe target", now); err != nil {
		t.Fatalf("idempotent reject failed: %v", err)
	}

	// Approving a REJECTED approval MUST FAIL
	if err := store.ApproveApproval(ctx, app.ApprovalID, "operator_alice", now); !errors.Is(err, storage.ErrInvalidApprovalTransition) {
		t.Fatalf("expected ErrInvalidApprovalTransition when approving REJECTED, got %v", err)
	}
}

// TEST: CancelApproval lifecycle
func TestSQLite_CancelApproval_Lifecycle(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cancel_lifecycle.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	now := time.Now().UTC()
	app := makeTestApproval("app-dec-can", "dec-can", "inc-can", "node-01", types.ApprovalStatusPending, 10*time.Minute)

	if err := store.RecordApproval(ctx, app); err != nil {
		t.Fatalf("RecordApproval failed: %v", err)
	}

	// Cancel
	if err := store.CancelApproval(ctx, app.ApprovalID, "operator changed priority", now); err != nil {
		t.Fatalf("CancelApproval failed: %v", err)
	}

	got, err := store.GetApproval(ctx, app.ApprovalID)
	if err != nil {
		t.Fatalf("GetApproval failed: %v", err)
	}
	if got.Status != types.ApprovalStatusCancelled {
		t.Fatalf("expected CANCELLED status, got %s", got.Status)
	}

	// Idempotent re-cancel
	if err := store.CancelApproval(ctx, app.ApprovalID, "operator changed priority", now); err != nil {
		t.Fatalf("idempotent cancel failed: %v", err)
	}
}

// TEST: ConsumeApproval semantics, single-consumption, and double-spend prevention
func TestSQLite_ConsumeApproval_SingleConsumption(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "consume_approval.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	now := time.Now().UTC()
	app := makeTestApproval("app-dec-con", "dec-con", "inc-con", "node-01", types.ApprovalStatusPending, 10*time.Minute)

	if err := store.RecordApproval(ctx, app); err != nil {
		t.Fatalf("RecordApproval failed: %v", err)
	}

	// Consuming a PENDING approval must fail
	if err := store.ConsumeApproval(ctx, app.ApprovalID, now); !errors.Is(err, storage.ErrInvalidApprovalTransition) {
		t.Fatalf("expected ErrInvalidApprovalTransition on PENDING approval, got %v", err)
	}

	// Approve it
	if err := store.ApproveApproval(ctx, app.ApprovalID, "operator_alice", now); err != nil {
		t.Fatalf("ApproveApproval failed: %v", err)
	}

	// First consumption: MUST SUCCEED
	if err := store.ConsumeApproval(ctx, app.ApprovalID, now.Add(time.Second)); err != nil {
		t.Fatalf("ConsumeApproval failed on first call: %v", err)
	}

	got, err := store.GetApproval(ctx, app.ApprovalID)
	if err != nil {
		t.Fatalf("GetApproval failed: %v", err)
	}
	if got.Status != types.ApprovalStatusConsumed || got.ConsumedAt == nil {
		t.Fatalf("expected CONSUMED status with ConsumedAt timestamp, got status=%s, consumedAt=%v", got.Status, got.ConsumedAt)
	}

	// Second consumption (replay attack): MUST FAIL with ErrApprovalAlreadyConsumed
	if err := store.ConsumeApproval(ctx, app.ApprovalID, now.Add(2*time.Second)); !errors.Is(err, storage.ErrApprovalAlreadyConsumed) {
		t.Fatalf("expected ErrApprovalAlreadyConsumed on replay, got %v", err)
	}

	// Approving a CONSUMED approval MUST FAIL
	if err := store.ApproveApproval(ctx, app.ApprovalID, "operator_alice", now); !errors.Is(err, storage.ErrInvalidApprovalTransition) {
		t.Fatalf("expected ErrInvalidApprovalTransition on consumed approval, got %v", err)
	}
}

// TEST: Concurrent double consumption race (50 simultaneous goroutines racing to consume the same approval)
func TestSQLite_ConcurrentConsumptionRace(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "concurrent_consumption.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	now := time.Now().UTC()
	app := makeTestApproval("app-race-1", "dec-race-1", "inc-race-1", "node-01", types.ApprovalStatusPending, 10*time.Minute)

	if err := store.RecordApproval(ctx, app); err != nil {
		t.Fatalf("RecordApproval failed: %v", err)
	}
	if err := store.ApproveApproval(ctx, app.ApprovalID, "operator_alice", now); err != nil {
		t.Fatalf("ApproveApproval failed: %v", err)
	}

	const concurrency = 50
	var wg sync.WaitGroup
	var successCount int32
	var alreadyConsumedCount int32

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := store.ConsumeApproval(ctx, app.ApprovalID, now.Add(time.Millisecond))
			if err == nil {
				atomic.AddInt32(&successCount, 1)
			} else if errors.Is(err, storage.ErrApprovalAlreadyConsumed) {
				atomic.AddInt32(&alreadyConsumedCount, 1)
			}
		}()
	}

	wg.Wait()

	if successCount != 1 {
		t.Fatalf("expected EXACTLY 1 successful consumption, got %d (already consumed: %d)", successCount, alreadyConsumedCount)
	}
	if alreadyConsumedCount != concurrency-1 {
		t.Fatalf("expected %d goroutines to receive ErrApprovalAlreadyConsumed, got %d", concurrency-1, alreadyConsumedCount)
	}
}

// TEST: Restart persistence across process reboot
func TestSQLite_Approval_SurviveCloseReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "restart_approval.db")
	now := time.Now().UTC()

	// 1. Open store, record several approvals in different states
	store1, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}

	ctx := context.Background()
	app1 := makeTestApproval("app-1", "dec-1", "inc-1", "node-01", types.ApprovalStatusPending, 10*time.Minute)
	app2 := makeTestApproval("app-2", "dec-2", "inc-1", "node-01", types.ApprovalStatusPending, 10*time.Minute)
	app3 := makeTestApproval("app-3", "dec-3", "inc-1", "node-01", types.ApprovalStatusPending, 10*time.Minute)

	_ = store1.RecordApproval(ctx, app1)
	_ = store1.RecordApproval(ctx, app2)
	_ = store1.RecordApproval(ctx, app3)

	_ = store1.ApproveApproval(ctx, app2.ApprovalID, "operator_alice", now)
	_ = store1.ApproveApproval(ctx, app3.ApprovalID, "operator_bob", now)
	_ = store1.ConsumeApproval(ctx, app3.ApprovalID, now)

	// Close store (process shutdown)
	if err := store1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 2. Reopen store (process reboot)
	store2, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer store2.Close()

	// Verify all states survived intact
	got1, err := store2.GetApproval(ctx, "app-1")
	if err != nil || got1.Status != types.ApprovalStatusPending {
		t.Fatalf("app-1 did not survive as PENDING: got status %s, err: %v", got1.Status, err)
	}

	got2, err := store2.GetApproval(ctx, "app-2")
	if err != nil || got2.Status != types.ApprovalStatusApproved || got2.ApprovedBy != "operator_alice" {
		t.Fatalf("app-2 did not survive as APPROVED: got status %s, approvedBy %s", got2.Status, got2.ApprovedBy)
	}

	got3, err := store2.GetApproval(ctx, "app-3")
	if err != nil || got3.Status != types.ApprovalStatusConsumed || got3.ConsumedAt == nil {
		t.Fatalf("app-3 did not survive as CONSUMED: got status %s", got3.Status)
	}
}

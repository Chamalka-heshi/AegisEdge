package storage_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
	_ "modernc.org/sqlite"
)

func makeTestVerification(verificationID, actionID, decisionID, incidentID, nodeID, metricName string, status types.VerificationStatus, ttl time.Duration) *storage.StoredVerification {
	now := time.Now().UTC()
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &storage.StoredVerification{
		VerificationID:       verificationID,
		IncidentID:           incidentID,
		ActionID:             actionID,
		DecisionID:           decisionID,
		NodeID:               nodeID,
		MetricName:           metricName,
		ConditionType:        "THRESHOLD_UPPER",
		RecoveryThreshold:    70.0,
		Comparator:           "<=",
		Status:               status,
		RequiredObservations: 3,
		ConsecutiveHealthy:   0,
		TotalObservations:    0,
		StartedAt:            now,
		ExpiresAt:            now.Add(ttl),
		Reason:               "verification initiated",
		Evidence:             map[string]string{"threshold": "70.0"},
		CreatedAt:            now,
		UpdatedAt:            now,
	}
}

// TEST: Migration v6 initializes correctly on a fresh database
func TestSQLite_MigrationV6_FreshDatabase(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "fresh_v6.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open raw sqlite: %v", err)
	}
	defer db.Close()

	// Verify schema_migrations has version 6
	var version int
	err = db.QueryRowContext(ctx, "SELECT MAX(version) FROM schema_migrations;").Scan(&version)
	if err != nil {
		t.Fatalf("failed to query schema_migrations: %v", err)
	}
	if version < 6 {
		t.Fatalf("expected migration version >= 6, got %d", version)
	}

	// Verify verification_records table exists
	var tableName string
	err = db.QueryRowContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name='verification_records';").Scan(&tableName)
	if err != nil {
		t.Fatalf("table verification_records does not exist: %v", err)
	}

	// Verify columns exist via PRAGMA table_info
	rows, err := db.QueryContext(ctx, "PRAGMA table_info(verification_records);")
	if err != nil {
		t.Fatalf("failed to query table_info for verification_records: %v", err)
	}
	defer rows.Close()

	foundCols := make(map[string]bool)
	for rows.Next() {
		var cid int
		var name, colType string
		var notNull, pk int
		var dfltValue sql.NullString
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dfltValue, &pk); err != nil {
			t.Fatalf("failed to scan table_info row: %v", err)
		}
		foundCols[name] = true
	}
	expectedCols := []string{
		"verification_id", "incident_id", "action_id", "decision_id", "node_id",
		"metric_name", "condition_type", "recovery_threshold", "comparator", "status",
		"required_observations", "consecutive_healthy", "total_observations",
		"started_at", "expires_at", "completed_at", "recovered_at",
		"last_observation_at", "last_observed_value", "reason", "evidence",
		"created_at", "updated_at",
	}
	for _, col := range expectedCols {
		if !foundCols[col] {
			t.Errorf("expected column %s not found in verification_records", col)
		}
	}

	// Verify required indexes exist
	expectedIndexes := []string{
		"idx_verification_records_incident",
		"idx_verification_records_node_metric",
		"idx_verification_records_status",
		"idx_verification_records_action",
	}
	for _, idx := range expectedIndexes {
		var idxName string
		err = db.QueryRowContext(ctx, "SELECT name FROM sqlite_master WHERE type='index' AND name=?;", idx).Scan(&idxName)
		if err != nil {
			t.Errorf("expected index %s was not found: %v", idx, err)
		}
	}
}

// TEST: Migration v5 to v6 upgrade
func TestSQLite_MigrationV6_UpgradeFromV5(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "upgrade_v5_v6.db")

	// 1. Create a simulated v5 database manually
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to create sqlite file: %v", err)
	}

	v5Setup := `
	CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
	INSERT INTO schema_migrations VALUES (1, '2026-09-20T00:00:00Z');
	INSERT INTO schema_migrations VALUES (2, '2026-09-22T00:00:00Z');
	INSERT INTO schema_migrations VALUES (3, '2026-09-24T00:00:00Z');
	INSERT INTO schema_migrations VALUES (4, '2026-10-01T00:00:00Z');
	INSERT INTO schema_migrations VALUES (5, '2026-10-03T00:00:00Z');

	CREATE TABLE telemetry_batches (batch_id TEXT PRIMARY KEY, node_id TEXT, sequence_number INTEGER, collected_at TEXT, sent_at TEXT, attempt INTEGER, payload TEXT, created_at TEXT, sync_status TEXT, published_at TEXT);
	CREATE TABLE incident_records (incident_id TEXT PRIMARY KEY, node_id TEXT, rule_name TEXT, severity TEXT, status TEXT, description TEXT, trigger_metric TEXT, trigger_value REAL, threshold REAL, evidence TEXT, triggered_at TEXT, updated_at TEXT, resolved_at TEXT, created_at TEXT, metric_name TEXT);
	CREATE TABLE incident_observations (anomaly_id TEXT PRIMARY KEY, incident_id TEXT, node_id TEXT, metric_name TEXT, detected_at TEXT, observed_value REAL, anomaly_score REAL, detection_method TEXT, evidence TEXT, created_at TEXT);
	CREATE TABLE mitigation_records (action_id TEXT PRIMARY KEY, decision_id TEXT NOT NULL UNIQUE, incident_id TEXT NOT NULL, node_id TEXT NOT NULL, action_type TEXT NOT NULL, target TEXT NOT NULL, status TEXT NOT NULL, mode TEXT NOT NULL, message TEXT NOT NULL, error_code TEXT, parameters TEXT, started_at TEXT NOT NULL, completed_at TEXT, duration_ms INTEGER NOT NULL DEFAULT 0, simulated INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
	CREATE TABLE approval_records (approval_id TEXT PRIMARY KEY, decision_id TEXT NOT NULL UNIQUE, incident_id TEXT NOT NULL, action_id TEXT NOT NULL, node_id TEXT NOT NULL, action_type TEXT NOT NULL, target TEXT NOT NULL, policy_version TEXT NOT NULL, decision_fingerprint TEXT NOT NULL, status TEXT NOT NULL, requested_at TEXT NOT NULL, expires_at TEXT NOT NULL, requested_by TEXT NOT NULL, approved_by TEXT NOT NULL DEFAULT '', rejected_by TEXT NOT NULL DEFAULT '', reason TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL, consumed_at TEXT);
	`
	if _, err := db.Exec(v5Setup); err != nil {
		t.Fatalf("failed to setup v5 schema: %v", err)
	}
	db.Close()

	// 2. Open using AegisEdge OpenSQLite, triggering migration v6
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed on v5 DB: %v", err)
	}
	defer store.Close()

	// 3. Verify upgraded version
	db2, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open raw upgraded DB: %v", err)
	}
	defer db2.Close()

	var version int
	if err := db2.QueryRow("SELECT MAX(version) FROM schema_migrations;").Scan(&version); err != nil || version < 6 {
		t.Fatalf("expected migration version >= 6, got %d (err: %v)", version, err)
	}

	// Verify verification_records table exists
	var tableName string
	if err := db2.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='verification_records';").Scan(&tableName); err != nil {
		t.Fatalf("table verification_records not found after upgrade: %v", err)
	}
}

// TEST: RecordVerification, GetVerification, GetVerificationByActionID, GetActiveVerification, Count
func TestSQLite_RecordAndGetVerification(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "record_verification.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	v := makeTestVerification("ver-1001", "act-1001", "dec-1001", "inc-1001", "node-01", "cpu_usage_percent", types.VerificationStatusPending, 5*time.Minute)

	// Record verification
	if err := store.RecordVerification(ctx, v); err != nil {
		t.Fatalf("RecordVerification failed: %v", err)
	}

	// Duplicate RecordVerification with same verification_id must fail
	if err := store.RecordVerification(ctx, v); !errors.Is(err, storage.ErrDuplicateVerification) {
		t.Fatalf("expected ErrDuplicateVerification for duplicate verification_id, got %v", err)
	}

	// GetVerification by ID
	got, err := store.GetVerification(ctx, v.VerificationID)
	if err != nil {
		t.Fatalf("GetVerification failed: %v", err)
	}
	if got.VerificationID != v.VerificationID || got.IncidentID != v.IncidentID || got.Status != types.VerificationStatusPending {
		t.Fatalf("retrieved verification mismatch: got %+v, want %+v", got, v)
	}
	if got.RecoveryThreshold != v.RecoveryThreshold || got.Comparator != v.Comparator {
		t.Fatalf("condition mismatch: got %f %s, want %f %s", got.RecoveryThreshold, got.Comparator, v.RecoveryThreshold, v.Comparator)
	}

	// GetVerificationByActionID
	gotByAct, err := store.GetVerificationByActionID(ctx, v.ActionID)
	if err != nil {
		t.Fatalf("GetVerificationByActionID failed: %v", err)
	}
	if gotByAct.VerificationID != v.VerificationID {
		t.Fatalf("mismatched verification retrieved by action_id: %s", gotByAct.VerificationID)
	}

	// GetActiveVerification
	active, err := store.GetActiveVerification(ctx, v.NodeID, v.MetricName)
	if err != nil {
		t.Fatalf("GetActiveVerification failed: %v", err)
	}
	if active == nil || active.VerificationID != v.VerificationID {
		t.Fatalf("expected active verification %s, got %+v", v.VerificationID, active)
	}

	// Non-existent active verification returns (nil, nil)
	none, err := store.GetActiveVerification(ctx, "nonexistent-node", v.MetricName)
	if err != nil || none != nil {
		t.Fatalf("expected nil for nonexistent active verification, got %+v (err: %v)", none, err)
	}

	// Non-existent GetVerification returns ErrVerificationNotFound
	if _, err := store.GetVerification(ctx, "nonexistent"); !errors.Is(err, storage.ErrVerificationNotFound) {
		t.Fatalf("expected ErrVerificationNotFound for nonexistent ID, got %v", err)
	}

	// Count
	cnt, err := store.CountVerifications(ctx)
	if err != nil || cnt != 1 {
		t.Fatalf("expected count 1, got %d (err: %v)", cnt, err)
	}
}

// TEST: UpdateVerificationProgress and terminal transition rules
func TestSQLite_UpdateVerificationProgress(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "update_progress.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	now := time.Now().UTC()
	v := makeTestVerification("ver-2001", "act-2001", "dec-2001", "inc-2001", "node-01", "cpu_usage_percent", types.VerificationStatusPending, 5*time.Minute)

	if err := store.RecordVerification(ctx, v); err != nil {
		t.Fatalf("RecordVerification failed: %v", err)
	}

	// Step 1: Progress update (still PENDING)
	obsAt := now.Add(time.Second)
	obsVal := 65.4
	err = store.UpdateVerificationProgress(
		ctx, v.VerificationID, types.VerificationStatusPending,
		1, 1, obsVal, &obsAt, nil, nil,
		"first healthy observation", map[string]string{"obs": "65.4"}, obsAt,
	)
	if err != nil {
		t.Fatalf("UpdateVerificationProgress step 1 failed: %v", err)
	}

	got, err := store.GetVerification(ctx, v.VerificationID)
	if err != nil {
		t.Fatalf("GetVerification failed: %v", err)
	}
	if got.ConsecutiveHealthy != 1 || got.TotalObservations != 1 || got.LastObservedValue == nil || *got.LastObservedValue != 65.4 {
		t.Fatalf("unexpected progress fields: %+v", got)
	}

	// Step 2: Transition to RECOVERED (terminal)
	recAt := now.Add(3 * time.Second)
	recVal := 61.2
	err = store.UpdateVerificationProgress(
		ctx, v.VerificationID, types.VerificationStatusRecovered,
		3, 3, recVal, &recAt, &recAt, &recAt,
		"recovery confirmed", map[string]string{"last_val": "61.2"}, recAt,
	)
	if err != nil {
		t.Fatalf("UpdateVerificationProgress step 2 failed: %v", err)
	}

	gotRec, err := store.GetVerification(ctx, v.VerificationID)
	if err != nil || gotRec.Status != types.VerificationStatusRecovered {
		t.Fatalf("expected RECOVERED status, got %+v (err: %v)", gotRec, err)
	}

	// Step 3: Modifying a terminal record should fail with ErrVerificationTerminal
	err = store.UpdateVerificationProgress(
		ctx, v.VerificationID, types.VerificationStatusTimedOut,
		0, 4, 80.0, nil, nil, nil, "tampering attempt", nil, recAt.Add(time.Second),
	)
	if !errors.Is(err, storage.ErrVerificationTerminal) {
		t.Fatalf("expected ErrVerificationTerminal when modifying terminal record, got %v", err)
	}
}

// TEST: ExpireStaleVerifications
func TestSQLite_ExpireStaleVerifications(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "expire_verifications.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	now := time.Now().UTC()

	// v1: expired (ExpiresAt in the past)
	v1 := makeTestVerification("ver-exp-1", "act-1", "dec-1", "inc-1", "node-01", "cpu_usage_percent", types.VerificationStatusPending, -time.Minute)
	v1.StartedAt = now.Add(-5 * time.Minute)
	v1.ExpiresAt = now.Add(-1 * time.Minute)

	// v2: not expired (ExpiresAt in the future)
	v2 := makeTestVerification("ver-exp-2", "act-2", "dec-2", "inc-2", "node-01", "memory_usage_percent", types.VerificationStatusPending, 10*time.Minute)

	_ = store.RecordVerification(ctx, v1)
	_ = store.RecordVerification(ctx, v2)

	expiredCount, err := store.ExpireStaleVerifications(ctx, now)
	if err != nil {
		t.Fatalf("ExpireStaleVerifications failed: %v", err)
	}
	if expiredCount != 1 {
		t.Fatalf("expected 1 expired verification, got %d", expiredCount)
	}

	got1, _ := store.GetVerification(ctx, "ver-exp-1")
	if got1.Status != types.VerificationStatusTimedOut {
		t.Fatalf("expected TIMED_OUT for v1, got %s", got1.Status)
	}

	got2, _ := store.GetVerification(ctx, "ver-exp-2")
	if got2.Status != types.VerificationStatusPending {
		t.Fatalf("expected PENDING for v2, got %s", got2.Status)
	}
}

// TEST: Restart recovery (RecoverInFlightVerifications) and SurviveCloseReopen
func TestSQLite_Verification_SurviveCloseReopenAndRecover(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "restart_recovery.db")
	store1, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}

	ctx := context.Background()
	now := time.Now().UTC()

	// Record 3 verifications:
	// 1. Pending but expired during shutdown
	vExpired := makeTestVerification("ver-offline-exp", "act-1", "dec-1", "inc-1", "node-01", "cpu_usage_percent", types.VerificationStatusPending, -time.Minute)
	vExpired.StartedAt = now.Add(-10 * time.Minute)
	vExpired.ExpiresAt = now.Add(-2 * time.Minute)

	// 2. Pending and still unexpired
	vActive := makeTestVerification("ver-offline-act", "act-2", "dec-2", "inc-2", "node-01", "memory_usage_percent", types.VerificationStatusPending, 15*time.Minute)

	// 3. Already recovered
	vRec := makeTestVerification("ver-offline-rec", "act-3", "dec-3", "inc-3", "node-01", "disk_usage_percent", types.VerificationStatusPending, 15*time.Minute)

	_ = store1.RecordVerification(ctx, vExpired)
	_ = store1.RecordVerification(ctx, vActive)
	_ = store1.RecordVerification(ctx, vRec)

	recTime := now.Add(-5 * time.Minute)
	_ = store1.UpdateVerificationProgress(ctx, vRec.VerificationID, types.VerificationStatusRecovered, 3, 3, 40.0, &recTime, &recTime, &recTime, "nominal", nil, recTime)

	// Shutdown process
	if err := store1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reboot process: open store2
	store2, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer store2.Close()

	// Execute startup crash recovery for verifications
	timedOutCount, err := store2.RecoverInFlightVerifications(ctx, now)
	if err != nil {
		t.Fatalf("RecoverInFlightVerifications failed: %v", err)
	}
	if timedOutCount != 1 {
		t.Fatalf("expected 1 timed out verification recovered, got %d", timedOutCount)
	}

	// Verify states
	gotExp, err := store2.GetVerification(ctx, "ver-offline-exp")
	if err != nil || gotExp.Status != types.VerificationStatusTimedOut {
		t.Fatalf("expected TIMED_OUT for vExpired, got %s (err: %v)", gotExp.Status, err)
	}

	gotAct, err := store2.GetVerification(ctx, "ver-offline-act")
	if err != nil || gotAct.Status != types.VerificationStatusPending {
		t.Fatalf("expected PENDING for vActive, got %s (err: %v)", gotAct.Status, err)
	}

	gotRec, err := store2.GetVerification(ctx, "ver-offline-rec")
	if err != nil || gotRec.Status != types.VerificationStatusRecovered {
		t.Fatalf("expected RECOVERED for vRec, got %s (err: %v)", gotRec.Status, err)
	}
}

// TEST: Concurrent progress updates do not corrupt SQLite state
func TestSQLite_ConcurrentVerificationProgress(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "concurrent_progress.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	now := time.Now().UTC()
	v := makeTestVerification("ver-conc-1", "act-conc-1", "dec-conc-1", "inc-conc-1", "node-01", "cpu_usage_percent", types.VerificationStatusPending, 10*time.Minute)

	if err := store.RecordVerification(ctx, v); err != nil {
		t.Fatalf("RecordVerification failed: %v", err)
	}

	const concurrency = 20
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		idx := i
		go func() {
			defer wg.Done()
			obsAt := now.Add(time.Duration(idx) * time.Millisecond)
			val := float64(50 + idx)
			_ = store.UpdateVerificationProgress(
				ctx, v.VerificationID, types.VerificationStatusPending,
				idx, idx, val, &obsAt, nil, nil, "concurrent progress update", nil, obsAt,
			)
		}()
	}
	wg.Wait()

	got, err := store.GetVerification(ctx, v.VerificationID)
	if err != nil {
		t.Fatalf("GetVerification failed after concurrent updates: %v", err)
	}
	if got.Status != types.VerificationStatusPending {
		t.Fatalf("expected status to remain PENDING, got %s", got.Status)
	}
}

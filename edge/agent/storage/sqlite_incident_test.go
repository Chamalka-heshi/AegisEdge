package storage_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
	_ "modernc.org/sqlite"
)

// TEST P: Migration initializes correctly on a fresh database
func TestSQLite_MigrationV3_FreshDatabase(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "fresh_v3.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	// Verify schema_migrations has version 3
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
	if version != 3 {
		t.Fatalf("expected schema version 3, got: %d", version)
	}

	// Verify incident_records and incident_observations tables exist
	tables := []string{"incident_records", "incident_observations"}
	for _, tbl := range tables {
		var name string
		err := db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", tbl).Scan(&name)
		if err != nil {
			t.Fatalf("table %s was not created by migration v3: %v", tbl, err)
		}
	}
}

// TEST Q: Migration upgrades an existing Phase 5.4 (v2) database correctly
func TestSQLite_MigrationV3_UpgradeFromV2(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "upgrade_v2_to_v3.db")

	// 1. Manually construct a v2 database
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}

	v2SQL := `
	CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
	INSERT INTO schema_migrations VALUES (1, '2026-09-20T00:00:00Z');
	INSERT INTO schema_migrations VALUES (2, '2026-09-22T00:00:00Z');

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
	VALUES ('b-pre-upgrade', 'node-legacy', 1, '2026-09-25T00:00:00Z', '{}', '2026-09-25T00:00:00Z', 'PENDING');
	`
	if _, err := db.Exec(v2SQL); err != nil {
		db.Close()
		t.Fatalf("failed to set up v2 database: %v", err)
	}
	db.Close()

	// 2. Open with OpenSQLite (triggers migration v3)
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed to upgrade v2 database: %v", err)
	}
	defer store.Close()

	// Verify legacy telemetry record survived upgrade
	rec, err := store.GetBatch(context.Background(), "b-pre-upgrade")
	if err != nil {
		t.Fatalf("failed to read pre-upgrade batch: %v", err)
	}
	if rec.NodeID != "node-legacy" {
		t.Errorf("pre-upgrade batch corrupted: got %s, want node-legacy", rec.NodeID)
	}

	// Verify new tables exist and version is 3
	db2, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open reopened failed: %v", err)
	}
	defer db2.Close()

	var version int
	if err := db2.QueryRow("SELECT MAX(version) FROM schema_migrations;").Scan(&version); err != nil || version != 3 {
		t.Fatalf("expected version 3 after upgrade, got: %d, err: %v", version, err)
	}
}

// TEST R & S: Incident and observation records survive SQLite close/reopen
func TestSQLite_IncidentAndObservation_SurviveCloseReopen(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "close_reopen_inc.db")

	store1, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite store1 failed: %v", err)
	}

	now := time.Now().UTC()
	obs := &storage.StoredObservation{
		AnomalyID:       "anom-survive-1",
		NodeID:          "node-dur",
		MetricName:      "cpu_usage_percent",
		DetectedAt:      now,
		ObservedValue:   95.5,
		AnomalyScore:    0.85,
		DetectionMethod: types.DetectionMethodStaticThreshold,
		Evidence:        map[string]string{"threshold": "90.0"},
	}

	inc := &types.Incident{
		IncidentID:    "inc-survive-1",
		NodeID:        "node-dur",
		RuleName:      "threshold_cpu_usage_percent",
		Severity:      types.SeverityCritical,
		Status:        types.StatusAnomalyDetected,
		Description:   "CPU threshold exceeded on node-dur",
		TriggerMetric: "cpu_usage_percent",
		TriggerValue:  95.5,
		Threshold:     90.0,
		Evidence:      map[string]string{"anomaly_id": "anom-survive-1"},
		TriggeredAt:   now,
		UpdatedAt:     now,
	}

	isNew, err := store1.PersistIncidentEvaluation(ctx, obs, inc)
	if err != nil {
		t.Fatalf("PersistIncidentEvaluation store1 failed: %v", err)
	}
	if !isNew {
		t.Fatal("expected isNew=true for initial evaluation")
	}

	// Cleanly close database 1
	if err := store1.Close(); err != nil {
		t.Fatalf("store1.Close failed: %v", err)
	}

	// Reopen database 2
	store2, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite store2 failed: %v", err)
	}
	defer store2.Close()

	// Verify observation survived
	has, err := store2.HasObservation(ctx, "anom-survive-1")
	if err != nil || !has {
		t.Fatalf("HasObservation returned %v, err: %v", has, err)
	}

	recentObs, err := store2.ListRecentObservations(ctx, "node-dur", "cpu_usage_percent", now.Add(-1*time.Minute), 10)
	if err != nil {
		t.Fatalf("ListRecentObservations failed: %v", err)
	}
	if len(recentObs) != 1 {
		t.Fatalf("expected 1 observation after reopen, got: %d", len(recentObs))
	}
	if recentObs[0].AnomalyID != "anom-survive-1" || recentObs[0].IncidentID != "inc-survive-1" {
		t.Errorf("observation corrupted after reopen: %+v", recentObs[0])
	}
	if recentObs[0].Evidence["threshold"] != "90.0" {
		t.Errorf("observation evidence corrupted: got %s", recentObs[0].Evidence["threshold"])
	}

	// Verify incident survived
	recoveredInc, err := store2.GetIncident(ctx, "inc-survive-1")
	if err != nil {
		t.Fatalf("GetIncident failed after reopen: %v", err)
	}
	if recoveredInc.IncidentID != "inc-survive-1" {
		t.Errorf("IncidentID want inc-survive-1, got %s", recoveredInc.IncidentID)
	}
	if recoveredInc.Status != types.StatusAnomalyDetected {
		t.Errorf("Status want ANOMALY_DETECTED, got %s", recoveredInc.Status)
	}
	if recoveredInc.Severity != types.SeverityCritical {
		t.Errorf("Severity want CRITICAL, got %s", recoveredInc.Severity)
	}
	if recoveredInc.Evidence["anomaly_id"] != "anom-survive-1" {
		t.Errorf("Evidence anomaly_id want anom-survive-1, got %s", recoveredInc.Evidence["anomaly_id"])
	}
}

// Transaction rollback test: verify failed incident validation rolls back observation
func TestSQLite_PersistIncidentEvaluation_RollbackOnInvalidIncident(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "tx_rollback.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	obs := &storage.StoredObservation{
		AnomalyID:       "anom-fail-tx-1",
		NodeID:          "node-1",
		MetricName:      "cpu_usage_percent",
		DetectedAt:      now,
		ObservedValue:   95.0,
		AnomalyScore:    0.5,
		DetectionMethod: types.DetectionMethodStaticThreshold,
	}

	// Incident with empty IncidentID => invalid!
	invalidInc := &types.Incident{
		IncidentID:  "", // invalid
		NodeID:      "node-1",
		RuleName:    "rule",
		Severity:    types.SeverityMedium,
		Status:      types.StatusAnomalyDetected,
		TriggeredAt: now,
	}

	_, err = store.PersistIncidentEvaluation(ctx, obs, invalidInc)
	if err == nil {
		t.Fatal("expected error on invalid incident, got nil")
	}

	// Verify observation was rolled back and does not exist in SQLite
	has, err := store.HasObservation(ctx, "anom-fail-tx-1")
	if err != nil {
		t.Fatalf("HasObservation check failed: %v", err)
	}
	if has {
		t.Fatal("critical invariant violated: observation was committed despite transaction failure!")
	}
}

// TEST: Complete Incident Contract Field Round-Trip
// Verifies that Incident -> SQLite -> close -> reopen -> GetIncident() preserves all meaningful fields bit-for-bit.
func TestSQLite_Incident_CompleteFieldRoundTrip(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "field_roundtrip.db")

	store1, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite store1 failed: %v", err)
	}

	t0 := time.Date(2026, 9, 27, 12, 0, 0, 123456000, time.UTC)
	obs := &storage.StoredObservation{
		AnomalyID:       "anom-rt-1",
		NodeID:          "node-omega",
		MetricName:      "cpu_usage_percent",
		DetectedAt:      t0,
		ObservedValue:   98.7654,
		AnomalyScore:    0.95,
		DetectionMethod: types.DetectionMethodStaticThreshold,
		Evidence:        map[string]string{"sample_id": "s-1"},
		CreatedAt:       t0,
	}

	inc := &types.Incident{
		IncidentID:    "inc-roundtrip-omega-01",
		NodeID:        "node-omega",
		RuleName:      "threshold_cpu_usage_percent",
		Severity:      types.SeverityCritical,
		Status:        types.StatusAnomalyDetected,
		Description:   "Critical CPU usage on node-omega",
		TriggerMetric: "cpu_usage_percent",
		TriggerValue:  98.7654,
		Threshold:     90.0,
		Evidence: map[string]string{
			"sample_id":  "s-1",
			"detector":   "threshold",
			"deviation":  "8.7654",
			"anomaly_id": "anom-rt-1",
		},
		TriggeredAt: t0,
		UpdatedAt:   t0,
		ResolvedAt:  nil,
	}

	isNew, err := store1.PersistIncidentEvaluation(ctx, obs, inc)
	if err != nil || !isNew {
		t.Fatalf("PersistIncidentEvaluation failed: isNew=%v, err=%v", isNew, err)
	}

	// Close store1
	if err := store1.Close(); err != nil {
		t.Fatalf("store1.Close failed: %v", err)
	}

	// Reopen store2
	store2, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite store2 failed: %v", err)
	}
	defer store2.Close()

	recovered, err := store2.GetIncident(ctx, "inc-roundtrip-omega-01")
	if err != nil {
		t.Fatalf("GetIncident failed after reopen: %v", err)
	}

	// Verify all meaningful fields
	if recovered.IncidentID != inc.IncidentID {
		t.Errorf("IncidentID: got %q, want %q", recovered.IncidentID, inc.IncidentID)
	}
	if recovered.NodeID != inc.NodeID {
		t.Errorf("NodeID: got %q, want %q", recovered.NodeID, inc.NodeID)
	}
	if recovered.RuleName != inc.RuleName {
		t.Errorf("RuleName: got %q, want %q", recovered.RuleName, inc.RuleName)
	}
	if recovered.Severity != inc.Severity {
		t.Errorf("Severity: got %q, want %q", recovered.Severity, inc.Severity)
	}
	if recovered.Status != inc.Status {
		t.Errorf("Status: got %q, want %q", recovered.Status, inc.Status)
	}
	if recovered.Description != inc.Description {
		t.Errorf("Description: got %q, want %q", recovered.Description, inc.Description)
	}
	if recovered.TriggerMetric != inc.TriggerMetric {
		t.Errorf("TriggerMetric: got %q, want %q", recovered.TriggerMetric, inc.TriggerMetric)
	}
	if recovered.TriggerValue != inc.TriggerValue {
		t.Errorf("TriggerValue: got %v, want %v", recovered.TriggerValue, inc.TriggerValue)
	}
	if recovered.Threshold != inc.Threshold {
		t.Errorf("Threshold: got %v, want %v", recovered.Threshold, inc.Threshold)
	}
	if !recovered.TriggeredAt.Equal(inc.TriggeredAt) {
		t.Errorf("TriggeredAt: got %v, want %v", recovered.TriggeredAt, inc.TriggeredAt)
	}
	if !recovered.UpdatedAt.Equal(inc.UpdatedAt) {
		t.Errorf("UpdatedAt: got %v, want %v", recovered.UpdatedAt, inc.UpdatedAt)
	}
	if recovered.ResolvedAt != nil {
		t.Errorf("ResolvedAt: expected nil, got %v", recovered.ResolvedAt)
	}
	if len(recovered.Evidence) != len(inc.Evidence) {
		t.Errorf("Evidence len: got %d, want %d", len(recovered.Evidence), len(inc.Evidence))
	}
	for k, wantV := range inc.Evidence {
		if gotV := recovered.Evidence[k]; gotV != wantV {
			t.Errorf("Evidence[%q]: got %q, want %q", k, gotV, wantV)
		}
	}

	// Verify lifecycle resolution round-trip
	t1 := t0.Add(10 * time.Minute)
	if err := store2.UpdateIncidentStatus(ctx, inc.IncidentID, types.StatusRecovered, t1, &t1); err != nil {
		t.Fatalf("UpdateIncidentStatus failed: %v", err)
	}

	// Reopen again and verify resolution fields
	if err := store2.Close(); err != nil {
		t.Fatalf("store2.Close failed: %v", err)
	}

	store3, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite store3 failed: %v", err)
	}
	defer store3.Close()

	resolvedInc, err := store3.GetIncident(ctx, inc.IncidentID)
	if err != nil {
		t.Fatalf("GetIncident failed after resolution reopen: %v", err)
	}
	if resolvedInc.Status != types.StatusRecovered {
		t.Errorf("Status after recovery: got %q, want RECOVERED", resolvedInc.Status)
	}
	if !resolvedInc.UpdatedAt.Equal(t1) {
		t.Errorf("UpdatedAt after recovery: got %v, want %v", resolvedInc.UpdatedAt, t1)
	}
	if resolvedInc.ResolvedAt == nil || !resolvedInc.ResolvedAt.Equal(t1) {
		t.Errorf("ResolvedAt after recovery: got %v, want %v", resolvedInc.ResolvedAt, t1)
	}
}

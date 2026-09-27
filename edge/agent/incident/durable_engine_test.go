package incident_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/incident"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
	_ "modernc.org/sqlite"
)

// Helper to open a test SQLite store
func newTestStore(t *testing.T, dbPath string) *storage.SQLiteStore {
	t.Helper()
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	return store
}

// TEST A: Anomaly inserted + incident created successfully in durable store
func TestDurableEngine_AnomalyInserted_IncidentCreated(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "testA_create.db")
	store := newTestStore(t, dbPath)
	defer store.Close()

	eng, err := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              3,
		WindowDuration: 5 * time.Minute,
		Store:          store,
	})
	if err != nil {
		t.Fatalf("NewLocalEngine failed: %v", err)
	}

	t0 := time.Now().UTC()
	sig1 := makeSignal("anom-A-1", "node-A", "cpu_usage_percent", 95.0, 90.0, 0.5, t0)
	sig2 := makeSignal("anom-A-2", "node-A", "cpu_usage_percent", 96.0, 90.0, 0.7, t0.Add(5*time.Second))

	inc1, err := eng.Process(ctx, sig1)
	if err != nil || inc1 != nil {
		t.Fatalf("expected nil incident for 1-of-2, got: %+v, err: %v", inc1, err)
	}

	// Verify obs 1 was durably recorded in SQLite
	has1, err := store.HasObservation(ctx, "anom-A-1")
	if err != nil || !has1 {
		t.Fatalf("observation 1 not recorded in SQLite: %v", err)
	}

	inc2, err := eng.Process(ctx, sig2)
	if err != nil || inc2 == nil {
		t.Fatalf("expected incident created for 2-of-2, got: %+v, err: %v", inc2, err)
	}

	// Verify incident was durably recorded in SQLite
	dbInc, err := store.GetIncident(ctx, inc2.IncidentID)
	if err != nil {
		t.Fatalf("failed to retrieve incident from SQLite: %v", err)
	}
	if dbInc.IncidentID != inc2.IncidentID {
		t.Errorf("db IncidentID mismatch: got %s, want %s", dbInc.IncidentID, inc2.IncidentID)
	}
	if dbInc.Status != types.StatusAnomalyDetected {
		t.Errorf("db Status mismatch: got %s, want ANOMALY_DETECTED", dbInc.Status)
	}
	if dbInc.Severity != types.SeverityHigh {
		t.Errorf("db Severity mismatch: got %s, want HIGH", dbInc.Severity)
	}
}

// TEST B: Duplicate anomaly in same process run
func TestDurableEngine_DuplicateAnomaly(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "testB_dup.db")
	store := newTestStore(t, dbPath)
	defer store.Close()

	eng, err := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              3,
		WindowDuration: 5 * time.Minute,
		Store:          store,
	})
	if err != nil {
		t.Fatalf("NewLocalEngine failed: %v", err)
	}

	t0 := time.Now().UTC()
	sig1 := makeSignal("anom-B-1", "node-B", "cpu_usage_percent", 95.0, 90.0, 0.5, t0)

	// Submit once
	_, err = eng.Process(ctx, sig1)
	if err != nil {
		t.Fatalf("first submission failed: %v", err)
	}

	// Submit second time (duplicate)
	inc2, err := eng.Process(ctx, sig1)
	if err != nil {
		t.Fatalf("duplicate submission returned error: %v", err)
	}
	if inc2 != nil {
		t.Fatalf("duplicate observation triggered incident: %+v", inc2)
	}

	// Check observations count in SQLite
	obsList, err := store.ListRecentObservations(ctx, "node-B", "cpu_usage_percent", t0.Add(-1*time.Minute), 10)
	if err != nil {
		t.Fatalf("ListRecentObservations failed: %v", err)
	}
	if len(obsList) != 1 {
		t.Fatalf("expected exactly 1 observation in SQLite, got %d", len(obsList))
	}
}

// TEST C: Duplicate anomaly after restart
func TestDurableEngine_DuplicateAnomaly_AfterRestart(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "testC_dup_restart.db")
	t0 := time.Now().UTC()

	// Instance 1
	store1 := newTestStore(t, dbPath)
	eng1, _ := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              3,
		WindowDuration: 5 * time.Minute,
		Store:          store1,
	})

	sig1 := makeSignal("anom-C-1", "node-C", "cpu_usage_percent", 95.0, 90.0, 0.5, t0)
	_, _ = eng1.Process(ctx, sig1)
	store1.Close()

	// Restart: Instance 2
	store2 := newTestStore(t, dbPath)
	defer store2.Close()

	eng2, _ := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              3,
		WindowDuration: 5 * time.Minute,
		Store:          store2,
	})
	if _, _, err := eng2.RecoverFromStore(ctx); err != nil {
		t.Fatalf("RecoverFromStore failed: %v", err)
	}

	// Submit identical sig1 again to instance 2
	inc, err := eng2.Process(ctx, sig1)
	if err != nil {
		t.Fatalf("processing duplicate after restart failed: %v", err)
	}
	if inc != nil {
		t.Fatalf("duplicate anomaly after restart triggered incident: %+v", inc)
	}

	// Verify only 1 observation in SQLite
	obsList, _ := store2.ListRecentObservations(ctx, "node-C", "cpu_usage_percent", t0.Add(-1*time.Minute), 10)
	if len(obsList) != 1 {
		t.Fatalf("expected exactly 1 observation in SQLite, got %d", len(obsList))
	}
}

// TEST D & T: Restart with active incident & IncidentID remains stable across restart
func TestDurableEngine_RestartWithActiveIncident_StableID(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "testD_active_restart.db")
	t0 := time.Now().UTC()

	// Instance 1: trigger active incident (2-of-2)
	store1 := newTestStore(t, dbPath)
	eng1, _ := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              3,
		WindowDuration: 5 * time.Minute,
		Store:          store1,
	})

	sig1 := makeSignal("anom-D-1", "node-D", "cpu_usage_percent", 92.0, 90.0, 0.4, t0)
	sig2 := makeSignal("anom-D-2", "node-D", "cpu_usage_percent", 94.0, 90.0, 0.5, t0.Add(5*time.Second))
	_, _ = eng1.Process(ctx, sig1)
	origInc, err := eng1.Process(ctx, sig2)
	if err != nil || origInc == nil {
		t.Fatalf("failed to create initial incident: %v", err)
	}
	origIncidentID := origInc.IncidentID
	store1.Close()

	// Instance 2: Simulate process restart
	store2 := newTestStore(t, dbPath)
	defer store2.Close()

	eng2, _ := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              3,
		WindowDuration: 5 * time.Minute,
		Store:          store2,
	})
	actCount, obsCount, err := eng2.RecoverFromStore(ctx)
	if err != nil {
		t.Fatalf("RecoverFromStore failed: %v", err)
	}
	if actCount != 1 {
		t.Fatalf("expected 1 active incident recovered, got %d", actCount)
	}
	if obsCount != 2 {
		t.Fatalf("expected 2 observations recovered, got %d", obsCount)
	}

	// Active incident must be queryable in eng2
	active := eng2.GetActiveIncident("node-D", "cpu_usage_percent")
	if active == nil {
		t.Fatal("expected active incident in eng2 after recovery, got nil")
	}
	if active.IncidentID != origIncidentID {
		t.Fatalf("recovered IncidentID mismatch: got %s, want %s", active.IncidentID, origIncidentID)
	}

	// Submit 3rd persistent breach
	sig3 := makeSignal("anom-D-3", "node-D", "cpu_usage_percent", 97.0, 90.0, 0.8, t0.Add(15*time.Second))
	updatedInc, err := eng2.Process(ctx, sig3)
	if err != nil {
		t.Fatalf("failed to process 3rd anomaly: %v", err)
	}
	if updatedInc.IncidentID != origIncidentID {
		t.Fatalf("CRITICAL: IncidentID mutated across restart! got %s, want %s", updatedInc.IncidentID, origIncidentID)
	}
	if updatedInc.TriggerValue != 97.0 {
		t.Errorf("updated TriggerValue mismatch: got %f, want 97.0", updatedInc.TriggerValue)
	}
}

// TEST E & F: Restart with partially completed M-of-N correlation (restart after 1st anomaly)
func TestDurableEngine_RestartPartiallyCompletedMofN(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "testE_partial.db")
	t0 := time.Now().UTC()

	// 1. Process 1st anomaly in eng1
	store1 := newTestStore(t, dbPath)
	eng1, _ := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              3,
		WindowDuration: 5 * time.Minute,
		Store:          store1,
	})

	sig1 := makeSignal("anom-E-1", "node-E", "cpu_usage_percent", 95.0, 90.0, 0.5, t0)
	inc1, err := eng1.Process(ctx, sig1)
	if err != nil || inc1 != nil {
		t.Fatalf("expected nil incident for 1-of-2, got: %+v, err: %v", inc1, err)
	}
	store1.Close()

	// 2. Restart agent and reconstruct engine state
	store2 := newTestStore(t, dbPath)
	defer store2.Close()

	eng2, _ := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              3,
		WindowDuration: 5 * time.Minute,
		Store:          store2,
	})
	actCount, obsCount, err := eng2.RecoverFromStore(ctx)
	if err != nil {
		t.Fatalf("RecoverFromStore failed: %v", err)
	}
	if actCount != 0 {
		t.Fatalf("expected 0 active incidents before threshold, got %d", actCount)
	}
	if obsCount != 1 {
		t.Fatalf("expected 1 observation recovered from store, got %d", obsCount)
	}

	// 3. Process 2nd anomaly in eng2 -> threshold M=2 reached!
	sig2 := makeSignal("anom-E-2", "node-E", "cpu_usage_percent", 96.0, 90.0, 0.6, t0.Add(10*time.Second))
	inc2, err := eng2.Process(ctx, sig2)
	if err != nil {
		t.Fatalf("failed to process 2nd anomaly in eng2: %v", err)
	}
	if inc2 == nil {
		t.Fatal("CRITICAL: M=2 correlation failed to trigger across restart boundary!")
	}
	if inc2.Status != types.StatusAnomalyDetected {
		t.Errorf("expected StatusAnomalyDetected, got: %s", inc2.Status)
	}

	// Verify incident in SQLite
	dbInc, err := store2.GetIncident(ctx, inc2.IncidentID)
	if err != nil || dbInc == nil {
		t.Fatalf("incident not found in SQLite: %v", err)
	}
}

// TEST G: Restart after second qualifying anomaly (already triggered incident)
func TestDurableEngine_RestartAfterSecondAnomaly(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "testG_second.db")
	t0 := time.Now().UTC()

	store1 := newTestStore(t, dbPath)
	eng1, _ := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              3,
		WindowDuration: 5 * time.Minute,
		Store:          store1,
	})

	sig1 := makeSignal("anom-G-1", "node-G", "cpu_usage_percent", 93.0, 90.0, 0.4, t0)
	sig2 := makeSignal("anom-G-2", "node-G", "cpu_usage_percent", 94.0, 90.0, 0.5, t0.Add(2*time.Second))
	_, _ = eng1.Process(ctx, sig1)
	inc, err := eng1.Process(ctx, sig2)
	if err != nil || inc == nil {
		t.Fatalf("failed to trigger incident on 2nd anomaly: %v", err)
	}
	incID := inc.IncidentID
	store1.Close()

	// Restart
	store2 := newTestStore(t, dbPath)
	defer store2.Close()
	eng2, _ := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              3,
		WindowDuration: 5 * time.Minute,
		Store:          store2,
	})
	_, _, err = eng2.RecoverFromStore(ctx)
	if err != nil {
		t.Fatalf("RecoverFromStore failed: %v", err)
	}

	active := eng2.GetActiveIncident("node-G", "cpu_usage_percent")
	if active == nil || active.IncidentID != incID {
		t.Fatalf("active incident not correctly recovered after second anomaly: %+v", active)
	}
}

// TEST H: Correlation window expiration
func TestDurableEngine_WindowExpiration(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "testH_exp.db")
	store := newTestStore(t, dbPath)
	defer store.Close()

	eng, _ := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              3,
		WindowDuration: 1 * time.Minute,
		Store:          store,
	})

	t0 := time.Now().UTC()
	// Observation 1 at t0
	sig1 := makeSignal("anom-H-1", "node-H", "cpu_usage_percent", 95.0, 90.0, 0.5, t0)
	_, _ = eng.Process(ctx, sig1)

	// Observation 2 at t0 + 2 minutes (exceeds 1-minute window)
	sig2 := makeSignal("anom-H-2", "node-H", "cpu_usage_percent", 96.0, 90.0, 0.6, t0.Add(2*time.Minute))
	inc2, err := eng.Process(ctx, sig2)
	if err != nil {
		t.Fatalf("unexpected error on sig2: %v", err)
	}
	// Because sig1 expired, count within window is only 1 => No incident!
	if inc2 != nil {
		t.Fatalf("expired observation contributed to correlation! Got incident: %+v", inc2)
	}
}

// TEST I: Out-of-order anomaly after restart
func TestDurableEngine_OutOfOrder_AfterRestart(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "testI_ooo.db")
	t0 := time.Now().UTC()

	store1 := newTestStore(t, dbPath)
	eng1, _ := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              3,
		WindowDuration: 2 * time.Minute,
		Store:          store1,
	})

	sigRecent := makeSignal("anom-I-recent", "node-I", "cpu_usage_percent", 95.0, 90.0, 0.5, t0)
	_, _ = eng1.Process(ctx, sigRecent)
	store1.Close()

	// Restart
	store2 := newTestStore(t, dbPath)
	defer store2.Close()
	eng2, _ := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              3,
		WindowDuration: 2 * time.Minute,
		Store:          store2,
	})
	_, _, _ = eng2.RecoverFromStore(ctx)

	// Submit stale anomaly detected 10 minutes prior to t0
	sigStale := makeSignal("anom-I-stale", "node-I", "cpu_usage_percent", 95.0, 90.0, 0.5, t0.Add(-10*time.Minute))
	_, err := eng2.Process(ctx, sigStale)
	if err == nil {
		t.Fatal("expected ErrStaleAnomalySignal for out-of-order signal predating window after restart, got nil")
	}
	if !errors.Is(err, incident.ErrStaleAnomalySignal) {
		t.Errorf("expected ErrStaleAnomalySignal, got: %v", err)
	}
}

// TEST J & K: Concurrent duplicate and multi-anomaly submissions
func TestDurableEngine_ConcurrentSubmissions(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "testJK_concurrent.db")
	store := newTestStore(t, dbPath)
	defer store.Close()

	eng, err := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              5,
		WindowDuration: 5 * time.Minute,
		Store:          store,
	})
	if err != nil {
		t.Fatalf("NewLocalEngine failed: %v", err)
	}

	t0 := time.Now().UTC()
	const workers = 40
	var wg sync.WaitGroup
	wg.Add(workers)

	// Half submit identical duplicate anomaly, half submit distinct anomalies
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			defer wg.Done()
			var sig *types.AnomalySignal
			if workerID%2 == 0 {
				// Duplicates of same AnomalyID
				sig = makeSignal("anom-dup-shared", "node-conc", "cpu_usage_percent", 95.0, 90.0, 0.5, t0)
			} else {
				// Distinct anomalies
				id := fmt.Sprintf("anom-distinct-%d", workerID)
				sig = makeSignal(id, "node-conc", "cpu_usage_percent", 96.0, 90.0, 0.6, t0.Add(time.Duration(workerID)*time.Millisecond))
			}
			_, _ = eng.Process(ctx, sig)
		}(i)
	}

	wg.Wait()

	// Verify only 1 active incident exists for node-conc:cpu_usage_percent
	activeList, err := store.ListActiveIncidents(ctx)
	if err != nil {
		t.Fatalf("ListActiveIncidents failed: %v", err)
	}
	var nodeConcIncidents int
	for _, inc := range activeList {
		if inc.NodeID == "node-conc" && inc.TriggerMetric == "cpu_usage_percent" {
			nodeConcIncidents++
		}
	}
	if nodeConcIncidents > 1 {
		t.Fatalf("CRITICAL: Concurrent race created %d duplicate incidents for the same stream!", nodeConcIncidents)
	}
}

// TEST L & M: Isolation of different nodes and different metrics in durable store
func TestDurableEngine_NodeAndMetricIsolation(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "testLM_iso.db")
	store := newTestStore(t, dbPath)
	defer store.Close()

	eng, _ := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              3,
		WindowDuration: 5 * time.Minute,
		Store:          store,
	})

	t0 := time.Now().UTC()

	// 1 signal to Node A CPU, 1 to Node B CPU, 1 to Node A Mem
	sig1 := makeSignal("anom-L1", "node-A", "cpu_usage_percent", 95.0, 90.0, 0.5, t0)
	sig2 := makeSignal("anom-L2", "node-B", "cpu_usage_percent", 95.0, 90.0, 0.5, t0)
	sig3 := makeSignal("anom-M1", "node-A", "memory_usage_percent", 95.0, 90.0, 0.5, t0)

	_, _ = eng.Process(ctx, sig1)
	_, _ = eng.Process(ctx, sig2)
	_, _ = eng.Process(ctx, sig3)

	// Since M=2, none should trigger an incident!
	activeList, err := store.ListActiveIncidents(ctx)
	if err != nil {
		t.Fatalf("ListActiveIncidents failed: %v", err)
	}
	if len(activeList) != 0 {
		t.Fatalf("expected 0 active incidents due to stream isolation, got %d", len(activeList))
	}
}

// TEST N: Invalid FSM transition rolls back and does not corrupt durable state
func TestDurableEngine_InvalidFSMTransition_Rollback(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "testN_fsm_roll.db")
	store := newTestStore(t, dbPath)
	defer store.Close()

	eng, _ := incident.NewLocalEngine(incident.PolicyConfig{
		M:              1,
		N:              1,
		WindowDuration: 5 * time.Minute,
		Store:          store,
	})

	t0 := time.Now().UTC()
	sig := makeSignal("anom-N-1", "node-N", "cpu_usage_percent", 95.0, 90.0, 0.5, t0)
	inc, err := eng.Process(ctx, sig)
	if err != nil || inc == nil {
		t.Fatalf("failed to create incident: %v", err)
	}

	// Attempt invalid transition: ANOMALY_DETECTED -> RECOVERED directly
	_, err = eng.TransitionActiveIncident(ctx, "node-N", "cpu_usage_percent", types.StatusRecovered, t0.Add(1*time.Second))
	if err == nil {
		t.Fatal("expected error on invalid transition, got nil")
	}

	// Verify durable status in SQLite remains ANOMALY_DETECTED
	dbInc, err := store.GetIncident(ctx, inc.IncidentID)
	if err != nil {
		t.Fatalf("GetIncident failed: %v", err)
	}
	if dbInc.Status != types.StatusAnomalyDetected {
		t.Fatalf("durable status was corrupted! got %s, want ANOMALY_DETECTED", dbInc.Status)
	}
}

// TEST: Commit-Before-Crash Recovery
// Scenario:
// 1. SQLite transaction commits successfully.
// 2. Process terminates before any in-memory engine update.
// 3. New process starts.
// 4. RecoverFromStore() reconstructs the same effective state.
// Verifies: observation remains present, incident remains present, IncidentID stable,
// active incident restored, duplicate AnomalyID does not create second lifecycle.
func TestDurableEngine_CommitBeforeCrash_Recovery(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "commit_before_crash.db")
	t0 := time.Now().UTC()

	// 1. Direct SQLite commit (simulating commit occurring, but process dying before in-memory state update)
	store1 := newTestStore(t, dbPath)
	obs := &storage.StoredObservation{
		AnomalyID:       "anom-crash-1",
		IncidentID:      "inc-crash-stable-01",
		NodeID:          "node-crash",
		MetricName:      "cpu_usage_percent",
		DetectedAt:      t0,
		ObservedValue:   96.0,
		AnomalyScore:    0.80,
		DetectionMethod: types.DetectionMethodStaticThreshold,
		Evidence:        map[string]string{"threshold": "90.0"},
		CreatedAt:       t0,
	}
	inc := &types.Incident{
		IncidentID:    "inc-crash-stable-01",
		NodeID:        "node-crash",
		RuleName:      "threshold_cpu_usage_percent",
		Severity:      types.SeverityHigh,
		Status:        types.StatusAnomalyDetected,
		Description:   "Crash recovery test incident",
		TriggerMetric: "cpu_usage_percent",
		TriggerValue:  96.0,
		Threshold:     90.0,
		Evidence: map[string]string{
			"anomaly_id": "anom-crash-1",
			"score":      "0.80",
		},
		TriggeredAt: t0,
		UpdatedAt:   t0,
	}

	isNew, err := store1.PersistIncidentEvaluation(ctx, obs, inc)
	if err != nil || !isNew {
		t.Fatalf("PersistIncidentEvaluation failed: isNew=%v, err=%v", isNew, err)
	}

	// 2. Process crashes / terminates abruptly: close store1
	if err := store1.Close(); err != nil {
		t.Fatalf("store1.Close failed: %v", err)
	}

	// 3. New process starts: open store2 and instantiate fresh LocalEngine
	store2 := newTestStore(t, dbPath)
	defer store2.Close()

	eng2, err := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              3,
		WindowDuration: 5 * time.Minute,
		Store:          store2,
	})
	if err != nil {
		t.Fatalf("NewLocalEngine failed: %v", err)
	}

	// 4. RecoverFromStore reconstructs authoritative state from SQLite
	actCount, obsCount, err := eng2.RecoverFromStore(ctx)
	if err != nil {
		t.Fatalf("RecoverFromStore failed: %v", err)
	}
	if actCount != 1 {
		t.Errorf("recovered active incidents: got %d, want 1", actCount)
	}
	if obsCount != 1 {
		t.Errorf("recovered observations: got %d, want 1", obsCount)
	}

	// Verification A: Observation remains present in SQLite
	hasObs, err := store2.HasObservation(ctx, "anom-crash-1")
	if err != nil || !hasObs {
		t.Fatalf("observation not present in store2: %v", err)
	}

	// Verification B: Incident remains present in SQLite with stable IncidentID
	dbInc, err := store2.GetIncident(ctx, "inc-crash-stable-01")
	if err != nil {
		t.Fatalf("GetIncident failed: %v", err)
	}
	if dbInc.IncidentID != "inc-crash-stable-01" {
		t.Errorf("IncidentID: got %s, want inc-crash-stable-01", dbInc.IncidentID)
	}
	if dbInc.Status != types.StatusAnomalyDetected {
		t.Errorf("Status: got %s, want ANOMALY_DETECTED", dbInc.Status)
	}

	// Verification C: Active incident state is restored in memory
	memInc := eng2.GetActiveIncident("node-crash", "cpu_usage_percent")
	if memInc == nil {
		t.Fatal("expected active incident in memory, got nil")
	}
	if memInc.IncidentID != "inc-crash-stable-01" {
		t.Errorf("memory IncidentID: got %s, want inc-crash-stable-01", memInc.IncidentID)
	}

	// Verification D: Submitting duplicate AnomalyID does not create a second lifecycle
	dupSig := makeSignal("anom-crash-1", "node-crash", "cpu_usage_percent", 96.0, 90.0, 0.80, t0)
	retInc, err := eng2.Process(ctx, dupSig)
	if err != nil {
		t.Fatalf("Process duplicate signal failed: %v", err)
	}
	if retInc == nil || retInc.IncidentID != "inc-crash-stable-01" {
		t.Fatalf("duplicate signal returned invalid incident: %+v", retInc)
	}

	// Raw SQLite row count check: still exactly 1 incident and 1 observation
	dbRaw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	defer dbRaw.Close()

	var incRows, obsRows int
	if err := dbRaw.QueryRow("SELECT COUNT(*) FROM incident_records;").Scan(&incRows); err != nil || incRows != 1 {
		t.Fatalf("expected 1 incident record in SQLite, got %d, err: %v", incRows, err)
	}
	if err := dbRaw.QueryRow("SELECT COUNT(*) FROM incident_observations;").Scan(&obsRows); err != nil || obsRows != 1 {
		t.Fatalf("expected 1 observation record in SQLite, got %d, err: %v", obsRows, err)
	}

	// Verification E: Submitting subsequent anomaly updates existing incident without new lifecycle
	sig2 := makeSignal("anom-crash-2", "node-crash", "cpu_usage_percent", 98.0, 90.0, 0.90, t0.Add(10*time.Second))
	updatedInc, err := eng2.Process(ctx, sig2)
	if err != nil {
		t.Fatalf("Process sig2 failed: %v", err)
	}
	if updatedInc.IncidentID != "inc-crash-stable-01" {
		t.Fatalf("subsequent anomaly created duplicate incident: got %s, want inc-crash-stable-01", updatedInc.IncidentID)
	}
	if updatedInc.TriggerValue != 98.0 {
		t.Errorf("TriggerValue: got %v, want 98.0", updatedInc.TriggerValue)
	}
	if err := dbRaw.QueryRow("SELECT COUNT(*) FROM incident_records;").Scan(&incRows); err != nil || incRows != 1 {
		t.Fatalf("expected still exactly 1 incident record in SQLite, got %d", incRows)
	}
}

// TEST: M-of-N Correlation Across Restart Without Duplicate Lifecycle
// Verifies:
// A1 -> A2 (incident created) -> restart -> A3
// does not create a duplicate lifecycle.
func TestDurableEngine_MofN_AcrossRestart_NoDuplicateLifecycle(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "mofn_no_dup_lifecycle.db")
	t0 := time.Now().UTC()

	store1 := newTestStore(t, dbPath)
	eng1, _ := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              3,
		WindowDuration: 5 * time.Minute,
		Store:          store1,
	})

	sig1 := makeSignal("anom-M1", "node-M", "cpu_usage_percent", 92.0, 90.0, 0.5, t0)
	sig2 := makeSignal("anom-M2", "node-M", "cpu_usage_percent", 94.0, 90.0, 0.6, t0.Add(5*time.Second))

	// A1: count=1, no incident
	inc1, err := eng1.Process(ctx, sig1)
	if err != nil || inc1 != nil {
		t.Fatalf("A1 created unexpected incident: %+v, err: %v", inc1, err)
	}

	// A2: count=2, incident created
	inc2, err := eng1.Process(ctx, sig2)
	if err != nil || inc2 == nil {
		t.Fatalf("A2 failed to create incident: %+v, err: %v", inc2, err)
	}
	primaryIncidentID := inc2.IncidentID
	store1.Close()

	// Restart
	store2 := newTestStore(t, dbPath)
	defer store2.Close()
	eng2, _ := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              3,
		WindowDuration: 5 * time.Minute,
		Store:          store2,
	})
	if _, _, err := eng2.RecoverFromStore(ctx); err != nil {
		t.Fatalf("RecoverFromStore failed: %v", err)
	}

	// A3 after restart: must correlate to existing incident, NOT create a second lifecycle
	sig3 := makeSignal("anom-M3", "node-M", "cpu_usage_percent", 97.0, 90.0, 0.8, t0.Add(15*time.Second))
	inc3, err := eng2.Process(ctx, sig3)
	if err != nil {
		t.Fatalf("Process A3 failed: %v", err)
	}
	if inc3.IncidentID != primaryIncidentID {
		t.Fatalf("CRITICAL: A3 created duplicate incident! got %s, want %s", inc3.IncidentID, primaryIncidentID)
	}

	// Check raw SQLite count: exactly 1 incident record
	dbRaw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	defer dbRaw.Close()

	var incCount int
	if err := dbRaw.QueryRow("SELECT COUNT(*) FROM incident_records WHERE node_id = 'node-M';").Scan(&incCount); err != nil || incCount != 1 {
		t.Fatalf("expected exactly 1 incident record in SQLite, got %d, err: %v", incCount, err)
	}
}

// TEST: Exact Duplicate Semantics
// Verifies sequence: A1 -> A2 -> duplicate A2 -> A3
// does NOT count duplicate A2 twice, both within one process and after restart.
func TestDurableEngine_ExactDuplicateSemantics(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()

	// Part A: Within single process
	t.Run("SingleProcess", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "dup_single_proc.db")
		store := newTestStore(t, dbPath)
		defer store.Close()

		eng, _ := incident.NewLocalEngine(incident.PolicyConfig{
			M:              2,
			N:              3,
			WindowDuration: 5 * time.Minute,
			Store:          store,
		})

		sig1 := makeSignal("anom-X1", "node-X", "cpu_usage_percent", 92.0, 90.0, 0.5, t0)
		sig2 := makeSignal("anom-X2", "node-X", "cpu_usage_percent", 94.0, 90.0, 0.6, t0.Add(5*time.Second))
		sig3 := makeSignal("anom-X3", "node-X", "cpu_usage_percent", 96.0, 90.0, 0.7, t0.Add(10*time.Second))

		// A1
		_, _ = eng.Process(ctx, sig1)
		// A2 -> triggers incident
		incA2, err := eng.Process(ctx, sig2)
		if err != nil || incA2 == nil {
			t.Fatalf("A2 failed: %v", err)
		}
		expectedID := incA2.IncidentID

		// Duplicate A2
		dupInc, err := eng.Process(ctx, sig2)
		if err != nil {
			t.Fatalf("duplicate A2 failed: %v", err)
		}
		if dupInc.IncidentID != expectedID {
			t.Errorf("duplicate A2 returned wrong ID: got %s, want %s", dupInc.IncidentID, expectedID)
		}

		// A3
		incA3, err := eng.Process(ctx, sig3)
		if err != nil {
			t.Fatalf("A3 failed: %v", err)
		}
		if incA3.IncidentID != expectedID {
			t.Errorf("A3 returned wrong ID: got %s, want %s", incA3.IncidentID, expectedID)
		}

		// Assert SQLite counts via raw SQL
		dbRaw, err := sql.Open("sqlite", dbPath)
		if err != nil {
			t.Fatalf("sql.Open failed: %v", err)
		}
		defer dbRaw.Close()

		var obsCount, incCount int
		if err := dbRaw.QueryRow("SELECT COUNT(*) FROM incident_observations;").Scan(&obsCount); err != nil || obsCount != 3 {
			t.Fatalf("expected exactly 3 observations in SQLite, got %d", obsCount)
		}
		if err := dbRaw.QueryRow("SELECT COUNT(*) FROM incident_records;").Scan(&incCount); err != nil || incCount != 1 {
			t.Fatalf("expected exactly 1 incident record in SQLite, got %d", incCount)
		}
	})

	// Part B: Across restart
	t.Run("AcrossRestart", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "dup_across_restart.db")

		sig1 := makeSignal("anom-Y1", "node-Y", "cpu_usage_percent", 92.0, 90.0, 0.5, t0)
		sig2 := makeSignal("anom-Y2", "node-Y", "cpu_usage_percent", 94.0, 90.0, 0.6, t0.Add(5*time.Second))
		sig3 := makeSignal("anom-Y3", "node-Y", "cpu_usage_percent", 96.0, 90.0, 0.7, t0.Add(10*time.Second))

		// Process 1: A1, A2
		store1 := newTestStore(t, dbPath)
		eng1, _ := incident.NewLocalEngine(incident.PolicyConfig{
			M:              2,
			N:              3,
			WindowDuration: 5 * time.Minute,
			Store:          store1,
		})
		_, _ = eng1.Process(ctx, sig1)
		incA2, err := eng1.Process(ctx, sig2)
		if err != nil || incA2 == nil {
			t.Fatalf("A2 failed: %v", err)
		}
		expectedID := incA2.IncidentID
		store1.Close()

		// Process 2: Restart -> duplicate A2 -> A3
		store2 := newTestStore(t, dbPath)
		defer store2.Close()
		eng2, _ := incident.NewLocalEngine(incident.PolicyConfig{
			M:              2,
			N:              3,
			WindowDuration: 5 * time.Minute,
			Store:          store2,
		})
		if _, _, err := eng2.RecoverFromStore(ctx); err != nil {
			t.Fatalf("RecoverFromStore failed: %v", err)
		}

		// Submit duplicate A2
		dupInc, err := eng2.Process(ctx, sig2)
		if err != nil {
			t.Fatalf("duplicate A2 after restart failed: %v", err)
		}
		if dupInc.IncidentID != expectedID {
			t.Errorf("duplicate A2 returned wrong ID: got %s, want %s", dupInc.IncidentID, expectedID)
		}

		// Submit A3
		incA3, err := eng2.Process(ctx, sig3)
		if err != nil {
			t.Fatalf("A3 after restart failed: %v", err)
		}
		if incA3.IncidentID != expectedID {
			t.Errorf("A3 returned wrong ID: got %s, want %s", incA3.IncidentID, expectedID)
		}

		// Assert SQLite counts via raw SQL
		dbRaw, err := sql.Open("sqlite", dbPath)
		if err != nil {
			t.Fatalf("sql.Open failed: %v", err)
		}
		defer dbRaw.Close()

		var obsCount, incCount int
		if err := dbRaw.QueryRow("SELECT COUNT(*) FROM incident_observations;").Scan(&obsCount); err != nil || obsCount != 3 {
			t.Fatalf("expected exactly 3 observations in SQLite, got %d", obsCount)
		}
		if err := dbRaw.QueryRow("SELECT COUNT(*) FROM incident_records;").Scan(&incCount); err != nil || incCount != 1 {
			t.Fatalf("expected exactly 1 incident record in SQLite, got %d", incCount)
		}
	})
}

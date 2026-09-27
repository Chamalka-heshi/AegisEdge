package incident_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/incident"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

func makeSignal(anomalyID, nodeID, metric string, val, expected, score float64, detectedAt time.Time) *types.AnomalySignal {
	return &types.AnomalySignal{
		AnomalyID:       anomalyID,
		NodeID:          nodeID,
		MetricName:      metric,
		ObservedValue:   val,
		ExpectedValue:   expected,
		Deviation:       val - expected,
		AnomalyScore:    score,
		DetectionMethod: types.DetectionMethodStaticThreshold,
		DetectedAt:      detectedAt,
		Evidence: map[string]string{
			"threshold":   fmt.Sprintf("%.2f", expected),
			"breach_type": "upper",
		},
		CorrelationID:   "batch-001",
		DetectorVersion: "1.0.0",
	}
}

// TEST 1 — Below Threshold: Insufficient anomalies (e.g. M=2, received 1) => No incident created
func TestIncidentEngine_BelowThreshold(t *testing.T) {
	eng, err := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              3,
		WindowDuration: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	ctx := context.Background()
	now := time.Now().UTC()
	sig1 := makeSignal("anom-001", "node-1", "cpu_usage_percent", 95.0, 90.0, 0.5, now)

	inc, err := eng.Process(ctx, sig1)
	if err != nil {
		t.Fatalf("unexpected error on below-threshold signal: %v", err)
	}
	if inc != nil {
		t.Fatalf("expected nil incident for 1-of-2 anomaly observations, got: %+v", inc)
	}

	if active := eng.GetActiveIncident("node-1", "cpu_usage_percent"); active != nil {
		t.Fatalf("expected no active incident in engine, got: %+v", active)
	}
}

// TEST 2 — M-of-N Trigger: M=2, N=3. Observation 1 => no incident; Observation 2 => incident created
func TestIncidentEngine_MofNTrigger(t *testing.T) {
	var idCounter int
	eng, err := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              3,
		WindowDuration: 5 * time.Minute,
		IDGenerator: func() (string, error) {
			idCounter++
			return fmt.Sprintf("inc-deterministic-%03d", idCounter), nil
		},
	})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	ctx := context.Background()
	now := time.Now().UTC()

	sig1 := makeSignal("anom-101", "node-1", "cpu_usage_percent", 92.0, 90.0, 0.4, now)
	sig2 := makeSignal("anom-102", "node-1", "cpu_usage_percent", 94.0, 90.0, 0.65, now.Add(10*time.Second))

	inc1, err := eng.Process(ctx, sig1)
	if err != nil {
		t.Fatalf("obs 1 error: %v", err)
	}
	if inc1 != nil {
		t.Fatalf("expected no incident after 1st observation, got: %+v", inc1)
	}

	inc2, err := eng.Process(ctx, sig2)
	if err != nil {
		t.Fatalf("obs 2 error: %v", err)
	}
	if inc2 == nil {
		t.Fatal("expected incident to be created after 2nd observation, got nil")
	}

	if inc2.IncidentID != "inc-deterministic-001" {
		t.Errorf("expected incident ID 'inc-deterministic-001', got: %s", inc2.IncidentID)
	}
	if inc2.Status != types.StatusAnomalyDetected {
		t.Errorf("expected StatusAnomalyDetected, got: %s", inc2.Status)
	}
	if inc2.Severity != types.SeverityHigh {
		t.Errorf("expected SeverityHigh for score 0.65, got: %s", inc2.Severity)
	}
	if err := inc2.Validate(); err != nil {
		t.Errorf("created incident failed domain validation: %v", err)
	}
}

// TEST 3 — Persistent Anomaly: Additional qualifying anomaly while incident is active => Same IncidentID
func TestIncidentEngine_PersistentAnomaly(t *testing.T) {
	eng, err := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              3,
		WindowDuration: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	ctx := context.Background()
	now := time.Now().UTC()

	sig1 := makeSignal("anom-201", "node-1", "cpu_usage_percent", 92.0, 90.0, 0.4, now)
	sig2 := makeSignal("anom-202", "node-1", "cpu_usage_percent", 94.0, 90.0, 0.5, now.Add(5*time.Second))
	sig3 := makeSignal("anom-203", "node-1", "cpu_usage_percent", 96.0, 90.0, 0.7, now.Add(10*time.Second))

	_, _ = eng.Process(ctx, sig1)
	incInit, err := eng.Process(ctx, sig2)
	if err != nil || incInit == nil {
		t.Fatalf("failed to trigger initial incident: %v", err)
	}
	initialIncidentID := incInit.IncidentID

	// 3rd anomaly while still active
	incPersist, err := eng.Process(ctx, sig3)
	if err != nil {
		t.Fatalf("failed to process persistent anomaly: %v", err)
	}
	if incPersist == nil {
		t.Fatal("expected active incident returned for persistent anomaly, got nil")
	}

	if incPersist.IncidentID != initialIncidentID {
		t.Fatalf("expected IncidentID to remain stable (%s), but got %s", initialIncidentID, incPersist.IncidentID)
	}
	if incPersist.TriggerValue != 96.0 {
		t.Errorf("expected updated trigger value 96.0, got: %f", incPersist.TriggerValue)
	}
	if incPersist.Evidence["latest_anomaly_id"] != "anom-203" {
		t.Errorf("expected latest_anomaly_id 'anom-203', got: %s", incPersist.Evidence["latest_anomaly_id"])
	}
}

// TEST 4 — Duplicate AnomalyID: Processing identical AnomalySignal twice does not increment observation count
func TestIncidentEngine_DuplicateAnomalyID(t *testing.T) {
	eng, err := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              3,
		WindowDuration: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	ctx := context.Background()
	now := time.Now().UTC()

	sig1 := makeSignal("anom-dup-001", "node-1", "cpu_usage_percent", 95.0, 90.0, 0.5, now)

	// Send sig1 first time
	inc1, err := eng.Process(ctx, sig1)
	if err != nil || inc1 != nil {
		t.Fatalf("expected nil incident for 1st observation, got: %v, err: %v", inc1, err)
	}

	// Send EXACT SAME sig1 second time (replay/duplicate)
	inc2, err := eng.Process(ctx, sig1)
	if err != nil {
		t.Fatalf("unexpected error on duplicate signal: %v", err)
	}
	if inc2 != nil {
		t.Fatalf("duplicate AnomalyID must not trigger M=2 incident! Got incident: %+v", inc2)
	}

	// Now send a distinct second anomaly
	sig2 := makeSignal("anom-dup-002", "node-1", "cpu_usage_percent", 96.0, 90.0, 0.6, now.Add(5*time.Second))
	inc3, err := eng.Process(ctx, sig2)
	if err != nil {
		t.Fatalf("unexpected error on second distinct signal: %v", err)
	}
	if inc3 == nil {
		t.Fatal("expected incident created after second distinct observation")
	}
}

// TEST 5 — Node Isolation: Node A CPU + Node B CPU do not share correlation state
func TestIncidentEngine_NodeIsolation(t *testing.T) {
	eng, err := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              3,
		WindowDuration: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	ctx := context.Background()
	now := time.Now().UTC()

	sigNodeA := makeSignal("anom-nodeA-1", "node-A", "cpu_usage_percent", 95.0, 90.0, 0.5, now)
	sigNodeB := makeSignal("anom-nodeB-1", "node-B", "cpu_usage_percent", 95.0, 90.0, 0.5, now)

	incA, err := eng.Process(ctx, sigNodeA)
	if err != nil || incA != nil {
		t.Fatalf("expected nil incident for node A obs 1, got: %v, err: %v", incA, err)
	}

	incB, err := eng.Process(ctx, sigNodeB)
	if err != nil || incB != nil {
		t.Fatalf("expected nil incident for node B obs 1 (must be isolated from node A), got: %v, err: %v", incB, err)
	}
}

// TEST 6 — Metric Isolation: Node A CPU + Node A Memory do not share correlation state
func TestIncidentEngine_MetricIsolation(t *testing.T) {
	eng, err := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              3,
		WindowDuration: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	ctx := context.Background()
	now := time.Now().UTC()

	sigCPU := makeSignal("anom-cpu-1", "node-A", "cpu_usage_percent", 95.0, 90.0, 0.5, now)
	sigMem := makeSignal("anom-mem-1", "node-A", "memory_usage_percent", 95.0, 90.0, 0.5, now)

	incCPU, err := eng.Process(ctx, sigCPU)
	if err != nil || incCPU != nil {
		t.Fatalf("expected nil incident for CPU obs 1, got: %v, err: %v", incCPU, err)
	}

	incMem, err := eng.Process(ctx, sigMem)
	if err != nil || incMem != nil {
		t.Fatalf("expected nil incident for Mem obs 1 (must be isolated from CPU), got: %v, err: %v", incMem, err)
	}
}

// TEST 7 — Incident State Transition: Valid FSM transitions
func TestIncidentEngine_ValidStateTransitions(t *testing.T) {
	eng, err := incident.NewLocalEngine(incident.PolicyConfig{
		M:              1,
		N:              1,
		WindowDuration: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	ctx := context.Background()
	now := time.Now().UTC()
	sig := makeSignal("anom-fsm-1", "node-1", "cpu_usage_percent", 95.0, 90.0, 0.5, now)

	inc, err := eng.Process(ctx, sig)
	if err != nil || inc == nil {
		t.Fatalf("failed to create incident: %v", err)
	}
	if inc.Status != types.StatusAnomalyDetected {
		t.Fatalf("initial status want ANOMALY_DETECTED, got: %s", inc.Status)
	}

	// 1. Transition: ANOMALY_DETECTED -> MITIGATING
	inc, err = eng.TransitionActiveIncident(ctx, "node-1", "cpu_usage_percent", types.StatusMitigating, now.Add(1*time.Second))
	if err != nil {
		t.Fatalf("transition to MITIGATING failed: %v", err)
	}
	if inc.Status != types.StatusMitigating {
		t.Errorf("status want MITIGATING, got: %s", inc.Status)
	}

	// 2. Transition: MITIGATING -> RECOVERED
	inc, err = eng.TransitionActiveIncident(ctx, "node-1", "cpu_usage_percent", types.StatusRecovered, now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("transition to RECOVERED failed: %v", err)
	}
	if inc.Status != types.StatusRecovered {
		t.Errorf("status want RECOVERED, got: %s", inc.Status)
	}
	if inc.ResolvedAt == nil {
		t.Error("expected ResolvedAt timestamp to be set upon recovery")
	}

	// After recovery, stream activeIncident should be cleared
	if active := eng.GetActiveIncident("node-1", "cpu_usage_percent"); active != nil {
		t.Fatalf("expected active incident cleared after recovery, got: %+v", active)
	}
}

// TEST 8 — Invalid Transition: Attempt invalid FSM transition
func TestIncidentEngine_InvalidTransition(t *testing.T) {
	eng, err := incident.NewLocalEngine(incident.PolicyConfig{
		M:              1,
		N:              1,
		WindowDuration: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	ctx := context.Background()
	now := time.Now().UTC()
	sig := makeSignal("anom-inv-1", "node-1", "cpu_usage_percent", 95.0, 90.0, 0.5, now)

	inc, err := eng.Process(ctx, sig)
	if err != nil || inc == nil {
		t.Fatalf("failed to create incident: %v", err)
	}

	// Invalid transition: ANOMALY_DETECTED directly to RECOVERED (skipping MITIGATING / ESCALATED)
	_, err = eng.TransitionActiveIncident(ctx, "node-1", "cpu_usage_percent", types.StatusRecovered, now.Add(1*time.Second))
	if err == nil {
		t.Fatal("expected invalid transition error from ANOMALY_DETECTED directly to RECOVERED, got nil")
	}
	if !errors.Is(err, types.ErrInvalidStateTransition) {
		t.Errorf("expected ErrInvalidStateTransition, got: %v", err)
	}
}

// TEST 9 — Evidence Propagation: Verify all AnomalySignal evidence is correctly populated in Incident
func TestIncidentEngine_EvidencePropagation(t *testing.T) {
	eng, err := incident.NewLocalEngine(incident.PolicyConfig{
		M:              1,
		N:              1,
		WindowDuration: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	ctx := context.Background()
	now := time.Now().UTC()
	sig := makeSignal("anom-ev-1", "node-1", "cpu_usage_percent", 95.5, 90.0, 0.75, now)
	sig.CorrelationID = "batch-xyz-999"
	sig.Evidence["custom_diagnostic"] = "high_iowait"

	inc, err := eng.Process(ctx, sig)
	if err != nil || inc == nil {
		t.Fatalf("failed to process signal: %v", err)
	}

	// Verify minimum populated fields
	if inc.TriggerMetric != "cpu_usage_percent" {
		t.Errorf("TriggerMetric want cpu_usage_percent, got %s", inc.TriggerMetric)
	}
	if inc.TriggerValue != 95.5 {
		t.Errorf("TriggerValue want 95.5, got %f", inc.TriggerValue)
	}
	if inc.Threshold != 90.0 {
		t.Errorf("Threshold want 90.0, got %f", inc.Threshold)
	}
	if inc.Evidence["anomaly_id"] != "anom-ev-1" {
		t.Errorf("Evidence anomaly_id want anom-ev-1, got %s", inc.Evidence["anomaly_id"])
	}
	if inc.Evidence["correlation_id"] != "batch-xyz-999" {
		t.Errorf("Evidence correlation_id want batch-xyz-999, got %s", inc.Evidence["correlation_id"])
	}
	if inc.Evidence["custom_diagnostic"] != "high_iowait" {
		t.Errorf("Evidence custom_diagnostic want high_iowait, got %s", inc.Evidence["custom_diagnostic"])
	}
	if inc.Evidence["detection_method"] != types.DetectionMethodStaticThreshold {
		t.Errorf("Evidence detection_method want static_threshold, got %s", inc.Evidence["detection_method"])
	}
	if inc.Evidence["detector_version"] != "1.0.0" {
		t.Errorf("Evidence detector_version want 1.0.0, got %s", inc.Evidence["detector_version"])
	}
}

// TEST 10 — Severity Mapping: Verify deterministic mapping from score to IncidentSeverity
func TestIncidentEngine_SeverityMapping(t *testing.T) {
	cases := []struct {
		score    float64
		expected types.IncidentSeverity
	}{
		{0.95, types.SeverityCritical},
		{0.85, types.SeverityCritical},
		{0.84, types.SeverityHigh},
		{0.60, types.SeverityHigh},
		{0.59, types.SeverityMedium},
		{0.30, types.SeverityMedium},
		{0.29, types.SeverityLow},
		{0.00, types.SeverityLow},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("Score_%.2f", tc.score), func(t *testing.T) {
			got := incident.MapSeverity(tc.score)
			if got != tc.expected {
				t.Errorf("MapSeverity(%.2f) = %s, want %s", tc.score, got, tc.expected)
			}
		})
	}
}

// TEST 12 — Process Restart: Verify process-local state behavior on restart
func TestIncidentEngine_ProcessRestart(t *testing.T) {
	// First instance before restart
	eng1, err := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              3,
		WindowDuration: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create eng1: %v", err)
	}

	ctx := context.Background()
	now := time.Now().UTC()

	// Send 1 anomaly before restart
	sig1 := makeSignal("anom-rst-1", "node-1", "cpu_usage_percent", 95.0, 90.0, 0.5, now)
	inc1, err := eng1.Process(ctx, sig1)
	if err != nil || inc1 != nil {
		t.Fatalf("unexpected state before restart: %v, inc: %v", err, inc1)
	}

	// Simulate restart: re-instantiate engine
	eng2, err := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              3,
		WindowDuration: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create eng2: %v", err)
	}

	// Send second anomaly to new instance
	sig2 := makeSignal("anom-rst-2", "node-1", "cpu_usage_percent", 96.0, 90.0, 0.6, now.Add(10*time.Second))
	inc2, err := eng2.Process(ctx, sig2)
	if err != nil {
		t.Fatalf("error processing sig2 after restart: %v", err)
	}
	// Because in-memory correlation state was reset, this is only 1-of-2 for eng2
	if inc2 != nil {
		t.Fatalf("expected nil incident after restart because in-memory state is process-local; got %+v", inc2)
	}
}

// TEST 13 — Out-of-Order Signals: Signals outside sliding observation window are rejected as stale
func TestIncidentEngine_OutOfOrderSignals(t *testing.T) {
	eng, err := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              3,
		WindowDuration: 1 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	ctx := context.Background()
	t0 := time.Now().UTC()

	// 1. Process recent signal at t0
	sigRecent := makeSignal("anom-recent", "node-1", "cpu_usage_percent", 95.0, 90.0, 0.5, t0)
	_, err = eng.Process(ctx, sigRecent)
	if err != nil {
		t.Fatalf("unexpected error on sigRecent: %v", err)
	}

	// 2. Process stale signal detected 10 minutes prior to t0 (well outside 1m window)
	sigStale := makeSignal("anom-stale", "node-1", "cpu_usage_percent", 95.0, 90.0, 0.5, t0.Add(-10*time.Minute))
	_, err = eng.Process(ctx, sigStale)
	if err == nil {
		t.Fatal("expected error for signal predating observation window, got nil")
	}
	if !errors.Is(err, incident.ErrStaleAnomalySignal) {
		t.Errorf("expected ErrStaleAnomalySignal, got: %v", err)
	}
}

// TEST 14 — Multiple Independent Incidents: Node A CPU, Node A Memory, Node B CPU can maintain independent incidents
func TestIncidentEngine_MultipleIndependentIncidents(t *testing.T) {
	eng, err := incident.NewLocalEngine(incident.PolicyConfig{
		M:              1,
		N:              1,
		WindowDuration: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	ctx := context.Background()
	now := time.Now().UTC()

	sigA_CPU := makeSignal("anom-A-cpu", "node-A", "cpu_usage_percent", 95.0, 90.0, 0.5, now)
	sigA_Mem := makeSignal("anom-A-mem", "node-A", "memory_usage_percent", 92.0, 90.0, 0.4, now)
	sigB_CPU := makeSignal("anom-B-cpu", "node-B", "cpu_usage_percent", 98.0, 90.0, 0.8, now)

	inc1, err := eng.Process(ctx, sigA_CPU)
	if err != nil || inc1 == nil {
		t.Fatalf("failed to create incident 1: %v", err)
	}

	inc2, err := eng.Process(ctx, sigA_Mem)
	if err != nil || inc2 == nil {
		t.Fatalf("failed to create incident 2: %v", err)
	}

	inc3, err := eng.Process(ctx, sigB_CPU)
	if err != nil || inc3 == nil {
		t.Fatalf("failed to create incident 3: %v", err)
	}

	// Verify all three incidents are distinct
	ids := map[string]bool{
		inc1.IncidentID: true,
		inc2.IncidentID: true,
		inc3.IncidentID: true,
	}
	if len(ids) != 3 {
		t.Fatalf("expected 3 distinct IncidentIDs, got: %v", ids)
	}

	if eng.GetActiveIncident("node-A", "cpu_usage_percent") == nil {
		t.Error("missing active incident for node-A cpu")
	}
	if eng.GetActiveIncident("node-A", "memory_usage_percent") == nil {
		t.Error("missing active incident for node-A memory")
	}
	if eng.GetActiveIncident("node-B", "cpu_usage_percent") == nil {
		t.Error("missing active incident for node-B cpu")
	}
}

// TEST 15 — Concurrent Processing: Process anomaly signals concurrently without race conditions
func TestIncidentEngine_ConcurrentProcessing(t *testing.T) {
	eng, err := incident.NewLocalEngine(incident.PolicyConfig{
		M:              2,
		N:              5,
		WindowDuration: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	ctx := context.Background()
	now := time.Now().UTC()
	const workers = 40
	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func(workerID int) {
			defer wg.Done()
			node := fmt.Sprintf("node-%d", workerID%4)
			metric := "cpu_usage_percent"
			if workerID%2 == 1 {
				metric = "memory_usage_percent"
			}
			anomalyID := fmt.Sprintf("anom-worker-%d", workerID)
			sig := makeSignal(anomalyID, node, metric, 95.0, 90.0, 0.6, now.Add(time.Duration(workerID)*time.Millisecond))
			_, _ = eng.Process(ctx, sig)
		}(i)
	}

	wg.Wait()

	// Verify engine internal state remains consistent
	for n := 0; n < 4; n++ {
		node := fmt.Sprintf("node-%d", n)
		_ = eng.GetActiveIncident(node, "cpu_usage_percent")
		_ = eng.GetActiveIncident(node, "memory_usage_percent")
	}
}

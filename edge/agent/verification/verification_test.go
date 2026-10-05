package verification_test

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/detector"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/incident"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/verification"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
	_ "modernc.org/sqlite"
)

func makeTestRequest(nodeID, metricName string, status types.MitigationStatus, startedAt time.Time) verification.VerificationRequest {
	return verification.VerificationRequest{
		IncidentID:           "inc-test-01",
		ActionID:             "act-test-01",
		DecisionID:           "dec-test-01",
		NodeID:               nodeID,
		MitigationStatus:     status,
		Condition:            verification.NewThresholdUpperCondition(metricName, 70.0),
		StartedAt:            startedAt,
		Timeout:              5 * time.Minute,
		RequiredObservations: 3,
	}
}

// TEST 1 & 2: Verification creation and execution boundary
func TestVerification_CreationAndExecutionBoundary(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	clock := verification.NewFakeClock(now)
	cfg := verification.DefaultConfig()
	cfg.Clock = clock
	eng, err := verification.NewLocalEngine(cfg)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	ctx := context.Background()

	// Eligible: EXECUTED
	validReq := makeTestRequest("node-01", "cpu_usage_percent", types.MitigationStatusExecuted, now)
	res, err := eng.StartVerification(ctx, validReq)
	if err != nil {
		t.Fatalf("StartVerification failed for EXECUTED: %v", err)
	}
	if res.Status != types.VerificationStatusPending {
		t.Fatalf("expected status PENDING, got %s", res.Status)
	}
	if res.RequiredObservations != 3 {
		t.Fatalf("expected required observations 3, got %d", res.RequiredObservations)
	}

	// Ineligible: FAILED mitigation
	failedReq := makeTestRequest("node-02", "cpu_usage_percent", types.MitigationStatusFailed, now)
	_, err = eng.StartVerification(ctx, failedReq)
	if !errors.Is(err, verification.ErrInvalidMitigationStatus) {
		t.Fatalf("expected ErrInvalidMitigationStatus for FAILED, got %v", err)
	}

	// Ineligible: SKIPPED mitigation
	skippedReq := makeTestRequest("node-03", "cpu_usage_percent", types.MitigationStatusSkipped, now)
	_, err = eng.StartVerification(ctx, skippedReq)
	if !errors.Is(err, verification.ErrInvalidMitigationStatus) {
		t.Fatalf("expected ErrInvalidMitigationStatus for SKIPPED, got %v", err)
	}

	// Ineligible: UNKNOWN_RECONCILIATION_REQUIRED
	unkReq := makeTestRequest("node-04", "cpu_usage_percent", types.MitigationStatusUnknownReconciliationRequired, now)
	_, err = eng.StartVerification(ctx, unkReq)
	if !errors.Is(err, verification.ErrInvalidMitigationStatus) {
		t.Fatalf("expected ErrInvalidMitigationStatus for UNKNOWN_RECONCILIATION_REQUIRED, got %v", err)
	}

	// Ineligible: PENDING mitigation
	pendReq := makeTestRequest("node-05", "cpu_usage_percent", types.MitigationStatusPending, now)
	_, err = eng.StartVerification(ctx, pendReq)
	if !errors.Is(err, verification.ErrInvalidMitigationStatus) {
		t.Fatalf("expected ErrInvalidMitigationStatus for PENDING mitigation, got %v", err)
	}
}

// TEST 3, 4, 5, 6: Consecutive observation requirement, healthy streak, recovery confirmation, and unhealthy streak reset
func TestVerification_ConsecutiveObservationsAndStreakReset(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	clock := verification.NewFakeClock(now)
	cfg := verification.DefaultConfig()
	cfg.Clock = clock
	eng, _ := verification.NewLocalEngine(cfg)

	ctx := context.Background()
	req := makeTestRequest("node-01", "cpu_usage_percent", types.MitigationStatusExecuted, now)
	req.RequiredObservations = 3
	_, err := eng.StartVerification(ctx, req)
	if err != nil {
		t.Fatalf("StartVerification failed: %v", err)
	}

	// Observation 1: Healthy (65.0 <= 70.0) -> consecutive 1, still PENDING
	t1 := now.Add(10 * time.Second)
	s1 := types.MetricSample{NodeID: "node-01", Name: "cpu_usage_percent", Value: 65.0, Timestamp: t1}
	r1, err := eng.ProcessSample(ctx, s1)
	if err != nil || r1 == nil {
		t.Fatalf("ProcessSample 1 failed: %v", err)
	}
	if r1.Status != types.VerificationStatusPending || r1.ConsecutiveHealthy != 1 || r1.Observations != 1 {
		t.Fatalf("unexpected state after obs 1: %+v", r1)
	}

	// Observation 2: Healthy (62.0 <= 70.0) -> consecutive 2, still PENDING
	t2 := now.Add(20 * time.Second)
	s2 := types.MetricSample{NodeID: "node-01", Name: "cpu_usage_percent", Value: 62.0, Timestamp: t2}
	r2, err := eng.ProcessSample(ctx, s2)
	if err != nil || r2 == nil {
		t.Fatalf("ProcessSample 2 failed: %v", err)
	}
	if r2.Status != types.VerificationStatusPending || r2.ConsecutiveHealthy != 2 || r2.Observations != 2 {
		t.Fatalf("unexpected state after obs 2: %+v", r2)
	}

	// Observation 3: Unhealthy (75.0 > 70.0) -> streak resets to 0! Total observations = 3, still PENDING
	t3 := now.Add(30 * time.Second)
	s3 := types.MetricSample{NodeID: "node-01", Name: "cpu_usage_percent", Value: 75.0, Timestamp: t3}
	r3, err := eng.ProcessSample(ctx, s3)
	if err != nil || r3 == nil {
		t.Fatalf("ProcessSample 3 failed: %v", err)
	}
	if r3.Status != types.VerificationStatusPending || r3.ConsecutiveHealthy != 0 || r3.Observations != 3 {
		t.Fatalf("expected consecutive healthy 0 after unhealthy observation, got %+v", r3)
	}

	// Restart consecutive run: 3 consecutive healthy observations required
	// Obs 4: Healthy (68.0) -> consecutive 1
	t4 := now.Add(40 * time.Second)
	r4, _ := eng.ProcessSample(ctx, types.MetricSample{NodeID: "node-01", Name: "cpu_usage_percent", Value: 68.0, Timestamp: t4})
	if r4.ConsecutiveHealthy != 1 || r4.Status != types.VerificationStatusPending {
		t.Fatalf("obs 4 mismatch: %+v", r4)
	}

	// Obs 5: Healthy (64.0) -> consecutive 2
	t5 := now.Add(50 * time.Second)
	r5, _ := eng.ProcessSample(ctx, types.MetricSample{NodeID: "node-01", Name: "cpu_usage_percent", Value: 64.0, Timestamp: t5})
	if r5.ConsecutiveHealthy != 2 || r5.Status != types.VerificationStatusPending {
		t.Fatalf("obs 5 mismatch: %+v", r5)
	}

	// Obs 6: Healthy (60.0) -> consecutive 3 == RequiredObservations -> RECOVERED!
	t6 := now.Add(60 * time.Second)
	r6, err := eng.ProcessSample(ctx, types.MetricSample{NodeID: "node-01", Name: "cpu_usage_percent", Value: 60.0, Timestamp: t6})
	if err != nil || r6 == nil {
		t.Fatalf("obs 6 failed: %v", err)
	}
	if r6.Status != types.VerificationStatusRecovered {
		t.Fatalf("expected RECOVERED status, got %s", r6.Status)
	}
	if r6.RecoveredAt == nil || !r6.RecoveredAt.Equal(t6) {
		t.Fatalf("expected RecoveredAt %s, got %v", t6, r6.RecoveredAt)
	}
	if r6.CompletedAt == nil || !r6.CompletedAt.Equal(t6) {
		t.Fatalf("expected CompletedAt %s, got %v", t6, r6.CompletedAt)
	}
	if r6.Evidence["healthy_observations"] != "3" || r6.Evidence["last_value"] != "60.0000" {
		t.Fatalf("unexpected evidence: %+v", r6.Evidence)
	}

	// Once terminal (RECOVERED), further samples on this stream return (nil, nil) since active stream was cleared
	s7 := types.MetricSample{NodeID: "node-01", Name: "cpu_usage_percent", Value: 55.0, Timestamp: now.Add(70 * time.Second)}
	r7, err := eng.ProcessSample(ctx, s7)
	if err != nil || r7 != nil {
		t.Fatalf("expected (nil, nil) for post-terminal sample, got %+v (err: %v)", r7, err)
	}
}

// TEST 7: Timeout observation window
func TestVerification_Timeout(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	clock := verification.NewFakeClock(now)
	cfg := verification.DefaultConfig()
	cfg.Clock = clock
	eng, _ := verification.NewLocalEngine(cfg)

	ctx := context.Background()
	req := makeTestRequest("node-01", "cpu_usage_percent", types.MitigationStatusExecuted, now)
	req.Timeout = 2 * time.Minute
	_, _ = eng.StartVerification(ctx, req)

	// Sample right at timeout deadline (now + 2m)
	timeoutTime := now.Add(2 * time.Minute)
	sample := types.MetricSample{NodeID: "node-01", Name: "cpu_usage_percent", Value: 65.0, Timestamp: timeoutTime}
	res, err := eng.ProcessSample(ctx, sample)
	if err != nil {
		t.Fatalf("ProcessSample timeout failed: %v", err)
	}
	if res.Status != types.VerificationStatusTimedOut {
		t.Fatalf("expected TIMED_OUT status, got %s", res.Status)
	}

	// ReconcileTimeouts
	req2 := makeTestRequest("node-02", "cpu_usage_percent", types.MitigationStatusExecuted, now)
	req2.Timeout = 1 * time.Minute
	_, _ = eng.StartVerification(ctx, req2)

	// Advance clock past timeout
	clock.Advance(90 * time.Second)
	reconciled, err := eng.ReconcileTimeouts(ctx, clock.Now())
	if err != nil || reconciled != 1 {
		t.Fatalf("expected 1 reconciled timeout, got %d (err: %v)", reconciled, err)
	}
	got2, _ := eng.GetVerification(ctx, req2.ActionID)
	if got2 != nil && got2.Status != types.VerificationStatusTimedOut {
		t.Fatalf("expected TIMED_OUT for req2, got %s", got2.Status)
	}
}

// TEST 8: Old / Stale telemetry predating mitigation execution boundary
func TestVerification_StaleTelemetryRejected(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	clock := verification.NewFakeClock(now)
	cfg := verification.DefaultConfig()
	cfg.Clock = clock
	eng, _ := verification.NewLocalEngine(cfg)

	ctx := context.Background()
	req := makeTestRequest("node-01", "cpu_usage_percent", types.MitigationStatusExecuted, now)
	_, _ = eng.StartVerification(ctx, req)

	// Telemetry sample predating startedAt by 1 millisecond
	staleSample := types.MetricSample{
		NodeID:    "node-01",
		Name:      "cpu_usage_percent",
		Value:     60.0,
		Timestamp: now.Add(-time.Millisecond),
	}
	_, err := eng.ProcessSample(ctx, staleSample)
	if !errors.Is(err, verification.ErrStaleTelemetrySample) {
		t.Fatalf("expected ErrStaleTelemetrySample, got %v", err)
	}
}

// TEST 9 & 10: Duplicate telemetry suppression (idempotency based on SampleID)
func TestVerification_DuplicateTelemetrySuppression(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	clock := verification.NewFakeClock(now)
	cfg := verification.DefaultConfig()
	cfg.Clock = clock
	eng, _ := verification.NewLocalEngine(cfg)

	ctx := context.Background()
	req := makeTestRequest("node-01", "cpu_usage_percent", types.MitigationStatusExecuted, now)
	req.RequiredObservations = 10
	_, _ = eng.StartVerification(ctx, req)

	ts := now.Add(10 * time.Second)

	// Sample 1 with SampleID "samp-101"
	sample1 := types.MetricSample{
		SampleID:  "samp-101",
		NodeID:    "node-01",
		Name:      "cpu_usage_percent",
		Value:     65.0,
		Timestamp: ts,
	}

	// First evaluation succeeds
	res1, err := eng.ProcessSample(ctx, sample1)
	if err != nil || res1.ConsecutiveHealthy != 1 {
		t.Fatalf("first sample failed: %v", err)
	}

	// 1. Same SampleID twice -> duplicate (fails closed)
	_, err = eng.ProcessSample(ctx, sample1)
	if !errors.Is(err, verification.ErrDuplicateTelemetrySample) {
		t.Fatalf("expected ErrDuplicateTelemetrySample for identical SampleID, got %v", err)
	}

	// 2. Different SampleID, SAME timestamp -> NOT duplicate (evaluates successfully)
	sample2 := types.MetricSample{
		SampleID:  "samp-102",
		NodeID:    "node-01",
		Name:      "cpu_usage_percent",
		Value:     62.0,
		Timestamp: ts, // identical timestamp to sample1
	}
	res2, err := eng.ProcessSample(ctx, sample2)
	if err != nil {
		t.Fatalf("different SampleID with same timestamp must NOT be rejected as duplicate, got err: %v", err)
	}
	if res2.ConsecutiveHealthy != 2 {
		t.Fatalf("expected consecutive healthy 2, got %d", res2.ConsecutiveHealthy)
	}

	// 3. Different SampleID, SAME value -> NOT duplicate (evaluates successfully)
	sample3 := types.MetricSample{
		SampleID:  "samp-103",
		NodeID:    "node-01",
		Name:      "cpu_usage_percent",
		Value:     65.0, // identical value to sample1
		Timestamp: now.Add(20 * time.Second),
	}
	res3, err := eng.ProcessSample(ctx, sample3)
	if err != nil {
		t.Fatalf("different SampleID with same value must NOT be rejected as duplicate, got err: %v", err)
	}
	if res3.ConsecutiveHealthy != 3 {
		t.Fatalf("expected consecutive healthy 3, got %d", res3.ConsecutiveHealthy)
	}
}

// TEST 11, 12, 13, 14: Node, Metric, Incident, and Action/Decision Isolation
func TestVerification_IsolationMatrix(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	clock := verification.NewFakeClock(now)
	cfg := verification.DefaultConfig()
	cfg.Clock = clock
	eng, _ := verification.NewLocalEngine(cfg)

	ctx := context.Background()

	// Verification 1: Node A, cpu_usage_percent
	reqA := makeTestRequest("node-A", "cpu_usage_percent", types.MitigationStatusExecuted, now)
	reqA.IncidentID = "inc-A"
	reqA.ActionID = "act-A"
	vA, _ := eng.StartVerification(ctx, reqA)

	// Verification 2: Node B, cpu_usage_percent
	reqB := makeTestRequest("node-B", "cpu_usage_percent", types.MitigationStatusExecuted, now)
	reqB.IncidentID = "inc-B"
	reqB.ActionID = "act-B"
	vB, _ := eng.StartVerification(ctx, reqB)

	// Verification 3: Node A, memory_usage_percent
	reqM := makeTestRequest("node-A", "memory_usage_percent", types.MitigationStatusExecuted, now)
	reqM.IncidentID = "inc-M"
	reqM.ActionID = "act-M"
	vM, _ := eng.StartVerification(ctx, reqM)

	// Sample for Node A CPU
	sampleACPU := types.MetricSample{NodeID: "node-A", Name: "cpu_usage_percent", Value: 60.0, Timestamp: now.Add(time.Second)}
	resACPU, _ := eng.ProcessSample(ctx, sampleACPU)
	if resACPU.VerificationID != vA.VerificationID || resACPU.ConsecutiveHealthy != 1 {
		t.Fatalf("Node A CPU sample misdirected: %+v", resACPU)
	}

	// Verify Node B CPU was untouched
	actB := eng.GetActiveVerification("node-B", "cpu_usage_percent")
	if actB.ConsecutiveHealthy != 0 || actB.VerificationID != vB.VerificationID {
		t.Fatalf("Node B CPU affected by Node A sample: %+v", actB)
	}

	// Verify Node A Memory was untouched
	actM := eng.GetActiveVerification("node-A", "memory_usage_percent")
	if actM.ConsecutiveHealthy != 0 || actM.VerificationID != vM.VerificationID {
		t.Fatalf("Node A Memory affected by Node A CPU sample: %+v", actM)
	}
}

// TEST 18, 19, 20: SQLite persistence, close/reopen, and restart recovery
func TestVerification_WithSQLiteStore_PersistenceAndReboot(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "verification_reboot.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}

	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	clock := verification.NewFakeClock(now)
	cfg := verification.DefaultConfig()
	cfg.Store = store
	cfg.Clock = clock
	eng1, _ := verification.NewLocalEngine(cfg)

	ctx := context.Background()

	// 1. Start verification on eng1 at T0 with 10m timeout (deadline T0 + 10m)
	req1 := makeTestRequest("node-01", "cpu_usage_percent", types.MitigationStatusExecuted, now)
	req1.Timeout = 10 * time.Minute
	res1, err := eng1.StartVerification(ctx, req1)
	if err != nil {
		t.Fatalf("StartVerification failed: %v", err)
	}

	// Also start a second verification on eng1 with short 2m timeout (deadline T0 + 2m)
	req2 := makeTestRequest("node-02", "cpu_usage_percent", types.MitigationStatusExecuted, now)
	req2.ActionID = "act-test-02"
	req2.DecisionID = "dec-test-02"
	req2.Timeout = 2 * time.Minute
	res2, err := eng1.StartVerification(ctx, req2)
	if err != nil {
		t.Fatalf("StartVerification 2 failed: %v", err)
	}

	// 2. Submit one healthy sample for node-01 with explicit SampleID
	s1 := types.MetricSample{
		SampleID:  "sample-reboot-1",
		NodeID:    "node-01",
		Name:      "cpu_usage_percent",
		Value:     65.0,
		Timestamp: now.Add(time.Second),
	}
	_, _ = eng1.ProcessSample(ctx, s1)

	// 3. Advance clock by 5 minutes (past req2 deadline T0+2m, but before req1 deadline T0+10m)
	clock.Advance(5 * time.Minute)

	// 4. Close store (simulating agent shutdown/crash)
	_ = store.Close()

	// 5. Reopen store (simulating agent restart)
	store2, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("reopening store failed: %v", err)
	}
	defer store2.Close()

	cfg2 := verification.DefaultConfig()
	cfg2.Store = store2
	cfg2.Clock = clock
	eng2, _ := verification.NewLocalEngine(cfg2)

	// 6. Recover from store: req1 (before deadline) restored as PENDING; req2 (past deadline) transitioned to TIMED_OUT!
	restored, timedOut, err := eng2.RecoverFromStore(ctx)
	if err != nil {
		t.Fatalf("RecoverFromStore failed: %v", err)
	}
	if restored != 1 {
		t.Fatalf("expected 1 restored verification (before deadline), got %d", restored)
	}
	if timedOut != 1 {
		t.Fatalf("expected 1 timed out verification (after deadline), got %d", timedOut)
	}

	// Verify req2 was reconciled to TIMED_OUT
	gotTimedOut, err := eng2.GetVerification(ctx, res2.VerificationID)
	if err != nil || gotTimedOut.Status != types.VerificationStatusTimedOut {
		t.Fatalf("expected req2 to be TIMED_OUT across restart: got %+v (err: %v)", gotTimedOut, err)
	}

	// 7. Verify restored req1 state matches
	active := eng2.GetActiveVerification("node-01", "cpu_usage_percent")
	if active == nil || active.VerificationID != res1.VerificationID {
		t.Fatalf("expected active verification to be restored, got %+v", active)
	}
	if active.Status != types.VerificationStatusPending {
		t.Fatalf("expected req1 to remain PENDING before deadline, got %s", active.Status)
	}
	if active.ConsecutiveHealthy != 1 || active.Observations != 1 {
		t.Fatalf("restored progress fields mismatch: %+v", active)
	}

	// 8. Test Duplicate Suppression Across Restart: Re-submitting "sample-reboot-1" must fail closed with ErrDuplicateTelemetrySample
	_, err = eng2.ProcessSample(ctx, s1)
	if !errors.Is(err, verification.ErrDuplicateTelemetrySample) {
		t.Fatalf("expected ErrDuplicateTelemetrySample for sample-reboot-1 after restart, got %v", err)
	}

	// 9. Complete recovery on eng2 with new samples
	s2 := types.MetricSample{SampleID: "sample-reboot-2", NodeID: "node-01", Name: "cpu_usage_percent", Value: 64.0, Timestamp: now.Add(6 * time.Minute)}
	s3 := types.MetricSample{SampleID: "sample-reboot-3", NodeID: "node-01", Name: "cpu_usage_percent", Value: 63.0, Timestamp: now.Add(7 * time.Minute)}
	_, _ = eng2.ProcessSample(ctx, s2)
	finalRes, err := eng2.ProcessSample(ctx, s3)
	if err != nil || finalRes.Status != types.VerificationStatusRecovered {
		t.Fatalf("expected RECOVERED status on eng2: %+v (err: %v)", finalRes, err)
	}
}

// TEST 21 & 22: Concurrent telemetry and concurrent duplicate race
func TestVerification_ConcurrentTelemetryAndDuplicateRace(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	clock := verification.NewFakeClock(now)
	cfg := verification.DefaultConfig()
	cfg.Clock = clock
	eng, _ := verification.NewLocalEngine(cfg)

	ctx := context.Background()
	req := makeTestRequest("node-01", "cpu_usage_percent", types.MitigationStatusExecuted, now)
	req.RequiredObservations = 100
	_, _ = eng.StartVerification(ctx, req)

	// 50 concurrent goroutines racing with the EXACT same sample
	const concurrency = 50
	var wg sync.WaitGroup
	var successCount int32
	var duplicateCount int32

	identicalSample := types.MetricSample{
		NodeID:    "node-01",
		Name:      "cpu_usage_percent",
		Value:     60.0,
		Timestamp: now.Add(time.Second),
	}

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := eng.ProcessSample(ctx, identicalSample)
			if err == nil {
				atomic.AddInt32(&successCount, 1)
			} else if errors.Is(err, verification.ErrDuplicateTelemetrySample) {
				atomic.AddInt32(&duplicateCount, 1)
			}
		}()
	}
	wg.Wait()

	if successCount != 1 {
		t.Fatalf("expected EXACTLY 1 successful sample evaluation, got %d (duplicates: %d)", successCount, duplicateCount)
	}
	if duplicateCount != concurrency-1 {
		t.Fatalf("expected %d duplicate rejections, got %d", concurrency-1, duplicateCount)
	}
}

// TEST 25 & 26: Safe handoff to IncidentEngine obeying canonical Incident FSM
func TestVerification_HandoffToIncidentEngine(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	clock := verification.NewFakeClock(now)
	cfg := verification.DefaultConfig()
	cfg.Clock = clock
	vEng, _ := verification.NewLocalEngine(cfg)

	ctx := context.Background()

	// Scenario A: Incident is in ANOMALY_DETECTED
	// Canonical FSM does NOT allow direct transition to RECOVERED from ANOMALY_DETECTED.
	// As per Section M: HandoffToIncidentEngine must leave incident unchanged and return without error.
	incEngA, _ := incident.NewLocalEngine(incident.DefaultPolicyConfig())
	rawIncA := &types.Incident{
		IncidentID:    "inc-test-A",
		NodeID:        "node-01",
		RuleName:      "threshold_cpu_usage_percent",
		Severity:      types.SeverityHigh,
		Status:        types.StatusAnomalyDetected,
		Description:   "CPU high",
		TriggerMetric: "cpu_usage_percent",
		TriggerValue:  95.0,
		Threshold:     80.0,
		TriggeredAt:   now,
		UpdatedAt:     now,
	}
	incA, err := incEngA.RegisterIncident(ctx, rawIncA)
	if err != nil || incA == nil || incA.Status != types.StatusAnomalyDetected {
		t.Fatalf("expected active incident in ANOMALY_DETECTED: %+v (err: %v)", incA, err)
	}

	reqA := makeTestRequest("node-01", "cpu_usage_percent", types.MitigationStatusExecuted, now)
	reqA.IncidentID = incA.IncidentID
	reqA.RequiredObservations = 1
	resA, _ := vEng.StartVerification(ctx, reqA)

	// Recover verification
	_, _ = vEng.ProcessSample(ctx, types.MetricSample{NodeID: "node-01", Name: "cpu_usage_percent", Value: 60.0, Timestamp: now.Add(2 * time.Second)})

	// Handoff on ANOMALY_DETECTED incident: leaves status unchanged (safe non-forgery)
	returnedIncA, err := vEng.HandoffToIncidentEngine(ctx, resA.VerificationID, incEngA)
	if err != nil {
		t.Fatalf("HandoffToIncidentEngine on ANOMALY_DETECTED returned error: %v", err)
	}
	if returnedIncA.Status != types.StatusAnomalyDetected {
		t.Fatalf("expected incident status to remain ANOMALY_DETECTED, got %s", returnedIncA.Status)
	}

	// Scenario B: Incident is in MITIGATING
	// Canonical FSM permits MITIGATING -> RECOVERED.
	incEngB, _ := incident.NewLocalEngine(incident.DefaultPolicyConfig())
	rawIncB := &types.Incident{
		IncidentID:    "inc-test-B",
		NodeID:        "node-01",
		RuleName:      "threshold_cpu_usage_percent",
		Severity:      types.SeverityHigh,
		Status:        types.StatusAnomalyDetected,
		Description:   "CPU high",
		TriggerMetric: "cpu_usage_percent",
		TriggerValue:  95.0,
		Threshold:     80.0,
		TriggeredAt:   now,
		UpdatedAt:     now,
	}
	_, err = incEngB.RegisterIncident(ctx, rawIncB)
	if err != nil {
		t.Fatalf("RegisterIncident B failed: %v", err)
	}
	// Transition to MITIGATING
	mitigatingInc, err := incEngB.TransitionActiveIncident(ctx, "node-01", "cpu_usage_percent", types.StatusMitigating, now.Add(time.Second))
	if err != nil || mitigatingInc.Status != types.StatusMitigating {
		t.Fatalf("expected MITIGATING incident, got %+v (err: %v)", mitigatingInc, err)
	}

	reqB := makeTestRequest("node-01", "cpu_usage_percent", types.MitigationStatusExecuted, now)
	reqB.IncidentID = mitigatingInc.IncidentID
	reqB.RequiredObservations = 1
	resB, _ := vEng.StartVerification(ctx, reqB)
	_, _ = vEng.ProcessSample(ctx, types.MetricSample{NodeID: "node-01", Name: "cpu_usage_percent", Value: 60.0, Timestamp: now.Add(3 * time.Second)})

	// Handoff on MITIGATING incident: successfully executes canonical transition to RECOVERED!
	recoveredIncB, err := vEng.HandoffToIncidentEngine(ctx, resB.VerificationID, incEngB)
	if err != nil {
		t.Fatalf("HandoffToIncidentEngine on MITIGATING incident failed: %v", err)
	}
	if recoveredIncB.Status != types.StatusRecovered {
		t.Fatalf("expected incident status RECOVERED, got %s", recoveredIncB.Status)
	}

	// Scenario C: Incident is in ESCALATED
	// Contract: verification only resolves incident if incident is currently in MITIGATING.
	// For ESCALATED, HandoffToIncidentEngine leaves status unchanged.
	incEngC, _ := incident.NewLocalEngine(incident.DefaultPolicyConfig())
	rawIncC := &types.Incident{
		IncidentID:    "inc-test-C",
		NodeID:        "node-01",
		RuleName:      "threshold_cpu_usage_percent",
		Severity:      types.SeverityHigh,
		Status:        types.StatusAnomalyDetected,
		Description:   "CPU high",
		TriggerMetric: "cpu_usage_percent",
		TriggerValue:  95.0,
		Threshold:     80.0,
		TriggeredAt:   now,
		UpdatedAt:     now,
	}
	_, _ = incEngC.RegisterIncident(ctx, rawIncC)
	escalatedInc, err := incEngC.TransitionActiveIncident(ctx, "node-01", "cpu_usage_percent", types.StatusEscalated, now.Add(time.Second))
	if err != nil || escalatedInc.Status != types.StatusEscalated {
		t.Fatalf("expected ESCALATED incident, got %+v (err: %v)", escalatedInc, err)
	}

	reqC := makeTestRequest("node-01", "cpu_usage_percent", types.MitigationStatusExecuted, now)
	reqC.IncidentID = escalatedInc.IncidentID
	reqC.RequiredObservations = 1
	resC, _ := vEng.StartVerification(ctx, reqC)
	_, _ = vEng.ProcessSample(ctx, types.MetricSample{NodeID: "node-01", Name: "cpu_usage_percent", Value: 60.0, Timestamp: now.Add(4 * time.Second)})

	returnedIncC, err := vEng.HandoffToIncidentEngine(ctx, resC.VerificationID, incEngC)
	if err != nil {
		t.Fatalf("HandoffToIncidentEngine on ESCALATED returned error: %v", err)
	}
	if returnedIncC.Status != types.StatusEscalated {
		t.Fatalf("expected incident status to remain ESCALATED, got %s", returnedIncC.Status)
	}

	// Scenario D: Incident is in NORMAL (or no active incident)
	// Contract: leaves incident unchanged / returns ErrNoActiveIncident
	incEngD, _ := incident.NewLocalEngine(incident.DefaultPolicyConfig())
	reqD := makeTestRequest("node-01", "cpu_usage_percent", types.MitigationStatusExecuted, now)
	reqD.IncidentID = "inc-test-D-nonexistent"
	reqD.RequiredObservations = 1
	resD, _ := vEng.StartVerification(ctx, reqD)
	_, _ = vEng.ProcessSample(ctx, types.MetricSample{NodeID: "node-01", Name: "cpu_usage_percent", Value: 60.0, Timestamp: now.Add(5 * time.Second)})

	_, err = vEng.HandoffToIncidentEngine(ctx, resD.VerificationID, incEngD)
	if !errors.Is(err, incident.ErrNoActiveIncident) {
		t.Fatalf("expected ErrNoActiveIncident when incident is NORMAL/absent, got %v", err)
	}
}

// TEST 27: Reuse ThresholdDetector hysteresis semantics
func TestVerification_HysteresisSemantics(t *testing.T) {
	upperThreshold := 80.0
	upperRecovery := 70.0
	lowerThreshold := 20.0
	lowerRecovery := 25.0

	rule := &detector.ThresholdRule{
		MetricName:             "cpu_usage_percent",
		UpperThreshold:         &upperThreshold,
		UpperRecoveryThreshold: &upperRecovery,
		LowerThreshold:         &lowerThreshold,
		LowerRecoveryThreshold: &lowerRecovery,
	}

	// Upper breach: recovery condition must be <= 70.0 (NOT < 80.0)
	condUpper, err := verification.NewConditionFromThresholdRule(rule, true)
	if err != nil {
		t.Fatalf("NewConditionFromThresholdRule upper failed: %v", err)
	}
	if condUpper.RecoveryThreshold != 70.0 || condUpper.Comparator != "<=" {
		t.Fatalf("expected upper recovery condition <= 70.0, got %f %s", condUpper.RecoveryThreshold, condUpper.Comparator)
	}

	// Value = 75.0 (below 80 breach threshold, but above 70 recovery threshold) -> NOT healthy!
	healthy, _ := condUpper.Evaluate(75.0)
	if healthy {
		t.Fatalf("75.0 should not be healthy under hysteresis recovery threshold 70.0")
	}

	// Value = 69.9 -> healthy!
	healthy, _ = condUpper.Evaluate(69.9)
	if !healthy {
		t.Fatalf("69.9 should be healthy")
	}

	// Lower breach: recovery condition must be >= 25.0
	condLower, err := verification.NewConditionFromThresholdRule(rule, false)
	if err != nil {
		t.Fatalf("NewConditionFromThresholdRule lower failed: %v", err)
	}
	if condLower.RecoveryThreshold != 25.0 || condLower.Comparator != ">=" {
		t.Fatalf("expected lower recovery condition >= 25.0, got %f %s", condLower.RecoveryThreshold, condLower.Comparator)
	}

	// Value = 22.0 (above lower breach 20, but below recovery 25) -> NOT healthy!
	healthy, _ = condLower.Evaluate(22.0)
	if healthy {
		t.Fatalf("22.0 should not be healthy under hysteresis recovery threshold 25.0")
	}
	healthy, _ = condLower.Evaluate(26.0)
	if !healthy {
		t.Fatalf("26.0 should be healthy")
	}
}

// TEST: Failure Modes Matrix (A through T)
func TestVerification_FailureModesMatrix(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	clock := verification.NewFakeClock(now)
	cfg := verification.DefaultConfig()
	cfg.Clock = clock
	eng, _ := verification.NewLocalEngine(cfg)

	ctx := context.Background()

	// Mode A: Verification record missing
	_, err := eng.GetVerification(ctx, "nonexistent-id")
	if !errors.Is(err, verification.ErrVerificationNotFound) {
		t.Fatalf("Mode A: expected ErrVerificationNotFound, got %v", err)
	}

	// Mode L: Unknown metric
	res, err := eng.ProcessSample(ctx, types.MetricSample{NodeID: "node-01", Name: "unknown_metric", Value: 50.0, Timestamp: now.Add(time.Second)})
	if err != nil || res != nil {
		t.Fatalf("Mode L: expected (nil, nil) for unknown metric, got %+v (err: %v)", res, err)
	}

	// Mode M: Unsupported recovery condition
	badCond := verification.RecoveryCondition{
		Type:              "INVALID_TYPE",
		MetricName:        "cpu_usage_percent",
		RecoveryThreshold: 50.0,
		Comparator:        "<=",
	}
	_, err = badCond.Evaluate(40.0)
	if !errors.Is(err, verification.ErrUnsupportedRecoveryCondition) {
		t.Fatalf("Mode M: expected ErrUnsupportedRecoveryCondition, got %v", err)
	}

	// Mode Q: Node mismatch
	req := makeTestRequest("node-alpha", "cpu_usage_percent", types.MitigationStatusExecuted, now)
	_, _ = eng.StartVerification(ctx, req)
	resNodeMismatch, err := eng.ProcessSample(ctx, types.MetricSample{NodeID: "node-beta", Name: "cpu_usage_percent", Value: 60.0, Timestamp: now.Add(time.Second)})
	if err != nil || resNodeMismatch != nil {
		t.Fatalf("Mode Q: expected (nil, nil) when node does not match active verification, got %+v", resNodeMismatch)
	}

	// Mode T: Invalid configuration
	badCfg1 := verification.VerificationConfig{RequiredConsecutiveObservations: 0, VerificationTimeout: time.Minute}
	if err := badCfg1.Validate(); !errors.Is(err, verification.ErrInvalidRequiredObservations) {
		t.Fatalf("Mode T: expected ErrInvalidRequiredObservations, got %v", err)
	}
	badCfg2 := verification.VerificationConfig{RequiredConsecutiveObservations: 3, VerificationTimeout: -time.Minute}
	if err := badCfg2.Validate(); !errors.Is(err, verification.ErrInvalidVerificationTimeout) {
		t.Fatalf("Mode T: expected ErrInvalidVerificationTimeout, got %v", err)
	}

	// NaN and Inf metric values rejected
	nanSample := types.MetricSample{NodeID: "node-alpha", Name: "cpu_usage_percent", Value: math.NaN(), Timestamp: now.Add(time.Second)}
	_, err = eng.ProcessSample(ctx, nanSample)
	if !errors.Is(err, verification.ErrNaNMetricValue) {
		t.Fatalf("expected ErrNaNMetricValue for NaN, got %v", err)
	}

	infSample := types.MetricSample{NodeID: "node-alpha", Name: "cpu_usage_percent", Value: math.Inf(1), Timestamp: now.Add(time.Second)}
	_, err = eng.ProcessSample(ctx, infSample)
	if !errors.Is(err, verification.ErrNaNMetricValue) {
		t.Fatalf("expected ErrNaNMetricValue for Inf, got %v", err)
	}

	// FailVerification: Explicit terminal transition to NOT_RECOVERED
	failReq := makeTestRequest("node-fail", "cpu_usage_percent", types.MitigationStatusExecuted, now)
	resFail, _ := eng.StartVerification(ctx, failReq)
	err = eng.FailVerification(ctx, resFail.VerificationID, "supervisor rejected recovery")
	if err != nil {
		t.Fatalf("FailVerification failed: %v", err)
	}
	gotFail, _ := eng.GetVerification(ctx, resFail.VerificationID)
	if gotFail.Status != types.VerificationStatusNotRecovered {
		t.Fatalf("expected NOT_RECOVERED status, got %s", gotFail.Status)
	}
	// Active stream was deleted upon entering terminal state
	if eng.GetActiveVerification("node-fail", "cpu_usage_percent") != nil {
		t.Fatalf("terminal NOT_RECOVERED should not remain in active streams")
	}
}

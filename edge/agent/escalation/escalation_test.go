package escalation

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/incident"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

func setupTestEngine(t *testing.T, policy EscalationPolicy, clock Clock) (*LocalEngine, storage.EscalationStore) {
	t.Helper()
	dbDir := t.TempDir()
	dbPath := filepath.Join(dbDir, "test.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	engine, err := NewEngine(store, policy, clock)
	if err != nil {
		t.Fatalf("NewEngine failed: %v", err)
	}

	return engine, store
}

// ============================================================================
// Scenario Group 1: Engine Initialization & Validation
// ============================================================================

func TestEngine_InitializationValidation(t *testing.T) {
	policy := DefaultPolicy()
	clock := NewFakeClock(time.Now())

	// Nil store
	if _, err := NewEngine(nil, policy, clock); err != ErrNilStore {
		t.Errorf("expected ErrNilStore, got %v", err)
	}

	// Invalid policy
	invalidPolicy := policy
	invalidPolicy.MaxAutomaticRetries = -1
	if _, err := NewEngine(&storage.SQLiteStore{}, invalidPolicy, clock); err == nil {
		t.Errorf("expected error for invalid policy, got nil")
	}

	// Default clock fallback
	dbDir := t.TempDir()
	store, _ := storage.OpenSQLite(filepath.Join(dbDir, "clk.db"))
	defer store.Close()
	eng, err := NewEngine(store, policy, nil)
	if err != nil || eng.clock == nil {
		t.Errorf("expected RealClock fallback when nil clock provided")
	}
}

func TestEngine_RequestValidation(t *testing.T) {
	engine, _ := setupTestEngine(t, DefaultPolicy(), NewFakeClock(time.Now()))
	ctx := context.Background()

	// Empty IncidentID
	req := FailureEvaluationRequest{
		IncidentID:            "",
		NodeID:                "node-1",
		ActionID:              "act-1",
		FailureClassification: types.FailureMitigationFailed,
	}
	if _, err := engine.EvaluateFailure(ctx, req); err != ErrEmptyIncidentID {
		t.Errorf("expected ErrEmptyIncidentID, got %v", err)
	}

	// Empty NodeID
	req.IncidentID = "inc-1"
	req.NodeID = ""
	if _, err := engine.EvaluateFailure(ctx, req); err != ErrEmptyNodeID {
		t.Errorf("expected ErrEmptyNodeID, got %v", err)
	}

	// Empty ActionID
	req.NodeID = "node-1"
	req.ActionID = ""
	if _, err := engine.EvaluateFailure(ctx, req); err != ErrEmptyActionID {
		t.Errorf("expected ErrEmptyActionID, got %v", err)
	}

	// Invalid FailureClassification
	req.ActionID = "act-1"
	req.FailureClassification = types.FailureClassification("INVALID_CLASS")
	if _, err := engine.EvaluateFailure(ctx, req); err != types.ErrInvalidFailureClassification {
		t.Errorf("expected ErrInvalidFailureClassification, got %v", err)
	}
}

// ============================================================================
// Scenario Group 2: Deterministic Identity & Non-Randomness
// ============================================================================

func TestEngine_DeterministicIdentity(t *testing.T) {
	id1 := ComputeEscalationID("inc-1", "node-1", "act-1", types.FailureMitigationFailed, "v1.0.0", 1)
	id2 := ComputeEscalationID("inc-1", "node-1", "act-1", types.FailureMitigationFailed, "v1.0.0", 1)

	if id1 != id2 {
		t.Errorf("ComputeEscalationID must be strictly deterministic; got %s and %s", id1, id2)
	}

	// Different attempt
	id3 := ComputeEscalationID("inc-1", "node-1", "act-1", types.FailureMitigationFailed, "v1.0.0", 2)
	if id1 == id3 {
		t.Errorf("different attempt must produce different ID")
	}

	// Different action
	id4 := ComputeEscalationID("inc-1", "node-1", "act-2", types.FailureMitigationFailed, "v1.0.0", 1)
	if id1 == id4 {
		t.Errorf("different action must produce different ID")
	}

	// Different policy version
	id5 := ComputeEscalationID("inc-1", "node-1", "act-1", types.FailureMitigationFailed, "v2.0.0", 1)
	if id1 == id5 {
		t.Errorf("different policy version must produce different ID")
	}
}

// ============================================================================
// Scenario Group 3: Bounded Retries & Budget Exhaustion
// ============================================================================

func TestEngine_BoundedRetries_ThresholdTrips(t *testing.T) {
	policy := EscalationPolicy{
		MaxAutomaticRetries:            3,
		RetryCooldown:                  1 * time.Minute,
		FailureWindow:                  30 * time.Minute,
		CircuitBreakerFailureThreshold: 3,
		PolicyVersion:                  "v1.0.0",
	}

	startTime := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	clock := NewFakeClock(startTime)
	engine, store := setupTestEngine(t, policy, clock)
	ctx := context.Background()

	req := FailureEvaluationRequest{
		IncidentID:            "inc-bounded",
		NodeID:                "node-bounded",
		ActionID:              "act-bounded",
		FailureClassification: types.FailureMitigationFailed,
		Reason:                "simulated execution failed",
	}

	// Attempt 1: retry permitted
	d1, err := engine.EvaluateFailure(ctx, req)
	if err != nil {
		t.Fatalf("attempt 1 failed: %v", err)
	}
	if d1.Action != DecisionActionRetry || d1.Attempt != 1 {
		t.Fatalf("expected RETRY attempt 1, got %+v", d1)
	}
	if d1.CircuitState != types.CircuitClosed {
		t.Fatalf("circuit must remain CLOSED on attempt 1")
	}

	// Advance clock past cooldown
	clock.Advance(2 * time.Minute)

	// Attempt 2: retry permitted
	d2, err := engine.EvaluateFailure(ctx, req)
	if err != nil {
		t.Fatalf("attempt 2 failed: %v", err)
	}
	if d2.Action != DecisionActionRetry || d2.Attempt != 2 {
		t.Fatalf("expected RETRY attempt 2, got %+v", d2)
	}

	// Advance clock past cooldown
	clock.Advance(2 * time.Minute)

	// Attempt 3: reaches CircuitBreakerFailureThreshold (3) -> trips to OPEN
	d3, err := engine.EvaluateFailure(ctx, req)
	if err != nil {
		t.Fatalf("attempt 3 failed: %v", err)
	}
	if d3.Action != DecisionActionEscalate {
		t.Fatalf("expected ESCALATE on threshold reach, got %+v", d3)
	}
	if d3.CircuitState != types.CircuitOpen {
		t.Fatalf("expected circuit OPEN, got %s", d3.CircuitState)
	}
	if !d3.ShouldTransitionIncident {
		t.Fatalf("incident transition should be requested")
	}

	// Verify circuit breaker state persisted
	cb, err := store.GetCircuitState(ctx, "node-bounded", "inc-bounded")
	if err != nil || cb == nil {
		t.Fatalf("failed to retrieve circuit breaker state: %v", err)
	}
	if cb.State != types.CircuitOpen || cb.FailureCount != 3 {
		t.Fatalf("persisted circuit breaker mismatch: %+v", cb)
	}
}

// ============================================================================
// Scenario Group 4: Cooldown Enforcement & CanRetry
// ============================================================================

func TestEngine_CooldownEnforcement(t *testing.T) {
	policy := EscalationPolicy{
		MaxAutomaticRetries:            3,
		RetryCooldown:                  5 * time.Minute,
		FailureWindow:                  30 * time.Minute,
		CircuitBreakerFailureThreshold: 5,
		PolicyVersion:                  "v1.0.0",
	}

	startTime := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	clock := NewFakeClock(startTime)
	engine, _ := setupTestEngine(t, policy, clock)
	ctx := context.Background()

	// Initial check before any failures -> CanRetry is true
	canRetry, _, err := engine.CanRetry(ctx, "node-cd", "inc-cd")
	if err != nil || !canRetry {
		t.Fatalf("expected canRetry true before any failures, got %v", canRetry)
	}

	// Record a failure
	req := FailureEvaluationRequest{
		IncidentID:            "inc-cd",
		NodeID:                "node-cd",
		ActionID:              "act-cd",
		FailureClassification: types.FailureMitigationFailed,
	}
	_, err = engine.EvaluateFailure(ctx, req)
	if err != nil {
		t.Fatalf("EvaluateFailure failed: %v", err)
	}

	// Immediately after failure (0 minutes elapsed) -> CanRetry is false
	canRetry, nextRetry, err := engine.CanRetry(ctx, "node-cd", "inc-cd")
	if err != nil {
		t.Fatalf("CanRetry failed: %v", err)
	}
	if canRetry {
		t.Fatalf("CanRetry must be false during cooldown")
	}
	expectedNext := startTime.Add(5 * time.Minute)
	if nextRetry == nil || !nextRetry.Equal(expectedNext) {
		t.Fatalf("expected next retry %v, got %v", expectedNext, nextRetry)
	}

	// Advance clock by 4 minutes (still inside cooldown)
	clock.Advance(4 * time.Minute)
	canRetry, _, _ = engine.CanRetry(ctx, "node-cd", "inc-cd")
	if canRetry {
		t.Fatalf("CanRetry must remain false at minute 4")
	}

	// Advance clock by 2 minutes (6 minutes total, cooldown passed)
	clock.Advance(2 * time.Minute)
	canRetry, _, _ = engine.CanRetry(ctx, "node-cd", "inc-cd")
	if !canRetry {
		t.Fatalf("CanRetry must be true after cooldown expired")
	}
}

// ============================================================================
// Scenario Group 5: Circuit Breaker Lifecycle & Explicit Reset
// ============================================================================

func TestEngine_CircuitBreaker_ExplicitReset(t *testing.T) {
	policy := EscalationPolicy{
		MaxAutomaticRetries:            2,
		RetryCooldown:                  1 * time.Minute,
		FailureWindow:                  30 * time.Minute,
		CircuitBreakerFailureThreshold: 2,
		PolicyVersion:                  "v1.0.0",
	}

	startTime := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	clock := NewFakeClock(startTime)
	engine, store := setupTestEngine(t, policy, clock)
	ctx := context.Background()

	req := FailureEvaluationRequest{
		IncidentID:            "inc-cb",
		NodeID:                "node-cb",
		ActionID:              "act-cb",
		FailureClassification: types.FailureMitigationFailed,
	}

	// Trip circuit breaker
	engine.EvaluateFailure(ctx, req)
	clock.Advance(2 * time.Minute)
	d2, _ := engine.EvaluateFailure(ctx, req)
	if d2.CircuitState != types.CircuitOpen {
		t.Fatalf("expected circuit OPEN, got %s", d2.CircuitState)
	}

	// While circuit is OPEN, CanRetry must be false
	canRetry, _, _ := engine.CanRetry(ctx, "node-cb", "inc-cb")
	if canRetry {
		t.Fatalf("CanRetry must be false when circuit is OPEN")
	}

	// Subsequent evaluation immediately escalates with FailureCircuitBreakerOpen
	clock.Advance(10 * time.Minute)
	d3, _ := engine.EvaluateFailure(ctx, req)
	if d3.Action != DecisionActionEscalate || d3.Classification != types.FailureCircuitBreakerOpen {
		t.Fatalf("expected immediate escalation under OPEN breaker, got %+v", d3)
	}

	// Reset without operator identity rejected
	if err := engine.ResetCircuitBreaker(ctx, "node-cb", "inc-cb", "", "reason"); err != ErrEmptyResetBy {
		t.Fatalf("expected ErrEmptyResetBy, got %v", err)
	}

	// Explicit reset by operator
	clock.Advance(5 * time.Minute)
	if err := engine.ResetCircuitBreaker(ctx, "node-cb", "inc-cb", "operator-alice", "host recovered manually"); err != nil {
		t.Fatalf("ResetCircuitBreaker failed: %v", err)
	}

	// Verify circuit breaker is now CLOSED
	cbStatus, err := engine.GetCircuitBreakerStatus(ctx, "node-cb", "inc-cb")
	if err != nil {
		t.Fatalf("GetCircuitBreakerStatus failed: %v", err)
	}
	if cbStatus.State != types.CircuitClosed || cbStatus.FailureCount != 0 {
		t.Fatalf("circuit state after reset must be CLOSED with 0 failures, got %+v", cbStatus)
	}
	if cbStatus.ResetBy != "operator-alice" {
		t.Fatalf("expected reset_by operator-alice, got %s", cbStatus.ResetBy)
	}

	// Verify persistence in store
	cbStored, _ := store.GetCircuitState(ctx, "node-cb", "inc-cb")
	if cbStored.State != types.CircuitClosed {
		t.Fatalf("expected CLOSED in store, got %s", cbStored.State)
	}

	// CanRetry is now true
	canRetry, _, _ = engine.CanRetry(ctx, "node-cb", "inc-cb")
	if !canRetry {
		t.Fatalf("CanRetry must be true after circuit breaker reset")
	}
}

// ============================================================================
// Scenario Group 6: Failure Classifications & Immediate Escalation
// ============================================================================

func TestEngine_ImmediateEscalations(t *testing.T) {
	policy := DefaultPolicy()
	engine, _ := setupTestEngine(t, policy, NewFakeClock(time.Now()))
	ctx := context.Background()

	// 1. UNKNOWN_RECONCILIATION_REQUIRED -> immediate escalation
	req1 := FailureEvaluationRequest{
		IncidentID:            "inc-imm-1",
		NodeID:                "node-1",
		ActionID:              "act-1",
		FailureClassification: types.FailureUnknownReconciliationRequired,
		Reason:                "host actuator crashed mid-flight",
	}
	d1, err := engine.EvaluateFailure(ctx, req1)
	if err != nil {
		t.Fatalf("EvaluateFailure failed: %v", err)
	}
	if d1.Action != DecisionActionEscalate || d1.Classification != types.FailureUnknownReconciliationRequired {
		t.Fatalf("expected immediate escalation for UNKNOWN_RECONCILIATION_REQUIRED, got %+v", d1)
	}

	// 2. VERIFICATION_REJECTED -> immediate escalation
	req2 := FailureEvaluationRequest{
		IncidentID:            "inc-imm-2",
		NodeID:                "node-1",
		ActionID:              "act-2",
		FailureClassification: types.FailureVerificationRejected,
		Reason:                "remediation degraded metric further",
	}
	d2, err := engine.EvaluateFailure(ctx, req2)
	if err != nil {
		t.Fatalf("EvaluateFailure failed: %v", err)
	}
	if d2.Action != DecisionActionEscalate || d2.Classification != types.FailureVerificationRejected {
		t.Fatalf("expected immediate escalation for VERIFICATION_REJECTED, got %+v", d2)
	}

	// 3. VERIFICATION_TIMED_OUT -> permitted retry within budget
	req3 := FailureEvaluationRequest{
		IncidentID:            "inc-imm-3",
		NodeID:                "node-1",
		ActionID:              "act-3",
		FailureClassification: types.FailureVerificationTimedOut,
		Reason:                "observations did not stabilize before timeout",
	}
	d3, err := engine.EvaluateFailure(ctx, req3)
	if err != nil {
		t.Fatalf("EvaluateFailure failed: %v", err)
	}
	if d3.Action != DecisionActionRetry || d3.Classification != types.FailureVerificationTimedOut {
		t.Fatalf("expected RETRY for initial VERIFICATION_TIMED_OUT, got %+v", d3)
	}
}

// ============================================================================
// Scenario Group 7: Sliding Failure Window
// ============================================================================

func TestEngine_SlidingFailureWindow(t *testing.T) {
	policy := EscalationPolicy{
		MaxAutomaticRetries:            3,
		RetryCooldown:                  1 * time.Minute,
		FailureWindow:                  10 * time.Minute,
		CircuitBreakerFailureThreshold: 3,
		PolicyVersion:                  "v1.0.0",
	}

	startTime := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	clock := NewFakeClock(startTime)
	engine, _ := setupTestEngine(t, policy, clock)
	ctx := context.Background()

	req := FailureEvaluationRequest{
		IncidentID:            "inc-window",
		NodeID:                "node-window",
		ActionID:              "act-window",
		FailureClassification: types.FailureMitigationFailed,
	}

	// Failure 1 at T+0
	d1, _ := engine.EvaluateFailure(ctx, req)
	if d1.Attempt != 1 {
		t.Fatalf("expected attempt 1, got %d", d1.Attempt)
	}

	// Advance past cooldown but within window (T+5m)
	clock.Advance(5 * time.Minute)
	d2, _ := engine.EvaluateFailure(ctx, req)
	if d2.Attempt != 2 {
		t.Fatalf("expected attempt 2, got %d", d2.Attempt)
	}

	// Advance clock so failure 1 expires (>10m ago) but failure 2 remains (<10m ago) (T+12m total)
	clock.Advance(7 * time.Minute)
	d3, _ := engine.EvaluateFailure(ctx, req)

	// Since only failure 2 remains in window, failure 3 is attempt 2 in this window!
	// It should NOT trip the breaker (threshold = 3)!
	if d3.Action != DecisionActionRetry {
		t.Fatalf("expected retry because older failure expired outside window, got %+v", d3)
	}
	if d3.Attempt != 2 {
		t.Fatalf("expected attempt 2 after window slide, got %d", d3.Attempt)
	}
}

// ============================================================================
// Scenario Group 8: Survival Across Process Restarts
// ============================================================================

func TestEngine_SurvivalAcrossRestart(t *testing.T) {
	dbDir := t.TempDir()
	dbPath := filepath.Join(dbDir, "restart.db")
	store1, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}

	policy := EscalationPolicy{
		MaxAutomaticRetries:            2,
		RetryCooldown:                  5 * time.Minute,
		FailureWindow:                  30 * time.Minute,
		CircuitBreakerFailureThreshold: 2,
		PolicyVersion:                  "v1.0.0",
	}

	startTime := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	clock := NewFakeClock(startTime)
	engine1, _ := NewEngine(store1, policy, clock)
	ctx := context.Background()

	req := FailureEvaluationRequest{
		IncidentID:            "inc-restart",
		NodeID:                "node-restart",
		ActionID:              "act-restart",
		FailureClassification: types.FailureMitigationFailed,
	}

	// Attempt 1
	engine1.EvaluateFailure(ctx, req)
	clock.Advance(6 * time.Minute)

	// Attempt 2 -> trips circuit breaker to OPEN
	engine1.EvaluateFailure(ctx, req)

	// Close store1 (simulating process termination)
	store1.Close()

	// Reopen store2 in new process lifecycle
	store2, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("reopening store failed: %v", err)
	}
	defer store2.Close()

	engine2, err := NewEngine(store2, policy, clock)
	if err != nil {
		t.Fatalf("NewEngine2 failed: %v", err)
	}

	// Verify circuit breaker is still OPEN immediately upon recovery
	cb, err := engine2.GetCircuitBreakerStatus(ctx, "node-restart", "inc-restart")
	if err != nil {
		t.Fatalf("GetCircuitBreakerStatus failed: %v", err)
	}
	if cb.State != types.CircuitOpen {
		t.Fatalf("circuit breaker must remain OPEN after restart, got %s", cb.State)
	}

	canRetry, _, err := engine2.CanRetry(ctx, "node-restart", "inc-restart")
	if err != nil || canRetry {
		t.Fatalf("CanRetry must be false across restart when breaker was OPEN")
	}
}

// ============================================================================
// Scenario Group 9: Incident Engine Safe Handoff
// ============================================================================

func TestEngine_HandoffToIncidentEngine(t *testing.T) {
	policy := DefaultPolicy()
	now := time.Now().UTC()
	clock := NewFakeClock(now)
	engine, _ := setupTestEngine(t, policy, clock)
	ctx := context.Background()

	// Create real incident engine in in-memory mode
	incEngine, err := incident.NewLocalEngine(incident.PolicyConfig{
		M:              1,
		N:              3,
		WindowDuration: 10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewLocalEngine failed: %v", err)
	}

	// 1. Nil IncidentEngine rejected
	if err := engine.HandoffToIncidentEngine(ctx, nil, "node-1", "cpu", types.StatusEscalated, now); err != ErrNilIncidentEngine {
		t.Errorf("expected ErrNilIncidentEngine, got %v", err)
	}

	// 2. Non-ESCALATED target rejected
	if err := engine.HandoffToIncidentEngine(ctx, incEngine, "node-1", "cpu", types.StatusRecovered, now); err != ErrInvalidTargetStatus {
		t.Errorf("expected ErrInvalidTargetStatus, got %v", err)
	}

	// 3. No active incident -> error returned
	if err := engine.HandoffToIncidentEngine(ctx, incEngine, "node-empty", "cpu", types.StatusEscalated, now); err == nil {
		t.Errorf("expected error on empty incident handoff, got nil")
	}

	// 4. Register active incident in ANOMALY_DETECTED
	inc := &types.Incident{
		IncidentID:    "inc-handoff-1",
		NodeID:        "node-handoff",
		RuleName:      "cpu_high",
		Severity:      types.SeverityHigh,
		Status:        types.StatusAnomalyDetected,
		Description:   "high cpu usage",
		TriggerMetric: "cpu_usage",
		TriggeredAt:   now,
		UpdatedAt:     now,
	}
	_, err = incEngine.RegisterIncident(ctx, inc)
	if err != nil {
		t.Fatalf("RegisterIncident failed: %v", err)
	}

	// Handoff: ANOMALY_DETECTED -> ESCALATED
	if err := engine.HandoffToIncidentEngine(ctx, incEngine, "node-handoff", "cpu_usage", types.StatusEscalated, now); err != nil {
		t.Fatalf("HandoffToIncidentEngine failed: %v", err)
	}

	active := incEngine.GetActiveIncident("node-handoff", "cpu_usage")
	if active.Status != types.StatusEscalated {
		t.Fatalf("expected status ESCALATED, got %s", active.Status)
	}

	// Idempotent second handoff: already ESCALATED -> clean no-op
	if err := engine.HandoffToIncidentEngine(ctx, incEngine, "node-handoff", "cpu_usage", types.StatusEscalated, now); err != nil {
		t.Fatalf("subsequent handoff must be idempotent clean no-op: %v", err)
	}
}

// ============================================================================
// Scenario Group 10: Thread Safety & Concurrency
// ============================================================================

func TestEngine_Concurrency(t *testing.T) {
	policy := EscalationPolicy{
		MaxAutomaticRetries:            10,
		RetryCooldown:                  100 * time.Millisecond,
		FailureWindow:                  30 * time.Minute,
		CircuitBreakerFailureThreshold: 20,
		PolicyVersion:                  "v1.0.0",
	}

	clock := NewFakeClock(time.Now())
	engine, store := setupTestEngine(t, policy, clock)
	ctx := context.Background()

	var wg sync.WaitGroup
	workers := 20
	decisions := make([]*EscalationDecision, workers)
	var mu sync.Mutex

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			req := FailureEvaluationRequest{
				IncidentID:            "inc-concurrent",
				NodeID:                "node-concurrent",
				ActionID:              "act-concurrent",
				FailureClassification: types.FailureMitigationFailed,
			}
			d, err := engine.EvaluateFailure(ctx, req)
			if err == nil && d != nil {
				mu.Lock()
				decisions[workerID] = d
				mu.Unlock()
			}
			_, _, _ = engine.CanRetry(ctx, "node-concurrent", "inc-concurrent")
			_, _ = engine.GetCircuitBreakerStatus(ctx, "node-concurrent", "inc-concurrent")
		}(i)
	}

	wg.Wait()

	// 1. Verify circuit state remains consistent
	cb, err := engine.GetCircuitBreakerStatus(ctx, "node-concurrent", "inc-concurrent")
	if err != nil {
		t.Fatalf("GetCircuitBreakerStatus failed: %v", err)
	}
	if !cb.State.IsValid() {
		t.Fatalf("circuit state must be valid, got %s", cb.State)
	}

	// 2. Verify duplicate escalation identity is idempotent and bounded
	count, err := store.CountEscalations(ctx)
	if err != nil {
		t.Fatalf("CountEscalations failed: %v", err)
	}
	if count > 3 {
		t.Fatalf("concurrent identical failure events must not create unbounded records; got %d records", count)
	}

	// 3. Verify retry budget cannot be exceeded by concurrent evaluations
	for _, d := range decisions {
		if d != nil && d.Attempt > policy.MaxAutomaticRetries+1 {
			t.Fatalf("decision attempt %d exceeded retry budget max %d", d.Attempt, policy.MaxAutomaticRetries+1)
		}
	}
}

// ============================================================================
// Scenario Group 11: Policy Validation
// ============================================================================

func TestEscalationPolicy_Validation(t *testing.T) {
	valid := DefaultPolicy()
	if err := valid.Validate(); err != nil {
		t.Errorf("DefaultPolicy must be valid, got %v", err)
	}

	// Empty policy version
	p1 := valid
	p1.PolicyVersion = "  "
	if err := p1.Validate(); err != ErrInvalidPolicyVersion {
		t.Errorf("expected ErrInvalidPolicyVersion, got %v", err)
	}

	// Negative retries
	p2 := valid
	p2.MaxAutomaticRetries = -1
	if err := p2.Validate(); err != ErrNegativeMaxRetries {
		t.Errorf("expected ErrNegativeMaxRetries, got %v", err)
	}

	// Negative cooldown
	p3 := valid
	p3.RetryCooldown = -1 * time.Second
	if err := p3.Validate(); err != ErrNegativeCooldown {
		t.Errorf("expected ErrNegativeCooldown, got %v", err)
	}

	// Zero or negative failure window
	p4 := valid
	p4.FailureWindow = 0
	if err := p4.Validate(); err != ErrInvalidFailureWindow {
		t.Errorf("expected ErrInvalidFailureWindow, got %v", err)
	}

	// Zero or negative circuit threshold
	p5 := valid
	p5.CircuitBreakerFailureThreshold = 0
	if err := p5.Validate(); err != ErrInvalidCircuitThreshold {
		t.Errorf("expected ErrInvalidCircuitThreshold, got %v", err)
	}
}

// ============================================================================
// Audit Scenario 1: Exact Retry Budget Bound Proof
// ============================================================================

func TestEngine_Audit_RetryBudgetBound(t *testing.T) {
	// MaxAutomaticRetries = 3: exactly 3 automatic retries permitted after initial failure.
	// Total maximum attempts before circuit breaker trips = 4 (1 initial + 3 retries).
	policy := EscalationPolicy{
		MaxAutomaticRetries:            3,
		RetryCooldown:                  1 * time.Minute,
		FailureWindow:                  60 * time.Minute,
		CircuitBreakerFailureThreshold: 10, // higher so retry budget triggers first
		PolicyVersion:                  "v1.0.0",
	}

	clock := NewFakeClock(time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC))
	engine, store := setupTestEngine(t, policy, clock)
	ctx := context.Background()

	req := FailureEvaluationRequest{
		IncidentID:            "inc-exact-budget",
		NodeID:                "node-exact-budget",
		ActionID:              "act-exact-budget",
		FailureClassification: types.FailureMitigationFailed,
	}

	// 1. Initial attempt fails (Attempt 1) -> Retry #1 permitted
	d1, err := engine.EvaluateFailure(ctx, req)
	if err != nil || d1.Action != DecisionActionRetry || d1.Attempt != 1 {
		t.Fatalf("attempt 1: expected RETRY attempt 1, got %+v", d1)
	}

	clock.Advance(2 * time.Minute)

	// 2. Retry #1 fails (Attempt 2) -> Retry #2 permitted
	d2, err := engine.EvaluateFailure(ctx, req)
	if err != nil || d2.Action != DecisionActionRetry || d2.Attempt != 2 {
		t.Fatalf("attempt 2: expected RETRY attempt 2, got %+v", d2)
	}

	clock.Advance(2 * time.Minute)

	// 3. Retry #2 fails (Attempt 3) -> Retry #3 permitted
	d3, err := engine.EvaluateFailure(ctx, req)
	if err != nil || d3.Action != DecisionActionRetry || d3.Attempt != 3 {
		t.Fatalf("attempt 3: expected RETRY attempt 3, got %+v", d3)
	}

	clock.Advance(2 * time.Minute)

	// 4. Retry #3 fails (Attempt 4) -> 3 retries completed! Retry budget EXHAUSTED!
	d4, err := engine.EvaluateFailure(ctx, req)
	if err != nil {
		t.Fatalf("attempt 4 failed: %v", err)
	}
	if d4.Action != DecisionActionEscalate {
		t.Fatalf("attempt 4: expected ESCALATE, got %s", d4.Action)
	}
	if d4.Classification != types.FailureRetryBudgetExhausted {
		t.Fatalf("attempt 4: expected RETRY_BUDGET_EXHAUSTED, got %s", d4.Classification)
	}
	if d4.CircuitState != types.CircuitOpen {
		t.Fatalf("attempt 4: expected circuit OPEN, got %s", d4.CircuitState)
	}

	// Verify circuit breaker in store is now OPEN
	cb, _ := store.GetCircuitState(ctx, "node-exact-budget", "inc-exact-budget")
	if cb == nil || cb.State != types.CircuitOpen {
		t.Fatalf("expected circuit OPEN in store, got %+v", cb)
	}

	// Verify CanRetry is strictly false
	canRetry, _, _ := engine.CanRetry(ctx, "node-exact-budget", "inc-exact-budget")
	if canRetry {
		t.Fatalf("CanRetry must be false after budget exhausted")
	}
}

// ============================================================================
// Audit Scenario 2: Circuit Breaker Scope Isolation (NodeID, IncidentID)
// ============================================================================

func TestEngine_Audit_CircuitBreakerScopeIsolation(t *testing.T) {
	policy := EscalationPolicy{
		MaxAutomaticRetries:            1,
		RetryCooldown:                  1 * time.Minute,
		FailureWindow:                  30 * time.Minute,
		CircuitBreakerFailureThreshold: 1, // trips on first failure
		PolicyVersion:                  "v1.0.0",
	}

	clock := NewFakeClock(time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC))
	engine, _ := setupTestEngine(t, policy, clock)
	ctx := context.Background()

	// Trip circuit breaker for (Node-1, Incident-A)
	reqA := FailureEvaluationRequest{
		IncidentID:            "incident-A",
		NodeID:                "node-1",
		ActionID:              "act-1",
		FailureClassification: types.FailureMitigationFailed,
	}
	dA, err := engine.EvaluateFailure(ctx, reqA)
	if err != nil || dA.CircuitState != types.CircuitOpen {
		t.Fatalf("expected circuit OPEN for (node-1, incident-A), got %+v", dA)
	}

	// Check (Node-1, Incident-B): must remain CLOSED!
	canRetryB, _, err := engine.CanRetry(ctx, "node-1", "incident-B")
	if err != nil || !canRetryB {
		t.Fatalf("incident-B on node-1 must NOT be affected by incident-A circuit trip")
	}
	statusB, err := engine.GetCircuitBreakerStatus(ctx, "node-1", "incident-B")
	if err != nil || statusB.State != types.CircuitClosed {
		t.Fatalf("circuit for (node-1, incident-B) must be CLOSED, got %+v", statusB)
	}

	// Check (Node-2, Incident-A): must remain CLOSED!
	canRetryNode2, _, err := engine.CanRetry(ctx, "node-2", "incident-A")
	if err != nil || !canRetryNode2 {
		t.Fatalf("incident-A on node-2 must NOT be affected by node-1 circuit trip")
	}
	statusNode2, err := engine.GetCircuitBreakerStatus(ctx, "node-2", "incident-A")
	if err != nil || statusNode2.State != types.CircuitClosed {
		t.Fatalf("circuit for (node-2, incident-A) must be CLOSED, got %+v", statusNode2)
	}
}

// ============================================================================
// Audit Scenario 3: Cooldown Active Does Not Increment Failure Count
// ============================================================================

func TestEngine_Audit_CooldownEvaluationDoesNotIncrementFailures(t *testing.T) {
	policy := EscalationPolicy{
		MaxAutomaticRetries:            3,
		RetryCooldown:                  5 * time.Minute,
		FailureWindow:                  30 * time.Minute,
		CircuitBreakerFailureThreshold: 3,
		PolicyVersion:                  "v1.0.0",
	}

	startTime := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	clock := NewFakeClock(startTime)
	engine, store := setupTestEngine(t, policy, clock)
	ctx := context.Background()

	req := FailureEvaluationRequest{
		IncidentID:            "inc-cooldown-audit",
		NodeID:                "node-cooldown-audit",
		ActionID:              "act-cooldown-audit",
		FailureClassification: types.FailureMitigationFailed,
	}

	// Initial failure at T+0
	d1, err := engine.EvaluateFailure(ctx, req)
	if err != nil || d1.Action != DecisionActionRetry || d1.Attempt != 1 {
		t.Fatalf("initial failure: expected RETRY attempt 1, got %+v", d1)
	}

	countBefore, _ := store.CountEscalations(ctx)
	if countBefore != 1 {
		t.Fatalf("expected exactly 1 escalation record before cooldown checks, got %d", countBefore)
	}

	// Evaluate at T+1m (cooldown active)
	clock.Advance(1 * time.Minute)
	d2, err := engine.EvaluateFailure(ctx, req)
	if err != nil {
		t.Fatalf("evaluation at T+1m failed: %v", err)
	}
	if d2.Action != DecisionActionNone || d2.Classification != types.FailureCooldownActive {
		t.Fatalf("expected DecisionActionNone and FailureCooldownActive during cooldown, got %+v", d2)
	}

	// Evaluate at T+3m (cooldown still active)
	clock.Advance(2 * time.Minute)
	d3, err := engine.EvaluateFailure(ctx, req)
	if err != nil || d3.Action != DecisionActionNone || d3.Classification != types.FailureCooldownActive {
		t.Fatalf("expected DecisionActionNone during cooldown at T+3m, got %+v", d3)
	}

	// PROVE: Failure count in window and total records in SQLite did NOT increment!
	failuresInWindow, _ := store.CountFailuresInWindow(ctx, "node-cooldown-audit", "inc-cooldown-audit", startTime.Add(-10*time.Minute))
	if failuresInWindow != 1 {
		t.Fatalf("cooldown evaluations must NOT increment failures in window; expected 1, got %d", failuresInWindow)
	}
	countAfter, _ := store.CountEscalations(ctx)
	if countAfter != 1 {
		t.Fatalf("cooldown evaluations must NOT create new escalation records; expected 1, got %d", countAfter)
	}

	// Now advance past cooldown (T+6m total, >5m cooldown)
	clock.Advance(3 * time.Minute)
	d4, err := engine.EvaluateFailure(ctx, req)
	if err != nil {
		t.Fatalf("post-cooldown evaluation failed: %v", err)
	}
	if d4.Action != DecisionActionRetry || d4.Attempt != 2 {
		t.Fatalf("post-cooldown actual failure must count as Attempt 2, got %+v", d4)
	}
	countPostCooldown, _ := store.CountEscalations(ctx)
	if countPostCooldown != 2 {
		t.Fatalf("expected 2 escalation records after real 2nd failure, got %d", countPostCooldown)
	}
}

// ============================================================================
// Audit Scenario 4: Mitigation Results Integration
// ============================================================================

func TestEngine_Audit_EvaluateMitigationResult(t *testing.T) {
	policy := DefaultPolicy()
	engine, _ := setupTestEngine(t, policy, NewFakeClock(time.Now()))
	ctx := context.Background()

	// 1. EXECUTED -> NO escalation
	mSuccess := &types.MitigationAction{
		ActionID:   "act-succ",
		IncidentID: "inc-1",
		ActionType: types.ActionSimulatedThrottle,
		Status:     types.MitigationStatusExecuted,
	}
	d1, err := engine.EvaluateMitigationResult(ctx, "node-1", mSuccess)
	if err != nil || d1.Action != DecisionActionNone {
		t.Fatalf("EXECUTED mitigation must produce DecisionActionNone, got %+v", d1)
	}

	// 2. PENDING / EXECUTING / SKIPPED -> NO escalation
	mPending := &types.MitigationAction{
		ActionID:   "act-pend",
		IncidentID: "inc-1",
		ActionType: types.ActionSimulatedThrottle,
		Status:     types.MitigationStatusPending,
	}
	d2, err := engine.EvaluateMitigationResult(ctx, "node-1", mPending)
	if err != nil || d2.Action != DecisionActionNone {
		t.Fatalf("PENDING mitigation must produce DecisionActionNone, got %+v", d2)
	}

	// 3. FAILED -> evaluates failure (Retry permitted on attempt 1)
	mFailed := &types.MitigationAction{
		ActionID:   "act-fail",
		IncidentID: "inc-1",
		ActionType: types.ActionSimulatedThrottle,
		Status:     types.MitigationStatusFailed,
		Error:      "exit status 1",
	}
	d3, err := engine.EvaluateMitigationResult(ctx, "node-1", mFailed)
	if err != nil || d3.Action != DecisionActionRetry || d3.Classification != types.FailureMitigationFailed {
		t.Fatalf("FAILED mitigation must evaluate to RETRY on attempt 1, got %+v", d3)
	}

	// 4. UNKNOWN_RECONCILIATION_REQUIRED -> immediate escalation, NEVER permission to retry!
	mUnknown := &types.MitigationAction{
		ActionID:   "act-unk",
		IncidentID: "inc-unk",
		ActionType: types.ActionSimulatedThrottle,
		Status:     types.MitigationStatusUnknownReconciliationRequired,
		Error:      "crashed during invocation",
	}
	d4, err := engine.EvaluateMitigationResult(ctx, "node-1", mUnknown)
	if err != nil {
		t.Fatalf("UNKNOWN_RECONCILIATION_REQUIRED evaluation failed: %v", err)
	}
	if d4.Action != DecisionActionEscalate {
		t.Fatalf("UNKNOWN_RECONCILIATION_REQUIRED must ESCALATE immediately; got %s", d4.Action)
	}
	if d4.Classification != types.FailureUnknownReconciliationRequired {
		t.Fatalf("expected classification UNKNOWN_RECONCILIATION_REQUIRED, got %s", d4.Classification)
	}
	if !d4.ShouldTransitionIncident {
		t.Fatalf("UNKNOWN_RECONCILIATION_REQUIRED must request incident transition")
	}
}

// ============================================================================
// Audit Scenario 5: Verification Results Integration
// ============================================================================

func TestEngine_Audit_EvaluateVerificationResult(t *testing.T) {
	policy := DefaultPolicy()
	engine, _ := setupTestEngine(t, policy, NewFakeClock(time.Now()))
	ctx := context.Background()

	// 1. RECOVERED -> NO escalation
	vRec := &storage.StoredVerification{
		VerificationID: "v-rec",
		IncidentID:     "inc-v1",
		NodeID:         "node-v1",
		ActionID:       "act-v1",
		Status:         types.VerificationStatusRecovered,
	}
	d1, err := engine.EvaluateVerificationResult(ctx, vRec)
	if err != nil || d1.Action != DecisionActionNone {
		t.Fatalf("RECOVERED verification must produce DecisionActionNone, got %+v", d1)
	}

	// 2. PENDING -> NO escalation
	vPend := &storage.StoredVerification{
		VerificationID: "v-pend",
		IncidentID:     "inc-v1",
		NodeID:         "node-v1",
		ActionID:       "act-v1",
		Status:         types.VerificationStatusPending,
	}
	d2, err := engine.EvaluateVerificationResult(ctx, vPend)
	if err != nil || d2.Action != DecisionActionNone {
		t.Fatalf("PENDING verification must produce DecisionActionNone, got %+v", d2)
	}

	// 3. CANCELLED -> NO automatic retry
	vCancel := &storage.StoredVerification{
		VerificationID: "v-cancel",
		IncidentID:     "inc-v1",
		NodeID:         "node-v1",
		ActionID:       "act-v1",
		Status:         types.VerificationStatusCancelled,
	}
	d3, err := engine.EvaluateVerificationResult(ctx, vCancel)
	if err != nil || d3.Action != DecisionActionNone {
		t.Fatalf("CANCELLED verification must produce DecisionActionNone, got %+v", d3)
	}

	// 4. TIMED_OUT -> evaluates failure (FailureVerificationTimedOut)
	vTimeout := &storage.StoredVerification{
		VerificationID: "v-to",
		IncidentID:     "inc-to",
		NodeID:         "node-to",
		ActionID:       "act-to",
		MetricName:     "cpu_usage",
		Status:         types.VerificationStatusTimedOut,
		Reason:         "streak did not reach 3",
	}
	d4, err := engine.EvaluateVerificationResult(ctx, vTimeout)
	if err != nil || d4.Action != DecisionActionRetry || d4.Classification != types.FailureVerificationTimedOut {
		t.Fatalf("TIMED_OUT verification must evaluate to RETRY on attempt 1, got %+v", d4)
	}

	// 5. NOT_RECOVERED -> evaluates failure (FailureVerificationRejected -> immediate escalation)
	vNotRec := &storage.StoredVerification{
		VerificationID: "v-nr",
		IncidentID:     "inc-nr",
		NodeID:         "node-nr",
		ActionID:       "act-nr",
		MetricName:     "cpu_usage",
		Status:         types.VerificationStatusNotRecovered,
		Reason:         "metric regressed significantly",
	}
	d5, err := engine.EvaluateVerificationResult(ctx, vNotRec)
	if err != nil || d5.Action != DecisionActionEscalate || d5.Classification != types.FailureVerificationRejected {
		t.Fatalf("NOT_RECOVERED verification must evaluate to ESCALATE, got %+v", d5)
	}
}

// ============================================================================
// Audit Scenario 6: Incident FSM Authority Transitions
// ============================================================================

func TestEngine_Audit_IncidentFSMAuthorityTransitions(t *testing.T) {
	policy := DefaultPolicy()
	now := time.Now().UTC()
	clock := NewFakeClock(now)
	engine, _ := setupTestEngine(t, policy, clock)
	ctx := context.Background()

	incEngine, err := incident.NewLocalEngine(incident.PolicyConfig{
		M:              1,
		N:              3,
		WindowDuration: 10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewLocalEngine failed: %v", err)
	}

	// 1. ANOMALY_DETECTED -> ESCALATED: Allowed
	inc1 := &types.Incident{
		IncidentID:    "inc-fsm-1",
		NodeID:        "node-fsm",
		RuleName:      "cpu_high",
		Severity:      types.SeverityHigh,
		Status:        types.StatusAnomalyDetected,
		TriggerMetric: "cpu_usage",
		TriggeredAt:   now,
		UpdatedAt:     now,
	}
	_, err = incEngine.RegisterIncident(ctx, inc1)
	if err != nil {
		t.Fatalf("RegisterIncident 1 failed: %v", err)
	}
	if err := engine.HandoffToIncidentEngine(ctx, incEngine, "node-fsm", "cpu_usage", types.StatusEscalated, now); err != nil {
		t.Fatalf("ANOMALY_DETECTED -> ESCALATED must be allowed: %v", err)
	}
	if incEngine.GetActiveIncident("node-fsm", "cpu_usage").Status != types.StatusEscalated {
		t.Fatalf("expected status ESCALATED")
	}

	// 2. ESCALATED -> ESCALATED: Idempotent no-op
	if err := engine.HandoffToIncidentEngine(ctx, incEngine, "node-fsm", "cpu_usage", types.StatusEscalated, now); err != nil {
		t.Fatalf("ESCALATED -> ESCALATED must be idempotent clean no-op: %v", err)
	}

	// Clean up stream
	_, _ = incEngine.CloseActiveIncident(ctx, "node-fsm", "cpu_usage", now)

	// 3. MITIGATING -> ESCALATED: Allowed
	inc2 := &types.Incident{
		IncidentID:    "inc-fsm-2",
		NodeID:        "node-fsm",
		RuleName:      "cpu_high",
		Severity:      types.SeverityHigh,
		Status:        types.StatusAnomalyDetected,
		TriggerMetric: "cpu_usage",
		TriggeredAt:   now,
		UpdatedAt:     now,
	}
	_, _ = incEngine.RegisterIncident(ctx, inc2)
	_, _ = incEngine.TransitionActiveIncident(ctx, "node-fsm", "cpu_usage", types.StatusMitigating, now)
	if err := engine.HandoffToIncidentEngine(ctx, incEngine, "node-fsm", "cpu_usage", types.StatusEscalated, now); err != nil {
		t.Fatalf("MITIGATING -> ESCALATED must be allowed: %v", err)
	}
	if incEngine.GetActiveIncident("node-fsm", "cpu_usage").Status != types.StatusEscalated {
		t.Fatalf("expected status ESCALATED")
	}

	// Clean up stream
	_, _ = incEngine.CloseActiveIncident(ctx, "node-fsm", "cpu_usage", now)

	// 4. RECOVERED -> ESCALATED: Rejected by canonical Incident FSM
	inc3 := &types.Incident{
		IncidentID:    "inc-fsm-3",
		NodeID:        "node-fsm",
		RuleName:      "cpu_high",
		Severity:      types.SeverityHigh,
		Status:        types.StatusAnomalyDetected,
		TriggerMetric: "cpu_usage",
		TriggeredAt:   now,
		UpdatedAt:     now,
	}
	_, _ = incEngine.RegisterIncident(ctx, inc3)
	_, _ = incEngine.TransitionActiveIncident(ctx, "node-fsm", "cpu_usage", types.StatusMitigating, now)
	_, _ = incEngine.ResolveIncident(ctx, "node-fsm", "cpu_usage", now) // Now RECOVERED
	err = engine.HandoffToIncidentEngine(ctx, incEngine, "node-fsm", "cpu_usage", types.StatusEscalated, now)
	if err == nil {
		t.Fatalf("RECOVERED -> ESCALATED must be rejected by canonical FSM")
	}

	// 5. NORMAL -> ESCALATED: Rejected by canonical Incident FSM
	_, _ = incEngine.TransitionActiveIncident(ctx, "node-fsm", "cpu_usage", types.StatusNormal, now) // Now NORMAL
	err = engine.HandoffToIncidentEngine(ctx, incEngine, "node-fsm", "cpu_usage", types.StatusEscalated, now)
	if err == nil {
		t.Fatalf("NORMAL -> ESCALATED must be rejected by canonical FSM")
	}
}

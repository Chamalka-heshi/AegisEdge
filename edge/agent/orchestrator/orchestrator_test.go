package orchestrator_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/escalation"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/incident"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/metrics"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/orchestrator"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/response"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/verification"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
	_ "modernc.org/sqlite"
)

type testHarness struct {
	dbDir       string
	store       *storage.SQLiteStore
	policy      *response.RuleBasedPolicy
	validator   *response.StandardSafetyValidator
	executor    *response.SimulatedExecutor
	vEngine     *verification.LocalEngine
	escEngine   *escalation.LocalEngine
	incEngine   *incident.LocalEngine
	approvalMgr *response.ApprovalManager
	clock       *orchestrator.FakeClock
	orch        *orchestrator.DefaultOrchestrator
}

func setupTestHarness(t *testing.T, customEscPolicy *escalation.EscalationPolicy) *testHarness {
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

	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	clock := orchestrator.NewFakeClock(now)

	// Policy (ExecutionModeAutoExecute)
	policy := response.NewDefaultPolicy()
	if err := policy.SetExecutionMode(response.ExecutionModeAutoExecute); err != nil {
		t.Fatalf("SetExecutionMode failed: %v", err)
	}

	// Validator
	validator := response.NewStandardValidator()

	// Executor
	executor := response.NewSimulatedExecutor(validator)
	executor.WithApprovalStore(store)
	executor.WithClock(clock)

	// Verification Engine
	vCfg := verification.DefaultConfig()
	vCfg.Store = store
	vCfg.Clock = clock
	vCfg.RequiredConsecutiveObservations = 2
	vCfg.VerificationTimeout = 5 * time.Minute
	vEngine, err := verification.NewLocalEngine(vCfg)
	if err != nil {
		t.Fatalf("NewLocalEngine failed: %v", err)
	}

	// Escalation Engine
	escPolicy := escalation.DefaultPolicy()
	if customEscPolicy != nil {
		escPolicy = *customEscPolicy
	}
	escEngine, err := escalation.NewEngine(store, escPolicy, clock)
	if err != nil {
		t.Fatalf("NewEngine failed: %v", err)
	}

	// Incident Engine
	incCfg := incident.DefaultPolicyConfig()
	incEngine, err := incident.NewLocalEngine(incCfg)
	if err != nil {
		t.Fatalf("NewLocalEngine failed: %v", err)
	}

	// Approval Manager
	approvalMgr := response.NewApprovalManager(store, clock)

	// Orchestrator
	orchCfg := orchestrator.DefaultConfig()
	orchCfg.DefaultRequiredObservations = 2
	orch, err := orchestrator.NewOrchestrator(
		policy,
		validator,
		executor,
		vEngine,
		escEngine,
		incEngine,
		store,
		orchCfg,
		clock,
	)
	if err != nil {
		t.Fatalf("NewOrchestrator failed: %v", err)
	}
	orch.WithApprovalManager(approvalMgr)

	return &testHarness{
		dbDir:       dbDir,
		store:       store,
		policy:      policy,
		validator:   validator,
		executor:    executor,
		vEngine:     vEngine,
		escEngine:   escEngine,
		incEngine:   incEngine,
		approvalMgr: approvalMgr,
		clock:       clock,
		orch:        orch,
	}
}

func makeIncident(nodeID, metric string, severity types.IncidentSeverity, now time.Time) *types.Incident {
	return &types.Incident{
		IncidentID:    "inc-" + nodeID + "-" + metric,
		NodeID:        nodeID,
		RuleName:      "test_rule",
		Severity:      severity,
		Status:        types.StatusAnomalyDetected,
		TriggerMetric: metric,
		TriggerValue:  85.0,
		Threshold:     80.0,
		TriggeredAt:   now,
		UpdatedAt:     now,
	}
}

// ============================================================================
// TEST SCENARIOS A THROUGH T
// ============================================================================

// Scenario A: Happy path
// Incident -> policy -> validation -> simulated execution -> verification -> recovered
func TestOrchestrator_ScenarioA_HappyPath(t *testing.T) {
	h := setupTestHarness(t, nil)
	ctx := context.Background()
	now := h.clock.Now()

	inc := makeIncident("node-happy", "cpu_usage_percent", types.SeverityHigh, now)
	_, err := h.incEngine.RegisterIncident(ctx, inc)
	if err != nil {
		t.Fatalf("RegisterIncident failed: %v", err)
	}

	// Telemetry samples below recovery threshold (70.0)
	sample1 := types.MetricSample{
		NodeID:    "node-happy",
		Name:      "cpu_usage_percent",
		Value:     60.0,
		Timestamp: now.Add(1 * time.Second),
	}
	sample2 := types.MetricSample{
		NodeID:    "node-happy",
		Name:      "cpu_usage_percent",
		Value:     62.0,
		Timestamp: now.Add(2 * time.Second),
	}

	res, err := h.orch.OrchestrateIncident(ctx, inc, orchestrator.WithTelemetrySamples(sample1, sample2))
	if err != nil {
		t.Fatalf("OrchestrateIncident failed: %v", err)
	}

	if res.State != orchestrator.StateRecovered {
		t.Errorf("expected StateRecovered, got %s", res.State)
	}
	if res.ExecutionResult == nil || res.ExecutionResult.Status != types.MitigationStatusExecuted {
		t.Errorf("expected ExecutionResult EXECUTED, got %+v", res.ExecutionResult)
	}
	if res.VerificationResult == nil || res.VerificationResult.Status != types.VerificationStatusRecovered {
		t.Errorf("expected VerificationResult RECOVERED, got %+v", res.VerificationResult)
	}

	// Verify Incident FSM: active incident is resolved/cleared
	active := h.incEngine.GetActiveIncident("node-happy", "cpu_usage_percent")
	if active != nil && active.Status != types.StatusRecovered {
		t.Errorf("expected active incident resolved, got %+v", active)
	}
}

type countingRetryExecutor struct {
	mu       sync.Mutex
	calls    int
	fallback response.ActionExecutor
}

func (r *countingRetryExecutor) Execute(ctx context.Context, dec *response.ResponseDecision, inc *types.Incident) (*response.ExecutionResult, error) {
	return r.ExecuteValidated(ctx, &response.ValidatedDecision{Decision: dec, SafetyResult: &response.SafetyResult{Allowed: true}})
}

func (r *countingRetryExecutor) ExecuteValidated(ctx context.Context, vd *response.ValidatedDecision) (*response.ExecutionResult, error) {
	r.mu.Lock()
	r.calls++
	c := r.calls
	r.mu.Unlock()

	if c == 1 {
		return &response.ExecutionResult{
			ExecutionID: "exec-fail-1",
			DecisionID:  vd.Decision.DecisionID,
			IncidentID:  vd.Decision.IncidentID,
			NodeID:      vd.Decision.NodeID,
			ActionType:  vd.Decision.ActionType,
			Target:      vd.Decision.Target,
			Status:      types.MitigationStatusFailed,
			Message:     "simulated hardware error",
			ErrorCode:   "SIM_HW_ERR",
		}, errors.New("simulated hardware error")
	}
	return r.fallback.ExecuteValidated(ctx, vd)
}

func (r *countingRetryExecutor) ExecuteWithApproval(ctx context.Context, dec *response.ResponseDecision, inc *types.Incident, appID string) (*response.ExecutionResult, error) {
	return r.fallback.ExecuteWithApproval(ctx, dec, inc, appID)
}

// Scenario B: Failed mitigation -> escalation -> RETRY -> second attempt
func TestOrchestrator_ScenarioB_FailedMitigation_Retry(t *testing.T) {
	escPolicy := escalation.EscalationPolicy{
		MaxAutomaticRetries:            2,
		RetryCooldown:                  0, // instant retry permitted
		FailureWindow:                  30 * time.Minute,
		CircuitBreakerFailureThreshold: 5,
		PolicyVersion:                  "v1.0.0",
	}
	h := setupTestHarness(t, &escPolicy)
	ctx := context.Background()
	now := h.clock.Now()

	inc := makeIncident("node-retry", "cpu_usage_percent", types.SeverityHigh, now)
	_, _ = h.incEngine.RegisterIncident(ctx, inc)

	// Replace executor with counting retry executor
	retryExec := &countingRetryExecutor{fallback: h.executor}
	cfg := orchestrator.DefaultConfig()
	cfg.DefaultRequiredObservations = 2
	orch, err := orchestrator.NewOrchestrator(
		h.policy,
		h.validator,
		retryExec,
		h.vEngine,
		h.escEngine,
		h.incEngine,
		h.store,
		cfg,
		h.clock,
	)
	if err != nil {
		t.Fatalf("NewOrchestrator failed: %v", err)
	}

	sample1 := types.MetricSample{
		NodeID:    "node-retry",
		Name:      "cpu_usage_percent",
		Value:     55.0,
		Timestamp: now.Add(1 * time.Second),
	}
	sample2 := types.MetricSample{
		NodeID:    "node-retry",
		Name:      "cpu_usage_percent",
		Value:     56.0,
		Timestamp: now.Add(2 * time.Second),
	}

	res, err := orch.OrchestrateIncident(ctx, inc, orchestrator.WithTelemetrySamples(sample1, sample2))
	if err != nil {
		t.Fatalf("OrchestrateIncident failed: %v", err)
	}

	if retryExec.calls != 2 {
		t.Errorf("expected exactly 2 execution calls (1 failure + 1 retry), got %d", retryExec.calls)
	}
	if res.State != orchestrator.StateRecovered {
		t.Errorf("expected StateRecovered after retry, got %s", res.State)
	}
}

type alwaysFailExecutor struct {
	calls int
}

func (a *alwaysFailExecutor) Execute(ctx context.Context, dec *response.ResponseDecision, inc *types.Incident) (*response.ExecutionResult, error) {
	return a.ExecuteValidated(ctx, nil)
}

func (a *alwaysFailExecutor) ExecuteValidated(ctx context.Context, vd *response.ValidatedDecision) (*response.ExecutionResult, error) {
	a.calls++
	decID := "dec-unknown"
	incID := "inc-unknown"
	nodeID := "node-unknown"
	actType := types.ActionSimulatedThrottle
	target := "unknown"
	if vd != nil && vd.Decision != nil {
		decID = vd.Decision.DecisionID
		incID = vd.Decision.IncidentID
		nodeID = vd.Decision.NodeID
		actType = vd.Decision.ActionType
		target = vd.Decision.Target
	}
	return &response.ExecutionResult{
		ExecutionID: "exec-always-fail",
		DecisionID:  decID,
		IncidentID:  incID,
		NodeID:      nodeID,
		ActionType:  actType,
		Target:      target,
		Status:      types.MitigationStatusFailed,
		Message:     "permanent simulation failure",
		ErrorCode:   "PERM_FAIL",
	}, errors.New("permanent simulation failure")
}

func (a *alwaysFailExecutor) ExecuteWithApproval(ctx context.Context, dec *response.ResponseDecision, inc *types.Incident, appID string) (*response.ExecutionResult, error) {
	return a.ExecuteValidated(ctx, nil)
}

// Scenario C: Retry budget exhausted -> ESCALATE -> IncidentEngine
func TestOrchestrator_ScenarioC_RetryBudgetExhausted(t *testing.T) {
	escPolicy := escalation.EscalationPolicy{
		MaxAutomaticRetries:            1, // 1 retry allowed, 2nd failure exhausts budget
		RetryCooldown:                  0,
		FailureWindow:                  30 * time.Minute,
		CircuitBreakerFailureThreshold: 10,
		PolicyVersion:                  "v1.0.0",
	}
	h := setupTestHarness(t, &escPolicy)
	ctx := context.Background()
	now := h.clock.Now()

	inc := makeIncident("node-budget", "cpu_usage_percent", types.SeverityHigh, now)
	_, _ = h.incEngine.RegisterIncident(ctx, inc)

	failExec := &alwaysFailExecutor{}
	cfg := orchestrator.DefaultConfig()
	orch, _ := orchestrator.NewOrchestrator(
		h.policy,
		h.validator,
		failExec,
		h.vEngine,
		h.escEngine,
		h.incEngine,
		h.store,
		cfg,
		h.clock,
	)

	res, err := orch.OrchestrateIncident(ctx, inc)
	if err != nil {
		t.Fatalf("OrchestrateIncident unexpected error: %v", err)
	}

	if res.State != orchestrator.StateEscalated {
		t.Errorf("expected StateEscalated, got %s", res.State)
	}
	if failExec.calls != 2 {
		t.Errorf("expected 2 execution attempts before budget exhaustion, got %d", failExec.calls)
	}

	// Verify Incident transitioned to ESCALATED
	active := h.incEngine.GetActiveIncident("node-budget", "cpu_usage_percent")
	if active == nil || active.Status != types.StatusEscalated {
		t.Errorf("expected incident in ESCALATED status, got %+v", active)
	}
}

// Scenario D: Circuit breaker OPEN -> no execution -> escalation
func TestOrchestrator_ScenarioD_CircuitBreakerOpen(t *testing.T) {
	h := setupTestHarness(t, nil)
	ctx := context.Background()
	now := h.clock.Now()

	inc := makeIncident("node-cb", "cpu_usage_percent", types.SeverityHigh, now)
	_, _ = h.incEngine.RegisterIncident(ctx, inc)

	// Pre-trip circuit breaker to OPEN in SQLite store
	err := h.store.SetCircuitState(ctx, &storage.StoredCircuitState{
		NodeID:       "node-cb",
		IncidentID:   inc.IncidentID,
		State:        types.CircuitOpen,
		FailureCount: 3,
		UpdatedAt:    now,
	})
	if err != nil {
		t.Fatalf("SetCircuitState failed: %v", err)
	}

	res, err := h.orch.OrchestrateIncident(ctx, inc)
	if !errors.Is(err, orchestrator.ErrCircuitBreakerOpen) {
		t.Errorf("expected ErrCircuitBreakerOpen, got %v", err)
	}
	if res.State != orchestrator.StateEscalated {
		t.Errorf("expected StateEscalated, got %s", res.State)
	}
	if h.executor.SimulationCount() != 0 {
		t.Errorf("expected 0 simulations when circuit breaker is OPEN, got %d", h.executor.SimulationCount())
	}
}

// Scenario E: Cooldown active -> no execution -> no duplicate failure count
func TestOrchestrator_ScenarioE_CooldownActive(t *testing.T) {
	escPolicy := escalation.EscalationPolicy{
		MaxAutomaticRetries:            3,
		RetryCooldown:                  5 * time.Minute,
		FailureWindow:                  30 * time.Minute,
		CircuitBreakerFailureThreshold: 5,
		PolicyVersion:                  "v1.0.0",
	}
	h := setupTestHarness(t, &escPolicy)
	ctx := context.Background()
	now := h.clock.Now()

	inc := makeIncident("node-cd", "cpu_usage_percent", types.SeverityHigh, now)
	_, _ = h.incEngine.RegisterIncident(ctx, inc)

	// Record a prior failure 1 minute ago (cooldown lasts 5 minutes)
	priorTime := now.Add(-1 * time.Minute)
	err := h.store.RecordEscalation(ctx, &storage.StoredEscalation{
		EscalationID:          "esc-prior-01",
		IncidentID:            inc.IncidentID,
		NodeID:                inc.NodeID,
		ActionID:              "act-prior-01",
		PolicyVersion:         "v1.0.0",
		FailureClassification: types.FailureMitigationFailed,
		Status:                types.EscalationStatusPending,
		Attempt:               1,
		MaxAttempts:           3,
		Reason:                "prior attempt failed",
		CreatedAt:             priorTime,
		UpdatedAt:             priorTime,
	})
	if err != nil {
		t.Fatalf("RecordEscalation failed: %v", err)
	}

	// Also record prior mitigation in store so orchestrator detects prior attempt
	err = h.store.RecordMitigation(ctx, &storage.StoredMitigation{
		ActionID:    "act-prior-01",
		DecisionID:  "dec-prior-01",
		IncidentID:  inc.IncidentID,
		NodeID:      inc.NodeID,
		ActionType:  types.ActionSimulatedThrottle,
		Target:      "telemetry_generator",
		Status:      types.MitigationStatusFailed,
		Mode:        "AUTO_EXECUTE",
		StartedAt:   priorTime,
		CompletedAt: &priorTime,
		CreatedAt:   priorTime,
		UpdatedAt:   priorTime,
	})
	if err != nil {
		t.Fatalf("RecordMitigation failed: %v", err)
	}

	countBefore, _ := h.store.CountFailuresInWindow(ctx, inc.NodeID, inc.IncidentID, now.Add(-30*time.Minute))

	res, err := h.orch.OrchestrateIncident(ctx, inc)
	if !errors.Is(err, orchestrator.ErrCooldownActive) {
		t.Errorf("expected ErrCooldownActive, got %v", err)
	}
	if res.State != orchestrator.StateFailed {
		t.Errorf("expected StateFailed during cooldown, got %s", res.State)
	}

	// Verify failure count did not increase
	countAfter, _ := h.store.CountFailuresInWindow(ctx, inc.NodeID, inc.IncidentID, now.Add(-30*time.Minute))
	if countAfter != countBefore {
		t.Errorf("expected failure count unchanged (%d), got %d", countBefore, countAfter)
	}
	if h.executor.SimulationCount() != 0 {
		t.Errorf("expected 0 executions during cooldown, got %d", h.executor.SimulationCount())
	}
}

// Scenario F: Approval-required action -> approval missing -> WAITING_APPROVAL, no execution
func TestOrchestrator_ScenarioF_ApprovalMissing(t *testing.T) {
	h := setupTestHarness(t, nil)
	ctx := context.Background()
	now := h.clock.Now()

	// Add an APPROVAL_REQUIRED rule
	h.policy.AddRule(response.PolicyRule{
		RuleID:             "rule-app-req",
		Priority:           200,
		Enabled:            true,
		MetricName:         "disk_usage_percent",
		MinimumSeverity:    types.SeverityHigh,
		ActionType:         types.ActionSimulatedIsolate,
		Target:             "isolated_network",
		AuthorizationClass: response.AuthClassApprovalRequired,
		PolicyVersion:      "1.0.0",
		Reason:             "network isolation requires human operator approval",
	})

	inc := makeIncident("node-app", "disk_usage_percent", types.SeverityHigh, now)
	_, _ = h.incEngine.RegisterIncident(ctx, inc)

	res, err := h.orch.OrchestrateIncident(ctx, inc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.State != orchestrator.StateWaitingApproval {
		t.Errorf("expected StateWaitingApproval, got %s", res.State)
	}
	if h.executor.SimulationCount() != 0 {
		t.Errorf("expected 0 executions when approval is missing, got %d", h.executor.SimulationCount())
	}

	// Verify approval request was recorded in store in PENDING status
	approvals, err := h.store.ListApprovals(ctx, inc.IncidentID, 10)
	if err != nil || len(approvals) != 1 {
		t.Fatalf("expected 1 pending approval recorded, got %d (err: %v)", len(approvals), err)
	}
	if approvals[0].Status != types.ApprovalStatusPending {
		t.Errorf("expected approval status PENDING, got %s", approvals[0].Status)
	}
}

// Scenario G: Approval-required action -> valid approval -> execution
func TestOrchestrator_ScenarioG_ValidApproval_Execution(t *testing.T) {
	h := setupTestHarness(t, nil)
	ctx := context.Background()
	now := h.clock.Now()

	h.policy.AddRule(response.PolicyRule{
		RuleID:             "rule-app-req-g",
		Priority:           200,
		Enabled:            true,
		MetricName:         "disk_usage_percent",
		MinimumSeverity:    types.SeverityHigh,
		ActionType:         types.ActionSimulatedIsolate,
		Target:             "isolated_network",
		AuthorizationClass: response.AuthClassApprovalRequired,
		PolicyVersion:      "1.0.0",
		Reason:             "network isolation requires operator authorization",
	})

	inc := makeIncident("node-app-g", "disk_usage_percent", types.SeverityHigh, now)
	_, _ = h.incEngine.RegisterIncident(ctx, inc)

	// Evaluate policy to build candidate decision
	dec, err := h.policy.Evaluate(ctx, inc)
	if err != nil {
		t.Fatalf("policy.Evaluate failed: %v", err)
	}

	// Request and approve authorization
	appReq := response.ApprovalRequest{
		Decision:    dec,
		RequestedBy: "operator-alice",
		Reason:      "maintenance authorization",
	}
	app, err := h.approvalMgr.RequestApproval(ctx, appReq)
	if err != nil {
		t.Fatalf("RequestApproval failed: %v", err)
	}
	if err := h.approvalMgr.Approve(ctx, app.ApprovalID, "operator-alice"); err != nil {
		t.Fatalf("Approve failed: %v", err)
	}

	// Refresh approval object to APPROVED status
	storedApp, _ := h.store.GetApproval(ctx, app.ApprovalID)
	app = response.StoredToApproval(storedApp)

	sample1 := types.MetricSample{
		NodeID:    "node-app-g",
		Name:      "disk_usage_percent",
		Value:     60.0,
		Timestamp: now.Add(1 * time.Second),
	}
	sample2 := types.MetricSample{
		NodeID:    "node-app-g",
		Name:      "disk_usage_percent",
		Value:     61.0,
		Timestamp: now.Add(2 * time.Second),
	}

	res, err := h.orch.OrchestrateIncident(ctx, inc,
		orchestrator.WithApproval(app),
		orchestrator.WithTelemetrySamples(sample1, sample2),
	)
	if err != nil {
		t.Fatalf("OrchestrateIncident failed: %v", err)
	}

	if res.State != orchestrator.StateRecovered {
		t.Errorf("expected StateRecovered, got %s", res.State)
	}
	if res.ExecutionResult == nil || res.ExecutionResult.Status != types.MitigationStatusExecuted {
		t.Errorf("expected EXECUTED execution result, got %+v", res.ExecutionResult)
	}
}

// Scenario H: Expired approval -> no execution
func TestOrchestrator_ScenarioH_ExpiredApproval(t *testing.T) {
	h := setupTestHarness(t, nil)
	ctx := context.Background()
	now := h.clock.Now()

	h.policy.AddRule(response.PolicyRule{
		RuleID:             "rule-app-exp",
		Priority:           200,
		Enabled:            true,
		MetricName:         "disk_usage_percent",
		MinimumSeverity:    types.SeverityHigh,
		ActionType:         types.ActionSimulatedIsolate,
		Target:             "isolated_network",
		AuthorizationClass: response.AuthClassApprovalRequired,
		PolicyVersion:      "1.0.0",
	})

	inc := makeIncident("node-app-h", "disk_usage_percent", types.SeverityHigh, now)
	_, _ = h.incEngine.RegisterIncident(ctx, inc)

	dec, _ := h.policy.Evaluate(ctx, inc)
	appReq := response.ApprovalRequest{Decision: dec, RequestedBy: "op-1"}
	app, _ := h.approvalMgr.RequestApproval(ctx, appReq)
	_ = h.approvalMgr.Approve(ctx, app.ApprovalID, "op-1")
	storedApp, _ := h.store.GetApproval(ctx, app.ApprovalID)
	app = response.StoredToApproval(storedApp)

	// Make approval expired
	app.ExpiresAt = now.Add(-10 * time.Minute)

	res, err := h.orch.OrchestrateIncident(ctx, inc, orchestrator.WithApproval(app))
	if !errors.Is(err, orchestrator.ErrApprovalInvalid) {
		t.Errorf("expected ErrApprovalInvalid, got %v", err)
	}
	if res.State != orchestrator.StateFailed {
		t.Errorf("expected StateFailed for expired approval, got %s", res.State)
	}
	if h.executor.SimulationCount() != 0 {
		t.Errorf("expected 0 simulations for expired approval, got %d", h.executor.SimulationCount())
	}
}

// Scenario I: Tampered approval -> no execution
func TestOrchestrator_ScenarioI_TamperedApproval(t *testing.T) {
	h := setupTestHarness(t, nil)
	ctx := context.Background()
	now := h.clock.Now()

	h.policy.AddRule(response.PolicyRule{
		RuleID:             "rule-app-tamp",
		Priority:           200,
		Enabled:            true,
		MetricName:         "disk_usage_percent",
		MinimumSeverity:    types.SeverityHigh,
		ActionType:         types.ActionSimulatedIsolate,
		Target:             "isolated_network",
		AuthorizationClass: response.AuthClassApprovalRequired,
		PolicyVersion:      "1.0.0",
	})

	inc := makeIncident("node-app-i", "disk_usage_percent", types.SeverityHigh, now)
	_, _ = h.incEngine.RegisterIncident(ctx, inc)

	dec, _ := h.policy.Evaluate(ctx, inc)
	appReq := response.ApprovalRequest{Decision: dec, RequestedBy: "op-1"}
	app, _ := h.approvalMgr.RequestApproval(ctx, appReq)
	_ = h.approvalMgr.Approve(ctx, app.ApprovalID, "op-1")
	storedApp, _ := h.store.GetApproval(ctx, app.ApprovalID)
	app = response.StoredToApproval(storedApp)

	// Tamper fingerprint
	app.DecisionFingerprint = "fp-tampered-fingerprint-payload"

	res, err := h.orch.OrchestrateIncident(ctx, inc, orchestrator.WithApproval(app))
	if !errors.Is(err, orchestrator.ErrApprovalInvalid) {
		t.Errorf("expected ErrApprovalInvalid for tampered approval, got %v", err)
	}
	if res.State != orchestrator.StateFailed {
		t.Errorf("expected StateFailed, got %s", res.State)
	}
	if h.executor.SimulationCount() != 0 {
		t.Errorf("expected 0 simulations for tampered approval, got %d", h.executor.SimulationCount())
	}
}

type failingStore struct {
	storage.MitigationStore
	failRecord bool
}

func (f *failingStore) ListMitigations(ctx context.Context, incidentID string, limit int) ([]*storage.StoredMitigation, error) {
	if !f.failRecord {
		return nil, errors.New("simulated database I/O error")
	}
	return f.MitigationStore.ListMitigations(ctx, incidentID, limit)
}

func (f *failingStore) RecordMitigation(ctx context.Context, m *storage.StoredMitigation) error {
	if f.failRecord {
		return errors.New("simulated disk full error on RecordMitigation")
	}
	return f.MitigationStore.RecordMitigation(ctx, m)
}

// Scenario J: Persistence failure -> no execution, fail closed
func TestOrchestrator_ScenarioJ_PersistenceFailure(t *testing.T) {
	// Case 1: ListMitigations query failure
	{
		h := setupTestHarness(t, nil)
		ctx := context.Background()
		now := h.clock.Now()

		inc := makeIncident("node-pers-fail-1", "cpu_usage_percent", types.SeverityHigh, now)
		_, _ = h.incEngine.RegisterIncident(ctx, inc)

		badStore := &failingStore{MitigationStore: h.store, failRecord: false}
		cfg := orchestrator.DefaultConfig()
		orch, _ := orchestrator.NewOrchestrator(
			h.policy,
			h.validator,
			h.executor,
			h.vEngine,
			h.escEngine,
			h.incEngine,
			badStore,
			cfg,
			h.clock,
		)

		res, err := orch.OrchestrateIncident(ctx, inc)
		if !errors.Is(err, orchestrator.ErrPersistenceFailed) {
			t.Errorf("expected ErrPersistenceFailed on list query failure, got %v", err)
		}
		if res.State != orchestrator.StateFailed {
			t.Errorf("expected StateFailed on persistence failure, got %s", res.State)
		}
		if h.executor.SimulationCount() != 0 {
			t.Errorf("expected 0 executions on persistence failure, got %d", h.executor.SimulationCount())
		}
	}

	// Case 2: RecordMitigation fails prior to execution invocation
	// Invariant: durable mitigation record/state -> successful persistence -> executor invocation
	// If persistence fails: executor MUST NOT be invoked, orchestration fails closed, no retry should be fabricated locally.
	{
		h := setupTestHarness(t, nil)
		ctx := context.Background()
		now := h.clock.Now()

		inc := makeIncident("node-pers-fail-2", "cpu_usage_percent", types.SeverityHigh, now)
		_, _ = h.incEngine.RegisterIncident(ctx, inc)

		badStore := &failingStore{MitigationStore: h.store, failRecord: true}
		cfg := orchestrator.DefaultConfig()
		orch, _ := orchestrator.NewOrchestrator(
			h.policy,
			h.validator,
			h.executor,
			h.vEngine,
			h.escEngine,
			h.incEngine,
			badStore,
			cfg,
			h.clock,
		)

		res, err := orch.OrchestrateIncident(ctx, inc)
		if !errors.Is(err, orchestrator.ErrPersistenceFailed) {
			t.Errorf("expected ErrPersistenceFailed on record failure, got %v", err)
		}
		if res.State != orchestrator.StateFailed {
			t.Errorf("expected StateFailed on persistence record failure, got %s", res.State)
		}
		if h.executor.SimulationCount() != 0 {
			t.Errorf("expected 0 executions when persistence fails prior to execution, got %d", h.executor.SimulationCount())
		}
	}
}

type noActionPolicy struct {
	returnErr bool
}

func (p *noActionPolicy) Name() string    { return "no_action_policy" }
func (p *noActionPolicy) Version() string { return "1.0.0" }
func (p *noActionPolicy) Evaluate(ctx context.Context, inc *types.Incident) (*response.ResponseDecision, error) {
	if p.returnErr {
		return nil, response.ErrNoRuleMatched
	}
	return &response.ResponseDecision{
		DecisionID:         "dec-no-action",
		IncidentID:         inc.IncidentID,
		NodeID:             inc.NodeID,
		ActionType:         types.MitigationActionType("NO_ACTION"),
		AuthorizationClass: response.AuthClassAutoExecute,
	}, nil
}

// TestOrchestrator_PolicyNoAction verifies that when response policy determines NO_ACTION:
// - no mitigation executed
// - orchestration stops for that cycle
// - Incident state remains unchanged (NOT marked RECOVERED)
func TestOrchestrator_PolicyNoAction(t *testing.T) {
	// Case 1: Policy returns ActionType: "NO_ACTION"
	{
		h := setupTestHarness(t, nil)
		ctx := context.Background()
		now := h.clock.Now()

		inc := makeIncident("node-no-action-1", "custom_metric", types.SeverityLow, now)
		_, _ = h.incEngine.RegisterIncident(ctx, inc)

		noActPol := &noActionPolicy{returnErr: false}
		cfg := orchestrator.DefaultConfig()
		orch, _ := orchestrator.NewOrchestrator(
			noActPol,
			h.validator,
			h.executor,
			h.vEngine,
			h.escEngine,
			h.incEngine,
			h.store,
			cfg,
			h.clock,
		)

		res, err := orch.OrchestrateIncident(ctx, inc)
		if err != nil {
			t.Fatalf("unexpected error on NO_ACTION: %v", err)
		}

		if res.State != orchestrator.StateFailed {
			t.Errorf("expected StateFailed (stopped) on NO_ACTION, got %s", res.State)
		}
		if h.executor.SimulationCount() != 0 {
			t.Errorf("expected 0 executions on NO_ACTION, got %d", h.executor.SimulationCount())
		}

		// Verify Incident state remains ANOMALY_DETECTED, NOT marked RECOVERED
		active := h.incEngine.GetActiveIncident("node-no-action-1", "custom_metric")
		if active == nil {
			t.Fatalf("expected incident to remain active in IncidentEngine, got nil")
		}
		if active.Status != types.StatusAnomalyDetected {
			t.Errorf("expected incident status to remain ANOMALY_DETECTED, got %s", active.Status)
		}
	}

	// Case 2: Policy returns ErrNoRuleMatched
	{
		h := setupTestHarness(t, nil)
		ctx := context.Background()
		now := h.clock.Now()

		inc := makeIncident("node-no-action-2", "custom_metric", types.SeverityLow, now)
		_, _ = h.incEngine.RegisterIncident(ctx, inc)

		noActPol := &noActionPolicy{returnErr: true}
		cfg := orchestrator.DefaultConfig()
		orch, _ := orchestrator.NewOrchestrator(
			noActPol,
			h.validator,
			h.executor,
			h.vEngine,
			h.escEngine,
			h.incEngine,
			h.store,
			cfg,
			h.clock,
		)

		res, err := orch.OrchestrateIncident(ctx, inc)
		if err != nil {
			t.Fatalf("unexpected error on ErrNoRuleMatched: %v", err)
		}

		if res.State != orchestrator.StateFailed {
			t.Errorf("expected StateFailed (stopped) on ErrNoRuleMatched, got %s", res.State)
		}
		if h.executor.SimulationCount() != 0 {
			t.Errorf("expected 0 executions on ErrNoRuleMatched, got %d", h.executor.SimulationCount())
		}

		// Verify Incident state remains ANOMALY_DETECTED, NOT marked RECOVERED
		active := h.incEngine.GetActiveIncident("node-no-action-2", "custom_metric")
		if active == nil {
			t.Fatalf("expected incident to remain active in IncidentEngine, got nil")
		}
		if active.Status != types.StatusAnomalyDetected {
			t.Errorf("expected incident status to remain ANOMALY_DETECTED, got %s", active.Status)
		}
	}
}

// TestOrchestrator_DefenseInDepthCeiling verifies:
// MaxAutomaticRetries = 3 allows 1 initial + 3 retries = 4 total executions.
// MaxExecutionCeiling = 4 ensures no 5th execution can occur.
func TestOrchestrator_DefenseInDepthCeiling(t *testing.T) {
	escPolicy := escalation.EscalationPolicy{
		MaxAutomaticRetries:            3,
		RetryCooldown:                  0,
		FailureWindow:                  30 * time.Minute,
		CircuitBreakerFailureThreshold: 10,
		PolicyVersion:                  "v1.0.0",
	}
	h := setupTestHarness(t, &escPolicy)
	ctx := context.Background()
	now := h.clock.Now()

	inc := makeIncident("node-ceiling", "cpu_usage_percent", types.SeverityHigh, now)
	_, _ = h.incEngine.RegisterIncident(ctx, inc)

	failExec := &alwaysFailExecutor{}
	cfg := orchestrator.DefaultConfig() // MaxExecutionCeiling = 4
	if cfg.MaxExecutionCeiling != 4 {
		t.Fatalf("expected DefaultConfig.MaxExecutionCeiling == 4, got %d", cfg.MaxExecutionCeiling)
	}

	orch, _ := orchestrator.NewOrchestrator(
		h.policy,
		h.validator,
		failExec,
		h.vEngine,
		h.escEngine,
		h.incEngine,
		h.store,
		cfg,
		h.clock,
	)

	res, err := orch.OrchestrateIncident(ctx, inc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Initial execution (1) + 3 retries = 4 total executions
	if failExec.calls != 4 {
		t.Errorf("expected exactly 4 executions (1 initial + 3 retries), got %d", failExec.calls)
	}
	if res.State != orchestrator.StateEscalated {
		t.Errorf("expected StateEscalated after budget exhausted, got %s", res.State)
	}
}

// Scenario K: Duplicate orchestration -> one logical execution
func TestOrchestrator_ScenarioK_DuplicateOrchestration(t *testing.T) {
	h := setupTestHarness(t, nil)
	ctx := context.Background()
	now := h.clock.Now()

	inc := makeIncident("node-dup", "cpu_usage_percent", types.SeverityHigh, now)
	_, _ = h.incEngine.RegisterIncident(ctx, inc)

	sample1 := types.MetricSample{
		NodeID:    "node-dup",
		Name:      "cpu_usage_percent",
		Value:     60.0,
		Timestamp: now.Add(1 * time.Second),
	}
	sample2 := types.MetricSample{
		NodeID:    "node-dup",
		Name:      "cpu_usage_percent",
		Value:     61.0,
		Timestamp: now.Add(2 * time.Second),
	}

	res1, err1 := h.orch.OrchestrateIncident(ctx, inc, orchestrator.WithTelemetrySamples(sample1, sample2))
	if err1 != nil {
		t.Fatalf("call 1 failed: %v", err1)
	}

	simCountAfterFirst := h.executor.SimulationCount()

	// Repeated identical call
	res2, err2 := h.orch.OrchestrateIncident(ctx, inc)
	if err2 != nil {
		t.Fatalf("call 2 failed: %v", err2)
	}

	if res1 != res2 {
		t.Errorf("expected identical result pointer for duplicate call")
	}
	if h.executor.SimulationCount() != simCountAfterFirst {
		t.Errorf("expected zero additional simulations on duplicate call, got %d vs %d",
			h.executor.SimulationCount(), simCountAfterFirst)
	}
}

// Scenario L: Concurrent duplicate orchestration -> 20 goroutines, one logical execution
func TestOrchestrator_ScenarioL_ConcurrentDuplicateOrchestration(t *testing.T) {
	h := setupTestHarness(t, nil)
	ctx := context.Background()
	now := h.clock.Now()

	inc := makeIncident("node-conc", "cpu_usage_percent", types.SeverityHigh, now)
	_, _ = h.incEngine.RegisterIncident(ctx, inc)

	sample1 := types.MetricSample{
		NodeID:    "node-conc",
		Name:      "cpu_usage_percent",
		Value:     60.0,
		Timestamp: now.Add(1 * time.Second),
	}
	sample2 := types.MetricSample{
		NodeID:    "node-conc",
		Name:      "cpu_usage_percent",
		Value:     61.0,
		Timestamp: now.Add(2 * time.Second),
	}

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)

	results := make([]*orchestrator.OrchestrationResult, goroutines)
	errorsList := make([]error, goroutines)

	for i := 0; i < goroutines; i++ {
		idx := i
		go func() {
			defer wg.Done()
			results[idx], errorsList[idx] = h.orch.OrchestrateIncident(ctx, inc,
				orchestrator.WithTelemetrySamples(sample1, sample2),
			)
		}()
	}

	wg.Wait()

	// Verify all returned without error and identical state
	for i := 0; i < goroutines; i++ {
		if errorsList[i] != nil {
			t.Errorf("goroutine %d failed: %v", i, errorsList[i])
		}
		if results[i] == nil || results[i].State != orchestrator.StateRecovered {
			t.Errorf("goroutine %d did not observe StateRecovered", i)
		}
	}

	if h.executor.SimulationCount() != 1 {
		t.Errorf("expected exactly 1 simulation execution across 20 concurrent goroutines, got %d", h.executor.SimulationCount())
	}

	// Also verify isolation between concurrent DIFFERENT incidents
	incB := makeIncident("node-conc-b", "memory_usage_percent", types.SeverityCritical, now)
	_, _ = h.incEngine.RegisterIncident(ctx, incB)
	sampleB1 := types.MetricSample{
		NodeID:    "node-conc-b",
		Name:      "memory_usage_percent",
		Value:     50.0,
		Timestamp: now.Add(1 * time.Second),
	}
	sampleB2 := types.MetricSample{
		NodeID:    "node-conc-b",
		Name:      "memory_usage_percent",
		Value:     51.0,
		Timestamp: now.Add(2 * time.Second),
	}
	resB, errB := h.orch.OrchestrateIncident(ctx, incB, orchestrator.WithTelemetrySamples(sampleB1, sampleB2))
	if errB != nil {
		t.Fatalf("incident B orchestration failed: %v", errB)
	}
	if resB.State != orchestrator.StateRecovered {
		t.Errorf("incident B did not recover, got %s", resB.State)
	}
	if h.executor.SimulationCount() != 2 {
		t.Errorf("expected 2 total simulations (1 for A, 1 for B), got %d", h.executor.SimulationCount())
	}
}

// Scenario M: Cancellation -> no additional execution
func TestOrchestrator_ScenarioM_Cancellation(t *testing.T) {
	h := setupTestHarness(t, nil)
	now := h.clock.Now()

	inc := makeIncident("node-cancel", "cpu_usage_percent", types.SeverityHigh, now)
	_, _ = h.incEngine.RegisterIncident(context.Background(), inc)

	// Pre-cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res, err := h.orch.OrchestrateIncident(ctx, inc)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	if res.State != orchestrator.StateCancelled {
		t.Errorf("expected StateCancelled, got %s", res.State)
	}
	if h.executor.SimulationCount() != 0 {
		t.Errorf("expected 0 simulations on cancelled context, got %d", h.executor.SimulationCount())
	}
}

// Scenario N: Recovered incident -> no retry/execution
func TestOrchestrator_ScenarioN_RecoveredIncident(t *testing.T) {
	h := setupTestHarness(t, nil)
	ctx := context.Background()
	now := h.clock.Now()

	inc := makeIncident("node-rec", "cpu_usage_percent", types.SeverityHigh, now)
	inc.Status = types.StatusRecovered // Already recovered!

	res, err := h.orch.OrchestrateIncident(ctx, inc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.State != orchestrator.StateRecovered {
		t.Errorf("expected StateRecovered for already-recovered incident, got %s", res.State)
	}
	if h.executor.SimulationCount() != 0 {
		t.Errorf("expected 0 executions for recovered incident, got %d", h.executor.SimulationCount())
	}
}

// Scenario O: Stale incident -> no invalid Incident transition
func TestOrchestrator_ScenarioO_StaleIncident(t *testing.T) {
	h := setupTestHarness(t, nil)
	ctx := context.Background()
	now := h.clock.Now()

	// Incident with NORMAL status
	inc := makeIncident("node-norm", "cpu_usage_percent", types.SeverityHigh, now)
	inc.Status = types.StatusNormal

	res, err := h.orch.OrchestrateIncident(ctx, inc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.State != orchestrator.StateRecovered {
		t.Errorf("expected non-actionable suppression, got %s", res.State)
	}

	// Verify IncidentEngine did NOT perform an invalid transition
	active := h.incEngine.GetActiveIncident("node-norm", "cpu_usage_percent")
	if active != nil {
		t.Errorf("expected no active incident registered, found %+v", active)
	}
}

// Scenario P: Unknown reconciliation state -> no unsafe retry
func TestOrchestrator_ScenarioP_UnknownReconciliation(t *testing.T) {
	h := setupTestHarness(t, nil)
	ctx := context.Background()
	now := h.clock.Now()

	inc := makeIncident("node-unk", "cpu_usage_percent", types.SeverityHigh, now)
	_, _ = h.incEngine.RegisterIncident(ctx, inc)

	// Pre-insert mitigation in UNKNOWN_RECONCILIATION_REQUIRED
	err := h.store.RecordMitigation(ctx, &storage.StoredMitigation{
		ActionID:    "act-unk-01",
		DecisionID:  "dec-unk-01",
		IncidentID:  inc.IncidentID,
		NodeID:      inc.NodeID,
		ActionType:  types.ActionSimulatedThrottle,
		Target:      "telemetry_generator",
		Status:      types.MitigationStatusUnknownReconciliationRequired,
		Mode:        "AUTO_EXECUTE",
		StartedAt:   now,
		CompletedAt: &now,
		CreatedAt:   now,
		UpdatedAt:   now,
	})
	if err != nil {
		t.Fatalf("RecordMitigation failed: %v", err)
	}

	res, err := h.orch.OrchestrateIncident(ctx, inc)
	if !errors.Is(err, orchestrator.ErrUnknownReconciliation) {
		t.Errorf("expected ErrUnknownReconciliation, got %v", err)
	}
	if res.State != orchestrator.StateFailed {
		t.Errorf("expected StateFailed, got %s", res.State)
	}
	if h.executor.SimulationCount() != 0 {
		t.Errorf("expected 0 new executions when unknown reconciliation present, got %d", h.executor.SimulationCount())
	}
}

// Scenario Q: Verification timeout -> escalation evaluation
func TestOrchestrator_ScenarioQ_VerificationTimeout(t *testing.T) {
	escPolicy := escalation.EscalationPolicy{
		MaxAutomaticRetries:            1,
		RetryCooldown:                  0,
		FailureWindow:                  30 * time.Minute,
		CircuitBreakerFailureThreshold: 5,
		PolicyVersion:                  "v1.0.0",
	}
	h := setupTestHarness(t, &escPolicy)
	ctx := context.Background()
	now := h.clock.Now()

	inc := makeIncident("node-timeout", "cpu_usage_percent", types.SeverityHigh, now)
	_, _ = h.incEngine.RegisterIncident(ctx, inc)

	// Orchestrate WITHOUT samples (verification starts in PENDING)
	res, err := h.orch.OrchestrateIncident(ctx, inc, orchestrator.WithVerificationTimeout(1*time.Minute))
	if err != nil {
		t.Fatalf("initial orchestration failed: %v", err)
	}
	if res.State != orchestrator.StateVerifying {
		t.Errorf("expected StateVerifying, got %s", res.State)
	}

	// Advance clock past verification timeout
	h.clock.Advance(2 * time.Minute)

	// Reconcile timeouts in verification engine
	timedOut, err := h.vEngine.ReconcileTimeouts(ctx, h.clock.Now())
	if err != nil || timedOut != 1 {
		t.Fatalf("ReconcileTimeouts expected 1, got %d (err: %v)", timedOut, err)
	}

	// Feed a subsequent sample to trigger ProcessTelemetry
	sample := types.MetricSample{
		NodeID:    "node-timeout",
		Name:      "cpu_usage_percent",
		Value:     88.0,
		Timestamp: h.clock.Now(),
	}
	vRes, _ := h.orch.ProcessTelemetry(ctx, sample)
	if vRes != nil && vRes.Status != types.VerificationStatusTimedOut {
		t.Errorf("expected verification status TIMED_OUT, got %s", vRes.Status)
	}
}

// Scenario R: Verification recovered -> orchestration stops
func TestOrchestrator_ScenarioR_VerificationRecovered_Stops(t *testing.T) {
	h := setupTestHarness(t, nil)
	ctx := context.Background()
	now := h.clock.Now()

	inc := makeIncident("node-rec-stop", "cpu_usage_percent", types.SeverityHigh, now)
	_, _ = h.incEngine.RegisterIncident(ctx, inc)

	// Step 1: Start orchestration without telemetry (enters StateVerifying)
	res1, err := h.orch.OrchestrateIncident(ctx, inc)
	if err != nil {
		t.Fatalf("orchestration step 1 failed: %v", err)
	}
	if res1.State != orchestrator.StateVerifying {
		t.Errorf("expected StateVerifying, got %s", res1.State)
	}

	// Step 2: Feed recovering telemetry samples through ProcessTelemetry
	s1 := types.MetricSample{
		NodeID:    "node-rec-stop",
		Name:      "cpu_usage_percent",
		Value:     60.0,
		Timestamp: now.Add(1 * time.Second),
	}
	s2 := types.MetricSample{
		NodeID:    "node-rec-stop",
		Name:      "cpu_usage_percent",
		Value:     62.0,
		Timestamp: now.Add(2 * time.Second),
	}

	_, _ = h.orch.ProcessTelemetry(ctx, s1)
	vRes2, err := h.orch.ProcessTelemetry(ctx, s2)
	if err != nil {
		t.Fatalf("ProcessTelemetry s2 failed: %v", err)
	}
	if vRes2.Status != types.VerificationStatusRecovered {
		t.Errorf("expected VerificationStatusRecovered, got %s", vRes2.Status)
	}

	// Check that orchestrator state is now StateRecovered
	st, ok := h.orch.GetOrchestrationState(inc.IncidentID)
	if !ok || st != orchestrator.StateRecovered {
		t.Errorf("expected cached state StateRecovered, got %s (found: %v)", st, ok)
	}

	// Subsequent call to OrchestrateIncident terminates immediately without new execution
	simsBefore := h.executor.SimulationCount()
	res3, _ := h.orch.OrchestrateIncident(ctx, inc)
	if res3.State != orchestrator.StateRecovered {
		t.Errorf("expected StateRecovered, got %s", res3.State)
	}
	if h.executor.SimulationCount() != simsBefore {
		t.Errorf("expected 0 new simulations after recovery, got %d vs %d", h.executor.SimulationCount(), simsBefore)
	}
}

// Scenario S: Restart persistence -> orchestration does not lose mitigation identity/state
func TestOrchestrator_ScenarioS_RestartPersistence(t *testing.T) {
	dbDir := t.TempDir()
	dbPath := filepath.Join(dbDir, "restart.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	clock := orchestrator.NewFakeClock(now)

	policy := response.NewDefaultPolicy()
	_ = policy.SetExecutionMode(response.ExecutionModeAutoExecute)
	validator := response.NewStandardValidator()
	executor := response.NewSimulatedExecutor(validator)
	executor.WithClock(clock)

	vCfg := verification.DefaultConfig()
	vCfg.Store = store
	vCfg.Clock = clock
	vCfg.RequiredConsecutiveObservations = 2
	vEng, _ := verification.NewLocalEngine(vCfg)

	escEng, _ := escalation.NewEngine(store, escalation.DefaultPolicy(), clock)
	incCfg := incident.DefaultPolicyConfig()
	incEng, _ := incident.NewLocalEngine(incCfg)

	cfg := orchestrator.DefaultConfig()
	cfg.DefaultRequiredObservations = 2
	orch1, _ := orchestrator.NewOrchestrator(policy, validator, executor, vEng, escEng, incEng, store, cfg, clock)

	ctx := context.Background()
	inc := makeIncident("node-restart", "cpu_usage_percent", types.SeverityHigh, now)
	_, _ = incEng.RegisterIncident(ctx, inc)

	sample1 := types.MetricSample{NodeID: "node-restart", Name: "cpu_usage_percent", Value: 60.0, Timestamp: now.Add(1 * time.Second)}
	sample2 := types.MetricSample{NodeID: "node-restart", Name: "cpu_usage_percent", Value: 61.0, Timestamp: now.Add(2 * time.Second)}

	res1, err := orch1.OrchestrateIncident(ctx, inc, orchestrator.WithTelemetrySamples(sample1, sample2))
	if err != nil || res1.State != orchestrator.StateRecovered {
		t.Fatalf("orch1 failed: %v, state: %s", err, res1.State)
	}

	// Verify mitigation is durably in SQLite
	mitigations, err := store.ListMitigations(ctx, inc.IncidentID, 10)
	if err != nil || len(mitigations) != 1 {
		t.Fatalf("expected 1 persisted mitigation, got %d", len(mitigations))
	}
	savedActionID := mitigations[0].ActionID

	// Simulate full process restart with new Orchestrator instance pointing to the same SQLite file
	orch2, err := orchestrator.NewOrchestrator(policy, validator, executor, vEng, escEng, incEng, store, cfg, clock)
	if err != nil {
		t.Fatalf("NewOrchestrator after restart failed: %v", err)
	}

	// Verify that orch2 recognizes recovered state and produces identical result
	res2, err := orch2.OrchestrateIncident(ctx, inc)
	if err != nil || res2.State != orchestrator.StateRecovered {
		t.Fatalf("orch2 failed: %v, state: %s", err, res2.State)
	}

	// Fetch mitigation from store via second orchestrator dependency
	recoveredMits, err := store.ListMitigations(ctx, inc.IncidentID, 10)
	if err != nil || len(recoveredMits) != 1 {
		t.Fatalf("expected 1 recovered mitigation, got %d", len(recoveredMits))
	}
	if recoveredMits[0].ActionID != savedActionID {
		t.Errorf("action identity mismatch after restart: got %s, want %s", recoveredMits[0].ActionID, savedActionID)
	}
	_ = store.Close()
}

// Scenario T: Static security check -> verifies zero real host execution APIs
func TestOrchestrator_ScenarioT_StaticSecurityCheck(t *testing.T) {
	orchestratorSrcPath := "orchestrator.go"
	data, err := os.ReadFile(orchestratorSrcPath)
	if err != nil {
		t.Fatalf("failed to read %s: %v", orchestratorSrcPath, err)
	}
	content := strings.ToLower(string(data))

	bannedTerms := []string{
		"os/exec",
		"exec.command",
		"powershell",
		"cmd.exe",
		"ssh",
		"docker",
		"kubectl",
		"aws",
	}

	for _, term := range bannedTerms {
		if strings.Contains(content, term) {
			t.Errorf("CRITICAL SECURITY VIOLATION: prohibited term %q found in %s", term, orchestratorSrcPath)
		}
	}
}

// Event handler observability verification
func TestOrchestrator_EventObservability(t *testing.T) {
	h := setupTestHarness(t, nil)
	ctx := context.Background()
	now := h.clock.Now()

	var eventsReceived int64
	h.orch.RegisterEventHandler(func(event orchestrator.OrchestrationEvent) {
		atomic.AddInt64(&eventsReceived, 1)
		if event.IncidentID == "" {
			t.Errorf("event missing IncidentID")
		}
		if event.NodeID == "" {
			t.Errorf("event missing NodeID")
		}
	})

	inc := makeIncident("node-obs", "cpu_usage_percent", types.SeverityHigh, now)
	_, _ = h.incEngine.RegisterIncident(ctx, inc)

	sample1 := types.MetricSample{NodeID: "node-obs", Name: "cpu_usage_percent", Value: 60.0, Timestamp: now.Add(1 * time.Second)}
	sample2 := types.MetricSample{NodeID: "node-obs", Name: "cpu_usage_percent", Value: 61.0, Timestamp: now.Add(2 * time.Second)}

	_, err := h.orch.OrchestrateIncident(ctx, inc, orchestrator.WithTelemetrySamples(sample1, sample2))
	if err != nil {
		t.Fatalf("orchestration failed: %v", err)
	}

	if atomic.LoadInt64(&eventsReceived) == 0 {
		t.Errorf("expected at least 1 auditable orchestration event, got 0")
	}
}

// TestOrchestrator_MetricsObservability verifies that orchestrator operations emit Prometheus-compatible metrics.
func TestOrchestrator_MetricsObservability(t *testing.T) {
	h := setupTestHarness(t, nil)
	ctx := context.Background()
	now := h.clock.Now()

	metricRec := metrics.NewDefaultRecorder(nil)
	h.orch.WithMetricsRecorder(metricRec)

	inc := makeIncident("node-metrics-01", "cpu_usage_percent", types.SeverityHigh, now)
	_, _ = h.incEngine.RegisterIncident(ctx, inc)

	sample1 := types.MetricSample{NodeID: "node-metrics-01", Name: "cpu_usage_percent", Value: 60.0, Timestamp: now.Add(1 * time.Second)}
	sample2 := types.MetricSample{NodeID: "node-metrics-01", Name: "cpu_usage_percent", Value: 61.0, Timestamp: now.Add(2 * time.Second)}

	res, err := h.orch.OrchestrateIncident(ctx, inc, orchestrator.WithTelemetrySamples(sample1, sample2))
	if err != nil {
		t.Fatalf("orchestration failed: %v", err)
	}
	if res.State != orchestrator.StateRecovered {
		t.Fatalf("expected StateRecovered, got %s", res.State)
	}

	out := metricRec.Registry().FormatPrometheus()

	// Verify required operational metrics are present and observed
	expectedMetrics := []string{
		`aegisedge_response_decisions_total{authorization_class="AUTO_EXECUTE"} 1`,
		`aegisedge_safety_validations_total{outcome="passed"} 1`,
		`aegisedge_mitigations_total{action_type="SIMULATED_THROTTLE",status="started"} 1`,
		`aegisedge_mitigations_total{action_type="SIMULATED_THROTTLE",status="executed"} 1`,
		`aegisedge_mitigation_duration_seconds_count{action_type="SIMULATED_THROTTLE"} 1`,
		`aegisedge_verifications_total{status="started"} 1`,
		`aegisedge_verifications_total{status="recovered"} 1`,
		`aegisedge_orchestrations_total{outcome="completed"} 1`,
		`aegisedge_orchestration_duration_seconds_count 1`,
		`aegisedge_incidents_recovered_total 1`,
		`aegisedge_audit_events_total`,
	}

	for _, em := range expectedMetrics {
		if !strings.Contains(out, em) {
			t.Errorf("missing expected metric in orchestrator output:\n%s\ngot:\n%s", em, out)
		}
	}
}

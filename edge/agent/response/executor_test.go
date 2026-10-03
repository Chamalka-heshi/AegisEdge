package response_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/response"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// TEST: Valid SIMULATED_THROTTLE execution
func TestExecutor_SimulatedThrottle_Success(t *testing.T) {
	ctx := context.Background()
	validator := response.NewStandardValidator()
	executor := response.NewSimulatedExecutor(validator)

	inc := makeTestIncident("inc-ex-001", "node-1", "cpu_usage_percent", types.SeverityCritical, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")

	res, err := executor.Execute(ctx, dec, inc)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if res == nil {
		t.Fatal("expected non-nil ExecutionResult")
	}

	if res.Status != types.MitigationStatusExecuted {
		t.Errorf("Status: got %s, want %s", res.Status, types.MitigationStatusExecuted)
	}
	if !res.Simulated {
		t.Error("expected Simulated to be true")
	}
	if res.DecisionID != dec.DecisionID {
		t.Errorf("DecisionID: got %s, want %s", res.DecisionID, dec.DecisionID)
	}
	if res.IncidentID != inc.IncidentID {
		t.Errorf("IncidentID: got %s, want %s", res.IncidentID, inc.IncidentID)
	}
	if res.NodeID != inc.NodeID {
		t.Errorf("NodeID: got %s, want %s", res.NodeID, inc.NodeID)
	}
	if res.ActionType != types.ActionSimulatedThrottle {
		t.Errorf("ActionType: got %s, want %s", res.ActionType, types.ActionSimulatedThrottle)
	}
	if res.Target != "telemetry_generator" {
		t.Errorf("Target: got %s, want telemetry_generator", res.Target)
	}
	if res.StartedAt.IsZero() || res.CompletedAt.IsZero() {
		t.Error("expected non-zero timestamps")
	}
	if res.Duration < 0 {
		t.Errorf("negative duration: %v", res.Duration)
	}

	// Verify ToMitigationAction mapping
	mit := res.ToMitigationAction()
	if err := mit.Validate(); err != nil {
		t.Fatalf("ToMitigationAction produced invalid contract: %v", err)
	}
	if mit.Status != types.MitigationStatusExecuted {
		t.Errorf("MitigationAction.Status: got %s, want EXECUTED", mit.Status)
	}
}

// TEST: All 4 supported simulated action types
func TestExecutor_AllSupportedActionTypes(t *testing.T) {
	ctx := context.Background()

	actions := []struct {
		actionType types.MitigationActionType
		target     string
		params     map[string]string
	}{
		{types.ActionSimulatedThrottle, "telemetry_generator", map[string]string{"throttle_percent": "50", "duration_sec": "300"}},
		{types.ActionSimulatedRestart, "collector", map[string]string{"grace_period_sec": "10"}},
		{types.ActionSimulatedIsolate, "node-local", map[string]string{}},
		{types.ActionSimulatedAlert, "local_syslog", map[string]string{"priority": "HIGH"}},
	}

	for _, tc := range actions {
		t.Run(string(tc.actionType), func(t *testing.T) {
			validator := response.NewStandardValidator()
			executor := response.NewSimulatedExecutor(validator)

			inc := makeTestIncident("inc-all-"+string(tc.actionType), "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
			dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", tc.actionType, tc.target)
			dec.Parameters = tc.params

			res, err := executor.Execute(ctx, dec, inc)
			if err != nil {
				t.Fatalf("Execute %s failed: %v", tc.actionType, err)
			}
			if res.Status != types.MitigationStatusExecuted {
				t.Fatalf("expected EXECUTED, got %s (msg: %s)", res.Status, res.Message)
			}
			if !res.Simulated {
				t.Error("expected Simulated to be true")
			}
			mit := res.ToMitigationAction()
			if err := mit.Validate(); err != nil {
				t.Fatalf("MitigationAction invalid: %v", err)
			}
		})
	}
}

// TEST: ValidatedDecision construction and ExecuteValidated
func TestExecutor_ValidatedDecisionWrapper(t *testing.T) {
	ctx := context.Background()
	validator := response.NewStandardValidator()
	executor := response.NewSimulatedExecutor(validator)

	inc := makeTestIncident("inc-vd-01", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")

	safetyRes, err := validator.Validate(ctx, dec, inc)
	if err != nil {
		t.Fatalf("Validate error: %v", err)
	}

	// Successful ValidatedDecision creation
	vd, err := response.NewValidatedDecision(dec, safetyRes)
	if err != nil {
		t.Fatalf("NewValidatedDecision failed: %v", err)
	}

	res, err := executor.ExecuteValidated(ctx, vd)
	if err != nil {
		t.Fatalf("ExecuteValidated failed: %v", err)
	}
	if res.Status != types.MitigationStatusExecuted {
		t.Errorf("got status %s, want EXECUTED", res.Status)
	}

	// Rejection when safetyRes.Allowed is false
	safetyResFailed := *safetyRes
	safetyResFailed.Allowed = false
	safetyResFailed.ValidationCode = response.ValidationCodeForbiddenAction
	if _, err := response.NewValidatedDecision(dec, &safetyResFailed); err == nil {
		t.Error("expected NewValidatedDecision to reject disallowed safety result")
	}

	// Rejection when DecisionID mismatches
	safetyResMismatch := *safetyRes
	safetyResMismatch.DecisionID = "dec-different-id"
	if _, err := response.NewValidatedDecision(dec, &safetyResMismatch); err == nil {
		t.Error("expected NewValidatedDecision to reject mismatched DecisionID")
	}

	// Rejection when arguments are nil
	if _, err := response.NewValidatedDecision(nil, safetyRes); err == nil {
		t.Error("expected error for nil decision")
	}
	if _, err := response.NewValidatedDecision(dec, nil); err == nil {
		t.Error("expected error for nil safety result")
	}
}

// TEST: Idempotency (repeated execution of the exact same decision returns cached result)
func TestExecutor_IdempotentExecution(t *testing.T) {
	ctx := context.Background()
	validator := response.NewStandardValidator()
	executor := response.NewSimulatedExecutor(validator)

	inc := makeTestIncident("inc-idem-01", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")

	res1, err := executor.Execute(ctx, dec, inc)
	if err != nil {
		t.Fatalf("first Execute failed: %v", err)
	}

	// Execute again with identical decision
	res2, err := executor.Execute(ctx, dec, inc)
	if err != nil {
		t.Fatalf("second Execute failed: %v", err)
	}

	if res1.ExecutionID != res2.ExecutionID {
		t.Errorf("idempotency violation: ExecutionIDs must match! got %s vs %s", res1.ExecutionID, res2.ExecutionID)
	}
	if res1.StartedAt != res2.StartedAt {
		t.Errorf("idempotency violation: StartedAt must be identical from cache")
	}

	// Verify retrieval via GetExecution
	retrieved, ok := executor.GetExecution(dec.DecisionID)
	if !ok || retrieved.ExecutionID != res1.ExecutionID {
		t.Errorf("GetExecution failed to retrieve cached result")
	}
}

// TEST: Context cancellation before and during execution
func TestExecutor_ContextCancellation(t *testing.T) {
	validator := response.NewStandardValidator()
	executor := response.NewSimulatedExecutor(validator)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	inc := makeTestIncident("inc-cancel-01", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")

	res, err := executor.Execute(ctx, dec, inc)
	if err == nil {
		t.Error("expected error for cancelled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	if res == nil || res.Status != types.MitigationStatusFailed {
		t.Errorf("expected FAILED status on cancellation, got %+v", res)
	}
	if res.ErrorCode != "CONTEXT_CANCELLED" {
		t.Errorf("expected CONTEXT_CANCELLED, got %s", res.ErrorCode)
	}
}

// TEST: Approval-required action does not execute; returns SKIPPED
func TestExecutor_ApprovalRequired_Skipped(t *testing.T) {
	ctx := context.Background()
	validator := response.NewStandardValidator()
	executor := response.NewSimulatedExecutor(validator)

	inc := makeTestIncident("inc-appr-01", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")
	dec.AuthorizationClass = response.AuthClassApprovalRequired

	res, err := executor.Execute(ctx, dec, inc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != types.MitigationStatusSkipped {
		t.Fatalf("CRITICAL: approval required action must be SKIPPED, got %s", res.Status)
	}
	if res.Simulated != true {
		t.Error("expected Simulated to be true")
	}
}

// TEST: Forbidden action does not execute; returns FAILED
func TestExecutor_ForbiddenAction_Failed(t *testing.T) {
	ctx := context.Background()
	validator := response.NewStandardValidator()
	executor := response.NewSimulatedExecutor(validator)

	inc := makeTestIncident("inc-forbid-01", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")
	dec.AuthorizationClass = response.AuthClassForbidden

	res, err := executor.Execute(ctx, dec, inc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != types.MitigationStatusFailed {
		t.Fatalf("CRITICAL: forbidden action must FAIL, got status %s", res.Status)
	}
	if res.ErrorCode != string(response.ValidationCodeForbiddenAction) {
		t.Errorf("expected FORBIDDEN_ACTION error code, got %s", res.ErrorCode)
	}
}

// TEST: Unknown action type fails closed
func TestExecutor_UnknownAction_FailsClosed(t *testing.T) {
	ctx := context.Background()
	validator := response.NewStandardValidator()
	executor := response.NewSimulatedExecutor(validator)

	inc := makeTestIncident("inc-unk-01", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.MitigationActionType("EXECUTE_RAW_SHELL"), "telemetry_generator")

	res, err := executor.Execute(ctx, dec, inc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != types.MitigationStatusFailed {
		t.Fatalf("CRITICAL: unknown action was not FAILED! got status %s", res.Status)
	}
}

// TEST: Nil arguments rejected
func TestExecutor_NilArguments(t *testing.T) {
	ctx := context.Background()
	validator := response.NewStandardValidator()
	executor := response.NewSimulatedExecutor(validator)

	inc := makeTestIncident("inc-nil-ex", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")

	if _, err := executor.Execute(ctx, nil, inc); err == nil {
		t.Error("expected error for nil decision")
	}
	if _, err := executor.Execute(ctx, dec, nil); err == nil {
		t.Error("expected error for nil incident")
	}
}

// TEST: Security injection rejection at execution boundary
func TestExecutor_SecurityInjectionsRejected(t *testing.T) {
	ctx := context.Background()
	validator := response.NewStandardValidator()
	executor := response.NewSimulatedExecutor(validator)

	inc := makeTestIncident("inc-sec-ex", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)

	injections := []struct {
		name       string
		target     string
		paramKey   string
		paramValue string
	}{
		{"shell injection in target", "telemetry_generator; rm -rf /", "k", "v"},
		{"backticks in target", "telemetry`reboot`", "k", "v"},
		{"powershell execution", "powershell.exe -Command Stop-Computer", "k", "v"},
		{"raw bash target", "bash", "k", "v"},
		{"eval in parameter", "telemetry_generator", "eval", "eval('exploit')"},
		{"shell script in parameter", "telemetry_generator", "cmd", "/bin/sh -c 'id'"},
	}

	for _, tc := range injections {
		t.Run(tc.name, func(t *testing.T) {
			dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, tc.target)
			dec.Parameters[tc.paramKey] = tc.paramValue

			res, err := executor.Execute(ctx, dec, inc)
			if err != nil {
				t.Fatalf("unexpected execution error: %v", err)
			}
			if res.Status == types.MitigationStatusExecuted {
				t.Fatalf("SECURITY BREACH: injection %q was executed by SimulatedExecutor!", tc.name)
			}
			if res.Status != types.MitigationStatusFailed {
				t.Errorf("expected status FAILED, got %s", res.Status)
			}
		})
	}
}

// CONCURRENCY TEST: Multiple goroutines executing the exact same decision concurrently
func TestExecutor_ConcurrentIdempotentAccess(t *testing.T) {
	ctx := context.Background()
	validator := response.NewStandardValidator()
	executor := response.NewSimulatedExecutor(validator)

	inc := makeTestIncident("inc-conc-01", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")

	var wg sync.WaitGroup
	workers := 50
	results := make([]*response.ExecutionResult, workers)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			res, err := executor.Execute(ctx, dec, inc)
			if err != nil {
				t.Errorf("worker %d Execute failed: %v", workerID, err)
				return
			}
			results[workerID] = res
		}(w)
	}

	wg.Wait()

	// All workers must receive the exact same ExecutionID and Status
	firstExecutionID := results[0].ExecutionID
	for i, r := range results {
		if r == nil {
			t.Fatalf("worker %d returned nil result", i)
		}
		if r.ExecutionID != firstExecutionID {
			t.Errorf("worker %d got different ExecutionID: %s vs %s", i, r.ExecutionID, firstExecutionID)
		}
		if r.Status != types.MitigationStatusExecuted {
			t.Errorf("worker %d got status %s, want EXECUTED", i, r.Status)
		}
	}

	// Verify that the simulation body ran exactly once across all concurrent callers
	if count := executor.SimulationCount(); count != 1 {
		t.Fatalf("CRITICAL CONCURRENCY VIOLATION: expected simulationCount == 1, got %d", count)
	}
}

// TEST: Simulation success does NOT auto-recover the incident (Incident FSM Invariant)
func TestExecutor_SimulationSuccessDoesNotAutoRecoverIncident(t *testing.T) {
	ctx := context.Background()
	validator := response.NewStandardValidator()
	executor := response.NewSimulatedExecutor(validator)

	inc := makeTestIncident("inc-fsm-01", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")

	res, err := executor.Execute(ctx, dec, inc)
	if err != nil || res.Status != types.MitigationStatusExecuted {
		t.Fatalf("Execute failed: res=%+v, err=%v", res, err)
	}

	// Architectural Invariant: Incident status must NOT become RECOVERED
	if inc.Status == types.StatusRecovered {
		t.Fatalf("INVARIANT VIOLATED: Simulation success must NOT transition incident to RECOVERED without telemetry verification!")
	}
	if inc.Status != types.StatusAnomalyDetected {
		t.Errorf("expected incident status to remain ANOMALY_DETECTED, got %s", inc.Status)
	}
}

// FULL PIPELINE TEST: Policy -> Decision -> Validator -> Executor
func TestFullPipeline_PolicyToValidatorToExecutor(t *testing.T) {
	ctx := context.Background()
	policy := response.NewDefaultPolicy()
	validator := response.NewStandardValidator()
	executor := response.NewSimulatedExecutor(validator)

	// 1. Actionable incident
	inc := makeTestIncident("inc-pipe-01", "node-1", "cpu_usage_percent", types.SeverityCritical, types.StatusAnomalyDetected)

	// 2. Deterministic policy evaluation
	dec, err := policy.Evaluate(ctx, inc)
	if err != nil {
		t.Fatalf("Policy Evaluate failed: %v", err)
	}

	// 3. Executor dispatches re-validation and simulation
	res, err := executor.Execute(ctx, dec, inc)
	if err != nil {
		t.Fatalf("Executor Execute failed: %v", err)
	}
	if res.Status != types.MitigationStatusExecuted {
		t.Fatalf("expected EXECUTED, got %s (msg: %s)", res.Status, res.Message)
	}
	if !res.Simulated {
		t.Error("expected Simulated to be true")
	}

	// 4. Map to MitigationAction and append to Incident mitigations
	mit := res.ToMitigationAction()
	inc.Mitigations = append(inc.Mitigations, mit)

	// 5. Validate that incident payload with mitigation satisfies contracts
	if err := inc.Validate(); err != nil {
		t.Fatalf("Incident validation failed after appending MitigationAction: %v", err)
	}
}

// TEST: Full lifecycle with durable SQLite MitigationStore and restart retrieval
func TestExecutor_WithDurableStore_FullLifecycleAndRestart(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "exec_durable.db")

	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	validator := response.NewStandardValidator()
	executor1 := response.NewSimulatedExecutorWithStore(validator, store)

	inc := makeTestIncident("inc-dur-01", "node-1", "cpu_usage_percent", types.SeverityCritical, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")

	// 1. Initial execution with durable store
	res1, err := executor1.Execute(ctx, dec, inc)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if res1.Status != types.MitigationStatusExecuted {
		t.Fatalf("expected EXECUTED status, got %s", res1.Status)
	}
	if executor1.SimulationCount() != 1 {
		t.Fatalf("expected simulationCount == 1, got %d", executor1.SimulationCount())
	}

	// 2. Verify durable SQLite persistence
	stored, err := store.GetMitigationByDecisionID(ctx, dec.DecisionID)
	if err != nil {
		t.Fatalf("GetMitigationByDecisionID failed: %v", err)
	}
	if stored.Status != types.MitigationStatusExecuted {
		t.Errorf("stored status mismatch: got %s, want EXECUTED", stored.Status)
	}
	if stored.ActionID != res1.ToMitigationAction().ActionID {
		t.Errorf("stored ActionID mismatch: got %s, want %s", stored.ActionID, res1.ToMitigationAction().ActionID)
	}

	// 3. Simulate process restart by constructing a new executor instance (empty in-memory cache)
	executor2 := response.NewSimulatedExecutorWithStore(validator, store)

	// 4. Execute the same decision on the restarted executor
	res2, err := executor2.Execute(ctx, dec, inc)
	if err != nil {
		t.Fatalf("Execute on restarted executor failed: %v", err)
	}

	// 5. Verify retrieved from durable store without re-executing simulation
	if executor2.SimulationCount() != 0 {
		t.Fatalf("IDEMPOTENCY FAILURE: restarted executor ran simulation (count=%d), expected 0 (retrieved from store)", executor2.SimulationCount())
	}
	if res2.ExecutionID != res1.ExecutionID {
		t.Errorf("ExecutionID mismatch across restart: got %s, want %s", res2.ExecutionID, res1.ExecutionID)
	}
	if res2.Status != types.MitigationStatusExecuted {
		t.Errorf("Status mismatch across restart: got %s, want EXECUTED", res2.Status)
	}

	// 6. Architectural Invariant: Incident status must NOT become RECOVERED
	if inc.Status != types.StatusAnomalyDetected {
		t.Errorf("INVARIANT VIOLATED: incident status changed to %s, expected ANOMALY_DETECTED", inc.Status)
	}
}

// TEST: Approval-required decision with durable store is persisted as SKIPPED
func TestExecutor_WithDurableStore_ApprovalRequired_PersistedSkipped(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "exec_approval_skipped.db")

	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	validator := response.NewStandardValidator()
	executor := response.NewSimulatedExecutorWithStore(validator, store)

	inc := makeTestIncident("inc-appr-dur", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")
	dec.AuthorizationClass = response.AuthClassApprovalRequired

	res, err := executor.Execute(ctx, dec, inc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != types.MitigationStatusSkipped {
		t.Fatalf("expected SKIPPED status, got %s", res.Status)
	}

	// Verify persisted as SKIPPED in store
	stored, err := store.GetMitigationByDecisionID(ctx, dec.DecisionID)
	if err != nil {
		t.Fatalf("GetMitigationByDecisionID failed: %v", err)
	}
	if stored.Status != types.MitigationStatusSkipped {
		t.Errorf("expected stored status SKIPPED, got %s", stored.Status)
	}
}

// TEST: Rejected decision with durable store is persisted as FAILED
func TestExecutor_WithDurableStore_Rejected_PersistedFailed(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "exec_rejected_failed.db")

	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	validator := response.NewStandardValidator()
	executor := response.NewSimulatedExecutorWithStore(validator, store)

	inc := makeTestIncident("inc-forbid-dur", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")
	dec.AuthorizationClass = response.AuthClassForbidden

	res, err := executor.Execute(ctx, dec, inc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Status != types.MitigationStatusFailed {
		t.Fatalf("expected FAILED status, got %s", res.Status)
	}

	// Verify persisted as FAILED in store
	stored, err := store.GetMitigationByDecisionID(ctx, dec.DecisionID)
	if err != nil {
		t.Fatalf("GetMitigationByDecisionID failed: %v", err)
	}
	if stored.Status != types.MitigationStatusFailed {
		t.Errorf("expected stored status FAILED, got %s", stored.Status)
	}
}

// TEST: Post-restart recovery of in-flight execution to UNKNOWN_RECONCILIATION_REQUIRED
func TestExecutor_WithDurableStore_CrashRecoveryToUnknown(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "exec_crash_recovery.db")

	// 1. First run: record mitigation in EXECUTING state
	store1, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	now := time.Now().UTC()
	inFlightMit := &storage.StoredMitigation{
		ActionID:   "act-crash-exec-1",
		DecisionID: "dec-crash-exec-1",
		IncidentID: "inc-crash-1",
		NodeID:     "node-1",
		ActionType: types.ActionSimulatedThrottle,
		Target:     "telemetry_generator",
		Status:     types.MitigationStatusExecuting,
		Mode:       "AUTO_EXECUTE",
		Message:    "simulated execution in progress",
		StartedAt:  now,
		Simulated:  true,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err := store1.RecordMitigation(ctx, inFlightMit); err != nil {
		t.Fatalf("RecordMitigation failed: %v", err)
	}
	// Simulate crash: close store while in EXECUTING state
	_ = store1.Close()

	// 2. Process restarts: open store and run startup recovery
	store2, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("reopening store failed: %v", err)
	}
	defer store2.Close()

	recovered, err := store2.RecoverInFlightMitigations(ctx)
	if err != nil {
		t.Fatalf("RecoverInFlightMitigations failed: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("expected 1 recovered in-flight record, got %d", recovered)
	}

	// 3. Construct new executor on restarted node
	validator := response.NewStandardValidator()
	executor := response.NewSimulatedExecutorWithStore(validator, store2)

	inc := makeTestIncident("inc-crash-1", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")
	dec.DecisionID = "dec-crash-exec-1"

	// 4. Execute decision: must retrieve existing UNKNOWN record without re-executing simulation
	res, err := executor.Execute(ctx, dec, inc)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if res.Status != types.MitigationStatusUnknownReconciliationRequired {
		t.Fatalf("CRITICAL: expected status UNKNOWN_RECONCILIATION_REQUIRED, got %s", res.Status)
	}
	if executor.SimulationCount() != 0 {
		t.Fatalf("IDEMPOTENCY FAILURE: simulation was re-executed (count=%d), expected 0", executor.SimulationCount())
	}
}

// ============================================================================
// Phase 6.5 Operator Approval Execution Integration Tests
// ============================================================================

// TEST: ExecuteWithApproval succeeds with valid operator approval, consumes approval, and records EXECUTED
func TestExecutor_ExecuteWithApproval_Success(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "exec_approval_success.db")

	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	validator := response.NewStandardValidator()
	executor := response.NewSimulatedExecutorWithStore(validator, store)

	inc := makeTestIncident("inc-app-01", "node-1", "cpu_usage_percent", types.SeverityCritical, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")
	dec.AuthorizationClass = response.AuthClassApprovalRequired
	dec.ExecutionMode = response.ExecutionModeApprovalRequired

	now := time.Now().UTC()
	req := response.ApprovalRequest{
		Decision:    dec,
		RequestedBy: "operator_dev",
		TTL:         10 * time.Minute,
		Reason:      "manual throttle authorization",
	}
	app, err := response.NewApprovalFromRequest(req, now)
	if err != nil {
		t.Fatalf("NewApprovalFromRequest failed: %v", err)
	}

	// Persist and approve the record
	if err := store.RecordApproval(ctx, app.ToStored()); err != nil {
		t.Fatalf("RecordApproval failed: %v", err)
	}
	if err := store.ApproveApproval(ctx, app.ApprovalID, "operator_supervisor", now); err != nil {
		t.Fatalf("ApproveApproval failed: %v", err)
	}

	// 1. Execute with valid approval
	res, err := executor.ExecuteWithApproval(ctx, dec, inc, app.ApprovalID)
	if err != nil {
		t.Fatalf("ExecuteWithApproval failed: %v", err)
	}
	if res.Status != types.MitigationStatusExecuted {
		t.Fatalf("expected EXECUTED status, got %s", res.Status)
	}
	if executor.SimulationCount() != 1 {
		t.Fatalf("expected simulationCount == 1, got %d", executor.SimulationCount())
	}

	// 2. Verify approval status is now CONSUMED in durable storage
	storedApp, err := store.GetApproval(ctx, app.ApprovalID)
	if err != nil {
		t.Fatalf("GetApproval failed: %v", err)
	}
	if storedApp.Status != types.ApprovalStatusConsumed || storedApp.ConsumedAt == nil {
		t.Fatalf("expected approval status CONSUMED with ConsumedAt, got status=%s, consumedAt=%v", storedApp.Status, storedApp.ConsumedAt)
	}

	// 3. Verify mitigation was recorded in durable storage
	mit, err := store.GetMitigationByDecisionID(ctx, dec.DecisionID)
	if err != nil {
		t.Fatalf("GetMitigationByDecisionID failed: %v", err)
	}
	if mit.Status != types.MitigationStatusExecuted {
		t.Fatalf("stored mitigation status mismatch: got %s, want EXECUTED", mit.Status)
	}
}

// TEST: Replay attack prevention: second execution with already-consumed approval must fail closed
func TestExecutor_ExecuteWithApproval_ReplayAttackPrevented(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "exec_replay_prevented.db")

	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	validator := response.NewStandardValidator()
	executor := response.NewSimulatedExecutorWithStore(validator, store)

	inc := makeTestIncident("inc-replay-01", "node-1", "cpu_usage_percent", types.SeverityCritical, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")
	dec.AuthorizationClass = response.AuthClassApprovalRequired
	dec.ExecutionMode = response.ExecutionModeApprovalRequired

	now := time.Now().UTC()
	req := response.ApprovalRequest{
		Decision:    dec,
		RequestedBy: "operator_dev",
		TTL:         10 * time.Minute,
	}
	app, err := response.NewApprovalFromRequest(req, now)
	if err != nil {
		t.Fatalf("NewApprovalFromRequest failed: %v", err)
	}

	_ = store.RecordApproval(ctx, app.ToStored())
	_ = store.ApproveApproval(ctx, app.ApprovalID, "operator_supervisor", now)

	// First execution succeeds
	res1, err := executor.ExecuteWithApproval(ctx, dec, inc, app.ApprovalID)
	if err != nil || res1.Status != types.MitigationStatusExecuted {
		t.Fatalf("first execution failed: %v", err)
	}
	if executor.SimulationCount() != 1 {
		t.Fatalf("expected simulationCount == 1, got %d", executor.SimulationCount())
	}

	// Create a new decision ID with the SAME approval ID to simulate an operator trying to reuse approval for a new action
	dec2 := *dec
	dec2.DecisionID = "dec-replay-new-id"
	dec2.Parameters = map[string]string{"throttle_percent": "75", "duration_sec": "60"}

	// Replay execution attempt MUST FAIL CLOSED
	_, err = executor.ExecuteWithApproval(ctx, &dec2, inc, app.ApprovalID)
	if err == nil {
		t.Fatalf("expected replay execution attempt to fail closed, but it succeeded")
	}

	// Simulation count must NOT have increased!
	if executor.SimulationCount() != 1 {
		t.Fatalf("CRITICAL SAFETY BREACH: replay attack executed simulated action (simulationCount=%d)", executor.SimulationCount())
	}
}

// TEST: FORBIDDEN action can NEVER be authorized by operator approval
func TestExecutor_ExecuteWithApproval_ForbiddenNeverOverridden(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "exec_forbidden_override.db")

	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	validator := response.NewStandardValidator()
	executor := response.NewSimulatedExecutorWithStore(validator, store)

	inc := makeTestIncident("inc-forb-01", "node-1", "cpu_usage_percent", types.SeverityCritical, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedRestart, "auth_service")
	dec.AuthorizationClass = response.AuthClassForbidden

	// Attempting to execute with an approval ID MUST FAIL CLOSED
	res, err := executor.ExecuteWithApproval(ctx, dec, inc, "app-fake-or-real")
	if !errors.Is(err, response.ErrForbiddenNotOverridable) {
		t.Fatalf("expected ErrForbiddenNotOverridable, got %v", err)
	}
	if res.Status != types.MitigationStatusFailed {
		t.Fatalf("expected FAILED status for forbidden action, got %s", res.Status)
	}
	if executor.SimulationCount() != 0 {
		t.Fatalf("forbidden action executed simulation body! count=%d", executor.SimulationCount())
	}
}

// TEST: Parameter tampering causes decision fingerprint mismatch and fails closed
func TestExecutor_ExecuteWithApproval_ParameterTamperingDetected(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "exec_tampering.db")

	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	validator := response.NewStandardValidator()
	executor := response.NewSimulatedExecutorWithStore(validator, store)

	inc := makeTestIncident("inc-tamp-01", "node-1", "cpu_usage_percent", types.SeverityCritical, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")
	dec.AuthorizationClass = response.AuthClassApprovalRequired
	dec.Parameters = map[string]string{"throttle_percent": "50", "duration_sec": "60"}

	now := time.Now().UTC()
	req := response.ApprovalRequest{
		Decision:    dec,
		RequestedBy: "operator_dev",
		TTL:         10 * time.Minute,
	}
	app, err := response.NewApprovalFromRequest(req, now)
	if err != nil {
		t.Fatalf("NewApprovalFromRequest failed: %v", err)
	}

	_ = store.RecordApproval(ctx, app.ToStored())
	_ = store.ApproveApproval(ctx, app.ApprovalID, "operator_supervisor", now)

	// Tamper with decision parameters prior to execution
	decTampered := *dec
	decTampered.Parameters = map[string]string{"throttle_percent": "99", "duration_sec": "60"}

	// Must fail closed due to fingerprint mismatch
	_, err = executor.ExecuteWithApproval(ctx, &decTampered, inc, app.ApprovalID)
	if !errors.Is(err, response.ErrApprovalDecisionMismatch) {
		t.Fatalf("expected ErrApprovalDecisionMismatch for tampered parameters, got %v", err)
	}
	if executor.SimulationCount() != 0 {
		t.Fatalf("tampered action executed simulation body! count=%d", executor.SimulationCount())
	}
}

// TEST: Auto-delegation in Execute() when approved approval exists in store
func TestExecutor_Execute_AutoDelegatesWhenApproved(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "exec_autodelegate.db")

	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	validator := response.NewStandardValidator()
	executor := response.NewSimulatedExecutorWithStore(validator, store)

	inc := makeTestIncident("inc-auto-01", "node-1", "cpu_usage_percent", types.SeverityCritical, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")
	dec.AuthorizationClass = response.AuthClassApprovalRequired
	dec.ExecutionMode = response.ExecutionModeApprovalRequired

	now := time.Now().UTC()
	req := response.ApprovalRequest{
		Decision:    dec,
		RequestedBy: "operator_dev",
		TTL:         10 * time.Minute,
	}
	app, err := response.NewApprovalFromRequest(req, now)
	if err != nil {
		t.Fatalf("NewApprovalFromRequest failed: %v", err)
	}

	_ = store.RecordApproval(ctx, app.ToStored())
	_ = store.ApproveApproval(ctx, app.ApprovalID, "operator_supervisor", now)

	// Call standard Execute() without passing approvalID explicitly
	res, err := executor.Execute(ctx, dec, inc)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if res.Status != types.MitigationStatusExecuted {
		t.Fatalf("expected EXECUTED status after auto-delegation, got %s", res.Status)
	}
	if executor.SimulationCount() != 1 {
		t.Fatalf("expected simulationCount == 1, got %d", executor.SimulationCount())
	}

	// Verify approval was consumed
	storedApp, err := store.GetApproval(ctx, app.ApprovalID)
	if err != nil || storedApp.Status != types.ApprovalStatusConsumed {
		t.Fatalf("expected approval to be CONSUMED after execution, got %v", storedApp)
	}
}

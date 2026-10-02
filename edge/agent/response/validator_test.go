package response_test

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/response"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

func makeTestDecision(incidentID, nodeID, policyName, policyVer string, actionType types.MitigationActionType, target string) *response.ResponseDecision {
	decID := response.ComputeDecisionID(incidentID, policyName, policyVer, actionType, target)
	return &response.ResponseDecision{
		DecisionID:         decID,
		IncidentID:         incidentID,
		NodeID:             nodeID,
		RuleID:             "rule-test-01",
		ActionType:         actionType,
		Target:             target,
		Parameters:         map[string]string{"throttle_percent": "50", "duration_sec": "300"},
		AuthorizationClass: response.AuthClassAutoExecute,
		Reason:             "Test valid decision",
		PolicyName:         policyName,
		PolicyVersion:      policyVer,
		ExecutionMode:      response.ExecutionModeDryRun,
		DecisionTimestamp:  time.Now().UTC(),
	}
}

// TEST: Valid allowlisted action passes safety validation
func TestValidator_ValidAllowlistedAction_Allowed(t *testing.T) {
	ctx := context.Background()
	validator := response.NewStandardValidator()

	inc := makeTestIncident("inc-v-001", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")

	res, err := validator.Validate(ctx, dec, inc)
	if err != nil {
		t.Fatalf("Validate returned unexpected error: %v", err)
	}
	if !res.Allowed {
		t.Fatalf("expected decision to be ALLOWED, but got REJECTED with reason: %s (violations: %v)", res.Reason, res.Violations)
	}
	if res.ValidationCode != response.ValidationCodeAllowed {
		t.Errorf("expected ValidationCode ALLOWED, got %s", res.ValidationCode)
	}
	if len(res.Violations) != 0 {
		t.Errorf("expected 0 violations, got %v", res.Violations)
	}
}

// TEST: Nil incident or nil decision returns error
func TestValidator_NilArguments(t *testing.T) {
	ctx := context.Background()
	validator := response.NewStandardValidator()
	inc := makeTestIncident("inc-nil", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")

	if _, err := validator.Validate(ctx, nil, inc); err == nil {
		t.Error("expected error for nil decision, got nil")
	}
	if _, err := validator.Validate(ctx, dec, nil); err == nil {
		t.Error("expected error for nil incident, got nil")
	}
}

// TEST: Unknown action rejected
func TestValidator_UnknownAction_Rejected(t *testing.T) {
	ctx := context.Background()
	validator := response.NewStandardValidator()

	inc := makeTestIncident("inc-v-002", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.MitigationActionType("KILL_PROCESS"), "collector")

	res, err := validator.Validate(ctx, dec, inc)
	if err != nil {
		t.Fatalf("Validate error: %v", err)
	}
	if res.Allowed {
		t.Fatal("CRITICAL: unknown action was ALLOWED by safety validator!")
	}
	if res.ValidationCode != response.ValidationCodeUnknownActionType {
		t.Errorf("expected UNKNOWN_ACTION_TYPE, got %s", res.ValidationCode)
	}
}

// TEST: Forbidden action rejected
func TestValidator_ForbiddenAction_Rejected(t *testing.T) {
	ctx := context.Background()
	validator := response.NewStandardValidator()

	inc := makeTestIncident("inc-v-forbid", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")
	dec.AuthorizationClass = response.AuthClassForbidden

	res, err := validator.Validate(ctx, dec, inc)
	if err != nil {
		t.Fatalf("Validate error: %v", err)
	}
	if res.Allowed {
		t.Fatal("CRITICAL: forbidden action was ALLOWED by safety validator!")
	}
	if res.ValidationCode != response.ValidationCodeForbiddenAction {
		t.Errorf("expected FORBIDDEN_ACTION, got %s", res.ValidationCode)
	}
}

// TEST: Approval required action rejected (never automatically authorized in Phase 6.2)
func TestValidator_ApprovalRequired_RejectedInPhase62(t *testing.T) {
	ctx := context.Background()
	validator := response.NewStandardValidator()

	inc := makeTestIncident("inc-v-appr", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")
	dec.AuthorizationClass = response.AuthClassApprovalRequired

	res, err := validator.Validate(ctx, dec, inc)
	if err != nil {
		t.Fatalf("Validate error: %v", err)
	}
	if res.Allowed {
		t.Fatal("CRITICAL: approval required action was auto-authorized!")
	}
	if res.ValidationCode != response.ValidationCodeInvalidAuthClass {
		t.Errorf("expected INVALID_AUTHORIZATION_CLASS, got %s", res.ValidationCode)
	}
}

// TEST: Invalid or mismatched IncidentID rejected
func TestValidator_IncidentIDMismatch_Rejected(t *testing.T) {
	ctx := context.Background()
	validator := response.NewStandardValidator()

	inc := makeTestIncident("inc-real-001", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
	dec := makeTestDecision("inc-fake-002", inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")

	res, err := validator.Validate(ctx, dec, inc)
	if err != nil {
		t.Fatalf("Validate error: %v", err)
	}
	if res.Allowed {
		t.Fatal("mismatched IncidentID was allowed by validator")
	}
	if res.ValidationCode != response.ValidationCodeIdentityMismatch {
		t.Errorf("expected IDENTITY_MISMATCH, got %s", res.ValidationCode)
	}
}

// TEST: Missing or mismatched NodeID rejected
func TestValidator_NodeIDMismatch_Rejected(t *testing.T) {
	ctx := context.Background()
	validator := response.NewStandardValidator()

	inc := makeTestIncident("inc-v-003", "node-alpha", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, "node-beta", "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")

	res, err := validator.Validate(ctx, dec, inc)
	if err != nil {
		t.Fatalf("Validate error: %v", err)
	}
	if res.Allowed {
		t.Fatal("mismatched NodeID was allowed by validator")
	}
	if res.ValidationCode != response.ValidationCodeIdentityMismatch {
		t.Errorf("expected IDENTITY_MISMATCH, got %s", res.ValidationCode)
	}
}

// TEST: Unsupported incident states rejected (NORMAL, RECOVERED, ESCALATED)
func TestValidator_IncompatibleIncidentStates_Rejected(t *testing.T) {
	ctx := context.Background()
	validator := response.NewStandardValidator()

	incompatibleStates := []types.IncidentStatus{
		types.StatusNormal,
		types.StatusRecovered,
		types.StatusEscalated,
	}

	for _, st := range incompatibleStates {
		inc := makeTestIncident("inc-v-st", "node-1", "cpu_usage_percent", types.SeverityHigh, st)
		dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")

		res, err := validator.Validate(ctx, dec, inc)
		if err != nil {
			t.Fatalf("Validate error on state %s: %v", st, err)
		}
		if res.Allowed {
			t.Errorf("incident state %s must be REJECTED, but was ALLOWED", st)
		}
		if res.ValidationCode != response.ValidationCodeIncidentStateNotActionable {
			t.Errorf("state %s: expected INCIDENT_STATE_NOT_ACTIONABLE, got %s", st, res.ValidationCode)
		}
	}
}

// TEST: Unrecognized policy name rejected
func TestValidator_UnrecognizedPolicy_Rejected(t *testing.T) {
	ctx := context.Background()
	validator := response.NewStandardValidator()

	inc := makeTestIncident("inc-v-pol", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "malicious_unregistered_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")

	res, err := validator.Validate(ctx, dec, inc)
	if err != nil {
		t.Fatalf("Validate error: %v", err)
	}
	if res.Allowed {
		t.Fatal("unrecognized policy was allowed by validator")
	}
	if res.ValidationCode != response.ValidationCodePolicyMismatch {
		t.Errorf("expected POLICY_MISMATCH, got %s", res.ValidationCode)
	}
}

// TEST: Missing policy version rejected
func TestValidator_MissingPolicyVersion_Rejected(t *testing.T) {
	ctx := context.Background()
	validator := response.NewStandardValidator()

	inc := makeTestIncident("inc-v-ver", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "", types.ActionSimulatedThrottle, "telemetry_generator")

	res, err := validator.Validate(ctx, dec, inc)
	if err != nil {
		t.Fatalf("Validate error: %v", err)
	}
	if res.Allowed {
		t.Fatal("missing policy version was allowed by validator")
	}
}

// TEST: Malformed decision ID rejected (tampered DecisionID)
func TestValidator_TamperedDecisionID_Rejected(t *testing.T) {
	ctx := context.Background()
	validator := response.NewStandardValidator()

	inc := makeTestIncident("inc-v-tamp", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")
	dec.DecisionID = "tampered-id-123"

	res, err := validator.Validate(ctx, dec, inc)
	if err != nil {
		t.Fatalf("Validate error: %v", err)
	}
	if res.Allowed {
		t.Fatal("tampered DecisionID was allowed by validator")
	}
	if res.ValidationCode != response.ValidationCodeInvalidDecisionID {
		t.Errorf("expected INVALID_DECISION_ID, got %s", res.ValidationCode)
	}
}

// TEST: Cooldown rejection and expiration
func TestValidator_CooldownRejection(t *testing.T) {
	ctx := context.Background()
	validator := response.NewStandardValidator()

	inc := makeTestIncident("inc-cool-1", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")

	// Initially, target is not cooling down
	res1, err := validator.Validate(ctx, dec, inc)
	if err != nil || !res1.Allowed {
		t.Fatalf("initial validation failed: res=%+v, err=%v", res1, err)
	}

	// Record execution with 2 second cooldown
	validator.RecordExecution("telemetry_generator", 2*time.Second)

	// Immediate next validation on same target must be rejected with COOLDOWN_ACTIVE
	res2, err := validator.Validate(ctx, dec, inc)
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
	if res2.Allowed {
		t.Fatal("decision was allowed while target was in active cooldown window!")
	}
	if res2.ValidationCode != response.ValidationCodeCooldownActive {
		t.Errorf("expected COOLDOWN_ACTIVE, got %s", res2.ValidationCode)
	}

	// A different target remains unaffected
	decOther := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedAlert, "local_syslog")
	resOther, err := validator.Validate(ctx, decOther, inc)
	if err != nil || !resOther.Allowed {
		t.Errorf("unrelated target was blocked by cooldown: %+v", resOther)
	}

	// Reset cooldowns restores eligibility
	validator.ResetCooldowns()
	res3, err := validator.Validate(ctx, dec, inc)
	if err != nil || !res3.Allowed {
		t.Fatalf("post-reset validation failed: res=%+v, err=%v", res3, err)
	}
}

// TEST: Parameter range checks
func TestValidator_ParameterRangeValidation(t *testing.T) {
	ctx := context.Background()
	validator := response.NewStandardValidator()

	inc := makeTestIncident("inc-v-rng", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)

	// Negative throttle percent
	dec1 := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")
	dec1.Parameters["throttle_percent"] = "-10"
	res1, _ := validator.Validate(ctx, dec1, inc)
	if res1.Allowed {
		t.Error("negative throttle_percent must be rejected")
	}

	// Excessive throttle percent > 100
	dec2 := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")
	dec2.Parameters["throttle_percent"] = "150"
	res2, _ := validator.Validate(ctx, dec2, inc)
	if res2.Allowed {
		t.Error("throttle_percent > 100 must be rejected")
	}

	// Excessive restart grace period > 60
	dec3 := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedRestart, "collector")
	dec3.Parameters["grace_period_sec"] = "120"
	res3, _ := validator.Validate(ctx, dec3, inc)
	if res3.Allowed {
		t.Error("grace_period_sec > 60 must be rejected")
	}
}

// SECURITY TESTS: Injection attempts in Target and Parameters rejected
func TestValidator_SecurityInjectionAttempts_Rejected(t *testing.T) {
	ctx := context.Background()
	validator := response.NewStandardValidator()

	inc := makeTestIncident("inc-sec", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)

	injections := []struct {
		name       string
		target     string
		paramKey   string
		paramValue string
	}{
		{"shell command chaining", "worker; rm -rf /", "k", "v"},
		{"command substitution", "worker`shutdown`", "k", "v"},
		{"pipe redirection", "worker | cat /etc/shadow", "k", "v"},
		{"parameter injection", "telemetry_generator", "cmd", "/bin/sh -c evil"},
		{"eval injection", "telemetry_generator", "script", "eval('delete')"},
		{"powershell execution", "powershell.exe -Command Stop-Computer", "k", "v"},
		{"raw bash target", "bash", "k", "v"},
		{"raw kill target", "kill", "k", "v"},
		{"slash command path", "/bin/kill", "k", "v"},
	}

	for _, tc := range injections {
		t.Run(tc.name, func(t *testing.T) {
			dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, tc.target)
			dec.Parameters[tc.paramKey] = tc.paramValue
			res, err := validator.Validate(ctx, dec, inc)
			if err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}
			if res.Allowed {
				t.Fatalf("SECURITY VIOLATION: injection %q was ALLOWED by safety validator!", tc.name)
			}
		})
	}
}

// TEST: Ensure no generic "Command" or executable string field exists on ResponseDecision
func TestValidator_NoArbitraryCommandField(t *testing.T) {
	valType := reflect.TypeOf(response.ResponseDecision{})
	for i := 0; i < valType.NumField(); i++ {
		field := valType.Field(i)
		fieldName := field.Name
		if fieldName == "Command" || fieldName == "ShellCommand" || fieldName == "Exec" || fieldName == "Script" {
			t.Fatalf("CRITICAL SECURITY INVARIANT VIOLATED: ResponseDecision contains arbitrary executable field %q!", fieldName)
		}
	}
}

// CONCURRENCY TEST: Concurrent validation and cooldown tracking
func TestValidator_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	validator := response.NewStandardValidator()

	var wg sync.WaitGroup
	workers := 20
	iterations := 50

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				incID := fmt.Sprintf("inc-c-%d-%d", workerID, i)
				inc := makeTestIncident(incID, "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
				dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")

				// Concurrently validate
				res, err := validator.Validate(ctx, dec, inc)
				if err != nil {
					t.Errorf("concurrent validate error: %v", err)
					return
				}
				if res == nil {
					t.Errorf("concurrent validate returned nil result")
					return
				}

				// Concurrently record cooldowns
				if i%5 == 0 {
					validator.RecordExecution("telemetry_generator", 10*time.Millisecond)
				}
			}
		}(w)
	}

	wg.Wait()
}

// FULL LOGICAL FLOW TESTS
func TestFullLogicalFlow_IncidentToDecisionToAllowed(t *testing.T) {
	ctx := context.Background()
	policy := response.NewDefaultPolicy()
	validator := response.NewStandardValidator()

	// 1. Valid actionable incident
	inc := makeTestIncident("inc-flow-01", "node-1", "cpu_usage_percent", types.SeverityCritical, types.StatusAnomalyDetected)

	// 2. Policy evaluation
	dec, err := policy.Evaluate(ctx, inc)
	if err != nil {
		t.Fatalf("Policy Evaluate failed: %v", err)
	}
	if dec == nil {
		t.Fatal("expected non-nil decision from policy")
	}

	// 3. Safety validation
	res, err := validator.Validate(ctx, dec, inc)
	if err != nil {
		t.Fatalf("Safety Validate failed: %v", err)
	}
	if !res.Allowed {
		t.Fatalf("full flow failed: decision was unexpectedly rejected: %s", res.Reason)
	}
	if res.ValidationCode != response.ValidationCodeAllowed {
		t.Errorf("expected ALLOWED, got %s", res.ValidationCode)
	}
}

func TestFullLogicalFlow_IncidentToDecisionToRejected(t *testing.T) {
	ctx := context.Background()
	policy := response.NewDefaultPolicy()
	validator := response.NewStandardValidator()

	// 1. Escalated incident (requires human approval)
	inc := makeTestIncident("inc-flow-02", "node-1", "cpu_usage_percent", types.SeverityCritical, types.StatusEscalated)

	// 2. Policy evaluation fails closed
	dec, err := policy.Evaluate(ctx, inc)
	if dec != nil || err == nil {
		t.Fatalf("expected policy to reject escalated incident, got dec=%+v, err=%v", dec, err)
	}

	// 3. Fabricate a rogue decision for an escalated incident and submit to validator
	rogueDec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.ActionSimulatedThrottle, "telemetry_generator")
	res, err := validator.Validate(ctx, rogueDec, inc)
	if err != nil {
		t.Fatalf("Validate error: %v", err)
	}
	if res.Allowed {
		t.Fatal("rogue decision for escalated incident was allowed by validator")
	}
	if res.ValidationCode != response.ValidationCodeIncidentStateNotActionable {
		t.Errorf("expected INCIDENT_STATE_NOT_ACTIONABLE, got %s", res.ValidationCode)
	}
}

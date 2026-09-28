package response_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/response"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

func makeTestDecision(incidentID, nodeID, policyName, policyVer string, actionType types.MitigationActionType, target string) *response.ResponseDecision {
	decID := response.ComputeDecisionID(incidentID, policyName, policyVer, actionType, target)
	return &response.ResponseDecision{
		DecisionID:        decID,
		IncidentID:        incidentID,
		NodeID:            nodeID,
		ActionType:        actionType,
		Target:            target,
		Parameters:        map[string]string{"throttle_percent": "50", "duration_sec": "300"},
		Reason:            "Test valid decision",
		PolicyName:        policyName,
		PolicyVersion:     policyVer,
		ExecutionMode:     response.ExecutionModeDryRun,
		DecisionTimestamp: time.Now().UTC(),
	}
}

// TEST A: Valid allowlisted action passes safety validation
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
	if len(res.Violations) != 0 {
		t.Errorf("expected 0 violations, got %v", res.Violations)
	}
}

// TEST B & C: Unknown or empty action rejected
func TestValidator_UnknownAction_Rejected(t *testing.T) {
	ctx := context.Background()
	validator := response.NewStandardValidator()

	inc := makeTestIncident("inc-v-002", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)

	// Non-allowlisted action
	dec := makeTestDecision(inc.IncidentID, inc.NodeID, "default_edge_response_policy", "1.0.0", types.MitigationActionType("KILL_PROCESS"), "collector")

	res, err := validator.Validate(ctx, dec, inc)
	if err != nil {
		t.Fatalf("Validate error: %v", err)
	}
	if res.Allowed {
		t.Fatal("CRITICAL: unknown action was ALLOWED by safety validator!")
	}
}

// TEST D: Invalid or mismatched IncidentID rejected
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
}

// TEST E: Missing or mismatched NodeID rejected
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
}

// TEST F: Unsupported incident states rejected (NORMAL, RECOVERED, ESCALATED)
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
	}
}

// TEST L: Unrecognized policy name rejected
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
}

// TEST M: Malformed decision ID rejected (tampered DecisionID)
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
}

// TEST N: Parameter range checks
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
		{"parameter injection", "worker", "cmd", "/bin/sh -c evil"},
		{"eval injection", "worker", "script", "eval('delete')"},
		{"powershell execution", "powershell.exe -Command Stop-Computer", "k", "v"},
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
}

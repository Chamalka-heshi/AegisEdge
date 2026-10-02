package response_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/response"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

func makeTestIncident(id, nodeID, metric string, sev types.IncidentSeverity, status types.IncidentStatus) *types.Incident {
	now := time.Now().UTC()
	return &types.Incident{
		IncidentID:    id,
		NodeID:        nodeID,
		RuleName:      "threshold_" + metric,
		Severity:      sev,
		Status:        status,
		Description:   "Test incident for " + metric,
		TriggerMetric: metric,
		TriggerValue:  95.0,
		Threshold:     90.0,
		Evidence: map[string]string{
			"observed": "95.00",
			"limit":    "90.00",
		},
		TriggeredAt: now,
		UpdatedAt:   now,
	}
}

// TEST: Valid actionable incident produces deterministic decision with exact rule match
func TestPolicy_ValidActionableIncident(t *testing.T) {
	ctx := context.Background()
	pol := response.NewDefaultPolicy()

	inc := makeTestIncident("inc-001", "node-1", "cpu_usage_percent", types.SeverityCritical, types.StatusAnomalyDetected)

	dec, err := pol.Evaluate(ctx, inc)
	if err != nil {
		t.Fatalf("Evaluate failed: %v", err)
	}
	if dec == nil {
		t.Fatal("expected non-nil decision")
	}

	if dec.IncidentID != "inc-001" {
		t.Errorf("IncidentID: got %q, want inc-001", dec.IncidentID)
	}
	if dec.NodeID != "node-1" {
		t.Errorf("NodeID: got %q, want node-1", dec.NodeID)
	}
	if dec.ActionType != types.ActionSimulatedThrottle {
		t.Errorf("ActionType: got %q, want SIMULATED_THROTTLE", dec.ActionType)
	}
	if dec.Target != "telemetry_generator" {
		t.Errorf("Target: got %q, want telemetry_generator", dec.Target)
	}
	if dec.ExecutionMode != response.ExecutionModeDryRun {
		t.Errorf("ExecutionMode: got %q, want DRY_RUN", dec.ExecutionMode)
	}
	if dec.AuthorizationClass != response.AuthClassAutoExecute {
		t.Errorf("AuthorizationClass: got %q, want AUTO_EXECUTE", dec.AuthorizationClass)
	}
	if dec.RuleID != "rule-cpu-throttle-01" {
		t.Errorf("RuleID: got %q, want rule-cpu-throttle-01", dec.RuleID)
	}
	if dec.PolicyVersion != "1.0.0" {
		t.Errorf("PolicyVersion: got %q, want 1.0.0", dec.PolicyVersion)
	}
	if dec.Parameters["throttle_percent"] != "50" {
		t.Errorf("throttle_percent: got %q, want 50", dec.Parameters["throttle_percent"])
	}

	// Verify DecisionID matches expected deterministic calculation
	expectedID := response.ComputeDecisionID("inc-001", pol.Name(), pol.Version(), types.ActionSimulatedThrottle, "telemetry_generator")
	if dec.DecisionID != expectedID {
		t.Errorf("DecisionID: got %q, want %q", dec.DecisionID, expectedID)
	}
}

// TEST: Repeated evaluation produces the exact same logical DecisionID (Deterministic & Idempotent)
func TestPolicy_DeterministicDecisionID(t *testing.T) {
	ctx := context.Background()
	pol := response.NewDefaultPolicy()

	inc := makeTestIncident("inc-002", "node-1", "memory_usage_percent", types.SeverityCritical, types.StatusAnomalyDetected)

	dec1, err := pol.Evaluate(ctx, inc)
	if err != nil {
		t.Fatalf("first Evaluate failed: %v", err)
	}

	// Re-evaluate 50 times with identical input
	for i := 0; i < 50; i++ {
		dec2, err := pol.Evaluate(ctx, inc)
		if err != nil {
			t.Fatalf("iteration %d Evaluate failed: %v", i, err)
		}
		if dec2.DecisionID != dec1.DecisionID {
			t.Fatalf("CRITICAL: DecisionID non-deterministic at iter %d! got %s, want %s", i, dec2.DecisionID, dec1.DecisionID)
		}
		if dec2.ActionType != dec1.ActionType || dec2.Target != dec1.Target {
			t.Fatalf("decision content mutated at iter %d", i)
		}
	}
}

// TEST: Different inputs produce different DecisionIDs
func TestPolicy_DifferentInputsProduceDifferentIDs(t *testing.T) {
	id1 := response.ComputeDecisionID("inc-001", "pol", "1.0.0", types.ActionSimulatedThrottle, "target1")
	id2 := response.ComputeDecisionID("inc-002", "pol", "1.0.0", types.ActionSimulatedThrottle, "target1")
	id3 := response.ComputeDecisionID("inc-001", "pol", "1.0.0", types.ActionSimulatedRestart, "target1")
	id4 := response.ComputeDecisionID("inc-001", "pol", "1.0.0", types.ActionSimulatedThrottle, "target2")
	id5 := response.ComputeDecisionID("inc-001", "pol", "2.0.0", types.ActionSimulatedThrottle, "target1")

	ids := map[string]string{
		"id1": id1,
		"id2": id2,
		"id3": id3,
		"id4": id4,
		"id5": id5,
	}

	seen := make(map[string]string)
	for name, val := range ids {
		if prev, ok := seen[val]; ok {
			t.Fatalf("collision between %s and %s: both generated %s", prev, name, val)
		}
		seen[val] = name
	}
}

// TEST: Severity matching and thresholds
func TestPolicy_SeverityMatching(t *testing.T) {
	ctx := context.Background()
	pol := response.NewDefaultPolicy()

	// Memory rule requires CRITICAL. A HIGH memory incident should not trigger restart rule, but should fall back to fallback rule
	incMemHigh := makeTestIncident("inc-mem-high", "node-1", "memory_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
	dec, err := pol.Evaluate(ctx, incMemHigh)
	if err != nil {
		t.Fatalf("Evaluate failed: %v", err)
	}
	// Fallback rule gives SIMULATED_ALERT on local_syslog
	if dec.ActionType != types.ActionSimulatedAlert || dec.Target != "local_syslog" {
		t.Errorf("expected fallback alert for sub-critical memory incident, got action=%s target=%s", dec.ActionType, dec.Target)
	}

	// When raised to CRITICAL, memory restart rule matches
	incMemCrit := makeTestIncident("inc-mem-crit", "node-1", "memory_usage_percent", types.SeverityCritical, types.StatusAnomalyDetected)
	decCrit, err := pol.Evaluate(ctx, incMemCrit)
	if err != nil {
		t.Fatalf("Evaluate failed: %v", err)
	}
	if decCrit.ActionType != types.ActionSimulatedRestart || decCrit.Target != "collector" {
		t.Errorf("expected restart collector for critical memory incident, got action=%s target=%s", decCrit.ActionType, decCrit.Target)
	}
}

// TEST: Precedence rules (specific metric > generic metric, priority, severity, tie-breaker)
func TestPolicy_PrecedenceOrdering(t *testing.T) {
	ctx := context.Background()
	pol := response.NewDefaultPolicy()

	// Configure overlapping rules to verify precedence
	rules := []response.PolicyRule{
		{
			RuleID:             "generic-rule-high-pri",
			Priority:           1000,
			Enabled:            true,
			MetricName:         "*", // Generic wildcard
			MinimumSeverity:    types.SeverityLow,
			ActionType:         types.ActionSimulatedAlert,
			Target:             "generic_logger",
			AuthorizationClass: response.AuthClassAutoExecute,
			PolicyVersion:      "1.0.0",
		},
		{
			RuleID:             "specific-metric-lower-pri",
			Priority:           10,
			Enabled:            true,
			MetricName:         "disk_io_iops", // Specific metric
			MinimumSeverity:    types.SeverityMedium,
			ActionType:         types.ActionSimulatedThrottle,
			Target:             "io_controller",
			AuthorizationClass: response.AuthClassAutoExecute,
			PolicyVersion:      "1.0.0",
		},
	}
	pol.SetRules(rules)

	inc := makeTestIncident("inc-prec-1", "node-1", "disk_io_iops", types.SeverityHigh, types.StatusAnomalyDetected)
	dec, err := pol.Evaluate(ctx, inc)
	if err != nil {
		t.Fatalf("Evaluate failed: %v", err)
	}

	// Specific metric must win over generic wildcard even if generic has higher numerical priority
	if dec.RuleID != "specific-metric-lower-pri" {
		t.Errorf("precedence violation: specific metric rule must win, got %s", dec.RuleID)
	}
	if dec.ActionType != types.ActionSimulatedThrottle {
		t.Errorf("expected throttle action, got %s", dec.ActionType)
	}

	// When metric is something else, generic rule matches
	incOther := makeTestIncident("inc-prec-2", "node-1", "network_rx_bytes", types.SeverityHigh, types.StatusAnomalyDetected)
	decOther, err := pol.Evaluate(ctx, incOther)
	if err != nil {
		t.Fatalf("Evaluate failed: %v", err)
	}
	if decOther.RuleID != "generic-rule-high-pri" {
		t.Errorf("expected generic rule match for unmatched metric, got %s", decOther.RuleID)
	}
}

// TEST: Disabled rules are skipped
func TestPolicy_DisabledRulesSkipped(t *testing.T) {
	ctx := context.Background()
	pol := response.NewDefaultPolicy()

	rules := []response.PolicyRule{
		{
			RuleID:             "disabled-rule-cpu",
			Priority:           500,
			Enabled:            false, // DISABLED
			MetricName:         "cpu_usage_percent",
			MinimumSeverity:    types.SeverityLow,
			ActionType:         types.ActionSimulatedThrottle,
			Target:             "disabled_target",
			AuthorizationClass: response.AuthClassAutoExecute,
			PolicyVersion:      "1.0.0",
		},
		{
			RuleID:             "active-rule-cpu",
			Priority:           100,
			Enabled:            true,
			MetricName:         "cpu_usage_percent",
			MinimumSeverity:    types.SeverityLow,
			ActionType:         types.ActionSimulatedAlert,
			Target:             "local_syslog",
			AuthorizationClass: response.AuthClassAutoExecute,
			PolicyVersion:      "1.0.0",
		},
	}
	pol.SetRules(rules)
	pol.ClearFallbackRule()

	inc := makeTestIncident("inc-dis-1", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
	dec, err := pol.Evaluate(ctx, inc)
	if err != nil {
		t.Fatalf("Evaluate failed: %v", err)
	}
	if dec.RuleID != "active-rule-cpu" {
		t.Fatalf("expected disabled rule to be skipped, got %s", dec.RuleID)
	}
}

// TEST: No matching rule returns ErrNoRuleMatched when fallback is absent or fails severity
func TestPolicy_NoMatchingRule(t *testing.T) {
	ctx := context.Background()
	pol := response.NewDefaultPolicy()
	pol.ClearFallbackRule()
	pol.SetRules([]response.PolicyRule{
		{
			RuleID:             "rule-disk-only",
			Priority:           10,
			Enabled:            true,
			MetricName:         "disk_usage_percent",
			MinimumSeverity:    types.SeverityCritical,
			ActionType:         types.ActionSimulatedAlert,
			Target:             "local_syslog",
			AuthorizationClass: response.AuthClassAutoExecute,
			PolicyVersion:      "1.0.0",
		},
	})

	inc := makeTestIncident("inc-nomatch", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
	dec, err := pol.Evaluate(ctx, inc)
	if dec != nil {
		t.Fatalf("expected nil decision, got %+v", dec)
	}
	if !errors.Is(err, response.ErrNoRuleMatched) {
		t.Fatalf("expected ErrNoRuleMatched, got: %v", err)
	}
}

// TEST: All supported action types can be produced
func TestPolicy_AllSupportedActionTypes(t *testing.T) {
	ctx := context.Background()
	actionTypes := []types.MitigationActionType{
		types.ActionSimulatedThrottle,
		types.ActionSimulatedRestart,
		types.ActionSimulatedIsolate,
		types.ActionSimulatedAlert,
	}

	for _, act := range actionTypes {
		t.Run(string(act), func(t *testing.T) {
			pol := response.NewDefaultPolicy()
			pol.SetRules([]response.PolicyRule{
				{
					RuleID:             "rule-" + string(act),
					Priority:           100,
					Enabled:            true,
					MetricName:         "test_metric",
					MinimumSeverity:    types.SeverityLow,
					ActionType:         act,
					Target:             "collector",
					AuthorizationClass: response.AuthClassAutoExecute,
					PolicyVersion:      "1.0.0",
				},
			})

			inc := makeTestIncident("inc-"+string(act), "node-1", "test_metric", types.SeverityMedium, types.StatusAnomalyDetected)
			dec, err := pol.Evaluate(ctx, inc)
			if err != nil {
				t.Fatalf("Evaluate error for %s: %v", act, err)
			}
			if dec.ActionType != act {
				t.Errorf("got ActionType %s, want %s", dec.ActionType, act)
			}
		})
	}
}

// TEST: Recovered incident does not generate an automatic mitigation
func TestPolicy_RecoveredIncident_Rejected(t *testing.T) {
	ctx := context.Background()
	pol := response.NewDefaultPolicy()

	inc := makeTestIncident("inc-rec-1", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusRecovered)

	dec, err := pol.Evaluate(ctx, inc)
	if dec != nil {
		t.Errorf("expected nil decision for recovered incident, got %+v", dec)
	}
	if !errors.Is(err, response.ErrIncidentRecovered) {
		t.Errorf("expected ErrIncidentRecovered, got: %v", err)
	}
}

// TEST: Escalated incident does not generate an automatic mitigation
func TestPolicy_EscalatedIncident_Rejected(t *testing.T) {
	ctx := context.Background()
	pol := response.NewDefaultPolicy()

	inc := makeTestIncident("inc-esc-1", "node-1", "cpu_usage_percent", types.SeverityCritical, types.StatusEscalated)

	dec, err := pol.Evaluate(ctx, inc)
	if dec != nil {
		t.Errorf("expected nil decision for escalated incident, got %+v", dec)
	}
	if !errors.Is(err, response.ErrIncidentEscalated) {
		t.Errorf("expected ErrIncidentEscalated, got: %v", err)
	}
}

// TEST: Mitigating incident does not create duplicate action
func TestPolicy_MitigatingIncident_Rejected(t *testing.T) {
	ctx := context.Background()
	pol := response.NewDefaultPolicy()

	inc := makeTestIncident("inc-mit-1", "node-1", "cpu_usage_percent", types.SeverityHigh, types.StatusMitigating)

	dec, err := pol.Evaluate(ctx, inc)
	if dec != nil {
		t.Errorf("expected nil decision for mitigating incident, got %+v", dec)
	}
	if !errors.Is(err, response.ErrIncidentAlreadyMitigating) {
		t.Errorf("expected ErrIncidentAlreadyMitigating, got: %v", err)
	}
}

// TEST: Normal incident produces no mitigation
func TestPolicy_NormalIncident_Rejected(t *testing.T) {
	ctx := context.Background()
	pol := response.NewDefaultPolicy()

	inc := makeTestIncident("inc-norm-1", "node-1", "cpu_usage_percent", types.SeverityLow, types.StatusNormal)

	dec, err := pol.Evaluate(ctx, inc)
	if dec != nil {
		t.Errorf("expected nil decision for normal incident, got %+v", dec)
	}
	if !errors.Is(err, response.ErrIncidentNotActionable) {
		t.Errorf("expected ErrIncidentNotActionable, got: %v", err)
	}
}

// TEST: Multiple nodes and multiple incidents remain isolated
func TestPolicy_IsolationBetweenNodesAndIncidents(t *testing.T) {
	ctx := context.Background()
	pol := response.NewDefaultPolicy()

	incNodeA := makeTestIncident("inc-nodeA", "node-A", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)
	incNodeB := makeTestIncident("inc-nodeB", "node-B", "cpu_usage_percent", types.SeverityHigh, types.StatusAnomalyDetected)

	decA, err := pol.Evaluate(ctx, incNodeA)
	if err != nil {
		t.Fatalf("eval Node A failed: %v", err)
	}
	decB, err := pol.Evaluate(ctx, incNodeB)
	if err != nil {
		t.Fatalf("eval Node B failed: %v", err)
	}

	if decA.DecisionID == decB.DecisionID {
		t.Errorf("cross-node collision: DecisionIDs must be distinct! both got %s", decA.DecisionID)
	}
	if decA.NodeID != "node-A" || decB.NodeID != "node-B" {
		t.Errorf("node identity bleed across decisions: decA=%s, decB=%s", decA.NodeID, decB.NodeID)
	}
}

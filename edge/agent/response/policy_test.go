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

// TEST G: Valid actionable incident produces deterministic decision
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
	if dec.Parameters["throttle_percent"] != "50" {
		t.Errorf("throttle_percent: got %q, want 50", dec.Parameters["throttle_percent"])
	}

	// Verify DecisionID matches expected deterministic calculation
	expectedID := response.ComputeDecisionID("inc-001", pol.Name(), pol.Version(), types.ActionSimulatedThrottle, "telemetry_generator")
	if dec.DecisionID != expectedID {
		t.Errorf("DecisionID: got %q, want %q", dec.DecisionID, expectedID)
	}
}

// TEST H & K: Repeated evaluation produces the same logical DecisionID (Deterministic & Idempotent)
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

// TEST I: Recovered incident does not generate an automatic mitigation
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

// TEST J: Escalated incident does not generate an automatic mitigation
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

// TEST: Mitigating incident does not create another duplicate action
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

// TEST P & Q: Multiple nodes and multiple incidents remain isolated
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

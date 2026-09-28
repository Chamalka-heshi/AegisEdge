package response

import (
	"context"
	"errors"
	"fmt"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// Common response policy errors.
var (
	ErrNilIncident               = errors.New("incident cannot be nil")
	ErrIncidentNotActionable     = errors.New("incident status is not actionable")
	ErrIncidentAlreadyMitigating = errors.New("incident is already in mitigating state; duplicate action suppressed")
	ErrIncidentRecovered         = errors.New("incident is recovered; no mitigation action required")
	ErrIncidentEscalated         = errors.New("incident is escalated; automatic action prohibited without operator approval")
	ErrNoRuleMatched             = errors.New("no response rule matched incident criteria")
)

// ResponsePolicy evaluates an active incident and deterministically selects a candidate ResponseDecision.
type ResponsePolicy interface {
	Name() string
	Version() string
	Evaluate(ctx context.Context, inc *types.Incident) (*ResponseDecision, error)
}

// PolicyRule defines an explicit deterministic mapping from metric and severity to an allowlisted action.
type PolicyRule struct {
	MetricName  string
	MinSeverity types.IncidentSeverity
	ActionType  types.MitigationActionType
	Target      string
	Parameters  map[string]string
	Reason      string
}

// RuleBasedPolicy implements ResponsePolicy using explicit, deterministic rule lookup tables.
type RuleBasedPolicy struct {
	name          string
	version       string
	executionMode ExecutionMode
	rules         []PolicyRule
	fallbackRule  PolicyRule
}

// NewDefaultPolicy constructs the standard deterministic response policy (v1.0.0).
// In accordance with ADR-0012, default execution mode is ExecutionModeDryRun.
// IMPORTANT: All action targets (e.g. "telemetry_generator", "collector", "local_syslog")
// are strictly SIMULATION / DEMONSTRATION targets used to model remediation decisions.
// They do NOT represent or interact with real host processes or deployed infrastructure.
func NewDefaultPolicy() *RuleBasedPolicy {
	return &RuleBasedPolicy{
		name:          "default_edge_response_policy",
		version:       "1.0.0",
		executionMode: ExecutionModeDryRun,
		rules: []PolicyRule{
			{
				MetricName:  "cpu_usage_percent",
				MinSeverity: types.SeverityHigh,
				ActionType:  types.ActionSimulatedThrottle,
				Target:      "telemetry_generator",
				Parameters: map[string]string{
					"throttle_percent": "50",
					"duration_sec":     "300",
				},
				Reason: "CPU threshold exceeded with HIGH/CRITICAL severity; proposing throttle",
			},
			{
				MetricName:  "memory_usage_percent",
				MinSeverity: types.SeverityCritical,
				ActionType:  types.ActionSimulatedRestart,
				Target:      "collector",
				Parameters: map[string]string{
					"grace_period_sec": "10",
				},
				Reason: "Memory threshold exceeded with CRITICAL severity; proposing worker restart",
			},
			{
				MetricName:  "disk_usage_percent",
				MinSeverity: types.SeverityHigh,
				ActionType:  types.ActionSimulatedAlert,
				Target:      "local_syslog",
				Parameters: map[string]string{
					"priority": "HIGH",
				},
				Reason: "Disk threshold exceeded; proposing local high-priority alert",
			},
		},
		fallbackRule: PolicyRule{
			ActionType: types.ActionSimulatedAlert,
			Target:     "local_syslog",
			Parameters: map[string]string{
				"priority": "LOW",
			},
			Reason: "Standard operational anomaly breach; proposing low-priority alert",
		},
	}
}

// Name returns the policy identifier.
func (p *RuleBasedPolicy) Name() string {
	return p.name
}

// Version returns the policy semver.
func (p *RuleBasedPolicy) Version() string {
	return p.version
}

// SetExecutionMode overrides the default execution mode (e.g. for testing dry-run vs auto-execute).
func (p *RuleBasedPolicy) SetExecutionMode(mode ExecutionMode) error {
	if !mode.IsValid() {
		return ErrInvalidExecMode
	}
	p.executionMode = mode
	return nil
}

// Evaluate applies deterministic policy rules to the Incident.
func (p *RuleBasedPolicy) Evaluate(ctx context.Context, inc *types.Incident) (*ResponseDecision, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if inc == nil {
		return nil, ErrNilIncident
	}
	if err := inc.Validate(); err != nil {
		return nil, fmt.Errorf("invalid incident payload: %w", err)
	}

	// 1. Enforce Incident FSM State Boundaries
	switch inc.Status {
	case types.StatusNormal:
		return nil, ErrIncidentNotActionable
	case types.StatusMitigating:
		return nil, ErrIncidentAlreadyMitigating
	case types.StatusRecovered:
		return nil, ErrIncidentRecovered
	case types.StatusEscalated:
		return nil, ErrIncidentEscalated
	case types.StatusAnomalyDetected:
		// Valid actionable state: proceed
	default:
		return nil, fmt.Errorf("%w: unrecognized status %s", ErrIncidentNotActionable, inc.Status)
	}

	// 2. Deterministic Rule Matching
	selectedRule := p.fallbackRule
	for _, r := range p.rules {
		if r.MetricName == inc.TriggerMetric && severityMeetsOrExceeds(inc.Severity, r.MinSeverity) {
			selectedRule = r
			break
		}
	}

	// 3. Compute Deterministic DecisionID (Independent of Timestamps)
	decisionID := ComputeDecisionID(inc.IncidentID, p.name, p.version, selectedRule.ActionType, selectedRule.Target)

	// 4. Copy parameters safely
	params := make(map[string]string, len(selectedRule.Parameters))
	for k, v := range selectedRule.Parameters {
		params[k] = v
	}

	// 5. Build Evidence Summary
	evidenceSummary := make(map[string]string, len(inc.Evidence))
	for k, v := range inc.Evidence {
		evidenceSummary[k] = v
	}

	decision := &ResponseDecision{
		DecisionID:        decisionID,
		IncidentID:        inc.IncidentID,
		NodeID:            inc.NodeID,
		ActionType:        selectedRule.ActionType,
		Target:            selectedRule.Target,
		Parameters:        params,
		Reason:            selectedRule.Reason,
		PolicyName:        p.name,
		PolicyVersion:     p.version,
		ExecutionMode:     p.executionMode,
		DecisionTimestamp: inc.UpdatedAt, // Stable logical reference time
		EvidenceSummary:   evidenceSummary,
	}

	return decision, nil
}

// severityRank maps Severity to numerical order for >= comparisons.
func severityRank(s types.IncidentSeverity) int {
	switch s {
	case types.SeverityCritical:
		return 4
	case types.SeverityHigh:
		return 3
	case types.SeverityMedium:
		return 2
	case types.SeverityLow:
		return 1
	default:
		return 0
	}
}

func severityMeetsOrExceeds(actual, minRequired types.IncidentSeverity) bool {
	return severityRank(actual) >= severityRank(minRequired)
}

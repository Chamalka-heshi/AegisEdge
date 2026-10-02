package response

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

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
	RuleID             string                     `json:"rule_id"`
	Priority           int                        `json:"priority"` // Higher numerical value = higher precedence
	Enabled            bool                       `json:"enabled"`
	MetricName         string                     `json:"metric_name,omitempty"` // Empty or "*" matches any metric
	MinimumSeverity    types.IncidentSeverity     `json:"minimum_severity"`
	ActionType         types.MitigationActionType `json:"action_type"`
	Target             string                     `json:"target"`
	Parameters         map[string]string          `json:"parameters,omitempty"`
	AuthorizationClass AuthorizationClass         `json:"authorization_class"`
	Cooldown           time.Duration              `json:"cooldown"`
	PolicyVersion      string                     `json:"policy_version"`
	Reason             string                     `json:"reason"`
}

// RuleBasedPolicy implements ResponsePolicy using explicit, deterministic rule lookup tables.
type RuleBasedPolicy struct {
	name          string
	version       string
	executionMode ExecutionMode
	rules         []PolicyRule
	fallbackRule  *PolicyRule
}

// NewDefaultPolicy constructs the standard deterministic response policy (v1.0.0).
// In accordance with ADR-0012, default execution mode is ExecutionModeDryRun.
// IMPORTANT: All action targets (e.g. "telemetry_generator", "collector", "local_syslog")
// are strictly SIMULATION / DEMONSTRATION targets used to model remediation decisions.
// They do NOT represent or interact with real host processes or deployed infrastructure.
func NewDefaultPolicy() *RuleBasedPolicy {
	ver := "1.0.0"
	pName := "default_edge_response_policy"
	return &RuleBasedPolicy{
		name:          pName,
		version:       ver,
		executionMode: ExecutionModeDryRun,
		rules: []PolicyRule{
			{
				RuleID:          "rule-cpu-throttle-01",
				Priority:        100,
				Enabled:         true,
				MetricName:      "cpu_usage_percent",
				MinimumSeverity: types.SeverityHigh,
				ActionType:      types.ActionSimulatedThrottle,
				Target:          "telemetry_generator",
				Parameters: map[string]string{
					"throttle_percent": "50",
					"duration_sec":     "300",
				},
				AuthorizationClass: AuthClassAutoExecute,
				Cooldown:           300 * time.Second,
				PolicyVersion:      ver,
				Reason:             "CPU threshold exceeded with HIGH/CRITICAL severity; proposing throttle",
			},
			{
				RuleID:          "rule-mem-restart-01",
				Priority:        100,
				Enabled:         true,
				MetricName:      "memory_usage_percent",
				MinimumSeverity: types.SeverityCritical,
				ActionType:      types.ActionSimulatedRestart,
				Target:          "collector",
				Parameters: map[string]string{
					"grace_period_sec": "10",
				},
				AuthorizationClass: AuthClassAutoExecute,
				Cooldown:           300 * time.Second,
				PolicyVersion:      ver,
				Reason:             "Memory threshold exceeded with CRITICAL severity; proposing worker restart",
			},
			{
				RuleID:          "rule-disk-alert-01",
				Priority:        50,
				Enabled:         true,
				MetricName:      "disk_usage_percent",
				MinimumSeverity: types.SeverityHigh,
				ActionType:      types.ActionSimulatedAlert,
				Target:          "local_syslog",
				Parameters: map[string]string{
					"priority": "HIGH",
				},
				AuthorizationClass: AuthClassAutoExecute,
				Cooldown:           60 * time.Second,
				PolicyVersion:      ver,
				Reason:             "Disk threshold exceeded; proposing local high-priority alert",
			},
		},
		fallbackRule: &PolicyRule{
			RuleID:          "rule-fallback-alert",
			Priority:        0,
			Enabled:         true,
			MinimumSeverity: types.SeverityLow,
			ActionType:      types.ActionSimulatedAlert,
			Target:          "local_syslog",
			Parameters: map[string]string{
				"priority": "LOW",
			},
			AuthorizationClass: AuthClassAutoExecute,
			Cooldown:           60 * time.Second,
			PolicyVersion:      ver,
			Reason:             "Standard operational anomaly breach; proposing low-priority alert",
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

// AddRule appends an additional deterministic rule.
func (p *RuleBasedPolicy) AddRule(rule PolicyRule) {
	p.rules = append(p.rules, rule)
}

// SetRules replaces all policy rules.
func (p *RuleBasedPolicy) SetRules(rules []PolicyRule) {
	p.rules = make([]PolicyRule, len(rules))
	copy(p.rules, rules)
}

// Rules returns a copy of current rules.
func (p *RuleBasedPolicy) Rules() []PolicyRule {
	cp := make([]PolicyRule, len(p.rules))
	copy(cp, p.rules)
	return cp
}

// SetFallbackRule sets or overrides the fallback rule.
func (p *RuleBasedPolicy) SetFallbackRule(rule PolicyRule) {
	p.fallbackRule = &rule
}

// ClearFallbackRule removes the fallback rule.
func (p *RuleBasedPolicy) ClearFallbackRule() {
	p.fallbackRule = nil
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

	// 2. Deterministic Rule Matching with Explicit Precedence
	// A candidate rule must:
	// a) Be enabled
	// b) Match the metric (exact match or wildcard/empty)
	// c) Have minimum severity satisfied
	var candidates []PolicyRule
	for _, r := range p.rules {
		if !r.Enabled {
			continue
		}
		if !severityMeetsOrExceeds(inc.Severity, r.MinimumSeverity) {
			continue
		}
		metricMatch := r.MetricName == inc.TriggerMetric || r.MetricName == "" || r.MetricName == "*"
		if metricMatch {
			candidates = append(candidates, r)
		}
	}

	var selectedRule *PolicyRule

	if len(candidates) > 0 {
		// Sort candidates strictly deterministically:
		// 1. Specific metric match (exact TriggerMetric) > General/Wildcard
		// 2. Priority descending (higher priority first)
		// 3. MinimumSeverity descending (more specific severity first)
		// 4. RuleID ascending (lexicographical tie-breaker)
		sort.SliceStable(candidates, func(i, j int) bool {
			a, b := candidates[i], candidates[j]
			aSpecific := a.MetricName == inc.TriggerMetric && a.MetricName != "" && a.MetricName != "*"
			bSpecific := b.MetricName == inc.TriggerMetric && b.MetricName != "" && b.MetricName != "*"
			if aSpecific != bSpecific {
				return aSpecific // true if a is specific and b is generic
			}
			if a.Priority != b.Priority {
				return a.Priority > b.Priority
			}
			aSev := severityRank(a.MinimumSeverity)
			bSev := severityRank(b.MinimumSeverity)
			if aSev != bSev {
				return aSev > bSev
			}
			return a.RuleID < b.RuleID
		})
		selectedRule = &candidates[0]
	} else if p.fallbackRule != nil && p.fallbackRule.Enabled && severityMeetsOrExceeds(inc.Severity, p.fallbackRule.MinimumSeverity) {
		selectedRule = p.fallbackRule
	}

	if selectedRule == nil {
		return nil, ErrNoRuleMatched
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
		DecisionID:         decisionID,
		IncidentID:         inc.IncidentID,
		NodeID:             inc.NodeID,
		RuleID:             selectedRule.RuleID,
		ActionType:         selectedRule.ActionType,
		Target:             selectedRule.Target,
		Parameters:         params,
		AuthorizationClass: selectedRule.AuthorizationClass,
		Reason:             selectedRule.Reason,
		PolicyName:         p.name,
		PolicyVersion:      p.version,
		ExecutionMode:      p.executionMode,
		DecisionTimestamp:  inc.UpdatedAt, // Stable logical reference time
		EvidenceSummary:    evidenceSummary,
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

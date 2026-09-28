package response

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// Common validator domain errors.
var (
	ErrNilDecision = errors.New("response decision cannot be nil")
)

// SafetyValidator enforces fail-closed safety constraints on candidate response decisions.
type SafetyValidator interface {
	Validate(ctx context.Context, dec *ResponseDecision, inc *types.Incident) (*SafetyResult, error)
}

// SafetyResult documents the deterministic outcome of a safety validation check.
type SafetyResult struct {
	Allowed     bool                       `json:"allowed"`
	DecisionID  string                     `json:"decision_id"`
	ActionType  types.MitigationActionType `json:"action_type"`
	Reason      string                     `json:"reason"`
	Violations  []string                   `json:"violations,omitempty"`
	ValidatedAt time.Time                  `json:"validated_at"`
}

// StandardSafetyValidator enforces closed runtime allowlists backed by strongly typed action identifiers, parameter ranges, and security boundaries.
type StandardSafetyValidator struct {
	allowedPolicies map[string]struct{}
}

// NewStandardValidator constructs a new StandardSafetyValidator.
func NewStandardValidator() *StandardSafetyValidator {
	return &StandardSafetyValidator{
		allowedPolicies: map[string]struct{}{
			"default_edge_response_policy": {},
		},
	}
}

// RegisterAllowedPolicy registers a recognized policy name.
func (v *StandardSafetyValidator) RegisterAllowedPolicy(policyName string) {
	if strings.TrimSpace(policyName) != "" {
		v.allowedPolicies[strings.TrimSpace(policyName)] = struct{}{}
	}
}

// Validate executes all safety checks against the proposed decision and active incident.
// It fails closed: any violation causes the action to be REJECTED.
func (v *StandardSafetyValidator) Validate(ctx context.Context, dec *ResponseDecision, inc *types.Incident) (*SafetyResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if dec == nil {
		return nil, ErrNilDecision
	}
	if inc == nil {
		return nil, ErrNilIncident
	}

	now := time.Now().UTC()
	var violations []string

	// 1. Explicit Allowlist Verification
	if !IsAllowlisted(dec.ActionType) {
		violations = append(violations, fmt.Sprintf("action %q is not on the safety allowlist", dec.ActionType))
	}

	// 2. Incident & Node Identity Verification
	if strings.TrimSpace(dec.IncidentID) == "" {
		violations = append(violations, "decision contains empty IncidentID")
	} else if dec.IncidentID != inc.IncidentID {
		violations = append(violations, fmt.Sprintf("decision IncidentID %q does not match incident IncidentID %q", dec.IncidentID, inc.IncidentID))
	}

	if strings.TrimSpace(dec.NodeID) == "" {
		violations = append(violations, "decision contains empty NodeID")
	} else if dec.NodeID != inc.NodeID {
		violations = append(violations, fmt.Sprintf("decision NodeID %q does not match incident NodeID %q", dec.NodeID, inc.NodeID))
	}

	// 3. Target Sanitization & Validation
	if strings.TrimSpace(dec.Target) == "" {
		violations = append(violations, "decision contains empty Target")
	} else if ContainsDangerousPayload(dec.Target) {
		violations = append(violations, fmt.Sprintf("target %q contains prohibited shell characters or keywords", dec.Target))
	}

	// 4. Policy Name and Version Verification
	if strings.TrimSpace(dec.PolicyName) == "" {
		violations = append(violations, "decision contains empty PolicyName")
	} else if _, ok := v.allowedPolicies[dec.PolicyName]; !ok {
		violations = append(violations, fmt.Sprintf("unrecognized policy name %q", dec.PolicyName))
	}

	if strings.TrimSpace(dec.PolicyVersion) == "" {
		violations = append(violations, "decision contains empty PolicyVersion")
	}

	// 5. Execution Mode Validation
	if !dec.ExecutionMode.IsValid() {
		violations = append(violations, fmt.Sprintf("invalid execution mode %q", dec.ExecutionMode))
	}

	// 6. Incident FSM State Compatibility
	// An action can only be validated for an active incident in ANOMALY_DETECTED or MITIGATING state.
	switch inc.Status {
	case types.StatusAnomalyDetected, types.StatusMitigating:
		// Compatible states
	case types.StatusNormal:
		violations = append(violations, "incident is in NORMAL state; mitigation actions are prohibited")
	case types.StatusRecovered:
		violations = append(violations, "incident is in RECOVERED state; mitigation actions are prohibited")
	case types.StatusEscalated:
		violations = append(violations, "incident is in ESCALATED state; automated mitigation requires human operator override")
	default:
		violations = append(violations, fmt.Sprintf("incident status %q is unsupported for mitigation", inc.Status))
	}

	// 7. Deterministic Identity Verification (DecisionID Consistency)
	expectedID := ComputeDecisionID(dec.IncidentID, dec.PolicyName, dec.PolicyVersion, dec.ActionType, dec.Target)
	if dec.DecisionID != expectedID {
		violations = append(violations, fmt.Sprintf("decision ID mismatch: got %q, expected %q", dec.DecisionID, expectedID))
	}

	// 8. Parameter Bounds and Prohibited Payload Scanning
	for k, val := range dec.Parameters {
		if ContainsDangerousPayload(k) || ContainsDangerousPayload(val) {
			violations = append(violations, fmt.Sprintf("parameter %q=%q contains prohibited shell or executable syntax", k, val))
		}
	}

	// Action-specific parameter range validation
	switch dec.ActionType {
	case types.ActionSimulatedThrottle:
		if pctStr, ok := dec.Parameters["throttle_percent"]; ok {
			pct, err := strconv.Atoi(pctStr)
			if err != nil || pct < 1 || pct > 100 {
				violations = append(violations, fmt.Sprintf("throttle_percent must be an integer between 1 and 100, got %q", pctStr))
			}
		}
		if durStr, ok := dec.Parameters["duration_sec"]; ok {
			dur, err := strconv.Atoi(durStr)
			if err != nil || dur < 1 || dur > 3600 {
				violations = append(violations, fmt.Sprintf("duration_sec must be an integer between 1 and 3600, got %q", durStr))
			}
		}
	case types.ActionSimulatedRestart:
		if graceStr, ok := dec.Parameters["grace_period_sec"]; ok {
			grace, err := strconv.Atoi(graceStr)
			if err != nil || grace < 1 || grace > 60 {
				violations = append(violations, fmt.Sprintf("grace_period_sec must be an integer between 1 and 60, got %q", graceStr))
			}
		}
	}

	// 9. Fail-Closed Final Evaluation
	if len(violations) > 0 {
		return &SafetyResult{
			Allowed:     false,
			DecisionID:  dec.DecisionID,
			ActionType:  dec.ActionType,
			Reason:      fmt.Sprintf("safety validation failed: %s", strings.Join(violations, "; ")),
			Violations:  violations,
			ValidatedAt: now,
		}, nil
	}

	return &SafetyResult{
		Allowed:     true,
		DecisionID:  dec.DecisionID,
		ActionType:  dec.ActionType,
		Reason:      "all safety constraints satisfied",
		Violations:  nil,
		ValidatedAt: now,
	}, nil
}

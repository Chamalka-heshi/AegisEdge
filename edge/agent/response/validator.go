package response

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// Common validator domain errors.
var (
	ErrNilDecision = errors.New("response decision cannot be nil")
)

// ValidationCode provides typed, machine-readable validation outcome codes.
type ValidationCode string

const (
	ValidationCodeAllowed                    ValidationCode = "ALLOWED"
	ValidationCodeInvalidIncident            ValidationCode = "INVALID_INCIDENT"
	ValidationCodeIncidentStateNotActionable ValidationCode = "INCIDENT_STATE_NOT_ACTIONABLE"
	ValidationCodeUnknownActionType          ValidationCode = "UNKNOWN_ACTION_TYPE"
	ValidationCodeForbiddenAction            ValidationCode = "FORBIDDEN_ACTION"
	ValidationCodeInvalidAuthClass           ValidationCode = "INVALID_AUTHORIZATION_CLASS"
	ValidationCodeInvalidTarget              ValidationCode = "INVALID_TARGET"
	ValidationCodeMissingPolicyVersion       ValidationCode = "MISSING_POLICY_VERSION"
	ValidationCodeInvalidDecisionID          ValidationCode = "INVALID_DECISION_ID"
	ValidationCodePolicyMismatch             ValidationCode = "POLICY_MISMATCH"
	ValidationCodeCooldownActive             ValidationCode = "COOLDOWN_ACTIVE"
	ValidationCodeInvalidParameters          ValidationCode = "INVALID_PARAMETERS"
	ValidationCodeIdentityMismatch           ValidationCode = "IDENTITY_MISMATCH"
)

// SafetyValidator enforces fail-closed safety constraints on candidate response decisions.
type SafetyValidator interface {
	Validate(ctx context.Context, dec *ResponseDecision, inc *types.Incident) (*SafetyResult, error)
}

// SafetyResult documents the deterministic outcome of a safety validation check.
type SafetyResult struct {
	Allowed        bool                       `json:"allowed"`
	ValidationCode ValidationCode             `json:"validation_code"`
	DecisionID     string                     `json:"decision_id"`
	ActionType     types.MitigationActionType `json:"action_type"`
	PolicyVersion  string                     `json:"policy_version"`
	Reason         string                     `json:"reason"`
	Violations     []string                   `json:"violations,omitempty"`
	ValidatedAt    time.Time                  `json:"validated_at"`
}

// InMemoryCooldownTracker manages process-local cooldowns per target.
// ARCHITECTURAL INVARIANT: Cooldown state is strictly process-local and in-memory (non-durable).
// Cooldown state does NOT survive process restarts. Persistent mitigation and cooldown tracking belongs to Phase 6.4.
type InMemoryCooldownTracker struct {
	mu      sync.RWMutex
	entries map[string]time.Time // key: target -> expiration timestamp
}

// NewInMemoryCooldownTracker constructs a new in-memory cooldown tracker.
func NewInMemoryCooldownTracker() *InMemoryCooldownTracker {
	return &InMemoryCooldownTracker{
		entries: make(map[string]time.Time),
	}
}

// IsCoolingDown checks whether a target is currently in a cooldown window.
func (t *InMemoryCooldownTracker) IsCoolingDown(target string, now time.Time) (bool, time.Duration) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	exp, ok := t.entries[target]
	if !ok {
		return false, 0
	}
	if now.Before(exp) {
		return true, exp.Sub(now)
	}
	return false, 0
}

// Record sets a cooldown expiration for the given target.
func (t *InMemoryCooldownTracker) Record(target string, cooldown time.Duration, now time.Time) {
	if cooldown <= 0 || strings.TrimSpace(target) == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	t.entries[target] = now.Add(cooldown)
}

// Reset clears all in-memory cooldowns.
func (t *InMemoryCooldownTracker) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.entries = make(map[string]time.Time)
}

// StandardSafetyValidator enforces closed runtime allowlists backed by strongly typed action identifiers, parameter ranges, and security boundaries.
type StandardSafetyValidator struct {
	mu              sync.RWMutex
	allowedPolicies map[string]struct{}
	cooldowns       *InMemoryCooldownTracker
}

// NewStandardValidator constructs a new StandardSafetyValidator.
func NewStandardValidator() *StandardSafetyValidator {
	return &StandardSafetyValidator{
		allowedPolicies: map[string]struct{}{
			"default_edge_response_policy": {},
		},
		cooldowns: NewInMemoryCooldownTracker(),
	}
}

// RegisterAllowedPolicy registers a recognized policy name.
func (v *StandardSafetyValidator) RegisterAllowedPolicy(policyName string) {
	if strings.TrimSpace(policyName) != "" {
		v.mu.Lock()
		defer v.mu.Unlock()
		v.allowedPolicies[strings.TrimSpace(policyName)] = struct{}{}
	}
}

// RecordExecution registers an authorized action execution into the process-local cooldown tracker.
// NOTE: Cooldown state is process-local and non-durable in Phase 6.2.
func (v *StandardSafetyValidator) RecordExecution(target string, cooldown time.Duration) {
	v.cooldowns.Record(target, cooldown, time.Now().UTC())
}

// RecordDecision registers an authorized decision into the cooldown tracker using its configured cooldown.
func (v *StandardSafetyValidator) RecordDecision(dec *ResponseDecision, cooldown time.Duration) {
	if dec != nil {
		v.RecordExecution(dec.Target, cooldown)
	}
}

// IsCoolingDown checks whether the target is currently in an active cooldown window.
func (v *StandardSafetyValidator) IsCoolingDown(target string) (bool, time.Duration) {
	return v.cooldowns.IsCoolingDown(target, time.Now().UTC())
}

// ResetCooldowns clears the active cooldown tracker.
func (v *StandardSafetyValidator) ResetCooldowns() {
	v.cooldowns.Reset()
}

// Validate executes all safety checks against the proposed decision and active incident.
// It fails closed: any violation causes the action to be REJECTED.
// In automated validation without operator approval, AuthClassApprovalRequired actions are rejected.
func (v *StandardSafetyValidator) Validate(ctx context.Context, dec *ResponseDecision, inc *types.Incident) (*SafetyResult, error) {
	return v.validateInternal(ctx, dec, inc, false)
}

// ValidateApproved executes all safety checks against an action that has received verified operator approval.
// CRITICAL SAFETY INVARIANT: Operator approval NEVER overrides FORBIDDEN actions, non-allowlisted actions,
// invalid targets, or prohibited parameters. If any safety boundary is breached, the action is REJECTED.
func (v *StandardSafetyValidator) ValidateApproved(ctx context.Context, dec *ResponseDecision, inc *types.Incident) (*SafetyResult, error) {
	return v.validateInternal(ctx, dec, inc, true)
}

func (v *StandardSafetyValidator) validateInternal(ctx context.Context, dec *ResponseDecision, inc *types.Incident, approved bool) (*SafetyResult, error) {
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
	code := ValidationCodeAllowed

	// 1. Explicit Allowlist Verification
	if !IsAllowlisted(dec.ActionType) {
		violations = append(violations, fmt.Sprintf("action %q is not on the safety allowlist", dec.ActionType))
		if code == ValidationCodeAllowed {
			code = ValidationCodeUnknownActionType
		}
	}

	// 2. Incident & Node Identity Verification
	if strings.TrimSpace(dec.IncidentID) == "" {
		violations = append(violations, "decision contains empty IncidentID")
		if code == ValidationCodeAllowed {
			code = ValidationCodeIdentityMismatch
		}
	} else if dec.IncidentID != inc.IncidentID {
		violations = append(violations, fmt.Sprintf("decision IncidentID %q does not match incident IncidentID %q", dec.IncidentID, inc.IncidentID))
		if code == ValidationCodeAllowed {
			code = ValidationCodeIdentityMismatch
		}
	}

	if strings.TrimSpace(dec.NodeID) == "" {
		violations = append(violations, "decision contains empty NodeID")
		if code == ValidationCodeAllowed {
			code = ValidationCodeIdentityMismatch
		}
	} else if dec.NodeID != inc.NodeID {
		violations = append(violations, fmt.Sprintf("decision NodeID %q does not match incident NodeID %q", dec.NodeID, inc.NodeID))
		if code == ValidationCodeAllowed {
			code = ValidationCodeIdentityMismatch
		}
	}

	// 3. Target Sanitization & Validation
	if strings.TrimSpace(dec.Target) == "" {
		violations = append(violations, "decision contains empty Target")
		if code == ValidationCodeAllowed {
			code = ValidationCodeInvalidTarget
		}
	} else if !IsValidTarget(dec.Target) {
		violations = append(violations, fmt.Sprintf("target %q is invalid or contains prohibited characters/commands", dec.Target))
		if code == ValidationCodeAllowed {
			code = ValidationCodeInvalidTarget
		}
	}

	// 4. Authorization Class Verification
	if !dec.AuthorizationClass.IsValid() {
		violations = append(violations, fmt.Sprintf("invalid authorization class %q", dec.AuthorizationClass))
		if code == ValidationCodeAllowed {
			code = ValidationCodeInvalidAuthClass
		}
	} else if dec.AuthorizationClass == AuthClassForbidden {
		// INVARIANT: FORBIDDEN actions can NEVER be executed, even with operator approval.
		violations = append(violations, fmt.Sprintf("action %q on target %q is classified as FORBIDDEN and cannot be authorized", dec.ActionType, dec.Target))
		if code == ValidationCodeAllowed {
			code = ValidationCodeForbiddenAction
		}
	} else if dec.AuthorizationClass == AuthClassApprovalRequired && !approved {
		violations = append(violations, "action requires operator approval; automatic authorization is prohibited without verified approval")
		if code == ValidationCodeAllowed {
			code = ValidationCodeInvalidAuthClass
		}
	}

	// 5. Policy Name and Version Verification
	v.mu.RLock()
	_, policyRegistered := v.allowedPolicies[dec.PolicyName]
	v.mu.RUnlock()

	if strings.TrimSpace(dec.PolicyName) == "" {
		violations = append(violations, "decision contains empty PolicyName")
		if code == ValidationCodeAllowed {
			code = ValidationCodePolicyMismatch
		}
	} else if !policyRegistered {
		violations = append(violations, fmt.Sprintf("unrecognized policy name %q", dec.PolicyName))
		if code == ValidationCodeAllowed {
			code = ValidationCodePolicyMismatch
		}
	}

	if strings.TrimSpace(dec.PolicyVersion) == "" {
		violations = append(violations, "decision contains empty PolicyVersion")
		if code == ValidationCodeAllowed {
			code = ValidationCodeMissingPolicyVersion
		}
	}

	// 6. Execution Mode Validation
	if !dec.ExecutionMode.IsValid() {
		violations = append(violations, fmt.Sprintf("invalid execution mode %q", dec.ExecutionMode))
		if code == ValidationCodeAllowed {
			code = ValidationCodeInvalidAuthClass
		}
	}

	// 7. Incident FSM State Compatibility
	// An action can only be validated for an active incident in ANOMALY_DETECTED or MITIGATING state.
	switch inc.Status {
	case types.StatusAnomalyDetected, types.StatusMitigating:
		// Compatible states
	case types.StatusNormal:
		violations = append(violations, "incident is in NORMAL state; mitigation actions are prohibited")
		if code == ValidationCodeAllowed {
			code = ValidationCodeIncidentStateNotActionable
		}
	case types.StatusRecovered:
		violations = append(violations, "incident is in RECOVERED state; mitigation actions are prohibited")
		if code == ValidationCodeAllowed {
			code = ValidationCodeIncidentStateNotActionable
		}
	case types.StatusEscalated:
		violations = append(violations, "incident is in ESCALATED state; automated mitigation requires human operator override")
		if code == ValidationCodeAllowed {
			code = ValidationCodeIncidentStateNotActionable
		}
	default:
		violations = append(violations, fmt.Sprintf("incident status %q is unsupported for mitigation", inc.Status))
		if code == ValidationCodeAllowed {
			code = ValidationCodeIncidentStateNotActionable
		}
	}

	// 8. Deterministic Identity Verification (DecisionID Consistency)
	expectedID := ComputeDecisionID(dec.IncidentID, dec.PolicyName, dec.PolicyVersion, dec.ActionType, dec.Target)
	if dec.DecisionID != expectedID {
		violations = append(violations, fmt.Sprintf("decision ID mismatch: got %q, expected %q", dec.DecisionID, expectedID))
		if code == ValidationCodeAllowed {
			code = ValidationCodeInvalidDecisionID
		}
	}

	// 9. Process-Local Cooldown Verification
	if coolingDown, remaining := v.cooldowns.IsCoolingDown(dec.Target, now); coolingDown {
		violations = append(violations, fmt.Sprintf("action target %q is currently cooling down (remaining: %v)", dec.Target, remaining.Round(time.Millisecond)))
		if code == ValidationCodeAllowed {
			code = ValidationCodeCooldownActive
		}
	}

	// 10. Parameter Bounds and Prohibited Payload Scanning
	for k, val := range dec.Parameters {
		if ContainsDangerousPayload(k) || ContainsDangerousPayload(val) {
			violations = append(violations, fmt.Sprintf("parameter %q=%q contains prohibited shell or executable syntax", k, val))
			if code == ValidationCodeAllowed {
				code = ValidationCodeInvalidParameters
			}
		}
	}

	// Action-specific parameter range validation
	switch dec.ActionType {
	case types.ActionSimulatedThrottle:
		if pctStr, ok := dec.Parameters["throttle_percent"]; ok {
			pct, err := strconv.Atoi(pctStr)
			if err != nil || pct < 1 || pct > 100 {
				violations = append(violations, fmt.Sprintf("throttle_percent must be an integer between 1 and 100, got %q", pctStr))
				if code == ValidationCodeAllowed {
					code = ValidationCodeInvalidParameters
				}
			}
		}
		if durStr, ok := dec.Parameters["duration_sec"]; ok {
			dur, err := strconv.Atoi(durStr)
			if err != nil || dur < 1 || dur > 3600 {
				violations = append(violations, fmt.Sprintf("duration_sec must be an integer between 1 and 3600, got %q", durStr))
				if code == ValidationCodeAllowed {
					code = ValidationCodeInvalidParameters
				}
			}
		}
	case types.ActionSimulatedRestart:
		if graceStr, ok := dec.Parameters["grace_period_sec"]; ok {
			grace, err := strconv.Atoi(graceStr)
			if err != nil || grace < 1 || grace > 60 {
				violations = append(violations, fmt.Sprintf("grace_period_sec must be an integer between 1 and 60, got %q", graceStr))
				if code == ValidationCodeAllowed {
					code = ValidationCodeInvalidParameters
				}
			}
		}
	}

	// 11. Fail-Closed Final Evaluation
	if len(violations) > 0 {
		return &SafetyResult{
			Allowed:        false,
			ValidationCode: code,
			DecisionID:     dec.DecisionID,
			ActionType:     dec.ActionType,
			PolicyVersion:  dec.PolicyVersion,
			Reason:         fmt.Sprintf("safety validation failed: %s", strings.Join(violations, "; ")),
			Violations:     violations,
			ValidatedAt:    now,
		}, nil
	}

	return &SafetyResult{
		Allowed:        true,
		ValidationCode: ValidationCodeAllowed,
		DecisionID:     dec.DecisionID,
		ActionType:     dec.ActionType,
		PolicyVersion:  dec.PolicyVersion,
		Reason:         "all safety constraints satisfied",
		Violations:     nil,
		ValidatedAt:    now,
	}, nil
}

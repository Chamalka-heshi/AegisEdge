package escalation

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// DecisionAction represents the deterministic action decided by the escalation engine.
type DecisionAction string

const (
	DecisionActionRetry    DecisionAction = "RETRY"
	DecisionActionEscalate DecisionAction = "ESCALATE"
	DecisionActionNone     DecisionAction = "NONE"
)

// Policy domain errors.
var (
	ErrInvalidPolicyVersion    = errors.New("policy version cannot be empty")
	ErrNegativeMaxRetries      = errors.New("max_automatic_retries cannot be negative")
	ErrNegativeCooldown        = errors.New("retry_cooldown cannot be negative")
	ErrInvalidFailureWindow    = errors.New("failure_window must be strictly positive")
	ErrInvalidCircuitThreshold = errors.New("circuit_breaker_failure_threshold must be strictly positive")
)

// EscalationPolicy configures deterministic retry limits, cooldown intervals, failure windows, and circuit breakers.
type EscalationPolicy struct {
	// MaxAutomaticRetries is the maximum number of automatic mitigation retries permitted AFTER the initial failed mitigation attempt.
	// For MaxAutomaticRetries = 3, the system permits at most 3 retries following the initial failure (1 initial + 3 retries = 4 total mitigation executions max).
	// Once attempt count reaches MaxAutomaticRetries + 1 (i.e. attempt 4 fails), the retry budget is exhausted.
	MaxAutomaticRetries int `json:"max_automatic_retries"`

	// RetryCooldown is the minimum duration that must elapse before attempting another mitigation. Default: 5m.
	RetryCooldown time.Duration `json:"retry_cooldown"`

	// FailureWindow is the sliding window within which failure attempts are counted. Default: 30m.
	FailureWindow time.Duration `json:"failure_window"`

	// CircuitBreakerFailureThreshold is the number of failures within FailureWindow for a specific (NodeID, IncidentID) pair
	// that trips the circuit breaker to OPEN. Default: 3.
	CircuitBreakerFailureThreshold int `json:"circuit_breaker_failure_threshold"`

	// PolicyVersion tracks the version of the active escalation policy. Default: "v1.0.0".
	PolicyVersion string `json:"policy_version"`
}

// DefaultPolicy returns the canonical production default escalation policy.
func DefaultPolicy() EscalationPolicy {
	return EscalationPolicy{
		MaxAutomaticRetries:            3,
		RetryCooldown:                  5 * time.Minute,
		FailureWindow:                  30 * time.Minute,
		CircuitBreakerFailureThreshold: 3,
		PolicyVersion:                  "v1.0.0",
	}
}

// Validate checks the configuration constraints on the policy.
func (p EscalationPolicy) Validate() error {
	if strings.TrimSpace(p.PolicyVersion) == "" {
		return ErrInvalidPolicyVersion
	}
	if p.MaxAutomaticRetries < 0 {
		return ErrNegativeMaxRetries
	}
	if p.RetryCooldown < 0 {
		return ErrNegativeCooldown
	}
	if p.FailureWindow <= 0 {
		return ErrInvalidFailureWindow
	}
	if p.CircuitBreakerFailureThreshold <= 0 {
		return ErrInvalidCircuitThreshold
	}
	return nil
}

// EscalationDecision encapsulates the evaluated outcome for a failure event.
type EscalationDecision struct {
	// Action specifies whether the system should retry, escalate, or take no immediate action.
	Action DecisionAction `json:"action"`

	// Classification specifies the root classification of the failure event.
	Classification types.FailureClassification `json:"classification"`

	// Attempt is the attempt sequence number for this failure.
	Attempt int `json:"attempt"`

	// NextRetryAt indicates when a subsequent retry is permissible if Action is RETRY.
	NextRetryAt *time.Time `json:"next_retry_at,omitempty"`

	// CircuitState reflects the state of the circuit breaker following this evaluation.
	CircuitState types.CircuitState `json:"circuit_state"`

	// Reason explains the decision rationale.
	Reason string `json:"reason"`

	// ShouldTransitionIncident indicates whether the parent incident should be transitioned to ESCALATED.
	ShouldTransitionIncident bool `json:"should_transition_incident"`

	// TargetIncidentStatus is the canonical incident status to transition to if ShouldTransitionIncident is true.
	TargetIncidentStatus types.IncidentStatus `json:"target_incident_status,omitempty"`
}

// String provides a human-readable representation of the decision.
func (d EscalationDecision) String() string {
	return fmt.Sprintf("Decision[Action=%s, Classification=%s, Attempt=%d, Circuit=%s, Reason=%s]",
		d.Action, d.Classification, d.Attempt, d.CircuitState, d.Reason)
}

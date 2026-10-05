package escalation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/incident"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// Engine domain errors.
var (
	ErrNilStore            = errors.New("escalation store cannot be nil")
	ErrNilIncidentEngine   = errors.New("incident engine cannot be nil")
	ErrEmptyResetBy        = errors.New("reset_by operator identity cannot be empty")
	ErrInvalidTargetStatus = errors.New("target incident status for escalation handoff must be ESCALATED")
	ErrEmptyIncidentID     = errors.New("incident_id cannot be empty")
	ErrEmptyNodeID         = errors.New("node_id cannot be empty")
	ErrEmptyActionID       = errors.New("action_id cannot be empty")
)

// FailureEvaluationRequest carries the parameters needed to evaluate a remediation or verification failure.
type FailureEvaluationRequest struct {
	IncidentID            string                      `json:"incident_id"`
	NodeID                string                      `json:"node_id"`
	ActionID              string                      `json:"action_id"`
	MetricName            string                      `json:"metric_name"`
	FailureClassification types.FailureClassification `json:"failure_classification"`
	Reason                string                      `json:"reason"`
	Evidence              map[string]string           `json:"evidence,omitempty"`
}

// Engine defines the contract for deterministic escalation evaluation, cooldown enforcement, and circuit breaking.
type Engine interface {
	// EvaluateFailure processes a failure event and returns a deterministic EscalationDecision.
	EvaluateFailure(ctx context.Context, req FailureEvaluationRequest) (*EscalationDecision, error)

	// EvaluateMitigationResult evaluates the execution status of a MitigationAction and invokes failure evaluation if failed.
	EvaluateMitigationResult(ctx context.Context, nodeID string, m *types.MitigationAction) (*EscalationDecision, error)

	// EvaluateVerificationResult evaluates the status of a closed-loop incident recovery verification.
	EvaluateVerificationResult(ctx context.Context, v *storage.StoredVerification) (*EscalationDecision, error)

	// CanRetry determines whether an autonomous retry is permitted right now according to cooldown and circuit breaker.
	CanRetry(ctx context.Context, nodeID, incidentID string) (bool, *time.Time, error)

	// ResetCircuitBreaker explicitly resets an OPEN circuit breaker to CLOSED with operator identity.
	ResetCircuitBreaker(ctx context.Context, nodeID, incidentID, resetBy, reason string) error

	// GetCircuitBreakerStatus returns the current circuit breaker state for a node and incident.
	GetCircuitBreakerStatus(ctx context.Context, nodeID, incidentID string) (*storage.StoredCircuitState, error)

	// HandoffToIncidentEngine safely transitions the active incident to ESCALATED without mutating state directly.
	HandoffToIncidentEngine(ctx context.Context, incEngine incident.IncidentEngine, nodeID, metricName string, target types.IncidentStatus, now time.Time) error
}

// LocalEngine implements Engine with thread-safe SQLite-backed state tracking.
type LocalEngine struct {
	mu     sync.RWMutex
	store  storage.EscalationStore
	policy EscalationPolicy
	clock  Clock
}

// NewEngine initializes a LocalEngine with the given store, policy, and clock.
func NewEngine(store storage.EscalationStore, policy EscalationPolicy, clock Clock) (*LocalEngine, error) {
	if store == nil {
		return nil, ErrNilStore
	}
	if err := policy.Validate(); err != nil {
		return nil, fmt.Errorf("invalid escalation policy: %w", err)
	}
	if clock == nil {
		clock = RealClock{}
	}

	return &LocalEngine{
		store:  store,
		policy: policy,
		clock:  clock,
	}, nil
}

// EvaluateFailure processes a failure event and computes a deterministic EscalationDecision.
func (e *LocalEngine) EvaluateFailure(ctx context.Context, req FailureEvaluationRequest) (*EscalationDecision, error) {
	if strings.TrimSpace(req.IncidentID) == "" {
		return nil, ErrEmptyIncidentID
	}
	if strings.TrimSpace(req.NodeID) == "" {
		return nil, ErrEmptyNodeID
	}
	if strings.TrimSpace(req.ActionID) == "" {
		return nil, ErrEmptyActionID
	}
	if !req.FailureClassification.IsValid() {
		return nil, types.ErrInvalidFailureClassification
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	now := e.clock.Now()

	// 1. Check existing circuit breaker state
	cbState, err := e.store.GetCircuitState(ctx, req.NodeID, req.IncidentID)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve circuit breaker state: %w", err)
	}

	if cbState != nil && cbState.State == types.CircuitOpen {
		// Circuit breaker is already OPEN; fail-closed: autonomous remediation strictly prohibited
		escID := ComputeEscalationID(req.IncidentID, req.NodeID, req.ActionID, types.FailureCircuitBreakerOpen, e.policy.PolicyVersion, cbState.FailureCount+1)
		storedEsc := &storage.StoredEscalation{
			EscalationID:          escID,
			IncidentID:            req.IncidentID,
			NodeID:                req.NodeID,
			ActionID:              req.ActionID,
			PolicyVersion:         e.policy.PolicyVersion,
			FailureClassification: types.FailureCircuitBreakerOpen,
			Status:                types.EscalationStatusEscalated,
			Attempt:               cbState.FailureCount + 1,
			MaxAttempts:           e.policy.MaxAutomaticRetries,
			Reason:                "circuit breaker is OPEN; autonomous remediation prohibited",
			Evidence:              req.Evidence,
			CreatedAt:             now,
			UpdatedAt:             now,
		}
		_ = e.store.RecordEscalation(ctx, storedEsc)

		return &EscalationDecision{
			Action:                   DecisionActionEscalate,
			Classification:           types.FailureCircuitBreakerOpen,
			Attempt:                  cbState.FailureCount + 1,
			CircuitState:             types.CircuitOpen,
			Reason:                   "circuit breaker is OPEN; autonomous remediation prohibited",
			ShouldTransitionIncident: true,
			TargetIncidentStatus:     types.StatusEscalated,
		}, nil
	}

	// 2. Cooldown check: A COOLDOWN_ACTIVE evaluation must NOT count as a new mitigation failure.
	if req.FailureClassification == types.FailureCooldownActive {
		return &EscalationDecision{
			Action:                   DecisionActionNone,
			Classification:           types.FailureCooldownActive,
			CircuitState:             types.CircuitClosed,
			Reason:                   "cooldown inquiry evaluated; no failure recorded",
			ShouldTransitionIncident: false,
		}, nil
	}

	latest, err := e.store.GetLatestEscalation(ctx, req.NodeID, req.IncidentID)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve latest escalation: %w", err)
	}
	if latest != nil {
		cooldownEnd := latest.CreatedAt.Add(e.policy.RetryCooldown)
		if now.Before(cooldownEnd) {
			// Cooldown is active. Reject premature failure evaluation without incrementing failure count or creating new record.
			return &EscalationDecision{
				Action:                   DecisionActionNone,
				Classification:           types.FailureCooldownActive,
				Attempt:                  latest.Attempt,
				NextRetryAt:              &cooldownEnd,
				CircuitState:             types.CircuitClosed,
				Reason:                   fmt.Sprintf("cooldown active until %s; cannot evaluate new failure or retry during cooldown", cooldownEnd.Format(time.RFC3339)),
				ShouldTransitionIncident: false,
			}, nil
		}
	}

	// 3. Count failure history in sliding window
	since := now.Add(-e.policy.FailureWindow)
	failuresInWindow, err := e.store.CountFailuresInWindow(ctx, req.NodeID, req.IncidentID, since)
	if err != nil {
		return nil, fmt.Errorf("failed to count failures in window: %w", err)
	}

	currentAttempt := failuresInWindow + 1

	// 3. Immediate escalation classifications
	if req.FailureClassification == types.FailureUnknownReconciliationRequired {
		escID := ComputeEscalationID(req.IncidentID, req.NodeID, req.ActionID, req.FailureClassification, e.policy.PolicyVersion, currentAttempt)
		storedEsc := &storage.StoredEscalation{
			EscalationID:          escID,
			IncidentID:            req.IncidentID,
			NodeID:                req.NodeID,
			ActionID:              req.ActionID,
			PolicyVersion:         e.policy.PolicyVersion,
			FailureClassification: req.FailureClassification,
			Status:                types.EscalationStatusEscalated,
			Attempt:               currentAttempt,
			MaxAttempts:           e.policy.MaxAutomaticRetries,
			Reason:                "unknown reconciliation requires human operator intervention",
			Evidence:              req.Evidence,
			CreatedAt:             now,
			UpdatedAt:             now,
		}
		_ = e.store.RecordEscalation(ctx, storedEsc)

		return &EscalationDecision{
			Action:                   DecisionActionEscalate,
			Classification:           req.FailureClassification,
			Attempt:                  currentAttempt,
			CircuitState:             types.CircuitClosed,
			Reason:                   "unknown reconciliation requires human operator intervention",
			ShouldTransitionIncident: true,
			TargetIncidentStatus:     types.StatusEscalated,
		}, nil
	}

	if req.FailureClassification == types.FailureVerificationRejected {
		escID := ComputeEscalationID(req.IncidentID, req.NodeID, req.ActionID, req.FailureClassification, e.policy.PolicyVersion, currentAttempt)
		storedEsc := &storage.StoredEscalation{
			EscalationID:          escID,
			IncidentID:            req.IncidentID,
			NodeID:                req.NodeID,
			ActionID:              req.ActionID,
			PolicyVersion:         e.policy.PolicyVersion,
			FailureClassification: req.FailureClassification,
			Status:                types.EscalationStatusEscalated,
			Attempt:               currentAttempt,
			MaxAttempts:           e.policy.MaxAutomaticRetries,
			Reason:                "verification rejected; remediation worsened or failed recovery condition",
			Evidence:              req.Evidence,
			CreatedAt:             now,
			UpdatedAt:             now,
		}
		_ = e.store.RecordEscalation(ctx, storedEsc)

		return &EscalationDecision{
			Action:                   DecisionActionEscalate,
			Classification:           req.FailureClassification,
			Attempt:                  currentAttempt,
			CircuitState:             types.CircuitClosed,
			Reason:                   "verification rejected; remediation worsened or failed recovery condition",
			ShouldTransitionIncident: true,
			TargetIncidentStatus:     types.StatusEscalated,
		}, nil
	}

	// 4. Trip circuit breaker if failure threshold reached
	if currentAttempt >= e.policy.CircuitBreakerFailureThreshold {
		trippedState := &storage.StoredCircuitState{
			NodeID:       req.NodeID,
			IncidentID:   req.IncidentID,
			State:        types.CircuitOpen,
			FailureCount: currentAttempt,
			TrippedAt:    &now,
			UpdatedAt:    now,
		}
		if err := e.store.SetCircuitState(ctx, trippedState); err != nil {
			return nil, fmt.Errorf("failed to persist circuit breaker trip: %w", err)
		}

		escID := ComputeEscalationID(req.IncidentID, req.NodeID, req.ActionID, types.FailureCircuitBreakerOpen, e.policy.PolicyVersion, currentAttempt)
		storedEsc := &storage.StoredEscalation{
			EscalationID:          escID,
			IncidentID:            req.IncidentID,
			NodeID:                req.NodeID,
			ActionID:              req.ActionID,
			PolicyVersion:         e.policy.PolicyVersion,
			FailureClassification: types.FailureCircuitBreakerOpen,
			Status:                types.EscalationStatusEscalated,
			Attempt:               currentAttempt,
			MaxAttempts:           e.policy.MaxAutomaticRetries,
			Reason:                fmt.Sprintf("circuit breaker failure threshold (%d) reached; circuit tripped to OPEN", e.policy.CircuitBreakerFailureThreshold),
			Evidence:              req.Evidence,
			CreatedAt:             now,
			UpdatedAt:             now,
		}
		_ = e.store.RecordEscalation(ctx, storedEsc)

		return &EscalationDecision{
			Action:                   DecisionActionEscalate,
			Classification:           types.FailureCircuitBreakerOpen,
			Attempt:                  currentAttempt,
			CircuitState:             types.CircuitOpen,
			Reason:                   fmt.Sprintf("circuit breaker failure threshold (%d) reached; circuit tripped to OPEN", e.policy.CircuitBreakerFailureThreshold),
			ShouldTransitionIncident: true,
			TargetIncidentStatus:     types.StatusEscalated,
		}, nil
	}

	// 5. Check retry budget exhaustion
	if currentAttempt > e.policy.MaxAutomaticRetries {
		trippedState := &storage.StoredCircuitState{
			NodeID:       req.NodeID,
			IncidentID:   req.IncidentID,
			State:        types.CircuitOpen,
			FailureCount: currentAttempt,
			TrippedAt:    &now,
			UpdatedAt:    now,
		}
		if err := e.store.SetCircuitState(ctx, trippedState); err != nil {
			return nil, fmt.Errorf("failed to persist circuit breaker trip: %w", err)
		}

		escID := ComputeEscalationID(req.IncidentID, req.NodeID, req.ActionID, types.FailureRetryBudgetExhausted, e.policy.PolicyVersion, currentAttempt)
		storedEsc := &storage.StoredEscalation{
			EscalationID:          escID,
			IncidentID:            req.IncidentID,
			NodeID:                req.NodeID,
			ActionID:              req.ActionID,
			PolicyVersion:         e.policy.PolicyVersion,
			FailureClassification: types.FailureRetryBudgetExhausted,
			Status:                types.EscalationStatusEscalated,
			Attempt:               currentAttempt,
			MaxAttempts:           e.policy.MaxAutomaticRetries,
			Reason:                fmt.Sprintf("retry budget exhausted (%d attempts); escalating to operator", e.policy.MaxAutomaticRetries),
			Evidence:              req.Evidence,
			CreatedAt:             now,
			UpdatedAt:             now,
		}
		_ = e.store.RecordEscalation(ctx, storedEsc)

		return &EscalationDecision{
			Action:                   DecisionActionEscalate,
			Classification:           types.FailureRetryBudgetExhausted,
			Attempt:                  currentAttempt,
			CircuitState:             types.CircuitOpen,
			Reason:                   fmt.Sprintf("retry budget exhausted (%d attempts); escalating to operator", e.policy.MaxAutomaticRetries),
			ShouldTransitionIncident: true,
			TargetIncidentStatus:     types.StatusEscalated,
		}, nil
	}

	// 6. Record failure and schedule retry after cooldown
	escID := ComputeEscalationID(req.IncidentID, req.NodeID, req.ActionID, req.FailureClassification, e.policy.PolicyVersion, currentAttempt)
	storedEsc := &storage.StoredEscalation{
		EscalationID:          escID,
		IncidentID:            req.IncidentID,
		NodeID:                req.NodeID,
		ActionID:              req.ActionID,
		PolicyVersion:         e.policy.PolicyVersion,
		FailureClassification: req.FailureClassification,
		Status:                types.EscalationStatusPending,
		Attempt:               currentAttempt,
		MaxAttempts:           e.policy.MaxAutomaticRetries,
		Reason:                req.Reason,
		Evidence:              req.Evidence,
		CreatedAt:             now,
		UpdatedAt:             now,
	}
	if err := e.store.RecordEscalation(ctx, storedEsc); err != nil && !errors.Is(err, storage.ErrDuplicateEscalation) {
		return nil, fmt.Errorf("failed to record escalation: %w", err)
	}

	nextRetryAt := now.Add(e.policy.RetryCooldown)
	return &EscalationDecision{
		Action:                   DecisionActionRetry,
		Classification:           req.FailureClassification,
		Attempt:                  currentAttempt,
		NextRetryAt:              &nextRetryAt,
		CircuitState:             types.CircuitClosed,
		Reason:                   fmt.Sprintf("retry %d/%d permitted after cooldown (%v)", currentAttempt, e.policy.MaxAutomaticRetries, e.policy.RetryCooldown),
		ShouldTransitionIncident: false,
	}, nil
}

// EvaluateMitigationResult evaluates the execution status of a MitigationAction.
// If the action failed or entered unknown reconciliation, it invokes failure evaluation.
// Successful executions (EXECUTED) or in-flight/skipped actions produce DecisionActionNone.
func (e *LocalEngine) EvaluateMitigationResult(ctx context.Context, nodeID string, m *types.MitigationAction) (*EscalationDecision, error) {
	if m == nil {
		return nil, errors.New("mitigation action cannot be nil")
	}
	if strings.TrimSpace(nodeID) == "" {
		return nil, ErrEmptyNodeID
	}

	switch m.Status {
	case types.MitigationStatusExecuted:
		// Successful execution produces NO escalation
		return &EscalationDecision{
			Action:       DecisionActionNone,
			CircuitState: types.CircuitClosed,
			Reason:       "mitigation executed successfully; awaiting verification",
		}, nil

	case types.MitigationStatusPending, types.MitigationStatusExecuting, types.MitigationStatusSkipped:
		return &EscalationDecision{
			Action:       DecisionActionNone,
			CircuitState: types.CircuitClosed,
			Reason:       fmt.Sprintf("mitigation status %s requires no escalation", m.Status),
		}, nil

	case types.MitigationStatusFailed:
		reason := m.Error
		if reason == "" {
			reason = m.Message
		}
		if reason == "" {
			reason = "simulated mitigation execution failed"
		}
		req := FailureEvaluationRequest{
			IncidentID:            m.IncidentID,
			NodeID:                nodeID,
			ActionID:              m.ActionID,
			FailureClassification: types.FailureMitigationFailed,
			Reason:                reason,
		}
		return e.EvaluateFailure(ctx, req)

	case types.MitigationStatusUnknownReconciliationRequired:
		reason := m.Error
		if reason == "" {
			reason = m.Message
		}
		if reason == "" {
			reason = "mitigation entered unknown reconciliation required; autonomous retry strictly forbidden"
		}
		req := FailureEvaluationRequest{
			IncidentID:            m.IncidentID,
			NodeID:                nodeID,
			ActionID:              m.ActionID,
			FailureClassification: types.FailureUnknownReconciliationRequired,
			Reason:                reason,
		}
		return e.EvaluateFailure(ctx, req)

	default:
		return nil, fmt.Errorf("unrecognized mitigation status %s", m.Status)
	}
}

// EvaluateVerificationResult evaluates the status of a closed-loop incident recovery verification.
// Expected behavior:
// - RECOVERED: NO escalation
// - PENDING: NO escalation
// - TIMED_OUT: escalation evaluation (FailureVerificationTimedOut)
// - NOT_RECOVERED: escalation evaluation (FailureVerificationRejected)
// - CANCELLED: NO automatic retry
func (e *LocalEngine) EvaluateVerificationResult(ctx context.Context, v *storage.StoredVerification) (*EscalationDecision, error) {
	if v == nil {
		return nil, errors.New("verification record cannot be nil")
	}

	switch v.Status {
	case types.VerificationStatusRecovered:
		return &EscalationDecision{
			Action:       DecisionActionNone,
			CircuitState: types.CircuitClosed,
			Reason:       "verification confirmed incident recovery; no escalation required",
		}, nil

	case types.VerificationStatusPending:
		return &EscalationDecision{
			Action:       DecisionActionNone,
			CircuitState: types.CircuitClosed,
			Reason:       "verification pending telemetry observations; no escalation required",
		}, nil

	case types.VerificationStatusCancelled:
		return &EscalationDecision{
			Action:       DecisionActionNone,
			CircuitState: types.CircuitClosed,
			Reason:       "verification cancelled; automatic retry prohibited",
		}, nil

	case types.VerificationStatusTimedOut:
		reason := v.Reason
		if reason == "" {
			reason = "closed-loop verification timed out without satisfying recovery condition"
		}
		req := FailureEvaluationRequest{
			IncidentID:            v.IncidentID,
			NodeID:                v.NodeID,
			ActionID:              v.ActionID,
			MetricName:            v.MetricName,
			FailureClassification: types.FailureVerificationTimedOut,
			Reason:                reason,
			Evidence:              v.Evidence,
		}
		return e.EvaluateFailure(ctx, req)

	case types.VerificationStatusNotRecovered:
		reason := v.Reason
		if reason == "" {
			reason = "closed-loop verification reached terminal NOT_RECOVERED outcome"
		}
		req := FailureEvaluationRequest{
			IncidentID:            v.IncidentID,
			NodeID:                v.NodeID,
			ActionID:              v.ActionID,
			MetricName:            v.MetricName,
			FailureClassification: types.FailureVerificationRejected,
			Reason:                reason,
			Evidence:              v.Evidence,
		}
		return e.EvaluateFailure(ctx, req)

	default:
		return nil, fmt.Errorf("unrecognized verification status %s", v.Status)
	}
}

// CanRetry checks whether a subsequent retry is permitted given cooldown and circuit breaker state.
func (e *LocalEngine) CanRetry(ctx context.Context, nodeID, incidentID string) (bool, *time.Time, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	now := e.clock.Now()

	// 1. Check circuit breaker
	cbState, err := e.store.GetCircuitState(ctx, nodeID, incidentID)
	if err != nil {
		return false, nil, fmt.Errorf("failed to get circuit state: %w", err)
	}
	if cbState != nil && cbState.State == types.CircuitOpen {
		return false, nil, nil
	}

	// 2. Check cooldown from latest escalation
	latest, err := e.store.GetLatestEscalation(ctx, nodeID, incidentID)
	if err != nil {
		return false, nil, fmt.Errorf("failed to get latest escalation: %w", err)
	}
	if latest != nil {
		cooldownEnd := latest.CreatedAt.Add(e.policy.RetryCooldown)
		if now.Before(cooldownEnd) {
			return false, &cooldownEnd, nil
		}
	}

	return true, nil, nil
}

// ResetCircuitBreaker explicitly resets an OPEN circuit breaker to CLOSED.
// Requires valid operator identity in resetBy.
func (e *LocalEngine) ResetCircuitBreaker(ctx context.Context, nodeID, incidentID, resetBy, reason string) error {
	if strings.TrimSpace(resetBy) == "" {
		return ErrEmptyResetBy
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	now := e.clock.Now()

	cbState, err := e.store.GetCircuitState(ctx, nodeID, incidentID)
	if err != nil {
		return fmt.Errorf("failed to get circuit state: %w", err)
	}

	if cbState == nil {
		// Nothing to reset
		return nil
	}

	resetState := &storage.StoredCircuitState{
		NodeID:       nodeID,
		IncidentID:   incidentID,
		State:        types.CircuitClosed,
		FailureCount: 0,
		TrippedAt:    nil,
		ResetAt:      &now,
		ResetBy:      resetBy,
		UpdatedAt:    now,
	}

	return e.store.SetCircuitState(ctx, resetState)
}

// GetCircuitBreakerStatus returns the current circuit breaker state for a node and incident.
func (e *LocalEngine) GetCircuitBreakerStatus(ctx context.Context, nodeID, incidentID string) (*storage.StoredCircuitState, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	state, err := e.store.GetCircuitState(ctx, nodeID, incidentID)
	if err != nil {
		return nil, fmt.Errorf("failed to get circuit state: %w", err)
	}
	if state == nil {
		return &storage.StoredCircuitState{
			NodeID:       nodeID,
			IncidentID:   incidentID,
			State:        types.CircuitClosed,
			FailureCount: 0,
			UpdatedAt:    e.clock.Now(),
		}, nil
	}
	return state, nil
}

// HandoffToIncidentEngine safely transitions the active incident to ESCALATED according to canonical Incident FSM.
// It never directly mutates the Incident struct and never bypasses IncidentEngine validation.
func (e *LocalEngine) HandoffToIncidentEngine(
	ctx context.Context,
	incEngine incident.IncidentEngine,
	nodeID, metricName string,
	target types.IncidentStatus,
	now time.Time,
) error {
	if incEngine == nil {
		return ErrNilIncidentEngine
	}
	if target != types.StatusEscalated {
		return ErrInvalidTargetStatus
	}

	active := incEngine.GetActiveIncident(nodeID, metricName)
	if active != nil && active.Status == types.StatusEscalated {
		// Idempotent check: already escalated
		return nil
	}

	_, err := incEngine.TransitionActiveIncident(ctx, nodeID, metricName, target, now)
	if err != nil {
		return fmt.Errorf("incident engine transition failed: %w", err)
	}

	return nil
}

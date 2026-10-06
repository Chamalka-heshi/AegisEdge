package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/escalation"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/incident"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/response"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/verification"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// OrchestrationState defines the discrete lifecycle states of an incident orchestration cycle.
// ARCHITECTURAL INVARIANT: OrchestrationState is distinct from canonical IncidentStatus.
// Orchestrator states must never be injected into shared/types.IncidentStatus.
type OrchestrationState string

const (
	StatePending         OrchestrationState = "PENDING"
	StateRunning         OrchestrationState = "RUNNING"
	StateWaitingApproval OrchestrationState = "WAITING_APPROVAL"
	StateExecuting       OrchestrationState = "EXECUTING"
	StateVerifying       OrchestrationState = "VERIFYING"
	StateRetryAuthorized OrchestrationState = "RETRY_AUTHORIZED"
	StateEscalated       OrchestrationState = "ESCALATED"
	StateRecovered       OrchestrationState = "RECOVERED"
	StateFailed          OrchestrationState = "FAILED"
	StateCancelled       OrchestrationState = "CANCELLED"
)

// IsValid checks whether the orchestration state is recognized.
func (s OrchestrationState) IsValid() bool {
	switch s {
	case StatePending, StateRunning, StateWaitingApproval, StateExecuting,
		StateVerifying, StateRetryAuthorized, StateEscalated, StateRecovered,
		StateFailed, StateCancelled:
		return true
	default:
		return false
	}
}

// IsTerminal checks whether the orchestration state represents a terminal completion state.
func (s OrchestrationState) IsTerminal() bool {
	switch s {
	case StateEscalated, StateRecovered, StateFailed, StateCancelled:
		return true
	default:
		return false
	}
}

// Domain errors for incident orchestration.
var (
	ErrNilIncident            = errors.New("incident cannot be nil")
	ErrIncidentNotActionable  = errors.New("incident status is not actionable")
	ErrNilPolicyEngine        = errors.New("policy engine cannot be nil")
	ErrNilSafetyValidator     = errors.New("safety validator cannot be nil")
	ErrNilExecutor            = errors.New("action executor cannot be nil")
	ErrNilVerificationEngine  = errors.New("verification engine cannot be nil")
	ErrNilEscalationEngine    = errors.New("escalation engine cannot be nil")
	ErrNilIncidentEngine      = errors.New("incident engine cannot be nil")
	ErrNilMitigationStore     = errors.New("mitigation store cannot be nil")
	ErrForbiddenAction        = errors.New("action is forbidden by response policy")
	ErrSafetyValidationFailed = errors.New("decision failed safety validation")
	ErrApprovalMissing        = errors.New("operator approval required but missing")
	ErrApprovalExpired        = errors.New("operator approval has expired")
	ErrApprovalInvalid        = errors.New("operator approval validation failed")
	ErrPersistenceFailed      = errors.New("durable mitigation persistence failed")
	ErrOrchestrationCancelled = errors.New("orchestration was cancelled")
	ErrRetryBudgetExhausted   = errors.New("retry budget exhausted")
	ErrCircuitBreakerOpen     = errors.New("circuit breaker is OPEN; autonomous remediation prohibited")
	ErrCooldownActive         = errors.New("remediation cooldown is active; retry delayed")
	ErrUnknownReconciliation  = errors.New("mitigation is in UNKNOWN_RECONCILIATION_REQUIRED; autonomous retry prohibited")
	ErrMaxAttemptsExceeded    = errors.New("maximum orchestration attempt ceiling exceeded")
	ErrDuplicateOrchestration = errors.New("duplicate orchestration request for terminal incident")
)

// Clock abstracts system time to ensure deterministic testing.
type Clock interface {
	Now() time.Time
	Since(t time.Time) time.Duration
}

// RealClock returns the current UTC system time.
type RealClock struct{}

func (RealClock) Now() time.Time {
	return time.Now().UTC()
}

func (RealClock) Since(t time.Time) time.Duration {
	return time.Now().UTC().Sub(t)
}

// FakeClock provides a thread-safe synthetic clock for deterministic testing.
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewFakeClock constructs a FakeClock initialized to the given time.
func NewFakeClock(t time.Time) *FakeClock {
	return &FakeClock{now: t}
}

// Now returns the current synthetic time.
func (f *FakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Since returns the elapsed duration since t.
func (f *FakeClock) Since(t time.Time) time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now.Sub(t)
}

// Advance moves synthetic time forward by the given duration.
func (f *FakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// Set explicitly updates the synthetic time.
func (f *FakeClock) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = t
}

// OrchestrationEvent documents a structured, auditable domain event during orchestration.
// SECURITY BOUNDARY: Contains ZERO secrets, credentials, arbitrary command strings,
// or sensitive approval material.
type OrchestrationEvent struct {
	IncidentID         string                     `json:"incident_id"`
	NodeID             string                     `json:"node_id"`
	DecisionID         string                     `json:"decision_id,omitempty"`
	ActionID           string                     `json:"action_id,omitempty"`
	Attempt            int                        `json:"attempt"`
	State              OrchestrationState         `json:"state"`
	PolicyVersion      string                     `json:"policy_version,omitempty"`
	ActionType         types.MitigationActionType `json:"action_type,omitempty"`
	Target             string                     `json:"target,omitempty"`
	ExecutionStatus    types.MitigationStatus     `json:"execution_status,omitempty"`
	VerificationStatus types.VerificationStatus   `json:"verification_status,omitempty"`
	EscalationDecision escalation.DecisionAction  `json:"escalation_decision,omitempty"`
	Timestamp          time.Time                  `json:"timestamp"`
	Message            string                     `json:"message,omitempty"`
	Error              string                     `json:"error,omitempty"`
}

// OrchestrationResult captures the observable outcome of an incident response orchestration cycle.
type OrchestrationResult struct {
	IncidentID         string                           `json:"incident_id"`
	NodeID             string                           `json:"node_id"`
	DecisionID         string                           `json:"decision_id,omitempty"`
	ActionID           string                           `json:"action_id,omitempty"`
	State              OrchestrationState               `json:"state"`
	TotalAttempts      int                              `json:"total_attempts"`
	ExecutionResult    *response.ExecutionResult        `json:"execution_result,omitempty"`
	VerificationResult *verification.VerificationResult `json:"verification_result,omitempty"`
	EscalationDecision *escalation.EscalationDecision   `json:"escalation_decision,omitempty"`
	Message            string                           `json:"message"`
	Error              error                            `json:"error,omitempty"`
	CompletedAt        time.Time                        `json:"completed_at"`
}

// Config defines the operational parameters for the incident response orchestrator.
type Config struct {
	// MaxExecutionCeiling is a defense-in-depth safety ceiling preventing runaway retry loops (default: 4).
	// The hard ceiling of 4 is defense-in-depth and agrees with the configured maximum of 3 automatic retries.
	// It cannot authorize an additional execution beyond the escalation policy.
	MaxExecutionCeiling int `json:"max_execution_ceiling"`

	// DefaultVerificationTimeout defines the observation window for recovery verification (default: 5m).
	DefaultVerificationTimeout time.Duration `json:"default_verification_timeout"`

	// DefaultRequiredObservations defines required consecutive healthy telemetry samples (default: 3).
	DefaultRequiredObservations int `json:"default_required_observations"`
}

// DefaultConfig returns recommended production defaults.
func DefaultConfig() Config {
	return Config{
		MaxExecutionCeiling:         4,
		DefaultVerificationTimeout:  5 * time.Minute,
		DefaultRequiredObservations: 3,
	}
}

// Validate verifies internal bounds of Config.
func (c *Config) Validate() error {
	if c.MaxExecutionCeiling <= 0 {
		c.MaxExecutionCeiling = 4
	}
	if c.DefaultVerificationTimeout <= 0 {
		c.DefaultVerificationTimeout = 5 * time.Minute
	}
	if c.DefaultRequiredObservations <= 0 {
		c.DefaultRequiredObservations = 3
	}
	return nil
}

// Options encapsulates optional overrides and inputs for an orchestration cycle.
type Options struct {
	Approval            *response.OperatorApproval
	RecoveryCondition   *verification.RecoveryCondition
	VerificationTimeout time.Duration
	TelemetrySamples    []types.MetricSample
}

// Option configures an Options instance.
type Option func(*Options)

// WithApproval provides an externally supplied, verified operator approval record.
func WithApproval(app *response.OperatorApproval) Option {
	return func(o *Options) {
		o.Approval = app
	}
}

// WithRecoveryCondition supplies a custom telemetry recovery predicate.
func WithRecoveryCondition(cond verification.RecoveryCondition) Option {
	return func(o *Options) {
		o.RecoveryCondition = &cond
	}
}

// WithVerificationTimeout overrides the default verification timeout duration.
func WithVerificationTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.VerificationTimeout = timeout
	}
}

// WithTelemetrySamples provides subsequent telemetry samples to synchronously evaluate verification.
func WithTelemetrySamples(samples ...types.MetricSample) Option {
	return func(o *Options) {
		o.TelemetrySamples = append(o.TelemetrySamples, samples...)
	}
}

// Orchestrator coordinates the bounded, fail-closed incident response workflow.
type Orchestrator interface {
	// OrchestrateIncident coordinates a controlled, bounded response cycle for the given active incident.
	OrchestrateIncident(ctx context.Context, inc *types.Incident, opts ...Option) (*OrchestrationResult, error)

	// ProcessTelemetry ingests subsequent telemetry samples and evaluates active verifications.
	ProcessTelemetry(ctx context.Context, sample types.MetricSample) (*verification.VerificationResult, error)

	// GetOrchestrationState returns the current orchestration state for an incident.
	GetOrchestrationState(incidentID string) (OrchestrationState, bool)

	// RegisterEventHandler registers a listener for auditable orchestration events.
	RegisterEventHandler(handler func(event OrchestrationEvent))
}

type inFlightOrchestration struct {
	done chan struct{}
	res  *OrchestrationResult
	err  error
}

type approvedValidator interface {
	ValidateApproved(ctx context.Context, dec *response.ResponseDecision, inc *types.Incident) (*response.SafetyResult, error)
}

// DefaultOrchestrator implements Orchestrator coordinating all existing domain components.
// CRITICAL SAFETY BOUNDARY: This component executes ZERO real host remediation.
// Actuation is restricted to existing SIMULATED_* actions.
type DefaultOrchestrator struct {
	mu            sync.RWMutex
	policy        response.ResponsePolicy
	validator     response.SafetyValidator
	executor      response.ActionExecutor
	vEngine       verification.Engine
	escEngine     escalation.Engine
	incEngine     incident.IncidentEngine
	mitStore      storage.MitigationStore
	vStore        storage.VerificationStore
	approvalMgr   *response.ApprovalManager
	cfg           Config
	clock         Clock
	inFlight      map[string]*inFlightOrchestration
	stateCache    map[string]OrchestrationState
	history       map[string]*OrchestrationResult
	eventHandlers []func(event OrchestrationEvent)
}

// NewOrchestrator constructs a DefaultOrchestrator validating that all required dependencies are non-nil.
func NewOrchestrator(
	policy response.ResponsePolicy,
	validator response.SafetyValidator,
	executor response.ActionExecutor,
	vEngine verification.Engine,
	escEngine escalation.Engine,
	incEngine incident.IncidentEngine,
	mitStore storage.MitigationStore,
	cfg Config,
	clock Clock,
) (*DefaultOrchestrator, error) {
	if policy == nil {
		return nil, ErrNilPolicyEngine
	}
	if validator == nil {
		return nil, ErrNilSafetyValidator
	}
	if executor == nil {
		return nil, ErrNilExecutor
	}
	if vEngine == nil {
		return nil, ErrNilVerificationEngine
	}
	if escEngine == nil {
		return nil, ErrNilEscalationEngine
	}
	if incEngine == nil {
		return nil, ErrNilIncidentEngine
	}
	if mitStore == nil {
		return nil, ErrNilMitigationStore
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if clock == nil {
		clock = RealClock{}
	}

	var vStore storage.VerificationStore
	if vs, ok := mitStore.(storage.VerificationStore); ok {
		vStore = vs
	}

	return &DefaultOrchestrator{
		policy:     policy,
		validator:  validator,
		executor:   executor,
		vEngine:    vEngine,
		escEngine:  escEngine,
		incEngine:  incEngine,
		mitStore:   mitStore,
		vStore:     vStore,
		cfg:        cfg,
		clock:      clock,
		inFlight:   make(map[string]*inFlightOrchestration),
		stateCache: make(map[string]OrchestrationState),
		history:    make(map[string]*OrchestrationResult),
	}, nil
}

// WithApprovalManager associates an ApprovalManager for requesting human operator authorizations.
func (o *DefaultOrchestrator) WithApprovalManager(mgr *response.ApprovalManager) *DefaultOrchestrator {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.approvalMgr = mgr
	return o
}

// RegisterEventHandler registers a listener for auditable orchestration events.
func (o *DefaultOrchestrator) RegisterEventHandler(handler func(event OrchestrationEvent)) {
	if handler == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.eventHandlers = append(o.eventHandlers, handler)
}

// GetOrchestrationState returns the current cached orchestration state for an incident.
func (o *DefaultOrchestrator) GetOrchestrationState(incidentID string) (OrchestrationState, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	st, ok := o.stateCache[incidentID]
	return st, ok
}

// OrchestrateIncident coordinates a controlled, bounded response cycle for the given active incident.
// Concurrently calling OrchestrateIncident for the same IncidentID suppresses duplicate executions;
// exactly one leader executes the workflow and all callers receive the identical result.
func (o *DefaultOrchestrator) OrchestrateIncident(ctx context.Context, inc *types.Incident, opts ...Option) (*OrchestrationResult, error) {
	now := o.clock.Now()

	// 1. Context check
	if err := ctx.Err(); err != nil {
		return o.cancelledResult(inc, err)
	}

	// 2. Incident validation
	if inc == nil {
		return nil, ErrNilIncident
	}
	if err := inc.Validate(); err != nil {
		return nil, fmt.Errorf("invalid incident: %w", err)
	}

	// 3. Stale / Non-actionable Incident Check
	// If incident is already in RECOVERED or NORMAL, no mitigation action is required.
	if inc.Status == types.StatusRecovered || inc.Status == types.StatusNormal {
		res := &OrchestrationResult{
			IncidentID:  inc.IncidentID,
			NodeID:      inc.NodeID,
			State:       StateRecovered,
			Message:     fmt.Sprintf("incident is in non-actionable status %s; mitigation suppressed", inc.Status),
			CompletedAt: now,
		}
		o.setState(inc.IncidentID, StateRecovered)
		return res, nil
	}

	// 4. Concurrency & Idempotency Gate
	o.mu.Lock()
	if hist, ok := o.history[inc.IncidentID]; ok {
		o.mu.Unlock()
		return hist, nil
	}

	if flight, inProg := o.inFlight[inc.IncidentID]; inProg {
		o.mu.Unlock()
		select {
		case <-flight.done:
			return flight.res, flight.err
		case <-ctx.Done():
			return o.cancelledResult(inc, ctx.Err())
		}
	}

	flight := &inFlightOrchestration{
		done: make(chan struct{}),
	}
	o.inFlight[inc.IncidentID] = flight
	o.stateCache[inc.IncidentID] = StateRunning
	o.mu.Unlock()

	var finalRes *OrchestrationResult
	var finalErr error

	defer func() {
		o.mu.Lock()
		flight.res = finalRes
		flight.err = finalErr
		close(flight.done)
		delete(o.inFlight, inc.IncidentID)
		if finalRes != nil {
			o.stateCache[inc.IncidentID] = finalRes.State
			if finalRes.State.IsTerminal() {
				o.history[inc.IncidentID] = finalRes
			}
		}
		o.mu.Unlock()
	}()

	options := Options{}
	for _, opt := range opts {
		opt(&options)
	}

	finalRes, finalErr = o.executeCycle(ctx, inc, options)
	return finalRes, finalErr
}

// executeCycle executes the deterministic, fail-closed orchestration loop.
func (o *DefaultOrchestrator) executeCycle(ctx context.Context, inc *types.Incident, options Options) (*OrchestrationResult, error) {
	now := o.clock.Now()

	// 1. Persistence query check
	prevMitigations, err := o.mitStore.ListMitigations(ctx, inc.IncidentID, 100)
	if err != nil {
		res := o.newResult(inc, StateFailed, 0, "", "", "failed to query prior mitigations from store: "+err.Error(), ErrPersistenceFailed)
		o.emitEvent(res, 0, "", "", types.MitigationActionType(""), "", types.MitigationStatus(""), types.VerificationStatus(""), escalation.DecisionActionNone)
		return res, ErrPersistenceFailed
	}

	// 2. Check if incident recovery was already confirmed in durable verification store
	if o.vStore != nil {
		verifs, vErr := o.vStore.ListVerifications(ctx, inc.IncidentID, 10)
		if vErr == nil && len(verifs) > 0 {
			latestV := verifs[len(verifs)-1]
			if latestV.Status == types.VerificationStatusRecovered {
				res := o.newResult(inc, StateRecovered, len(prevMitigations), latestV.DecisionID, latestV.ActionID, "incident recovery already confirmed in durable store", nil)
				o.setState(inc.IncidentID, StateRecovered)
				return res, nil
			}
		}
	}

	// 2. Check for unknown reconciliation state
	for _, m := range prevMitigations {
		if m.Status == types.MitigationStatusUnknownReconciliationRequired {
			res := o.newResult(inc, StateFailed, len(prevMitigations), m.ActionID, m.DecisionID, "prior mitigation in unknown reconciliation state; autonomous retry prohibited", ErrUnknownReconciliation)
			o.emitEvent(res, len(prevMitigations), m.DecisionID, m.ActionID, m.ActionType, m.Target, m.Status, types.VerificationStatus(""), escalation.DecisionActionNone)
			return res, ErrUnknownReconciliation
		}
	}

	// 3. Upfront Circuit Breaker Check
	cbStatus, err := o.escEngine.GetCircuitBreakerStatus(ctx, inc.NodeID, inc.IncidentID)
	if err != nil {
		res := o.newResult(inc, StateFailed, 0, "", "", "failed to retrieve circuit breaker status: "+err.Error(), err)
		return res, err
	}
	if cbStatus != nil && cbStatus.State == types.CircuitOpen {
		evalReq := escalation.FailureEvaluationRequest{
			IncidentID:            inc.IncidentID,
			NodeID:                inc.NodeID,
			ActionID:              "cb-open-check",
			FailureClassification: types.FailureCircuitBreakerOpen,
			Reason:                "circuit breaker is OPEN; autonomous remediation prohibited",
		}
		escDec, _ := o.escEngine.EvaluateFailure(ctx, evalReq)

		// Transition active incident to ESCALATED if not already escalated
		if inc.Status != types.StatusEscalated {
			_ = o.escEngine.HandoffToIncidentEngine(ctx, o.incEngine, inc.NodeID, incidentMetric(inc), types.StatusEscalated, now)
		}

		res := o.newResult(inc, StateEscalated, len(prevMitigations), "", "", "circuit breaker is OPEN; autonomous remediation prohibited; incident escalated", ErrCircuitBreakerOpen)
		res.EscalationDecision = escDec
		o.emitEvent(res, len(prevMitigations), "", "", types.MitigationActionType(""), "", types.MitigationStatus(""), types.VerificationStatus(""), escalation.DecisionActionEscalate)
		return res, ErrCircuitBreakerOpen
	}

	// 4. Upfront Cooldown Check
	if len(prevMitigations) > 0 {
		canRetry, nextRetry, err := o.escEngine.CanRetry(ctx, inc.NodeID, inc.IncidentID)
		if err != nil {
			res := o.newResult(inc, StateFailed, len(prevMitigations), "", "", "retry check failed: "+err.Error(), err)
			return res, err
		}
		if !canRetry {
			msg := "remediation cooldown is active; retry delayed"
			if nextRetry != nil {
				msg = fmt.Sprintf("remediation cooldown active until %v", nextRetry)
			}
			res := o.newResult(inc, StateFailed, len(prevMitigations), "", "", msg, ErrCooldownActive)
			o.emitEvent(res, len(prevMitigations), "", "", types.MitigationActionType(""), "", types.MitigationStatus(""), types.VerificationStatus(""), escalation.DecisionActionNone)
			return res, ErrCooldownActive
		}
	}

	// 5. Bounded Loop
	attempt := len(prevMitigations) + 1
	for {
		if err := ctx.Err(); err != nil {
			return o.cancelledResult(inc, err)
		}
		if attempt > o.cfg.MaxExecutionCeiling {
			res := o.newResult(inc, StateFailed, attempt-1, "", "", "hard safety ceiling exceeded", ErrMaxAttemptsExceeded)
			return res, ErrMaxAttemptsExceeded
		}

		// A. Policy Evaluation
		evalInc := inc
		if inc.Status != types.StatusAnomalyDetected {
			evalInc = &types.Incident{
				IncidentID:    inc.IncidentID,
				NodeID:        inc.NodeID,
				RuleName:      inc.RuleName,
				Severity:      inc.Severity,
				Status:        types.StatusAnomalyDetected,
				TriggerMetric: inc.TriggerMetric,
				TriggeredAt:   inc.TriggeredAt,
			}
		}
		dec, err := o.policy.Evaluate(ctx, evalInc)
		if err != nil {
			if errors.Is(err, response.ErrNoRuleMatched) {
				// NO_ACTION semantics: no mitigation executed, cycle stops, incident state remains unchanged
				res := o.newResult(inc, StateFailed, attempt, "", "", "response policy determined NO_ACTION; orchestration stopped for cycle; incident state remains unchanged", nil)
				o.emitEvent(res, attempt, "", "", types.MitigationActionType(""), "", types.MitigationStatusSkipped, types.VerificationStatus(""), escalation.DecisionActionNone)
				return res, nil
			}
			res := o.newResult(inc, StateFailed, attempt, "", "", "policy evaluation failed: "+err.Error(), err)
			return res, err
		}
		if dec == nil {
			res := o.newResult(inc, StateFailed, attempt, "", "", "policy produced nil decision", errors.New("policy produced nil decision"))
			return res, res.Error
		}

		// Handle NO_ACTION: no mitigation executed, cycle stops, incident state remains unchanged
		if dec.ActionType == "" || dec.ActionType == "NO_ACTION" {
			res := o.newResult(inc, StateFailed, attempt, dec.DecisionID, "", "response policy determined NO_ACTION; orchestration stopped for cycle; incident state remains unchanged", nil)
			o.emitEvent(res, attempt, dec.DecisionID, "", dec.ActionType, dec.Target, types.MitigationStatusSkipped, types.VerificationStatus(""), escalation.DecisionActionNone)
			return res, nil
		}

		// Fail closed on FORBIDDEN action immediately
		if dec.AuthorizationClass == response.AuthClassForbidden {
			res := o.newResult(inc, StateFailed, attempt, dec.DecisionID, "", "action is classified as FORBIDDEN by response policy", ErrForbiddenAction)
			o.emitEvent(res, attempt, dec.DecisionID, "", dec.ActionType, dec.Target, types.MitigationStatus(""), types.VerificationStatus(""), escalation.DecisionActionNone)
			return res, ErrForbiddenAction
		}

		// B. Approval Gate for APPROVAL_REQUIRED actions
		if dec.AuthorizationClass == response.AuthClassApprovalRequired {
			if options.Approval == nil {
				if o.approvalMgr != nil {
					req := response.ApprovalRequest{
						Decision:    dec,
						RequestedBy: "orchestrator",
						Reason:      dec.Reason,
					}
					_, _ = o.approvalMgr.RequestApproval(ctx, req)
				}
				res := o.newResult(inc, StateWaitingApproval, attempt, dec.DecisionID, "", "operator approval required; waiting for approval", ErrApprovalMissing)
				o.emitEvent(res, attempt, dec.DecisionID, "", dec.ActionType, dec.Target, types.MitigationStatus(""), types.VerificationStatus(""), escalation.DecisionActionNone)
				return res, nil
			}

			appErr := response.ValidateApprovalForExecution(options.Approval, dec, inc, o.clock.Now())
			if appErr != nil {
				res := o.newResult(inc, StateFailed, attempt, dec.DecisionID, options.Approval.ActionID, "approval validation failed: "+appErr.Error(), ErrApprovalInvalid)
				evalReq := escalation.FailureEvaluationRequest{
					IncidentID:            inc.IncidentID,
					NodeID:                inc.NodeID,
					ActionID:              options.Approval.ActionID,
					FailureClassification: types.FailureMitigationFailed,
					Reason:                "approval validation failed: " + appErr.Error(),
				}
				_, _ = o.escEngine.EvaluateFailure(ctx, evalReq)
				o.emitEvent(res, attempt, dec.DecisionID, options.Approval.ActionID, dec.ActionType, dec.Target, types.MitigationStatusFailed, types.VerificationStatus(""), escalation.DecisionActionNone)
				return res, ErrApprovalInvalid
			}
		}

		// C. Pre-Execution Safety Validation Boundary
		var safetyRes *response.SafetyResult
		if av, ok := o.validator.(approvedValidator); ok && dec.AuthorizationClass == response.AuthClassApprovalRequired && options.Approval != nil {
			safetyRes, err = av.ValidateApproved(ctx, dec, inc)
		} else {
			safetyRes, err = o.validator.Validate(ctx, dec, inc)
		}

		if err != nil || safetyRes == nil || !safetyRes.Allowed {
			errMsg := "safety validation failed"
			if safetyRes != nil && safetyRes.Reason != "" {
				errMsg = fmt.Sprintf("safety validation rejected: %s (%s)", safetyRes.Reason, safetyRes.ValidationCode)
			} else if err != nil {
				errMsg = fmt.Sprintf("safety validation error: %v", err)
			}
			res := o.newResult(inc, StateFailed, attempt, dec.DecisionID, "", errMsg, ErrSafetyValidationFailed)
			o.emitEvent(res, attempt, dec.DecisionID, "", dec.ActionType, dec.Target, types.MitigationStatus(""), types.VerificationStatus(""), escalation.DecisionActionNone)
			return res, ErrSafetyValidationFailed
		}

		validatedDec, err := response.NewValidatedDecision(dec, safetyRes)
		if err != nil {
			res := o.newResult(inc, StateFailed, attempt, dec.DecisionID, "", "failed to construct validated decision: "+err.Error(), err)
			return res, err
		}

		// Invariant: Durable persistence precedes simulated actuation.
		// The orchestrator requires the mitigation record to be successfully persisted before invoking the executor.
		actionID := fmt.Sprintf("act-%s", dec.DecisionID[4:])
		recordDecisionID := dec.DecisionID
		if attempt > 1 {
			actionID = fmt.Sprintf("%s-r%d", actionID, attempt)
			recordDecisionID = fmt.Sprintf("%s-r%d", recordDecisionID, attempt)
		}
		inFlightRec := &storage.StoredMitigation{
			ActionID:   actionID,
			DecisionID: recordDecisionID,
			IncidentID: inc.IncidentID,
			NodeID:     dec.NodeID,
			ActionType: dec.ActionType,
			Target:     dec.Target,
			Status:     types.MitigationStatusExecuting,
			Mode:       string(dec.ExecutionMode),
			Message:    "simulated action execution started",
			Parameters: dec.Parameters,
			StartedAt:  o.clock.Now(),
			DurationMs: 0,
			Simulated:  true,
			CreatedAt:  o.clock.Now(),
			UpdatedAt:  o.clock.Now(),
		}
		if err := o.mitStore.RecordMitigation(ctx, inFlightRec); err != nil {
			// If persistence fails: executor MUST NOT be invoked, orchestration fails closed, no retry should be fabricated locally.
			res := o.newResult(inc, StateFailed, attempt, dec.DecisionID, actionID, "failed to persist mitigation record prior to execution: "+err.Error(), ErrPersistenceFailed)
			o.emitEvent(res, attempt, dec.DecisionID, actionID, dec.ActionType, dec.Target, types.MitigationStatus(""), types.VerificationStatus(""), escalation.DecisionActionNone)
			return res, ErrPersistenceFailed
		}

		// D. Incident FSM Transition to MITIGATING
		if inc.Status == types.StatusAnomalyDetected {
			tInc, tErr := o.incEngine.TransitionActiveIncident(ctx, inc.NodeID, incidentMetric(inc), types.StatusMitigating, o.clock.Now())
			if tErr == nil && tInc != nil {
				inc = tInc
			}
		}

		// E. Simulated Mitigation Execution
		o.setState(inc.IncidentID, StateExecuting)
		var execRes *response.ExecutionResult
		var execErr error

		if dec.AuthorizationClass == response.AuthClassApprovalRequired && options.Approval != nil {
			execRes, execErr = o.executor.ExecuteWithApproval(ctx, dec, inc, options.Approval.ApprovalID)
		} else {
			execRes, execErr = o.executor.ExecuteValidated(ctx, validatedDec)
		}

		if execErr != nil || execRes == nil || execRes.Status != types.MitigationStatusExecuted {
			actionID := ""
			if execRes != nil {
				actionID = execRes.ExecutionID
			} else {
				actionID = fmt.Sprintf("act-%s", dec.DecisionID[4:])
			}
			mitAction := types.MitigationAction{
				ActionID:    actionID,
				IncidentID:  inc.IncidentID,
				ActionType:  dec.ActionType,
				Target:      dec.Target,
				Status:      types.MitigationStatusFailed,
				TriggeredAt: o.clock.Now(),
			}
			if execRes != nil {
				mitAction = execRes.ToMitigationAction()
			}
			if execErr != nil {
				mitAction.Error = execErr.Error()
			}

			// Update persisted mitigation record to failed
			_ = o.mitStore.MarkMitigationFailed(ctx, actionID, mitAction.Message, mitAction.Error, o.clock.Now(), 0)

			escDec, escErr := o.escEngine.EvaluateMitigationResult(ctx, inc.NodeID, &mitAction)
			if escErr != nil {
				res := o.newResult(inc, StateFailed, attempt, dec.DecisionID, actionID, "escalation evaluation failed: "+escErr.Error(), escErr)
				return res, escErr
			}

			res := o.newResult(inc, StateFailed, attempt, dec.DecisionID, actionID, "mitigation execution failed", execErr)
			res.ExecutionResult = execRes
			res.EscalationDecision = escDec

			o.emitEvent(res, attempt, dec.DecisionID, actionID, dec.ActionType, dec.Target, mitAction.Status, types.VerificationStatus(""), escDec.Action)

			switch escDec.Action {
			case escalation.DecisionActionNone:
				return res, execErr
			case escalation.DecisionActionEscalate:
				_ = o.escEngine.HandoffToIncidentEngine(ctx, o.incEngine, inc.NodeID, incidentMetric(inc), types.StatusEscalated, o.clock.Now())
				res.State = StateEscalated
				res.Message = "mitigation failed; incident escalated"
				return res, nil
			case escalation.DecisionActionRetry:
				canRetry, _, _ := o.escEngine.CanRetry(ctx, inc.NodeID, inc.IncidentID)
				if !canRetry {
					res.State = StateFailed
					res.Message = "retry authorized by policy but delayed by cooldown/circuit"
					return res, nil
				}
				res.State = StateRetryAuthorized
				attempt++
				continue
			}
		}

		// Update persisted mitigation record to executed
		_ = o.mitStore.MarkMitigationExecuted(ctx, actionID, execRes.Message, execRes.CompletedAt, execRes.Duration.Milliseconds())

		// F. Closed-Loop Verification
		o.setState(inc.IncidentID, StateVerifying)
		cond := verification.NewThresholdUpperCondition(incidentMetric(inc), 70.0)
		if options.RecoveryCondition != nil {
			cond = *options.RecoveryCondition
		}
		vTimeout := o.cfg.DefaultVerificationTimeout
		if options.VerificationTimeout > 0 {
			vTimeout = options.VerificationTimeout
		}

		vReq := verification.VerificationRequest{
			IncidentID:           inc.IncidentID,
			ActionID:             execRes.ExecutionID,
			DecisionID:           execRes.DecisionID,
			NodeID:               execRes.NodeID,
			MitigationStatus:     execRes.Status,
			Condition:            cond,
			StartedAt:            o.clock.Now(),
			Timeout:              vTimeout,
			RequiredObservations: o.cfg.DefaultRequiredObservations,
		}

		vRes, vErr := o.vEngine.StartVerification(ctx, vReq)
		if vErr != nil {
			res := o.newResult(inc, StateFailed, attempt, execRes.DecisionID, execRes.ExecutionID, "failed to start verification: "+vErr.Error(), vErr)
			res.ExecutionResult = execRes
			return res, vErr
		}

		// Synchronously ingest telemetry samples if supplied
		for _, s := range options.TelemetrySamples {
			if updatedV, sErr := o.vEngine.ProcessSample(ctx, s); sErr == nil && updatedV != nil {
				vRes = updatedV
			}
		}

		// G. Verification Outcome Handling
		switch vRes.Status {
		case types.VerificationStatusRecovered:
			_, _ = o.vEngine.HandoffToIncidentEngine(ctx, vRes.VerificationID, o.incEngine)
			res := o.newResult(inc, StateRecovered, attempt, execRes.DecisionID, execRes.ExecutionID, "verification confirmed recovery; incident resolved", nil)
			res.ExecutionResult = execRes
			res.VerificationResult = vRes
			o.emitEvent(res, attempt, execRes.DecisionID, execRes.ExecutionID, execRes.ActionType, execRes.Target, execRes.Status, vRes.Status, escalation.DecisionActionNone)
			return res, nil

		case types.VerificationStatusPending:
			res := o.newResult(inc, StateVerifying, attempt, execRes.DecisionID, execRes.ExecutionID, "mitigation executed; verification is observing telemetry", nil)
			res.ExecutionResult = execRes
			res.VerificationResult = vRes
			o.emitEvent(res, attempt, execRes.DecisionID, execRes.ExecutionID, execRes.ActionType, execRes.Target, execRes.Status, vRes.Status, escalation.DecisionActionNone)
			return res, nil

		default:
			// TIMED_OUT, NOT_RECOVERED, CANCELLED
			storedV := &storage.StoredVerification{
				VerificationID:       vRes.VerificationID,
				IncidentID:           vRes.IncidentID,
				ActionID:             vRes.ActionID,
				DecisionID:           vRes.DecisionID,
				NodeID:               vRes.NodeID,
				MetricName:           vRes.MetricName,
				Status:               vRes.Status,
				RequiredObservations: vRes.RequiredObservations,
				ConsecutiveHealthy:   vRes.ConsecutiveHealthy,
				TotalObservations:    vRes.Observations,
				StartedAt:            vRes.StartedAt,
				ExpiresAt:            vRes.ExpiresAt,
				CompletedAt:          vRes.CompletedAt,
				RecoveredAt:          vRes.RecoveredAt,
				Reason:               vRes.Reason,
				Evidence:             vRes.Evidence,
			}

			escDec, escErr := o.escEngine.EvaluateVerificationResult(ctx, storedV)
			if escErr != nil {
				res := o.newResult(inc, StateFailed, attempt, execRes.DecisionID, execRes.ExecutionID, "escalation evaluation failed: "+escErr.Error(), escErr)
				res.ExecutionResult = execRes
				res.VerificationResult = vRes
				return res, escErr
			}

			res := o.newResult(inc, StateFailed, attempt, execRes.DecisionID, execRes.ExecutionID, "verification failed: "+vRes.Reason, nil)
			res.ExecutionResult = execRes
			res.VerificationResult = vRes
			res.EscalationDecision = escDec

			o.emitEvent(res, attempt, execRes.DecisionID, execRes.ExecutionID, execRes.ActionType, execRes.Target, execRes.Status, vRes.Status, escDec.Action)

			switch escDec.Action {
			case escalation.DecisionActionNone:
				return res, nil
			case escalation.DecisionActionEscalate:
				_ = o.escEngine.HandoffToIncidentEngine(ctx, o.incEngine, inc.NodeID, incidentMetric(inc), types.StatusEscalated, o.clock.Now())
				res.State = StateEscalated
				res.Message = "verification failed; incident escalated"
				return res, nil
			case escalation.DecisionActionRetry:
				canRetry, _, _ := o.escEngine.CanRetry(ctx, inc.NodeID, inc.IncidentID)
				if !canRetry {
					res.State = StateFailed
					res.Message = "retry authorized by policy but delayed by cooldown/circuit"
					return res, nil
				}
				res.State = StateRetryAuthorized
				attempt++
				continue
			}
		}
	}
}

// ProcessTelemetry ingests subsequent telemetry samples and evaluates active verifications.
func (o *DefaultOrchestrator) ProcessTelemetry(ctx context.Context, sample types.MetricSample) (*verification.VerificationResult, error) {
	if err := sample.Validate(); err != nil {
		return nil, err
	}
	vRes, err := o.vEngine.ProcessSample(ctx, sample)
	if err != nil {
		return nil, err
	}
	if vRes == nil {
		return nil, nil
	}

	now := o.clock.Now()
	if vRes.Status == types.VerificationStatusRecovered {
		_, _ = o.vEngine.HandoffToIncidentEngine(ctx, vRes.VerificationID, o.incEngine)
		o.mu.Lock()
		o.stateCache[vRes.IncidentID] = StateRecovered
		res := &OrchestrationResult{
			IncidentID:         vRes.IncidentID,
			NodeID:             vRes.NodeID,
			DecisionID:         vRes.DecisionID,
			ActionID:           vRes.ActionID,
			State:              StateRecovered,
			VerificationResult: vRes,
			Message:            "verification confirmed recovery via telemetry stream",
			CompletedAt:        now,
		}
		o.history[vRes.IncidentID] = res
		o.mu.Unlock()
		o.emitEvent(res, 1, vRes.DecisionID, vRes.ActionID, types.MitigationActionType(""), "", types.MitigationStatusExecuted, vRes.Status, escalation.DecisionActionNone)
	} else if vRes.Status == types.VerificationStatusTimedOut || vRes.Status == types.VerificationStatusNotRecovered {
		storedV := &storage.StoredVerification{
			VerificationID:       vRes.VerificationID,
			IncidentID:           vRes.IncidentID,
			ActionID:             vRes.ActionID,
			DecisionID:           vRes.DecisionID,
			NodeID:               vRes.NodeID,
			MetricName:           vRes.MetricName,
			Status:               vRes.Status,
			RequiredObservations: vRes.RequiredObservations,
			ConsecutiveHealthy:   vRes.ConsecutiveHealthy,
			TotalObservations:    vRes.Observations,
			StartedAt:            vRes.StartedAt,
			ExpiresAt:            vRes.ExpiresAt,
			CompletedAt:          vRes.CompletedAt,
			RecoveredAt:          vRes.RecoveredAt,
			Reason:               vRes.Reason,
			Evidence:             vRes.Evidence,
		}
		escDec, _ := o.escEngine.EvaluateVerificationResult(ctx, storedV)
		if escDec != nil && escDec.Action == escalation.DecisionActionEscalate {
			_ = o.escEngine.HandoffToIncidentEngine(ctx, o.incEngine, vRes.NodeID, vRes.MetricName, types.StatusEscalated, now)
			o.mu.Lock()
			o.stateCache[vRes.IncidentID] = StateEscalated
			res := &OrchestrationResult{
				IncidentID:         vRes.IncidentID,
				NodeID:             vRes.NodeID,
				DecisionID:         vRes.DecisionID,
				ActionID:           vRes.ActionID,
				State:              StateEscalated,
				VerificationResult: vRes,
				EscalationDecision: escDec,
				Message:            "verification failed; incident escalated",
				CompletedAt:        now,
			}
			o.history[vRes.IncidentID] = res
			o.mu.Unlock()
			o.emitEvent(res, 1, vRes.DecisionID, vRes.ActionID, types.MitigationActionType(""), "", types.MitigationStatusExecuted, vRes.Status, escDec.Action)
		}
	}

	return vRes, nil
}

func (o *DefaultOrchestrator) newResult(inc *types.Incident, state OrchestrationState, attempt int, decID, actID, msg string, err error) *OrchestrationResult {
	incID := ""
	nodeID := ""
	if inc != nil {
		incID = inc.IncidentID
		nodeID = inc.NodeID
	}
	return &OrchestrationResult{
		IncidentID:    incID,
		NodeID:        nodeID,
		DecisionID:    decID,
		ActionID:      actID,
		State:         state,
		TotalAttempts: attempt,
		Message:       msg,
		Error:         err,
		CompletedAt:   o.clock.Now(),
	}
}

func (o *DefaultOrchestrator) cancelledResult(inc *types.Incident, err error) (*OrchestrationResult, error) {
	incID := ""
	nodeID := ""
	if inc != nil {
		incID = inc.IncidentID
		nodeID = inc.NodeID
	}
	res := &OrchestrationResult{
		IncidentID:  incID,
		NodeID:      nodeID,
		State:       StateCancelled,
		Message:     "orchestration cancelled by context",
		Error:       ErrOrchestrationCancelled,
		CompletedAt: o.clock.Now(),
	}
	if incID != "" {
		o.setState(incID, StateCancelled)
	}
	return res, err
}

func (o *DefaultOrchestrator) setState(incidentID string, state OrchestrationState) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.stateCache[incidentID] = state
}

func (o *DefaultOrchestrator) emitEvent(
	res *OrchestrationResult,
	attempt int,
	decID, actID string,
	actionType types.MitigationActionType,
	target string,
	execStatus types.MitigationStatus,
	vStatus types.VerificationStatus,
	escDec escalation.DecisionAction,
) {
	event := OrchestrationEvent{
		IncidentID:         res.IncidentID,
		NodeID:             res.NodeID,
		DecisionID:         decID,
		ActionID:           actID,
		Attempt:            attempt,
		State:              res.State,
		ActionType:         actionType,
		Target:             target,
		ExecutionStatus:    execStatus,
		VerificationStatus: vStatus,
		EscalationDecision: escDec,
		Timestamp:          o.clock.Now(),
		Message:            res.Message,
	}
	if res.Error != nil {
		event.Error = res.Error.Error()
	}

	o.mu.RLock()
	handlers := make([]func(event OrchestrationEvent), len(o.eventHandlers))
	copy(handlers, o.eventHandlers)
	o.mu.RUnlock()

	for _, h := range handlers {
		h(event)
	}
}

func incidentMetric(inc *types.Incident) string {
	if inc != nil && inc.TriggerMetric != "" {
		return inc.TriggerMetric
	}
	return "system_metric"
}

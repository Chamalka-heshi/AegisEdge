package response

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// Common executor domain errors.
var (
	ErrNilSafetyResult        = errors.New("safety result cannot be nil")
	ErrUnvalidatedDecision    = errors.New("decision has not passed safety validation")
	ErrDecisionResultMismatch = errors.New("safety result decision id does not match decision")
	ErrExecutionCancelled     = errors.New("action execution was cancelled")
	ErrUnknownActionType      = errors.New("unknown action type cannot be executed")
)

// ExecutionResult documents the structured, observable outcome of a simulated action execution.
// In Phase 6.3, all execution is strictly SIMULATED with ZERO real host side effects.
type ExecutionResult struct {
	ExecutionID string                     `json:"execution_id"`
	DecisionID  string                     `json:"decision_id"`
	IncidentID  string                     `json:"incident_id"`
	NodeID      string                     `json:"node_id"`
	ActionType  types.MitigationActionType `json:"action_type"`
	Target      string                     `json:"target"`
	Status      types.MitigationStatus     `json:"status"`
	Mode        ExecutionMode              `json:"mode"`
	Message     string                     `json:"message"`
	ErrorCode   string                     `json:"error_code,omitempty"`
	StartedAt   time.Time                  `json:"started_at"`
	CompletedAt time.Time                  `json:"completed_at"`
	Duration    time.Duration              `json:"duration_ns"`
	Simulated   bool                       `json:"simulated"`
}

// ToMitigationAction maps the ExecutionResult to the canonical shared domain MitigationAction.
func (r *ExecutionResult) ToMitigationAction() types.MitigationAction {
	completed := r.CompletedAt
	actionID := r.ExecutionID
	if strings.HasPrefix(r.DecisionID, "dec-") {
		actionID = fmt.Sprintf("act-%s", r.DecisionID[4:])
	}
	return types.MitigationAction{
		ActionID:    actionID,
		IncidentID:  r.IncidentID,
		ActionType:  r.ActionType,
		Target:      r.Target,
		Status:      r.Status,
		Message:     r.Message,
		Error:       r.ErrorCode,
		TriggeredAt: r.StartedAt,
		CompletedAt: &completed,
	}
}

// ValidatedDecision binds a candidate ResponseDecision to its successful SafetyResult.
// ARCHITECTURAL INVARIANT: Unvalidated decisions cannot enter the execution pipeline.
type ValidatedDecision struct {
	Decision     *ResponseDecision `json:"decision"`
	SafetyResult *SafetyResult     `json:"safety_result"`
}

// NewValidatedDecision constructs a verified ValidatedDecision wrapper.
// It fails closed if the decision or safety result is nil, if the decision IDs mismatch,
// or if the safety validator did not mark the decision as ALLOWED.
func NewValidatedDecision(dec *ResponseDecision, res *SafetyResult) (*ValidatedDecision, error) {
	if dec == nil {
		return nil, ErrNilDecision
	}
	if res == nil {
		return nil, ErrNilSafetyResult
	}
	if dec.DecisionID != res.DecisionID {
		return nil, fmt.Errorf("%w: decision %q vs result %q", ErrDecisionResultMismatch, dec.DecisionID, res.DecisionID)
	}
	if !res.Allowed {
		return nil, fmt.Errorf("%w: validation code %s: %s", ErrUnvalidatedDecision, res.ValidationCode, res.Reason)
	}
	return &ValidatedDecision{
		Decision:     dec,
		SafetyResult: res,
	}, nil
}

// ActionExecutor executes or simulates validated mitigation decisions.
// In Phase 6.3/6.5, all execution is strictly simulated with zero real host mutations.
type ActionExecutor interface {
	Execute(ctx context.Context, dec *ResponseDecision, inc *types.Incident) (*ExecutionResult, error)
	ExecuteValidated(ctx context.Context, vd *ValidatedDecision) (*ExecutionResult, error)
	ExecuteWithApproval(ctx context.Context, dec *ResponseDecision, inc *types.Incident, approvalID string) (*ExecutionResult, error)
}

// inFlightExecution tracks an in-flight validation and simulation attempt for a DecisionID,
// preventing concurrent duplicate simulations.
type inFlightExecution struct {
	done chan struct{}
	res  *ExecutionResult
	err  error
}

// SimulatedExecutor implements ActionExecutor for controlled, observable simulation.
// CRITICAL SAFETY BOUNDARY: This component performs ZERO real host operations.
// No shell execution, no process termination, no service restart, no network isolation,
// and no filesystem mutations are performed.
type SimulatedExecutor struct {
	mu              sync.RWMutex
	validator       SafetyValidator
	store           storage.MitigationStore       // durable mitigation store (Phase 6.4)
	approvalStore   storage.ApprovalStore         // durable approval store (Phase 6.5)
	clock           Clock                         // clock abstraction for deterministic testing (Phase 6.5)
	history         map[string]*ExecutionResult   // in-memory fast cache (key: DecisionID -> cached result)
	inFlight        map[string]*inFlightExecution // key: DecisionID -> active execution
	simulationCount int64                         // atomic counter tracking actual simulation body executions
}

// NewSimulatedExecutor constructs a SimulatedExecutor using the provided safety validator.
// If validator is nil, NewStandardValidator() is used by default.
func NewSimulatedExecutor(v SafetyValidator) *SimulatedExecutor {
	if v == nil {
		v = NewStandardValidator()
	}
	return &SimulatedExecutor{
		validator: v,
		clock:     RealClock{},
		history:   make(map[string]*ExecutionResult),
		inFlight:  make(map[string]*inFlightExecution),
	}
}

// NewSimulatedExecutorWithStore constructs a SimulatedExecutor wired with a durable storage.MitigationStore.
func NewSimulatedExecutorWithStore(v SafetyValidator, store storage.MitigationStore) *SimulatedExecutor {
	exec := NewSimulatedExecutor(v)
	exec.store = store
	if as, ok := store.(storage.ApprovalStore); ok {
		exec.approvalStore = as
	}
	return exec
}

// WithStore assigns a durable storage.MitigationStore to the SimulatedExecutor.
func (e *SimulatedExecutor) WithStore(store storage.MitigationStore) *SimulatedExecutor {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.store = store
	if as, ok := store.(storage.ApprovalStore); ok && e.approvalStore == nil {
		e.approvalStore = as
	}
	return e
}

// WithApprovalStore assigns a durable storage.ApprovalStore to the SimulatedExecutor.
func (e *SimulatedExecutor) WithApprovalStore(as storage.ApprovalStore) *SimulatedExecutor {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.approvalStore = as
	return e
}

// WithClock sets a custom clock abstraction for deterministic testing.
func (e *SimulatedExecutor) WithClock(c Clock) *SimulatedExecutor {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.clock = c
	return e
}

func (e *SimulatedExecutor) now() time.Time {
	if e.clock != nil {
		return e.clock.Now()
	}
	return time.Now().UTC()
}

// SimulationCount returns the total number of times a simulation body was actually dispatched.
// This is used for instrumentation and concurrency verification.
func (e *SimulatedExecutor) SimulationCount() int64 {
	return atomic.LoadInt64(&e.simulationCount)
}

// Execute performs end-to-end safety revalidation and simulation for the given decision and incident.
// It fails closed: unvalidated, forbidden, or out-of-bounds decisions will never execute.
// Concurrently calling Execute with the same DecisionID suppresses duplicate simulations; exactly one
// goroutine executes the simulation body and all callers receive the identical result.
func (e *SimulatedExecutor) Execute(ctx context.Context, dec *ResponseDecision, inc *types.Incident) (*ExecutionResult, error) {
	startedAt := time.Now().UTC()

	// 1. Context check
	if err := ctx.Err(); err != nil {
		decID := ""
		incID := ""
		nodeID := ""
		var actionType types.MitigationActionType
		target := ""
		mode := ExecutionModeDryRun
		if dec != nil {
			decID = dec.DecisionID
			incID = dec.IncidentID
			nodeID = dec.NodeID
			actionType = dec.ActionType
			target = dec.Target
			mode = dec.ExecutionMode
		}
		res := &ExecutionResult{
			ExecutionID: fmt.Sprintf("exec-cancelled-%d", startedAt.UnixNano()),
			DecisionID:  decID,
			IncidentID:  incID,
			NodeID:      nodeID,
			ActionType:  actionType,
			Target:      target,
			Status:      types.MitigationStatusFailed,
			Mode:        mode,
			Message:     fmt.Sprintf("action execution cancelled before start: %v", err),
			ErrorCode:   "CONTEXT_CANCELLED",
			StartedAt:   startedAt,
			CompletedAt: startedAt,
			Duration:    0,
			Simulated:   true,
		}
		return res, err
	}

	// 2. Argument validation
	if dec == nil {
		return nil, ErrNilDecision
	}
	if inc == nil {
		return nil, ErrNilIncident
	}

	// 3. Check in-memory history first
	e.mu.RLock()
	if cached, ok := e.history[dec.DecisionID]; ok {
		e.mu.RUnlock()
		return cached, nil
	}
	e.mu.RUnlock()

	// Check durable store if configured (handles post-restart identity and idempotency)
	if e.store != nil {
		if stored, err := e.store.GetMitigationByDecisionID(ctx, dec.DecisionID); err == nil && stored != nil {
			cached := storedToExecutionResult(stored)
			e.mu.Lock()
			e.history[dec.DecisionID] = cached
			e.mu.Unlock()
			return cached, nil
		}
	}

	// Check if this decision requires operator approval and whether an approved record exists in durable storage
	if dec.AuthorizationClass == AuthClassApprovalRequired && e.approvalStore != nil {
		app, err := e.approvalStore.GetApprovalByDecisionID(ctx, dec.DecisionID)
		if err == nil && app != nil && app.Status == types.ApprovalStatusApproved {
			return e.ExecuteWithApproval(ctx, dec, inc, app.ApprovalID)
		}
	}

	e.mu.Lock()
	// Re-check history in case another goroutine populated it
	if cached, ok := e.history[dec.DecisionID]; ok {
		e.mu.Unlock()
		return cached, nil
	}
	if flight, inProg := e.inFlight[dec.DecisionID]; inProg {
		e.mu.Unlock()
		// Another goroutine is already validating and simulating this exact DecisionID!
		// Await the leader's completion or context cancellation.
		select {
		case <-flight.done:
			return flight.res, flight.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// Register this goroutine as the leader for dec.DecisionID
	flight := &inFlightExecution{
		done: make(chan struct{}),
	}
	e.inFlight[dec.DecisionID] = flight
	e.mu.Unlock()

	var finalRes *ExecutionResult
	var finalErr error

	defer func() {
		e.mu.Lock()
		flight.res = finalRes
		flight.err = finalErr
		if finalRes != nil && finalRes.Status == types.MitigationStatusExecuted {
			e.history[dec.DecisionID] = finalRes
		}
		delete(e.inFlight, dec.DecisionID)
		close(flight.done)
		e.mu.Unlock()
	}()

	// 4. Revalidation at the Security Boundary (Fail-Closed)
	safetyRes, err := e.validator.Validate(ctx, dec, inc)
	if err != nil {
		finalErr = fmt.Errorf("safety validation error: %w", err)
		return nil, finalErr
	}

	if !safetyRes.Allowed {
		completedAt := time.Now().UTC()
		status := types.MitigationStatusFailed
		errCode := string(safetyRes.ValidationCode)

		// Operator approval required maps to SKIPPED in automated simulation
		if dec.AuthorizationClass == AuthClassApprovalRequired || dec.ExecutionMode == ExecutionModeApprovalRequired {
			status = types.MitigationStatusSkipped
		}

		finalRes = &ExecutionResult{
			ExecutionID: fmt.Sprintf("exec-%s", dec.DecisionID[4:]),
			DecisionID:  dec.DecisionID,
			IncidentID:  dec.IncidentID,
			NodeID:      dec.NodeID,
			ActionType:  dec.ActionType,
			Target:      dec.Target,
			Status:      status,
			Mode:        dec.ExecutionMode,
			Message:     fmt.Sprintf("safety validation rejected action: %s", safetyRes.Reason),
			ErrorCode:   errCode,
			StartedAt:   startedAt,
			CompletedAt: completedAt,
			Duration:    completedAt.Sub(startedAt),
			Simulated:   true,
		}

		if e.store != nil {
			stored := executionResultToStored(finalRes, dec.Parameters)
			_ = e.store.RecordMitigation(ctx, stored)
		}
		return finalRes, nil
	}

	// 5. Construct ValidatedDecision wrapper
	vd, err := NewValidatedDecision(dec, safetyRes)
	if err != nil {
		finalErr = fmt.Errorf("failed to create validated decision: %w", err)
		return nil, finalErr
	}

	// 5b. Persist in-flight state as EXECUTING in durable store (Phase 6.4)
	actionID := fmt.Sprintf("act-%s", dec.DecisionID[4:])
	if e.store != nil {
		inFlightRec := &storage.StoredMitigation{
			ActionID:   actionID,
			DecisionID: dec.DecisionID,
			IncidentID: dec.IncidentID,
			NodeID:     dec.NodeID,
			ActionType: dec.ActionType,
			Target:     dec.Target,
			Status:     types.MitigationStatusExecuting,
			Mode:       string(dec.ExecutionMode),
			Message:    "simulated action execution started",
			Parameters: dec.Parameters,
			StartedAt:  startedAt,
			DurationMs: 0,
			Simulated:  true,
			CreatedAt:  startedAt,
			UpdatedAt:  startedAt,
		}
		_ = e.store.RecordMitigation(ctx, inFlightRec)
	}

	// 6. Execute Simulation
	finalRes, finalErr = e.executeSimulation(ctx, vd, startedAt)
	if finalErr != nil {
		if e.store != nil {
			var errCode string
			if finalRes != nil {
				errCode = finalRes.ErrorCode
			}
			now := time.Now().UTC()
			_ = e.store.MarkMitigationFailed(ctx, actionID, finalErr.Error(), errCode, now, now.Sub(startedAt).Milliseconds())
		}
		return finalRes, finalErr
	}

	// 7. Persist completion in durable store (Phase 6.4)
	if e.store != nil && finalRes != nil {
		if finalRes.Status == types.MitigationStatusExecuted {
			_ = e.store.MarkMitigationExecuted(ctx, actionID, finalRes.Message, finalRes.CompletedAt, finalRes.Duration.Milliseconds())
		} else {
			_ = e.store.MarkMitigationFailed(ctx, actionID, finalRes.Message, finalRes.ErrorCode, finalRes.CompletedAt, finalRes.Duration.Milliseconds())
		}
	}

	// 8. Inform standard validator of action execution if applicable
	if sv, ok := e.validator.(*StandardSafetyValidator); ok {
		sv.RecordExecution(dec.Target, 300*time.Second)
	}

	return finalRes, nil
}

// ExecuteValidated executes a pre-validated decision wrapper.
func (e *SimulatedExecutor) ExecuteValidated(ctx context.Context, vd *ValidatedDecision) (*ExecutionResult, error) {
	startedAt := time.Now().UTC()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if vd == nil || vd.Decision == nil || vd.SafetyResult == nil {
		return nil, ErrUnvalidatedDecision
	}
	if !vd.SafetyResult.Allowed {
		return nil, fmt.Errorf("%w: safety result allowed=false", ErrUnvalidatedDecision)
	}

	decID := vd.Decision.DecisionID

	// Check in-memory history first
	e.mu.RLock()
	if cached, ok := e.history[decID]; ok {
		e.mu.RUnlock()
		return cached, nil
	}
	e.mu.RUnlock()

	// Check durable store if configured
	if e.store != nil {
		if stored, err := e.store.GetMitigationByDecisionID(ctx, decID); err == nil && stored != nil {
			cached := storedToExecutionResult(stored)
			e.mu.Lock()
			e.history[decID] = cached
			e.mu.Unlock()
			return cached, nil
		}
	}

	e.mu.Lock()
	if cached, ok := e.history[decID]; ok {
		e.mu.Unlock()
		return cached, nil
	}
	if flight, inProg := e.inFlight[decID]; inProg {
		e.mu.Unlock()
		select {
		case <-flight.done:
			return flight.res, flight.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	flight := &inFlightExecution{
		done: make(chan struct{}),
	}
	e.inFlight[decID] = flight
	e.mu.Unlock()

	var finalRes *ExecutionResult
	var finalErr error

	defer func() {
		e.mu.Lock()
		flight.res = finalRes
		flight.err = finalErr
		if finalRes != nil && finalRes.Status == types.MitigationStatusExecuted {
			e.history[decID] = finalRes
		}
		delete(e.inFlight, decID)
		close(flight.done)
		e.mu.Unlock()
	}()

	actionID := fmt.Sprintf("act-%s", vd.Decision.DecisionID[4:])
	if e.store != nil {
		inFlightRec := &storage.StoredMitigation{
			ActionID:   actionID,
			DecisionID: vd.Decision.DecisionID,
			IncidentID: vd.Decision.IncidentID,
			NodeID:     vd.Decision.NodeID,
			ActionType: vd.Decision.ActionType,
			Target:     vd.Decision.Target,
			Status:     types.MitigationStatusExecuting,
			Mode:       string(vd.Decision.ExecutionMode),
			Message:    "simulated action execution started",
			Parameters: vd.Decision.Parameters,
			StartedAt:  startedAt,
			DurationMs: 0,
			Simulated:  true,
			CreatedAt:  startedAt,
			UpdatedAt:  startedAt,
		}
		_ = e.store.RecordMitigation(ctx, inFlightRec)
	}

	finalRes, finalErr = e.executeSimulation(ctx, vd, startedAt)
	if finalErr != nil {
		if e.store != nil {
			var errCode string
			if finalRes != nil {
				errCode = finalRes.ErrorCode
			}
			now := time.Now().UTC()
			_ = e.store.MarkMitigationFailed(ctx, actionID, finalErr.Error(), errCode, now, now.Sub(startedAt).Milliseconds())
		}
		return finalRes, finalErr
	}

	if e.store != nil && finalRes != nil {
		if finalRes.Status == types.MitigationStatusExecuted {
			_ = e.store.MarkMitigationExecuted(ctx, actionID, finalRes.Message, finalRes.CompletedAt, finalRes.Duration.Milliseconds())
		} else {
			_ = e.store.MarkMitigationFailed(ctx, actionID, finalRes.Message, finalRes.ErrorCode, finalRes.CompletedAt, finalRes.Duration.Milliseconds())
		}
	}

	if sv, ok := e.validator.(*StandardSafetyValidator); ok {
		sv.RecordExecution(vd.Decision.Target, 300*time.Second)
	}

	return finalRes, nil
}

// executeSimulation dispatches the safe simulated action handler.
// In Phase 6.3, this produces controlled, descriptive simulation logs with zero real host mutation.
func (e *SimulatedExecutor) executeSimulation(ctx context.Context, vd *ValidatedDecision, startedAt time.Time) (*ExecutionResult, error) {
	dec := vd.Decision

	// Check context cancellation immediately prior to simulation dispatch
	if err := ctx.Err(); err != nil {
		completedAt := time.Now().UTC()
		return &ExecutionResult{
			ExecutionID: fmt.Sprintf("exec-%s", dec.DecisionID[4:]),
			DecisionID:  dec.DecisionID,
			IncidentID:  dec.IncidentID,
			NodeID:      dec.NodeID,
			ActionType:  dec.ActionType,
			Target:      dec.Target,
			Status:      types.MitigationStatusFailed,
			Mode:        dec.ExecutionMode,
			Message:     fmt.Sprintf("action execution cancelled before simulation: %v", err),
			ErrorCode:   "CONTEXT_CANCELLED",
			StartedAt:   startedAt,
			CompletedAt: completedAt,
			Duration:    completedAt.Sub(startedAt),
			Simulated:   true,
		}, err
	}

	// Increment atomic simulation counter
	atomic.AddInt64(&e.simulationCount, 1)

	// Handled simulation dispatch
	var message string
	switch dec.ActionType {
	case types.ActionSimulatedThrottle:
		pct := dec.Parameters["throttle_percent"]
		dur := dec.Parameters["duration_sec"]
		message = fmt.Sprintf("[SIMULATED] Throttling target %q (percent=%s%%, duration=%ss). Zero host mutations performed.", dec.Target, pct, dur)

	case types.ActionSimulatedRestart:
		grace := dec.Parameters["grace_period_sec"]
		message = fmt.Sprintf("[SIMULATED] Restarting target %q (grace_period=%ss). Zero host mutations performed.", dec.Target, grace)

	case types.ActionSimulatedIsolate:
		message = fmt.Sprintf("[SIMULATED] Isolating target %q. Zero host mutations performed.", dec.Target)

	case types.ActionSimulatedAlert:
		priority := dec.Parameters["priority"]
		message = fmt.Sprintf("[SIMULATED] Emitting alert for target %q (priority=%s). Zero host mutations performed.", dec.Target, priority)

	default:
		completedAt := time.Now().UTC()
		return &ExecutionResult{
			ExecutionID: fmt.Sprintf("exec-%s", dec.DecisionID[4:]),
			DecisionID:  dec.DecisionID,
			IncidentID:  dec.IncidentID,
			NodeID:      dec.NodeID,
			ActionType:  dec.ActionType,
			Target:      dec.Target,
			Status:      types.MitigationStatusFailed,
			Mode:        dec.ExecutionMode,
			Message:     fmt.Sprintf("unknown action type %q cannot be simulated", dec.ActionType),
			ErrorCode:   string(ValidationCodeUnknownActionType),
			StartedAt:   startedAt,
			CompletedAt: completedAt,
			Duration:    completedAt.Sub(startedAt),
			Simulated:   true,
		}, ErrUnknownActionType
	}

	completedAt := time.Now().UTC()
	result := &ExecutionResult{
		ExecutionID: fmt.Sprintf("exec-%s", dec.DecisionID[4:]),
		DecisionID:  dec.DecisionID,
		IncidentID:  dec.IncidentID,
		NodeID:      dec.NodeID,
		ActionType:  dec.ActionType,
		Target:      dec.Target,
		Status:      types.MitigationStatusExecuted,
		Mode:        dec.ExecutionMode,
		Message:     message,
		ErrorCode:   "",
		StartedAt:   startedAt,
		CompletedAt: completedAt,
		Duration:    completedAt.Sub(startedAt),
		Simulated:   true,
	}

	return result, nil
}

// GetExecution retrieves a previously executed simulation result by its DecisionID.
func (e *SimulatedExecutor) GetExecution(decisionID string) (*ExecutionResult, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	res, ok := e.history[decisionID]
	return res, ok
}

// History returns a snapshot of all cached execution results.
func (e *SimulatedExecutor) History() []*ExecutionResult {
	e.mu.RLock()
	defer e.mu.RUnlock()
	results := make([]*ExecutionResult, 0, len(e.history))
	for _, r := range e.history {
		results = append(results, r)
	}
	return results
}

// ResetHistory clears all cached execution results and resets instrumentation.
func (e *SimulatedExecutor) ResetHistory() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.history = make(map[string]*ExecutionResult)
	e.inFlight = make(map[string]*inFlightExecution)
	atomic.StoreInt64(&e.simulationCount, 0)
}

func storedToExecutionResult(m *storage.StoredMitigation) *ExecutionResult {
	if m == nil {
		return nil
	}
	var completedAt time.Time
	if m.CompletedAt != nil {
		completedAt = *m.CompletedAt
	} else {
		completedAt = m.UpdatedAt
	}
	return &ExecutionResult{
		ExecutionID: fmt.Sprintf("exec-%s", m.DecisionID[4:]),
		DecisionID:  m.DecisionID,
		IncidentID:  m.IncidentID,
		NodeID:      m.NodeID,
		ActionType:  m.ActionType,
		Target:      m.Target,
		Status:      m.Status,
		Mode:        ExecutionMode(m.Mode),
		Message:     m.Message,
		ErrorCode:   m.ErrorCode,
		StartedAt:   m.StartedAt,
		CompletedAt: completedAt,
		Duration:    time.Duration(m.DurationMs) * time.Millisecond,
		Simulated:   m.Simulated,
	}
}

func executionResultToStored(r *ExecutionResult, params map[string]string) *storage.StoredMitigation {
	if r == nil {
		return nil
	}
	var completedAt *time.Time
	if !r.CompletedAt.IsZero() {
		completedAt = &r.CompletedAt
	}
	actionID := r.ExecutionID
	if strings.HasPrefix(r.DecisionID, "dec-") {
		actionID = fmt.Sprintf("act-%s", r.DecisionID[4:])
	}
	return &storage.StoredMitigation{
		ActionID:    actionID,
		DecisionID:  r.DecisionID,
		IncidentID:  r.IncidentID,
		NodeID:      r.NodeID,
		ActionType:  r.ActionType,
		Target:      r.Target,
		Status:      r.Status,
		Mode:        string(r.Mode),
		Message:     r.Message,
		ErrorCode:   r.ErrorCode,
		Parameters:  params,
		StartedAt:   r.StartedAt,
		CompletedAt: completedAt,
		DurationMs:  r.Duration.Milliseconds(),
		Simulated:   r.Simulated,
		CreatedAt:   r.StartedAt,
		UpdatedAt:   r.CompletedAt,
	}
}

// ExecuteWithApproval performs execution-time authorization gate checks against a durable operator approval
// before executing the simulated action.
//
// CRITICAL SAFETY INVARIANTS:
// 1. FORBIDDEN actions can NEVER be executed, even with an approval.
// 2. The approval is loaded fresh from durable storage (never trusted in-memory).
// 3. All 14 decision-binding constraints are evaluated.
// 4. Approval is atomically consumed in durable storage before simulation occurs (prevents double consumption).
// 5. If ANY check fails, the executor fails closed with zero simulated mutation.
func (e *SimulatedExecutor) ExecuteWithApproval(ctx context.Context, dec *ResponseDecision, inc *types.Incident, approvalID string) (*ExecutionResult, error) {
	startedAt := e.now()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if dec == nil {
		return nil, ErrNilDecision
	}
	if inc == nil {
		return nil, ErrNilIncident
	}
	if strings.TrimSpace(approvalID) == "" {
		return nil, errors.New("approval_id cannot be empty")
	}

	// CRITICAL SAFETY INVARIANT 1: FORBIDDEN actions can NEVER be executed, even with operator approval.
	if dec.AuthorizationClass == AuthClassForbidden {
		completedAt := e.now()
		res := &ExecutionResult{
			ExecutionID: fmt.Sprintf("exec-%s", dec.DecisionID[4:]),
			DecisionID:  dec.DecisionID,
			IncidentID:  dec.IncidentID,
			NodeID:      dec.NodeID,
			ActionType:  dec.ActionType,
			Target:      dec.Target,
			Status:      types.MitigationStatusFailed,
			Mode:        dec.ExecutionMode,
			Message:     "action is classified as FORBIDDEN and cannot be authorized by operator approval",
			ErrorCode:   string(ValidationCodeForbiddenAction),
			StartedAt:   startedAt,
			CompletedAt: completedAt,
			Duration:    completedAt.Sub(startedAt),
			Simulated:   true,
		}
		return res, ErrForbiddenNotOverridable
	}

	if e.approvalStore == nil {
		return nil, ErrNilApprovalStore
	}

	// Check in-memory history first (idempotent lookup)
	e.mu.RLock()
	if cached, ok := e.history[dec.DecisionID]; ok {
		e.mu.RUnlock()
		return cached, nil
	}
	e.mu.RUnlock()

	// Check durable mitigation store if configured (restart recovery)
	if e.store != nil {
		if stored, err := e.store.GetMitigationByDecisionID(ctx, dec.DecisionID); err == nil && stored != nil {
			cached := storedToExecutionResult(stored)
			e.mu.Lock()
			e.history[dec.DecisionID] = cached
			e.mu.Unlock()
			return cached, nil
		}
	}

	// In-flight concurrency deduplication
	e.mu.Lock()
	if cached, ok := e.history[dec.DecisionID]; ok {
		e.mu.Unlock()
		return cached, nil
	}
	if flight, inProg := e.inFlight[dec.DecisionID]; inProg {
		e.mu.Unlock()
		select {
		case <-flight.done:
			return flight.res, flight.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	flight := &inFlightExecution{
		done: make(chan struct{}),
	}
	e.inFlight[dec.DecisionID] = flight
	e.mu.Unlock()

	var finalRes *ExecutionResult
	var finalErr error

	defer func() {
		e.mu.Lock()
		flight.res = finalRes
		flight.err = finalErr
		if finalRes != nil && finalRes.Status == types.MitigationStatusExecuted {
			e.history[dec.DecisionID] = finalRes
		}
		delete(e.inFlight, dec.DecisionID)
		close(flight.done)
		e.mu.Unlock()
	}()

	// Load approval record from durable storage
	storedApp, err := e.approvalStore.GetApproval(ctx, approvalID)
	if err != nil {
		finalErr = fmt.Errorf("failed to load approval %s: %w", approvalID, err)
		return nil, finalErr
	}

	app := StoredToApproval(storedApp)
	now := e.now()

	// 14-point execution-time validation gate
	if err := ValidateApprovalForExecution(app, dec, inc, now); err != nil {
		completedAt := e.now()
		finalRes = &ExecutionResult{
			ExecutionID: fmt.Sprintf("exec-%s", dec.DecisionID[4:]),
			DecisionID:  dec.DecisionID,
			IncidentID:  dec.IncidentID,
			NodeID:      dec.NodeID,
			ActionType:  dec.ActionType,
			Target:      dec.Target,
			Status:      types.MitigationStatusFailed,
			Mode:        dec.ExecutionMode,
			Message:     fmt.Sprintf("execution-time approval validation failed: %v", err),
			ErrorCode:   "APPROVAL_VALIDATION_FAILED",
			StartedAt:   startedAt,
			CompletedAt: completedAt,
			Duration:    completedAt.Sub(startedAt),
			Simulated:   true,
		}
		if e.store != nil {
			stored := executionResultToStored(finalRes, dec.Parameters)
			_ = e.store.RecordMitigation(ctx, stored)
		}
		finalErr = err
		return finalRes, finalErr
	}

	// Validate against standard safety validator (allowlist, parameters, targets)
	var safetyRes *SafetyResult
	if sv, ok := e.validator.(*StandardSafetyValidator); ok {
		safetyRes, err = sv.ValidateApproved(ctx, dec, inc)
	} else {
		safetyRes, err = e.validator.Validate(ctx, dec, inc)
	}
	if err != nil {
		finalErr = fmt.Errorf("safety validation error: %w", err)
		return nil, finalErr
	}
	if !safetyRes.Allowed {
		completedAt := e.now()
		finalRes = &ExecutionResult{
			ExecutionID: fmt.Sprintf("exec-%s", dec.DecisionID[4:]),
			DecisionID:  dec.DecisionID,
			IncidentID:  dec.IncidentID,
			NodeID:      dec.NodeID,
			ActionType:  dec.ActionType,
			Target:      dec.Target,
			Status:      types.MitigationStatusFailed,
			Mode:        dec.ExecutionMode,
			Message:     fmt.Sprintf("safety validation rejected approved action: %s", safetyRes.Reason),
			ErrorCode:   string(safetyRes.ValidationCode),
			StartedAt:   startedAt,
			CompletedAt: completedAt,
			Duration:    completedAt.Sub(startedAt),
			Simulated:   true,
		}
		if e.store != nil {
			stored := executionResultToStored(finalRes, dec.Parameters)
			_ = e.store.RecordMitigation(ctx, stored)
		}
		return finalRes, nil
	}

	// Atomic consumption of the approval record (PREVENTS DOUBLE-SPENDING / CONCURRENT RACES)
	if err := e.approvalStore.ConsumeApproval(ctx, app.ApprovalID, now); err != nil {
		completedAt := e.now()
		finalRes = &ExecutionResult{
			ExecutionID: fmt.Sprintf("exec-%s", dec.DecisionID[4:]),
			DecisionID:  dec.DecisionID,
			IncidentID:  dec.IncidentID,
			NodeID:      dec.NodeID,
			ActionType:  dec.ActionType,
			Target:      dec.Target,
			Status:      types.MitigationStatusFailed,
			Mode:        dec.ExecutionMode,
			Message:     fmt.Sprintf("approval consumption failed (already consumed or expired): %v", err),
			ErrorCode:   "APPROVAL_CONSUMPTION_FAILED",
			StartedAt:   startedAt,
			CompletedAt: completedAt,
			Duration:    completedAt.Sub(startedAt),
			Simulated:   true,
		}
		finalErr = err
		return finalRes, finalErr
	}

	// Construct ValidatedDecision wrapper
	vd, err := NewValidatedDecision(dec, safetyRes)
	if err != nil {
		finalErr = fmt.Errorf("failed to create validated decision: %w", err)
		return nil, finalErr
	}

	// Persist in-flight state as EXECUTING in durable store (Phase 6.4)
	actionID := fmt.Sprintf("act-%s", dec.DecisionID[4:])
	if e.store != nil {
		inFlightRec := &storage.StoredMitigation{
			ActionID:   actionID,
			DecisionID: dec.DecisionID,
			IncidentID: dec.IncidentID,
			NodeID:     dec.NodeID,
			ActionType: dec.ActionType,
			Target:     dec.Target,
			Status:     types.MitigationStatusExecuting,
			Mode:       string(dec.ExecutionMode),
			Message:    fmt.Sprintf("approved simulated action execution started (approval: %s, approver: %s)", app.ApprovalID, app.ApprovedBy),
			Parameters: dec.Parameters,
			StartedAt:  startedAt,
			DurationMs: 0,
			Simulated:  true,
			CreatedAt:  startedAt,
			UpdatedAt:  startedAt,
		}
		_ = e.store.RecordMitigation(ctx, inFlightRec)
	}

	// Execute Simulation
	finalRes, finalErr = e.executeSimulation(ctx, vd, startedAt)
	if finalErr != nil {
		if e.store != nil {
			var errCode string
			if finalRes != nil {
				errCode = finalRes.ErrorCode
			}
			nowTime := e.now()
			_ = e.store.MarkMitigationFailed(ctx, actionID, finalErr.Error(), errCode, nowTime, nowTime.Sub(startedAt).Milliseconds())
		}
		return finalRes, finalErr
	}

	// Persist completion in durable store (Phase 6.4)
	if e.store != nil && finalRes != nil {
		if finalRes.Status == types.MitigationStatusExecuted {
			_ = e.store.MarkMitigationExecuted(ctx, actionID, finalRes.Message, finalRes.CompletedAt, finalRes.Duration.Milliseconds())
		} else {
			_ = e.store.MarkMitigationFailed(ctx, actionID, finalRes.Message, finalRes.ErrorCode, finalRes.CompletedAt, finalRes.Duration.Milliseconds())
		}
	}

	if sv, ok := e.validator.(*StandardSafetyValidator); ok {
		sv.RecordExecution(dec.Target, 300*time.Second)
	}

	return finalRes, nil
}

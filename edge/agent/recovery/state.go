package recovery

import (
	"errors"
	"sync"
)

// State defines the bounded recovery state model.
type State string

const (
	StateRecoveryNotRequired State = "RECOVERY_NOT_REQUIRED"
	StateRecoveryLoading     State = "RECOVERY_LOADING"
	StateRecoveryReconciling State = "RECOVERY_RECONCILING"
	StateRecoveryBlocked     State = "RECOVERY_BLOCKED"
	StateRecoveryComplete    State = "RECOVERY_COMPLETE"
	StateRecoveryFailed      State = "RECOVERY_FAILED"
)

// Domain recovery errors.
var (
	ErrInvalidState      = errors.New("invalid recovery state")
	ErrInvalidTransition = errors.New("invalid recovery state transition")
)

// IsValidState returns true if s is one of the recognized bounded recovery states.
func IsValidState(s State) bool {
	switch s {
	case StateRecoveryNotRequired,
		StateRecoveryLoading,
		StateRecoveryReconciling,
		StateRecoveryBlocked,
		StateRecoveryComplete,
		StateRecoveryFailed:
		return true
	default:
		return false
	}
}

// IsValidTransition validates permitted movements between recovery states.
func IsValidTransition(from, to State) bool {
	if !IsValidState(from) || !IsValidState(to) {
		return false
	}
	if from == to {
		return false
	}

	switch from {
	case StateRecoveryLoading:
		return to == StateRecoveryNotRequired ||
			to == StateRecoveryReconciling ||
			to == StateRecoveryBlocked ||
			to == StateRecoveryFailed
	case StateRecoveryReconciling:
		return to == StateRecoveryComplete ||
			to == StateRecoveryBlocked ||
			to == StateRecoveryFailed
	case StateRecoveryBlocked:
		return to == StateRecoveryReconciling ||
			to == StateRecoveryFailed
	case StateRecoveryNotRequired:
		return to == StateRecoveryLoading
	case StateRecoveryComplete:
		return to == StateRecoveryLoading
	case StateRecoveryFailed:
		return to == StateRecoveryLoading
	default:
		return false
	}
}

// Tracker provides thread-safe access to the recovery lifecycle state.
type Tracker interface {
	State() State
	Transition(to State) error
	Reason() string
	SetReason(reason string)
	IsComplete() bool
	IsBlocked() bool
}

type defaultTracker struct {
	mu     sync.RWMutex
	state  State
	reason string
}

// NewTracker constructs a Tracker starting in StateRecoveryLoading.
func NewTracker() Tracker {
	return &defaultTracker{
		state: StateRecoveryLoading,
	}
}

// State returns the current recovery lifecycle state.
func (t *defaultTracker) State() State {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.state
}

// Transition moves the recovery state machine to a new state if valid.
func (t *defaultTracker) Transition(to State) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !IsValidTransition(t.state, to) {
		return ErrInvalidTransition
	}
	t.state = to
	return nil
}

// Reason returns the explainability reason associated with the current recovery state.
func (t *defaultTracker) Reason() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.reason
}

// SetReason updates the explainability reason.
func (t *defaultTracker) SetReason(reason string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.reason = reason
}

// IsComplete returns true if recovery is complete or was not required.
func (t *defaultTracker) IsComplete() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.state == StateRecoveryComplete || t.state == StateRecoveryNotRequired
}

// IsBlocked returns true if recovery is currently blocked.
func (t *defaultTracker) IsBlocked() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.state == StateRecoveryBlocked
}

package health

import (
	"sync"
	"time"
)

// Tracker provides thread-safe lifecycle and health status management for the edge agent.
type Tracker interface {
	State() ServiceState
	Transition(to ServiceState) error
	RegisterCheck(name CheckName, required bool) error
	SetCheckStatus(name CheckName, status CheckStatus, msg string) error
	GetCheck(name CheckName) (CheckInfo, bool)
	Snapshot() Snapshot
	IsLive() bool
	IsReady() bool
	Shutdown()
}

type defaultTracker struct {
	mu     sync.RWMutex
	state  ServiceState
	checks map[CheckName]CheckInfo
}

// NewTracker creates an initialized health Tracker starting in StateInitializing.
func NewTracker() Tracker {
	return &defaultTracker{
		state:  StateInitializing,
		checks: make(map[CheckName]CheckInfo),
	}
}

// State returns the current lifecycle state.
func (t *defaultTracker) State() ServiceState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.state
}

// Transition performs an explicit lifecycle state transition if permitted.
func (t *defaultTracker) Transition(to ServiceState) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !IsValidTransition(t.state, to) {
		return ErrInvalidTransition
	}

	t.state = to
	return nil
}

// RegisterCheck registers a subsystem check with required/optional semantics.
func (t *defaultTracker) RegisterCheck(name CheckName, required bool) error {
	if !IsValidCheckName(name) {
		return ErrUnknownCheckName
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	existing, exists := t.checks[name]
	if exists {
		existing.Required = required
		t.checks[name] = existing
		return nil
	}

	t.checks[name] = CheckInfo{
		Name:       name,
		Status:     StatusUnknown,
		Required:   required,
		Message:    MsgInitialized,
		ObservedAt: time.Now().UTC(),
	}
	return nil
}

// SetCheckStatus updates the status and diagnostic message of a registered check.
func (t *defaultTracker) SetCheckStatus(name CheckName, status CheckStatus, msg string) error {
	if !IsValidCheckName(name) {
		return ErrUnknownCheckName
	}
	if !IsValidCheckStatus(status) {
		return ErrUnknownCheckStatus
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	sanitized := SanitizeMessage(msg)
	info, exists := t.checks[name]
	if !exists {
		info = CheckInfo{
			Name:     name,
			Required: true, // Default to required if not explicitly registered
		}
	}
	info.Status = status
	info.Message = sanitized
	info.ObservedAt = time.Now().UTC()
	t.checks[name] = info

	// Automatic state adjustments when in READY or DEGRADED
	if t.state == StateReady {
		if info.Required && (status == StatusFailed || status == StatusDegraded || status == StatusUnknown) {
			t.state = StateDegraded
		}
	} else if t.state == StateDegraded {
		if t.areAllRequiredOkLocked() {
			t.state = StateReady
		}
	}

	return nil
}

// GetCheck returns a copy of a specific check's status.
func (t *defaultTracker) GetCheck(name CheckName) (CheckInfo, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	info, ok := t.checks[name]
	return info, ok
}

// IsLive returns true if the service is not in SHUTTING_DOWN.
func (t *defaultTracker) IsLive() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.state != StateShuttingDown
}

// IsReady returns true if state is READY and all required checks are OK.
func (t *defaultTracker) IsReady() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.state != StateReady {
		return false
	}
	return t.areAllRequiredOkLocked()
}

// Shutdown transitions the service to SHUTTING_DOWN.
func (t *defaultTracker) Shutdown() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.state = StateShuttingDown
}

// Snapshot returns a thread-safe, immutable point-in-time snapshot of the health state.
func (t *defaultTracker) Snapshot() Snapshot {
	t.mu.RLock()
	defer t.mu.RUnlock()

	checksCopy := make(map[CheckName]CheckInfo, len(t.checks))
	allRequiredOk := true
	allChecksOk := true

	for name, info := range t.checks {
		checksCopy[name] = info
		if info.Required && info.Status != StatusOk {
			allRequiredOk = false
		}
		if info.Status != StatusOk {
			allChecksOk = false
		}
	}

	live := t.state != StateShuttingDown
	ready := (t.state == StateReady) && allRequiredOk
	healthy := (t.state == StateReady) && allChecksOk

	return Snapshot{
		State:     t.state,
		Live:      live,
		Ready:     ready,
		Healthy:   healthy,
		Checks:    checksCopy,
		Timestamp: time.Now().UTC(),
	}
}

func (t *defaultTracker) areAllRequiredOkLocked() bool {
	for _, info := range t.checks {
		if info.Required && info.Status != StatusOk {
			return false
		}
	}
	return true
}

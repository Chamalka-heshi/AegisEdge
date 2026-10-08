package coordination

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/audit"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/health"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/metrics"
)

// State represents the discrete connection lifecycle state between edge and control plane.
type State int

const (
	StateDisconnected State = 0
	StateConnecting   State = 1
	StateConnected    State = 2
	StateDegraded     State = 3
	StateStopping     State = 4
)

func (s State) String() string {
	switch s {
	case StateDisconnected:
		return "DISCONNECTED"
	case StateConnecting:
		return "CONNECTING"
	case StateConnected:
		return "CONNECTED"
	case StateDegraded:
		return "DEGRADED"
	case StateStopping:
		return "STOPPING"
	default:
		return "UNKNOWN"
	}
}

// IsConnected returns true if the edge agent currently has an active connection to control plane.
func (s State) IsConnected() bool {
	return s == StateConnected
}

// IsDegraded returns true if communication with the control plane is currently impaired.
func (s State) IsDegraded() bool {
	return s == StateDegraded
}

// StateTracker defines the concurrency-safe interface for tracking and observing coordination state.
type StateTracker interface {
	State() State
	LastConnected() time.Time
	LastHeartbeat() time.Time
	LastError() string
	Transition(ctx context.Context, newState State, reason string)
	RecordHeartbeat(ctx context.Context, success bool, d time.Duration, errMsg string)
}

// Tracker tracks connection state and synchronizes health, metrics, and audit subsystems.
type Tracker struct {
	mu            sync.RWMutex
	state         State
	lastConnected time.Time
	lastHeartbeat time.Time
	lastError     string
	nodeID        string
	metrics       metrics.Recorder
	healthTracker health.Tracker
	auditRec      audit.Recorder
}

// NewTracker constructs a Tracker initialized in StateDisconnected.
func NewTracker(nodeID string, metricsRec metrics.Recorder, healthTracker health.Tracker, auditRec audit.Recorder) *Tracker {
	if metricsRec == nil {
		metricsRec = metrics.NoopRecorder{}
	}
	t := &Tracker{
		state:         StateDisconnected,
		nodeID:        nodeID,
		metrics:       metricsRec,
		healthTracker: healthTracker,
		auditRec:      auditRec,
	}
	t.metrics.SetControlPlaneConnectionState(int(StateDisconnected))
	if t.healthTracker != nil {
		_ = t.healthTracker.SetCheckStatus(health.CheckControlPlane, health.StatusDegraded, "disconnected from control plane")
	}
	return t
}

// State returns current connection state thread-safely.
func (t *Tracker) State() State {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.state
}

// LastConnected returns the timestamp of the most recent successful connection.
func (t *Tracker) LastConnected() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.lastConnected
}

// LastHeartbeat returns the timestamp of the most recent successful heartbeat.
func (t *Tracker) LastHeartbeat() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.lastHeartbeat
}

// LastError returns the most recent error message, if any.
func (t *Tracker) LastError() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.lastError
}

// Transition changes connection state, updating metrics, health check, and audit trail.
func (t *Tracker) Transition(ctx context.Context, newState State, reason string) {
	t.mu.Lock()
	oldState := t.state
	if oldState == newState && t.lastError == reason {
		t.mu.Unlock()
		return
	}
	t.state = newState
	if newState == StateConnected {
		t.lastConnected = time.Now().UTC()
		t.lastError = ""
	} else if reason != "" {
		t.lastError = reason
	}
	t.mu.Unlock()

	// Update Prometheus metrics
	t.metrics.SetControlPlaneConnectionState(int(newState))

	// Update Health Check (CheckControlPlane is non-critical/optional: does not degrade readiness)
	if t.healthTracker != nil {
		switch newState {
		case StateConnected:
			_ = t.healthTracker.SetCheckStatus(health.CheckControlPlane, health.StatusOk, "connected to control plane")
		case StateConnecting:
			_ = t.healthTracker.SetCheckStatus(health.CheckControlPlane, health.StatusDegraded, "connecting to control plane")
		case StateDegraded:
			msg := "control plane connection degraded"
			if reason != "" {
				msg = fmt.Sprintf("control plane connection degraded: %s", reason)
			}
			_ = t.healthTracker.SetCheckStatus(health.CheckControlPlane, health.StatusDegraded, msg)
		case StateDisconnected:
			msg := "disconnected from control plane"
			if reason != "" {
				msg = fmt.Sprintf("disconnected from control plane: %s", reason)
			}
			_ = t.healthTracker.SetCheckStatus(health.CheckControlPlane, health.StatusDegraded, msg)
		case StateStopping:
			_ = t.healthTracker.SetCheckStatus(health.CheckControlPlane, health.StatusFailed, "stopping coordination")
		}
	}

	// Emit audit event on significant transitions
	if t.auditRec != nil {
		now := time.Now().UTC()
		if newState == StateConnected && oldState != StateConnected {
			t.metrics.RecordControlPlaneConnection("connected")
			_ = t.auditRec.Record(ctx, audit.AuditEvent{
				EventType:  audit.EventTypeControlPlaneConnected,
				NodeID:     t.nodeID,
				IncidentID: "system-coordination",
				Result:     audit.ResultSuccess,
				Reason:     reason,
				Timestamp:  now,
			})
		} else if oldState == StateConnected && newState != StateConnected {
			t.metrics.RecordControlPlaneConnection("disconnected")
			_ = t.auditRec.Record(ctx, audit.AuditEvent{
				EventType:  audit.EventTypeControlPlaneDisconnected,
				NodeID:     t.nodeID,
				IncidentID: "system-coordination",
				Result:     audit.ResultFailed,
				Reason:     reason,
				Timestamp:  now,
			})
		}
	}
}

// RecordHeartbeat logs a heartbeat attempt outcome and updates corresponding telemetry.
func (t *Tracker) RecordHeartbeat(ctx context.Context, success bool, d time.Duration, errMsg string) {
	now := time.Now().UTC()
	t.metrics.ObserveControlPlaneHeartbeatDuration(d)

	if success {
		t.mu.Lock()
		t.lastHeartbeat = now
		t.mu.Unlock()

		t.metrics.RecordControlPlaneHeartbeat("success")
		if t.auditRec != nil {
			_ = t.auditRec.Record(ctx, audit.AuditEvent{
				EventType:  audit.EventTypeControlPlaneHeartbeatSuccess,
				NodeID:     t.nodeID,
				IncidentID: "system-coordination",
				Result:     audit.ResultSuccess,
				Timestamp:  now,
			})
		}
	} else {
		t.mu.Lock()
		t.lastError = errMsg
		t.mu.Unlock()

		t.metrics.RecordControlPlaneHeartbeat("failed")
		if t.auditRec != nil {
			_ = t.auditRec.Record(ctx, audit.AuditEvent{
				EventType:  audit.EventTypeControlPlaneHeartbeatFailed,
				NodeID:     t.nodeID,
				IncidentID: "system-coordination",
				Result:     audit.ResultFailed,
				Reason:     errMsg,
				Timestamp:  now,
			})
		}
	}
}

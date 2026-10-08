package health

import (
	"errors"
	"strings"
	"time"
)

// ServiceState defines the bounded service lifecycle state model.
type ServiceState string

const (
	StateInitializing ServiceState = "INITIALIZING"
	StateReady        ServiceState = "READY"
	StateDegraded     ServiceState = "DEGRADED"
	StateShuttingDown ServiceState = "SHUTTING_DOWN"
)

// CheckStatus defines the bounded health state for a single dependency check.
type CheckStatus string

const (
	StatusOk       CheckStatus = "OK"
	StatusDegraded CheckStatus = "DEGRADED"
	StatusFailed   CheckStatus = "FAILED"
	StatusUnknown  CheckStatus = "UNKNOWN"
)

// CheckName defines the fixed allowlist of check identifiers.
type CheckName string

const (
	CheckConfig         CheckName = "config"
	CheckStorage        CheckName = "storage"
	CheckTelemetry      CheckName = "telemetry"
	CheckIncidentEngine CheckName = "incident_engine"
	CheckResponseEngine CheckName = "response_engine"
	CheckMetrics        CheckName = "metrics"
	CheckRecovery       CheckName = "recovery"
	CheckControlPlane   CheckName = "control_plane"
)

// AllowedCheckNames contains all valid check names permitted in the health model.
var AllowedCheckNames = map[CheckName]struct{}{
	CheckConfig:         {},
	CheckStorage:        {},
	CheckTelemetry:      {},
	CheckIncidentEngine: {},
	CheckResponseEngine: {},
	CheckMetrics:        {},
	CheckRecovery:       {},
	CheckControlPlane:   {},
}

// Bounded diagnostic messages.
const (
	MsgInitialized = "initialized"
	MsgAvailable   = "available"
	MsgUnavailable = "unavailable"
	MsgDisabled    = "disabled"
	MsgDegraded    = "degraded"
	MsgFailed      = "failed"
	MsgListening   = "listening"
	MsgRunning     = "running"
)

// Health domain errors.
var (
	ErrInvalidState       = errors.New("invalid service state")
	ErrInvalidTransition  = errors.New("invalid state transition")
	ErrUnknownCheckName   = errors.New("unknown or disallowed health check name")
	ErrUnknownCheckStatus = errors.New("unknown health check status")
)

// CheckInfo represents the bounded state of a specific registered check.
type CheckInfo struct {
	Name       CheckName   `json:"name"`
	Status     CheckStatus `json:"status"`
	Required   bool        `json:"required"`
	Message    string      `json:"message"`
	ObservedAt time.Time   `json:"observed_at"`
}

// Snapshot provides a point-in-time, read-only representation of edge agent health.
type Snapshot struct {
	State     ServiceState            `json:"state"`
	Live      bool                    `json:"live"`
	Ready     bool                    `json:"ready"`
	Healthy   bool                    `json:"healthy"`
	Checks    map[CheckName]CheckInfo `json:"checks"`
	Timestamp time.Time               `json:"timestamp"`
}

// IsValidState returns true if state is one of the four bounded lifecycle states.
func IsValidState(s ServiceState) bool {
	switch s {
	case StateInitializing, StateReady, StateDegraded, StateShuttingDown:
		return true
	default:
		return false
	}
}

// IsValidCheckStatus returns true if status is one of the four bounded check statuses.
func IsValidCheckStatus(s CheckStatus) bool {
	switch s {
	case StatusOk, StatusDegraded, StatusFailed, StatusUnknown:
		return true
	default:
		return false
	}
}

// IsValidCheckName returns true if name is within the fixed allowlist.
func IsValidCheckName(name CheckName) bool {
	_, ok := AllowedCheckNames[name]
	return ok
}

// IsValidTransition returns true if moving from `from` to `to` is an authorized lifecycle transition.
func IsValidTransition(from, to ServiceState) bool {
	if !IsValidState(from) || !IsValidState(to) {
		return false
	}
	if from == to {
		return true
	}
	switch from {
	case StateInitializing:
		// Initializing can become Ready, Degraded, or ShuttingDown
		return to == StateReady || to == StateDegraded || to == StateShuttingDown
	case StateReady:
		// Ready can become Degraded or ShuttingDown
		return to == StateDegraded || to == StateShuttingDown
	case StateDegraded:
		// Degraded can recover to Ready, or proceed to ShuttingDown
		return to == StateReady || to == StateShuttingDown
	case StateShuttingDown:
		// ShuttingDown is terminal; no transitions out of ShuttingDown
		return false
	default:
		return false
	}
}

// SanitizeMessage ensures that diagnostics do not expose paths, tokens, SQL, IDs, or arbitrary error strings.
func SanitizeMessage(msg string) string {
	m := strings.TrimSpace(msg)
	if m == "" {
		return MsgAvailable
	}

	lower := strings.ToLower(m)
	// Reject paths, URLs, secrets, SQL, environment variables, IDs, or internal tokens fail-closed
	if strings.Contains(m, "/") || strings.Contains(m, "\\") ||
		strings.Contains(m, ":") || strings.Contains(m, "=") ||
		strings.Contains(m, "$") || strings.Contains(m, "%") ||
		strings.Contains(m, "@") || strings.Contains(m, ";") ||
		strings.Contains(lower, "select") || strings.Contains(lower, "insert") ||
		strings.Contains(lower, "update") || strings.Contains(lower, "delete") ||
		strings.Contains(lower, "from") || strings.Contains(lower, "where") ||
		strings.Contains(lower, "token") || strings.Contains(lower, "key") ||
		strings.Contains(lower, "secret") || strings.Contains(lower, "pass") ||
		strings.Contains(lower, "error") || strings.Contains(lower, "panic") ||
		strings.Contains(lower, "exception") || strings.Contains(lower, "inc-") ||
		strings.Contains(lower, "dec-") || strings.Contains(lower, "act-") ||
		strings.Contains(lower, "corr-") || strings.Contains(lower, "appr-") ||
		strings.Contains(lower, "escl-") || strings.Contains(lower, "evt-") {
		return MsgDegraded
	}

	// Limit length to 32 characters
	if len(m) > 32 {
		m = m[:32]
	}
	return m
}

package types

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// Common validation errors for domain contracts.
var (
	ErrEmptyNodeID              = errors.New("node_id cannot be empty")
	ErrEmptyBatchID             = errors.New("batch_id cannot be empty")
	ErrEmptyIncidentID          = errors.New("incident_id cannot be empty")
	ErrEmptyHostname            = errors.New("hostname cannot be empty")
	ErrInvalidSequence          = errors.New("sequence_number must be greater than or equal to zero")
	ErrEmptyMetrics             = errors.New("telemetry batch must contain at least one metric sample")
	ErrInvalidSeverity          = errors.New("invalid incident severity")
	ErrInvalidStatus            = errors.New("invalid incident status")
	ErrInvalidNodeStatus        = errors.New("invalid node status")
	ErrInvalidTimestamp         = errors.New("timestamp must be positive and non-zero")
	ErrEmptyMetricName          = errors.New("metric name cannot be empty")
	ErrInvalidMetricValue       = errors.New("metric value must be a valid finite number (not NaN or Inf)")
	ErrEmptyActionID            = errors.New("action_id cannot be empty")
	ErrInvalidActionType        = errors.New("invalid or unauthorized mitigation action type")
	ErrInvalidActionStatus      = errors.New("invalid mitigation action status")
	ErrEmptyRuleName            = errors.New("rule_name cannot be empty")
	ErrInvalidStateTransition   = errors.New("illegal incident state transition")
	ErrMismatchedIncidentID     = errors.New("mitigation incident_id does not match parent incident_id")
	ErrMismatchedNodeID         = errors.New("metric sample node_id does not match batch node_id")
	ErrInvalidResolvedTimestamp = errors.New("resolved_at cannot be prior to triggered_at")
	ErrInvalidCompletedTime     = errors.New("completed_at cannot be prior to triggered_at")
)

// ============================================================================
// Node Registration
// ============================================================================

// NodeRegistration contains device identity and capability metadata sent during enrollment.
type NodeRegistration struct {
	NodeID       string            `json:"node_id"`
	Hostname     string            `json:"hostname"`
	OS           string            `json:"os"`
	Architecture string            `json:"architecture"`
	IPAddress    string            `json:"ip_address,omitempty"`
	AgentVersion string            `json:"agent_version,omitempty"`
	Capabilities []string          `json:"capabilities,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`
	RegisteredAt time.Time         `json:"registered_at"`
}

// Validate verifies the NodeRegistration payload.
func (n *NodeRegistration) Validate() error {
	if strings.TrimSpace(n.NodeID) == "" {
		return ErrEmptyNodeID
	}
	if strings.TrimSpace(n.Hostname) == "" {
		return ErrEmptyHostname
	}
	if n.RegisteredAt.IsZero() {
		return ErrInvalidTimestamp
	}
	return nil
}

// ============================================================================
// Heartbeat & Node Status
// ============================================================================

// NodeStatus represents the operational state of an edge node.
type NodeStatus string

const (
	NodeStatusHealthy  NodeStatus = "HEALTHY"
	NodeStatusDegraded NodeStatus = "DEGRADED"
	NodeStatusOffline  NodeStatus = "OFFLINE"
)

// IsValid checks whether the node status is recognized.
func (s NodeStatus) IsValid() bool {
	switch s {
	case NodeStatusHealthy, NodeStatusDegraded, NodeStatusOffline:
		return true
	default:
		return false
	}
}

// Heartbeat represents a periodic liveness and sequencing ping from an edge node.
type Heartbeat struct {
	NodeID         string             `json:"node_id"`
	SequenceNumber int64              `json:"sequence_number"`
	Timestamp      time.Time          `json:"timestamp"`
	Status         NodeStatus         `json:"status"`
	MetricsSummary map[string]float64 `json:"metrics_summary,omitempty"`
}

// Validate verifies the Heartbeat payload.
func (h *Heartbeat) Validate() error {
	if strings.TrimSpace(h.NodeID) == "" {
		return ErrEmptyNodeID
	}
	if h.SequenceNumber < 0 {
		return ErrInvalidSequence
	}
	if h.Timestamp.IsZero() {
		return ErrInvalidTimestamp
	}
	if !h.Status.IsValid() {
		return ErrInvalidNodeStatus
	}
	return nil
}

// ============================================================================
// Metric Samples & Telemetry Batches
// ============================================================================

// MetricSample represents a single point-in-time sensor or telemetry measurement.
type MetricSample struct {
	SampleID  string            `json:"sample_id,omitempty"`
	NodeID    string            `json:"node_id,omitempty"`
	Name      string            `json:"name"`
	Value     float64           `json:"value"`
	Unit      string            `json:"unit,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
}

// Validate verifies the MetricSample payload.
func (m *MetricSample) Validate() error {
	if strings.TrimSpace(m.Name) == "" {
		return ErrEmptyMetricName
	}
	if math.IsNaN(m.Value) || math.IsInf(m.Value, 0) {
		return ErrInvalidMetricValue
	}
	if m.Timestamp.IsZero() {
		return ErrInvalidTimestamp
	}
	return nil
}

// TelemetryBatch represents a sequence-numbered batch of metric samples buffered at the edge.
// The BatchID serves as a stable idempotency key during upstream network retries.
type TelemetryBatch struct {
	BatchID        string         `json:"batch_id"`
	NodeID         string         `json:"node_id"`
	SequenceNumber int64          `json:"sequence_number"`
	CollectedAt    time.Time      `json:"collected_at"`
	SentAt         *time.Time     `json:"sent_at,omitempty"`
	Attempt        int            `json:"attempt,omitempty"`
	Metrics        []MetricSample `json:"metrics"`
}

// Validate verifies the TelemetryBatch payload.
func (b *TelemetryBatch) Validate() error {
	if strings.TrimSpace(b.BatchID) == "" {
		return ErrEmptyBatchID
	}
	if strings.TrimSpace(b.NodeID) == "" {
		return ErrEmptyNodeID
	}
	if b.SequenceNumber < 0 {
		return ErrInvalidSequence
	}
	if b.CollectedAt.IsZero() {
		return ErrInvalidTimestamp
	}
	if len(b.Metrics) == 0 {
		return ErrEmptyMetrics
	}
	for i := range b.Metrics {
		if err := b.Metrics[i].Validate(); err != nil {
			return err
		}
		// If sample specifies a NodeID, it must match the batch NodeID
		if b.Metrics[i].NodeID != "" && b.Metrics[i].NodeID != b.NodeID {
			return ErrMismatchedNodeID
		}
	}
	return nil
}

// ============================================================================
// Mitigation Actions (Safe Allowlisted Operations)
// ============================================================================

// MitigationActionType represents an explicit, allowlisted remediation operation.
// Arbitrary shell commands or arbitrary process manipulation are strictly prohibited.
type MitigationActionType string

const (
	ActionSimulatedThrottle MitigationActionType = "SIMULATED_THROTTLE"
	ActionSimulatedRestart  MitigationActionType = "SIMULATED_RESTART"
	ActionSimulatedIsolate  MitigationActionType = "SIMULATED_ISOLATE"
	ActionSimulatedAlert    MitigationActionType = "SIMULATED_ALERT"
)

// IsValid checks whether the action type is part of the allowlist.
func (t MitigationActionType) IsValid() bool {
	switch t {
	case ActionSimulatedThrottle, ActionSimulatedRestart, ActionSimulatedIsolate, ActionSimulatedAlert:
		return true
	default:
		return false
	}
}

// MitigationStatus represents the execution state of an automated mitigation.
type MitigationStatus string

const (
	MitigationStatusPending   MitigationStatus = "PENDING"
	MitigationStatusExecuting MitigationStatus = "EXECUTING"
	MitigationStatusExecuted  MitigationStatus = "EXECUTED"
	MitigationStatusFailed    MitigationStatus = "FAILED"
	MitigationStatusSkipped   MitigationStatus = "SKIPPED"
)

// IsValid checks whether the mitigation status is recognized.
func (s MitigationStatus) IsValid() bool {
	switch s {
	case MitigationStatusPending, MitigationStatusExecuting, MitigationStatusExecuted, MitigationStatusFailed, MitigationStatusSkipped:
		return true
	default:
		return false
	}
}

// MitigationAction records an automated, allowlisted remediation action taken at the edge.
type MitigationAction struct {
	ActionID    string               `json:"action_id"`
	IncidentID  string               `json:"incident_id"`
	ActionType  MitigationActionType `json:"action_type"`
	Target      string               `json:"target"`
	Status      MitigationStatus     `json:"status"`
	Message     string               `json:"message,omitempty"`
	Error       string               `json:"error,omitempty"`
	TriggeredAt time.Time            `json:"triggered_at"`
	CompletedAt *time.Time           `json:"completed_at,omitempty"`
}

// Validate verifies the MitigationAction.
func (a *MitigationAction) Validate() error {
	if strings.TrimSpace(a.ActionID) == "" {
		return ErrEmptyActionID
	}
	if strings.TrimSpace(a.IncidentID) == "" {
		return ErrEmptyIncidentID
	}
	if !a.ActionType.IsValid() {
		return ErrInvalidActionType
	}
	if !a.Status.IsValid() {
		return ErrInvalidActionStatus
	}
	if a.TriggeredAt.IsZero() {
		return ErrInvalidTimestamp
	}
	if a.CompletedAt != nil && a.CompletedAt.Before(a.TriggeredAt) {
		return ErrInvalidCompletedTime
	}
	return nil
}

// ============================================================================
// Incident & Lifecycle State Machine
// ============================================================================

// IncidentSeverity denotes the urgency level of an incident.
type IncidentSeverity string

const (
	SeverityLow      IncidentSeverity = "LOW"
	SeverityMedium   IncidentSeverity = "MEDIUM"
	SeverityHigh     IncidentSeverity = "HIGH"
	SeverityCritical IncidentSeverity = "CRITICAL"
)

// IsValid checks whether the severity is recognized.
func (s IncidentSeverity) IsValid() bool {
	switch s {
	case SeverityLow, SeverityMedium, SeverityHigh, SeverityCritical:
		return true
	default:
		return false
	}
}

// IncidentStatus denotes the deterministic lifecycle state of an incident.
type IncidentStatus string

const (
	StatusNormal          IncidentStatus = "NORMAL"
	StatusAnomalyDetected IncidentStatus = "ANOMALY_DETECTED"
	StatusMitigating      IncidentStatus = "MITIGATING"
	StatusRecovered       IncidentStatus = "RECOVERED"
	StatusEscalated       IncidentStatus = "ESCALATED"
)

// IsValid checks whether the status is recognized.
func (s IncidentStatus) IsValid() bool {
	switch s {
	case StatusNormal, StatusAnomalyDetected, StatusMitigating, StatusRecovered, StatusEscalated:
		return true
	default:
		return false
	}
}

// CanTransitionTo enforces the deterministic state machine rules:
//
//	NORMAL -> ANOMALY_DETECTED
//	ANOMALY_DETECTED -> MITIGATING | ESCALATED
//	MITIGATING -> RECOVERED | ESCALATED
//	RECOVERED -> NORMAL
//	ESCALATED -> RECOVERED | NORMAL (operator resolution)
func (s IncidentStatus) CanTransitionTo(next IncidentStatus) bool {
	switch s {
	case StatusNormal:
		return next == StatusAnomalyDetected
	case StatusAnomalyDetected:
		return next == StatusMitigating || next == StatusEscalated
	case StatusMitigating:
		return next == StatusRecovered || next == StatusEscalated
	case StatusRecovered:
		return next == StatusNormal
	case StatusEscalated:
		return next == StatusRecovered || next == StatusNormal
	default:
		return false
	}
}

// Incident represents an autonomous incident detected, tracked, and mitigated at the edge.
// Its IncidentID is generated at the edge and serves as the idempotency key for central ingestion.
type Incident struct {
	IncidentID    string             `json:"incident_id"`
	NodeID        string             `json:"node_id"`
	RuleName      string             `json:"rule_name"`
	Severity      IncidentSeverity   `json:"severity"`
	Status        IncidentStatus     `json:"status"`
	Description   string             `json:"description"`
	TriggerMetric string             `json:"trigger_metric,omitempty"`
	TriggerValue  float64            `json:"trigger_value,omitempty"`
	Threshold     float64            `json:"threshold,omitempty"`
	Evidence      map[string]string  `json:"evidence,omitempty"`
	TriggeredAt   time.Time          `json:"triggered_at"`
	UpdatedAt     time.Time          `json:"updated_at"`
	ResolvedAt    *time.Time         `json:"resolved_at,omitempty"`
	Mitigations   []MitigationAction `json:"mitigations,omitempty"`
}

// Validate verifies the Incident payload.
func (inc *Incident) Validate() error {
	if strings.TrimSpace(inc.IncidentID) == "" {
		return ErrEmptyIncidentID
	}
	if strings.TrimSpace(inc.NodeID) == "" {
		return ErrEmptyNodeID
	}
	if strings.TrimSpace(inc.RuleName) == "" {
		return ErrEmptyRuleName
	}
	if !inc.Severity.IsValid() {
		return ErrInvalidSeverity
	}
	if !inc.Status.IsValid() {
		return ErrInvalidStatus
	}
	if inc.TriggeredAt.IsZero() {
		return ErrInvalidTimestamp
	}
	if inc.ResolvedAt != nil && inc.ResolvedAt.Before(inc.TriggeredAt) {
		return ErrInvalidResolvedTimestamp
	}
	for i := range inc.Mitigations {
		if err := inc.Mitigations[i].Validate(); err != nil {
			return err
		}
		if inc.Mitigations[i].IncidentID != inc.IncidentID {
			return ErrMismatchedIncidentID
		}
	}
	return nil
}

// TransitionTo applies a validated state transition, updating status and timestamps.
func (inc *Incident) TransitionTo(next IncidentStatus, now time.Time) error {
	if !inc.Status.CanTransitionTo(next) {
		return fmt.Errorf("%w: cannot transition from %s to %s", ErrInvalidStateTransition, inc.Status, next)
	}
	inc.Status = next
	inc.UpdatedAt = now
	if next == StatusRecovered || next == StatusNormal {
		inc.ResolvedAt = &now
	}
	return nil
}

package audit

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
)

// Domain errors for audit subsystem.
var (
	ErrInvalidEventType  = errors.New("unsupported or invalid audit event type")
	ErrMissingIdentifier = errors.New("required identifier is missing in audit event")
	ErrOversizedPayload  = errors.New("audit event payload exceeds configured size limits")
	ErrProhibitedContent = errors.New("audit event contains prohibited credential or executable pattern")
	ErrDuplicateEvent    = errors.New("audit event already exists")
	ErrEventNotFound     = errors.New("audit event not found")
)

// EventType defines the controlled allowlist of discrete incident response lifecycle transitions.
type EventType string

const (
	// Detection & Incident FSM
	EventTypeAnomalyDetected      EventType = "ANOMALY_DETECTED"
	EventTypeIncidentCreated      EventType = "INCIDENT_CREATED"
	EventTypeIncidentStateChanged EventType = "INCIDENT_STATE_CHANGED"

	// Policy & Safety Evaluation
	EventTypeResponseDecisionCreated EventType = "RESPONSE_DECISION_CREATED"
	EventTypeSafetyValidated         EventType = "SAFETY_VALIDATED"
	EventTypeSafetyRejected          EventType = "SAFETY_REJECTED"

	// Human-in-the-Loop Operator Authorization
	EventTypeApprovalRequested EventType = "APPROVAL_REQUESTED"
	EventTypeApprovalGranted   EventType = "APPROVAL_GRANTED"
	EventTypeApprovalRejected  EventType = "APPROVAL_REJECTED"
	EventTypeApprovalExpired   EventType = "APPROVAL_EXPIRED"
	EventTypeApprovalCancelled EventType = "APPROVAL_CANCELLED"

	// Mitigation Persistence & Simulated Execution
	EventTypeMitigationRecorded EventType = "MITIGATION_RECORDED"
	EventTypeMitigationStarted  EventType = "MITIGATION_STARTED"
	EventTypeMitigationExecuted EventType = "MITIGATION_EXECUTED"
	EventTypeMitigationFailed   EventType = "MITIGATION_FAILED"

	// Recovery Verification
	EventTypeVerificationStarted   EventType = "VERIFICATION_STARTED"
	EventTypeVerificationProgress  EventType = "VERIFICATION_PROGRESS"
	EventTypeVerificationRecovered EventType = "VERIFICATION_RECOVERED"
	EventTypeVerificationTimedOut  EventType = "VERIFICATION_TIMED_OUT"
	EventTypeVerificationRejected  EventType = "VERIFICATION_REJECTED"

	// Escalation, Retry & Circuit Breaker
	EventTypeEscalationEvaluated EventType = "ESCALATION_EVALUATED"
	EventTypeRetryAuthorized     EventType = "RETRY_AUTHORIZED"
	EventTypeCircuitOpened       EventType = "CIRCUIT_OPENED"

	// Orchestration Lifecycle
	EventTypeOrchestrationCompleted EventType = "ORCHESTRATION_COMPLETED"
	EventTypeOrchestrationStopped   EventType = "ORCHESTRATION_STOPPED"

	// Persistent Runtime Recovery Lifecycle (Phase 6.13)
	EventTypeRecoveryStarted                 EventType = "RECOVERY_STARTED"
	EventTypeRecoveryStateLoaded             EventType = "RECOVERY_STATE_LOADED"
	EventTypeRecoveryReconciliationRequired  EventType = "RECOVERY_RECONCILIATION_REQUIRED"
	EventTypeRecoveryReconciliationCompleted EventType = "RECOVERY_RECONCILIATION_COMPLETED"
	EventTypeRecoveryBlocked                 EventType = "RECOVERY_BLOCKED"
	EventTypeRecoveryFailed                  EventType = "RECOVERY_FAILED"
	EventTypeRecoveryCompleted               EventType = "RECOVERY_COMPLETED"

	// Edge ↔ Control-Plane Coordination (Phase 6.14)
	EventTypeControlPlaneRegStarted       EventType = "CONTROL_PLANE_REGISTRATION_STARTED"
	EventTypeControlPlaneRegSuccess       EventType = "CONTROL_PLANE_REGISTRATION_SUCCEEDED"
	EventTypeControlPlaneRegFailed        EventType = "CONTROL_PLANE_REGISTRATION_FAILED"
	EventTypeControlPlaneConnected        EventType = "CONTROL_PLANE_CONNECTED"
	EventTypeControlPlaneDisconnected     EventType = "CONTROL_PLANE_DISCONNECTED"
	EventTypeControlPlaneHeartbeatSuccess EventType = "CONTROL_PLANE_HEARTBEAT_SUCCEEDED"
	EventTypeControlPlaneHeartbeatFailed  EventType = "CONTROL_PLANE_HEARTBEAT_FAILED"
	EventTypeControlPlaneReconnectSched   EventType = "CONTROL_PLANE_RECONNECT_SCHEDULED"
)

// IsValid checks whether an EventType is a member of the controlled allowlist.
func (t EventType) IsValid() bool {
	switch t {
	case EventTypeAnomalyDetected,
		EventTypeIncidentCreated,
		EventTypeIncidentStateChanged,
		EventTypeResponseDecisionCreated,
		EventTypeSafetyValidated,
		EventTypeSafetyRejected,
		EventTypeApprovalRequested,
		EventTypeApprovalGranted,
		EventTypeApprovalRejected,
		EventTypeApprovalExpired,
		EventTypeApprovalCancelled,
		EventTypeMitigationRecorded,
		EventTypeMitigationStarted,
		EventTypeMitigationExecuted,
		EventTypeMitigationFailed,
		EventTypeVerificationStarted,
		EventTypeVerificationProgress,
		EventTypeVerificationRecovered,
		EventTypeVerificationTimedOut,
		EventTypeVerificationRejected,
		EventTypeEscalationEvaluated,
		EventTypeRetryAuthorized,
		EventTypeCircuitOpened,
		EventTypeOrchestrationCompleted,
		EventTypeOrchestrationStopped,
		EventTypeRecoveryStarted,
		EventTypeRecoveryStateLoaded,
		EventTypeRecoveryReconciliationRequired,
		EventTypeRecoveryReconciliationCompleted,
		EventTypeRecoveryBlocked,
		EventTypeRecoveryFailed,
		EventTypeRecoveryCompleted,
		EventTypeControlPlaneRegStarted,
		EventTypeControlPlaneRegSuccess,
		EventTypeControlPlaneRegFailed,
		EventTypeControlPlaneConnected,
		EventTypeControlPlaneDisconnected,
		EventTypeControlPlaneHeartbeatSuccess,
		EventTypeControlPlaneHeartbeatFailed,
		EventTypeControlPlaneReconnectSched:
		return true
	default:
		return false
	}
}

// Result status strings for explainability metadata.
const (
	ResultSuccess     = "success"
	ResultRejected    = "rejected"
	ResultFailed      = "failed"
	ResultTimedOut    = "timed_out"
	ResultSkipped     = "skipped"
	ResultRetry       = "retry"
	ResultEscalated   = "escalated"
	ResultCircuitOpen = "circuit_open"
	ResultBlocked     = "blocked"
)

// AuditEvent represents a structured, durable domain audit record for incident response explainability.
// SECURITY BOUNDARY: Never contains arbitrary executable shell commands, credentials, private keys, or secrets.
type AuditEvent struct {
	EventID            string            `json:"event_id"`
	EventType          EventType         `json:"event_type"`
	Timestamp          time.Time         `json:"timestamp"`
	NodeID             string            `json:"node_id"`
	IncidentID         string            `json:"incident_id"`
	DecisionID         string            `json:"decision_id,omitempty"`
	ActionID           string            `json:"action_id,omitempty"`
	ApprovalID         string            `json:"approval_id,omitempty"`
	MitigationRecordID string            `json:"mitigation_record_id,omitempty"`
	VerificationID     string            `json:"verification_id,omitempty"`
	EscalationID       string            `json:"escalation_id,omitempty"`
	CorrelationID      string            `json:"correlation_id,omitempty"`
	PolicyVersion      string            `json:"policy_version,omitempty"`
	Actor              string            `json:"actor,omitempty"`
	Result             string            `json:"result,omitempty"`
	Reason             string            `json:"reason,omitempty"`
	Metadata           map[string]string `json:"metadata,omitempty"`
}

// Validate verifies required identifiers, controlled event types, data bounds, and security invariants.
func (e *AuditEvent) Validate() error {
	if strings.TrimSpace(e.EventID) == "" {
		return fmt.Errorf("%w: event_id is required", ErrMissingIdentifier)
	}
	if len(e.EventID) > storage.MaxAuditStringLength {
		return fmt.Errorf("%w: event_id exceeds maximum length of %d", ErrOversizedPayload, storage.MaxAuditStringLength)
	}

	if !e.EventType.IsValid() {
		return fmt.Errorf("%w: %q", ErrInvalidEventType, e.EventType)
	}

	if e.Timestamp.IsZero() {
		return fmt.Errorf("%w: timestamp cannot be zero", ErrMissingIdentifier)
	}

	if strings.TrimSpace(e.NodeID) == "" {
		return fmt.Errorf("%w: node_id is required", ErrMissingIdentifier)
	}
	if len(e.NodeID) > storage.MaxAuditStringLength {
		return fmt.Errorf("%w: node_id exceeds maximum length of %d", ErrOversizedPayload, storage.MaxAuditStringLength)
	}

	if strings.TrimSpace(e.IncidentID) == "" {
		return fmt.Errorf("%w: incident_id is required", ErrMissingIdentifier)
	}
	if len(e.IncidentID) > storage.MaxAuditStringLength {
		return fmt.Errorf("%w: incident_id exceeds maximum length of %d", ErrOversizedPayload, storage.MaxAuditStringLength)
	}

	if len(e.DecisionID) > storage.MaxAuditStringLength ||
		len(e.ActionID) > storage.MaxAuditStringLength ||
		len(e.ApprovalID) > storage.MaxAuditStringLength ||
		len(e.MitigationRecordID) > storage.MaxAuditStringLength ||
		len(e.VerificationID) > storage.MaxAuditStringLength ||
		len(e.EscalationID) > storage.MaxAuditStringLength ||
		len(e.CorrelationID) > storage.MaxAuditStringLength ||
		len(e.PolicyVersion) > storage.MaxAuditStringLength ||
		len(e.Actor) > storage.MaxAuditStringLength ||
		len(e.Result) > storage.MaxAuditStringLength {
		return fmt.Errorf("%w: sub-identifier or attribute string exceeds length of %d", ErrOversizedPayload, storage.MaxAuditStringLength)
	}

	if len(e.Reason) > storage.MaxAuditReasonLength {
		return fmt.Errorf("%w: reason string exceeds length of %d", ErrOversizedPayload, storage.MaxAuditReasonLength)
	}

	if len(e.Metadata) > storage.MaxAuditMetadataEntries {
		return fmt.Errorf("%w: metadata entries exceed limit of %d", ErrOversizedPayload, storage.MaxAuditMetadataEntries)
	}

	totalMetaBytes := 0
	for k, v := range e.Metadata {
		if len(k) > storage.MaxAuditStringLength || len(v) > storage.MaxAuditReasonLength {
			return fmt.Errorf("%w: metadata key/value exceeds length bounds", ErrOversizedPayload)
		}
		totalMetaBytes += len(k) + len(v)

		// Security scan: prohibit credentials, tokens, and arbitrary command strings
		lowerK := strings.ToLower(k)
		lowerV := strings.ToLower(v)
		if strings.Contains(lowerK, "password") || strings.Contains(lowerK, "secret") ||
			strings.Contains(lowerK, "private_key") || strings.Contains(lowerK, "token") ||
			strings.Contains(lowerK, "credential") || strings.Contains(lowerV, "bearer ") {
			return fmt.Errorf("%w: sensitive credential pattern rejected in audit metadata (%s)", ErrProhibitedContent, k)
		}
		if strings.Contains(lowerV, "/bin/sh") || strings.Contains(lowerV, "powershell") ||
			strings.Contains(lowerV, "cmd.exe") || strings.Contains(lowerV, "exec.command") {
			return fmt.Errorf("%w: executable command pattern rejected in audit metadata (%s)", ErrProhibitedContent, k)
		}
	}
	if totalMetaBytes > storage.MaxAuditMetadataBytes {
		return fmt.Errorf("%w: metadata total size %d exceeds limit %d", ErrOversizedPayload, totalMetaBytes, storage.MaxAuditMetadataBytes)
	}

	return nil
}

// ToStored maps an AuditEvent to the persistent SQLite storage representation.
func (e *AuditEvent) ToStored() *storage.StoredAuditEvent {
	metaCopy := make(map[string]string, len(e.Metadata))
	for k, v := range e.Metadata {
		metaCopy[k] = v
	}

	return &storage.StoredAuditEvent{
		EventID:            e.EventID,
		EventType:          string(e.EventType),
		Timestamp:          e.Timestamp,
		NodeID:             e.NodeID,
		IncidentID:         e.IncidentID,
		DecisionID:         e.DecisionID,
		ActionID:           e.ActionID,
		ApprovalID:         e.ApprovalID,
		MitigationRecordID: e.MitigationRecordID,
		VerificationID:     e.VerificationID,
		EscalationID:       e.EscalationID,
		CorrelationID:      e.CorrelationID,
		PolicyVersion:      e.PolicyVersion,
		Actor:              e.Actor,
		Result:             e.Result,
		Reason:             e.Reason,
		Metadata:           metaCopy,
	}
}

// AuditEventFromStored reconstructs an AuditEvent from the SQLite storage representation.
func AuditEventFromStored(s *storage.StoredAuditEvent) AuditEvent {
	if s == nil {
		return AuditEvent{}
	}
	metaCopy := make(map[string]string, len(s.Metadata))
	for k, v := range s.Metadata {
		metaCopy[k] = v
	}

	return AuditEvent{
		EventID:            s.EventID,
		EventType:          EventType(s.EventType),
		Timestamp:          s.Timestamp,
		NodeID:             s.NodeID,
		IncidentID:         s.IncidentID,
		DecisionID:         s.DecisionID,
		ActionID:           s.ActionID,
		ApprovalID:         s.ApprovalID,
		MitigationRecordID: s.MitigationRecordID,
		VerificationID:     s.VerificationID,
		EscalationID:       s.EscalationID,
		CorrelationID:      s.CorrelationID,
		PolicyVersion:      s.PolicyVersion,
		Actor:              s.Actor,
		Result:             s.Result,
		Reason:             s.Reason,
		Metadata:           metaCopy,
	}
}

// ComputeEventID computes a deterministic, idempotent EventID based on canonical event fields.
// Retries with different attempt numbers or discriminators yield distinct, non-colliding EventIDs.
func ComputeEventID(nodeID, incidentID string, eventType EventType, attempt int, discriminator string) string {
	raw := fmt.Sprintf("%s|%s|%s|%d|%s", nodeID, incidentID, eventType, attempt, discriminator)
	hash := sha256.Sum256([]byte(raw))
	return fmt.Sprintf("evt-%x", hash[:16])
}

// ComputeCorrelationID derives the local correlation identifier binding all events of an incident response workflow.
func ComputeCorrelationID(incidentID string) string {
	if incidentID == "" {
		return ""
	}
	return fmt.Sprintf("corr-%s", incidentID)
}

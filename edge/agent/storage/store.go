package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// Common storage domain errors.
var (
	ErrDuplicateBatch                = errors.New("batch with the given batch_id already exists")
	ErrBatchNotFound                 = errors.New("batch not found")
	ErrStoreClosed                   = errors.New("storage engine is closed")
	ErrInvalidBatch                  = errors.New("cannot persist invalid telemetry batch")
	ErrDuplicateAnomaly              = errors.New("anomaly with the given anomaly_id already exists")
	ErrIncidentNotFound              = errors.New("incident not found")
	ErrInvalidIncident               = errors.New("cannot persist invalid incident")
	ErrDuplicateMitigation           = errors.New("mitigation with the given action_id or decision_id already exists")
	ErrMitigationNotFound            = errors.New("mitigation not found")
	ErrInvalidMitigation             = errors.New("cannot persist invalid mitigation record")
	ErrInvalidMitigationTransition   = errors.New("invalid mitigation status transition")
	ErrDuplicateApproval             = errors.New("approval with the given decision_id or approval_id already exists")
	ErrApprovalNotFound              = errors.New("approval not found")
	ErrInvalidApproval               = errors.New("cannot persist invalid approval record")
	ErrInvalidApprovalTransition     = errors.New("invalid approval status transition")
	ErrApprovalExpired               = errors.New("approval has expired")
	ErrApprovalAlreadyConsumed       = errors.New("approval has already been consumed")
	ErrForbiddenNotOverridable       = errors.New("forbidden actions cannot be approved or overridden")
	ErrCannotRejectApproved          = errors.New("cannot reject an already approved approval")
	ErrDuplicateVerification         = errors.New("verification with the given verification_id already exists")
	ErrVerificationNotFound          = errors.New("verification not found")
	ErrInvalidVerification           = errors.New("cannot persist invalid verification record")
	ErrInvalidVerificationTransition = errors.New("invalid verification status transition")
	ErrVerificationExpired           = errors.New("verification has timed out or expired")
	ErrVerificationTerminal          = errors.New("verification is in a terminal status and cannot be modified")
	ErrDuplicateEscalation           = errors.New("escalation record with the given escalation_id already exists")
	ErrEscalationNotFound            = errors.New("escalation record not found")
	ErrInvalidEscalation             = errors.New("cannot persist invalid escalation record")
	ErrInvalidCircuitStateRecord     = errors.New("cannot persist invalid circuit breaker state")
	ErrDuplicateAuditEvent           = errors.New("audit event with the given event_id already exists")
	ErrAuditEventNotFound            = errors.New("audit event not found")
	ErrInvalidAuditEvent             = errors.New("cannot persist invalid audit event")
	ErrDuplicateCheckpoint           = errors.New("checkpoint with the given checkpoint_id already exists")
	ErrCheckpointNotFound            = errors.New("checkpoint not found")
	ErrInvalidCheckpoint             = errors.New("cannot persist invalid checkpoint record")
)

// SyncStatus represents the synchronization lifecycle state of a locally stored batch.
type SyncStatus string

const (
	SyncStatusPending   SyncStatus = "PENDING"
	SyncStatusSyncing   SyncStatus = "SYNCING"
	SyncStatusSynced    SyncStatus = "SYNCED"
	SyncStatusFailed    SyncStatus = "FAILED"
	SyncStatusPublished SyncStatus = "PUBLISHED"
)

// IsValid checks whether the sync status is recognized.
func (s SyncStatus) IsValid() bool {
	switch s {
	case SyncStatusPending, SyncStatusSyncing, SyncStatusSynced, SyncStatusFailed, SyncStatusPublished:
		return true
	default:
		return false
	}
}

// Record represents a persisted telemetry batch record as stored in the local database.
type Record struct {
	BatchID        string               `json:"batch_id"`
	NodeID         string               `json:"node_id"`
	SequenceNumber int64                `json:"sequence_number"`
	CollectedAt    time.Time            `json:"collected_at"`
	SentAt         *time.Time           `json:"sent_at,omitempty"`
	Attempt        int                  `json:"attempt"`
	Payload        types.TelemetryBatch `json:"payload"`
	CreatedAt      time.Time            `json:"created_at"`
	SyncStatus     SyncStatus           `json:"sync_status"`
}

// Store defines the local storage contract for the edge node.
// Implementations must commit batches according to configured storage durability semantics
// before returning success. Local persistence acts as the system's first durable buffering boundary.
type Store interface {
	// PersistBatch transactionally validates and persists a telemetry batch with PENDING status.
	// If a batch with the same BatchID already exists, ErrDuplicateBatch is returned.
	PersistBatch(ctx context.Context, batch *types.TelemetryBatch) error

	// GetBatch retrieves a single persisted telemetry record by its unique BatchID.
	GetBatch(ctx context.Context, batchID string) (*Record, error)

	// GetPendingBatches retrieves batches that are awaiting synchronization, ordered by sequence_number.
	GetPendingBatches(ctx context.Context, limit int) ([]*Record, error)

	// GetPendingNodes returns distinct NodeIDs with batches currently in PENDING status.
	GetPendingNodes(ctx context.Context) ([]string, error)

	// GetPendingBatchesByNode retrieves pending batches for a specific node, ordered by sequence_number ASC.
	// This enables node-isolated synchronization pipelines.
	GetPendingBatchesByNode(ctx context.Context, nodeID string, limit int) ([]*Record, error)

	// MarkBatchSynced updates the batch's sync_status to SYNCED and sets sent_at.
	// Returns ErrBatchNotFound if the batch does not exist.
	MarkBatchSynced(ctx context.Context, batchID string, sentAt time.Time) error

	// RecordSyncAttempt increments the logical sync attempt counter by 1 and updates sent_at,
	// keeping sync_status as PENDING. This represents a failed synchronization cycle.
	// Returns ErrBatchNotFound if the batch does not exist.
	RecordSyncAttempt(ctx context.Context, batchID string, sentAt time.Time) error

	// MarkBatchPublished updates the batch's sync_status to PUBLISHED and sets published_at.
	// This indicates a successful NATS JetStream PubAck was received.
	// Returns ErrBatchNotFound if the batch does not exist.
	MarkBatchPublished(ctx context.Context, batchID string, publishedAt time.Time) error

	// GetLatestSequenceNumber returns the highest sequence number persisted for a given node.
	// If no records exist for the node, it returns -1.
	GetLatestSequenceNumber(ctx context.Context, nodeID string) (int64, error)

	// CountBatches returns the total count of telemetry batches stored locally.
	CountBatches(ctx context.Context) (int64, error)

	// Close cleanly terminates database connections and checkpoints the WAL journal.
	Close() error
}

// StoredObservation represents a durably recorded anomaly observation in SQLite.
type StoredObservation struct {
	AnomalyID       string            `json:"anomaly_id"`
	IncidentID      string            `json:"incident_id,omitempty"`
	NodeID          string            `json:"node_id"`
	MetricName      string            `json:"metric_name"`
	DetectedAt      time.Time         `json:"detected_at"`
	ObservedValue   float64           `json:"observed_value"`
	AnomalyScore    float64           `json:"anomaly_score"`
	DetectionMethod string            `json:"detection_method"`
	Evidence        map[string]string `json:"evidence,omitempty"`
	CreatedAt       time.Time         `json:"created_at"`
}

// IncidentStore defines the durable persistence contract for incidents and anomaly observations.
type IncidentStore interface {
	// HasObservation checks if an AnomalyID has already been durably recorded (idempotency check).
	HasObservation(ctx context.Context, anomalyID string) (bool, error)

	// RecordObservation inserts an anomaly observation into SQLite.
	// Returns ErrDuplicateAnomaly if the anomaly_id already exists.
	RecordObservation(ctx context.Context, obs *StoredObservation) error

	// GetActiveIncident retrieves the current active incident for nodeID and metricName.
	// Returns (nil, nil) if no active incident exists.
	GetActiveIncident(ctx context.Context, nodeID, metricName string) (*types.Incident, error)

	// GetIncident retrieves an incident by its unique IncidentID.
	// Returns (nil, ErrIncidentNotFound) if it does not exist.
	GetIncident(ctx context.Context, incidentID string) (*types.Incident, error)

	// ListActiveIncidents retrieves all incidents currently in active status (ANOMALY_DETECTED, MITIGATING, ESCALATED).
	ListActiveIncidents(ctx context.Context) ([]*types.Incident, error)

	// ListRecentObservations retrieves recent observations for a correlation stream within a temporal window,
	// ordered chronologically by detected_at ASC.
	ListRecentObservations(ctx context.Context, nodeID, metricName string, since time.Time, limit int) ([]*StoredObservation, error)

	// ListAllRecentObservations retrieves recent observations across all streams detected at or after since,
	// ordered chronologically by detected_at ASC.
	ListAllRecentObservations(ctx context.Context, since time.Time, limit int) ([]*StoredObservation, error)

	// PersistIncidentEvaluation executes an atomic transaction for:
	// 1. Checking anomaly idempotency (returns isNew=false if already processed)
	// 2. Persisting the anomaly observation
	// 3. Creating or updating the Incident domain entity (if non-nil)
	// Returns (isNew, error). If any operation fails, the transaction rolls back cleanly.
	PersistIncidentEvaluation(ctx context.Context, obs *StoredObservation, inc *types.Incident) (bool, error)

	// UpdateIncidentStatus updates the status, updated_at, and resolved_at of an incident.
	UpdateIncidentStatus(ctx context.Context, incidentID string, status types.IncidentStatus, updatedAt time.Time, resolvedAt *time.Time) error
}

// StoredMitigation represents a durably recorded mitigation action in SQLite.
type StoredMitigation struct {
	ActionID    string                     `json:"action_id"`
	DecisionID  string                     `json:"decision_id"`
	IncidentID  string                     `json:"incident_id"`
	NodeID      string                     `json:"node_id"`
	ActionType  types.MitigationActionType `json:"action_type"`
	Target      string                     `json:"target"`
	Status      types.MitigationStatus     `json:"status"`
	Mode        string                     `json:"mode"`
	Message     string                     `json:"message"`
	ErrorCode   string                     `json:"error_code,omitempty"`
	Parameters  map[string]string          `json:"parameters,omitempty"`
	StartedAt   time.Time                  `json:"started_at"`
	CompletedAt *time.Time                 `json:"completed_at,omitempty"`
	DurationMs  int64                      `json:"duration_ms"`
	Simulated   bool                       `json:"simulated"`
	CreatedAt   time.Time                  `json:"created_at"`
	UpdatedAt   time.Time                  `json:"updated_at"`
}

// Validate checks for structural and semantic validity of the StoredMitigation.
func (m *StoredMitigation) Validate() error {
	if m == nil {
		return ErrInvalidMitigation
	}
	if strings.TrimSpace(m.ActionID) == "" {
		return fmt.Errorf("%w: action_id cannot be empty", ErrInvalidMitigation)
	}
	if strings.TrimSpace(m.DecisionID) == "" {
		return fmt.Errorf("%w: decision_id cannot be empty", ErrInvalidMitigation)
	}
	if strings.TrimSpace(m.IncidentID) == "" {
		return fmt.Errorf("%w: incident_id cannot be empty", ErrInvalidMitigation)
	}
	if strings.TrimSpace(m.NodeID) == "" {
		return fmt.Errorf("%w: node_id cannot be empty", ErrInvalidMitigation)
	}
	if !m.ActionType.IsValid() {
		return fmt.Errorf("%w: invalid action_type %q", ErrInvalidMitigation, m.ActionType)
	}
	if strings.TrimSpace(m.Target) == "" {
		return fmt.Errorf("%w: target cannot be empty", ErrInvalidMitigation)
	}
	if !m.Status.IsValid() {
		return fmt.Errorf("%w: invalid mitigation status %q", ErrInvalidMitigation, m.Status)
	}
	if m.StartedAt.IsZero() {
		return fmt.Errorf("%w: started_at timestamp cannot be zero", ErrInvalidMitigation)
	}
	if m.CompletedAt != nil && m.CompletedAt.Before(m.StartedAt) {
		return fmt.Errorf("%w: completed_at cannot be before started_at", ErrInvalidMitigation)
	}
	return nil
}

// ToMitigationAction converts the StoredMitigation to the shared types.MitigationAction contract.
func (m *StoredMitigation) ToMitigationAction() types.MitigationAction {
	return types.MitigationAction{
		ActionID:    m.ActionID,
		IncidentID:  m.IncidentID,
		ActionType:  m.ActionType,
		Target:      m.Target,
		Status:      m.Status,
		Message:     m.Message,
		Error:       m.ErrorCode,
		TriggeredAt: m.StartedAt,
		CompletedAt: m.CompletedAt,
	}
}

// CanMitigationTransition enforces safe, deterministic state machine transitions for mitigations:
//
//	PENDING   -> EXECUTING | SKIPPED | FAILED
//	EXECUTING -> EXECUTED  | FAILED  | UNKNOWN_RECONCILIATION_REQUIRED
//
// Terminal states (EXECUTED, FAILED, SKIPPED, UNKNOWN_RECONCILIATION_REQUIRED) cannot transition further.
// Specifically, UNKNOWN_RECONCILIATION_REQUIRED is never automatically converted back to EXECUTED.
func CanMitigationTransition(current, next types.MitigationStatus) bool {
	if current == next {
		return true
	}
	switch current {
	case types.MitigationStatusPending:
		return next == types.MitigationStatusExecuting ||
			next == types.MitigationStatusSkipped ||
			next == types.MitigationStatusFailed
	case types.MitigationStatusExecuting:
		return next == types.MitigationStatusExecuted ||
			next == types.MitigationStatusFailed ||
			next == types.MitigationStatusUnknownReconciliationRequired
	default:
		return false
	}
}

// MitigationStore defines the durable persistence and recovery contract for automated mitigations.
// Local SQLite WAL storage serves as the durable authority for mitigation identity and history.
type MitigationStore interface {
	// RecordMitigation transactionally validates and persists a mitigation record.
	// Returns ErrDuplicateMitigation if action_id or decision_id already exists.
	RecordMitigation(ctx context.Context, m *StoredMitigation) error

	// GetMitigation retrieves a mitigation record by its unique ActionID.
	// Returns (nil, ErrMitigationNotFound) if not found.
	GetMitigation(ctx context.Context, actionID string) (*StoredMitigation, error)

	// GetMitigationByDecisionID retrieves a mitigation record by its deterministic DecisionID.
	// Returns (nil, ErrMitigationNotFound) if not found.
	GetMitigationByDecisionID(ctx context.Context, decisionID string) (*StoredMitigation, error)

	// ListMitigations retrieves mitigation records matching an optional incidentID (or all if empty),
	// ordered by started_at ASC, bounded by limit.
	ListMitigations(ctx context.Context, incidentID string, limit int) ([]*StoredMitigation, error)

	// UpdateMitigationStatus updates the status, completion details, and updated_at of an existing mitigation.
	// Returns ErrInvalidMitigationTransition if the transition violates state machine rules.
	UpdateMitigationStatus(ctx context.Context, actionID string, status types.MitigationStatus, message, errCode string, completedAt *time.Time, durationMs int64) error

	// MarkMitigationExecuting updates an existing mitigation to EXECUTING status.
	MarkMitigationExecuting(ctx context.Context, actionID string, startedAt time.Time) error

	// MarkMitigationExecuted updates an existing mitigation to EXECUTED with completion details.
	MarkMitigationExecuted(ctx context.Context, actionID string, message string, completedAt time.Time, durationMs int64) error

	// MarkMitigationFailed updates an existing mitigation to FAILED with error details.
	MarkMitigationFailed(ctx context.Context, actionID string, message, errCode string, completedAt time.Time, durationMs int64) error

	// MarkMitigationUnknown updates an existing mitigation to UNKNOWN_RECONCILIATION_REQUIRED.
	MarkMitigationUnknown(ctx context.Context, actionID string, message, errCode string, updatedAt time.Time) error

	// RecoverInFlightMitigations transitions any mitigation stranded in EXECUTING status to
	// UNKNOWN_RECONCILIATION_REQUIRED upon startup/crash recovery.
	// Returns the number of transitioned records.
	RecoverInFlightMitigations(ctx context.Context) (int64, error)

	// ListMitigationsByStatus retrieves mitigation records matching a specific status, ordered by started_at ASC.
	ListMitigationsByStatus(ctx context.Context, status types.MitigationStatus, limit int) ([]*StoredMitigation, error)

	// CountMitigations returns the total count of mitigation records stored.
	CountMitigations(ctx context.Context) (int64, error)
}

// StoredApproval represents a durably recorded operator approval in SQLite.
type StoredApproval struct {
	ApprovalID          string                     `json:"approval_id"`
	DecisionID          string                     `json:"decision_id"`
	IncidentID          string                     `json:"incident_id"`
	ActionID            string                     `json:"action_id"`
	NodeID              string                     `json:"node_id"`
	ActionType          types.MitigationActionType `json:"action_type"`
	Target              string                     `json:"target"`
	PolicyVersion       string                     `json:"policy_version"`
	DecisionFingerprint string                     `json:"decision_fingerprint"`
	Status              types.ApprovalStatus       `json:"status"`
	RequestedAt         time.Time                  `json:"requested_at"`
	ExpiresAt           time.Time                  `json:"expires_at"`
	RequestedBy         string                     `json:"requested_by"`
	ApprovedBy          string                     `json:"approved_by"`
	RejectedBy          string                     `json:"rejected_by"`
	Reason              string                     `json:"reason"`
	CreatedAt           time.Time                  `json:"created_at"`
	UpdatedAt           time.Time                  `json:"updated_at"`
	ConsumedAt          *time.Time                 `json:"consumed_at,omitempty"`
}

// Validate checks the structural validity of StoredApproval.
func (a *StoredApproval) Validate() error {
	if strings.TrimSpace(a.ApprovalID) == "" {
		return fmt.Errorf("%w: approval_id cannot be empty", ErrInvalidApproval)
	}
	if strings.TrimSpace(a.DecisionID) == "" {
		return fmt.Errorf("%w: decision_id cannot be empty", ErrInvalidApproval)
	}
	if strings.TrimSpace(a.IncidentID) == "" {
		return fmt.Errorf("%w: incident_id cannot be empty", ErrInvalidApproval)
	}
	if strings.TrimSpace(a.ActionID) == "" {
		return fmt.Errorf("%w: action_id cannot be empty", ErrInvalidApproval)
	}
	if strings.TrimSpace(a.NodeID) == "" {
		return fmt.Errorf("%w: node_id cannot be empty", ErrInvalidApproval)
	}
	if !a.ActionType.IsValid() {
		return fmt.Errorf("%w: invalid action_type %q", ErrInvalidApproval, a.ActionType)
	}
	if strings.TrimSpace(a.Target) == "" {
		return fmt.Errorf("%w: target cannot be empty", ErrInvalidApproval)
	}
	if strings.TrimSpace(a.PolicyVersion) == "" {
		return fmt.Errorf("%w: policy_version cannot be empty", ErrInvalidApproval)
	}
	if strings.TrimSpace(a.DecisionFingerprint) == "" {
		return fmt.Errorf("%w: decision_fingerprint cannot be empty", ErrInvalidApproval)
	}
	if !a.Status.IsValid() {
		return fmt.Errorf("%w: invalid approval status %q", ErrInvalidApproval, a.Status)
	}
	if a.RequestedAt.IsZero() {
		return fmt.Errorf("%w: requested_at cannot be zero", ErrInvalidApproval)
	}
	if a.ExpiresAt.IsZero() {
		return fmt.Errorf("%w: expires_at cannot be zero", ErrInvalidApproval)
	}
	if a.ExpiresAt.Before(a.RequestedAt) {
		return fmt.Errorf("%w: expires_at cannot be before requested_at", ErrInvalidApproval)
	}
	if strings.TrimSpace(a.RequestedBy) == "" {
		return fmt.Errorf("%w: requested_by cannot be empty", ErrInvalidApproval)
	}
	return nil
}

// CanApprovalTransition enforces safe, deterministic state machine transitions for approvals:
//
//	PENDING  -> APPROVED | REJECTED | EXPIRED | CANCELLED
//	APPROVED -> CONSUMED | EXPIRED
//
// Terminal states (CONSUMED, REJECTED, EXPIRED, CANCELLED) cannot transition further.
func CanApprovalTransition(current, next types.ApprovalStatus) bool {
	return current.CanTransitionTo(next)
}

// ApprovalStore defines the durable persistence and recovery contract for operator approvals.
// Local SQLite WAL storage serves as the durable authority for approval records and decision bindings.
type ApprovalStore interface {
	// RecordApproval transactionally validates and persists an approval record in PENDING status.
	// Returns ErrDuplicateApproval if an approval with the same approval_id or decision_id already exists.
	RecordApproval(ctx context.Context, app *StoredApproval) error

	// GetApproval retrieves an approval record by its unique ApprovalID.
	// Returns (nil, ErrApprovalNotFound) if not found.
	GetApproval(ctx context.Context, approvalID string) (*StoredApproval, error)

	// GetApprovalByDecisionID retrieves an approval record by its unique DecisionID.
	// Returns (nil, ErrApprovalNotFound) if not found.
	GetApprovalByDecisionID(ctx context.Context, decisionID string) (*StoredApproval, error)

	// ApproveApproval transitions a PENDING approval to APPROVED.
	// Idempotent: if already APPROVED with matching approvedBy, returns nil.
	ApproveApproval(ctx context.Context, approvalID, approvedBy string, now time.Time) error

	// RejectApproval transitions a PENDING approval to REJECTED.
	RejectApproval(ctx context.Context, approvalID, rejectedBy, reason string, now time.Time) error

	// CancelApproval transitions a PENDING approval to CANCELLED.
	CancelApproval(ctx context.Context, approvalID, reason string, now time.Time) error

	// ConsumeApproval atomically transitions an APPROVED approval to CONSUMED.
	// Fails if the approval is expired, not approved, or already consumed.
	ConsumeApproval(ctx context.Context, approvalID string, now time.Time) error

	// ExpireStaleApprovals transitions any unconsumed approvals past their expiration time to EXPIRED.
	ExpireStaleApprovals(ctx context.Context, now time.Time) (int64, error)

	// ListApprovals retrieves approval records matching an optional incidentID (or all if empty),
	// ordered by requested_at ASC, bounded by limit.
	ListApprovals(ctx context.Context, incidentID string, limit int) ([]*StoredApproval, error)

	// CountApprovals returns the total count of approval records stored.
	CountApprovals(ctx context.Context) (int64, error)
}

// StoredVerification represents a durably recorded incident recovery verification in SQLite.
type StoredVerification struct {
	VerificationID       string                   `json:"verification_id"`
	IncidentID           string                   `json:"incident_id"`
	ActionID             string                   `json:"action_id"`
	DecisionID           string                   `json:"decision_id"`
	NodeID               string                   `json:"node_id"`
	MetricName           string                   `json:"metric_name"`
	ConditionType        string                   `json:"condition_type"`
	RecoveryThreshold    float64                  `json:"recovery_threshold"`
	Comparator           string                   `json:"comparator"`
	Status               types.VerificationStatus `json:"status"`
	RequiredObservations int                      `json:"required_observations"`
	ConsecutiveHealthy   int                      `json:"consecutive_healthy"`
	TotalObservations    int                      `json:"total_observations"`
	StartedAt            time.Time                `json:"started_at"`
	ExpiresAt            time.Time                `json:"expires_at"`
	CompletedAt          *time.Time               `json:"completed_at,omitempty"`
	RecoveredAt          *time.Time               `json:"recovered_at,omitempty"`
	LastObservationAt    *time.Time               `json:"last_observation_at,omitempty"`
	LastObservedValue    *float64                 `json:"last_observed_value,omitempty"`
	Reason               string                   `json:"reason"`
	Evidence             map[string]string        `json:"evidence,omitempty"`
	CreatedAt            time.Time                `json:"created_at"`
	UpdatedAt            time.Time                `json:"updated_at"`
}

// Validate checks the structural validity of StoredVerification.
func (v *StoredVerification) Validate() error {
	if v == nil {
		return ErrInvalidVerification
	}
	if strings.TrimSpace(v.VerificationID) == "" {
		return fmt.Errorf("%w: verification_id cannot be empty", ErrInvalidVerification)
	}
	if strings.TrimSpace(v.IncidentID) == "" {
		return fmt.Errorf("%w: incident_id cannot be empty", ErrInvalidVerification)
	}
	if strings.TrimSpace(v.ActionID) == "" {
		return fmt.Errorf("%w: action_id cannot be empty", ErrInvalidVerification)
	}
	if strings.TrimSpace(v.DecisionID) == "" {
		return fmt.Errorf("%w: decision_id cannot be empty", ErrInvalidVerification)
	}
	if strings.TrimSpace(v.NodeID) == "" {
		return fmt.Errorf("%w: node_id cannot be empty", ErrInvalidVerification)
	}
	if strings.TrimSpace(v.MetricName) == "" {
		return fmt.Errorf("%w: metric_name cannot be empty", ErrInvalidVerification)
	}
	if strings.TrimSpace(v.ConditionType) == "" {
		return fmt.Errorf("%w: condition_type cannot be empty", ErrInvalidVerification)
	}
	if !v.Status.IsValid() {
		return fmt.Errorf("%w: invalid verification status %q", ErrInvalidVerification, v.Status)
	}
	if v.RequiredObservations <= 0 {
		return fmt.Errorf("%w: required_observations must be positive", ErrInvalidVerification)
	}
	if v.StartedAt.IsZero() {
		return fmt.Errorf("%w: started_at cannot be zero", ErrInvalidVerification)
	}
	if v.ExpiresAt.IsZero() {
		return fmt.Errorf("%w: expires_at cannot be zero", ErrInvalidVerification)
	}
	if v.ExpiresAt.Before(v.StartedAt) {
		return fmt.Errorf("%w: expires_at cannot be before started_at", ErrInvalidVerification)
	}
	return nil
}

// CanVerificationTransition enforces safe, deterministic state machine transitions for verifications:
//
//	PENDING -> RECOVERED | NOT_RECOVERED | TIMED_OUT | CANCELLED
//
// Terminal states cannot transition further.
func CanVerificationTransition(current, next types.VerificationStatus) bool {
	return current.CanTransitionTo(next)
}

// VerificationStore defines the durable persistence and recovery contract for incident recovery verifications.
// Local SQLite WAL storage serves as the durable authority for verification state.
type VerificationStore interface {
	// RecordVerification transactionally validates and persists a verification record in PENDING status.
	// Returns ErrDuplicateVerification if a verification with the same verification_id already exists.
	RecordVerification(ctx context.Context, v *StoredVerification) error

	// GetVerification retrieves a verification record by its unique VerificationID.
	// Returns (nil, ErrVerificationNotFound) if not found.
	GetVerification(ctx context.Context, verificationID string) (*StoredVerification, error)

	// GetVerificationByActionID retrieves a verification record by its ActionID.
	// Returns (nil, ErrVerificationNotFound) if not found.
	GetVerificationByActionID(ctx context.Context, actionID string) (*StoredVerification, error)

	// GetActiveVerification retrieves the active (PENDING) verification for a node and metric stream, or nil if none.
	GetActiveVerification(ctx context.Context, nodeID, metricName string) (*StoredVerification, error)

	// UpdateVerificationProgress updates observations, consecutive healthy count, status, timestamps, and reason.
	UpdateVerificationProgress(ctx context.Context, verificationID string, status types.VerificationStatus, consecutiveHealthy, totalObservations int, lastObservedValue float64, lastObsAt, completedAt, recoveredAt *time.Time, reason string, evidence map[string]string, now time.Time) error

	// ExpireStaleVerifications transitions any PENDING verifications past their expiration time to TIMED_OUT.
	ExpireStaleVerifications(ctx context.Context, now time.Time) (int64, error)

	// ListVerifications retrieves verification records matching an optional incidentID (or all if empty),
	// ordered by started_at ASC, bounded by limit.
	ListVerifications(ctx context.Context, incidentID string, limit int) ([]*StoredVerification, error)

	// CountVerifications returns the total count of verification records stored.
	CountVerifications(ctx context.Context) (int64, error)
}

// StoredEscalation represents a durably recorded failure escalation record in SQLite.
type StoredEscalation struct {
	EscalationID          string                      `json:"escalation_id"`
	IncidentID            string                      `json:"incident_id"`
	NodeID                string                      `json:"node_id"`
	ActionID              string                      `json:"action_id"`
	PolicyVersion         string                      `json:"policy_version"`
	FailureClassification types.FailureClassification `json:"failure_classification"`
	Status                types.EscalationStatus      `json:"status"`
	Attempt               int                         `json:"attempt"`
	MaxAttempts           int                         `json:"max_attempts"`
	Reason                string                      `json:"reason"`
	Evidence              map[string]string           `json:"evidence,omitempty"`
	CreatedAt             time.Time                   `json:"created_at"`
	UpdatedAt             time.Time                   `json:"updated_at"`
	ResolvedAt            *time.Time                  `json:"resolved_at,omitempty"`
}

// Validate verifies the StoredEscalation payload before persistence.
func (e *StoredEscalation) Validate() error {
	if strings.TrimSpace(e.EscalationID) == "" {
		return fmt.Errorf("%w: escalation_id cannot be empty", ErrInvalidEscalation)
	}
	if strings.TrimSpace(e.IncidentID) == "" {
		return fmt.Errorf("%w: incident_id cannot be empty", ErrInvalidEscalation)
	}
	if strings.TrimSpace(e.NodeID) == "" {
		return fmt.Errorf("%w: node_id cannot be empty", ErrInvalidEscalation)
	}
	if !e.FailureClassification.IsValid() {
		return fmt.Errorf("%w: invalid failure classification %q", ErrInvalidEscalation, e.FailureClassification)
	}
	if !e.Status.IsValid() {
		return fmt.Errorf("%w: invalid escalation status %q", ErrInvalidEscalation, e.Status)
	}
	if e.Attempt < 0 {
		return fmt.Errorf("%w: attempt cannot be negative", ErrInvalidEscalation)
	}
	if e.MaxAttempts < 0 {
		return fmt.Errorf("%w: max_attempts cannot be negative", ErrInvalidEscalation)
	}
	if e.CreatedAt.IsZero() {
		return fmt.Errorf("%w: created_at cannot be zero", ErrInvalidEscalation)
	}
	if e.ResolvedAt != nil && e.ResolvedAt.Before(e.CreatedAt) {
		return fmt.Errorf("%w: resolved_at cannot be before created_at", ErrInvalidEscalation)
	}
	return nil
}

// StoredCircuitState records the persistent circuit breaker state for a node and incident in SQLite.
type StoredCircuitState struct {
	NodeID       string             `json:"node_id"`
	IncidentID   string             `json:"incident_id"`
	State        types.CircuitState `json:"circuit_state"`
	FailureCount int                `json:"failure_count"`
	TrippedAt    *time.Time         `json:"tripped_at,omitempty"`
	ResetAt      *time.Time         `json:"reset_at,omitempty"`
	ResetBy      string             `json:"reset_by,omitempty"`
	UpdatedAt    time.Time          `json:"updated_at"`
}

// Validate verifies the StoredCircuitState payload.
func (c *StoredCircuitState) Validate() error {
	if strings.TrimSpace(c.NodeID) == "" {
		return fmt.Errorf("%w: node_id cannot be empty", ErrInvalidCircuitStateRecord)
	}
	if strings.TrimSpace(c.IncidentID) == "" {
		return fmt.Errorf("%w: incident_id cannot be empty", ErrInvalidCircuitStateRecord)
	}
	if !c.State.IsValid() {
		return fmt.Errorf("%w: invalid circuit state %q", ErrInvalidCircuitStateRecord, c.State)
	}
	if c.FailureCount < 0 {
		return fmt.Errorf("%w: failure_count cannot be negative", ErrInvalidCircuitStateRecord)
	}
	if c.UpdatedAt.IsZero() {
		return fmt.Errorf("%w: updated_at cannot be zero", ErrInvalidCircuitStateRecord)
	}
	return nil
}

// EscalationStore defines the durable persistence and recovery contract for incident escalations and circuit breaker states.
type EscalationStore interface {
	// RecordEscalation transactionally validates and persists an escalation record.
	// Returns ErrDuplicateEscalation if a record with the same escalation_id already exists.
	RecordEscalation(ctx context.Context, esc *StoredEscalation) error

	// GetEscalation retrieves an escalation record by its unique EscalationID.
	// Returns (nil, ErrEscalationNotFound) if not found.
	GetEscalation(ctx context.Context, escalationID string) (*StoredEscalation, error)

	// ListEscalations retrieves escalation records matching an optional incidentID (or all if empty),
	// ordered by created_at ASC, bounded by limit.
	ListEscalations(ctx context.Context, incidentID string, limit int) ([]*StoredEscalation, error)

	// CountFailuresInWindow counts how many failure records occurred for a given node and incident within [since, now].
	CountFailuresInWindow(ctx context.Context, nodeID, incidentID string, since time.Time) (int, error)

	// GetLatestEscalation retrieves the most recent escalation record for a given node and incident, or nil if none.
	GetLatestEscalation(ctx context.Context, nodeID, incidentID string) (*StoredEscalation, error)

	// UpdateEscalationStatus updates status, resolved_at, reason, and updated_at.
	UpdateEscalationStatus(ctx context.Context, escalationID string, next types.EscalationStatus, resolvedAt *time.Time, reason string, now time.Time) error

	// GetCircuitState retrieves the circuit breaker state for a node and incident, or nil if none.
	GetCircuitState(ctx context.Context, nodeID, incidentID string) (*StoredCircuitState, error)

	// SetCircuitState persists or updates the circuit breaker state.
	SetCircuitState(ctx context.Context, state *StoredCircuitState) error

	// CountEscalations returns total escalation records stored.
	CountEscalations(ctx context.Context) (int64, error)
}

// Bounded data limits for audit events.
const (
	DefaultAuditQueryLimit  = 100
	MaxAuditQueryLimit      = 1000
	MaxAuditMetadataBytes   = 8192
	MaxAuditMetadataEntries = 32
	MaxAuditReasonLength    = 1024
	MaxAuditStringLength    = 128
)

// StoredAuditEvent represents a durably recorded audit event in SQLite.
type StoredAuditEvent struct {
	EventID            string            `json:"event_id"`
	EventType          string            `json:"event_type"`
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

// Validate verifies bounds, non-empty identifiers, and security constraints on StoredAuditEvent.
func (e *StoredAuditEvent) Validate() error {
	if strings.TrimSpace(e.EventID) == "" {
		return fmt.Errorf("%w: event_id cannot be empty", ErrInvalidAuditEvent)
	}
	if len(e.EventID) > MaxAuditStringLength {
		return fmt.Errorf("%w: event_id exceeds max length of %d", ErrInvalidAuditEvent, MaxAuditStringLength)
	}
	if strings.TrimSpace(e.EventType) == "" {
		return fmt.Errorf("%w: event_type cannot be empty", ErrInvalidAuditEvent)
	}
	if len(e.EventType) > MaxAuditStringLength {
		return fmt.Errorf("%w: event_type exceeds max length of %d", ErrInvalidAuditEvent, MaxAuditStringLength)
	}
	if e.Timestamp.IsZero() {
		return fmt.Errorf("%w: timestamp cannot be zero", ErrInvalidAuditEvent)
	}
	if strings.TrimSpace(e.NodeID) == "" {
		return fmt.Errorf("%w: node_id cannot be empty", ErrInvalidAuditEvent)
	}
	if len(e.NodeID) > MaxAuditStringLength {
		return fmt.Errorf("%w: node_id exceeds max length of %d", ErrInvalidAuditEvent, MaxAuditStringLength)
	}
	if strings.TrimSpace(e.IncidentID) == "" {
		return fmt.Errorf("%w: incident_id cannot be empty", ErrInvalidAuditEvent)
	}
	if len(e.IncidentID) > MaxAuditStringLength {
		return fmt.Errorf("%w: incident_id exceeds max length of %d", ErrInvalidAuditEvent, MaxAuditStringLength)
	}
	if len(e.DecisionID) > MaxAuditStringLength {
		return fmt.Errorf("%w: decision_id exceeds max length of %d", ErrInvalidAuditEvent, MaxAuditStringLength)
	}
	if len(e.ActionID) > MaxAuditStringLength {
		return fmt.Errorf("%w: action_id exceeds max length of %d", ErrInvalidAuditEvent, MaxAuditStringLength)
	}
	if len(e.ApprovalID) > MaxAuditStringLength {
		return fmt.Errorf("%w: approval_id exceeds max length of %d", ErrInvalidAuditEvent, MaxAuditStringLength)
	}
	if len(e.MitigationRecordID) > MaxAuditStringLength {
		return fmt.Errorf("%w: mitigation_record_id exceeds max length of %d", ErrInvalidAuditEvent, MaxAuditStringLength)
	}
	if len(e.VerificationID) > MaxAuditStringLength {
		return fmt.Errorf("%w: verification_id exceeds max length of %d", ErrInvalidAuditEvent, MaxAuditStringLength)
	}
	if len(e.EscalationID) > MaxAuditStringLength {
		return fmt.Errorf("%w: escalation_id exceeds max length of %d", ErrInvalidAuditEvent, MaxAuditStringLength)
	}
	if len(e.CorrelationID) > MaxAuditStringLength {
		return fmt.Errorf("%w: correlation_id exceeds max length of %d", ErrInvalidAuditEvent, MaxAuditStringLength)
	}
	if len(e.PolicyVersion) > MaxAuditStringLength {
		return fmt.Errorf("%w: policy_version exceeds max length of %d", ErrInvalidAuditEvent, MaxAuditStringLength)
	}
	if len(e.Actor) > MaxAuditStringLength {
		return fmt.Errorf("%w: actor exceeds max length of %d", ErrInvalidAuditEvent, MaxAuditStringLength)
	}
	if len(e.Result) > MaxAuditStringLength {
		return fmt.Errorf("%w: result exceeds max length of %d", ErrInvalidAuditEvent, MaxAuditStringLength)
	}
	if len(e.Reason) > MaxAuditReasonLength {
		return fmt.Errorf("%w: reason exceeds max length of %d", ErrInvalidAuditEvent, MaxAuditReasonLength)
	}
	if len(e.Metadata) > MaxAuditMetadataEntries {
		return fmt.Errorf("%w: metadata exceeds maximum entry count of %d", ErrInvalidAuditEvent, MaxAuditMetadataEntries)
	}

	totalMetaBytes := 0
	for k, v := range e.Metadata {
		if len(k) > MaxAuditStringLength || len(v) > MaxAuditReasonLength {
			return fmt.Errorf("%w: metadata key/value exceeds length bounds", ErrInvalidAuditEvent)
		}
		totalMetaBytes += len(k) + len(v)
		// Security check: reject potential credentials or prohibited commands
		lowerK := strings.ToLower(k)
		lowerV := strings.ToLower(v)
		if strings.Contains(lowerK, "password") || strings.Contains(lowerK, "secret") ||
			strings.Contains(lowerK, "private_key") || strings.Contains(lowerK, "token") ||
			strings.Contains(lowerK, "credential") || strings.Contains(lowerV, "bearer ") {
			return fmt.Errorf("%w: sensitive credential pattern rejected in audit metadata", ErrInvalidAuditEvent)
		}
		if strings.Contains(lowerV, "/bin/sh") || strings.Contains(lowerV, "powershell") ||
			strings.Contains(lowerV, "cmd.exe") || strings.Contains(lowerV, "exec.command") {
			return fmt.Errorf("%w: executable command pattern rejected in audit metadata", ErrInvalidAuditEvent)
		}
	}
	if totalMetaBytes > MaxAuditMetadataBytes {
		return fmt.Errorf("%w: metadata total size %d exceeds max allowed %d bytes", ErrInvalidAuditEvent, totalMetaBytes, MaxAuditMetadataBytes)
	}
	return nil
}

// AuditFilter defines optional query filter criteria for listing audit events.
type AuditFilter struct {
	IncidentID    string    `json:"incident_id,omitempty"`
	NodeID        string    `json:"node_id,omitempty"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	EventType     string    `json:"event_type,omitempty"`
	Since         time.Time `json:"since,omitempty"`
	Until         time.Time `json:"until,omitempty"`
	Limit         int       `json:"limit,omitempty"`
}

// AuditStore defines the durable persistence and query contract for incident response audit events.
type AuditStore interface {
	// RecordAuditEvent transactionally validates and persists an audit event.
	// Returns ErrDuplicateAuditEvent if an event with the same event_id already exists.
	RecordAuditEvent(ctx context.Context, event *StoredAuditEvent) error

	// GetAuditEvent retrieves an audit event by its unique event_id.
	// Returns (nil, ErrAuditEventNotFound) if not found.
	GetAuditEvent(ctx context.Context, eventID string) (*StoredAuditEvent, error)

	// ListAuditEventsByIncident retrieves audit events for an incident_id,
	// ordered chronologically (timestamp ASC, event_id ASC), bounded by limit.
	ListAuditEventsByIncident(ctx context.Context, incidentID string, limit int) ([]*StoredAuditEvent, error)

	// ListAuditEventsByCorrelation retrieves audit events sharing a correlation_id,
	// ordered chronologically (timestamp ASC, event_id ASC), bounded by limit.
	ListAuditEventsByCorrelation(ctx context.Context, correlationID string, limit int) ([]*StoredAuditEvent, error)

	// ListAuditEventsByNode retrieves audit events for a node_id,
	// ordered chronologically (timestamp ASC, event_id ASC), bounded by limit.
	ListAuditEventsByNode(ctx context.Context, nodeID string, limit int) ([]*StoredAuditEvent, error)

	// ListAuditEvents retrieves audit events matching an optional filter, bounded by limit.
	ListAuditEvents(ctx context.Context, filter AuditFilter) ([]*StoredAuditEvent, error)

	// CountAuditEvents returns the total count of audit events stored.
	CountAuditEvents(ctx context.Context) (int64, error)
}

// StoredCheckpoint represents a durably recorded runtime recovery checkpoint in SQLite.
type StoredCheckpoint struct {
	CheckpointID          string     `json:"checkpoint_id"`
	NodeID                string     `json:"node_id"`
	State                 string     `json:"state"`
	StartedAt             time.Time  `json:"started_at"`
	CompletedAt           *time.Time `json:"completed_at,omitempty"`
	RecoveryReason        string     `json:"recovery_reason,omitempty"`
	LastReconciledAttempt int        `json:"last_reconciled_attempt"`
	SchemaVersion         int        `json:"schema_version"`
	CreatedAt             time.Time  `json:"created_at"`
	UpdatedAt             time.Time  `json:"updated_at"`
}

// Validate checks for structural and security validity of StoredCheckpoint.
func (c *StoredCheckpoint) Validate() error {
	if c == nil {
		return ErrInvalidCheckpoint
	}
	if strings.TrimSpace(c.CheckpointID) == "" {
		return fmt.Errorf("%w: checkpoint_id cannot be empty", ErrInvalidCheckpoint)
	}
	if strings.TrimSpace(c.NodeID) == "" {
		return fmt.Errorf("%w: node_id cannot be empty", ErrInvalidCheckpoint)
	}
	if strings.TrimSpace(c.State) == "" {
		return fmt.Errorf("%w: state cannot be empty", ErrInvalidCheckpoint)
	}
	if c.StartedAt.IsZero() {
		return fmt.Errorf("%w: started_at timestamp cannot be zero", ErrInvalidCheckpoint)
	}
	if c.SchemaVersion <= 0 {
		return fmt.Errorf("%w: schema_version must be positive", ErrInvalidCheckpoint)
	}

	// Security: reject credentials, tokens, or executable commands in recovery reason
	lowerReason := strings.ToLower(c.RecoveryReason)
	if strings.Contains(lowerReason, "password") || strings.Contains(lowerReason, "secret") ||
		strings.Contains(lowerReason, "bearer ") || strings.Contains(lowerReason, "token") {
		return fmt.Errorf("%w: sensitive credential pattern rejected in recovery reason", ErrInvalidCheckpoint)
	}
	if strings.Contains(lowerReason, "/bin/sh") || strings.Contains(lowerReason, "powershell") ||
		strings.Contains(lowerReason, "cmd.exe") || strings.Contains(lowerReason, "exec.command") {
		return fmt.Errorf("%w: executable command pattern rejected in recovery reason", ErrInvalidCheckpoint)
	}
	return nil
}

// CheckpointStore defines durable persistence for runtime recovery checkpoints.
type CheckpointStore interface {
	// SaveCheckpoint transactionally validates and persists or updates a runtime checkpoint.
	SaveCheckpoint(ctx context.Context, cp *StoredCheckpoint) error

	// GetLatestCheckpoint retrieves the most recent runtime checkpoint for a node.
	// Returns (nil, nil) if no checkpoint exists yet.
	GetLatestCheckpoint(ctx context.Context, nodeID string) (*StoredCheckpoint, error)

	// ListCheckpoints retrieves runtime checkpoints for a node ordered chronologically (created_at DESC), bounded by limit.
	ListCheckpoints(ctx context.Context, nodeID string, limit int) ([]*StoredCheckpoint, error)
}

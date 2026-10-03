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
	ErrDuplicateBatch              = errors.New("batch with the given batch_id already exists")
	ErrBatchNotFound               = errors.New("batch not found")
	ErrStoreClosed                 = errors.New("storage engine is closed")
	ErrInvalidBatch                = errors.New("cannot persist invalid telemetry batch")
	ErrDuplicateAnomaly            = errors.New("anomaly with the given anomaly_id already exists")
	ErrIncidentNotFound            = errors.New("incident not found")
	ErrInvalidIncident             = errors.New("cannot persist invalid incident")
	ErrDuplicateMitigation         = errors.New("mitigation with the given action_id or decision_id already exists")
	ErrMitigationNotFound          = errors.New("mitigation not found")
	ErrInvalidMitigation           = errors.New("cannot persist invalid mitigation record")
	ErrInvalidMitigationTransition = errors.New("invalid mitigation status transition")
	ErrDuplicateApproval           = errors.New("approval with the given decision_id or approval_id already exists")
	ErrApprovalNotFound            = errors.New("approval not found")
	ErrInvalidApproval             = errors.New("cannot persist invalid approval record")
	ErrInvalidApprovalTransition   = errors.New("invalid approval status transition")
	ErrApprovalExpired             = errors.New("approval has expired")
	ErrApprovalAlreadyConsumed     = errors.New("approval has already been consumed")
	ErrApprovalDecisionMismatch    = errors.New("approval decision binding mismatch")
	ErrForbiddenNotOverridable     = errors.New("forbidden actions cannot be approved or overridden")
	ErrCannotRejectApproved        = errors.New("cannot reject an already approved approval")
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

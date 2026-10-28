package storage

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// Common storage errors for the control-plane idempotency layer.
var (
	ErrDuplicateBatch    = errors.New("duplicate batch already recorded")
	ErrStoreClosed       = errors.New("idempotency store is closed")
	ErrInvalidMeta       = errors.New("invalid batch metadata")
	ErrBatchNotFound     = errors.New("batch not found in store")
	ErrCorruptPayload    = errors.New("corrupt telemetry payload")
	ErrNodeNotFound      = errors.New("node not found in registry")
	ErrCorruptNodeRecord = errors.New("corrupt node record")

	// Quarantine storage errors.
	ErrQuarantineNotFound      = errors.New("quarantine record not found")
	ErrAlreadyIngested         = errors.New("telemetry batch already durably ingested; skipping quarantine")
	ErrInvalidQuarantineRecord = errors.New("invalid quarantine record")
	ErrInvalidStatusTransition = errors.New("invalid quarantine status transition")
	ErrStatusConflict          = errors.New("quarantine status conflict")
	ErrPayloadTooLarge         = errors.New("quarantine payload exceeds maximum allowed size")
	ErrFailureReasonTooLong    = errors.New("quarantine failure reason exceeds maximum allowed length")
	ErrPayloadMismatch         = errors.New("quarantine record deduplication key exists with different payload")
	ErrSourceSequenceOverflow  = errors.New("source sequence exceeds maximum supported integer (math.MaxInt64)")
)

// PayloadConflictError is returned when attempting to record a quarantine entry with an existing
// deduplication key but different payload contents.
type PayloadConflictError struct {
	DeduplicationKey string
	ExistingSize     int
	IncomingSize     int
}

func (e *PayloadConflictError) Error() string {
	return fmt.Sprintf("payload mismatch for quarantine deduplication key %q: existing size %d bytes, incoming size %d bytes", e.DeduplicationKey, e.ExistingSize, e.IncomingSize)
}

func (e *PayloadConflictError) Unwrap() error {
	return ErrPayloadMismatch
}

// InvalidTransitionError captures details about an illegal status change.
type InvalidTransitionError struct {
	Current QuarantineStatus
	Target  QuarantineStatus
	Reason  string
}

func (e *InvalidTransitionError) Error() string {
	return fmt.Sprintf("cannot transition quarantine record from %q to %q: %s", e.Current, e.Target, e.Reason)
}

func (e *InvalidTransitionError) Unwrap() error {
	return ErrInvalidStatusTransition
}

// CorruptPayloadError captures diagnostic information about a corrupted stored payload.
type CorruptPayloadError struct {
	NodeID  string
	BatchID string
	Err     error
}

func (e *CorruptPayloadError) Error() string {
	return fmt.Sprintf("corrupt payload for node %q, batch %q: %v", e.NodeID, e.BatchID, e.Err)
}

func (e *CorruptPayloadError) Unwrap() error {
	return ErrCorruptPayload
}

// CorruptNodeRecordError captures diagnostic information about a corrupted stored node record.
type CorruptNodeRecordError struct {
	NodeID string
	Err    error
}

func (e *CorruptNodeRecordError) Error() string {
	return fmt.Sprintf("corrupt node record for node %q: %v", e.NodeID, e.Err)
}

func (e *CorruptNodeRecordError) Unwrap() error {
	return ErrCorruptNodeRecord
}

// BatchMeta holds minimal metadata for a telemetry batch.
type BatchMeta struct {
	NodeID         string    `json:"node_id"`
	BatchID        string    `json:"batch_id"`
	SequenceNumber int64     `json:"sequence_number"`
	CollectedAt    time.Time `json:"collected_at"`
	IngestedAt     time.Time `json:"ingested_at"`
}

// Validate ensures that all mandatory fields are present and valid.
func (m BatchMeta) Validate() error {
	if strings.TrimSpace(m.NodeID) == "" {
		return fmt.Errorf("%w: node_id cannot be empty", ErrInvalidMeta)
	}
	if strings.TrimSpace(m.BatchID) == "" {
		return fmt.Errorf("%w: batch_id cannot be empty", ErrInvalidMeta)
	}
	if m.SequenceNumber < 0 {
		return fmt.Errorf("%w: sequence_number cannot be negative", ErrInvalidMeta)
	}
	if m.CollectedAt.IsZero() {
		return fmt.Errorf("%w: collected_at cannot be zero", ErrInvalidMeta)
	}
	if m.IngestedAt.IsZero() {
		return fmt.Errorf("%w: ingested_at cannot be zero", ErrInvalidMeta)
	}
	return nil
}

// StoredBatch captures a persisted batch with its ingestion timestamp.
type StoredBatch struct {
	Batch      types.TelemetryBatch `json:"batch"`
	IngestedAt time.Time            `json:"ingested_at"`
}

// NodeRecord represents an edge node registered in the control plane.
type NodeRecord struct {
	NodeID            string            `json:"node_id"`
	Status            types.NodeStatus  `json:"status"`
	LastSeen          time.Time         `json:"last_seen"`
	RegisteredAt      time.Time         `json:"registered_at"`
	AgentVersion      string            `json:"agent_version,omitempty"`
	ProtocolVersion   string            `json:"protocol_version,omitempty"`
	Hostname          string            `json:"hostname,omitempty"`
	OS                string            `json:"os,omitempty"`
	Architecture      string            `json:"architecture,omitempty"`
	Capabilities      []string          `json:"capabilities,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	SequenceNumber    int64             `json:"sequence_number,omitempty"`
	HeartbeatSequence int64             `json:"heartbeat_sequence,omitempty"`
	MetadataVersion   int64             `json:"metadata_version,omitempty"`
	UpdatedAt         time.Time         `json:"updated_at,omitempty"`
}

// IdempotencyStore defines the contract for persistent control-plane deduplication, payload storage,
// and node registry management.
type IdempotencyStore interface {
	// RecordBatch atomically records the batch's idempotency metadata and complete payload,
	// and updates registered node activity within a single database transaction.
	// Returns (true, nil) if the batch was newly recorded.
	// Returns (false, nil) if the batch was already recorded (duplicate).
	// Returns (false, err) if a storage failure occurred and the transaction was rolled back.
	RecordBatch(ctx context.Context, batch *types.TelemetryBatch, ingestedAt time.Time) (bool, error)

	// HasBatch reports whether the batch identified by (nodeID, batchID) has been recorded.
	HasBatch(ctx context.Context, nodeID, batchID string) (bool, error)

	// GetPayload retrieves the full persisted batch payload by node ID and batch ID.
	GetPayload(ctx context.Context, nodeID, batchID string) (*types.TelemetryBatch, error)

	// GetRecentBatches retrieves up to limit recent persisted batches in chronological order (oldest to newest).
	GetRecentBatches(ctx context.Context, limit int) ([]*StoredBatch, error)

	// Count returns the total number of uniquely recorded batches.
	Count(ctx context.Context) (int, error)

	// RegisterNode idempotently enrolls a new node or updates mutable metadata on re-registration.
	// Returns (true, nil) if newly enrolled.
	// Returns (false, nil) if duplicate registration (with existing RegisteredAt and SequenceNumber preserved).
	RegisterNode(ctx context.Context, record *NodeRecord) (bool, error)

	// GetNode retrieves a registered node by node ID.
	// Returns (nil, ErrNodeNotFound) if the node does not exist.
	GetNode(ctx context.Context, nodeID string) (*NodeRecord, error)

	// ListNodes returns all registered nodes ordered deterministically by registered_at ASC, node_id ASC.
	ListNodes(ctx context.Context) ([]*NodeRecord, error)

	// UpdateNodeHeartbeat updates an existing node's status, last_seen, and advances sequence_number monotonically.
	// Returns ErrNodeNotFound if the node is not registered.
	UpdateNodeHeartbeat(ctx context.Context, nodeID string, status types.NodeStatus, seqNum int64, heartbeatTime time.Time) error

	// UpdateNodeTelemetryActivity advances last_seen and sequence_number monotonically for an existing node.
	// Returns (true, nil) if an existing node record was updated, (false, nil) if the node is not registered.
	UpdateNodeTelemetryActivity(ctx context.Context, nodeID string, seqNum int64, ingestedAt time.Time) (bool, error)

	// Ping verifies that the underlying storage is responsive and available.
	// Returns ErrStoreClosed if the store has been closed.
	// Returns context errors (e.g. context.Canceled, context.DeadlineExceeded) if ctx is done.
	Ping(ctx context.Context) error

	// Close flushes and releases underlying storage resources.
	Close() error
}

const (
	// MaxQuarantinePayloadSize bounds stored raw payload to 1 MB.
	MaxQuarantinePayloadSize = 1024 * 1024

	// MaxFailureReasonLength bounds diagnostic error text to 2048 characters.
	MaxFailureReasonLength = 2048

	// MaxStatusReasonLength bounds status change explanation to 1024 characters.
	MaxStatusReasonLength = 1024

	// MaxDeduplicationKeyLength bounds deduplication identity keys to 256 characters.
	MaxDeduplicationKeyLength = 256
)

// QuarantineStatus defines the operational lifecycle states of a quarantined batch.
type QuarantineStatus string

const (
	StatusQuarantined QuarantineStatus = "quarantined"
	StatusReplayed    QuarantineStatus = "replayed"
	StatusDiscarded   QuarantineStatus = "discarded"
)

// Valid returns true if the status is one of the recognized lifecycle states.
func (s QuarantineStatus) Valid() bool {
	switch s {
	case StatusQuarantined, StatusReplayed, StatusDiscarded:
		return true
	default:
		return false
	}
}

// IsTerminal returns true if the status represents a final, non-revertible state.
func (s QuarantineStatus) IsTerminal() bool {
	return s == StatusReplayed || s == StatusDiscarded
}

// IsValidQuarantineTransition evaluates if target status is allowed from current status.
func IsValidQuarantineTransition(current, target QuarantineStatus) bool {
	if !current.Valid() || !target.Valid() {
		return false
	}
	// Terminal states cannot transition to any other status
	if current.IsTerminal() {
		return false
	}
	// From quarantined, transitions to replayed or discarded are allowed
	if current == StatusQuarantined {
		return target == StatusReplayed || target == StatusDiscarded
	}
	return false
}

// QuarantineCategory classifies the underlying reason why telemetry ingestion failed.
type QuarantineCategory string

const (
	CategoryDeserialization   QuarantineCategory = "deserialization_error"
	CategorySchemaValidation  QuarantineCategory = "schema_validation"
	CategoryDeliveryExhausted QuarantineCategory = "delivery_exhausted"
	CategoryStorageFailure    QuarantineCategory = "storage_failure"
	CategoryUnknown           QuarantineCategory = "unknown"
)

// Valid returns true if the category is recognized.
func (c QuarantineCategory) Valid() bool {
	switch c {
	case CategoryDeserialization, CategorySchemaValidation, CategoryDeliveryExhausted, CategoryStorageFailure, CategoryUnknown:
		return true
	default:
		return false
	}
}

// QuarantineRecord encapsulates a failed or unprocessable telemetry batch persisted
// for operator diagnosis, post-mortem audit, and eventual replay.
type QuarantineRecord struct {
	ID                  string             `json:"id"`
	DeduplicationKey    string             `json:"deduplication_key"`
	SourceStream        string             `json:"source_stream,omitempty"`
	SourceSequence      *uint64            `json:"source_sequence,omitempty"`
	SourceSubject       string             `json:"source_subject,omitempty"`
	NodeID              string             `json:"node_id,omitempty"`
	BatchID             string             `json:"batch_id,omitempty"`
	Payload             []byte             `json:"payload"`
	FailureCategory     QuarantineCategory `json:"failure_category"`
	FailureReason       string             `json:"failure_reason"`
	LastFailureCategory QuarantineCategory `json:"last_failure_category,omitempty"`
	LastFailureReason   string             `json:"last_failure_reason,omitempty"`
	DeliveryCount       int                `json:"delivery_count"`
	Attempts            int                `json:"attempts"`
	FirstQuarantinedAt  time.Time          `json:"first_quarantined_at"`
	LastQuarantinedAt   time.Time          `json:"last_quarantined_at"`
	Status              QuarantineStatus   `json:"status"`
	StatusUpdatedAt     time.Time          `json:"status_updated_at"`
	StatusReason        string             `json:"status_reason,omitempty"`
	StatusUpdatedBy     string             `json:"status_updated_by,omitempty"`
	ReplayMetadata      map[string]any     `json:"replay_metadata,omitempty"`
}

// Validate verifies mandatory constraints and bounds on the quarantine record.
func (r *QuarantineRecord) Validate() error {
	if r == nil {
		return fmt.Errorf("%w: record cannot be nil", ErrInvalidQuarantineRecord)
	}
	if len(r.Payload) > MaxQuarantinePayloadSize {
		return fmt.Errorf("%w: payload exceeds maximum size %d bytes (got %d)", ErrPayloadTooLarge, MaxQuarantinePayloadSize, len(r.Payload))
	}
	if len(r.FailureReason) > MaxFailureReasonLength {
		return fmt.Errorf("%w: failure reason exceeds maximum length %d (got %d)", ErrFailureReasonTooLong, MaxFailureReasonLength, len(r.FailureReason))
	}
	if len(r.LastFailureReason) > MaxFailureReasonLength {
		return fmt.Errorf("%w: last failure reason exceeds maximum length %d (got %d)", ErrFailureReasonTooLong, MaxFailureReasonLength, len(r.LastFailureReason))
	}
	if r.DeliveryCount < 0 {
		return fmt.Errorf("%w: delivery_count cannot be negative", ErrInvalidQuarantineRecord)
	}
	if r.Attempts < 0 {
		return fmt.Errorf("%w: attempts cannot be negative", ErrInvalidQuarantineRecord)
	}
	if r.SourceSequence != nil && *r.SourceSequence > math.MaxInt64 {
		return fmt.Errorf("%w: %d exceeds maximum supported sequence (%d)", ErrSourceSequenceOverflow, *r.SourceSequence, int64(math.MaxInt64))
	}
	if r.FailureCategory != "" && !r.FailureCategory.Valid() {
		return fmt.Errorf("%w: invalid failure category %q", ErrInvalidQuarantineRecord, r.FailureCategory)
	}
	if r.LastFailureCategory != "" && !r.LastFailureCategory.Valid() {
		return fmt.Errorf("%w: invalid last failure category %q", ErrInvalidQuarantineRecord, r.LastFailureCategory)
	}
	if r.Status != "" && !r.Status.Valid() {
		return fmt.Errorf("%w: invalid status %q", ErrInvalidQuarantineRecord, r.Status)
	}
	if len(r.DeduplicationKey) > MaxDeduplicationKeyLength {
		return fmt.Errorf("%w: deduplication key exceeds maximum length %d", ErrInvalidQuarantineRecord, MaxDeduplicationKeyLength)
	}
	return nil
}

// QuarantineFilter defines criteria for paginated listing of quarantined records.
type QuarantineFilter struct {
	Status QuarantineStatus `json:"status,omitempty"`
	NodeID string           `json:"node_id,omitempty"`
	Limit  int              `json:"limit,omitempty"`
	Offset int              `json:"offset,omitempty"`
}

// QuarantineStatusUpdate specifies a validated state transition request.
type QuarantineStatusUpdate struct {
	ID             string           `json:"id"`
	ExpectedStatus QuarantineStatus `json:"expected_status,omitempty"`
	NewStatus      QuarantineStatus `json:"new_status"`
	Reason         string           `json:"reason,omitempty"`
	UpdatedBy      string           `json:"updated_by,omitempty"`
	Timestamp      time.Time        `json:"timestamp,omitempty"`
}

// QuarantineStore defines the minimal contract for persisting, retrieving,
// listing, and transitioning control-plane telemetry quarantine records.
type QuarantineStore interface {
	// RecordQuarantine persists a quarantine record idempotently.
	// If a record with the same deduplication identity already exists, it is updated
	// without creating a duplicate row, preserving terminal states.
	RecordQuarantine(ctx context.Context, record *QuarantineRecord) (*QuarantineRecord, error)

	// GetQuarantineRecord retrieves a quarantined record by its stable ID.
	// Returns (nil, ErrQuarantineNotFound) if not found.
	GetQuarantineRecord(ctx context.Context, id string) (*QuarantineRecord, error)

	// ListQuarantineRecords returns a deterministic paginated list of quarantine records,
	// optionally filtered by status or node ID, ordered by last_quarantined_at DESC, id ASC.
	ListQuarantineRecords(ctx context.Context, filter QuarantineFilter) ([]*QuarantineRecord, error)

	// UpdateQuarantineStatus updates a record's status using a validated, explicit transition.
	// Prevents overwriting terminal states and returns ErrInvalidStatusTransition or ErrStatusConflict.
	UpdateQuarantineStatus(ctx context.Context, update QuarantineStatusUpdate) (*QuarantineRecord, error)
}

// DeriveDeduplicationKey computes a deterministic, collision-resistant identity for quarantine deduplication.
// Priority:
// 1. Source stream + source sequence (when stream is non-empty and seq != nil)
// 2. Node ID + batch ID (length-prefixed to prevent delimiter collisions, e.g. "batch:<len1>:<node>:<len2>:<batch>")
// 3. Fallback: SHA-256 hash of subject + raw payload
func DeriveDeduplicationKey(stream string, seq *uint64, nodeID, batchID string, subject string, payload []byte) string {
	cleanStream := strings.TrimSpace(stream)
	if cleanStream != "" && seq != nil {
		return fmt.Sprintf("stream:%s:%d", cleanStream, *seq)
	}

	cleanNode := strings.TrimSpace(nodeID)
	cleanBatch := strings.TrimSpace(batchID)
	if cleanNode != "" && cleanBatch != "" {
		return fmt.Sprintf("batch:%d:%s:%d:%s", len(cleanNode), cleanNode, len(cleanBatch), cleanBatch)
	}

	h := sha256.New()
	h.Write([]byte(strings.TrimSpace(subject)))
	h.Write([]byte{0})
	h.Write(payload)
	return fmt.Sprintf("payload:sha256:%x", h.Sum(nil))
}

var secretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(bearer\s+)[A-Za-z0-9_\-\.]{8,}`),
	regexp.MustCompile(`(?i)(password\s*[:=]\s*)[^\s,;]+`),
	regexp.MustCompile(`(?i)(secret\s*[:=]\s*)[^\s,;]+`),
	regexp.MustCompile(`(?i)(token\s*[:=]\s*)[A-Za-z0-9_\-\.]{8,}`),
}

func redactSecrets(s string) string {
	for _, re := range secretPatterns {
		s = re.ReplaceAllString(s, "${1}[REDACTED]")
	}
	return s
}

// SanitizeFailureReason strips non-printable/control characters (except standard whitespace),
// redacts common credential patterns, and bounds length to MaxFailureReasonLength bytes while
// preserving valid UTF-8 encoding across truncation boundaries.
func SanitizeFailureReason(reason string) string {
	if reason == "" {
		return ""
	}

	// 0. Ensure base string contains valid UTF-8 code points
	validUTF8 := strings.ToValidUTF8(reason, "")

	// 1. Strip null bytes and control chars (preserve \n, \r, \t)
	var b strings.Builder
	b.Grow(len(validUTF8))
	for _, r := range validUTF8 {
		if r == 0 {
			continue
		}
		if r < 32 && r != '\n' && r != '\r' && r != '\t' {
			continue
		}
		b.WriteRune(r)
	}
	cleaned := b.String()

	// 2. Redact sensitive patterns (bearer tokens, passwords, secrets)
	cleaned = redactSecrets(cleaned)

	// 3. Bound length to MaxFailureReasonLength bytes while respecting UTF-8 rune boundaries.
	// We walk backward from the cutoff point until reaching a rune start byte, guaranteeing
	// that multi-byte Unicode runes are never split across the boundary.
	if len(cleaned) > MaxFailureReasonLength {
		target := MaxFailureReasonLength - len("...")
		for target > 0 && !utf8.RuneStart(cleaned[target]) {
			target--
		}
		cleaned = cleaned[:target] + "..."
	}
	return cleaned
}

// GenerateQuarantineID produces a cryptographically randomized, prefix-formatted ID.
func GenerateQuarantineID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("failed to generate quarantine id: %w", err)
	}
	return fmt.Sprintf("qrn-%x", b), nil
}

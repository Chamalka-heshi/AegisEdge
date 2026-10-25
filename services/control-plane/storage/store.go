package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

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
)

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

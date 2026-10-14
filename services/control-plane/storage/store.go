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
	ErrDuplicateBatch = errors.New("duplicate batch already recorded")
	ErrStoreClosed    = errors.New("idempotency store is closed")
	ErrInvalidMeta    = errors.New("invalid batch metadata")
	ErrBatchNotFound  = errors.New("batch not found in store")
)

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

// IdempotencyStore defines the contract for persistent control-plane deduplication and payload storage.
type IdempotencyStore interface {
	// RecordBatch atomically records the batch's idempotency metadata and complete payload
	// within a single database transaction.
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

	// Close flushes and releases underlying storage resources.
	Close() error
}

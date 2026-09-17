package storage

import (
	"context"
	"errors"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// Common storage domain errors.
var (
	ErrDuplicateBatch = errors.New("batch with the given batch_id already exists")
	ErrBatchNotFound  = errors.New("batch not found")
	ErrStoreClosed    = errors.New("storage engine is closed")
	ErrInvalidBatch   = errors.New("cannot persist invalid telemetry batch")
)

// SyncStatus represents the synchronization lifecycle state of a locally stored batch.
type SyncStatus string

const (
	SyncStatusPending SyncStatus = "PENDING"
	SyncStatusSyncing SyncStatus = "SYNCING"
	SyncStatusSynced  SyncStatus = "SYNCED"
	SyncStatusFailed  SyncStatus = "FAILED"
)

// IsValid checks whether the sync status is recognized.
func (s SyncStatus) IsValid() bool {
	switch s {
	case SyncStatusPending, SyncStatusSyncing, SyncStatusSynced, SyncStatusFailed:
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

	// GetLatestSequenceNumber returns the highest sequence number persisted for a given node.
	// If no records exist for the node, it returns -1.
	GetLatestSequenceNumber(ctx context.Context, nodeID string) (int64, error)

	// CountBatches returns the total count of telemetry batches stored locally.
	CountBatches(ctx context.Context) (int64, error)

	// Close cleanly terminates database connections and checkpoints the WAL journal.
	Close() error
}

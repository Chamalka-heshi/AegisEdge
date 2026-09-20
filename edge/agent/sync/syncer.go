package sync

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
)

// SyncStats captures telemetry metrics for a synchronization cycle.
type SyncStats struct {
	TotalSynced   int
	TotalFailed   int
	NodeSuccesses map[string]int
	NodeFailures  map[string]error
}

// Syncer coordinates durable local SQLite storage with the upstream synchronization client.
// It enforces per-node sequence ordering, node-level failure isolation, and retry attempt tracking.
type Syncer struct {
	store  storage.Store
	client Client
	logger *slog.Logger
}

// NewSyncer instantiates a new Syncer coordinator.
func NewSyncer(store storage.Store, client Client, logger *slog.Logger) *Syncer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Syncer{
		store:  store,
		client: client,
		logger: logger,
	}
}

// SyncPendingBatches executes a synchronization cycle across all pending nodes:
//
//  1. Discovers distinct nodes with pending batches via GetPendingNodes.
//  2. For each node, retrieves pending records ordered strictly by sequence_number ASC.
//  3. Transmits each batch to the control plane.
//  4. On success (accepted or already_accepted), transitions local status to SYNCED.
//  5. On failure (all transient HTTP retries exhausted for that batch):
//     - Calls RecordSyncAttempt ONCE (incrementing the logical attempt counter in SQLite).
//     - Leaves sync_status as PENDING (never deletes or marks FAILED).
//     - Halts sequence progression for THAT node (preserving strict per-node sequence order).
//     - Continues synchronizing other nodes independently (providing node failure isolation).
func (s *Syncer) SyncPendingBatches(ctx context.Context, limitPerNode int) (*SyncStats, error) {
	if limitPerNode <= 0 {
		limitPerNode = 50
	}

	nodes, err := s.store.GetPendingNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve pending nodes: %w", err)
	}

	stats := &SyncStats{
		NodeSuccesses: make(map[string]int),
		NodeFailures:  make(map[string]error),
	}

	for _, nodeID := range nodes {
		if ctx.Err() != nil {
			return stats, ctx.Err()
		}

		batches, err := s.store.GetPendingBatchesByNode(ctx, nodeID, limitPerNode)
		if err != nil {
			s.logger.Error("failed to query pending batches for node",
				slog.String("node_id", nodeID),
				slog.Any("error", err),
			)
			stats.NodeFailures[nodeID] = err
			continue
		}

		for _, rec := range batches {
			if ctx.Err() != nil {
				return stats, ctx.Err()
			}

			sentAt := time.Now().UTC()
			rec.Payload.SentAt = &sentAt

			result, err := s.client.SyncBatch(ctx, &rec.Payload)
			if err != nil {
				// Failed sync cycle for this batch.
				// Increment persisted logical attempt count exactly once for this failed cycle.
				s.logger.Warn("synchronization cycle failed for batch; recording attempt and halting node sequence",
					slog.String("node_id", nodeID),
					slog.String("batch_id", rec.BatchID),
					slog.Int64("sequence_number", rec.SequenceNumber),
					slog.Any("error", err),
				)

				if recErr := s.store.RecordSyncAttempt(ctx, rec.BatchID, sentAt); recErr != nil {
					s.logger.Error("failed to record sync attempt in local storage",
						slog.String("batch_id", rec.BatchID),
						slog.Any("error", recErr),
					)
				}

				stats.TotalFailed++
				stats.NodeFailures[nodeID] = err

				// Per-node failure isolation:
				// Halt subsequent batches for THIS node to preserve (NodeID, SequenceNumber) order,
				// but break out of the inner loop so the outer loop continues to other nodes!
				break
			}

			// Successful sync (status is "accepted" or "already_accepted")
			if markErr := s.store.MarkBatchSynced(ctx, rec.BatchID, sentAt); markErr != nil {
				s.logger.Error("failed to mark batch synced in storage",
					slog.String("batch_id", rec.BatchID),
					slog.Any("error", markErr),
				)
				stats.NodeFailures[nodeID] = markErr
				break
			}

			s.logger.Info("batch successfully synchronized to control plane",
				slog.String("node_id", nodeID),
				slog.String("batch_id", rec.BatchID),
				slog.Int64("sequence_number", rec.SequenceNumber),
				slog.String("status", result.Status),
				slog.Int("http_attempts", result.HTTPAttempts),
			)

			stats.TotalSynced++
			stats.NodeSuccesses[nodeID]++
		}
	}

	return stats, nil
}

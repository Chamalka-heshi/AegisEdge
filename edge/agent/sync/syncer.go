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

// Syncer coordinates durable local SQLite storage with the upstream synchronization transport.
// It enforces per-node sequence ordering, node-level failure isolation, and retry attempt tracking.
//
// Transport selection:
//   - When publisher is non-nil, batches are published via NATS JetStream (BatchPublisher).
//   - When publisher is nil and client is non-nil, batches are synced via HTTP (Client).
//   - Only one transport is active per synchronization cycle. There is no automatic fallback.
type Syncer struct {
	store     storage.Store
	client    Client
	publisher BatchPublisher
	logger    *slog.Logger
}

// NewSyncer instantiates a new Syncer coordinator with the HTTP transport.
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

// NewSyncerWithPublisher instantiates a new Syncer coordinator with the NATS transport.
func NewSyncerWithPublisher(store storage.Store, publisher BatchPublisher, logger *slog.Logger) *Syncer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Syncer{
		store:     store,
		publisher: publisher,
		logger:    logger,
	}
}

// SyncPendingBatches executes a synchronization cycle across all pending nodes:
//
//  1. Discovers distinct nodes with pending batches via GetPendingNodes.
//  2. For each node, retrieves pending records ordered strictly by sequence_number ASC.
//  3. Transmits each batch via the active transport (NATS or HTTP).
//  4. On success:
//     - NATS path: transitions local status to PUBLISHED (requires JetStream PubAck).
//     - HTTP path: transitions local status to SYNCED (requires HTTP 2xx).
//  5. On failure (all transient retries exhausted for that batch):
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

			var syncErr error

			if s.publisher != nil {
				// NATS transport path
				syncErr = s.publisher.PublishBatch(ctx, &rec.Payload)
				if syncErr == nil {
					// JetStream PubAck received — mark PUBLISHED
					if markErr := s.store.MarkBatchPublished(ctx, rec.BatchID, sentAt); markErr != nil {
						s.logger.Error("failed to mark batch published in storage",
							slog.String("batch_id", rec.BatchID),
							slog.Any("error", markErr),
						)
						stats.NodeFailures[nodeID] = markErr
						break
					}

					s.logger.Info("batch successfully published to JetStream",
						slog.String("node_id", nodeID),
						slog.String("batch_id", rec.BatchID),
						slog.Int64("sequence_number", rec.SequenceNumber),
					)
				}
			} else if s.client != nil {
				// HTTP transport path (existing Phase 3 behavior)
				rec.Payload.SentAt = &sentAt

				result, err := s.client.SyncBatch(ctx, &rec.Payload)
				syncErr = err
				if syncErr == nil {
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
				}
			} else {
				// No transport configured
				break
			}

			if syncErr != nil {
				// Failed sync cycle for this batch.
				// Increment persisted logical attempt count exactly once for this failed cycle.
				s.logger.Warn("synchronization cycle failed for batch; recording attempt and halting node sequence",
					slog.String("node_id", nodeID),
					slog.String("batch_id", rec.BatchID),
					slog.Int64("sequence_number", rec.SequenceNumber),
					slog.Any("error", syncErr),
				)

				if recErr := s.store.RecordSyncAttempt(ctx, rec.BatchID, sentAt); recErr != nil {
					s.logger.Error("failed to record sync attempt in local storage",
						slog.String("batch_id", rec.BatchID),
						slog.Any("error", recErr),
					)
				}

				stats.TotalFailed++
				stats.NodeFailures[nodeID] = syncErr

				// Per-node failure isolation:
				// Halt subsequent batches for THIS node to preserve (NodeID, SequenceNumber) order,
				// but break out of the inner loop so the outer loop continues to other nodes!
				break
			}

			stats.TotalSynced++
			stats.NodeSuccesses[nodeID]++
		}
	}

	return stats, nil
}

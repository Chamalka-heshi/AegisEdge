package sync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
)

// DefaultMaxSyncAttempts is the default number of failed logical synchronization cycles
// before an unrecoverable transient batch is quarantined to prevent head-of-line blocking.
const DefaultMaxSyncAttempts = 10

// SyncStats captures telemetry metrics for a synchronization cycle.
type SyncStats struct {
	TotalSynced      int
	TotalFailed      int
	TotalQuarantined int
	NodeSuccesses    map[string]int
	NodeFailures     map[string]error
	NodeQuarantines  map[string]int
}

// SyncerOption configures optional parameters on Syncer coordinator instances.
type SyncerOption func(*Syncer)

// WithMaxSyncAttempts overrides the retry threshold (in logical sync cycles) before quarantine.
func WithMaxSyncAttempts(maxAttempts int) SyncerOption {
	return func(s *Syncer) {
		if maxAttempts > 0 {
			s.maxAttempts = maxAttempts
		}
	}
}

// WithAuditStore injects an audit store for durable recording of quarantine and requeue events.
func WithAuditStore(as storage.AuditStore) SyncerOption {
	return func(s *Syncer) {
		s.auditStore = as
	}
}

// Syncer coordinates durable local SQLite storage with the upstream synchronization transport.
// It enforces per-node sequence ordering, node-level failure isolation, and retry attempt tracking.
//
// Transport selection:
//   - When publisher is non-nil, batches are published via NATS JetStream (BatchPublisher).
//   - When publisher is nil and client is non-nil, batches are synced via HTTP (Client).
//   - Only one transport is active per synchronization cycle. There is no automatic fallback.
type Syncer struct {
	store       storage.Store
	client      Client
	publisher   BatchPublisher
	logger      *slog.Logger
	maxAttempts int
	auditStore  storage.AuditStore
}

// NewSyncer instantiates a new Syncer coordinator with the HTTP transport.
func NewSyncer(store storage.Store, client Client, logger *slog.Logger, opts ...SyncerOption) *Syncer {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Syncer{
		store:       store,
		client:      client,
		logger:      logger,
		maxAttempts: DefaultMaxSyncAttempts,
	}
	if as, ok := store.(storage.AuditStore); ok {
		s.auditStore = as
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// NewSyncerWithPublisher instantiates a new Syncer coordinator with the NATS transport.
func NewSyncerWithPublisher(store storage.Store, publisher BatchPublisher, logger *slog.Logger, opts ...SyncerOption) *Syncer {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Syncer{
		store:       store,
		publisher:   publisher,
		logger:      logger,
		maxAttempts: DefaultMaxSyncAttempts,
	}
	if as, ok := store.(storage.AuditStore); ok {
		s.auditStore = as
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// isPermanentSyncError classifies whether an error returned by the transport is permanent and non-retryable.
// Permanent failures include:
//   - HTTP 4xx validation failures (ErrClientValidation: 400 Bad Request, 405 Method Not Allowed, 413 Payload Too Large).
//   - Invalid NATS subject/node ID characters (ErrInvalidNodeID).
//
// Unknown errors default to transient unless proven permanent.
func isPermanentSyncError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrClientValidation) {
		return true
	}
	if errors.Is(err, ErrInvalidNodeID) {
		return true
	}
	return false
}

// SyncPendingBatches executes a synchronization cycle across all pending nodes:
//
//  1. Discovers distinct nodes with pending batches via GetPendingNodes.
//  2. For each node, retrieves pending records ordered strictly by sequence_number ASC.
//  3. Transmits each batch via the active transport (NATS or HTTP).
//  4. On success:
//     - NATS path: transitions local status to PUBLISHED (requires JetStream PubAck).
//     - HTTP path: transitions local status to SYNCED (requires HTTP 2xx).
//  5. On failure:
//     - If parent context is cancelled/expired: terminates promptly without recording attempt or quarantining.
//     - If permanent failure OR retry threshold exhausted: transitions batch to FAILED (quarantine),
//     emits an audit event, and continues to subsequent batches for that node (preventing head-of-line blocking).
//     - If transient failure below threshold: increments logical attempt counter in SQLite, preserves PENDING status,
//     and halts sequence progression for THAT node during this cycle.
//  6. Preserves node failure isolation: other nodes continue processing independently.
func (s *Syncer) SyncPendingBatches(ctx context.Context, limitPerNode int) (*SyncStats, error) {
	if limitPerNode <= 0 {
		limitPerNode = 50
	}

	nodes, err := s.store.GetPendingNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve pending nodes: %w", err)
	}

	stats := &SyncStats{
		NodeSuccesses:   make(map[string]int),
		NodeFailures:    make(map[string]error),
		NodeQuarantines: make(map[string]int),
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
				// Check if parent context was cancelled or expired
				if ctx.Err() != nil {
					return stats, ctx.Err()
				}

				isPermanent := isPermanentSyncError(syncErr)
				isExhausted := (rec.Attempt + 1) >= s.maxAttempts

				if isPermanent || isExhausted {
					var reason string
					if isPermanent {
						reason = fmt.Sprintf("permanent sync failure: %v", syncErr)
					} else {
						reason = fmt.Sprintf("sync retry threshold (%d logical cycles) exhausted: %v", s.maxAttempts, syncErr)
					}

					// 1. Persist batch as FAILED using MarkBatchQuarantined
					if qErr := s.store.MarkBatchQuarantined(ctx, rec.BatchID, reason, sentAt); qErr != nil {
						s.logger.Error("failed to mark batch quarantined; halting node sequence",
							slog.String("node_id", nodeID),
							slog.String("batch_id", rec.BatchID),
							slog.Any("error", qErr),
						)
						stats.NodeFailures[nodeID] = fmt.Errorf("quarantine persistence failed: %w", qErr)
						stats.TotalFailed++
						break // Safe stop: never proceed if quarantine persistence failed
					}

					// 2. Increment quarantine statistics after quarantine transition succeeds
					stats.TotalQuarantined++
					stats.NodeQuarantines[nodeID]++
					s.logger.Warn("batch quarantined due to unrecoverable failure",
						slog.String("node_id", nodeID),
						slog.String("batch_id", rec.BatchID),
						slog.Int64("sequence_number", rec.SequenceNumber),
						slog.Bool("permanent", isPermanent),
						slog.Bool("exhausted", isExhausted),
						slog.String("reason", reason),
					)

					// 3. Record audit event
					if s.auditStore != nil {
						auditEvt := &storage.StoredAuditEvent{
							EventID:    fmt.Sprintf("audit-quarantine-%s-%d", rec.BatchID, sentAt.UnixNano()),
							EventType:  "TELEMETRY_BATCH_QUARANTINED",
							Timestamp:  sentAt,
							NodeID:     rec.NodeID,
							IncidentID: fmt.Sprintf("quarantine-%s", rec.BatchID),
							Actor:      "syncer",
							Result:     "quarantined",
							Reason:     reason,
							Metadata: map[string]string{
								"batch_id":        rec.BatchID,
								"sequence_number": fmt.Sprintf("%d", rec.SequenceNumber),
								"attempts":        fmt.Sprintf("%d", rec.Attempt+1),
								"permanent":       fmt.Sprintf("%t", isPermanent),
							},
						}
						if auditErr := s.auditStore.RecordAuditEvent(ctx, auditEvt); auditErr != nil {
							s.logger.Error("failed to record audit event for quarantined batch",
								slog.String("batch_id", rec.BatchID),
								slog.Any("error", auditErr),
							)
						}
					}

					// 4. Continue to next batch for THIS node in the same cycle!
					// Head-of-line blocking is prevented since this poison batch is quarantined.
					continue
				}

				// Transient failure below retry threshold:
				// Increment persisted logical attempt count exactly once for this failed cycle.
				s.logger.Warn("synchronization cycle failed for batch; recording attempt and halting node sequence",
					slog.String("node_id", nodeID),
					slog.String("batch_id", rec.BatchID),
					slog.Int64("sequence_number", rec.SequenceNumber),
					slog.Int("attempt", rec.Attempt+1),
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

// RequeueBatch resets a quarantined batch in storage back to PENDING, clearing its failure metadata
// and resetting its attempt counter to 0 so it can be retried in subsequent sync cycles.
// If an audit store is configured, it emits an audit event for the requeue operation.
func (s *Syncer) RequeueBatch(ctx context.Context, batchID string) error {
	if err := s.store.RequeueQuarantinedBatch(ctx, batchID); err != nil {
		return err
	}

	if s.auditStore != nil {
		nodeID := "unknown"
		var seqNum int64 = -1
		if rec, err := s.store.GetBatch(ctx, batchID); err == nil && rec != nil {
			nodeID = rec.NodeID
			seqNum = rec.SequenceNumber
		}

		now := time.Now().UTC()
		auditEvt := &storage.StoredAuditEvent{
			EventID:    fmt.Sprintf("audit-requeue-%s-%d", batchID, now.UnixNano()),
			EventType:  "TELEMETRY_BATCH_REQUEUED",
			Timestamp:  now,
			NodeID:     nodeID,
			IncidentID: fmt.Sprintf("requeue-%s", batchID),
			Actor:      "operator",
			Result:     "requeued",
			Reason:     "operator requested manual requeue of quarantined telemetry batch",
			Metadata: map[string]string{
				"batch_id":        batchID,
				"sequence_number": fmt.Sprintf("%d", seqNum),
			},
		}
		if auditErr := s.auditStore.RecordAuditEvent(ctx, auditEvt); auditErr != nil {
			s.logger.Error("failed to record audit event for requeued batch",
				slog.String("batch_id", batchID),
				slog.Any("error", auditErr),
			)
		}
	}

	return nil
}

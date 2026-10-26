package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Chamalka-heshi/AegisEdge/services/control-plane/server"
	"github.com/Chamalka-heshi/AegisEdge/services/control-plane/storage"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// Consumer errors.
var (
	ErrConsumerClosed = errors.New("consumer is closed")
)

// IngestionHandler defines the contract for processing ingested telemetry batches.
// Implementations must be idempotent with respect to BatchID.
type IngestionHandler interface {
	// IngestBatch processes a validated telemetry batch.
	// Returns ErrDuplicateBatch if the batch has already been ingested (idempotent no-op).
	// Returns nil on successful ingestion.
	// Returns other errors on processing failure.
	IngestBatch(ctx context.Context, batch *types.TelemetryBatch) error
}

// Default consumer tuning parameters
const (
	DefaultMaxDeliver    = 5
	DefaultAckWait       = 30 * time.Second
	DefaultMaxAckPending = 1000
	DefaultRetryBackoff  = 100 * time.Millisecond
)

// ConsumerConfig holds configuration for the telemetry consumer.
type ConsumerConfig struct {
	NATSURL         string
	StreamName      string
	ConsumerName    string
	MaxDeliver      int                     // Maximum delivery attempts for a message before broker advisory (default: 5)
	AckWait         time.Duration           // Server acknowledgment timeout before redelivery (default: 30s)
	MaxAckPending   int                     // Max outstanding unacknowledged messages (default: 1000)
	RetryBackoff    time.Duration           // Base backoff delay for transient retries (default: 100ms)
	QuarantineStore storage.QuarantineStore // Durable quarantine store foundation
}

// recoveryItem encapsulates an exhausted message queued for background recovery persistence.
type recoveryItem struct {
	streamSeq     uint64
	deliveryCount int
	category      storage.QuarantineCategory
	reason        string
	subject       string
	nodeID        string
	batchID       string
	data          []byte
}

// ConsumerOption configures TelemetryConsumer instances.
type ConsumerOption func(*TelemetryConsumer)

// WithQuarantineStore provides an explicit quarantine store implementation.
func WithQuarantineStore(qs storage.QuarantineStore) ConsumerOption {
	return func(c *TelemetryConsumer) {
		c.quarantineStore = qs
	}
}

// TelemetryConsumer subscribes to the AEGISEDGE_TELEMETRY JetStream stream
// and processes telemetry events with at-least-once delivery, durable SQLite-backed
// idempotency, and poison-message containment.
//
// Reliability & Failure Semantics:
//   - Valid event processed successfully → ACK (committed to SQLite WAL before ACK)
//   - Duplicate batch (already ingested) → ACK (idempotent no-op; no duplicate DB rows)
//   - Permanent validation failure (malformed JSON, invalid envelope, invalid batch schema)
//     → Persisted to durable SQLite quarantine store, then terminated via TermWithReason
//     to prevent infinite redelivery loops.
//   - Transient failure (database locked, storage I/O, network) → Delayed NAK (NakWithDelay)
//     with exponential backoff, up to MaxDeliver attempts.
//   - Exhausted transient deliveries (>= MaxDeliver) → Persisted to durable SQLite quarantine
//     store as CategoryDeliveryExhausted, exhausted metric incremented, NAKed to trigger broker
//     max-deliveries advisory.
//   - Quarantine persistence failure at MaxDeliver → Submitted to asynchronous recovery worker;
//     unacknowledged sequence remains inspectable from stream and is recovered on restart.
//   - Process shutdown / context cancellation → Leave message unacknowledged so it can
//     be retried by the next control-plane process instance.
type TelemetryConsumer struct {
	conn                *nats.Conn
	handler             IngestionHandler
	logger              *slog.Logger
	config              ConsumerConfig
	js                  jetstream.JetStream
	stream              jetstream.Stream
	cons                jetstream.Consumer
	quarantineStore     storage.QuarantineStore
	ownsStore           bool
	recoveryQueue       chan recoveryItem
	recoveryWg          sync.WaitGroup
	ctx                 context.Context
	cancel              context.CancelFunc
	running             atomic.Bool
	consumed            atomic.Int64
	exhaustedDeliveries atomic.Int64
	quarantinedTotal    atomic.Int64
	recoveredTotal      atomic.Int64
}

// NewTelemetryConsumer creates a new consumer connected to the NATS JetStream stream.
func NewTelemetryConsumer(cfg ConsumerConfig, handler IngestionHandler, logger *slog.Logger, opts ...ConsumerOption) (*TelemetryConsumer, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.StreamName == "" {
		cfg.StreamName = "AEGISEDGE_TELEMETRY"
	}
	if cfg.ConsumerName == "" {
		cfg.ConsumerName = "control-plane-telemetry"
	}
	if cfg.MaxDeliver <= 0 {
		cfg.MaxDeliver = DefaultMaxDeliver
	}
	if cfg.AckWait <= 0 {
		cfg.AckWait = DefaultAckWait
	}
	if cfg.MaxAckPending <= 0 {
		cfg.MaxAckPending = DefaultMaxAckPending
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = DefaultRetryBackoff
	}

	nc, err := nats.Connect(cfg.NATSURL,
		nats.Name("aegisedge-consumer"),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(60),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	stream, err := js.Stream(context.Background(), cfg.StreamName)
	if err != nil {
		// Attempt to create stream if not existing
		stream, err = js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
			Name:      cfg.StreamName,
			Subjects:  []string{"aegisedge.v1.telemetry.*"},
			Retention: jetstream.LimitsPolicy,
		})
		if err != nil {
			nc.Close()
			return nil, fmt.Errorf("failed to get or create JetStream stream: %w", err)
		}
	}

	// Create or get durable consumer on the telemetry stream with explicit delivery bounds
	cons, err := js.CreateOrUpdateConsumer(context.Background(), cfg.StreamName, jetstream.ConsumerConfig{
		Durable:       cfg.ConsumerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: "aegisedge.v1.telemetry.*",
		MaxDeliver:    cfg.MaxDeliver,
		AckWait:       cfg.AckWait,
		MaxAckPending: cfg.MaxAckPending,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream consumer: %w", err)
	}

	var qs storage.QuarantineStore = cfg.QuarantineStore
	var ownsStore bool
	if qs == nil {
		memStore, err := storage.OpenSQLite(":memory:")
		if err != nil {
			nc.Close()
			return nil, fmt.Errorf("failed to initialize default in-memory quarantine store: %w", err)
		}
		qs = memStore
		ownsStore = true
	}

	ctx, cancel := context.WithCancel(context.Background())

	tc := &TelemetryConsumer{
		conn:            nc,
		handler:         handler,
		logger:          logger,
		config:          cfg,
		js:              js,
		stream:          stream,
		cons:            cons,
		quarantineStore: qs,
		ownsStore:       ownsStore,
		recoveryQueue:   make(chan recoveryItem, 256),
		ctx:             ctx,
		cancel:          cancel,
	}

	for _, opt := range opts {
		opt(tc)
	}

	logger.Info("NATS JetStream consumer initialized",
		slog.String("stream", cfg.StreamName),
		slog.String("consumer", cfg.ConsumerName),
		slog.Int("max_deliver", cfg.MaxDeliver),
		slog.Duration("ack_wait", cfg.AckWait),
		slog.Int("max_ack_pending", cfg.MaxAckPending),
	)

	return tc, nil
}

// Start begins consuming messages in the background.
func (c *TelemetryConsumer) Start() error {
	if c.running.Load() {
		return nil
	}
	c.running.Store(true)

	// Run recovery on any unacknowledged delivered messages from prior run
	go func() {
		if count, err := c.RecoverUnacknowledged(c.ctx); err == nil && count > 0 {
			c.logger.Info("recovered unacknowledged messages on startup", slog.Int("recovered_count", count))
		}
	}()

	c.recoveryWg.Add(1)
	go c.recoveryWorkerLoop()

	go c.consumeLoop()
	return nil
}

// consumeLoop continuously fetches and processes messages.
func (c *TelemetryConsumer) consumeLoop() {
	c.logger.Info("telemetry consumer started")
	defer c.running.Store(false)

	for {
		if c.ctx.Err() != nil {
			return
		}

		msgs, err := c.cons.Fetch(10, jetstream.FetchMaxWait(2*time.Second))
		if err != nil {
			if c.ctx.Err() != nil {
				return
			}
			// Transient fetch errors (e.g., timeout) are expected
			continue
		}

		for msg := range msgs.Messages() {
			if c.ctx.Err() != nil {
				return
			}
			c.processMessage(msg)
		}

		if msgs.Error() != nil && c.ctx.Err() == nil {
			// Log non-context errors
			if !errors.Is(msgs.Error(), context.Canceled) && !errors.Is(msgs.Error(), context.DeadlineExceeded) {
				c.logger.Warn("fetch cycle error", slog.Any("error", msgs.Error()))
			}
		}
	}
}

// payloadPreview returns a bounded preview of payload bytes for safe diagnostic logging.
func payloadPreview(data []byte) string {
	const maxLen = 128
	if len(data) <= maxLen {
		return string(data)
	}
	return string(data[:maxLen]) + "...(truncated)"
}

// termMessage terminates a permanently poisoned message to prevent infinite redelivery loops.
// Uses TermWithReason if supported, falling back to Term.
func termMessage(msg jetstream.Msg, reason string) {
	if err := msg.TermWithReason(reason); err != nil {
		_ = msg.Term()
	}
}

// extractNodeIDFromSubject extracts the trailing node identifier from a standard telemetry subject.
func extractNodeIDFromSubject(subject string) string {
	const prefix = "aegisedge.v1.telemetry."
	if strings.HasPrefix(subject, prefix) {
		return strings.TrimPrefix(subject, prefix)
	}
	return ""
}

// recordQuarantine creates and idempotently persists a quarantine record.
func (c *TelemetryConsumer) recordQuarantine(ctx context.Context, msg jetstream.Msg, stream string, seq *uint64, nodeID, batchID string, category storage.QuarantineCategory, reason string, deliveryCount int) error {
	if c.quarantineStore == nil {
		return errors.New("quarantine store not configured")
	}

	data := msg.Data()
	subject := msg.Subject()
	dedupKey := storage.DeriveDeduplicationKey(stream, seq, nodeID, batchID, subject, data)
	qid, err := storage.GenerateQuarantineID()
	if err != nil {
		return fmt.Errorf("generate quarantine id failed: %w", err)
	}

	now := time.Now().UTC()
	record := &storage.QuarantineRecord{
		ID:                 qid,
		DeduplicationKey:   dedupKey,
		SourceStream:       stream,
		SourceSequence:     seq,
		SourceSubject:      subject,
		NodeID:             nodeID,
		BatchID:            batchID,
		Payload:            data,
		FailureCategory:    category,
		FailureReason:      storage.SanitizeFailureReason(reason),
		DeliveryCount:      deliveryCount,
		Attempts:           1,
		FirstQuarantinedAt: now,
		LastQuarantinedAt:  now,
		Status:             storage.StatusQuarantined,
		StatusUpdatedAt:    now,
	}

	_, err = c.quarantineStore.RecordQuarantine(ctx, record)
	return err
}

// enqueueRecovery enqueues an exhausted delivery message for asynchronous recovery retry.
func (c *TelemetryConsumer) enqueueRecovery(seq *uint64, deliveryCount int, category storage.QuarantineCategory, reason, subject, nodeID, batchID string, data []byte) {
	if seq == nil {
		return
	}
	item := recoveryItem{
		streamSeq:     *seq,
		deliveryCount: deliveryCount,
		category:      category,
		reason:        reason,
		subject:       subject,
		nodeID:        nodeID,
		batchID:       batchID,
		data:          data,
	}
	select {
	case c.recoveryQueue <- item:
		c.logger.Info("enqueued exhausted message for asynchronous recovery",
			slog.Uint64("stream_seq", *seq),
			slog.String("subject", subject),
		)
	default:
		c.logger.Error("recovery queue full; unable to enqueue exhausted message",
			slog.Uint64("stream_seq", *seq),
			slog.String("subject", subject),
		)
	}
}

// recoveryWorkerLoop processes asynchronously enqueued exhausted messages.
func (c *TelemetryConsumer) recoveryWorkerLoop() {
	defer c.recoveryWg.Done()
	c.logger.Info("telemetry recovery worker started")

	for {
		select {
		case <-c.ctx.Done():
			c.logger.Info("telemetry recovery worker stopping on context cancellation")
			return
		case item, ok := <-c.recoveryQueue:
			if !ok {
				return
			}
			c.retryRecoverItem(item)
		}
	}
}

// retryRecoverItem attempts to persist a failed quarantine record with bounded backoff.
func (c *TelemetryConsumer) retryRecoverItem(item recoveryItem) {
	backoff := 50 * time.Millisecond
	const maxBackoff = 2 * time.Second

	for {
		if c.ctx.Err() != nil {
			return
		}

		dedupKey := storage.DeriveDeduplicationKey(c.config.StreamName, &item.streamSeq, item.nodeID, item.batchID, item.subject, item.data)
		qid, _ := storage.GenerateQuarantineID()
		now := time.Now().UTC()
		record := &storage.QuarantineRecord{
			ID:                 qid,
			DeduplicationKey:   dedupKey,
			SourceStream:       c.config.StreamName,
			SourceSequence:     &item.streamSeq,
			SourceSubject:      item.subject,
			NodeID:             item.nodeID,
			BatchID:            item.batchID,
			Payload:            item.data,
			FailureCategory:    item.category,
			FailureReason:      storage.SanitizeFailureReason(item.reason),
			DeliveryCount:      item.deliveryCount,
			Attempts:           1,
			FirstQuarantinedAt: now,
			LastQuarantinedAt:  now,
			Status:             storage.StatusQuarantined,
			StatusUpdatedAt:    now,
		}

		if c.quarantineStore != nil {
			_, err := c.quarantineStore.RecordQuarantine(c.ctx, record)
			if err == nil {
				c.quarantinedTotal.Add(1)
				c.recoveredTotal.Add(1)
				c.logger.Info("exhausted message successfully recovered into durable quarantine",
					slog.Uint64("stream_seq", item.streamSeq),
					slog.String("subject", item.subject),
				)
				return
			}
			if errors.Is(err, storage.ErrPayloadMismatch) {
				c.logger.Error("CRITICAL: recovery aborted due to payload mismatch conflict",
					slog.Uint64("stream_seq", item.streamSeq),
					slog.Any("error", err),
				)
				return
			}
		}

		select {
		case <-c.ctx.Done():
			return
		case <-time.After(backoff):
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// processMessage handles a single JetStream message with explicit permanent vs transient error discrimination.
func (c *TelemetryConsumer) processMessage(msg jetstream.Msg) {
	// Pre-check: if consumer is shutting down, leave message unacknowledged for the next instance
	if c.ctx.Err() != nil {
		return
	}

	meta, _ := msg.Metadata()
	deliveryCount := uint64(1)
	var streamSeq *uint64
	streamName := c.config.StreamName
	if meta != nil {
		deliveryCount = meta.NumDelivered
		seq := meta.Sequence.Stream
		streamSeq = &seq
		if meta.Stream != "" {
			streamName = meta.Stream
		}
	}

	// handlePermanentFailure coordinates logging, quarantine persistence, and conditional termination
	handlePermanentFailure := func(category storage.QuarantineCategory, failureErr error, reasonPrefix string, nodeID, batchID string) {
		reason := reasonPrefix
		if failureErr != nil {
			reason = fmt.Sprintf("%s: %v", reasonPrefix, failureErr)
		}
		c.logger.Error("permanent telemetry event failure; quarantining message",
			slog.String("stream", streamName),
			slog.String("consumer", c.config.ConsumerName),
			slog.String("subject", msg.Subject()),
			slog.String("node_id", nodeID),
			slog.String("batch_id", batchID),
			slog.Uint64("delivery_count", deliveryCount),
			slog.Int("payload_bytes", len(msg.Data())),
			slog.String("failure_category", string(category)),
			slog.Any("error", failureErr),
		)

		err := c.recordQuarantine(c.ctx, msg, streamName, streamSeq, nodeID, batchID, category, reason, int(deliveryCount))
		if err == nil {
			c.quarantinedTotal.Add(1)
			termMessage(msg, reason)
			return
		}

		// Handle payload conflict: if ErrPayloadMismatch, log integrity conflict and do not overwrite or term
		if errors.Is(err, storage.ErrPayloadMismatch) {
			c.logger.Error("CRITICAL: quarantine payload conflict detected; deduplication identity already bound to differing payload",
				slog.String("subject", msg.Subject()),
				slog.Any("error", err),
			)
			_ = msg.Nak()
			return
		}

		// Quarantine storage write failed (transient DB error)
		c.logger.Error("failed to record quarantine for permanent failure",
			slog.String("subject", msg.Subject()),
			slog.Uint64("delivery_count", deliveryCount),
			slog.Any("error", err),
		)

		if deliveryCount < uint64(c.config.MaxDeliver) {
			backoffMultiplier := time.Duration(1 << (deliveryCount - 1))
			delay := c.config.RetryBackoff * backoffMultiplier
			if delay > 5*time.Second {
				delay = 5 * time.Second
			}
			_ = msg.NakWithDelay(delay)
			return
		}

		// Delivery limit reached and quarantine write failed
		c.exhaustedDeliveries.Add(1)
		c.enqueueRecovery(streamSeq, int(deliveryCount), category, reason, msg.Subject(), nodeID, batchID, msg.Data())
		_ = msg.Nak()
	}

	// 1. Decode event envelope
	var event types.TelemetryEvent
	if err := json.Unmarshal(msg.Data(), &event); err != nil {
		nodeID := extractNodeIDFromSubject(msg.Subject())
		var batchID string
		if msg.Headers() != nil {
			batchID = msg.Headers().Get("Nats-Msg-Id")
		}
		handlePermanentFailure(storage.CategoryDeserialization, err, "malformed_json", nodeID, batchID)
		return
	}

	// 2. Validate envelope schema
	if err := event.Validate(); err != nil {
		nodeID := event.SourceNodeID
		if nodeID == "" {
			nodeID = extractNodeIDFromSubject(msg.Subject())
		}
		handlePermanentFailure(storage.CategorySchemaValidation, err, "invalid_envelope", nodeID, "")
		return
	}

	// 3. Validate batch domain payload presence and schema
	if event.Payload == nil {
		handlePermanentFailure(storage.CategorySchemaValidation, nil, "nil_payload", event.SourceNodeID, "")
		return
	}

	if err := event.Payload.Validate(); err != nil {
		handlePermanentFailure(storage.CategorySchemaValidation, err, "invalid_domain_payload", event.Payload.NodeID, event.Payload.BatchID)
		return
	}

	// 4. Pre-ingestion shutdown check
	if c.ctx.Err() != nil {
		return
	}

	// 5. Ingest through idempotent storage boundary
	err := c.handler.IngestBatch(c.ctx, event.Payload)

	// Post-ingestion race check: if context was canceled during ingestion, do NOT NAK or ACK or Term
	if c.ctx.Err() != nil || errors.Is(err, context.Canceled) {
		c.logger.Info("ingestion interrupted by consumer shutdown; leaving message unacknowledged",
			slog.String("event_id", event.EventID),
			slog.String("subject", msg.Subject()),
		)
		return
	}

	if err != nil {
		// Idempotent duplicate: already ingested previously
		if errors.Is(err, server.ErrDuplicateBatch) {
			c.logger.Info("duplicate telemetry event acknowledged (idempotent)",
				slog.String("event_id", event.EventID),
				slog.String("node_id", event.SourceNodeID),
				slog.String("batch_id", event.Payload.BatchID),
			)
			_ = msg.Ack()
			c.consumed.Add(1)
			return
		}

		// Permanent invalid metadata error from storage
		if errors.Is(err, storage.ErrInvalidMeta) {
			handlePermanentFailure(storage.CategorySchemaValidation, err, "invalid_storage_meta", event.Payload.NodeID, event.Payload.BatchID)
			return
		}

		// Transient error (SQLite busy/locked, storage I/O, server reload error)
		if deliveryCount >= uint64(c.config.MaxDeliver) {
			// Delivery limit reached: JetStream server will cease redelivering this message to this consumer group
			// and publish an advisory to $JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.<STREAM>.<CONSUMER>.
			c.exhaustedDeliveries.Add(1)
			reason := fmt.Sprintf("delivery limit reached (%d/%d): storage failure: %v", deliveryCount, c.config.MaxDeliver, err)
			qErr := c.recordQuarantine(c.ctx, msg, streamName, streamSeq, event.SourceNodeID, event.Payload.BatchID, storage.CategoryDeliveryExhausted, reason, int(deliveryCount))
			if qErr == nil {
				c.quarantinedTotal.Add(1)
				c.logger.Error("telemetry event delivery attempts exhausted on transient failure; durably quarantined",
					slog.String("stream", streamName),
					slog.String("consumer", c.config.ConsumerName),
					slog.String("subject", msg.Subject()),
					slog.String("event_id", event.EventID),
					slog.String("batch_id", event.Payload.BatchID),
					slog.String("node_id", event.SourceNodeID),
					slog.Uint64("delivery_count", deliveryCount),
					slog.Int("max_deliver", c.config.MaxDeliver),
					slog.String("failure_category", "delivery_exhausted"),
					slog.Any("error", err),
				)
				_ = msg.Nak()
				return
			}

			// Quarantine write failed at MaxDeliver
			c.logger.Error("CRITICAL: telemetry event delivery attempts exhausted and quarantine persistence failed",
				slog.String("subject", msg.Subject()),
				slog.Uint64("delivery_count", deliveryCount),
				slog.Any("storage_error", err),
				slog.Any("quarantine_error", qErr),
			)
			c.enqueueRecovery(streamSeq, int(deliveryCount), storage.CategoryDeliveryExhausted, reason, msg.Subject(), event.SourceNodeID, event.Payload.BatchID, msg.Data())
			_ = msg.Nak()
			return
		}

		// Transient failure under delivery limit: apply bounded exponential backoff
		backoffMultiplier := time.Duration(1 << (deliveryCount - 1))
		delay := c.config.RetryBackoff * backoffMultiplier
		if delay > 5*time.Second {
			delay = 5 * time.Second
		}

		c.logger.Warn("telemetry event processing failed; scheduling delayed retry",
			slog.String("event_id", event.EventID),
			slog.String("batch_id", event.Payload.BatchID),
			slog.String("node_id", event.SourceNodeID),
			slog.Uint64("delivery_count", deliveryCount),
			slog.Int("max_deliver", c.config.MaxDeliver),
			slog.Duration("retry_delay", delay),
			slog.String("failure_category", "transient_error"),
			slog.Any("error", err),
		)
		_ = msg.NakWithDelay(delay)
		return
	}

	// 6. Success: ACK only after durable ingestion
	_ = msg.Ack()
	c.consumed.Add(1)

	c.logger.Info("telemetry event processed and acknowledged",
		slog.String("event_id", event.EventID),
		slog.String("node_id", event.SourceNodeID),
		slog.String("batch_id", event.Payload.BatchID),
		slog.Int64("sequence_number", event.SequenceNumber),
	)
}

// Consumed returns the total number of messages successfully consumed and ACKed.
func (c *TelemetryConsumer) Consumed() int64 {
	return c.consumed.Load()
}

// ExhaustedDeliveries returns the total count of transient delivery exhaustion events observed
// by this consumer instance during its lifetime.
func (c *TelemetryConsumer) ExhaustedDeliveries() int64 {
	return c.exhaustedDeliveries.Load()
}

// Quarantined returns the total number of messages recorded into quarantine by this consumer instance.
func (c *TelemetryConsumer) Quarantined() int64 {
	return c.quarantinedTotal.Load()
}

// Recovered returns the total number of messages recovered through the exhaustion recovery mechanism.
func (c *TelemetryConsumer) Recovered() int64 {
	return c.recoveredTotal.Load()
}

// QuarantineStore returns the quarantine store backing this consumer.
func (c *TelemetryConsumer) QuarantineStore() storage.QuarantineStore {
	return c.quarantineStore
}

// Stream returns the underlying JetStream stream handle.
func (c *TelemetryConsumer) Stream() jetstream.Stream {
	return c.stream
}

// Consumer returns the underlying JetStream consumer handle.
func (c *TelemetryConsumer) Consumer() jetstream.Consumer {
	return c.cons
}

// RecoverStreamSequence retrieves the raw message directly from JetStream by stream sequence number,
// inspects it, and durably persists it to the quarantine store.
func (c *TelemetryConsumer) RecoverStreamSequence(ctx context.Context, seq uint64) (*storage.QuarantineRecord, error) {
	if c.stream == nil {
		return nil, errors.New("stream handle not available")
	}
	if c.quarantineStore == nil {
		return nil, errors.New("quarantine store not configured")
	}

	rawMsg, err := c.stream.GetMsg(ctx, seq)
	if err != nil {
		return nil, fmt.Errorf("failed to get stream message %d: %w", seq, err)
	}

	var event types.TelemetryEvent
	cat := storage.CategoryDeliveryExhausted
	reason := "recovered from stream after delivery exhaustion"
	var nodeID, batchID string

	if err := json.Unmarshal(rawMsg.Data, &event); err != nil {
		cat = storage.CategoryDeserialization
		reason = fmt.Sprintf("malformed_json: %v", err)
		nodeID = extractNodeIDFromSubject(rawMsg.Subject)
		if rawMsg.Header != nil {
			batchID = rawMsg.Header.Get("Nats-Msg-Id")
		}
	} else if err := event.Validate(); err != nil {
		cat = storage.CategorySchemaValidation
		reason = fmt.Sprintf("invalid_envelope: %v", err)
		nodeID = event.SourceNodeID
		if nodeID == "" {
			nodeID = extractNodeIDFromSubject(rawMsg.Subject)
		}
	} else if event.Payload == nil {
		cat = storage.CategorySchemaValidation
		reason = "nil_payload"
		nodeID = event.SourceNodeID
	} else if err := event.Payload.Validate(); err != nil {
		cat = storage.CategorySchemaValidation
		reason = fmt.Sprintf("invalid_domain_payload: %v", err)
		nodeID = event.Payload.NodeID
		batchID = event.Payload.BatchID
	} else {
		nodeID = event.SourceNodeID
		if event.Payload != nil {
			batchID = event.Payload.BatchID
		}
	}

	dedupKey := storage.DeriveDeduplicationKey(c.config.StreamName, &seq, nodeID, batchID, rawMsg.Subject, rawMsg.Data)
	qid, err := storage.GenerateQuarantineID()
	if err != nil {
		return nil, fmt.Errorf("generate quarantine id failed: %w", err)
	}

	now := time.Now().UTC()
	record := &storage.QuarantineRecord{
		ID:                 qid,
		DeduplicationKey:   dedupKey,
		SourceStream:       c.config.StreamName,
		SourceSequence:     &seq,
		SourceSubject:      rawMsg.Subject,
		NodeID:             nodeID,
		BatchID:            batchID,
		Payload:            rawMsg.Data,
		FailureCategory:    cat,
		FailureReason:      storage.SanitizeFailureReason(reason),
		DeliveryCount:      c.config.MaxDeliver,
		Attempts:           1,
		FirstQuarantinedAt: now,
		LastQuarantinedAt:  now,
		Status:             storage.StatusQuarantined,
		StatusUpdatedAt:    now,
	}

	saved, err := c.quarantineStore.RecordQuarantine(ctx, record)
	if err != nil {
		return nil, err
	}
	c.quarantinedTotal.Add(1)
	c.recoveredTotal.Add(1)
	return saved, nil
}

// RecoverUnacknowledged inspects the consumer's pending unacknowledged message sequence range
// and recovers each into durable quarantine. This ensures delivery-exhausted messages that failed
// quarantine persistence across a previous process restart are reliably recovered.
func (c *TelemetryConsumer) RecoverUnacknowledged(ctx context.Context) (int, error) {
	if c.cons == nil {
		return 0, errors.New("consumer handle not available")
	}

	info, err := c.cons.Info(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get consumer info: %w", err)
	}

	if info.NumAckPending == 0 || info.Delivered.Stream <= info.AckFloor.Stream {
		return 0, nil
	}

	recovered := 0
	for seq := info.AckFloor.Stream + 1; seq <= info.Delivered.Stream; seq++ {
		if ctx.Err() != nil {
			return recovered, ctx.Err()
		}
		_, err := c.RecoverStreamSequence(ctx, seq)
		if err != nil {
			c.logger.Warn("recovery of unacknowledged stream message failed",
				slog.Uint64("stream_seq", seq),
				slog.Any("error", err),
			)
			continue
		}
		recovered++
	}
	return recovered, nil
}

// Stop gracefully stops the consumer.
func (c *TelemetryConsumer) Stop() {
	c.cancel()
	c.recoveryWg.Wait()
	if c.conn != nil {
		c.conn.Close()
	}
	if c.ownsStore && c.quarantineStore != nil {
		if closer, ok := c.quarantineStore.(io.Closer); ok {
			_ = closer.Close()
		}
	}
	c.logger.Info("telemetry consumer stopped")
}

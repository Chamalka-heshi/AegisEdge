package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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
	NATSURL       string
	StreamName    string
	ConsumerName  string
	MaxDeliver    int           // Maximum delivery attempts for a message before broker advisory (default: 5)
	AckWait       time.Duration // Server acknowledgment timeout before redelivery (default: 30s)
	MaxAckPending int           // Max outstanding unacknowledged messages (default: 1000)
	RetryBackoff  time.Duration // Base backoff delay for transient retries (default: 100ms)
}

// TelemetryConsumer subscribes to the AEGISEDGE_TELEMETRY JetStream stream
// and processes telemetry events with at-least-once delivery, durable SQLite-backed
// idempotency, and poison-message containment.
//
// Reliability & Failure Semantics:
//   - Valid event processed successfully → ACK (committed to SQLite WAL before ACK)
//   - Duplicate batch (already ingested) → ACK (idempotent no-op; no duplicate DB rows)
//   - Permanent validation failure (malformed JSON, invalid envelope, invalid batch schema)
//     → Terminated via TermWithReason to prevent infinite redelivery loops. The raw message
//     remains durably preserved in the AEGISEDGE_TELEMETRY stream for forensic audit.
//   - Transient failure (database locked, storage I/O, network) → Delayed NAK (NakWithDelay)
//     with exponential backoff, up to MaxDeliver attempts.
//   - Exhausted transient deliveries (>= MaxDeliver) → JetStream server halts redelivery
//     and emits a consumer advisory. The message remains preserved in the stream on disk.
//   - Process shutdown / context cancellation → Leave message unacknowledged so it can
//     be retried by the next control-plane process instance.
type TelemetryConsumer struct {
	conn     *nats.Conn
	handler  IngestionHandler
	logger   *slog.Logger
	config   ConsumerConfig
	cons     jetstream.Consumer
	ctx      context.Context
	cancel   context.CancelFunc
	running  atomic.Bool
	consumed atomic.Int64
}

// NewTelemetryConsumer creates a new consumer connected to the NATS JetStream stream.
func NewTelemetryConsumer(cfg ConsumerConfig, handler IngestionHandler, logger *slog.Logger) (*TelemetryConsumer, error) {
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

	ctx, cancel := context.WithCancel(context.Background())

	logger.Info("NATS JetStream consumer initialized",
		slog.String("stream", cfg.StreamName),
		slog.String("consumer", cfg.ConsumerName),
		slog.Int("max_deliver", cfg.MaxDeliver),
		slog.Duration("ack_wait", cfg.AckWait),
		slog.Int("max_ack_pending", cfg.MaxAckPending),
	)

	return &TelemetryConsumer{
		conn:    nc,
		handler: handler,
		logger:  logger,
		config:  cfg,
		cons:    cons,
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

// Start begins consuming messages in the background.
func (c *TelemetryConsumer) Start() error {
	if c.running.Load() {
		return nil
	}
	c.running.Store(true)

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

// processMessage handles a single JetStream message with explicit permanent vs transient error discrimination.
func (c *TelemetryConsumer) processMessage(msg jetstream.Msg) {
	// Pre-check: if consumer is shutting down, leave message unacknowledged for the next instance
	if c.ctx.Err() != nil {
		return
	}

	meta, _ := msg.Metadata()
	deliveryCount := uint64(1)
	if meta != nil {
		deliveryCount = meta.NumDelivered
	}

	// 1. Decode event envelope
	var event types.TelemetryEvent
	if err := json.Unmarshal(msg.Data(), &event); err != nil {
		c.logger.Error("malformed telemetry event envelope (permanent failure, terminating message)",
			slog.String("stream", c.config.StreamName),
			slog.String("consumer", c.config.ConsumerName),
			slog.String("subject", msg.Subject()),
			slog.Uint64("delivery_count", deliveryCount),
			slog.Int("payload_bytes", len(msg.Data())),
			slog.String("payload_preview", payloadPreview(msg.Data())),
			slog.String("failure_category", "malformed_json"),
			slog.Any("error", err),
		)
		termMessage(msg, fmt.Sprintf("malformed_json: %v", err))
		return
	}

	// 2. Validate envelope schema
	if err := event.Validate(); err != nil {
		c.logger.Error("telemetry event envelope validation failed (permanent failure, terminating message)",
			slog.String("stream", c.config.StreamName),
			slog.String("consumer", c.config.ConsumerName),
			slog.String("subject", msg.Subject()),
			slog.String("event_id", event.EventID),
			slog.String("node_id", event.SourceNodeID),
			slog.Uint64("delivery_count", deliveryCount),
			slog.Int("payload_bytes", len(msg.Data())),
			slog.String("failure_category", "invalid_envelope"),
			slog.Any("error", err),
		)
		termMessage(msg, fmt.Sprintf("invalid_envelope: %v", err))
		return
	}

	// 3. Validate batch domain payload presence and schema
	if event.Payload == nil {
		c.logger.Error("telemetry event missing batch payload (permanent failure, terminating message)",
			slog.String("stream", c.config.StreamName),
			slog.String("consumer", c.config.ConsumerName),
			slog.String("subject", msg.Subject()),
			slog.String("event_id", event.EventID),
			slog.String("node_id", event.SourceNodeID),
			slog.Uint64("delivery_count", deliveryCount),
			slog.String("failure_category", "nil_payload"),
		)
		termMessage(msg, "nil_payload")
		return
	}

	if err := event.Payload.Validate(); err != nil {
		c.logger.Error("telemetry batch domain validation failed (permanent failure, terminating message)",
			slog.String("stream", c.config.StreamName),
			slog.String("consumer", c.config.ConsumerName),
			slog.String("subject", msg.Subject()),
			slog.String("event_id", event.EventID),
			slog.String("batch_id", event.Payload.BatchID),
			slog.String("node_id", event.Payload.NodeID),
			slog.Uint64("delivery_count", deliveryCount),
			slog.String("failure_category", "invalid_domain_payload"),
			slog.Any("error", err),
		)
		termMessage(msg, fmt.Sprintf("invalid_domain_payload: %v", err))
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

		// Check for permanent invalid metadata error from storage
		if errors.Is(err, storage.ErrInvalidMeta) {
			c.logger.Error("batch rejected by storage as invalid metadata (permanent failure, terminating message)",
				slog.String("stream", c.config.StreamName),
				slog.String("consumer", c.config.ConsumerName),
				slog.String("event_id", event.EventID),
				slog.String("batch_id", event.Payload.BatchID),
				slog.Uint64("delivery_count", deliveryCount),
				slog.String("failure_category", "invalid_storage_meta"),
				slog.Any("error", err),
			)
			termMessage(msg, fmt.Sprintf("invalid_storage_meta: %v", err))
			return
		}

		// Transient error (SQLite busy/locked, storage I/O, server reload error)
		if deliveryCount >= uint64(c.config.MaxDeliver) {
			// Delivery limit reached: JetStream server will cease redelivering this message to this consumer group
			// and publish an advisory to $JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.<STREAM>.<CONSUMER>.
			// CRITICAL: We do NOT Terminate it so the message remains intact in the stream for operator inspection/replay.
			c.logger.Error("CRITICAL: telemetry event delivery attempts exhausted on transient failure",
				slog.String("stream", c.config.StreamName),
				slog.String("consumer", c.config.ConsumerName),
				slog.String("subject", msg.Subject()),
				slog.String("event_id", event.EventID),
				slog.String("batch_id", event.Payload.BatchID),
				slog.String("node_id", event.SourceNodeID),
				slog.Uint64("delivery_count", deliveryCount),
				slog.Int("max_deliver", c.config.MaxDeliver),
				slog.String("failure_category", "exhausted_delivery"),
				slog.Any("error", err),
			)
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

// Stop gracefully stops the consumer.
func (c *TelemetryConsumer) Stop() {
	c.cancel()
	if c.conn != nil {
		c.conn.Close()
	}
	c.logger.Info("telemetry consumer stopped")
}

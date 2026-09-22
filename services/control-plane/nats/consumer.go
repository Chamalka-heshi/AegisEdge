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

// ConsumerConfig holds configuration for the telemetry consumer.
type ConsumerConfig struct {
	NATSURL      string
	StreamName   string
	ConsumerName string
}

// TelemetryConsumer subscribes to the AEGISEDGE_TELEMETRY JetStream stream
// and processes telemetry events with at-least-once delivery and idempotent processing.
//
// ACK semantics:
//   - Valid event processed successfully → ACK
//   - Duplicate batch (already ingested) → ACK (idempotent)
//   - Processing failure → NAK (JetStream may redeliver)
//   - Invalid envelope or payload → NAK (no dead-letter in this phase)
//
// Limitation (Phase 4.3):
// Duplicate detection is idempotent during the lifetime of the current control-plane process.
// Persistent duplicate detection across control-plane restarts is NOT provided by the
// current in-memory ingestion boundary and is deferred to a future persistent ingestion layer.
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

	// Create or get durable consumer on the telemetry stream
	cons, err := js.CreateOrUpdateConsumer(context.Background(), cfg.StreamName, jetstream.ConsumerConfig{
		Durable:       cfg.ConsumerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: "aegisedge.v1.telemetry.*",
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream consumer: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	logger.Info("NATS JetStream consumer initialized",
		slog.String("stream", cfg.StreamName),
		slog.String("consumer", cfg.ConsumerName),
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

// processMessage handles a single JetStream message.
func (c *TelemetryConsumer) processMessage(msg jetstream.Msg) {
	// 1. Decode event envelope
	var event types.TelemetryEvent
	if err := json.Unmarshal(msg.Data(), &event); err != nil {
		c.logger.Error("failed to decode telemetry event envelope",
			slog.Any("error", err),
			slog.String("subject", msg.Subject()),
		)
		_ = msg.Nak()
		return
	}

	// 2. Validate envelope
	if err := event.Validate(); err != nil {
		c.logger.Error("telemetry event envelope validation failed",
			slog.String("event_id", event.EventID),
			slog.Any("error", err),
		)
		_ = msg.Nak()
		return
	}

	// 3. Process through idempotent ingestion boundary
	err := c.handler.IngestBatch(c.ctx, event.Payload)
	if err != nil {
		if errors.Is(err, server.ErrDuplicateBatch) {
			// Duplicate delivery — no duplicate logical insertion — ACK
			c.logger.Info("duplicate telemetry event acknowledged (idempotent)",
				slog.String("event_id", event.EventID),
				slog.String("node_id", event.SourceNodeID),
			)
			_ = msg.Ack()
			c.consumed.Add(1)
			return
		}

		// Processing failure — NO ACK — allow JetStream redelivery
		c.logger.Error("telemetry event processing failed; not acknowledging",
			slog.String("event_id", event.EventID),
			slog.Any("error", err),
		)
		_ = msg.Nak()
		return
	}

	// 4. Success — ACK only after successful processing
	_ = msg.Ack()
	c.consumed.Add(1)

	c.logger.Info("telemetry event processed and acknowledged",
		slog.String("event_id", event.EventID),
		slog.String("node_id", event.SourceNodeID),
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

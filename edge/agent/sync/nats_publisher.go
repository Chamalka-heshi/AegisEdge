package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// NATS publisher errors.
var (
	ErrNATSNotConnected = errors.New("NATS connection is not established")
	ErrInvalidNodeID    = errors.New("node_id contains invalid NATS subject characters")
	ErrPublishNack      = errors.New("JetStream did not acknowledge publication")
)

// NATSPublisherConfig holds configuration for the NATS publisher.
type NATSPublisherConfig struct {
	URL            string
	PublishTimeout time.Duration
}

// NATSPublisher implements BatchPublisher using NATS JetStream.
//
// It publishes telemetry events to subjects of the form:
//
//	aegisedge.v1.telemetry.<node_id>
//
// Publication is confirmed only after receiving a JetStream PubAck.
// The NATS Msg-Id header is set to BatchID for broker-level deduplication.
type NATSPublisher struct {
	conn   *nats.Conn
	js     jetstream.JetStream
	config NATSPublisherConfig
	logger *slog.Logger
}

// NewNATSPublisher establishes a reusable NATS connection and JetStream context.
// The connection is reused across publish calls — a new connection is NOT created per batch.
func NewNATSPublisher(cfg NATSPublisherConfig, logger *slog.Logger) (*NATSPublisher, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.PublishTimeout <= 0 {
		cfg.PublishTimeout = 5 * time.Second
	}

	nc, err := nats.Connect(cfg.URL,
		nats.Name("aegisedge-publisher"),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(60),
		nats.ReconnectWait(2*time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				logger.Warn("NATS disconnected", slog.Any("error", err))
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Info("NATS reconnected", slog.String("url", nc.ConnectedUrl()))
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	logger.Info("NATS JetStream publisher initialized",
		slog.String("url", cfg.URL),
		slog.Duration("publish_timeout", cfg.PublishTimeout),
	)

	return &NATSPublisher{
		conn:   nc,
		js:     js,
		config: cfg,
		logger: logger,
	}, nil
}

// PublishBatch publishes an already-persisted TelemetryBatch to NATS JetStream.
//
// The method:
//  1. Validates the subject derived from batch.NodeID.
//  2. Constructs a TelemetryEvent envelope (EventID = BatchID).
//  3. Serializes the envelope to JSON.
//  4. Publishes with Nats-Msg-Id = BatchID for broker-level deduplication.
//  5. Waits for JetStream PubAck.
//  6. Returns nil ONLY after confirmed PubAck.
//
// A retry reuses the exact same BatchID/EventID — no new event ID is generated.
func (p *NATSPublisher) PublishBatch(ctx context.Context, batch *types.TelemetryBatch) error {
	if p.conn == nil || p.conn.IsClosed() {
		return ErrNATSNotConnected
	}

	// 1. Derive and validate subject
	subject, err := buildSubject(batch.NodeID)
	if err != nil {
		return err
	}

	// 2. Construct event envelope
	publishedAt := time.Now().UTC()
	event := types.NewTelemetryEvent(batch, publishedAt)

	// 3. Serialize
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal telemetry event: %w", err)
	}

	// 4. Build NATS message with dedup header
	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{},
	}
	msg.Header.Set("Nats-Msg-Id", batch.BatchID)

	// 5. Publish and wait for JetStream PubAck
	pubCtx, cancel := context.WithTimeout(ctx, p.config.PublishTimeout)
	defer cancel()

	ack, err := p.js.PublishMsg(pubCtx, msg)
	if err != nil {
		return fmt.Errorf("JetStream publish failed: %w", err)
	}

	if ack == nil {
		return ErrPublishNack
	}

	p.logger.Info("telemetry batch published to JetStream",
		slog.String("batch_id", batch.BatchID),
		slog.String("node_id", batch.NodeID),
		slog.String("subject", subject),
		slog.Uint64("stream_seq", ack.Sequence),
		slog.Bool("duplicate", ack.Duplicate),
	)

	return nil
}

// Close drains and closes the NATS connection cleanly.
func (p *NATSPublisher) Close() error {
	if p.conn == nil {
		return nil
	}
	if err := p.conn.Drain(); err != nil {
		p.logger.Warn("NATS drain error during close", slog.Any("error", err))
	}
	p.conn.Close()
	p.logger.Info("NATS publisher closed")
	return nil
}

// buildSubject constructs the NATS subject for a given NodeID.
// Returns an error if the NodeID contains characters that are invalid in NATS subjects.
func buildSubject(nodeID string) (string, error) {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return "", fmt.Errorf("%w: empty node_id", ErrInvalidNodeID)
	}
	// NATS subject tokens cannot contain: space, '.', '>', '*'
	if strings.ContainsAny(nodeID, " .>*") {
		return "", fmt.Errorf("%w: %q contains prohibited characters (space, '.', '>', '*')", ErrInvalidNodeID, nodeID)
	}
	return fmt.Sprintf("aegisedge.v1.telemetry.%s", nodeID), nil
}

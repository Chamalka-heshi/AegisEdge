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
	ErrConsumerClosed  = errors.New("consumer is closed")
	ErrAlreadyIngested = errors.New("telemetry batch already durably ingested; skipping quarantine")
	ErrNotEligible     = errors.New("message is valid and has not reached delivery exhaustion; skipping quarantine")
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

// IdempotencyChecker queries whether a batch has already been committed to durable storage.
type IdempotencyChecker interface {
	HasBatch(ctx context.Context, nodeID, batchID string) (bool, error)
}

// Default consumer tuning parameters
const (
	DefaultMaxDeliver           = 5
	DefaultAckWait              = 30 * time.Second
	DefaultMaxAckPending        = 1000
	DefaultRetryBackoff         = 100 * time.Millisecond
	DefaultMaxRecoveryBatch     = 100
	DefaultRecoveryScanInterval = 500 * time.Millisecond
)

// ConsumerConfig holds configuration for the telemetry consumer.
type ConsumerConfig struct {
	NATSURL              string
	StreamName           string
	ConsumerName         string
	AdvisoryStreamName   string                  // Dedicated JetStream stream capturing max-delivery advisories (default: "<StreamName>_ADVISORIES")
	AdvisoryConsumerName string                  // Durable consumer name on the advisory stream (default: "<ConsumerName>-advisories")
	MaxDeliver           int                     // Maximum delivery attempts for a message before broker advisory (default: 5)
	AckWait              time.Duration           // Server acknowledgment timeout before redelivery (default: 30s)
	MaxAckPending        int                     // Max outstanding unacknowledged messages (default: 1000)
	RetryBackoff         time.Duration           // Base backoff delay for transient retries (default: 100ms)
	QuarantineStore      storage.QuarantineStore // Durable quarantine store foundation
	IdempotencyStore     IdempotencyChecker      // Authoritative idempotency store for checking already-ingested batches
	MaxRecoveryBatch     int                     // Maximum sequences inspected per recovery pass (default: 100)
	RecoveryScanInterval time.Duration           // Interval between recovery passes (default: 500ms)
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
	attempts      int
	lastAttempt   time.Time
}

// scanRetryItem encapsulates a stream sequence that encountered a transient recovery error.
type scanRetryItem struct {
	seq         uint64
	attempts    int
	lastAttempt time.Time
	lastErr     error
}

// MaxDeliverAdvisory represents the canonical NATS JetStream broker delivery-exhaustion advisory payload.
type MaxDeliverAdvisory struct {
	Type       string    `json:"type"`
	ID         string    `json:"id,omitempty"`
	Timestamp  time.Time `json:"timestamp,omitempty"`
	Stream     string    `json:"stream"`
	Consumer   string    `json:"consumer"`
	StreamSeq  uint64    `json:"stream_seq"`
	Deliveries uint64    `json:"deliveries"`
}

// recoveryOptions specifies modifiers for recovery calls.
type recoveryOptions struct {
	forceExhaustion bool
}

// RecoveryOption configures recovery operations.
type RecoveryOption func(*recoveryOptions)

// WithExhaustionEvidence indicates that the caller has external/advisory proof that this sequence exceeded MaxDeliver.
func WithExhaustionEvidence() RecoveryOption {
	return func(o *recoveryOptions) {
		o.forceExhaustion = true
	}
}

// ConsumerOption configures TelemetryConsumer instances.
type ConsumerOption func(*TelemetryConsumer)

// WithAdvisoryStreamName sets the stream name for capturing broker delivery exhaustion advisories.
func WithAdvisoryStreamName(name string) ConsumerOption {
	return func(c *TelemetryConsumer) {
		if name != "" {
			c.config.AdvisoryStreamName = name
		}
	}
}

// WithAdvisoryConsumerName sets the durable consumer name on the advisory stream.
func WithAdvisoryConsumerName(name string) ConsumerOption {
	return func(c *TelemetryConsumer) {
		if name != "" {
			c.config.AdvisoryConsumerName = name
		}
	}
}

// WithQuarantineStore provides an explicit quarantine store implementation.
func WithQuarantineStore(qs storage.QuarantineStore) ConsumerOption {
	return func(c *TelemetryConsumer) {
		c.quarantineStore = qs
	}
}

// WithIdempotencyStore provides an explicit authoritative idempotency store implementation.
func WithIdempotencyStore(is IdempotencyChecker) ConsumerOption {
	return func(c *TelemetryConsumer) {
		c.idempotencyStore = is
	}
}

// WithMaxRecoveryBatch sets the maximum sequences inspected per recovery pass.
func WithMaxRecoveryBatch(batchSize int) ConsumerOption {
	return func(c *TelemetryConsumer) {
		if batchSize > 0 {
			c.config.MaxRecoveryBatch = batchSize
		}
	}
}

// WithRecoveryScanInterval sets the interval between background recovery passes.
func WithRecoveryScanInterval(interval time.Duration) ConsumerOption {
	return func(c *TelemetryConsumer) {
		if interval > 0 {
			c.config.RecoveryScanInterval = interval
		}
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
	conn                   *nats.Conn
	handler                IngestionHandler
	logger                 *slog.Logger
	config                 ConsumerConfig
	js                     jetstream.JetStream
	stream                 jetstream.Stream
	cons                   jetstream.Consumer
	advStream              jetstream.Stream
	advCons                jetstream.Consumer
	advSub                 *nats.Subscription
	quarantineStore        storage.QuarantineStore
	idempotencyStore       IdempotencyChecker
	ownsStore              bool
	recoveryMu             sync.Mutex
	pendingExhausted       map[uint64]*recoveryItem
	pendingScanRetries     map[uint64]*scanRetryItem
	exhaustedSeqs          map[uint64]bool
	recoveryCursor         uint64
	recoveryWg             sync.WaitGroup
	ctx                    context.Context
	cancel                 context.CancelFunc
	running                atomic.Bool
	stopped                atomic.Bool
	consumed               atomic.Int64
	exhaustedDeliveries    atomic.Int64
	quarantinedTotal       atomic.Int64
	recoveredTotal         atomic.Int64
	advisoryStreamMsgs     atomic.Int64
	advisoryStreamBytes    atomic.Int64
	advisoryPendingAck     atomic.Int64
	advisoryPendingBacklog atomic.Int64
	advisoryFetchErrors    atomic.Int64
	advisoryEvictions      atomic.Int64
	lastAdvisoryFirstSeq   uint64
	lastHealthInspect      time.Time
}

// natsSubjectCovers reports whether pattern covers target subject pattern.
func natsSubjectCovers(pattern, target string) bool {
	if pattern == target || pattern == ">" {
		return true
	}
	pTokens := strings.Split(pattern, ".")
	tTokens := strings.Split(target, ".")
	for i, pTok := range pTokens {
		if pTok == ">" {
			return true
		}
		if i >= len(tTokens) {
			return false
		}
		if pTok != "*" && pTok != tTokens[i] {
			return false
		}
	}
	return len(pTokens) == len(tTokens)
}

// validateAdvisoryStreamConfig validates that an existing advisory stream has compatible configuration.
func validateAdvisoryStreamConfig(streamName string, cfg jetstream.StreamConfig, requiredSubjectFilter string) error {
	covered := false
	for _, subj := range cfg.Subjects {
		if natsSubjectCovers(subj, requiredSubjectFilter) {
			covered = true
			break
		}
	}
	if !covered {
		return fmt.Errorf("incompatible advisory stream %q: subjects %v do not cover required subject pattern %q",
			streamName, cfg.Subjects, requiredSubjectFilter)
	}

	if cfg.Retention != jetstream.LimitsPolicy {
		return fmt.Errorf("incompatible advisory stream %q: retention policy is %s, expected LimitsPolicy",
			streamName, cfg.Retention.String())
	}

	if cfg.Discard != jetstream.DiscardOld {
		return fmt.Errorf("incompatible advisory stream %q: discard policy is %s, expected DiscardOld",
			streamName, cfg.Discard.String())
	}

	return nil
}

// validateAdvisoryConsumerConfig validates that an existing advisory consumer has compatible configuration.
func validateAdvisoryConsumerConfig(consumerName string, cfg jetstream.ConsumerConfig, expectedFilterSubject string) error {
	if cfg.AckPolicy != jetstream.AckExplicitPolicy {
		return fmt.Errorf("incompatible advisory consumer %q: ack policy is %s, expected AckExplicit",
			consumerName, cfg.AckPolicy.String())
	}

	if cfg.FilterSubject != "" && cfg.FilterSubject != expectedFilterSubject {
		return fmt.Errorf("incompatible advisory consumer %q: filter subject %q does not match required %q",
			consumerName, cfg.FilterSubject, expectedFilterSubject)
	}

	return nil
}

// NewTelemetryConsumer creates a new consumer connected to the NATS JetStream stream.
func NewTelemetryConsumer(cfg ConsumerConfig, handler IngestionHandler, logger *slog.Logger, opts ...ConsumerOption) (*TelemetryConsumer, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.StreamName == "" {
		cfg.StreamName = "AEGISEDGE_TELEMETRY"
	}
	if strings.ContainsAny(cfg.StreamName, " \t\r\n.*>") {
		return nil, fmt.Errorf("invalid stream name %q: must not contain whitespace, '.', '*', or '>'", cfg.StreamName)
	}
	if cfg.ConsumerName == "" {
		cfg.ConsumerName = "control-plane-telemetry"
	}
	if strings.ContainsAny(cfg.ConsumerName, " \t\r\n.*>") {
		return nil, fmt.Errorf("invalid consumer name %q: must not contain whitespace, '.', '*', or '>'", cfg.ConsumerName)
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
	if cfg.MaxRecoveryBatch <= 0 {
		cfg.MaxRecoveryBatch = DefaultMaxRecoveryBatch
	}
	if cfg.RecoveryScanInterval <= 0 {
		cfg.RecoveryScanInterval = DefaultRecoveryScanInterval
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

	// 3. Durable advisory stream capturing broker delivery-exhaustion events
	if cfg.AdvisoryStreamName == "" {
		cfg.AdvisoryStreamName = fmt.Sprintf("%s_ADVISORIES", cfg.StreamName)
	}
	if strings.ContainsAny(cfg.AdvisoryStreamName, " \t\r\n.*>") {
		nc.Close()
		return nil, fmt.Errorf("invalid advisory stream name %q: must not contain whitespace, '.', '*', or '>'", cfg.AdvisoryStreamName)
	}
	if cfg.AdvisoryConsumerName == "" {
		cfg.AdvisoryConsumerName = fmt.Sprintf("%s-advisories", cfg.ConsumerName)
	}
	if strings.ContainsAny(cfg.AdvisoryConsumerName, " \t\r\n.*>") {
		nc.Close()
		return nil, fmt.Errorf("invalid advisory consumer name %q: must not contain whitespace, '.', '*', or '>'", cfg.AdvisoryConsumerName)
	}

	advSubjectFilter := fmt.Sprintf("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.%s.>", cfg.StreamName)
	advConsFilter := fmt.Sprintf("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.%s.%s", cfg.StreamName, cfg.ConsumerName)

	advStream, err := js.Stream(context.Background(), cfg.AdvisoryStreamName)
	if err == nil {
		// Existing stream: validate its configuration
		sInfo, sErr := advStream.Info(context.Background())
		if sErr != nil {
			nc.Close()
			return nil, fmt.Errorf("failed to inspect existing advisory stream %q: %w", cfg.AdvisoryStreamName, sErr)
		}
		if vErr := validateAdvisoryStreamConfig(cfg.AdvisoryStreamName, sInfo.Config, advSubjectFilter); vErr != nil {
			nc.Close()
			return nil, vErr
		}
	} else {
		advStream, err = js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
			Name:      cfg.AdvisoryStreamName,
			Subjects:  []string{advSubjectFilter},
			Retention: jetstream.LimitsPolicy,
			MaxAge:    7 * 24 * time.Hour,
			Discard:   jetstream.DiscardOld,
		})
		if err != nil {
			nc.Close()
			return nil, fmt.Errorf("failed to create durable advisory stream %q: %w", cfg.AdvisoryStreamName, err)
		}
	}

	var advCons jetstream.Consumer
	if advStream != nil {
		existingCons, cErr := advStream.Consumer(context.Background(), cfg.AdvisoryConsumerName)
		if cErr == nil {
			cInfo, ciErr := existingCons.Info(context.Background())
			if ciErr != nil {
				nc.Close()
				return nil, fmt.Errorf("failed to inspect existing advisory consumer %q: %w", cfg.AdvisoryConsumerName, ciErr)
			}
			if cvErr := validateAdvisoryConsumerConfig(cfg.AdvisoryConsumerName, cInfo.Config, advConsFilter); cvErr != nil {
				nc.Close()
				return nil, cvErr
			}
			advCons = existingCons
		} else {
			advCons, err = js.CreateOrUpdateConsumer(context.Background(), cfg.AdvisoryStreamName, jetstream.ConsumerConfig{
				Durable:       cfg.AdvisoryConsumerName,
				AckPolicy:     jetstream.AckExplicitPolicy,
				FilterSubject: advConsFilter,
				DeliverPolicy: jetstream.DeliverAllPolicy,
				MaxDeliver:    -1, // Resilient unlimited redelivery
				AckWait:       30 * time.Second,
			})
			if err != nil {
				nc.Close()
				return nil, fmt.Errorf("failed to create durable advisory consumer %q: %w", cfg.AdvisoryConsumerName, err)
			}
		}
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

	var is IdempotencyChecker = cfg.IdempotencyStore
	if is == nil {
		if asIdemp, ok := cfg.QuarantineStore.(IdempotencyChecker); ok {
			is = asIdemp
		} else if asIdemp, ok := handler.(IdempotencyChecker); ok {
			is = asIdemp
		} else if ownsStore {
			if asIdemp, ok := qs.(IdempotencyChecker); ok {
				is = asIdemp
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	tc := &TelemetryConsumer{
		conn:               nc,
		handler:            handler,
		logger:             logger,
		config:             cfg,
		js:                 js,
		stream:             stream,
		cons:               cons,
		advStream:          advStream,
		advCons:            advCons,
		quarantineStore:    qs,
		idempotencyStore:   is,
		ownsStore:          ownsStore,
		pendingExhausted:   make(map[uint64]*recoveryItem),
		pendingScanRetries: make(map[uint64]*scanRetryItem),
		exhaustedSeqs:      make(map[uint64]bool),
		ctx:                ctx,
		cancel:             cancel,
	}

	for _, opt := range opts {
		opt(tc)
	}

	// Subscribe to broker max-delivery advisory notifications for real-time notification
	advSubj := fmt.Sprintf("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.%s.%s", cfg.StreamName, cfg.ConsumerName)
	advSub, err := nc.Subscribe(advSubj, func(m *nats.Msg) {
		tc.handleAdvisoryData(m.Data, nil)
	})
	if err == nil {
		tc.advSub = advSub
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
	if !c.running.CompareAndSwap(false, true) {
		return nil
	}

	c.recoveryWg.Add(1)
	go c.recoveryWorkerLoop()

	c.recoveryWg.Add(1)
	go c.consumeLoop()

	return nil
}

// consumeLoop continuously fetches and processes messages.
func (c *TelemetryConsumer) consumeLoop() {
	defer c.recoveryWg.Done()
	c.logger.Info("telemetry consumer started")
	defer c.running.Store(false)

	for {
		if c.ctx.Err() != nil {
			return
		}

		msgs, err := c.cons.Fetch(10, jetstream.FetchMaxWait(500*time.Millisecond))
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

// markExhausted records a stream sequence as exhausted and schedules it for background recovery persistence.
func (c *TelemetryConsumer) markExhausted(seq *uint64, deliveryCount int, category storage.QuarantineCategory, reason, subject, nodeID, batchID string, data []byte) {
	if seq == nil {
		return
	}
	c.recoveryMu.Lock()
	defer c.recoveryMu.Unlock()

	c.exhaustedSeqs[*seq] = true
	if _, exists := c.pendingExhausted[*seq]; !exists {
		c.pendingExhausted[*seq] = &recoveryItem{
			streamSeq:     *seq,
			deliveryCount: deliveryCount,
			category:      category,
			reason:        reason,
			subject:       subject,
			nodeID:        nodeID,
			batchID:       batchID,
			data:          data,
		}
		c.logger.Info("tracked exhausted message for recovery",
			slog.Uint64("stream_seq", *seq),
			slog.String("subject", subject),
			slog.Int("pending_count", len(c.pendingExhausted)),
		)
	}
}

// enqueueRecovery enqueues an exhausted delivery message for background recovery persistence.
func (c *TelemetryConsumer) enqueueRecovery(seq *uint64, deliveryCount int, category storage.QuarantineCategory, reason, subject, nodeID, batchID string, data []byte) {
	c.markExhausted(seq, deliveryCount, category, reason, subject, nodeID, batchID, data)
}

// MarkSequenceExhausted registers external evidence that a stream sequence has reached delivery exhaustion.
func (c *TelemetryConsumer) MarkSequenceExhausted(seq uint64) {
	c.recoveryMu.Lock()
	c.exhaustedSeqs[seq] = true
	if _, exists := c.pendingExhausted[seq]; !exists {
		c.pendingExhausted[seq] = &recoveryItem{
			streamSeq:     seq,
			deliveryCount: c.config.MaxDeliver,
			category:      storage.CategoryDeliveryExhausted,
			reason:        "marked exhausted by operator or broker advisory",
		}
	}
	c.recoveryMu.Unlock()
}

// hasExhaustionEvidence reports whether the consumer has observed or received delivery exhaustion evidence for seq.
func (c *TelemetryConsumer) hasExhaustionEvidence(seq uint64) bool {
	c.recoveryMu.Lock()
	defer c.recoveryMu.Unlock()
	return c.exhaustedSeqs[seq]
}

// PendingRecoveryCount returns the current number of exhausted items awaiting durable quarantine persistence.
func (c *TelemetryConsumer) PendingRecoveryCount() int {
	c.recoveryMu.Lock()
	defer c.recoveryMu.Unlock()
	return len(c.pendingExhausted)
}

// PendingScanRetriesCount returns the current number of sequences with active scan retry obligations.
func (c *TelemetryConsumer) PendingScanRetriesCount() int {
	c.recoveryMu.Lock()
	defer c.recoveryMu.Unlock()
	return len(c.pendingScanRetries)
}

// validateAdvisory validates the canonical NATS advisory payload fields.
func (c *TelemetryConsumer) validateAdvisory(data []byte) (*MaxDeliverAdvisory, error) {
	var adv MaxDeliverAdvisory
	if err := json.Unmarshal(data, &adv); err != nil {
		return nil, fmt.Errorf("malformed advisory json: %w", err)
	}

	if adv.StreamSeq == 0 {
		return nil, errors.New("advisory missing or zero stream_seq")
	}

	if adv.Stream != "" && adv.Stream != c.config.StreamName {
		return nil, fmt.Errorf("mismatched stream in advisory: expected %s, got %s", c.config.StreamName, adv.Stream)
	}

	if adv.Consumer != "" && adv.Consumer != c.config.ConsumerName {
		return nil, fmt.Errorf("mismatched consumer in advisory: expected %s, got %s", c.config.ConsumerName, adv.Consumer)
	}

	if adv.Deliveries > 0 && adv.Deliveries < uint64(c.config.MaxDeliver) {
		return nil, fmt.Errorf("advisory deliveries %d below configured MaxDeliver threshold %d", adv.Deliveries, c.config.MaxDeliver)
	}

	return &adv, nil
}

// handleAdvisoryData validates, registers, and reconciles a max-deliveries advisory.
func (c *TelemetryConsumer) handleAdvisoryData(data []byte, ackMsg jetstream.Msg) {
	adv, err := c.validateAdvisory(data)
	if err != nil {
		c.logger.Warn("rejected malformed or mismatched advisory payload",
			slog.Any("error", err),
		)
		if ackMsg != nil {
			_ = ackMsg.Ack()
		}
		return
	}

	seq := adv.StreamSeq
	c.recoveryMu.Lock()
	c.exhaustedSeqs[seq] = true
	c.recoveryMu.Unlock()

	_, rErr := c.RecoverStreamSequence(c.ctx, seq, WithExhaustionEvidence())
	if rErr == nil {
		c.logger.Info("advisory successfully recovered message into durable quarantine",
			slog.Uint64("stream_seq", seq),
		)
		if ackMsg != nil {
			_ = ackMsg.Ack()
		}
		return
	}

	if errors.Is(rErr, ErrAlreadyIngested) {
		c.logger.Info("advisory stream message already committed to storage; skipping quarantine",
			slog.Uint64("stream_seq", seq),
		)
		if ackMsg != nil {
			_ = ackMsg.Ack()
		}
		return
	}

	if errors.Is(rErr, jetstream.ErrMsgNotFound) {
		c.logger.Warn("advisory source message missing or expired from stream; cannot recover",
			slog.Uint64("stream_seq", seq),
		)
		if ackMsg != nil {
			_ = ackMsg.Ack()
		}
		return
	}

	if errors.Is(rErr, storage.ErrPayloadMismatch) {
		c.logger.Error("CRITICAL: recovery payload mismatch conflict on advisory; aborting",
			slog.Uint64("stream_seq", seq),
			slog.Any("error", rErr),
		)
		if ackMsg != nil {
			_ = ackMsg.Ack()
		}
		return
	}

	// Transient error (e.g. SQLite database locked): record in memory for retry,
	// and NAK the advisory with backoff so JetStream redelivers it while keeping it durable.
	if ackMsg != nil {
		if err := ackMsg.NakWithDelay(c.config.RetryBackoff); err != nil {
			_ = ackMsg.Nak()
		}
	}
	c.markExhausted(&seq, int(adv.Deliveries), storage.CategoryDeliveryExhausted, "broker max-deliveries advisory received", "", "", "", nil)
}

// reconcileDurableAdvisories fetches and processes unacknowledged advisories from the durable advisory stream.
func (c *TelemetryConsumer) reconcileDurableAdvisories(ctx context.Context) {
	if c.advCons == nil {
		return
	}

	batchSize := c.config.MaxRecoveryBatch
	if batchSize <= 0 {
		batchSize = DefaultMaxRecoveryBatch
	}

	msgs, err := c.advCons.Fetch(batchSize, jetstream.FetchMaxWait(500*time.Millisecond))
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		if strings.Contains(err.Error(), "no messages") {
			return
		}
		c.advisoryFetchErrors.Add(1)
		c.logger.Warn("advisory consumer fetch failed", slog.Any("error", err))
		return
	}

	for msg := range msgs.Messages() {
		if ctx.Err() != nil {
			return
		}
		c.handleAdvisoryData(msg.Data(), msg)
	}

	if msgs.Error() != nil && ctx.Err() == nil {
		if !errors.Is(msgs.Error(), context.Canceled) && !errors.Is(msgs.Error(), context.DeadlineExceeded) && !strings.Contains(msgs.Error().Error(), "no messages") {
			c.advisoryFetchErrors.Add(1)
			c.logger.Warn("advisory message batch iteration error", slog.Any("error", msgs.Error()))
		}
	}
}

// inspectAdvisoryStreamHealth queries the advisory stream and consumer metadata to track capacity and detect eviction.
func (c *TelemetryConsumer) inspectAdvisoryStreamHealth(ctx context.Context) {
	if c.advStream == nil {
		return
	}

	sInfo, err := c.advStream.Info(ctx)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			c.logger.Debug("failed to query advisory stream info", slog.Any("error", err))
		}
		return
	}

	c.advisoryStreamMsgs.Store(int64(sInfo.State.Msgs))
	c.advisoryStreamBytes.Store(int64(sInfo.State.Bytes))

	// Capacity threshold warnings (>= 80%)
	if sInfo.Config.MaxMsgs > 0 && sInfo.State.Msgs >= uint64(float64(sInfo.Config.MaxMsgs)*0.8) {
		c.logger.Warn("advisory stream message capacity threshold exceeded (>= 80%)",
			slog.Uint64("messages", sInfo.State.Msgs),
			slog.Int64("max_messages", sInfo.Config.MaxMsgs),
		)
	}
	if sInfo.Config.MaxBytes > 0 && sInfo.State.Bytes >= uint64(float64(sInfo.Config.MaxBytes)*0.8) {
		c.logger.Warn("advisory stream byte capacity threshold exceeded (>= 80%)",
			slog.Uint64("bytes", sInfo.State.Bytes),
			slog.Int64("max_bytes", sInfo.Config.MaxBytes),
		)
	}

	// Eviction detection: FirstSeq advancement under DiscardOld with stream at capacity
	c.recoveryMu.Lock()
	prevFirstSeq := c.lastAdvisoryFirstSeq
	c.lastAdvisoryFirstSeq = sInfo.State.FirstSeq
	c.recoveryMu.Unlock()

	isAtCapacity := (sInfo.Config.MaxMsgs > 0 && sInfo.State.Msgs >= uint64(sInfo.Config.MaxMsgs)) ||
		(sInfo.Config.MaxBytes > 0 && sInfo.State.Bytes >= uint64(sInfo.Config.MaxBytes))

	if sInfo.Config.Discard == jetstream.DiscardOld && isAtCapacity {
		var evictedCount int64
		if prevFirstSeq > 0 && sInfo.State.FirstSeq > prevFirstSeq {
			evictedCount = int64(sInfo.State.FirstSeq - prevFirstSeq)
		} else if prevFirstSeq == 0 && sInfo.State.FirstSeq > 1 {
			evictedCount = int64(sInfo.State.FirstSeq - 1)
		}
		if evictedCount > 0 {
			c.advisoryEvictions.Add(evictedCount)
			c.logger.Warn("advisory stream capacity eviction detected under DiscardOld; unacknowledged advisories may have been pruned",
				slog.Uint64("previous_first_seq", prevFirstSeq),
				slog.Uint64("current_first_seq", sInfo.State.FirstSeq),
				slog.Int64("evicted_count", evictedCount),
			)
		}
	}

	if c.advCons != nil {
		cInfo, cErr := c.advCons.Info(ctx)
		if cErr == nil {
			c.advisoryPendingBacklog.Store(int64(cInfo.NumPending))
			c.advisoryPendingAck.Store(int64(cInfo.NumAckPending))
		}
	}
}

// processPendingRecoveryPass iterates over all pending exhausted items and attempts to persist each
// to the durable quarantine store with independent per-item backoff.
func (c *TelemetryConsumer) processPendingRecoveryPass() {
	c.recoveryMu.Lock()
	if len(c.pendingExhausted) == 0 {
		c.recoveryMu.Unlock()
		return
	}

	now := time.Now()
	var toProcess []*recoveryItem
	for _, item := range c.pendingExhausted {
		backoff := 50 * time.Millisecond
		if item.attempts > 0 {
			shift := item.attempts - 1
			if shift > 6 {
				shift = 6
			}
			backoff = (50 * time.Millisecond) << shift
			if backoff > 2*time.Second {
				backoff = 2 * time.Second
			}
		}
		if now.Sub(item.lastAttempt) >= backoff {
			toProcess = append(toProcess, item)
		}
	}
	c.recoveryMu.Unlock()

	for _, item := range toProcess {
		if c.ctx.Err() != nil {
			return
		}

		data := item.data
		subject := item.subject
		nodeID := item.nodeID
		batchID := item.batchID
		if (len(data) == 0 || nodeID == "" || batchID == "") && c.stream != nil {
			rawMsg, err := c.stream.GetMsg(c.ctx, item.streamSeq)
			if err == nil {
				if len(data) == 0 {
					data = rawMsg.Data
				}
				if subject == "" {
					subject = rawMsg.Subject
				}
				var ev types.TelemetryEvent
				if json.Unmarshal(rawMsg.Data, &ev) == nil {
					if nodeID == "" {
						nodeID = ev.SourceNodeID
					}
					if batchID == "" && ev.Payload != nil {
						batchID = ev.Payload.BatchID
					}
				}
				if nodeID == "" {
					nodeID = extractNodeIDFromSubject(rawMsg.Subject)
				}
				if batchID == "" && rawMsg.Header != nil {
					batchID = rawMsg.Header.Get("Nats-Msg-Id")
				}
			}
		}

		dedupKey := storage.DeriveDeduplicationKey(c.config.StreamName, &item.streamSeq, nodeID, batchID, subject, data)
		qid, err := storage.GenerateQuarantineID()
		if err != nil {
			continue
		}
		recordTime := time.Now().UTC()
		rec := &storage.QuarantineRecord{
			ID:                 qid,
			DeduplicationKey:   dedupKey,
			SourceStream:       c.config.StreamName,
			SourceSequence:     &item.streamSeq,
			SourceSubject:      subject,
			NodeID:             nodeID,
			BatchID:            batchID,
			Payload:            data,
			FailureCategory:    item.category,
			FailureReason:      storage.SanitizeFailureReason(item.reason),
			DeliveryCount:      item.deliveryCount,
			Attempts:           1,
			FirstQuarantinedAt: recordTime,
			LastQuarantinedAt:  recordTime,
			Status:             storage.StatusQuarantined,
			StatusUpdatedAt:    recordTime,
		}

		if c.quarantineStore != nil {
			_, qErr := c.quarantineStore.RecordQuarantine(c.ctx, rec)
			if qErr == nil {
				c.quarantinedTotal.Add(1)
				c.recoveredTotal.Add(1)
				c.recoveryMu.Lock()
				delete(c.pendingExhausted, item.streamSeq)
				c.recoveryMu.Unlock()
				c.logger.Info("exhausted message successfully recovered into durable quarantine",
					slog.Uint64("stream_seq", item.streamSeq),
					slog.String("subject", subject),
				)
				continue
			}

			if errors.Is(qErr, storage.ErrPayloadMismatch) {
				c.logger.Error("CRITICAL: recovery aborted due to payload mismatch conflict",
					slog.Uint64("stream_seq", item.streamSeq),
					slog.Any("error", qErr),
				)
				c.recoveryMu.Lock()
				delete(c.pendingExhausted, item.streamSeq)
				c.recoveryMu.Unlock()
				continue
			}

			c.recoveryMu.Lock()
			if current, ok := c.pendingExhausted[item.streamSeq]; ok {
				current.attempts++
				current.lastAttempt = time.Now()
			}
			c.recoveryMu.Unlock()
			c.logger.Warn("quarantine write failed during background recovery retry; will retry with backoff",
				slog.Uint64("stream_seq", item.streamSeq),
				slog.Int("attempts", item.attempts+1),
				slog.Any("error", qErr),
			)
		}
	}
}

// recoveryWorkerLoop processes asynchronously tracked exhausted messages, durable advisories, and periodic scan passes.
func (c *TelemetryConsumer) recoveryWorkerLoop() {
	defer c.recoveryWg.Done()
	c.logger.Info("telemetry recovery worker started")

	ticker := time.NewTicker(c.config.RecoveryScanInterval)
	defer ticker.Stop()

	// Initial scan pass on startup: inspect stream health, reconcile durable advisories first, then pending retries, then scan
	c.inspectAdvisoryStreamHealth(c.ctx)
	c.reconcileDurableAdvisories(c.ctx)
	c.processPendingRecoveryPass()
	if _, err := c.RecoverUnacknowledged(c.ctx); err != nil && !errors.Is(err, context.Canceled) {
		c.logger.Debug("initial recovery pass completed", slog.Any("error", err))
	}

	for {
		select {
		case <-c.ctx.Done():
			c.logger.Info("telemetry recovery worker stopping on context cancellation")
			return
		case <-ticker.C:
			c.inspectAdvisoryStreamHealth(c.ctx)
			c.reconcileDurableAdvisories(c.ctx)
			c.processPendingRecoveryPass()
			if _, err := c.RecoverUnacknowledged(c.ctx); err != nil && !errors.Is(err, context.Canceled) {
				c.logger.Debug("periodic recovery scan pass error", slog.Any("error", err))
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

// AdvisoryStreamMessages returns the current message count in the advisory stream.
func (c *TelemetryConsumer) AdvisoryStreamMessages() int64 {
	return c.advisoryStreamMsgs.Load()
}

// AdvisoryStreamBytes returns the current byte size of the advisory stream.
func (c *TelemetryConsumer) AdvisoryStreamBytes() int64 {
	return c.advisoryStreamBytes.Load()
}

// AdvisoryPendingBacklog returns the number of pending advisories awaiting consumer retrieval.
func (c *TelemetryConsumer) AdvisoryPendingBacklog() int64 {
	return c.advisoryPendingBacklog.Load()
}

// AdvisoryPendingAck returns the number of delivered advisories awaiting acknowledgment.
func (c *TelemetryConsumer) AdvisoryPendingAck() int64 {
	return c.advisoryPendingAck.Load()
}

// AdvisoryFetchErrors returns the total number of errors encountered while fetching advisories.
func (c *TelemetryConsumer) AdvisoryFetchErrors() int64 {
	return c.advisoryFetchErrors.Load()
}

// AdvisoryEvictions returns the total number of advisories detected evicted under stream limits.
func (c *TelemetryConsumer) AdvisoryEvictions() int64 {
	return c.advisoryEvictions.Load()
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
// inspects it, queries durable idempotency storage, and durably quarantines it if eligible.
//
// Eligibility rules:
//   - Deserialization or schema validation failure: unconditionally quarantined (poison message).
//   - Valid schema:
//     1. Query IdempotencyStore. If already ingested, returns ErrAlreadyIngested (NEVER quarantined).
//     2. If idempotency check fails with an error, returns that error (NEVER misclassified).
//     3. If not ingested, only quarantined if delivery exhaustion evidence exists
//     (recorded via consumer MaxDeliver processing, server advisory, or WithExhaustionEvidence).
//     Otherwise returns ErrNotEligible.
func (c *TelemetryConsumer) RecoverStreamSequence(ctx context.Context, seq uint64, opts ...RecoveryOption) (*storage.QuarantineRecord, error) {
	if c.stream == nil {
		return nil, errors.New("stream handle not available")
	}
	if c.quarantineStore == nil {
		return nil, errors.New("quarantine store not configured")
	}

	var ro recoveryOptions
	for _, opt := range opts {
		opt(&ro)
	}

	rawMsg, err := c.stream.GetMsg(ctx, seq)
	if err != nil {
		return nil, fmt.Errorf("failed to get stream message %d: %w", seq, err)
	}

	var event types.TelemetryEvent
	cat := storage.CategoryDeliveryExhausted
	reason := "recovered from stream after delivery exhaustion"
	var nodeID, batchID string
	isPoison := false

	if err := json.Unmarshal(rawMsg.Data, &event); err != nil {
		cat = storage.CategoryDeserialization
		reason = fmt.Sprintf("malformed_json: %v", err)
		nodeID = extractNodeIDFromSubject(rawMsg.Subject)
		if rawMsg.Header != nil {
			batchID = rawMsg.Header.Get("Nats-Msg-Id")
		}
		isPoison = true
	} else if err := event.Validate(); err != nil {
		cat = storage.CategorySchemaValidation
		reason = fmt.Sprintf("invalid_envelope: %v", err)
		nodeID = event.SourceNodeID
		if nodeID == "" {
			nodeID = extractNodeIDFromSubject(rawMsg.Subject)
		}
		isPoison = true
	} else if event.Payload == nil {
		cat = storage.CategorySchemaValidation
		reason = "nil_payload"
		nodeID = event.SourceNodeID
		isPoison = true
	} else if err := event.Payload.Validate(); err != nil {
		cat = storage.CategorySchemaValidation
		reason = fmt.Sprintf("invalid_domain_payload: %v", err)
		nodeID = event.Payload.NodeID
		batchID = event.Payload.BatchID
		isPoison = true
	} else {
		nodeID = event.SourceNodeID
		if event.Payload != nil {
			batchID = event.Payload.BatchID
		}
	}

	// If the payload is schema-valid, enforce strict eligibility rules:
	if !isPoison {
		// 1. Authoritative idempotency check
		if c.idempotencyStore != nil {
			has, chkErr := c.idempotencyStore.HasBatch(ctx, nodeID, batchID)
			if chkErr != nil {
				return nil, fmt.Errorf("idempotency check failed for node=%s batch=%s: %w", nodeID, batchID, chkErr)
			}
			if has {
				return nil, ErrAlreadyIngested
			}
		}

		// 2. Exhaustion evidence check
		hasEvidence := ro.forceExhaustion || c.hasExhaustionEvidence(seq)
		if !hasEvidence {
			return nil, ErrNotEligible
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
	c.recoveryMu.Lock()
	delete(c.pendingExhausted, seq)
	c.recoveryMu.Unlock()
	return saved, nil
}

// RecoverStreamRange inspects and recovers messages in the given stream sequence interval [startSeq, endSeq] (inclusive).
func (c *TelemetryConsumer) RecoverStreamRange(ctx context.Context, startSeq, endSeq uint64, opts ...RecoveryOption) (int, error) {
	if startSeq > endSeq {
		return 0, nil
	}
	recovered := 0
	for seq := startSeq; seq <= endSeq; seq++ {
		if ctx.Err() != nil {
			return recovered, ctx.Err()
		}
		_, err := c.RecoverStreamSequence(ctx, seq, opts...)
		if err != nil {
			if errors.Is(err, ErrAlreadyIngested) || errors.Is(err, ErrNotEligible) || errors.Is(err, jetstream.ErrMsgNotFound) {
				continue
			}
			c.logger.Warn("stream range recovery inspection failed",
				slog.Uint64("stream_seq", seq),
				slog.Any("error", err),
			)
			continue
		}
		recovered++
	}
	return recovered, nil
}

// RecoverUnacknowledged inspects the consumer's pending unacknowledged message sequence range
// in bounded passes up to MaxRecoveryBatch, advancing recoveryCursor and tracking retry obligations
// on transient errors so sequences are never permanently skipped.
func (c *TelemetryConsumer) RecoverUnacknowledged(ctx context.Context) (int, error) {
	if c.cons == nil {
		return 0, errors.New("consumer handle not available")
	}

	info, err := c.cons.Info(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get consumer info: %w", err)
	}

	recovered := 0
	processed := make(map[uint64]bool)

	// 1. Process pending scan retries from earlier transient errors with per-item backoff
	c.recoveryMu.Lock()
	var toRetry []*scanRetryItem
	now := time.Now()
	for _, item := range c.pendingScanRetries {
		backoff := 50 * time.Millisecond
		if item.attempts > 0 {
			shift := item.attempts - 1
			if shift > 6 {
				shift = 6
			}
			backoff = (50 * time.Millisecond) << shift
			if backoff > 2*time.Second {
				backoff = 2 * time.Second
			}
		}
		if now.Sub(item.lastAttempt) >= backoff {
			toRetry = append(toRetry, item)
		}
	}
	c.recoveryMu.Unlock()

	for _, item := range toRetry {
		if ctx.Err() != nil {
			return recovered, ctx.Err()
		}
		_, rErr := c.RecoverStreamSequence(ctx, item.seq)
		if rErr == nil {
			recovered++
			processed[item.seq] = true
			c.recoveryMu.Lock()
			delete(c.pendingScanRetries, item.seq)
			c.recoveryMu.Unlock()
			continue
		}
		if errors.Is(rErr, ErrAlreadyIngested) || errors.Is(rErr, ErrNotEligible) || errors.Is(rErr, jetstream.ErrMsgNotFound) {
			processed[item.seq] = true
			c.recoveryMu.Lock()
			delete(c.pendingScanRetries, item.seq)
			c.recoveryMu.Unlock()
			continue
		}
		// Transient error on retry
		c.recoveryMu.Lock()
		if cur, ok := c.pendingScanRetries[item.seq]; ok {
			cur.attempts++
			cur.lastAttempt = time.Now()
			cur.lastErr = rErr
			if cur.attempts > 15 {
				c.logger.Error("scan retry obligation exceeded maximum attempts; dropping retry",
					slog.Uint64("stream_seq", item.seq),
					slog.Any("error", rErr),
				)
				delete(c.pendingScanRetries, item.seq)
			}
		}
		c.recoveryMu.Unlock()
	}

	if info.NumAckPending == 0 || info.Delivered.Stream <= info.AckFloor.Stream {
		return recovered, nil
	}

	c.recoveryMu.Lock()
	startSeq := info.AckFloor.Stream + 1
	if c.recoveryCursor > startSeq {
		startSeq = c.recoveryCursor
	}
	batchSize := uint64(c.config.MaxRecoveryBatch)
	if batchSize == 0 {
		batchSize = DefaultMaxRecoveryBatch
	}
	endSeq := startSeq + batchSize - 1
	if endSeq > info.Delivered.Stream {
		endSeq = info.Delivered.Stream
	}
	c.recoveryMu.Unlock()

	if startSeq > endSeq {
		return recovered, nil
	}

	for seq := startSeq; seq <= endSeq; seq++ {
		if ctx.Err() != nil {
			return recovered, ctx.Err()
		}
		if processed[seq] {
			continue
		}
		_, rErr := c.RecoverStreamSequence(ctx, seq)
		if rErr != nil {
			if errors.Is(rErr, ErrAlreadyIngested) || errors.Is(rErr, ErrNotEligible) || errors.Is(rErr, jetstream.ErrMsgNotFound) {
				c.recoveryMu.Lock()
				delete(c.pendingScanRetries, seq)
				c.recoveryMu.Unlock()
				continue
			}
			// Transient error encountered during scan pass: record retry obligation so it is retried on the next pass
			c.logger.Warn("recovery scan inspection failed; recorded retry obligation",
				slog.Uint64("stream_seq", seq),
				slog.Any("error", rErr),
			)
			c.recoveryMu.Lock()
			if _, exists := c.pendingScanRetries[seq]; !exists {
				c.pendingScanRetries[seq] = &scanRetryItem{
					seq:         seq,
					attempts:    1,
					lastAttempt: time.Now(),
					lastErr:     rErr,
				}
			}
			c.recoveryMu.Unlock()
			continue
		}
		recovered++
		c.recoveryMu.Lock()
		delete(c.pendingScanRetries, seq)
		c.recoveryMu.Unlock()
	}

	c.recoveryMu.Lock()
	if endSeq >= info.Delivered.Stream {
		c.recoveryCursor = info.AckFloor.Stream + 1
	} else {
		c.recoveryCursor = endSeq + 1
	}
	c.recoveryMu.Unlock()

	return recovered, nil
}

// Stop gracefully stops the consumer.
func (c *TelemetryConsumer) Stop() {
	if c.stopped.Swap(true) {
		return
	}
	c.running.Store(false)
	c.cancel()
	c.recoveryWg.Wait()
	if c.advSub != nil {
		_ = c.advSub.Unsubscribe()
	}
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

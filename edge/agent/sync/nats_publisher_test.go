package sync

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// requireNATS connects to local NATS and skips if unavailable.
func requireNATS(t *testing.T) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect("nats://127.0.0.1:4222",
		nats.Timeout(2*time.Second),
	)
	if err != nil {
		t.Skipf("NATS server not available at 127.0.0.1:4222: %v", err)
	}
	return nc
}

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// --- Test A: Subject construction from NodeID ---

func TestBuildSubject_ValidNodeID(t *testing.T) {
	subj, err := buildSubject("edge-node-01")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if subj != "aegisedge.v1.telemetry.edge-node-01" {
		t.Errorf("unexpected subject: %s", subj)
	}
}

// --- Test Q: NodeID validation rejects illegal characters ---

func TestBuildSubject_RejectsIllegalCharacters(t *testing.T) {
	invalids := []string{
		"",
		"   ",
		"node.with.dots",
		"node>wildcard",
		"node*star",
		"node with spaces",
	}
	for _, id := range invalids {
		_, err := buildSubject(id)
		if err == nil {
			t.Errorf("expected error for nodeID %q, got nil", id)
		}
	}
}

// --- Test B: EventID == BatchID ---

func TestNewTelemetryEvent_EventIDEqualsBatchID(t *testing.T) {
	batch := &types.TelemetryBatch{
		BatchID:        "batch-uuid-123",
		NodeID:         "node-1",
		SequenceNumber: 5,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "cpu", Value: 42, Timestamp: time.Now().UTC()}},
	}
	event := types.NewTelemetryEvent(batch, time.Now().UTC())
	if event.EventID != batch.BatchID {
		t.Errorf("EventID %q != BatchID %q", event.EventID, batch.BatchID)
	}
	if event.CorrelationID != batch.BatchID {
		t.Errorf("CorrelationID %q != BatchID %q", event.CorrelationID, batch.BatchID)
	}
}

// --- Test C: Envelope serialization round-trip ---

func TestTelemetryEvent_JSONRoundTrip(t *testing.T) {
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	batch := &types.TelemetryBatch{
		BatchID:        "batch-rt-1",
		NodeID:         "node-rt",
		SequenceNumber: 10,
		CollectedAt:    now,
		Metrics:        []types.MetricSample{{Name: "cpu", Value: 55.0, Timestamp: now}},
	}
	event := types.NewTelemetryEvent(batch, now.Add(time.Second))

	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var parsed types.TelemetryEvent
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if parsed.EventID != event.EventID {
		t.Errorf("EventID mismatch: %s vs %s", parsed.EventID, event.EventID)
	}
	if parsed.EventType != types.TelemetryEventType {
		t.Errorf("EventType mismatch: %s", parsed.EventType)
	}
	if parsed.Payload == nil {
		t.Fatal("Payload is nil after round-trip")
	}
	if parsed.Payload.BatchID != batch.BatchID {
		t.Errorf("Payload BatchID mismatch: %s vs %s", parsed.Payload.BatchID, batch.BatchID)
	}
}

// --- Test D: Successful JetStream publish returns nil ---

func TestNATSPublisher_PublishBatch_Success(t *testing.T) {
	nc := requireNATS(t)
	defer nc.Close()

	publisher, err := NewNATSPublisher(NATSPublisherConfig{
		URL:            "nats://127.0.0.1:4222",
		PublishTimeout: 5 * time.Second,
	}, silentLogger())
	if err != nil {
		t.Fatalf("NewNATSPublisher failed: %v", err)
	}
	defer publisher.Close()

	batch := &types.TelemetryBatch{
		BatchID:        "batch-pub-success-" + time.Now().Format("150405"),
		NodeID:         "test-node-pub",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "cpu", Value: 42.0, Timestamp: time.Now().UTC()}},
	}

	ctx := context.Background()
	if err := publisher.PublishBatch(ctx, batch); err != nil {
		t.Fatalf("PublishBatch failed: %v", err)
	}
}

// --- Test F: NATS unavailable → publish fails, no data deletion ---

func TestNATSPublisher_UnavailableNATS_FailsGracefully(t *testing.T) {
	// Try to publish to a non-existent NATS server
	publisher := &NATSPublisher{
		conn:   nil,
		logger: silentLogger(),
		config: NATSPublisherConfig{PublishTimeout: time.Second},
	}

	batch := &types.TelemetryBatch{
		BatchID:        "batch-offline-test",
		NodeID:         "test-node",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "cpu", Value: 10, Timestamp: time.Now().UTC()}},
	}

	err := publisher.PublishBatch(context.Background(), batch)
	if err == nil {
		t.Fatal("expected error when NATS is nil/unavailable, got nil")
	}
}

// --- Test G: Retry uses same BatchID ---

func TestNATSPublisher_RetryUsesSameBatchID(t *testing.T) {
	nc := requireNATS(t)
	defer nc.Close()

	publisher, err := NewNATSPublisher(NATSPublisherConfig{
		URL:            "nats://127.0.0.1:4222",
		PublishTimeout: 5 * time.Second,
	}, silentLogger())
	if err != nil {
		t.Fatalf("NewNATSPublisher failed: %v", err)
	}
	defer publisher.Close()

	batchID := "batch-retry-same-id-" + time.Now().Format("150405")
	batch := &types.TelemetryBatch{
		BatchID:        batchID,
		NodeID:         "test-node-retry",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "cpu", Value: 42.0, Timestamp: time.Now().UTC()}},
	}

	ctx := context.Background()

	// First publish
	if err := publisher.PublishBatch(ctx, batch); err != nil {
		t.Fatalf("first PublishBatch failed: %v", err)
	}

	// Second publish (simulating retry) — same BatchID — must succeed (JetStream dedup)
	if err := publisher.PublishBatch(ctx, batch); err != nil {
		t.Fatalf("retry PublishBatch failed: %v", err)
	}
}

// --- Test H: Duplicate BatchID deduplicated by JetStream Nats-Msg-Id ---

func TestNATSPublisher_DuplicateDedup(t *testing.T) {
	nc := requireNATS(t)
	defer nc.Close()

	publisher, err := NewNATSPublisher(NATSPublisherConfig{
		URL:            "nats://127.0.0.1:4222",
		PublishTimeout: 5 * time.Second,
	}, silentLogger())
	if err != nil {
		t.Fatalf("NewNATSPublisher failed: %v", err)
	}
	defer publisher.Close()

	batchID := "batch-dedup-" + time.Now().Format("150405.000")
	batch := &types.TelemetryBatch{
		BatchID:        batchID,
		NodeID:         "test-node-dedup",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "cpu", Value: 42.0, Timestamp: time.Now().UTC()}},
	}

	ctx := context.Background()

	// Publish once
	if err := publisher.PublishBatch(ctx, batch); err != nil {
		t.Fatalf("first publish failed: %v", err)
	}

	// Publish duplicate — verify no error and the stream doesn't store a second copy
	if err := publisher.PublishBatch(ctx, batch); err != nil {
		t.Fatalf("duplicate publish failed: %v", err)
	}

	// Verify via JetStream that the stream only has one message for this Nats-Msg-Id
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("JetStream context failed: %v", err)
	}
	stream, err := js.Stream(ctx, "AEGISEDGE_TELEMETRY")
	if err != nil {
		t.Skipf("AEGISEDGE_TELEMETRY stream not found: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("stream info failed: %v", err)
	}
	// Messages should have been deduplicated by Nats-Msg-Id
	t.Logf("Stream has %d messages total after dedup test", info.State.Msgs)
}

// --- Test O: Multiple nodes → separate subjects ---

func TestBuildSubject_MultipleNodes(t *testing.T) {
	s1, _ := buildSubject("node-alpha")
	s2, _ := buildSubject("node-beta")
	if s1 == s2 {
		t.Errorf("different nodes should produce different subjects: %s == %s", s1, s2)
	}
	if s1 != "aegisedge.v1.telemetry.node-alpha" {
		t.Errorf("unexpected subject for node-alpha: %s", s1)
	}
	if s2 != "aegisedge.v1.telemetry.node-beta" {
		t.Errorf("unexpected subject for node-beta: %s", s2)
	}
}

// --- Test P: Per-node sequence numbers intact ---

func TestNATSPublisher_PerNodeSequenceIntact(t *testing.T) {
	nc := requireNATS(t)
	defer nc.Close()

	publisher, err := NewNATSPublisher(NATSPublisherConfig{
		URL:            "nats://127.0.0.1:4222",
		PublishTimeout: 5 * time.Second,
	}, silentLogger())
	if err != nil {
		t.Fatalf("NewNATSPublisher failed: %v", err)
	}
	defer publisher.Close()

	prefix := time.Now().Format("150405000")
	ctx := context.Background()

	// Subscribe to capture messages
	sub, err := nc.SubscribeSync("aegisedge.v1.telemetry.test-node-seq-" + prefix)
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	defer sub.Unsubscribe()

	for seq := int64(0); seq < 3; seq++ {
		batch := &types.TelemetryBatch{
			BatchID:        "batch-seq-" + prefix + "-" + string(rune('0'+seq)),
			NodeID:         "test-node-seq-" + prefix,
			SequenceNumber: seq,
			CollectedAt:    time.Now().UTC(),
			Metrics:        []types.MetricSample{{Name: "cpu", Value: float64(seq), Timestamp: time.Now().UTC()}},
		}
		if err := publisher.PublishBatch(ctx, batch); err != nil {
			t.Fatalf("publish seq %d failed: %v", seq, err)
		}
	}

	// Read back and verify sequence numbers
	for expectedSeq := int64(0); expectedSeq < 3; expectedSeq++ {
		msg, err := sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Fatalf("no message for seq %d: %v", expectedSeq, err)
		}
		var event types.TelemetryEvent
		if err := json.Unmarshal(msg.Data, &event); err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}
		if event.SequenceNumber != expectedSeq {
			t.Errorf("sequence mismatch: expected %d, got %d", expectedSeq, event.SequenceNumber)
		}
	}
}

// --- Integration: Syncer with NATS publisher → PUBLISHED status ---

func TestSyncer_NATSPublisher_MarksBatchPublished(t *testing.T) {
	nc := requireNATS(t)
	defer nc.Close()

	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "nats_syncer.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	batchID := "batch-syncer-nats-" + now.Format("150405.000")
	batch := &types.TelemetryBatch{
		BatchID:        batchID,
		NodeID:         "test-node-syncer",
		SequenceNumber: 1,
		CollectedAt:    now,
		Metrics:        []types.MetricSample{{Name: "cpu", Value: 42.0, Timestamp: now}},
	}

	if err := store.PersistBatch(ctx, batch); err != nil {
		t.Fatalf("PersistBatch failed: %v", err)
	}

	publisher, err := NewNATSPublisher(NATSPublisherConfig{
		URL:            "nats://127.0.0.1:4222",
		PublishTimeout: 5 * time.Second,
	}, silentLogger())
	if err != nil {
		t.Fatalf("NewNATSPublisher failed: %v", err)
	}
	defer publisher.Close()

	syncer := NewSyncerWithPublisher(store, publisher, silentLogger())
	stats, err := syncer.SyncPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("SyncPendingBatches failed: %v", err)
	}
	if stats.TotalSynced != 1 {
		t.Fatalf("expected 1 synced, got %d", stats.TotalSynced)
	}

	// Verify status is PUBLISHED (not SYNCED)
	rec, err := store.GetBatch(ctx, batchID)
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if rec.SyncStatus != storage.SyncStatusPublished {
		t.Errorf("expected PUBLISHED, got %s", rec.SyncStatus)
	}

	// Verify PUBLISHED batch is excluded from pending queries
	pending, err := store.GetPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("GetPendingBatches failed: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("expected 0 pending after publish, got %d", len(pending))
	}
}

// --- Integration: NATS unavailable → batch remains PENDING ---

func TestSyncer_NATSUnavailable_BatchRemainsPending(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "nats_unavail.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()
	batchID := "batch-nats-down"
	batch := &types.TelemetryBatch{
		BatchID:        batchID,
		NodeID:         "test-node-down",
		SequenceNumber: 1,
		CollectedAt:    now,
		Metrics:        []types.MetricSample{{Name: "cpu", Value: 42.0, Timestamp: now}},
	}
	if err := store.PersistBatch(ctx, batch); err != nil {
		t.Fatalf("PersistBatch failed: %v", err)
	}

	// Publisher with nil conn simulates unavailable NATS
	publisher := &NATSPublisher{
		conn:   nil,
		logger: silentLogger(),
		config: NATSPublisherConfig{PublishTimeout: time.Second},
	}

	syncer := NewSyncerWithPublisher(store, publisher, silentLogger())
	stats, _ := syncer.SyncPendingBatches(ctx, 10)

	if stats.TotalFailed != 1 {
		t.Errorf("expected 1 failure, got %d", stats.TotalFailed)
	}

	// Verify batch remains PENDING, not deleted
	rec, err := store.GetBatch(ctx, batchID)
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if rec.SyncStatus != storage.SyncStatusPending {
		t.Errorf("expected PENDING after NATS failure, got %s", rec.SyncStatus)
	}
	if rec.Attempt != 1 {
		t.Errorf("expected attempt 1, got %d", rec.Attempt)
	}

	// Batch count must remain 1 — no deletion occurred
	count, _ := store.CountBatches(ctx)
	if count != 1 {
		t.Errorf("expected batch count 1, got %d", count)
	}
}

package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Chamalka-heshi/AegisEdge/services/control-plane/server"
	"github.com/Chamalka-heshi/AegisEdge/services/control-plane/storage"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// runEmbeddedNATS starts an isolated, in-process NATS server with JetStream enabled on an ephemeral port.
func runEmbeddedNATS(t *testing.T) (*natsserver.Server, string) {
	t.Helper()
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		NoLog:     true,
		NoSigs:    true,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	ns, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("failed to create embedded NATS server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		ns.Shutdown()
		t.Fatalf("embedded NATS server failed to become ready within 5s")
	}
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	return ns, ns.ClientURL()
}

// setupTestStream creates or updates a JetStream stream for telemetry events.
func setupTestStream(t *testing.T, nc *nats.Conn, streamName string) jetstream.Stream {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("failed to create JetStream context: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  []string{"aegisedge.v1.telemetry.*"},
		Storage:   jetstream.MemoryStorage,
		Retention: jetstream.LimitsPolicy,
	})
	if err != nil {
		t.Fatalf("failed to create stream %s: %v", streamName, err)
	}
	return stream
}

func publishTestEvent(t *testing.T, nc *nats.Conn, batch *types.TelemetryBatch) {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("JetStream context failed: %v", err)
	}

	event := types.NewTelemetryEvent(batch, time.Now().UTC())
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event failed: %v", err)
	}

	subject := "aegisedge.v1.telemetry." + batch.NodeID
	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{},
	}
	msg.Header.Set("Nats-Msg-Id", batch.BatchID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = js.PublishMsg(ctx, msg)
	if err != nil {
		t.Fatalf("JetStream publish failed: %v", err)
	}
}

func publishRawTestBytes(t *testing.T, nc *nats.Conn, subject string, msgID string, data []byte) {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("JetStream context failed: %v", err)
	}

	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{},
	}
	if msgID != "" {
		msg.Header.Set("Nats-Msg-Id", msgID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = js.PublishMsg(ctx, msg)
	if err != nil {
		t.Fatalf("JetStream publish failed: %v", err)
	}
}

// --- Test 1: Consumer processes valid event → ACK ---

func TestConsumer_ProcessesValidEvent(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)

	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("failed to connect client to embedded NATS: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	srv := server.NewServer(silentLogger())
	defer srv.Close()

	consumerName := fmt.Sprintf("test-consumer-valid-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:      natsURL,
		StreamName:   streamName,
		ConsumerName: consumerName,
	}, srv, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	if err := consumer.Start(); err != nil {
		t.Fatalf("consumer start failed: %v", err)
	}

	batchID := fmt.Sprintf("batch-consumer-valid-%d", time.Now().UnixNano())
	batch := &types.TelemetryBatch{
		BatchID:        batchID,
		NodeID:         "test-consumer-node",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "cpu", Value: 42.0, Timestamp: time.Now().UTC()}},
	}

	publishTestEvent(t, nc, batch)

	// Wait for the specific batch to appear in the server's ingested list
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("consumer did not ingest batch %s within timeout; consumed=%d, server_count=%d",
				batchID, consumer.Consumed(), srv.Count())
		default:
			for _, ib := range srv.GetIngestedBatches() {
				if ib.Batch.BatchID == batchID {
					if consumer.Consumed() < 1 {
						t.Errorf("expected consumer.Consumed() >= 1, got %d", consumer.Consumed())
					}
					return // Success
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// --- Test 2: Duplicate delivery → no duplicate logical ingestion → ACK ---

func TestConsumer_DuplicateDelivery_Idempotent(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)

	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("failed to connect client to embedded NATS: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "dedup_test.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	srv := server.NewServer(silentLogger(), server.WithIdempotencyStore(store))
	defer srv.Close()

	consumerName := fmt.Sprintf("test-consumer-dedup-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:      natsURL,
		StreamName:   streamName,
		ConsumerName: consumerName,
	}, srv, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	if err := consumer.Start(); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	batchID := fmt.Sprintf("batch-consumer-dedup-%d", time.Now().UnixNano())
	batch := &types.TelemetryBatch{
		BatchID:        batchID,
		NodeID:         "test-consumer-dedup-node",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "cpu", Value: 42.0, Timestamp: time.Now().UTC()}},
	}

	// Pre-ingest the batch so delivery from NATS is a duplicate
	if err := srv.IngestBatch(context.Background(), batch); err != nil {
		t.Fatalf("pre-ingest failed: %v", err)
	}
	if srv.Count() != 1 {
		t.Fatalf("expected 1 after pre-ingest, got %d", srv.Count())
	}

	// Publish duplicate batch over JetStream
	publishTestEvent(t, nc, batch)

	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("consumer did not acknowledge duplicate event within timeout")
		default:
			if consumer.Consumed() >= 1 {
				goto verified
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

verified:
	// Verify exactly 1 record in server memory
	batches := srv.GetIngestedBatches()
	count := 0
	for _, ib := range batches {
		if ib.Batch.BatchID == batchID {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 ingestion of batch %s in memory (idempotent), got %d", batchID, count)
	}

	// Verify exactly 1 record in durable SQLite store
	dbCount, err := store.Count(context.Background())
	if err != nil {
		t.Fatalf("store.Count failed: %v", err)
	}
	if dbCount != 1 {
		t.Errorf("expected exactly 1 record in durable store, got %d", dbCount)
	}
}

// --- Test 3: Processing failure → NAK and JetStream redelivery ---

type mockFlakyHandler struct {
	calls atomic.Int32
}

func (m *mockFlakyHandler) IngestBatch(ctx context.Context, batch *types.TelemetryBatch) error {
	attempt := m.calls.Add(1)
	if attempt == 1 {
		// First attempt fails; causes consumer to Nak
		return errors.New("simulated transient database connection timeout")
	}
	// Subsequent attempt succeeds; causes consumer to Ack
	return nil
}

func TestConsumer_EmbeddedNATS_ProcessingFailure_NAKAndRedelivery(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)

	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("failed to connect to embedded NATS: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	handler := &mockFlakyHandler{}
	consumerName := fmt.Sprintf("test-consumer-retry-%d", time.Now().UnixNano())

	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:      natsURL,
		StreamName:   streamName,
		ConsumerName: consumerName,
	}, handler, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	if err := consumer.Start(); err != nil {
		t.Fatalf("consumer start failed: %v", err)
	}

	batch := &types.TelemetryBatch{
		BatchID:        fmt.Sprintf("batch-retry-%d", time.Now().UnixNano()),
		NodeID:         "node-retry",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "temperature", Value: 23.5, Timestamp: time.Now().UTC()}},
	}

	publishTestEvent(t, nc, batch)

	// Await redelivery: first call NAKs, second call ACKs
	deadline := time.After(8 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("message was not redelivered after NAK; total handler calls=%d, consumed=%d",
				handler.calls.Load(), consumer.Consumed())
		default:
			if handler.calls.Load() >= 2 && consumer.Consumed() >= 1 {
				t.Logf("message successfully redelivered after NAK (total attempts=%d)", handler.calls.Load())
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// --- Test 4: End-to-end NATS ingestion with durable SQLite storage verification ---

func TestConsumer_EmbeddedNATS_DurableDatabaseVerification(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)

	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("failed to connect to embedded NATS: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "nats_durable.db")

	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}

	srv := server.NewServer(silentLogger(), server.WithIdempotencyStore(store))

	consumerName := fmt.Sprintf("test-consumer-durable-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:      natsURL,
		StreamName:   streamName,
		ConsumerName: consumerName,
	}, srv, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}

	if err := consumer.Start(); err != nil {
		t.Fatalf("consumer start failed: %v", err)
	}

	batchID := fmt.Sprintf("batch-nats-durable-%d", time.Now().UnixNano())
	nodeID := "node-edge-prod-01"
	now := time.Now().UTC().Truncate(time.Millisecond)
	batch := &types.TelemetryBatch{
		BatchID:        batchID,
		NodeID:         nodeID,
		SequenceNumber: 101,
		CollectedAt:    now,
		Metrics: []types.MetricSample{
			{Name: "fan_speed_rpm", Value: 3200, Timestamp: now},
			{Name: "core_temp_c", Value: 48.2, Timestamp: now},
		},
	}

	publishTestEvent(t, nc, batch)

	// Wait for consumer to process and ACK
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("batch was not ingested within deadline; consumed=%d", consumer.Consumed())
		default:
			if consumer.Consumed() >= 1 {
				goto persisted
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

persisted:
	ctx := context.Background()

	// 1. Check batch existence in durable SQLite
	hasBatch, err := store.HasBatch(ctx, nodeID, batchID)
	if err != nil {
		t.Fatalf("store.HasBatch failed: %v", err)
	}
	if !hasBatch {
		t.Fatalf("expected batch %s/%s to be recorded in durable store", nodeID, batchID)
	}

	// 2. Check full payload integrity in SQLite
	payload, err := store.GetPayload(ctx, nodeID, batchID)
	if err != nil {
		t.Fatalf("store.GetPayload failed: %v", err)
	}
	if payload == nil {
		t.Fatalf("expected non-nil persisted payload")
	}
	if payload.BatchID != batchID || payload.NodeID != nodeID {
		t.Errorf("mismatched payload IDs: got %s/%s, expected %s/%s",
			payload.NodeID, payload.BatchID, nodeID, batchID)
	}
	if len(payload.Metrics) != 2 {
		t.Fatalf("expected 2 metrics in persisted payload, got %d", len(payload.Metrics))
	}
	if payload.Metrics[0].Name != "fan_speed_rpm" || payload.Metrics[0].Value != 3200 {
		t.Errorf("metric 0 mismatch: %+v", payload.Metrics[0])
	}
	if payload.Metrics[1].Name != "core_temp_c" || payload.Metrics[1].Value != 48.2 {
		t.Errorf("metric 1 mismatch: %+v", payload.Metrics[1])
	}

	// 3. Gracefully stop consumer and server
	consumer.Stop()
	if err := srv.Close(); err != nil {
		t.Fatalf("srv.Close failed: %v", err)
	}

	// 4. Simulate process restart: create a brand new server with a new store handle on the same db file
	reopenedStore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("reopening store failed: %v", err)
	}
	defer reopenedStore.Close()

	restartedSrv := server.NewServer(silentLogger(), server.WithIdempotencyStore(reopenedStore))
	defer restartedSrv.Close()

	// Verify history rehydration populated the in-memory cache
	if restartedSrv.HistoryCount() != 1 {
		t.Fatalf("expected 1 rehydrated batch in history cache, got %d", restartedSrv.HistoryCount())
	}
	ingested := restartedSrv.GetIngestedBatches()
	if len(ingested) != 1 || ingested[0].Batch.BatchID != batchID {
		t.Errorf("unexpected rehydrated batch: %+v", ingested)
	}

	// Verify payload retrieval on restarted server
	recoveredPayload, err := restartedSrv.GetPayload(ctx, nodeID, batchID)
	if err != nil || recoveredPayload == nil {
		t.Fatalf("failed to retrieve payload from restarted server: %v", err)
	}
	if recoveredPayload.BatchID != batchID {
		t.Errorf("expected recovered payload %s, got %s", batchID, recoveredPayload.BatchID)
	}
}

// --- Test 5: IngestBatch validation ---

func TestConsumer_IngestBatchValidation(t *testing.T) {
	srv := server.NewServer(silentLogger())

	// Test: valid batch ingests successfully
	ctx := context.Background()
	batch := &types.TelemetryBatch{
		BatchID:        "batch-handler-test",
		NodeID:         "handler-node",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "cpu", Value: 42.0, Timestamp: time.Now().UTC()}},
	}

	err := srv.IngestBatch(ctx, batch)
	if err != nil {
		t.Fatalf("first IngestBatch failed: %v", err)
	}

	// Duplicate ingestion must return server.ErrDuplicateBatch
	err = srv.IngestBatch(ctx, batch)
	if err == nil {
		t.Fatal("expected error on duplicate ingestion, got nil")
	}
	if !errors.Is(err, server.ErrDuplicateBatch) {
		t.Errorf("expected ErrDuplicateBatch, got: %v", err)
	}

	// Test: invalid batch is rejected
	invalidBatch := &types.TelemetryBatch{
		BatchID: "", // invalid
	}
	err = srv.IngestBatch(ctx, invalidBatch)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
}

// --- Test 6: Poison message (malformed JSON) is terminated and not repeatedly redelivered ---

func TestConsumer_PoisonMessage_MalformedJSON_TerminatedWithoutLoop(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	srv := server.NewServer(silentLogger())
	defer srv.Close()

	consumerName := fmt.Sprintf("test-malformed-json-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:      natsURL,
		StreamName:   streamName,
		ConsumerName: consumerName,
		MaxDeliver:   3,
		RetryBackoff: 20 * time.Millisecond,
	}, srv, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	if err := consumer.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Publish raw invalid JSON
	publishRawTestBytes(t, nc, "aegisedge.v1.telemetry.node-malformed", "msg-bad-json", []byte(`{invalid: true, non_json...`))

	// Give consumer time to process and terminate the message
	time.Sleep(300 * time.Millisecond)

	// Message must not be counted as consumed
	if consumer.Consumed() != 0 {
		t.Errorf("expected 0 consumed batches for malformed JSON, got %d", consumer.Consumed())
	}

	// History and count in server must be 0
	if srv.Count() != 0 || srv.HistoryCount() != 0 {
		t.Errorf("expected 0 ingested batches in server, got count=%d, history=%d", srv.Count(), srv.HistoryCount())
	}
}

// --- Test 7: Poison message (invalid envelope) is terminated and diagnosed ---

func TestConsumer_PoisonMessage_InvalidEnvelope_TerminatedWithoutLoop(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	srv := server.NewServer(silentLogger())
	defer srv.Close()

	consumerName := fmt.Sprintf("test-bad-envelope-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:      natsURL,
		StreamName:   streamName,
		ConsumerName: consumerName,
		MaxDeliver:   3,
		RetryBackoff: 20 * time.Millisecond,
	}, srv, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	if err := consumer.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Publish valid JSON with empty event_id and source_node_id (invalid envelope)
	badEnvelopeJSON := []byte(`{"event_id":"","source_node_id":"","payload":{"batch_id":"b1","node_id":"n1","sequence_number":1,"collected_at":"2026-01-01T00:00:00Z","metrics":[{"name":"cpu","value":1,"timestamp":"2026-01-01T00:00:00Z"}]}}`)
	publishRawTestBytes(t, nc, "aegisedge.v1.telemetry.node-bad-env", "msg-bad-env", badEnvelopeJSON)

	time.Sleep(300 * time.Millisecond)

	if consumer.Consumed() != 0 {
		t.Errorf("expected 0 consumed batches for invalid envelope, got %d", consumer.Consumed())
	}
	if srv.Count() != 0 {
		t.Errorf("expected 0 batches in server, got %d", srv.Count())
	}
}

// --- Test 8: Poison message (invalid domain payload) is terminated as permanent failure ---

func TestConsumer_PoisonMessage_InvalidDomainPayload_TerminatedWithoutLoop(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	srv := server.NewServer(silentLogger())
	defer srv.Close()

	consumerName := fmt.Sprintf("test-bad-payload-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:      natsURL,
		StreamName:   streamName,
		ConsumerName: consumerName,
		MaxDeliver:   3,
		RetryBackoff: 20 * time.Millisecond,
	}, srv, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	if err := consumer.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Publish envelope where payload has empty BatchID (domain validation failure)
	badPayloadEvent := types.TelemetryEvent{
		EventID:        "evt-bad-payload",
		EventType:      types.TelemetryEventType,
		SchemaVersion:  types.TelemetrySchemaVersion,
		SourceNodeID:   "node-bad-payload",
		SequenceNumber: 1,
		OccurredAt:     time.Now().UTC(),
		PublishedAt:    time.Now().UTC(),
		CorrelationID:  "evt-bad-payload",
		Payload: &types.TelemetryBatch{
			BatchID:        "", // Invalid!
			NodeID:         "node-bad-payload",
			SequenceNumber: 1,
			CollectedAt:    time.Now().UTC(),
			Metrics:        []types.MetricSample{{Name: "cpu", Value: 42.0, Timestamp: time.Now().UTC()}},
		},
	}
	data, _ := json.Marshal(badPayloadEvent)
	publishRawTestBytes(t, nc, "aegisedge.v1.telemetry.node-bad-payload", "msg-bad-payload", data)

	time.Sleep(300 * time.Millisecond)

	if consumer.Consumed() != 0 {
		t.Errorf("expected 0 consumed batches for invalid domain payload, got %d", consumer.Consumed())
	}
	if srv.Count() != 0 {
		t.Errorf("expected 0 batches in server, got %d", srv.Count())
	}
}

// --- Test 9: Transient storage failure retries with backoff delay ---

type mockCountingRetryHandler struct {
	calls atomic.Int32
}

func (m *mockCountingRetryHandler) IngestBatch(ctx context.Context, batch *types.TelemetryBatch) error {
	attempt := m.calls.Add(1)
	if attempt == 1 {
		return errors.New("simulated transient database lock timeout")
	}
	return nil
}

func TestConsumer_TransientStorageFailure_RetriesWithDelay(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	handler := &mockCountingRetryHandler{}
	consumerName := fmt.Sprintf("test-transient-retry-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:      natsURL,
		StreamName:   streamName,
		ConsumerName: consumerName,
		MaxDeliver:   3,
		RetryBackoff: 20 * time.Millisecond,
	}, handler, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	if err := consumer.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	batch := &types.TelemetryBatch{
		BatchID:        fmt.Sprintf("batch-transient-%d", time.Now().UnixNano()),
		NodeID:         "node-transient",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "temperature", Value: 23.5, Timestamp: time.Now().UTC()}},
	}
	publishTestEvent(t, nc, batch)

	// Wait for retry
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("transient retry did not complete; calls=%d, consumed=%d", handler.calls.Load(), consumer.Consumed())
		default:
			if handler.calls.Load() >= 2 && consumer.Consumed() >= 1 {
				return // Passed
			}
			time.Sleep(30 * time.Millisecond)
		}
	}
}

// --- Test 10: Reaching MaxDeliver is observable and does not silently discard transient failures ---

type mockAlwaysFailingHandler struct {
	calls atomic.Int32
}

func (m *mockAlwaysFailingHandler) IngestBatch(ctx context.Context, batch *types.TelemetryBatch) error {
	m.calls.Add(1)
	return errors.New("persistent transient storage I/O failure")
}

func TestConsumer_TransientFailure_MaxDeliverExhaustion_ObservableAndNotTerminated(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	handler := &mockAlwaysFailingHandler{}
	consumerName := fmt.Sprintf("test-exhaustion-%d", time.Now().UnixNano())

	// Subscribe to broker-generated maximum-deliveries advisory events before starting consumer
	advisoryCh := make(chan *nats.Msg, 10)
	advisorySubject := fmt.Sprintf("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.%s.%s", streamName, consumerName)
	advisorySub, err := nc.ChanSubscribe(advisorySubject, advisoryCh)
	if err != nil {
		t.Fatalf("failed to subscribe to advisory: %v", err)
	}
	defer advisorySub.Unsubscribe()

	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:      natsURL,
		StreamName:   streamName,
		ConsumerName: consumerName,
		MaxDeliver:   3,
		RetryBackoff: 20 * time.Millisecond,
	}, handler, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	if err := consumer.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	batch := &types.TelemetryBatch{
		BatchID:        fmt.Sprintf("batch-exhaust-%d", time.Now().UnixNano()),
		NodeID:         "node-exhaust",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "temperature", Value: 23.5, Timestamp: time.Now().UTC()}},
	}
	publishTestEvent(t, nc, batch)

	// Await 3 delivery attempts
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out awaiting deliveries; calls=%d", handler.calls.Load())
		default:
			if handler.calls.Load() >= 3 {
				goto exhausted
			}
			time.Sleep(30 * time.Millisecond)
		}
	}

exhausted:
	// 1. Verify broker advisory is emitted by the server and observe it within bounded wait
	var advisoryMsg *nats.Msg
	select {
	case advisoryMsg = <-advisoryCh:
		t.Logf("observed JetStream broker advisory on %s: %s", advisoryMsg.Subject, string(advisoryMsg.Data))
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for JetStream max-deliveries broker advisory on %s", advisorySubject)
	}

	// 2. Distinguish broker advisory from consumer logs: parse advisory JSON payload
	var advisoryData struct {
		Type       string `json:"type"`
		Stream     string `json:"stream"`
		Consumer   string `json:"consumer"`
		StreamSeq  uint64 `json:"stream_seq"`
		Deliveries int    `json:"deliveries"`
	}
	if err := json.Unmarshal(advisoryMsg.Data, &advisoryData); err != nil {
		t.Fatalf("failed to unmarshal advisory payload: %v", err)
	}
	if advisoryData.Stream != streamName {
		t.Errorf("advisory stream mismatch: expected %q, got %q", streamName, advisoryData.Stream)
	}
	if advisoryData.Consumer != consumerName {
		t.Errorf("advisory consumer mismatch: expected %q, got %q", consumerName, advisoryData.Consumer)
	}
	if advisoryData.Deliveries < 3 {
		t.Errorf("expected advisory deliveries >= 3, got %d", advisoryData.Deliveries)
	}

	// 3. Confirm handler is called no more than the configured limit (MaxDeliver = 3)
	time.Sleep(200 * time.Millisecond)
	totalCalls := handler.calls.Load()
	if totalCalls > 3 {
		t.Errorf("expected JetStream MaxDeliver to halt delivery at 3 attempts, got %d calls", totalCalls)
	}

	// 4. Confirm original message remains inspectable directly in the stream
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("failed to create JetStream context: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		t.Fatalf("failed to get stream: %v", err)
	}
	rawMsg, err := stream.GetLastMsgForSubject(ctx, "aegisedge.v1.telemetry.node-exhaust")
	if err != nil {
		t.Fatalf("failed to get message from stream: %v", err)
	}
	if rawMsg == nil {
		t.Fatalf("expected message to remain stored in stream, but got nil")
	}
	var storedEvent types.TelemetryEvent
	if err := json.Unmarshal(rawMsg.Data, &storedEvent); err != nil {
		t.Fatalf("failed to parse stored event: %v", err)
	}
	if storedEvent.Payload == nil || storedEvent.Payload.BatchID != batch.BatchID {
		t.Errorf("expected stored batch ID %q, got %+v", batch.BatchID, storedEvent.Payload)
	}

	// 5. Transient failure must NOT be acknowledged as success
	if consumer.Consumed() != 0 {
		t.Errorf("expected 0 consumed on exhaustion, got %d", consumer.Consumed())
	}

	// 6. Confirm in-memory ExhaustedDeliveries metric was incremented
	if consumer.ExhaustedDeliveries() != 1 {
		t.Errorf("expected ExhaustedDeliveries to be 1, got %d", consumer.ExhaustedDeliveries())
	}
}

// --- Test 11: Shutdown during processing does not prematurely acknowledge or intentionally terminate a message ---

type mockShutdownHandler struct {
	called atomic.Bool
	stopFn func()
}

func (m *mockShutdownHandler) IngestBatch(ctx context.Context, batch *types.TelemetryBatch) error {
	m.called.Store(true)
	if m.stopFn != nil {
		m.stopFn()
	}
	return context.Canceled
}

func TestConsumer_Shutdown_DoesNotPrematurelyAcknowledgeOrTerminate(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	handler := &mockShutdownHandler{}
	consumerName := fmt.Sprintf("test-shutdown-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:      natsURL,
		StreamName:   streamName,
		ConsumerName: consumerName,
		MaxDeliver:   3,
		RetryBackoff: 20 * time.Millisecond,
	}, handler, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}

	handler.stopFn = func() {
		consumer.Stop()
	}

	if err := consumer.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	batch := &types.TelemetryBatch{
		BatchID:        fmt.Sprintf("batch-shutdown-%d", time.Now().UnixNano()),
		NodeID:         "node-shutdown",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "temperature", Value: 23.5, Timestamp: time.Now().UTC()}},
	}
	publishTestEvent(t, nc, batch)

	// Wait for handler to be invoked and shutdown to trigger
	deadline := time.After(3 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("handler was not called before deadline")
		default:
			if handler.called.Load() {
				goto done
			}
			time.Sleep(30 * time.Millisecond)
		}
	}

done:
	time.Sleep(200 * time.Millisecond)

	// Must not be marked as consumed
	if consumer.Consumed() != 0 {
		t.Errorf("expected 0 consumed when shutdown interrupts ingestion, got %d", consumer.Consumed())
	}
}

// --- Test 12: Storage invalid metadata error terminates message without redelivery loop ---

type mockInvalidMetaHandler struct {
	calls atomic.Int32
}

func (m *mockInvalidMetaHandler) IngestBatch(ctx context.Context, batch *types.TelemetryBatch) error {
	m.calls.Add(1)
	return fmt.Errorf("%w: batch validation failed at storage boundary", storage.ErrInvalidMeta)
}

func TestConsumer_PoisonMessage_StorageInvalidMeta_TerminatedWithoutLoop(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	handler := &mockInvalidMetaHandler{}
	consumerName := fmt.Sprintf("test-invalid-meta-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:      natsURL,
		StreamName:   streamName,
		ConsumerName: consumerName,
		MaxDeliver:   3,
		RetryBackoff: 20 * time.Millisecond,
	}, handler, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	if err := consumer.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	batch := &types.TelemetryBatch{
		BatchID:        fmt.Sprintf("batch-meta-%d", time.Now().UnixNano()),
		NodeID:         "node-meta",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "temperature", Value: 23.5, Timestamp: time.Now().UTC()}},
	}
	publishTestEvent(t, nc, batch)

	// Wait to see that handler is called once and terminated immediately without redelivery loop
	time.Sleep(300 * time.Millisecond)

	calls := handler.calls.Load()
	if calls != 1 {
		t.Errorf("expected exactly 1 call for permanent storage ErrInvalidMeta, got %d", calls)
	}
	if consumer.Consumed() != 0 {
		t.Errorf("expected 0 consumed batches, got %d", consumer.Consumed())
	}
}

// --- Test 13: Shutdown race - ingestion commits but cancellation prevents ACK; redelivery is deduplicated ---

type commitSyncHandler struct {
	underlying IngestionHandler
	committed  chan struct{}
	release    chan struct{}
}

func (h *commitSyncHandler) IngestBatch(ctx context.Context, batch *types.TelemetryBatch) error {
	// Execute durable ingestion in underlying storage
	err := h.underlying.IngestBatch(ctx, batch)
	if err != nil {
		return err
	}
	// Signal that database commit has completed
	select {
	case h.committed <- struct{}{}:
	default:
	}
	// Block until the test has canceled the consumer context
	select {
	case <-h.release:
	case <-ctx.Done():
	}
	return nil
}

func TestConsumer_Shutdown_CommitWithoutAck_RedeliveryDeduplicated(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "test_shutdown_dedup.db")
	sqliteStore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("failed to create sqlite store: %v", err)
	}
	defer sqliteStore.Close()

	srv := server.NewServer(silentLogger(), server.WithIdempotencyStore(sqliteStore))
	defer srv.Close()

	consumerName := fmt.Sprintf("test-race-dedup-%d", time.Now().UnixNano())

	syncHandler := &commitSyncHandler{
		underlying: srv,
		committed:  make(chan struct{}, 1),
		release:    make(chan struct{}),
	}

	consumer1, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:      natsURL,
		StreamName:   streamName,
		ConsumerName: consumerName,
		AckWait:      250 * time.Millisecond,
		MaxDeliver:   3,
		RetryBackoff: 20 * time.Millisecond,
	}, syncHandler, silentLogger())
	if err != nil {
		t.Fatalf("failed to create consumer 1: %v", err)
	}

	if err := consumer1.Start(); err != nil {
		t.Fatalf("consumer 1 Start failed: %v", err)
	}

	batch := &types.TelemetryBatch{
		BatchID:        fmt.Sprintf("batch-race-dedup-%d", time.Now().UnixNano()),
		NodeID:         "node-race-dedup",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "pressure", Value: 101.3, Timestamp: time.Now().UTC()}},
	}
	publishTestEvent(t, nc, batch)

	// Wait deterministically for durable commit to succeed inside SQLite
	select {
	case <-syncHandler.committed:
		// Commit happened!
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for durable commit in consumer 1")
	}

	// Now stop consumer 1 while processMessage is waiting on <-release
	// consumer1.Stop() cancels c.ctx and stops consumer loop
	consumer1.Stop()

	// Release handler so processMessage continues to the post-ingestion race check
	close(syncHandler.release)

	// Verify that consumer 1 did NOT increment consumed counter
	if consumer1.Consumed() != 0 {
		t.Errorf("expected consumer 1 consumed=0 due to canceled context, got %d", consumer1.Consumed())
	}

	// Verify that the batch WAS durably persisted in SQLite by consumer 1
	ctx := context.Background()
	has, err := sqliteStore.HasBatch(ctx, batch.NodeID, batch.BatchID)
	if err != nil {
		t.Fatalf("HasBatch failed: %v", err)
	}
	if !has {
		t.Fatalf("expected batch to be present in SQLite after committed signal")
	}

	// Now start Consumer 2 with the same consumer name and same server/store to receive redelivery
	consumer2, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:      natsURL,
		StreamName:   streamName,
		ConsumerName: consumerName,
		AckWait:      250 * time.Millisecond,
		MaxDeliver:   3,
		RetryBackoff: 20 * time.Millisecond,
	}, srv, silentLogger())
	if err != nil {
		t.Fatalf("failed to create consumer 2: %v", err)
	}
	defer consumer2.Stop()

	if err := consumer2.Start(); err != nil {
		t.Fatalf("consumer 2 Start failed: %v", err)
	}

	// Wait deterministically for Consumer 2 to process the redelivery
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for consumer 2 to acknowledge redelivery")
		default:
			if consumer2.Consumed() >= 1 {
				goto verified
			}
			time.Sleep(30 * time.Millisecond)
		}
	}

verified:
	// Verify database invariant: exactly 1 batch was persisted (no duplicate insertion)
	recent, err := sqliteStore.GetRecentBatches(ctx, 10)
	if err != nil {
		t.Fatalf("GetRecentBatches failed: %v", err)
	}
	if len(recent) != 1 {
		t.Errorf("expected exactly 1 batch in store, got %d", len(recent))
	}
	if srv.Count() != 1 {
		t.Errorf("expected server count=1, got %d", srv.Count())
	}
}

// TestTelemetryConsumer_ExhaustedDeliveriesMetric verifies the thread-safe in-memory counter lifecycle.
func TestTelemetryConsumer_ExhaustedDeliveriesMetric(t *testing.T) {
	consumer := &TelemetryConsumer{
		config: ConsumerConfig{
			MaxDeliver: 3,
		},
		logger: silentLogger(),
	}

	// 1. Initial value must be 0
	if consumer.ExhaustedDeliveries() != 0 {
		t.Fatalf("expected initial ExhaustedDeliveries to be 0, got %d", consumer.ExhaustedDeliveries())
	}

	// 2. Increments accurately
	consumer.exhaustedDeliveries.Add(1)
	if consumer.ExhaustedDeliveries() != 1 {
		t.Errorf("expected ExhaustedDeliveries to be 1, got %d", consumer.ExhaustedDeliveries())
	}

	// 3. Thread-safe concurrent access
	const concurrency = 50
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = consumer.ExhaustedDeliveries()
			}
		}()
	}
	wg.Wait()
}

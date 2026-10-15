package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
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

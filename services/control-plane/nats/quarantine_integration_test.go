package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Chamalka-heshi/AegisEdge/services/control-plane/server"
	"github.com/Chamalka-heshi/AegisEdge/services/control-plane/storage"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// mockFailingQuarantineStore wraps a real QuarantineStore but allows simulating transient failures.
type mockFailingQuarantineStore struct {
	mu           sync.Mutex
	underlying   storage.QuarantineStore
	failCount    atomic.Int32
	recordCalls  atomic.Int32
	shouldFail   atomic.Bool
	conflictMode atomic.Bool
}

func newMockFailingQuarantineStore(underlying storage.QuarantineStore) *mockFailingQuarantineStore {
	m := &mockFailingQuarantineStore{underlying: underlying}
	m.shouldFail.Store(true)
	return m
}

func (m *mockFailingQuarantineStore) RecordQuarantine(ctx context.Context, record *storage.QuarantineRecord) (*storage.QuarantineRecord, error) {
	m.recordCalls.Add(1)
	if m.conflictMode.Load() {
		return nil, fmt.Errorf("%w: simulated payload mismatch conflict", storage.ErrPayloadMismatch)
	}
	if m.shouldFail.Load() {
		m.failCount.Add(1)
		return nil, errors.New("simulated transient database lock timeout")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.underlying.RecordQuarantine(ctx, record)
}

func (m *mockFailingQuarantineStore) GetQuarantineRecord(ctx context.Context, id string) (*storage.QuarantineRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.underlying.GetQuarantineRecord(ctx, id)
}

func (m *mockFailingQuarantineStore) ListQuarantineRecords(ctx context.Context, filter storage.QuarantineFilter) ([]*storage.QuarantineRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.underlying.ListQuarantineRecords(ctx, filter)
}

func (m *mockFailingQuarantineStore) UpdateQuarantineStatus(ctx context.Context, update storage.QuarantineStatusUpdate) (*storage.QuarantineRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.underlying.UpdateQuarantineStatus(ctx, update)
}

// 1. Test scenario: Malformed JSON is durably quarantined before source ACK/TERM
func TestConsumer_Quarantine_MalformedJSON_DurableBeforeTerm(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_malformed.db")
	qstore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer qstore.Close()

	srv := server.NewServer(silentLogger(), server.WithIdempotencyStore(qstore))
	defer srv.Close()

	consumerName := fmt.Sprintf("test-q-malformed-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      3,
		RetryBackoff:    20 * time.Millisecond,
		QuarantineStore: qstore,
	}, srv, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	if err := consumer.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Publish malformed JSON message
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	badPayload := []byte(`{"event_id": "bad-json", "payload": {broken-json`)
	_, err = js.Publish(ctx, "aegisedge.v1.telemetry.node-malformed", badPayload)
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	// Wait until consumer processes and quarantines the message
	deadline := time.After(3 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out awaiting malformed message quarantine; quarantined=%d", consumer.Quarantined())
		default:
			if consumer.Quarantined() >= 1 {
				goto verified
			}
			time.Sleep(30 * time.Millisecond)
		}
	}

verified:
	// Verify quarantine record in durable SQLite database
	records, err := qstore.ListQuarantineRecords(ctx, storage.QuarantineFilter{})
	if err != nil {
		t.Fatalf("ListQuarantineRecords failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 quarantine record, got %d", len(records))
	}
	rec := records[0]
	if rec.FailureCategory != storage.CategoryDeserialization {
		t.Errorf("expected CategoryDeserialization, got %s", rec.FailureCategory)
	}
	if string(rec.Payload) != string(badPayload) {
		t.Errorf("payload mismatch: expected %s, got %s", string(badPayload), string(rec.Payload))
	}
	if rec.Status != storage.StatusQuarantined {
		t.Errorf("expected status quarantined, got %s", rec.Status)
	}
	if rec.NodeID != "node-malformed" {
		t.Errorf("expected nodeID node-malformed, got %s", rec.NodeID)
	}
	// Consumed should be 0 (poison message is not valid telemetry)
	if consumer.Consumed() != 0 {
		t.Errorf("expected 0 consumed, got %d", consumer.Consumed())
	}
}

// 2. Test scenario: Invalid envelope or telemetry schema is durably quarantined
func TestConsumer_Quarantine_InvalidEnvelopeAndPayload_DurableBeforeTerm(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_schema.db")
	qstore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer qstore.Close()

	srv := server.NewServer(silentLogger(), server.WithIdempotencyStore(qstore))
	defer srv.Close()

	consumerName := fmt.Sprintf("test-q-schema-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      3,
		RetryBackoff:    20 * time.Millisecond,
		QuarantineStore: qstore,
	}, srv, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	if err := consumer.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 2a: Invalid envelope schema (empty event_id and future OccurredAt)
	invalidEnv := types.TelemetryEvent{
		EventID:        "", // Invalid: empty
		EventType:      "telemetry.batch",
		SourceNodeID:   "node-invalid-env",
		OccurredAt:     time.Now().UTC().Add(24 * time.Hour), // Invalid: clock skew
		PublishedAt:    time.Now().UTC(),
		SequenceNumber: 1,
		Payload:        nil,
	}
	data, _ := json.Marshal(invalidEnv)
	_, err = js.Publish(ctx, "aegisedge.v1.telemetry.node-invalid-env", data)
	if err != nil {
		t.Fatalf("publish invalid envelope failed: %v", err)
	}

	// 2b: Invalid domain payload (missing BatchID)
	invalidDomain := types.TelemetryEvent{
		EventID:        "evt-bad-domain-01",
		EventType:      "telemetry.batch",
		SourceNodeID:   "node-invalid-domain",
		OccurredAt:     time.Now().UTC(),
		PublishedAt:    time.Now().UTC(),
		SequenceNumber: 1,
		Payload: &types.TelemetryBatch{
			BatchID:        "", // Invalid: empty
			NodeID:         "node-invalid-domain",
			SequenceNumber: 1,
			CollectedAt:    time.Now().UTC(),
		},
	}
	data2, _ := json.Marshal(invalidDomain)
	_, err = js.Publish(ctx, "aegisedge.v1.telemetry.node-invalid-domain", data2)
	if err != nil {
		t.Fatalf("publish invalid domain failed: %v", err)
	}

	// Wait until consumer processes both
	deadline := time.After(4 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out awaiting schema quarantine; quarantined=%d", consumer.Quarantined())
		default:
			if consumer.Quarantined() >= 2 {
				goto verified
			}
			time.Sleep(30 * time.Millisecond)
		}
	}

verified:
	records, err := qstore.ListQuarantineRecords(ctx, storage.QuarantineFilter{})
	if err != nil {
		t.Fatalf("ListQuarantineRecords failed: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 quarantine records, got %d", len(records))
	}
	for _, r := range records {
		if r.FailureCategory != storage.CategorySchemaValidation {
			t.Errorf("expected CategorySchemaValidation, got %s", r.FailureCategory)
		}
		if r.Status != storage.StatusQuarantined {
			t.Errorf("expected status quarantined, got %s", r.Status)
		}
	}
}

// 3. Test scenario: Quarantine persistence failure does not ACK or TERM the source message
func TestConsumer_Quarantine_PersistenceFailure_DoesNotAckOrTerm(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_persist_fail.db")
	realStore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer realStore.Close()

	// Wrap in failing mock
	failingStore := newMockFailingQuarantineStore(realStore)

	srv := server.NewServer(silentLogger(), server.WithIdempotencyStore(realStore))
	defer srv.Close()

	consumerName := fmt.Sprintf("test-q-fail-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      5,
		RetryBackoff:    20 * time.Millisecond,
		QuarantineStore: failingStore,
	}, srv, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	if err := consumer.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	badPayload := []byte(`{not valid json}`)
	_, err = js.Publish(ctx, "aegisedge.v1.telemetry.node-persist-fail", badPayload)
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	// Because quarantine persistence fails, the message is NOT ACKed and NOT Terminated.
	// It must be redelivered (failCount >= 2 indicates multiple redelivery attempts occurred!)
	deadline := time.After(4 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out awaiting redeliveries on quarantine failure; failCount=%d", failingStore.failCount.Load())
		default:
			if failingStore.failCount.Load() >= 2 {
				goto verified
			}
			time.Sleep(30 * time.Millisecond)
		}
	}

verified:
	// Confirm not acknowledged
	if consumer.Consumed() != 0 {
		t.Errorf("expected 0 consumed, got %d", consumer.Consumed())
	}
	// Confirm quarantined count is 0 because persistence failed
	if consumer.Quarantined() != 0 {
		t.Errorf("expected 0 quarantined, got %d", consumer.Quarantined())
	}
}

// 4. Test scenario: Repeated delivery of the same source message does not create duplicate quarantine records
func TestConsumer_Quarantine_RepeatedDelivery_Deduplicated(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_dedup.db")
	qstore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer qstore.Close()

	srv := server.NewServer(silentLogger(), server.WithIdempotencyStore(qstore))
	defer srv.Close()

	consumerName := fmt.Sprintf("test-q-dedup-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      5,
		RetryBackoff:    20 * time.Millisecond,
		QuarantineStore: qstore,
	}, srv, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	// Test direct idempotent record quarantine calls
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	streamSeq := uint64(42)
	rec := &storage.QuarantineRecord{
		ID:                 "qrn-test-dedup-01",
		DeduplicationKey:   storage.DeriveDeduplicationKey(streamName, &streamSeq, "node-1", "batch-1", "aegisedge.v1.telemetry.node-1", []byte("payload")),
		SourceStream:       streamName,
		SourceSequence:     &streamSeq,
		SourceSubject:      "aegisedge.v1.telemetry.node-1",
		NodeID:             "node-1",
		BatchID:            "batch-1",
		Payload:            []byte("payload"),
		FailureCategory:    storage.CategoryDeserialization,
		FailureReason:      "initial error",
		DeliveryCount:      1,
		Attempts:           1,
		FirstQuarantinedAt: time.Now().UTC(),
		LastQuarantinedAt:  time.Now().UTC(),
		Status:             storage.StatusQuarantined,
		StatusUpdatedAt:    time.Now().UTC(),
	}

	// First insertion
	_, err = qstore.RecordQuarantine(ctx, rec)
	if err != nil {
		t.Fatalf("first RecordQuarantine failed: %v", err)
	}

	// Second insertion (simulating redelivery of same message)
	rec2 := &storage.QuarantineRecord{
		ID:                 "qrn-test-dedup-02", // different ID, same DedupKey
		DeduplicationKey:   rec.DeduplicationKey,
		SourceStream:       streamName,
		SourceSequence:     &streamSeq,
		SourceSubject:      rec.SourceSubject,
		NodeID:             rec.NodeID,
		BatchID:            rec.BatchID,
		Payload:            rec.Payload,
		FailureCategory:    storage.CategoryDeserialization,
		FailureReason:      "repeated delivery error",
		DeliveryCount:      2,
		Attempts:           1,
		FirstQuarantinedAt: time.Now().UTC(),
		LastQuarantinedAt:  time.Now().UTC(),
		Status:             storage.StatusQuarantined,
		StatusUpdatedAt:    time.Now().UTC(),
	}
	saved, err := qstore.RecordQuarantine(ctx, rec2)
	if err != nil {
		t.Fatalf("second RecordQuarantine failed: %v", err)
	}

	// Verify deduplication
	records, err := qstore.ListQuarantineRecords(ctx, storage.QuarantineFilter{})
	if err != nil {
		t.Fatalf("ListQuarantineRecords failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected exactly 1 quarantine record after repeated delivery, got %d", len(records))
	}
	if records[0].Attempts != 2 {
		t.Errorf("expected attempts=2, got %d", records[0].Attempts)
	}
	if records[0].DeliveryCount != 2 {
		t.Errorf("expected delivery_count=2, got %d", records[0].DeliveryCount)
	}
	if records[0].FailureReason != "initial error" {
		t.Errorf("expected initial failure reason preserved, got %s", records[0].FailureReason)
	}
	if saved.ID != "qrn-test-dedup-01" {
		t.Errorf("expected original stable ID preserved, got %s", saved.ID)
	}
}

// 5. Test scenario: Same deduplication key with different payload returns typed conflict error
func TestConsumer_Quarantine_PayloadConflict_PreservesOriginal(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "quarantine_conflict.db")
	qstore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer qstore.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	streamSeq := uint64(100)
	dedupKey := fmt.Sprintf("stream:TEST_STREAM:%d", streamSeq)

	// Original record
	original := &storage.QuarantineRecord{
		ID:                 "qrn-conflict-orig",
		DeduplicationKey:   dedupKey,
		SourceStream:       "TEST_STREAM",
		SourceSequence:     &streamSeq,
		Payload:            []byte("original trusted payload A"),
		FailureCategory:    storage.CategoryDeserialization,
		FailureReason:      "original reason",
		DeliveryCount:      1,
		Attempts:           1,
		FirstQuarantinedAt: time.Now().UTC(),
		LastQuarantinedAt:  time.Now().UTC(),
		Status:             storage.StatusQuarantined,
		StatusUpdatedAt:    time.Now().UTC(),
	}
	_, err = qstore.RecordQuarantine(ctx, original)
	if err != nil {
		t.Fatalf("initial RecordQuarantine failed: %v", err)
	}

	// Conflicting record with different payload under same key
	conflicting := &storage.QuarantineRecord{
		ID:                 "qrn-conflict-new",
		DeduplicationKey:   dedupKey,
		SourceStream:       "TEST_STREAM",
		SourceSequence:     &streamSeq,
		Payload:            []byte("conflicting different payload B"),
		FailureCategory:    storage.CategoryDeserialization,
		FailureReason:      "conflicting reason",
		DeliveryCount:      2,
		Attempts:           1,
		FirstQuarantinedAt: time.Now().UTC(),
		LastQuarantinedAt:  time.Now().UTC(),
		Status:             storage.StatusQuarantined,
		StatusUpdatedAt:    time.Now().UTC(),
	}
	_, err = qstore.RecordQuarantine(ctx, conflicting)
	if err == nil {
		t.Fatalf("expected ErrPayloadMismatch error on payload mismatch, got nil")
	}
	if !errors.Is(err, storage.ErrPayloadMismatch) {
		t.Errorf("expected ErrPayloadMismatch, got %v", err)
	}

	// Verify original payload remains intact
	fetched, err := qstore.GetQuarantineRecord(ctx, "qrn-conflict-orig")
	if err != nil {
		t.Fatalf("GetQuarantineRecord failed: %v", err)
	}
	if string(fetched.Payload) != "original trusted payload A" {
		t.Errorf("original payload corrupted: got %s", string(fetched.Payload))
	}
}

// 6 & 7. Test scenario: Valid telemetry is durably committed before ACK, duplicate is idempotent
func TestConsumer_Quarantine_ValidTelemetryCommitted_IdempotentDuplicate(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_valid.db")
	qstore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer qstore.Close()

	srv := server.NewServer(silentLogger(), server.WithIdempotencyStore(qstore))
	defer srv.Close()

	consumerName := fmt.Sprintf("test-q-valid-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      3,
		RetryBackoff:    20 * time.Millisecond,
		QuarantineStore: qstore,
	}, srv, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	if err := consumer.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	batch := &types.TelemetryBatch{
		BatchID:        fmt.Sprintf("batch-valid-q-%d", time.Now().UnixNano()),
		NodeID:         "node-valid-q",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "cpu_usage", Value: 42.0, Timestamp: time.Now().UTC()}},
	}

	// Publish first delivery
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	event := types.NewTelemetryEvent(batch, time.Now().UTC())
	eventData, _ := json.Marshal(event)
	subject := "aegisedge.v1.telemetry." + batch.NodeID

	// First publish
	msg1 := &nats.Msg{
		Subject: subject,
		Data:    eventData,
		Header:  nats.Header{},
	}
	msg1.Header.Set("Nats-Msg-Id", batch.BatchID+"-attempt1")
	if _, err := js.PublishMsg(ctx, msg1); err != nil {
		t.Fatalf("first publish failed: %v", err)
	}

	deadline := time.After(3 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out awaiting first ingestion")
		default:
			if consumer.Consumed() >= 1 {
				goto firstDone
			}
			time.Sleep(30 * time.Millisecond)
		}
	}

firstDone:
	// Verify valid telemetry was stored in SQLite
	stored, err := qstore.GetPayload(ctx, batch.NodeID, batch.BatchID)
	if err != nil || stored == nil {
		t.Fatalf("expected batch in store, got err: %v", err)
	}
	// Verify 0 records in quarantine
	qRecords, _ := qstore.ListQuarantineRecords(ctx, storage.QuarantineFilter{})
	if len(qRecords) != 0 {
		t.Errorf("expected 0 quarantine records for valid telemetry, got %d", len(qRecords))
	}

	// Publish duplicate delivery of same batch (e.g. redelivery with different broker message id)
	msg2 := &nats.Msg{
		Subject: subject,
		Data:    eventData,
		Header:  nats.Header{},
	}
	msg2.Header.Set("Nats-Msg-Id", batch.BatchID+"-attempt2")
	if _, err := js.PublishMsg(ctx, msg2); err != nil {
		t.Fatalf("duplicate publish failed: %v", err)
	}

	deadline2 := time.After(3 * time.Second)
	for {
		select {
		case <-deadline2:
			t.Fatalf("timed out awaiting duplicate ingestion")
		default:
			if consumer.Consumed() >= 2 {
				goto dupDone
			}
			time.Sleep(30 * time.Millisecond)
		}
	}

dupDone:
	// Count in store must still be 1 (idempotent deduplication)
	count, _ := qstore.Count(ctx)
	if count != 1 {
		t.Errorf("expected count=1 after duplicate, got %d", count)
	}
}

// 8. Test scenario: Temporary storage failure follows retry policy without prematurely quarantining
func TestConsumer_Quarantine_TemporaryStorageFailure_RetriesWithoutPrematureQuarantine(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_temp_retry.db")
	qstore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer qstore.Close()

	// Handler fails on first attempt, succeeds on second attempt (MaxDeliver = 5)
	handler := &mockFlakyHandler{}
	consumerName := fmt.Sprintf("test-q-temp-retry-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      5,
		RetryBackoff:    20 * time.Millisecond,
		QuarantineStore: qstore,
	}, handler, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	if err := consumer.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	batch := &types.TelemetryBatch{
		BatchID:        fmt.Sprintf("batch-flaky-%d", time.Now().UnixNano()),
		NodeID:         "node-flaky",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "mem_usage", Value: 80.0, Timestamp: time.Now().UTC()}},
	}
	publishTestEvent(t, nc, batch)

	deadline := time.After(4 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out awaiting retry completion; consumed=%d", consumer.Consumed())
		default:
			if consumer.Consumed() >= 1 {
				goto retryDone
			}
			time.Sleep(30 * time.Millisecond)
		}
	}

retryDone:
	// Verify that the transient error did NOT prematurely quarantine the valid message
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	records, _ := qstore.ListQuarantineRecords(ctx, storage.QuarantineFilter{})
	if len(records) != 0 {
		t.Errorf("expected 0 quarantine records for transient failure that succeeded on retry, got %d", len(records))
	}
	if consumer.ExhaustedDeliveries() != 0 {
		t.Errorf("expected 0 exhausted deliveries, got %d", consumer.ExhaustedDeliveries())
	}
}

// 9. Test scenario: Delivery exhaustion activates quarantine persistence and exhaustion metric
func TestConsumer_Quarantine_DeliveryExhaustion_QuarantinedAndMetricIncremented(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_exhaust.db")
	qstore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer qstore.Close()

	handler := &mockAlwaysFailingHandler{}
	consumerName := fmt.Sprintf("test-q-exhaust-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      3,
		RetryBackoff:    20 * time.Millisecond,
		QuarantineStore: qstore,
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
		NodeID:         "node-exhaust-01",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "disk_io", Value: 99.0, Timestamp: time.Now().UTC()}},
	}
	publishTestEvent(t, nc, batch)

	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out awaiting delivery exhaustion; calls=%d, exhausted=%d", handler.calls.Load(), consumer.ExhaustedDeliveries())
		default:
			if consumer.ExhaustedDeliveries() >= 1 && consumer.Quarantined() >= 1 {
				goto exhaustDone
			}
			time.Sleep(30 * time.Millisecond)
		}
	}

exhaustDone:
	if consumer.ExhaustedDeliveries() != 1 {
		t.Errorf("expected ExhaustedDeliveries=1, got %d", consumer.ExhaustedDeliveries())
	}
	// Verify quarantine record created in durable SQLite with CategoryDeliveryExhausted
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	records, err := qstore.ListQuarantineRecords(ctx, storage.QuarantineFilter{})
	if err != nil {
		t.Fatalf("ListQuarantineRecords failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 quarantine record on exhaustion, got %d", len(records))
	}
	if records[0].FailureCategory != storage.CategoryDeliveryExhausted {
		t.Errorf("expected CategoryDeliveryExhausted, got %s", records[0].FailureCategory)
	}
	if records[0].NodeID != "node-exhaust-01" {
		t.Errorf("expected node-exhaust-01, got %s", records[0].NodeID)
	}
}

// 10. Test scenario: Quarantine persistence failure at MaxDeliver activates recovery queue
func TestConsumer_Quarantine_PersistenceFailureAtMaxDeliver_ActivatesRecovery(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_rec_worker.db")
	realStore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer realStore.Close()

	failingStore := newMockFailingQuarantineStore(realStore)

	handler := &mockAlwaysFailingHandler{}
	consumerName := fmt.Sprintf("test-q-rec-worker-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      2,
		RetryBackoff:    20 * time.Millisecond,
		QuarantineStore: failingStore,
	}, handler, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	if err := consumer.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	batch := &types.TelemetryBatch{
		BatchID:        fmt.Sprintf("batch-rec-%d", time.Now().UnixNano()),
		NodeID:         "node-rec-01",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "temperature", Value: 25.0, Timestamp: time.Now().UTC()}},
	}
	publishTestEvent(t, nc, batch)

	// Wait until MaxDeliver is reached and exhausted deliveries incremented
	deadline := time.After(4 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out awaiting exhaustion; calls=%d, exhausted=%d", handler.calls.Load(), consumer.ExhaustedDeliveries())
		default:
			if consumer.ExhaustedDeliveries() >= 1 {
				goto exhausted
			}
			time.Sleep(30 * time.Millisecond)
		}
	}

exhausted:
	// Unblock the store so the asynchronous recovery worker can succeed
	failingStore.shouldFail.Store(false)

	// Await recovery worker success
	deadlineRec := time.After(4 * time.Second)
	for {
		select {
		case <-deadlineRec:
			t.Fatalf("timed out awaiting background recovery; recovered=%d", consumer.Recovered())
		default:
			if consumer.Recovered() >= 1 {
				goto recoveryDone
			}
			time.Sleep(30 * time.Millisecond)
		}
	}

recoveryDone:
	if consumer.Recovered() != 1 {
		t.Errorf("expected Recovered=1, got %d", consumer.Recovered())
	}
	// Verify record in SQLite store
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	records, err := realStore.ListQuarantineRecords(ctx, storage.QuarantineFilter{})
	if err != nil {
		t.Fatalf("ListQuarantineRecords failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 recovered quarantine record, got %d", len(records))
	}
	if records[0].FailureCategory != storage.CategoryDeliveryExhausted {
		t.Errorf("expected CategoryDeliveryExhausted, got %s", records[0].FailureCategory)
	}
}

// 11. Test scenario: Recovery survives control-plane process restart
func TestConsumer_Recovery_SurvivesProcessRestart(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_restart.db")
	realStore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer realStore.Close()

	failingStore := newMockFailingQuarantineStore(realStore)
	handler := &mockAlwaysFailingHandler{}
	consumerName := fmt.Sprintf("test-restart-cons-%d", time.Now().UnixNano())

	// Phase 1: Start consumer 1 with failing quarantine store
	consumer1, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      2,
		RetryBackoff:    20 * time.Millisecond,
		QuarantineStore: failingStore,
	}, handler, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer 1 failed: %v", err)
	}

	if err := consumer1.Start(); err != nil {
		t.Fatalf("Start 1 failed: %v", err)
	}

	batch := &types.TelemetryBatch{
		BatchID:        fmt.Sprintf("batch-restart-%d", time.Now().UnixNano()),
		NodeID:         "node-restart",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "restart_metric", Value: 12.3, Timestamp: time.Now().UTC()}},
	}
	publishTestEvent(t, nc, batch)

	// Wait until consumer 1 hits MaxDeliver
	deadline := time.After(4 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for consumer 1 to exhaust deliveries")
		default:
			if consumer1.ExhaustedDeliveries() >= 1 {
				goto crashProcess
			}
			time.Sleep(30 * time.Millisecond)
		}
	}

crashProcess:
	// Simulate process shutdown/restart while database was failing: stop consumer 1
	consumer1.Stop()

	// Verify that quarantine record has NOT been written yet
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	recs, _ := realStore.ListQuarantineRecords(ctx, storage.QuarantineFilter{})
	if len(recs) != 0 {
		t.Fatalf("expected 0 quarantine records before restart, got %d", len(recs))
	}

	// Phase 2: Start consumer 2 (restarted control-plane process) with working database store
	consumer2, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      2,
		RetryBackoff:    20 * time.Millisecond,
		QuarantineStore: realStore, // Working store!
	}, handler, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer 2 failed: %v", err)
	}
	defer consumer2.Stop()

	// Execute stream sequence recovery for the message that was exhausted across restart
	rec, err := consumer2.RecoverStreamSequence(ctx, 1)
	if err != nil {
		t.Fatalf("RecoverStreamSequence failed: %v", err)
	}
	if rec == nil {
		t.Fatalf("expected non-nil recovered record")
	}

	// Verify that the message was safely and durably recovered into SQLite quarantine
	records, err := realStore.ListQuarantineRecords(ctx, storage.QuarantineFilter{})
	if err != nil {
		t.Fatalf("ListQuarantineRecords failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected exactly 1 recovered record in quarantine store, got %d", len(records))
	}
	if records[0].NodeID != "node-restart" {
		t.Errorf("expected node-restart, got %s", records[0].NodeID)
	}
	if records[0].BatchID != batch.BatchID {
		t.Errorf("expected batch ID %s, got %s", batch.BatchID, records[0].BatchID)
	}
	if records[0].FailureCategory != storage.CategoryDeliveryExhausted {
		t.Errorf("expected CategoryDeliveryExhausted, got %s", records[0].FailureCategory)
	}
}

// 12. Test scenario: Shutdown cancels recovery workers cleanly without acknowledging unfinished operations
func TestConsumer_Recovery_ShutdownCancelsWorkersCleanly(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_clean_shutdown.db")
	realStore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer realStore.Close()

	failingStore := newMockFailingQuarantineStore(realStore)
	handler := &mockAlwaysFailingHandler{}
	consumerName := fmt.Sprintf("test-clean-shutdown-%d", time.Now().UnixNano())

	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      2,
		RetryBackoff:    20 * time.Millisecond,
		QuarantineStore: failingStore,
	}, handler, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}

	if err := consumer.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	batch := &types.TelemetryBatch{
		BatchID:        fmt.Sprintf("batch-shutdown-%d", time.Now().UnixNano()),
		NodeID:         "node-shutdown",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "active", Value: 1.0, Timestamp: time.Now().UTC()}},
	}
	publishTestEvent(t, nc, batch)

	// Wait for exhaustion attempt
	deadline := time.After(4 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out awaiting exhaustion")
		default:
			if consumer.ExhaustedDeliveries() >= 1 {
				goto stopConsumer
			}
			time.Sleep(30 * time.Millisecond)
		}
	}

stopConsumer:
	stoppedChan := make(chan struct{})
	go func() {
		consumer.Stop()
		close(stoppedChan)
	}()

	select {
	case <-stoppedChan:
		// Success: worker canceled cleanly and exited without blocking
	case <-time.After(3 * time.Second):
		t.Fatalf("consumer.Stop blocked or failed to cleanly terminate recovery worker")
	}

	// Verify that unfinished message was NOT acknowledged
	if consumer.Consumed() != 0 {
		t.Errorf("expected 0 consumed, got %d", consumer.Consumed())
	}
}

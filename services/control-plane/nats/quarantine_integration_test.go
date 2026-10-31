package nats

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
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

func (m *mockFailingQuarantineStore) ReplayQuarantineRecord(ctx context.Context, req storage.QuarantineReplayRequest) (*storage.QuarantineReplayResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.underlying.ReplayQuarantineRecord(ctx, req)
}

func (m *mockFailingQuarantineStore) DiscardQuarantineRecord(ctx context.Context, id, updatedBy, reason string) (*storage.QuarantineRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.underlying.DiscardQuarantineRecord(ctx, id, updatedBy, reason)
}

func (m *mockFailingQuarantineStore) PruneQuarantinePayloads(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.underlying.PruneQuarantinePayloads(ctx, cutoff, limit)
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
	rec, err := consumer2.RecoverStreamSequence(ctx, 1, WithExhaustionEvidence())
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

// 13. Test scenario: A valid batch already committed to SQLite is encountered during recovery and is NOT quarantined
func TestConsumer_Recovery_AlreadyIngestedBatch_NeverQuarantined(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_already_ingested.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	srv := server.NewServer(silentLogger(), server.WithIdempotencyStore(store))
	defer srv.Close()

	ctx := context.Background()
	batch := &types.TelemetryBatch{
		BatchID:        fmt.Sprintf("batch-committed-%d", time.Now().UnixNano()),
		NodeID:         "node-committed",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "cpu_usage", Value: 42.0, Timestamp: time.Now().UTC()}},
	}

	// Durably commit the batch to storage first
	if err := srv.IngestBatch(ctx, batch); err != nil {
		t.Fatalf("IngestBatch failed: %v", err)
	}

	// Publish the exact same batch event to NATS JetStream (seq 1)
	publishTestEvent(t, nc, batch)

	consumerName := fmt.Sprintf("test-recovery-already-ingested-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:          natsURL,
		StreamName:       streamName,
		ConsumerName:     consumerName,
		MaxDeliver:       3,
		QuarantineStore:  store,
		IdempotencyStore: store,
	}, srv, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	// Attempt recovery on sequence 1
	rec, rErr := consumer.RecoverStreamSequence(ctx, 1)
	if !errors.Is(rErr, ErrAlreadyIngested) {
		t.Fatalf("expected ErrAlreadyIngested, got: %v (rec: %+v)", rErr, rec)
	}

	// Verify that quarantine store has ZERO records
	recs, err := store.ListQuarantineRecords(ctx, storage.QuarantineFilter{})
	if err != nil {
		t.Fatalf("ListQuarantineRecords failed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("CRITICAL: committed telemetry was wrongly quarantined; expected 0 records, got %d", len(recs))
	}

	// Verify that RecoverStreamRange also skips it cleanly
	recovered, err := consumer.RecoverStreamRange(ctx, 1, 1)
	if err != nil {
		t.Fatalf("RecoverStreamRange failed: %v", err)
	}
	if recovered != 0 {
		t.Fatalf("expected 0 recovered, got %d", recovered)
	}
}

// 14. Test scenario: A valid batch not yet committed is NOT automatically classified as exhausted without evidence
func TestConsumer_Recovery_UncommittedValidBatch_NotClassifiedWithoutEvidence(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_uncommitted.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	srv := server.NewServer(silentLogger(), server.WithIdempotencyStore(store))
	defer srv.Close()

	ctx := context.Background()
	batch := &types.TelemetryBatch{
		BatchID:        fmt.Sprintf("batch-uncommitted-%d", time.Now().UnixNano()),
		NodeID:         "node-uncommitted",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "mem_free", Value: 1024.0, Timestamp: time.Now().UTC()}},
	}

	// Publish to JetStream (seq 1), but do not ingest and do not exhaust
	publishTestEvent(t, nc, batch)

	consumerName := fmt.Sprintf("test-recovery-uncommitted-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:          natsURL,
		StreamName:       streamName,
		ConsumerName:     consumerName,
		MaxDeliver:       3,
		QuarantineStore:  store,
		IdempotencyStore: store,
	}, srv, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	// Calling RecoverStreamSequence without exhaustion evidence must return ErrNotEligible
	rec, rErr := consumer.RecoverStreamSequence(ctx, 1)
	if !errors.Is(rErr, ErrNotEligible) {
		t.Fatalf("expected ErrNotEligible, got: %v (rec: %+v)", rErr, rec)
	}

	// Verify quarantine store remains empty
	recs, err := store.ListQuarantineRecords(ctx, storage.QuarantineFilter{})
	if err != nil {
		t.Fatalf("ListQuarantineRecords failed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("expected 0 quarantine records, got %d", len(recs))
	}

	// Now provide exhaustion evidence via WithExhaustionEvidence option
	rec, rErr = consumer.RecoverStreamSequence(ctx, 1, WithExhaustionEvidence())
	if rErr != nil {
		t.Fatalf("expected success with exhaustion evidence, got: %v", rErr)
	}
	if rec == nil || rec.FailureCategory != storage.CategoryDeliveryExhausted {
		t.Fatalf("expected CategoryDeliveryExhausted, got: %+v", rec)
	}
}

// 15. Test scenario: Idempotency lookup failure does not cause false quarantine
type mockFailingIdempotencyStore struct {
	err error
}

func (m *mockFailingIdempotencyStore) HasBatch(ctx context.Context, nodeID, batchID string) (bool, error) {
	return false, m.err
}

func TestConsumer_Recovery_IdempotencyLookupFailure_DoesNotCauseQuarantine(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_idemp_fail.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	batch := &types.TelemetryBatch{
		BatchID:        fmt.Sprintf("batch-idemp-err-%d", time.Now().UnixNano()),
		NodeID:         "node-idemp-err",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "disk_io", Value: 5.0, Timestamp: time.Now().UTC()}},
	}
	publishTestEvent(t, nc, batch)

	failingIdemp := &mockFailingIdempotencyStore{err: errors.New("simulated database disk I/O error")}
	handler := &mockAlwaysFailingHandler{}

	consumerName := fmt.Sprintf("test-recovery-idemp-fail-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:          natsURL,
		StreamName:       streamName,
		ConsumerName:     consumerName,
		MaxDeliver:       3,
		QuarantineStore:  store,
		IdempotencyStore: failingIdemp,
	}, handler, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	// Attempt recovery: lookup fails
	rec, rErr := consumer.RecoverStreamSequence(ctx, 1)
	if rErr == nil {
		t.Fatalf("expected error from idempotency failure, got nil (rec: %+v)", rec)
	}
	if !strings.Contains(rErr.Error(), "idempotency check failed") {
		t.Fatalf("expected idempotency check failure message, got: %v", rErr)
	}

	// Verify no quarantine record was written
	recs, err := store.ListQuarantineRecords(ctx, storage.QuarantineFilter{})
	if err != nil {
		t.Fatalf("ListQuarantineRecords failed: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("expected 0 quarantine records on lookup error, got %d", len(recs))
	}
}

// 16. Test scenario: Map tracker handles large number of exhausted items without dropping
func TestConsumer_Recovery_MapTracker_NoDropsOnRepeatedFailures(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_map_tracker.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	handler := &mockAlwaysFailingHandler{}
	consumerName := fmt.Sprintf("test-recovery-map-tracker-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      3,
		QuarantineStore: store,
	}, handler, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	// Enqueue 350 items (exceeds previous 256-element channel)
	const count = 350
	for i := 1; i <= count; i++ {
		seq := uint64(i)
		consumer.markExhausted(&seq, 3, storage.CategoryDeliveryExhausted, "exhausted test", "aegisedge.v1.telemetry.node-1", "node-1", fmt.Sprintf("batch-%d", i), []byte(`{}`))
	}

	// Verify all 350 are safely retained in pending recovery map
	if pending := consumer.PendingRecoveryCount(); pending != count {
		t.Fatalf("expected %d pending items, got %d (items were dropped!)", count, pending)
	}

	// Verify hasExhaustionEvidence for all of them
	for i := 1; i <= count; i++ {
		if !consumer.hasExhaustionEvidence(uint64(i)) {
			t.Fatalf("missing exhaustion evidence for sequence %d", i)
		}
	}
}

// 17. Test scenario: Failed quarantine write is retried after storage becomes available without restarting consumer
func TestConsumer_Recovery_FailedQuarantine_RetriedWhenStorageAvailable(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_retry_avail.db")
	realStore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer realStore.Close()

	failingStore := newMockFailingQuarantineStore(realStore)
	failingStore.shouldFail.Store(true)

	handler := &mockAlwaysFailingHandler{}
	consumerName := fmt.Sprintf("test-recovery-retry-avail-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:              natsURL,
		StreamName:           streamName,
		ConsumerName:         consumerName,
		MaxDeliver:           3,
		QuarantineStore:      failingStore,
		RecoveryScanInterval: 50 * time.Millisecond,
	}, handler, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	// Track an exhausted message
	seq := uint64(42)
	payload := []byte(`{"event_id":"evt-retry","source_node_id":"node-retry"}`)
	consumer.markExhausted(&seq, 3, storage.CategoryDeliveryExhausted, "exhausted retry test", "aegisedge.v1.telemetry.node-retry", "node-retry", "batch-retry-42", payload)

	if consumer.PendingRecoveryCount() != 1 {
		t.Fatalf("expected 1 pending item, got %d", consumer.PendingRecoveryCount())
	}

	// Execute recovery pass while storage is failing
	consumer.processPendingRecoveryPass()

	// Item must remain pending
	if consumer.PendingRecoveryCount() != 1 {
		t.Fatalf("item was prematurely dropped during failure, pending count: %d", consumer.PendingRecoveryCount())
	}

	// Heal storage
	failingStore.shouldFail.Store(false)

	// Reset item's lastAttempt to bypass backoff delay
	consumer.recoveryMu.Lock()
	if item, ok := consumer.pendingExhausted[seq]; ok {
		item.lastAttempt = time.Now().Add(-5 * time.Second)
	}
	consumer.recoveryMu.Unlock()

	// Execute recovery pass with working storage
	consumer.processPendingRecoveryPass()

	// Pending count should now be 0
	if pending := consumer.PendingRecoveryCount(); pending != 0 {
		t.Fatalf("expected 0 pending items after recovery pass, got %d", pending)
	}

	// Verify record in real store
	ctx := context.Background()
	recs, err := realStore.ListQuarantineRecords(ctx, storage.QuarantineFilter{})
	if err != nil {
		t.Fatalf("ListQuarantineRecords failed: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 quarantined record, got %d", len(recs))
	}
	if recs[0].BatchID != "batch-retry-42" {
		t.Errorf("expected batch-retry-42, got %s", recs[0].BatchID)
	}
}

// 18. Test scenario: Single unavailable-storage item does not indefinitely block other recovery work
type selectiveFailingStore struct {
	underlying storage.QuarantineStore
	failSeq    uint64
}

func (s *selectiveFailingStore) RecordQuarantine(ctx context.Context, record *storage.QuarantineRecord) (*storage.QuarantineRecord, error) {
	if record.SourceSequence != nil && *record.SourceSequence == s.failSeq {
		return nil, errors.New("simulated error on failSeq")
	}
	return s.underlying.RecordQuarantine(ctx, record)
}

func (s *selectiveFailingStore) GetQuarantineRecord(ctx context.Context, id string) (*storage.QuarantineRecord, error) {
	return s.underlying.GetQuarantineRecord(ctx, id)
}

func (s *selectiveFailingStore) ListQuarantineRecords(ctx context.Context, filter storage.QuarantineFilter) ([]*storage.QuarantineRecord, error) {
	return s.underlying.ListQuarantineRecords(ctx, filter)
}

func (s *selectiveFailingStore) UpdateQuarantineStatus(ctx context.Context, update storage.QuarantineStatusUpdate) (*storage.QuarantineRecord, error) {
	return s.underlying.UpdateQuarantineStatus(ctx, update)
}

func (s *selectiveFailingStore) ReplayQuarantineRecord(ctx context.Context, req storage.QuarantineReplayRequest) (*storage.QuarantineReplayResult, error) {
	return s.underlying.ReplayQuarantineRecord(ctx, req)
}

func (s *selectiveFailingStore) DiscardQuarantineRecord(ctx context.Context, id, updatedBy, reason string) (*storage.QuarantineRecord, error) {
	return s.underlying.DiscardQuarantineRecord(ctx, id, updatedBy, reason)
}

func (s *selectiveFailingStore) PruneQuarantinePayloads(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	return s.underlying.PruneQuarantinePayloads(ctx, cutoff, limit)
}

func TestConsumer_Recovery_SingleUnavailableItem_DoesNotBlockOthers(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_selective.db")
	realStore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer realStore.Close()

	store := &selectiveFailingStore{underlying: realStore, failSeq: 10}
	handler := &mockAlwaysFailingHandler{}
	consumerName := fmt.Sprintf("test-recovery-selective-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      3,
		QuarantineStore: store,
	}, handler, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	// Enqueue failing seq 10 and healthy seq 20
	seq10 := uint64(10)
	seq20 := uint64(20)
	consumer.markExhausted(&seq10, 3, storage.CategoryDeliveryExhausted, "failing item", "aegisedge.v1.telemetry.node-1", "node-1", "batch-10", []byte(`{}`))
	consumer.markExhausted(&seq20, 3, storage.CategoryDeliveryExhausted, "healthy item", "aegisedge.v1.telemetry.node-2", "node-2", "batch-20", []byte(`{}`))

	if consumer.PendingRecoveryCount() != 2 {
		t.Fatalf("expected 2 pending items, got %d", consumer.PendingRecoveryCount())
	}

	consumer.processPendingRecoveryPass()

	// Seq 20 must have succeeded and been removed from pending, leaving only seq 10
	if consumer.PendingRecoveryCount() != 1 {
		t.Fatalf("expected exactly 1 item remaining pending (seq 10), got %d", consumer.PendingRecoveryCount())
	}

	ctx := context.Background()
	recs, err := realStore.ListQuarantineRecords(ctx, storage.QuarantineFilter{})
	if err != nil {
		t.Fatalf("ListQuarantineRecords failed: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 quarantined record, got %d", len(recs))
	}
	if recs[0].BatchID != "batch-20" {
		t.Errorf("expected batch-20 quarantined, got %s", recs[0].BatchID)
	}
}

// 19. Test scenario: Large sequence span is processed in bounded passes and eventually continues
func TestConsumer_Recovery_BoundedScan_AdvancesCursor(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_bounded_scan.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	handler := &mockAlwaysFailingHandler{}
	consumerName := fmt.Sprintf("test-recovery-bounded-scan-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:          natsURL,
		StreamName:       streamName,
		ConsumerName:     consumerName,
		MaxDeliver:       3,
		MaxRecoveryBatch: 2, // Bounded to 2 per pass
		QuarantineStore:  store,
	}, handler, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	// Publish 5 invalid messages directly to stream
	for i := 1; i <= 5; i++ {
		_, pErr := nc.Request(fmt.Sprintf("aegisedge.v1.telemetry.node-%d", i), []byte(`invalid-json-payload`), 2*time.Second)
		if pErr != nil {
			_ = nc.Publish(fmt.Sprintf("aegisedge.v1.telemetry.node-%d", i), []byte(`invalid-json-payload`))
		}
	}
	_ = nc.Flush()

	ctx := context.Background()
	// Manually test bounded interval recovery with RecoverStreamRange
	rec1, err1 := consumer.RecoverStreamRange(ctx, 1, 2)
	if err1 != nil {
		t.Fatalf("pass 1 failed: %v", err1)
	}
	if rec1 != 2 {
		t.Fatalf("expected 2 recovered in pass 1, got %d", rec1)
	}

	rec2, err2 := consumer.RecoverStreamRange(ctx, 3, 4)
	if err2 != nil {
		t.Fatalf("pass 2 failed: %v", err2)
	}
	if rec2 != 2 {
		t.Fatalf("expected 2 recovered in pass 2, got %d", rec2)
	}

	rec3, err3 := consumer.RecoverStreamRange(ctx, 5, 5)
	if err3 != nil {
		t.Fatalf("pass 3 failed: %v", err3)
	}
	if rec3 != 1 {
		t.Fatalf("expected 1 recovered in pass 3, got %d", rec3)
	}

	// Verify total quarantined
	recs, err := store.ListQuarantineRecords(ctx, storage.QuarantineFilter{})
	if err != nil {
		t.Fatalf("ListQuarantineRecords failed: %v", err)
	}
	if len(recs) != 5 {
		t.Fatalf("expected 5 records quarantined across bounded passes, got %d", len(recs))
	}
}

// 20. Test scenario: Expired/deleted messages are handled gracefully without false success
func TestConsumer_Recovery_ExpiredOrDeletedMessages_HandledGracefully(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_deleted_msg.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	handler := &mockAlwaysFailingHandler{}
	consumerName := fmt.Sprintf("test-recovery-deleted-msg-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      3,
		QuarantineStore: store,
	}, handler, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	// Sequence 999 does not exist in the stream
	ctx := context.Background()
	_, rErr := consumer.RecoverStreamSequence(ctx, 999)
	if rErr == nil {
		t.Fatalf("expected error for non-existent sequence 999, got nil")
	}

	// RecoverStreamRange over range [990, 1000] should handle missing messages without crashing
	recovered, err := consumer.RecoverStreamRange(ctx, 990, 1000)
	if err != nil {
		t.Fatalf("RecoverStreamRange returned unexpected error: %v", err)
	}
	if recovered != 0 {
		t.Fatalf("expected 0 recovered for missing messages, got %d", recovered)
	}
}

// 21. Test scenario: Startup recovery is tracked in WaitGroup and exits cleanly on shutdown
func TestConsumer_Recovery_StartupTrackedInWaitGroup_CleanShutdown(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_wg_shutdown.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	srv := server.NewServer(silentLogger(), server.WithIdempotencyStore(store))
	defer srv.Close()

	consumerName := fmt.Sprintf("test-recovery-wg-shutdown-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      3,
		QuarantineStore: store,
	}, srv, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}

	if err := consumer.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Calling Start again is an idempotent no-op
	if err := consumer.Start(); err != nil {
		t.Fatalf("second Start failed: %v", err)
	}

	// Stop must wait on WaitGroup and cancel all workers cleanly
	stopped := make(chan struct{})
	go func() {
		consumer.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
		// Clean exit
	case <-time.After(3 * time.Second):
		t.Fatalf("consumer.Stop timed out waiting for recoveryWg")
	}

	// Calling Stop again is an idempotent no-op
	consumer.Stop()
}

// Verification test for Phase 2: verify JetStream stream can capture $JS.EVENT.ADVISORY.>
func TestJetStream_AdvisoryStream_CreationAndCapture(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}

	streamName := "AEGISEDGE_ADVISORIES_TEST"
	cfg := jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  []string{"$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>"},
		Retention: jetstream.LimitsPolicy,
	}

	str, err := js.CreateOrUpdateStream(context.Background(), cfg)
	if err != nil {
		t.Fatalf("CreateOrUpdateStream failed on $JS.EVENT.ADVISORY.>: %v", err)
	}

	info, err := str.Info(context.Background())
	if err != nil {
		t.Fatalf("str.Info failed: %v", err)
	}
	t.Logf("Created stream %s with subjects: %v", info.Config.Name, info.Config.Subjects)

	// Publish an advisory event to the subject
	advSubject := "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.TEST.C1"
	advPayload := []byte(`{"type":"io.nats.jetstream.advisory.v1.max_deliver","stream":"TEST","consumer":"C1","stream_seq":10,"deliveries":5}`)
	if err := nc.Publish(advSubject, advPayload); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	_ = nc.Flush()

	// Verify the stream received it
	sInfo, err := str.Info(context.Background())
	if err != nil {
		t.Fatalf("str.Info failed: %v", err)
	}
	if sInfo.State.Msgs != 1 {
		t.Fatalf("expected stream to contain 1 message, got %d", sInfo.State.Msgs)
	}
	rawMsg, err := str.GetMsg(context.Background(), 1)
	if err != nil {
		t.Fatalf("GetMsg failed: %v", err)
	}
	t.Logf("Stream message 1 data: %s", string(rawMsg.Data))
}

// 22. Test scenario: Max-delivery advisory published while control plane is offline
// -> on restart, advisory is rediscovered from durable stream and message is quarantined.
func TestConsumer_Advisory_DurableAcrossDowntime_QuarantinesOnRestart(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_advisory_downtime.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	consumerName := fmt.Sprintf("test-adv-offline-%d", time.Now().UnixNano())
	handler := &mockAlwaysFailingHandler{}

	// Initialize consumer 1 briefly so stream, consumer, and advisory stream are created
	consumer1, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      3,
		QuarantineStore: store,
	}, handler, silentLogger())
	if err != nil {
		t.Fatalf("consumer 1 creation failed: %v", err)
	}
	// Stop consumer 1 - control plane is now OFFLINE
	consumer1.Stop()

	// While control plane is offline, publish a telemetry message directly to stream
	batch := &types.TelemetryBatch{
		BatchID:        fmt.Sprintf("batch-dt-%d", time.Now().UnixNano()),
		NodeID:         "node-downtime",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "temp", Value: 42.5, Timestamp: time.Now().UTC()}},
	}
	publishTestEvent(t, nc, batch)
	seq := uint64(1)

	// Publish max-deliver advisory while control plane is offline
	advSubject := fmt.Sprintf("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.%s.%s", streamName, consumerName)
	advPayload := fmt.Sprintf(`{"type":"io.nats.jetstream.advisory.v1.max_deliver","stream":"%s","consumer":"%s","stream_seq":%d,"deliveries":3}`,
		streamName, consumerName, seq)
	if err := nc.Publish(advSubject, []byte(advPayload)); err != nil {
		t.Fatalf("publish advisory failed: %v", err)
	}
	_ = nc.Flush()

	// Consumer starts up afresh with the same persistent SQLite store
	consumer2, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      3,
		QuarantineStore: store,
	}, handler, silentLogger())
	if err != nil {
		t.Fatalf("consumer 2 creation failed: %v", err)
	}
	defer consumer2.Stop()

	if err := consumer2.Start(); err != nil {
		t.Fatalf("consumer 2 Start failed: %v", err)
	}

	// Verify the advisory was discovered and processed into quarantine without needing manual WithExhaustionEvidence()
	ctx := context.Background()
	var recs []*storage.QuarantineRecord
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		recs, _ = store.ListQuarantineRecords(ctx, storage.QuarantineFilter{})
		if len(recs) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if len(recs) == 0 {
		t.Fatalf("expected telemetry message to be quarantined via durable advisory on restart, but none found")
	}
	if recs[0].SourceSequence == nil || *recs[0].SourceSequence != seq {
		t.Errorf("expected quarantined sequence %d, got %v", seq, recs[0].SourceSequence)
	}
	if recs[0].FailureCategory != storage.CategoryDeliveryExhausted {
		t.Errorf("expected CategoryDeliveryExhausted, got %s", recs[0].FailureCategory)
	}
}

// 23. Test scenario: Simulated SQLite outage during exhaustion
// -> advisory or recovery item is retried until store recovers without losing evidence.
func TestConsumer_Advisory_StoreOutage_RetriesUntilRestored(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_adv_outage.db")
	realStore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer realStore.Close()

	mockStore := newMockFailingQuarantineStore(realStore)
	mockStore.shouldFail.Store(true) // Store is broken

	consumerName := fmt.Sprintf("test-adv-outage-%d", time.Now().UnixNano())
	handler := &mockAlwaysFailingHandler{}

	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      3,
		QuarantineStore: mockStore,
	}, handler, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	// Publish message to telemetry stream
	batch := &types.TelemetryBatch{
		BatchID:        "batch-outage-1",
		NodeID:         "node-outage",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "m1", Value: 10.0, Timestamp: time.Now().UTC()}},
	}
	publishTestEvent(t, nc, batch)
	seq := uint64(1)

	// Trigger advisory handling while store is broken
	advPayload := fmt.Sprintf(`{"type":"io.nats.jetstream.advisory.v1.max_deliver","stream":"%s","consumer":"%s","stream_seq":%d,"deliveries":3}`,
		streamName, consumerName, seq)
	consumer.handleAdvisoryData([]byte(advPayload), nil)

	// Since store failed, seq must be preserved in pendingExhausted
	if consumer.PendingRecoveryCount() != 1 {
		t.Fatalf("expected 1 pending recovery item, got %d", consumer.PendingRecoveryCount())
	}

	// Restore store health
	mockStore.shouldFail.Store(false)

	// Process recovery pass
	consumer.processPendingRecoveryPass()

	// Now pending count must be 0 and record must be in durable store
	if consumer.PendingRecoveryCount() != 0 {
		t.Fatalf("expected 0 pending recovery items after restore, got %d", consumer.PendingRecoveryCount())
	}

	recs, err := realStore.ListQuarantineRecords(context.Background(), storage.QuarantineFilter{})
	if err != nil {
		t.Fatalf("ListQuarantineRecords failed: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 record quarantined, got %d", len(recs))
	}
	if recs[0].BatchID != "batch-outage-1" {
		t.Errorf("expected batch-outage-1, got %s", recs[0].BatchID)
	}
}

// 24. Test scenario: Transient failure during RecoverUnacknowledged
// -> places sequence in pendingScanRetries and retries on subsequent pass.
func TestConsumer_Recovery_ScanRetryBackoff_RetriesPendingObligations(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_scan_retry.db")
	realStore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer realStore.Close()

	// Publish an invalid message
	_, err = nc.Request("aegisedge.v1.telemetry.node-retry", []byte(`invalid json`), 2*time.Second)
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	mockStore := newMockFailingQuarantineStore(realStore)
	mockStore.shouldFail.Store(true) // Fail initial quarantine attempt

	consumerName := fmt.Sprintf("test-scan-retry-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      3,
		QuarantineStore: mockStore,
	}, &mockAlwaysFailingHandler{}, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	// Fetch message once so consumer has delivered it and NumAckPending > 0
	msgs, fErr := consumer.cons.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	if fErr != nil {
		t.Fatalf("Fetch failed: %v", fErr)
	}
	for msg := range msgs.Messages() {
		// NAK message so it remains unacknowledged
		_ = msg.Nak()
	}

	// First pass: scan encounters store error
	ctx := context.Background()
	_, _ = consumer.RecoverUnacknowledged(ctx)

	// Verify retry obligation was recorded
	if consumer.PendingScanRetriesCount() != 1 {
		t.Fatalf("expected 1 pending scan retry, got %d", consumer.PendingScanRetriesCount())
	}

	// Restore store
	mockStore.shouldFail.Store(false)

	// Wait 60ms to satisfy the initial 50ms backoff
	time.Sleep(60 * time.Millisecond)

	// Second pass: retries pending obligations
	recovered, err := consumer.RecoverUnacknowledged(ctx)
	if err != nil {
		t.Fatalf("second pass failed: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("expected 1 recovered on second pass, got %d", recovered)
	}
	if consumer.PendingScanRetriesCount() != 0 {
		t.Fatalf("expected 0 pending scan retries after recovery, got %d", consumer.PendingScanRetriesCount())
	}
}

// 25. Test scenario: Missing or expired message during recovery
// -> drops retry obligation cleanly without infinite retry loop.
func TestConsumer_Recovery_MissingMessage_CleansUpRetryObligation(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_missing_retry.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	consumerName := fmt.Sprintf("test-missing-clean-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      3,
		QuarantineStore: store,
	}, &mockAlwaysFailingHandler{}, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	// 1. Advisory for non-existent sequence
	advPayload := fmt.Sprintf(`{"type":"io.nats.jetstream.advisory.v1.max_deliver","stream":"%s","consumer":"%s","stream_seq":8888,"deliveries":3}`,
		streamName, consumerName)
	consumer.handleAdvisoryData([]byte(advPayload), nil)
	// Missing message must not be tracked in pendingExhausted
	if consumer.PendingRecoveryCount() != 0 {
		t.Fatalf("expected 0 pending recovery items for missing sequence, got %d", consumer.PendingRecoveryCount())
	}

	// 2. Put a synthetic retry obligation for sequence 8888 in pendingScanRetries
	consumer.recoveryMu.Lock()
	consumer.pendingScanRetries[8888] = &scanRetryItem{
		seq:         8888,
		attempts:    1,
		lastAttempt: time.Now().Add(-1 * time.Second),
		lastErr:     errors.New("initial transient error"),
	}
	consumer.recoveryMu.Unlock()

	if consumer.PendingScanRetriesCount() != 1 {
		t.Fatalf("expected 1 pending scan retry, got %d", consumer.PendingScanRetriesCount())
	}

	// Run RecoverUnacknowledged - sequence 8888 will return jetstream.ErrMsgNotFound
	// and should be removed from pendingScanRetries
	ctx := context.Background()
	_, _ = consumer.RecoverUnacknowledged(ctx)

	if consumer.PendingScanRetriesCount() != 0 {
		t.Fatalf("expected missing sequence to be dropped from pendingScanRetries, got %d", consumer.PendingScanRetriesCount())
	}
}

// 26. Test scenario: Advisory payload validation
// -> malformed, mismatched, zero sequence, and sub-threshold deliveries are rejected.
func TestConsumer_Advisory_PayloadValidation(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_adv_validation.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	consumerName := "test-validation-cons"
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      5,
		QuarantineStore: store,
	}, &mockAlwaysFailingHandler{}, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	cases := []struct {
		name    string
		payload string
		wantErr string
	}{
		{
			name:    "Malformed JSON",
			payload: `{"bad json`,
			wantErr: "malformed advisory json",
		},
		{
			name:    "Zero stream sequence",
			payload: `{"stream":"AEGISEDGE_TELEMETRY","consumer":"test-validation-cons","stream_seq":0,"deliveries":5}`,
			wantErr: "missing or zero stream_seq",
		},
		{
			name:    "Mismatched stream",
			payload: `{"stream":"OTHER_STREAM","consumer":"test-validation-cons","stream_seq":10,"deliveries":5}`,
			wantErr: "mismatched stream",
		},
		{
			name:    "Mismatched consumer",
			payload: `{"stream":"AEGISEDGE_TELEMETRY","consumer":"other-cons","stream_seq":10,"deliveries":5}`,
			wantErr: "mismatched consumer",
		},
		{
			name:    "Deliveries below MaxDeliver",
			payload: `{"stream":"AEGISEDGE_TELEMETRY","consumer":"test-validation-cons","stream_seq":10,"deliveries":3}`,
			wantErr: "below configured MaxDeliver threshold",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := consumer.validateAdvisory([]byte(tc.payload))
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("expected error %q to contain %q", err.Error(), tc.wantErr)
			}
			// Also ensure handleAdvisoryData does not record or crash
			consumer.handleAdvisoryData([]byte(tc.payload), nil)
			if consumer.hasExhaustionEvidence(10) {
				t.Errorf("rejected advisory must not register exhaustion evidence")
			}
		})
	}
}

// 27. Test scenario: Duplicate advisory replay is idempotent
// -> replaying the same advisory does not duplicate or corrupt quarantine records.
func TestConsumer_Advisory_DuplicateReplay_Idempotent(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_adv_idemp.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	consumerName := fmt.Sprintf("test-adv-idemp-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      3,
		QuarantineStore: store,
	}, &mockAlwaysFailingHandler{}, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	// Publish message
	batch := &types.TelemetryBatch{
		BatchID:        "b-idemp-1",
		NodeID:         "node-idemp",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "m1", Value: 10.0, Timestamp: time.Now().UTC()}},
	}
	publishTestEvent(t, nc, batch)
	seq := uint64(1)

	advPayload := fmt.Appendf(nil, `{"type":"io.nats.jetstream.advisory.v1.max_deliver","stream":"%s","consumer":"%s","stream_seq":%d,"deliveries":3}`,
		streamName, consumerName, seq)

	// Replay 1
	consumer.handleAdvisoryData(advPayload, nil)
	// Replay 2
	consumer.handleAdvisoryData(advPayload, nil)
	// Replay 3
	consumer.handleAdvisoryData(advPayload, nil)

	ctx := context.Background()
	recs, err := store.ListQuarantineRecords(ctx, storage.QuarantineFilter{})
	if err != nil {
		t.Fatalf("ListQuarantineRecords failed: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected exactly 1 quarantined record after duplicate replays, got %d", len(recs))
	}
	if recs[0].BatchID != "b-idemp-1" {
		t.Errorf("expected batch-idemp-1, got %s", recs[0].BatchID)
	}
	if recs[0].Attempts < 3 {
		t.Errorf("expected attempts to be incremented on repeat quarantining, got %d", recs[0].Attempts)
	}
}

// 28. Test scenario: Shutdown waits for both consumeLoop and recoveryWorkerLoop
// -> neither goroutine leaks or panics on shutdown.
func TestConsumer_Shutdown_TracksBothLoops_CleanExit(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_dual_shutdown.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	consumerName := fmt.Sprintf("test-dual-shutdown-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      3,
		QuarantineStore: store,
	}, &mockAlwaysFailingHandler{}, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}

	if err := consumer.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Verify consumer is running
	if !consumer.running.Load() {
		t.Fatalf("expected consumer to be marked running")
	}

	stopped := make(chan struct{})
	go func() {
		consumer.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
		// Clean exit
	case <-time.After(5 * time.Second):
		t.Fatalf("Stop() timed out waiting for both recoveryWorkerLoop and consumeLoop")
	}

	if consumer.running.Load() {
		t.Fatalf("expected running to be false after Stop()")
	}

	// Calling Stop again must be a clean idempotent no-op
	consumer.Stop()
}

// 29. Test scenario: First-run infrastructure provisioning and idempotent repeated provisioning
func TestConsumer_AdvisoryStream_ProvisioningIdempotency(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_prov_idemp.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	consumerName := fmt.Sprintf("test-prov-idemp-%d", time.Now().UnixNano())
	cfg := ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		QuarantineStore: store,
	}

	// First initialization: creates stream and consumer from scratch
	c1, err := NewTelemetryConsumer(cfg, &mockAlwaysFailingHandler{}, silentLogger())
	if err != nil {
		t.Fatalf("first NewTelemetryConsumer failed: %v", err)
	}
	defer c1.Stop()

	// Inspect created advisory stream
	advStream, err := js.Stream(context.Background(), c1.config.AdvisoryStreamName)
	if err != nil {
		t.Fatalf("failed to query created advisory stream: %v", err)
	}
	sInfo, err := advStream.Info(context.Background())
	if err != nil {
		t.Fatalf("failed to get advisory stream info: %v", err)
	}
	if sInfo.Config.Retention != jetstream.LimitsPolicy {
		t.Errorf("expected LimitsPolicy, got %v", sInfo.Config.Retention)
	}
	if sInfo.Config.Discard != jetstream.DiscardOld {
		t.Errorf("expected DiscardOld, got %v", sInfo.Config.Discard)
	}
	if sInfo.Config.MaxAge != 7*24*time.Hour {
		t.Errorf("expected 7 days MaxAge, got %v", sInfo.Config.MaxAge)
	}

	// Inspect created advisory consumer
	advCons, err := advStream.Consumer(context.Background(), c1.config.AdvisoryConsumerName)
	if err != nil {
		t.Fatalf("failed to query created advisory consumer: %v", err)
	}
	cInfo, err := advCons.Info(context.Background())
	if err != nil {
		t.Fatalf("failed to get advisory consumer info: %v", err)
	}
	if cInfo.Config.AckPolicy != jetstream.AckExplicitPolicy {
		t.Errorf("expected AckExplicitPolicy, got %v", cInfo.Config.AckPolicy)
	}
	if cInfo.Config.MaxDeliver != -1 {
		t.Errorf("expected MaxDeliver: -1 (unlimited), got %d", cInfo.Config.MaxDeliver)
	}

	c1.Stop()

	// Second initialization: connects to existing compatible stream and consumer idempotently
	c2, err := NewTelemetryConsumer(cfg, &mockAlwaysFailingHandler{}, silentLogger())
	if err != nil {
		t.Fatalf("second NewTelemetryConsumer failed to connect to existing stream idempotently: %v", err)
	}
	defer c2.Stop()
}

// 30. Test scenario: Existing advisory stream and consumer configuration validation
func TestConsumer_AdvisoryStream_ExistingConfigValidation(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_cfg_val.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	// Test A: Incompatible subjects
	incompatSubjStream := "INCOMPAT_SUBJ_STREAM"
	_, err = js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:      incompatSubjStream,
		Subjects:  []string{"unrelated.subject.>"},
		Retention: jetstream.LimitsPolicy,
		Discard:   jetstream.DiscardOld,
	})
	if err != nil {
		t.Fatalf("create stream failed: %v", err)
	}

	_, err = NewTelemetryConsumer(ConsumerConfig{
		NATSURL:            natsURL,
		StreamName:         streamName,
		ConsumerName:       "test-val-c1",
		AdvisoryStreamName: incompatSubjStream,
		QuarantineStore:    store,
	}, &mockAlwaysFailingHandler{}, silentLogger())
	if err == nil || !strings.Contains(err.Error(), "do not cover required subject pattern") {
		t.Fatalf("expected incompatible subjects error, got: %v", err)
	}

	// Test B: Incompatible retention policy (InterestPolicy)
	incompatRetStream := "INCOMPAT_RET_STREAM"
	_, err = js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:      incompatRetStream,
		Subjects:  []string{fmt.Sprintf("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.%s.>", streamName)},
		Retention: jetstream.InterestPolicy,
		Discard:   jetstream.DiscardOld,
	})
	if err != nil {
		t.Fatalf("create stream failed: %v", err)
	}

	_, err = NewTelemetryConsumer(ConsumerConfig{
		NATSURL:            natsURL,
		StreamName:         streamName,
		ConsumerName:       "test-val-c2",
		AdvisoryStreamName: incompatRetStream,
		QuarantineStore:    store,
	}, &mockAlwaysFailingHandler{}, silentLogger())
	if err == nil || !strings.Contains(err.Error(), "expected LimitsPolicy") {
		t.Fatalf("expected incompatible retention error, got: %v", err)
	}
	_ = js.DeleteStream(context.Background(), incompatRetStream)

	// Test C: Incompatible discard policy (DiscardNew)
	incompatDiscardStream := "INCOMPAT_DISCARD_STREAM"
	_, err = js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:      incompatDiscardStream,
		Subjects:  []string{fmt.Sprintf("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.%s.>", streamName)},
		Retention: jetstream.LimitsPolicy,
		Discard:   jetstream.DiscardNew,
	})
	if err != nil {
		t.Fatalf("create stream failed: %v", err)
	}

	_, err = NewTelemetryConsumer(ConsumerConfig{
		NATSURL:            natsURL,
		StreamName:         streamName,
		ConsumerName:       "test-val-c3",
		AdvisoryStreamName: incompatDiscardStream,
		QuarantineStore:    store,
	}, &mockAlwaysFailingHandler{}, silentLogger())
	if err == nil || !strings.Contains(err.Error(), "expected DiscardOld") {
		t.Fatalf("expected incompatible discard error, got: %v", err)
	}
	_ = js.DeleteStream(context.Background(), incompatDiscardStream)

	// Test D: Incompatible consumer filter subject
	compatStream := "COMPAT_STREAM"
	_, err = js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:      compatStream,
		Subjects:  []string{fmt.Sprintf("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.%s.>", streamName)},
		Retention: jetstream.LimitsPolicy,
		Discard:   jetstream.DiscardOld,
	})
	if err != nil {
		t.Fatalf("create stream failed: %v", err)
	}

	incompatConsName := "incompat-filter-cons"
	_, err = js.CreateOrUpdateConsumer(context.Background(), compatStream, jetstream.ConsumerConfig{
		Durable:       incompatConsName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: "wrong.subject.filter",
	})
	if err != nil {
		t.Fatalf("create consumer failed: %v", err)
	}

	_, err = NewTelemetryConsumer(ConsumerConfig{
		NATSURL:              natsURL,
		StreamName:           streamName,
		ConsumerName:         "test-val-c4",
		AdvisoryStreamName:   compatStream,
		AdvisoryConsumerName: incompatConsName,
		QuarantineStore:      store,
	}, &mockAlwaysFailingHandler{}, silentLogger())
	if err == nil || !strings.Contains(err.Error(), "does not match required") {
		t.Fatalf("expected incompatible consumer filter error, got: %v", err)
	}
}

// 31. Test scenario: Advisory delivery failures beyond previous delivery limit (10), followed by recovery
func TestConsumer_Advisory_DeliveryBeyondFormerLimit_RecoversWhenStoreRestored(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_adv_resilience.db")
	realStore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer realStore.Close()

	mockStore := newMockFailingQuarantineStore(realStore)
	mockStore.shouldFail.Store(true) // Storage is initially down

	consumerName := fmt.Sprintf("test-resilient-adv-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      3,
		QuarantineStore: mockStore,
	}, &mockAlwaysFailingHandler{}, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	// Publish valid telemetry event to stream
	batch := &types.TelemetryBatch{
		BatchID:        "batch-resilient-1",
		NodeID:         "node-resilient",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "m1", Value: 100.0, Timestamp: time.Now().UTC()}},
	}
	publishTestEvent(t, nc, batch)
	seq := uint64(1)

	advPayload := fmt.Appendf(nil, `{"type":"io.nats.jetstream.advisory.v1.max_deliver","stream":"%s","consumer":"%s","stream_seq":%d,"deliveries":3}`,
		streamName, consumerName, seq)

	// Simulate 12 failures (exceeding former MaxDeliver limit of 10)
	for attempt := 1; attempt <= 12; attempt++ {
		consumer.handleAdvisoryData(advPayload, nil)
	}

	// Obligation must not be lost or deleted
	if consumer.PendingRecoveryCount() != 1 {
		t.Fatalf("expected 1 pending recovery item after 12 failures, got %d", consumer.PendingRecoveryCount())
	}

	// Restore database health
	mockStore.shouldFail.Store(false)

	// Process recovery pass - must succeed without consumer recreation
	consumer.processPendingRecoveryPass()

	if consumer.PendingRecoveryCount() != 0 {
		t.Fatalf("expected 0 pending recovery items after store restore, got %d", consumer.PendingRecoveryCount())
	}

	recs, err := realStore.ListQuarantineRecords(context.Background(), storage.QuarantineFilter{})
	if err != nil {
		t.Fatalf("ListQuarantineRecords failed: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 record in quarantine store, got %d", len(recs))
	}
	if recs[0].BatchID != "batch-resilient-1" {
		t.Errorf("expected batch-resilient-1, got %s", recs[0].BatchID)
	}
	if recs[0].FailureCategory != storage.CategoryDeliveryExhausted {
		t.Errorf("expected CategoryDeliveryExhausted, got %s", recs[0].FailureCategory)
	}
}

// 32. Test scenario: Advisory stream capacity exhaustion and eviction detection under DiscardOld
func TestConsumer_AdvisoryStream_CapacityExhaustionAndEvictionMetrics(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_adv_capacity.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}

	advStreamName := "AEGISEDGE_TEST_CAPACITY_ADVISORIES"
	advSubject := fmt.Sprintf("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.%s.>", streamName)

	// Pre-create small advisory stream with MaxMsgs=3 and DiscardOld
	_, err = js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:      advStreamName,
		Subjects:  []string{advSubject},
		Retention: jetstream.LimitsPolicy,
		MaxMsgs:   3,
		Discard:   jetstream.DiscardOld,
	})
	if err != nil {
		t.Fatalf("failed to create small advisory stream: %v", err)
	}

	consumerName := fmt.Sprintf("test-cap-cons-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:            natsURL,
		StreamName:         streamName,
		ConsumerName:       consumerName,
		AdvisoryStreamName: advStreamName,
		MaxDeliver:         3,
		QuarantineStore:    store,
	}, &mockAlwaysFailingHandler{}, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	// Initial health inspection: 0 messages, 0 evictions
	consumer.inspectAdvisoryStreamHealth(context.Background())
	if msgs := consumer.AdvisoryStreamMessages(); msgs != 0 {
		t.Errorf("expected 0 initial messages, got %d", msgs)
	}

	// Publish 6 advisories to subject using JetStream publish to wait for stream ack
	targetSubj := fmt.Sprintf("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.%s.%s", streamName, consumerName)
	for i := 1; i <= 6; i++ {
		payload := fmt.Sprintf(`{"type":"io.nats.jetstream.advisory.v1.max_deliver","stream":"%s","consumer":"%s","stream_seq":%d,"deliveries":3}`,
			streamName, consumerName, i)
		ack, pErr := js.Publish(context.Background(), targetSubj, []byte(payload))
		if pErr != nil {
			t.Fatalf("publish advisory %d failed: %v", i, pErr)
		}
		t.Logf("Published advisory %d: stream=%s, seq=%d", i, ack.Stream, ack.Sequence)
	}

	// Run health inspection
	consumer.inspectAdvisoryStreamHealth(context.Background())

	// Stream messages must be capped at 3 due to DiscardOld
	if msgs := consumer.AdvisoryStreamMessages(); msgs != 3 {
		t.Errorf("expected 3 stream messages after saturation, got %d", msgs)
	}

	// Evictions must detect that sequences 1, 2, 3 were evicted
	if evictions := consumer.AdvisoryEvictions(); evictions < 3 {
		t.Errorf("expected at least 3 evictions detected, got %d", evictions)
	}
}

// 33. Test scenario: Missing/evicted advisory does not prevent direct authoritative quarantine at MaxDeliver
func TestConsumer_Quarantine_MissingOrEvictedAdvisory_DirectAuthoritativeQuarantine(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_direct_auth.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	handler := &mockAlwaysFailingHandler{}
	consumerName := fmt.Sprintf("test-direct-auth-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      2, // MaxDeliver = 2
		RetryBackoff:    20 * time.Millisecond,
		QuarantineStore: store,
	}, handler, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	if err := consumer.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	batch := &types.TelemetryBatch{
		BatchID:        fmt.Sprintf("batch-direct-auth-%d", time.Now().UnixNano()),
		NodeID:         "node-direct-auth",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "cpu", Value: 85.0, Timestamp: time.Now().UTC()}},
	}
	publishTestEvent(t, nc, batch)

	// Await direct authoritative quarantine at MaxDeliver
	deadline := time.After(4 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out awaiting direct quarantine; calls=%d, exhausted=%d, quarantined=%d",
				handler.calls.Load(), consumer.ExhaustedDeliveries(), consumer.Quarantined())
		default:
			if consumer.ExhaustedDeliveries() >= 1 && consumer.Quarantined() >= 1 {
				goto directQuarantineDone
			}
			time.Sleep(30 * time.Millisecond)
		}
	}

directQuarantineDone:
	// Verify quarantine record committed into durable SQLite
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	records, err := store.ListQuarantineRecords(ctx, storage.QuarantineFilter{})
	if err != nil {
		t.Fatalf("ListQuarantineRecords failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 quarantine record committed directly, got %d", len(records))
	}
	if records[0].FailureCategory != storage.CategoryDeliveryExhausted {
		t.Errorf("expected CategoryDeliveryExhausted, got %s", records[0].FailureCategory)
	}
	if records[0].BatchID != batch.BatchID {
		t.Errorf("expected batch ID %s, got %s", batch.BatchID, records[0].BatchID)
	}

	// Verify message was terminated (termMessage) and delivery attempts ceased
	time.Sleep(100 * time.Millisecond)
	callsBefore := handler.calls.Load()
	time.Sleep(200 * time.Millisecond)
	if handler.calls.Load() != callsBefore {
		t.Errorf("handler continued receiving deliveries after termination: calls before %d, now %d",
			callsBefore, handler.calls.Load())
	}
}

// 34. Test scenario: Advisory fetch error is observable in metrics and reconciliation resumes
func TestConsumer_Advisory_TransientFetchError_ObservableAndResumes(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_adv_fetch_err.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	consumerName := fmt.Sprintf("test-adv-fetch-err-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      3,
		QuarantineStore: store,
	}, &mockAlwaysFailingHandler{}, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	// Initial fetch error count should be 0
	if errs := consumer.AdvisoryFetchErrors(); errs != 0 {
		t.Errorf("expected 0 initial fetch errors, got %d", errs)
	}

	// Delete the underlying consumer from JetStream to trigger a real fetch error
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	_ = js.DeleteConsumer(context.Background(), consumer.config.AdvisoryStreamName, consumer.config.AdvisoryConsumerName)

	// Attempt reconciliation - must record fetch error instead of failing silently
	consumer.reconcileDurableAdvisories(context.Background())
	if errs := consumer.AdvisoryFetchErrors(); errs < 1 {
		t.Errorf("expected at least 1 advisory fetch error recorded, got %d", errs)
	}
}

// 35. Test scenario: Quarantine persistence failure at MaxDeliver followed by restart and successful recovery through a retained durable advisory.
func TestConsumer_RecoveryBoundary_PersistenceFailure_RestartAndRecoverViaDurableAdvisory(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_adv_restart.db")
	realStore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer realStore.Close()

	mockStore := newMockFailingQuarantineStore(realStore)
	mockStore.shouldFail.Store(true) // Quarantine storage fails initially

	consumerName := fmt.Sprintf("test-adv-restart-%d", time.Now().UnixNano())
	cfg := ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      2,
		AckWait:         500 * time.Millisecond,
		RetryBackoff:    20 * time.Millisecond,
		QuarantineStore: mockStore,
	}

	handler := &mockAlwaysFailingHandler{}
	consumer1, err := NewTelemetryConsumer(cfg, handler, silentLogger())
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
		Metrics:        []types.MetricSample{{Name: "m1", Value: 42.0, Timestamp: time.Now().UTC()}},
	}
	publishTestEvent(t, nc, batch)

	// Await delivery exhaustion where quarantine write failed
	deadline := time.After(4 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for exhaustion; calls=%d, exhausted=%d",
				handler.calls.Load(), consumer1.ExhaustedDeliveries())
		default:
			if consumer1.ExhaustedDeliveries() >= 1 && consumer1.PendingRecoveryCount() >= 1 {
				goto exhaustionReached
			}
			time.Sleep(30 * time.Millisecond)
		}
	}

exhaustionReached:
	// Await the broker emitting and storing the advisory in the durable advisory stream
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	advStream, err := js.Stream(context.Background(), consumer1.config.AdvisoryStreamName)
	if err != nil {
		t.Fatalf("failed to get advisory stream: %v", err)
	}
	advDeadline := time.After(4 * time.Second)
	for {
		sInfo, sErr := advStream.Info(context.Background())
		if sErr == nil && sInfo.State.Msgs >= 1 {
			break
		}
		select {
		case <-advDeadline:
			t.Fatalf("timed out awaiting broker advisory in stream; err=%v, state=%+v", sErr, sInfo)
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

	// Verify that quarantine persistence did NOT succeed in realStore yet
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	records, err := realStore.ListQuarantineRecords(ctx, storage.QuarantineFilter{})
	if err != nil {
		t.Fatalf("ListQuarantineRecords failed: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected 0 quarantine records before restart, got %d", len(records))
	}

	// Simulate process crash: Stop consumer1 completely.
	// This wipes consumer1's in-memory pendingExhausted map!
	consumer1.Stop()

	// Restore database health
	mockStore.shouldFail.Store(false)

	// Start consumer2 (representing process restart after crash)
	cfg.QuarantineStore = realStore
	cfg.AckWait = 500 * time.Millisecond
	consumer2, err := NewTelemetryConsumer(cfg, handler, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer 2 failed: %v", err)
	}
	defer consumer2.Stop()

	// Initial in-memory state of restarted process is clean
	if consumer2.PendingRecoveryCount() != 0 {
		t.Fatalf("expected 0 initial pending recovery items in restarted consumer, got %d", consumer2.PendingRecoveryCount())
	}

	// Start consumer2 background loops to process durable advisories
	if err := consumer2.Start(); err != nil {
		t.Fatalf("consumer2 Start failed: %v", err)
	}

	// Await recovery through the retained durable advisory
	recDeadline := time.After(6 * time.Second)
	recovered := false
	for {
		records, err = realStore.ListQuarantineRecords(context.Background(), storage.QuarantineFilter{})
		if err == nil && len(records) == 1 {
			recovered = true
			break
		}
		select {
		case <-recDeadline:
			goto recDone
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

recDone:
	if !recovered {
		t.Fatalf("failed to recover message through retained durable advisory after restart; records in DB=%d", len(records))
	}

	if records[0].BatchID != batch.BatchID {
		t.Errorf("expected recovered batch ID %s, got %s", batch.BatchID, records[0].BatchID)
	}
	if records[0].FailureCategory != storage.CategoryDeliveryExhausted {
		t.Errorf("expected CategoryDeliveryExhausted, got %s", records[0].FailureCategory)
	}
}

// 36. Test scenario: Compound failure limitation when persistence fails AND no recoverable advisory remains.
// Demonstrates that schema-valid messages are protected from speculative quarantine, requiring explicit operator intervention.
func TestConsumer_RecoveryBoundary_CompoundFailure_AdvisoryMissingRequiresIntervention(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_compound_fail.db")
	realStore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer realStore.Close()

	mockStore := newMockFailingQuarantineStore(realStore)
	mockStore.shouldFail.Store(true)

	consumerName := fmt.Sprintf("test-compound-%d", time.Now().UnixNano())
	cfg := ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    consumerName,
		MaxDeliver:      2,
		AckWait:         500 * time.Millisecond,
		RetryBackoff:    20 * time.Millisecond,
		QuarantineStore: mockStore,
	}

	consumer1, err := NewTelemetryConsumer(cfg, &mockAlwaysFailingHandler{}, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer 1 failed: %v", err)
	}

	if err := consumer1.Start(); err != nil {
		t.Fatalf("Start 1 failed: %v", err)
	}

	batch := &types.TelemetryBatch{
		BatchID:        fmt.Sprintf("batch-compound-%d", time.Now().UnixNano()),
		NodeID:         "node-compound",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "m1", Value: 55.0, Timestamp: time.Now().UTC()}},
	}
	publishTestEvent(t, nc, batch)

	// Wait until delivery exhaustion attempt occurs
	deadline := time.After(4 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out awaiting exhaustion")
		default:
			if consumer1.ExhaustedDeliveries() >= 1 {
				goto exhausted
			}
			time.Sleep(30 * time.Millisecond)
		}
	}

exhausted:
	// Process crash: Stop consumer 1
	consumer1.Stop()

	// Now simulate the missing advisory condition: purge the advisory stream
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	advStream, err := js.Stream(context.Background(), consumer1.config.AdvisoryStreamName)
	if err == nil {
		_ = advStream.Purge(context.Background())
	}

	// Restart consumer 2 with healthy store
	cfg.QuarantineStore = realStore
	consumer2, err := NewTelemetryConsumer(cfg, &mockAlwaysFailingHandler{}, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer 2 failed: %v", err)
	}
	defer consumer2.Stop()

	// Step 1: Automated unacknowledged scan safely refuses to quarantine schema-valid message
	// because neither in-memory nor broker advisory evidence exists for seq=1.
	seq := uint64(1)
	ctx := context.Background()
	_, rErr := consumer2.RecoverStreamSequence(ctx, seq)
	if !errors.Is(rErr, ErrNotEligible) {
		t.Fatalf("expected ErrNotEligible protecting in-flight message without advisory evidence, got: %v", rErr)
	}

	// Verify database is still empty (no speculative quarantine)
	recs, err := realStore.ListQuarantineRecords(ctx, storage.QuarantineFilter{})
	if err != nil || len(recs) != 0 {
		t.Fatalf("expected 0 quarantine records, got %d (err: %v)", len(recs), err)
	}

	// Step 2: Operator marks sequence exhausted (external authoritative intervention)
	consumer2.MarkSequenceExhausted(seq)

	// Now recovery succeeds authoritatively
	saved, rErr := consumer2.RecoverStreamSequence(ctx, seq)
	if rErr != nil {
		t.Fatalf("expected recovery to succeed after operator intervention, got: %v", rErr)
	}
	if saved.BatchID != batch.BatchID {
		t.Errorf("expected recovered batch ID %s, got %s", batch.BatchID, saved.BatchID)
	}
}

// 37. Test scenario: Correct ACK / NAK / TERM ordering around durable persistence.
func TestConsumer_Ordering_ACK_NAK_TERM_AroundDurablePersistence(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_ordering.db")
	realStore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer realStore.Close()

	// Part A: Successful ingestion -> ACK only after durable write
	batch := &types.TelemetryBatch{
		BatchID:        "batch-ordering-success",
		NodeID:         "node-order",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "m1", Value: 10.0, Timestamp: time.Now().UTC()}},
	}

	consumerName := fmt.Sprintf("test-order-ack-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:          natsURL,
		StreamName:       streamName,
		ConsumerName:     consumerName,
		QuarantineStore:  realStore,
		IdempotencyStore: realStore,
	}, server.NewServer(silentLogger(), server.WithIdempotencyStore(realStore)), silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	if err := consumer.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	publishTestEvent(t, nc, batch)

	// Await consumption
	deadline := time.After(3 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for consumption")
		default:
			if consumer.Consumed() >= 1 {
				goto successConsumed
			}
			time.Sleep(30 * time.Millisecond)
		}
	}

successConsumed:
	has, err := realStore.HasBatch(context.Background(), batch.NodeID, batch.BatchID)
	if err != nil || !has {
		t.Fatalf("batch not durably ingested prior to ACK: has=%v, err=%v", has, err)
	}

	// Part B: Poison pill with storage failure -> NAK (with delay) instead of premature TERM
	mockStore := newMockFailingQuarantineStore(realStore)
	mockStore.shouldFail.Store(true)

	poisonConsumerName := fmt.Sprintf("test-order-poison-%d", time.Now().UnixNano())
	poisonConsumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:         natsURL,
		StreamName:      streamName,
		ConsumerName:    poisonConsumerName,
		MaxDeliver:      3,
		QuarantineStore: mockStore,
	}, &mockAlwaysFailingHandler{}, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer poison failed: %v", err)
	}
	defer poisonConsumer.Stop()

	if err := poisonConsumer.Start(); err != nil {
		t.Fatalf("Start poison failed: %v", err)
	}

	// Publish malformed poison JSON
	_, err = nc.Request(fmt.Sprintf("aegisedge.v1.telemetry.%s", batch.NodeID), []byte(`{invalid json`), 2*time.Second)
	_ = err // Request might time out since consumer does not reply, which is expected

	// Wait for delivery attempts with storage failure
	time.Sleep(500 * time.Millisecond)
	if poisonConsumer.Quarantined() != 0 {
		t.Errorf("poison message was marked quarantined despite storage write failure: %d", poisonConsumer.Quarantined())
	}
}

// 38. Test scenario: processPendingRecoveryPass recognizes ErrAlreadyIngested and immediately clears obligation without infinite retry loop
func TestConsumer_Recovery_PendingExhausted_AlreadyIngestedClearsObligation(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_already_ingested_clear.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	consumerName := fmt.Sprintf("test-clear-already-ingested-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:          natsURL,
		StreamName:       streamName,
		ConsumerName:     consumerName,
		QuarantineStore:  store,
		IdempotencyStore: store,
	}, &mockAlwaysFailingHandler{}, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	// Pre-ingest batch into store
	nodeID := "node-clear-test"
	batchID := "batch-clear-test"
	batch := &types.TelemetryBatch{
		NodeID:         nodeID,
		BatchID:        batchID,
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "m1", Value: 10.0, Timestamp: time.Now().UTC()}},
	}
	if _, err := store.RecordBatch(context.Background(), batch, time.Now().UTC()); err != nil {
		t.Fatalf("RecordBatch failed: %v", err)
	}

	// Enqueue recovery for this batch in pendingExhausted
	seq := uint64(42)
	consumer.enqueueRecovery(&seq, 3, storage.CategoryDeliveryExhausted, "exhausted delivery", "aegisedge.v1.telemetry."+nodeID, nodeID, batchID, []byte(`{}`))
	if consumer.PendingRecoveryCount() != 1 {
		t.Fatalf("expected 1 pending recovery item, got %d", consumer.PendingRecoveryCount())
	}

	// Run recovery pass
	consumer.processPendingRecoveryPass()

	// Obligation must be cleared immediately (not retried) because batch was already ingested
	if consumer.PendingRecoveryCount() != 0 {
		t.Fatalf("expected 0 pending recovery items after already-ingested resolution, got %d", consumer.PendingRecoveryCount())
	}

	// Verify no quarantine record was inserted
	records, err := store.ListQuarantineRecords(context.Background(), storage.QuarantineFilter{})
	if err != nil || len(records) != 0 {
		t.Fatalf("expected 0 quarantine records, got %d (err: %v)", len(records), err)
	}
}

// deliverAndNak fetches messages once through the consumer and NAKs them so NumAckPending > 0
func deliverAndNak(t *testing.T, consumer *TelemetryConsumer, count int) {
	t.Helper()
	msgs, err := consumer.cons.Fetch(count, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	for msg := range msgs.Messages() {
		_ = msg.Nak()
	}
}

// 39. Test scenario: Unacknowledged sequence returning ErrNotEligible is tracked and gauge updated without duplicates
func TestConsumer_StalledUnacknowledged_TrackedAndGaugeUpdated(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_stalled_track.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	consumerName := fmt.Sprintf("test-stalled-track-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:          natsURL,
		StreamName:       streamName,
		ConsumerName:     consumerName,
		QuarantineStore:  store,
		IdempotencyStore: store,
	}, &mockAlwaysFailingHandler{}, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	// Publish a valid telemetry event
	batch := &types.TelemetryBatch{
		NodeID:         "node-stalled-1",
		BatchID:        "batch-stalled-1",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "m1", Value: 10.0, Timestamp: time.Now().UTC()}},
	}
	publishTestEvent(t, nc, batch)
	deliverAndNak(t, consumer, 1)

	ctx := context.Background()

	// Initial scan: sequence 1 is unacknowledged, valid, not ingested, and has no exhaustion evidence
	recovered, err := consumer.RecoverUnacknowledged(ctx)
	if err != nil {
		t.Fatalf("RecoverUnacknowledged failed: %v", err)
	}
	if recovered != 0 {
		t.Fatalf("expected 0 recovered, got %d", recovered)
	}

	// Verify sequence 1 is tracked and gauge updated
	if consumer.StalledUnacknowledgedSequences() != 1 {
		t.Fatalf("expected StalledUnacknowledgedSequences=1, got %d", consumer.StalledUnacknowledgedSequences())
	}
	stalled := consumer.StalledSequences()
	if len(stalled) != 1 {
		t.Fatalf("expected 1 stalled sequence item, got %d", len(stalled))
	}
	if stalled[0].StreamSequence != 1 {
		t.Errorf("expected tracked stream_seq=1, got %d", stalled[0].StreamSequence)
	}

	// Repeated scan: must not create duplicate entries
	recovered, err = consumer.RecoverUnacknowledged(ctx)
	if err != nil {
		t.Fatalf("second RecoverUnacknowledged failed: %v", err)
	}
	if recovered != 0 {
		t.Fatalf("expected 0 recovered on second scan, got %d", recovered)
	}
	if consumer.StalledUnacknowledgedSequences() != 1 {
		t.Fatalf("expected StalledUnacknowledgedSequences to remain 1, got %d", consumer.StalledUnacknowledgedSequences())
	}
	stalled = consumer.StalledSequences()
	if len(stalled) != 1 {
		t.Fatalf("expected 1 stalled item, got %d", len(stalled))
	}
}

// 40a. Test scenario: Resolution via operator recovery clears stalled tracking and updates gauge
func TestConsumer_StalledUnacknowledged_ResolutionViaRecovery(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_stalled_res_rec.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	consumerName := fmt.Sprintf("test-stalled-res-rec-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:          natsURL,
		StreamName:       streamName,
		ConsumerName:     consumerName,
		QuarantineStore:  store,
		IdempotencyStore: store,
	}, &mockAlwaysFailingHandler{}, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	batch1 := &types.TelemetryBatch{
		NodeID:         "node-res-1",
		BatchID:        "batch-res-1",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "m1", Value: 10.0, Timestamp: time.Now().UTC()}},
	}
	publishTestEvent(t, nc, batch1)
	deliverAndNak(t, consumer, 1)

	ctx := context.Background()
	_, _ = consumer.RecoverUnacknowledged(ctx)
	if consumer.StalledUnacknowledgedSequences() != 1 {
		t.Fatalf("expected 1 stalled sequence, got %d", consumer.StalledUnacknowledgedSequences())
	}

	// Operator manually forces quarantine
	saved, rErr := consumer.RecoverStreamSequence(ctx, 1, WithForceExhaustion(true))
	if rErr != nil {
		t.Fatalf("RecoverStreamSequence failed: %v", rErr)
	}
	if saved == nil {
		t.Fatal("expected non-nil saved quarantine record")
	}

	// Stalled entry must be immediately cleared and gauge decremented to 0
	if consumer.StalledUnacknowledgedSequences() != 0 {
		t.Fatalf("expected StalledUnacknowledgedSequences=0 after recovery, got %d", consumer.StalledUnacknowledgedSequences())
	}
	if len(consumer.StalledSequences()) != 0 {
		t.Fatalf("expected 0 stalled items after recovery, got %d", len(consumer.StalledSequences()))
	}
}

// 40b. Test scenario: Resolution via prior batch ingestion clears stalled tracking and updates gauge
func TestConsumer_StalledUnacknowledged_ResolutionViaIngestion(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_stalled_res_ing.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	consumerName := fmt.Sprintf("test-stalled-res-ing-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:          natsURL,
		StreamName:       streamName,
		ConsumerName:     consumerName,
		QuarantineStore:  store,
		IdempotencyStore: store,
	}, &mockAlwaysFailingHandler{}, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	batch2 := &types.TelemetryBatch{
		NodeID:         "node-res-2",
		BatchID:        "batch-res-2",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "m1", Value: 20.0, Timestamp: time.Now().UTC()}},
	}
	publishTestEvent(t, nc, batch2)
	deliverAndNak(t, consumer, 1)

	ctx := context.Background()
	_, _ = consumer.RecoverUnacknowledged(ctx)
	if consumer.StalledUnacknowledgedSequences() != 1 {
		t.Fatalf("expected 1 stalled sequence for batch2, got %d", consumer.StalledUnacknowledgedSequences())
	}

	// Batch2 is committed to ingested storage
	if _, err := store.RecordBatch(ctx, batch2, time.Now().UTC()); err != nil {
		t.Fatalf("RecordBatch failed: %v", err)
	}

	// Next scan observes ErrAlreadyIngested and clears tracking
	_, _ = consumer.RecoverUnacknowledged(ctx)
	if consumer.StalledUnacknowledgedSequences() != 0 {
		t.Fatalf("expected StalledUnacknowledgedSequences=0 after batch2 ingestion, got %d", consumer.StalledUnacknowledgedSequences())
	}
	if len(consumer.StalledSequences()) != 0 {
		t.Fatalf("expected 0 stalled items after batch2 ingestion, got %d", len(consumer.StalledSequences()))
	}
}

// 41. Test scenario: Warning threshold and rate limiting prevent log flooding
func TestConsumer_StalledUnacknowledged_WarningThresholdAndRateLimiting(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_stalled_warn.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	var logBuf bytes.Buffer
	textHandler := slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn})
	logger := slog.New(textHandler)

	simulatedTime := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	clockMu := sync.Mutex{}
	clock := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return simulatedTime
	}

	consumerName := fmt.Sprintf("test-stalled-warn-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:                 natsURL,
		StreamName:              streamName,
		ConsumerName:            consumerName,
		QuarantineStore:         store,
		StalledWarningThreshold: 5 * time.Second,
	}, &mockAlwaysFailingHandler{}, logger, WithClock(clock))
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	batch := &types.TelemetryBatch{
		NodeID:         "node-warn-1",
		BatchID:        "batch-warn-1",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "m1", Value: 10.0, Timestamp: time.Now().UTC()}},
	}
	publishTestEvent(t, nc, batch)
	deliverAndNak(t, consumer, 1)

	ctx := context.Background()

	// Initial scan at elapsed=0s (< 5s threshold): no warning emitted
	_, err = consumer.RecoverUnacknowledged(ctx)
	if err != nil {
		t.Fatalf("RecoverUnacknowledged failed: %v", err)
	}
	if logBuf.Len() > 0 {
		t.Fatalf("expected no warning log on initial scan, got: %s", logBuf.String())
	}
	stalled := consumer.StalledSequences()
	if len(stalled) != 1 || stalled[0].WarnEmitted {
		t.Fatalf("expected 1 stalled item with WarnEmitted=false")
	}

	// Advance clock past 5s threshold
	clockMu.Lock()
	simulatedTime = simulatedTime.Add(6 * time.Second)
	clockMu.Unlock()

	// Second scan at elapsed=6s: warning must be emitted on state transition
	_, err = consumer.RecoverUnacknowledged(ctx)
	if err != nil {
		t.Fatalf("second RecoverUnacknowledged failed: %v", err)
	}

	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "unacknowledged stream sequence holding ack floor without authoritative exhaustion evidence") {
		t.Fatalf("expected warning log message, got:\n%s", logOutput)
	}
	if !strings.Contains(logOutput, "stream_seq=1") {
		t.Errorf("expected stream_seq=1 in log output:\n%s", logOutput)
	}
	if !strings.Contains(logOutput, "missing authoritative exhaustion evidence") {
		t.Errorf("expected reason in log output:\n%s", logOutput)
	}
	if !strings.Contains(logOutput, "remediation=") {
		t.Errorf("expected remediation in log output:\n%s", logOutput)
	}
	stalled = consumer.StalledSequences()
	if len(stalled) != 1 || !stalled[0].WarnEmitted {
		t.Fatalf("expected 1 stalled item with WarnEmitted=true")
	}

	// Clear log buffer
	logBuf.Reset()

	// Third scan shortly after: must NOT emit warning again (zero log flooding)
	clockMu.Lock()
	simulatedTime = simulatedTime.Add(1 * time.Second)
	clockMu.Unlock()

	_, err = consumer.RecoverUnacknowledged(ctx)
	if err != nil {
		t.Fatalf("third RecoverUnacknowledged failed: %v", err)
	}
	if logBuf.Len() > 0 {
		t.Fatalf("expected zero log flooding on subsequent scan, got: %s", logBuf.String())
	}
}

// 42. Test scenario: Stalled detection never force-quarantines, ACKs, or TERMs a valid message solely because it is stalled
func TestConsumer_StalledUnacknowledged_SafetyInvariants_NeverSpeculativelyQuarantinedOrAcked(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_stalled_safety.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	consumerName := fmt.Sprintf("test-stalled-safety-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:          natsURL,
		StreamName:       streamName,
		ConsumerName:     consumerName,
		QuarantineStore:  store,
		IdempotencyStore: store,
	}, &mockAlwaysFailingHandler{}, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	batch := &types.TelemetryBatch{
		NodeID:         "node-safety-1",
		BatchID:        "batch-safety-1",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics:        []types.MetricSample{{Name: "m1", Value: 10.0, Timestamp: time.Now().UTC()}},
	}
	publishTestEvent(t, nc, batch)
	deliverAndNak(t, consumer, 1)

	ctx := context.Background()

	// Repeatedly scan 5 times
	for i := 0; i < 5; i++ {
		_, err := consumer.RecoverUnacknowledged(ctx)
		if err != nil {
			t.Fatalf("scan %d failed: %v", i, err)
		}
	}

	// Verify message was NEVER quarantined
	if consumer.Quarantined() != 0 {
		t.Fatalf("expected 0 quarantined messages, got %d", consumer.Quarantined())
	}
	records, err := store.ListQuarantineRecords(ctx, storage.QuarantineFilter{})
	if err != nil || len(records) != 0 {
		t.Fatalf("expected 0 quarantine records in DB, got %d (err: %v)", len(records), err)
	}

	// Verify message was NOT acknowledged or terminated: it remains pending in consumer
	info, err := consumer.Consumer().Info(ctx)
	if err != nil {
		t.Fatalf("Consumer.Info failed: %v", err)
	}
	if info.NumAckPending != 1 {
		t.Fatalf("expected 1 message still pending acknowledgment in JetStream, got %d", info.NumAckPending)
	}
}

// 43. Test scenario: Concurrent scans, queries, and resolutions do not race or corrupt tracking state
func TestConsumer_StalledUnacknowledged_ConcurrentScansAndResolution_RaceSafety(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer nc.Close()

	streamName := "AEGISEDGE_TELEMETRY"
	setupTestStream(t, nc, streamName)

	dbPath := filepath.Join(t.TempDir(), "quarantine_stalled_concurrent.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	consumerName := fmt.Sprintf("test-stalled-concurrent-%d", time.Now().UnixNano())
	consumer, err := NewTelemetryConsumer(ConsumerConfig{
		NATSURL:          natsURL,
		StreamName:       streamName,
		ConsumerName:     consumerName,
		QuarantineStore:  store,
		IdempotencyStore: store,
	}, &mockAlwaysFailingHandler{}, silentLogger())
	if err != nil {
		t.Fatalf("NewTelemetryConsumer failed: %v", err)
	}
	defer consumer.Stop()

	// Publish 3 valid batches
	for i := 1; i <= 3; i++ {
		batch := &types.TelemetryBatch{
			NodeID:         fmt.Sprintf("node-conc-%d", i),
			BatchID:        fmt.Sprintf("batch-conc-%d", i),
			SequenceNumber: int64(i),
			CollectedAt:    time.Now().UTC(),
			Metrics:        []types.MetricSample{{Name: "m", Value: float64(i), Timestamp: time.Now().UTC()}},
		}
		publishTestEvent(t, nc, batch)
	}
	deliverAndNak(t, consumer, 3)

	ctx := context.Background()
	var wg sync.WaitGroup

	// 10 concurrent workers performing scans, getters, and resolution
	for w := 0; w < 10; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for iter := 0; iter < 50; iter++ {
				switch iter % 4 {
				case 0:
					_, _ = consumer.RecoverUnacknowledged(ctx)
				case 1:
					_ = consumer.StalledUnacknowledgedSequences()
				case 2:
					_ = consumer.StalledSequences()
				case 3:
					consumer.clearStalledSequence(uint64((iter % 3) + 1))
				}
			}
		}(w)
	}

	wg.Wait()

	// Final scan to verify tracker remains usable and uncorrupted
	_, err = consumer.RecoverUnacknowledged(ctx)
	if err != nil {
		t.Fatalf("final RecoverUnacknowledged failed: %v", err)
	}
	count := consumer.StalledUnacknowledgedSequences()
	stalled := consumer.StalledSequences()
	if int64(len(stalled)) != count {
		t.Fatalf("mismatch between StalledUnacknowledgedSequences (%d) and len(StalledSequences) (%d)", count, len(stalled))
	}
}

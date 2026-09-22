package types

import (
	"testing"
	"time"
)

func TestTelemetryEvent_Validate_Valid(t *testing.T) {
	now := time.Now().UTC()
	batch := &TelemetryBatch{
		BatchID:        "batch-1",
		NodeID:         "node-1",
		SequenceNumber: 1,
		CollectedAt:    now,
		Metrics:        []MetricSample{{Name: "cpu", Value: 42.0, Timestamp: now}},
	}
	event := NewTelemetryEvent(batch, now)
	if err := event.Validate(); err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestTelemetryEvent_Validate_EmptyEventID(t *testing.T) {
	event := &TelemetryEvent{
		EventID:       "",
		EventType:     TelemetryEventType,
		SchemaVersion: TelemetrySchemaVersion,
		SourceNodeID:  "node-1",
		OccurredAt:    time.Now().UTC(),
		PublishedAt:   time.Now().UTC(),
		Payload:       &TelemetryBatch{BatchID: "b", NodeID: "n", SequenceNumber: 1, CollectedAt: time.Now().UTC(), Metrics: []MetricSample{{Name: "cpu", Value: 1, Timestamp: time.Now().UTC()}}},
	}
	if err := event.Validate(); err != ErrEmptyEventID {
		t.Errorf("expected ErrEmptyEventID, got %v", err)
	}
}

func TestTelemetryEvent_Validate_NilPayload(t *testing.T) {
	event := &TelemetryEvent{
		EventID:       "e1",
		EventType:     TelemetryEventType,
		SchemaVersion: TelemetrySchemaVersion,
		SourceNodeID:  "node-1",
		OccurredAt:    time.Now().UTC(),
		PublishedAt:   time.Now().UTC(),
		Payload:       nil,
	}
	if err := event.Validate(); err != ErrNilPayload {
		t.Errorf("expected ErrNilPayload, got %v", err)
	}
}

func TestNewTelemetryEvent_Fields(t *testing.T) {
	now := time.Now().UTC()
	batch := &TelemetryBatch{
		BatchID:        "batch-42",
		NodeID:         "node-42",
		SequenceNumber: 42,
		CollectedAt:    now.Add(-5 * time.Second),
		Metrics:        []MetricSample{{Name: "cpu", Value: 42.0, Timestamp: now}},
	}
	event := NewTelemetryEvent(batch, now)

	if event.EventID != batch.BatchID {
		t.Errorf("EventID: want %q, got %q", batch.BatchID, event.EventID)
	}
	if event.EventType != TelemetryEventType {
		t.Errorf("EventType: want %q, got %q", TelemetryEventType, event.EventType)
	}
	if event.SchemaVersion != TelemetrySchemaVersion {
		t.Errorf("SchemaVersion: want %q, got %q", TelemetrySchemaVersion, event.SchemaVersion)
	}
	if event.SourceNodeID != batch.NodeID {
		t.Errorf("SourceNodeID: want %q, got %q", batch.NodeID, event.SourceNodeID)
	}
	if event.SequenceNumber != batch.SequenceNumber {
		t.Errorf("SequenceNumber: want %d, got %d", batch.SequenceNumber, event.SequenceNumber)
	}
	if !event.OccurredAt.Equal(batch.CollectedAt) {
		t.Errorf("OccurredAt: want %v, got %v", batch.CollectedAt, event.OccurredAt)
	}
	if !event.PublishedAt.Equal(now) {
		t.Errorf("PublishedAt: want %v, got %v", now, event.PublishedAt)
	}
	if event.CorrelationID != batch.BatchID {
		t.Errorf("CorrelationID: want %q, got %q", batch.BatchID, event.CorrelationID)
	}
}

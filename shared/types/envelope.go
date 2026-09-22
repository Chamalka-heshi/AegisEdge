package types

import (
	"errors"
	"strings"
	"time"
)

// Event envelope validation errors.
var (
	ErrEmptyEventID      = errors.New("event_id cannot be empty")
	ErrEmptyEventType    = errors.New("event_type cannot be empty")
	ErrEmptySchemaVer    = errors.New("schema_version cannot be empty")
	ErrEmptySourceNodeID = errors.New("source_node_id cannot be empty")
	ErrZeroOccurredAt    = errors.New("occurred_at must be positive and non-zero")
	ErrZeroPublishedAt   = errors.New("published_at must be positive and non-zero")
	ErrNilPayload        = errors.New("payload cannot be nil")
)

// TelemetryEventType is the canonical event type for telemetry batch events.
const TelemetryEventType = "aegisedge.telemetry.batch"

// TelemetrySchemaVersion is the current schema version for telemetry events.
const TelemetrySchemaVersion = "1.0.0"

// TelemetryEvent is the canonical event envelope for publishing telemetry batches
// to NATS JetStream, as specified in ADR-0006.
//
// Key identity invariant: EventID == BatchID for telemetry events.
// A retry MUST reuse the exact same EventID — no new event ID is generated on retry.
type TelemetryEvent struct {
	EventID        string          `json:"event_id"`
	EventType      string          `json:"event_type"`
	SchemaVersion  string          `json:"schema_version"`
	SourceNodeID   string          `json:"source_node_id"`
	SequenceNumber int64           `json:"sequence_number"`
	OccurredAt     time.Time       `json:"occurred_at"`
	PublishedAt    time.Time       `json:"published_at"`
	CorrelationID  string          `json:"correlation_id"`
	Payload        *TelemetryBatch `json:"payload"`
}

// Validate verifies the TelemetryEvent envelope fields.
func (e *TelemetryEvent) Validate() error {
	if strings.TrimSpace(e.EventID) == "" {
		return ErrEmptyEventID
	}
	if strings.TrimSpace(e.EventType) == "" {
		return ErrEmptyEventType
	}
	if strings.TrimSpace(e.SchemaVersion) == "" {
		return ErrEmptySchemaVer
	}
	if strings.TrimSpace(e.SourceNodeID) == "" {
		return ErrEmptySourceNodeID
	}
	if e.OccurredAt.IsZero() {
		return ErrZeroOccurredAt
	}
	if e.PublishedAt.IsZero() {
		return ErrZeroPublishedAt
	}
	if e.Payload == nil {
		return ErrNilPayload
	}
	return e.Payload.Validate()
}

// NewTelemetryEvent constructs a TelemetryEvent envelope from an already-persisted batch.
// The EventID is set to batch.BatchID. OccurredAt preserves the immutable CollectedAt timestamp.
// PublishedAt is set to the provided publish time (the time of actual publication, not persistence).
func NewTelemetryEvent(batch *TelemetryBatch, publishedAt time.Time) *TelemetryEvent {
	return &TelemetryEvent{
		EventID:        batch.BatchID,
		EventType:      TelemetryEventType,
		SchemaVersion:  TelemetrySchemaVersion,
		SourceNodeID:   batch.NodeID,
		SequenceNumber: batch.SequenceNumber,
		OccurredAt:     batch.CollectedAt,
		PublishedAt:    publishedAt,
		CorrelationID:  batch.BatchID,
		Payload:        batch,
	}
}

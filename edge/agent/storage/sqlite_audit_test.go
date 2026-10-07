package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestAuditStore(t *testing.T) (*SQLiteStore, string) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit_test.db")
	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("failed to create test SQLite store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store, dbPath
}

func sampleStoredAuditEvent(eventID, eventType, incidentID, nodeID, corrID string) *StoredAuditEvent {
	return &StoredAuditEvent{
		EventID:            eventID,
		EventType:          eventType,
		Timestamp:          time.Now().UTC(),
		NodeID:             nodeID,
		IncidentID:         incidentID,
		DecisionID:         "dec-" + incidentID,
		ActionID:           "act-" + incidentID,
		ApprovalID:         "app-" + incidentID,
		MitigationRecordID: "mit-" + incidentID,
		VerificationID:     "ver-" + incidentID,
		EscalationID:       "esc-" + incidentID,
		CorrelationID:      corrID,
		PolicyVersion:      "v1.0",
		Actor:              "orchestrator",
		Result:             "SUCCESS",
		Reason:             "mitigation executed successfully",
		Metadata: map[string]string{
			"component": "agent",
			"severity":  "HIGH",
		},
	}
}

func TestAuditStore_MigrationV8(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestAuditStore(t)

	count, err := store.CountAuditEvents(ctx)
	if err != nil {
		t.Fatalf("failed to count audit events after migration: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 audit events initially, got %d", count)
	}
}

func TestAuditStore_RecordAndGet(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestAuditStore(t)

	evt := sampleStoredAuditEvent("evt-001", "MITIGATION_EXECUTED", "inc-100", "node-1", "corr-100")
	if err := store.RecordAuditEvent(ctx, evt); err != nil {
		t.Fatalf("failed to record audit event: %v", err)
	}

	retrieved, err := store.GetAuditEvent(ctx, "evt-001")
	if err != nil {
		t.Fatalf("failed to retrieve audit event: %v", err)
	}
	if retrieved.EventID != evt.EventID {
		t.Errorf("expected EventID %s, got %s", evt.EventID, retrieved.EventID)
	}
	if retrieved.EventType != evt.EventType {
		t.Errorf("expected EventType %s, got %s", evt.EventType, retrieved.EventType)
	}
	if retrieved.IncidentID != evt.IncidentID {
		t.Errorf("expected IncidentID %s, got %s", evt.IncidentID, retrieved.IncidentID)
	}
	if retrieved.CorrelationID != evt.CorrelationID {
		t.Errorf("expected CorrelationID %s, got %s", evt.CorrelationID, retrieved.CorrelationID)
	}
	if retrieved.Metadata["component"] != "agent" {
		t.Errorf("expected metadata component 'agent', got %s", retrieved.Metadata["component"])
	}

	// Not found check
	_, err = store.GetAuditEvent(ctx, "non-existent")
	if err != ErrAuditEventNotFound {
		t.Errorf("expected ErrAuditEventNotFound, got %v", err)
	}
}

func TestAuditStore_IdempotencyDuplicateInsertion(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestAuditStore(t)

	evt := sampleStoredAuditEvent("evt-dup-1", "RESPONSE_DECISION_CREATED", "inc-200", "node-1", "corr-200")
	if err := store.RecordAuditEvent(ctx, evt); err != nil {
		t.Fatalf("first record should succeed: %v", err)
	}

	// Second write with identical event_id
	err := store.RecordAuditEvent(ctx, evt)
	if err != ErrDuplicateAuditEvent {
		t.Fatalf("expected ErrDuplicateAuditEvent on duplicate insertion, got %v", err)
	}

	// Verify count is still exactly 1
	count, err := store.CountAuditEvents(ctx)
	if err != nil {
		t.Fatalf("failed to count audit events: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 audit event after duplicate insertion, got %d", count)
	}
}

func TestAuditStore_RestartDurability(t *testing.T) {
	ctx := context.Background()
	store1, dbPath := newTestAuditStore(t)

	now := time.Now().UTC().Truncate(time.Millisecond)
	evt := &StoredAuditEvent{
		EventID:            "evt-restart-1",
		EventType:          "SAFETY_VALIDATED",
		Timestamp:          now,
		NodeID:             "node-prod-1",
		IncidentID:         "inc-restart",
		DecisionID:         "dec-999",
		ActionID:           "act-999",
		ApprovalID:         "app-999",
		MitigationRecordID: "mit-999",
		VerificationID:     "ver-999",
		EscalationID:       "esc-999",
		CorrelationID:      "corr-restart",
		PolicyVersion:      "v2.1",
		Actor:              "safety-validator",
		Result:             "VALIDATED",
		Reason:             "all safety checks passed",
		Metadata: map[string]string{
			"environment": "edge-tier1",
			"score":       "0.99",
		},
	}

	if err := store1.RecordAuditEvent(ctx, evt); err != nil {
		t.Fatalf("failed to record event in store1: %v", err)
	}

	// Close store1
	if err := store1.Close(); err != nil {
		t.Fatalf("failed to close store1: %v", err)
	}

	// Reopen on same dbPath
	store2, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}
	defer store2.Close()

	retrieved, err := store2.GetAuditEvent(ctx, "evt-restart-1")
	if err != nil {
		t.Fatalf("failed to retrieve event after restart: %v", err)
	}
	if retrieved.EventID != evt.EventID {
		t.Errorf("EventID mismatch after restart: got %s, want %s", retrieved.EventID, evt.EventID)
	}
	if retrieved.EventType != evt.EventType {
		t.Errorf("EventType mismatch: got %s, want %s", retrieved.EventType, evt.EventType)
	}
	if retrieved.Actor != evt.Actor {
		t.Errorf("Actor mismatch: got %s, want %s", retrieved.Actor, evt.Actor)
	}
	if retrieved.Metadata["environment"] != "edge-tier1" {
		t.Errorf("Metadata mismatch: got %s, want 'edge-tier1'", retrieved.Metadata["environment"])
	}
}

func TestAuditStore_QueryByIncidentCorrelationNode(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestAuditStore(t)

	baseTime := time.Now().UTC()
	events := []*StoredAuditEvent{
		{
			EventID:       "evt-q-1",
			EventType:     "INCIDENT_CREATED",
			Timestamp:     baseTime.Add(1 * time.Second),
			NodeID:        "node-A",
			IncidentID:    "inc-1",
			CorrelationID: "corr-1",
		},
		{
			EventID:       "evt-q-2",
			EventType:     "RESPONSE_DECISION_CREATED",
			Timestamp:     baseTime.Add(2 * time.Second),
			NodeID:        "node-A",
			IncidentID:    "inc-1",
			CorrelationID: "corr-1",
		},
		{
			EventID:       "evt-q-3",
			EventType:     "INCIDENT_CREATED",
			Timestamp:     baseTime.Add(3 * time.Second),
			NodeID:        "node-B",
			IncidentID:    "inc-2",
			CorrelationID: "corr-2",
		},
	}

	for _, e := range events {
		if err := store.RecordAuditEvent(ctx, e); err != nil {
			t.Fatalf("failed to record %s: %v", e.EventID, err)
		}
	}

	// Query by Incident
	incEvents, err := store.ListAuditEventsByIncident(ctx, "inc-1", 10)
	if err != nil {
		t.Fatalf("failed to query by incident: %v", err)
	}
	if len(incEvents) != 2 {
		t.Fatalf("expected 2 events for inc-1, got %d", len(incEvents))
	}
	if incEvents[0].EventID != "evt-q-1" || incEvents[1].EventID != "evt-q-2" {
		t.Errorf("unexpected incident ordering: %s, %s", incEvents[0].EventID, incEvents[1].EventID)
	}

	// Query by Correlation
	corrEvents, err := store.ListAuditEventsByCorrelation(ctx, "corr-1", 10)
	if err != nil {
		t.Fatalf("failed to query by correlation: %v", err)
	}
	if len(corrEvents) != 2 {
		t.Fatalf("expected 2 events for corr-1, got %d", len(corrEvents))
	}

	// Query by Node
	nodeEvents, err := store.ListAuditEventsByNode(ctx, "node-B", 10)
	if err != nil {
		t.Fatalf("failed to query by node: %v", err)
	}
	if len(nodeEvents) != 1 {
		t.Fatalf("expected 1 event for node-B, got %d", len(nodeEvents))
	}
	if nodeEvents[0].EventID != "evt-q-3" {
		t.Errorf("expected evt-q-3, got %s", nodeEvents[0].EventID)
	}
}

func TestAuditStore_BoundedQueries(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestAuditStore(t)

	baseTime := time.Now().UTC()
	for i := 0; i < 20; i++ {
		evt := &StoredAuditEvent{
			EventID:       fmt.Sprintf("evt-bound-%02d", i),
			EventType:     "VERIFICATION_PROGRESS",
			Timestamp:     baseTime.Add(time.Duration(i) * time.Second),
			NodeID:        "node-bound",
			IncidentID:    "inc-bound",
			CorrelationID: "corr-bound",
		}
		if err := store.RecordAuditEvent(ctx, evt); err != nil {
			t.Fatalf("failed to record event: %v", err)
		}
	}

	// Bounded query limit = 5
	limited, err := store.ListAuditEventsByIncident(ctx, "inc-bound", 5)
	if err != nil {
		t.Fatalf("failed to query with limit: %v", err)
	}
	if len(limited) != 5 {
		t.Fatalf("expected 5 events, got %d", len(limited))
	}

	// Non-positive limit defaults to DefaultAuditQueryLimit (100)
	defaultLim, err := store.ListAuditEventsByIncident(ctx, "inc-bound", 0)
	if err != nil {
		t.Fatalf("failed to query with limit 0: %v", err)
	}
	if len(defaultLim) != 20 {
		t.Fatalf("expected all 20 events with default limit, got %d", len(defaultLim))
	}
}

func TestAuditStore_ConcurrentWrites(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestAuditStore(t)

	var wg sync.WaitGroup
	errCh := make(chan error, 50)
	numGoroutines := 20

	baseTime := time.Now().UTC()
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			evt := &StoredAuditEvent{
				EventID:       fmt.Sprintf("evt-conc-%03d", idx),
				EventType:     "MITIGATION_EXECUTED",
				Timestamp:     baseTime.Add(time.Duration(idx) * time.Millisecond),
				NodeID:        fmt.Sprintf("node-%d", idx%3),
				IncidentID:    fmt.Sprintf("inc-%d", idx%5),
				CorrelationID: fmt.Sprintf("corr-%d", idx%5),
			}
			if err := store.RecordAuditEvent(ctx, evt); err != nil {
				errCh <- err
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("concurrent audit write failed: %v", err)
	}

	count, err := store.CountAuditEvents(ctx)
	if err != nil {
		t.Fatalf("failed to count audit events: %v", err)
	}
	if count != int64(numGoroutines) {
		t.Fatalf("expected %d events, got %d", numGoroutines, count)
	}
}

func TestAuditStore_ValidationBounds(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestAuditStore(t)

	// Missing EventID
	e1 := sampleStoredAuditEvent("", "ANOMALY_DETECTED", "inc-1", "node-1", "corr-1")
	if err := store.RecordAuditEvent(ctx, e1); err == nil {
		t.Errorf("expected error for empty event_id")
	}

	// Missing IncidentID
	e2 := sampleStoredAuditEvent("evt-bad", "ANOMALY_DETECTED", "", "node-1", "corr-1")
	if err := store.RecordAuditEvent(ctx, e2); err == nil {
		t.Errorf("expected error for empty incident_id")
	}

	// Missing NodeID
	e3 := sampleStoredAuditEvent("evt-bad", "ANOMALY_DETECTED", "inc-1", "", "corr-1")
	if err := store.RecordAuditEvent(ctx, e3); err == nil {
		t.Errorf("expected error for empty node_id")
	}

	// Zero Timestamp
	e4 := sampleStoredAuditEvent("evt-bad", "ANOMALY_DETECTED", "inc-1", "node-1", "corr-1")
	e4.Timestamp = time.Time{}
	if err := store.RecordAuditEvent(ctx, e4); err == nil {
		t.Errorf("expected error for zero timestamp")
	}

	// Oversized metadata (>8192 bytes)
	e5 := sampleStoredAuditEvent("evt-bad", "ANOMALY_DETECTED", "inc-1", "node-1", "corr-1")
	e5.Metadata = map[string]string{
		"huge": strings.Repeat("A", 8200),
	}
	if err := store.RecordAuditEvent(ctx, e5); err == nil {
		t.Errorf("expected error for oversized metadata")
	}

	// Sensitive credential rejected in metadata
	e6 := sampleStoredAuditEvent("evt-bad", "ANOMALY_DETECTED", "inc-1", "node-1", "corr-1")
	e6.Metadata = map[string]string{
		"password": "supersecretpassword",
	}
	if err := store.RecordAuditEvent(ctx, e6); err == nil {
		t.Errorf("expected error for credential in metadata")
	}

	// Prohibited command rejected in metadata
	e7 := sampleStoredAuditEvent("evt-bad", "ANOMALY_DETECTED", "inc-1", "node-1", "corr-1")
	e7.Metadata = map[string]string{
		"script": "/bin/sh -c reboot",
	}
	if err := store.RecordAuditEvent(ctx, e7); err == nil {
		t.Errorf("expected error for prohibited command in metadata")
	}
}

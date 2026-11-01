package server

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/services/control-plane/metrics"
	"github.com/Chamalka-heshi/AegisEdge/services/control-plane/storage"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// mockFailingPruneStore wraps a storage.QuarantineStore and fails PruneQuarantinePayloads.
type mockFailingPruneStore struct {
	storage.QuarantineStore
	pruneErr error
}

func (m *mockFailingPruneStore) PruneQuarantinePayloads(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	if m.pruneErr != nil {
		return 0, m.pruneErr
	}
	return m.QuarantineStore.PruneQuarantinePayloads(ctx, cutoff, limit)
}

// TestQuarantinePrune_LifecycleRequirements covers all Stage 6.31B.8 pruning scenarios:
// 1. Terminal 'replayed' records older than cutoff have payload cleared.
// 2. Terminal 'discarded' records older than cutoff have payload cleared.
// 3. Active 'quarantined' records older than cutoff retain payload.
// 4. Terminal records newer than cutoff retain payload.
// 5. Quarantine metadata remains available after payload pruning.
// 6. PayloadSizeBytes correctly reports zero after pruning.
// 7. Batch-size limits are respected.
// 8. RunPrunePass invokes quarantine pruning.
// 11. Metric increments by the actual number of successfully pruned payloads.
// 12. Repeated pruning passes do not double-count already-cleared payloads.
func TestQuarantinePrune_LifecycleRequirements(t *testing.T) {
	ctx := context.Background()
	m := metrics.NewMetrics()
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	srv := NewServer(newDiscardLogger(),
		WithIdempotencyStore(store),
		WithMetrics(m),
		WithPruningConfig(PruningConfig{
			Enabled:                 true,
			TelemetryRetentionDays:  30,
			QuarantineRetentionDays: 30,
			PruneInterval:           time.Hour,
			PruneBatchSize:          100,
		}),
	)

	now := time.Now().UTC()
	cutoff := now.Add(-30 * 24 * time.Hour)
	oldTime := cutoff.Add(-5 * 24 * time.Hour)
	newTime := cutoff.Add(5 * 24 * time.Hour)

	// Record 1: Terminal 'replayed' older than cutoff -> MUST BE PRUNED
	r1, err := store.RecordQuarantine(ctx, &storage.QuarantineRecord{
		NodeID:             "node-1",
		BatchID:            "batch-replayed-old",
		Payload:            []byte("payload-replayed-old-data-12345"),
		FailureCategory:    storage.CategoryDeliveryExhausted,
		FailureReason:      "max deliveries reached",
		FirstQuarantinedAt: oldTime,
		LastQuarantinedAt:  oldTime,
	})
	if err != nil {
		t.Fatalf("RecordQuarantine r1 failed: %v", err)
	}
	_, err = store.UpdateQuarantineStatus(ctx, storage.QuarantineStatusUpdate{
		ID:        r1.ID,
		NewStatus: storage.StatusReplayed,
		Timestamp: oldTime,
		UpdatedBy: "operator-admin",
		Reason:    "replay succeeded",
	})
	if err != nil {
		t.Fatalf("UpdateQuarantineStatus r1 failed: %v", err)
	}

	// Record 2: Terminal 'discarded' older than cutoff -> MUST BE PRUNED
	r2, err := store.RecordQuarantine(ctx, &storage.QuarantineRecord{
		NodeID:             "node-2",
		BatchID:            "batch-discarded-old",
		Payload:            []byte("payload-discarded-old-data-67890"),
		FailureCategory:    storage.CategorySchemaValidation,
		FailureReason:      "malformed payload schema",
		FirstQuarantinedAt: oldTime,
		LastQuarantinedAt:  oldTime,
	})
	if err != nil {
		t.Fatalf("RecordQuarantine r2 failed: %v", err)
	}
	_, err = store.UpdateQuarantineStatus(ctx, storage.QuarantineStatusUpdate{
		ID:        r2.ID,
		NewStatus: storage.StatusDiscarded,
		Timestamp: oldTime,
		UpdatedBy: "operator-admin",
		Reason:    "discard confirmed",
	})
	if err != nil {
		t.Fatalf("UpdateQuarantineStatus r2 failed: %v", err)
	}

	// Record 3: Active 'quarantined' older than cutoff -> MUST NOT BE PRUNED (Active Protection)
	r3, err := store.RecordQuarantine(ctx, &storage.QuarantineRecord{
		NodeID:             "node-3",
		BatchID:            "batch-active-old",
		Payload:            []byte("payload-active-old-evidence-must-retain"),
		FailureCategory:    storage.CategoryDeliveryExhausted,
		FailureReason:      "active investigation needed",
		FirstQuarantinedAt: oldTime,
		LastQuarantinedAt:  oldTime,
	})
	if err != nil {
		t.Fatalf("RecordQuarantine r3 failed: %v", err)
	}

	// Record 4: Terminal 'replayed' newer than cutoff -> MUST NOT BE PRUNED
	r4, err := store.RecordQuarantine(ctx, &storage.QuarantineRecord{
		NodeID:             "node-4",
		BatchID:            "batch-replayed-new",
		Payload:            []byte("payload-replayed-new-retain"),
		FailureCategory:    storage.CategoryDeliveryExhausted,
		FailureReason:      "recently replayed",
		FirstQuarantinedAt: newTime,
		LastQuarantinedAt:  newTime,
	})
	if err != nil {
		t.Fatalf("RecordQuarantine r4 failed: %v", err)
	}
	_, err = store.UpdateQuarantineStatus(ctx, storage.QuarantineStatusUpdate{
		ID:        r4.ID,
		NewStatus: storage.StatusReplayed,
		Timestamp: newTime,
		UpdatedBy: "operator-admin",
	})
	if err != nil {
		t.Fatalf("UpdateQuarantineStatus r4 failed: %v", err)
	}

	// Record 5: Terminal 'discarded' newer than cutoff -> MUST NOT BE PRUNED
	r5, err := store.RecordQuarantine(ctx, &storage.QuarantineRecord{
		NodeID:             "node-5",
		BatchID:            "batch-discarded-new",
		Payload:            []byte("payload-discarded-new-retain"),
		FailureCategory:    storage.CategorySchemaValidation,
		FailureReason:      "recently discarded",
		FirstQuarantinedAt: newTime,
		LastQuarantinedAt:  newTime,
	})
	if err != nil {
		t.Fatalf("RecordQuarantine r5 failed: %v", err)
	}
	_, err = store.UpdateQuarantineStatus(ctx, storage.QuarantineStatusUpdate{
		ID:        r5.ID,
		NewStatus: storage.StatusDiscarded,
		Timestamp: newTime,
		UpdatedBy: "operator-admin",
	})
	if err != nil {
		t.Fatalf("UpdateQuarantineStatus r5 failed: %v", err)
	}

	// Verify initial metric value is 0
	initialExp := m.Registry().FormatPrometheus()
	if !strings.Contains(initialExp, "aegisedge_quarantine_pruned_payloads_total 0") {
		t.Fatalf("expected initial quarantine pruned metric to be 0, got:\n%s", initialExp)
	}

	// Execute RunPrunePass (Requirement 8: RunPrunePass invokes quarantine pruning)
	pruned, err := srv.RunPrunePass(ctx)
	if err != nil {
		t.Fatalf("RunPrunePass failed: %v", err)
	}
	// Expected 2 records pruned (r1 and r2)
	if pruned != 2 {
		t.Fatalf("expected 2 records pruned, got %d", pruned)
	}

	// Requirement 11: Metric increments by actual pruned count (2)
	expAfterPrune := m.Registry().FormatPrometheus()
	if !strings.Contains(expAfterPrune, "aegisedge_quarantine_pruned_payloads_total 2") {
		t.Fatalf("expected metric to report 2 pruned payloads, got:\n%s", expAfterPrune)
	}

	// Verification 1: Terminal replayed older than cutoff has payload cleared
	r1After, err := store.GetQuarantineRecord(ctx, r1.ID)
	if err != nil {
		t.Fatalf("GetQuarantineRecord r1 failed: %v", err)
	}
	if len(r1After.Payload) != 0 {
		t.Fatalf("expected r1 payload to be empty, got %d bytes", len(r1After.Payload))
	}

	// Verification 2: Terminal discarded older than cutoff has payload cleared
	r2After, err := store.GetQuarantineRecord(ctx, r2.ID)
	if err != nil {
		t.Fatalf("GetQuarantineRecord r2 failed: %v", err)
	}
	if len(r2After.Payload) != 0 {
		t.Fatalf("expected r2 payload to be empty, got %d bytes", len(r2After.Payload))
	}

	// Verification 3: Active quarantined older than cutoff retains payload
	r3After, err := store.GetQuarantineRecord(ctx, r3.ID)
	if err != nil {
		t.Fatalf("GetQuarantineRecord r3 failed: %v", err)
	}
	if string(r3After.Payload) != "payload-active-old-evidence-must-retain" {
		t.Fatalf("expected r3 payload to be preserved, got %q", string(r3After.Payload))
	}
	if r3After.Status != storage.StatusQuarantined {
		t.Fatalf("expected r3 status 'quarantined', got %s", r3After.Status)
	}

	// Verification 4: Terminal records newer than cutoff retain payload
	r4After, err := store.GetQuarantineRecord(ctx, r4.ID)
	if err != nil {
		t.Fatalf("GetQuarantineRecord r4 failed: %v", err)
	}
	if string(r4After.Payload) != "payload-replayed-new-retain" {
		t.Fatalf("expected r4 payload preserved, got %q", string(r4After.Payload))
	}

	r5After, err := store.GetQuarantineRecord(ctx, r5.ID)
	if err != nil {
		t.Fatalf("GetQuarantineRecord r5 failed: %v", err)
	}
	if string(r5After.Payload) != "payload-discarded-new-retain" {
		t.Fatalf("expected r5 payload preserved, got %q", string(r5After.Payload))
	}

	// Verification 5: Quarantine metadata remains available after payload pruning
	if r1After.NodeID != "node-1" || r1After.BatchID != "batch-replayed-old" {
		t.Fatalf("metadata mismatch for r1: node=%s, batch=%s", r1After.NodeID, r1After.BatchID)
	}
	if r1After.FailureReason != "max deliveries reached" {
		t.Fatalf("failure diagnostics mismatch for r1: %s", r1After.FailureReason)
	}
	if r1After.Status != storage.StatusReplayed || r1After.StatusUpdatedBy != "operator-admin" || r1After.StatusReason != "replay succeeded" {
		t.Fatalf("status audit mismatch for r1: status=%s, op=%s, reason=%s", r1After.Status, r1After.StatusUpdatedBy, r1After.StatusReason)
	}

	if r2After.NodeID != "node-2" || r2After.BatchID != "batch-discarded-old" {
		t.Fatalf("metadata mismatch for r2: node=%s, batch=%s", r2After.NodeID, r2After.BatchID)
	}
	if r2After.FailureReason != "malformed payload schema" {
		t.Fatalf("failure diagnostics mismatch for r2: %s", r2After.FailureReason)
	}
	if r2After.Status != storage.StatusDiscarded || r2After.StatusUpdatedBy != "operator-admin" || r2After.StatusReason != "discard confirmed" {
		t.Fatalf("status audit mismatch for r2: status=%s, op=%s, reason=%s", r2After.Status, r2After.StatusUpdatedBy, r2After.StatusReason)
	}

	// Verification 6: PayloadSizeBytes correctly reports zero after pruning
	if r1After.PayloadSizeBytes != 0 {
		t.Fatalf("expected r1 PayloadSizeBytes 0, got %d", r1After.PayloadSizeBytes)
	}
	if r2After.PayloadSizeBytes != 0 {
		t.Fatalf("expected r2 PayloadSizeBytes 0, got %d", r2After.PayloadSizeBytes)
	}
	// Check via ListQuarantineRecords as well
	records, err := store.ListQuarantineRecords(ctx, storage.QuarantineFilter{Limit: 10})
	if err != nil {
		t.Fatalf("ListQuarantineRecords failed: %v", err)
	}
	for _, rec := range records {
		if rec.ID == r1.ID || rec.ID == r2.ID {
			if rec.PayloadSizeBytes != 0 {
				t.Fatalf("expected list PayloadSizeBytes 0 for pruned rec %s, got %d", rec.ID, rec.PayloadSizeBytes)
			}
		}
	}

	// Requirement 12: Repeated pruning passes do not double-count already-cleared payloads
	secondPruned, err := srv.RunPrunePass(ctx)
	if err != nil {
		t.Fatalf("second RunPrunePass failed: %v", err)
	}
	if secondPruned != 0 {
		t.Fatalf("expected 0 payloads pruned on second pass, got %d", secondPruned)
	}

	expAfterSecond := m.Registry().FormatPrometheus()
	if !strings.Contains(expAfterSecond, "aegisedge_quarantine_pruned_payloads_total 2") {
		t.Fatalf("expected metric to remain 2 after second pass, got:\n%s", expAfterSecond)
	}
}

// TestQuarantinePrune_BatchSizeLimits verifies that pruning respects the configured batch size limit (Requirement 7).
func TestQuarantinePrune_BatchSizeLimits(t *testing.T) {
	ctx := context.Background()
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	srv := NewServer(newDiscardLogger(),
		WithIdempotencyStore(store),
		WithPruningConfig(PruningConfig{
			Enabled:                 true,
			TelemetryRetentionDays:  30,
			QuarantineRetentionDays: 30,
			PruneInterval:           time.Hour,
			PruneBatchSize:          2, // Limit = 2
		}),
	)

	oldTime := time.Now().UTC().Add(-40 * 24 * time.Hour)

	// Seed 5 terminal replayed records older than cutoff
	for i := 1; i <= 5; i++ {
		rec, err := store.RecordQuarantine(ctx, &storage.QuarantineRecord{
			NodeID:             "node-batch",
			BatchID:            "batch-lim-" + string(rune('0'+i)),
			Payload:            []byte("data-evidence"),
			FailureCategory:    storage.CategoryDeliveryExhausted,
			FailureReason:      "batch limit test",
			FirstQuarantinedAt: oldTime,
			LastQuarantinedAt:  oldTime,
		})
		if err != nil {
			t.Fatalf("RecordQuarantine %d failed: %v", i, err)
		}
		_, err = store.UpdateQuarantineStatus(ctx, storage.QuarantineStatusUpdate{
			ID:        rec.ID,
			NewStatus: storage.StatusReplayed,
			Timestamp: oldTime,
		})
		if err != nil {
			t.Fatalf("UpdateQuarantineStatus %d failed: %v", i, err)
		}
	}

	// Pass 1: should prune 2
	p1, err := srv.RunQuarantinePrunePass(ctx)
	if err != nil {
		t.Fatalf("Pass 1 failed: %v", err)
	}
	if p1 != 2 {
		t.Fatalf("expected pass 1 to prune 2, got %d", p1)
	}

	// Pass 2: should prune 2
	p2, err := srv.RunQuarantinePrunePass(ctx)
	if err != nil {
		t.Fatalf("Pass 2 failed: %v", err)
	}
	if p2 != 2 {
		t.Fatalf("expected pass 2 to prune 2, got %d", p2)
	}

	// Pass 3: should prune remaining 1
	p3, err := srv.RunQuarantinePrunePass(ctx)
	if err != nil {
		t.Fatalf("Pass 3 failed: %v", err)
	}
	if p3 != 1 {
		t.Fatalf("expected pass 3 to prune 1, got %d", p3)
	}

	// Pass 4: should prune 0
	p4, err := srv.RunQuarantinePrunePass(ctx)
	if err != nil {
		t.Fatalf("Pass 4 failed: %v", err)
	}
	if p4 != 0 {
		t.Fatalf("expected pass 4 to prune 0, got %d", p4)
	}
}

// TestQuarantinePrune_ErrorIsolation verifies that quarantine pruning failures do not
// prevent telemetry ingestion or crash the server (Requirement 9).
func TestQuarantinePrune_ErrorIsolation(t *testing.T) {
	ctx := context.Background()
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	failingStore := &mockFailingPruneStore{
		QuarantineStore: store,
		pruneErr:        errors.New("simulated disk i/o error during quarantine prune"),
	}

	srv := NewServer(newDiscardLogger(),
		WithIdempotencyStore(store),
		WithQuarantineStore(failingStore),
		WithPruningConfig(PruningConfig{
			Enabled:                 true,
			TelemetryRetentionDays:  30,
			QuarantineRetentionDays: 30,
			PruneInterval:           time.Hour,
			PruneBatchSize:          100,
		}),
	)

	// RunPrunePass returns error from failing quarantine prune
	_, pruneErr := srv.RunPrunePass(ctx)
	if pruneErr == nil {
		t.Fatalf("expected error from failing store")
	}

	// Server must remain operational and continue ingesting telemetry
	now := time.Now().UTC()
	b := &types.TelemetryBatch{
		NodeID:         "node-healthy",
		BatchID:        "batch-healthy-1",
		SequenceNumber: 1,
		CollectedAt:    now,
		Metrics: []types.MetricSample{
			{Name: "cpu_usage", Value: 50.0, Timestamp: now},
		},
	}

	// Ingest batch via server store
	ok, err := store.RecordBatch(ctx, b, now)
	if err != nil || !ok {
		t.Fatalf("expected successful telemetry ingestion despite quarantine prune error, got ok=%v, err=%v", ok, err)
	}

	// Telemetry payload must be retrievable
	ingested, err := srv.GetPayload(ctx, "node-healthy", "batch-healthy-1")
	if err != nil {
		t.Fatalf("GetPayload failed: %v", err)
	}
	if ingested.NodeID != "node-healthy" {
		t.Fatalf("unexpected node ID: %s", ingested.NodeID)
	}
}

// TestQuarantinePrune_ContextCancellation verifies that context cancellation is handled cleanly (Requirement 10).
func TestQuarantinePrune_ContextCancellation(t *testing.T) {
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	srv := NewServer(newDiscardLogger(),
		WithIdempotencyStore(store),
		WithPruningConfig(PruningConfig{
			Enabled:                 true,
			TelemetryRetentionDays:  30,
			QuarantineRetentionDays: 30,
			PruneInterval:           time.Hour,
			PruneBatchSize:          100,
		}),
	)

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	// RunQuarantinePrunePass with canceled context
	_, err = srv.RunQuarantinePrunePass(canceledCtx)
	if err == nil {
		t.Fatalf("expected error on canceled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled error, got: %v", err)
	}

	// RunPrunePass with canceled context
	_, err = srv.RunPrunePass(canceledCtx)
	if err == nil {
		t.Fatalf("expected error on canceled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled error, got: %v", err)
	}
}

// TestQuarantinePrune_ConfigurationValidation verifies validation of PruningConfig (Requirement 13).
func TestQuarantinePrune_ConfigurationValidation(t *testing.T) {
	// 1. Disabled pruning is always valid
	disabledCfg := PruningConfig{
		Enabled:                 false,
		TelemetryRetentionDays:  0,
		QuarantineRetentionDays: 0,
		PruneInterval:           0,
		PruneBatchSize:          0,
	}
	if err := disabledCfg.Validate(); err != nil {
		t.Fatalf("expected disabled config to be valid, got: %v", err)
	}

	// 2. Default configuration is valid when enabled
	defCfg := DefaultPruningConfig()
	defCfg.Enabled = true
	if err := defCfg.Validate(); err != nil {
		t.Fatalf("expected default config to be valid when enabled, got: %v", err)
	}

	// 3. Zero or negative QuarantineRetentionDays must fail when enabled
	invalidQDaysZero := PruningConfig{
		Enabled:                 true,
		TelemetryRetentionDays:  30,
		QuarantineRetentionDays: 0,
		PruneInterval:           time.Hour,
		PruneBatchSize:          100,
	}
	if err := invalidQDaysZero.Validate(); err == nil {
		t.Fatalf("expected error for zero QuarantineRetentionDays")
	}

	invalidQDaysNeg := PruningConfig{
		Enabled:                 true,
		TelemetryRetentionDays:  30,
		QuarantineRetentionDays: -10,
		PruneInterval:           time.Hour,
		PruneBatchSize:          100,
	}
	if err := invalidQDaysNeg.Validate(); err == nil {
		t.Fatalf("expected error for negative QuarantineRetentionDays")
	}

	// 4. Zero or negative TelemetryRetentionDays must fail when enabled
	invalidTelDays := PruningConfig{
		Enabled:                 true,
		TelemetryRetentionDays:  0,
		QuarantineRetentionDays: 30,
		PruneInterval:           time.Hour,
		PruneBatchSize:          100,
	}
	if err := invalidTelDays.Validate(); err == nil {
		t.Fatalf("expected error for zero TelemetryRetentionDays")
	}

	// 5. WithPruningConfig defaults zero QuarantineRetentionDays for backward compatibility
	validCfgOmittedQ := PruningConfig{
		Enabled:                true,
		TelemetryRetentionDays: 30,
		PruneInterval:          time.Hour,
		PruneBatchSize:         100,
	}
	opt := WithPruningConfig(validCfgOmittedQ)
	s := &Server{}
	if err := opt(s); err != nil {
		t.Fatalf("WithPruningConfig failed: %v", err)
	}
	if s.pruneConfig.QuarantineRetentionDays != DefaultQuarantineRetentionDays {
		t.Fatalf("expected WithPruningConfig to default QuarantineRetentionDays to %d, got %d",
			DefaultQuarantineRetentionDays, s.pruneConfig.QuarantineRetentionDays)
	}
}

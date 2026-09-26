package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/detector"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	edgesync "github.com/Chamalka-heshi/AegisEdge/edge/agent/sync"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

func floatPtr(v float64) *float64 {
	return &v
}

// customGenerator allows tests to inject specific telemetry batches into the pipeline.
type customGenerator struct {
	mu       sync.Mutex
	sequence int64
	batches  []*types.TelemetryBatch
	cursor   int
}

func newCustomGenerator(batches ...*types.TelemetryBatch) *customGenerator {
	return &customGenerator{batches: batches}
}

func (g *customGenerator) GenerateBatch(ctx context.Context) (*types.TelemetryBatch, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.cursor >= len(g.batches) {
		return nil, errors.New("no more test batches")
	}
	b := g.batches[g.cursor]
	g.cursor++
	g.sequence = b.SequenceNumber
	return b, nil
}

func (g *customGenerator) CurrentSequence() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.sequence
}

// faultyDetector simulates a failing detector to verify error isolation.
type faultyDetector struct {
	err error
}

func (f *faultyDetector) Name() string    { return "faulty_detector" }
func (f *faultyDetector) Version() string { return "1.0.0" }
func (f *faultyDetector) Detect(ctx context.Context, sample types.MetricSample) (*types.AnomalySignal, error) {
	return nil, f.err
}
func (f *faultyDetector) DetectBatch(ctx context.Context, batch *types.TelemetryBatch) ([]*types.AnomalySignal, error) {
	return nil, f.err
}

// spyDetector tracks whether DetectBatch was called.
type spyDetector struct {
	mu           sync.Mutex
	called       bool
	receivedArgs *types.TelemetryBatch
}

func (s *spyDetector) Name() string    { return "spy_detector" }
func (s *spyDetector) Version() string { return "1.0.0" }
func (s *spyDetector) Detect(ctx context.Context, sample types.MetricSample) (*types.AnomalySignal, error) {
	return nil, nil
}
func (s *spyDetector) DetectBatch(ctx context.Context, batch *types.TelemetryBatch) ([]*types.AnomalySignal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.called = true
	s.receivedArgs = batch
	return nil, nil
}

// ----------------------------------------------------------------------------
// TEST 1: Normal Telemetry
// Flow: Generate -> Validate -> SQLite Persist -> Detect -> No Anomaly -> Succeeded
// ----------------------------------------------------------------------------
func TestPipeline_NormalTelemetry(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test1_normal.db")
	logger := newDiscardLogger()

	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	det, err := detector.NewThresholdDetector(detector.ThresholdDetectorConfig{
		Version: "1.0.0",
		Rules:   detector.DefaultRules(),
	})
	if err != nil {
		t.Fatalf("failed to create detector: %v", err)
	}

	now := time.Now().UTC()
	normalBatch := &types.TelemetryBatch{
		BatchID:        "batch-normal-101",
		NodeID:         "node-test-1",
		SequenceNumber: 0,
		CollectedAt:    now,
		Metrics: []types.MetricSample{
			{NodeID: "node-test-1", Name: "cpu_usage_percent", Value: 45.0, Timestamp: now},
			{NodeID: "node-test-1", Name: "memory_usage_percent", Value: 60.0, Timestamp: now},
			{NodeID: "node-test-1", Name: "disk_usage_percent", Value: 35.0, Timestamp: now},
		},
	}
	gen := newCustomGenerator(normalBatch)

	var capturedAnomalies []*types.AnomalySignal
	anomalyHandler := func(ctx context.Context, sig *types.AnomalySignal) {
		capturedAnomalies = append(capturedAnomalies, sig)
	}

	batch, signals, err := runCollectionStep(ctx, gen, store, det, logger, anomalyHandler)
	if err != nil {
		t.Fatalf("runCollectionStep failed: %v", err)
	}
	if batch.BatchID != "batch-normal-101" {
		t.Errorf("expected batch_id %q, got %q", "batch-normal-101", batch.BatchID)
	}
	if len(signals) != 0 {
		t.Errorf("expected 0 anomaly signals, got %d", len(signals))
	}
	if len(capturedAnomalies) != 0 {
		t.Errorf("expected 0 captured anomalies, got %d", len(capturedAnomalies))
	}

	// Verify persistence in SQLite
	rec, err := store.GetBatch(ctx, "batch-normal-101")
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if rec.SyncStatus != storage.SyncStatusPending {
		t.Errorf("expected status PENDING, got %s", rec.SyncStatus)
	}
}

// ----------------------------------------------------------------------------
// TEST 2: Anomalous Telemetry
// Flow: Generate -> Validate -> SQLite Persist -> Detect -> AnomalySignal -> Persisted
// ----------------------------------------------------------------------------
func TestPipeline_AnomalousTelemetry(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test2_anomaly.db")
	logger := newDiscardLogger()

	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	det, err := detector.NewThresholdDetector(detector.ThresholdDetectorConfig{
		Version: "1.0.0",
		Rules:   detector.DefaultRules(), // CPU threshold is 90.0
	})
	if err != nil {
		t.Fatalf("failed to create detector: %v", err)
	}

	now := time.Now().UTC()
	anomBatch := &types.TelemetryBatch{
		BatchID:        "batch-anom-202",
		NodeID:         "node-test-2",
		SequenceNumber: 0,
		CollectedAt:    now,
		Metrics: []types.MetricSample{
			{NodeID: "node-test-2", Name: "cpu_usage_percent", Value: 95.0, Timestamp: now}, // Breach: 95 > 90
		},
	}
	gen := newCustomGenerator(anomBatch)

	var handledAnomalies []*types.AnomalySignal
	anomalyHandler := func(ctx context.Context, sig *types.AnomalySignal) {
		handledAnomalies = append(handledAnomalies, sig)
	}

	batch, signals, err := runCollectionStep(ctx, gen, store, det, logger, anomalyHandler)
	if err != nil {
		t.Fatalf("runCollectionStep failed: %v", err)
	}
	if batch.BatchID != "batch-anom-202" {
		t.Errorf("expected batch_id %q, got %q", "batch-anom-202", batch.BatchID)
	}

	// Verify AnomalySignal produced
	if len(signals) != 1 {
		t.Fatalf("expected exactly 1 anomaly signal, got %d", len(signals))
	}
	sig := signals[0]
	if sig.MetricName != "cpu_usage_percent" {
		t.Errorf("expected metric 'cpu_usage_percent', got %q", sig.MetricName)
	}
	if sig.ObservedValue != 95.0 {
		t.Errorf("expected observed value 95.0, got %f", sig.ObservedValue)
	}
	if sig.CorrelationID != "batch-anom-202" {
		t.Errorf("expected CorrelationID 'batch-anom-202', got %q", sig.CorrelationID)
	}
	if sig.NodeID != "node-test-2" {
		t.Errorf("expected NodeID 'node-test-2', got %q", sig.NodeID)
	}

	// Verify handler received the exact signal
	if len(handledAnomalies) != 1 {
		t.Fatalf("expected handler to receive 1 signal, got %d", len(handledAnomalies))
	}
	if handledAnomalies[0].AnomalyID != sig.AnomalyID {
		t.Errorf("expected handler anomaly ID %q, got %q", sig.AnomalyID, handledAnomalies[0].AnomalyID)
	}

	// Critical Invariant: Durability is preserved
	rec, err := store.GetBatch(ctx, "batch-anom-202")
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if rec.SyncStatus != storage.SyncStatusPending {
		t.Errorf("expected status PENDING, got %s", rec.SyncStatus)
	}
}

// ----------------------------------------------------------------------------
// TEST 3: Detector Failure Error Isolation
// Flow: Generate -> Persist Succeeds -> Detector Returns Error -> Telemetry Remains Persisted
// ----------------------------------------------------------------------------
func TestPipeline_DetectorFailure_ErrorIsolation(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test3_detector_fail.db")
	logger := newDiscardLogger()

	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	faulty := &faultyDetector{err: errors.New("simulated algorithmic failure")}

	now := time.Now().UTC()
	batchData := &types.TelemetryBatch{
		BatchID:        "batch-fail-303",
		NodeID:         "node-test-3",
		SequenceNumber: 0,
		CollectedAt:    now,
		Metrics: []types.MetricSample{
			{NodeID: "node-test-3", Name: "cpu_usage_percent", Value: 50.0, Timestamp: now},
		},
	}
	gen := newCustomGenerator(batchData)

	// Step must NOT return error because persistence succeeded
	batch, signals, err := runCollectionStep(ctx, gen, store, faulty, logger)
	if err != nil {
		t.Fatalf("expected collection step to succeed despite detector error, got error: %v", err)
	}
	if batch.BatchID != "batch-fail-303" {
		t.Errorf("expected batch_id %q, got %q", "batch-fail-303", batch.BatchID)
	}
	if len(signals) != 0 {
		t.Errorf("expected 0 signals on detector error, got %d", len(signals))
	}

	// Verify telemetry remains successfully persisted in SQLite WAL
	rec, err := store.GetBatch(ctx, "batch-fail-303")
	if err != nil {
		t.Fatalf("persisted batch missing from database: %v", err)
	}
	if rec.SyncStatus != storage.SyncStatusPending {
		t.Errorf("expected status PENDING, got %s", rec.SyncStatus)
	}
}

// ----------------------------------------------------------------------------
// TEST 4: Detector Failure Does Not Affect Synchronization
// Flow: Detector fails -> Batch persisted -> Syncer successfully syncs batch
// ----------------------------------------------------------------------------
func TestPipeline_DetectorFailureDoesNotAffectSync(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test4_sync.db")
	logger := newDiscardLogger()

	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	faulty := &faultyDetector{err: errors.New("broken detector")}

	now := time.Now().UTC()
	batchData := &types.TelemetryBatch{
		BatchID:        "batch-sync-404",
		NodeID:         "node-test-4",
		SequenceNumber: 0,
		CollectedAt:    now,
		Metrics: []types.MetricSample{
			{NodeID: "node-test-4", Name: "cpu_usage_percent", Value: 55.0, Timestamp: now},
		},
	}
	gen := newCustomGenerator(batchData)

	// Run collection step with failing detector
	_, _, err = runCollectionStep(ctx, gen, store, faulty, logger)
	if err != nil {
		t.Fatalf("collection step failed: %v", err)
	}

	// Mock control plane HTTP server
	var receivedBatchID string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var incoming types.TelemetryBatch
		_ = json.NewDecoder(r.Body).Decode(&incoming)
		receivedBatchID = incoming.BatchID

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":   "accepted",
			"batch_id": incoming.BatchID,
		})
	}))
	defer ts.Close()

	// Initialize syncer with mock control plane
	client := edgesync.NewHTTPClient(ts.URL)
	syncer := edgesync.NewSyncer(store, client, logger)

	// Sync pending batches
	stats, err := syncer.SyncPendingBatches(ctx, 10)
	if err != nil {
		t.Fatalf("SyncPendingBatches failed: %v", err)
	}
	if stats.TotalSynced != 1 {
		t.Fatalf("expected 1 batch synced, got %d", stats.TotalSynced)
	}
	if receivedBatchID != "batch-sync-404" {
		t.Errorf("expected synced batch 'batch-sync-404', got %q", receivedBatchID)
	}

	// Verify batch is now marked SYNCED in SQLite
	rec, err := store.GetBatch(ctx, "batch-sync-404")
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if rec.SyncStatus != storage.SyncStatusSynced {
		t.Errorf("expected status SYNCED, got %s", rec.SyncStatus)
	}
}

// ----------------------------------------------------------------------------
// TEST 5: Multiple Metrics (Normal, Anomalous, Normal)
// ----------------------------------------------------------------------------
func TestPipeline_MultipleMetrics(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test5_multi_metrics.db")
	logger := newDiscardLogger()

	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	det, err := detector.NewThresholdDetector(detector.ThresholdDetectorConfig{
		Version: "1.0.0",
		Rules:   detector.DefaultRules(), // memory_usage_percent threshold is 90.0
	})
	if err != nil {
		t.Fatalf("failed to create detector: %v", err)
	}

	now := time.Now().UTC()
	mixedBatch := &types.TelemetryBatch{
		BatchID:        "batch-mixed-505",
		NodeID:         "node-test-5",
		SequenceNumber: 0,
		CollectedAt:    now,
		Metrics: []types.MetricSample{
			{NodeID: "node-test-5", Name: "cpu_usage_percent", Value: 50.0, Timestamp: now},    // Normal
			{NodeID: "node-test-5", Name: "memory_usage_percent", Value: 96.0, Timestamp: now}, // Anomalous (>90)
			{NodeID: "node-test-5", Name: "disk_usage_percent", Value: 35.0, Timestamp: now},   // Normal
			{NodeID: "node-test-5", Name: "temperature_celsius", Value: 48.0, Timestamp: now},  // Normal (<85)
		},
	}
	gen := newCustomGenerator(mixedBatch)

	batch, signals, err := runCollectionStep(ctx, gen, store, det, logger)
	if err != nil {
		t.Fatalf("runCollectionStep failed: %v", err)
	}

	// Exactly 1 anomaly produced
	if len(signals) != 1 {
		t.Fatalf("expected exactly 1 anomaly signal, got %d", len(signals))
	}
	if signals[0].MetricName != "memory_usage_percent" {
		t.Errorf("expected anomaly for 'memory_usage_percent', got %q", signals[0].MetricName)
	}
	if signals[0].ObservedValue != 96.0 {
		t.Errorf("expected observed value 96.0, got %f", signals[0].ObservedValue)
	}

	// All 4 metrics preserved in database
	rec, err := store.GetBatch(ctx, batch.BatchID)
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if len(rec.Payload.Metrics) != 4 {
		t.Errorf("expected all 4 metrics persisted, got %d", len(rec.Payload.Metrics))
	}
}

// ----------------------------------------------------------------------------
// TEST 6: Multiple Nodes Isolation
// ----------------------------------------------------------------------------
func TestPipeline_MultipleNodes_Isolation(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test6_multi_nodes.db")
	logger := newDiscardLogger()

	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	det, err := detector.NewThresholdDetector(detector.ThresholdDetectorConfig{
		Version: "1.0.0",
		Rules:   detector.DefaultRules(),
	})
	if err != nil {
		t.Fatalf("failed to create detector: %v", err)
	}

	now := time.Now().UTC()

	// 1. Node A breaches CPU (95 > 90)
	batchA1 := &types.TelemetryBatch{
		BatchID: "batch-A1", NodeID: "node-A", SequenceNumber: 0, CollectedAt: now,
		Metrics: []types.MetricSample{{NodeID: "node-A", Name: "cpu_usage_percent", Value: 95.0, Timestamp: now}},
	}
	_, sigA1, err := runCollectionStep(ctx, newCustomGenerator(batchA1), store, det, logger)
	if err != nil || len(sigA1) != 1 {
		t.Fatalf("expected breach on Node A, got sigs=%d, err=%v", len(sigA1), err)
	}

	// 2. Node B sends nominal CPU (50 < 90) -> No anomaly
	batchB1 := &types.TelemetryBatch{
		BatchID: "batch-B1", NodeID: "node-B", SequenceNumber: 0, CollectedAt: now,
		Metrics: []types.MetricSample{{NodeID: "node-B", Name: "cpu_usage_percent", Value: 50.0, Timestamp: now}},
	}
	_, sigB1, err := runCollectionStep(ctx, newCustomGenerator(batchB1), store, det, logger)
	if err != nil || len(sigB1) != 0 {
		t.Fatalf("expected nominal on Node B, got sigs=%d, err=%v", len(sigB1), err)
	}

	// 3. Node A sends nominal Memory (60 < 90) -> No anomaly
	batchA2 := &types.TelemetryBatch{
		BatchID: "batch-A2", NodeID: "node-A", SequenceNumber: 1, CollectedAt: now.Add(time.Second),
		Metrics: []types.MetricSample{{NodeID: "node-A", Name: "memory_usage_percent", Value: 60.0, Timestamp: now.Add(time.Second)}},
	}
	_, sigA2, err := runCollectionStep(ctx, newCustomGenerator(batchA2), store, det, logger)
	if err != nil || len(sigA2) != 0 {
		t.Fatalf("expected nominal on Node A memory, got sigs=%d, err=%v", len(sigA2), err)
	}

	// 4. Node A sends continuing CPU breach (96 > 90) -> Persistent breach (no duplicate)
	batchA3 := &types.TelemetryBatch{
		BatchID: "batch-A3", NodeID: "node-A", SequenceNumber: 2, CollectedAt: now.Add(2 * time.Second),
		Metrics: []types.MetricSample{{NodeID: "node-A", Name: "cpu_usage_percent", Value: 96.0, Timestamp: now.Add(2 * time.Second)}},
	}
	_, sigA3, err := runCollectionStep(ctx, newCustomGenerator(batchA3), store, det, logger)
	if err != nil || len(sigA3) != 0 {
		t.Fatalf("expected persistent breach on Node A CPU to return 0 signals, got %d", len(sigA3))
	}

	// 5. Node B now breaches CPU (94 > 90) -> MUST emit NEW anomaly for Node B!
	batchB2 := &types.TelemetryBatch{
		BatchID: "batch-B2", NodeID: "node-B", SequenceNumber: 1, CollectedAt: now.Add(2 * time.Second),
		Metrics: []types.MetricSample{{NodeID: "node-B", Name: "cpu_usage_percent", Value: 94.0, Timestamp: now.Add(2 * time.Second)}},
	}
	_, sigB2, err := runCollectionStep(ctx, newCustomGenerator(batchB2), store, det, logger)
	if err != nil || len(sigB2) != 1 {
		t.Fatalf("expected breach on Node B, got sigs=%d, err=%v", len(sigB2), err)
	}
	if sigB2[0].NodeID != "node-B" {
		t.Errorf("expected NodeID 'node-B', got %q", sigB2[0].NodeID)
	}
}

// ----------------------------------------------------------------------------
// TEST 7: Persistent Breach and Recovery Flow
// Sequence: Breach -> Persistent breach -> Recovery -> New breach
// ----------------------------------------------------------------------------
func TestPipeline_PersistentBreachAndRecovery(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test7_persistent.db")
	logger := newDiscardLogger()

	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	det, err := detector.NewThresholdDetector(detector.ThresholdDetectorConfig{
		Version: "1.0.0",
		Rules:   detector.DefaultRules(), // CPU: breach > 90, recovery <= 80
	})
	if err != nil {
		t.Fatalf("failed to create detector: %v", err)
	}

	now := time.Now().UTC()

	// 1. Initial breach onset (95.0 > 90.0) -> Emits AnomalySignal
	b1 := &types.TelemetryBatch{
		BatchID: "b-01", NodeID: "node-persist", SequenceNumber: 0, CollectedAt: now,
		Metrics: []types.MetricSample{{NodeID: "node-persist", Name: "cpu_usage_percent", Value: 95.0, Timestamp: now}},
	}
	_, sigs1, err := runCollectionStep(ctx, newCustomGenerator(b1), store, det, logger)
	if err != nil || len(sigs1) != 1 {
		t.Fatalf("cycle 1: expected 1 anomaly, got %d, err=%v", len(sigs1), err)
	}

	// 2. Persistent breach (96.0) -> 0 signals
	b2 := &types.TelemetryBatch{
		BatchID: "b-02", NodeID: "node-persist", SequenceNumber: 1, CollectedAt: now.Add(time.Second),
		Metrics: []types.MetricSample{{NodeID: "node-persist", Name: "cpu_usage_percent", Value: 96.0, Timestamp: now.Add(time.Second)}},
	}
	_, sigs2, err := runCollectionStep(ctx, newCustomGenerator(b2), store, det, logger)
	if err != nil || len(sigs2) != 0 {
		t.Fatalf("cycle 2: expected 0 anomalies (persistent breach), got %d, err=%v", len(sigs2), err)
	}

	// 3. In recovery band (85.0: 80 < 85 <= 90) -> 0 signals
	b3 := &types.TelemetryBatch{
		BatchID: "b-03", NodeID: "node-persist", SequenceNumber: 2, CollectedAt: now.Add(2 * time.Second),
		Metrics: []types.MetricSample{{NodeID: "node-persist", Name: "cpu_usage_percent", Value: 85.0, Timestamp: now.Add(2 * time.Second)}},
	}
	_, sigs3, err := runCollectionStep(ctx, newCustomGenerator(b3), store, det, logger)
	if err != nil || len(sigs3) != 0 {
		t.Fatalf("cycle 3: expected 0 anomalies in recovery band, got %d, err=%v", len(sigs3), err)
	}

	// 4. Cleared below recovery threshold (78.0 <= 80) -> Cleared, 0 signals
	b4 := &types.TelemetryBatch{
		BatchID: "b-04", NodeID: "node-persist", SequenceNumber: 3, CollectedAt: now.Add(3 * time.Second),
		Metrics: []types.MetricSample{{NodeID: "node-persist", Name: "cpu_usage_percent", Value: 78.0, Timestamp: now.Add(3 * time.Second)}},
	}
	_, sigs4, err := runCollectionStep(ctx, newCustomGenerator(b4), store, det, logger)
	if err != nil || len(sigs4) != 0 {
		t.Fatalf("cycle 4: expected 0 anomalies upon clearing, got %d, err=%v", len(sigs4), err)
	}

	// 5. Nominal (85.0 <= 90) -> 0 signals
	b5 := &types.TelemetryBatch{
		BatchID: "b-05", NodeID: "node-persist", SequenceNumber: 4, CollectedAt: now.Add(4 * time.Second),
		Metrics: []types.MetricSample{{NodeID: "node-persist", Name: "cpu_usage_percent", Value: 85.0, Timestamp: now.Add(4 * time.Second)}},
	}
	_, sigs5, err := runCollectionStep(ctx, newCustomGenerator(b5), store, det, logger)
	if err != nil || len(sigs5) != 0 {
		t.Fatalf("cycle 5: expected 0 anomalies at nominal 85.0, got %d, err=%v", len(sigs5), err)
	}

	// 6. New breach onset (92.0 > 90.0) -> Emits NEW AnomalySignal!
	b6 := &types.TelemetryBatch{
		BatchID: "b-06", NodeID: "node-persist", SequenceNumber: 5, CollectedAt: now.Add(5 * time.Second),
		Metrics: []types.MetricSample{{NodeID: "node-persist", Name: "cpu_usage_percent", Value: 92.0, Timestamp: now.Add(5 * time.Second)}},
	}
	_, sigs6, err := runCollectionStep(ctx, newCustomGenerator(b6), store, det, logger)
	if err != nil || len(sigs6) != 1 {
		t.Fatalf("cycle 6: expected 1 NEW anomaly on re-breach, got %d, err=%v", len(sigs6), err)
	}
	if sigs6[0].AnomalyID == sigs1[0].AnomalyID {
		t.Errorf("expected distinct AnomalyID for new breach, got same ID")
	}

	// Verify all 6 batches are safely stored in SQLite
	count, err := store.CountBatches(ctx)
	if err != nil {
		t.Fatalf("CountBatches failed: %v", err)
	}
	if count != 6 {
		t.Errorf("expected all 6 batches persisted, got %d", count)
	}
}

// ----------------------------------------------------------------------------
// TEST 8: Restart Semantics (Process-Local Detector State Resets)
// ----------------------------------------------------------------------------
func TestPipeline_RestartSemantics(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test8_restart.db")
	logger := newDiscardLogger()

	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	now := time.Now().UTC()

	// --- Process 1 Lifetime ---
	det1, err := detector.NewThresholdDetector(detector.ThresholdDetectorConfig{
		Version: "1.0.0",
		Rules:   detector.DefaultRules(),
	})
	if err != nil {
		t.Fatalf("failed to create detector 1: %v", err)
	}

	b1 := &types.TelemetryBatch{
		BatchID: "b-restart-1", NodeID: "node-restart", SequenceNumber: 0, CollectedAt: now,
		Metrics: []types.MetricSample{{NodeID: "node-restart", Name: "cpu_usage_percent", Value: 95.0, Timestamp: now}},
	}
	_, sigs1, err := runCollectionStep(ctx, newCustomGenerator(b1), store, det1, logger)
	if err != nil || len(sigs1) != 1 {
		t.Fatalf("process 1 cycle 1: expected 1 anomaly, got %d, err=%v", len(sigs1), err)
	}

	b2 := &types.TelemetryBatch{
		BatchID: "b-restart-2", NodeID: "node-restart", SequenceNumber: 1, CollectedAt: now.Add(time.Second),
		Metrics: []types.MetricSample{{NodeID: "node-restart", Name: "cpu_usage_percent", Value: 96.0, Timestamp: now.Add(time.Second)}},
	}
	_, sigs2, err := runCollectionStep(ctx, newCustomGenerator(b2), store, det1, logger)
	if err != nil || len(sigs2) != 0 {
		t.Fatalf("process 1 cycle 2: expected 0 anomalies (persistent breach), got %d, err=%v", len(sigs2), err)
	}

	// --- SIMULATE PROCESS RESTART ---
	// In Version 1, detector hysteresis state is process-local and resets upon restart.
	// Detector 2 represents the fresh edge process upon reboot.
	det2, err := detector.NewThresholdDetector(detector.ThresholdDetectorConfig{
		Version: "1.0.0",
		Rules:   detector.DefaultRules(),
	})
	if err != nil {
		t.Fatalf("failed to create detector 2: %v", err)
	}

	// Cycle 3: Value 96.0 arrived after restart.
	// Since detector 2 starts fresh (cold-start semantics), it instantaneously evaluates
	// 96.0 against the static threshold guardrail (W=1) as a breach onset for this process lifetime.
	b3 := &types.TelemetryBatch{
		BatchID: "b-restart-3", NodeID: "node-restart", SequenceNumber: 2, CollectedAt: now.Add(2 * time.Second),
		Metrics: []types.MetricSample{{NodeID: "node-restart", Name: "cpu_usage_percent", Value: 96.0, Timestamp: now.Add(2 * time.Second)}},
	}
	_, sigs3, err := runCollectionStep(ctx, newCustomGenerator(b3), store, det2, logger)
	if err != nil {
		t.Fatalf("process 2 cycle 3 failed: %v", err)
	}
	if len(sigs3) != 1 {
		t.Fatalf("process 2 cycle 3: expected fresh detector to emit cold-start safety breach (W=1), got %d", len(sigs3))
	}
	if sigs3[0].ObservedValue != 96.0 {
		t.Errorf("expected observed value 96.0, got %f", sigs3[0].ObservedValue)
	}

	// Verify all 3 batches are intact in the durable SQLite store across the simulated restart
	count, err := store.CountBatches(ctx)
	if err != nil {
		t.Fatalf("CountBatches failed: %v", err)
	}
	if count != 3 {
		t.Errorf("expected all 3 batches persisted, got %d", count)
	}
}

// ----------------------------------------------------------------------------
// TEST 9: Persistence Failure Semantics
// Flow: Persistence fails -> Detector is NEVER called -> Telemetry rejected
// ----------------------------------------------------------------------------
func TestPipeline_PersistenceFailure_DoesNotRunDetector(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test9_persist_fail.db")
	logger := newDiscardLogger()

	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}

	// Force persistence failure by closing the store prematurely
	_ = store.Close()

	spy := &spyDetector{}
	now := time.Now().UTC()
	b := &types.TelemetryBatch{
		BatchID: "b-fail-pers", NodeID: "node-fail", SequenceNumber: 0, CollectedAt: now,
		Metrics: []types.MetricSample{{NodeID: "node-fail", Name: "cpu_usage_percent", Value: 99.0, Timestamp: now}},
	}
	gen := newCustomGenerator(b)

	// Step must fail
	_, _, err = runCollectionStep(ctx, gen, store, spy, logger)
	if err == nil {
		t.Fatal("expected error on persistence failure, got nil")
	}

	// CRITICAL: Detector MUST NOT be called if persistence fails
	spy.mu.Lock()
	called := spy.called
	spy.mu.Unlock()
	if called {
		t.Fatal("critical invariant violated: detector was executed despite persistence failure!")
	}
}

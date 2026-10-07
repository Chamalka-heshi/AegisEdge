package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/detector"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/metrics"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/telemetry"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

func newDiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestRunCollectionStep_SuccessAndConfirmation(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "step_test.db")
	logger := newDiscardLogger()

	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	nodeID := "node-step-test"
	gen, err := telemetry.NewSimulatedGenerator(nodeID, 0)
	if err != nil {
		t.Fatalf("NewSimulatedGenerator failed: %v", err)
	}

	// Execute step 1
	b1, _, err := runCollectionStep(ctx, gen, store, nil, logger)
	if err != nil {
		t.Fatalf("runCollectionStep 1 failed: %v", err)
	}
	if b1.SequenceNumber != 0 {
		t.Errorf("expected sequence 0, got %d", b1.SequenceNumber)
	}

	// Verify it was persisted and can be queried directly from store
	rec1, err := store.GetBatch(ctx, b1.BatchID)
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if rec1.SyncStatus != storage.SyncStatusPending {
		t.Errorf("expected status PENDING, got %s", rec1.SyncStatus)
	}

	// Execute step 2
	b2, _, err := runCollectionStep(ctx, gen, store, nil, logger)
	if err != nil {
		t.Fatalf("runCollectionStep 2 failed: %v", err)
	}
	if b2.SequenceNumber != 1 {
		t.Errorf("expected sequence 1, got %d", b2.SequenceNumber)
	}

	// Verify total count in database is 2
	count, err := store.CountBatches(ctx)
	if err != nil {
		t.Fatalf("CountBatches failed: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 batches stored, got %d", count)
	}
}

func TestRunCollectionStep_PersistenceFailure(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "failure_step.db")
	logger := newDiscardLogger()

	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}

	// Close store prematurely to induce persistence failure
	_ = store.Close()

	gen, _ := telemetry.NewSimulatedGenerator("node-err", 0)

	// Collection step must return an error and not pretend it was accepted
	_, _, err = runCollectionStep(ctx, gen, store, nil, logger)
	if err == nil {
		t.Fatal("expected error on closed store, got nil")
	}
}

type mockFaultyDetector struct {
	name    string
	version string
	err     error
}

func (m *mockFaultyDetector) Name() string    { return m.name }
func (m *mockFaultyDetector) Version() string { return m.version }
func (m *mockFaultyDetector) Detect(ctx context.Context, sample types.MetricSample) (*types.AnomalySignal, error) {
	return nil, m.err
}
func (m *mockFaultyDetector) DetectBatch(ctx context.Context, batch *types.TelemetryBatch) ([]*types.AnomalySignal, error) {
	return nil, m.err
}

func TestInstrumentedDetector_MetricsRecording(t *testing.T) {
	ctx := context.Background()
	rec := metrics.NewDefaultRecorder(nil)

	// 1. Threshold detector with normal sample (no anomaly, no error)
	rawDet, err := detector.NewThresholdDetector(detector.ThresholdDetectorConfig{
		Version: "1.0.0",
		Rules:   detector.DefaultRules(),
	})
	if err != nil {
		t.Fatalf("failed to create threshold detector: %v", err)
	}
	instDet := newInstrumentedDetector(rawDet, rec)

	normalBatch := &types.TelemetryBatch{
		BatchID:        "b-normal",
		NodeID:         "node-test",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics: []types.MetricSample{
			{NodeID: "node-test", Name: "cpu_usage_percent", Value: 50.0, Timestamp: time.Now().UTC()},
		},
	}

	sigs, err := instDet.DetectBatch(ctx, normalBatch)
	if err != nil {
		t.Fatalf("unexpected error on normal batch: %v", err)
	}
	if len(sigs) != 0 {
		t.Fatalf("expected 0 anomalies, got %d", len(sigs))
	}

	prom := rec.Registry().FormatPrometheus()
	if !strings.Contains(prom, `aegisedge_detector_duration_seconds_count{method="threshold"} 1`) {
		t.Errorf("expected detector duration to be recorded for normal batch, got:\n%s", prom)
	}
	if strings.Contains(prom, `aegisedge_detector_errors_total{`) {
		t.Errorf("expected NO detector errors for normal batch, got:\n%s", prom)
	}
	if strings.Contains(prom, `aegisedge_anomalies_detected_total{`) {
		t.Errorf("expected NO anomalies for normal batch, got:\n%s", prom)
	}

	// 2. Anomaly batch (CPU at 99.0 > threshold 90.0)
	anomalyBatch := &types.TelemetryBatch{
		BatchID:        "b-anomaly",
		NodeID:         "node-test",
		SequenceNumber: 2,
		CollectedAt:    time.Now().UTC(),
		Metrics: []types.MetricSample{
			{NodeID: "node-test", Name: "cpu_usage_percent", Value: 99.0, Timestamp: time.Now().UTC()},
		},
	}

	sigs, err = instDet.DetectBatch(ctx, anomalyBatch)
	if err != nil {
		t.Fatalf("unexpected error on anomalous batch: %v", err)
	}
	if len(sigs) != 1 {
		t.Fatalf("expected 1 anomaly, got %d", len(sigs))
	}

	prom = rec.Registry().FormatPrometheus()
	if !strings.Contains(prom, `aegisedge_anomalies_detected_total{method="threshold"} 1`) {
		t.Errorf("expected anomaly detected to be recorded, got:\n%s", prom)
	}

	// 3. Faulty detector: returns evaluation error
	faulty := &mockFaultyDetector{
		name:    "threshold",
		version: "1.0.0",
		err:     errors.New("simulated detection failure"),
	}
	instFaulty := newInstrumentedDetector(faulty, rec)

	_, err = instFaulty.DetectBatch(ctx, normalBatch)
	if err == nil {
		t.Fatal("expected error from faulty detector, got nil")
	}

	prom = rec.Registry().FormatPrometheus()
	if !strings.Contains(prom, `aegisedge_detector_errors_total{method="threshold"} 1`) {
		t.Errorf("expected detector error to be recorded, got:\n%s", prom)
	}
}

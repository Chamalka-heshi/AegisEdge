package detector

import (
	"context"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// helperBuildValidManifest constructs a minimal, mathematically predictable valid MLModelManifest.
//
// Tree structure (2 trees, 4 features):
// Tree 0:
//
//	Node 0 (root): split feature 0 (cpu) at 0.0 -> left child 1, right child 2
//	Node 1 (leaf): feature -1, size 1 (c(1) = 0)
//	Node 2 (leaf): feature -1, size 1 (c(1) = 0)
//
// Tree 1:
//
//	Node 0 (root): split feature 3 (temp) at 0.0 -> left child 1, right child 2
//	Node 1 (leaf): feature -1, size 1 (c(1) = 0)
//	Node 2 (leaf): feature -1, size 1 (c(1) = 0)
func helperBuildValidManifest(threshold float64) *MLModelManifest {
	manifest := &MLModelManifest{
		ModelID:              "iforest-dev-001",
		ModelVersion:         "1.0.0",
		Algorithm:            AlgorithmIsolationForest,
		FeatureSchemaVersion: FeatureSchemaV1,
		TrainingDatasetID:    "ds-2026-normal-baseline",
		CreatedAt:            time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC),
		InputDimensions:      4,
		SupportedMetrics: []string{
			"cpu_usage_percent",
			"memory_usage_percent",
			"disk_usage_percent",
			"temperature_celsius",
		},
		NormalizationParams: []FeatureNormalizationParams{
			{MetricName: "cpu_usage_percent", Mean: 50.0, StdDev: 10.0, Min: 0.0, Max: 100.0},
			{MetricName: "memory_usage_percent", Mean: 60.0, StdDev: 15.0, Min: 0.0, Max: 100.0},
			{MetricName: "disk_usage_percent", Mean: 40.0, StdDev: 5.0, Min: 0.0, Max: 100.0},
			{MetricName: "temperature_celsius", Mean: 50.0, StdDev: 10.0, Min: -40.0, Max: 125.0},
		},
		Trees: []IsolationTree{
			{
				RootIndex: 0,
				Nodes: []IsolationTreeNode{
					{FeatureIndex: 0, SplitValue: 0.0, LeftChild: 1, RightChild: 2, Size: 0},
					{FeatureIndex: -1, SplitValue: 0.0, LeftChild: -1, RightChild: -1, Size: 1},
					{FeatureIndex: -1, SplitValue: 0.0, LeftChild: -1, RightChild: -1, Size: 1},
				},
			},
			{
				RootIndex: 0,
				Nodes: []IsolationTreeNode{
					{FeatureIndex: 3, SplitValue: 0.0, LeftChild: 1, RightChild: 2, Size: 0},
					{FeatureIndex: -1, SplitValue: 0.0, LeftChild: -1, RightChild: -1, Size: 1},
					{FeatureIndex: -1, SplitValue: 0.0, LeftChild: -1, RightChild: -1, Size: 1},
				},
			},
		},
		SubSampleSize:     256,
		DecisionThreshold: threshold,
		Status:            ModelStatusActive,
	}

	checksum, _ := manifest.ComputeChecksum()
	manifest.ChecksumSHA256 = checksum
	return manifest
}

// -----------------------------------------------------------------------------
// 1. MODEL VALIDATION TESTS
// -----------------------------------------------------------------------------

func TestModelValidation_ValidManifest(t *testing.T) {
	manifest := helperBuildValidManifest(0.60)
	if err := manifest.Validate(); err != nil {
		t.Fatalf("expected valid manifest, got error: %v", err)
	}
}

func TestModelValidation_NilManifest(t *testing.T) {
	var manifest *MLModelManifest
	if err := manifest.Validate(); err == nil || !strings.Contains(err.Error(), "manifest cannot be nil") {
		t.Fatalf("expected nil manifest error, got: %v", err)
	}
}

func TestModelValidation_EmptyIDs(t *testing.T) {
	m1 := helperBuildValidManifest(0.60)
	m1.ModelID = "   "
	if err := m1.Validate(); err == nil || !strings.Contains(err.Error(), "model_id cannot be empty") {
		t.Fatalf("expected empty model_id error, got: %v", err)
	}

	m2 := helperBuildValidManifest(0.60)
	m2.ModelVersion = ""
	if err := m2.Validate(); err == nil || !strings.Contains(err.Error(), "model_version cannot be empty") {
		t.Fatalf("expected empty model_version error, got: %v", err)
	}
}

func TestModelValidation_UnsupportedAlgorithm(t *testing.T) {
	m := helperBuildValidManifest(0.60)
	m.Algorithm = "deep_autoencoder"
	if err := m.Validate(); err == nil || !strings.Contains(err.Error(), "unsupported algorithm") {
		t.Fatalf("expected unsupported algorithm error, got: %v", err)
	}
}

func TestModelValidation_UnsupportedFeatureSchema(t *testing.T) {
	m := helperBuildValidManifest(0.60)
	m.FeatureSchemaVersion = "features.v2.0.0"
	if err := m.Validate(); err == nil || !strings.Contains(err.Error(), "unsupported feature_schema_version") {
		t.Fatalf("expected unsupported feature schema error, got: %v", err)
	}
}

func TestModelValidation_InvalidDimensionsAndMetrics(t *testing.T) {
	// Zero dimensions
	m1 := helperBuildValidManifest(0.60)
	m1.InputDimensions = 0
	if err := m1.Validate(); err == nil || !strings.Contains(err.Error(), "input_dimensions") {
		t.Fatalf("expected invalid input_dimensions error, got: %v", err)
	}

	// Dimension mismatch
	m2 := helperBuildValidManifest(0.60)
	m2.InputDimensions = 5
	if err := m2.Validate(); err == nil || !strings.Contains(err.Error(), "len(supported_metrics)") {
		t.Fatalf("expected dimension mismatch error, got: %v", err)
	}

	// Duplicate metric name
	m3 := helperBuildValidManifest(0.60)
	m3.SupportedMetrics[1] = "cpu_usage_percent"
	m3.NormalizationParams[1].MetricName = "cpu_usage_percent"
	if err := m3.Validate(); err == nil || !strings.Contains(err.Error(), "duplicate metric") {
		t.Fatalf("expected duplicate metric error, got: %v", err)
	}

	// Unsupported metric name
	m4 := helperBuildValidManifest(0.60)
	m4.SupportedMetrics[0] = "gpu_fan_speed"
	m4.NormalizationParams[0].MetricName = "gpu_fan_speed"
	if err := m4.Validate(); err == nil || !strings.Contains(err.Error(), "not in canonical schema") {
		t.Fatalf("expected unsupported metric error, got: %v", err)
	}
}

func TestModelValidation_InvalidNormalizationParams(t *testing.T) {
	// Zero or negative stddev
	m1 := helperBuildValidManifest(0.60)
	m1.NormalizationParams[0].StdDev = 0.0
	if err := m1.Validate(); err == nil || !strings.Contains(err.Error(), "invalid stddev") {
		t.Fatalf("expected zero stddev error, got: %v", err)
	}

	// NaN mean
	m2 := helperBuildValidManifest(0.60)
	m2.NormalizationParams[0].Mean = math.NaN()
	if err := m2.Validate(); err == nil || !strings.Contains(err.Error(), "non-finite mean") {
		t.Fatalf("expected NaN mean error, got: %v", err)
	}

	// Min > Max
	m3 := helperBuildValidManifest(0.60)
	m3.NormalizationParams[0].Min = 100.0
	m3.NormalizationParams[0].Max = 50.0
	if err := m3.Validate(); err == nil || !strings.Contains(err.Error(), "min (100.000000) > max (50.000000)") {
		t.Fatalf("expected min > max error, got: %v", err)
	}
}

func TestModelValidation_InvalidThresholdAndSubsample(t *testing.T) {
	// DecisionThreshold <= 0.0
	m1 := helperBuildValidManifest(0.60)
	m1.DecisionThreshold = 0.0
	if err := m1.Validate(); err == nil || !strings.Contains(err.Error(), "decision_threshold") {
		t.Fatalf("expected invalid decision_threshold error, got: %v", err)
	}

	// DecisionThreshold >= 1.0
	m2 := helperBuildValidManifest(0.60)
	m2.DecisionThreshold = 1.05
	if err := m2.Validate(); err == nil || !strings.Contains(err.Error(), "decision_threshold") {
		t.Fatalf("expected invalid decision_threshold error, got: %v", err)
	}

	// SubSampleSize < 2
	m3 := helperBuildValidManifest(0.60)
	m3.SubSampleSize = 1
	if err := m3.Validate(); err == nil || !strings.Contains(err.Error(), "sub_sample_size") {
		t.Fatalf("expected invalid sub_sample_size error, got: %v", err)
	}
}

func TestModelValidation_TreeTopologyAndCycles(t *testing.T) {
	// Empty trees
	m1 := helperBuildValidManifest(0.60)
	m1.Trees = nil
	if err := m1.Validate(); err == nil || !strings.Contains(err.Error(), "model must contain at least one isolation tree") {
		t.Fatalf("expected empty trees error, got: %v", err)
	}

	// Invalid root index
	m2 := helperBuildValidManifest(0.60)
	m2.Trees[0].RootIndex = 99
	if err := m2.Validate(); err == nil || !strings.Contains(err.Error(), "invalid root index") {
		t.Fatalf("expected invalid root index error, got: %v", err)
	}

	// Invalid child index
	m3 := helperBuildValidManifest(0.60)
	m3.Trees[0].Nodes[0].LeftChild = 99
	if err := m3.Validate(); err == nil || !strings.Contains(err.Error(), "invalid left_child") {
		t.Fatalf("expected invalid left_child error, got: %v", err)
	}

	// Out-of-bounds feature index
	m4 := helperBuildValidManifest(0.60)
	m4.Trees[0].Nodes[0].FeatureIndex = 10
	if err := m4.Validate(); err == nil || !strings.Contains(err.Error(), "out of bounds") {
		t.Fatalf("expected out-of-bounds feature index error, got: %v", err)
	}

	// Malformed leaf node (size < 1)
	m5 := helperBuildValidManifest(0.60)
	m5.Trees[0].Nodes[1].Size = 0
	if err := m5.Validate(); err == nil || !strings.Contains(err.Error(), "invalid size") {
		t.Fatalf("expected invalid leaf size error, got: %v", err)
	}

	// Cycle / multiple paths to same node (node 0 points left to 1, right to 1)
	m6 := helperBuildValidManifest(0.60)
	m6.Trees[0].Nodes[0].RightChild = 1
	if err := m6.Validate(); err == nil || !strings.Contains(err.Error(), "identical left and right child") {
		t.Fatalf("expected cycle/identical child error, got: %v", err)
	}

	// Unreachable node (node 3 added to Nodes slice but never referenced)
	m7 := helperBuildValidManifest(0.60)
	m7.Trees[0].Nodes = append(m7.Trees[0].Nodes, IsolationTreeNode{
		FeatureIndex: -1, SplitValue: 0.0, LeftChild: -1, RightChild: -1, Size: 1,
	})
	if err := m7.Validate(); err == nil || !strings.Contains(err.Error(), "unreachable from root") {
		t.Fatalf("expected unreachable node error, got: %v", err)
	}
}

func TestModelValidation_ChecksumVerification(t *testing.T) {
	manifest := helperBuildValidManifest(0.60)
	manifest.ChecksumSHA256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	err := manifest.Validate()
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected checksum mismatch error, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// 2. MATHEMATICAL CORRECTNESS & ISOLATION FOREST EQUATIONS
// -----------------------------------------------------------------------------

func TestMathematicalCorrectness_ExpectedPathLength(t *testing.T) {
	// Base cases
	if c0 := ExpectedPathLength(0); c0 != 0.0 {
		t.Fatalf("c(0) must be 0.0, got %f", c0)
	}
	if c1 := ExpectedPathLength(1); c1 != 0.0 {
		t.Fatalf("c(1) must be 0.0, got %f", c1)
	}
	if c2 := ExpectedPathLength(2); c2 != 1.0 {
		t.Fatalf("c(2) must be 1.0, got %f", c2)
	}

	// For n = 256:
	// c(256) = 2 * (ln(255) + 0.5772156649) - 2 * (255) / 256
	// ln(255) = 5.541263545...
	// c(256) ~= 10.24477...
	c256 := ExpectedPathLength(256)
	expectedC256 := 2.0*(math.Log(255.0)+EulerMascheroni) - (2.0 * 255.0 / 256.0)
	if math.Abs(c256-expectedC256) > 1e-9 {
		t.Fatalf("c(256) mismatch: got %.9f, expected %.9f", c256, expectedC256)
	}
	if math.Abs(c256-10.24477) > 0.001 {
		t.Fatalf("c(256) outside expected range ~10.245: got %f", c256)
	}

	// Leaf correction for n = 10:
	// c(10) = 2 * (ln(9) + 0.5772156649) - 2 * 9 / 10 = 3.74888...
	c10 := ExpectedPathLength(10)
	expectedC10 := 2.0*(math.Log(9.0)+EulerMascheroni) - (2.0 * 9.0 / 10.0)
	if math.Abs(c10-expectedC10) > 1e-9 {
		t.Fatalf("c(10) mismatch: got %.9f, expected %.9f", c10, expectedC10)
	}
}

func TestMathematicalCorrectness_HandVerifiableInference(t *testing.T) {
	manifest := helperBuildValidManifest(0.60)
	det, err := NewMLDetector(MLDetectorConfig{Manifest: manifest})
	if err != nil {
		t.Fatalf("failed to create MLDetector: %v", err)
	}

	// 1. An observation that terminates at left leaf of Tree 0 and left leaf of Tree 1:
	// Tree 0 splits feature 0 at 0.0 (normalized).
	// Feature 0 has mean 50.0, stddev 10.0.
	// Raw CPU = 30.0 -> normalized = (30 - 50)/10 = -2.0 < 0.0 -> left child (leaf 1, size 1).
	// Path length in Tree 0 = 1 edge + c(1) = 1.0.
	// Tree 1 splits feature 3 at 0.0 (normalized).
	// Feature 3 has mean 50.0, stddev 10.0.
	// Raw Temp = 30.0 -> normalized = (30 - 50)/10 = -2.0 < 0.0 -> left child (leaf 1, size 1).
	// Path length in Tree 1 = 1 edge + c(1) = 1.0.
	//
	// Mean path length E(h) = (1.0 + 1.0)/2 = 1.0.
	// c(psi=256) ~= 10.24477.
	// Score s = 2^(-1.0 / c(256)) = 2^(-1.0 / 10.24477) ~= 0.93458.
	// Decision threshold tau = 0.60.
	// s >= 0.60 -> Anomaly detected!
	// Intensity = (s - 0.60) / (1.0 - 0.60) = (0.93458 - 0.60) / 0.40 ~= 0.83645.

	rawVector := []float64{30.0, 60.0, 40.0, 30.0}
	normVector, err := det.NormalizeFeatures(rawVector)
	if err != nil {
		t.Fatalf("normalization failed: %v", err)
	}

	avgPath, score, topFeature, err := det.Score(normVector)
	if err != nil {
		t.Fatalf("scoring failed: %v", err)
	}

	if math.Abs(avgPath-1.0) > 1e-9 {
		t.Fatalf("expected avgPath 1.0, got %f", avgPath)
	}

	expectedScore := math.Pow(2.0, -1.0/ExpectedPathLength(256))
	if math.Abs(score-expectedScore) > 1e-9 {
		t.Fatalf("expected score %f, got %f", expectedScore, score)
	}
	if topFeature != "cpu_usage_percent" && topFeature != "temperature_celsius" {
		t.Fatalf("unexpected top diagnostic feature: %s", topFeature)
	}

	// 2. An observation that takes right leaf of both trees:
	// Raw CPU = 70.0 -> normalized = +2.0 >= 0.0 -> right child (leaf 2, size 1).
	// Raw Temp = 70.0 -> normalized = +2.0 >= 0.0 -> right child (leaf 2, size 1).
	// Both produce path length 1.0. Score is identical.
	rawVector2 := []float64{70.0, 60.0, 40.0, 70.0}
	normVector2, _ := det.NormalizeFeatures(rawVector2)
	avgPath2, score2, _, _ := det.Score(normVector2)
	if math.Abs(avgPath2-1.0) > 1e-9 || math.Abs(score2-expectedScore) > 1e-9 {
		t.Fatalf("expected symmetric score %f, got %f", expectedScore, score2)
	}
}

// -----------------------------------------------------------------------------
// 3. FEATURE EXTRACTION & NORMALIZATION TESTS
// -----------------------------------------------------------------------------

func TestFeatureExtraction_ExactOrderAndMissing(t *testing.T) {
	manifest := helperBuildValidManifest(0.60)
	det, err := NewMLDetector(MLDetectorConfig{Manifest: manifest})
	if err != nil {
		t.Fatalf("failed to create detector: %v", err)
	}

	now := time.Now().UTC()

	// Complete batch
	batch := &types.TelemetryBatch{
		BatchID:        "batch-ml-001",
		NodeID:         "node-test",
		SequenceNumber: 1,
		CollectedAt:    now,
		Metrics: []types.MetricSample{
			{Name: "temperature_celsius", Value: 65.0, Unit: "celsius", Timestamp: now},
			{Name: "cpu_usage_percent", Value: 45.0, Unit: "percent", Timestamp: now},
			{Name: "disk_usage_percent", Value: 38.0, Unit: "percent", Timestamp: now},
			{Name: "memory_usage_percent", Value: 55.0, Unit: "percent", Timestamp: now},
		},
	}

	features, err := det.ExtractFeaturesFromBatch(batch)
	if err != nil {
		t.Fatalf("feature extraction failed: %v", err)
	}

	// Must match canonical order: cpu (index 0), mem (index 1), disk (index 2), temp (index 3)
	if features[0] != 45.0 || features[1] != 55.0 || features[2] != 38.0 || features[3] != 65.0 {
		t.Fatalf("feature vector out of canonical order: got %v", features)
	}

	// Missing metric (cpu missing)
	incompleteBatch := &types.TelemetryBatch{
		BatchID:        "batch-ml-incomplete",
		NodeID:         "node-test",
		SequenceNumber: 2,
		CollectedAt:    now,
		Metrics: []types.MetricSample{
			{Name: "temperature_celsius", Value: 65.0, Unit: "celsius", Timestamp: now},
			{Name: "disk_usage_percent", Value: 38.0, Unit: "percent", Timestamp: now},
			{Name: "memory_usage_percent", Value: 55.0, Unit: "percent", Timestamp: now},
		},
	}

	_, err = det.ExtractFeaturesFromBatch(incompleteBatch)
	if err == nil || !strings.Contains(err.Error(), "missing required telemetry metric") {
		t.Fatalf("expected missing feature error, got: %v", err)
	}

	// Duplicate metric in batch
	dupBatch := &types.TelemetryBatch{
		BatchID:        "batch-ml-dup",
		NodeID:         "node-test",
		SequenceNumber: 3,
		CollectedAt:    now,
		Metrics: []types.MetricSample{
			{Name: "cpu_usage_percent", Value: 45.0, Unit: "percent", Timestamp: now},
			{Name: "cpu_usage_percent", Value: 50.0, Unit: "percent", Timestamp: now},
			{Name: "memory_usage_percent", Value: 55.0, Unit: "percent", Timestamp: now},
			{Name: "disk_usage_percent", Value: 38.0, Unit: "percent", Timestamp: now},
			{Name: "temperature_celsius", Value: 65.0, Unit: "celsius", Timestamp: now},
		},
	}

	_, err = det.ExtractFeaturesFromBatch(dupBatch)
	if err == nil || !strings.Contains(err.Error(), "duplicate metric") {
		t.Fatalf("expected duplicate metric error, got: %v", err)
	}
}

func TestNormalization_DeterministicZScore(t *testing.T) {
	manifest := helperBuildValidManifest(0.60)
	det, err := NewMLDetector(MLDetectorConfig{Manifest: manifest})
	if err != nil {
		t.Fatalf("failed to create detector: %v", err)
	}

	raw := []float64{60.0, 75.0, 45.0, 70.0}
	norm, err := det.NormalizeFeatures(raw)
	if err != nil {
		t.Fatalf("normalization failed: %v", err)
	}

	// cpu: (60 - 50)/10 = 1.0
	// mem: (75 - 60)/15 = 1.0
	// disk: (45 - 40)/5 = 1.0
	// temp: (70 - 50)/10 = 2.0
	expected := []float64{1.0, 1.0, 1.0, 2.0}
	for i := range expected {
		if math.Abs(norm[i]-expected[i]) > 1e-9 {
			t.Fatalf("index %d normalized value mismatch: got %f, expected %f", i, norm[i], expected[i])
		}
	}
}

// -----------------------------------------------------------------------------
// 4. DETECTOR INTERFACE & FULL PIPELINE TESTS
// -----------------------------------------------------------------------------

func TestMLDetector_DetectBatch_NormalVsAnomalous(t *testing.T) {
	// Construct a multi-level tree where deep path is normal (low score) and shallow path is anomalous
	manifest := &MLModelManifest{
		ModelID:              "iforest-deep-001",
		ModelVersion:         "1.0.0",
		Algorithm:            AlgorithmIsolationForest,
		FeatureSchemaVersion: FeatureSchemaV1,
		CreatedAt:            time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC),
		InputDimensions:      4,
		SupportedMetrics: []string{
			"cpu_usage_percent",
			"memory_usage_percent",
			"disk_usage_percent",
			"temperature_celsius",
		},
		NormalizationParams: []FeatureNormalizationParams{
			{MetricName: "cpu_usage_percent", Mean: 50.0, StdDev: 10.0, Min: 0.0, Max: 100.0},
			{MetricName: "memory_usage_percent", Mean: 60.0, StdDev: 10.0, Min: 0.0, Max: 100.0},
			{MetricName: "disk_usage_percent", Mean: 40.0, StdDev: 10.0, Min: 0.0, Max: 100.0},
			{MetricName: "temperature_celsius", Mean: 50.0, StdDev: 10.0, Min: -40.0, Max: 125.0},
		},
		// Single tree with multi-level depth:
		// Root 0: cpu < 2.0 -> left child 1, right child 2 (shallow leaf, path 1)
		// Left child 1: mem < 2.0 -> left child 3, right child 4 (shallow leaf, path 2)
		// Left child 3: disk < 2.0 -> left child 5, right child 6 (shallow leaf, path 3)
		// Left child 5: temp < 2.0 -> left child 7 (deep leaf, path 4, size 50), right child 8 (path 4, size 1)
		Trees: []IsolationTree{
			{
				RootIndex: 0,
				Nodes: []IsolationTreeNode{
					{FeatureIndex: 0, SplitValue: 2.0, LeftChild: 1, RightChild: 2, Size: 0},
					{FeatureIndex: 1, SplitValue: 2.0, LeftChild: 3, RightChild: 4, Size: 0},
					{FeatureIndex: -1, SplitValue: 0.0, LeftChild: -1, RightChild: -1, Size: 1}, // shallow leaf
					{FeatureIndex: 2, SplitValue: 2.0, LeftChild: 5, RightChild: 6, Size: 0},
					{FeatureIndex: -1, SplitValue: 0.0, LeftChild: -1, RightChild: -1, Size: 1},
					{FeatureIndex: 3, SplitValue: 2.0, LeftChild: 7, RightChild: 8, Size: 0},
					{FeatureIndex: -1, SplitValue: 0.0, LeftChild: -1, RightChild: -1, Size: 1},
					{FeatureIndex: -1, SplitValue: 0.0, LeftChild: -1, RightChild: -1, Size: 50}, // deep normal leaf
					{FeatureIndex: -1, SplitValue: 0.0, LeftChild: -1, RightChild: -1, Size: 1},
				},
			},
		},
		SubSampleSize:     256,
		DecisionThreshold: 0.70, // anomaly threshold
		Status:            ModelStatusActive,
	}

	det, err := NewMLDetector(MLDetectorConfig{Manifest: manifest})
	if err != nil {
		t.Fatalf("failed to create detector: %v", err)
	}

	ctx := context.Background()
	now := time.Now().UTC()

	// 1. Normal observation: all metrics nominal (normalized < 2.0)
	// Traverses 0 -> 1 -> 3 -> 5 -> 7 (leaf with size 50)
	// Path length = 4 + c(50) = 4 + ~6.75 ~= 10.75
	// Score s = 2^(-10.75 / 10.245) ~= 0.48 < 0.70 -> Normal (nil signal)
	normalBatch := &types.TelemetryBatch{
		BatchID:        "batch-normal-001",
		NodeID:         "node-edge-01",
		SequenceNumber: 1,
		CollectedAt:    now,
		Metrics: []types.MetricSample{
			{NodeID: "node-edge-01", Name: "cpu_usage_percent", Value: 50.0, Unit: "percent", Timestamp: now},
			{NodeID: "node-edge-01", Name: "memory_usage_percent", Value: 60.0, Unit: "percent", Timestamp: now},
			{NodeID: "node-edge-01", Name: "disk_usage_percent", Value: 40.0, Unit: "percent", Timestamp: now},
			{NodeID: "node-edge-01", Name: "temperature_celsius", Value: 50.0, Unit: "celsius", Timestamp: now},
		},
	}

	signals, err := det.DetectBatch(ctx, normalBatch)
	if err != nil {
		t.Fatalf("DetectBatch normal failed: %v", err)
	}
	if len(signals) != 0 {
		t.Fatalf("expected 0 signals for normal batch, got %d", len(signals))
	}

	// 2. Anomalous observation: extreme CPU (normalized >= 2.0)
	// Traverses 0 -> 2 (shallow leaf with size 1)
	// Path length = 1 + c(1) = 1.0
	// Score s = 2^(-1.0 / 10.245) ~= 0.934 >= 0.70 -> Anomaly!
	anomBatch := &types.TelemetryBatch{
		BatchID:        "batch-anom-001",
		NodeID:         "node-edge-01",
		SequenceNumber: 2,
		CollectedAt:    now.Add(time.Second),
		Metrics: []types.MetricSample{
			{NodeID: "node-edge-01", Name: "cpu_usage_percent", Value: 95.0, Unit: "percent", Timestamp: now.Add(time.Second)},
			{NodeID: "node-edge-01", Name: "memory_usage_percent", Value: 60.0, Unit: "percent", Timestamp: now.Add(time.Second)},
			{NodeID: "node-edge-01", Name: "disk_usage_percent", Value: 40.0, Unit: "percent", Timestamp: now.Add(time.Second)},
			{NodeID: "node-edge-01", Name: "temperature_celsius", Value: 50.0, Unit: "celsius", Timestamp: now.Add(time.Second)},
		},
	}

	signals, err = det.DetectBatch(ctx, anomBatch)
	if err != nil {
		t.Fatalf("DetectBatch anomaly failed: %v", err)
	}
	if len(signals) != 1 {
		t.Fatalf("expected 1 signal for anomalous batch, got %d", len(signals))
	}

	sig := signals[0]
	if sig.DetectionMethod != types.DetectionMethodMLIsolationForest {
		t.Fatalf("unexpected detection method: %s", sig.DetectionMethod)
	}
	if sig.MetricName != MetricIdentifierCompositeMultivariate {
		t.Fatalf("unexpected metric name: %s", sig.MetricName)
	}
	if sig.CorrelationID != "batch-anom-001" {
		t.Fatalf("expected correlation ID 'batch-anom-001', got %s", sig.CorrelationID)
	}
	if sig.AnomalyScore <= 0.0 || sig.AnomalyScore > 1.0 {
		t.Fatalf("anomaly score out of range (0.0, 1.0]: %f", sig.AnomalyScore)
	}
	if sig.Evidence["model_id"] != "iforest-deep-001" {
		t.Fatalf("unexpected evidence model_id: %s", sig.Evidence["model_id"])
	}
	if sig.Evidence["raw_score"] == "" || sig.Evidence["avg_path_length"] == "" {
		t.Fatalf("missing diagnostic evidence fields: %v", sig.Evidence)
	}

	// 3. Map to Incident verification
	inc, err := det.MapToIncident(sig)
	if err != nil {
		t.Fatalf("MapToIncident failed: %v", err)
	}
	if inc.Status != types.StatusAnomalyDetected {
		t.Fatalf("incident status must be ANOMALY_DETECTED, got %s", inc.Status)
	}
	if inc.TriggerMetric != MetricIdentifierCompositeMultivariate {
		t.Fatalf("incident trigger metric mismatch: %s", inc.TriggerMetric)
	}
	if inc.RuleName != "ml_isolation_forest" {
		t.Fatalf("incident rule name mismatch: %s", inc.RuleName)
	}
}

func TestMLDetector_Detect_Buffering(t *testing.T) {
	manifest := helperBuildValidManifest(0.60)
	det, err := NewMLDetector(MLDetectorConfig{Manifest: manifest})
	if err != nil {
		t.Fatalf("failed to create detector: %v", err)
	}

	ctx := context.Background()
	now := time.Now().UTC()

	// Feed 3 of 4 metrics: should return (nil, nil) while buffering
	s1 := types.MetricSample{NodeID: "node-1", Name: "cpu_usage_percent", Value: 30.0, Unit: "percent", Timestamp: now}
	s2 := types.MetricSample{NodeID: "node-1", Name: "memory_usage_percent", Value: 60.0, Unit: "percent", Timestamp: now}
	s3 := types.MetricSample{NodeID: "node-1", Name: "disk_usage_percent", Value: 40.0, Unit: "percent", Timestamp: now}

	sig, err := det.Detect(ctx, s1)
	if err != nil || sig != nil {
		t.Fatalf("expected nil, nil on partial sample, got: sig=%v, err=%v", sig, err)
	}
	sig, err = det.Detect(ctx, s2)
	if err != nil || sig != nil {
		t.Fatalf("expected nil, nil on partial sample, got: sig=%v, err=%v", sig, err)
	}
	sig, err = det.Detect(ctx, s3)
	if err != nil || sig != nil {
		t.Fatalf("expected nil, nil on partial sample, got: sig=%v, err=%v", sig, err)
	}

	// Feed 4th metric: completes the vector -> triggers evaluation (score ~0.93 >= 0.60 -> anomaly)
	s4 := types.MetricSample{NodeID: "node-1", Name: "temperature_celsius", Value: 30.0, Unit: "celsius", Timestamp: now}
	sig, err = det.Detect(ctx, s4)
	if err != nil {
		t.Fatalf("Detect failed on complete vector: %v", err)
	}
	if sig == nil {
		t.Fatalf("expected anomaly signal on complete vector, got nil")
	}
	if sig.DetectionMethod != types.DetectionMethodMLIsolationForest {
		t.Fatalf("unexpected detection method: %s", sig.DetectionMethod)
	}
}

// -----------------------------------------------------------------------------
// 5. DETERMINISM & IMMUTABILITY TESTS
// -----------------------------------------------------------------------------

func TestMLDetector_DeterministicAnomalyID(t *testing.T) {
	manifest := helperBuildValidManifest(0.60)
	det1, _ := NewMLDetector(MLDetectorConfig{Manifest: manifest})
	det2, _ := NewMLDetector(MLDetectorConfig{Manifest: manifest})

	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	batch := &types.TelemetryBatch{
		BatchID:        "batch-det-001",
		NodeID:         "node-det",
		SequenceNumber: 1,
		CollectedAt:    now,
		Metrics: []types.MetricSample{
			{NodeID: "node-det", Name: "cpu_usage_percent", Value: 30.0, Unit: "percent", Timestamp: now},
			{NodeID: "node-det", Name: "memory_usage_percent", Value: 60.0, Unit: "percent", Timestamp: now},
			{NodeID: "node-det", Name: "disk_usage_percent", Value: 40.0, Unit: "percent", Timestamp: now},
			{NodeID: "node-det", Name: "temperature_celsius", Value: 30.0, Unit: "celsius", Timestamp: now},
		},
	}

	sigs1, _ := det1.DetectBatch(context.Background(), batch)
	sigs2, _ := det2.DetectBatch(context.Background(), batch)

	if len(sigs1) != 1 || len(sigs2) != 1 {
		t.Fatalf("expected 1 signal each, got %d and %d", len(sigs1), len(sigs2))
	}

	if sigs1[0].AnomalyID != sigs2[0].AnomalyID {
		t.Fatalf("anomaly IDs must be identical across instances: %s != %s",
			sigs1[0].AnomalyID, sigs2[0].AnomalyID)
	}
	if sigs1[0].AnomalyScore != sigs2[0].AnomalyScore {
		t.Fatalf("anomaly scores must be identical: %f != %f",
			sigs1[0].AnomalyScore, sigs2[0].AnomalyScore)
	}
}

func TestMLDetector_Immutability(t *testing.T) {
	manifest := helperBuildValidManifest(0.60)
	det, _ := NewMLDetector(MLDetectorConfig{Manifest: manifest})

	// Modifying original manifest should not mutate detector's internal state
	manifest.DecisionThreshold = 0.99
	manifest.Trees[0].Nodes[0].SplitValue = 999.0

	model := det.Model()
	if model.DecisionThreshold != 0.60 {
		t.Fatalf("detector internal model was mutated! DecisionThreshold = %f", model.DecisionThreshold)
	}
	if model.Trees[0].Nodes[0].SplitValue != 0.0 {
		t.Fatalf("detector internal tree was mutated! SplitValue = %f", model.Trees[0].Nodes[0].SplitValue)
	}
}

// -----------------------------------------------------------------------------
// 6. FAIL-SAFE & CONCURRENCY TESTS
// -----------------------------------------------------------------------------

func TestMLDetector_FailSafe_MissingFeaturesSkipsEvaluation(t *testing.T) {
	manifest := helperBuildValidManifest(0.60)
	det, _ := NewMLDetector(MLDetectorConfig{Manifest: manifest})

	now := time.Now().UTC()
	// Batch missing temperature_celsius (only 3 metrics)
	incompleteBatch := &types.TelemetryBatch{
		BatchID:        "batch-incomplete",
		NodeID:         "node-01",
		SequenceNumber: 1,
		CollectedAt:    now,
		Metrics: []types.MetricSample{
			{NodeID: "node-01", Name: "cpu_usage_percent", Value: 95.0, Unit: "percent", Timestamp: now},
			{NodeID: "node-01", Name: "memory_usage_percent", Value: 60.0, Unit: "percent", Timestamp: now},
			{NodeID: "node-01", Name: "disk_usage_percent", Value: 40.0, Unit: "percent", Timestamp: now},
		},
	}

	// Must safely skip inference without error and without emitting anomaly
	sigs, err := det.DetectBatch(context.Background(), incompleteBatch)
	if err != nil {
		t.Fatalf("expected nil error on missing features, got %v", err)
	}
	if len(sigs) != 0 {
		t.Fatalf("expected 0 signals on incomplete batch, got %d", len(sigs))
	}
}

func TestMLDetector_FailSafe_InactiveModelSkips(t *testing.T) {
	manifest := helperBuildValidManifest(0.60)
	det, _ := NewMLDetector(MLDetectorConfig{Manifest: manifest})

	// Disable model internally via reflection or by constructing with active then changing
	det.manifest.Status = ModelStatusDisabled

	now := time.Now().UTC()
	anomBatch := &types.TelemetryBatch{
		BatchID:        "batch-anom",
		NodeID:         "node-01",
		SequenceNumber: 1,
		CollectedAt:    now,
		Metrics: []types.MetricSample{
			{NodeID: "node-01", Name: "cpu_usage_percent", Value: 95.0, Unit: "percent", Timestamp: now},
			{NodeID: "node-01", Name: "memory_usage_percent", Value: 60.0, Unit: "percent", Timestamp: now},
			{NodeID: "node-01", Name: "disk_usage_percent", Value: 40.0, Unit: "percent", Timestamp: now},
			{NodeID: "node-01", Name: "temperature_celsius", Value: 95.0, Unit: "celsius", Timestamp: now},
		},
	}

	sigs, err := det.DetectBatch(context.Background(), anomBatch)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(sigs) != 0 {
		t.Fatalf("expected 0 signals when model is disabled, got %d", len(sigs))
	}
}

func TestMLDetector_Concurrency(t *testing.T) {
	manifest := helperBuildValidManifest(0.60)
	det, err := NewMLDetector(MLDetectorConfig{Manifest: manifest})
	if err != nil {
		t.Fatalf("failed to create detector: %v", err)
	}

	now := time.Now().UTC()
	batch := &types.TelemetryBatch{
		BatchID:        "batch-concurrent-001",
		NodeID:         "node-concurrent",
		SequenceNumber: 1,
		CollectedAt:    now,
		Metrics: []types.MetricSample{
			{NodeID: "node-concurrent", Name: "cpu_usage_percent", Value: 30.0, Unit: "percent", Timestamp: now},
			{NodeID: "node-concurrent", Name: "memory_usage_percent", Value: 60.0, Unit: "percent", Timestamp: now},
			{NodeID: "node-concurrent", Name: "disk_usage_percent", Value: 40.0, Unit: "percent", Timestamp: now},
			{NodeID: "node-concurrent", Name: "temperature_celsius", Value: 30.0, Unit: "celsius", Timestamp: now},
		},
	}

	const goroutines = 20
	const iterations = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				sigs, err := det.DetectBatch(context.Background(), batch)
				if err != nil {
					t.Errorf("concurrent DetectBatch failed: %v", err)
					return
				}
				if len(sigs) != 1 {
					t.Errorf("expected 1 signal, got %d", len(sigs))
					return
				}
			}
		}()
	}

	wg.Wait()
}

// -----------------------------------------------------------------------------
// 7. EMPIRICAL BENCHMARKS (Baseline Observation Only)
// -----------------------------------------------------------------------------

func BenchmarkMLDetector_SingleInference(b *testing.B) {
	manifest := helperBuildValidManifest(0.60)
	det, err := NewMLDetector(MLDetectorConfig{Manifest: manifest})
	if err != nil {
		b.Fatalf("failed to create detector: %v", err)
	}

	rawVector := []float64{45.0, 55.0, 38.0, 52.0}
	ctx := context.Background()
	now := time.Now().UTC()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = det.DetectVector(ctx, "bench-node", now, "sample-001", rawVector)
	}
}

func BenchmarkMLDetector_BatchInference(b *testing.B) {
	manifest := helperBuildValidManifest(0.60)
	det, err := NewMLDetector(MLDetectorConfig{Manifest: manifest})
	if err != nil {
		b.Fatalf("failed to create detector: %v", err)
	}

	now := time.Now().UTC()
	batch := &types.TelemetryBatch{
		BatchID:        "batch-bench-001",
		NodeID:         "bench-node",
		SequenceNumber: 1,
		CollectedAt:    now,
		Metrics: []types.MetricSample{
			{NodeID: "bench-node", Name: "cpu_usage_percent", Value: 45.0, Unit: "percent", Timestamp: now},
			{NodeID: "bench-node", Name: "memory_usage_percent", Value: 55.0, Unit: "percent", Timestamp: now},
			{NodeID: "bench-node", Name: "disk_usage_percent", Value: 38.0, Unit: "percent", Timestamp: now},
			{NodeID: "bench-node", Name: "temperature_celsius", Value: 52.0, Unit: "celsius", Timestamp: now},
		},
	}

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = det.DetectBatch(ctx, batch)
	}
}

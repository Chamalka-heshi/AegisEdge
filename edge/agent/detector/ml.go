package detector

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// MetricIdentifierCompositeMultivariate is the canonical metric identifier emitted by MLDetector
// representing a multivariate observation across all supported telemetry dimensions.
const MetricIdentifierCompositeMultivariate = "composite_multivariate"

// MaxBufferedSamples bounds the number of pending multi-metric sample sets in the alignment buffer.
// Rationale: Prevents memory growth if incomplete sample batches are received.
const MaxBufferedSamples = 1000

// MLDetectorConfig configures the pure-Go Isolation Forest anomaly detector.
type MLDetectorConfig struct {
	Manifest    *MLModelManifest
	IDGenerator IDGeneratorFunc
}

// bufferKey uniquely identifies a pending telemetry vector assembly window.
type bufferKey struct {
	NodeID       string
	TimestampKey string // formatted timestamp (RFC3339Nano)
}

// MLDetector implements a safe, deterministic, pure-Go Isolation Forest anomaly detector
// satisfying the canonical Detector interface.
//
// Architectural Invariants (ADR-0009):
//   - Zero External ML Runtimes: Pure Go binary decision tree traversal; zero Cgo, ONNX, or Python dependencies.
//   - Network Independence: Evaluates telemetry locally in-memory; zero network calls or external queries.
//   - Deterministic Inference: Given the same immutable model artifact, feature schema, normalization parameters,
//     and feature vector, inference produces identical path lengths, anomaly scores, and AnomalyIDs across all architectures.
//   - Thread-Safe & Immutable: Once initialized, the model manifest is read-only.
//   - Standard Isolation Forest Equations: Computes c(psi), E(h(x)), and anomaly score s(x, psi) = 2^(-E(h)/c(psi)).
//   - Normalized AnomalyScore: Linearly normalized intensity in [0.0, 1.0] above decision threshold.
//     It is NOT a probability, likelihood, confidence level, or p-value.
//   - Fail-Safe Operation: Missing models, invalid schemas, or missing metrics skip inference safely without emitting false alarms.
//   - Non-Destructive Observer: Never invokes remediation or mutates host system state.
type MLDetector struct {
	manifest    *MLModelManifest
	cPsi        float64 // precomputed c(manifest.SubSampleSize)
	idGenerator IDGeneratorFunc

	// Alignment buffer for single-metric Detect calls
	bufferMu sync.Mutex
	buffer   map[bufferKey]map[string]float64
}

// NewMLDetector initializes and validates a new MLDetector instance.
func NewMLDetector(cfg MLDetectorConfig) (*MLDetector, error) {
	if cfg.Manifest == nil {
		return nil, ErrNilModelManifest
	}

	if err := cfg.Manifest.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidRuleConfig, err)
	}

	// Deep clone manifest to guarantee caller-side immutability
	immutableManifest := cfg.Manifest.Clone()

	idGen := cfg.IDGenerator
	if idGen == nil {
		idGen = DefaultDeterministicAnomalyID
	}

	cPsi := ExpectedPathLength(float64(immutableManifest.SubSampleSize))

	return &MLDetector{
		manifest:    immutableManifest,
		cPsi:        cPsi,
		idGenerator: idGen,
		buffer:      make(map[bufferKey]map[string]float64),
	}, nil
}

// Name returns the identifier of this detector implementation.
func (d *MLDetector) Name() string {
	return "ml_isolation_forest"
}

// Version returns the version string of the loaded model artifact.
func (d *MLDetector) Version() string {
	return d.manifest.ModelVersion
}

// Model returns a deep copy of the loaded model manifest.
func (d *MLDetector) Model() *MLModelManifest {
	return d.manifest.Clone()
}

// SetIDGenerator allows overriding the deterministic anomaly ID generator (e.g. for testing).
func (d *MLDetector) SetIDGenerator(fn IDGeneratorFunc) {
	d.bufferMu.Lock()
	defer d.bufferMu.Unlock()
	d.idGenerator = fn
}

// ResetBuffer clears all in-memory alignment buffer entries.
func (d *MLDetector) ResetBuffer() {
	d.bufferMu.Lock()
	defer d.bufferMu.Unlock()
	d.buffer = make(map[bufferKey]map[string]float64)
}

// ExtractFeaturesFromBatch extracts and validates canonical telemetry metrics from a TelemetryBatch
// into a deterministic float64 slice ordered by the model's SupportedMetrics schema.
func (d *MLDetector) ExtractFeaturesFromBatch(batch *types.TelemetryBatch) ([]float64, error) {
	if batch == nil {
		return nil, ErrNilBatch
	}

	// Map present metrics
	metricMap := make(map[string]float64, len(batch.Metrics))
	for _, m := range batch.Metrics {
		if err := m.Validate(); err != nil {
			return nil, fmt.Errorf("invalid metric %q: %w", m.Name, err)
		}
		if _, exists := metricMap[m.Name]; exists {
			return nil, fmt.Errorf("%w: duplicate metric %q in batch", ErrDuplicateMetricName, m.Name)
		}
		metricMap[m.Name] = m.Value
	}

	// Assemble feature vector in exact schema order
	features := make([]float64, d.manifest.InputDimensions)
	for i, name := range d.manifest.SupportedMetrics {
		val, exists := metricMap[name]
		if !exists {
			return nil, fmt.Errorf("%w: %q", ErrMissingRequiredFeature, name)
		}
		if math.IsNaN(val) || math.IsInf(val, 0) {
			return nil, fmt.Errorf("%w: %q has value %f", ErrInvalidFeatureValue, name, val)
		}
		features[i] = val
	}

	return features, nil
}

// NormalizeFeatures applies the model's immutable normalization parameters to a raw feature vector:
//
//	z_i = (x_i - mean_i) / std_dev_i
//
// Normalization parameters are strictly frozen in the model artifact and never calculated from current telemetry.
func (d *MLDetector) NormalizeFeatures(raw []float64) ([]float64, error) {
	if len(raw) != d.manifest.InputDimensions {
		return nil, fmt.Errorf("%w: expected %d, got %d", ErrDimensionMismatch, d.manifest.InputDimensions, len(raw))
	}

	norm := make([]float64, d.manifest.InputDimensions)
	for i, v := range raw {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, fmt.Errorf("%w: index %d has non-finite value", ErrInvalidFeatureValue, i)
		}
		p := d.manifest.NormalizationParams[i]
		norm[i] = (v - p.Mean) / p.StdDev
	}

	return norm, nil
}

// TraverseTree evaluates a single isolation tree for a normalized feature vector.
// Returns the accumulated path length (including leaf correction) and the root split feature index.
func (d *MLDetector) TraverseTree(tree *IsolationTree, normVector []float64) (float64, int, error) {
	currIdx := tree.RootIndex
	nodes := tree.Nodes
	depth := 0

	rootSplitFeature := -1
	if !nodes[currIdx].IsLeaf() {
		rootSplitFeature = nodes[currIdx].FeatureIndex
	}

	for depth <= MaxTreeDepth {
		node := nodes[currIdx]
		if node.IsLeaf() {
			// Apply standard leaf path-length correction c(n) for leaves with n > 1 samples
			correction := ExpectedPathLength(float64(node.Size))
			return float64(depth) + correction, rootSplitFeature, nil
		}

		if node.FeatureIndex < 0 || node.FeatureIndex >= len(normVector) {
			return 0, -1, fmt.Errorf("%w: index %d out of range", ErrInvalidFeatureIndex, node.FeatureIndex)
		}

		featureVal := normVector[node.FeatureIndex]
		if featureVal < node.SplitValue {
			currIdx = node.LeftChild
		} else {
			currIdx = node.RightChild
		}
		depth++
	}

	return 0, -1, fmt.Errorf("%w: traversal exceeded depth %d", ErrExcessiveTreeDepth, MaxTreeDepth)
}

// Score evaluates all trees in the forest and computes the average path length and Isolation Forest anomaly score:
//
//	s(x, psi) = 2^(-E(h(x)) / c(psi))
func (d *MLDetector) Score(normVector []float64) (avgPathLength float64, score float64, diagnosticFeature string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("runtime catch during isolation forest evaluation: %v", r)
		}
	}()

	totalPathLength := 0.0
	treeCount := len(d.manifest.Trees)
	splitCounts := make(map[int]int)

	for _, tree := range d.manifest.Trees {
		pathLen, rootFeature, tErr := d.TraverseTree(&tree, normVector)
		if tErr != nil {
			return 0, 0, "", tErr
		}
		totalPathLength += pathLen
		if rootFeature >= 0 {
			splitCounts[rootFeature]++
		}
	}

	avgPathLength = totalPathLength / float64(treeCount)

	// Determine dominant diagnostic root feature (most frequent root split decision)
	maxSplits := -1
	dominantFeatureIdx := -1
	for featIdx, count := range splitCounts {
		if count > maxSplits {
			maxSplits = count
			dominantFeatureIdx = featIdx
		}
	}
	if dominantFeatureIdx >= 0 && dominantFeatureIdx < len(d.manifest.SupportedMetrics) {
		diagnosticFeature = d.manifest.SupportedMetrics[dominantFeatureIdx]
	}

	// Compute standard Isolation Forest score: s = 2^(-E(h) / c(psi))
	if d.cPsi > 0 {
		score = math.Pow(2.0, -avgPathLength/d.cPsi)
	} else {
		score = 0.5
	}

	return avgPathLength, score, diagnosticFeature, nil
}

// DetectVector evaluates a validated raw feature vector against the Isolation Forest model.
// Returns an AnomalySignal if raw score >= DecisionThreshold; returns (nil, nil) if normal.
func (d *MLDetector) DetectVector(ctx context.Context, nodeID string, timestamp time.Time, sampleID string, rawFeatures []float64) (*types.AnomalySignal, error) {
	if d.manifest.Status != ModelStatusActive {
		return nil, nil // Safe degradation: inactive model skips inference
	}

	if nodeID == "" {
		nodeID = "local-edge"
	}

	normVector, err := d.NormalizeFeatures(rawFeatures)
	if err != nil {
		return nil, err
	}

	avgPathLength, rawScore, diagFeature, err := d.Score(normVector)
	if err != nil {
		return nil, fmt.Errorf("isolation forest inference failed: %w", err)
	}

	tau := d.manifest.DecisionThreshold
	if rawScore < tau {
		// Normal observation: score below anomaly threshold
		return nil, nil
	}

	// Compute normalized AnomalyScore in [0.0, 1.0] (intensity above threshold)
	intensity := math.Min(1.0, math.Max(0.0, (rawScore-tau)/(1.0-tau)))

	// Deterministic AnomalyID generation derived from domain identity and breach context
	anomalyID, err := d.idGenerator(nodeID, MetricIdentifierCompositeMultivariate, d.manifest.ModelVersion, timestamp, sampleID, rawScore)
	if err != nil {
		return nil, fmt.Errorf("failed to generate deterministic anomaly ID: %w", err)
	}

	// Format safe diagnostic feature representation
	var featStrs []string
	for i, m := range d.manifest.SupportedMetrics {
		featStrs = append(featStrs, fmt.Sprintf("%s=%.2f", m, rawFeatures[i]))
	}

	evidence := map[string]string{
		"model_id":                d.manifest.ModelID,
		"model_version":           d.manifest.ModelVersion,
		"feature_schema_version":  d.manifest.FeatureSchemaVersion,
		"raw_score":               fmt.Sprintf("%.6f", rawScore),
		"decision_threshold":      fmt.Sprintf("%.4f", tau),
		"avg_path_length":         fmt.Sprintf("%.6f", avgPathLength),
		"tree_count":              strconv.Itoa(len(d.manifest.Trees)),
		"sub_sample_size":         strconv.Itoa(d.manifest.SubSampleSize),
		"features":                strings.Join(featStrs, ","),
		"diagnostic_path_feature": diagFeature,
		"rule_name":               "ml_isolation_forest",
		"detector_version":        d.manifest.ModelVersion,
		"description": fmt.Sprintf("Isolation Forest anomaly detected: raw score %.4f >= threshold %.4f (intensity %.4f, avg path %.4f)",
			rawScore, tau, intensity, avgPathLength),
	}

	sig := &types.AnomalySignal{
		AnomalyID:       anomalyID,
		NodeID:          nodeID,
		MetricName:      MetricIdentifierCompositeMultivariate,
		ObservedValue:   rawScore,
		ExpectedValue:   tau,
		Deviation:       rawScore - tau,
		AnomalyScore:    intensity,
		DetectionMethod: types.DetectionMethodMLIsolationForest,
		DetectedAt:      timestamp.UTC(),
		Evidence:        evidence,
		DetectorVersion: d.manifest.ModelVersion,
	}

	if err := sig.Validate(); err != nil {
		return nil, fmt.Errorf("generated ML anomaly signal failed validation: %w", err)
	}

	return sig, nil
}

// Detect evaluates a single MetricSample against the Isolation Forest model.
// Because Isolation Forest requires a full multivariate feature vector, Detect buffers incoming samples
// partitioned by (NodeID, Timestamp). When all canonical metrics for the window arrive, inference is evaluated.
// If any metric is absent or the sample fails validation, inference is safely deferred or skipped.
func (d *MLDetector) Detect(ctx context.Context, sample types.MetricSample) (*types.AnomalySignal, error) {
	if err := sample.Validate(); err != nil {
		return nil, err
	}

	if d.manifest.Status != ModelStatusActive {
		return nil, nil // Safe degradation: inactive model skips inference
	}

	// Verify sample belongs to supported schema
	var isSupported bool
	for _, supported := range d.manifest.SupportedMetrics {
		if sample.Name == supported {
			isSupported = true
			break
		}
	}
	if !isSupported {
		return nil, nil // Safe skip: metric unmonitored by this model
	}

	nodeID := sample.NodeID
	if nodeID == "" {
		nodeID = "local-edge"
	}

	timeKey := sample.Timestamp.UTC().Format(time.RFC3339Nano)
	key := bufferKey{
		NodeID:       nodeID,
		TimestampKey: timeKey,
	}

	d.bufferMu.Lock()
	defer d.bufferMu.Unlock()

	// Evict oldest buffer entries if threshold exceeded
	if len(d.buffer) > MaxBufferedSamples {
		for k := range d.buffer {
			delete(d.buffer, k)
			break
		}
	}

	entry, exists := d.buffer[key]
	if !exists {
		entry = make(map[string]float64, d.manifest.InputDimensions)
		d.buffer[key] = entry
	}
	entry[sample.Name] = sample.Value

	// Check if full feature vector has arrived
	if len(entry) < d.manifest.InputDimensions {
		return nil, nil // Vector incomplete; wait for remaining telemetry samples
	}

	// Extract features in canonical schema order
	features := make([]float64, d.manifest.InputDimensions)
	for i, name := range d.manifest.SupportedMetrics {
		features[i] = entry[name]
	}

	// Full vector assembled: clean buffer entry
	delete(d.buffer, key)

	return d.DetectVector(ctx, nodeID, sample.Timestamp, sample.SampleID, features)
}

// DetectBatch evaluates all metrics in a TelemetryBatch against the Isolation Forest model.
// Extracts the full multivariate vector from the batch and performs inference.
// In accordance with ADR-0009 Matrix ID H: if any required feature is missing from the batch,
// inference is safely skipped without returning an error or generating false anomalies.
func (d *MLDetector) DetectBatch(ctx context.Context, batch *types.TelemetryBatch) ([]*types.AnomalySignal, error) {
	if batch == nil {
		return nil, ErrNilBatch
	}
	if err := batch.Validate(); err != nil {
		return nil, err
	}

	if d.manifest.Status != ModelStatusActive {
		return []*types.AnomalySignal{}, nil
	}

	features, err := d.ExtractFeaturesFromBatch(batch)
	if err != nil {
		if strings.Contains(err.Error(), ErrMissingRequiredFeature.Error()) {
			// ADR-0009 Scenario H: Missing vector metrics -> safely skip evaluation
			return []*types.AnomalySignal{}, nil
		}
		return nil, err
	}

	sig, err := d.DetectVector(ctx, batch.NodeID, batch.CollectedAt, batch.BatchID, features)
	if err != nil {
		return nil, err
	}

	if sig != nil {
		if sig.CorrelationID == "" && batch.BatchID != "" {
			sig.CorrelationID = batch.BatchID
		}
		return []*types.AnomalySignal{sig}, nil
	}

	return []*types.AnomalySignal{}, nil
}

// MapToIncident converts an AnomalySignal produced by this detector to a formal Incident
// using the shared canonical mapping boundary.
func (d *MLDetector) MapToIncident(sig *types.AnomalySignal) (*types.Incident, error) {
	return MapAnomalyToIncident(sig)
}

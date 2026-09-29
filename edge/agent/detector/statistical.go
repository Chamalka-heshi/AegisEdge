package detector

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// Statistical detector validation errors.
var (
	ErrInvalidWindowSize      = errors.New("window_size must be a positive integer")
	ErrInvalidMinObservations = errors.New("min_observations must be at least 2 and not exceed window_size")
	ErrInvalidZScoreThreshold = errors.New("z_score_threshold must be a finite positive number")
	ErrInvalidMaxZScore       = errors.New("max_z_score must be a finite number greater than or equal to z_score_threshold")
)

// StatisticalRule defines the rolling statistical evaluation parameters for a specific metric.
type StatisticalRule struct {
	// MetricName is the exact telemetry metric name to evaluate (e.g. "cpu_usage_percent").
	MetricName string

	// WindowSize is the maximum number of historical observations retained in the rolling window.
	// Must be > 0.
	WindowSize int

	// MinObservations is the minimum number of historical observations required before statistical detection begins (warmup period).
	// Must be >= 2 and <= WindowSize.
	MinObservations int

	// ZScoreThreshold is the number of standard deviations from the historical mean required to trigger an anomaly.
	// Must be a finite positive number (> 0.0). Note: this is an initial engineering threshold, not an empirically validated universal constant.
	ZScoreThreshold float64

	// MaxZScore is the scaling factor for AnomalyScore normalization into [0.0, 1.0].
	// If nil, defaults to 2.0 * ZScoreThreshold.
	// Must be >= ZScoreThreshold if provided.
	MaxZScore *float64

	// MinStdDevEpsilon defines the threshold below which standard deviation is treated as effectively zero.
	// If <= 0, defaults to 1e-9.
	MinStdDevEpsilon float64

	// MinMeaningfulDifference defines the minimum absolute difference (|current - mean|) required to trigger
	// an anomaly when standard deviation is zero or <= MinStdDevEpsilon.
	// If <= 0, defaults to 1e-6.
	MinMeaningfulDifference float64
}

// Validate verifies the internal consistency and numerical validity of a StatisticalRule.
func (r *StatisticalRule) Validate() error {
	if strings.TrimSpace(r.MetricName) == "" {
		return ErrEmptyRuleMetricName
	}
	if r.WindowSize <= 0 {
		return ErrInvalidWindowSize
	}
	if r.MinObservations < 2 || r.MinObservations > r.WindowSize {
		return ErrInvalidMinObservations
	}
	if math.IsNaN(r.ZScoreThreshold) || math.IsInf(r.ZScoreThreshold, 0) || r.ZScoreThreshold <= 0 {
		return ErrInvalidZScoreThreshold
	}
	if r.MaxZScore != nil {
		maxZ := *r.MaxZScore
		if math.IsNaN(maxZ) || math.IsInf(maxZ, 0) || maxZ < r.ZScoreThreshold {
			return ErrInvalidMaxZScore
		}
	}
	return nil
}

// StatisticalDetectorConfig configures the rolling statistical anomaly detector.
type StatisticalDetectorConfig struct {
	Version string
	Rules   []StatisticalRule
}

// streamKey uniquely partitions rolling statistical state by device and metric without delimiter collision risks.
type streamKey struct {
	NodeID     string
	MetricName string
}

// StatisticalDetector implements a deterministic, local-first rolling statistical anomaly detector.
//
// Key Architectural Principles (ADR-0009):
//   - Local & Offline-First: Operates entirely in-memory on the edge node with zero network dependencies.
//   - Rolling Window: Maintains a bounded history of observations per (NodeID, MetricName) stream.
//   - Uncontaminated Baseline: The current observation is evaluated against prior history before being appended.
//   - Warm-Up Period: Enforces MinObservations before emitting statistical signals to avoid false alarms during cold start.
//   - Zero/Near-Zero StdDev: Deterministically handles zero-variance baselines without producing NaN, +Inf, or -Inf.
//   - Deterministic Identity: Employs DefaultDeterministicAnomalyID using SHA-256 for stable anomaly provenance.
//   - Normalized AnomalyScore: Derived deterministically from z_score / maxZScore into [0.0, 1.0].
//     It is NOT a probability, confidence level, or p-value.
//   - Thread-Safe: Guarded by a sync.RWMutex for concurrent evaluation.
//   - Non-Destructive Observer: Performs NO host mutations, command execution, or remediation actions.
type StatisticalDetector struct {
	mu          sync.RWMutex
	name        string
	version     string
	rules       map[string]StatisticalRule
	states      map[streamKey][]float64
	idGenerator IDGeneratorFunc
}

// NewStatisticalDetector initializes and validates a new StatisticalDetector instance.
func NewStatisticalDetector(cfg StatisticalDetectorConfig) (*StatisticalDetector, error) {
	ver := strings.TrimSpace(cfg.Version)
	if ver == "" {
		ver = "1.0.0"
	}

	if len(cfg.Rules) == 0 {
		return nil, fmt.Errorf("%w: at least one rule required", ErrInvalidRuleConfig)
	}

	ruleMap := make(map[string]StatisticalRule, len(cfg.Rules))
	for _, rule := range cfg.Rules {
		if err := rule.Validate(); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidRuleConfig, err)
		}
		if _, exists := ruleMap[rule.MetricName]; exists {
			return nil, fmt.Errorf("%w: metric %q", ErrDuplicateMetricRule, rule.MetricName)
		}
		ruleMap[rule.MetricName] = rule
	}

	return &StatisticalDetector{
		name:        "rolling_statistical_detector",
		version:     ver,
		rules:       ruleMap,
		states:      make(map[streamKey][]float64),
		idGenerator: DefaultDeterministicAnomalyID,
	}, nil
}

// Name returns the identifier of this detector implementation.
func (d *StatisticalDetector) Name() string {
	return d.name
}

// Version returns the version string of the detector algorithm or rule set.
func (d *StatisticalDetector) Version() string {
	return d.version
}

// SetIDGenerator allows overriding the deterministic ID generator (e.g. for testing).
func (d *StatisticalDetector) SetIDGenerator(fn IDGeneratorFunc) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.idGenerator = fn
}

// ResetState clears all in-memory rolling history across all streams.
func (d *StatisticalDetector) ResetState() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.states = make(map[streamKey][]float64)
}

// GetHistory returns a copy of the current rolling history for a given node and metric (useful for test assertions).
func (d *StatisticalDetector) GetHistory(nodeID, metricName string) []float64 {
	if nodeID == "" {
		nodeID = "local-edge"
	}
	key := streamKey{
		NodeID:     nodeID,
		MetricName: metricName,
	}

	d.mu.RLock()
	defer d.mu.RUnlock()

	hist, exists := d.states[key]
	if !exists {
		return nil
	}
	cp := make([]float64, len(hist))
	copy(cp, hist)
	return cp
}

// Detect evaluates a single MetricSample against configured rolling statistical rules.
//
// Pipeline flow:
//  1. Validate sample (reject NaN, +Inf, -Inf, empty names).
//  2. Lookup rule; if unconfigured, return (nil, nil) safely.
//  3. Isolate state by NodeID + MetricName.
//  4. If history len < MinObservations (warm-up), append sample and return (nil, nil).
//  5. Compute baseline (mean, stddev) using ONLY prior history (no contamination by current value).
//  6. Handle zero/near-zero stddev deterministically without producing NaN/Inf.
//  7. Evaluate z-score against ZScoreThreshold.
//  8. Append current observation to history, evicting oldest if len > WindowSize.
//  9. If anomalous, emit validated AnomalySignal; otherwise return (nil, nil).
func (d *StatisticalDetector) Detect(ctx context.Context, sample types.MetricSample) (*types.AnomalySignal, error) {
	if err := sample.Validate(); err != nil {
		return nil, err
	}

	d.mu.RLock()
	rule, exists := d.rules[sample.Name]
	d.mu.RUnlock()

	// Missing metric configuration: not an anomaly, safely ignored (ADR-0009 §13)
	if !exists {
		return nil, nil
	}

	nodeID := sample.NodeID
	if nodeID == "" {
		nodeID = "local-edge"
	}
	key := streamKey{
		NodeID:     nodeID,
		MetricName: sample.Name,
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	history := d.states[key]
	n := len(history)

	// Warm-up check: must have at least MinObservations historical samples
	if n < rule.MinObservations {
		newHist := append(history, sample.Value)
		if len(newHist) > rule.WindowSize {
			newHist = newHist[len(newHist)-rule.WindowSize:]
		}
		d.states[key] = newHist
		return nil, nil
	}

	// 1. Calculate baseline on prior history only (uncontaminated)
	var sum float64
	for _, v := range history {
		sum += v
	}
	mean := sum / float64(n)

	var varSum float64
	for _, v := range history {
		diff := v - mean
		varSum += diff * diff
	}
	stdDev := math.Sqrt(varSum / float64(n))

	// 2. Evaluate current observation against baseline
	eps := rule.MinStdDevEpsilon
	if eps <= 0 {
		eps = 1e-9
	}
	minDiff := rule.MinMeaningfulDifference
	if minDiff <= 0 {
		minDiff = 1e-6
	}

	deviation := sample.Value - mean
	absDiff := math.Abs(deviation)

	var isAnomaly bool
	var zScore float64
	var isZeroStdDev bool

	if stdDev <= eps {
		isZeroStdDev = true
		if absDiff > minDiff {
			// Step-change anomaly relative to invariant baseline:
			// Deterministically assign zScore scaled to relative difference from mean:
			ref := math.Max(math.Abs(mean), 1.0)
			zScore = math.Max(rule.ZScoreThreshold, (absDiff/ref)*rule.ZScoreThreshold)
			isAnomaly = true
		} else {
			// Approximately equal to mean within zero-variance tolerance: no anomaly
			zScore = 0.0
			isAnomaly = false
		}
	} else {
		zScore = absDiff / stdDev
		if zScore >= rule.ZScoreThreshold {
			isAnomaly = true
		}
	}

	// 3. Append current observation to history (bounded rolling window)
	newHist := append(history, sample.Value)
	if len(newHist) > rule.WindowSize {
		newHist = newHist[len(newHist)-rule.WindowSize:]
	}
	d.states[key] = newHist

	if !isAnomaly {
		return nil, nil
	}

	// 4. Compute normalized AnomalyScore in [0.0, 1.0]
	maxZ := rule.ZScoreThreshold * 2.0
	if rule.MaxZScore != nil {
		maxZ = *rule.MaxZScore
	}
	score := math.Min(1.0, math.Max(0.0, zScore/maxZ))

	// 5. Generate deterministic AnomalyID
	anomalyID, err := d.idGenerator(nodeID, sample.Name, d.version, sample.Timestamp, sample.SampleID, sample.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to generate anomaly ID: %w", err)
	}

	direction := "upper"
	if sample.Value < mean {
		direction = "lower"
	}

	evidence := map[string]string{
		"direction":          direction,
		"mean":               fmt.Sprintf("%.6f", mean),
		"standard_deviation": fmt.Sprintf("%.6f", stdDev),
		"z_score":            fmt.Sprintf("%.6f", zScore),
		"z_threshold":        fmt.Sprintf("%.4f", rule.ZScoreThreshold),
		"window_size":        strconv.Itoa(rule.WindowSize),
		"observation_count":  strconv.Itoa(n),
		"rule_name":          fmt.Sprintf("statistical_%s", sample.Name),
		"detector_version":   d.version,
		"description": fmt.Sprintf("Statistical anomaly detected on %s: observed %.4f (mean %.4f, stddev %.4f, z %.4f)",
			sample.Name, sample.Value, mean, stdDev, zScore),
	}
	if isZeroStdDev {
		evidence["zero_stddev"] = "true"
	}

	sig := &types.AnomalySignal{
		AnomalyID:       anomalyID,
		NodeID:          nodeID,
		MetricName:      sample.Name,
		ObservedValue:   sample.Value,
		ExpectedValue:   mean,
		Deviation:       deviation,
		AnomalyScore:    score,
		DetectionMethod: types.DetectionMethodZScore,
		DetectedAt:      sample.Timestamp.UTC(),
		Evidence:        evidence,
		DetectorVersion: d.version,
	}

	if err := sig.Validate(); err != nil {
		return nil, fmt.Errorf("generated statistical anomaly signal failed validation: %w", err)
	}

	return sig, nil
}

// DetectBatch evaluates all metrics in a TelemetryBatch against configured statistical rules.
// Returns a slice of detected AnomalySignals (empty slice if none detected).
func (d *StatisticalDetector) DetectBatch(ctx context.Context, batch *types.TelemetryBatch) ([]*types.AnomalySignal, error) {
	if batch == nil {
		return nil, ErrNilBatch
	}
	if err := batch.Validate(); err != nil {
		return nil, err
	}

	signals := make([]*types.AnomalySignal, 0)
	for _, sample := range batch.Metrics {
		sig, err := d.Detect(ctx, sample)
		if err != nil {
			return nil, err
		}
		if sig != nil {
			if sig.CorrelationID == "" && batch.BatchID != "" {
				sig.CorrelationID = batch.BatchID
			}
			signals = append(signals, sig)
		}
	}

	return signals, nil
}

// MapToIncident converts an AnomalySignal produced by this detector to a formal Incident
// using the shared canonical mapping boundary.
func (d *StatisticalDetector) MapToIncident(sig *types.AnomalySignal) (*types.Incident, error) {
	return MapAnomalyToIncident(sig)
}

// DefaultStatisticalRules returns standard recommended baseline rolling statistical rules for generated edge telemetry.
func DefaultStatisticalRules() []StatisticalRule {
	return []StatisticalRule{
		{
			MetricName:      "cpu_usage_percent",
			WindowSize:      20,
			MinObservations: 10,
			ZScoreThreshold: 3.0,
		},
		{
			MetricName:      "memory_usage_percent",
			WindowSize:      20,
			MinObservations: 10,
			ZScoreThreshold: 3.0,
		},
		{
			MetricName:      "disk_usage_percent",
			WindowSize:      20,
			MinObservations: 10,
			ZScoreThreshold: 3.0,
		},
		{
			MetricName:      "temperature_celsius",
			WindowSize:      20,
			MinObservations: 10,
			ZScoreThreshold: 3.0,
		},
	}
}

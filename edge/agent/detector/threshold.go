package detector

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// Specific threshold rule validation errors.
var (
	ErrEmptyRuleMetricName       = errors.New("rule metric_name cannot be empty")
	ErrNoThresholdDefined        = errors.New("rule must define at least one threshold (UpperThreshold or LowerThreshold)")
	ErrInvalidUpperThreshold     = errors.New("upper_threshold must be a valid finite number")
	ErrInvalidLowerThreshold     = errors.New("lower_threshold must be a valid finite number")
	ErrInvalidRecoveryThreshold  = errors.New("recovery_threshold must be a valid finite number")
	ErrInvalidExpectedValueRule  = errors.New("expected_value must be a valid finite number")
	ErrInvalidMaxDeviation       = errors.New("max_deviation must be a valid finite positive number")
	ErrUpperLessThanLower        = errors.New("upper_threshold cannot be less than lower_threshold")
	ErrUpperRecoveryTooHigh      = errors.New("upper_recovery_threshold cannot be greater than upper_threshold")
	ErrLowerRecoveryTooLow       = errors.New("lower_recovery_threshold cannot be less than lower_threshold")
	ErrNegativeThresholdDisallow = errors.New("threshold cannot be negative when DisallowNegative is true")
)

// ThresholdRule specifies static boundary thresholds and hysteresis parameters for a single metric.
type ThresholdRule struct {
	// MetricName is the exact telemetry metric name to evaluate (e.g., "cpu_usage_percent").
	MetricName string

	// UpperThreshold is the upper breach ceiling (e.g. 90.0).
	// If non-nil, readings > UpperThreshold trigger an upper anomaly breach.
	UpperThreshold *float64

	// UpperRecoveryThreshold is the hysteresis recovery threshold for upper breaches.
	// If non-nil, once breached, readings must fall <= UpperRecoveryThreshold to clear the anomaly.
	// Must be <= UpperThreshold. If nil, defaults to UpperThreshold (zero hysteresis).
	UpperRecoveryThreshold *float64

	// LowerThreshold is the lower breach floor (e.g. 10.0).
	// If non-nil, readings < LowerThreshold trigger a lower anomaly breach.
	LowerThreshold *float64

	// LowerRecoveryThreshold is the hysteresis recovery threshold for lower breaches.
	// If non-nil, once breached, readings must rise >= LowerRecoveryThreshold to clear the anomaly.
	// Must be >= LowerThreshold. If nil, defaults to LowerThreshold (zero hysteresis).
	LowerRecoveryThreshold *float64

	// ExpectedValue is the baseline nominal value for deviation calculations.
	// If nil, defaults to UpperThreshold (for upper breaches) or LowerThreshold (for lower breaches).
	ExpectedValue *float64

	// MaxDeviation is the scaling factor for AnomalyScore normalization into [0.0, 1.0].
	// If nil or <= 0, defaults to |ExpectedValue| (or 100.0 if ExpectedValue is 0).
	MaxDeviation *float64

	// DisallowNegative indicates whether negative metric values are prohibited for this specific metric.
	// By default (false), telemetry metrics are signed and legitimate negative values are permitted.
	// If set to true, negative metric values or negative thresholds are rejected as invalid.
	DisallowNegative bool
}

// Validate verifies the internal consistency and numerical validity of a ThresholdRule.
func (r *ThresholdRule) Validate() error {
	if strings.TrimSpace(r.MetricName) == "" {
		return ErrEmptyRuleMetricName
	}
	if r.UpperThreshold == nil && r.LowerThreshold == nil {
		return ErrNoThresholdDefined
	}

	if r.UpperThreshold != nil {
		v := *r.UpperThreshold
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return ErrInvalidUpperThreshold
		}
		if r.DisallowNegative && v < 0 {
			return ErrNegativeThresholdDisallow
		}
		if r.UpperRecoveryThreshold != nil {
			rec := *r.UpperRecoveryThreshold
			if math.IsNaN(rec) || math.IsInf(rec, 0) {
				return ErrInvalidRecoveryThreshold
			}
			if r.DisallowNegative && rec < 0 {
				return ErrNegativeThresholdDisallow
			}
			if rec > v {
				return ErrUpperRecoveryTooHigh
			}
		}
	}

	if r.LowerThreshold != nil {
		v := *r.LowerThreshold
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return ErrInvalidLowerThreshold
		}
		if r.DisallowNegative && v < 0 {
			return ErrNegativeThresholdDisallow
		}
		if r.LowerRecoveryThreshold != nil {
			rec := *r.LowerRecoveryThreshold
			if math.IsNaN(rec) || math.IsInf(rec, 0) {
				return ErrInvalidRecoveryThreshold
			}
			if r.DisallowNegative && rec < 0 {
				return ErrNegativeThresholdDisallow
			}
			if rec < v {
				return ErrLowerRecoveryTooLow
			}
		}
	}

	if r.UpperThreshold != nil && r.LowerThreshold != nil {
		if *r.UpperThreshold < *r.LowerThreshold {
			return ErrUpperLessThanLower
		}
	}

	if r.ExpectedValue != nil {
		exp := *r.ExpectedValue
		if math.IsNaN(exp) || math.IsInf(exp, 0) {
			return ErrInvalidExpectedValueRule
		}
		if r.DisallowNegative && exp < 0 {
			return ErrNegativeThresholdDisallow
		}
	}

	if r.MaxDeviation != nil {
		dev := *r.MaxDeviation
		if math.IsNaN(dev) || math.IsInf(dev, 0) || dev <= 0 {
			return ErrInvalidMaxDeviation
		}
	}

	return nil
}

// metricState tracks active hysteresis breach state per node and metric.
type metricState struct {
	upperActive bool
	lowerActive bool
}

// ThresholdDetectorConfig configures the static threshold detector.
type ThresholdDetectorConfig struct {
	Version string
	Rules   []ThresholdRule
}

// IDGeneratorFunc defines the signature for anomaly ID generation.
type IDGeneratorFunc func(nodeID, metricName, version string, timestamp time.Time, sampleID string, value float64) (string, error)

// ThresholdDetector implements a deterministic, offline static threshold anomaly detector.
// In accordance with ADR-0009 §10, §11, §18, and §20:
// - Evaluates single-sample (W=1) telemetry instantaneously (zero warmup lag).
// - Operates 100% offline with zero external network, NATS, or cloud dependencies.
// - Supports upper ceilings, lower floors, and dual-threshold hysteresis recovery bands.
// - Emits ONE AnomalySignal when a breach onset occurs; avoids duplicate signals for a continuously active breach.
// - Clears the active breach when the metric reaches the recovery threshold; permits new anomaly on subsequent breach.
// - Derives AnomalyID deterministically from stable domain context (node, metric, timestamp, value, version).
// - Normalizes mathematical AnomalyScore in [0.0, 1.0] independently of incident severity.
type ThresholdDetector struct {
	mu          sync.RWMutex
	name        string
	version     string
	rules       map[string]ThresholdRule
	states      map[string]metricState
	idGenerator IDGeneratorFunc
}

// NewThresholdDetector initializes a validated ThresholdDetector instance.
func NewThresholdDetector(cfg ThresholdDetectorConfig) (*ThresholdDetector, error) {
	ver := strings.TrimSpace(cfg.Version)
	if ver == "" {
		ver = "1.0.0"
	}

	if len(cfg.Rules) == 0 {
		return nil, fmt.Errorf("%w: at least one rule required", ErrInvalidRuleConfig)
	}

	ruleMap := make(map[string]ThresholdRule, len(cfg.Rules))
	for _, rule := range cfg.Rules {
		if err := rule.Validate(); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidRuleConfig, err)
		}
		if _, exists := ruleMap[rule.MetricName]; exists {
			return nil, fmt.Errorf("%w: metric %q", ErrDuplicateMetricRule, rule.MetricName)
		}
		ruleMap[rule.MetricName] = rule
	}

	return &ThresholdDetector{
		name:        "static_threshold_detector",
		version:     ver,
		rules:       ruleMap,
		states:      make(map[string]metricState),
		idGenerator: DefaultDeterministicAnomalyID,
	}, nil
}

// Name returns the identifier of this detector implementation.
func (d *ThresholdDetector) Name() string {
	return d.name
}

// Version returns the version string of the detector algorithm or rule set.
func (d *ThresholdDetector) Version() string {
	return d.version
}

// SetIDGenerator allows overriding the default deterministic ID generator for testing.
func (d *ThresholdDetector) SetIDGenerator(fn IDGeneratorFunc) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.idGenerator = fn
}

// ResetState clears all in-memory hysteresis breach tracking.
func (d *ThresholdDetector) ResetState() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.states = make(map[string]metricState)
}

// Detect evaluates a single MetricSample against configured detection rules.
// In accordance with ADR-0009 persistent-breach semantics:
// - Emits ONE AnomalySignal upon breach onset (when state transitions from nominal to breached).
// - For continuously active persistent breaches, returns (nil, nil) to prevent duplicate signal flooding.
// - Clears the active breach when the metric recovers past the recovery threshold.
// - Once recovered, any subsequent threshold breach generates a new AnomalySignal.
func (d *ThresholdDetector) Detect(ctx context.Context, sample types.MetricSample) (*types.AnomalySignal, error) {
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

	if rule.DisallowNegative && sample.Value < 0 {
		return nil, fmt.Errorf("negative metric value %f disallowed for %q: %w", sample.Value, sample.Name, types.ErrInvalidMetricValue)
	}

	nodeID := sample.NodeID
	if nodeID == "" {
		nodeID = "local-edge"
	}
	stateKey := nodeID + ":" + sample.Name

	d.mu.Lock()
	defer d.mu.Unlock()

	st := d.states[stateKey]

	var newUpperBreach bool
	var newLowerBreach bool

	// 1. Evaluate Upper Threshold with Hysteresis & Persistent Breach Semantics
	if rule.UpperThreshold != nil {
		high := *rule.UpperThreshold
		recovery := high
		if rule.UpperRecoveryThreshold != nil {
			recovery = *rule.UpperRecoveryThreshold
		}

		if st.upperActive {
			if sample.Value <= recovery {
				// Cleared: reading dropped to or below recovery threshold
				st.upperActive = false
			}
			// If still > recovery, it remains in the active breach episode (persistent breach, no duplicate signal)
		} else {
			if sample.Value > high {
				// New breach onset!
				st.upperActive = true
				newUpperBreach = true
			}
			// If sample.Value <= high, exact boundary or nominal: not a breach
		}
	}

	// 2. Evaluate Lower Threshold with Hysteresis & Persistent Breach Semantics
	if rule.LowerThreshold != nil {
		low := *rule.LowerThreshold
		recovery := low
		if rule.LowerRecoveryThreshold != nil {
			recovery = *rule.LowerRecoveryThreshold
		}

		if st.lowerActive {
			if sample.Value >= recovery {
				// Cleared: reading rose to or above recovery threshold
				st.lowerActive = false
			}
			// If still < recovery, it remains in the active breach episode (persistent breach, no duplicate signal)
		} else {
			if sample.Value < low {
				// New breach onset!
				st.lowerActive = true
				newLowerBreach = true
			}
			// If sample.Value >= low, exact boundary or nominal: not a breach
		}
	}

	d.states[stateKey] = st

	// 3. Emit AnomalySignal ONLY on new breach onset (ADR-0009 persistent-breach semantics)
	if newUpperBreach {
		high := *rule.UpperThreshold
		recovery := high
		if rule.UpperRecoveryThreshold != nil {
			recovery = *rule.UpperRecoveryThreshold
		}

		expected := high
		if rule.ExpectedValue != nil {
			expected = *rule.ExpectedValue
		}

		deviation := sample.Value - expected
		maxDev := math.Abs(expected)
		if rule.MaxDeviation != nil && *rule.MaxDeviation > 0 {
			maxDev = *rule.MaxDeviation
		} else if maxDev == 0 {
			maxDev = 100.0
		}
		score := math.Min(1.0, math.Max(0.0, math.Abs(deviation)/maxDev))

		anomalyID, err := d.idGenerator(nodeID, sample.Name, d.version, sample.Timestamp, sample.SampleID, sample.Value)
		if err != nil {
			return nil, fmt.Errorf("failed to generate anomaly ID: %w", err)
		}

		sig := &types.AnomalySignal{
			AnomalyID:       anomalyID,
			NodeID:          nodeID,
			MetricName:      sample.Name,
			ObservedValue:   sample.Value,
			ExpectedValue:   expected,
			Deviation:       deviation,
			AnomalyScore:    score,
			DetectionMethod: types.DetectionMethodStaticThreshold,
			DetectedAt:      sample.Timestamp.UTC(),
			Evidence: map[string]string{
				"direction":       "upper",
				"state":           "breached",
				"threshold":       fmt.Sprintf("%.4f", high),
				"recovery_thresh": fmt.Sprintf("%.4f", recovery),
			},
			DetectorVersion: d.version,
		}

		if err := sig.Validate(); err != nil {
			return nil, fmt.Errorf("generated anomaly signal failed validation: %w", err)
		}
		return sig, nil
	}

	if newLowerBreach {
		low := *rule.LowerThreshold
		recovery := low
		if rule.LowerRecoveryThreshold != nil {
			recovery = *rule.LowerRecoveryThreshold
		}

		expected := low
		if rule.ExpectedValue != nil {
			expected = *rule.ExpectedValue
		}

		deviation := sample.Value - expected
		maxDev := math.Abs(expected)
		if rule.MaxDeviation != nil && *rule.MaxDeviation > 0 {
			maxDev = *rule.MaxDeviation
		} else if maxDev == 0 {
			maxDev = 100.0
		}
		score := math.Min(1.0, math.Max(0.0, math.Abs(deviation)/maxDev))

		anomalyID, err := d.idGenerator(nodeID, sample.Name, d.version, sample.Timestamp, sample.SampleID, sample.Value)
		if err != nil {
			return nil, fmt.Errorf("failed to generate anomaly ID: %w", err)
		}

		sig := &types.AnomalySignal{
			AnomalyID:       anomalyID,
			NodeID:          nodeID,
			MetricName:      sample.Name,
			ObservedValue:   sample.Value,
			ExpectedValue:   expected,
			Deviation:       deviation,
			AnomalyScore:    score,
			DetectionMethod: types.DetectionMethodStaticThreshold,
			DetectedAt:      sample.Timestamp.UTC(),
			Evidence: map[string]string{
				"direction":       "lower",
				"state":           "breached",
				"threshold":       fmt.Sprintf("%.4f", low),
				"recovery_thresh": fmt.Sprintf("%.4f", recovery),
			},
			DetectorVersion: d.version,
		}

		if err := sig.Validate(); err != nil {
			return nil, fmt.Errorf("generated anomaly signal failed validation: %w", err)
		}
		return sig, nil
	}

	return nil, nil
}

// DetectBatch evaluates all metrics in a TelemetryBatch against configured detection rules.
// Returns a slice of detected AnomalySignals (empty slice if none detected).
func (d *ThresholdDetector) DetectBatch(ctx context.Context, batch *types.TelemetryBatch) ([]*types.AnomalySignal, error) {
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

// DefaultDeterministicAnomalyID derives an RFC 4122 compliant UUID deterministically
// from stable domain identity and breach context using SHA-256.
//
// Identical (NodeID, MetricName, Version, Timestamp, SampleID, Value) inputs
// produce the exact same AnomalyID across all detector instances and executions.
func DefaultDeterministicAnomalyID(nodeID, metricName, version string, timestamp time.Time, sampleID string, value float64) (string, error) {
	h := sha256.New()
	h.Write([]byte("AegisEdge:AnomalyID:v1\n"))
	h.Write([]byte(nodeID + "\n"))
	h.Write([]byte(metricName + "\n"))
	h.Write([]byte(version + "\n"))
	h.Write([]byte(timestamp.UTC().Format(time.RFC3339Nano) + "\n"))
	h.Write([]byte(sampleID + "\n"))
	h.Write([]byte(strconv.FormatFloat(value, 'g', -1, 64) + "\n"))
	sum := h.Sum(nil)

	// Format as RFC 4122 UUID:
	// Set version to 4 (0x40)
	// Set variant to RFC 4122 (0x80)
	var b [16]byte
	copy(b[:], sum[:16])
	b[6] = (b[6] & 0x0f) | 0x40 // RFC 4122 UUIDv4 format
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4],
		b[4:6],
		b[6:8],
		b[8:10],
		b[10:16],
	), nil
}

// DefaultRules returns standard baseline threshold rules for generated edge telemetry.
func DefaultRules() []ThresholdRule {
	highCPU := 90.0
	recCPU := 80.0
	expCPU := 50.0

	highMem := 90.0
	recMem := 80.0
	expMem := 60.0

	highDisk := 90.0
	recDisk := 85.0
	expDisk := 40.0

	highTemp := 85.0
	recTemp := 75.0
	expTemp := 50.0

	return []ThresholdRule{
		{
			MetricName:             "cpu_usage_percent",
			UpperThreshold:         &highCPU,
			UpperRecoveryThreshold: &recCPU,
			ExpectedValue:          &expCPU,
			DisallowNegative:       true,
		},
		{
			MetricName:             "memory_usage_percent",
			UpperThreshold:         &highMem,
			UpperRecoveryThreshold: &recMem,
			ExpectedValue:          &expMem,
			DisallowNegative:       true,
		},
		{
			MetricName:             "disk_usage_percent",
			UpperThreshold:         &highDisk,
			UpperRecoveryThreshold: &recDisk,
			ExpectedValue:          &expDisk,
			DisallowNegative:       true,
		},
		{
			MetricName:             "temperature_celsius",
			UpperThreshold:         &highTemp,
			UpperRecoveryThreshold: &recTemp,
			ExpectedValue:          &expTemp,
			DisallowNegative:       false,
		},
	}
}

package types

import (
	"errors"
	"math"
	"strings"
	"time"
)

// Anomaly domain validation errors.
var (
	ErrEmptyAnomalyID       = errors.New("anomaly_id cannot be empty")
	ErrEmptyDetectionMethod = errors.New("detection_method cannot be empty")
	ErrEmptyDetectorVersion = errors.New("detector_version cannot be empty")
	ErrInvalidAnomalyScore  = errors.New("anomaly_score must be a finite number between 0.0 and 1.0")
	ErrInvalidDeviation     = errors.New("deviation must be a valid finite number (not NaN or Inf)")
	ErrInvalidExpectedValue = errors.New("expected_value must be a valid finite number (not NaN or Inf)")
	ErrInvalidObservedValue = errors.New("observed_value must be a valid finite number (not NaN or Inf)")
)

// Standard detection method identifiers.
const (
	DetectionMethodStaticThreshold = "static_threshold"
)

// AnomalySignal represents a discrete mathematical deviation detected in a telemetry stream.
// It is an intermediate domain entity emitted by detectors and consumed by the Incident Engine.
// ADR-0009 §8 enforces strict separation between mathematical AnomalyScore [0.0, 1.0]
// and operational IncidentSeverity (LOW, MEDIUM, HIGH, CRITICAL).
type AnomalySignal struct {
	// AnomalyID is the globally unique identifier for this detection instance (UUIDv4).
	AnomalyID string `json:"anomaly_id"`

	// NodeID identifies the edge device on which the anomaly was detected.
	NodeID string `json:"node_id"`

	// MetricName identifies the specific telemetry metric evaluated (e.g. "cpu_utilization_percent").
	MetricName string `json:"metric_name"`

	// ObservedValue is the numerical value that triggered the detection.
	ObservedValue float64 `json:"observed_value"`

	// ExpectedValue is the baseline, nominal threshold, or predicted center point.
	ExpectedValue float64 `json:"expected_value"`

	// Deviation is the mathematical distance between observed and expected (Observed - Expected).
	Deviation float64 `json:"deviation"`

	// AnomalyScore is the normalized deviation magnitude [0.0 to 1.0], or statistical z-score.
	AnomalyScore float64 `json:"anomaly_score"`

	// DetectionMethod identifies the algorithm or strategy (e.g., "static_threshold", "ewma", "z_score").
	DetectionMethod string `json:"detection_method"`

	// DetectedAt is the immutable edge UTC timestamp when the anomaly was recognized.
	DetectedAt time.Time `json:"detected_at"`

	// Evidence provides diagnostic key-value context (e.g. threshold, direction, window_size).
	Evidence map[string]string `json:"evidence,omitempty"`

	// CorrelationID links the signal to a causal batch or trace context.
	CorrelationID string `json:"correlation_id,omitempty"`

	// DetectorVersion is the SemVer identifier of the rule set or model producing this signal.
	DetectorVersion string `json:"detector_version"`
}

// Validate verifies the AnomalySignal domain contract according to ADR-0009 §8.3.
func (a *AnomalySignal) Validate() error {
	if strings.TrimSpace(a.AnomalyID) == "" {
		return ErrEmptyAnomalyID
	}
	if strings.TrimSpace(a.NodeID) == "" {
		return ErrEmptyNodeID
	}
	if strings.TrimSpace(a.MetricName) == "" {
		return ErrEmptyMetricName
	}
	if math.IsNaN(a.ObservedValue) || math.IsInf(a.ObservedValue, 0) {
		return ErrInvalidObservedValue
	}
	if math.IsNaN(a.ExpectedValue) || math.IsInf(a.ExpectedValue, 0) {
		return ErrInvalidExpectedValue
	}
	if math.IsNaN(a.Deviation) || math.IsInf(a.Deviation, 0) {
		return ErrInvalidDeviation
	}
	if math.IsNaN(a.AnomalyScore) || math.IsInf(a.AnomalyScore, 0) || a.AnomalyScore < 0.0 || a.AnomalyScore > 1.0 {
		return ErrInvalidAnomalyScore
	}
	if strings.TrimSpace(a.DetectionMethod) == "" {
		return ErrEmptyDetectionMethod
	}
	if a.DetectedAt.IsZero() {
		return ErrInvalidTimestamp
	}
	if strings.TrimSpace(a.DetectorVersion) == "" {
		return ErrEmptyDetectorVersion
	}
	return nil
}

package types

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"
)

func validAnomalySignal() AnomalySignal {
	return AnomalySignal{
		AnomalyID:       "11111111-2222-3333-4444-555555555555",
		NodeID:          "edge-node-01",
		MetricName:      "cpu_usage_percent",
		ObservedValue:   95.5,
		ExpectedValue:   90.0,
		Deviation:       5.5,
		AnomalyScore:    0.55,
		DetectionMethod: DetectionMethodStaticThreshold,
		DetectedAt:      time.Now().UTC(),
		Evidence: map[string]string{
			"threshold": "90.0",
			"direction": "upper",
		},
		CorrelationID:   "batch-uuid-1234",
		DetectorVersion: "1.0.0",
	}
}

func TestAnomalySignal_Validate_Valid(t *testing.T) {
	sig := validAnomalySignal()
	if err := sig.Validate(); err != nil {
		t.Fatalf("expected valid anomaly signal to pass validation, got: %v", err)
	}

	// Boundary scores: 0.0 and 1.0 are valid
	sig.AnomalyScore = 0.0
	if err := sig.Validate(); err != nil {
		t.Fatalf("expected anomaly score 0.0 to be valid, got: %v", err)
	}

	sig.AnomalyScore = 1.0
	if err := sig.Validate(); err != nil {
		t.Fatalf("expected anomaly score 1.0 to be valid, got: %v", err)
	}

	// Optional fields can be empty
	sig.Evidence = nil
	sig.CorrelationID = ""
	if err := sig.Validate(); err != nil {
		t.Fatalf("expected signal with nil optional fields to be valid, got: %v", err)
	}
}

func TestAnomalySignal_Validate_Errors(t *testing.T) {
	tests := []struct {
		name        string
		modify      func(s *AnomalySignal)
		expectedErr error
	}{
		{
			name:        "empty anomaly_id",
			modify:      func(s *AnomalySignal) { s.AnomalyID = "" },
			expectedErr: ErrEmptyAnomalyID,
		},
		{
			name:        "whitespace anomaly_id",
			modify:      func(s *AnomalySignal) { s.AnomalyID = "   \t\n" },
			expectedErr: ErrEmptyAnomalyID,
		},
		{
			name:        "empty node_id",
			modify:      func(s *AnomalySignal) { s.NodeID = "" },
			expectedErr: ErrEmptyNodeID,
		},
		{
			name:        "whitespace node_id",
			modify:      func(s *AnomalySignal) { s.NodeID = "  " },
			expectedErr: ErrEmptyNodeID,
		},
		{
			name:        "empty metric_name",
			modify:      func(s *AnomalySignal) { s.MetricName = "" },
			expectedErr: ErrEmptyMetricName,
		},
		{
			name:        "whitespace metric_name",
			modify:      func(s *AnomalySignal) { s.MetricName = " \t " },
			expectedErr: ErrEmptyMetricName,
		},
		{
			name:        "observed_value NaN",
			modify:      func(s *AnomalySignal) { s.ObservedValue = math.NaN() },
			expectedErr: ErrInvalidObservedValue,
		},
		{
			name:        "observed_value +Inf",
			modify:      func(s *AnomalySignal) { s.ObservedValue = math.Inf(1) },
			expectedErr: ErrInvalidObservedValue,
		},
		{
			name:        "observed_value -Inf",
			modify:      func(s *AnomalySignal) { s.ObservedValue = math.Inf(-1) },
			expectedErr: ErrInvalidObservedValue,
		},
		{
			name:        "expected_value NaN",
			modify:      func(s *AnomalySignal) { s.ExpectedValue = math.NaN() },
			expectedErr: ErrInvalidExpectedValue,
		},
		{
			name:        "expected_value +Inf",
			modify:      func(s *AnomalySignal) { s.ExpectedValue = math.Inf(1) },
			expectedErr: ErrInvalidExpectedValue,
		},
		{
			name:        "expected_value -Inf",
			modify:      func(s *AnomalySignal) { s.ExpectedValue = math.Inf(-1) },
			expectedErr: ErrInvalidExpectedValue,
		},
		{
			name:        "deviation NaN",
			modify:      func(s *AnomalySignal) { s.Deviation = math.NaN() },
			expectedErr: ErrInvalidDeviation,
		},
		{
			name:        "deviation +Inf",
			modify:      func(s *AnomalySignal) { s.Deviation = math.Inf(1) },
			expectedErr: ErrInvalidDeviation,
		},
		{
			name:        "deviation -Inf",
			modify:      func(s *AnomalySignal) { s.Deviation = math.Inf(-1) },
			expectedErr: ErrInvalidDeviation,
		},
		{
			name:        "anomaly_score NaN",
			modify:      func(s *AnomalySignal) { s.AnomalyScore = math.NaN() },
			expectedErr: ErrInvalidAnomalyScore,
		},
		{
			name:        "anomaly_score +Inf",
			modify:      func(s *AnomalySignal) { s.AnomalyScore = math.Inf(1) },
			expectedErr: ErrInvalidAnomalyScore,
		},
		{
			name:        "anomaly_score -Inf",
			modify:      func(s *AnomalySignal) { s.AnomalyScore = math.Inf(-1) },
			expectedErr: ErrInvalidAnomalyScore,
		},
		{
			name:        "anomaly_score negative",
			modify:      func(s *AnomalySignal) { s.AnomalyScore = -0.01 },
			expectedErr: ErrInvalidAnomalyScore,
		},
		{
			name:        "anomaly_score greater than 1.0",
			modify:      func(s *AnomalySignal) { s.AnomalyScore = 1.0001 },
			expectedErr: ErrInvalidAnomalyScore,
		},
		{
			name:        "empty detection_method",
			modify:      func(s *AnomalySignal) { s.DetectionMethod = "" },
			expectedErr: ErrEmptyDetectionMethod,
		},
		{
			name:        "whitespace detection_method",
			modify:      func(s *AnomalySignal) { s.DetectionMethod = "   " },
			expectedErr: ErrEmptyDetectionMethod,
		},
		{
			name:        "zero detected_at timestamp",
			modify:      func(s *AnomalySignal) { s.DetectedAt = time.Time{} },
			expectedErr: ErrInvalidTimestamp,
		},
		{
			name:        "empty detector_version",
			modify:      func(s *AnomalySignal) { s.DetectorVersion = "" },
			expectedErr: ErrEmptyDetectorVersion,
		},
		{
			name:        "whitespace detector_version",
			modify:      func(s *AnomalySignal) { s.DetectorVersion = "\t  " },
			expectedErr: ErrEmptyDetectorVersion,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := validAnomalySignal()
			tt.modify(&s)
			err := s.Validate()
			if err == nil {
				t.Fatalf("expected error %v, got nil", tt.expectedErr)
			}
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("expected error %v, got %v", tt.expectedErr, err)
			}
		})
	}
}

func TestAnomalySignal_JSONRoundTrip(t *testing.T) {
	orig := validAnomalySignal()

	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("failed to marshal AnomalySignal to JSON: %v", err)
	}

	var decoded AnomalySignal
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal AnomalySignal from JSON: %v", err)
	}

	if decoded.AnomalyID != orig.AnomalyID {
		t.Errorf("expected AnomalyID %q, got %q", orig.AnomalyID, decoded.AnomalyID)
	}
	if decoded.NodeID != orig.NodeID {
		t.Errorf("expected NodeID %q, got %q", orig.NodeID, decoded.NodeID)
	}
	if decoded.MetricName != orig.MetricName {
		t.Errorf("expected MetricName %q, got %q", orig.MetricName, decoded.MetricName)
	}
	if decoded.ObservedValue != orig.ObservedValue {
		t.Errorf("expected ObservedValue %f, got %f", orig.ObservedValue, decoded.ObservedValue)
	}
	if decoded.ExpectedValue != orig.ExpectedValue {
		t.Errorf("expected ExpectedValue %f, got %f", orig.ExpectedValue, decoded.ExpectedValue)
	}
	if decoded.Deviation != orig.Deviation {
		t.Errorf("expected Deviation %f, got %f", orig.Deviation, decoded.Deviation)
	}
	if decoded.AnomalyScore != orig.AnomalyScore {
		t.Errorf("expected AnomalyScore %f, got %f", orig.AnomalyScore, decoded.AnomalyScore)
	}
	if decoded.DetectionMethod != orig.DetectionMethod {
		t.Errorf("expected DetectionMethod %q, got %q", orig.DetectionMethod, decoded.DetectionMethod)
	}
	if !decoded.DetectedAt.Equal(orig.DetectedAt) {
		t.Errorf("expected DetectedAt %v, got %v", orig.DetectedAt, decoded.DetectedAt)
	}
	if decoded.Evidence["threshold"] != orig.Evidence["threshold"] {
		t.Errorf("expected Evidence threshold %q, got %q", orig.Evidence["threshold"], decoded.Evidence["threshold"])
	}
	if decoded.CorrelationID != orig.CorrelationID {
		t.Errorf("expected CorrelationID %q, got %q", orig.CorrelationID, decoded.CorrelationID)
	}
	if decoded.DetectorVersion != orig.DetectorVersion {
		t.Errorf("expected DetectorVersion %q, got %q", orig.DetectorVersion, decoded.DetectorVersion)
	}

	if err := decoded.Validate(); err != nil {
		t.Fatalf("decoded signal failed validation: %v", err)
	}
}

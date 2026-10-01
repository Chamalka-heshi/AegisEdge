package ml

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func helperValidManifest() *ModelManifest {
	return &ModelManifest{
		ModelID:              "iforest-test-v1",
		ModelVersion:         "1.0.0",
		Algorithm:            AlgorithmIsolationForest,
		FeatureSchemaVersion: FeatureSchemaV1,
		TrainingDatasetID:    "ds-test",
		CreatedAt:            time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
		InputDimensions:      4,
		SupportedMetrics: []string{
			"cpu_usage_percent",
			"memory_usage_percent",
			"disk_usage_percent",
			"temperature_celsius",
		},
		NormalizationParams: []FeatureNormalizationParams{
			{MetricName: "cpu_usage_percent", Mean: 50.0, StdDev: 10.0, Min: 0.0, Max: 100.0},
			{MetricName: "memory_usage_percent", Mean: 50.0, StdDev: 10.0, Min: 0.0, Max: 100.0},
			{MetricName: "disk_usage_percent", Mean: 50.0, StdDev: 10.0, Min: 0.0, Max: 100.0},
			{MetricName: "temperature_celsius", Mean: 50.0, StdDev: 10.0, Min: 0.0, Max: 100.0},
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
		},
		SubSampleSize:     256,
		DecisionThreshold: 0.60,
		Status:            ModelStatusActive,
	}
}

func TestModelManifest_Valid(t *testing.T) {
	m := helperValidManifest()
	sum, err := m.ComputeChecksum()
	if err != nil {
		t.Fatalf("checksum computation failed: %v", err)
	}
	m.ChecksumSHA256 = sum

	if err := m.Validate(); err != nil {
		t.Fatalf("expected valid manifest, got error: %v", err)
	}
}

func TestModelManifest_InvalidMetadata(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(m *ModelManifest)
		wantErr string
	}{
		{
			name: "empty model_id",
			mutate: func(m *ModelManifest) {
				m.ModelID = ""
			},
			wantErr: "model_id cannot be empty",
		},
		{
			name: "empty model_version",
			mutate: func(m *ModelManifest) {
				m.ModelVersion = ""
			},
			wantErr: "model_version cannot be empty",
		},
		{
			name: "unsupported algorithm",
			mutate: func(m *ModelManifest) {
				m.Algorithm = "random_forest"
			},
			wantErr: "unsupported algorithm",
		},
		{
			name: "incompatible feature schema",
			mutate: func(m *ModelManifest) {
				m.FeatureSchemaVersion = "features.v2.0.0"
			},
			wantErr: "unsupported feature_schema_version",
		},
		{
			name: "invalid status",
			mutate: func(m *ModelManifest) {
				m.Status = ModelStatusDisabled
			},
			wantErr: "status is",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := helperValidManifest()
			tc.mutate(m)
			err := m.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got: %v", tc.wantErr, err)
			}
		})
	}
}

func TestModelManifest_DimensionAndMetricValidation(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(m *ModelManifest)
		wantErr string
	}{
		{
			name: "input dimensions zero",
			mutate: func(m *ModelManifest) {
				m.InputDimensions = 0
			},
			wantErr: "input_dimensions 0 out of bounds",
		},
		{
			name: "input dimensions mismatch supported metrics length",
			mutate: func(m *ModelManifest) {
				m.InputDimensions = 5
			},
			wantErr: "len(supported_metrics)=4 != input_dimensions=5",
		},
		{
			name: "unsupported metric name",
			mutate: func(m *ModelManifest) {
				m.SupportedMetrics[0] = "unsupported_metric"
				m.NormalizationParams[0].MetricName = "unsupported_metric"
			},
			wantErr: "not in canonical schema",
		},
		{
			name: "duplicate metric name",
			mutate: func(m *ModelManifest) {
				m.SupportedMetrics[1] = m.SupportedMetrics[0]
				m.NormalizationParams[1].MetricName = m.SupportedMetrics[0]
			},
			wantErr: "duplicate metric",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := helperValidManifest()
			tc.mutate(m)
			err := m.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got: %v", tc.wantErr, err)
			}
		})
	}
}

func TestModelManifest_ChecksumValidation(t *testing.T) {
	m := helperValidManifest()
	sum, err := m.ComputeChecksum()
	if err != nil {
		t.Fatalf("compute checksum failed: %v", err)
	}

	// Valid checksum
	m.ChecksumSHA256 = sum
	if err := m.Validate(); err != nil {
		t.Fatalf("expected valid checksum pass, got: %v", err)
	}

	// Tampered checksum
	m.ChecksumSHA256 = "0000000000000000000000000000000000000000000000000000000000000000"
	if err := m.Validate(); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected checksum mismatch, got: %v", err)
	}
}

func TestModelManifest_JSONSerializationRoundTrip(t *testing.T) {
	m := helperValidManifest()
	sum, _ := m.ComputeChecksum()
	m.ChecksumSHA256 = sum

	data, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("json marshal failed: %v", err)
	}

	var roundTrip ModelManifest
	if err := json.Unmarshal(data, &roundTrip); err != nil {
		t.Fatalf("json unmarshal failed: %v", err)
	}

	if err := roundTrip.Validate(); err != nil {
		t.Fatalf("unmarshaled manifest validation failed: %v", err)
	}

	if roundTrip.ModelID != m.ModelID || roundTrip.ChecksumSHA256 != m.ChecksumSHA256 {
		t.Fatalf("roundtrip mismatch: got %v, want %v", roundTrip.ModelID, m.ModelID)
	}
}

func TestModelManifest_ResourceCeilings(t *testing.T) {
	m := helperValidManifest()
	m.SubSampleSize = MaxSubSampleSize + 1
	if err := m.Validate(); err == nil || !strings.Contains(err.Error(), "must be in [2,") {
		t.Fatalf("expected subsample limit error, got: %v", err)
	}
}

func TestExpectedPathLength(t *testing.T) {
	if c0 := ExpectedPathLength(0); c0 != 0.0 {
		t.Fatalf("expected c(0)=0.0, got %f", c0)
	}
	if c1 := ExpectedPathLength(1); c1 != 0.0 {
		t.Fatalf("expected c(1)=0.0, got %f", c1)
	}
	if c2 := ExpectedPathLength(2); c2 != 1.0 {
		t.Fatalf("expected c(2)=1.0, got %f", c2)
	}
	c256 := ExpectedPathLength(256)
	if c256 <= 9.0 || c256 >= 11.0 {
		t.Fatalf("expected c(256) around ~10.0, got %f", c256)
	}
}

func TestModelManifest_SerializeDeserializeRoundTrip(t *testing.T) {
	m := helperValidManifest()
	sum, err := m.ComputeChecksum()
	if err != nil {
		t.Fatalf("checksum failed: %v", err)
	}
	m.ChecksumSHA256 = sum

	data, err := Serialize(m)
	if err != nil {
		t.Fatalf("serialize failed: %v", err)
	}

	loaded, err := Deserialize(data)
	if err != nil {
		t.Fatalf("deserialize failed: %v", err)
	}

	if loaded.ChecksumSHA256 != m.ChecksumSHA256 {
		t.Fatalf("checksum mismatch: got %s, want %s", loaded.ChecksumSHA256, m.ChecksumSHA256)
	}
	if loaded.ModelID != m.ModelID {
		t.Fatalf("model ID mismatch: got %s, want %s", loaded.ModelID, m.ModelID)
	}
}

func TestModelManifest_Deserialize_CorruptAndEmpty(t *testing.T) {
	// Empty data
	_, err := Deserialize([]byte(""))
	if err == nil || !strings.Contains(err.Error(), "empty data") {
		t.Fatalf("expected empty data error, got: %v", err)
	}

	// Malformed JSON
	_, err = Deserialize([]byte("{not json}"))
	if err == nil || !strings.Contains(err.Error(), "JSON unmarshal error") {
		t.Fatalf("expected unmarshal error, got: %v", err)
	}

	// Invalid model
	m := helperValidManifest()
	m.ModelID = "" // invalid
	data, _ := json.Marshal(m)
	_, err = Deserialize(data)
	if err == nil || !strings.Contains(err.Error(), "model_id cannot be empty") {
		t.Fatalf("expected validation error, got: %v", err)
	}
}

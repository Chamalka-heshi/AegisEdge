package detector

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

func newTestStatisticalDetector(t *testing.T) *StatisticalDetector {
	t.Helper()
	cfg := StatisticalDetectorConfig{
		Version: "1.0.0",
		Rules: []StatisticalRule{
			{
				MetricName:      "cpu_usage_percent",
				WindowSize:      10,
				MinObservations: 5,
				ZScoreThreshold: 3.0,
			},
			{
				MetricName:      "memory_usage_percent",
				WindowSize:      10,
				MinObservations: 5,
				ZScoreThreshold: 3.0,
			},
			{
				MetricName:      "disk_usage_percent",
				WindowSize:      10,
				MinObservations: 5,
				ZScoreThreshold: 3.0,
			},
		},
	}
	det, err := NewStatisticalDetector(cfg)
	if err != nil {
		t.Fatalf("failed to create statistical detector: %v", err)
	}
	return det
}

// 1. Detector construction with valid configuration
func TestStatisticalDetector_ValidConstruction(t *testing.T) {
	det := newTestStatisticalDetector(t)
	if det.Name() != "rolling_statistical_detector" {
		t.Errorf("expected name 'rolling_statistical_detector', got %q", det.Name())
	}
	if det.Version() != "1.0.0" {
		t.Errorf("expected version '1.0.0', got %q", det.Version())
	}

	// Default rules helper
	defaultRules := DefaultStatisticalRules()
	if len(defaultRules) == 0 {
		t.Fatal("expected non-empty default rules")
	}
	detDefaults, err := NewStatisticalDetector(StatisticalDetectorConfig{
		Version: "2.0.0",
		Rules:   defaultRules,
	})
	if err != nil {
		t.Fatalf("failed to construct detector with default rules: %v", err)
	}
	if detDefaults.Version() != "2.0.0" {
		t.Errorf("expected version '2.0.0', got %q", detDefaults.Version())
	}
}

// 2. Invalid configuration
func TestStatisticalDetector_InvalidConfiguration(t *testing.T) {
	tests := []struct {
		name    string
		cfg     StatisticalDetectorConfig
		wantErr error
	}{
		{
			name: "no rules",
			cfg: StatisticalDetectorConfig{
				Rules: []StatisticalRule{},
			},
			wantErr: ErrInvalidRuleConfig,
		},
		{
			name: "empty metric name",
			cfg: StatisticalDetectorConfig{
				Rules: []StatisticalRule{
					{MetricName: "", WindowSize: 10, MinObservations: 5, ZScoreThreshold: 3.0},
				},
			},
			wantErr: ErrEmptyRuleMetricName,
		},
		{
			name: "zero window size",
			cfg: StatisticalDetectorConfig{
				Rules: []StatisticalRule{
					{MetricName: "cpu", WindowSize: 0, MinObservations: 5, ZScoreThreshold: 3.0},
				},
			},
			wantErr: ErrInvalidWindowSize,
		},
		{
			name: "negative window size",
			cfg: StatisticalDetectorConfig{
				Rules: []StatisticalRule{
					{MetricName: "cpu", WindowSize: -5, MinObservations: 5, ZScoreThreshold: 3.0},
				},
			},
			wantErr: ErrInvalidWindowSize,
		},
		{
			name: "min observations < 2",
			cfg: StatisticalDetectorConfig{
				Rules: []StatisticalRule{
					{MetricName: "cpu", WindowSize: 10, MinObservations: 1, ZScoreThreshold: 3.0},
				},
			},
			wantErr: ErrInvalidMinObservations,
		},
		{
			name: "min observations > window size",
			cfg: StatisticalDetectorConfig{
				Rules: []StatisticalRule{
					{MetricName: "cpu", WindowSize: 5, MinObservations: 10, ZScoreThreshold: 3.0},
				},
			},
			wantErr: ErrInvalidMinObservations,
		},
		{
			name: "zero z_score_threshold",
			cfg: StatisticalDetectorConfig{
				Rules: []StatisticalRule{
					{MetricName: "cpu", WindowSize: 10, MinObservations: 5, ZScoreThreshold: 0.0},
				},
			},
			wantErr: ErrInvalidZScoreThreshold,
		},
		{
			name: "negative z_score_threshold",
			cfg: StatisticalDetectorConfig{
				Rules: []StatisticalRule{
					{MetricName: "cpu", WindowSize: 10, MinObservations: 5, ZScoreThreshold: -2.5},
				},
			},
			wantErr: ErrInvalidZScoreThreshold,
		},
		{
			name: "NaN z_score_threshold",
			cfg: StatisticalDetectorConfig{
				Rules: []StatisticalRule{
					{MetricName: "cpu", WindowSize: 10, MinObservations: 5, ZScoreThreshold: math.NaN()},
				},
			},
			wantErr: ErrInvalidZScoreThreshold,
		},
		{
			name: "Inf z_score_threshold",
			cfg: StatisticalDetectorConfig{
				Rules: []StatisticalRule{
					{MetricName: "cpu", WindowSize: 10, MinObservations: 5, ZScoreThreshold: math.Inf(1)},
				},
			},
			wantErr: ErrInvalidZScoreThreshold,
		},
		{
			name: "max_z_score < z_score_threshold",
			cfg: StatisticalDetectorConfig{
				Rules: []StatisticalRule{
					{
						MetricName:      "cpu",
						WindowSize:      10,
						MinObservations: 5,
						ZScoreThreshold: 3.0,
						MaxZScore:       floatPtr(2.0),
					},
				},
			},
			wantErr: ErrInvalidMaxZScore,
		},
		{
			name: "duplicate metric rules",
			cfg: StatisticalDetectorConfig{
				Rules: []StatisticalRule{
					{MetricName: "cpu", WindowSize: 10, MinObservations: 5, ZScoreThreshold: 3.0},
					{MetricName: "cpu", WindowSize: 20, MinObservations: 10, ZScoreThreshold: 4.0},
				},
			},
			wantErr: ErrDuplicateMetricRule,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewStatisticalDetector(tc.cfg)
			if err == nil {
				t.Fatalf("expected error %v, got nil", tc.wantErr)
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected error wrapping %v, got %v", tc.wantErr, err)
			}
		})
	}
}

// 3. Warm-up period: no anomalies emitted before MinObservations
func TestStatisticalDetector_WarmUpPeriod(t *testing.T) {
	det := newTestStatisticalDetector(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// MinObservations = 5. Feed 4 extreme spike samples; must NOT emit an anomaly
	spikes := []float64{100.0, 200.0, 300.0, 400.0}
	for i, v := range spikes {
		sig, err := det.Detect(ctx, types.MetricSample{
			NodeID:    "node-01",
			Name:      "cpu_usage_percent",
			Value:     v,
			Timestamp: now.Add(time.Duration(i) * time.Second),
		})
		if err != nil {
			t.Fatalf("step %d: unexpected error: %v", i, err)
		}
		if sig != nil {
			t.Fatalf("step %d: expected nil during warmup, got signal: %+v", i, sig)
		}
	}

	hist := det.GetHistory("node-01", "cpu_usage_percent")
	if len(hist) != 4 {
		t.Fatalf("expected history length 4, got %d", len(hist))
	}

	// 5th sample arrives (history currently has 4 samples < 5) -> STILL warmup!
	sig, err := det.Detect(ctx, types.MetricSample{
		NodeID:    "node-01",
		Name:      "cpu_usage_percent",
		Value:     500.0,
		Timestamp: now.Add(4 * time.Second),
	})
	if err != nil {
		t.Fatalf("step 4: unexpected error: %v", err)
	}
	if sig != nil {
		t.Fatalf("step 4: expected nil signal when history len was 4 < MinObservations, got %+v", sig)
	}

	// History now has 5 samples. The next sample (6th) will be evaluated against the baseline of 5 samples.
	hist = det.GetHistory("node-01", "cpu_usage_percent")
	if len(hist) != 5 {
		t.Fatalf("expected history length 5, got %d", len(hist))
	}
}

// 4. Normal values: stable telemetry produces no anomalies
func TestStatisticalDetector_NormalValues(t *testing.T) {
	det := newTestStatisticalDetector(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// Feed warmup history
	baseline := []float64{44.0, 45.0, 43.0, 46.0, 44.0}
	for i, v := range baseline {
		_, err := det.Detect(ctx, types.MetricSample{
			NodeID:    "node-01",
			Name:      "cpu_usage_percent",
			Value:     v,
			Timestamp: now.Add(time.Duration(i) * time.Second),
		})
		if err != nil {
			t.Fatalf("warmup step %d: %v", i, err)
		}
	}

	// Feed normal subsequent readings
	subsequent := []float64{45.0, 44.0, 43.5, 45.5, 44.2}
	for i, v := range subsequent {
		sig, err := det.Detect(ctx, types.MetricSample{
			NodeID:    "node-01",
			Name:      "cpu_usage_percent",
			Value:     v,
			Timestamp: now.Add(time.Duration(5+i) * time.Second),
		})
		if err != nil {
			t.Fatalf("subsequent step %d: %v", i, err)
		}
		if sig != nil {
			t.Fatalf("subsequent step %d: expected nil for normal value %f, got %+v", i, v, sig)
		}
	}
}

// 5. Obvious statistical anomaly: value far outside baseline triggers AnomalySignal
func TestStatisticalDetector_ObviousStatisticalAnomaly(t *testing.T) {
	det := newTestStatisticalDetector(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// History: 42, 44, 45, 43, 46, 44, 45 (mean ~44.14, stddev ~1.12)
	history := []float64{42.0, 44.0, 45.0, 43.0, 46.0, 44.0, 45.0}
	for i, v := range history {
		_, err := det.Detect(ctx, types.MetricSample{
			NodeID:    "node-01",
			Name:      "cpu_usage_percent",
			Value:     v,
			Timestamp: now.Add(time.Duration(i) * time.Second),
		})
		if err != nil {
			t.Fatalf("warmup step %d: %v", i, err)
		}
	}

	// New value: 82.0 (massive departure from mean ~44.14)
	spikeTime := now.Add(10 * time.Second)
	sig, err := det.Detect(ctx, types.MetricSample{
		SampleID:  "sample-spike-82",
		NodeID:    "node-01",
		Name:      "cpu_usage_percent",
		Value:     82.0,
		Timestamp: spikeTime,
	})
	if err != nil {
		t.Fatalf("unexpected error on spike: %v", err)
	}
	if sig == nil {
		t.Fatal("expected anomaly signal for spike value 82.0, got nil")
	}

	if sig.NodeID != "node-01" {
		t.Errorf("expected NodeID 'node-01', got %q", sig.NodeID)
	}
	if sig.MetricName != "cpu_usage_percent" {
		t.Errorf("expected MetricName 'cpu_usage_percent', got %q", sig.MetricName)
	}
	if sig.ObservedValue != 82.0 {
		t.Errorf("expected ObservedValue 82.0, got %f", sig.ObservedValue)
	}
	if sig.DetectionMethod != types.DetectionMethodZScore {
		t.Errorf("expected DetectionMethod %q, got %q", types.DetectionMethodZScore, sig.DetectionMethod)
	}
	if sig.AnomalyScore <= 0.0 || sig.AnomalyScore > 1.0 {
		t.Errorf("expected AnomalyScore in (0, 1], got %f", sig.AnomalyScore)
	}
	if sig.Evidence["direction"] != "upper" {
		t.Errorf("expected direction 'upper', got %q", sig.Evidence["direction"])
	}
	if err := sig.Validate(); err != nil {
		t.Errorf("emitted signal failed validation: %v", err)
	}
}

// 6. Anomaly below z-score threshold: moderately elevated value below ZScoreThreshold produces no anomaly
func TestStatisticalDetector_AnomalyBelowZScoreThreshold(t *testing.T) {
	det := newTestStatisticalDetector(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// History: 42, 44, 45, 43, 46, 44, 45 (mean ~44.14, stddev ~1.12)
	history := []float64{42.0, 44.0, 45.0, 43.0, 46.0, 44.0, 45.0}
	for i, v := range history {
		_, err := det.Detect(ctx, types.MetricSample{
			NodeID:    "node-01",
			Name:      "cpu_usage_percent",
			Value:     v,
			Timestamp: now.Add(time.Duration(i) * time.Second),
		})
		if err != nil {
			t.Fatalf("warmup step %d: %v", i, err)
		}
	}

	// Value 46.5: diff ~ 2.36, z ~ 2.36 / 1.12 ~ 2.1 < 3.0 threshold
	sig, err := det.Detect(ctx, types.MetricSample{
		NodeID:    "node-01",
		Name:      "cpu_usage_percent",
		Value:     46.5,
		Timestamp: now.Add(10 * time.Second),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sig != nil {
		t.Fatalf("expected nil signal for z < 3.0, got: %+v", sig)
	}
}

// 7. Rolling window eviction: history bounded by WindowSize
func TestStatisticalDetector_RollingWindowEviction(t *testing.T) {
	det := newTestStatisticalDetector(t) // WindowSize = 10
	ctx := context.Background()
	now := time.Now().UTC()

	// Feed 15 sequential observations: 1, 2, 3, ..., 15
	for i := 1; i <= 15; i++ {
		_, err := det.Detect(ctx, types.MetricSample{
			NodeID:    "node-01",
			Name:      "cpu_usage_percent",
			Value:     float64(i),
			Timestamp: now.Add(time.Duration(i) * time.Second),
		})
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
	}

	hist := det.GetHistory("node-01", "cpu_usage_percent")
	if len(hist) != 10 {
		t.Fatalf("expected history length bounded at 10, got %d", len(hist))
	}

	// Oldest 5 (1, 2, 3, 4, 5) must be evicted; history must contain 6 through 15
	for i, expected := range []float64{6, 7, 8, 9, 10, 11, 12, 13, 14, 15} {
		if hist[i] != expected {
			t.Errorf("history[%d] = %f, want %f", i, hist[i], expected)
		}
	}
}

// 8. Current observation excluded from its own baseline
func TestStatisticalDetector_CurrentObservationExcludedFromOwnBaseline(t *testing.T) {
	det := newTestStatisticalDetector(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// Feed 5 identical values: 10, 10, 10, 10, 10
	for i := 0; i < 5; i++ {
		_, err := det.Detect(ctx, types.MetricSample{
			NodeID:    "node-01",
			Name:      "cpu_usage_percent",
			Value:     10.0,
			Timestamp: now.Add(time.Duration(i) * time.Second),
		})
		if err != nil {
			t.Fatalf("warmup %d: %v", i, err)
		}
	}

	// Now send a spike: 100.0.
	// If the current observation contaminated the baseline, mean would be (50+100)/6 = 25.0.
	// Because it is excluded, mean MUST be exactly 10.0 and stddev MUST be 0.0.
	sig, err := det.Detect(ctx, types.MetricSample{
		NodeID:    "node-01",
		Name:      "cpu_usage_percent",
		Value:     100.0,
		Timestamp: now.Add(10 * time.Second),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sig == nil {
		t.Fatal("expected anomaly signal, got nil")
	}

	if sig.ExpectedValue != 10.0 {
		t.Errorf("expected baseline mean 10.0 (uncontaminated), got %f", sig.ExpectedValue)
	}
	if sig.Deviation != 90.0 {
		t.Errorf("expected deviation 90.0 (100.0 - 10.0), got %f", sig.Deviation)
	}
	if sig.Evidence["mean"] != "10.000000" {
		t.Errorf("expected evidence mean '10.000000', got %q", sig.Evidence["mean"])
	}
	if sig.Evidence["standard_deviation"] != "0.000000" {
		t.Errorf("expected evidence stddev '0.000000', got %q", sig.Evidence["standard_deviation"])
	}
}

// 9. Zero standard deviation: flat history handled deterministically without NaN/Inf
func TestStatisticalDetector_ZeroStandardDeviation(t *testing.T) {
	det := newTestStatisticalDetector(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// History: 50, 50, 50, 50, 50 (stddev == 0.0)
	for i := 0; i < 5; i++ {
		_, err := det.Detect(ctx, types.MetricSample{
			NodeID:    "node-01",
			Name:      "cpu_usage_percent",
			Value:     50.0,
			Timestamp: now.Add(time.Duration(i) * time.Second),
		})
		if err != nil {
			t.Fatalf("warmup %d: %v", i, err)
		}
	}

	// 9A: Current value approximately equal to mean (50.0) -> NO anomaly, no NaN/Inf
	sigNom, err := det.Detect(ctx, types.MetricSample{
		NodeID:    "node-01",
		Name:      "cpu_usage_percent",
		Value:     50.0,
		Timestamp: now.Add(6 * time.Second),
	})
	if err != nil {
		t.Fatalf("unexpected error on nominal reading with zero stddev: %v", err)
	}
	if sigNom != nil {
		t.Fatalf("expected nil signal when current equals invariant mean, got %+v", sigNom)
	}

	// 9B: Current value meaningfully different (80.0) -> ANOMALY emitted, no NaN/Inf
	sigBreach, err := det.Detect(ctx, types.MetricSample{
		NodeID:    "node-01",
		Name:      "cpu_usage_percent",
		Value:     80.0,
		Timestamp: now.Add(7 * time.Second),
	})
	if err != nil {
		t.Fatalf("unexpected error on breach reading with zero stddev: %v", err)
	}
	if sigBreach == nil {
		t.Fatal("expected anomaly signal for step-change on zero stddev baseline, got nil")
	}

	if math.IsNaN(sigBreach.AnomalyScore) || math.IsInf(sigBreach.AnomalyScore, 0) {
		t.Fatalf("AnomalyScore is NaN or Inf: %f", sigBreach.AnomalyScore)
	}
	if math.IsNaN(sigBreach.Deviation) || math.IsInf(sigBreach.Deviation, 0) {
		t.Fatalf("Deviation is NaN or Inf: %f", sigBreach.Deviation)
	}
	if sigBreach.Evidence["zero_stddev"] != "true" {
		t.Errorf("expected evidence zero_stddev='true', got %q", sigBreach.Evidence["zero_stddev"])
	}
	if err := sigBreach.Validate(); err != nil {
		t.Errorf("signal failed domain validation: %v", err)
	}
}

// 10. Near-zero standard deviation: extremely small variance (stddev <= epsilon)
func TestStatisticalDetector_NearZeroStandardDeviation(t *testing.T) {
	det := newTestStatisticalDetector(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// History with tiny variance: 50.000000000001, 50.000000000002, ... (stddev ~ 1e-12 <= epsilon 1e-9)
	for i := 0; i < 5; i++ {
		v := 50.0 + float64(i)*1e-12
		_, err := det.Detect(ctx, types.MetricSample{
			NodeID:    "node-01",
			Name:      "cpu_usage_percent",
			Value:     v,
			Timestamp: now.Add(time.Duration(i) * time.Second),
		})
		if err != nil {
			t.Fatalf("warmup %d: %v", i, err)
		}
	}

	// Reading with minor difference within epsilon: no anomaly
	sigNom, err := det.Detect(ctx, types.MetricSample{
		NodeID:    "node-01",
		Name:      "cpu_usage_percent",
		Value:     50.000000000005,
		Timestamp: now.Add(6 * time.Second),
	})
	if err != nil || sigNom != nil {
		t.Fatalf("expected nil for near-zero stddev micro-fluctuation, got sig=%v, err=%v", sigNom, err)
	}

	// Reading with large step-change (70.0): anomaly emitted cleanly
	sigBreach, err := det.Detect(ctx, types.MetricSample{
		NodeID:    "node-01",
		Name:      "cpu_usage_percent",
		Value:     70.0,
		Timestamp: now.Add(7 * time.Second),
	})
	if err != nil || sigBreach == nil {
		t.Fatalf("expected anomaly signal on near-zero stddev breach, got sig=%v, err=%v", sigBreach, err)
	}
	if err := sigBreach.Validate(); err != nil {
		t.Errorf("near-zero stddev signal failed validation: %v", err)
	}
}

// 11. NaN input rejected and does not contaminate rolling history
func TestStatisticalDetector_NaNInput(t *testing.T) {
	det := newTestStatisticalDetector(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// Seed 3 valid samples
	for i := 0; i < 3; i++ {
		_, _ = det.Detect(ctx, types.MetricSample{
			NodeID: "node-01", Name: "cpu_usage_percent", Value: 50.0, Timestamp: now.Add(time.Duration(i) * time.Second),
		})
	}

	// Attempt NaN sample
	_, err := det.Detect(ctx, types.MetricSample{
		NodeID: "node-01", Name: "cpu_usage_percent", Value: math.NaN(), Timestamp: now.Add(4 * time.Second),
	})
	if err == nil {
		t.Fatal("expected error for NaN sample, got nil")
	}
	if !errors.Is(err, types.ErrInvalidMetricValue) {
		t.Errorf("expected error wrapping ErrInvalidMetricValue, got %v", err)
	}

	// Verify history length remains 3 and contains no NaN
	hist := det.GetHistory("node-01", "cpu_usage_percent")
	if len(hist) != 3 {
		t.Fatalf("expected history length 3, got %d", len(hist))
	}
	for _, v := range hist {
		if math.IsNaN(v) {
			t.Fatal("found NaN in rolling history after rejected sample")
		}
	}
}

// 12. Inf input rejected and does not contaminate rolling history
func TestStatisticalDetector_InfInput(t *testing.T) {
	det := newTestStatisticalDetector(t)
	ctx := context.Background()
	now := time.Now().UTC()

	for _, infVal := range []float64{math.Inf(1), math.Inf(-1)} {
		_, err := det.Detect(ctx, types.MetricSample{
			NodeID: "node-01", Name: "cpu_usage_percent", Value: infVal, Timestamp: now,
		})
		if err == nil {
			t.Fatalf("expected error for Inf sample %f, got nil", infVal)
		}
		if !errors.Is(err, types.ErrInvalidMetricValue) {
			t.Errorf("expected error wrapping ErrInvalidMetricValue, got %v", err)
		}
	}

	hist := det.GetHistory("node-01", "cpu_usage_percent")
	if len(hist) != 0 {
		t.Fatalf("expected empty history, got length %d", len(hist))
	}
}

// 13. State isolation: Node A does not influence Node B
func TestStatisticalDetector_StateIsolation_Node(t *testing.T) {
	det := newTestStatisticalDetector(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// Warmup Node A with low values [10, 10, 10, 10, 10]
	for i := 0; i < 5; i++ {
		_, _ = det.Detect(ctx, types.MetricSample{
			NodeID: "node-a", Name: "cpu_usage_percent", Value: 10.0, Timestamp: now.Add(time.Duration(i) * time.Second),
		})
	}

	// Warmup Node B with high values [90, 90, 90, 90, 90]
	for i := 0; i < 5; i++ {
		_, _ = det.Detect(ctx, types.MetricSample{
			NodeID: "node-b", Name: "cpu_usage_percent", Value: 90.0, Timestamp: now.Add(time.Duration(i) * time.Second),
		})
	}

	// Test value 90.0 on Node A -> massive anomaly relative to [10, 10, 10, 10, 10]
	sigA, err := det.Detect(ctx, types.MetricSample{
		NodeID: "node-a", Name: "cpu_usage_percent", Value: 90.0, Timestamp: now.Add(10 * time.Second),
	})
	if err != nil || sigA == nil {
		t.Fatalf("expected anomaly on node-a for value 90.0, got sig=%v, err=%v", sigA, err)
	}
	if sigA.ExpectedValue != 10.0 {
		t.Errorf("expected node-a baseline 10.0, got %f", sigA.ExpectedValue)
	}

	// Test value 90.0 on Node B -> nominal relative to [90, 90, 90, 90, 90]
	sigB, err := det.Detect(ctx, types.MetricSample{
		NodeID: "node-b", Name: "cpu_usage_percent", Value: 90.0, Timestamp: now.Add(10 * time.Second),
	})
	if err != nil {
		t.Fatalf("unexpected error on node-b: %v", err)
	}
	if sigB != nil {
		t.Fatalf("expected nil signal on node-b for value 90.0, got %+v", sigB)
	}
}

// 14. Metric isolation: CPU does not influence Memory on the same node
func TestStatisticalDetector_StateIsolation_Metric(t *testing.T) {
	det := newTestStatisticalDetector(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// Warmup CPU with values [10, 10, 10, 10, 10]
	for i := 0; i < 5; i++ {
		_, _ = det.Detect(ctx, types.MetricSample{
			NodeID: "node-01", Name: "cpu_usage_percent", Value: 10.0, Timestamp: now.Add(time.Duration(i) * time.Second),
		})
	}

	// Warmup Memory with values [80, 80, 80, 80, 80]
	for i := 0; i < 5; i++ {
		_, _ = det.Detect(ctx, types.MetricSample{
			NodeID: "node-01", Name: "memory_usage_percent", Value: 80.0, Timestamp: now.Add(time.Duration(i) * time.Second),
		})
	}

	// Value 80.0 on CPU -> anomaly
	sigCPU, err := det.Detect(ctx, types.MetricSample{
		NodeID: "node-01", Name: "cpu_usage_percent", Value: 80.0, Timestamp: now.Add(10 * time.Second),
	})
	if err != nil || sigCPU == nil {
		t.Fatalf("expected CPU anomaly, got sig=%v, err=%v", sigCPU, err)
	}

	// Value 80.0 on Memory -> nominal
	sigMem, err := det.Detect(ctx, types.MetricSample{
		NodeID: "node-01", Name: "memory_usage_percent", Value: 80.0, Timestamp: now.Add(10 * time.Second),
	})
	if err != nil || sigMem != nil {
		t.Fatalf("expected Memory nominal, got sig=%v, err=%v", sigMem, err)
	}
}

// 14B. Delimiter collision safety: structured streamKey prevents cross-stream collision
// between Node "edge-01" / Metric ":cpu" and Node "edge-01:" / Metric "cpu".
func TestStatisticalDetector_StateIsolation_DelimiterCollision(t *testing.T) {
	cfg := StatisticalDetectorConfig{
		Version: "1.0.0",
		Rules: []StatisticalRule{
			{MetricName: ":cpu", WindowSize: 10, MinObservations: 5, ZScoreThreshold: 3.0},
			{MetricName: "cpu", WindowSize: 10, MinObservations: 5, ZScoreThreshold: 3.0},
		},
	}
	det, err := NewStatisticalDetector(cfg)
	if err != nil {
		t.Fatalf("failed to construct detector: %v", err)
	}
	ctx := context.Background()
	now := time.Now().UTC()

	// Warmup Stream 1: Node "edge-01" / Metric ":cpu" with values [10, 10, 10, 10, 10]
	for i := 0; i < 5; i++ {
		_, _ = det.Detect(ctx, types.MetricSample{
			NodeID: "edge-01", Name: ":cpu", Value: 10.0, Timestamp: now.Add(time.Duration(i) * time.Second),
		})
	}

	// Warmup Stream 2: Node "edge-01:" / Metric "cpu" with values [90, 90, 90, 90, 90]
	// Note: If keys were strings concatenated as "nodeID:metricName", both streams would produce "edge-01::cpu"!
	for i := 0; i < 5; i++ {
		_, _ = det.Detect(ctx, types.MetricSample{
			NodeID: "edge-01:", Name: "cpu", Value: 90.0, Timestamp: now.Add(time.Duration(i) * time.Second),
		})
	}

	// Send reading 90.0 to Stream 1 ("edge-01", ":cpu") -> must be an ANOMALY relative to [10, 10, 10, 10, 10]
	sig1, err := det.Detect(ctx, types.MetricSample{
		NodeID: "edge-01", Name: ":cpu", Value: 90.0, Timestamp: now.Add(10 * time.Second),
	})
	if err != nil || sig1 == nil {
		t.Fatalf("expected anomaly on stream 1, got sig=%v, err=%v", sig1, err)
	}
	if sig1.ExpectedValue != 10.0 {
		t.Errorf("expected stream 1 baseline 10.0, got %f (collision detected)", sig1.ExpectedValue)
	}

	// Send reading 90.0 to Stream 2 ("edge-01:", "cpu") -> must be NOMINAL relative to [90, 90, 90, 90, 90]
	sig2, err := det.Detect(ctx, types.MetricSample{
		NodeID: "edge-01:", Name: "cpu", Value: 90.0, Timestamp: now.Add(10 * time.Second),
	})
	if err != nil || sig2 != nil {
		t.Fatalf("expected nominal on stream 2, got sig=%v, err=%v (collision detected)", sig2, err)
	}
}

// 15. Deterministic results: identical inputs produce identical outputs across instances
func TestStatisticalDetector_DeterministicResult(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)

	feed := func(d *StatisticalDetector) *types.AnomalySignal {
		for i := 0; i < 5; i++ {
			_, _ = d.Detect(ctx, types.MetricSample{
				NodeID: "node-01", Name: "cpu_usage_percent", Value: 40.0 + float64(i), Timestamp: now.Add(time.Duration(i) * time.Second),
			})
		}
		sig, _ := d.Detect(ctx, types.MetricSample{
			SampleID: "samp-99", NodeID: "node-01", Name: "cpu_usage_percent", Value: 95.0, Timestamp: now.Add(10 * time.Second),
		})
		return sig
	}

	det1 := newTestStatisticalDetector(t)
	det2 := newTestStatisticalDetector(t)

	sig1 := feed(det1)
	sig2 := feed(det2)

	if sig1 == nil || sig2 == nil {
		t.Fatal("expected non-nil signals from both instances")
	}

	if sig1.AnomalyID != sig2.AnomalyID {
		t.Errorf("AnomalyID mismatch: %q vs %q", sig1.AnomalyID, sig2.AnomalyID)
	}
	if sig1.ObservedValue != sig2.ObservedValue {
		t.Errorf("ObservedValue mismatch: %f vs %f", sig1.ObservedValue, sig2.ObservedValue)
	}
	if sig1.ExpectedValue != sig2.ExpectedValue {
		t.Errorf("ExpectedValue mismatch: %f vs %f", sig1.ExpectedValue, sig2.ExpectedValue)
	}
	if sig1.Deviation != sig2.Deviation {
		t.Errorf("Deviation mismatch: %f vs %f", sig1.Deviation, sig2.Deviation)
	}
	if sig1.AnomalyScore != sig2.AnomalyScore {
		t.Errorf("AnomalyScore mismatch: %f vs %f", sig1.AnomalyScore, sig2.AnomalyScore)
	}
}

// 16. Deterministic AnomalyID: stable SHA-256 derived UUID
func TestStatisticalDetector_DeterministicAnomalyID(t *testing.T) {
	det := newTestStatisticalDetector(t)
	ctx := context.Background()
	now := time.Date(2026, 9, 29, 14, 30, 0, 0, time.UTC)

	for i := 0; i < 5; i++ {
		_, _ = det.Detect(ctx, types.MetricSample{
			NodeID: "node-det", Name: "cpu_usage_percent", Value: 50.0, Timestamp: now.Add(time.Duration(i) * time.Second),
		})
	}

	sample := types.MetricSample{
		SampleID:  "sample-stable-123",
		NodeID:    "node-det",
		Name:      "cpu_usage_percent",
		Value:     99.0,
		Timestamp: now.Add(10 * time.Second),
	}

	sig1, err := det.Detect(ctx, sample)
	if err != nil || sig1 == nil {
		t.Fatalf("first detect failed: %v", err)
	}

	// Compute expected ID using exported DefaultDeterministicAnomalyID
	expectedID, err := DefaultDeterministicAnomalyID("node-det", "cpu_usage_percent", det.Version(), sample.Timestamp, sample.SampleID, sample.Value)
	if err != nil {
		t.Fatalf("failed to compute expected ID: %v", err)
	}

	if sig1.AnomalyID != expectedID {
		t.Errorf("AnomalyID mismatch: got %q, want %q", sig1.AnomalyID, expectedID)
	}
}

// 17. Evidence contains statistical values
func TestStatisticalDetector_EvidenceContainsStatisticalValues(t *testing.T) {
	det := newTestStatisticalDetector(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// 5 samples: 20, 22, 21, 23, 24 -> mean = 22.0
	for i, v := range []float64{20, 22, 21, 23, 24} {
		_, _ = det.Detect(ctx, types.MetricSample{
			NodeID: "node-01", Name: "cpu_usage_percent", Value: v, Timestamp: now.Add(time.Duration(i) * time.Second),
		})
	}

	sig, err := det.Detect(ctx, types.MetricSample{
		NodeID: "node-01", Name: "cpu_usage_percent", Value: 55.0, Timestamp: now.Add(10 * time.Second),
	})
	if err != nil || sig == nil {
		t.Fatalf("expected anomaly, got sig=%v, err=%v", sig, err)
	}

	requiredKeys := []string{
		"mean", "standard_deviation", "z_score", "z_threshold",
		"window_size", "observation_count", "rule_name", "detector_version",
		"direction", "description",
	}
	for _, k := range requiredKeys {
		if _, ok := sig.Evidence[k]; !ok {
			t.Errorf("missing evidence key %q", k)
		}
	}
	if sig.Evidence["direction"] != "upper" {
		t.Errorf("expected direction 'upper', got %q", sig.Evidence["direction"])
	}
}

// 18. AnomalyScore remains bounded in [0.0, 1.0] across all scenarios
func TestStatisticalDetector_ScoreRemainsWithinDocumentedRange(t *testing.T) {
	det := newTestStatisticalDetector(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// Warmup
	for i := 0; i < 5; i++ {
		_, _ = det.Detect(ctx, types.MetricSample{
			NodeID: "node-01", Name: "cpu_usage_percent", Value: 50.0, Timestamp: now.Add(time.Duration(i) * time.Second),
		})
	}

	// Extreme spike: 1,000,000
	sigHuge, err := det.Detect(ctx, types.MetricSample{
		NodeID: "node-01", Name: "cpu_usage_percent", Value: 1000000.0, Timestamp: now.Add(6 * time.Second),
	})
	if err != nil || sigHuge == nil {
		t.Fatalf("huge spike failed: %v", err)
	}
	if sigHuge.AnomalyScore < 0.0 || sigHuge.AnomalyScore > 1.0 {
		t.Errorf("score out of range for huge spike: %f", sigHuge.AnomalyScore)
	}
	if sigHuge.AnomalyScore != 1.0 {
		t.Errorf("expected score 1.0 for huge spike, got %f", sigHuge.AnomalyScore)
	}

	// Lower spike: test on a clean baseline
	det.ResetState()
	for i := 0; i < 5; i++ {
		_, _ = det.Detect(ctx, types.MetricSample{
			NodeID: "node-01", Name: "cpu_usage_percent", Value: 50.0, Timestamp: now.Add(time.Duration(10+i) * time.Second),
		})
	}

	sigLow, err := det.Detect(ctx, types.MetricSample{
		NodeID: "node-01", Name: "cpu_usage_percent", Value: -500.0, Timestamp: now.Add(20 * time.Second),
	})
	if err != nil || sigLow == nil {
		t.Fatalf("low spike failed: %v", err)
	}
	if sigLow.AnomalyScore < 0.0 || sigLow.AnomalyScore > 1.0 {
		t.Errorf("score out of range for low spike: %f", sigLow.AnomalyScore)
	}
	if sigLow.Evidence["direction"] != "lower" {
		t.Errorf("expected direction 'lower', got %q", sigLow.Evidence["direction"])
	}
}

// 19. Concurrent detector access
func TestStatisticalDetector_ConcurrentAccess(t *testing.T) {
	det := newTestStatisticalDetector(t)
	ctx := context.Background()
	now := time.Now().UTC()

	var wg sync.WaitGroup
	workers := 25
	iterations := 40

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			nodeID := "node-concurrent"
			metric := "cpu_usage_percent"
			if workerID%2 == 0 {
				metric = "memory_usage_percent"
			}

			for i := 0; i < iterations; i++ {
				val := 50.0
				if i == 30 {
					val = 150.0 // trigger potential anomaly
				}
				sample := types.MetricSample{
					NodeID:    nodeID,
					Name:      metric,
					Value:     val,
					Timestamp: now.Add(time.Duration(i) * time.Millisecond),
				}
				_, err := det.Detect(ctx, sample)
				if err != nil {
					t.Errorf("worker %d step %d: error %v", workerID, i, err)
					return
				}
			}
		}(w)
	}

	wg.Wait()
}

// 20. Repeated identical processing behavior
func TestStatisticalDetector_RepeatedIdenticalProcessing(t *testing.T) {
	det := newTestStatisticalDetector(t)
	ctx := context.Background()
	now := time.Date(2026, 9, 29, 15, 0, 0, 0, time.UTC)

	series := []float64{40, 42, 41, 43, 42, 85, 42, 41, 43, 90}

	runSeries := func() []string {
		det.ResetState()
		var ids []string
		for i, v := range series {
			sig, err := det.Detect(ctx, types.MetricSample{
				SampleID:  "sample-rep",
				NodeID:    "node-rep",
				Name:      "cpu_usage_percent",
				Value:     v,
				Timestamp: now.Add(time.Duration(i) * time.Second),
			})
			if err != nil {
				t.Fatalf("step %d: %v", i, err)
			}
			if sig != nil {
				ids = append(ids, sig.AnomalyID)
			} else {
				ids = append(ids, "nominal")
			}
		}
		return ids
	}

	run1 := runSeries()
	run2 := runSeries()

	if len(run1) != len(run2) {
		t.Fatalf("length mismatch: %d vs %d", len(run1), len(run2))
	}
	for i := range run1 {
		if run1[i] != run2[i] {
			t.Errorf("step %d mismatch: %q vs %q", i, run1[i], run2[i])
		}
	}
}

// 21. DetectBatch with mixed telemetry and CorrelationID propagation
func TestStatisticalDetector_DetectBatch(t *testing.T) {
	det := newTestStatisticalDetector(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// Seed warmup
	for i := 0; i < 5; i++ {
		_, _ = det.Detect(ctx, types.MetricSample{
			NodeID: "node-01", Name: "cpu_usage_percent", Value: 50.0, Timestamp: now.Add(time.Duration(i) * time.Second),
		})
		_, _ = det.Detect(ctx, types.MetricSample{
			NodeID: "node-01", Name: "memory_usage_percent", Value: 50.0, Timestamp: now.Add(time.Duration(i) * time.Second),
		})
	}

	batch := &types.TelemetryBatch{
		BatchID:        "batch-stat-999",
		NodeID:         "node-01",
		SequenceNumber: 15,
		CollectedAt:    now.Add(10 * time.Second),
		Metrics: []types.MetricSample{
			{NodeID: "node-01", Name: "cpu_usage_percent", Value: 95.0, Timestamp: now.Add(10 * time.Second)},    // Spike
			{NodeID: "node-01", Name: "memory_usage_percent", Value: 50.0, Timestamp: now.Add(10 * time.Second)}, // Normal
			{NodeID: "node-01", Name: "unconfigured_metric", Value: 999.0, Timestamp: now.Add(10 * time.Second)}, // Unconfigured
		},
	}

	signals, err := det.DetectBatch(ctx, batch)
	if err != nil {
		t.Fatalf("DetectBatch failed: %v", err)
	}

	if len(signals) != 1 {
		t.Fatalf("expected exactly 1 anomaly signal, got %d", len(signals))
	}
	if signals[0].MetricName != "cpu_usage_percent" {
		t.Errorf("expected cpu_usage_percent signal, got %q", signals[0].MetricName)
	}
	if signals[0].CorrelationID != "batch-stat-999" {
		t.Errorf("expected CorrelationID 'batch-stat-999', got %q", signals[0].CorrelationID)
	}

	// Nil batch check
	_, err = det.DetectBatch(ctx, nil)
	if !errors.Is(err, ErrNilBatch) {
		t.Errorf("expected ErrNilBatch on nil batch, got %v", err)
	}
}

// 22. MapToIncident integration with canonical Incident contract
func TestStatisticalDetector_MapToIncident(t *testing.T) {
	det := newTestStatisticalDetector(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// Seed warmup
	for i := 0; i < 5; i++ {
		_, _ = det.Detect(ctx, types.MetricSample{
			NodeID: "node-01", Name: "cpu_usage_percent", Value: 40.0, Timestamp: now.Add(time.Duration(i) * time.Second),
		})
	}

	sig, err := det.Detect(ctx, types.MetricSample{
		NodeID: "node-01", Name: "cpu_usage_percent", Value: 95.0, Timestamp: now.Add(10 * time.Second),
	})
	if err != nil || sig == nil {
		t.Fatalf("failed to generate anomaly signal: %v", err)
	}

	inc, err := det.MapToIncident(sig)
	if err != nil {
		t.Fatalf("MapToIncident failed: %v", err)
	}
	if inc == nil {
		t.Fatal("expected non-nil incident")
	}

	if inc.NodeID != "node-01" {
		t.Errorf("expected NodeID 'node-01', got %q", inc.NodeID)
	}
	if inc.TriggerMetric != "cpu_usage_percent" {
		t.Errorf("expected TriggerMetric 'cpu_usage_percent', got %q", inc.TriggerMetric)
	}
	if inc.Status != types.StatusAnomalyDetected {
		t.Errorf("expected Status ANOMALY_DETECTED, got %v", inc.Status)
	}
	if inc.RuleName != "statistical_cpu_usage_percent" {
		t.Errorf("expected RuleName 'statistical_cpu_usage_percent', got %q", inc.RuleName)
	}
	if inc.TriggerValue != 95.0 {
		t.Errorf("expected TriggerValue 95.0, got %f", inc.TriggerValue)
	}
	if inc.Evidence["detection_method"] != types.DetectionMethodZScore {
		t.Errorf("expected Evidence detection_method %q, got %q", types.DetectionMethodZScore, inc.Evidence["detection_method"])
	}
	if err := inc.Validate(); err != nil {
		t.Errorf("mapped incident failed domain validation: %v", err)
	}
}

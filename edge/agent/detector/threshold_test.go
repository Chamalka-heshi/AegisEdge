package detector

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

func floatPtr(v float64) *float64 {
	return &v
}

func newTestDetector(t *testing.T) *ThresholdDetector {
	t.Helper()
	cfg := ThresholdDetectorConfig{
		Version: "1.0.0",
		Rules: []ThresholdRule{
			{
				MetricName:             "cpu_usage_percent",
				UpperThreshold:         floatPtr(90.0),
				UpperRecoveryThreshold: floatPtr(80.0),
				ExpectedValue:          floatPtr(50.0),
				MaxDeviation:           floatPtr(50.0),
				DisallowNegative:       true, // CPU usage cannot be negative
			},
			{
				MetricName:             "memory_free_percent",
				LowerThreshold:         floatPtr(15.0),
				LowerRecoveryThreshold: floatPtr(25.0),
				ExpectedValue:          floatPtr(50.0),
				MaxDeviation:           floatPtr(50.0),
				DisallowNegative:       true, // Memory percentage cannot be negative
			},
			{
				MetricName:       "disk_usage_percent",
				UpperThreshold:   floatPtr(95.0),
				ExpectedValue:    floatPtr(50.0),
				DisallowNegative: true,
			},
			{
				// Signed metric: temperature in Celsius (allows negative values by default)
				MetricName:             "ambient_temperature_celsius",
				UpperThreshold:         floatPtr(45.0),
				UpperRecoveryThreshold: floatPtr(35.0),
				LowerThreshold:         floatPtr(-20.0),
				LowerRecoveryThreshold: floatPtr(-10.0),
				ExpectedValue:          floatPtr(20.0),
				MaxDeviation:           floatPtr(50.0),
				DisallowNegative:       false,
			},
			{
				// Signed metric: WiFi / Cellular RSSI in dBm (always negative in normal operation)
				MetricName:             "wireless_rssi_dbm",
				LowerThreshold:         floatPtr(-85.0),
				LowerRecoveryThreshold: floatPtr(-75.0),
				ExpectedValue:          floatPtr(-60.0),
				MaxDeviation:           floatPtr(40.0),
				DisallowNegative:       false,
			},
		},
	}

	det, err := NewThresholdDetector(cfg)
	if err != nil {
		t.Fatalf("failed to create test detector: %v", err)
	}
	return det
}

// 1. Normal telemetry
func TestThresholdDetector_NormalTelemetry(t *testing.T) {
	det := newTestDetector(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// Normal CPU sample (50% < 90%)
	cpuSample := types.MetricSample{
		NodeID:    "node-01",
		Name:      "cpu_usage_percent",
		Value:     50.0,
		Timestamp: now,
	}
	sig, err := det.Detect(ctx, cpuSample)
	if err != nil {
		t.Fatalf("unexpected error on normal sample: %v", err)
	}
	if sig != nil {
		t.Fatalf("expected nil signal for normal reading, got: %+v", sig)
	}

	// Normal Memory sample (40% > 15%)
	memSample := types.MetricSample{
		NodeID:    "node-01",
		Name:      "memory_free_percent",
		Value:     40.0,
		Timestamp: now,
	}
	sig, err = det.Detect(ctx, memSample)
	if err != nil {
		t.Fatalf("unexpected error on normal memory sample: %v", err)
	}
	if sig != nil {
		t.Fatalf("expected nil signal for normal reading, got: %+v", sig)
	}

	// Normal batch
	batch := &types.TelemetryBatch{
		BatchID:        "batch-normal-1",
		NodeID:         "node-01",
		SequenceNumber: 1,
		CollectedAt:    now,
		Metrics:        []types.MetricSample{cpuSample, memSample},
	}
	signals, err := det.DetectBatch(ctx, batch)
	if err != nil {
		t.Fatalf("unexpected error on normal batch: %v", err)
	}
	if len(signals) != 0 {
		t.Fatalf("expected 0 signals for normal batch, got %d", len(signals))
	}
}

// 2. Threshold crossing (Upper & Lower)
func TestThresholdDetector_ThresholdCrossing(t *testing.T) {
	det := newTestDetector(t)
	ctx := context.Background()
	now := time.Now().UTC()

	t.Run("upper threshold crossing", func(t *testing.T) {
		sample := types.MetricSample{
			NodeID:    "node-01",
			Name:      "cpu_usage_percent",
			Value:     95.0, // > 90.0
			Timestamp: now,
		}
		sig, err := det.Detect(ctx, sample)
		if err != nil {
			t.Fatalf("unexpected error on threshold crossing: %v", err)
		}
		if sig == nil {
			t.Fatal("expected anomaly signal on threshold crossing, got nil")
		}

		if sig.NodeID != "node-01" {
			t.Errorf("expected NodeID 'node-01', got %q", sig.NodeID)
		}
		if sig.MetricName != "cpu_usage_percent" {
			t.Errorf("expected MetricName 'cpu_usage_percent', got %q", sig.MetricName)
		}
		if sig.ObservedValue != 95.0 {
			t.Errorf("expected ObservedValue 95.0, got %f", sig.ObservedValue)
		}
		if sig.ExpectedValue != 50.0 {
			t.Errorf("expected ExpectedValue 50.0, got %f", sig.ExpectedValue)
		}
		if sig.Deviation != 45.0 {
			t.Errorf("expected Deviation 45.0, got %f", sig.Deviation)
		}
		// score = min(1.0, |45.0| / 50.0) = 0.9
		expectedScore := 45.0 / 50.0
		if math.Abs(sig.AnomalyScore-expectedScore) > 1e-6 {
			t.Errorf("expected AnomalyScore %f, got %f", expectedScore, sig.AnomalyScore)
		}
		if sig.DetectionMethod != types.DetectionMethodStaticThreshold {
			t.Errorf("expected DetectionMethod %q, got %q", types.DetectionMethodStaticThreshold, sig.DetectionMethod)
		}
		if sig.Evidence["direction"] != "upper" {
			t.Errorf("expected direction 'upper', got %q", sig.Evidence["direction"])
		}
		if sig.Evidence["state"] != "breached" {
			t.Errorf("expected state 'breached', got %q", sig.Evidence["state"])
		}
		if err := sig.Validate(); err != nil {
			t.Fatalf("emitted signal failed validation: %v", err)
		}
	})

	t.Run("lower threshold crossing", func(t *testing.T) {
		det.ResetState()
		sample := types.MetricSample{
			NodeID:    "node-01",
			Name:      "memory_free_percent",
			Value:     10.0, // < 15.0
			Timestamp: now,
		}
		sig, err := det.Detect(ctx, sample)
		if err != nil {
			t.Fatalf("unexpected error on lower threshold crossing: %v", err)
		}
		if sig == nil {
			t.Fatal("expected anomaly signal on lower threshold crossing, got nil")
		}

		if sig.MetricName != "memory_free_percent" {
			t.Errorf("expected MetricName 'memory_free_percent', got %q", sig.MetricName)
		}
		if sig.ObservedValue != 10.0 {
			t.Errorf("expected ObservedValue 10.0, got %f", sig.ObservedValue)
		}
		if sig.ExpectedValue != 50.0 {
			t.Errorf("expected ExpectedValue 50.0, got %f", sig.ExpectedValue)
		}
		if sig.Deviation != -40.0 {
			t.Errorf("expected Deviation -40.0, got %f", sig.Deviation)
		}
		// score = min(1.0, |-40.0| / 50.0) = 0.8
		expectedScore := 40.0 / 50.0
		if math.Abs(sig.AnomalyScore-expectedScore) > 1e-6 {
			t.Errorf("expected AnomalyScore %f, got %f", expectedScore, sig.AnomalyScore)
		}
		if sig.Evidence["direction"] != "lower" {
			t.Errorf("expected direction 'lower', got %q", sig.Evidence["direction"])
		}
		if sig.Evidence["state"] != "breached" {
			t.Errorf("expected state 'breached', got %q", sig.Evidence["state"])
		}
		if err := sig.Validate(); err != nil {
			t.Fatalf("emitted signal failed validation: %v", err)
		}
	})
}

// 3. Exact boundary conditions
func TestThresholdDetector_ExactBoundaryConditions(t *testing.T) {
	det := newTestDetector(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// Exact Upper Threshold: Value == 90.0 is within limits (strictly > 90 is breach)
	sampleUpperExact := types.MetricSample{
		NodeID:    "node-01",
		Name:      "cpu_usage_percent",
		Value:     90.0,
		Timestamp: now,
	}
	sig, err := det.Detect(ctx, sampleUpperExact)
	if err != nil {
		t.Fatalf("unexpected error at exact upper boundary: %v", err)
	}
	if sig != nil {
		t.Fatalf("expected nil at exact upper threshold, got anomaly: %+v", sig)
	}

	// Exact Lower Threshold: Value == 15.0 is within limits (strictly < 15 is breach)
	sampleLowerExact := types.MetricSample{
		NodeID:    "node-01",
		Name:      "memory_free_percent",
		Value:     15.0,
		Timestamp: now,
	}
	sig, err = det.Detect(ctx, sampleLowerExact)
	if err != nil {
		t.Fatalf("unexpected error at exact lower boundary: %v", err)
	}
	if sig != nil {
		t.Fatalf("expected nil at exact lower threshold, got anomaly: %+v", sig)
	}

	// Exact Upper Recovery Threshold during breach: Value == 80.0 clears the breach
	// First trigger breach
	_, _ = det.Detect(ctx, types.MetricSample{
		NodeID:    "node-01",
		Name:      "cpu_usage_percent",
		Value:     92.0,
		Timestamp: now,
	})
	// Now test exact recovery threshold: 80.0
	sig, err = det.Detect(ctx, types.MetricSample{
		NodeID:    "node-01",
		Name:      "cpu_usage_percent",
		Value:     80.0, // Value <= 80.0 clears
		Timestamp: now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("unexpected error at exact recovery boundary: %v", err)
	}
	if sig != nil {
		t.Fatalf("expected nil at exact recovery threshold (cleared), got: %+v", sig)
	}

	// Exact Lower Recovery Threshold during breach: Value == 25.0 clears the breach
	// First trigger breach
	_, _ = det.Detect(ctx, types.MetricSample{
		NodeID:    "node-01",
		Name:      "memory_free_percent",
		Value:     10.0,
		Timestamp: now,
	})
	// Now test exact recovery threshold: 25.0
	sig, err = det.Detect(ctx, types.MetricSample{
		NodeID:    "node-01",
		Name:      "memory_free_percent",
		Value:     25.0, // Value >= 25.0 clears
		Timestamp: now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("unexpected error at exact recovery boundary: %v", err)
	}
	if sig != nil {
		t.Fatalf("expected nil at exact lower recovery threshold (cleared), got: %+v", sig)
	}
}

// 4. Persistent breach & recovery semantics
// Verifies ADR-0009 intended semantics:
// - Emits ONE anomaly signal when breach begins
// - For continuously active persistent breach (95 -> 96 -> 97 -> 98), avoids producing duplicate signals
// - After recovery, a new breach generates a new logical anomaly
func TestThresholdDetector_PersistentBreachAndRecoverySemantics(t *testing.T) {
	det := newTestDetector(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// Upper threshold rule: UpperThreshold=90.0, UpperRecoveryThreshold=80.0
	// Sequence: 95 -> 96 -> 97 -> 98 -> 85 -> 78 -> 85 -> 92

	// Step 1: Initial breach onset (95.0 > 90.0) -> MUST emit AnomalySignal
	sig1, err := det.Detect(ctx, types.MetricSample{
		NodeID: "node-01", Name: "cpu_usage_percent", Value: 95.0, Timestamp: now,
	})
	if err != nil {
		t.Fatalf("unexpected error on breach onset: %v", err)
	}
	if sig1 == nil {
		t.Fatal("expected anomaly signal on breach onset (95.0), got nil")
	}
	if sig1.ObservedValue != 95.0 {
		t.Errorf("expected ObservedValue 95.0, got %f", sig1.ObservedValue)
	}

	// Step 2: Persistent breach (96.0) -> MUST NOT emit duplicate signal
	sig2, err := det.Detect(ctx, types.MetricSample{
		NodeID: "node-01", Name: "cpu_usage_percent", Value: 96.0, Timestamp: now.Add(1 * time.Second),
	})
	if err != nil {
		t.Fatalf("unexpected error on persistent breach: %v", err)
	}
	if sig2 != nil {
		t.Fatalf("expected nil on persistent breach (96.0), got duplicate signal: %+v", sig2)
	}

	// Step 3: Persistent breach continues (97.0) -> MUST NOT emit duplicate signal
	sig3, err := det.Detect(ctx, types.MetricSample{
		NodeID: "node-01", Name: "cpu_usage_percent", Value: 97.0, Timestamp: now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("unexpected error on persistent breach: %v", err)
	}
	if sig3 != nil {
		t.Fatalf("expected nil on persistent breach (97.0), got duplicate signal: %+v", sig3)
	}

	// Step 4: Persistent breach continues (98.0) -> MUST NOT emit duplicate signal
	sig4, err := det.Detect(ctx, types.MetricSample{
		NodeID: "node-01", Name: "cpu_usage_percent", Value: 98.0, Timestamp: now.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatalf("unexpected error on persistent breach: %v", err)
	}
	if sig4 != nil {
		t.Fatalf("expected nil on persistent breach (98.0), got duplicate signal: %+v", sig4)
	}

	// Step 5: Reading drops to 85.0 (below breach 90.0, but still inside hysteresis band > 80.0 recovery)
	// Still in active breach episode, not yet recovered -> MUST NOT emit signal
	sig5, err := det.Detect(ctx, types.MetricSample{
		NodeID: "node-01", Name: "cpu_usage_percent", Value: 85.0, Timestamp: now.Add(4 * time.Second),
	})
	if err != nil {
		t.Fatalf("unexpected error in hysteresis band: %v", err)
	}
	if sig5 != nil {
		t.Fatalf("expected nil in hysteresis band (85.0), got: %+v", sig5)
	}

	// Step 6: Reading drops to 78.0 (<= 80.0 recovery threshold) -> Clears active breach!
	sig6, err := det.Detect(ctx, types.MetricSample{
		NodeID: "node-01", Name: "cpu_usage_percent", Value: 78.0, Timestamp: now.Add(5 * time.Second),
	})
	if err != nil {
		t.Fatalf("unexpected error on recovery: %v", err)
	}
	if sig6 != nil {
		t.Fatalf("expected nil on recovery clearing (78.0), got: %+v", sig6)
	}

	// Step 7: Reading rises to 85.0 (nominal envelope, since now cleared and 85.0 <= 90.0) -> Nominal
	sig7, err := det.Detect(ctx, types.MetricSample{
		NodeID: "node-01", Name: "cpu_usage_percent", Value: 85.0, Timestamp: now.Add(6 * time.Second),
	})
	if err != nil {
		t.Fatalf("unexpected error at nominal 85.0: %v", err)
	}
	if sig7 != nil {
		t.Fatalf("expected nil at nominal 85.0, got: %+v", sig7)
	}

	// Step 8: Reading breaches threshold again (92.0 > 90.0) -> MUST emit a NEW AnomalySignal!
	sig8, err := det.Detect(ctx, types.MetricSample{
		NodeID: "node-01", Name: "cpu_usage_percent", Value: 92.0, Timestamp: now.Add(7 * time.Second),
	})
	if err != nil {
		t.Fatalf("unexpected error on new breach: %v", err)
	}
	if sig8 == nil {
		t.Fatal("expected NEW anomaly signal on re-breach (92.0), got nil")
	}
	if sig8.ObservedValue != 92.0 {
		t.Errorf("expected ObservedValue 92.0, got %f", sig8.ObservedValue)
	}
	if sig8.AnomalyID == sig1.AnomalyID {
		t.Errorf("expected new distinct AnomalyID for new breach, got same ID: %q", sig8.AnomalyID)
	}
}

// 5. Signed / Negative metric semantics
func TestThresholdDetector_SignedMetricSemantics(t *testing.T) {
	det := newTestDetector(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// Test 5A: Legitimate signed metric with negative values (ambient temperature: normal at -10°C)
	sampleTempNormal := types.MetricSample{
		NodeID:    "node-weather",
		Name:      "ambient_temperature_celsius",
		Value:     -10.0, // within [-20.0, 45.0]
		Timestamp: now,
	}
	sig, err := det.Detect(ctx, sampleTempNormal)
	if err != nil {
		t.Fatalf("expected signed metric with negative value to be valid, got: %v", err)
	}
	if sig != nil {
		t.Fatalf("expected nil for normal negative temperature (-10°C), got: %+v", sig)
	}

	// Test 5B: Legitimate signed metric lower breach (-25°C < -20°C lower threshold)
	sampleTempBreach := types.MetricSample{
		NodeID:    "node-weather",
		Name:      "ambient_temperature_celsius",
		Value:     -25.0, // < -20.0
		Timestamp: now.Add(time.Second),
	}
	sigTempBreach, err := det.Detect(ctx, sampleTempBreach)
	if err != nil {
		t.Fatalf("unexpected error on signed lower breach: %v", err)
	}
	if sigTempBreach == nil {
		t.Fatal("expected anomaly signal for negative temperature breach (-25°C), got nil")
	}
	if sigTempBreach.ObservedValue != -25.0 {
		t.Errorf("expected ObservedValue -25.0, got %f", sigTempBreach.ObservedValue)
	}
	if sigTempBreach.ExpectedValue != 20.0 {
		t.Errorf("expected ExpectedValue 20.0, got %f", sigTempBreach.ExpectedValue)
	}
	if sigTempBreach.Deviation != -45.0 {
		t.Errorf("expected Deviation -45.0, got %f", sigTempBreach.Deviation)
	}
	if sigTempBreach.Evidence["direction"] != "lower" {
		t.Errorf("expected direction 'lower', got %q", sigTempBreach.Evidence["direction"])
	}

	// Test 5C: Always-negative RF metric (wireless_rssi_dbm: -92 dBm < -85 dBm lower threshold)
	sampleRSSI := types.MetricSample{
		NodeID:    "node-gateway",
		Name:      "wireless_rssi_dbm",
		Value:     -92.0, // < -85.0
		Timestamp: now,
	}
	sigRSSI, err := det.Detect(ctx, sampleRSSI)
	if err != nil {
		t.Fatalf("unexpected error on RSSI evaluation: %v", err)
	}
	if sigRSSI == nil {
		t.Fatal("expected anomaly signal for poor RSSI (-92 dBm), got nil")
	}
	if sigRSSI.ObservedValue != -92.0 {
		t.Errorf("expected ObservedValue -92.0, got %f", sigRSSI.ObservedValue)
	}
	if sigRSSI.ExpectedValue != -60.0 {
		t.Errorf("expected ExpectedValue -60.0, got %f", sigRSSI.ExpectedValue)
	}
	// deviation = -92.0 - (-60.0) = -32.0
	if math.Abs(sigRSSI.Deviation-(-32.0)) > 1e-6 {
		t.Errorf("expected Deviation -32.0, got %f", sigRSSI.Deviation)
	}
	// score = |-32.0| / 40.0 = 0.8
	expectedScore := 32.0 / 40.0
	if math.Abs(sigRSSI.AnomalyScore-expectedScore) > 1e-6 {
		t.Errorf("expected AnomalyScore %f, got %f", expectedScore, sigRSSI.AnomalyScore)
	}

	// Test 5D: Signed metric with nil ExpectedValue defaulting to LowerThreshold
	detNilExpected, err := NewThresholdDetector(ThresholdDetectorConfig{
		Version: "1.0.0",
		Rules: []ThresholdRule{
			{
				MetricName:       "power_flow_watts",
				LowerThreshold:   floatPtr(-100.0),
				DisallowNegative: false,
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to create detector with nil ExpectedValue: %v", err)
	}
	sigPower, err := detNilExpected.Detect(ctx, types.MetricSample{
		NodeID:    "node-battery",
		Name:      "power_flow_watts",
		Value:     -120.0, // < -100.0
		Timestamp: now,
	})
	if err != nil || sigPower == nil {
		t.Fatalf("expected anomaly signal for power_flow_watts breach, got sig=%v, err=%v", sigPower, err)
	}
	if sigPower.ExpectedValue != -100.0 {
		t.Errorf("expected ExpectedValue -100.0 (defaulting to threshold), got %f", sigPower.ExpectedValue)
	}
	if sigPower.Deviation != -20.0 {
		t.Errorf("expected Deviation -20.0, got %f", sigPower.Deviation)
	}
	// score = |-20.0| / |-100.0| = 0.2
	if math.Abs(sigPower.AnomalyScore-0.2) > 1e-6 {
		t.Errorf("expected AnomalyScore 0.2, got %f", sigPower.AnomalyScore)
	}

	// Test 5E: DisallowNegative constraint properly enforced on non-negative metrics
	sampleNegativeCPU := types.MetricSample{
		NodeID:    "node-01",
		Name:      "cpu_usage_percent",
		Value:     -5.0, // CPU usage cannot be negative
		Timestamp: now,
	}
	_, err = det.Detect(ctx, sampleNegativeCPU)
	if err == nil {
		t.Fatal("expected error for negative CPU sample with DisallowNegative=true, got nil")
	}
	if !errors.Is(err, types.ErrInvalidMetricValue) {
		t.Errorf("expected error wrapping ErrInvalidMetricValue, got: %v", err)
	}
}

// 6. State isolation across NodeID and MetricName
// Specifically verifies:
// - Node A / CPU
// - Node B / CPU
// - Node A / Memory
// A breach in one detector state must not affect another NodeID + MetricName state.
func TestThresholdDetector_StateIsolation_NodeAndMetric(t *testing.T) {
	det := newTestDetector(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// 1. Node A / CPU breaches
	sigA_CPU, err := det.Detect(ctx, types.MetricSample{
		NodeID: "node-a", Name: "cpu_usage_percent", Value: 95.0, Timestamp: now,
	})
	if err != nil || sigA_CPU == nil {
		t.Fatalf("expected breach on node-a/cpu, got sig=%v, err=%v", sigA_CPU, err)
	}

	// 2. Node B / CPU receives nominal reading -> MUST be nominal
	sigB_CPU_Nom, err := det.Detect(ctx, types.MetricSample{
		NodeID: "node-b", Name: "cpu_usage_percent", Value: 50.0, Timestamp: now,
	})
	if err != nil || sigB_CPU_Nom != nil {
		t.Fatalf("expected nominal on node-b/cpu, got sig=%v, err=%v", sigB_CPU_Nom, err)
	}

	// 3. Node A / Memory receives nominal reading -> MUST be nominal
	sigA_Mem_Nom, err := det.Detect(ctx, types.MetricSample{
		NodeID: "node-a", Name: "memory_free_percent", Value: 40.0, Timestamp: now,
	})
	if err != nil || sigA_Mem_Nom != nil {
		t.Fatalf("expected nominal on node-a/memory, got sig=%v, err=%v", sigA_Mem_Nom, err)
	}

	// 4. Node A / CPU sends second breach reading -> Persistent breach -> MUST return nil
	sigA_CPU_Persist, err := det.Detect(ctx, types.MetricSample{
		NodeID: "node-a", Name: "cpu_usage_percent", Value: 96.0, Timestamp: now.Add(time.Second),
	})
	if err != nil || sigA_CPU_Persist != nil {
		t.Fatalf("expected persistent breach on node-a/cpu to return nil, got sig=%v, err=%v", sigA_CPU_Persist, err)
	}

	// 5. Node B / CPU now breaches -> MUST emit NEW AnomalySignal (proves Node B was NOT affected by Node A)
	sigB_CPU_Breach, err := det.Detect(ctx, types.MetricSample{
		NodeID: "node-b", Name: "cpu_usage_percent", Value: 94.0, Timestamp: now.Add(time.Second),
	})
	if err != nil || sigB_CPU_Breach == nil {
		t.Fatalf("expected breach on node-b/cpu, got sig=%v, err=%v", sigB_CPU_Breach, err)
	}
	if sigB_CPU_Breach.NodeID != "node-b" {
		t.Errorf("expected NodeID 'node-b', got %q", sigB_CPU_Breach.NodeID)
	}

	// 6. Node A / Memory now breaches -> MUST emit NEW AnomalySignal (proves Node A/Memory was NOT affected by Node A/CPU)
	sigA_Mem_Breach, err := det.Detect(ctx, types.MetricSample{
		NodeID: "node-a", Name: "memory_free_percent", Value: 10.0, Timestamp: now.Add(time.Second),
	})
	if err != nil || sigA_Mem_Breach == nil {
		t.Fatalf("expected breach on node-a/memory, got sig=%v, err=%v", sigA_Mem_Breach, err)
	}
	if sigA_Mem_Breach.MetricName != "memory_free_percent" {
		t.Errorf("expected MetricName 'memory_free_percent', got %q", sigA_Mem_Breach.MetricName)
	}

	// 7. Node A / CPU recovers (75.0 <= 80.0) -> Clears Node A / CPU
	sigA_CPU_Rec, err := det.Detect(ctx, types.MetricSample{
		NodeID: "node-a", Name: "cpu_usage_percent", Value: 75.0, Timestamp: now.Add(2 * time.Second),
	})
	if err != nil || sigA_CPU_Rec != nil {
		t.Fatalf("expected node-a/cpu recovery to return nil, got sig=%v, err=%v", sigA_CPU_Rec, err)
	}

	// 8. Node B / CPU sends continuing breach reading -> MUST still be in breach (persistent, returns nil)
	sigB_CPU_StillBreach, err := det.Detect(ctx, types.MetricSample{
		NodeID: "node-b", Name: "cpu_usage_percent", Value: 95.0, Timestamp: now.Add(2 * time.Second),
	})
	if err != nil || sigB_CPU_StillBreach != nil {
		t.Fatalf("expected node-b/cpu to remain in active breach (nil), got sig=%v, err=%v", sigB_CPU_StillBreach, err)
	}

	// 9. Node A / Memory sends continuing breach reading -> MUST still be in breach (persistent, returns nil)
	sigA_Mem_StillBreach, err := det.Detect(ctx, types.MetricSample{
		NodeID: "node-a", Name: "memory_free_percent", Value: 8.0, Timestamp: now.Add(2 * time.Second),
	})
	if err != nil || sigA_Mem_StillBreach != nil {
		t.Fatalf("expected node-a/memory to remain in active breach (nil), got sig=%v, err=%v", sigA_Mem_StillBreach, err)
	}

	// 10. Node A / CPU triggers a NEW breach -> MUST emit NEW signal!
	sigA_CPU_NewBreach, err := det.Detect(ctx, types.MetricSample{
		NodeID: "node-a", Name: "cpu_usage_percent", Value: 93.0, Timestamp: now.Add(3 * time.Second),
	})
	if err != nil || sigA_CPU_NewBreach == nil {
		t.Fatalf("expected new breach on node-a/cpu after recovery, got sig=%v, err=%v", sigA_CPU_NewBreach, err)
	}
}

// 7. Determinism: Same input + same detector config => same AnomalyID and same decision across independent instances
func TestThresholdDetector_Determinism(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	cfg := ThresholdDetectorConfig{
		Version: "1.0.0",
		Rules: []ThresholdRule{
			{
				MetricName:             "cpu_usage_percent",
				UpperThreshold:         floatPtr(80.0),
				UpperRecoveryThreshold: floatPtr(70.0),
				ExpectedValue:          floatPtr(50.0),
				MaxDeviation:           floatPtr(50.0),
				DisallowNegative:       true,
			},
		},
	}

	// Create two completely independent detector instances with identical configuration
	det1, err := NewThresholdDetector(cfg)
	if err != nil {
		t.Fatalf("failed to create detector 1: %v", err)
	}
	det2, err := NewThresholdDetector(cfg)
	if err != nil {
		t.Fatalf("failed to create detector 2: %v", err)
	}

	samples := []types.MetricSample{
		{NodeID: "node-01", Name: "cpu_usage_percent", Value: 50.0, Timestamp: now},
		{NodeID: "node-01", Name: "cpu_usage_percent", Value: 85.0, Timestamp: now.Add(time.Second)},
		{NodeID: "node-01", Name: "cpu_usage_percent", Value: 88.0, Timestamp: now.Add(2 * time.Second)},
		{NodeID: "node-01", Name: "cpu_usage_percent", Value: 65.0, Timestamp: now.Add(3 * time.Second)},
		{NodeID: "node-01", Name: "cpu_usage_percent", Value: 95.0, Timestamp: now.Add(4 * time.Second)},
	}

	for i, s := range samples {
		sig1, err1 := det1.Detect(ctx, s)
		sig2, err2 := det2.Detect(ctx, s)

		if (err1 == nil) != (err2 == nil) {
			t.Fatalf("sample %d: error mismatch: %v vs %v", i, err1, err2)
		}
		if (sig1 == nil) != (sig2 == nil) {
			t.Fatalf("sample %d: signal presence mismatch: %v vs %v", i, sig1, sig2)
		}
		if sig1 != nil && sig2 != nil {
			// CRITICAL: AnomalyID must be 100% deterministic across instances without random generation
			if sig1.AnomalyID != sig2.AnomalyID {
				t.Fatalf("sample %d: AnomalyID mismatch between independent instances: %q vs %q", i, sig1.AnomalyID, sig2.AnomalyID)
			}
			if sig1.NodeID != sig2.NodeID {
				t.Errorf("sample %d: NodeID mismatch: %q vs %q", i, sig1.NodeID, sig2.NodeID)
			}
			if sig1.MetricName != sig2.MetricName {
				t.Errorf("sample %d: MetricName mismatch: %q vs %q", i, sig1.MetricName, sig2.MetricName)
			}
			if sig1.ObservedValue != sig2.ObservedValue {
				t.Errorf("sample %d: ObservedValue mismatch: %f vs %f", i, sig1.ObservedValue, sig2.ObservedValue)
			}
			if sig1.ExpectedValue != sig2.ExpectedValue {
				t.Errorf("sample %d: ExpectedValue mismatch: %f vs %f", i, sig1.ExpectedValue, sig2.ExpectedValue)
			}
			if sig1.Deviation != sig2.Deviation {
				t.Errorf("sample %d: Deviation mismatch: %f vs %f", i, sig1.Deviation, sig2.Deviation)
			}
			if sig1.AnomalyScore != sig2.AnomalyScore {
				t.Errorf("sample %d: AnomalyScore mismatch: %f vs %f", i, sig1.AnomalyScore, sig2.AnomalyScore)
			}
			if sig1.DetectionMethod != sig2.DetectionMethod {
				t.Errorf("sample %d: DetectionMethod mismatch: %q vs %q", i, sig1.DetectionMethod, sig2.DetectionMethod)
			}
			if !sig1.DetectedAt.Equal(sig2.DetectedAt) {
				t.Errorf("sample %d: DetectedAt mismatch: %v vs %v", i, sig1.DetectedAt, sig2.DetectedAt)
			}
			if sig1.DetectorVersion != sig2.DetectorVersion {
				t.Errorf("sample %d: DetectorVersion mismatch: %q vs %q", i, sig1.DetectorVersion, sig2.DetectorVersion)
			}
		}
	}
}

// 8. Invalid values (NaN, Inf, negative where prohibited)
func TestThresholdDetector_InvalidValues(t *testing.T) {
	det := newTestDetector(t)
	ctx := context.Background()
	now := time.Now().UTC()

	tests := []struct {
		name        string
		sample      types.MetricSample
		expectedErr error
	}{
		{
			name: "NaN value rejected",
			sample: types.MetricSample{
				NodeID:    "node-01",
				Name:      "cpu_usage_percent",
				Value:     math.NaN(),
				Timestamp: now,
			},
			expectedErr: types.ErrInvalidMetricValue,
		},
		{
			name: "+Inf value rejected",
			sample: types.MetricSample{
				NodeID:    "node-01",
				Name:      "cpu_usage_percent",
				Value:     math.Inf(1),
				Timestamp: now,
			},
			expectedErr: types.ErrInvalidMetricValue,
		},
		{
			name: "-Inf value rejected",
			sample: types.MetricSample{
				NodeID:    "node-01",
				Name:      "cpu_usage_percent",
				Value:     math.Inf(-1),
				Timestamp: now,
			},
			expectedErr: types.ErrInvalidMetricValue,
		},
		{
			name: "negative value rejected when DisallowNegative is true",
			sample: types.MetricSample{
				NodeID:    "node-01",
				Name:      "cpu_usage_percent",
				Value:     -5.0,
				Timestamp: now,
			},
			expectedErr: types.ErrInvalidMetricValue,
		},
		{
			name: "empty metric name rejected",
			sample: types.MetricSample{
				NodeID:    "node-01",
				Name:      "",
				Value:     50.0,
				Timestamp: now,
			},
			expectedErr: types.ErrEmptyMetricName,
		},
		{
			name: "zero timestamp rejected",
			sample: types.MetricSample{
				NodeID:    "node-01",
				Name:      "cpu_usage_percent",
				Value:     50.0,
				Timestamp: time.Time{},
			},
			expectedErr: types.ErrInvalidTimestamp,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sig, err := det.Detect(ctx, tt.sample)
			if err == nil {
				t.Fatalf("expected error %v, got nil (sig=%+v)", tt.expectedErr, sig)
			}
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("expected error wrapping %v, got %v", tt.expectedErr, err)
			}
		})
	}
}

// 9. Missing metric configuration
func TestThresholdDetector_MissingMetricConfiguration(t *testing.T) {
	det := newTestDetector(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// Metric with no rule configured should be safely ignored
	sample := types.MetricSample{
		NodeID:    "node-01",
		Name:      "unconfigured_sensor_humidity",
		Value:     99.9,
		Timestamp: now,
	}

	sig, err := det.Detect(ctx, sample)
	if err != nil {
		t.Fatalf("expected no error for unconfigured metric, got: %v", err)
	}
	if sig != nil {
		t.Fatalf("expected nil signal for unconfigured metric, got: %+v", sig)
	}
}

// 10. Invalid detector configuration
func TestThresholdDetector_InvalidConfiguration(t *testing.T) {
	tests := []struct {
		name        string
		cfg         ThresholdDetectorConfig
		expectedErr error
	}{
		{
			name: "empty rules",
			cfg: ThresholdDetectorConfig{
				Version: "1.0.0",
				Rules:   []ThresholdRule{},
			},
			expectedErr: ErrInvalidRuleConfig,
		},
		{
			name: "rule with empty metric name",
			cfg: ThresholdDetectorConfig{
				Rules: []ThresholdRule{
					{MetricName: "", UpperThreshold: floatPtr(90.0)},
				},
			},
			expectedErr: ErrInvalidRuleConfig,
		},
		{
			name: "rule with no thresholds defined",
			cfg: ThresholdDetectorConfig{
				Rules: []ThresholdRule{
					{MetricName: "cpu_usage_percent"},
				},
			},
			expectedErr: ErrInvalidRuleConfig,
		},
		{
			name: "upper recovery threshold higher than upper threshold",
			cfg: ThresholdDetectorConfig{
				Rules: []ThresholdRule{
					{
						MetricName:             "cpu_usage_percent",
						UpperThreshold:         floatPtr(90.0),
						UpperRecoveryThreshold: floatPtr(95.0), // Invalid: 95 > 90
					},
				},
			},
			expectedErr: ErrInvalidRuleConfig,
		},
		{
			name: "lower recovery threshold lower than lower threshold",
			cfg: ThresholdDetectorConfig{
				Rules: []ThresholdRule{
					{
						MetricName:             "mem_free",
						LowerThreshold:         floatPtr(20.0),
						LowerRecoveryThreshold: floatPtr(10.0), // Invalid: 10 < 20
					},
				},
			},
			expectedErr: ErrInvalidRuleConfig,
		},
		{
			name: "upper threshold less than lower threshold",
			cfg: ThresholdDetectorConfig{
				Rules: []ThresholdRule{
					{
						MetricName:     "pressure",
						UpperThreshold: floatPtr(50.0),
						LowerThreshold: floatPtr(80.0), // Invalid: upper < lower
					},
				},
			},
			expectedErr: ErrInvalidRuleConfig,
		},
		{
			name: "NaN in upper threshold",
			cfg: ThresholdDetectorConfig{
				Rules: []ThresholdRule{
					{
						MetricName:     "cpu",
						UpperThreshold: floatPtr(math.NaN()),
					},
				},
			},
			expectedErr: ErrInvalidRuleConfig,
		},
		{
			name: "negative threshold when DisallowNegative is true",
			cfg: ThresholdDetectorConfig{
				Rules: []ThresholdRule{
					{
						MetricName:       "cpu",
						UpperThreshold:   floatPtr(-5.0),
						DisallowNegative: true,
					},
				},
			},
			expectedErr: ErrInvalidRuleConfig,
		},
		{
			name: "non-positive max deviation",
			cfg: ThresholdDetectorConfig{
				Rules: []ThresholdRule{
					{
						MetricName:     "cpu",
						UpperThreshold: floatPtr(90.0),
						MaxDeviation:   floatPtr(0.0),
					},
				},
			},
			expectedErr: ErrInvalidRuleConfig,
		},
		{
			name: "duplicate metric rules",
			cfg: ThresholdDetectorConfig{
				Rules: []ThresholdRule{
					{MetricName: "cpu", UpperThreshold: floatPtr(90.0)},
					{MetricName: "cpu", UpperThreshold: floatPtr(95.0)},
				},
			},
			expectedErr: ErrDuplicateMetricRule,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewThresholdDetector(tt.cfg)
			if err == nil {
				t.Fatalf("expected error %v, got nil", tt.expectedErr)
			}
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("expected error wrapping %v, got %v", tt.expectedErr, err)
			}
		})
	}
}

// 11. Cold-start behavior
func TestThresholdDetector_ColdStartBehavior(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()

	// Cold start with nominal reading: instantaneous evaluation without lag or false alarms
	det := newTestDetector(t)
	sampleNominal := types.MetricSample{
		NodeID:    "node-cold",
		Name:      "cpu_usage_percent",
		Value:     45.0,
		Timestamp: now,
	}
	sig, err := det.Detect(ctx, sampleNominal)
	if err != nil {
		t.Fatalf("unexpected cold-start error: %v", err)
	}
	if sig != nil {
		t.Fatalf("expected nil on first nominal sample, got: %+v", sig)
	}

	// Cold start with breaching reading: instantaneous detection catches critical spike immediately
	detFresh := newTestDetector(t)
	sampleBreach := types.MetricSample{
		NodeID:    "node-cold",
		Name:      "cpu_usage_percent",
		Value:     98.0,
		Timestamp: now,
	}
	sigBreach, err := detFresh.Detect(ctx, sampleBreach)
	if err != nil {
		t.Fatalf("unexpected cold-start breach error: %v", err)
	}
	if sigBreach == nil {
		t.Fatal("expected immediate anomaly detection on cold-start spike (W=1 safety ceiling), got nil")
	}
	if sigBreach.ObservedValue != 98.0 {
		t.Errorf("expected ObservedValue 98.0, got %f", sigBreach.ObservedValue)
	}
}

// 12. DetectBatch with mixed telemetry and CorrelationID propagation
func TestThresholdDetector_DetectBatch(t *testing.T) {
	det := newTestDetector(t)
	ctx := context.Background()
	now := time.Now().UTC()

	batch := &types.TelemetryBatch{
		BatchID:        "batch-mixed-uuid-99",
		NodeID:         "node-01",
		SequenceNumber: 10,
		CollectedAt:    now,
		Metrics: []types.MetricSample{
			{NodeID: "node-01", Name: "cpu_usage_percent", Value: 96.0, Timestamp: now},           // Breached (>90)
			{NodeID: "node-01", Name: "memory_free_percent", Value: 35.0, Timestamp: now},         // Normal (>15)
			{NodeID: "node-01", Name: "disk_usage_percent", Value: 98.0, Timestamp: now},          // Breached (>95)
			{NodeID: "node-01", Name: "unconfigured_metric", Value: 1000.0, Timestamp: now},       // Unconfigured
			{NodeID: "node-01", Name: "ambient_temperature_celsius", Value: 22.0, Timestamp: now}, // Normal
		},
	}

	signals, err := det.DetectBatch(ctx, batch)
	if err != nil {
		t.Fatalf("unexpected error in DetectBatch: %v", err)
	}

	if len(signals) != 2 {
		t.Fatalf("expected exactly 2 anomaly signals, got %d", len(signals))
	}

	// Verify correlation ID propagation
	for _, s := range signals {
		if s.CorrelationID != "batch-mixed-uuid-99" {
			t.Errorf("expected CorrelationID 'batch-mixed-uuid-99', got %q", s.CorrelationID)
		}
		if s.NodeID != "node-01" {
			t.Errorf("expected NodeID 'node-01', got %q", s.NodeID)
		}
		if err := s.Validate(); err != nil {
			t.Errorf("batch signal failed validation: %v", err)
		}
	}

	// Verify nil batch returns ErrNilBatch
	_, err = det.DetectBatch(ctx, nil)
	if !errors.Is(err, ErrNilBatch) {
		t.Errorf("expected ErrNilBatch on nil batch, got %v", err)
	}

	// Verify invalid batch returns error
	invalidBatch := &types.TelemetryBatch{
		BatchID: "", // empty BatchID is invalid
	}
	_, err = det.DetectBatch(ctx, invalidBatch)
	if err == nil {
		t.Error("expected error on invalid batch, got nil")
	}
}

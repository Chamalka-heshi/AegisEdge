package types

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

// ============================================================================
// NodeRegistration Tests
// ============================================================================

func TestNodeRegistration_Validate(t *testing.T) {
	now := time.Now().UTC()

	tests := []struct {
		name    string
		node    NodeRegistration
		wantErr error
	}{
		{
			name: "valid node registration with metadata",
			node: NodeRegistration{
				NodeID:       "edge-node-01",
				Hostname:     "gateway-alpha",
				OS:           "linux",
				Architecture: "arm64",
				IPAddress:    "192.168.1.100",
				AgentVersion: "0.1.0",
				Capabilities: []string{"simulated_actuator", "system_metrics"},
				Labels:       map[string]string{"env": "edge-prod", "rack": "r12"},
				RegisteredAt: now,
			},
			wantErr: nil,
		},
		{
			name: "empty node_id",
			node: NodeRegistration{
				NodeID:       "",
				Hostname:     "gateway-alpha",
				RegisteredAt: now,
			},
			wantErr: ErrEmptyNodeID,
		},
		{
			name: "whitespace node_id",
			node: NodeRegistration{
				NodeID:       "   ",
				Hostname:     "gateway-alpha",
				RegisteredAt: now,
			},
			wantErr: ErrEmptyNodeID,
		},
		{
			name: "empty hostname",
			node: NodeRegistration{
				NodeID:       "edge-node-01",
				Hostname:     "",
				RegisteredAt: now,
			},
			wantErr: ErrEmptyHostname,
		},
		{
			name: "zero registered_at",
			node: NodeRegistration{
				NodeID:       "edge-node-01",
				Hostname:     "gateway-alpha",
				RegisteredAt: time.Time{},
			},
			wantErr: ErrInvalidTimestamp,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.node.Validate()
			if err != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// ============================================================================
// Heartbeat Tests
// ============================================================================

func TestHeartbeat_Validate(t *testing.T) {
	now := time.Now().UTC()

	tests := []struct {
		name      string
		heartbeat Heartbeat
		wantErr   error
	}{
		{
			name: "valid heartbeat",
			heartbeat: Heartbeat{
				NodeID:         "edge-node-01",
				SequenceNumber: 104,
				Timestamp:      now,
				Status:         NodeStatusHealthy,
				MetricsSummary: map[string]float64{"cpu_pct": 24.5, "mem_pct": 42.1},
			},
			wantErr: nil,
		},
		{
			name: "empty node_id",
			heartbeat: Heartbeat{
				NodeID:         "",
				SequenceNumber: 0,
				Timestamp:      now,
				Status:         NodeStatusHealthy,
			},
			wantErr: ErrEmptyNodeID,
		},
		{
			name: "negative sequence number",
			heartbeat: Heartbeat{
				NodeID:         "edge-node-01",
				SequenceNumber: -1,
				Timestamp:      now,
				Status:         NodeStatusHealthy,
			},
			wantErr: ErrInvalidSequence,
		},
		{
			name: "zero timestamp",
			heartbeat: Heartbeat{
				NodeID:         "edge-node-01",
				SequenceNumber: 0,
				Timestamp:      time.Time{},
				Status:         NodeStatusHealthy,
			},
			wantErr: ErrInvalidTimestamp,
		},
		{
			name: "invalid node status",
			heartbeat: Heartbeat{
				NodeID:         "edge-node-01",
				SequenceNumber: 0,
				Timestamp:      now,
				Status:         NodeStatus("UNKNOWN"),
			},
			wantErr: ErrInvalidNodeStatus,
		},
		{
			name: "large sequence number boundary",
			heartbeat: Heartbeat{
				NodeID:         "edge-node-01",
				SequenceNumber: math.MaxInt64,
				Timestamp:      now,
				Status:         NodeStatusHealthy,
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.heartbeat.Validate()
			if err != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// ============================================================================
// MetricSample & TelemetryBatch Tests
// ============================================================================

func TestMetricSample_Validate(t *testing.T) {
	now := time.Now().UTC()

	tests := []struct {
		name    string
		sample  MetricSample
		wantErr error
	}{
		{
			name: "valid sample",
			sample: MetricSample{
				SampleID:  "sample-001",
				NodeID:    "edge-node-01",
				Name:      "cpu_temp_celsius",
				Value:     58.3,
				Unit:      "celsius",
				Labels:    map[string]string{"core": "0"},
				Timestamp: now,
			},
			wantErr: nil,
		},
		{
			name: "empty name",
			sample: MetricSample{
				Name:      "",
				Value:     12.0,
				Timestamp: now,
			},
			wantErr: ErrEmptyMetricName,
		},
		{
			name: "zero timestamp",
			sample: MetricSample{
				Name:      "cpu_pct",
				Value:     12.0,
				Timestamp: time.Time{},
			},
			wantErr: ErrInvalidTimestamp,
		},
		{
			name: "NaN value rejected",
			sample: MetricSample{
				Name:      "cpu_pct",
				Value:     math.NaN(),
				Timestamp: now,
			},
			wantErr: ErrInvalidMetricValue,
		},
		{
			name: "+Inf value rejected",
			sample: MetricSample{
				Name:      "cpu_pct",
				Value:     math.Inf(1),
				Timestamp: now,
			},
			wantErr: ErrInvalidMetricValue,
		},
		{
			name: "-Inf value rejected",
			sample: MetricSample{
				Name:      "cpu_pct",
				Value:     math.Inf(-1),
				Timestamp: now,
			},
			wantErr: ErrInvalidMetricValue,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.sample.Validate()
			if err != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTelemetryBatch_Validate(t *testing.T) {
	now := time.Now().UTC()

	validMetrics := []MetricSample{
		{Name: "cpu_usage_pct", Value: 85.5, Unit: "percent", Timestamp: now},
		{Name: "memory_usage_bytes", Value: 1024000, Unit: "bytes", Timestamp: now},
	}

	tests := []struct {
		name    string
		batch   TelemetryBatch
		wantErr error
	}{
		{
			name: "valid telemetry batch with retry metadata",
			batch: TelemetryBatch{
				BatchID:        "batch-uuid-001",
				NodeID:         "edge-node-01",
				SequenceNumber: 1,
				CollectedAt:    now,
				SentAt:         &now,
				Attempt:        2,
				Metrics:        validMetrics,
			},
			wantErr: nil,
		},
		{
			name: "empty batch_id",
			batch: TelemetryBatch{
				BatchID:        "",
				NodeID:         "edge-node-01",
				SequenceNumber: 1,
				CollectedAt:    now,
				Metrics:        validMetrics,
			},
			wantErr: ErrEmptyBatchID,
		},
		{
			name: "empty node_id",
			batch: TelemetryBatch{
				BatchID:        "batch-uuid-001",
				NodeID:         "",
				SequenceNumber: 1,
				CollectedAt:    now,
				Metrics:        validMetrics,
			},
			wantErr: ErrEmptyNodeID,
		},
		{
			name: "negative sequence number",
			batch: TelemetryBatch{
				BatchID:        "batch-001",
				NodeID:         "edge-node-01",
				SequenceNumber: -1,
				CollectedAt:    now,
				Metrics:        validMetrics,
			},
			wantErr: ErrInvalidSequence,
		},
		{
			name: "zero collected_at",
			batch: TelemetryBatch{
				BatchID:        "batch-001",
				NodeID:         "edge-node-01",
				SequenceNumber: 0,
				CollectedAt:    time.Time{},
				Metrics:        validMetrics,
			},
			wantErr: ErrInvalidTimestamp,
		},
		{
			name: "empty metrics slice",
			batch: TelemetryBatch{
				BatchID:        "batch-001",
				NodeID:         "edge-node-01",
				SequenceNumber: 0,
				CollectedAt:    now,
				Metrics:        nil,
			},
			wantErr: ErrEmptyMetrics,
		},
		{
			name: "invalid sample inside batch",
			batch: TelemetryBatch{
				BatchID:        "batch-001",
				NodeID:         "edge-node-01",
				SequenceNumber: 0,
				CollectedAt:    now,
				Metrics: []MetricSample{
					{Name: "", Value: 10, Unit: "pct", Timestamp: now},
				},
			},
			wantErr: ErrEmptyMetricName,
		},
		{
			name: "mismatched sample node_id",
			batch: TelemetryBatch{
				BatchID:        "batch-001",
				NodeID:         "edge-node-01",
				SequenceNumber: 0,
				CollectedAt:    now,
				Metrics: []MetricSample{
					{NodeID: "edge-node-FOREIGN", Name: "cpu", Value: 10, Timestamp: now},
				},
			},
			wantErr: ErrMismatchedNodeID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.batch.Validate()
			if err != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// ============================================================================
// MitigationAction Tests (Allowlist & Safety)
// ============================================================================

func TestMitigationAction_Validate(t *testing.T) {
	now := time.Now().UTC()
	completed := now.Add(500 * time.Millisecond)
	prior := now.Add(-1 * time.Minute)

	tests := []struct {
		name    string
		action  MitigationAction
		wantErr error
	}{
		{
			name: "valid simulated throttle action",
			action: MitigationAction{
				ActionID:    "act-001",
				IncidentID:  "inc-001",
				ActionType:  ActionSimulatedThrottle,
				Target:      "workload-worker",
				Status:      MitigationStatusExecuted,
				Message:     "Throttled CPU quota to 50%",
				TriggeredAt: now,
				CompletedAt: &completed,
			},
			wantErr: nil,
		},
		{
			name: "valid unknown reconciliation required status",
			action: MitigationAction{
				ActionID:    "act-002",
				IncidentID:  "inc-001",
				ActionType:  ActionSimulatedRestart,
				Target:      "workload-worker",
				Status:      MitigationStatusUnknownReconciliationRequired,
				Message:     "Execution unconfirmed after restart",
				TriggeredAt: now,
			},
			wantErr: nil,
		},
		{
			name: "empty action_id",
			action: MitigationAction{
				ActionID:    "",
				IncidentID:  "inc-001",
				ActionType:  ActionSimulatedRestart,
				Target:      "workload-worker",
				Status:      MitigationStatusPending,
				TriggeredAt: now,
			},
			wantErr: ErrEmptyActionID,
		},
		{
			name: "empty incident_id",
			action: MitigationAction{
				ActionID:    "act-001",
				IncidentID:  "",
				ActionType:  ActionSimulatedRestart,
				Target:      "workload-worker",
				Status:      MitigationStatusPending,
				TriggeredAt: now,
			},
			wantErr: ErrEmptyIncidentID,
		},
		{
			name: "unauthorized action type (arbitrary shell command rejected)",
			action: MitigationAction{
				ActionID:    "act-001",
				IncidentID:  "inc-001",
				ActionType:  MitigationActionType("rm -rf /"),
				Target:      "system",
				Status:      MitigationStatusPending,
				TriggeredAt: now,
			},
			wantErr: ErrInvalidActionType,
		},
		{
			name: "unauthorized action type (kill arbitrary process rejected)",
			action: MitigationAction{
				ActionID:    "act-001",
				IncidentID:  "inc-001",
				ActionType:  MitigationActionType("KILL_PROCESS_PID"),
				Target:      "system",
				Status:      MitigationStatusPending,
				TriggeredAt: now,
			},
			wantErr: ErrInvalidActionType,
		},
		{
			name: "invalid status",
			action: MitigationAction{
				ActionID:    "act-001",
				IncidentID:  "inc-001",
				ActionType:  ActionSimulatedAlert,
				Target:      "slack-channel",
				Status:      MitigationStatus("INVALID_STATUS"),
				TriggeredAt: now,
			},
			wantErr: ErrInvalidActionStatus,
		},
		{
			name: "zero triggered_at",
			action: MitigationAction{
				ActionID:    "act-001",
				IncidentID:  "inc-001",
				ActionType:  ActionSimulatedAlert,
				Target:      "slack",
				Status:      MitigationStatusPending,
				TriggeredAt: time.Time{},
			},
			wantErr: ErrInvalidTimestamp,
		},
		{
			name: "completed_at prior to triggered_at",
			action: MitigationAction{
				ActionID:    "act-001",
				IncidentID:  "inc-001",
				ActionType:  ActionSimulatedRestart,
				Target:      "service-a",
				Status:      MitigationStatusExecuted,
				TriggeredAt: now,
				CompletedAt: &prior,
			},
			wantErr: ErrInvalidCompletedTime,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.action.Validate()
			if err != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// ============================================================================
// Incident Validation & Relationship Tests
// ============================================================================

func TestIncident_Validate(t *testing.T) {
	now := time.Now().UTC()
	resolved := now.Add(2 * time.Minute)
	invalidResolved := now.Add(-10 * time.Second)

	tests := []struct {
		name     string
		incident Incident
		wantErr  error
	}{
		{
			name: "valid incident with matching nested mitigation",
			incident: Incident{
				IncidentID:    "inc-001",
				NodeID:        "edge-node-01",
				RuleName:      "HighMemoryRule",
				Severity:      SeverityCritical,
				Status:        StatusMitigating,
				Description:   "Memory exceeded 95% threshold for 3 consecutive intervals",
				TriggerMetric: "mem_usage_pct",
				TriggerValue:  96.4,
				Threshold:     90.0,
				TriggeredAt:   now,
				UpdatedAt:     now,
				Mitigations: []MitigationAction{
					{
						ActionID:    "act-001",
						IncidentID:  "inc-001",
						ActionType:  ActionSimulatedRestart,
						Target:      "workload-worker",
						Status:      MitigationStatusExecuted,
						TriggeredAt: now,
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "empty incident_id",
			incident: Incident{
				IncidentID:  "",
				NodeID:      "edge-node-01",
				RuleName:    "Rule",
				Severity:    SeverityLow,
				Status:      StatusNormal,
				TriggeredAt: now,
			},
			wantErr: ErrEmptyIncidentID,
		},
		{
			name: "empty node_id",
			incident: Incident{
				IncidentID:  "inc-001",
				NodeID:      "",
				RuleName:    "Rule",
				Severity:    SeverityLow,
				Status:      StatusNormal,
				TriggeredAt: now,
			},
			wantErr: ErrEmptyNodeID,
		},
		{
			name: "empty rule_name",
			incident: Incident{
				IncidentID:  "inc-001",
				NodeID:      "edge-node-01",
				RuleName:    "",
				Severity:    SeverityLow,
				Status:      StatusNormal,
				TriggeredAt: now,
			},
			wantErr: ErrEmptyRuleName,
		},
		{
			name: "invalid severity",
			incident: Incident{
				IncidentID:  "inc-001",
				NodeID:      "edge-node-01",
				RuleName:    "Rule",
				Severity:    IncidentSeverity("DISASTER"),
				Status:      StatusAnomalyDetected,
				TriggeredAt: now,
			},
			wantErr: ErrInvalidSeverity,
		},
		{
			name: "invalid status",
			incident: Incident{
				IncidentID:  "inc-001",
				NodeID:      "edge-node-01",
				RuleName:    "Rule",
				Severity:    SeverityHigh,
				Status:      IncidentStatus("UNKNOWN"),
				TriggeredAt: now,
			},
			wantErr: ErrInvalidStatus,
		},
		{
			name: "resolved_at prior to triggered_at",
			incident: Incident{
				IncidentID:  "inc-001",
				NodeID:      "edge-node-01",
				RuleName:    "Rule",
				Severity:    SeverityMedium,
				Status:      StatusRecovered,
				TriggeredAt: now,
				ResolvedAt:  &invalidResolved,
			},
			wantErr: ErrInvalidResolvedTimestamp,
		},
		{
			name: "nested mitigation with mismatched incident_id",
			incident: Incident{
				IncidentID:  "inc-001",
				NodeID:      "edge-node-01",
				RuleName:    "Rule",
				Severity:    SeverityHigh,
				Status:      StatusMitigating,
				TriggeredAt: now,
				Mitigations: []MitigationAction{
					{
						ActionID:    "act-001",
						IncidentID:  "inc-OTHER-999", // mismatch!
						ActionType:  ActionSimulatedRestart,
						Target:      "proc",
						Status:      MitigationStatusExecuted,
						TriggeredAt: now,
					},
				},
			},
			wantErr: ErrMismatchedIncidentID,
		},
		{
			name: "valid resolved incident",
			incident: Incident{
				IncidentID:  "inc-001",
				NodeID:      "edge-node-01",
				RuleName:    "Rule",
				Severity:    SeverityMedium,
				Status:      StatusRecovered,
				TriggeredAt: now,
				ResolvedAt:  &resolved,
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.incident.Validate()
			if err != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// ============================================================================
// Deterministic State Machine Transition Tests
// ============================================================================

func TestIncidentStatus_CanTransitionTo(t *testing.T) {
	validTransitions := []struct {
		from IncidentStatus
		to   IncidentStatus
	}{
		{StatusNormal, StatusAnomalyDetected},
		{StatusAnomalyDetected, StatusMitigating},
		{StatusAnomalyDetected, StatusEscalated},
		{StatusMitigating, StatusRecovered},
		{StatusMitigating, StatusEscalated},
		{StatusRecovered, StatusNormal},
		{StatusEscalated, StatusRecovered},
		{StatusEscalated, StatusNormal},
	}

	for _, vt := range validTransitions {
		if !vt.from.CanTransitionTo(vt.to) {
			t.Errorf("expected valid transition from %s to %s", vt.from, vt.to)
		}
	}

	invalidTransitions := []struct {
		from IncidentStatus
		to   IncidentStatus
	}{
		{StatusNormal, StatusMitigating},
		{StatusNormal, StatusRecovered},
		{StatusNormal, StatusEscalated},
		{StatusAnomalyDetected, StatusNormal},
		{StatusAnomalyDetected, StatusRecovered},
		{StatusMitigating, StatusNormal},
		{StatusMitigating, StatusAnomalyDetected},
		{StatusRecovered, StatusMitigating},
		{StatusRecovered, StatusEscalated},
		{StatusRecovered, StatusAnomalyDetected},
	}

	for _, it := range invalidTransitions {
		if it.from.CanTransitionTo(it.to) {
			t.Errorf("expected invalid transition from %s to %s to be rejected", it.from, it.to)
		}
	}
}

func TestIncident_TransitionTo(t *testing.T) {
	t0 := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	t1 := t0.Add(10 * time.Second)
	t2 := t1.Add(30 * time.Second)
	t3 := t2.Add(60 * time.Second)

	inc := Incident{
		IncidentID:  "inc-fsm-001",
		NodeID:      "node-01",
		RuleName:    "HighLoad",
		Severity:    SeverityHigh,
		Status:      StatusNormal,
		TriggeredAt: t0,
		UpdatedAt:   t0,
	}

	// 1. NORMAL -> ANOMALY_DETECTED
	if err := inc.TransitionTo(StatusAnomalyDetected, t1); err != nil {
		t.Fatalf("Transition to ANOMALY_DETECTED failed: %v", err)
	}
	if inc.Status != StatusAnomalyDetected || !inc.UpdatedAt.Equal(t1) {
		t.Errorf("State mismatch: got status %s, updated %v", inc.Status, inc.UpdatedAt)
	}

	// 2. Invalid jump: ANOMALY_DETECTED -> NORMAL should fail
	if err := inc.TransitionTo(StatusNormal, t2); err == nil {
		t.Fatal("Expected error on invalid transition ANOMALY_DETECTED -> NORMAL, got nil")
	}

	// 3. ANOMALY_DETECTED -> MITIGATING
	if err := inc.TransitionTo(StatusMitigating, t2); err != nil {
		t.Fatalf("Transition to MITIGATING failed: %v", err)
	}

	// 4. MITIGATING -> RECOVERED
	if err := inc.TransitionTo(StatusRecovered, t3); err != nil {
		t.Fatalf("Transition to RECOVERED failed: %v", err)
	}
	if inc.ResolvedAt == nil || !inc.ResolvedAt.Equal(t3) {
		t.Errorf("ResolvedAt not set correctly: got %v, want %v", inc.ResolvedAt, t3)
	}

	// 5. RECOVERED -> NORMAL (incident reset)
	t4 := t3.Add(5 * time.Minute)
	if err := inc.TransitionTo(StatusNormal, t4); err != nil {
		t.Fatalf("Transition to NORMAL failed: %v", err)
	}
	if inc.Status != StatusNormal {
		t.Errorf("Final state mismatch: got %s, want NORMAL", inc.Status)
	}
}

// ============================================================================
// JSON Serialization, Deserialization, and Idempotency Tests
// ============================================================================

func TestTelemetryBatch_JSONRoundTrip(t *testing.T) {
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	sampleID := "sample-uuid-42"

	orig := TelemetryBatch{
		BatchID:        "batch-uuid-1234-abcd",
		NodeID:         "node-alpha-77",
		SequenceNumber: 42000000000, // Large sequence number to test int64 JSON handling
		CollectedAt:    now,
		SentAt:         &now,
		Attempt:        3,
		Metrics: []MetricSample{
			{
				SampleID:  sampleID,
				NodeID:    "node-alpha-77",
				Name:      "cpu_load_pct",
				Value:     72.4,
				Unit:      "percent",
				Labels:    map[string]string{"core": "all"},
				Timestamp: now,
			},
			{
				Name:      "disk_free_bytes",
				Value:     10737418240,
				Unit:      "bytes",
				Timestamp: now,
			},
		},
	}

	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	// Verify JSON keys are explicit and snake_case
	jsonStr := string(data)
	expectedKeys := []string{
		`"batch_id"`, `"node_id"`, `"sequence_number"`,
		`"collected_at"`, `"sent_at"`, `"attempt"`, `"metrics"`,
	}
	for _, k := range expectedKeys {
		if !strings.Contains(jsonStr, k) {
			t.Errorf("Expected JSON key %s not found in output: %s", k, jsonStr)
		}
	}

	var parsed TelemetryBatch
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	// Verify stable IDs and numbers survive intact
	if parsed.BatchID != orig.BatchID {
		t.Errorf("BatchID corrupted: got %s, want %s", parsed.BatchID, orig.BatchID)
	}
	if parsed.NodeID != orig.NodeID {
		t.Errorf("NodeID corrupted: got %s, want %s", parsed.NodeID, orig.NodeID)
	}
	if parsed.SequenceNumber != orig.SequenceNumber {
		t.Errorf("SequenceNumber corrupted: got %d, want %d", parsed.SequenceNumber, orig.SequenceNumber)
	}
	if len(parsed.Metrics) != len(orig.Metrics) {
		t.Fatalf("Metrics count mismatch: got %d, want %d", len(parsed.Metrics), len(orig.Metrics))
	}
	if parsed.Metrics[0].SampleID != orig.Metrics[0].SampleID {
		t.Errorf("SampleID corrupted: got %s, want %s", parsed.Metrics[0].SampleID, orig.Metrics[0].SampleID)
	}
}

func TestIncident_JSONRoundTrip(t *testing.T) {
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	resolved := now.Add(5 * time.Minute)

	orig := Incident{
		IncidentID:    "inc-uuid-9999",
		NodeID:        "edge-node-01",
		RuleName:      "DiskSpaceExhaustion",
		Severity:      SeverityCritical,
		Status:        StatusRecovered,
		Description:   "Root partition free space dropped below 5%",
		TriggerMetric: "disk_free_pct",
		TriggerValue:  4.2,
		Threshold:     5.0,
		Evidence:      map[string]string{"path": "/var/log", "delta": "-2GB"},
		TriggeredAt:   now,
		UpdatedAt:     resolved,
		ResolvedAt:    &resolved,
		Mitigations: []MitigationAction{
			{
				ActionID:    "act-uuid-1111",
				IncidentID:  "inc-uuid-9999",
				ActionType:  ActionSimulatedThrottle,
				Target:      "log-collector",
				Status:      MitigationStatusExecuted,
				Message:     "Throttled logging buffer",
				TriggeredAt: now,
				CompletedAt: &resolved,
			},
		},
	}

	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var parsed Incident
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if parsed.IncidentID != orig.IncidentID {
		t.Errorf("IncidentID corrupted: got %s, want %s", parsed.IncidentID, orig.IncidentID)
	}
	if parsed.Severity != orig.Severity {
		t.Errorf("Severity corrupted: got %s, want %s", parsed.Severity, orig.Severity)
	}
	if parsed.Status != orig.Status {
		t.Errorf("Status corrupted: got %s, want %s", parsed.Status, orig.Status)
	}
	if len(parsed.Mitigations) != 1 {
		t.Fatalf("Mitigations count mismatch: got %d, want 1", len(parsed.Mitigations))
	}
	if parsed.Mitigations[0].ActionType != ActionSimulatedThrottle {
		t.Errorf("ActionType corrupted: got %s, want %s", parsed.Mitigations[0].ActionType, ActionSimulatedThrottle)
	}
}

// TestDuplicateDeliveryIdempotency verifies that repeated serialization/deserialization
// preserves stable IDs without modification (essential for idempotent ingestion).
func TestDuplicateDeliveryIdempotency(t *testing.T) {
	now := time.Now().UTC()
	incidentID := "stable-edge-uuid-555"

	inc := Incident{
		IncidentID:  incidentID,
		NodeID:      "node-1",
		RuleName:    "CpuSpike",
		Severity:    SeverityHigh,
		Status:      StatusAnomalyDetected,
		TriggeredAt: now,
	}

	// Simulate first delivery attempt
	data1, err := json.Marshal(inc)
	if err != nil {
		t.Fatalf("marshal 1 failed: %v", err)
	}

	// Simulate retry delivery attempt (e.g. after network timeout)
	data2, err := json.Marshal(inc)
	if err != nil {
		t.Fatalf("marshal 2 failed: %v", err)
	}

	var parsed1, parsed2 Incident
	if err := json.Unmarshal(data1, &parsed1); err != nil {
		t.Fatalf("unmarshal 1 failed: %v", err)
	}
	if err := json.Unmarshal(data2, &parsed2); err != nil {
		t.Fatalf("unmarshal 2 failed: %v", err)
	}

	if parsed1.IncidentID != incidentID || parsed2.IncidentID != incidentID {
		t.Fatalf("IncidentID mutated across attempts: %s vs %s", parsed1.IncidentID, parsed2.IncidentID)
	}
	if parsed1.IncidentID != parsed2.IncidentID {
		t.Errorf("Identical event payloads produced different IDs on retry")
	}
}

// ============================================================================
// ApprovalStatus Tests
// ============================================================================

func TestApprovalStatus_LifecycleAndTransitions(t *testing.T) {
	validStatuses := []ApprovalStatus{
		ApprovalStatusPending,
		ApprovalStatusApproved,
		ApprovalStatusConsumed,
		ApprovalStatusRejected,
		ApprovalStatusExpired,
		ApprovalStatusCancelled,
	}

	for _, s := range validStatuses {
		if !s.IsValid() {
			t.Errorf("expected status %q to be valid", s)
		}
	}

	if ApprovalStatus("INVALID").IsValid() {
		t.Errorf("expected INVALID status to be rejected")
	}

	// Terminal states
	terminalStatuses := []ApprovalStatus{
		ApprovalStatusConsumed,
		ApprovalStatusRejected,
		ApprovalStatusExpired,
		ApprovalStatusCancelled,
	}
	for _, s := range terminalStatuses {
		if !s.IsTerminal() {
			t.Errorf("expected status %q to be terminal", s)
		}
	}
	if ApprovalStatusPending.IsTerminal() {
		t.Errorf("PENDING should not be terminal")
	}
	if ApprovalStatusApproved.IsTerminal() {
		t.Errorf("APPROVED should not be terminal")
	}

	// Transitions from PENDING
	if !ApprovalStatusPending.CanTransitionTo(ApprovalStatusApproved) {
		t.Errorf("PENDING -> APPROVED should be permitted")
	}
	if !ApprovalStatusPending.CanTransitionTo(ApprovalStatusRejected) {
		t.Errorf("PENDING -> REJECTED should be permitted")
	}
	if !ApprovalStatusPending.CanTransitionTo(ApprovalStatusExpired) {
		t.Errorf("PENDING -> EXPIRED should be permitted")
	}
	if !ApprovalStatusPending.CanTransitionTo(ApprovalStatusCancelled) {
		t.Errorf("PENDING -> CANCELLED should be permitted")
	}
	if ApprovalStatusPending.CanTransitionTo(ApprovalStatusConsumed) {
		t.Errorf("PENDING -> CONSUMED directly should be prohibited")
	}

	// Transitions from APPROVED
	if !ApprovalStatusApproved.CanTransitionTo(ApprovalStatusConsumed) {
		t.Errorf("APPROVED -> CONSUMED should be permitted")
	}
	if !ApprovalStatusApproved.CanTransitionTo(ApprovalStatusExpired) {
		t.Errorf("APPROVED -> EXPIRED should be permitted")
	}
	if ApprovalStatusApproved.CanTransitionTo(ApprovalStatusPending) {
		t.Errorf("APPROVED -> PENDING should be prohibited")
	}
	if ApprovalStatusApproved.CanTransitionTo(ApprovalStatusRejected) {
		t.Errorf("APPROVED -> REJECTED should be prohibited")
	}
	if ApprovalStatusApproved.CanTransitionTo(ApprovalStatusCancelled) {
		t.Errorf("APPROVED -> CANCELLED should be prohibited")
	}

	// Terminal states cannot transition further
	for _, term := range terminalStatuses {
		for _, next := range validStatuses {
			if term != next && term.CanTransitionTo(next) {
				t.Errorf("terminal status %q should not be able to transition to %q", term, next)
			}
		}
	}
}

// ============================================================================
// VerificationStatus Lifecycle Tests
// ============================================================================

func TestVerificationStatus_LifecycleAndTransitions(t *testing.T) {
	validStatuses := []VerificationStatus{
		VerificationStatusPending,
		VerificationStatusRecovered,
		VerificationStatusNotRecovered,
		VerificationStatusTimedOut,
		VerificationStatusCancelled,
	}

	for _, s := range validStatuses {
		if !s.IsValid() {
			t.Errorf("expected status %q to be valid", s)
		}
	}

	invalid := VerificationStatus("UNKNOWN_STATUS")
	if invalid.IsValid() {
		t.Errorf("expected invalid status to return false")
	}

	terminalStatuses := []VerificationStatus{
		VerificationStatusRecovered,
		VerificationStatusNotRecovered,
		VerificationStatusTimedOut,
		VerificationStatusCancelled,
	}
	for _, s := range terminalStatuses {
		if !s.IsTerminal() {
			t.Errorf("expected status %q to be terminal", s)
		}
	}
	if VerificationStatusPending.IsTerminal() {
		t.Errorf("PENDING should not be terminal")
	}

	// Transitions from PENDING
	allowedFromPending := []VerificationStatus{
		VerificationStatusRecovered,
		VerificationStatusNotRecovered,
		VerificationStatusTimedOut,
		VerificationStatusCancelled,
	}
	for _, next := range allowedFromPending {
		if !VerificationStatusPending.CanTransitionTo(next) {
			t.Errorf("PENDING -> %s should be permitted", next)
		}
	}

	// Self-transition is permitted
	if !VerificationStatusPending.CanTransitionTo(VerificationStatusPending) {
		t.Errorf("PENDING -> PENDING should be permitted")
	}

	// Terminal states cannot transition further
	for _, term := range terminalStatuses {
		for _, next := range validStatuses {
			if term != next && term.CanTransitionTo(next) {
				t.Errorf("terminal status %q should not be able to transition to %q", term, next)
			}
		}
	}
}

// Regression Test: Proves that the original canonical Incident FSM remains strictly unchanged.
// The established canonical Incident FSM is:
//
//	NORMAL -> ANOMALY_DETECTED -> MITIGATING | ESCALATED -> RECOVERED -> NORMAL
//
// Phase 6.6 does NOT introduce CONFIRMED, CLOSED, or alter these invariants.
func TestIncidentFSM_OriginalCanonicalLifecycleRemainsUnchanged(t *testing.T) {
	// 1. Verify exactly the 5 canonical IncidentStatus values exist and are valid
	canonicalStatuses := []IncidentStatus{
		StatusNormal,
		StatusAnomalyDetected,
		StatusMitigating,
		StatusRecovered,
		StatusEscalated,
	}
	for _, s := range canonicalStatuses {
		if !s.IsValid() {
			t.Errorf("expected canonical status %q to be valid", s)
		}
	}

	// Prohibit non-canonical statuses like CONFIRMED or CLOSED
	nonCanonical := []IncidentStatus{"CONFIRMED", "CLOSED", "RESOLVED", "TRIAGED"}
	for _, nc := range nonCanonical {
		if nc.IsValid() {
			t.Errorf("non-canonical status %q must NOT be recognized as valid", nc)
		}
	}

	// 2. Test exact canonical transitions
	// NORMAL -> ANOMALY_DETECTED only
	if !StatusNormal.CanTransitionTo(StatusAnomalyDetected) {
		t.Errorf("NORMAL -> ANOMALY_DETECTED must be allowed")
	}
	if StatusNormal.CanTransitionTo(StatusMitigating) || StatusNormal.CanTransitionTo(StatusRecovered) {
		t.Errorf("NORMAL cannot directly jump to MITIGATING or RECOVERED")
	}

	// ANOMALY_DETECTED -> MITIGATING | ESCALATED only (NEVER directly to RECOVERED)
	if !StatusAnomalyDetected.CanTransitionTo(StatusMitigating) {
		t.Errorf("ANOMALY_DETECTED -> MITIGATING must be allowed")
	}
	if !StatusAnomalyDetected.CanTransitionTo(StatusEscalated) {
		t.Errorf("ANOMALY_DETECTED -> ESCALATED must be allowed")
	}
	if StatusAnomalyDetected.CanTransitionTo(StatusRecovered) {
		t.Errorf("ANOMALY_DETECTED must NEVER directly transition to RECOVERED")
	}

	// MITIGATING -> RECOVERED | ESCALATED
	if !StatusMitigating.CanTransitionTo(StatusRecovered) {
		t.Errorf("MITIGATING -> RECOVERED must be allowed")
	}
	if !StatusMitigating.CanTransitionTo(StatusEscalated) {
		t.Errorf("MITIGATING -> ESCALATED must be allowed")
	}
	if StatusMitigating.CanTransitionTo(StatusNormal) {
		t.Errorf("MITIGATING cannot transition directly to NORMAL")
	}

	// RECOVERED -> NORMAL
	if !StatusRecovered.CanTransitionTo(StatusNormal) {
		t.Errorf("RECOVERED -> NORMAL must be allowed")
	}
	if StatusRecovered.CanTransitionTo(StatusMitigating) {
		t.Errorf("RECOVERED cannot transition directly to MITIGATING")
	}

	// ESCALATED -> RECOVERED | NORMAL
	if !StatusEscalated.CanTransitionTo(StatusRecovered) {
		t.Errorf("ESCALATED -> RECOVERED must be allowed")
	}
	if !StatusEscalated.CanTransitionTo(StatusNormal) {
		t.Errorf("ESCALATED -> NORMAL must be allowed")
	}
}

// ============================================================================
// Phase 6.7 Escalation & Circuit Breaker Tests
// ============================================================================

func TestFailureClassification_IsValid(t *testing.T) {
	valid := []FailureClassification{
		FailureMitigationFailed,
		FailureVerificationTimedOut,
		FailureVerificationRejected,
		FailureUnknownReconciliationRequired,
		FailureCooldownActive,
		FailureRetryBudgetExhausted,
		FailureCircuitBreakerOpen,
	}

	for _, fc := range valid {
		if !fc.IsValid() {
			t.Errorf("expected classification %q to be valid", fc)
		}
	}

	invalid := []FailureClassification{"", "RANDOM_FAILURE", "TIMEOUT", "FAILED"}
	for _, ifc := range invalid {
		if ifc.IsValid() {
			t.Errorf("expected classification %q to be invalid", ifc)
		}
	}
}

func TestCircuitState_IsValid(t *testing.T) {
	if !CircuitClosed.IsValid() {
		t.Errorf("CircuitClosed must be valid")
	}
	if !CircuitOpen.IsValid() {
		t.Errorf("CircuitOpen must be valid")
	}
	if CircuitState("HALF_OPEN").IsValid() {
		t.Errorf("HALF_OPEN must not be valid (Phase 6.7 strictly supports CLOSED -> OPEN -> explicit reset -> CLOSED)")
	}
	if CircuitState("").IsValid() {
		t.Errorf("empty circuit state must not be valid")
	}
}

func TestEscalationStatus_Lifecycle(t *testing.T) {
	valid := []EscalationStatus{
		EscalationStatusPending,
		EscalationStatusEscalated,
		EscalationStatusResolved,
	}
	for _, s := range valid {
		if !s.IsValid() {
			t.Errorf("expected status %q to be valid", s)
		}
	}
	if EscalationStatus("INVALID").IsValid() {
		t.Errorf("expected INVALID to not be valid")
	}

	// Terminal check
	if EscalationStatusPending.IsTerminal() {
		t.Errorf("PENDING must not be terminal")
	}
	if EscalationStatusEscalated.IsTerminal() {
		t.Errorf("ESCALATED must not be terminal")
	}
	if !EscalationStatusResolved.IsTerminal() {
		t.Errorf("RESOLVED must be terminal")
	}

	// Transitions
	if !EscalationStatusPending.CanTransitionTo(EscalationStatusEscalated) {
		t.Errorf("PENDING -> ESCALATED must be allowed")
	}
	if !EscalationStatusPending.CanTransitionTo(EscalationStatusResolved) {
		t.Errorf("PENDING -> RESOLVED must be allowed")
	}
	if !EscalationStatusEscalated.CanTransitionTo(EscalationStatusResolved) {
		t.Errorf("ESCALATED -> RESOLVED must be allowed")
	}
	if EscalationStatusEscalated.CanTransitionTo(EscalationStatusPending) {
		t.Errorf("ESCALATED -> PENDING must be rejected")
	}
	if EscalationStatusResolved.CanTransitionTo(EscalationStatusPending) {
		t.Errorf("RESOLVED -> PENDING must be rejected")
	}
	if EscalationStatusResolved.CanTransitionTo(EscalationStatusEscalated) {
		t.Errorf("RESOLVED -> ESCALATED must be rejected")
	}
	// Self transitions are allowed idempotent no-ops
	if !EscalationStatusEscalated.CanTransitionTo(EscalationStatusEscalated) {
		t.Errorf("self-transition must be allowed")
	}
}

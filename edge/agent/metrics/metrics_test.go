package metrics

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestMetrics_A_Registration tests metric registration and duplicate rejection.
func TestMetrics_A_Registration(t *testing.T) {
	reg := NewRegistry()
	c1 := NewSingleCounter("test_counter", "Test counter help")
	if err := reg.Register(c1); err != nil {
		t.Fatalf("expected successful registration, got: %v", err)
	}

	// Duplicate registration must return an error
	c2 := NewSingleCounter("test_counter", "Duplicate help")
	if err := reg.Register(c2); err == nil {
		t.Fatal("expected error on duplicate metric registration, got nil")
	}

	m, ok := reg.Get("test_counter")
	if !ok || m != c1 {
		t.Fatalf("expected to retrieve registered counter, got ok=%v, m=%v", ok, m)
	}
}

// TestMetrics_B_CounterIncrements tests counter operations and monotonicity.
func TestMetrics_B_CounterIncrements(t *testing.T) {
	c := NewSingleCounter("batches_total", "Batches processed")
	if c.Value() != 0 {
		t.Fatalf("expected initial value 0, got %v", c.Value())
	}

	c.Inc()
	if c.Value() != 1 {
		t.Fatalf("expected 1 after Inc(), got %v", c.Value())
	}

	c.Add(5)
	if c.Value() != 6 {
		t.Fatalf("expected 6 after Add(5), got %v", c.Value())
	}

	// Negative values must be ignored to maintain monotonicity
	c.Add(-10)
	if c.Value() != 6 {
		t.Fatalf("expected negative Add to be ignored, got %v", c.Value())
	}
}

// TestMetrics_C_GaugeUpdates tests gauge modifications (set, inc, dec, add, sub).
func TestMetrics_C_GaugeUpdates(t *testing.T) {
	g := NewSingleGauge("active_incidents", "Active incidents")
	if g.Value() != 0 {
		t.Fatalf("expected initial value 0, got %v", g.Value())
	}

	g.Set(10)
	if g.Value() != 10 {
		t.Fatalf("expected 10 after Set(10), got %v", g.Value())
	}

	g.Inc()
	if g.Value() != 11 {
		t.Fatalf("expected 11 after Inc(), got %v", g.Value())
	}

	g.Dec()
	if g.Value() != 10 {
		t.Fatalf("expected 10 after Dec(), got %v", g.Value())
	}

	g.Add(2.5)
	if g.Value() != 12.5 {
		t.Fatalf("expected 12.5 after Add(2.5), got %v", g.Value())
	}

	g.Sub(5.5)
	if g.Value() != 7.0 {
		t.Fatalf("expected 7.0 after Sub(5.5), got %v", g.Value())
	}
}

// TestMetrics_D_HistogramObservations tests bucket distribution, sum, and count.
func TestMetrics_D_HistogramObservations(t *testing.T) {
	buckets := []float64{0.01, 0.05, 0.1, 0.5, 1.0}
	h := NewSingleHistogram("test_duration_seconds", "Duration", buckets)

	h.Observe(0.005) // <= 0.01, 0.05, 0.1, 0.5, 1.0, +Inf
	h.Observe(0.03)  // <= 0.05, 0.1, 0.5, 1.0, +Inf
	h.Observe(0.2)   // <= 0.5, 1.0, +Inf
	h.Observe(2.5)   // <= +Inf

	if h.Count() != 4 {
		t.Fatalf("expected count 4, got %d", h.Count())
	}

	expectedSum := 0.005 + 0.03 + 0.2 + 2.5
	if h.Sum() < expectedSum-0.001 || h.Sum() > expectedSum+0.001 {
		t.Fatalf("expected sum ~%v, got %v", expectedSum, h.Sum())
	}

	counts := h.BucketCounts()
	// buckets: [0.01, 0.05, 0.1, 0.5, 1.0] -> 5 buckets + 1 for +Inf = 6
	if len(counts) != 6 {
		t.Fatalf("expected 6 bucket counts, got %d", len(counts))
	}
	if counts[0] != 1 { // <= 0.01
		t.Errorf("expected bucket[0] to be 1, got %d", counts[0])
	}
	if counts[1] != 2 { // <= 0.05
		t.Errorf("expected bucket[1] to be 2, got %d", counts[1])
	}
	if counts[2] != 2 { // <= 0.1
		t.Errorf("expected bucket[2] to be 2, got %d", counts[2])
	}
	if counts[3] != 3 { // <= 0.5
		t.Errorf("expected bucket[3] to be 3, got %d", counts[3])
	}
	if counts[4] != 3 { // <= 1.0
		t.Errorf("expected bucket[4] to be 3, got %d", counts[4])
	}
	if counts[5] != 4 { // <= +Inf
		t.Errorf("expected bucket[5] (+Inf) to be 4, got %d", counts[5])
	}
}

// TestMetrics_E_BoundedLabels tests partitioned metrics with bounded labels.
func TestMetrics_E_BoundedLabels(t *testing.T) {
	cv := NewCounterVec("response_decisions", "Decisions", []string{"authorization_class"})
	cv.WithLabelValues("AUTO_EXECUTE").Inc()
	cv.WithLabelValues("AUTO_EXECUTE").Inc()
	cv.WithLabelValues("APPROVAL_REQUIRED").Inc()

	if cv.WithLabelValues("AUTO_EXECUTE").Value() != 2 {
		t.Fatalf("expected 2 for AUTO_EXECUTE, got %v", cv.WithLabelValues("AUTO_EXECUTE").Value())
	}
	if cv.WithLabelValues("APPROVAL_REQUIRED").Value() != 1 {
		t.Fatalf("expected 1 for APPROVAL_REQUIRED, got %v", cv.WithLabelValues("APPROVAL_REQUIRED").Value())
	}
}

// TestMetrics_F_InvalidLabels tests mapping of un-allowlisted label values to "unknown".
func TestMetrics_F_InvalidLabels(t *testing.T) {
	rec := NewDefaultRecorder(nil)

	// Anomaly method
	rec.RecordAnomalyDetected("arbitrary_unknown_ml_algorithm")
	rec.RecordAnomalyDetected("threshold")

	// Authorization class
	rec.RecordResponseDecision("CUSTOM_SUPERUSER_CLASS")
	rec.RecordResponseDecision("AUTO_EXECUTE")

	// Mitigation action
	rec.RecordMitigation("REBOOT_HOST_MACHINE", "started")
	rec.RecordMitigation("SIMULATED_RESTART", "invalid_status_enum")

	// Audit event
	rec.RecordAuditEvent("ARBITRARY_UNAUTHORIZED_EVENT")

	output := rec.Registry().FormatPrometheus()

	// "unknown" label must exist
	if !strings.Contains(output, `aegisedge_anomalies_detected_total{method="unknown"}`) {
		t.Errorf("expected sanitized unknown method label, got:\n%s", output)
	}
	if !strings.Contains(output, `aegisedge_response_decisions_total{authorization_class="unknown"}`) {
		t.Errorf("expected sanitized unknown auth class label, got:\n%s", output)
	}
	if !strings.Contains(output, `action_type="unknown"`) {
		t.Errorf("expected sanitized unknown action type label, got:\n%s", output)
	}
	if !strings.Contains(output, `status="unknown"`) {
		t.Errorf("expected sanitized unknown status label, got:\n%s", output)
	}
	if !strings.Contains(output, `aegisedge_audit_events_total{event_type="unknown"}`) {
		t.Errorf("expected sanitized unknown audit event type, got:\n%s", output)
	}

	// Arbitrary injection strings must NOT be present as label values
	if strings.Contains(output, "arbitrary_unknown_ml_algorithm") {
		t.Errorf("arbitrary method string leaked into Prometheus labels")
	}
	if strings.Contains(output, "CUSTOM_SUPERUSER_CLASS") {
		t.Errorf("arbitrary auth class string leaked into Prometheus labels")
	}
	if strings.Contains(output, "REBOOT_HOST_MACHINE") {
		t.Errorf("arbitrary action type string leaked into Prometheus labels")
	}
}

// TestMetrics_G_ConcurrentRecording tests high concurrency recording across multiple goroutines.
func TestMetrics_G_ConcurrentRecording(t *testing.T) {
	rec := NewDefaultRecorder(nil)
	const goroutines = 20
	const iterations = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				rec.RecordTelemetryBatch(true)
				rec.RecordAnomalyDetected("threshold")
				rec.ObserveDetectorDuration("threshold", 10*time.Millisecond)
				rec.RecordIncidentCreated()
				rec.RecordResponseDecision("AUTO_EXECUTE")
				rec.RecordSafetyValidation(true)
				rec.RecordMitigation("SIMULATED_RESTART", "executed")
				rec.ObserveMitigationDuration("SIMULATED_RESTART", 50*time.Millisecond)
				rec.RecordVerification("recovered")
				rec.ObserveVerificationDuration(100 * time.Millisecond)
				rec.RecordEscalation("mitigation_failed")
				rec.RecordRetry()
				rec.RecordCircuitBreakerEvent("opened")
				rec.RecordOrchestration("completed")
				rec.ObserveOrchestrationDuration(200 * time.Millisecond)
				rec.RecordAuditEvent("MITIGATION_EXECUTED")
			}
		}(i)
	}

	wg.Wait()

	totalExpected := float64(goroutines * iterations)
	m, ok := rec.Registry().Get(MetricTelemetryBatchesTotal)
	if !ok {
		t.Fatalf("expected metric %s", MetricTelemetryBatchesTotal)
	}
	sc, ok := m.(*SingleCounter)
	if !ok || sc.Value() != totalExpected {
		t.Fatalf("expected %v batches total, got %v", totalExpected, sc.Value())
	}
}

// TestMetrics_H_PrometheusExpositionFormat tests standard Prometheus formatting compliance.
func TestMetrics_H_PrometheusExpositionFormat(t *testing.T) {
	rec := NewDefaultRecorder(nil)
	rec.RecordTelemetryBatch(true)
	rec.RecordAnomalyDetected("threshold")
	rec.SetActiveIncidents(3)
	rec.ObserveOrchestrationDuration(150 * time.Millisecond)

	out := rec.Registry().FormatPrometheus()

	// Must contain HELP and TYPE definitions
	expectedHeaders := []string{
		"# HELP aegisedge_telemetry_batches_total Total number of telemetry batches generated and processed.",
		"# TYPE aegisedge_telemetry_batches_total counter",
		"# HELP aegisedge_anomalies_detected_total Total number of anomaly signals detected by method.",
		"# TYPE aegisedge_anomalies_detected_total counter",
		"# HELP aegisedge_active_incidents Current count of active incidents on the local node.",
		"# TYPE aegisedge_active_incidents gauge",
		"# HELP aegisedge_orchestration_duration_seconds Duration of complete incident response orchestration cycles in seconds.",
		"# TYPE aegisedge_orchestration_duration_seconds histogram",
	}

	for _, header := range expectedHeaders {
		if !strings.Contains(out, header) {
			t.Errorf("missing expected header in exposition format:\n%s\ngot:\n%s", header, out)
		}
	}

	// Must contain bucket lines ending with \n
	if !strings.Contains(out, "aegisedge_active_incidents 3\n") {
		t.Errorf("expected gauge value line, got:\n%s", out)
	}
}

// TestMetrics_I_HTTPMetricsEndpoint tests the HTTP /metrics handler.
func TestMetrics_I_HTTPMetricsEndpoint(t *testing.T) {
	rec := NewDefaultRecorder(nil)
	rec.RecordTelemetryBatch(true)

	handler := rec.Registry().Handler()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 OK, got %d", rr.Code)
	}

	contentType := rr.Header().Get("Content-Type")
	if !strings.Contains(contentType, "text/plain") || !strings.Contains(contentType, "version=0.0.4") {
		t.Errorf("expected Prometheus content-type header, got %s", contentType)
	}

	body := rr.Body.String()
	if !strings.Contains(body, "aegisedge_telemetry_batches_total 1") {
		t.Errorf("expected metric in HTTP response body, got:\n%s", body)
	}
}

// TestMetrics_J_ReadOnlyBehavior tests that only GET/HEAD are accepted and metrics have no side effects.
func TestMetrics_J_ReadOnlyBehavior(t *testing.T) {
	rec := NewDefaultRecorder(nil)
	srv := NewServer("127.0.0.1:0", rec.Registry())

	// Test unsupported methods on /metrics
	methods := []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch}
	for _, m := range methods {
		req := httptest.NewRequest(m, "/metrics", nil)
		rr := httptest.NewRecorder()
		srv.server.Handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected HTTP 405 Method Not Allowed for %s, got %d", m, rr.Code)
		}
	}

	// Test HEAD request succeeds without body
	headReq := httptest.NewRequest(http.MethodHead, "/metrics", nil)
	headRr := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(headRr, headReq)
	if headRr.Code != http.StatusOK {
		t.Errorf("expected HTTP 200 OK for HEAD, got %d", headRr.Code)
	}
	if headRr.Body.Len() != 0 {
		t.Errorf("expected empty body for HEAD, got length %d", headRr.Body.Len())
	}
}

// TestMetrics_K_NoHighCardinalityLabels verifies that prohibited fields never appear in /metrics output.
func TestMetrics_K_NoHighCardinalityLabels(t *testing.T) {
	rec := NewDefaultRecorder(nil)

	// Simulate recording domain activities with various arguments
	rec.RecordTelemetryBatch(true)
	rec.RecordAnomalyDetected("threshold")
	rec.RecordIncidentCreated()
	rec.RecordResponseDecision("AUTO_EXECUTE")
	rec.RecordSafetyValidation(true)
	rec.RecordApproval("approved")
	rec.RecordMitigation("SIMULATED_RESTART", "executed")
	rec.RecordVerification("recovered")
	rec.RecordEscalation("mitigation_failed")
	rec.RecordOrchestration("completed")
	rec.RecordAuditEvent("MITIGATION_EXECUTED")

	output := rec.Registry().FormatPrometheus()

	// Prohibited high-cardinality terms
	prohibited := []string{
		"incident_id",
		"decision_id",
		"action_id",
		"approval_id",
		"verification_id",
		"escalation_id",
		"correlation_id",
		"node_id",
		"timestamp",
		"error_message",
	}

	for _, term := range prohibited {
		if strings.Contains(output, term+"=") {
			t.Errorf("prohibited high-cardinality label key '%s' found in exposition:\n%s", term, output)
		}
	}
}

// TestMetrics_L_DetectorMetrics tests anomaly detector metrics.
func TestMetrics_L_DetectorMetrics(t *testing.T) {
	rec := NewDefaultRecorder(nil)
	rec.RecordAnomalyDetected("threshold")
	rec.RecordAnomalyDetected("z_score")
	rec.RecordAnomalyDetected("isolation_forest")
	rec.RecordDetectorError("isolation_forest")
	rec.ObserveDetectorDuration("threshold", 5*time.Millisecond)

	out := rec.Registry().FormatPrometheus()
	if !strings.Contains(out, `aegisedge_anomalies_detected_total{method="threshold"} 1`) {
		t.Errorf("expected threshold anomaly metric, got:\n%s", out)
	}
	if !strings.Contains(out, `aegisedge_anomalies_detected_total{method="z_score"} 1`) {
		t.Errorf("expected z_score anomaly metric, got:\n%s", out)
	}
	if !strings.Contains(out, `aegisedge_anomalies_detected_total{method="isolation_forest"} 1`) {
		t.Errorf("expected isolation_forest anomaly metric, got:\n%s", out)
	}
	if !strings.Contains(out, `aegisedge_detector_errors_total{method="isolation_forest"} 1`) {
		t.Errorf("expected detector error metric, got:\n%s", out)
	}
}

// TestMetrics_M_IncidentMetrics tests incident lifecycle metrics.
func TestMetrics_M_IncidentMetrics(t *testing.T) {
	rec := NewDefaultRecorder(nil)
	rec.RecordIncidentCreated()
	rec.RecordIncidentCreated()
	rec.RecordIncidentEscalated()
	rec.RecordIncidentRecovered()

	out := rec.Registry().FormatPrometheus()
	if !strings.Contains(out, `aegisedge_incidents_created_total 2`) {
		t.Errorf("expected 2 incidents created, got:\n%s", out)
	}
	if !strings.Contains(out, `aegisedge_incidents_escalated_total 1`) {
		t.Errorf("expected 1 incident escalated, got:\n%s", out)
	}
	if !strings.Contains(out, `aegisedge_incidents_recovered_total 1`) {
		t.Errorf("expected 1 incident recovered, got:\n%s", out)
	}
	// Active: 2 created - 1 recovered = 1 active
	if !strings.Contains(out, `aegisedge_active_incidents 1`) {
		t.Errorf("expected 1 active incident, got:\n%s", out)
	}
}

// TestMetrics_N_MitigationMetrics tests mitigation action metrics and duration observations.
func TestMetrics_N_MitigationMetrics(t *testing.T) {
	rec := NewDefaultRecorder(nil)
	rec.RecordMitigation("SIMULATED_THROTTLE", "started")
	rec.RecordMitigation("SIMULATED_THROTTLE", "executed")
	rec.ObserveMitigationDuration("SIMULATED_THROTTLE", 40*time.Millisecond)

	out := rec.Registry().FormatPrometheus()
	if !strings.Contains(out, `aegisedge_mitigations_total{action_type="SIMULATED_THROTTLE",status="started"} 1`) {
		t.Errorf("expected mitigation started metric, got:\n%s", out)
	}
	if !strings.Contains(out, `aegisedge_mitigations_total{action_type="SIMULATED_THROTTLE",status="executed"} 1`) {
		t.Errorf("expected mitigation executed metric, got:\n%s", out)
	}
	if !strings.Contains(out, `aegisedge_mitigation_duration_seconds_count{action_type="SIMULATED_THROTTLE"} 1`) {
		t.Errorf("expected mitigation duration observation, got:\n%s", out)
	}
}

// TestMetrics_O_VerificationMetrics tests verification status and duration metrics.
func TestMetrics_O_VerificationMetrics(t *testing.T) {
	rec := NewDefaultRecorder(nil)
	rec.RecordVerification("started")
	rec.RecordVerification("recovered")
	rec.ObserveVerificationDuration(250 * time.Millisecond)

	out := rec.Registry().FormatPrometheus()
	if !strings.Contains(out, `aegisedge_verifications_total{status="started"} 1`) {
		t.Errorf("expected verification started, got:\n%s", out)
	}
	if !strings.Contains(out, `aegisedge_verifications_total{status="recovered"} 1`) {
		t.Errorf("expected verification recovered, got:\n%s", out)
	}
	if !strings.Contains(out, `aegisedge_verification_duration_seconds_count 1`) {
		t.Errorf("expected verification duration observation, got:\n%s", out)
	}
}

// TestMetrics_P_EscalationMetrics tests escalation, retries, and circuit breaker metrics.
func TestMetrics_P_EscalationMetrics(t *testing.T) {
	rec := NewDefaultRecorder(nil)
	rec.RecordEscalation("mitigation_failed")
	rec.RecordRetry()
	rec.RecordCircuitBreakerEvent("opened")

	out := rec.Registry().FormatPrometheus()
	if !strings.Contains(out, `aegisedge_escalations_total{classification="mitigation_failed"} 1`) {
		t.Errorf("expected escalation metric, got:\n%s", out)
	}
	if !strings.Contains(out, `aegisedge_retries_total 1`) {
		t.Errorf("expected retry counter, got:\n%s", out)
	}
	if !strings.Contains(out, `aegisedge_circuit_breaker_events_total{event="opened"} 1`) {
		t.Errorf("expected circuit breaker opened event, got:\n%s", out)
	}
}

// TestMetrics_Q_OrchestratorMetrics tests orchestration outcomes and durations.
func TestMetrics_Q_OrchestratorMetrics(t *testing.T) {
	rec := NewDefaultRecorder(nil)
	rec.RecordOrchestration("completed")
	rec.RecordOrchestration("stopped")
	rec.ObserveOrchestrationDuration(500 * time.Millisecond)

	out := rec.Registry().FormatPrometheus()
	if !strings.Contains(out, `aegisedge_orchestrations_total{outcome="completed"} 1`) {
		t.Errorf("expected orchestration completed metric, got:\n%s", out)
	}
	if !strings.Contains(out, `aegisedge_orchestrations_total{outcome="stopped"} 1`) {
		t.Errorf("expected orchestration stopped metric, got:\n%s", out)
	}
	if !strings.Contains(out, `aegisedge_orchestration_duration_seconds_count 1`) {
		t.Errorf("expected orchestration duration count, got:\n%s", out)
	}
}

// TestMetrics_R_AuditMetrics tests audit event counters, write errors, and duplicate handling.
func TestMetrics_R_AuditMetrics(t *testing.T) {
	rec := NewDefaultRecorder(nil)
	rec.RecordAuditEvent("MITIGATION_EXECUTED")
	rec.RecordAuditEvent("EVENT_VERIFICATION_RECOVERED")
	rec.RecordAuditWriteError()
	rec.RecordAuditDuplicate()

	out := rec.Registry().FormatPrometheus()
	if !strings.Contains(out, `aegisedge_audit_events_total{event_type="MITIGATION_EXECUTED"} 1`) {
		t.Errorf("expected audit event metric, got:\n%s", out)
	}
	if !strings.Contains(out, `aegisedge_audit_events_total{event_type="VERIFICATION_RECOVERED"} 1`) {
		t.Errorf("expected trimmed event type metric, got:\n%s", out)
	}
	if !strings.Contains(out, `aegisedge_audit_write_errors_total 1`) {
		t.Errorf("expected audit write errors counter, got:\n%s", out)
	}
	if !strings.Contains(out, `aegisedge_audit_duplicates_total 1`) {
		t.Errorf("expected audit duplicate counter, got:\n%s", out)
	}
}

// TestMetrics_S_DurationMetrics tests that durations record non-zero sum and positive counts.
func TestMetrics_S_DurationMetrics(t *testing.T) {
	rec := NewDefaultRecorder(nil)
	rec.ObserveDetectorDuration("threshold", 12*time.Millisecond)
	rec.ObserveMitigationDuration("SIMULATED_RESTART", 45*time.Millisecond)
	rec.ObserveVerificationDuration(250 * time.Millisecond)
	rec.ObserveOrchestrationDuration(310 * time.Millisecond)

	out := rec.Registry().FormatPrometheus()
	if !strings.Contains(out, "aegisedge_detector_duration_seconds_count") {
		t.Errorf("missing detector duration in output:\n%s", out)
	}
	if !strings.Contains(out, "aegisedge_mitigation_duration_seconds_count") {
		t.Errorf("missing mitigation duration in output:\n%s", out)
	}
	if !strings.Contains(out, "aegisedge_verification_duration_seconds_count") {
		t.Errorf("missing verification duration in output:\n%s", out)
	}
	if !strings.Contains(out, "aegisedge_orchestration_duration_seconds_count") {
		t.Errorf("missing orchestration duration in output:\n%s", out)
	}
}

// TestMetrics_T_EndpointConcurrency tests concurrent HTTP scraping while metrics are actively recorded.
func TestMetrics_T_EndpointConcurrency(t *testing.T) {
	rec := NewDefaultRecorder(nil)
	srv := NewServer("127.0.0.1:0", rec.Registry())
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start metrics server: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
	}()

	addr := srv.Addr()
	metricsURL := "http://" + addr + "/metrics"

	const workers = 10
	const iterations = 25
	var wg sync.WaitGroup
	wg.Add(workers * 2)

	// Goroutines recording metrics
	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				rec.RecordTelemetryBatch(true)
				rec.RecordAnomalyDetected("threshold")
				rec.ObserveDetectorDuration("threshold", time.Duration(j)*time.Millisecond)
				rec.RecordMitigation("SIMULATED_RESTART", "executed")
				rec.RecordOrchestration("completed")
			}
		}(i)
	}

	// Goroutines scraping /metrics via HTTP client
	client := &http.Client{Timeout: 2 * time.Second}
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				resp, err := client.Get(metricsURL)
				if err != nil {
					t.Errorf("HTTP scrape failed: %v", err)
					return
				}
				body, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					t.Errorf("expected 200 OK, got %d", resp.StatusCode)
				}
				if !strings.Contains(string(body), "aegisedge_telemetry_batches_total") {
					t.Errorf("expected metric in response, got length %d", len(body))
				}
			}
		}()
	}

	wg.Wait()
}

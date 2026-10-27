package metrics

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSingleCounter(t *testing.T) {
	c := NewSingleCounter("test_counter", "A test counter")
	if c.Name() != "test_counter" || c.Type() != MetricTypeCounter {
		t.Fatalf("unexpected name or type: %s, %s", c.Name(), c.Type())
	}
	if c.Value() != 0 {
		t.Fatalf("expected 0, got %f", c.Value())
	}

	c.Inc()
	c.Add(4.5)
	c.Add(-2) // Should be ignored
	c.Add(0)  // Should be ignored

	if c.Value() != 5.5 {
		t.Fatalf("expected 5.5, got %f", c.Value())
	}

	var sb strings.Builder
	if err := c.WritePrometheus(&sb); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := sb.String()
	if !strings.Contains(out, "# HELP test_counter A test counter\n") ||
		!strings.Contains(out, "# TYPE test_counter counter\n") ||
		!strings.Contains(out, "test_counter 5.5\n") {
		t.Fatalf("unexpected output:\n%s", out)
	}
}

func TestFuncCounter(t *testing.T) {
	val := 42.0
	fc := NewFuncCounter("test_func_counter", "Dynamic counter", func() float64 {
		return val
	})
	if fc.Value() != 42.0 {
		t.Fatalf("expected 42.0, got %f", fc.Value())
	}
	val = 100.0
	if fc.Value() != 100.0 {
		t.Fatalf("expected 100.0, got %f", fc.Value())
	}

	nilFc := NewFuncCounter("nil_fn", "Nil dynamic counter", nil)
	if nilFc.Value() != 0 {
		t.Fatalf("expected 0 for nil fn, got %f", nilFc.Value())
	}
}

func TestCounterVec(t *testing.T) {
	cv := NewCounterVec("http_requests_total", "HTTP requests", []string{"method", "code"})
	cv.WithLabelValues("GET", "200").Inc()
	cv.WithLabelValues("POST", "201").Add(3)
	cv.WithLabelValues("GET", "200").Inc()

	var sb strings.Builder
	if err := cv.WritePrometheus(&sb); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := sb.String()
	if !strings.Contains(out, "http_requests_total{method=\"GET\",code=\"200\"} 2\n") ||
		!strings.Contains(out, "http_requests_total{method=\"POST\",code=\"201\"} 3\n") {
		t.Fatalf("unexpected output:\n%s", out)
	}
}

func TestSingleGauge(t *testing.T) {
	g := NewSingleGauge("test_gauge", "A test gauge")
	g.Set(10.0)
	g.Inc()
	g.Dec()
	g.Add(5)
	g.Sub(3)

	if g.Value() != 12.0 {
		t.Fatalf("expected 12.0, got %f", g.Value())
	}

	var sb strings.Builder
	if err := g.WritePrometheus(&sb); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := sb.String()
	if !strings.Contains(out, "# TYPE test_gauge gauge\n") ||
		!strings.Contains(out, "test_gauge 12\n") {
		t.Fatalf("unexpected output:\n%s", out)
	}
}

func TestFuncGauge(t *testing.T) {
	count := 7
	fg := NewFuncGauge("active_nodes", "Active nodes gauge", func() float64 {
		return float64(count)
	})
	if fg.Value() != 7 {
		t.Fatalf("expected 7, got %f", fg.Value())
	}

	nilFg := NewFuncGauge("nil_gauge", "Nil gauge", nil)
	if nilFg.Value() != 0 {
		t.Fatalf("expected 0 for nil fn, got %f", nilFg.Value())
	}
}

func TestSingleHistogram(t *testing.T) {
	h := NewSingleHistogram("probe_duration_seconds", "Probe latency", []float64{0.01, 0.05, 0.1})
	h.Observe(0.005)
	h.Observe(0.02)
	h.Observe(0.07)
	h.Observe(0.5)

	if h.Count() != 4 {
		t.Fatalf("expected count 4, got %d", h.Count())
	}

	var sb strings.Builder
	if err := h.WritePrometheus(&sb); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := sb.String()
	if !strings.Contains(out, "probe_duration_seconds_bucket{le=\"0.01\"} 1\n") ||
		!strings.Contains(out, "probe_duration_seconds_bucket{le=\"0.05\"} 2\n") ||
		!strings.Contains(out, "probe_duration_seconds_bucket{le=\"0.1\"} 3\n") ||
		!strings.Contains(out, "probe_duration_seconds_bucket{le=\"+Inf\"} 4\n") ||
		!strings.Contains(out, "probe_duration_seconds_count 4\n") {
		t.Fatalf("unexpected output:\n%s", out)
	}
}

func TestRegistry(t *testing.T) {
	r := NewRegistry()
	c := NewSingleCounter("c1", "counter 1")
	if err := r.Register(c); err != nil {
		t.Fatalf("failed to register: %v", err)
	}
	if err := r.Register(c); err == nil {
		t.Fatalf("expected error registering duplicate metric, got nil")
	}

	got, ok := r.Get("c1")
	if !ok || got.Name() != "c1" {
		t.Fatalf("expected c1, got %v", got)
	}
	_, ok = r.Get("nonexistent")
	if ok {
		t.Fatalf("expected false for nonexistent metric")
	}
}

func TestRegistryHandler(t *testing.T) {
	r := NewRegistry()
	c := NewSingleCounter("test_counter", "counter help")
	c.Inc()
	_ = r.Register(c)

	handler := r.Handler()

	// Test GET
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	res := rec.Result()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); ct != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatalf("unexpected Content-Type: %s", ct)
	}
	body, _ := io.ReadAll(res.Body)
	if !strings.Contains(string(body), "test_counter 1\n") {
		t.Fatalf("unexpected body: %s", string(body))
	}

	// Test HEAD
	headReq := httptest.NewRequest(http.MethodHead, "/metrics", nil)
	headRec := httptest.NewRecorder()
	handler.ServeHTTP(headRec, headReq)

	headRes := headRec.Result()
	if headRes.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", headRes.StatusCode)
	}
	if ct := headRes.Header.Get("Content-Type"); ct != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatalf("unexpected Content-Type: %s", ct)
	}
	headBody, _ := io.ReadAll(headRes.Body)
	if len(headBody) != 0 {
		t.Fatalf("expected empty body for HEAD, got %d bytes", len(headBody))
	}

	// Test unsupported methods (POST, PUT, DELETE)
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		mReq := httptest.NewRequest(method, "/metrics", nil)
		mRec := httptest.NewRecorder()
		handler.ServeHTTP(mRec, mReq)

		mRes := mRec.Result()
		if mRes.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("expected 405 Method Not Allowed for %s, got %d", method, mRes.StatusCode)
		}
		if allow := mRes.Header.Get("Allow"); allow != "GET, HEAD" {
			t.Fatalf("expected Allow: GET, HEAD for %s, got %s", method, allow)
		}
	}
}

func TestControlPlaneMetrics(t *testing.T) {
	m := NewMetrics()

	// Record Ingested Batches
	m.RecordIngestedBatch(TransportHTTP, StatusAccepted)
	m.RecordIngestedBatch(TransportHTTP, StatusAccepted)
	m.RecordIngestedBatch(TransportHTTP, StatusDuplicate)
	m.RecordIngestedBatch(TransportNATS, StatusAccepted)
	m.RecordIngestedBatch("invalid_transport", "invalid_status") // fallback to unknown

	// Record Storage Probes
	m.RecordStorageProbeDuration(50 * time.Millisecond)
	m.RecordStorageProbeDurationSeconds(0.012)
	m.RecordStorageProbeFailure()

	// Record Auth Failures
	m.RecordAuthFailure(ReasonInvalidSignature)
	m.RecordAuthFailure(ReasonForbidden)
	m.RecordAuthFailure("missing_auth_headers")         // maps to missing_auth
	m.RecordAuthFailure("arbitrary_unknown_reason_123") // maps to unknown

	// Dynamic sources
	m.SetNATSExhaustedDeliveriesSource(func() float64 {
		return 3
	})
	m.SetRegisteredNodesSource(func() float64 {
		return 15
	})
	m.SetNATSAdvisoryStreamMessagesSource(func() float64 {
		return 42
	})
	m.SetNATSAdvisoryStreamBytesSource(func() float64 {
		return 8192
	})
	m.SetNATSAdvisoryPendingBacklogSource(func() float64 {
		return 5
	})
	m.SetNATSAdvisoryPendingAckSource(func() float64 {
		return 2
	})
	m.SetNATSAdvisoryFetchErrorsSource(func() float64 {
		return 1
	})
	m.SetNATSAdvisoryEvictionsSource(func() float64 {
		return 4
	})
	m.SetNATSQuarantinedSource(func() float64 {
		return 7
	})
	m.SetNATSRecoveredSource(func() float64 {
		return 6
	})

	output := m.Registry().FormatPrometheus()

	// Verify all required metrics are present
	requiredMetrics := []string{
		MetricIngestedBatchesTotal,
		MetricStorageProbeDurationSeconds,
		MetricStorageProbeFailuresTotal,
		MetricAuthFailuresTotal,
		MetricNATSExhaustedDeliveriesTotal,
		MetricRegisteredNodesCount,
		MetricNATSAdvisoryStreamMessages,
		MetricNATSAdvisoryStreamBytes,
		MetricNATSAdvisoryPendingBacklog,
		MetricNATSAdvisoryPendingAck,
		MetricNATSAdvisoryFetchErrorsTotal,
		MetricNATSAdvisoryEvictionsTotal,
		MetricNATSQuarantinedTotal,
		MetricNATSRecoveredTotal,
	}
	for _, name := range requiredMetrics {
		if !strings.Contains(output, name) {
			t.Fatalf("missing required metric %s in exposition:\n%s", name, output)
		}
	}

	// Verify specific values
	expectedSubstrings := []string{
		`aegisedge_control_plane_ingested_batches_total{transport="http",status="accepted"} 2`,
		`aegisedge_control_plane_ingested_batches_total{transport="http",status="duplicate"} 1`,
		`aegisedge_control_plane_ingested_batches_total{transport="nats",status="accepted"} 1`,
		`aegisedge_control_plane_ingested_batches_total{transport="unknown",status="unknown"} 1`,
		`aegisedge_control_plane_storage_probe_failures_total 1`,
		`aegisedge_control_plane_auth_failures_total{reason="invalid_signature"} 1`,
		`aegisedge_control_plane_auth_failures_total{reason="forbidden"} 1`,
		`aegisedge_control_plane_auth_failures_total{reason="missing_auth"} 1`,
		`aegisedge_control_plane_auth_failures_total{reason="unknown"} 1`,
		`aegisedge_control_plane_nats_exhausted_deliveries_total 3`,
		`aegisedge_control_plane_registered_nodes_count 15`,
		`aegisedge_control_plane_nats_advisory_stream_messages 42`,
		`aegisedge_control_plane_nats_advisory_stream_bytes 8192`,
		`aegisedge_control_plane_nats_advisory_pending_backlog 5`,
		`aegisedge_control_plane_nats_advisory_pending_ack 2`,
		`aegisedge_control_plane_nats_advisory_fetch_errors_total 1`,
		`aegisedge_control_plane_nats_advisory_evictions_total 4`,
		`aegisedge_control_plane_nats_quarantined_total 7`,
		`aegisedge_control_plane_nats_recovered_total 6`,
	}
	for _, sub := range expectedSubstrings {
		if !strings.Contains(output, sub) {
			t.Fatalf("expected output to contain %q, but got:\n%s", sub, output)
		}
	}
}

func TestControlPlaneMetrics_Concurrency(t *testing.T) {
	m := NewMetrics()
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			m.RecordIngestedBatch(TransportHTTP, StatusAccepted)
			m.RecordStorageProbeDuration(10 * time.Millisecond)
			m.RecordStorageProbeFailure()
			m.RecordAuthFailure(ReasonUnauthorized)
			_ = m.Registry().FormatPrometheus()
		}(i)
	}

	wg.Wait()

	out := m.Registry().FormatPrometheus()
	if !strings.Contains(out, `aegisedge_control_plane_storage_probe_failures_total 50`) {
		t.Fatalf("expected probe failures total 50, output:\n%s", out)
	}
}

func TestEscapeHelp(t *testing.T) {
	// Test escaping of backslashes and literal newlines in HELP text
	helpText := "Line 1 with \\ backslash\nLine 2 with \n newline"
	c := NewSingleCounter("metric_help_test", helpText)

	var sb strings.Builder
	if err := c.WritePrometheus(&sb); err != nil {
		t.Fatalf("WritePrometheus failed: %v", err)
	}
	out := sb.String()

	expectedHelp := "# HELP metric_help_test Line 1 with \\\\ backslash\\nLine 2 with \\n newline\n"
	if !strings.Contains(out, expectedHelp) {
		t.Fatalf("expected escaped help:\n%q\ngot:\n%q", expectedHelp, out)
	}
}

func TestCounterVec_LabelEscaping(t *testing.T) {
	cv := NewCounterVec("escaped_metric_total", "Test label escaping", []string{"path", "msg", "quote"})
	// Label values containing quotes, backslashes, and newlines
	cv.WithLabelValues("C:\\AegisEdge\\wal", "alert:\nsevere", `val "with quotes"`).Inc()

	var sb strings.Builder
	if err := cv.WritePrometheus(&sb); err != nil {
		t.Fatalf("WritePrometheus failed: %v", err)
	}
	out := sb.String()

	expectedSample := `escaped_metric_total{path="C:\\AegisEdge\\wal",msg="alert:\nsevere",quote="val \"with quotes\""} 1`
	if !strings.Contains(out, expectedSample) {
		t.Fatalf("expected escaped sample:\n%s\ngot output:\n%s", expectedSample, out)
	}

	// Verify no unescaped raw newlines were injected inside label brackets
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 3 { // # HELP, # TYPE, and 1 metric line
		t.Fatalf("expected exactly 3 lines, got %d:\n%s", len(lines), out)
	}
}

func TestSingleHistogram_SnapshotConsistency(t *testing.T) {
	h := NewSingleHistogram("concurrent_probe_latency_seconds", "Latency", []float64{0.001, 0.01, 0.1, 1.0})

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Concurrently write observations
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			val := float64(id) * 0.005
			for {
				select {
				case <-stop:
					return
				default:
					h.Observe(val)
				}
			}
		}(i)
	}

	// Concurrently scrape and verify that +Inf count equals _count in every single scrape
	for i := 0; i < 100; i++ {
		var sb strings.Builder
		if err := h.WritePrometheus(&sb); err != nil {
			t.Fatalf("WritePrometheus failed: %v", err)
		}
		out := sb.String()

		// Extract +Inf count and _count
		lines := strings.Split(out, "\n")
		var infCount, totalCount string
		for _, l := range lines {
			if strings.Contains(l, `_bucket{le="+Inf"}`) {
				fields := strings.Fields(l)
				if len(fields) == 2 {
					infCount = fields[1]
				}
			}
			if strings.HasPrefix(l, "concurrent_probe_latency_seconds_count ") {
				fields := strings.Fields(l)
				if len(fields) == 2 {
					totalCount = fields[1]
				}
			}
		}

		if infCount == "" || totalCount == "" {
			t.Fatalf("missing +Inf or count line in output:\n%s", out)
		}
		if infCount != totalCount {
			t.Fatalf("inconsistent snapshot: +Inf bucket=%s != count=%s", infCount, totalCount)
		}
	}

	close(stop)
	wg.Wait()
}

func TestNameAndLabelValidation(t *testing.T) {
	r := NewRegistry()

	// 1. Valid metric names
	validNames := []string{
		"aegisedge_total",
		"http_requests_total",
		"metric:with:colons",
		"_internal_counter",
		"METRIC_UPPER_CASE_123",
		":colon_start",
	}
	for _, name := range validNames {
		c := NewSingleCounter(name, "help")
		if err := r.Register(c); err != nil {
			t.Fatalf("expected valid metric name %q to register, got error: %v", name, err)
		}
	}

	// 2. Duplicate registration
	dup := NewSingleCounter("aegisedge_total", "duplicate help")
	if err := r.Register(dup); err == nil {
		t.Fatalf("expected error registering duplicate metric, got nil")
	}

	// 3. Invalid metric names
	invalidNames := []string{
		"",
		"123starts_with_digit",
		"metric-with-hyphens",
		"metric.with.dots",
		"metric with spaces",
		"metric@special",
		"metric#hash",
	}
	for _, name := range invalidNames {
		c := NewSingleCounter(name, "help")
		if err := r.Register(c); err == nil {
			t.Fatalf("expected error for invalid metric name %q, got nil", name)
		}
	}

	// 4. Valid and invalid label names
	validLabels := [][]string{
		{"transport", "status"},
		{"_private_label", "NODE_123"},
		{"code"},
	}
	for i, labels := range validLabels {
		cv := NewCounterVec(fmt.Sprintf("vec_valid_%d", i), "help", labels)
		if err := r.Register(cv); err != nil {
			t.Fatalf("expected valid labels %v to register, got: %v", labels, err)
		}
	}

	invalidLabels := [][]string{
		{""},
		{"123digit_start"},
		{"label-with-hyphen"},
		{"label:with:colon"}, // Colons not allowed in label names
		{"label with space"},
		{"label.with.dot"},
	}
	for i, labels := range invalidLabels {
		cv := NewCounterVec(fmt.Sprintf("vec_invalid_%d", i), "help", labels)
		if err := r.Register(cv); err == nil {
			t.Fatalf("expected error for invalid labels %v, got nil", labels)
		}
	}
}

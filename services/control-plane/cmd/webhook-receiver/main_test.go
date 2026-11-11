package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWebhookReceiver_Health(t *testing.T) {
	state := NewServerState()
	handler := NewHandler(state)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "OK") {
		t.Errorf("expected body to contain OK, got %q", w.Body.String())
	}

	// POST not allowed on /health
	reqPost := httptest.NewRequest(http.MethodPost, "/health", nil)
	wPost := httptest.NewRecorder()
	handler.ServeHTTP(wPost, reqPost)
	if wPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 Method Not Allowed, got %d", wPost.Code)
	}
}

func TestWebhookReceiver_MetricsExposition_WithRunID(t *testing.T) {
	state := NewServerState()
	handler := NewHandler(state)

	// Update trigger val=1, instances=2, run_id=run-abc123
	reqTrigger := httptest.NewRequest(http.MethodPost, "/api/trigger?val=1&instances=2&run_id=run-abc123", nil)
	wTrigger := httptest.NewRecorder()
	handler.ServeHTTP(wTrigger, reqTrigger)
	if wTrigger.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from trigger, got %d: %s", wTrigger.Code, wTrigger.Body.String())
	}

	// Scrape metrics
	reqMetrics := httptest.NewRequest(http.MethodGet, "/test-metrics", nil)
	wMetrics := httptest.NewRecorder()
	handler.ServeHTTP(wMetrics, reqMetrics)
	if wMetrics.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from metrics, got %d", wMetrics.Code)
	}

	body := wMetrics.Body.String()
	if !strings.Contains(body, `aegisedge_lifecycle_test_metric{node="node-a",test_run_id="run-abc123"} 1`) {
		t.Errorf("missing node-a with test_run_id metric in exposition: %s", body)
	}
	if !strings.Contains(body, `aegisedge_lifecycle_test_metric{node="node-b",test_run_id="run-abc123"} 1`) {
		t.Errorf("missing node-b with test_run_id metric in exposition: %s", body)
	}

	// Single instance with run_id
	reqSingle := httptest.NewRequest(http.MethodPost, "/api/trigger?val=2&instances=1&run_id=single-456", nil)
	wSingle := httptest.NewRecorder()
	handler.ServeHTTP(wSingle, reqSingle)
	if wSingle.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from single instance trigger: %d", wSingle.Code)
	}

	reqMetrics2 := httptest.NewRequest(http.MethodGet, "/test-metrics", nil)
	wMetrics2 := httptest.NewRecorder()
	handler.ServeHTTP(wMetrics2, reqMetrics2)
	body2 := wMetrics2.Body.String()
	if !strings.Contains(body2, `aegisedge_lifecycle_test_metric{test_run_id="single-456"} 2`) {
		t.Errorf("missing single instance metric with test_run_id: %s", body2)
	}
}

func TestWebhookReceiver_InstancesValidation(t *testing.T) {
	state := NewServerState()
	handler := NewHandler(state)

	invalidCases := []string{
		"/api/trigger?val=1&instances=0",
		"/api/trigger?val=1&instances=-5",
		"/api/trigger?val=1&instances=abc",
		"/api/trigger?val=1&instances=11",
		"/api/trigger?val=1&instances=9999",
	}

	for _, tc := range invalidCases {
		req := httptest.NewRequest(http.MethodPost, tc, nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Errorf("expected 400 Bad Request for %q, got %d", tc, w.Code)
		}
	}

	// Valid cases
	validCases := []string{
		"/api/trigger?val=1&instances=1",
		"/api/trigger?val=1&instances=2",
		"/api/trigger?val=1&instances=10",
	}

	for _, tc := range validCases {
		req := httptest.NewRequest(http.MethodPost, tc, nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("expected 200 OK for %q, got %d", tc, w.Code)
		}
	}
}

func TestWebhookReceiver_PayloadValidation(t *testing.T) {
	state := NewServerState()
	handler := NewHandler(state)

	// Valid payload
	validPayload := `{
		"version": "4",
		"status": "firing",
		"receiver": "warning-receiver",
		"alerts": []
	}`
	req := httptest.NewRequest(http.MethodPost, "/alerts/warning", strings.NewReader(validPayload))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 OK for valid webhook, got %d: %s", w.Code, w.Body.String())
	}

	// Missing version
	invalidPayload := `{
		"status": "firing",
		"receiver": "warning-receiver"
	}`
	req2 := httptest.NewRequest(http.MethodPost, "/alerts/warning", strings.NewReader(invalidPayload))
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, req2)
	if w2.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for missing version, got %d", w2.Code)
	}

	// Invalid JSON
	req3 := httptest.NewRequest(http.MethodPost, "/alerts/warning", strings.NewReader("{invalid-json"))
	w3 := httptest.NewRecorder()
	handler.ServeHTTP(w3, req3)
	if w3.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for invalid JSON, got %d", w3.Code)
	}

	// GET method not allowed on webhook
	req4 := httptest.NewRequest(http.MethodGet, "/alerts/warning", nil)
	w4 := httptest.NewRecorder()
	handler.ServeHTTP(w4, req4)
	if w4.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 Method Not Allowed for GET, got %d", w4.Code)
	}
}

func TestWebhookReceiver_EventQueryAndClear(t *testing.T) {
	state := NewServerState()
	handler := NewHandler(state)

	payload := `{
		"version": "4",
		"status": "firing",
		"receiver": "critical-receiver",
		"alerts": []
	}`
	req := httptest.NewRequest(http.MethodPost, "/alerts/critical", strings.NewReader(payload))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("failed to post alert: %d", w.Code)
	}

	// GET events
	reqGet := httptest.NewRequest(http.MethodGet, "/api/events", nil)
	wGet := httptest.NewRecorder()
	handler.ServeHTTP(wGet, reqGet)
	if wGet.Code != http.StatusOK {
		t.Fatalf("failed to get events: %d", wGet.Code)
	}

	var events []RecordedEvent
	if err := json.Unmarshal(wGet.Body.Bytes(), &events); err != nil {
		t.Fatalf("failed to unmarshal events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Route != "critical" {
		t.Errorf("expected route critical, got %q", events[0].Route)
	}

	// DELETE events
	reqDel := httptest.NewRequest(http.MethodDelete, "/api/events", nil)
	wDel := httptest.NewRecorder()
	handler.ServeHTTP(wDel, reqDel)
	if wDel.Code != http.StatusOK {
		t.Fatalf("failed to delete events: %d", wDel.Code)
	}

	// GET events again
	wGet2 := httptest.NewRecorder()
	handler.ServeHTTP(wGet2, httptest.NewRequest(http.MethodGet, "/api/events", nil))
	var eventsAfter []RecordedEvent
	_ = json.Unmarshal(wGet2.Body.Bytes(), &eventsAfter)
	if len(eventsAfter) != 0 {
		t.Errorf("expected 0 events after delete, got %d", len(eventsAfter))
	}
}

func TestWebhookReceiver_OversizedPayload(t *testing.T) {
	state := NewServerState()
	handler := NewHandler(state)

	// Oversized body (> 1 MiB)
	hugeBody := bytes.Repeat([]byte("a"), (1<<20)+1024)
	req := httptest.NewRequest(http.MethodPost, "/alerts/default", bytes.NewReader(hugeBody))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("expected 413 Request Entity Too Large for oversized payload, got %d", w.Code)
	}
}

func TestWebhookReceiver_ServerTimeouts(t *testing.T) {
	state := NewServerState()
	handler := NewHandler(state)
	srv := NewServer(9095, handler)

	if srv.ReadHeaderTimeout != 5*time.Second {
		t.Errorf("expected ReadHeaderTimeout 5s, got %v", srv.ReadHeaderTimeout)
	}
	if srv.ReadTimeout != 10*time.Second {
		t.Errorf("expected ReadTimeout 10s, got %v", srv.ReadTimeout)
	}
	if srv.WriteTimeout != 10*time.Second {
		t.Errorf("expected WriteTimeout 10s, got %v", srv.WriteTimeout)
	}
	if srv.IdleTimeout != 60*time.Second {
		t.Errorf("expected IdleTimeout 60s, got %v", srv.IdleTimeout)
	}
}

// TestWebhookReceiver_ReadHeaderTimeout_Behavior verifies that the HTTP server actively enforces
// ReadHeaderTimeout by closing connections from clients that stall during header transmission.
//
// Testing Limitation Documentation:
// The production ReadHeaderTimeout is 5 seconds (with 10s Read/Write timeouts). Testing literal
// 5-10 second sleeps in rapid unit tests is impractical because it introduces unacceptable test
// latency and OS-level timing flakiness. Therefore, functional timeout enforcement is tested
// here using NewServerWithTimeouts with an accelerated duration (100ms) on an ephemeral TCP listener,
// while the production values are asserted in TestWebhookReceiver_ServerTimeouts and validated
// in the end-to-end integration test suite (scripts/test-alertmanager-lifecycle.ps1).
func TestWebhookReceiver_ReadHeaderTimeout_Behavior(t *testing.T) {
	state := NewServerState()
	handler := NewHandler(state)

	acceleratedTimeouts := ServerTimeouts{
		ReadHeaderTimeout: 100 * time.Millisecond,
		ReadTimeout:       200 * time.Millisecond,
		WriteTimeout:      200 * time.Millisecond,
		IdleTimeout:       1 * time.Second,
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to bind ephemeral listener: %v", err)
	}
	defer listener.Close()

	srv := NewServerWithTimeouts(0, handler, acceleratedTimeouts)
	go func() {
		_ = srv.Serve(listener)
	}()
	defer srv.Close()

	conn, err := net.DialTimeout("tcp", listener.Addr().String(), 500*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to dial server: %v", err)
	}
	defer conn.Close()

	// Send an incomplete HTTP request header without the terminating "\r\n\r\n"
	_, err = conn.Write([]byte("POST /alerts/warning HTTP/1.1\r\nHost: 127.0.0.1\r\n"))
	if err != nil {
		t.Fatalf("failed to write partial header: %v", err)
	}

	// Wait beyond the 100ms ReadHeaderTimeout
	time.Sleep(200 * time.Millisecond)

	// Attempt to read from the connection; the server should have severed the connection
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	buf := make([]byte, 128)
	n, readErr := conn.Read(buf)
	if readErr == nil {
		t.Fatalf("expected connection to be closed by server, but read %d bytes: %s", n, string(buf[:n]))
	}

	var netErr net.Error
	if errors.As(readErr, &netErr) && netErr.Timeout() {
		t.Fatalf("client read timed out waiting for server to close connection: server did not enforce ReadHeaderTimeout")
	}

	// EOF, connection reset, or closed network connection indicates server severed the connection
	if errors.Is(readErr, io.EOF) ||
		strings.Contains(readErr.Error(), "forcibly closed") ||
		strings.Contains(readErr.Error(), "connection reset") ||
		strings.Contains(readErr.Error(), "use of closed network connection") {
		return
	}

	t.Fatalf("unexpected read error: %v", readErr)
}

func TestWebhookReceiver_BearerAuth_WebhookEndpoint(t *testing.T) {
	authToken := "super-secure-test-token-43-chars-long-1234"
	state := NewServerState()
	handler := NewHandlerWithAuth(state, authToken)

	validPayload := `{
		"version": "4",
		"groupKey": "test",
		"status": "firing",
		"receiver": "warning-receiver",
		"alerts": [{"status": "firing", "labels": {"alertname": "Test"}}]
	}`

	// 1. Missing Authorization header -> 401
	reqMissing := httptest.NewRequest(http.MethodPost, "/alerts/warning", bytes.NewBufferString(validPayload))
	wMissing := httptest.NewRecorder()
	handler.ServeHTTP(wMissing, reqMissing)
	if wMissing.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized for missing auth header, got %d", wMissing.Code)
	}

	// 2. Invalid Bearer token -> 401
	reqInvalid := httptest.NewRequest(http.MethodPost, "/alerts/warning", bytes.NewBufferString(validPayload))
	reqInvalid.Header.Set("Authorization", "Bearer invalid-token-123")
	wInvalid := httptest.NewRecorder()
	handler.ServeHTTP(wInvalid, reqInvalid)
	if wInvalid.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized for invalid token, got %d", wInvalid.Code)
	}

	// 3. Valid Bearer token -> 200 OK
	reqValid := httptest.NewRequest(http.MethodPost, "/alerts/warning", bytes.NewBufferString(validPayload))
	reqValid.Header.Set("Authorization", "Bearer "+authToken)
	wValid := httptest.NewRecorder()
	handler.ServeHTTP(wValid, reqValid)
	if wValid.Code != http.StatusOK {
		t.Errorf("expected 200 OK for valid token, got %d: %s", wValid.Code, wValid.Body.String())
	}
}

func TestWebhookReceiver_BearerAuth_ControlEndpoints(t *testing.T) {
	authToken := "test-secret-token-abcdef1234567890"
	state := NewServerState()
	handler := NewHandlerWithAuth(state, authToken)

	// /api/trigger without auth -> 401
	reqTrigNoAuth := httptest.NewRequest(http.MethodPost, "/api/trigger?val=1", nil)
	wTrigNoAuth := httptest.NewRecorder()
	handler.ServeHTTP(wTrigNoAuth, reqTrigNoAuth)
	if wTrigNoAuth.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for /api/trigger without auth, got %d", wTrigNoAuth.Code)
	}

	// /api/trigger with auth -> 200
	reqTrigAuth := httptest.NewRequest(http.MethodPost, "/api/trigger?val=1", nil)
	reqTrigAuth.Header.Set("Authorization", "Bearer "+authToken)
	wTrigAuth := httptest.NewRecorder()
	handler.ServeHTTP(wTrigAuth, reqTrigAuth)
	if wTrigAuth.Code != http.StatusOK {
		t.Errorf("expected 200 for /api/trigger with auth, got %d", wTrigAuth.Code)
	}

	// /api/trigger/inhibition without auth -> 401
	reqInhibitNoAuth := httptest.NewRequest(http.MethodPost, "/api/trigger/inhibition?val=1", nil)
	wInhibitNoAuth := httptest.NewRecorder()
	handler.ServeHTTP(wInhibitNoAuth, reqInhibitNoAuth)
	if wInhibitNoAuth.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for /api/trigger/inhibition without auth, got %d", wInhibitNoAuth.Code)
	}

	// /api/trigger/inhibition with auth -> 200
	reqInhibitAuth := httptest.NewRequest(http.MethodPost, "/api/trigger/inhibition?val=1", nil)
	reqInhibitAuth.Header.Set("Authorization", "Bearer "+authToken)
	wInhibitAuth := httptest.NewRecorder()
	handler.ServeHTTP(wInhibitAuth, reqInhibitAuth)
	if wInhibitAuth.Code != http.StatusOK {
		t.Errorf("expected 200 for /api/trigger/inhibition with auth, got %d", wInhibitAuth.Code)
	}

	// /api/events without auth -> 401
	reqEvNoAuth := httptest.NewRequest(http.MethodGet, "/api/events", nil)
	wEvNoAuth := httptest.NewRecorder()
	handler.ServeHTTP(wEvNoAuth, reqEvNoAuth)
	if wEvNoAuth.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for /api/events without auth, got %d", wEvNoAuth.Code)
	}

	// /api/events with auth -> 200
	reqEvAuth := httptest.NewRequest(http.MethodGet, "/api/events", nil)
	reqEvAuth.Header.Set("Authorization", "Bearer "+authToken)
	wEvAuth := httptest.NewRecorder()
	handler.ServeHTTP(wEvAuth, reqEvAuth)
	if wEvAuth.Code != http.StatusOK {
		t.Errorf("expected 200 for /api/events with auth, got %d", wEvAuth.Code)
	}
}

func TestWebhookReceiver_Unauthenticated_Endpoints(t *testing.T) {
	authToken := "test-secret-token-abcdef1234567890"
	state := NewServerState()
	handler := NewHandlerWithAuth(state, authToken)

	// /health must remain accessible without auth for container healthchecks
	reqHealth := httptest.NewRequest(http.MethodGet, "/health", nil)
	wHealth := httptest.NewRecorder()
	handler.ServeHTTP(wHealth, reqHealth)
	if wHealth.Code != http.StatusOK {
		t.Errorf("expected 200 for /health without auth, got %d", wHealth.Code)
	}

	// /test-metrics must remain accessible via unauthenticated GET for Prometheus scraper
	reqMetrics := httptest.NewRequest(http.MethodGet, "/test-metrics", nil)
	wMetrics := httptest.NewRecorder()
	handler.ServeHTTP(wMetrics, reqMetrics)
	if wMetrics.Code != http.StatusOK {
		t.Errorf("expected 200 for /test-metrics without auth, got %d", wMetrics.Code)
	}
}

func TestWebhookReceiver_SimultaneousInhibitionTrigger(t *testing.T) {
	state := NewServerState()
	handler := NewHandler(state)

	// Trigger simultaneous metrics with run_id
	req := httptest.NewRequest(http.MethodPost, "/api/trigger/inhibition?val=1&run_id=simul-run-123", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from trigger/inhibition, got %d: %s", w.Code, w.Body.String())
	}

	// Scrape metrics
	reqMetrics := httptest.NewRequest(http.MethodGet, "/test-metrics", nil)
	wMetrics := httptest.NewRecorder()
	handler.ServeHTTP(wMetrics, reqMetrics)
	if wMetrics.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from test-metrics, got %d", wMetrics.Code)
	}

	body := wMetrics.Body.String()
	expWarning := `aegisedge_lifecycle_test_metric{alert_family="lifecycle",alert_type="warning",tier="integration-test",test_run_id="simul-run-123"} 1`
	expCritical := `aegisedge_lifecycle_test_metric{alert_family="lifecycle",alert_type="critical",tier="integration-test",test_run_id="simul-run-123"} 1`
	expUnrelated := `aegisedge_lifecycle_test_metric{alert_family="unrelated",alert_type="warning",tier="integration-test",test_run_id="simul-run-123"} 1`

	if !strings.Contains(body, expWarning) {
		t.Errorf("missing expected warning metric: %s\nin:\n%s", expWarning, body)
	}
	if !strings.Contains(body, expCritical) {
		t.Errorf("missing expected critical metric: %s\nin:\n%s", expCritical, body)
	}
	if !strings.Contains(body, expUnrelated) {
		t.Errorf("missing expected unrelated warning metric: %s\nin:\n%s", expUnrelated, body)
	}

	// Clear metrics
	reqClear := httptest.NewRequest(http.MethodPost, "/api/trigger/inhibition?val=0&run_id=simul-run-123", nil)
	wClear := httptest.NewRecorder()
	handler.ServeHTTP(wClear, reqClear)
	if wClear.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from clear, got %d", wClear.Code)
	}

	reqMetricsClear := httptest.NewRequest(http.MethodGet, "/test-metrics", nil)
	wMetricsClear := httptest.NewRecorder()
	handler.ServeHTTP(wMetricsClear, reqMetricsClear)
	bodyClear := wMetricsClear.Body.String()
	if strings.Contains(bodyClear, `"} 1`) {
		t.Errorf("expected all metrics to be 0 after clearance, got:\n%s", bodyClear)
	}
}

func TestWebhookReceiver_LoadAuthToken(t *testing.T) {
	tmpDir := t.TempDir()

	// 1. Valid token file (43 chars Base64 ASCII)
	validToken := "dGhpcy1pcy1hLXNlY3VyZS00My1jaGFyLXRlc3QtdG9rZW4"
	validFile := filepath.Join(tmpDir, "valid_token")
	if err := os.WriteFile(validFile, []byte(validToken+"\n"), 0600); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	tok, err := loadAuthToken(validFile)
	if err != nil {
		t.Fatalf("unexpected error loading valid token: %v", err)
	}
	if tok != validToken {
		t.Errorf("expected %q, got %q", validToken, tok)
	}

	// 2. Token file too short (< 16 chars)
	shortFile := filepath.Join(tmpDir, "short_token")
	if err := os.WriteFile(shortFile, []byte("short"), 0600); err != nil {
		t.Fatalf("failed to write short token file: %v", err)
	}
	_, errShort := loadAuthToken(shortFile)
	if errShort == nil {
		t.Errorf("expected error for token shorter than 16 chars, got nil")
	}

	// 3. Missing file
	_, errMissing := loadAuthToken(filepath.Join(tmpDir, "non_existent"))
	if errMissing == nil {
		t.Errorf("expected error for non-existent file, got nil")
	}

	// 4. Empty path
	tokEmpty, errEmpty := loadAuthToken("")
	if errEmpty != nil || tokEmpty != "" {
		t.Errorf("expected empty token and nil error for empty path, got tok=%q, err=%v", tokEmpty, errEmpty)
	}
}

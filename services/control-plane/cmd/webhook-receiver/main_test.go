package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

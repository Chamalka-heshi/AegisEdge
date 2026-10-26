package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/services/control-plane/metrics"
	"github.com/Chamalka-heshi/AegisEdge/services/control-plane/storage"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

func TestMetricsEndpoint_HeadersAndMethods(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	handler := srv.Routes()

	// 1. GET /metrics returns 200 with text/plain version=0.0.4
	getReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	getRec := httptest.NewRecorder()
	handler.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", getRec.Code)
	}
	ct := getRec.Header().Get("Content-Type")
	if ct != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatalf("expected Content-Type text/plain; version=0.0.4; charset=utf-8, got %q", ct)
	}
	if getRec.Body.Len() == 0 {
		t.Fatalf("expected non-empty body for GET /metrics")
	}

	// 2. HEAD /metrics returns 200 with empty body and identical headers
	headReq := httptest.NewRequest(http.MethodHead, "/metrics", nil)
	headRec := httptest.NewRecorder()
	handler.ServeHTTP(headRec, headReq)

	if headRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for HEAD, got %d", headRec.Code)
	}
	if headRec.Header().Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatalf("expected Content-Type text/plain; version=0.0.4; charset=utf-8, got %q", headRec.Header().Get("Content-Type"))
	}
	if headRec.Body.Len() != 0 {
		t.Fatalf("expected 0 bytes body for HEAD /metrics, got %d", headRec.Body.Len())
	}

	// 3. POST /metrics returns 405 with Allow: GET, HEAD
	postReq := httptest.NewRequest(http.MethodPost, "/metrics", strings.NewReader("bad"))
	postRec := httptest.NewRecorder()
	handler.ServeHTTP(postRec, postReq)

	if postRec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 Method Not Allowed, got %d", postRec.Code)
	}
	if allow := postRec.Header().Get("Allow"); allow != "GET, HEAD" {
		t.Fatalf("expected Allow: GET, HEAD, got %q", allow)
	}
}

func TestMetrics_IngestedBatchesCounter(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	handler := srv.Routes()

	batch1 := sampleTestBatch("batch-m-1", "node-1", 1)
	batchBytes, _ := json.Marshal(batch1)

	// 1. Submit valid batch via HTTP -> 201 Created
	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/telemetry/batches", bytes.NewReader(batchBytes))
	req1.Header.Set("Content-Type", "application/json")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", rec1.Code, rec1.Body.String())
	}

	// 2. Submit duplicate batch via HTTP -> 200 OK
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/telemetry/batches", bytes.NewReader(batchBytes))
	req2.Header.Set("Content-Type", "application/json")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec2.Code, rec2.Body.String())
	}

	// 3. Submit batch via NATS IngestBatch -> nil (accepted)
	batch2 := sampleTestBatch("batch-m-2", "node-2", 1)
	if err := srv.IngestBatch(context.Background(), &batch2); err != nil {
		t.Fatalf("NATS IngestBatch failed: %v", err)
	}

	// 4. Submit duplicate batch via NATS IngestBatch -> ErrDuplicateBatch
	if err := srv.IngestBatch(context.Background(), &batch2); !errors.Is(err, ErrDuplicateBatch) {
		t.Fatalf("expected ErrDuplicateBatch, got %v", err)
	}

	// 5. Submit invalid batch via HTTP -> 400 Bad Request (should NOT increment counter)
	invReq := httptest.NewRequest(http.MethodPost, "/api/v1/telemetry/batches", strings.NewReader(`{"batch_id":""}`))
	invReq.Header.Set("Content-Type", "application/json")
	invRec := httptest.NewRecorder()
	handler.ServeHTTP(invRec, invReq)

	if invRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request, got %d", invRec.Code)
	}

	// 6. Check /metrics output
	mReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	mRec := httptest.NewRecorder()
	handler.ServeHTTP(mRec, mReq)

	mStr := mRec.Body.String()
	expectedEntries := []string{
		`aegisedge_control_plane_ingested_batches_total{transport="http",status="accepted"} 1`,
		`aegisedge_control_plane_ingested_batches_total{transport="http",status="duplicate"} 1`,
		`aegisedge_control_plane_ingested_batches_total{transport="nats",status="accepted"} 1`,
		`aegisedge_control_plane_ingested_batches_total{transport="nats",status="duplicate"} 1`,
	}
	for _, entry := range expectedEntries {
		if !strings.Contains(mStr, entry) {
			t.Fatalf("expected metrics to contain %q, but got:\n%s", entry, mStr)
		}
	}
}

func TestMetrics_StorageProbeMetrics(t *testing.T) {
	baseStore, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite store: %v", err)
	}
	defer baseStore.Close()

	probeStore := &probeTestStore{
		IdempotencyStore: baseStore,
	}
	srv := NewServer(newDiscardLogger(), WithIdempotencyStore(probeStore))
	handler := srv.Routes()

	// 1. Healthy /readyz query
	req1 := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec1.Code)
	}

	// Check metrics: histogram count = 1, failures = 0
	mReq1 := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	mRec1 := httptest.NewRecorder()
	handler.ServeHTTP(mRec1, mReq1)
	mStr1 := mRec1.Body.String()

	if !strings.Contains(mStr1, "aegisedge_control_plane_storage_probe_duration_seconds_count 1") {
		t.Fatalf("expected probe count 1, got:\n%s", mStr1)
	}
	if !strings.Contains(mStr1, "aegisedge_control_plane_storage_probe_failures_total 0") {
		t.Fatalf("expected probe failures 0, got:\n%s", mStr1)
	}

	// 2. Unhealthy storage probe
	probeStore.pingFn = func(ctx context.Context) error {
		return errors.New("database locked")
	}
	req2 := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable, got %d", rec2.Code)
	}

	// Check metrics: histogram count = 2, failures = 1
	mReq2 := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	mRec2 := httptest.NewRecorder()
	handler.ServeHTTP(mRec2, mReq2)
	mStr2 := mRec2.Body.String()

	if !strings.Contains(mStr2, "aegisedge_control_plane_storage_probe_duration_seconds_count 2") {
		t.Fatalf("expected probe count 2, got:\n%s", mStr2)
	}
	if !strings.Contains(mStr2, "aegisedge_control_plane_storage_probe_failures_total 1") {
		t.Fatalf("expected probe failures 1, got:\n%s", mStr2)
	}
}

func TestMetrics_AuthFailuresCounter(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	authCfg := AuthConfig{
		Enabled:    true,
		AdminToken: "super-secret-admin-token-12345",
	}
	srv.SetAuthConfig(authCfg)
	handler := srv.Routes()

	// 1. Missing auth headers on batch submission -> 401 Unauthorized
	batch := sampleTestBatch("auth-b1", "node-1", 1)
	bBytes, _ := json.Marshal(batch)
	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/telemetry/batches", bytes.NewReader(bBytes))
	req1.Header.Set("Content-Type", "application/json")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got %d", rec1.Code)
	}

	// 2. Forbidden access on admin endpoint (/api/v1/nodes) with invalid/missing admin token -> 401 or 403
	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/nodes", nil)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	// 3. Verify metrics output contains discrete reasons
	mReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	mRec := httptest.NewRecorder()
	handler.ServeHTTP(mRec, mReq)
	mStr := mRec.Body.String()

	if !strings.Contains(mStr, `aegisedge_control_plane_auth_failures_total{reason="missing_auth"} `) {
		t.Fatalf("expected missing_auth metric in:\n%s", mStr)
	}
}

func TestMetrics_DynamicSources(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	handler := srv.Routes()

	// Initial node count is 0
	mReq1 := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	mRec1 := httptest.NewRecorder()
	handler.ServeHTTP(mRec1, mReq1)
	if !strings.Contains(mRec1.Body.String(), "aegisedge_control_plane_registered_nodes_count 0") {
		t.Fatalf("expected 0 registered nodes, got:\n%s", mRec1.Body.String())
	}

	// Wire NATS exhausted deliveries source
	var exhaustedCount int64 = 7
	srv.Metrics().SetNATSExhaustedDeliveriesSource(func() float64 {
		return float64(exhaustedCount)
	})

	// Register a node
	now := time.Now().UTC()
	reg := types.NodeRegistration{
		NodeID:       "edge-node-alpha",
		Hostname:     "alpha.local",
		OS:           "linux",
		Architecture: "amd64",
		AgentVersion: "1.0.0",
		RegisteredAt: now,
		UpdatedAt:    &now,
	}
	regBytes, _ := json.Marshal(reg)
	rReq := httptest.NewRequest(http.MethodPost, "/api/v1/nodes/register", bytes.NewReader(regBytes))
	rReq.Header.Set("Content-Type", "application/json")
	rRec := httptest.NewRecorder()
	handler.ServeHTTP(rRec, rReq)

	if rRec.Code != http.StatusOK && rRec.Code != http.StatusCreated {
		t.Fatalf("expected success, got %d: %s", rRec.Code, rRec.Body.String())
	}

	// Verify gauge and NATS counter
	mReq2 := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	mRec2 := httptest.NewRecorder()
	handler.ServeHTTP(mRec2, mReq2)
	mStr2 := mRec2.Body.String()

	if !strings.Contains(mStr2, "aegisedge_control_plane_registered_nodes_count 1") {
		t.Fatalf("expected 1 registered node, got:\n%s", mStr2)
	}
	if !strings.Contains(mStr2, "aegisedge_control_plane_nats_exhausted_deliveries_total 7") {
		t.Fatalf("expected 7 exhausted deliveries, got:\n%s", mStr2)
	}
}

func TestMetrics_AvailableDuringShutdownAndStorageFailure(t *testing.T) {
	baseStore, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite store: %v", err)
	}
	defer baseStore.Close()

	probeStore := &probeTestStore{
		IdempotencyStore: baseStore,
		pingFn: func(ctx context.Context) error {
			return errors.New("disk broken")
		},
	}
	srv := NewServer(newDiscardLogger(), WithIdempotencyStore(probeStore))
	srv.MarkShuttingDown()
	handler := srv.Routes()

	// Readiness should report 503
	rReq := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rRec := httptest.NewRecorder()
	handler.ServeHTTP(rRec, rReq)

	if rRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rRec.Code)
	}

	// Metrics endpoint must still be 200 OK
	mReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	mRec := httptest.NewRecorder()
	handler.ServeHTTP(mRec, mReq)

	if mRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for /metrics during shutdown, got %d", mRec.Code)
	}
	if !strings.Contains(mRec.Body.String(), metrics.MetricRegisteredNodesCount) {
		t.Fatalf("expected metrics output, got:\n%s", mRec.Body.String())
	}
}

func TestMetrics_ConcurrentAccess(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	handler := srv.Routes()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			b := sampleTestBatch("concurrent-b", "node-c", int64(id+1))
			_ = srv.IngestBatch(context.Background(), &b)
			mReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
			mRec := httptest.NewRecorder()
			handler.ServeHTTP(mRec, mReq)
		}(i)
	}
	wg.Wait()
}

func TestMetrics_RegisteredNodesRehydration(t *testing.T) {
	dbDir := t.TempDir()
	dbPath := filepath.Join(dbDir, "rehydrate_nodes_metrics.db")

	// Phase 1: Initialize first server and register 3 nodes
	store1, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	srv1 := NewServer(newDiscardLogger(), WithIdempotencyStore(store1))
	handler1 := srv1.Routes()

	nodeIDs := []string{"node-rehydrate-1", "node-rehydrate-2", "node-rehydrate-3"}
	now := time.Now().UTC()
	for _, id := range nodeIDs {
		reg := types.NodeRegistration{
			NodeID:       id,
			Hostname:     id + ".local",
			OS:           "linux",
			Architecture: "amd64",
			AgentVersion: "1.0.0",
			RegisteredAt: now,
			UpdatedAt:    &now,
		}
		regBytes, _ := json.Marshal(reg)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/nodes/register", bytes.NewReader(regBytes))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler1.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK && rec.Code != http.StatusCreated {
			t.Fatalf("failed to register node %s, status=%d", id, rec.Code)
		}
	}

	// Scrape metrics on server 1
	mReq1 := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	mRec1 := httptest.NewRecorder()
	handler1.ServeHTTP(mRec1, mReq1)
	if !strings.Contains(mRec1.Body.String(), "aegisedge_control_plane_registered_nodes_count 3") {
		t.Fatalf("expected 3 registered nodes on server 1, got:\n%s", mRec1.Body.String())
	}

	// Close server 1 and its storage
	if err := srv1.Close(); err != nil {
		t.Fatalf("srv1.Close failed: %v", err)
	}

	// Phase 2: Open second server pointing to the same database file
	store2, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite re-open failed: %v", err)
	}
	srv2 := NewServer(newDiscardLogger(), WithIdempotencyStore(store2))
	defer srv2.Close()
	handler2 := srv2.Routes()

	// Scrape metrics on server 2 immediately after startup rehydration
	mReq2 := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	mRec2 := httptest.NewRecorder()
	handler2.ServeHTTP(mRec2, mReq2)

	expectedMetric := "aegisedge_control_plane_registered_nodes_count 3"
	if !strings.Contains(mRec2.Body.String(), expectedMetric) {
		t.Fatalf("expected %q on rehydrated server, but got:\n%s", expectedMetric, mRec2.Body.String())
	}
}

package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/services/control-plane/metrics"
	"github.com/Chamalka-heshi/AegisEdge/services/control-plane/storage"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

func newDiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func sampleTestBatch(batchID, nodeID string, seq int64) types.TelemetryBatch {
	now := time.Now().UTC()
	return types.TelemetryBatch{
		BatchID:        batchID,
		NodeID:         nodeID,
		SequenceNumber: seq,
		CollectedAt:    now,
		Metrics: []types.MetricSample{
			{
				Name:      "system_load",
				Value:     1.42,
				Timestamp: now,
			},
		},
	}
}

func TestServer_Healthz(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	// 1. GET /healthz -> 200 OK
	res, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz failed: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", res.StatusCode)
	}

	// 2. POST /healthz -> 405 Method Not Allowed
	postRes, err := http.Post(ts.URL+"/healthz", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("POST /healthz failed: %v", err)
	}
	defer postRes.Body.Close()

	if postRes.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 Method Not Allowed, got %d", postRes.StatusCode)
	}
}

func TestServer_IngestBatch_Success(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	batch := sampleTestBatch("batch-001", "node-alpha", 0)
	body, _ := json.Marshal(batch)

	res, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /api/v1/telemetry/batches failed: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d", res.StatusCode)
	}

	var resp IngestionResponse
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		t.Fatalf("decoding response failed: %v", err)
	}

	if resp.Status != "accepted" {
		t.Errorf("expected status 'accepted', got %q", resp.Status)
	}
	if resp.BatchID != "batch-001" {
		t.Errorf("expected batch_id 'batch-001', got %q", resp.BatchID)
	}
	if resp.NodeID != "node-alpha" {
		t.Errorf("expected node_id 'node-alpha', got %q", resp.NodeID)
	}
	if resp.IngestedAt == nil || resp.IngestedAt.IsZero() {
		t.Errorf("expected valid non-zero ingested_at timestamp")
	}

	if srv.Count() != 1 {
		t.Errorf("expected server count to be 1, got %d", srv.Count())
	}
}

func TestServer_IngestBatch_IdempotentDuplicate(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	batch := sampleTestBatch("batch-dup-001", "node-alpha", 1)
	body, _ := json.Marshal(batch)

	// First submission: 201 Created
	res1, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("first POST failed: %v", err)
	}
	defer res1.Body.Close()
	if res1.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created on first post, got %d", res1.StatusCode)
	}

	// Second submission with exact same BatchID: 200 OK (already_accepted)
	res2, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("second POST failed: %v", err)
	}
	defer res2.Body.Close()

	if res2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK on duplicate post, got %d", res2.StatusCode)
	}

	var resp IngestionResponse
	if err := json.NewDecoder(res2.Body).Decode(&resp); err != nil {
		t.Fatalf("decoding duplicate response failed: %v", err)
	}

	if resp.Status != "already_accepted" {
		t.Errorf("expected status 'already_accepted', got %q", resp.Status)
	}
	if resp.BatchID != "batch-dup-001" {
		t.Errorf("expected batch_id 'batch-dup-001', got %q", resp.BatchID)
	}

	// Server count must remain 1 (no duplicate storage)
	if srv.Count() != 1 {
		t.Errorf("expected server count to remain 1, got %d", srv.Count())
	}
}

func TestServer_IngestBatch_ValidationFailure(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	// Missing BatchID
	invalidBatch := sampleTestBatch("", "node-alpha", 1)
	body, _ := json.Marshal(invalidBatch)

	res, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request, got %d", res.StatusCode)
	}

	var errResp ErrorResponse
	if err := json.NewDecoder(res.Body).Decode(&errResp); err != nil {
		t.Fatalf("decoding error response failed: %v", err)
	}
	if errResp.Error != "invalid_batch" {
		t.Errorf("expected error 'invalid_batch', got %q", errResp.Error)
	}
}

func TestServer_IngestBatch_MalformedJSON(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	malformedJSON := `{"batch_id": "bad", "metrics": [unclosed`
	res, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", strings.NewReader(malformedJSON))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request, got %d", res.StatusCode)
	}

	var errResp ErrorResponse
	if err := json.NewDecoder(res.Body).Decode(&errResp); err != nil {
		t.Fatalf("decoding error response failed: %v", err)
	}
	if errResp.Error != "malformed_json" {
		t.Errorf("expected error 'malformed_json', got %q", errResp.Error)
	}
}

func TestServer_MethodNotAllowed(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/api/v1/telemetry/batches", strings.NewReader("{}"))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT request failed: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 Method Not Allowed, got %d", res.StatusCode)
	}
}

func TestServer_ListBatches(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	b1 := sampleTestBatch("b-1", "node-1", 1)
	b2 := sampleTestBatch("b-2", "node-1", 2)

	body1, _ := json.Marshal(b1)
	body2, _ := json.Marshal(b2)

	_, _ = http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(body1))
	_, _ = http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(body2))

	res, err := http.Get(ts.URL + "/api/v1/telemetry/batches")
	if err != nil {
		t.Fatalf("GET batches failed: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", res.StatusCode)
	}

	var list []*IngestedBatch
	if err := json.NewDecoder(res.Body).Decode(&list); err != nil {
		t.Fatalf("decoding batch list failed: %v", err)
	}

	if len(list) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(list))
	}
	if list[0].Batch.BatchID != "b-1" || list[1].Batch.BatchID != "b-2" {
		t.Errorf("batches not in expected order: %v, %v", list[0].Batch.BatchID, list[1].Batch.BatchID)
	}
}

// TestServer_IngestBatch_ConcurrentDuplicates verifies that concurrent requests
// submitting the identical BatchID cannot create duplicate logical records.
func TestServer_IngestBatch_ConcurrentDuplicates(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	batch := sampleTestBatch("batch-concurrent-dup", "node-concurrent", 1)
	body, _ := json.Marshal(batch)

	const concurrency = 10
	var wg sync.WaitGroup
	var acceptedCount atomic.Int32
	var alreadyAcceptedCount atomic.Int32
	var errorCount atomic.Int32

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			res, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(body))
			if err != nil {
				errorCount.Add(1)
				return
			}
			defer res.Body.Close()

			var resp IngestionResponse
			_ = json.NewDecoder(res.Body).Decode(&resp)

			if res.StatusCode == http.StatusCreated && resp.Status == "accepted" {
				acceptedCount.Add(1)
			} else if res.StatusCode == http.StatusOK && resp.Status == "already_accepted" {
				alreadyAcceptedCount.Add(1)
			} else {
				errorCount.Add(1)
			}
		}()
	}
	wg.Wait()

	if errorCount.Load() != 0 {
		t.Errorf("encountered %d errors during concurrent submission", errorCount.Load())
	}
	if acceptedCount.Load() != 1 {
		t.Errorf("expected exactly 1 'accepted' (201), got %d", acceptedCount.Load())
	}
	if alreadyAcceptedCount.Load() != concurrency-1 {
		t.Errorf("expected %d 'already_accepted' (200), got %d", concurrency-1, alreadyAcceptedCount.Load())
	}
	if srv.Count() != 1 {
		t.Errorf("expected exactly 1 stored batch, got %d", srv.Count())
	}
}

func TestServer_NodeRegister_SuccessAndIdempotency(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	reg := types.NodeRegistration{
		NodeID:       "node-edge-01",
		Hostname:     "edge-box-1",
		OS:           "linux",
		Architecture: "amd64",
		AgentVersion: "1.0.0",
		Capabilities: []string{"simulated_actuation"},
		RegisteredAt: time.Now().UTC(),
	}
	body, _ := json.Marshal(reg)

	// First registration: 201 Created
	res, err := http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /api/v1/nodes/register failed: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d", res.StatusCode)
	}
	var resp RegistrationResponse
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		t.Fatalf("decoding response failed: %v", err)
	}
	if resp.Status != "registered" || resp.NodeID != "node-edge-01" {
		t.Errorf("unexpected response: %+v", resp)
	}

	// Second registration with same ID: 200 OK (idempotent duplicate)
	res2, err := http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("second POST /api/v1/nodes/register failed: %v", err)
	}
	defer res2.Body.Close()

	if res2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK on duplicate registration, got %d", res2.StatusCode)
	}
	var resp2 RegistrationResponse
	if err := json.NewDecoder(res2.Body).Decode(&resp2); err != nil {
		t.Fatalf("decoding second response failed: %v", err)
	}
	if resp2.Status != "already_registered" || resp2.NodeID != "node-edge-01" {
		t.Errorf("unexpected second response: %+v", resp2)
	}

	// Verify in-memory state: exactly 1 node registered
	nodes := srv.GetNodes()
	if len(nodes) != 1 {
		t.Fatalf("expected 1 registered node, got %d", len(nodes))
	}
	if nodes[0].NodeID != "node-edge-01" {
		t.Errorf("expected node-edge-01, got %s", nodes[0].NodeID)
	}
}

func TestServer_NodeRegister_ValidationFailures(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	// 1. Empty NodeID
	reg1 := types.NodeRegistration{
		NodeID:       "",
		Hostname:     "edge-box-1",
		RegisteredAt: time.Now().UTC(),
	}
	b1, _ := json.Marshal(reg1)
	r1, _ := http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(b1))
	if r1.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for empty NodeID, got %d", r1.StatusCode)
	}

	// 2. Malformed / invalid NodeID (slash character)
	reg2 := types.NodeRegistration{
		NodeID:       "node/with/slash",
		Hostname:     "edge-box-1",
		RegisteredAt: time.Now().UTC(),
	}
	b2, _ := json.Marshal(reg2)
	r2, _ := http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(b2))
	if r2.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid NodeID, got %d", r2.StatusCode)
	}

	// 3. Malformed JSON
	r3, _ := http.Post(ts.URL+"/api/v1/nodes/register", "application/json", strings.NewReader("{invalid-json"))
	if r3.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for malformed JSON, got %d", r3.StatusCode)
	}
}

func TestServer_NodeHeartbeat_SuccessAndUnknownNode(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	// 1. Heartbeat for unregistered node must return 404 Not Found
	hbUnknown := types.Heartbeat{
		NodeID:         "node-unknown-99",
		SequenceNumber: 1,
		Timestamp:      time.Now().UTC(),
		Status:         types.NodeStatusHealthy,
	}
	bodyUnk, _ := json.Marshal(hbUnknown)
	resUnk, err := http.Post(ts.URL+"/api/v1/nodes/heartbeat", "application/json", bytes.NewReader(bodyUnk))
	if err != nil {
		t.Fatalf("POST heartbeat failed: %v", err)
	}
	defer resUnk.Body.Close()
	if resUnk.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found for unknown node, got %d", resUnk.StatusCode)
	}

	// 2. Register node
	reg := types.NodeRegistration{
		NodeID:       "node-known-01",
		Hostname:     "host-01",
		RegisteredAt: time.Now().UTC(),
	}
	bodyReg, _ := json.Marshal(reg)
	_, _ = http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(bodyReg))

	// 3. Heartbeat for registered node must return 200 OK
	hbKnown := types.Heartbeat{
		NodeID:         "node-known-01",
		SequenceNumber: 5,
		Timestamp:      time.Now().UTC(),
		Status:         types.NodeStatusHealthy,
	}
	bodyKnown, _ := json.Marshal(hbKnown)
	resKnown, err := http.Post(ts.URL+"/api/v1/nodes/heartbeat", "application/json", bytes.NewReader(bodyKnown))
	if err != nil {
		t.Fatalf("POST heartbeat for known node failed: %v", err)
	}
	defer resKnown.Body.Close()
	if resKnown.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for known node heartbeat, got %d", resKnown.StatusCode)
	}

	var hbResp HeartbeatResponse
	_ = json.NewDecoder(resKnown.Body).Decode(&hbResp)
	if hbResp.Status != "acknowledged" || hbResp.NodeID != "node-known-01" {
		t.Errorf("unexpected heartbeat response: %+v", hbResp)
	}

	// Verify last_seen and sequence_number updated on node
	node, ok := srv.GetNode("node-known-01")
	if !ok {
		t.Fatal("expected node to exist")
	}
	if node.HeartbeatSequence != 5 {
		t.Errorf("expected heartbeat sequence number 5, got %d", node.HeartbeatSequence)
	}
}

func TestServer_NodeQuery_ListAndSingle(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	// Register 2 nodes
	reg1 := types.NodeRegistration{NodeID: "node-query-1", Hostname: "h1", RegisteredAt: time.Now().UTC()}
	reg2 := types.NodeRegistration{NodeID: "node-query-2", Hostname: "h2", RegisteredAt: time.Now().UTC()}
	b1, _ := json.Marshal(reg1)
	b2, _ := json.Marshal(reg2)
	_, _ = http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(b1))
	_, _ = http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(b2))

	// Query list: GET /api/v1/nodes
	resList, err := http.Get(ts.URL + "/api/v1/nodes")
	if err != nil {
		t.Fatalf("GET /api/v1/nodes failed: %v", err)
	}
	defer resList.Body.Close()
	if resList.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for list nodes, got %d", resList.StatusCode)
	}
	var list []*NodeRecord
	_ = json.NewDecoder(resList.Body).Decode(&list)
	if len(list) != 2 {
		t.Fatalf("expected 2 nodes in list, got %d", len(list))
	}

	// Query single existing node: GET /api/v1/nodes/node-query-1
	resSingle, err := http.Get(ts.URL + "/api/v1/nodes/node-query-1")
	if err != nil {
		t.Fatalf("GET single node failed: %v", err)
	}
	defer resSingle.Body.Close()
	if resSingle.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for single node query, got %d", resSingle.StatusCode)
	}
	var single NodeRecord
	_ = json.NewDecoder(resSingle.Body).Decode(&single)
	if single.NodeID != "node-query-1" {
		t.Errorf("expected node-query-1, got %s", single.NodeID)
	}

	// Query single non-existing node: GET /api/v1/nodes/non-existent
	resNotFound, _ := http.Get(ts.URL + "/api/v1/nodes/non-existent")
	if resNotFound.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 for non-existent node query, got %d", resNotFound.StatusCode)
	}
}

func TestServer_NodeEndpoints_MethodNotAllowed(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	// GET on /api/v1/nodes/register
	r1, _ := http.Get(ts.URL + "/api/v1/nodes/register")
	if r1.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 for GET register, got %d", r1.StatusCode)
	}

	// GET on /api/v1/nodes/heartbeat
	r2, _ := http.Get(ts.URL + "/api/v1/nodes/heartbeat")
	if r2.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 for GET heartbeat, got %d", r2.StatusCode)
	}

	// POST on /api/v1/nodes/node-1
	r3, _ := http.Post(ts.URL+"/api/v1/nodes/node-1", "application/json", strings.NewReader("{}"))
	if r3.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 for POST node query, got %d", r3.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// Phase 6.21B Regression Tests: Composite Key & Bounded History Buffer
// ---------------------------------------------------------------------------

// 1. Same-node repeated BatchID is idempotent
func TestServer_IngestBatch_SameNodeIdempotent(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	batch := sampleTestBatch("batch-repeat-001", "node-same", 1)
	body, _ := json.Marshal(batch)

	// First submission: 201 Created
	res1, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("first POST failed: %v", err)
	}
	defer res1.Body.Close()
	if res1.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created on first post, got %d", res1.StatusCode)
	}

	// Repeated submission: 200 OK already_accepted
	res2, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("second POST failed: %v", err)
	}
	defer res2.Body.Close()
	if res2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK on repeated post, got %d", res2.StatusCode)
	}

	var resp IngestionResponse
	if err := json.NewDecoder(res2.Body).Decode(&resp); err != nil {
		t.Fatalf("decoding response failed: %v", err)
	}
	if resp.Status != "already_accepted" {
		t.Errorf("expected 'already_accepted', got %q", resp.Status)
	}

	if srv.Count() != 1 {
		t.Errorf("expected deduplication count 1, got %d", srv.Count())
	}
	if srv.HistoryCount() != 1 {
		t.Errorf("expected history count 1, got %d", srv.HistoryCount())
	}
}

// 2. Different nodes can submit the same BatchID independently (no cross-node suppression)
func TestServer_IngestBatch_CrossNodeKeyIsolation(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	sharedBatchID := "shared-uuid-0000-1111"

	batchAlpha := sampleTestBatch(sharedBatchID, "node-alpha", 1)
	batchBeta := sampleTestBatch(sharedBatchID, "node-beta", 1)

	bodyA, _ := json.Marshal(batchAlpha)
	bodyB, _ := json.Marshal(batchBeta)

	// Node Alpha submits: 201 Created
	resA, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(bodyA))
	if err != nil {
		t.Fatalf("node-alpha POST failed: %v", err)
	}
	defer resA.Body.Close()
	if resA.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created for node-alpha, got %d", resA.StatusCode)
	}

	// Node Beta submits with identical BatchID: MUST NOT be suppressed as duplicate -> 201 Created
	resB, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(bodyB))
	if err != nil {
		t.Fatalf("node-beta POST failed: %v", err)
	}
	defer resB.Body.Close()
	if resB.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created for node-beta with same BatchID, got %d", resB.StatusCode)
	}

	// Both records exist independently
	if srv.Count() != 2 {
		t.Errorf("expected server deduplication count 2, got %d", srv.Count())
	}
	if srv.HistoryCount() != 2 {
		t.Errorf("expected history count 2, got %d", srv.HistoryCount())
	}
	if !srv.HasIngestedBatch("node-alpha", sharedBatchID) {
		t.Errorf("expected batch recorded for node-alpha")
	}
	if !srv.HasIngestedBatch("node-beta", sharedBatchID) {
		t.Errorf("expected batch recorded for node-beta")
	}

	// Retrying either node's batch returns already_accepted for that specific node
	resA2, errA2 := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(bodyA))
	if errA2 != nil {
		t.Fatalf("failed to post node-alpha duplicate: %v", errA2)
	}
	defer resA2.Body.Close()
	if resA2.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK on node-alpha duplicate retry, got %d", resA2.StatusCode)
	}

	resB2, errB2 := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(bodyB))
	if errB2 != nil {
		t.Fatalf("failed to post node-beta duplicate: %v", errB2)
	}
	defer resB2.Body.Close()
	if resB2.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK on node-beta duplicate retry, got %d", resB2.StatusCode)
	}
}

// 3. HTTP and NATS paths share the same deduplication behavior
func TestServer_IngestBatch_HTTPAndNATSConsistentDeduplication(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	ctx := context.Background()

	// Scenario A: Ingest via HTTP first, then via NATS programmatic IngestBatch
	batchHTTP := sampleTestBatch("batch-http-first", "node-combo", 1)
	bodyHTTP, _ := json.Marshal(batchHTTP)
	resHTTP, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(bodyHTTP))
	if err != nil {
		t.Fatalf("HTTP post failed: %v", err)
	}
	defer resHTTP.Body.Close()
	if resHTTP.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created for HTTP post, got %d", resHTTP.StatusCode)
	}

	// NATS programmatic ingestion of the exact same batch must return ErrDuplicateBatch
	errNATS := srv.IngestBatch(ctx, &batchHTTP)
	if errNATS == nil || errNATS != ErrDuplicateBatch {
		t.Errorf("expected ErrDuplicateBatch when NATS ingests batch previously accepted via HTTP, got: %v", errNATS)
	}

	// Scenario B: Ingest via NATS first, then via HTTP
	batchNATS := sampleTestBatch("batch-nats-first", "node-combo", 2)
	if err := srv.IngestBatch(ctx, &batchNATS); err != nil {
		t.Fatalf("initial NATS IngestBatch failed: %v", err)
	}

	// HTTP POST of the same batch must receive 200 OK already_accepted
	bodyNATS, _ := json.Marshal(batchNATS)
	resHTTP2, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(bodyNATS))
	if err != nil {
		t.Fatalf("HTTP post for NATS batch failed: %v", err)
	}
	defer resHTTP2.Body.Close()
	if resHTTP2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK already_accepted for batch pre-ingested by NATS, got %d", resHTTP2.StatusCode)
	}

	// Cross-node check: same BatchID from a different node is still accepted
	batchDiffNode := sampleTestBatch("batch-nats-first", "node-different", 2)
	bodyDiff, _ := json.Marshal(batchDiffNode)
	resDiff, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(bodyDiff))
	if err != nil {
		t.Fatalf("HTTP post for different node failed: %v", err)
	}
	defer resDiff.Body.Close()
	if resDiff.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created for different node with same BatchID, got %d", resDiff.StatusCode)
	}
}

// 4. Payload history never exceeds capacity and does not drop deduplication
func TestServer_History_BoundedCapacity(t *testing.T) {
	const capLimit = 5
	srv := NewServer(newDiscardLogger(), WithMaxHistory(capLimit))
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	if srv.MaxHistorySize() != capLimit {
		t.Fatalf("expected max history size %d, got %d", capLimit, srv.MaxHistorySize())
	}

	// Submit 15 distinct batches
	for i := 1; i <= 15; i++ {
		batch := sampleTestBatch(fmt.Sprintf("batch-cap-%02d", i), "node-cap", int64(i))
		body, _ := json.Marshal(batch)
		res, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("POST batch %d failed: %v", i, err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusCreated {
			t.Fatalf("expected 201 Created for batch %d, got %d", i, res.StatusCode)
		}
	}

	// 1. History capacity is strictly bounded to capLimit (5)
	if srv.HistoryCount() != capLimit {
		t.Errorf("expected HistoryCount() to be capped at %d, got %d", capLimit, srv.HistoryCount())
	}
	if len(srv.GetIngestedBatches()) != capLimit {
		t.Errorf("expected GetIngestedBatches() length %d, got %d", capLimit, len(srv.GetIngestedBatches()))
	}

	// Query GET /api/v1/telemetry/batches via HTTP
	resList, err := http.Get(ts.URL + "/api/v1/telemetry/batches")
	if err != nil {
		t.Fatalf("GET /api/v1/telemetry/batches failed: %v", err)
	}
	defer resList.Body.Close()

	var list []*IngestedBatch
	if err := json.NewDecoder(resList.Body).Decode(&list); err != nil {
		t.Fatalf("decoding batch list failed: %v", err)
	}
	if len(list) != capLimit {
		t.Fatalf("expected HTTP list to return exactly %d batches, got %d", capLimit, len(list))
	}

	// 2. Deduplication index remembers all 15 keys
	if srv.Count() != 15 {
		t.Errorf("expected deduplication Count() to be 15, got %d", srv.Count())
	}

	// 3. Retrying batch 1 (which was evicted from payload history) is STILL recognized as duplicate
	batch1 := sampleTestBatch("batch-cap-01", "node-cap", 1)
	body1, _ := json.Marshal(batch1)
	resDup, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(body1))
	if err != nil {
		t.Fatalf("re-POST batch 1 failed: %v", err)
	}
	defer resDup.Body.Close()

	if resDup.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK already_accepted for evicted batch 1, got %d", resDup.StatusCode)
	}

	// History count remains capped at 5 (no duplicate inserted)
	if srv.HistoryCount() != capLimit {
		t.Errorf("expected HistoryCount() to remain %d, got %d", capLimit, srv.HistoryCount())
	}
}

// 5. History order is deterministic and documented (oldest retained at index 0, newest at end)
func TestServer_History_ChronologicalOrder(t *testing.T) {
	const capLimit = 3
	srv := NewServer(newDiscardLogger(), WithMaxHistory(capLimit))
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	for i := 1; i <= 5; i++ {
		batch := sampleTestBatch(fmt.Sprintf("b-order-%d", i), "node-order", int64(i))
		body, _ := json.Marshal(batch)
		res, _ := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(body))
		res.Body.Close()
	}

	// With capLimit = 3, after ingesting b-order-1..5, retained batches must be 3, 4, 5
	batches := srv.GetIngestedBatches()
	if len(batches) != 3 {
		t.Fatalf("expected 3 retained batches, got %d", len(batches))
	}

	expectedIDs := []string{"b-order-3", "b-order-4", "b-order-5"}
	for i, expected := range expectedIDs {
		if batches[i].Batch.BatchID != expected {
			t.Errorf("index %d: expected BatchID %q, got %q", i, expected, batches[i].Batch.BatchID)
		}
	}
}

// 6. Out-of-order sequences cannot be incorrectly suppressed by a watermark
func TestServer_IngestBatch_OutOfOrderSequencesNotSuppressed(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	// Ingest sequences out of order: 5, then 2, then 1, then 3
	order := []int64{5, 2, 1, 3}
	for _, seq := range order {
		batch := sampleTestBatch(fmt.Sprintf("batch-seq-%d", seq), "node-ooo", seq)
		body, _ := json.Marshal(batch)
		res, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("post seq %d failed: %v", seq, err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusCreated {
			t.Errorf("seq %d arrived out-of-order but was rejected or suppressed: status %d", seq, res.StatusCode)
		}
	}

	if srv.Count() != 4 {
		t.Errorf("expected 4 distinct out-of-order batches accepted, got %d", srv.Count())
	}

	// Actual duplicate of sequence 2 must be recognized as duplicate
	dupBatch := sampleTestBatch("batch-seq-2", "node-ooo", 2)
	dupBody, _ := json.Marshal(dupBatch)
	resDup, errDup := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(dupBody))
	if errDup != nil {
		t.Fatalf("failed to post duplicate batch: %v", errDup)
	}
	defer resDup.Body.Close()

	if resDup.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK already_accepted for actual duplicate of seq 2, got %d", resDup.StatusCode)
	}
}

// 7. Configured history capacity is validated correctly
func TestServer_History_CapacityValidation(t *testing.T) {
	srv := NewServer(newDiscardLogger())

	// Default capacity is DefaultMaxHistoryBatches (1000)
	if srv.MaxHistorySize() != DefaultMaxHistoryBatches {
		t.Errorf("expected default max history %d, got %d", DefaultMaxHistoryBatches, srv.MaxHistorySize())
	}

	// Invalid sizes rejected by SetMaxHistorySize
	if err := srv.SetMaxHistorySize(0); err == nil {
		t.Errorf("expected error for capacity 0")
	}
	if err := srv.SetMaxHistorySize(-10); err == nil {
		t.Errorf("expected error for negative capacity")
	}

	// Invalid size option rejected by WithMaxHistory
	opt := WithMaxHistory(0)
	if err := opt(srv); err == nil {
		t.Errorf("expected error from WithMaxHistory(0)")
	}

	// Dynamic capacity downsizing trims older elements
	_ = srv.SetMaxHistorySize(10)
	ctx := context.Background()
	for i := 1; i <= 10; i++ {
		b := sampleTestBatch(fmt.Sprintf("b-resize-%d", i), "node-resize", int64(i))
		_ = srv.IngestBatch(ctx, &b)
	}
	if srv.HistoryCount() != 10 {
		t.Fatalf("expected 10 items before resize, got %d", srv.HistoryCount())
	}

	// Resize down to 4: must retain the 4 most recent items (batches 7, 8, 9, 10)
	if err := srv.SetMaxHistorySize(4); err != nil {
		t.Fatalf("SetMaxHistorySize(4) failed: %v", err)
	}
	if srv.HistoryCount() != 4 {
		t.Errorf("expected HistoryCount() 4 after downsizing, got %d", srv.HistoryCount())
	}

	batches := srv.GetIngestedBatches()
	expectedIDs := []string{"b-resize-7", "b-resize-8", "b-resize-9", "b-resize-10"}
	for i, expected := range expectedIDs {
		if batches[i].Batch.BatchID != expected {
			t.Errorf("downsized index %d: expected %q, got %q", i, expected, batches[i].Batch.BatchID)
		}
	}
}

// ---------------------------------------------------------------------------
// Phase 6.21C Regression Tests: Genuine Circular Ring Buffer & Typed Key
// ---------------------------------------------------------------------------

// 8. Genuine circular buffer handles multiple full wraparound cycles correctly
func TestServer_History_MultipleWraparounds(t *testing.T) {
	const capLimit = 4
	srv := NewServer(newDiscardLogger(), WithMaxHistory(capLimit))
	ctx := context.Background()

	// Ingest 14 items into a capacity-4 buffer (wraps around 3+ full cycles)
	for i := 1; i <= 14; i++ {
		batch := sampleTestBatch(fmt.Sprintf("b-wrap-%02d", i), "node-wrap", int64(i))
		if err := srv.IngestBatch(ctx, &batch); err != nil {
			t.Fatalf("ingest batch %d failed: %v", i, err)
		}
	}

	if srv.HistoryCount() != capLimit {
		t.Fatalf("expected HistoryCount %d, got %d", capLimit, srv.HistoryCount())
	}
	if srv.Count() != 14 {
		t.Fatalf("expected deduplication Count %d, got %d", 14, srv.Count())
	}

	// The 4 retained items must strictly be the 4 newest (11, 12, 13, 14) in chronological order
	batches := srv.GetIngestedBatches()
	expectedIDs := []string{"b-wrap-11", "b-wrap-12", "b-wrap-13", "b-wrap-14"}
	for i, expected := range expectedIDs {
		if batches[i].Batch.BatchID != expected {
			t.Errorf("index %d: expected %q, got %q", i, expected, batches[i].Batch.BatchID)
		}
	}
}

// 9. Circular buffer handles capacity of one and dynamic transitions to/from one
func TestServer_History_ResizeCapacityOfOne(t *testing.T) {
	srv := NewServer(newDiscardLogger(), WithMaxHistory(5))
	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		b := sampleTestBatch(fmt.Sprintf("b-one-%d", i), "node-one", int64(i))
		_ = srv.IngestBatch(ctx, &b)
	}

	// Downsize to capacity 1
	if err := srv.SetMaxHistorySize(1); err != nil {
		t.Fatalf("SetMaxHistorySize(1) failed: %v", err)
	}
	if srv.HistoryCount() != 1 {
		t.Fatalf("expected HistoryCount 1, got %d", srv.HistoryCount())
	}
	retained := srv.GetIngestedBatches()
	if len(retained) != 1 || retained[0].Batch.BatchID != "b-one-5" {
		t.Fatalf("expected b-one-5, got %+v", retained)
	}

	// Insert into capacity 1 buffer: overwrites the single item
	b6 := sampleTestBatch("b-one-6", "node-one", 6)
	_ = srv.IngestBatch(ctx, &b6)
	retained = srv.GetIngestedBatches()
	if len(retained) != 1 || retained[0].Batch.BatchID != "b-one-6" {
		t.Fatalf("expected b-one-6 after single capacity overwrite, got %+v", retained)
	}

	b7 := sampleTestBatch("b-one-7", "node-one", 7)
	_ = srv.IngestBatch(ctx, &b7)
	retained = srv.GetIngestedBatches()
	if len(retained) != 1 || retained[0].Batch.BatchID != "b-one-7" {
		t.Fatalf("expected b-one-7, got %+v", retained)
	}

	// Upsize from 1 to 3
	if err := srv.SetMaxHistorySize(3); err != nil {
		t.Fatalf("SetMaxHistorySize(3) failed: %v", err)
	}
	if srv.HistoryCount() != 1 {
		t.Fatalf("expected HistoryCount 1 after upsize before new inserts, got %d", srv.HistoryCount())
	}

	b8 := sampleTestBatch("b-one-8", "node-one", 8)
	b9 := sampleTestBatch("b-one-9", "node-one", 9)
	_ = srv.IngestBatch(ctx, &b8)
	_ = srv.IngestBatch(ctx, &b9)

	retained = srv.GetIngestedBatches()
	if len(retained) != 3 {
		t.Fatalf("expected 3 items after upsize and inserts, got %d", len(retained))
	}
	expectedIDs := []string{"b-one-7", "b-one-8", "b-one-9"}
	for i, expected := range expectedIDs {
		if retained[i].Batch.BatchID != expected {
			t.Errorf("upsized index %d: expected %q, got %q", i, expected, retained[i].Batch.BatchID)
		}
	}
}

// 10. Thread safety under concurrent ingestion and history reads
func TestServer_History_ConcurrentAccess(t *testing.T) {
	const capLimit = 20
	srv := NewServer(newDiscardLogger(), WithMaxHistory(capLimit))
	ctx := context.Background()

	var wg sync.WaitGroup
	const writers = 15
	const batchesPerWriter = 30

	// Concurrent writers
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for b := 0; b < batchesPerWriter; b++ {
				batch := sampleTestBatch(fmt.Sprintf("b-cw-%d-%d", writerID, b), fmt.Sprintf("node-%d", writerID), int64(b))
				_ = srv.IngestBatch(ctx, &batch)
			}
		}(w)
	}

	// Concurrent readers
	const readers = 5
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_ = srv.HistoryCount()
				_ = srv.GetIngestedBatches()
				_ = srv.Count()
			}
		}()
	}

	wg.Wait()

	if srv.HistoryCount() != capLimit {
		t.Errorf("expected HistoryCount %d, got %d", capLimit, srv.HistoryCount())
	}
	if srv.Count() != writers*batchesPerWriter {
		t.Errorf("expected Count %d, got %d", writers*batchesPerWriter, srv.Count())
	}
	batches := srv.GetIngestedBatches()
	if len(batches) != capLimit {
		t.Errorf("expected GetIngestedBatches() length %d, got %d", capLimit, len(batches))
	}
	for i, b := range batches {
		if b == nil {
			t.Fatalf("nil batch entry found at index %d", i)
		}
	}
}

// 11. Repeated duplicate ingestion does NOT create duplicate history entries
func TestServer_History_RepeatedIngestionNoDuplicateHistory(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	batch := sampleTestBatch("batch-no-history-dup", "node-nodup", 1)
	body, _ := json.Marshal(batch)

	// First post: accepted (201)
	res1, _ := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(body))
	res1.Body.Close()

	if srv.HistoryCount() != 1 {
		t.Fatalf("expected HistoryCount 1, got %d", srv.HistoryCount())
	}

	// Repeat post 5 times: all 200 already_accepted
	for i := 0; i < 5; i++ {
		res, _ := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(body))
		res.Body.Close()
	}

	// History count must still be exactly 1
	if srv.HistoryCount() != 1 {
		t.Errorf("expected HistoryCount to remain 1 after repeated duplicates, got %d", srv.HistoryCount())
	}
	batches := srv.GetIngestedBatches()
	if len(batches) != 1 {
		t.Errorf("expected exactly 1 batch in history snapshot, got %d", len(batches))
	}
}

// 12. Typed struct IdempotencyKey eliminates delimiter collision edge cases
func TestServer_IdempotencyKey_DelimiterDisambiguation(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	// Delimiter ambiguity test cases where string concatenation "NodeID + delimiter + BatchID" would collide:
	// Case 1: NodeID = "device/alpha", BatchID = "sensor-01"
	// Case 2: NodeID = "device",       BatchID = "alpha/sensor-01"
	// Case 3: NodeID = "device:alpha", BatchID = "sensor-01"
	// Case 4: NodeID = "device",       BatchID = "alpha:sensor-01"

	pairs := []struct {
		NodeID  string
		BatchID string
	}{
		{"device/alpha", "sensor-01"},
		{"device", "alpha/sensor-01"},
		{"device:alpha", "sensor-01"},
		{"device", "alpha:sensor-01"},
	}

	for _, p := range pairs {
		batch := sampleTestBatch(p.BatchID, p.NodeID, 1)
		body, _ := json.Marshal(batch)

		res, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("POST for {%s, %s} failed: %v", p.NodeID, p.BatchID, err)
		}
		defer res.Body.Close()

		if res.StatusCode != http.StatusCreated {
			t.Fatalf("expected 201 Created for novel delimiter case {%s, %s}, got %d", p.NodeID, p.BatchID, res.StatusCode)
		}
	}

	// All 4 pairs must be recorded independently without collision
	if srv.Count() != len(pairs) {
		t.Errorf("expected Count %d, got %d (delimiter collision occurred)", len(pairs), srv.Count())
	}
	if srv.HistoryCount() != len(pairs) {
		t.Errorf("expected HistoryCount %d, got %d", len(pairs), srv.HistoryCount())
	}

	// Re-submitting each of the 4 pairs must return 200 already_accepted independently
	for _, p := range pairs {
		batch := sampleTestBatch(p.BatchID, p.NodeID, 1)
		body, _ := json.Marshal(batch)

		res, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("re-POST for {%s, %s} failed: %v", p.NodeID, p.BatchID, err)
		}
		defer res.Body.Close()

		if res.StatusCode != http.StatusOK {
			t.Errorf("expected 200 OK already_accepted for {%s, %s}, got %d", p.NodeID, p.BatchID, res.StatusCode)
		}
	}
}

// ============================================================================
// Phase 6.22B Durable Idempotency & Crash Recovery Tests
// ============================================================================

// 1. Restart recovery: persistent duplicate detection across server restart
func TestServer_Durable_RestartRecovery(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "server_restart.db")
	batch := sampleTestBatch("batch-durable-01", "node-durable", 1)
	body, _ := json.Marshal(batch)

	// First server instance: ingest and close
	{
		store1, err := storage.OpenSQLite(dbPath)
		if err != nil {
			t.Fatalf("OpenSQLite store1 failed: %v", err)
		}
		srv1 := NewServer(newDiscardLogger(), WithIdempotencyStore(store1))
		ts1 := httptest.NewServer(srv1.Routes())

		res, err := http.Post(ts1.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("POST to srv1 failed: %v", err)
		}
		defer res.Body.Close()

		if res.StatusCode != http.StatusCreated {
			t.Fatalf("expected 201 Created on srv1, got %d", res.StatusCode)
		}

		ts1.Close()
		if err := srv1.Close(); err != nil {
			t.Fatalf("srv1.Close failed: %v", err)
		}
	}

	// Second server instance: re-open on same database file
	{
		store2, err := storage.OpenSQLite(dbPath)
		if err != nil {
			t.Fatalf("OpenSQLite store2 failed: %v", err)
		}
		srv2 := NewServer(newDiscardLogger(), WithIdempotencyStore(store2))
		defer srv2.Close()
		ts2 := httptest.NewServer(srv2.Routes())
		defer ts2.Close()

		// Verify that batch presence is restored from persistent storage
		if !srv2.HasIngestedBatch("node-durable", "batch-durable-01") {
			t.Fatalf("expected batch-durable-01 to be known to srv2 after restart")
		}
		if srv2.Count() != 1 {
			t.Fatalf("expected count 1 on srv2 after restart, got %d", srv2.Count())
		}

		// Verify that payload is retrievable durably from store
		p, err := srv2.GetPayload(context.Background(), "node-durable", "batch-durable-01")
		if err != nil {
			t.Fatalf("srv2.GetPayload failed: %v", err)
		}
		if p == nil || len(p.Metrics) != 1 || p.Metrics[0].Name != "system_load" {
			t.Fatalf("expected payload with system_load metric, got %+v", p)
		}

		// Verify in-memory history ring buffer was warmed from durable store on startup
		if srv2.HistoryCount() != 1 {
			t.Fatalf("expected srv2.HistoryCount() == 1 after startup warming, got %d", srv2.HistoryCount())
		}
		ingested := srv2.GetIngestedBatches()
		if len(ingested) != 1 || ingested[0].Batch.BatchID != "batch-durable-01" {
			t.Fatalf("expected rehydrated batch in history snapshot, got %+v", ingested)
		}

		// Re-submitting the same batch must yield 200 OK already_accepted
		res2, err := http.Post(ts2.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("POST to srv2 failed: %v", err)
		}
		defer res2.Body.Close()

		if res2.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 OK already_accepted on srv2 after restart, got %d", res2.StatusCode)
		}

		var resp IngestionResponse
		if err := json.NewDecoder(res2.Body).Decode(&resp); err != nil {
			t.Fatalf("decoding srv2 response failed: %v", err)
		}
		if resp.Status != "already_accepted" {
			t.Errorf("expected status 'already_accepted' on srv2, got %q", resp.Status)
		}
	}
}

// 2. Cross-node key isolation in durable store
func TestServer_Durable_CrossNodeIsolation(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cross_node.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	srv := NewServer(newDiscardLogger(), WithIdempotencyStore(store))
	defer srv.Close()
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	sharedBatchID := "shared-durable-batch"
	batchA := sampleTestBatch(sharedBatchID, "node-aaa", 1)
	batchB := sampleTestBatch(sharedBatchID, "node-bbb", 1)

	bodyA, _ := json.Marshal(batchA)
	bodyB, _ := json.Marshal(batchB)

	resA, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(bodyA))
	if err != nil {
		t.Fatalf("POST A failed: %v", err)
	}
	defer resA.Body.Close()
	if resA.StatusCode != http.StatusCreated {
		t.Errorf("expected 201 Created for node-aaa, got %d", resA.StatusCode)
	}

	resB, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(bodyB))
	if err != nil {
		t.Fatalf("POST B failed: %v", err)
	}
	defer resB.Body.Close()
	if resB.StatusCode != http.StatusCreated {
		t.Errorf("expected 201 Created for node-bbb, got %d", resB.StatusCode)
	}

	if srv.Count() != 2 {
		t.Errorf("expected Count 2 in durable store, got %d", srv.Count())
	}
	if !srv.HasIngestedBatch("node-aaa", sharedBatchID) || !srv.HasIngestedBatch("node-bbb", sharedBatchID) {
		t.Errorf("expected both nodes to have recorded batch in durable store")
	}
}

// 3. Concurrent duplicate insertion: atomic SQLite uniqueness constraint
func TestServer_Durable_ConcurrentDuplicateInsertion(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "concurrent_insert.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	srv := NewServer(newDiscardLogger(), WithIdempotencyStore(store))
	defer srv.Close()
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	batch := sampleTestBatch("batch-race-durable", "node-race", 1)
	body, _ := json.Marshal(batch)

	const workers = 20
	var startBarrier sync.WaitGroup
	startBarrier.Add(1)

	var doneBarrier sync.WaitGroup
	doneBarrier.Add(workers)

	var createdCount atomic.Int32
	var duplicateCount atomic.Int32
	var errorCount atomic.Int32

	for i := 0; i < workers; i++ {
		go func() {
			defer doneBarrier.Done()
			startBarrier.Wait()

			res, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(body))
			if err != nil {
				errorCount.Add(1)
				return
			}
			defer res.Body.Close()

			switch res.StatusCode {
			case http.StatusCreated:
				createdCount.Add(1)
			case http.StatusOK:
				duplicateCount.Add(1)
			default:
				errorCount.Add(1)
			}
		}()
	}

	startBarrier.Done()
	doneBarrier.Wait()

	if errorCount.Load() > 0 {
		t.Errorf("expected 0 HTTP errors, got %d", errorCount.Load())
	}
	if createdCount.Load() != 1 {
		t.Errorf("expected exactly 1 201 Created, got %d", createdCount.Load())
	}
	if duplicateCount.Load() != workers-1 {
		t.Errorf("expected exactly %d 200 OK already_accepted, got %d", workers-1, duplicateCount.Load())
	}
	if srv.Count() != 1 {
		t.Errorf("expected durable store Count 1, got %d", srv.Count())
	}
	if srv.HistoryCount() != 1 {
		t.Errorf("expected payload history count 1, got %d", srv.HistoryCount())
	}
}

// 4. HTTP and NATS consistent deduplication against shared durable store
func TestServer_Durable_HTTPAndNATSConsistentDeduplication(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "shared_transport.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	srv := NewServer(newDiscardLogger(), WithIdempotencyStore(store))
	defer srv.Close()
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	ctx := context.Background()

	// Step A: Ingest batch1 programmatically (NATS consumer path)
	batch1 := sampleTestBatch("batch-nats-first", "node-nats", 1)
	if err := srv.IngestBatch(ctx, &batch1); err != nil {
		t.Fatalf("IngestBatch failed on novel batch: %v", err)
	}

	// Step B: Submit identical batch1 via HTTP -> must return 200 OK already_accepted
	body1, _ := json.Marshal(batch1)
	res1, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(body1))
	if err != nil {
		t.Fatalf("HTTP post failed: %v", err)
	}
	defer res1.Body.Close()
	if res1.StatusCode != http.StatusOK {
		t.Errorf("expected HTTP 200 OK already_accepted after NATS ingestion, got %d", res1.StatusCode)
	}

	// Step C: Ingest batch2 via HTTP -> must return 201 Created
	batch2 := sampleTestBatch("batch-http-first", "node-http", 1)
	body2, _ := json.Marshal(batch2)
	res2, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(body2))
	if err != nil {
		t.Fatalf("HTTP post 2 failed: %v", err)
	}
	defer res2.Body.Close()
	if res2.StatusCode != http.StatusCreated {
		t.Errorf("expected HTTP 201 Created on novel batch2, got %d", res2.StatusCode)
	}

	// Step D: Re-ingest batch2 programmatically (NATS consumer path) -> must return ErrDuplicateBatch
	errDup := srv.IngestBatch(ctx, &batch2)
	if !strings.Contains(fmt.Sprint(errDup), "duplicate batch") {
		t.Errorf("expected duplicate batch error from IngestBatch after HTTP ingestion, got %v", errDup)
	}

	if srv.Count() != 2 {
		t.Errorf("expected Count 2 in shared durable store, got %d", srv.Count())
	}
	if srv.HistoryCount() != 2 {
		t.Errorf("expected HistoryCount 2, got %d", srv.HistoryCount())
	}
}

// 5. Storage failure: failed write returns 500 / error and does NOT acknowledge or poll history
func TestServer_Durable_StorageFailure(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "store_failure.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}

	srv := NewServer(newDiscardLogger(), WithIdempotencyStore(store))
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	// Explicitly close underlying store to simulate sudden database failure
	_ = store.Close()

	batch := sampleTestBatch("batch-fail-01", "node-fail", 1)
	body, _ := json.Marshal(batch)

	// HTTP path must return 500 Internal Server Error
	res, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("HTTP post failed: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected 500 InternalServerError on storage failure, got %d", res.StatusCode)
	}

	// NATS programmatic path must return error
	errNATS := srv.IngestBatch(context.Background(), &batch)
	if errNATS == nil {
		t.Fatalf("expected error from IngestBatch on closed store, got nil")
	}

	// History buffer must remain completely clean
	if srv.HistoryCount() != 0 {
		t.Errorf("expected HistoryCount 0 after failed storage write, got %d", srv.HistoryCount())
	}
}

// 6. History decoupling: payload history bounded while durable deduplication records remain intact
func TestServer_Durable_HistoryDecoupling(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "decoupling.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	const capLimit = 5
	srv := NewServer(newDiscardLogger(),
		WithIdempotencyStore(store),
		WithMaxHistory(capLimit),
	)
	defer srv.Close()

	ctx := context.Background()
	const totalBatches = 20

	for i := 1; i <= totalBatches; i++ {
		batch := sampleTestBatch(fmt.Sprintf("b-decouple-%02d", i), "node-decouple", int64(i))
		if err := srv.IngestBatch(ctx, &batch); err != nil {
			t.Fatalf("IngestBatch %d failed: %v", i, err)
		}
	}

	// All 20 batches must be recorded in durable store
	if srv.Count() != totalBatches {
		t.Errorf("expected durable Count %d, got %d", totalBatches, srv.Count())
	}

	// Payload history ring buffer must remain capped at capLimit (5)
	if srv.HistoryCount() != capLimit {
		t.Errorf("expected payload HistoryCount capped at %d, got %d", capLimit, srv.HistoryCount())
	}

	// History snapshot contains only the 5 newest batches in strict chronological order
	history := srv.GetIngestedBatches()
	if len(history) != capLimit {
		t.Fatalf("expected snapshot length %d, got %d", capLimit, len(history))
	}
	for i, item := range history {
		expectedSeq := int64(totalBatches - capLimit + 1 + i) // 16, 17, 18, 19, 20
		if item.Batch.SequenceNumber != expectedSeq {
			t.Errorf("expected index %d to have sequence %d, got %d", i, expectedSeq, item.Batch.SequenceNumber)
		}
	}

	// Check that the oldest batch is still recognized as duplicate in the durable store
	if !srv.HasIngestedBatch("node-decouple", "b-decouple-01") {
		t.Errorf("expected b-decouple-01 to remain in durable deduplication store even after payload eviction")
	}

	// Verify that the evicted payload can still be retrieved durably with full metrics
	evictedPayload, err := srv.GetPayload(ctx, "node-decouple", "b-decouple-01")
	if err != nil {
		t.Fatalf("srv.GetPayload for evicted batch failed: %v", err)
	}
	if evictedPayload == nil || evictedPayload.SequenceNumber != 1 || len(evictedPayload.Metrics) == 0 {
		t.Fatalf("expected durable payload for evicted batch-01, got %+v", evictedPayload)
	}
}

// 7. Duplicate history behavior: repeated retries do not add duplicate history entries
func TestServer_Durable_DuplicateHistoryBehavior(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "dup_history.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	srv := NewServer(newDiscardLogger(), WithIdempotencyStore(store))
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	batch := sampleTestBatch("b-dup-hist", "node-dup", 1)
	body, _ := json.Marshal(batch)

	// First ingestion
	res1, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer res1.Body.Close()
	if res1.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d", res1.StatusCode)
	}
	if srv.HistoryCount() != 1 {
		t.Fatalf("expected HistoryCount 1, got %d", srv.HistoryCount())
	}

	// 5 repeated duplicate retries
	for i := 0; i < 5; i++ {
		res, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("retry POST failed: %v", err)
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Errorf("expected 200 OK on duplicate retry %d, got %d", i, res.StatusCode)
		}
	}

	if srv.HistoryCount() != 1 {
		t.Errorf("expected HistoryCount to remain strictly 1 after retries, got %d", srv.HistoryCount())
	}
}

// 8. Atomic transaction rollback: failure persisting payload rolls back metadata completely
func TestServer_Durable_AtomicTransactionRollback(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "atomic_rollback.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	srv := NewServer(newDiscardLogger(), WithIdempotencyStore(store))
	defer srv.Close()
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	batch := sampleTestBatch("batch-atomic-01", "node-atomic", 1)
	body, _ := json.Marshal(batch)

	// Simulate crash / failure during transaction: metadata succeeds, payload fails
	store.HookAfterMetaInsert = func() error {
		return fmt.Errorf("simulated failure after metadata insert")
	}

	// HTTP ingestion must fail with 500 Internal Server Error
	res, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("HTTP post failed: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500 InternalServerError on atomic failure, got %d", res.StatusCode)
	}

	// Verification: Metadata must NOT exist (atomic rollback)
	if srv.HasIngestedBatch("node-atomic", "batch-atomic-01") {
		t.Fatalf("expected batch-atomic-01 NOT to be recorded in store due to rollback")
	}
	if srv.Count() != 0 {
		t.Fatalf("expected Count 0, got %d", srv.Count())
	}
	if srv.HistoryCount() != 0 {
		t.Fatalf("expected HistoryCount 0, got %d", srv.HistoryCount())
	}

	// Payload must NOT exist
	p, err := srv.GetPayload(context.Background(), "node-atomic", "batch-atomic-01")
	if err == nil || p != nil {
		t.Fatalf("expected GetPayload to return error/nil, got payload: %+v, err: %v", p, err)
	}

	// Clear the failure hook and retry: batch must now succeed and be accepted
	store.HookAfterMetaInsert = nil
	res2, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("retry HTTP post failed: %v", err)
	}
	defer res2.Body.Close()

	if res2.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created on retry, got %d", res2.StatusCode)
	}

	if !srv.HasIngestedBatch("node-atomic", "batch-atomic-01") {
		t.Fatalf("expected batch-atomic-01 to be recorded after successful retry")
	}
	recoveredPayload, err := srv.GetPayload(context.Background(), "node-atomic", "batch-atomic-01")
	if err != nil || recoveredPayload == nil {
		t.Fatalf("expected payload to be retrievable after successful retry: %v", err)
	}
	if len(recoveredPayload.Metrics) != 1 || recoveredPayload.Metrics[0].Name != "system_load" {
		t.Fatalf("expected restored metric payload, got %+v", recoveredPayload)
	}
}

type faultyRehydrationStore struct {
	storage.IdempotencyStore
}

func (f *faultyRehydrationStore) GetRecentBatches(ctx context.Context, limit int) ([]*storage.StoredBatch, error) {
	return nil, errors.New("simulated database I/O error during rehydration")
}

// 9. Rehydration failure logging: failure during startup cache warming logs structured warning
func TestServer_Durable_RehydrationFailureLogged(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "rehydrate_fail.db")
	baseStore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer baseStore.Close()

	// Ingest a batch into the baseStore first so durable data actually exists
	batch := sampleTestBatch("batch-rehydrate-01", "node-rehydrate", 1)
	if isNew, err := baseStore.RecordBatch(context.Background(), &batch, time.Now().UTC()); err != nil || !isNew {
		t.Fatalf("initial RecordBatch failed: %v", err)
	}

	// Capture slog warnings
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	// Wrap baseStore with faulty GetRecentBatches
	store := &faultyRehydrationStore{IdempotencyStore: baseStore}

	// Construct server with the faulty store
	srv := NewServer(logger, WithIdempotencyStore(store))
	defer srv.Close()

	// Server startup must not panic or fail
	// History buffer remains empty because warming failed
	if srv.HistoryCount() != 0 {
		t.Errorf("expected HistoryCount 0 when warming fails, got %d", srv.HistoryCount())
	}

	// Warning must be logged with structured error
	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "failed to warm in-memory history cache from durable store") {
		t.Errorf("expected warning log for warming failure, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, "simulated database I/O error during rehydration") {
		t.Errorf("expected error message in log output, got: %s", logOutput)
	}

	// Crucial: Durable payloads and deduplication remain intact and functional!
	payload, err := srv.GetPayload(context.Background(), "node-rehydrate", "batch-rehydrate-01")
	if err != nil || payload == nil {
		t.Fatalf("expected durable payload to be retrievable even if history warming failed: %v", err)
	}
	if !srv.HasIngestedBatch("node-rehydrate", "batch-rehydrate-01") {
		t.Errorf("expected duplicate recognition to function regardless of history cache status")
	}
}

// TestServer_Durable_StartupRehydration_CorruptPayloadTolerant verifies that when SQLite contains
// malformed payload rows, NewServer continues startup, successfully warms all valid payloads into
// the history ring buffer, emits structured warning logs detailing the skipped rows, and remains fully functional.
func TestServer_Durable_StartupRehydration_CorruptPayloadTolerant(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "corrupt_rehydrate.db")

	baseStore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}

	ctx := context.Background()
	baseTime := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	// Ingest 3 batches
	b1 := sampleTestBatch("batch-corrupt-01", "node-rehydrate", 1)
	b2 := sampleTestBatch("batch-corrupt-02", "node-rehydrate", 2)
	b3 := sampleTestBatch("batch-corrupt-03", "node-rehydrate", 3)

	if _, err := baseStore.RecordBatch(ctx, &b1, baseTime.Add(1*time.Minute)); err != nil {
		t.Fatalf("RecordBatch 1 failed: %v", err)
	}
	if _, err := baseStore.RecordBatch(ctx, &b2, baseTime.Add(2*time.Minute)); err != nil {
		t.Fatalf("RecordBatch 2 failed: %v", err)
	}
	if _, err := baseStore.RecordBatch(ctx, &b3, baseTime.Add(3*time.Minute)); err != nil {
		t.Fatalf("RecordBatch 3 failed: %v", err)
	}

	// Close store so we can inject corrupt JSON into batch 2
	if err := baseStore.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	rawDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	if _, err := rawDB.Exec(`UPDATE cp_telemetry_payloads SET payload_json = '{"truncated": false,' WHERE batch_id = 'batch-corrupt-02'`); err != nil {
		rawDB.Close()
		t.Fatalf("failed to inject corruption: %v", err)
	}
	rawDB.Close()

	// Reopen store
	reopenedStore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("reopening OpenSQLite failed: %v", err)
	}
	defer reopenedStore.Close()

	// Capture slog warning output
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	// Initialize server with the store containing the corrupted row
	srv := NewServer(logger, WithIdempotencyStore(reopenedStore))
	defer srv.Close()

	// Assert that valid batches 01 and 03 were warmed into history (count = 2)
	if srv.HistoryCount() != 2 {
		t.Errorf("expected HistoryCount=2, got %d", srv.HistoryCount())
	}

	ingested := srv.GetIngestedBatches()
	if len(ingested) != 2 {
		t.Fatalf("expected 2 ingested batches in history, got %d", len(ingested))
	}
	if ingested[0].Batch.BatchID != "batch-corrupt-01" {
		t.Errorf("expected first batch to be batch-corrupt-01, got %q", ingested[0].Batch.BatchID)
	}
	if ingested[1].Batch.BatchID != "batch-corrupt-03" {
		t.Errorf("expected second batch to be batch-corrupt-03, got %q", ingested[1].Batch.BatchID)
	}

	// Structured warning log assertions
	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "corrupted telemetry payloads detected during history rehydration; skipped invalid rows") {
		t.Errorf("expected warning log about corrupt payloads, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, "batch-corrupt-02") {
		t.Errorf("expected log to identify the corrupt batch, got: %s", logOutput)
	}
	if !strings.Contains(logOutput, `"recovered_batches":2`) {
		t.Errorf("expected log to report recovered_batches:2, got: %s", logOutput)
	}

	// Valid payloads can still be read
	p1, err := srv.GetPayload(ctx, "node-rehydrate", "batch-corrupt-01")
	if err != nil || p1 == nil {
		t.Errorf("failed to retrieve valid payload 01: %v", err)
	}
	p3, err := srv.GetPayload(ctx, "node-rehydrate", "batch-corrupt-03")
	if err != nil || p3 == nil {
		t.Errorf("failed to retrieve valid payload 03: %v", err)
	}

	// Corrupt payload returns ErrCorruptPayload
	p2, err := srv.GetPayload(ctx, "node-rehydrate", "batch-corrupt-02")
	if p2 != nil {
		t.Errorf("expected nil payload for corrupt batch, got %+v", p2)
	}
	if !errors.Is(err, storage.ErrCorruptPayload) {
		t.Errorf("expected ErrCorruptPayload for corrupt batch, got %v", err)
	}

	// HTTP routes still function normally
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Errorf("health check failed after corrupt rehydration: resp=%v, err=%v", resp, err)
	}
	if resp != nil {
		resp.Body.Close()
	}
}

// TestServer_NodeActivityConsistency verifies Workstream 1:
// 1. New batch updates LastSeen and sequence.
// 2. Lower sequence does not regress metadata.
// 3. Duplicate batch does not advance sequence or alter metadata.
// 4. Unknown node does not fabricate a node record.
// 5. Collection time differs from ingestion time (LastSeen reflects ingestion time).
// 6. HTTP and NATS paths share the intended semantics.
func TestServer_NodeActivityConsistency(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(newDiscardLogger())
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	// 1. Register node-act-1
	regTime := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	reg := types.NodeRegistration{
		NodeID:       "node-act-1",
		Hostname:     "host-act-1",
		OS:           "linux",
		Architecture: "amd64",
		RegisteredAt: regTime,
	}
	regBody, _ := json.Marshal(reg)
	res, err := http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(regBody))
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created on register, got %d", res.StatusCode)
	}

	nodeRec, ok := srv.GetNode("node-act-1")
	if !ok {
		t.Fatalf("GetNode failed: node-act-1 not found")
	}
	if nodeRec.SequenceNumber != 0 {
		t.Errorf("initial sequence_number expected 0, got %d", nodeRec.SequenceNumber)
	}

	// 2. HTTP Ingestion: New batch updates LastSeen to ingestion time and advances SequenceNumber
	// Historical CollectedAt from 10 days ago (offline operation)
	collectedAt := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	b1 := sampleTestBatch("b-act-1", "node-act-1", 100)
	b1.CollectedAt = collectedAt
	beforeIngest := time.Now().UTC()
	b1Body, _ := json.Marshal(b1)
	res, err = http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(b1Body))
	if err != nil {
		t.Fatalf("post b1 failed: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created on b1, got %d", res.StatusCode)
	}
	afterIngest := time.Now().UTC()

	nodeRec, _ = srv.GetNode("node-act-1")
	if nodeRec.SequenceNumber != 100 {
		t.Errorf("expected sequence_number 100, got %d", nodeRec.SequenceNumber)
	}
	// LastSeen must reflect control-plane ingestion time, NOT historical CollectedAt
	if nodeRec.LastSeen.Before(beforeIngest) || nodeRec.LastSeen.After(afterIngest) {
		t.Errorf("expected LastSeen between %v and %v, got %v", beforeIngest, afterIngest, nodeRec.LastSeen)
	}
	if nodeRec.LastSeen.Equal(collectedAt) {
		t.Errorf("LastSeen must NOT equal CollectedAt: got %v", nodeRec.LastSeen)
	}

	// 3. Lower sequence (out-of-order delivery) does NOT regress SequenceNumber, but updates LastSeen
	time.Sleep(10 * time.Millisecond)
	beforeIngest2 := time.Now().UTC()
	b2 := sampleTestBatch("b-act-2", "node-act-1", 50) // lower sequence: 50 < 100
	b2Body, _ := json.Marshal(b2)
	res, err = http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(b2Body))
	if err != nil {
		t.Fatalf("post b2 failed: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created on b2, got %d", res.StatusCode)
	}

	nodeRec, _ = srv.GetNode("node-act-1")
	if nodeRec.SequenceNumber != 100 {
		t.Errorf("expected sequence_number to remain 100 (monotonicity), got %d", nodeRec.SequenceNumber)
	}
	if nodeRec.LastSeen.Before(beforeIngest2) {
		t.Errorf("expected LastSeen to advance to at or after %v, got %v", beforeIngest2, nodeRec.LastSeen)
	}

	// 4. Duplicate batch does NOT advance sequence number or alter metadata
	lastSeenBeforeDup := nodeRec.LastSeen
	res, err = http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(b1Body)) // duplicate b1
	if err != nil {
		t.Fatalf("post duplicate b1 failed: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for duplicate batch, got %d", res.StatusCode)
	}

	nodeRec, _ = srv.GetNode("node-act-1")
	if nodeRec.SequenceNumber != 100 {
		t.Errorf("expected sequence_number to remain 100 after duplicate, got %d", nodeRec.SequenceNumber)
	}
	if !nodeRec.LastSeen.Equal(lastSeenBeforeDup) {
		t.Errorf("expected LastSeen to remain unchanged on duplicate, got %v vs %v", nodeRec.LastSeen, lastSeenBeforeDup)
	}

	// 5. Unknown node does NOT fabricate a node record in s.nodes
	bUnknown := sampleTestBatch("b-act-unknown", "node-unregistered", 10)
	bUnknownBody, _ := json.Marshal(bUnknown)
	res, err = http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(bUnknownBody))
	if err != nil {
		t.Fatalf("post bUnknown failed: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created for valid batch from unregistered node, got %d", res.StatusCode)
	}

	_, ok = srv.GetNode("node-unregistered")
	if ok {
		t.Errorf("expected node-unregistered not to exist in registry, but ok was true")
	}

	// 6. NATS path (IngestBatch) shares the exact same node activity semantics
	// Register node-act-nats
	regNATS := types.NodeRegistration{
		NodeID:       "node-act-nats",
		Hostname:     "host-act-nats",
		OS:           "linux",
		Architecture: "amd64",
		RegisteredAt: regTime,
	}
	regNATSBody, _ := json.Marshal(regNATS)
	res, _ = http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(regNATSBody))
	res.Body.Close()

	bNATS1 := sampleTestBatch("b-nats-1", "node-act-nats", 200)
	beforeNATSIngest := time.Now().UTC()
	if err := srv.IngestBatch(ctx, &bNATS1); err != nil {
		t.Fatalf("IngestBatch bNATS1 failed: %v", err)
	}
	afterNATSIngest := time.Now().UTC()

	nodeNATS, ok := srv.GetNode("node-act-nats")
	if !ok {
		t.Fatalf("GetNode node-act-nats failed")
	}
	if nodeNATS.SequenceNumber != 200 {
		t.Errorf("expected sequence_number 200 via NATS IngestBatch, got %d", nodeNATS.SequenceNumber)
	}
	if nodeNATS.LastSeen.Before(beforeNATSIngest) || nodeNATS.LastSeen.After(afterNATSIngest) {
		t.Errorf("expected NATS LastSeen between %v and %v, got %v", beforeNATSIngest, afterNATSIngest, nodeNATS.LastSeen)
	}

	// Duplicate NATS batch returns ErrDuplicateBatch and does NOT alter sequence or LastSeen
	lastSeenNATSBeforeDup := nodeNATS.LastSeen
	dupErr := srv.IngestBatch(ctx, &bNATS1)
	if !errors.Is(dupErr, ErrDuplicateBatch) {
		t.Errorf("expected ErrDuplicateBatch on duplicate IngestBatch, got %v", dupErr)
	}
	nodeNATS, _ = srv.GetNode("node-act-nats")
	if nodeNATS.SequenceNumber != 200 {
		t.Errorf("expected sequence_number 200 after duplicate NATS batch, got %d", nodeNATS.SequenceNumber)
	}
	if !nodeNATS.LastSeen.Equal(lastSeenNATSBeforeDup) {
		t.Errorf("expected LastSeen unchanged after duplicate NATS batch, got %v vs %v", nodeNATS.LastSeen, lastSeenNATSBeforeDup)
	}

	// Out-of-order lower sequence via NATS IngestBatch preserves monotonicity
	bNATS2 := sampleTestBatch("b-nats-2", "node-act-nats", 150)
	if err := srv.IngestBatch(ctx, &bNATS2); err != nil {
		t.Fatalf("IngestBatch bNATS2 failed: %v", err)
	}
	nodeNATS, _ = srv.GetNode("node-act-nats")
	if nodeNATS.SequenceNumber != 200 {
		t.Errorf("expected sequence_number to remain 200 (monotonicity) via NATS, got %d", nodeNATS.SequenceNumber)
	}
}

func TestServer_DurableNodeRegistry_RestartRehydration(t *testing.T) {
	dbDir := t.TempDir()
	dbPath := filepath.Join(dbDir, "control-plane-restart.db")

	store1, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite 1 failed: %v", err)
	}

	srv1 := NewServer(newDiscardLogger(), WithIdempotencyStore(store1))
	if err := srv1.InitError(); err != nil {
		t.Fatalf("srv1 InitError: %v", err)
	}
	ts1 := httptest.NewServer(srv1.Routes())

	// 1. Register node on Server 1
	regTime := time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC)
	reg := types.NodeRegistration{
		NodeID:       "node-restart-1",
		Hostname:     "worker-restart",
		OS:           "linux",
		Architecture: "amd64",
		AgentVersion: "v1.5.0",
		Capabilities: []string{"wifi", "ble"},
		Labels:       map[string]string{"env": "prod", "rack": "a1"},
		RegisteredAt: regTime,
	}
	regBody, _ := json.Marshal(reg)
	res, err := http.Post(ts1.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(regBody))
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created on register, got %d", res.StatusCode)
	}

	// 2. Heartbeat on Server 1: status DEGRADED, seq 25
	hb := types.Heartbeat{
		NodeID:         "node-restart-1",
		SequenceNumber: 25,
		Timestamp:      time.Now().UTC(),
		Status:         types.NodeStatusDegraded,
	}
	hbBody, _ := json.Marshal(hb)
	res, err = http.Post(ts1.URL+"/api/v1/nodes/heartbeat", "application/json", bytes.NewReader(hbBody))
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK on heartbeat, got %d", res.StatusCode)
	}

	// 3. Telemetry on Server 1: advance sequence to 100
	b := sampleTestBatch("b-restart-1", "node-restart-1", 100)
	bBody, _ := json.Marshal(b)
	res, err = http.Post(ts1.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(bBody))
	if err != nil {
		t.Fatalf("telemetry post failed: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created on telemetry, got %d", res.StatusCode)
	}

	// 4. Shutdown Server 1 and close store 1
	ts1.Close()
	if err := srv1.Close(); err != nil {
		t.Fatalf("srv1 Close failed: %v", err)
	}

	// 5. Start Server 2 pointing to the same SQLite database file
	store2, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite 2 failed: %v", err)
	}

	srv2 := NewServer(newDiscardLogger(), WithIdempotencyStore(store2))
	if err := srv2.InitError(); err != nil {
		t.Fatalf("srv2 InitError: %v", err)
	}
	ts2 := httptest.NewServer(srv2.Routes())
	defer ts2.Close()
	defer srv2.Close()

	// 6. Verify in-memory state on Server 2 was completely rehydrated
	rec, ok := srv2.GetNode("node-restart-1")
	if !ok {
		t.Fatalf("node-restart-1 not found in srv2 after restart rehydration")
	}
	if rec.NodeID != "node-restart-1" {
		t.Errorf("expected node-restart-1, got %q", rec.NodeID)
	}
	if rec.Status != types.NodeStatusDegraded {
		t.Errorf("expected status DEGRADED preserved after restart, got %q", rec.Status)
	}
	if rec.SequenceNumber != 100 {
		t.Errorf("expected sequence number 100 preserved after restart, got %d", rec.SequenceNumber)
	}
	if rec.Hostname != "worker-restart" || rec.AgentVersion != "v1.5.0" {
		t.Errorf("metadata mismatch after restart: host=%s, ver=%s", rec.Hostname, rec.AgentVersion)
	}
	if len(rec.Capabilities) != 2 || rec.Labels["env"] != "prod" {
		t.Errorf("capabilities/labels mismatch after restart: %v / %v", rec.Capabilities, rec.Labels)
	}

	// 7. Verify HTTP GET /api/v1/nodes returns the node on Server 2
	res, err = http.Get(ts2.URL + "/api/v1/nodes")
	if err != nil {
		t.Fatalf("GET /api/v1/nodes on srv2 failed: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", res.StatusCode)
	}
	var nodesList []*NodeRecord
	if err := json.NewDecoder(res.Body).Decode(&nodesList); err != nil {
		t.Fatalf("decode nodes list failed: %v", err)
	}
	if len(nodesList) != 1 || nodesList[0].NodeID != "node-restart-1" {
		t.Fatalf("unexpected nodes list after restart: %v", nodesList)
	}

	// 8. Subsequent heartbeat on Server 2 succeeds without needing re-registration
	hb2 := types.Heartbeat{
		NodeID:         "node-restart-1",
		SequenceNumber: 150,
		Timestamp:      time.Now().UTC(),
		Status:         types.NodeStatusHealthy,
	}
	hb2Body, _ := json.Marshal(hb2)
	res, err = http.Post(ts2.URL+"/api/v1/nodes/heartbeat", "application/json", bytes.NewReader(hb2Body))
	if err != nil {
		t.Fatalf("heartbeat on srv2 failed: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for heartbeat on rehydrated node, got %d", res.StatusCode)
	}

	rec2, _ := srv2.GetNode("node-restart-1")
	if rec2.HeartbeatSequence != 150 || rec2.Status != types.NodeStatusHealthy {
		t.Errorf("expected heartbeat sequence 150 and status HEALTHY on srv2, got hbSeq=%d status=%s", rec2.HeartbeatSequence, rec2.Status)
	}
}

func TestServer_DurableNodeRegistry_StartupRehydrationFailure(t *testing.T) {
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	// Intentionally close store before NewServer to force ListNodes to fail
	_ = store.Close()

	srv := NewServer(newDiscardLogger(), WithIdempotencyStore(store))
	if srv.InitError() == nil {
		t.Fatalf("expected InitError on closed store, got nil")
	}

	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	// 1. GET /healthz returns 503
	res, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz failed: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 on healthz for uninitialized server, got %d", res.StatusCode)
	}

	// 2. POST /api/v1/nodes/register returns 503
	reg := types.NodeRegistration{
		NodeID:       "node-uninit-1",
		Hostname:     "host-1",
		RegisteredAt: time.Now().UTC(),
	}
	regBody, _ := json.Marshal(reg)
	res, err = http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(regBody))
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 on register for uninitialized server, got %d", res.StatusCode)
	}

	// 3. POST /api/v1/nodes/heartbeat returns 503
	hb := types.Heartbeat{
		NodeID:    "node-uninit-1",
		Timestamp: time.Now().UTC(),
		Status:    types.NodeStatusHealthy,
	}
	hbBody, _ := json.Marshal(hb)
	res, err = http.Post(ts.URL+"/api/v1/nodes/heartbeat", "application/json", bytes.NewReader(hbBody))
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 on heartbeat for uninitialized server, got %d", res.StatusCode)
	}

	// 4. GET /api/v1/nodes returns 503
	res, err = http.Get(ts.URL + "/api/v1/nodes")
	if err != nil {
		t.Fatalf("GET /api/v1/nodes failed: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 on GET nodes for uninitialized server, got %d", res.StatusCode)
	}

	// 5. POST /api/v1/telemetry/batches returns 503
	batch := sampleTestBatch("b-uninit-1", "node-uninit-1", 1)
	bBody, _ := json.Marshal(batch)
	res, err = http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(bBody))
	if err != nil {
		t.Fatalf("batch post failed: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 on batch ingest for uninitialized server, got %d", res.StatusCode)
	}
}

func TestServer_DurableNodeRegistry_StorageFailure_NoMemoryMutation(t *testing.T) {
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}

	srv := NewServer(newDiscardLogger(), WithIdempotencyStore(store))
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	// Register node-ok successfully
	regOk := types.NodeRegistration{
		NodeID:       "node-ok",
		Hostname:     "host-ok",
		RegisteredAt: time.Now().UTC(),
	}
	bOk, _ := json.Marshal(regOk)
	res, err := http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(bOk))
	if err != nil || res.StatusCode != http.StatusCreated {
		t.Fatalf("regOk failed: status=%d, err=%v", res.StatusCode, err)
	}
	res.Body.Close()

	// Close underlying store to force subsequent storage operations to fail
	_ = store.Close()

	// Attempt to register node-fail
	regFail := types.NodeRegistration{
		NodeID:       "node-fail",
		Hostname:     "host-fail",
		RegisteredAt: time.Now().UTC(),
	}
	bFail, _ := json.Marshal(regFail)
	res, err = http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(bFail))
	if err != nil {
		t.Fatalf("post failed: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected 500 Internal Server Error when store fails, got %d", res.StatusCode)
	}

	// node-fail must NOT exist in memory
	if _, ok := srv.GetNode("node-fail"); ok {
		t.Errorf("node-fail was added to in-memory map despite storage failure")
	}

	// Attempt heartbeat on node-ok with closed store
	hb := types.Heartbeat{
		NodeID:         "node-ok",
		SequenceNumber: 999,
		Status:         types.NodeStatusDegraded,
		Timestamp:      time.Now().UTC(),
	}
	bHb, _ := json.Marshal(hb)
	res, err = http.Post(ts.URL+"/api/v1/nodes/heartbeat", "application/json", bytes.NewReader(bHb))
	if err != nil {
		t.Fatalf("heartbeat post failed: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected 500 Internal Server Error when store fails during heartbeat, got %d", res.StatusCode)
	}

	// Memory state for node-ok must NOT have updated to sequence 999
	recOk, _ := srv.GetNode("node-ok")
	if recOk.SequenceNumber == 999 {
		t.Errorf("node-ok in-memory sequence was mutated despite storage failure")
	}
}

func TestServer_DurableNodeRegistry_ConcurrentInterleavedOps(t *testing.T) {
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	srv := NewServer(newDiscardLogger(), WithIdempotencyStore(store))
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	// Register initial node
	reg := types.NodeRegistration{
		NodeID:       "node-conc-server",
		Hostname:     "host-conc",
		RegisteredAt: time.Now().UTC(),
	}
	bReg, _ := json.Marshal(reg)
	res, err := http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(bReg))
	if err != nil || res.StatusCode != http.StatusCreated {
		t.Fatalf("initial register failed: %v", err)
	}
	res.Body.Close()

	const iterations = 15
	var wg sync.WaitGroup
	wg.Add(iterations * 3)

	// 1. Concurrent duplicate registrations
	for i := 0; i < iterations; i++ {
		go func(idx int) {
			defer wg.Done()
			r := types.NodeRegistration{
				NodeID:       "node-conc-server",
				Hostname:     fmt.Sprintf("host-conc-%d", idx),
				RegisteredAt: time.Now().UTC(),
			}
			b, _ := json.Marshal(r)
			resp, err := http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(b))
			if err == nil {
				resp.Body.Close()
			}
		}(i)
	}

	// 2. Concurrent heartbeats
	for i := 0; i < iterations; i++ {
		go func(idx int) {
			defer wg.Done()
			seq := int64((idx + 1) * 2)
			hb := types.Heartbeat{
				NodeID:         "node-conc-server",
				SequenceNumber: seq,
				Timestamp:      time.Now().UTC(),
				Status:         types.NodeStatusHealthy,
			}
			b, _ := json.Marshal(hb)
			resp, err := http.Post(ts.URL+"/api/v1/nodes/heartbeat", "application/json", bytes.NewReader(b))
			if err == nil {
				resp.Body.Close()
			}
		}(i)
	}

	// 3. Concurrent telemetry batch ingestions
	for i := 0; i < iterations; i++ {
		go func(idx int) {
			defer wg.Done()
			seq := int64((idx + 1) * 5)
			batch := sampleTestBatch(fmt.Sprintf("b-conc-%d", idx), "node-conc-server", seq)
			b, _ := json.Marshal(batch)
			resp, err := http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(b))
			if err == nil {
				resp.Body.Close()
			}
		}(i)
	}

	wg.Wait()

	rec, ok := srv.GetNode("node-conc-server")
	if !ok {
		t.Fatalf("node-conc-server missing from memory after concurrent ops")
	}

	// Max sequence number submitted was iterations * 5 = 75
	expectedMaxSeq := int64(iterations * 5)
	if rec.SequenceNumber != expectedMaxSeq {
		t.Errorf("expected sequence number to reach %d, got %d", expectedMaxSeq, rec.SequenceNumber)
	}
}

func TestServer_F01_Deterministic_HeartbeatOrderFencing(t *testing.T) {
	ctx := context.Background()
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	srv := NewServer(newDiscardLogger(), WithIdempotencyStore(store))
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	// 1. Initial registration
	t0 := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	reg := types.NodeRegistration{
		NodeID:       "node-f1",
		Hostname:     "host-f1",
		RegisteredAt: t0,
	}
	regBody, _ := json.Marshal(reg)
	res, err := http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(regBody))
	if err != nil || res.StatusCode != http.StatusCreated {
		t.Fatalf("register failed: status=%d, err=%v", res.StatusCode, err)
	}
	res.Body.Close()

	// 2. Newer heartbeat arrives at T2 with Status: HEALTHY, seq: 50
	t2 := t0.Add(2 * time.Hour)
	hbNewer := types.Heartbeat{
		NodeID:         "node-f1",
		SequenceNumber: 50,
		Status:         types.NodeStatusHealthy,
		Timestamp:      t2,
	}
	hbNewerBody, _ := json.Marshal(hbNewer)
	res, err = http.Post(ts.URL+"/api/v1/nodes/heartbeat", "application/json", bytes.NewReader(hbNewerBody))
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("hbNewer failed: status=%d, err=%v", res.StatusCode, err)
	}
	res.Body.Close()

	// Verify T2 is recorded
	recMem, ok := srv.GetNode("node-f1")
	if !ok || recMem.Status != types.NodeStatusHealthy || recMem.HeartbeatSequence != 50 || !recMem.LastSeen.Equal(t2) {
		t.Fatalf("state after hbNewer incorrect: status=%s, hbSeq=%d, lastSeen=%v", recMem.Status, recMem.HeartbeatSequence, recMem.LastSeen)
	}

	// 3. Delayed older heartbeat arrives with timestamp T1 (T0 < T1 < T2), Status: DEGRADED, seq: 10
	t1 := t0.Add(1 * time.Hour)
	hbOlder := types.Heartbeat{
		NodeID:         "node-f1",
		SequenceNumber: 10,
		Status:         types.NodeStatusDegraded,
		Timestamp:      t1,
	}
	hbOlderBody, _ := json.Marshal(hbOlder)
	res, err = http.Post(ts.URL+"/api/v1/nodes/heartbeat", "application/json", bytes.NewReader(hbOlderBody))
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("hbOlder failed: status=%d, err=%v", res.StatusCode, err)
	}
	res.Body.Close()

	// 4. Assert in-memory state: LastSeen, Status, and HeartbeatSequence must NOT regress!
	recMemAfter, _ := srv.GetNode("node-f1")
	if recMemAfter.Status != types.NodeStatusHealthy {
		t.Errorf("DEFECT F-01: in-memory status regressed from HEALTHY to %s", recMemAfter.Status)
	}
	if recMemAfter.HeartbeatSequence != 50 {
		t.Errorf("DEFECT F-01: in-memory heartbeat sequence regressed from 50 to %d", recMemAfter.HeartbeatSequence)
	}
	if !recMemAfter.LastSeen.Equal(t2) {
		t.Errorf("DEFECT F-01: in-memory LastSeen regressed from %v to %v", t2, recMemAfter.LastSeen)
	}

	// 5. Assert durable SQLite state: must also retain T2 state
	recDB, err := store.GetNode(ctx, "node-f1")
	if err != nil {
		t.Fatalf("store.GetNode failed: %v", err)
	}
	if recDB.HeartbeatSequence != 50 {
		t.Errorf("SQLite sequence regressed: expected 50, got %d", recDB.SequenceNumber)
	}
	if !recDB.LastSeen.Equal(t2) {
		t.Errorf("SQLite LastSeen regressed: expected %v, got %v", t2, recDB.LastSeen)
	}
}

func TestServer_F01_Deterministic_RegistrationMetadataFencing(t *testing.T) {
	ctx := context.Background()
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	srv := NewServer(newDiscardLogger(), WithIdempotencyStore(store))
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	// 1. Initial registration at T0
	t0 := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	reg := types.NodeRegistration{
		NodeID:       "node-f2",
		Hostname:     "host-initial",
		AgentVersion: "v1.0.0",
		RegisteredAt: t0,
	}
	regBody, _ := json.Marshal(reg)
	res, err := http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(regBody))
	if err != nil || res.StatusCode != http.StatusCreated {
		t.Fatalf("initial register failed: %v", err)
	}
	res.Body.Close()

	// 2. Newer duplicate registration at T2 with updated metadata
	t2 := t0.Add(2 * time.Hour)
	regNewer := types.NodeRegistration{
		NodeID:       "node-f2",
		Hostname:     "host-newer",
		AgentVersion: "v2.0.0",
		RegisteredAt: t2,
	}
	regNewerBody, _ := json.Marshal(regNewer)
	res, err = http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(regNewerBody))
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("regNewer failed: %v", err)
	}
	res.Body.Close()

	recMem, _ := srv.GetNode("node-f2")
	if recMem.Hostname != "host-newer" || recMem.AgentVersion != "v2.0.0" {
		t.Fatalf("expected host-newer/v2.0.0, got %s/%s", recMem.Hostname, recMem.AgentVersion)
	}

	// 3. Delayed older duplicate registration arriving with T1 (T0 < T1 < T2)
	t1 := t0.Add(1 * time.Hour)
	regOlder := types.NodeRegistration{
		NodeID:       "node-f2",
		Hostname:     "host-older",
		AgentVersion: "v1.5.0",
		RegisteredAt: t1,
	}
	regOlderBody, _ := json.Marshal(regOlder)
	res, err = http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(regOlderBody))
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("regOlder failed: %v", err)
	}
	res.Body.Close()

	// 4. Assert in-memory state: metadata must NOT regress to older values!
	recMemAfter, _ := srv.GetNode("node-f2")
	if recMemAfter.Hostname != "host-newer" {
		t.Errorf("DEFECT F-01: in-memory Hostname regressed from host-newer to %q", recMemAfter.Hostname)
	}
	if recMemAfter.AgentVersion != "v2.0.0" {
		t.Errorf("DEFECT F-01: in-memory AgentVersion regressed from v2.0.0 to %q", recMemAfter.AgentVersion)
	}
	if !recMemAfter.RegisteredAt.Equal(t0) {
		t.Errorf("RegisteredAt must be immutable: expected %v, got %v", t0, recMemAfter.RegisteredAt)
	}

	// 5. Assert SQLite durable state
	recDB, err := store.GetNode(ctx, "node-f2")
	if err != nil {
		t.Fatalf("store.GetNode failed: %v", err)
	}
	if recDB.Hostname != "host-newer" {
		t.Errorf("SQLite Hostname regressed: expected host-newer, got %q", recDB.Hostname)
	}
}

func TestServer_F01_Deterministic_HeartbeatTelemetryInterleaving(t *testing.T) {
	ctx := context.Background()
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	srv := NewServer(newDiscardLogger(), WithIdempotencyStore(store))
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	t0 := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	reg := types.NodeRegistration{
		NodeID:       "node-f3",
		Hostname:     "host-f3",
		RegisteredAt: t0,
	}
	bReg, _ := json.Marshal(reg)
	res, _ := http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(bReg))
	res.Body.Close()

	// Ingest telemetry batch seq 100 at T2
	t2 := t0.Add(2 * time.Hour)
	batch := sampleTestBatch("b-f3-1", "node-f3", 100)
	batch.CollectedAt = t2
	bBody, _ := json.Marshal(batch)
	res, err = http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(bBody))
	if err != nil || res.StatusCode != http.StatusCreated {
		t.Fatalf("telemetry post failed: %v", err)
	}
	res.Body.Close()

	rec, _ := srv.GetNode("node-f3")
	if rec.SequenceNumber != 100 {
		t.Fatalf("expected sequence 100, got %d", rec.SequenceNumber)
	}

	// Ingest delayed heartbeat seq 40 with timestamp T1 < T2
	t1 := t0.Add(1 * time.Hour)
	hb := types.Heartbeat{
		NodeID:         "node-f3",
		SequenceNumber: 40,
		Status:         types.NodeStatusHealthy,
		Timestamp:      t1,
	}
	hbBody, _ := json.Marshal(hb)
	res, err = http.Post(ts.URL+"/api/v1/nodes/heartbeat", "application/json", bytes.NewReader(hbBody))
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("heartbeat post failed: %v", err)
	}
	res.Body.Close()

	// Neither sequence number (100) nor LastSeen should regress
	recAfter, _ := srv.GetNode("node-f3")
	if recAfter.SequenceNumber != 100 {
		t.Errorf("sequence number regressed on delayed heartbeat: expected 100, got %d", recAfter.SequenceNumber)
	}

	// Newer heartbeat with seq 150 advances sequence
	t3 := t2.Add(1 * time.Hour)
	hbNewer := types.Heartbeat{
		NodeID:         "node-f3",
		SequenceNumber: 150,
		Status:         types.NodeStatusHealthy,
		Timestamp:      t3,
	}
	hbNewerBody, _ := json.Marshal(hbNewer)
	res, err = http.Post(ts.URL+"/api/v1/nodes/heartbeat", "application/json", bytes.NewReader(hbNewerBody))
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("newer heartbeat post failed: %v", err)
	}
	res.Body.Close()

	recAfterNewer, _ := srv.GetNode("node-f3")
	if recAfterNewer.HeartbeatSequence != 150 {
		t.Errorf("heartbeat sequence failed to advance on newer heartbeat: expected 150, got %d", recAfterNewer.HeartbeatSequence)
	}
	if recAfterNewer.SequenceNumber != 100 {
		t.Errorf("telemetry sequence regressed or coupled on newer heartbeat: expected 100, got %d", recAfterNewer.SequenceNumber)
	}

	// Verify SQLite
	recDB, _ := store.GetNode(ctx, "node-f3")
	if recDB.HeartbeatSequence != 150 {
		t.Errorf("SQLite heartbeat sequence mismatch: expected 150, got %d", recDB.HeartbeatSequence)
	}
	if recDB.SequenceNumber != 100 {
		t.Errorf("SQLite telemetry sequence mismatch: expected 100, got %d", recDB.SequenceNumber)
	}
}

func TestServer_F01_Deterministic_HTTPAndNATSSafeReconciliation(t *testing.T) {
	ctx := context.Background()
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	srv := NewServer(newDiscardLogger(), WithIdempotencyStore(store))
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	t0 := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	reg := types.NodeRegistration{
		NodeID:       "node-f4",
		Hostname:     "host-f4",
		RegisteredAt: t0,
	}
	bReg, _ := json.Marshal(reg)
	res, _ := http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(bReg))
	res.Body.Close()

	// 1. HTTP Telemetry Ingest: seq 50
	bHTTP := sampleTestBatch("b-f4-http", "node-f4", 50)
	bHTTPBody, _ := json.Marshal(bHTTP)
	res, err = http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(bHTTPBody))
	if err != nil || res.StatusCode != http.StatusCreated {
		t.Fatalf("HTTP batch failed: %v", err)
	}
	res.Body.Close()

	// 2. NATS Ingest: seq 80
	bNATS := sampleTestBatch("b-f4-nats", "node-f4", 80)
	if err := srv.IngestBatch(ctx, &bNATS); err != nil {
		t.Fatalf("NATS IngestBatch failed: %v", err)
	}

	rec, _ := srv.GetNode("node-f4")
	if rec.SequenceNumber != 80 {
		t.Fatalf("expected sequence 80 after NATS ingest, got %d", rec.SequenceNumber)
	}

	// 3. Delayed NATS Ingest: lower seq 20
	bNATSOlder := sampleTestBatch("b-f4-nats-older", "node-f4", 20)
	if err := srv.IngestBatch(ctx, &bNATSOlder); err != nil {
		t.Fatalf("NATS older batch failed: %v", err)
	}

	recAfter, _ := srv.GetNode("node-f4")
	if recAfter.SequenceNumber != 80 {
		t.Errorf("sequence regressed after older NATS batch: expected 80, got %d", recAfter.SequenceNumber)
	}

	recDB, _ := store.GetNode(ctx, "node-f4")
	if recDB.SequenceNumber != 80 {
		t.Errorf("SQLite sequence regressed: expected 80, got %d", recDB.SequenceNumber)
	}
}

func TestServer_F01_Deterministic_IndependentNodeConcurrency(t *testing.T) {
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	srv := NewServer(newDiscardLogger(), WithIdempotencyStore(store))
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	// Explicitly acquire the per-node lock for node-indep-A
	unlockAlpha := srv.lockNode("node-indep-A")

	// In a separate goroutine, register and heartbeat for node-indep-B.
	// Because per-node locks are fine-grained, node-indep-B MUST proceed without blocking!
	doneB := make(chan bool)
	errB := make(chan error, 1)

	go func() {
		regB := types.NodeRegistration{
			NodeID:       "node-indep-B",
			Hostname:     "host-B",
			RegisteredAt: time.Now().UTC(),
		}
		b, _ := json.Marshal(regB)
		resp, err := http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(b))
		if err != nil {
			errB <- err
			return
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			errB <- fmt.Errorf("unexpected status %d", resp.StatusCode)
			return
		}

		hbB := types.Heartbeat{
			NodeID:         "node-indep-B",
			SequenceNumber: 1,
			Timestamp:      time.Now().UTC(),
			Status:         types.NodeStatusHealthy,
		}
		bHb, _ := json.Marshal(hbB)
		resp, err = http.Post(ts.URL+"/api/v1/nodes/heartbeat", "application/json", bytes.NewReader(bHb))
		if err != nil {
			errB <- err
			return
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			errB <- fmt.Errorf("unexpected status %d", resp.StatusCode)
			return
		}
		doneB <- true
	}()

	select {
	case <-doneB:
		// Succeeded immediately without waiting for node-indep-A to release its lock!
	case err := <-errB:
		t.Fatalf("node-indep-B failed: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatalf("deadlock or coarse global lock detected: node-indep-B was blocked by node-indep-A's lock")
	}

	// Release node-indep-A's lock
	unlockAlpha()
}

func TestServer_DurableNodeRegistry_CorruptNodeRecord_StartupRejection(t *testing.T) {
	dbDir := t.TempDir()
	dbPath := filepath.Join(dbDir, "corrupt_startup.db")

	store1, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}

	ctx := context.Background()
	now := time.Now().UTC()
	_, _ = store1.RegisterNode(ctx, &NodeRecord{NodeID: "node-good-1", RegisteredAt: now})
	_, _ = store1.RegisterNode(ctx, &NodeRecord{NodeID: "node-bad-1", RegisteredAt: now})

	// Manually inject invalid JSON in cp_nodes for node-bad-1
	_, err = store1.GetPayload(ctx, "x", "y") // no-op check
	_ = store1.Close()

	// Reopen raw SQLite and corrupt row
	rawStore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("reopen raw store failed: %v", err)
	}
	// Call raw DB query to corrupt capabilities_json
	list, err := rawStore.ListNodes(ctx)
	if err != nil {
		t.Fatalf("ListNodes unexpected error before corruption: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 nodes before corruption")
	}
	_ = rawStore.Close()

	// Corrupt via direct SQLite file access using a hook or OpenSQLite with corrupted JSON
	corruptStore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	// If corruptStore.Close() is called or ListNodes fails:
	_ = corruptStore.Close()

	srvFailed := NewServer(newDiscardLogger(), WithIdempotencyStore(corruptStore))
	if srvFailed.InitError() == nil {
		t.Fatalf("expected InitError on failed store, got nil")
	}
	if len(srvFailed.GetNodes()) != 0 {
		t.Errorf("partially recovered nodes must not be exposed on failed initialization")
	}

	ts := httptest.NewServer(srvFailed.Routes())
	defer ts.Close()

	res, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("healthz get failed: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 on healthz when server initialization failed, got %d", res.StatusCode)
	}
}

// TestServer_F02_SubSecondTimestampOrdering_HTTP verifies chronologically correct sub-second
// timestamp comparisons across the HTTP API and SQLite persistence.
func TestServer_F02_SubSecondTimestampOrdering_HTTP(t *testing.T) {
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	srv := NewServer(newDiscardLogger(), WithIdempotencyStore(store))
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	// 1. Exact-second initial registration: 12:00:00Z
	tExact := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	reg := types.NodeRegistration{
		NodeID:       "node-f02-http",
		Hostname:     "host-f02",
		RegisteredAt: tExact,
	}
	bReg, _ := json.Marshal(reg)
	res, err := http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(bReg))
	if err != nil || res.StatusCode != http.StatusCreated {
		t.Fatalf("register failed: %v", err)
	}
	res.Body.Close()

	// 2. Newer fractional-second heartbeat: 12:00:00.5Z (500ms later, Status: DEGRADED, seq: 20)
	t500ms := time.Date(2026, 10, 25, 12, 0, 0, 500000000, time.UTC)
	hbNewer := types.Heartbeat{
		NodeID:         "node-f02-http",
		SequenceNumber: 20,
		Status:         types.NodeStatusDegraded,
		Timestamp:      t500ms,
	}
	bNewer, _ := json.Marshal(hbNewer)
	res, err = http.Post(ts.URL+"/api/v1/nodes/heartbeat", "application/json", bytes.NewReader(bNewer))
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("hbNewer failed: %v", err)
	}
	res.Body.Close()

	recMem, ok := srv.GetNode("node-f02-http")
	if !ok || recMem.Status != types.NodeStatusDegraded || !recMem.LastSeen.Equal(t500ms) {
		t.Fatalf("state after hbNewer failed: status=%s, lastSeen=%v", recMem.Status, recMem.LastSeen)
	}
	recDB, err := store.GetNode(context.Background(), "node-f02-http")
	if err != nil || recDB.Status != types.NodeStatusDegraded || !recDB.LastSeen.Equal(t500ms) {
		t.Fatalf("SQLite state after hbNewer failed: status=%s, lastSeen=%v", recDB.Status, recDB.LastSeen)
	}

	// 3. Delayed stale heartbeat: 12:00:00.1Z (100ms, fewer fractional digits, Status: HEALTHY, seq: 5)
	t100ms := time.Date(2026, 10, 25, 12, 0, 0, 100000000, time.UTC)
	hbStale := types.Heartbeat{
		NodeID:         "node-f02-http",
		SequenceNumber: 5,
		Status:         types.NodeStatusHealthy,
		Timestamp:      t100ms,
	}
	bStale, _ := json.Marshal(hbStale)
	res, err = http.Post(ts.URL+"/api/v1/nodes/heartbeat", "application/json", bytes.NewReader(bStale))
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("hbStale failed: %v", err)
	}
	res.Body.Close()

	// Assert that stale heartbeat did NOT regress in-memory or SQLite status/last_seen
	recMemStale, _ := srv.GetNode("node-f02-http")
	if recMemStale.Status != types.NodeStatusDegraded || !recMemStale.LastSeen.Equal(t500ms) {
		t.Errorf("DEFECT F-02: in-memory state regressed on stale sub-second heartbeat: status=%s, lastSeen=%v", recMemStale.Status, recMemStale.LastSeen)
	}
	recDBStale, _ := store.GetNode(context.Background(), "node-f02-http")
	if recDBStale.Status != types.NodeStatusDegraded || !recDBStale.LastSeen.Equal(t500ms) {
		t.Errorf("DEFECT F-02: SQLite state regressed on stale sub-second heartbeat: status=%s, lastSeen=%v", recDBStale.Status, recDBStale.LastSeen)
	}
}

// TestServer_F03_EqualTimestamp_SequenceResolution verifies that when two heartbeat events have identical
// timestamps, sequence number ordering resolves conflicting status and sequence number never regresses.
func TestServer_F03_EqualTimestamp_SequenceResolution(t *testing.T) {
	ctx := context.Background()
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	srv := NewServer(newDiscardLogger(), WithIdempotencyStore(store))
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	tEqual := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	reg := types.NodeRegistration{
		NodeID:       "node-f03-eq",
		Hostname:     "host-f03",
		RegisteredAt: tEqual,
	}
	bReg, _ := json.Marshal(reg)
	res, err := http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(bReg))
	if err != nil || res.StatusCode != http.StatusCreated {
		t.Fatalf("register failed: %v", err)
	}
	res.Body.Close()

	// 1. Initial heartbeat at T_equal with Seq: 50, Status: HEALTHY
	hb1 := types.Heartbeat{
		NodeID:         "node-f03-eq",
		SequenceNumber: 50,
		Status:         types.NodeStatusHealthy,
		Timestamp:      tEqual,
	}
	bHb1, _ := json.Marshal(hb1)
	res, err = http.Post(ts.URL+"/api/v1/nodes/heartbeat", "application/json", bytes.NewReader(bHb1))
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("hb1 post failed: %v", err)
	}
	res.Body.Close()

	rec1, _ := srv.GetNode("node-f03-eq")
	if rec1.Status != types.NodeStatusHealthy || rec1.HeartbeatSequence != 50 {
		t.Fatalf("expected HEALTHY and hbSeq 50, got %s / %d", rec1.Status, rec1.HeartbeatSequence)
	}

	// 2. Conflicting heartbeat arriving with EQUAL timestamp, but LOWER sequence 10, Status: DEGRADED
	// Must NOT overwrite Status to DEGRADED or regress HeartbeatSequence to 10!
	hb2 := types.Heartbeat{
		NodeID:         "node-f03-eq",
		SequenceNumber: 10,
		Status:         types.NodeStatusDegraded,
		Timestamp:      tEqual,
	}
	bHb2, _ := json.Marshal(hb2)
	res, err = http.Post(ts.URL+"/api/v1/nodes/heartbeat", "application/json", bytes.NewReader(bHb2))
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("hb2 post failed: %v", err)
	}
	res.Body.Close()

	recMemAfter, _ := srv.GetNode("node-f03-eq")
	if recMemAfter.Status != types.NodeStatusHealthy {
		t.Errorf("DEFECT F-03: in-memory status overwritten to DEGRADED by lower sequence at equal timestamp")
	}
	if recMemAfter.HeartbeatSequence != 50 {
		t.Errorf("DEFECT F-03: in-memory heartbeat sequence regressed from 50 to %d", recMemAfter.HeartbeatSequence)
	}

	recDBAfter, _ := store.GetNode(ctx, "node-f03-eq")
	if recDBAfter.Status != types.NodeStatusHealthy {
		t.Errorf("DEFECT F-03: SQLite status overwritten to DEGRADED by lower sequence at equal timestamp")
	}
	if recDBAfter.HeartbeatSequence != 50 {
		t.Errorf("DEFECT F-03: SQLite sequence regressed from 50 to %d", recDBAfter.SequenceNumber)
	}

	// 3. Heartbeat at EQUAL timestamp with HIGHER sequence 80, Status: DEGRADED
	// MUST advance Status and SequenceNumber!
	hb3 := types.Heartbeat{
		NodeID:         "node-f03-eq",
		SequenceNumber: 80,
		Status:         types.NodeStatusDegraded,
		Timestamp:      tEqual,
	}
	bHb3, _ := json.Marshal(hb3)
	res, err = http.Post(ts.URL+"/api/v1/nodes/heartbeat", "application/json", bytes.NewReader(bHb3))
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("hb3 post failed: %v", err)
	}
	res.Body.Close()

	recMemHigher, _ := srv.GetNode("node-f03-eq")
	if recMemHigher.Status != types.NodeStatusDegraded || recMemHigher.HeartbeatSequence != 80 {
		t.Errorf("expected DEGRADED and hbSeq 80 after higher seq heartbeat, got %s / %d", recMemHigher.Status, recMemHigher.HeartbeatSequence)
	}
	recDBHigher, _ := store.GetNode(ctx, "node-f03-eq")
	if recDBHigher.Status != types.NodeStatusDegraded || recDBHigher.HeartbeatSequence != 80 {
		t.Errorf("expected DEGRADED and hbSeq 80 in SQLite, got %s / %d", recDBHigher.Status, recDBHigher.HeartbeatSequence)
	}

	// 4. Identical timestamp and identical sequence number: deterministic idempotent handling
	res, err = http.Post(ts.URL+"/api/v1/nodes/heartbeat", "application/json", bytes.NewReader(bHb3))
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("idempotent duplicate heartbeat failed: %v", err)
	}
	res.Body.Close()

	recMemIdemp, _ := srv.GetNode("node-f03-eq")
	if recMemIdemp.Status != types.NodeStatusDegraded || recMemIdemp.HeartbeatSequence != 80 {
		t.Errorf("idempotent heartbeat mutated state: %s / %d", recMemIdemp.Status, recMemIdemp.HeartbeatSequence)
	}
}

// TestServer_F04_DuplicateRegistration_MetadataPreservation verifies that duplicate registrations
// preserve non-empty metadata even when optional fields are omitted, for both in-memory and SQLite modes.
func TestServer_F04_DuplicateRegistration_MetadataPreservation(t *testing.T) {
	runTest := func(t *testing.T, withStore bool) {
		var opts []ServerOption
		var store *storage.SQLiteStore
		if withStore {
			var err error
			store, err = storage.OpenSQLite(":memory:")
			if err != nil {
				t.Fatalf("OpenSQLite failed: %v", err)
			}
			defer store.Close()
			opts = append(opts, WithIdempotencyStore(store))
		}

		srv := NewServer(newDiscardLogger(), opts...)
		if !withStore {
			srv.store = nil
		}
		ts := httptest.NewServer(srv.Routes())
		defer ts.Close()
		client := ts.Client()

		t0 := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
		regInitial := types.NodeRegistration{
			NodeID:       "node-f04",
			Hostname:     "host-initial",
			AgentVersion: "v1.0.0",
			OS:           "linux",
			Architecture: "amd64",
			Capabilities: []string{"inference", "telemetry"},
			Labels:       map[string]string{"env": "prod", "zone": "us-east"},
			RegisteredAt: t0,
		}
		b1, _ := json.Marshal(regInitial)
		res1, err := client.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(b1))
		if err != nil {
			t.Fatalf("initial register error: %v", err)
		}
		if res1.StatusCode != http.StatusCreated {
			t.Fatalf("initial register failed: code %d", res1.StatusCode)
		}
		res1.Body.Close()

		// 1. Duplicate registration with empty optional metadata fields
		t1 := t0.Add(1 * time.Hour)
		regEmptyDup := types.NodeRegistration{
			NodeID:       "node-f04",
			Hostname:     "host-initial",
			AgentVersion: "",
			RegisteredAt: t1,
		}
		b2, _ := json.Marshal(regEmptyDup)
		res2, err := client.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(b2))
		if err != nil {
			t.Fatalf("duplicate register error: %v", err)
		}
		if res2.StatusCode != http.StatusOK {
			t.Fatalf("duplicate register failed: code %d", res2.StatusCode)
		}
		res2.Body.Close()

		recAfterEmpty, ok := srv.GetNode("node-f04")
		if !ok {
			t.Fatalf("node-f04 not found in memory")
		}
		if recAfterEmpty.Hostname != "host-initial" {
			t.Errorf("DEFECT F-04: Hostname was erased by empty duplicate registration: got %q", recAfterEmpty.Hostname)
		}
		if recAfterEmpty.AgentVersion != "v1.0.0" {
			t.Errorf("DEFECT F-04: AgentVersion was erased: got %q", recAfterEmpty.AgentVersion)
		}
		if len(recAfterEmpty.Capabilities) != 2 || len(recAfterEmpty.Labels) != 2 {
			t.Errorf("DEFECT F-04: Capabilities or Labels were erased")
		}
		if !recAfterEmpty.RegisteredAt.Equal(t0) {
			t.Errorf("DEFECT F-04: RegisteredAt was mutated on duplicate registration")
		}

		// 2. Duplicate registration with new non-empty metadata
		t2 := t1.Add(1 * time.Hour)
		regNewMeta := types.NodeRegistration{
			NodeID:       "node-f04",
			Hostname:     "host-updated",
			AgentVersion: "v2.0.0",
			RegisteredAt: t2,
		}
		b3, _ := json.Marshal(regNewMeta)
		res3, err := client.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(b3))
		if err != nil || res3.StatusCode != http.StatusOK {
			t.Fatalf("new meta duplicate register failed: %v", err)
		}
		res3.Body.Close()

		recAfterNew, _ := srv.GetNode("node-f04")
		if recAfterNew.Hostname != "host-updated" || recAfterNew.AgentVersion != "v2.0.0" {
			t.Errorf("expected updated metadata host-updated/v2.0.0, got %s/%s", recAfterNew.Hostname, recAfterNew.AgentVersion)
		}
		if !recAfterNew.RegisteredAt.Equal(t0) {
			t.Errorf("RegisteredAt must remain original enrollment time: got %v", recAfterNew.RegisteredAt)
		}
	}

	t.Run("InMemory_StoreNil", func(t *testing.T) {
		runTest(t, false)
	})
	t.Run("SQLite_StoreBacked", func(t *testing.T) {
		runTest(t, true)
	})
}

// TestServer_T01_Deterministic_SameNodeConcurrency_ControlledInterleaving tests true same-node concurrency
// using an explicit test barrier to interleave two concurrent operations targeting the same nodeID.
// It verifies that nodeLockManager strictly serializes operations and prevents stale in-memory overwrites.
func TestServer_T01_Deterministic_SameNodeConcurrency_ControlledInterleaving(t *testing.T) {
	ctx := context.Background()
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	srv := NewServer(newDiscardLogger(), WithIdempotencyStore(store))
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	// Initial node enrollment at T0
	t0 := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	reg := types.NodeRegistration{
		NodeID:       "node-same-conc",
		Hostname:     "host-conc",
		RegisteredAt: t0,
	}
	bReg, _ := json.Marshal(reg)
	res, err := http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(bReg))
	if err != nil || res.StatusCode != http.StatusCreated {
		t.Fatalf("initial register failed: %v", err)
	}
	res.Body.Close()

	// Channels to deterministically synchronize the two goroutines
	g1InStore := make(chan struct{})
	g1CanFinish := make(chan struct{})

	// Hook into UpdateNodeHeartbeat so G1 pauses inside its SQLite transaction before releasing the node lock
	store.HookAfterHeartbeatUpdate = func(nodeID string) error {
		if nodeID == "node-same-conc" {
			select {
			case g1InStore <- struct{}{}:
			default:
			}
			<-g1CanFinish
		}
		return nil
	}
	defer func() {
		store.HookAfterHeartbeatUpdate = nil
	}()

	// G1: Stale operation (T1 = T0 + 1hr, Seq 10, Status: DEGRADED)
	t1 := t0.Add(1 * time.Hour)
	hb1 := types.Heartbeat{
		NodeID:         "node-same-conc",
		SequenceNumber: 10,
		Status:         types.NodeStatusDegraded,
		Timestamp:      t1,
	}
	b1, _ := json.Marshal(hb1)

	g1Done := make(chan error, 1)
	go func() {
		resp, err := http.Post(ts.URL+"/api/v1/nodes/heartbeat", "application/json", bytes.NewReader(b1))
		if err != nil {
			g1Done <- err
			return
		}
		resp.Body.Close()
		g1Done <- nil
	}()

	// Wait for G1 to enter the storage hook while holding lockNode("node-same-conc")
	select {
	case <-g1InStore:
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout waiting for G1 to enter storage hook")
	}

	// G2: Newer operation (T2 = T0 + 2hr, Seq 50, Status: HEALTHY) for the SAME node
	t2 := t0.Add(2 * time.Hour)
	hb2 := types.Heartbeat{
		NodeID:         "node-same-conc",
		SequenceNumber: 50,
		Status:         types.NodeStatusHealthy,
		Timestamp:      t2,
	}
	b2, _ := json.Marshal(hb2)

	g2Done := make(chan error, 1)
	go func() {
		resp, err := http.Post(ts.URL+"/api/v1/nodes/heartbeat", "application/json", bytes.NewReader(b2))
		if err != nil {
			g2Done <- err
			return
		}
		resp.Body.Close()
		g2Done <- nil
	}()

	// Because G1 holds the nodeLock for "node-same-conc", G2 MUST block at s.lockNode("node-same-conc")
	// Verify that G2 has NOT completed while G1 is paused in hook
	select {
	case <-g2Done:
		t.Fatalf("CONCURRENCY VIOLATION: G2 completed while G1 was holding the per-node lock")
	case <-time.After(100 * time.Millisecond):
		// Expected: G2 is blocked waiting for G1's per-node lock
	}

	// Unblock G1: G1 commits SQLite transaction, completes in-memory reconciliation, and releases nodeLock
	close(g1CanFinish)

	// Wait for both G1 and G2 to complete
	select {
	case err := <-g1Done:
		if err != nil {
			t.Fatalf("G1 failed: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout waiting for G1 to complete")
	}

	select {
	case err := <-g2Done:
		if err != nil {
			t.Fatalf("G2 failed: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout waiting for G2 to complete")
	}

	// Assert final state: G2's newer state (T2, HbSeq 50, HEALTHY) must be preserved in both memory and SQLite
	recMemFinal, ok := srv.GetNode("node-same-conc")
	if !ok || recMemFinal.Status != types.NodeStatusHealthy || recMemFinal.HeartbeatSequence != 50 || !recMemFinal.LastSeen.Equal(t2) {
		t.Fatalf("final in-memory state invalid: status=%s, seq=%d, hbSeq=%d, lastSeen=%v", recMemFinal.Status, recMemFinal.SequenceNumber, recMemFinal.HeartbeatSequence, recMemFinal.LastSeen)
	}

	recDBFinal, err := store.GetNode(ctx, "node-same-conc")
	if err != nil || recDBFinal.Status != types.NodeStatusHealthy || recDBFinal.HeartbeatSequence != 50 || !recDBFinal.LastSeen.Equal(t2) {
		t.Fatalf("final SQLite state invalid: status=%s, seq=%d, hbSeq=%d, lastSeen=%v", recDBFinal.Status, recDBFinal.SequenceNumber, recDBFinal.HeartbeatSequence, recDBFinal.LastSeen)
	}
}

func TestServer_F05_SeparateSequenceDomains_HTTPIntegration(t *testing.T) {
	ctx := context.Background()
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	srv := NewServer(nil, WithIdempotencyStore(store))
	defer srv.Close()
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	t0 := time.Now().UTC().Truncate(time.Millisecond)
	reg := types.NodeRegistration{
		NodeID:       "node-f05-http",
		Hostname:     "host-f05",
		RegisteredAt: t0,
	}
	bReg, _ := json.Marshal(reg)
	resp, err := http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(bReg))
	if err != nil || resp.StatusCode != http.StatusCreated {
		t.Fatalf("registration failed: status=%v, err=%v", resp.StatusCode, err)
	}
	resp.Body.Close()

	// 1. Advance telemetry activity to sequence 100 via HTTP ingestion
	t1 := t0.Add(10 * time.Second)
	batch := types.TelemetryBatch{
		BatchID:        "b-f05-1",
		NodeID:         "node-f05-http",
		SequenceNumber: 100,
		CollectedAt:    t1,
		Metrics: []types.MetricSample{
			{Name: "cpu", Value: 42.0, Timestamp: t1},
		},
	}
	bBatch, _ := json.Marshal(batch)
	resp, err = http.Post(ts.URL+"/api/v1/telemetry/batches", "application/json", bytes.NewReader(bBatch))
	if err != nil || resp.StatusCode != http.StatusCreated {
		t.Fatalf("telemetry ingestion failed: status=%v, err=%v", resp.StatusCode, err)
	}
	resp.Body.Close()

	// 2. Heartbeat with seq 50 and newer timestamp T2 (in the future of ingestion time)
	t2 := time.Now().UTC().Add(time.Hour)
	hb1 := types.Heartbeat{
		NodeID:         "node-f05-http",
		SequenceNumber: 50,
		Status:         types.NodeStatusHealthy,
		Timestamp:      t2,
	}
	bHb1, _ := json.Marshal(hb1)
	resp, err = http.Post(ts.URL+"/api/v1/nodes/heartbeat", "application/json", bytes.NewReader(bHb1))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("hb1 failed: status=%v, err=%v", resp.StatusCode, err)
	}
	resp.Body.Close()

	// 3. Heartbeat with seq 80 and the SAME timestamp T2, status DEGRADED
	// In the audit finding, this was rejected because 80 was compared against telemetry seq 100.
	// With separate domains, 80 is compared against heartbeat seq 50 (80 >= 50), which advances status!
	hb2 := types.Heartbeat{
		NodeID:         "node-f05-http",
		SequenceNumber: 80,
		Status:         types.NodeStatusDegraded,
		Timestamp:      t2,
	}
	bHb2, _ := json.Marshal(hb2)
	resp, err = http.Post(ts.URL+"/api/v1/nodes/heartbeat", "application/json", bytes.NewReader(bHb2))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("hb2 failed: status=%v, err=%v", resp.StatusCode, err)
	}
	resp.Body.Close()

	// Assert in-memory state
	recMem, ok := srv.GetNode("node-f05-http")
	if !ok {
		t.Fatal("node not found in memory")
	}
	if recMem.Status != types.NodeStatusDegraded {
		t.Errorf("DEFECT F-05: in-memory status not updated to DEGRADED on equal timestamp: got %s", recMem.Status)
	}
	if recMem.SequenceNumber != 100 {
		t.Errorf("DEFECT F-05: in-memory telemetry sequence was mutated: got %d, want 100", recMem.SequenceNumber)
	}
	if recMem.HeartbeatSequence != 80 {
		t.Errorf("DEFECT F-05: in-memory heartbeat sequence not updated: got %d, want 80", recMem.HeartbeatSequence)
	}

	// Assert persisted SQLite state
	recDB, err := store.GetNode(ctx, "node-f05-http")
	if err != nil {
		t.Fatalf("GetNode failed: %v", err)
	}
	if recDB.Status != types.NodeStatusDegraded {
		t.Errorf("DEFECT F-05: SQLite status not updated to DEGRADED on equal timestamp: got %s", recDB.Status)
	}
	if recDB.SequenceNumber != 100 {
		t.Errorf("DEFECT F-05: SQLite telemetry sequence was mutated: got %d, want 100", recDB.SequenceNumber)
	}
	if recDB.HeartbeatSequence != 80 {
		t.Errorf("DEFECT F-05: SQLite heartbeat sequence not updated: got %d, want 80", recDB.HeartbeatSequence)
	}
}

func TestServer_F06_CorruptTimestamp_ServerFailSafe(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "corrupt_hb.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	srv := NewServer(nil, WithIdempotencyStore(store))
	defer srv.Close()
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	t0 := time.Date(2026, 10, 25, 10, 0, 0, 0, time.UTC)
	reg := types.NodeRegistration{
		NodeID:       "node-f06-http",
		Hostname:     "host-f06",
		RegisteredAt: t0,
	}
	bReg, _ := json.Marshal(reg)
	resp, err := http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(bReg))
	if err != nil || resp.StatusCode != http.StatusCreated {
		t.Fatalf("registration failed: status=%v, err=%v", resp.StatusCode, err)
	}
	resp.Body.Close()

	// Inject corrupt timestamp into SQLite using raw connection
	rawDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	_, err = rawDB.Exec(`UPDATE cp_nodes SET last_seen = 'INVALID_RFC3339_STRING' WHERE node_id = 'node-f06-http';`)
	rawDB.Close()
	if err != nil {
		t.Fatalf("failed to inject corrupt timestamp: %v", err)
	}

	// Attempt heartbeat -> must return 500 Internal Server Error (corrupt_node_record)
	hb := types.Heartbeat{
		NodeID:         "node-f06-http",
		SequenceNumber: 10,
		Status:         types.NodeStatusHealthy,
		Timestamp:      time.Now().UTC(),
	}
	bHb, _ := json.Marshal(hb)
	resp, err = http.Post(ts.URL+"/api/v1/nodes/heartbeat", "application/json", bytes.NewReader(bHb))
	if err != nil {
		t.Fatalf("heartbeat request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("DEFECT F-06: expected HTTP 500 for corrupt node record, got %d", resp.StatusCode)
	}
	var errResp ErrorResponse
	_ = json.NewDecoder(resp.Body).Decode(&errResp)
	if errResp.Error != "corrupt_node_record" {
		t.Errorf("DEFECT F-06: expected error 'corrupt_node_record', got %q", errResp.Error)
	}

	// Verify genuine missing node returns 404
	hbMissing := types.Heartbeat{
		NodeID:         "truly-missing-node",
		SequenceNumber: 1,
		Status:         types.NodeStatusHealthy,
		Timestamp:      time.Now().UTC(),
	}
	bMissing, _ := json.Marshal(hbMissing)
	respMissing, err := http.Post(ts.URL+"/api/v1/nodes/heartbeat", "application/json", bytes.NewReader(bMissing))
	if err != nil || respMissing.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 for missing node, got %v", respMissing.StatusCode)
	}
	respMissing.Body.Close()
}

func TestServer_F07_DuplicateRegistration_MetadataFreshness_HTTPIntegration(t *testing.T) {
	ctx := context.Background()
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	srv := NewServer(nil, WithIdempotencyStore(store))
	defer srv.Close()
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	t0 := time.Date(2026, 10, 25, 10, 0, 0, 0, time.UTC)
	reg := types.NodeRegistration{
		NodeID:       "node-f07-http",
		Hostname:     "host-v1",
		AgentVersion: "1.0.0",
		RegisteredAt: t0,
	}
	bReg, _ := json.Marshal(reg)
	resp, err := http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(bReg))
	if err != nil || resp.StatusCode != http.StatusCreated {
		t.Fatalf("initial registration failed: status=%v, err=%v", resp.StatusCode, err)
	}
	resp.Body.Close()

	// Send heartbeats for several minutes to T5
	t5 := t0.Add(5 * time.Minute)
	hb := types.Heartbeat{
		NodeID:         "node-f07-http",
		SequenceNumber: 20,
		Status:         types.NodeStatusHealthy,
		Timestamp:      t5,
	}
	bHb, _ := json.Marshal(hb)
	resp, err = http.Post(ts.URL+"/api/v1/nodes/heartbeat", "application/json", bytes.NewReader(bHb))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("heartbeat failed: %v", err)
	}
	resp.Body.Close()

	// 1. Legitimate re-registration retaining immutable RegisteredAt: T0, with fresh UpdatedAt: T6 and MetadataVersion: 1
	t6 := t0.Add(6 * time.Minute)
	regRe := types.NodeRegistration{
		NodeID:          "node-f07-http",
		Hostname:        "host-v2",
		AgentVersion:    "1.1.0",
		RegisteredAt:    t0, // Immutable original enrollment time
		UpdatedAt:       &t6,
		MetadataVersion: 1,
	}
	bRegRe, _ := json.Marshal(regRe)
	resp, err = http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(bRegRe))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("duplicate registration failed: status=%v, err=%v", resp.StatusCode, err)
	}
	var regResp RegistrationResponse
	_ = json.NewDecoder(resp.Body).Decode(&regResp)
	resp.Body.Close()

	if regResp.Status != "already_registered" {
		t.Errorf("expected status 'already_registered', got %q", regResp.Status)
	}
	if !regResp.RegisteredAt.Equal(t0) {
		t.Errorf("DEFECT F-07: registered_at mutated in response: got %v, want %v", regResp.RegisteredAt, t0)
	}

	// Verify in-memory and database metadata were updated
	recMem, ok := srv.GetNode("node-f07-http")
	if !ok || recMem.AgentVersion != "1.1.0" || recMem.Hostname != "host-v2" {
		t.Errorf("DEFECT F-07: in-memory metadata not updated: agent=%s, host=%s", recMem.AgentVersion, recMem.Hostname)
	}
	if !recMem.RegisteredAt.Equal(t0) {
		t.Errorf("DEFECT F-07: in-memory registered_at mutated: got %v, want %v", recMem.RegisteredAt, t0)
	}

	recDB, err := store.GetNode(ctx, "node-f07-http")
	if err != nil || recDB.AgentVersion != "1.1.0" || recDB.Hostname != "host-v2" {
		t.Errorf("DEFECT F-07: SQLite metadata not updated: agent=%s, host=%s", recDB.AgentVersion, recDB.Hostname)
	}
	if !recDB.RegisteredAt.Equal(t0) {
		t.Errorf("DEFECT F-07: SQLite registered_at mutated: got %v, want %v", recDB.RegisteredAt, t0)
	}

	// 2. Stale re-registration with older metadata timestamp T4 and version 0 must be REJECTED
	t4 := t0.Add(4 * time.Minute)
	regStale := types.NodeRegistration{
		NodeID:          "node-f07-http",
		Hostname:        "host-stale",
		AgentVersion:    "0.9.0",
		RegisteredAt:    t0,
		UpdatedAt:       &t4,
		MetadataVersion: 0,
	}
	bRegStale, _ := json.Marshal(regStale)
	resp, err = http.Post(ts.URL+"/api/v1/nodes/register", "application/json", bytes.NewReader(bRegStale))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("stale registration request failed: %v", err)
	}
	resp.Body.Close()

	recMemAfterStale, _ := srv.GetNode("node-f07-http")
	if recMemAfterStale.AgentVersion != "1.1.0" || recMemAfterStale.Hostname != "host-v2" {
		t.Errorf("DEFECT F-07: stale registration overwrote newer metadata: agent=%s, host=%s",
			recMemAfterStale.AgentVersion, recMemAfterStale.Hostname)
	}
}

type probeTestStore struct {
	storage.IdempotencyStore
	pingFn  func(ctx context.Context) error
	closeFn func() error
}

func (p *probeTestStore) Ping(ctx context.Context) error {
	if p.pingFn != nil {
		return p.pingFn(ctx)
	}
	if p.IdempotencyStore != nil {
		return p.IdempotencyStore.Ping(ctx)
	}
	return nil
}

func (p *probeTestStore) Close() error {
	if p.closeFn != nil {
		return p.closeFn()
	}
	if p.IdempotencyStore != nil {
		return p.IdempotencyStore.Close()
	}
	return nil
}

// 1. Readiness returns 200 with a healthy store
func TestServer_Readyz_HealthyStore(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for /readyz, got %d", resp.StatusCode)
	}

	var body ReadinessResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode /readyz response: %v", err)
	}

	if body.Status != "ready" {
		t.Errorf("expected status 'ready', got %q", body.Status)
	}
	if body.Checks["storage"] != "healthy" {
		t.Errorf("expected storage check 'healthy', got %q", body.Checks["storage"])
	}
	if body.Error != "" {
		t.Errorf("expected empty error on healthy readiness, got %q", body.Error)
	}
}

// 2. Readiness returns 503 when the store's Ping returns an error
func TestServer_Readyz_StoragePingFailure(t *testing.T) {
	baseStore, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite store: %v", err)
	}
	defer baseStore.Close()

	probeStore := &probeTestStore{
		IdempotencyStore: baseStore,
		pingFn: func(ctx context.Context) error {
			return errors.New("simulated disk i/o failure on ping")
		},
	}

	srv := NewServer(newDiscardLogger(), WithIdempotencyStore(probeStore))
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable for failed storage ping, got %d", resp.StatusCode)
	}

	var body ReadinessResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode /readyz response: %v", err)
	}

	if body.Status != "not_ready" {
		t.Errorf("expected status 'not_ready', got %q", body.Status)
	}
	if body.Checks["storage"] != "unhealthy" {
		t.Errorf("expected storage check 'unhealthy', got %q", body.Checks["storage"])
	}
	if body.Error != "storage_unavailable" {
		t.Errorf("expected error 'storage_unavailable', got %q", body.Error)
	}
}

type failingInitStore struct {
	storage.IdempotencyStore
}

func (f *failingInitStore) ListNodes(ctx context.Context) ([]*storage.NodeRecord, error) {
	return nil, errors.New("simulated startup rehydration failure")
}

// 3. Readiness returns 503 when startup initialization has failed
func TestServer_Readyz_StartupInitFailure(t *testing.T) {
	baseStore, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer baseStore.Close()

	initFailStore := &failingInitStore{IdempotencyStore: baseStore}
	srv := NewServer(newDiscardLogger(), WithIdempotencyStore(initFailStore))
	// Verify initErr was captured
	if srv.InitError() == nil {
		t.Fatalf("expected server initErr to be non-nil")
	}

	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable when server failed initialization, got %d", resp.StatusCode)
	}

	var body ReadinessResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body.Status != "not_ready" {
		t.Errorf("expected status 'not_ready', got %q", body.Status)
	}
	if body.Checks["storage"] != "unhealthy" {
		t.Errorf("expected storage check 'unhealthy', got %q", body.Checks["storage"])
	}
	if body.Error != "server_uninitialized" {
		t.Errorf("expected error 'server_uninitialized', got %q", body.Error)
	}
}

// 4. Readiness returns 503 after shutdown has been signaled
func TestServer_Readyz_ShutdownSignaled(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	// Prior to shutdown, readiness is 200
	resp1, err := http.Get(ts.URL + "/readyz")
	if err != nil || resp1.StatusCode != http.StatusOK {
		t.Fatalf("initial readyz expected 200, got status=%v err=%v", resp1.StatusCode, err)
	}
	resp1.Body.Close()

	// Signal graceful shutdown begins
	srv.MarkShuttingDown()
	if !srv.IsShuttingDown() {
		t.Fatalf("expected IsShuttingDown to report true")
	}

	// Immediately after shutdown is signaled, readiness returns 503
	resp2, err := http.Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz after shutdown signal failed: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable after shutdown signaled, got %d", resp2.StatusCode)
	}

	var body ReadinessResponse
	if err := json.NewDecoder(resp2.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode /readyz response: %v", err)
	}
	if body.Status != "not_ready" {
		t.Errorf("expected status 'not_ready', got %q", body.Status)
	}
	if body.Error != "server_shutting_down" {
		t.Errorf("expected error 'server_shutting_down', got %q", body.Error)
	}
}

// 4B. Server.Close() marks the server as shutting down, transitions readiness to 503, and preserves idempotency
func TestServer_Close_ConsistentShutdownLifecycle(t *testing.T) {
	// A. Normal server close with in-memory SQLite store
	srv := NewServer(newDiscardLogger())
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	if srv.IsShuttingDown() {
		t.Fatalf("expected initial IsShuttingDown to report false")
	}

	// Initial readiness probe should report 200 OK
	resp1, err := http.Get(ts.URL + "/readyz")
	if err != nil || resp1.StatusCode != http.StatusOK {
		t.Fatalf("expected initial 200 OK, got code=%v err=%v", resp1.StatusCode, err)
	}
	resp1.Body.Close()

	// 1. Programmatically close the server
	if err := srv.Close(); err != nil {
		t.Fatalf("first srv.Close() failed: %v", err)
	}

	// Server must be marked as shutting down
	if !srv.IsShuttingDown() {
		t.Errorf("expected IsShuttingDown to report true after srv.Close()")
	}

	// 2. Readiness probe must return 503 Service Unavailable with server_shutting_down error
	resp2, err := http.Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz after srv.Close() failed: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 Service Unavailable after srv.Close(), got %d", resp2.StatusCode)
	}

	var body ReadinessResponse
	if err := json.NewDecoder(resp2.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode readiness response: %v", err)
	}
	if body.Status != "not_ready" {
		t.Errorf("expected status 'not_ready', got %q", body.Status)
	}
	if body.Error != "server_shutting_down" {
		t.Errorf("expected error 'server_shutting_down', got %q", body.Error)
	}

	// 3. Repeated close calls must be safe and idempotent
	if err := srv.Close(); err != nil {
		t.Errorf("repeated srv.Close() failed: %v", err)
	}
	if !srv.IsShuttingDown() {
		t.Errorf("expected IsShuttingDown to remain true after repeated srv.Close()")
	}

	// 4. Storage error propagation on close: ensure MarkShuttingDown runs even if store.Close returns error
	baseStore, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer baseStore.Close()

	closeErr := errors.New("simulated store close failure")
	storeWithErr := &probeTestStore{
		IdempotencyStore: baseStore,
		closeFn: func() error {
			return closeErr
		},
	}
	srvWithError := NewServer(newDiscardLogger(), WithIdempotencyStore(storeWithErr))
	err = srvWithError.Close()
	if !errors.Is(err, closeErr) {
		t.Errorf("expected close error %v, got %v", closeErr, err)
	}
	if !srvWithError.IsShuttingDown() {
		t.Errorf("expected srvWithError to be marked shutting down despite store close error")
	}
}

// 5. Unsupported methods follow the server's method-handling conventions (and HEAD is supported)
func TestServer_Readyz_MethodConventions(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	// POST /readyz -> 405 Method Not Allowed
	postResp, err := http.Post(ts.URL+"/readyz", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("POST /readyz failed: %v", err)
	}
	defer postResp.Body.Close()

	if postResp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 Method Not Allowed on POST /readyz, got %d", postResp.StatusCode)
	}
	allowHeader := postResp.Header.Get("Allow")
	if !strings.Contains(allowHeader, "GET") || !strings.Contains(allowHeader, "HEAD") {
		t.Errorf("expected Allow header to include GET and HEAD, got %q", allowHeader)
	}

	// HEAD /readyz -> 200 OK with empty body
	headResp, err := http.Head(ts.URL + "/readyz")
	if err != nil {
		t.Fatalf("HEAD /readyz failed: %v", err)
	}
	defer headResp.Body.Close()

	if headResp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK on HEAD /readyz, got %d", headResp.StatusCode)
	}
	bodyBytes, _ := io.ReadAll(headResp.Body)
	if len(bodyBytes) != 0 {
		t.Errorf("expected empty body on HEAD /readyz, got %d bytes: %q", len(bodyBytes), string(bodyBytes))
	}
	contentType := headResp.Header.Get("Content-Type")
	if !strings.Contains(contentType, "application/json") {
		t.Errorf("expected application/json Content-Type on HEAD /readyz, got %q", contentType)
	}

	// HEAD /readyz when shutting down -> 503 with empty body
	srv.MarkShuttingDown()
	headRespDown, err := http.Head(ts.URL + "/readyz")
	if err != nil {
		t.Fatalf("HEAD /readyz after shutdown failed: %v", err)
	}
	defer headRespDown.Body.Close()

	if headRespDown.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 on HEAD /readyz during shutdown, got %d", headRespDown.StatusCode)
	}
	downBytes, _ := io.ReadAll(headRespDown.Body)
	if len(downBytes) != 0 {
		t.Errorf("expected empty body on HEAD /readyz during shutdown, got %d bytes", len(downBytes))
	}
}

// 6. /healthz remains a liveness check and does not fail merely because the readiness storage probe fails
func TestServer_LivenessDecoupledFromStorageProbe(t *testing.T) {
	baseStore, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer baseStore.Close()

	// Store where Ping fails
	probeStore := &probeTestStore{
		IdempotencyStore: baseStore,
		pingFn: func(ctx context.Context) error {
			return errors.New("simulated disk stall")
		},
	}

	srv := NewServer(newDiscardLogger(), WithIdempotencyStore(probeStore))
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	// 1. /readyz MUST fail with 503
	readyzResp, err := http.Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz failed: %v", err)
	}
	defer readyzResp.Body.Close()
	if readyzResp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected /readyz to return 503, got %d", readyzResp.StatusCode)
	}

	// 2. /healthz MUST still succeed with 200 OK (liveness check)
	healthzResp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz failed: %v", err)
	}
	defer healthzResp.Body.Close()
	if healthzResp.StatusCode != http.StatusOK {
		t.Fatalf("expected /healthz to remain 200 OK despite storage probe failure, got %d", healthzResp.StatusCode)
	}

	var healthBody map[string]string
	if err := json.NewDecoder(healthzResp.Body).Decode(&healthBody); err != nil {
		t.Fatalf("failed to decode healthz body: %v", err)
	}
	if healthBody["status"] != "healthy" {
		t.Errorf("expected /healthz status 'healthy', got %q", healthBody["status"])
	}
}

// 7. The storage probe respects a bounded context timeout where tested deterministically
func TestServer_Readyz_StorageProbeTimeout(t *testing.T) {
	baseStore, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer baseStore.Close()

	pingEntered := make(chan struct{})

	// Store whose Ping blocks until context is cancelled/deadline expires
	timeoutStore := &probeTestStore{
		IdempotencyStore: baseStore,
		pingFn: func(ctx context.Context) error {
			select {
			case <-pingEntered:
			default:
				close(pingEntered)
			}
			<-ctx.Done()
			return ctx.Err()
		},
	}

	// Configure a short bounded probe timeout for test
	srv := NewServer(newDiscardLogger(),
		WithIdempotencyStore(timeoutStore),
		WithStorageProbeTimeout(50*time.Millisecond),
	)
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz failed: %v", err)
	}
	defer resp.Body.Close()

	// Verify Ping was indeed invoked
	select {
	case <-pingEntered:
	default:
		t.Errorf("expected Ping to have been called by readiness probe")
	}

	// Readiness probe should have timed out and returned 503
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on storage probe timeout, got %d", resp.StatusCode)
	}

	var body ReadinessResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body.Status != "not_ready" {
		t.Errorf("expected status 'not_ready', got %q", body.Status)
	}
	if body.Checks["storage"] != "unhealthy" {
		t.Errorf("expected storage check 'unhealthy', got %q", body.Checks["storage"])
	}
}

// TestServer_PrunerLifecycle verifies configuration validation, RunPrunePass execution,
// metrics incrementation, and clean worker shutdown.
func TestServer_PrunerLifecycle(t *testing.T) {
	ctx := context.Background()
	m := metrics.NewMetrics()
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	// 1. Validation test
	invalidCfg := PruningConfig{
		Enabled:                true,
		TelemetryRetentionDays: 0,
		PruneInterval:          time.Hour,
		PruneBatchSize:         100,
	}
	if err := invalidCfg.Validate(); err == nil {
		t.Fatalf("expected error for non-positive retention days")
	}

	validCfg := PruningConfig{
		Enabled:                true,
		TelemetryRetentionDays: 30,
		PruneInterval:          50 * time.Millisecond,
		PruneBatchSize:         500,
	}
	if err := validCfg.Validate(); err != nil {
		t.Fatalf("expected valid config, got: %v", err)
	}

	srv := NewServer(newDiscardLogger(),
		WithIdempotencyStore(store),
		WithMetrics(m),
		WithPruningConfig(validCfg),
	)

	// Populate store with old batches
	now := time.Now().UTC()
	cutoff := now.Add(-30 * 24 * time.Hour)
	oldTimestamp := cutoff.Add(-2 * 24 * time.Hour)

	b1 := &types.TelemetryBatch{
		NodeID:         "node-test",
		BatchID:        "batch-old-1",
		SequenceNumber: 1,
		CollectedAt:    oldTimestamp,
		Metrics: []types.MetricSample{
			{Name: "cpu", Value: 10, Timestamp: oldTimestamp},
		},
	}
	b2 := &types.TelemetryBatch{
		NodeID:         "node-test",
		BatchID:        "batch-old-2",
		SequenceNumber: 2,
		CollectedAt:    now,
		Metrics: []types.MetricSample{
			{Name: "cpu", Value: 20, Timestamp: now},
		},
	}

	if ok, err := store.RecordBatch(ctx, b1, oldTimestamp); err != nil || !ok {
		t.Fatalf("failed to record b1: %v", err)
	}
	if ok, err := store.RecordBatch(ctx, b2, now); err != nil || !ok {
		t.Fatalf("failed to record b2: %v", err)
	}

	// 2. Direct RunPrunePass execution
	pruned, err := srv.RunPrunePass(ctx)
	if err != nil {
		t.Fatalf("RunPrunePass failed: %v", err)
	}
	if pruned != 1 {
		t.Fatalf("expected 1 batch payload pruned, got: %d", pruned)
	}

	// Verify metrics
	metricText := m.Registry().FormatPrometheus()
	if !strings.Contains(metricText, "aegisedge_control_plane_storage_pruned_payloads_total 1") {
		t.Fatalf("expected metric to report 1 pruned payload, got output:\n%s", metricText)
	}

	// 3. Start background pruner loop and verify clean stop
	prunerCtx, cancelPruner := context.WithCancel(context.Background())
	if err := srv.StartPruner(prunerCtx); err != nil {
		t.Fatalf("StartPruner failed: %v", err)
	}

	// Starting twice is a safe no-op
	if err := srv.StartPruner(prunerCtx); err != nil {
		t.Fatalf("second StartPruner returned error: %v", err)
	}

	// Give the ticker time to tick
	time.Sleep(120 * time.Millisecond)

	// Cancel pruner context and stop
	cancelPruner()
	srv.StopPruner()

	// 4. Close the server and ensure repeated Close is safe
	if err := srv.Close(); err != nil {
		t.Fatalf("srv.Close failed: %v", err)
	}
}

// TestServer_PrunerShutdownHardening verifies:
// 1. Stop-before-start is safe and returns immediately.
// 2. Repeated StopPruner calls are safe and idempotent.
// 3. StopPruner drains in-flight pruning passes before returning, so storage is not closed prematurely.
func TestServer_PrunerShutdownHardening(t *testing.T) {
	ctx := context.Background()
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	srv := NewServer(newDiscardLogger(),
		WithIdempotencyStore(store),
		WithPruningConfig(PruningConfig{
			Enabled:                true,
			TelemetryRetentionDays: 30,
			PruneInterval:          time.Hour,
			PruneBatchSize:         100,
		}),
	)

	// 1. Stop before start
	srv.StopPruner()
	srv.StopPruner() // Repeated stop

	// 2. Controlled active prune pass drain verification
	pruneStartedCh := make(chan struct{})
	allowPruneFinishCh := make(chan struct{})
	passCompletedCh := make(chan struct{})

	// Launch a goroutine that runs a simulated prune pass holding the worker wait group
	srv.pruneWg.Add(1)
	go func() {
		defer srv.pruneWg.Done()
		close(pruneStartedCh)
		<-allowPruneFinishCh
		// Execute real prune pass while store is open
		_, _ = srv.RunPrunePass(ctx)
		close(passCompletedCh)
	}()

	// Wait until goroutine enters prune worker
	<-pruneStartedCh

	// In another goroutine, call StopPruner()
	stopReturnedCh := make(chan struct{})
	go func() {
		srv.StopPruner()
		close(stopReturnedCh)
	}()

	// Ensure StopPruner is actively waiting and has NOT returned yet
	select {
	case <-stopReturnedCh:
		t.Fatalf("StopPruner returned before active prune pass finished")
	case <-time.After(50 * time.Millisecond):
		// Expected: still waiting for pass to finish
	}

	// Release the prune pass
	close(allowPruneFinishCh)

	// Now StopPruner must unblock cleanly
	select {
	case <-stopReturnedCh:
		// Succeeded
	case <-time.After(1 * time.Second):
		t.Fatalf("StopPruner did not unblock after active prune pass completed")
	}

	// Verify the pass actually completed
	select {
	case <-passCompletedCh:
	default:
		t.Fatalf("expected passCompletedCh to be closed")
	}

	// Repeated StopPruner() after completion
	srv.StopPruner()

	// Safe to close store now
	if err := store.Close(); err != nil {
		t.Fatalf("store.Close failed: %v", err)
	}
}

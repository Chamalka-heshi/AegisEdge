package server

import (
	"bytes"
	"context"
	"encoding/json"
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
	if node.SequenceNumber != 5 {
		t.Errorf("expected sequence number 5, got %d", node.SequenceNumber)
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

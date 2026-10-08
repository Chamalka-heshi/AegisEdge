package server

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

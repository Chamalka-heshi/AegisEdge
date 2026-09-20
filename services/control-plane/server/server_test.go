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

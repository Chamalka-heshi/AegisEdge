package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/services/control-plane/metrics"
	"github.com/Chamalka-heshi/AegisEdge/services/control-plane/storage"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

func setupQuarantineTestServer(t *testing.T, authEnabled bool, adminToken string) (*Server, *storage.SQLiteStore, *httptest.Server) {
	t.Helper()
	store, err := storage.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}

	srv := NewServer(newDiscardLogger(), WithIdempotencyStore(store), WithQuarantineStore(store))
	srv.SetAuthConfig(AuthConfig{
		Enabled:          authEnabled,
		SharedSecret:     testSharedSecret,
		AdminToken:       adminToken,
		MaxClockSkew:     5 * time.Minute,
		ReplayProtection: true,
	})

	ts := httptest.NewServer(srv.Routes())
	t.Cleanup(func() {
		ts.Close()
		srv.Close()
	})

	return srv, store, ts
}

func doAdminRequest(t *testing.T, ts *httptest.Server, method, path, token string, body []byte) (*http.Response, []byte) {
	t.Helper()
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, ts.URL+path, bodyReader)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request %s %s failed: %v", method, path, err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}
	return resp, respBytes
}

func seedQuarantineRecord(t *testing.T, store storage.QuarantineStore, rec *storage.QuarantineRecord) *storage.QuarantineRecord {
	t.Helper()
	ctx := context.Background()
	saved, err := store.RecordQuarantine(ctx, rec)
	if err != nil {
		t.Fatalf("failed to seed quarantine record: %v", err)
	}
	return saved
}

func makeValidBatchPayload(nodeID, batchID string) []byte {
	batch := types.TelemetryBatch{
		BatchID:        batchID,
		NodeID:         nodeID,
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics: []types.MetricSample{
			{Name: "cpu_usage", Value: 42.5, Timestamp: time.Now().UTC()},
		},
	}
	event := types.TelemetryEvent{
		EventID:        "evt-" + batchID,
		EventType:      "telemetry.batch.v1",
		SchemaVersion:  "1.0",
		SourceNodeID:   nodeID,
		SequenceNumber: 1,
		OccurredAt:     time.Now().UTC(),
		PublishedAt:    time.Now().UTC(),
		Payload:        &batch,
	}
	data, _ := json.Marshal(event)
	return data
}

// ============================================================================
// Authentication and Authorization (Scenarios 1 - 7)
// ============================================================================

// 1. Missing admin credentials.
func TestQuarantineAPI_Auth_MissingAdminCredentials(t *testing.T) {
	_, _, ts := setupQuarantineTestServer(t, true, testAdminToken)

	resp, body := doAdminRequest(t, ts, http.MethodGet, "/api/v1/telemetry/quarantine", "", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got %d (body: %s)", resp.StatusCode, string(body))
	}
}

// 2. Invalid admin Bearer token.
func TestQuarantineAPI_Auth_InvalidAdminBearerToken(t *testing.T) {
	_, _, ts := setupQuarantineTestServer(t, true, testAdminToken)

	resp, body := doAdminRequest(t, ts, http.MethodGet, "/api/v1/telemetry/quarantine", "bogus-admin-token", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got %d (body: %s)", resp.StatusCode, string(body))
	}
}

// 3. Valid node HMAC credentials attempting quarantine access.
func TestQuarantineAPI_Auth_NodeHMACRejected(t *testing.T) {
	_, _, ts := setupQuarantineTestServer(t, true, testAdminToken)

	req := signedRequest(t, http.MethodGet, ts.URL+"/api/v1/telemetry/quarantine", "node-edge-01", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 403 Forbidden for node HMAC, got %d (body: %s)", resp.StatusCode, string(body))
	}
}

// 4. Authentication disabled with an admin token configured, but no Bearer token supplied.
func TestQuarantineAPI_Auth_DisabledMode_NoBearerSupplied(t *testing.T) {
	_, _, ts := setupQuarantineTestServer(t, false, testAdminToken)

	resp, body := doAdminRequest(t, ts, http.MethodGet, "/api/v1/telemetry/quarantine", "", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized when no token supplied, got %d (body: %s)", resp.StatusCode, string(body))
	}
}

// 5. Authentication disabled with a valid admin Bearer token.
func TestQuarantineAPI_Auth_DisabledMode_ValidAdminBearer(t *testing.T) {
	_, _, ts := setupQuarantineTestServer(t, false, testAdminToken)

	resp, body := doAdminRequest(t, ts, http.MethodGet, "/api/v1/telemetry/quarantine", testAdminToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK with valid admin token in disabled mode, got %d (body: %s)", resp.StatusCode, string(body))
	}
}

// 6. Authentication disabled without an admin token configured (fail closed 503).
func TestQuarantineAPI_Auth_DisabledMode_NoAdminTokenConfigured(t *testing.T) {
	_, _, ts := setupQuarantineTestServer(t, false, "")

	resp, body := doAdminRequest(t, ts, http.MethodGet, "/api/v1/telemetry/quarantine", "any-token", nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable when admin token is unconfigured, got %d (body: %s)", resp.StatusCode, string(body))
	}
}

// 7. Valid admin access when authentication is enabled.
func TestQuarantineAPI_Auth_EnabledMode_ValidAdminAccess(t *testing.T) {
	_, _, ts := setupQuarantineTestServer(t, true, testAdminToken)

	resp, body := doAdminRequest(t, ts, http.MethodGet, "/api/v1/telemetry/quarantine", testAdminToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK with valid admin token in enabled mode, got %d (body: %s)", resp.StatusCode, string(body))
	}
}

// ============================================================================
// List Endpoint (Scenarios 8 - 13)
// ============================================================================

// 8. List filtering by status.
func TestQuarantineAPI_List_FilterByStatus(t *testing.T) {
	_, store, ts := setupQuarantineTestServer(t, true, testAdminToken)

	rec1 := seedQuarantineRecord(t, store, &storage.QuarantineRecord{
		ID:              "qrn-stat-1",
		NodeID:          "node-1",
		BatchID:         "batch-1",
		Payload:         []byte("payload-1"),
		FailureCategory: storage.CategoryDeserialization,
	})
	seedQuarantineRecord(t, store, &storage.QuarantineRecord{
		ID:              "qrn-stat-2",
		NodeID:          "node-1",
		BatchID:         "batch-2",
		Payload:         []byte("payload-2"),
		FailureCategory: storage.CategoryDeserialization,
	})
	// Discard rec1
	_, err := store.DiscardQuarantineRecord(context.Background(), rec1.ID, "admin-test", "discarded in test")
	if err != nil {
		t.Fatalf("DiscardQuarantineRecord failed: %v", err)
	}

	// Filter status=quarantined -> rec2 only
	resp, body := doAdminRequest(t, ts, http.MethodGet, "/api/v1/telemetry/quarantine?status=quarantined", testAdminToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d (body: %s)", resp.StatusCode, string(body))
	}
	var listQ []*QuarantineListItem
	if err := json.Unmarshal(body, &listQ); err != nil {
		t.Fatalf("failed to unmarshal list: %v", err)
	}
	if len(listQ) != 1 || listQ[0].ID != "qrn-stat-2" {
		t.Fatalf("expected 1 quarantined record (qrn-stat-2), got %d records", len(listQ))
	}

	// Filter status=discarded -> rec1 only
	resp, body = doAdminRequest(t, ts, http.MethodGet, "/api/v1/telemetry/quarantine?status=discarded", testAdminToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d (body: %s)", resp.StatusCode, string(body))
	}
	var listD []*QuarantineListItem
	if err := json.Unmarshal(body, &listD); err != nil {
		t.Fatalf("failed to unmarshal list: %v", err)
	}
	if len(listD) != 1 || listD[0].ID != rec1.ID {
		t.Fatalf("expected 1 discarded record (%s), got %d records", rec1.ID, len(listD))
	}
}

// 9. List filtering by node ID.
func TestQuarantineAPI_List_FilterByNodeID(t *testing.T) {
	_, store, ts := setupQuarantineTestServer(t, true, testAdminToken)

	seedQuarantineRecord(t, store, &storage.QuarantineRecord{
		ID:              "qrn-node-a",
		NodeID:          "node-target",
		Payload:         []byte("payload-a"),
		FailureCategory: storage.CategoryDeserialization,
	})
	seedQuarantineRecord(t, store, &storage.QuarantineRecord{
		ID:              "qrn-node-b",
		NodeID:          "node-other",
		Payload:         []byte("payload-b"),
		FailureCategory: storage.CategoryDeserialization,
	})

	resp, body := doAdminRequest(t, ts, http.MethodGet, "/api/v1/telemetry/quarantine?node_id=node-target", testAdminToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d (body: %s)", resp.StatusCode, string(body))
	}
	var list []*QuarantineListItem
	if err := json.Unmarshal(body, &list); err != nil {
		t.Fatalf("failed to unmarshal list: %v", err)
	}
	if len(list) != 1 || list[0].NodeID != "node-target" {
		t.Fatalf("expected 1 record for node-target, got %d", len(list))
	}
}

// 10. List filtering by failure category.
func TestQuarantineAPI_List_FilterByFailureCategory(t *testing.T) {
	_, store, ts := setupQuarantineTestServer(t, true, testAdminToken)

	seedQuarantineRecord(t, store, &storage.QuarantineRecord{
		ID:              "qrn-cat-1",
		NodeID:          "node-1",
		Payload:         []byte("payload-1"),
		FailureCategory: storage.CategoryDeserialization,
	})
	seedQuarantineRecord(t, store, &storage.QuarantineRecord{
		ID:              "qrn-cat-2",
		NodeID:          "node-2",
		Payload:         []byte("payload-2"),
		FailureCategory: storage.CategorySchemaValidation,
	})

	resp, body := doAdminRequest(t, ts, http.MethodGet, "/api/v1/telemetry/quarantine?failure_category=schema_validation", testAdminToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d (body: %s)", resp.StatusCode, string(body))
	}
	var list []*QuarantineListItem
	if err := json.Unmarshal(body, &list); err != nil {
		t.Fatalf("failed to unmarshal list: %v", err)
	}
	if len(list) != 1 || list[0].FailureCategory != storage.CategorySchemaValidation {
		t.Fatalf("expected 1 record for schema_validation, got %d", len(list))
	}
}

// 11. Pagination and maximum-limit validation.
func TestQuarantineAPI_List_PaginationAndLimitValidation(t *testing.T) {
	_, store, ts := setupQuarantineTestServer(t, true, testAdminToken)

	for i := 1; i <= 6; i++ {
		seq := uint64(i)
		seedQuarantineRecord(t, store, &storage.QuarantineRecord{
			SourceStream:    "AEGISEDGE_TELEMETRY",
			SourceSequence:  &seq,
			NodeID:          "node-page",
			Payload:         fmt.Appendf(nil, "payload-%d", i),
			FailureCategory: storage.CategoryDeserialization,
		})
	}

	// Page 1: limit 2, offset 0
	resp, body := doAdminRequest(t, ts, http.MethodGet, "/api/v1/telemetry/quarantine?limit=2&offset=0", testAdminToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("page 1 failed: %d (body: %s)", resp.StatusCode, string(body))
	}
	var page1 []*QuarantineListItem
	json.Unmarshal(body, &page1)
	if len(page1) != 2 {
		t.Fatalf("expected 2 records on page 1, got %d", len(page1))
	}

	// Page 2: limit 2, offset 2
	resp, body = doAdminRequest(t, ts, http.MethodGet, "/api/v1/telemetry/quarantine?limit=2&offset=2", testAdminToken, nil)
	var page2 []*QuarantineListItem
	json.Unmarshal(body, &page2)
	if len(page2) != 2 {
		t.Fatalf("expected 2 records on page 2, got %d", len(page2))
	}
	if page1[0].ID == page2[0].ID {
		t.Fatalf("page 1 and page 2 contain identical record ID %s", page1[0].ID)
	}

	// Limit > 500 -> 400 Bad Request
	resp, _ = doAdminRequest(t, ts, http.MethodGet, "/api/v1/telemetry/quarantine?limit=501", testAdminToken, nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for limit 501, got %d", resp.StatusCode)
	}

	// Limit <= 0 -> 400 Bad Request
	resp, _ = doAdminRequest(t, ts, http.MethodGet, "/api/v1/telemetry/quarantine?limit=0", testAdminToken, nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for limit 0, got %d", resp.StatusCode)
	}

	// Negative offset -> 400 Bad Request
	resp, _ = doAdminRequest(t, ts, http.MethodGet, "/api/v1/telemetry/quarantine?offset=-1", testAdminToken, nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for negative offset, got %d", resp.StatusCode)
	}
}

// 12. List JSON excludes raw payload and base64 fields.
func TestQuarantineAPI_List_JSONExcludesRawPayloadAndBase64(t *testing.T) {
	_, store, ts := setupQuarantineTestServer(t, true, testAdminToken)

	seedQuarantineRecord(t, store, &storage.QuarantineRecord{
		ID:              "qrn-no-blob",
		NodeID:          "node-1",
		Payload:         []byte("raw-secret-payload-data"),
		FailureCategory: storage.CategoryDeserialization,
	})

	resp, body := doAdminRequest(t, ts, http.MethodGet, "/api/v1/telemetry/quarantine", testAdminToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	var rawMaps []map[string]any
	if err := json.Unmarshal(body, &rawMaps); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}
	if len(rawMaps) != 1 {
		t.Fatalf("expected 1 element, got %d", len(rawMaps))
	}
	recMap := rawMaps[0]
	if _, exists := recMap["payload"]; exists {
		t.Fatalf("forbidden key 'payload' found in list item: %v", recMap)
	}
	if _, exists := recMap["base64"]; exists {
		t.Fatalf("forbidden key 'base64' found in list item: %v", recMap)
	}
	if _, exists := recMap["payload_size_bytes"]; !exists {
		t.Fatalf("expected 'payload_size_bytes' in list item: %v", recMap)
	}
}

// 13. List payload-size values are correct, including after terminal payload cleanup.
func TestQuarantineAPI_List_PayloadSizeCorrect_IncludingAfterPrune(t *testing.T) {
	_, store, ts := setupQuarantineTestServer(t, true, testAdminToken)

	rawPayload := []byte("some-exact-sized-payload-bytes")
	rec := seedQuarantineRecord(t, store, &storage.QuarantineRecord{
		ID:              "qrn-prune-test",
		NodeID:          "node-1",
		Payload:         rawPayload,
		FailureCategory: storage.CategoryDeserialization,
	})

	// Before prune: payload_size_bytes matches raw length
	resp, body := doAdminRequest(t, ts, http.MethodGet, "/api/v1/telemetry/quarantine", testAdminToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK before prune, got %d", resp.StatusCode)
	}
	var listBefore []*QuarantineListItem
	json.Unmarshal(body, &listBefore)
	if len(listBefore) != 1 || listBefore[0].PayloadSizeBytes != len(rawPayload) {
		t.Fatalf("expected payload_size_bytes=%d before prune, got %d", len(rawPayload), listBefore[0].PayloadSizeBytes)
	}

	// Discard record to make it terminal
	_, err := store.DiscardQuarantineRecord(context.Background(), rec.ID, "admin-test", "terminal for pruning")
	if err != nil {
		t.Fatalf("DiscardQuarantineRecord failed: %v", err)
	}

	// Prune terminal payloads older than future cutoff
	pruned, err := store.PruneQuarantinePayloads(context.Background(), time.Now().UTC().Add(time.Hour), 10)
	if err != nil {
		t.Fatalf("PruneQuarantinePayloads failed: %v", err)
	}
	if pruned != 1 {
		t.Fatalf("expected 1 pruned payload, got %d", pruned)
	}

	// After prune: payload_size_bytes is 0
	resp, body = doAdminRequest(t, ts, http.MethodGet, "/api/v1/telemetry/quarantine", testAdminToken, nil)
	var listAfter []*QuarantineListItem
	json.Unmarshal(body, &listAfter)
	if len(listAfter) != 1 || listAfter[0].PayloadSizeBytes != 0 {
		t.Fatalf("expected payload_size_bytes=0 after prune, got %d", listAfter[0].PayloadSizeBytes)
	}
}

// ============================================================================
// Detail Endpoint (Scenarios 14 - 17)
// ============================================================================

// 14. Valid detail response contains base64 payload and parsed batch.
func TestQuarantineAPI_Detail_ValidWithBase64AndParsedBatch(t *testing.T) {
	_, store, ts := setupQuarantineTestServer(t, true, testAdminToken)

	validPayload := makeValidBatchPayload("node-valid", "batch-valid-100")
	rec := seedQuarantineRecord(t, store, &storage.QuarantineRecord{
		ID:              "qrn-valid-detail",
		NodeID:          "node-valid",
		BatchID:         "batch-valid-100",
		Payload:         validPayload,
		FailureCategory: storage.CategoryDeliveryExhausted,
	})

	resp, body := doAdminRequest(t, ts, http.MethodGet, "/api/v1/telemetry/quarantine/"+rec.ID, testAdminToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d (body: %s)", resp.StatusCode, string(body))
	}

	var detail QuarantineDetailResponse
	if err := json.Unmarshal(body, &detail); err != nil {
		t.Fatalf("failed to unmarshal detail: %v", err)
	}

	expectedB64 := base64.StdEncoding.EncodeToString(validPayload)
	if detail.Payload != expectedB64 {
		t.Fatalf("payload base64 mismatch: expected %q, got %q", expectedB64, detail.Payload)
	}
	if detail.PayloadSizeBytes != len(validPayload) {
		t.Fatalf("expected payload_size_bytes=%d, got %d", len(validPayload), detail.PayloadSizeBytes)
	}
	if detail.ParsedBatch == nil {
		t.Fatalf("expected non-nil parsed_batch for valid payload")
	}
	if detail.ParsedBatch.NodeID != "node-valid" || detail.ParsedBatch.BatchID != "batch-valid-100" {
		t.Fatalf("parsed_batch identity mismatch: node=%s, batch=%s", detail.ParsedBatch.NodeID, detail.ParsedBatch.BatchID)
	}
}

// 15. Corrupt payload returns raw base64 and null parsed batch.
func TestQuarantineAPI_Detail_CorruptPayloadReturnsRawBase64AndNullParsedBatch(t *testing.T) {
	_, store, ts := setupQuarantineTestServer(t, true, testAdminToken)

	corruptBytes := []byte("{{corrupt-not-valid-json}}")
	rec := seedQuarantineRecord(t, store, &storage.QuarantineRecord{
		ID:              "qrn-corrupt-detail",
		NodeID:          "node-corrupt",
		Payload:         corruptBytes,
		FailureCategory: storage.CategoryDeserialization,
	})

	resp, body := doAdminRequest(t, ts, http.MethodGet, "/api/v1/telemetry/quarantine/"+rec.ID, testAdminToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d (body: %s)", resp.StatusCode, string(body))
	}

	var detail QuarantineDetailResponse
	if err := json.Unmarshal(body, &detail); err != nil {
		t.Fatalf("failed to unmarshal detail: %v", err)
	}

	expectedB64 := base64.StdEncoding.EncodeToString(corruptBytes)
	if detail.Payload != expectedB64 {
		t.Fatalf("expected base64 raw payload %q, got %q", expectedB64, detail.Payload)
	}
	if detail.ParsedBatch != nil {
		t.Fatalf("expected parsed_batch to be null for corrupt payload, got %+v", detail.ParsedBatch)
	}
}

// 16. Pruned/empty payload returns empty base64 and null parsed batch.
func TestQuarantineAPI_Detail_PrunedOrEmptyPayloadReturnsEmptyBase64AndNullParsedBatch(t *testing.T) {
	_, store, ts := setupQuarantineTestServer(t, true, testAdminToken)

	rec := seedQuarantineRecord(t, store, &storage.QuarantineRecord{
		ID:              "qrn-empty-detail",
		NodeID:          "node-empty",
		Payload:         []byte{},
		FailureCategory: storage.CategoryStorageFailure,
	})

	resp, body := doAdminRequest(t, ts, http.MethodGet, "/api/v1/telemetry/quarantine/"+rec.ID, testAdminToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d (body: %s)", resp.StatusCode, string(body))
	}

	var detail QuarantineDetailResponse
	if err := json.Unmarshal(body, &detail); err != nil {
		t.Fatalf("failed to unmarshal detail: %v", err)
	}

	if detail.Payload != "" {
		t.Fatalf("expected empty base64 payload, got %q", detail.Payload)
	}
	if detail.PayloadSizeBytes != 0 {
		t.Fatalf("expected payload_size_bytes=0, got %d", detail.PayloadSizeBytes)
	}
	if detail.ParsedBatch != nil {
		t.Fatalf("expected parsed_batch=null, got %+v", detail.ParsedBatch)
	}
}

// 17. Missing record returns 404.
func TestQuarantineAPI_Detail_MissingRecordReturns404(t *testing.T) {
	_, _, ts := setupQuarantineTestServer(t, true, testAdminToken)

	resp, body := doAdminRequest(t, ts, http.MethodGet, "/api/v1/telemetry/quarantine/non-existent-id-1234", testAdminToken, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found, got %d (body: %s)", resp.StatusCode, string(body))
	}
}

// ============================================================================
// Replay and Discard (Scenarios 18 - 25)
// ============================================================================

// 18. Successful replay.
func TestQuarantineAPI_Replay_Success(t *testing.T) {
	srv, store, ts := setupQuarantineTestServer(t, true, testAdminToken)

	validPayload := makeValidBatchPayload("node-rep-1", "batch-rep-1")
	rec := seedQuarantineRecord(t, store, &storage.QuarantineRecord{
		ID:              "qrn-replay-success",
		NodeID:          "node-rep-1",
		BatchID:         "batch-rep-1",
		Payload:         validPayload,
		FailureCategory: storage.CategoryDeliveryExhausted,
	})

	reqBody, _ := json.Marshal(map[string]string{"reason": "operator replay verified batch"})
	resp, body := doAdminRequest(t, ts, http.MethodPost, "/api/v1/telemetry/quarantine/"+rec.ID+"/replay", testAdminToken, reqBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d (body: %s)", resp.StatusCode, string(body))
	}

	var replayResp QuarantineReplayResponse
	if err := json.Unmarshal(body, &replayResp); err != nil {
		t.Fatalf("failed to unmarshal replay response: %v", err)
	}
	if replayResp.Status != storage.StatusReplayed {
		t.Fatalf("expected status 'replayed', got %s", replayResp.Status)
	}
	if replayResp.AlreadyIngested {
		t.Fatalf("expected already_ingested=false for newly replayed batch")
	}

	// Verify durable storage has the batch
	persistedBatch, err := store.GetPayload(context.Background(), "node-rep-1", "batch-rep-1")
	if err != nil || persistedBatch == nil {
		t.Fatalf("expected batch to be persisted in durable store, got: %v", err)
	}

	// Verify in-memory history has the batch
	history := srv.GetIngestedBatches()
	if len(history) != 1 || history[0].Batch.BatchID != "batch-rep-1" {
		t.Fatalf("expected batch in history ring buffer, got %d entries", len(history))
	}
}

// 19. Already-ingested replay.
func TestQuarantineAPI_Replay_AlreadyIngested(t *testing.T) {
	_, store, ts := setupQuarantineTestServer(t, true, testAdminToken)

	validPayload := makeValidBatchPayload("node-pre", "batch-preingested")
	rec := seedQuarantineRecord(t, store, &storage.QuarantineRecord{
		ID:              "qrn-already-ingested",
		NodeID:          "node-pre",
		BatchID:         "batch-preingested",
		Payload:         validPayload,
		FailureCategory: storage.CategoryDeliveryExhausted,
	})

	// Ingest the batch into cp_ingested_batches directly to simulate alternate out-of-band ingestion
	nowStr := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := store.DB().ExecContext(context.Background(), `
		INSERT INTO cp_ingested_batches (node_id, batch_id, sequence_number, collected_at, ingested_at)
		VALUES (?, ?, ?, ?, ?);
	`, "node-pre", "batch-preingested", 1, nowStr, nowStr)
	if err != nil {
		t.Fatalf("failed to insert batch into cp_ingested_batches: %v", err)
	}

	resp, body := doAdminRequest(t, ts, http.MethodPost, "/api/v1/telemetry/quarantine/"+rec.ID+"/replay", testAdminToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d (body: %s)", resp.StatusCode, string(body))
	}

	var replayResp QuarantineReplayResponse
	if err := json.Unmarshal(body, &replayResp); err != nil {
		t.Fatalf("failed to unmarshal replay response: %v", err)
	}
	if !replayResp.AlreadyIngested {
		t.Fatalf("expected already_ingested=true, got false")
	}
	if replayResp.Status != storage.StatusReplayed {
		t.Fatalf("expected status 'replayed', got %s", replayResp.Status)
	}
}

// 20. Replay identity mismatch returns validation failure (422) and leaves record quarantined.
func TestQuarantineAPI_Replay_IdentityMismatchReturns422(t *testing.T) {
	_, store, ts := setupQuarantineTestServer(t, true, testAdminToken)

	// Payload contains node-different, batch-different
	payload := makeValidBatchPayload("node-different", "batch-different")
	rec := seedQuarantineRecord(t, store, &storage.QuarantineRecord{
		ID:              "qrn-mismatch-test",
		NodeID:          "node-expected",
		BatchID:         "batch-expected",
		Payload:         payload,
		FailureCategory: storage.CategoryDeliveryExhausted,
	})

	resp, body := doAdminRequest(t, ts, http.MethodPost, "/api/v1/telemetry/quarantine/"+rec.ID+"/replay", testAdminToken, nil)
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 Unprocessable Entity for identity mismatch, got %d (body: %s)", resp.StatusCode, string(body))
	}

	// Verify record remains quarantined
	saved, err := store.GetQuarantineRecord(context.Background(), rec.ID)
	if err != nil {
		t.Fatalf("GetQuarantineRecord failed: %v", err)
	}
	if saved.Status != storage.StatusQuarantined {
		t.Fatalf("expected record to remain quarantined, got %s", saved.Status)
	}
}

// 21. Successful discard.
func TestQuarantineAPI_Discard_Success(t *testing.T) {
	_, store, ts := setupQuarantineTestServer(t, true, testAdminToken)

	rec := seedQuarantineRecord(t, store, &storage.QuarantineRecord{
		ID:              "qrn-discard-success",
		NodeID:          "node-disc",
		Payload:         []byte("bad-payload-data"),
		FailureCategory: storage.CategoryDeserialization,
	})

	reqBody, _ := json.Marshal(map[string]string{"reason": "corrupt packet discarded by operator"})
	resp, body := doAdminRequest(t, ts, http.MethodPost, "/api/v1/telemetry/quarantine/"+rec.ID+"/discard", testAdminToken, reqBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d (body: %s)", resp.StatusCode, string(body))
	}

	var discResp QuarantineDiscardResponse
	if err := json.Unmarshal(body, &discResp); err != nil {
		t.Fatalf("failed to unmarshal discard response: %v", err)
	}
	if discResp.Status != storage.StatusDiscarded {
		t.Fatalf("expected status 'discarded', got %s", discResp.Status)
	}

	// Verify in durable storage
	saved, err := store.GetQuarantineRecord(context.Background(), rec.ID)
	if err != nil {
		t.Fatalf("GetQuarantineRecord failed: %v", err)
	}
	if saved.Status != storage.StatusDiscarded {
		t.Fatalf("expected status 'discarded' in store, got %s", saved.Status)
	}
	if saved.StatusUpdatedBy != "admin-token" {
		t.Fatalf("expected StatusUpdatedBy='admin-token', got %q", saved.StatusUpdatedBy)
	}
	if saved.StatusReason != "corrupt packet discarded by operator" {
		t.Fatalf("expected StatusReason preserved, got %q", saved.StatusReason)
	}
}

// 22. Replay of terminal record returns 409.
func TestQuarantineAPI_Replay_TerminalRecordReturns409(t *testing.T) {
	_, store, ts := setupQuarantineTestServer(t, true, testAdminToken)

	rec := seedQuarantineRecord(t, store, &storage.QuarantineRecord{
		ID:              "qrn-terminal-replay",
		NodeID:          "node-term",
		Payload:         makeValidBatchPayload("node-term", "batch-term"),
		FailureCategory: storage.CategoryDeliveryExhausted,
	})
	// Discard it to make it terminal
	_, err := store.DiscardQuarantineRecord(context.Background(), rec.ID, "admin-test", "terminal test")
	if err != nil {
		t.Fatalf("DiscardQuarantineRecord failed: %v", err)
	}

	resp, body := doAdminRequest(t, ts, http.MethodPost, "/api/v1/telemetry/quarantine/"+rec.ID+"/replay", testAdminToken, nil)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 Conflict when replaying terminal record, got %d (body: %s)", resp.StatusCode, string(body))
	}
}

// 23. Discard of terminal record returns 409.
func TestQuarantineAPI_Discard_TerminalRecordReturns409(t *testing.T) {
	_, store, ts := setupQuarantineTestServer(t, true, testAdminToken)

	rec := seedQuarantineRecord(t, store, &storage.QuarantineRecord{
		ID:              "qrn-terminal-discard",
		NodeID:          "node-term",
		Payload:         []byte("data"),
		FailureCategory: storage.CategoryDeliveryExhausted,
	})
	// Discard once
	_, err := store.DiscardQuarantineRecord(context.Background(), rec.ID, "admin-test", "discarded first")
	if err != nil {
		t.Fatalf("first discard failed: %v", err)
	}

	// Discard again via HTTP
	resp, body := doAdminRequest(t, ts, http.MethodPost, "/api/v1/telemetry/quarantine/"+rec.ID+"/discard", testAdminToken, nil)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 Conflict when discarding terminal record, got %d (body: %s)", resp.StatusCode, string(body))
	}
}

// 24. Concurrent replay requests do not cause multiple successful state transitions.
func TestQuarantineAPI_Replay_ConcurrentRequestsSingleStateTransition(t *testing.T) {
	_, store, ts := setupQuarantineTestServer(t, true, testAdminToken)

	validPayload := makeValidBatchPayload("node-conc", "batch-conc")
	rec := seedQuarantineRecord(t, store, &storage.QuarantineRecord{
		ID:              "qrn-conc-replay",
		NodeID:          "node-conc",
		BatchID:         "batch-conc",
		Payload:         validPayload,
		FailureCategory: storage.CategoryDeliveryExhausted,
	})

	const concurrency = 10
	var wg sync.WaitGroup
	statusCodes := make([]int, concurrency)

	// Barrier to align goroutines
	startCh := make(chan struct{})

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-startCh

			req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/telemetry/quarantine/"+rec.ID+"/replay", nil)
			req.Header.Set("Authorization", "Bearer "+testAdminToken)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			statusCodes[idx] = resp.StatusCode
		}(i)
	}

	close(startCh)
	wg.Wait()

	successCount := 0
	conflictCount := 0
	for _, code := range statusCodes {
		if code == http.StatusOK {
			successCount++
		} else if code == http.StatusConflict {
			conflictCount++
		}
	}

	if successCount != 1 {
		t.Fatalf("expected exactly 1 successful replay (200 OK), got %d (conflicts: %d, all: %v)", successCount, conflictCount, statusCodes)
	}
	if conflictCount != concurrency-1 {
		t.Fatalf("expected %d conflict responses (409), got %d", concurrency-1, conflictCount)
	}
}

// 25. Replay/discard uses the stored NodeID lock and serializes against same-node ingestion.
func TestQuarantineAPI_ReplayDiscard_NodeLockSerialization(t *testing.T) {
	srv, store, ts := setupQuarantineTestServer(t, true, testAdminToken)

	const targetNodeID = "node-serialized"
	validPayload := makeValidBatchPayload(targetNodeID, "batch-ser-1")
	rec := seedQuarantineRecord(t, store, &storage.QuarantineRecord{
		ID:              "qrn-lock-ser",
		NodeID:          targetNodeID,
		BatchID:         "batch-ser-1",
		Payload:         validPayload,
		FailureCategory: storage.CategoryDeliveryExhausted,
	})

	// Acquire node lock externally on the server for targetNodeID
	unlock := srv.lockNode(targetNodeID)

	replayCompleted := make(chan struct{})
	go func() {
		defer close(replayCompleted)
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/telemetry/quarantine/"+rec.ID+"/replay", nil)
		req.Header.Set("Authorization", "Bearer "+testAdminToken)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
		}
	}()

	// Verify replay is blocked while the lock is held
	select {
	case <-replayCompleted:
		t.Fatalf("replay completed while target node lock was actively held!")
	case <-time.After(100 * time.Millisecond):
		// Expected: replay is blocked waiting for lockNode
	}

	// Release lock
	unlock()

	// Now replay should complete promptly
	select {
	case <-replayCompleted:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatalf("replay timed out after node lock was released")
	}

	// Verify record transitioned to replayed
	saved, err := store.GetQuarantineRecord(context.Background(), rec.ID)
	if err != nil {
		t.Fatalf("GetQuarantineRecord failed: %v", err)
	}
	if saved.Status != storage.StatusReplayed {
		t.Fatalf("expected status 'replayed', got %s", saved.Status)
	}
}

// ============================================================================
// Routing and Errors (Scenarios 26 - 29)
// ============================================================================

// 26. Unsupported HTTP methods return 405 with correct Allow header.
func TestQuarantineAPI_Routing_UnsupportedMethodsReturn405(t *testing.T) {
	_, store, ts := setupQuarantineTestServer(t, true, testAdminToken)

	rec := seedQuarantineRecord(t, store, &storage.QuarantineRecord{
		ID:              "qrn-methods",
		NodeID:          "node-1",
		Payload:         []byte("data"),
		FailureCategory: storage.CategoryDeserialization,
	})

	cases := []struct {
		name          string
		method        string
		path          string
		expectedAllow string
	}{
		{"Collection POST", http.MethodPost, "/api/v1/telemetry/quarantine", "GET"},
		{"Collection PUT", http.MethodPut, "/api/v1/telemetry/quarantine", "GET"},
		{"Collection DELETE", http.MethodDelete, "/api/v1/telemetry/quarantine", "GET"},
		{"Detail POST", http.MethodPost, "/api/v1/telemetry/quarantine/" + rec.ID, "GET"},
		{"Detail PUT", http.MethodPut, "/api/v1/telemetry/quarantine/" + rec.ID, "GET"},
		{"Detail DELETE", http.MethodDelete, "/api/v1/telemetry/quarantine/" + rec.ID, "GET"},
		{"Replay GET", http.MethodGet, "/api/v1/telemetry/quarantine/" + rec.ID + "/replay", "POST"},
		{"Replay PUT", http.MethodPut, "/api/v1/telemetry/quarantine/" + rec.ID + "/replay", "POST"},
		{"Discard GET", http.MethodGet, "/api/v1/telemetry/quarantine/" + rec.ID + "/discard", "POST"},
		{"Discard DELETE", http.MethodDelete, "/api/v1/telemetry/quarantine/" + rec.ID + "/discard", "POST"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, body := doAdminRequest(t, ts, tc.method, tc.path, testAdminToken, nil)
			if resp.StatusCode != http.StatusMethodNotAllowed {
				t.Fatalf("expected 405 Method Not Allowed for %s %s, got %d (body: %s)", tc.method, tc.path, resp.StatusCode, string(body))
			}
			allow := resp.Header.Get("Allow")
			if allow != tc.expectedAllow {
				t.Fatalf("expected Allow header %q, got %q", tc.expectedAllow, allow)
			}
		})
	}
}

// 27. Malformed paths and unexpected extra path segments are rejected.
func TestQuarantineAPI_Routing_MalformedPathsAndExtraSegments(t *testing.T) {
	_, store, ts := setupQuarantineTestServer(t, true, testAdminToken)

	rec := seedQuarantineRecord(t, store, &storage.QuarantineRecord{
		ID:              "qrn-path-test",
		NodeID:          "node-1",
		Payload:         []byte("data"),
		FailureCategory: storage.CategoryDeserialization,
	})

	cases := []struct {
		name           string
		path           string
		expectedStatus int
	}{
		{"Extra path segments after replay", "/api/v1/telemetry/quarantine/" + rec.ID + "/replay/extra", http.StatusNotFound},
		{"Extra path segments after discard", "/api/v1/telemetry/quarantine/" + rec.ID + "/discard/extra", http.StatusNotFound},
		{"Unknown sub-action", "/api/v1/telemetry/quarantine/" + rec.ID + "/unknown-action", http.StatusNotFound},
		{"Deeply nested invalid path", "/api/v1/telemetry/quarantine/a/b/c/d/e", http.StatusNotFound},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, body := doAdminRequest(t, ts, http.MethodPost, tc.path, testAdminToken, nil)
			if resp.StatusCode != tc.expectedStatus {
				t.Fatalf("expected status %d for %s, got %d (body: %s)", tc.expectedStatus, tc.path, resp.StatusCode, string(body))
			}
		})
	}
}

// 28. Invalid query values return 400.
func TestQuarantineAPI_Routing_InvalidQueryValues(t *testing.T) {
	_, _, ts := setupQuarantineTestServer(t, true, testAdminToken)

	cases := []struct {
		name  string
		query string
	}{
		{"Invalid status", "status=non-existent-status"},
		{"Invalid failure category", "failure_category=unknown_category_xyz"},
		{"Non-numeric limit", "limit=abc"},
		{"Limit above 500", "limit=501"},
		{"Limit zero", "limit=0"},
		{"Limit negative", "limit=-10"},
		{"Non-numeric offset", "offset=xyz"},
		{"Negative offset", "offset=-1"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, body := doAdminRequest(t, ts, http.MethodGet, "/api/v1/telemetry/quarantine?"+tc.query, testAdminToken, nil)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("expected 400 Bad Request for query %q, got %d (body: %s)", tc.query, resp.StatusCode, string(body))
			}
		})
	}
}

// 29. Internal storage errors are sanitized.
func TestQuarantineAPI_Errors_StorageErrorsSanitized(t *testing.T) {
	srv, _, ts := setupQuarantineTestServer(t, true, testAdminToken)

	// Close storage explicitly to simulate internal database failure
	srv.Close()

	// 1. List request against closed storage
	resp, body := doAdminRequest(t, ts, http.MethodGet, "/api/v1/telemetry/quarantine", testAdminToken, nil)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500 for list on closed store, got %d", resp.StatusCode)
	}
	var errResp ErrorResponse
	if err := json.Unmarshal(body, &errResp); err != nil {
		t.Fatalf("failed to parse JSON error: %v", err)
	}
	if strings.Contains(strings.ToLower(errResp.Message), "sqlite") || strings.Contains(errResp.Message, ".db") {
		t.Fatalf("error message leaks internal storage details: %q", errResp.Message)
	}

	// 2. Detail request against closed storage
	resp, body = doAdminRequest(t, ts, http.MethodGet, "/api/v1/telemetry/quarantine/qrn-any", testAdminToken, nil)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500 for detail on closed store, got %d", resp.StatusCode)
	}
	json.Unmarshal(body, &errResp)
	if strings.Contains(strings.ToLower(errResp.Message), "sqlite") || strings.Contains(errResp.Message, ".db") {
		t.Fatalf("error message leaks internal storage details: %q", errResp.Message)
	}

	// 3. Replay request against closed storage
	resp, body = doAdminRequest(t, ts, http.MethodPost, "/api/v1/telemetry/quarantine/qrn-any/replay", testAdminToken, nil)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500 for replay on closed store, got %d", resp.StatusCode)
	}
	json.Unmarshal(body, &errResp)
	if strings.Contains(strings.ToLower(errResp.Message), "sqlite") || strings.Contains(errResp.Message, ".db") {
		t.Fatalf("error message leaks internal storage details: %q", errResp.Message)
	}

	// 4. Discard request against closed storage
	resp, body = doAdminRequest(t, ts, http.MethodPost, "/api/v1/telemetry/quarantine/qrn-any/discard", testAdminToken, nil)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500 for discard on closed store, got %d", resp.StatusCode)
	}
	json.Unmarshal(body, &errResp)
	if strings.Contains(strings.ToLower(errResp.Message), "sqlite") || strings.Contains(errResp.Message, ".db") {
		t.Fatalf("error message leaks internal storage details: %q", errResp.Message)
	}
}

// ============================================================================
// Administrative Quarantine Operation Metrics (Phase 1)
// ============================================================================

// 30. Administrative Quarantine Replay Metrics: outcomes correctly increment counters.
func TestQuarantineAPI_Metrics_ReplayOutcomes(t *testing.T) {
	srv, store, ts := setupQuarantineTestServer(t, true, testAdminToken)

	// Seed records
	validBatch := makeValidBatchPayload("node-metrics-replay", "batch-metrics-replay")
	seq1 := uint64(1001)
	recValid := seedQuarantineRecord(t, store, &storage.QuarantineRecord{
		ID:               "qrn-metric-rep-valid",
		DeduplicationKey: "dedup-metric-rep-valid",
		SourceStream:     "TELEMETRY",
		SourceSequence:   &seq1,
		SourceSubject:    "telemetry.node-metrics-replay",
		NodeID:           "node-metrics-replay",
		BatchID:          "batch-metrics-replay",
		Payload:          validBatch,
		PayloadSizeBytes: len(validBatch),
		Status:           storage.StatusQuarantined,
	})

	corruptBatch := []byte("not-valid-json-bytes")
	seq2 := uint64(1002)
	recCorrupt := seedQuarantineRecord(t, store, &storage.QuarantineRecord{
		ID:               "qrn-metric-rep-corrupt",
		DeduplicationKey: "dedup-metric-rep-corrupt",
		SourceStream:     "TELEMETRY",
		SourceSequence:   &seq2,
		SourceSubject:    "telemetry.node-metrics-replay",
		NodeID:           "node-metrics-replay",
		BatchID:          "batch-metrics-rep-corrupt",
		Payload:          corruptBatch,
		PayloadSizeBytes: len(corruptBatch),
		Status:           storage.StatusQuarantined,
	})

	seq3 := uint64(1003)
	recAlreadyReplayed := seedQuarantineRecord(t, store, &storage.QuarantineRecord{
		ID:               "qrn-metric-rep-conflict",
		DeduplicationKey: "dedup-metric-rep-conflict",
		SourceStream:     "TELEMETRY",
		SourceSequence:   &seq3,
		SourceSubject:    "telemetry.node-metrics-replay",
		NodeID:           "node-metrics-replay",
		BatchID:          "batch-metrics-rep-conflict",
		Payload:          validBatch,
		PayloadSizeBytes: len(validBatch),
		Status:           storage.StatusReplayed,
	})

	// 1. Success outcome -> 200
	resp, _ := doAdminRequest(t, ts, http.MethodPost, "/api/v1/telemetry/quarantine/"+recValid.ID+"/replay", testAdminToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// 2. Conflict outcome -> 409 (replaying already replayed record)
	resp, _ = doAdminRequest(t, ts, http.MethodPost, "/api/v1/telemetry/quarantine/"+recAlreadyReplayed.ID+"/replay", testAdminToken, nil)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409, got %d", resp.StatusCode)
	}

	// 3. Not Found outcome -> 404
	resp, _ = doAdminRequest(t, ts, http.MethodPost, "/api/v1/telemetry/quarantine/qrn-nonexistent-replay/replay", testAdminToken, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}

	// 4. Invalid outcome:
	// a) Reason too long -> 400
	longReason := strings.Repeat("A", storage.MaxStatusReasonLength+1)
	body, _ := json.Marshal(map[string]string{"reason": longReason})
	resp, _ = doAdminRequest(t, ts, http.MethodPost, "/api/v1/telemetry/quarantine/"+recCorrupt.ID+"/replay", testAdminToken, body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}

	// b) Corrupt payload -> 422
	resp, _ = doAdminRequest(t, ts, http.MethodPost, "/api/v1/telemetry/quarantine/"+recCorrupt.ID+"/replay", testAdminToken, nil)
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d", resp.StatusCode)
	}

	// Verify metrics so far
	out := srv.Metrics().Registry().FormatPrometheus()
	if !strings.Contains(out, `aegisedge_quarantine_replays_total{outcome="success"} 1`) {
		t.Fatalf("expected replays success=1, got:\n%s", out)
	}
	if !strings.Contains(out, `aegisedge_quarantine_replays_total{outcome="conflict"} 1`) {
		t.Fatalf("expected replays conflict=1, got:\n%s", out)
	}
	if !strings.Contains(out, `aegisedge_quarantine_replays_total{outcome="not_found"} 1`) {
		t.Fatalf("expected replays not_found=1, got:\n%s", out)
	}
	if !strings.Contains(out, `aegisedge_quarantine_replays_total{outcome="invalid"} 2`) {
		t.Fatalf("expected replays invalid=2, got:\n%s", out)
	}
	if !strings.Contains(out, `aegisedge_quarantine_replays_total{outcome="error"} 0`) {
		t.Fatalf("expected replays error=0, got:\n%s", out)
	}

	// 5. Error outcome -> 500 (closing store)
	srv.Close()
	resp, _ = doAdminRequest(t, ts, http.MethodPost, "/api/v1/telemetry/quarantine/qrn-metric-rep-valid/replay", testAdminToken, nil)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", resp.StatusCode)
	}

	out = srv.Metrics().Registry().FormatPrometheus()
	if !strings.Contains(out, `aegisedge_quarantine_replays_total{outcome="error"} 1`) {
		t.Fatalf("expected replays error=1, got:\n%s", out)
	}
}

// 31. Administrative Quarantine Discard Metrics: outcomes correctly increment counters.
func TestQuarantineAPI_Metrics_DiscardOutcomes(t *testing.T) {
	srv, store, ts := setupQuarantineTestServer(t, true, testAdminToken)

	seq2001 := uint64(2001)
	recValid := seedQuarantineRecord(t, store, &storage.QuarantineRecord{
		ID:               "qrn-metric-disc-valid",
		DeduplicationKey: "dedup-metric-disc-valid",
		SourceStream:     "TELEMETRY",
		SourceSequence:   &seq2001,
		SourceSubject:    "telemetry.node-metrics-discard",
		NodeID:           "node-metrics-discard",
		BatchID:          "batch-metrics-discard",
		Payload:          []byte("raw"),
		PayloadSizeBytes: 3,
		Status:           storage.StatusQuarantined,
	})

	seq2002 := uint64(2002)
	recAlreadyDiscarded := seedQuarantineRecord(t, store, &storage.QuarantineRecord{
		ID:               "qrn-metric-disc-conflict",
		DeduplicationKey: "dedup-metric-disc-conflict",
		SourceStream:     "TELEMETRY",
		SourceSequence:   &seq2002,
		SourceSubject:    "telemetry.node-metrics-discard",
		NodeID:           "node-metrics-discard",
		BatchID:          "batch-metrics-disc-conflict",
		Payload:          []byte("raw"),
		PayloadSizeBytes: 3,
		Status:           storage.StatusDiscarded,
	})

	// 1. Success outcome -> 200
	body, _ := json.Marshal(map[string]string{"reason": "administrator discard test"})
	resp, _ := doAdminRequest(t, ts, http.MethodPost, "/api/v1/telemetry/quarantine/"+recValid.ID+"/discard", testAdminToken, body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// 2. Conflict outcome -> 409
	resp, _ = doAdminRequest(t, ts, http.MethodPost, "/api/v1/telemetry/quarantine/"+recAlreadyDiscarded.ID+"/discard", testAdminToken, nil)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409, got %d", resp.StatusCode)
	}

	// 3. Not Found outcome -> 404
	resp, _ = doAdminRequest(t, ts, http.MethodPost, "/api/v1/telemetry/quarantine/qrn-nonexistent-discard/discard", testAdminToken, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}

	// 4. Invalid outcome:
	// a) Malformed JSON
	resp, _ = doAdminRequest(t, ts, http.MethodPost, "/api/v1/telemetry/quarantine/qrn-any/discard", testAdminToken, []byte("{invalid-json"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}

	// b) Reason exceeds max length
	longReason := strings.Repeat("B", storage.MaxStatusReasonLength+1)
	body, _ = json.Marshal(map[string]string{"reason": longReason})
	resp, _ = doAdminRequest(t, ts, http.MethodPost, "/api/v1/telemetry/quarantine/qrn-any/discard", testAdminToken, body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}

	// Verify metrics so far
	out := srv.Metrics().Registry().FormatPrometheus()
	if !strings.Contains(out, `aegisedge_quarantine_discards_total{outcome="success"} 1`) {
		t.Fatalf("expected discards success=1, got:\n%s", out)
	}
	if !strings.Contains(out, `aegisedge_quarantine_discards_total{outcome="conflict"} 1`) {
		t.Fatalf("expected discards conflict=1, got:\n%s", out)
	}
	if !strings.Contains(out, `aegisedge_quarantine_discards_total{outcome="not_found"} 1`) {
		t.Fatalf("expected discards not_found=1, got:\n%s", out)
	}
	if !strings.Contains(out, `aegisedge_quarantine_discards_total{outcome="invalid"} 2`) {
		t.Fatalf("expected discards invalid=2, got:\n%s", out)
	}
	if !strings.Contains(out, `aegisedge_quarantine_discards_total{outcome="error"} 0`) {
		t.Fatalf("expected discards error=0, got:\n%s", out)
	}

	// 5. Error outcome -> 500 (closing store)
	srv.Close()
	resp, _ = doAdminRequest(t, ts, http.MethodPost, "/api/v1/telemetry/quarantine/qrn-metric-disc-valid/discard", testAdminToken, nil)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", resp.StatusCode)
	}

	out = srv.Metrics().Registry().FormatPrometheus()
	if !strings.Contains(out, `aegisedge_quarantine_discards_total{outcome="error"} 1`) {
		t.Fatalf("expected discards error=1, got:\n%s", out)
	}
}

// 32. Quarantine operation metrics: Auth failures do NOT increment replay/discard counters.
func TestQuarantineAPI_Metrics_AuthFailureDoesNotIncrementQuarantineCounters(t *testing.T) {
	srv, _, ts := setupQuarantineTestServer(t, true, testAdminToken)

	// 1. Request with missing credentials -> 401
	resp, _ := doAdminRequest(t, ts, http.MethodPost, "/api/v1/telemetry/quarantine/qrn-auth-test/replay", "", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}

	// 2. Request with invalid bearer token -> 401
	resp, _ = doAdminRequest(t, ts, http.MethodPost, "/api/v1/telemetry/quarantine/qrn-auth-test/discard", "wrong-admin-token", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}

	// 3. Request with valid Node HMAC credentials (rejected as non-admin) -> 403
	req := signedRequest(t, http.MethodPost, ts.URL+"/api/v1/telemetry/quarantine/qrn-auth-test/replay", "node-test", nil)
	hmacResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("HMAC request failed: %v", err)
	}
	defer hmacResp.Body.Close()
	if hmacResp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for Node HMAC, got %d", hmacResp.StatusCode)
	}

	out := srv.Metrics().Registry().FormatPrometheus()

	// Verify all replay and discard counters are strictly 0
	expectedZeroes := []string{
		`aegisedge_quarantine_replays_total{outcome="success"} 0`,
		`aegisedge_quarantine_replays_total{outcome="not_found"} 0`,
		`aegisedge_quarantine_replays_total{outcome="conflict"} 0`,
		`aegisedge_quarantine_replays_total{outcome="invalid"} 0`,
		`aegisedge_quarantine_replays_total{outcome="error"} 0`,
		`aegisedge_quarantine_discards_total{outcome="success"} 0`,
		`aegisedge_quarantine_discards_total{outcome="not_found"} 0`,
		`aegisedge_quarantine_discards_total{outcome="conflict"} 0`,
		`aegisedge_quarantine_discards_total{outcome="invalid"} 0`,
		`aegisedge_quarantine_discards_total{outcome="error"} 0`,
	}
	for _, expected := range expectedZeroes {
		if !strings.Contains(out, expected) {
			t.Fatalf("expected %q, but got exposition:\n%s", expected, out)
		}
	}

	// But auth_failures_total was incremented!
	if !strings.Contains(out, `aegisedge_control_plane_auth_failures_total{reason="unauthorized"} 2`) {
		t.Fatalf("expected auth_failures unauthorized=2, got:\n%s", out)
	}
	if !strings.Contains(out, `aegisedge_control_plane_auth_failures_total{reason="forbidden"} 1`) {
		t.Fatalf("expected auth_failures forbidden=1, got:\n%s", out)
	}
}

// 33. Replay and discard counters operate independently.
func TestQuarantineAPI_Metrics_CounterIndependence(t *testing.T) {
	srv, store, ts := setupQuarantineTestServer(t, true, testAdminToken)

	validBatch := makeValidBatchPayload("node-indep", "batch-indep")
	seq3001 := uint64(3001)
	rec := seedQuarantineRecord(t, store, &storage.QuarantineRecord{
		ID:               "qrn-metric-indep",
		DeduplicationKey: "dedup-metric-indep",
		SourceStream:     "TELEMETRY",
		SourceSequence:   &seq3001,
		SourceSubject:    "telemetry.node-indep",
		NodeID:           "node-indep",
		BatchID:          "batch-indep",
		Payload:          validBatch,
		PayloadSizeBytes: len(validBatch),
		Status:           storage.StatusQuarantined,
	})

	// Perform 1 successful replay
	resp, _ := doAdminRequest(t, ts, http.MethodPost, "/api/v1/telemetry/quarantine/"+rec.ID+"/replay", testAdminToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	out := srv.Metrics().Registry().FormatPrometheus()

	// Replay success is 1, all others 0
	if !strings.Contains(out, `aegisedge_quarantine_replays_total{outcome="success"} 1`) {
		t.Fatalf("expected replay success=1, got:\n%s", out)
	}
	for _, outcome := range []string{
		metrics.QuarantineOutcomeSuccess,
		metrics.QuarantineOutcomeNotFound,
		metrics.QuarantineOutcomeConflict,
		metrics.QuarantineOutcomeInvalid,
		metrics.QuarantineOutcomeError,
	} {
		expDiscard := fmt.Sprintf(`aegisedge_quarantine_discards_total{outcome="%s"} 0`, outcome)
		if !strings.Contains(out, expDiscard) {
			t.Fatalf("expected %s, got:\n%s", expDiscard, out)
		}
	}
}

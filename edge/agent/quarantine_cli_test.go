package main

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

func createTestDBWithBatches(t *testing.T) (string, *storage.SQLiteStore) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "cli_test.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	return dbPath, store
}

func persistSampleBatch(t *testing.T, store storage.Store, batchID, nodeID string, seq int64, status storage.SyncStatus, reason string) {
	t.Helper()
	now := time.Date(2026, 10, 18, 20, 0, 0, 0, time.UTC)
	b := &types.TelemetryBatch{
		BatchID:        batchID,
		NodeID:         nodeID,
		SequenceNumber: seq,
		CollectedAt:    now,
		Metrics: []types.MetricSample{
			{Name: "cpu_usage", Value: 75.5, Unit: "percent", Timestamp: now},
			{Name: "mem_used", Value: 1048576, Unit: "bytes", Timestamp: now},
		},
	}
	ctx := context.Background()
	if err := store.PersistBatch(ctx, b); err != nil {
		t.Fatalf("PersistBatch %s failed: %v", batchID, err)
	}

	if status == storage.SyncStatusFailed {
		if err := store.MarkBatchQuarantined(ctx, batchID, reason, now.Add(5*time.Minute)); err != nil {
			t.Fatalf("MarkBatchQuarantined %s failed: %v", batchID, err)
		}
	} else if status == storage.SyncStatusSynced {
		if err := store.MarkBatchSynced(ctx, batchID, now.Add(1*time.Minute)); err != nil {
			t.Fatalf("MarkBatchSynced %s failed: %v", batchID, err)
		}
	}
}

// 1. Help flag returns 0 and prints usage to stdout.
func TestQuarantineCLI_Help(t *testing.T) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	code := RunQuarantine([]string{"--help"}, stdout, stderr)
	if code != 0 {
		t.Errorf("expected exit code 0 on --help, got %d", code)
	}
	if !strings.Contains(stdout.String(), "AegisEdge Quarantine Management CLI") {
		t.Errorf("expected usage in stdout, got:\n%s", stdout.String())
	}
}

// 2. Unknown subcommand returns 1 and prints error to stderr.
func TestQuarantineCLI_UnknownSubcommand(t *testing.T) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	code := RunQuarantine([]string{"delete", "b-1"}, stdout, stderr)
	if code != 1 {
		t.Errorf("expected exit code 1 on unknown subcommand, got %d", code)
	}
	if !strings.Contains(stderr.String(), `unknown quarantine command "delete"`) {
		t.Errorf("expected unknown command error in stderr, got:\n%s", stderr.String())
	}
}

// 3. No arguments returns 1 and prints usage to stderr.
func TestQuarantineCLI_NoArgs(t *testing.T) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	code := RunQuarantine([]string{}, stdout, stderr)
	if code != 1 {
		t.Errorf("expected exit code 1 with no args, got %d", code)
	}
	if !strings.Contains(stderr.String(), "Usage:") {
		t.Errorf("expected usage in stderr, got:\n%s", stderr.String())
	}
}

// 4. List command on empty database.
func TestQuarantineCLI_List_Empty(t *testing.T) {
	dbPath, store := createTestDBWithBatches(t)
	defer store.Close()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	code := RunQuarantine([]string{"list", "--db-path", dbPath}, stdout, stderr)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d. stderr:\n%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "No quarantined batches found.") {
		t.Errorf("expected empty message, got:\n%s", stdout.String())
	}
}

// 5. List command with quarantined batches (table output).
func TestQuarantineCLI_List_Populated(t *testing.T) {
	dbPath, store := createTestDBWithBatches(t)
	persistSampleBatch(t, store, "b-fail-1", "node-1", 10, storage.SyncStatusFailed, "permanent schema error")
	persistSampleBatch(t, store, "b-fail-2", "node-1", 11, storage.SyncStatusFailed, "retry threshold exhausted")
	persistSampleBatch(t, store, "b-synced-3", "node-1", 12, storage.SyncStatusSynced, "")
	store.Close()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	code := RunQuarantine([]string{"list", "--db-path", dbPath}, stdout, stderr)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d. stderr:\n%s", code, stderr.String())
	}

	out := stdout.String()
	if !strings.Contains(out, "BATCH ID") || !strings.Contains(out, "FAILED AT") {
		t.Errorf("expected table headers in output, got:\n%s", out)
	}
	if !strings.Contains(out, "b-fail-1") || !strings.Contains(out, "b-fail-2") {
		t.Errorf("expected quarantined batches in output, got:\n%s", out)
	}
	if strings.Contains(out, "b-synced-3") {
		t.Errorf("synced batch should not appear in quarantine list: %s", out)
	}
	if !strings.Contains(out, "Total quarantined: 2") {
		t.Errorf("expected total quarantined count 2, got:\n%s", out)
	}
}

// 6. List command with JSON formatting.
func TestQuarantineCLI_List_JSON(t *testing.T) {
	dbPath, store := createTestDBWithBatches(t)
	persistSampleBatch(t, store, "b-fail-json", "node-json", 1, storage.SyncStatusFailed, "test JSON failure")
	store.Close()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	code := RunQuarantine([]string{"list", "--db-path", dbPath, "--json"}, stdout, stderr)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d. stderr:\n%s", code, stderr.String())
	}

	var records []*storage.Record
	if err := json.Unmarshal(stdout.Bytes(), &records); err != nil {
		t.Fatalf("failed to parse JSON list output: %v\nOutput was:\n%s", err, stdout.String())
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record in JSON output, got %d", len(records))
	}
	if records[0].BatchID != "b-fail-json" || records[0].SyncStatus != storage.SyncStatusFailed {
		t.Errorf("unexpected record in JSON: %+v", records[0])
	}
}

// 7. List limit validation (1..1000).
func TestQuarantineCLI_List_LimitValidation(t *testing.T) {
	dbPath, store := createTestDBWithBatches(t)
	store.Close()

	// limit = 0 (below minimum 1)
	stderr := &bytes.Buffer{}
	code := RunQuarantine([]string{"list", "--db-path", dbPath, "--limit", "0"}, &bytes.Buffer{}, stderr)
	if code != 1 {
		t.Errorf("expected exit code 1 for limit 0, got %d", code)
	}
	if !strings.Contains(stderr.String(), "limit must be between 1 and 1000") {
		t.Errorf("expected limit bounds error, got:\n%s", stderr.String())
	}

	// limit = 1001 (above maximum 1000)
	stderr.Reset()
	code = RunQuarantine([]string{"list", "--db-path", dbPath, "--limit", "1001"}, &bytes.Buffer{}, stderr)
	if code != 1 {
		t.Errorf("expected exit code 1 for limit 1001, got %d", code)
	}
	if !strings.Contains(stderr.String(), "limit must be between 1 and 1000") {
		t.Errorf("expected limit bounds error, got:\n%s", stderr.String())
	}
}

// 8. Inspect command success (text summary).
func TestQuarantineCLI_Inspect_Success(t *testing.T) {
	dbPath, store := createTestDBWithBatches(t)
	persistSampleBatch(t, store, "b-inspect-target", "node-insp", 42, storage.SyncStatusFailed, "bad payload schema")
	store.Close()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	code := RunQuarantine([]string{"inspect", "b-inspect-target", "--db-path", dbPath}, stdout, stderr)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d. stderr:\n%s", code, stderr.String())
	}

	out := stdout.String()
	if !strings.Contains(out, "Batch ID:          b-inspect-target") {
		t.Errorf("expected Batch ID, got:\n%s", out)
	}
	if !strings.Contains(out, "Node ID:           node-insp") {
		t.Errorf("expected Node ID, got:\n%s", out)
	}
	if !strings.Contains(out, "Sequence Number:   42") {
		t.Errorf("expected Sequence Number, got:\n%s", out)
	}
	if !strings.Contains(out, "Status:            FAILED") {
		t.Errorf("expected Status FAILED, got:\n%s", out)
	}
	if !strings.Contains(out, "Failure Reason:    bad payload schema") {
		t.Errorf("expected Failure Reason, got:\n%s", out)
	}
	if !strings.Contains(out, "Metrics Count:     2 samples") {
		t.Errorf("expected metrics sample count, got:\n%s", out)
	}
	if !strings.Contains(out, "cpu_usage: 75.50 percent") {
		t.Errorf("expected cpu metric sample, got:\n%s", out)
	}
}

// 9. Inspect command JSON output.
func TestQuarantineCLI_Inspect_JSON(t *testing.T) {
	dbPath, store := createTestDBWithBatches(t)
	persistSampleBatch(t, store, "b-inspect-json", "node-insp-json", 99, storage.SyncStatusFailed, "json inspection test")
	store.Close()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	code := RunQuarantine([]string{"inspect", "b-inspect-json", "--db-path", dbPath, "--json"}, stdout, stderr)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d. stderr:\n%s", code, stderr.String())
	}

	var rec storage.Record
	if err := json.Unmarshal(stdout.Bytes(), &rec); err != nil {
		t.Fatalf("failed to unmarshal JSON inspect output: %v\nOutput was:\n%s", err, stdout.String())
	}
	if rec.BatchID != "b-inspect-json" || rec.NodeID != "node-insp-json" || rec.SequenceNumber != 99 {
		t.Errorf("unexpected record data: %+v", rec)
	}
	if len(rec.Payload.Metrics) != 2 {
		t.Errorf("expected 2 metrics in payload, got %d", len(rec.Payload.Metrics))
	}
}

// 10. Inspect command missing batch ID argument.
func TestQuarantineCLI_Inspect_MissingBatchID(t *testing.T) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	code := RunQuarantine([]string{"inspect"}, stdout, stderr)
	if code != 1 {
		t.Errorf("expected exit code 1 when batch ID missing, got %d", code)
	}
	if !strings.Contains(stderr.String(), "<batch-id> is required for inspect") {
		t.Errorf("expected missing argument error, got:\n%s", stderr.String())
	}
}

// 11. Inspect command batch not found.
func TestQuarantineCLI_Inspect_NotFound(t *testing.T) {
	dbPath, store := createTestDBWithBatches(t)
	store.Close()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	code := RunQuarantine([]string{"inspect", "nonexistent-batch", "--db-path", dbPath}, stdout, stderr)
	if code != 1 {
		t.Errorf("expected exit code 1 for missing batch, got %d", code)
	}
	if !strings.Contains(stderr.String(), `batch "nonexistent-batch" not found`) {
		t.Errorf("expected not found error in stderr, got:\n%s", stderr.String())
	}
}

// 12. Requeue command success: resets batch to PENDING, resets attempt to 0, emits audit event.
func TestQuarantineCLI_Requeue_Success(t *testing.T) {
	dbPath, store := createTestDBWithBatches(t)
	persistSampleBatch(t, store, "b-requeue-ok", "node-rq", 5, storage.SyncStatusFailed, "requeue test reason")
	store.Close()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	code := RunQuarantine([]string{"requeue", "b-requeue-ok", "--db-path", dbPath}, stdout, stderr)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d. stderr:\n%s", code, stderr.String())
	}

	if !strings.Contains(stdout.String(), `Batch "b-requeue-ok" successfully requeued to PENDING (attempts reset to 0)`) {
		t.Errorf("expected success message in stdout, got:\n%s", stdout.String())
	}

	// Verify post-requeue state in SQLite
	verifyStore, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("reopening store failed: %v", err)
	}
	defer verifyStore.Close()

	ctx := context.Background()
	rec, err := verifyStore.GetBatch(ctx, "b-requeue-ok")
	if err != nil {
		t.Fatalf("GetBatch failed: %v", err)
	}
	if rec.SyncStatus != storage.SyncStatusPending {
		t.Errorf("expected status PENDING after requeue, got %s", rec.SyncStatus)
	}
	if rec.Attempt != 0 {
		t.Errorf("expected attempt 0 after requeue, got %d", rec.Attempt)
	}
	if rec.FailedAt != nil {
		t.Errorf("expected failed_at nil after requeue, got %v", rec.FailedAt)
	}
	if rec.ErrorReason != "" {
		t.Errorf("expected error_reason empty after requeue, got %q", rec.ErrorReason)
	}

	// Verify audit event was recorded with CorrelationID = b-requeue-ok
	events, err := verifyStore.ListAuditEventsByCorrelation(ctx, "b-requeue-ok", 10)
	if err != nil {
		t.Fatalf("ListAuditEventsByCorrelation failed: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("expected at least 1 audit event for b-requeue-ok")
	}
	var rqEvt *storage.StoredAuditEvent
	for _, e := range events {
		if e.EventType == "TELEMETRY_BATCH_REQUEUED" {
			rqEvt = e
			break
		}
	}
	if rqEvt == nil {
		t.Fatal("expected TELEMETRY_BATCH_REQUEUED audit event")
	}
	if rqEvt.CorrelationID != "b-requeue-ok" {
		t.Errorf("expected CorrelationID b-requeue-ok, got %s", rqEvt.CorrelationID)
	}
}

// 13. Requeue command invalid state: cannot requeue batch in PENDING or SYNCED status.
func TestQuarantineCLI_Requeue_InvalidState(t *testing.T) {
	dbPath, store := createTestDBWithBatches(t)
	persistSampleBatch(t, store, "b-already-pending", "node-rq", 1, storage.SyncStatusPending, "")
	persistSampleBatch(t, store, "b-already-synced", "node-rq", 2, storage.SyncStatusSynced, "")
	store.Close()

	// 1. Requeue PENDING batch fails
	stderr := &bytes.Buffer{}
	code := RunQuarantine([]string{"requeue", "b-already-pending", "--db-path", dbPath}, &bytes.Buffer{}, stderr)
	if code != 1 {
		t.Errorf("expected exit code 1 for PENDING batch requeue, got %d", code)
	}
	if !strings.Contains(stderr.String(), "cannot requeue batch") {
		t.Errorf("expected cannot requeue error in stderr, got:\n%s", stderr.String())
	}

	// 2. Requeue SYNCED batch fails
	stderr.Reset()
	code = RunQuarantine([]string{"requeue", "b-already-synced", "--db-path", dbPath}, &bytes.Buffer{}, stderr)
	if code != 1 {
		t.Errorf("expected exit code 1 for SYNCED batch requeue, got %d", code)
	}
	if !strings.Contains(stderr.String(), "cannot requeue batch") {
		t.Errorf("expected cannot requeue error in stderr, got:\n%s", stderr.String())
	}
}

// 14. Requeue command missing batch ID argument.
func TestQuarantineCLI_Requeue_MissingBatchID(t *testing.T) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	code := RunQuarantine([]string{"requeue"}, stdout, stderr)
	if code != 1 {
		t.Errorf("expected exit code 1 when batch ID missing, got %d", code)
	}
	if !strings.Contains(stderr.String(), "<batch-id> is required for requeue") {
		t.Errorf("expected missing argument error in stderr, got:\n%s", stderr.String())
	}
}

// 15. Requeue command batch not found.
func TestQuarantineCLI_Requeue_NotFound(t *testing.T) {
	dbPath, store := createTestDBWithBatches(t)
	store.Close()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	code := RunQuarantine([]string{"requeue", "nonexistent-batch-id", "--db-path", dbPath}, stdout, stderr)
	if code != 1 {
		t.Errorf("expected exit code 1 for missing batch, got %d", code)
	}
	if !strings.Contains(stderr.String(), `batch "nonexistent-batch-id" not found`) {
		t.Errorf("expected not found error in stderr, got:\n%s", stderr.String())
	}
}

// 16. Resource cleanup: database is cleanly closed and unlocked after CLI command execution.
func TestQuarantineCLI_DatabaseCleanlyClosed(t *testing.T) {
	dbPath, store := createTestDBWithBatches(t)
	persistSampleBatch(t, store, "b-lock-test", "node-lock", 1, storage.SyncStatusFailed, "testing lock release")
	store.Close()

	// Execute inspect
	code := RunQuarantine([]string{"inspect", "b-lock-test", "--db-path", dbPath}, &bytes.Buffer{}, &bytes.Buffer{})
	if code != 0 {
		t.Fatalf("inspect failed: exit code %d", code)
	}

	// Should be able to immediately reopen database with exclusive lock without sqlite busy error
	storeReopen, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("failed to reopen database after CLI command: %v", err)
	}
	storeReopen.Close()
}

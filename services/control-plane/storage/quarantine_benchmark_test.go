package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// setupBenchmarkQuarantineStore initializes an isolated temporary SQLite database
// using the production OpenSQLite constructor and populates it with realistic records
// according to the specified status distribution (10% quarantined, 45% replayed, 45% discarded)
// and payload size.
func setupBenchmarkQuarantineStore(b *testing.B, totalRecords int, payloadSize int) (*SQLiteStore, int64) {
	b.Helper()

	tempDir, err := os.MkdirTemp("", "aegisedge-bench-quarantine-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	dbPath := filepath.Join(tempDir, "bench_quarantine.db")

	store, err := OpenSQLite(dbPath)
	if err != nil {
		_ = os.RemoveAll(tempDir)
		b.Fatalf("failed to open production sqlite store: %v", err)
	}

	b.Cleanup(func() {
		_ = store.Close()
		_ = os.RemoveAll(tempDir)
	})

	// Pre-generate realistic synthetic payload
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte((i*31 + 17) % 256)
	}

	// Calculate counts according to distribution:
	// 10% active quarantined, 45% replayed, 45% discarded
	quarantinedCount := totalRecords / 10
	replayedCount := (totalRecords * 45) / 100
	discardedCount := totalRecords - quarantinedCount - replayedCount

	now := time.Now().UTC()
	ctx := context.Background()

	insertSQL := `INSERT INTO cp_telemetry_quarantine (
		id, dedup_key, source_stream, source_sequence, source_subject,
		node_id, batch_id, payload, failure_category, failure_reason,
		last_failure_category, last_failure_reason, delivery_count, attempts,
		first_quarantined_at, last_quarantined_at, status, status_updated_at,
		status_reason, status_updated_by, replay_metadata_json
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	// Bulk-insert in transactions to avoid individual fsync overhead during fixture setup
	const batchSize = 2500
	currentIdx := 0

	insertBatch := func(status string, count int) {
		for count > 0 {
			chunk := count
			if chunk > batchSize {
				chunk = batchSize
			}

			tx, err := store.db.BeginTx(ctx, nil)
			if err != nil {
				b.Fatalf("failed to begin fixture transaction: %v", err)
			}

			stmt, err := tx.Prepare(insertSQL)
			if err != nil {
				_ = tx.Rollback()
				b.Fatalf("failed to prepare insert statement: %v", err)
			}

			for i := 0; i < chunk; i++ {
				currentIdx++
				id := fmt.Sprintf("qrn-bench-%08d", currentIdx)
				dedupKey := fmt.Sprintf("dedup-bench-%08d", currentIdx)
				nodeID := fmt.Sprintf("node-%04d", currentIdx%100)
				batchID := fmt.Sprintf("batch-%06d", currentIdx)
				timeStr := now.Add(time.Duration(currentIdx) * time.Millisecond).Format(time.RFC3339Nano)

				_, err := stmt.Exec(
					id, dedupKey, "AEGISEDGE_TELEMETRY", currentIdx, "aegisedge.v1.telemetry."+nodeID,
					nodeID, batchID, payload, "deserialization_error", "benchmark generated payload",
					"", "", 1, 1, timeStr, timeStr,
					status, timeStr, "benchmark status reason", "benchmark-operator", "{}",
				)
				if err != nil {
					_ = stmt.Close()
					_ = tx.Rollback()
					b.Fatalf("fixture insert failed at record %d: %v", currentIdx, err)
				}
			}

			if err := stmt.Close(); err != nil {
				_ = tx.Rollback()
				b.Fatalf("failed to close stmt: %v", err)
			}

			if err := tx.Commit(); err != nil {
				b.Fatalf("failed to commit fixture batch: %v", err)
			}

			count -= chunk
		}
	}

	insertBatch(string(StatusQuarantined), quarantinedCount)
	insertBatch(string(StatusReplayed), replayedCount)
	insertBatch(string(StatusDiscarded), discardedCount)

	expectedActive := int64(quarantinedCount)

	// Verify count matches expected distribution before timing starts
	actualCount, err := store.CountActiveQuarantineRecords(ctx)
	if err != nil {
		b.Fatalf("failed to verify initial active count: %v", err)
	}
	if actualCount != expectedActive {
		b.Fatalf("fixture mismatch: expected %d active records, got %d", expectedActive, actualCount)
	}

	return store, expectedActive
}

// verifyBenchmarkQueryPlan executes EXPLAIN QUERY PLAN and logs the chosen index.
func verifyBenchmarkQueryPlan(b *testing.B, store *SQLiteStore) {
	b.Helper()
	ctx := context.Background()
	rows, err := store.db.QueryContext(ctx, `EXPLAIN QUERY PLAN SELECT COUNT(*) FROM cp_telemetry_quarantine WHERE status = 'quarantined'`)
	if err != nil {
		b.Fatalf("EXPLAIN QUERY PLAN failed: %v", err)
	}
	defer rows.Close()

	var selectID, orderVal, fromVal int
	var detail string
	var planDetails []string
	for rows.Next() {
		if err := rows.Scan(&selectID, &orderVal, &fromVal, &detail); err != nil {
			b.Fatalf("scan query plan failed: %v", err)
		}
		planDetails = append(planDetails, detail)
	}
	if err := rows.Err(); err != nil {
		b.Fatalf("query plan iteration failed: %v", err)
	}

	fullPlan := strings.Join(planDetails, "; ")
	if !strings.Contains(fullPlan, "idx_cp_quarantine_status") || !strings.Contains(fullPlan, "COVERING INDEX") {
		b.Errorf("expected covering index on status, got: %s", fullPlan)
	}
	b.Logf("Query Plan: %s", fullPlan)
}

// BenchmarkCountActiveQuarantineRecords measures sequential execution of CountActiveQuarantineRecords
// across small (100), medium (1,000), large (10,000), and stress (100,000) datasets with 1 KiB and 64 KiB payloads.
func BenchmarkCountActiveQuarantineRecords(b *testing.B) {
	cases := []struct {
		records     int
		payloadSize int
		label       string
	}{
		{records: 100, payloadSize: 1024, label: "Records=100/Payload=1KiB"},
		{records: 100, payloadSize: 64 * 1024, label: "Records=100/Payload=64KiB"},
		{records: 1000, payloadSize: 1024, label: "Records=1000/Payload=1KiB"},
		{records: 1000, payloadSize: 64 * 1024, label: "Records=1000/Payload=64KiB"},
		{records: 10000, payloadSize: 1024, label: "Records=10000/Payload=1KiB"},
		{records: 10000, payloadSize: 64 * 1024, label: "Records=10000/Payload=64KiB"},
		{records: 100000, payloadSize: 1024, label: "Records=100000/Payload=1KiB"},
	}

	for _, tc := range cases {
		b.Run(tc.label, func(b *testing.B) {
			store, expectedActive := setupBenchmarkQuarantineStore(b, tc.records, tc.payloadSize)
			verifyBenchmarkQueryPlan(b, store)

			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				count, err := store.CountActiveQuarantineRecords(ctx)
				if err != nil {
					b.Fatalf("query failed at iteration %d: %v", i, err)
				}
				if count != expectedActive {
					b.Fatalf("count mismatch: got %d, want %d", count, expectedActive)
				}
			}
		})
	}
}

// BenchmarkCountActiveQuarantineRecordsParallel measures concurrent execution of CountActiveQuarantineRecords.
// In the production SQLiteStore configuration, SetMaxOpenConns(1) is enforced to ensure serial connection
// usage and preserve connection-scoped PRAGMAs. This benchmark quantifies connection pool wait time
// and proves race-freedom under concurrent caller contention.
func BenchmarkCountActiveQuarantineRecordsParallel(b *testing.B) {
	cases := []struct {
		records     int
		payloadSize int
		label       string
	}{
		{records: 1000, payloadSize: 1024, label: "Records=1000/Payload=1KiB"},
		{records: 10000, payloadSize: 1024, label: "Records=10000/Payload=1KiB"},
		{records: 10000, payloadSize: 64 * 1024, label: "Records=10000/Payload=64KiB"},
	}

	for _, tc := range cases {
		b.Run(tc.label, func(b *testing.B) {
			store, expectedActive := setupBenchmarkQuarantineStore(b, tc.records, tc.payloadSize)
			ctx := context.Background()

			var errCount atomic.Int64
			var mismatchCount atomic.Int64

			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					count, err := store.CountActiveQuarantineRecords(ctx)
					if err != nil {
						errCount.Add(1)
						continue
					}
					if count != expectedActive {
						mismatchCount.Add(1)
					}
				}
			})

			if errCount.Load() > 0 {
				b.Fatalf("encountered %d errors during parallel benchmark", errCount.Load())
			}
			if mismatchCount.Load() > 0 {
				b.Fatalf("encountered %d count mismatches during parallel benchmark", mismatchCount.Load())
			}
		})
	}
}

package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
	sqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// SQLiteStore implements IdempotencyStore using pure-Go SQLite in WAL mode.
type SQLiteStore struct {
	db     *sql.DB
	path   string
	closed bool
	mu     sync.RWMutex

	// HookAfterMetaInsert is an optional test hook called inside RecordBatch
	// after metadata is inserted into cp_ingested_batches but before payload is inserted.
	// Returning an error aborts the transaction and rolls back, simulating a crash/failure.
	HookAfterMetaInsert func() error
}

// OpenSQLite initializes a SQLite database connection, applies WAL pragmas,
// and ensures the idempotency and payload schemas are created idempotently.
func OpenSQLite(dbPath string) (*SQLiteStore, error) {
	if strings.TrimSpace(dbPath) == "" {
		return nil, errors.New("database path cannot be empty")
	}

	// Ensure parent directory exists for file-based databases
	if dbPath != ":memory:" {
		dir := filepath.Dir(dbPath)
		if dir != "." && dir != "" {
			if err := os.MkdirAll(dir, 0755); err != nil {
				return nil, fmt.Errorf("failed to create database directory: %w", err)
			}
		}
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	// Configure connection pool: a single connection avoids database-level write contention
	// and guarantees that connection-scoped PRAGMAs (synchronous=FULL, foreign_keys=ON, busy_timeout=5000)
	// remain permanently active on the single underlying connection used by all operations.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	store := &SQLiteStore{
		db:   db,
		path: dbPath,
	}

	if err := store.configurePragmas(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to configure sqlite pragmas: %w", err)
	}

	if err := store.initSchema(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to initialize sqlite schema: %w", err)
	}

	return store, nil
}

// configurePragmas applies performance and durability PRAGMAs.
// Note on synchronous=FULL: In WAL mode, synchronous=FULL issues a disk fsync on every
// transaction commit, guaranteeing that committed batches are physically persisted to non-volatile
// storage before network acknowledgment (HTTP 201 Created or NATS Ack).
func (s *SQLiteStore) configurePragmas() error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=FULL;",
		"PRAGMA busy_timeout=5000;",
		"PRAGMA foreign_keys=ON;",
	}

	for _, p := range pragmas {
		if _, err := s.db.Exec(p); err != nil {
			return fmt.Errorf("executing %q failed: %w", p, err)
		}
	}
	return nil
}

// initSchema creates the idempotency tracking table, payload table, and indexes if they do not exist.
func (s *SQLiteStore) initSchema() error {
	schema := `
	CREATE TABLE IF NOT EXISTS cp_ingested_batches (
		node_id         TEXT NOT NULL,
		batch_id        TEXT NOT NULL,
		sequence_number INTEGER NOT NULL,
		collected_at    DATETIME NOT NULL,
		ingested_at     DATETIME NOT NULL,
		created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (node_id, batch_id)
	);

	CREATE INDEX IF NOT EXISTS idx_cp_ingested_node_seq 
		ON cp_ingested_batches(node_id, sequence_number ASC);

	CREATE INDEX IF NOT EXISTS idx_cp_ingested_at 
		ON cp_ingested_batches(ingested_at ASC);

	CREATE TABLE IF NOT EXISTS cp_telemetry_payloads (
		node_id         TEXT NOT NULL,
		batch_id        TEXT NOT NULL,
		sequence_number INTEGER NOT NULL,
		collected_at    DATETIME NOT NULL,
		ingested_at     DATETIME NOT NULL,
		payload_json    TEXT NOT NULL,
		created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (node_id, batch_id),
		FOREIGN KEY (node_id, batch_id) REFERENCES cp_ingested_batches(node_id, batch_id) ON DELETE CASCADE
	);

	CREATE INDEX IF NOT EXISTS idx_cp_payloads_ingested_at 
		ON cp_telemetry_payloads(ingested_at ASC);
	`

	if _, err := s.db.Exec(schema); err != nil {
		return fmt.Errorf("failed to execute schema initialization: %w", err)
	}
	return nil
}

// RecordBatch atomically records both the idempotency metadata and the complete telemetry payload
// within a single SQLite transaction.
//
// Returns (true, nil) if newly recorded.
// Returns (false, nil) if already recorded (duplicate constraint detected).
// Returns (false, err) on genuine storage failure (transaction rolled back completely).
func (s *SQLiteStore) RecordBatch(ctx context.Context, batch *types.TelemetryBatch, ingestedAt time.Time) (bool, error) {
	if batch == nil {
		return false, fmt.Errorf("%w: batch cannot be nil", ErrInvalidMeta)
	}
	if err := batch.Validate(); err != nil {
		return false, fmt.Errorf("%w: %v", ErrInvalidMeta, err)
	}
	if ingestedAt.IsZero() {
		return false, fmt.Errorf("%w: ingested_at cannot be zero", ErrInvalidMeta)
	}

	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return false, ErrStoreClosed
	}
	s.mu.RUnlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // Safe: no-op if tx.Commit() succeeds

	collectedAtStr := batch.CollectedAt.UTC().Format(time.RFC3339Nano)
	ingestedAtStr := ingestedAt.UTC().Format(time.RFC3339Nano)

	// Step 1: Insert idempotency metadata into cp_ingested_batches
	queryMeta := `
	INSERT INTO cp_ingested_batches (node_id, batch_id, sequence_number, collected_at, ingested_at)
	VALUES (?, ?, ?, ?, ?);
	`
	_, err = tx.ExecContext(ctx, queryMeta,
		batch.NodeID,
		batch.BatchID,
		batch.SequenceNumber,
		collectedAtStr,
		ingestedAtStr,
	)
	if err != nil {
		if isDuplicateKeyError(err) {
			_ = tx.Rollback()
			return false, nil // Idempotent duplicate: already accepted previously
		}
		return false, fmt.Errorf("failed to record batch metadata: %w", err)
	}

	// Optional test hook for simulating crash between metadata and payload insert
	if s.HookAfterMetaInsert != nil {
		if err := s.HookAfterMetaInsert(); err != nil {
			_ = tx.Rollback()
			return false, fmt.Errorf("injected test failure after metadata insert: %w", err)
		}
	}

	// Step 2: Serialize complete payload
	payloadBytes, err := json.Marshal(batch)
	if err != nil {
		return false, fmt.Errorf("failed to marshal telemetry batch payload: %w", err)
	}

	// Step 3: Insert complete payload into cp_telemetry_payloads
	queryPayload := `
	INSERT INTO cp_telemetry_payloads (node_id, batch_id, sequence_number, collected_at, ingested_at, payload_json)
	VALUES (?, ?, ?, ?, ?, ?);
	`
	_, err = tx.ExecContext(ctx, queryPayload,
		batch.NodeID,
		batch.BatchID,
		batch.SequenceNumber,
		collectedAtStr,
		ingestedAtStr,
		string(payloadBytes),
	)
	if err != nil {
		return false, fmt.Errorf("failed to record batch payload: %w", err)
	}

	// Step 4: Commit both atomically
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("failed to commit batch transaction: %w", err)
	}

	return true, nil
}

// GetPayload retrieves the full persisted batch payload by node ID and batch ID.
func (s *SQLiteStore) GetPayload(ctx context.Context, nodeID, batchID string) (*types.TelemetryBatch, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, ErrStoreClosed
	}
	s.mu.RUnlock()

	query := `SELECT payload_json FROM cp_telemetry_payloads WHERE node_id = ? AND batch_id = ?;`
	var payloadJSON string
	err := s.db.QueryRowContext(ctx, query, nodeID, batchID).Scan(&payloadJSON)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrBatchNotFound
		}
		return nil, fmt.Errorf("failed to query batch payload: %w", err)
	}

	var batch types.TelemetryBatch
	if err := json.Unmarshal([]byte(payloadJSON), &batch); err != nil {
		return nil, &CorruptPayloadError{
			NodeID:  nodeID,
			BatchID: batchID,
			Err:     err,
		}
	}
	return &batch, nil
}

// GetRecentBatches retrieves up to limit recent persisted batches in chronological order (oldest to newest).
// If individual stored batch payloads are corrupted, it skips them without aborting recovery,
// recovers all other valid batches in chronological order, and returns an error combining diagnostics
// for all skipped rows (identifying their NodeID, BatchID, and JSON decode error).
func (s *SQLiteStore) GetRecentBatches(ctx context.Context, limit int) ([]*StoredBatch, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, ErrStoreClosed
	}
	s.mu.RUnlock()

	if limit <= 0 {
		limit = 1000
	}

	query := `
	SELECT node_id, batch_id, payload_json, ingested_at
	FROM (
		SELECT node_id, batch_id, payload_json, ingested_at, rowid
		FROM cp_telemetry_payloads
		ORDER BY rowid DESC
		LIMIT ?
	)
	ORDER BY rowid ASC;
	`
	rows, err := s.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query recent payloads: %w", err)
	}
	defer rows.Close()

	var out []*StoredBatch
	var corruptErrs []error

	for rows.Next() {
		var nodeID, batchID, payloadJSON, ingestedAtStr string
		if err := rows.Scan(&nodeID, &batchID, &payloadJSON, &ingestedAtStr); err != nil {
			return nil, fmt.Errorf("failed to scan payload row: %w", err)
		}
		var batch types.TelemetryBatch
		if err := json.Unmarshal([]byte(payloadJSON), &batch); err != nil {
			// Record diagnostic for skipped corrupt row without aborting the recovery of valid rows
			corruptErrs = append(corruptErrs, &CorruptPayloadError{
				NodeID:  nodeID,
				BatchID: batchID,
				Err:     err,
			})
			continue
		}
		ingestedAt, _ := time.Parse(time.RFC3339Nano, ingestedAtStr)
		out = append(out, &StoredBatch{
			Batch:      batch,
			IngestedAt: ingestedAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}
	if out == nil {
		out = []*StoredBatch{}
	}
	if len(corruptErrs) > 0 {
		return out, errors.Join(corruptErrs...)
	}
	return out, nil
}

// HasBatch reports whether a batch identified by (nodeID, batchID) exists in the store.
func (s *SQLiteStore) HasBatch(ctx context.Context, nodeID, batchID string) (bool, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return false, ErrStoreClosed
	}
	s.mu.RUnlock()

	var count int
	query := `SELECT COUNT(1) FROM cp_ingested_batches WHERE node_id = ? AND batch_id = ?;`
	err := s.db.QueryRowContext(ctx, query, nodeID, batchID).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check batch existence: %w", err)
	}
	return count > 0, nil
}

// Count returns the total number of uniquely recorded batches in the database.
func (s *SQLiteStore) Count(ctx context.Context) (int, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return 0, ErrStoreClosed
	}
	s.mu.RUnlock()

	var count int
	query := `SELECT COUNT(1) FROM cp_ingested_batches;`
	err := s.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to query batch count: %w", err)
	}
	return count, nil
}

// Close gracefully flushes and closes the database connection.
func (s *SQLiteStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true
	return s.db.Close()
}

// isDuplicateKeyError detects unique and primary-key constraint violations from SQLite.
// It strictly distinguishes duplicate key errors from non-uniqueness errors such as
// foreign-key failures, check-constraint failures, NOT NULL failures, or I/O errors.
func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrDuplicateBatch) {
		return true
	}
	var sqliteErr *sqlite.Error
	if errors.As(err, &sqliteErr) {
		code := sqliteErr.Code()
		if code == sqlite3.SQLITE_CONSTRAINT_UNIQUE || code == sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY {
			return true
		}
		// Any other SQLite error code (foreign key, check constraint, not null, disk full, etc.)
		// is not a duplicate batch and must not be classified as one.
		return false
	}
	// Fallback to narrow string matching if the driver error was wrapped without exposing *sqlite.Error.
	// Only match unambiguous uniqueness and primary key failures, never generic "constraint failed".
	errStr := err.Error()
	return strings.Contains(errStr, "UNIQUE constraint failed") ||
		strings.Contains(errStr, "PRIMARY KEY constraint failed")
}

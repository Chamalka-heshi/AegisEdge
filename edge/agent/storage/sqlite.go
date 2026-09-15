package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
	_ "modernc.org/sqlite"
)

// SQLiteStore implements the Store interface using SQLite in WAL mode.
type SQLiteStore struct {
	db     *sql.DB
	path   string
	closed bool
	mu     sync.RWMutex
}

// OpenSQLite initializes a SQLite database connection, enables WAL mode,
// and executes automatic schema migrations.
func OpenSQLite(dbPath string) (*SQLiteStore, error) {
	if dbPath == "" {
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

	// Configure connection pool for SQLite embedded access
	db.SetMaxOpenConns(1) // Single-writer model guarantees transactional safety in SQLite
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	store := &SQLiteStore{
		db:   db,
		path: dbPath,
	}

	// Apply durability PRAGMAs and schema migrations
	if err := store.configurePragmas(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to configure sqlite pragmas: %w", err)
	}

	if err := store.runMigrations(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to run database migrations: %w", err)
	}

	return store, nil
}

// configurePragmas applies WAL mode and concurrency tuning.
func (s *SQLiteStore) configurePragmas() error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=NORMAL;",
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

// runMigrations executes schema migrations sequentially inside a transaction.
func (s *SQLiteStore) runMigrations() error {
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	// 1. Ensure migrations table exists
	createMigrationsTable := `
	CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY,
		applied_at TEXT NOT NULL
	);`
	if _, err := tx.ExecContext(ctx, createMigrationsTable); err != nil {
		return fmt.Errorf("failed to create schema_migrations table: %w", err)
	}

	// 2. Query current migration version
	var currentVersion int
	row := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(version), 0) FROM schema_migrations;")
	if err := row.Scan(&currentVersion); err != nil {
		return fmt.Errorf("failed to query schema version: %w", err)
	}

	// 3. Migration v1: telemetry_batches table and performance indexes
	if currentVersion < 1 {
		createBatchesTable := `
		CREATE TABLE IF NOT EXISTS telemetry_batches (
			batch_id TEXT PRIMARY KEY,
			node_id TEXT NOT NULL,
			sequence_number INTEGER NOT NULL,
			collected_at TEXT NOT NULL,
			sent_at TEXT,
			attempt INTEGER NOT NULL DEFAULT 0,
			payload TEXT NOT NULL,
			created_at TEXT NOT NULL,
			sync_status TEXT NOT NULL DEFAULT 'PENDING'
		);
		CREATE INDEX IF NOT EXISTS idx_telemetry_batches_node_seq ON telemetry_batches(node_id, sequence_number);
		CREATE INDEX IF NOT EXISTS idx_telemetry_batches_sync_status ON telemetry_batches(sync_status);
		`
		if _, err := tx.ExecContext(ctx, createBatchesTable); err != nil {
			return fmt.Errorf("migration v1 failed: %w", err)
		}

		recordMigration := `INSERT INTO schema_migrations (version, applied_at) VALUES (1, ?);`
		if _, err := tx.ExecContext(ctx, recordMigration, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return fmt.Errorf("failed to record migration v1: %w", err)
		}
	}

	return tx.Commit()
}

// PersistBatch validates and transactionally commits a TelemetryBatch to SQLite WAL storage.
func (s *SQLiteStore) PersistBatch(ctx context.Context, batch *types.TelemetryBatch) error {
	if batch == nil {
		return ErrInvalidBatch
	}
	if err := batch.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidBatch, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	// Check for idempotency: check if batch_id already exists
	var exists int
	checkQuery := `SELECT 1 FROM telemetry_batches WHERE batch_id = ? LIMIT 1;`
	err := s.db.QueryRowContext(ctx, checkQuery, batch.BatchID).Scan(&exists)
	if err == nil {
		// Duplicate found
		return ErrDuplicateBatch
	} else if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to check batch existence: %w", err)
	}

	payloadBytes, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("failed to marshal batch payload: %w", err)
	}

	nowStr := time.Now().UTC().Format(time.RFC3339Nano)
	collectedStr := batch.CollectedAt.UTC().Format(time.RFC3339Nano)

	var sentAtStr sql.NullString
	if batch.SentAt != nil {
		sentAtStr = sql.NullString{
			String: batch.SentAt.UTC().Format(time.RFC3339Nano),
			Valid:  true,
		}
	}

	insertQuery := `
	INSERT INTO telemetry_batches (
		batch_id,
		node_id,
		sequence_number,
		collected_at,
		sent_at,
		attempt,
		payload,
		created_at,
		sync_status
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
	`

	_, err = s.db.ExecContext(
		ctx,
		insertQuery,
		batch.BatchID,
		batch.NodeID,
		batch.SequenceNumber,
		collectedStr,
		sentAtStr,
		batch.Attempt,
		string(payloadBytes),
		nowStr,
		string(SyncStatusPending),
	)
	if err != nil {
		return fmt.Errorf("failed to insert telemetry batch: %w", err)
	}

	return nil
}

// GetBatch retrieves a single persisted telemetry record by its BatchID.
func (s *SQLiteStore) GetBatch(ctx context.Context, batchID string) (*Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrStoreClosed
	}

	query := `
	SELECT 
		batch_id,
		node_id,
		sequence_number,
		collected_at,
		sent_at,
		attempt,
		payload,
		created_at,
		sync_status
	FROM telemetry_batches
	WHERE batch_id = ?;`

	row := s.db.QueryRowContext(ctx, query, batchID)
	return scanRecord(row)
}

// GetPendingBatches returns pending records ordered by sequence_number.
func (s *SQLiteStore) GetPendingBatches(ctx context.Context, limit int) ([]*Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrStoreClosed
	}

	if limit <= 0 {
		limit = 100
	}

	query := `
	SELECT 
		batch_id,
		node_id,
		sequence_number,
		collected_at,
		sent_at,
		attempt,
		payload,
		created_at,
		sync_status
	FROM telemetry_batches
	WHERE sync_status = ?
	ORDER BY sequence_number ASC
	LIMIT ?;`

	rows, err := s.db.QueryContext(ctx, query, string(SyncStatusPending), limit)
	if err != nil {
		return nil, fmt.Errorf("querying pending batches failed: %w", err)
	}
	defer rows.Close()

	var results []*Record
	for rows.Next() {
		rec, err := scanRecord(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating pending batches failed: %w", err)
	}

	return results, nil
}

// GetLatestSequenceNumber returns the highest sequence number stored for the given nodeID.
// If no batches exist, it returns -1.
func (s *SQLiteStore) GetLatestSequenceNumber(ctx context.Context, nodeID string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return -1, ErrStoreClosed
	}

	query := `SELECT COALESCE(MAX(sequence_number), -1) FROM telemetry_batches WHERE node_id = ?;`
	var maxSeq int64
	err := s.db.QueryRowContext(ctx, query, nodeID).Scan(&maxSeq)
	if err != nil {
		return -1, fmt.Errorf("failed to query latest sequence number: %w", err)
	}

	return maxSeq, nil
}

// CountBatches returns total rows stored in telemetry_batches.
func (s *SQLiteStore) CountBatches(ctx context.Context) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return 0, ErrStoreClosed
	}

	query := `SELECT COUNT(*) FROM telemetry_batches;`
	var count int64
	err := s.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count batches: %w", err)
	}

	return count, nil
}

// Close terminates the database connection and flushes write-ahead logs.
func (s *SQLiteStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	// Checkpoint WAL journal prior to close
	_, _ = s.db.Exec("PRAGMA wal_checkpoint(TRUNCATE);")
	return s.db.Close()
}

// scanner abstracts QueryRow vs Rows Scan.
type scanner interface {
	Scan(dest ...any) error
}

func scanRecord(s scanner) (*Record, error) {
	var (
		batchID      string
		nodeID       string
		seqNo        int64
		collectedStr string
		sentAtStr    sql.NullString
		attempt      int
		payloadJSON  string
		createdStr   string
		syncStatus   string
	)

	err := s.Scan(
		&batchID,
		&nodeID,
		&seqNo,
		&collectedStr,
		&sentAtStr,
		&attempt,
		&payloadJSON,
		&createdStr,
		&syncStatus,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrBatchNotFound
		}
		return nil, fmt.Errorf("failed to scan batch record: %w", err)
	}

	collectedAt, err := time.Parse(time.RFC3339Nano, collectedStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse collected_at timestamp: %w", err)
	}

	createdAt, err := time.Parse(time.RFC3339Nano, createdStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse created_at timestamp: %w", err)
	}

	var sentAt *time.Time
	if sentAtStr.Valid {
		t, err := time.Parse(time.RFC3339Nano, sentAtStr.String)
		if err == nil {
			sentAt = &t
		}
	}

	var payload types.TelemetryBatch
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal batch payload: %w", err)
	}

	return &Record{
		BatchID:        batchID,
		NodeID:         nodeID,
		SequenceNumber: seqNo,
		CollectedAt:    collectedAt,
		SentAt:         sentAt,
		Attempt:        attempt,
		Payload:        payload,
		CreatedAt:      createdAt,
		SyncStatus:     SyncStatus(syncStatus),
	}, nil
}

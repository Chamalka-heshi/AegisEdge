package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
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

	// 4. Migration v2: published_at column for NATS JetStream PUBLISHED state
	if currentVersion < 2 {
		addPublishedAt := `ALTER TABLE telemetry_batches ADD COLUMN published_at TEXT;`
		if _, err := tx.ExecContext(ctx, addPublishedAt); err != nil {
			return fmt.Errorf("migration v2 failed: %w", err)
		}

		recordMigration := `INSERT INTO schema_migrations (version, applied_at) VALUES (2, ?);`
		if _, err := tx.ExecContext(ctx, recordMigration, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return fmt.Errorf("failed to record migration v2: %w", err)
		}
	}

	// 5. Migration v3: incident_records and incident_observations tables
	if currentVersion < 3 {
		createIncidentTables := `
		CREATE TABLE IF NOT EXISTS incident_records (
			incident_id TEXT PRIMARY KEY,
			node_id TEXT NOT NULL,
			metric_name TEXT NOT NULL,
			rule_name TEXT NOT NULL,
			severity TEXT NOT NULL,
			status TEXT NOT NULL,
			description TEXT NOT NULL,
			trigger_metric TEXT NOT NULL,
			trigger_value REAL NOT NULL,
			threshold REAL NOT NULL,
			evidence TEXT NOT NULL,
			triggered_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			resolved_at TEXT,
			created_at TEXT NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_incident_records_node_metric ON incident_records(node_id, metric_name);
		CREATE INDEX IF NOT EXISTS idx_incident_records_status ON incident_records(status);

		CREATE TABLE IF NOT EXISTS incident_observations (
			anomaly_id TEXT PRIMARY KEY,
			incident_id TEXT,
			node_id TEXT NOT NULL,
			metric_name TEXT NOT NULL,
			detected_at TEXT NOT NULL,
			observed_value REAL NOT NULL,
			anomaly_score REAL NOT NULL,
			detection_method TEXT NOT NULL,
			evidence TEXT NOT NULL,
			created_at TEXT NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_incident_obs_node_metric_detected ON incident_observations(node_id, metric_name, detected_at);
		CREATE INDEX IF NOT EXISTS idx_incident_obs_incident_id ON incident_observations(incident_id);
		`
		if _, err := tx.ExecContext(ctx, createIncidentTables); err != nil {
			return fmt.Errorf("migration v3 failed: %w", err)
		}

		recordMigration := `INSERT INTO schema_migrations (version, applied_at) VALUES (3, ?);`
		if _, err := tx.ExecContext(ctx, recordMigration, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return fmt.Errorf("failed to record migration v3: %w", err)
		}
	}

	// 6. Migration v4: mitigation_records table and indexes for durable response persistence
	if currentVersion < 4 {
		createMitigationTable := `
		CREATE TABLE IF NOT EXISTS mitigation_records (
			action_id TEXT PRIMARY KEY,
			decision_id TEXT NOT NULL UNIQUE,
			incident_id TEXT NOT NULL,
			node_id TEXT NOT NULL,
			action_type TEXT NOT NULL,
			target TEXT NOT NULL,
			status TEXT NOT NULL,
			mode TEXT NOT NULL,
			message TEXT NOT NULL,
			error_code TEXT,
			parameters TEXT,
			started_at TEXT NOT NULL,
			completed_at TEXT,
			duration_ms INTEGER NOT NULL DEFAULT 0,
			simulated INTEGER NOT NULL DEFAULT 1,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_mitigation_records_incident_id ON mitigation_records(incident_id);
		CREATE INDEX IF NOT EXISTS idx_mitigation_records_node_id ON mitigation_records(node_id);
		CREATE INDEX IF NOT EXISTS idx_mitigation_records_status ON mitigation_records(status);
		`
		if _, err := tx.ExecContext(ctx, createMitigationTable); err != nil {
			return fmt.Errorf("migration v4 failed: %w", err)
		}

		recordMigration := `INSERT INTO schema_migrations (version, applied_at) VALUES (4, ?);`
		if _, err := tx.ExecContext(ctx, recordMigration, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return fmt.Errorf("failed to record migration v4: %w", err)
		}
	}

	// 7. Migration v5: approval_records table and indexes for operator approval boundary (Phase 6.5)
	if currentVersion < 5 {
		createApprovalTable := `
		CREATE TABLE IF NOT EXISTS approval_records (
			approval_id TEXT PRIMARY KEY,
			decision_id TEXT NOT NULL UNIQUE,
			incident_id TEXT NOT NULL,
			action_id TEXT NOT NULL,
			node_id TEXT NOT NULL,
			action_type TEXT NOT NULL,
			target TEXT NOT NULL,
			policy_version TEXT NOT NULL,
			decision_fingerprint TEXT NOT NULL,
			status TEXT NOT NULL,
			requested_at TEXT NOT NULL,
			expires_at TEXT NOT NULL,
			requested_by TEXT NOT NULL,
			approved_by TEXT NOT NULL DEFAULT '',
			rejected_by TEXT NOT NULL DEFAULT '',
			reason TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			consumed_at TEXT
		);
		CREATE INDEX IF NOT EXISTS idx_approval_records_incident ON approval_records(incident_id);
		CREATE INDEX IF NOT EXISTS idx_approval_records_node ON approval_records(node_id);
		CREATE INDEX IF NOT EXISTS idx_approval_records_status ON approval_records(status);
		CREATE INDEX IF NOT EXISTS idx_approval_records_action ON approval_records(action_id);
		`
		if _, err := tx.ExecContext(ctx, createApprovalTable); err != nil {
			return fmt.Errorf("migration v5 failed: %w", err)
		}

		recordMigration := `INSERT INTO schema_migrations (version, applied_at) VALUES (5, ?);`
		if _, err := tx.ExecContext(ctx, recordMigration, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return fmt.Errorf("failed to record migration v5: %w", err)
		}
	}

	// 8. Migration v6: verification_records table and indexes for closed-loop incident verification (Phase 6.6)
	if currentVersion < 6 {
		createVerificationTable := `
		CREATE TABLE IF NOT EXISTS verification_records (
			verification_id TEXT PRIMARY KEY,
			incident_id TEXT NOT NULL,
			action_id TEXT NOT NULL,
			decision_id TEXT NOT NULL,
			node_id TEXT NOT NULL,
			metric_name TEXT NOT NULL,
			condition_type TEXT NOT NULL,
			recovery_threshold REAL NOT NULL,
			comparator TEXT NOT NULL,
			status TEXT NOT NULL,
			required_observations INTEGER NOT NULL,
			consecutive_healthy INTEGER NOT NULL DEFAULT 0,
			total_observations INTEGER NOT NULL DEFAULT 0,
			started_at TEXT NOT NULL,
			expires_at TEXT NOT NULL,
			completed_at TEXT,
			recovered_at TEXT,
			last_observation_at TEXT,
			last_observed_value REAL,
			reason TEXT NOT NULL DEFAULT '',
			evidence TEXT NOT NULL DEFAULT '{}',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_verification_records_incident ON verification_records(incident_id);
		CREATE INDEX IF NOT EXISTS idx_verification_records_node_metric ON verification_records(node_id, metric_name);
		CREATE INDEX IF NOT EXISTS idx_verification_records_status ON verification_records(status);
		CREATE INDEX IF NOT EXISTS idx_verification_records_action ON verification_records(action_id);
		`
		if _, err := tx.ExecContext(ctx, createVerificationTable); err != nil {
			return fmt.Errorf("migration v6 failed: %w", err)
		}

		recordMigration := `INSERT INTO schema_migrations (version, applied_at) VALUES (6, ?);`
		if _, err := tx.ExecContext(ctx, recordMigration, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return fmt.Errorf("failed to record migration v6: %w", err)
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

// GetPendingNodes returns distinct NodeIDs that have batches awaiting synchronization.
func (s *SQLiteStore) GetPendingNodes(ctx context.Context) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrStoreClosed
	}

	query := `SELECT DISTINCT node_id FROM telemetry_batches WHERE sync_status = ? ORDER BY node_id ASC;`
	rows, err := s.db.QueryContext(ctx, query, string(SyncStatusPending))
	if err != nil {
		return nil, fmt.Errorf("querying pending nodes failed: %w", err)
	}
	defer rows.Close()

	var nodes []string
	for rows.Next() {
		var nodeID string
		if err := rows.Scan(&nodeID); err != nil {
			return nil, fmt.Errorf("scanning node_id failed: %w", err)
		}
		nodes = append(nodes, nodeID)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating pending nodes failed: %w", err)
	}

	return nodes, nil
}

// GetPendingBatchesByNode retrieves pending records for a specific node ordered strictly by sequence_number ASC.
func (s *SQLiteStore) GetPendingBatchesByNode(ctx context.Context, nodeID string, limit int) ([]*Record, error) {
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
	WHERE sync_status = ? AND node_id = ?
	ORDER BY sequence_number ASC
	LIMIT ?;`

	rows, err := s.db.QueryContext(ctx, query, string(SyncStatusPending), nodeID, limit)
	if err != nil {
		return nil, fmt.Errorf("querying pending batches for node %q failed: %w", nodeID, err)
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
		return nil, fmt.Errorf("iterating pending batches for node %q failed: %w", nodeID, err)
	}

	return results, nil
}

// MarkBatchSynced updates the batch's sync_status to SYNCED and records sent_at.
func (s *SQLiteStore) MarkBatchSynced(ctx context.Context, batchID string, sentAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	query := `UPDATE telemetry_batches SET sync_status = ?, sent_at = ? WHERE batch_id = ?;`
	sentAtStr := sentAt.UTC().Format(time.RFC3339Nano)
	res, err := s.db.ExecContext(ctx, query, string(SyncStatusSynced), sentAtStr, batchID)
	if err != nil {
		return fmt.Errorf("marking batch synced failed: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking rows affected failed: %w", err)
	}
	if rowsAffected == 0 {
		return ErrBatchNotFound
	}

	return nil
}

// RecordSyncAttempt increments the logical sync attempt counter by 1 and updates sent_at,
// keeping sync_status as PENDING. This represents a failed logical synchronization cycle.
func (s *SQLiteStore) RecordSyncAttempt(ctx context.Context, batchID string, sentAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	query := `UPDATE telemetry_batches SET attempt = attempt + 1, sent_at = ? WHERE batch_id = ?;`
	sentAtStr := sentAt.UTC().Format(time.RFC3339Nano)
	res, err := s.db.ExecContext(ctx, query, sentAtStr, batchID)
	if err != nil {
		return fmt.Errorf("recording sync attempt failed: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking rows affected failed: %w", err)
	}
	if rowsAffected == 0 {
		return ErrBatchNotFound
	}

	return nil
}

// MarkBatchPublished updates the batch's sync_status to PUBLISHED and records published_at.
// This indicates that a NATS JetStream PubAck was successfully received.
func (s *SQLiteStore) MarkBatchPublished(ctx context.Context, batchID string, publishedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStoreClosed
	}

	query := `UPDATE telemetry_batches SET sync_status = ?, published_at = ? WHERE batch_id = ?;`
	publishedAtStr := publishedAt.UTC().Format(time.RFC3339Nano)
	res, err := s.db.ExecContext(ctx, query, string(SyncStatusPublished), publishedAtStr, batchID)
	if err != nil {
		return fmt.Errorf("marking batch published failed: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking rows affected failed: %w", err)
	}
	if rowsAffected == 0 {
		return ErrBatchNotFound
	}

	return nil
}

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

// Ensure SQLiteStore implements IncidentStore.
var _ IncidentStore = (*SQLiteStore)(nil)

// HasObservation checks if an AnomalyID has already been durably recorded.
func (s *SQLiteStore) HasObservation(ctx context.Context, anomalyID string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return false, ErrStoreClosed
	}

	var exists int
	query := `SELECT 1 FROM incident_observations WHERE anomaly_id = ? LIMIT 1;`
	err := s.db.QueryRowContext(ctx, query, anomalyID).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// RecordObservation inserts an anomaly observation into SQLite.
func (s *SQLiteStore) RecordObservation(ctx context.Context, obs *StoredObservation) error {
	if obs == nil || obs.AnomalyID == "" {
		return errors.New("cannot record nil or empty anomaly observation")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}

	evidenceJSON, err := json.Marshal(obs.Evidence)
	if err != nil {
		return fmt.Errorf("failed to marshal observation evidence: %w", err)
	}

	now := time.Now().UTC()
	createdAt := obs.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}

	var incID *string
	if obs.IncidentID != "" {
		incID = &obs.IncidentID
	}

	query := `
	INSERT INTO incident_observations (
		anomaly_id, incident_id, node_id, metric_name, detected_at,
		observed_value, anomaly_score, detection_method, evidence, created_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
	`
	_, err = s.db.ExecContext(ctx, query,
		obs.AnomalyID,
		incID,
		obs.NodeID,
		obs.MetricName,
		obs.DetectedAt.UTC().Format(time.RFC3339Nano),
		obs.ObservedValue,
		obs.AnomalyScore,
		obs.DetectionMethod,
		string(evidenceJSON),
		createdAt.Format(time.RFC3339Nano),
	)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") || strings.Contains(err.Error(), "constraint failed") {
			return ErrDuplicateAnomaly
		}
		return fmt.Errorf("failed to record anomaly observation: %w", err)
	}
	return nil
}

// GetActiveIncident retrieves the current active incident for nodeID and metricName.
func (s *SQLiteStore) GetActiveIncident(ctx context.Context, nodeID, metricName string) (*types.Incident, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrStoreClosed
	}

	query := `
	SELECT incident_id, node_id, metric_name, rule_name, severity, status,
	       description, trigger_metric, trigger_value, threshold, evidence,
	       triggered_at, updated_at, resolved_at
	FROM incident_records
	WHERE node_id = ? AND metric_name = ? AND status IN ('ANOMALY_DETECTED', 'MITIGATING', 'ESCALATED')
	ORDER BY triggered_at DESC
	LIMIT 1;
	`
	row := s.db.QueryRowContext(ctx, query, nodeID, metricName)
	inc, err := scanIncident(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return inc, nil
}

// GetIncident retrieves an incident by its unique IncidentID.
func (s *SQLiteStore) GetIncident(ctx context.Context, incidentID string) (*types.Incident, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrStoreClosed
	}

	query := `
	SELECT incident_id, node_id, metric_name, rule_name, severity, status,
	       description, trigger_metric, trigger_value, threshold, evidence,
	       triggered_at, updated_at, resolved_at
	FROM incident_records
	WHERE incident_id = ?
	LIMIT 1;
	`
	row := s.db.QueryRowContext(ctx, query, incidentID)
	inc, err := scanIncident(row)
	if err == sql.ErrNoRows {
		return nil, ErrIncidentNotFound
	}
	if err != nil {
		return nil, err
	}
	return inc, nil
}

// ListActiveIncidents retrieves all incidents currently in active status.
func (s *SQLiteStore) ListActiveIncidents(ctx context.Context) ([]*types.Incident, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrStoreClosed
	}

	query := `
	SELECT incident_id, node_id, metric_name, rule_name, severity, status,
	       description, trigger_metric, trigger_value, threshold, evidence,
	       triggered_at, updated_at, resolved_at
	FROM incident_records
	WHERE status IN ('ANOMALY_DETECTED', 'MITIGATING', 'ESCALATED')
	ORDER BY triggered_at ASC;
	`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*types.Incident
	for rows.Next() {
		inc, err := scanIncident(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, inc)
	}
	return result, rows.Err()
}

// ListRecentObservations retrieves recent observations for a correlation stream.
func (s *SQLiteStore) ListRecentObservations(ctx context.Context, nodeID, metricName string, since time.Time, limit int) ([]*StoredObservation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrStoreClosed
	}

	if limit <= 0 {
		limit = 50
	}

	query := `
	SELECT anomaly_id, incident_id, node_id, metric_name, detected_at,
	       observed_value, anomaly_score, detection_method, evidence, created_at
	FROM incident_observations
	WHERE node_id = ? AND metric_name = ? AND detected_at >= ?
	ORDER BY detected_at ASC
	LIMIT ?;
	`
	rows, err := s.db.QueryContext(ctx, query, nodeID, metricName, since.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*StoredObservation
	for rows.Next() {
		var (
			obs          StoredObservation
			incID        sql.NullString
			detectedStr  string
			createdStr   string
			evidenceJSON string
		)
		if err := rows.Scan(
			&obs.AnomalyID,
			&incID,
			&obs.NodeID,
			&obs.MetricName,
			&detectedStr,
			&obs.ObservedValue,
			&obs.AnomalyScore,
			&obs.DetectionMethod,
			&evidenceJSON,
			&createdStr,
		); err != nil {
			return nil, err
		}
		if incID.Valid {
			obs.IncidentID = incID.String
		}
		obs.DetectedAt, _ = parseDBTime(detectedStr)
		obs.CreatedAt, _ = parseDBTime(createdStr)
		if evidenceJSON != "" {
			_ = json.Unmarshal([]byte(evidenceJSON), &obs.Evidence)
		}
		result = append(result, &obs)
	}
	return result, rows.Err()
}

// ListAllRecentObservations retrieves recent observations across all streams detected at or after since.
func (s *SQLiteStore) ListAllRecentObservations(ctx context.Context, since time.Time, limit int) ([]*StoredObservation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrStoreClosed
	}

	if limit <= 0 {
		limit = 500
	}

	query := `
	SELECT anomaly_id, incident_id, node_id, metric_name, detected_at,
	       observed_value, anomaly_score, detection_method, evidence, created_at
	FROM incident_observations
	WHERE detected_at >= ?
	ORDER BY detected_at ASC
	LIMIT ?;
	`
	rows, err := s.db.QueryContext(ctx, query, since.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*StoredObservation
	for rows.Next() {
		var (
			obs          StoredObservation
			incID        sql.NullString
			detectedStr  string
			createdStr   string
			evidenceJSON string
		)
		if err := rows.Scan(
			&obs.AnomalyID,
			&incID,
			&obs.NodeID,
			&obs.MetricName,
			&detectedStr,
			&obs.ObservedValue,
			&obs.AnomalyScore,
			&obs.DetectionMethod,
			&evidenceJSON,
			&createdStr,
		); err != nil {
			return nil, err
		}
		if incID.Valid {
			obs.IncidentID = incID.String
		}
		obs.DetectedAt, _ = parseDBTime(detectedStr)
		obs.CreatedAt, _ = parseDBTime(createdStr)
		if evidenceJSON != "" {
			_ = json.Unmarshal([]byte(evidenceJSON), &obs.Evidence)
		}
		result = append(result, &obs)
	}
	return result, rows.Err()
}

// PersistIncidentEvaluation executes an atomic transaction for observation insertion and optional incident update/creation.
func (s *SQLiteStore) PersistIncidentEvaluation(ctx context.Context, obs *StoredObservation, inc *types.Incident) (bool, error) {
	if obs == nil || obs.AnomalyID == "" {
		return false, errors.New("cannot persist nil observation or empty anomaly_id")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false, ErrStoreClosed
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("failed to begin incident evaluation tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	// 1. Idempotency Check
	var exists int
	checkQuery := `SELECT 1 FROM incident_observations WHERE anomaly_id = ? LIMIT 1;`
	err = tx.QueryRowContext(ctx, checkQuery, obs.AnomalyID).Scan(&exists)
	if err == nil {
		// Anomaly already exists! Idempotent no-op
		return false, nil
	} else if err != sql.ErrNoRows {
		return false, fmt.Errorf("checking observation idempotency failed: %w", err)
	}

	// 2. Persist Incident if provided
	if inc != nil {
		if err := inc.Validate(); err != nil {
			return false, fmt.Errorf("cannot persist invalid incident: %w", err)
		}

		evidenceJSON, err := json.Marshal(inc.Evidence)
		if err != nil {
			return false, fmt.Errorf("failed to marshal incident evidence: %w", err)
		}

		var resolvedStr *string
		if inc.ResolvedAt != nil {
			r := inc.ResolvedAt.UTC().Format(time.RFC3339Nano)
			resolvedStr = &r
		}

		// Check if incident already exists
		var incExists int
		checkIncQuery := `SELECT 1 FROM incident_records WHERE incident_id = ? LIMIT 1;`
		err = tx.QueryRowContext(ctx, checkIncQuery, inc.IncidentID).Scan(&incExists)
		if err == sql.ErrNoRows {
			// Insert new incident
			insertInc := `
			INSERT INTO incident_records (
				incident_id, node_id, metric_name, rule_name, severity, status,
				description, trigger_metric, trigger_value, threshold, evidence,
				triggered_at, updated_at, resolved_at, created_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
			`
			nowStr := time.Now().UTC().Format(time.RFC3339Nano)
			_, err = tx.ExecContext(ctx, insertInc,
				inc.IncidentID,
				inc.NodeID,
				inc.TriggerMetric,
				inc.RuleName,
				string(inc.Severity),
				string(inc.Status),
				inc.Description,
				inc.TriggerMetric,
				inc.TriggerValue,
				inc.Threshold,
				string(evidenceJSON),
				inc.TriggeredAt.UTC().Format(time.RFC3339Nano),
				inc.UpdatedAt.UTC().Format(time.RFC3339Nano),
				resolvedStr,
				nowStr,
			)
			if err != nil {
				return false, fmt.Errorf("failed to insert incident record: %w", err)
			}
		} else if err == nil {
			// Update existing incident
			updateInc := `
			UPDATE incident_records SET
				severity = ?,
				status = ?,
				description = ?,
				trigger_value = ?,
				threshold = ?,
				evidence = ?,
				updated_at = ?,
				resolved_at = ?
			WHERE incident_id = ?;
			`
			_, err = tx.ExecContext(ctx, updateInc,
				string(inc.Severity),
				string(inc.Status),
				inc.Description,
				inc.TriggerValue,
				inc.Threshold,
				string(evidenceJSON),
				inc.UpdatedAt.UTC().Format(time.RFC3339Nano),
				resolvedStr,
				inc.IncidentID,
			)
			if err != nil {
				return false, fmt.Errorf("failed to update incident record: %w", err)
			}
		} else {
			return false, fmt.Errorf("failed to check incident existence: %w", err)
		}

		// Associate observation with incident
		obs.IncidentID = inc.IncidentID
	}

	// 3. Persist Anomaly Observation
	obsEvidenceJSON, err := json.Marshal(obs.Evidence)
	if err != nil {
		return false, fmt.Errorf("failed to marshal observation evidence: %w", err)
	}

	now := time.Now().UTC()
	createdAt := obs.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}

	var incID *string
	if obs.IncidentID != "" {
		incID = &obs.IncidentID
	}

	insertObs := `
	INSERT INTO incident_observations (
		anomaly_id, incident_id, node_id, metric_name, detected_at,
		observed_value, anomaly_score, detection_method, evidence, created_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
	`
	_, err = tx.ExecContext(ctx, insertObs,
		obs.AnomalyID,
		incID,
		obs.NodeID,
		obs.MetricName,
		obs.DetectedAt.UTC().Format(time.RFC3339Nano),
		obs.ObservedValue,
		obs.AnomalyScore,
		obs.DetectionMethod,
		string(obsEvidenceJSON),
		createdAt.Format(time.RFC3339Nano),
	)
	if err != nil {
		return false, fmt.Errorf("failed to insert observation in evaluation tx: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("failed to commit evaluation tx: %w", err)
	}

	return true, nil
}

// UpdateIncidentStatus updates the status, updated_at, and resolved_at of an incident.
func (s *SQLiteStore) UpdateIncidentStatus(ctx context.Context, incidentID string, status types.IncidentStatus, updatedAt time.Time, resolvedAt *time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}

	var resolvedStr *string
	if resolvedAt != nil {
		r := resolvedAt.UTC().Format(time.RFC3339Nano)
		resolvedStr = &r
	}

	query := `
	UPDATE incident_records SET
		status = ?,
		updated_at = ?,
		resolved_at = ?
	WHERE incident_id = ?;
	`
	res, err := s.db.ExecContext(ctx, query,
		string(status),
		updatedAt.UTC().Format(time.RFC3339Nano),
		resolvedStr,
		incidentID,
	)
	if err != nil {
		return fmt.Errorf("failed to update incident status: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrIncidentNotFound
	}
	return nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanIncident(scanner rowScanner) (*types.Incident, error) {
	var (
		inc          types.Incident
		metricName   string
		sevStr       string
		statusStr    string
		evidenceJSON string
		trigStr      string
		updStr       string
		resStr       sql.NullString
	)

	if err := scanner.Scan(
		&inc.IncidentID,
		&inc.NodeID,
		&metricName,
		&inc.RuleName,
		&sevStr,
		&statusStr,
		&inc.Description,
		&inc.TriggerMetric,
		&inc.TriggerValue,
		&inc.Threshold,
		&evidenceJSON,
		&trigStr,
		&updStr,
		&resStr,
	); err != nil {
		return nil, err
	}

	inc.Severity = types.IncidentSeverity(sevStr)
	inc.Status = types.IncidentStatus(statusStr)
	inc.TriggeredAt, _ = parseDBTime(trigStr)
	inc.UpdatedAt, _ = parseDBTime(updStr)
	if resStr.Valid && resStr.String != "" {
		t, err := parseDBTime(resStr.String)
		if err == nil {
			inc.ResolvedAt = &t
		}
	}
	if evidenceJSON != "" {
		_ = json.Unmarshal([]byte(evidenceJSON), &inc.Evidence)
	}

	return &inc, nil
}

func parseDBTime(str string) (time.Time, error) {
	if str == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339Nano, str)
	if err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, str)
}

func formatDBTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func nullStringFromTimePtr(t *time.Time) sql.NullString {
	if t == nil || t.IsZero() {
		return sql.NullString{}
	}
	return sql.NullString{String: formatDBTime(*t), Valid: true}
}

// Ensure SQLiteStore implements MitigationStore, ApprovalStore, and VerificationStore.
var (
	_ MitigationStore   = (*SQLiteStore)(nil)
	_ ApprovalStore     = (*SQLiteStore)(nil)
	_ VerificationStore = (*SQLiteStore)(nil)
)

// RecordMitigation transactionally validates and persists a mitigation record.
// Returns ErrDuplicateMitigation if action_id or decision_id already exists.
func (s *SQLiteStore) RecordMitigation(ctx context.Context, m *StoredMitigation) error {
	if m == nil {
		return ErrInvalidMitigation
	}
	if err := m.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	// Duplicate check on action_id or decision_id
	var exists int
	checkQuery := `SELECT 1 FROM mitigation_records WHERE action_id = ? OR decision_id = ? LIMIT 1;`
	err = tx.QueryRowContext(ctx, checkQuery, m.ActionID, m.DecisionID).Scan(&exists)
	if err == nil {
		return ErrDuplicateMitigation
	} else if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to check mitigation existence: %w", err)
	}

	var paramsJSON sql.NullString
	if len(m.Parameters) > 0 {
		b, err := json.Marshal(m.Parameters)
		if err != nil {
			return fmt.Errorf("failed to marshal parameters: %w", err)
		}
		paramsJSON = sql.NullString{String: string(b), Valid: true}
	}

	var completedStr sql.NullString
	if m.CompletedAt != nil {
		completedStr = sql.NullString{
			String: m.CompletedAt.UTC().Format(time.RFC3339Nano),
			Valid:  true,
		}
	}

	var errCodeStr sql.NullString
	if m.ErrorCode != "" {
		errCodeStr = sql.NullString{String: m.ErrorCode, Valid: true}
	}

	now := time.Now().UTC()
	createdAt := m.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	updatedAt := m.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = now
	}

	simulatedInt := 0
	if m.Simulated {
		simulatedInt = 1
	}

	insertSQL := `
	INSERT INTO mitigation_records (
		action_id, decision_id, incident_id, node_id, action_type,
		target, status, mode, message, error_code, parameters,
		started_at, completed_at, duration_ms, simulated, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
	`
	_, err = tx.ExecContext(ctx, insertSQL,
		m.ActionID,
		m.DecisionID,
		m.IncidentID,
		m.NodeID,
		string(m.ActionType),
		m.Target,
		string(m.Status),
		m.Mode,
		m.Message,
		errCodeStr,
		paramsJSON,
		m.StartedAt.UTC().Format(time.RFC3339Nano),
		completedStr,
		m.DurationMs,
		simulatedInt,
		createdAt.Format(time.RFC3339Nano),
		updatedAt.Format(time.RFC3339Nano),
	)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") || strings.Contains(err.Error(), "constraint failed") {
			return ErrDuplicateMitigation
		}
		return fmt.Errorf("failed to insert mitigation record: %w", err)
	}

	return tx.Commit()
}

// GetMitigation retrieves a mitigation record by its unique ActionID.
func (s *SQLiteStore) GetMitigation(ctx context.Context, actionID string) (*StoredMitigation, error) {
	if strings.TrimSpace(actionID) == "" {
		return nil, errors.New("action_id cannot be empty")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrStoreClosed
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	query := `
	SELECT action_id, decision_id, incident_id, node_id, action_type,
	       target, status, mode, message, error_code, parameters,
	       started_at, completed_at, duration_ms, simulated, created_at, updated_at
	FROM mitigation_records
	WHERE action_id = ?
	LIMIT 1;
	`
	row := s.db.QueryRowContext(ctx, query, actionID)
	m, err := scanMitigation(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrMitigationNotFound
	}
	if err != nil {
		return nil, err
	}
	return m, nil
}

// GetMitigationByDecisionID retrieves a mitigation record by its deterministic DecisionID.
func (s *SQLiteStore) GetMitigationByDecisionID(ctx context.Context, decisionID string) (*StoredMitigation, error) {
	if strings.TrimSpace(decisionID) == "" {
		return nil, errors.New("decision_id cannot be empty")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrStoreClosed
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	query := `
	SELECT action_id, decision_id, incident_id, node_id, action_type,
	       target, status, mode, message, error_code, parameters,
	       started_at, completed_at, duration_ms, simulated, created_at, updated_at
	FROM mitigation_records
	WHERE decision_id = ?
	LIMIT 1;
	`
	row := s.db.QueryRowContext(ctx, query, decisionID)
	m, err := scanMitigation(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrMitigationNotFound
	}
	if err != nil {
		return nil, err
	}
	return m, nil
}

// ListMitigations retrieves mitigation records matching an optional incidentID (or all if empty),
// ordered by started_at ASC, bounded by limit.
func (s *SQLiteStore) ListMitigations(ctx context.Context, incidentID string, limit int) ([]*StoredMitigation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrStoreClosed
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if limit <= 0 {
		limit = 100
	}

	var (
		rows *sql.Rows
		err  error
	)
	if incidentID != "" {
		query := `
		SELECT action_id, decision_id, incident_id, node_id, action_type,
		       target, status, mode, message, error_code, parameters,
		       started_at, completed_at, duration_ms, simulated, created_at, updated_at
		FROM mitigation_records
		WHERE incident_id = ?
		ORDER BY started_at ASC
		LIMIT ?;
		`
		rows, err = s.db.QueryContext(ctx, query, incidentID, limit)
	} else {
		query := `
		SELECT action_id, decision_id, incident_id, node_id, action_type,
		       target, status, mode, message, error_code, parameters,
		       started_at, completed_at, duration_ms, simulated, created_at, updated_at
		FROM mitigation_records
		ORDER BY started_at ASC
		LIMIT ?;
		`
		rows, err = s.db.QueryContext(ctx, query, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*StoredMitigation
	for rows.Next() {
		m, err := scanMitigation(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, m)
	}
	return result, rows.Err()
}

// UpdateMitigationStatus updates the status, completion details, and updated_at of an existing mitigation.
// Returns ErrInvalidMitigationTransition if the transition violates state machine rules.
func (s *SQLiteStore) UpdateMitigationStatus(ctx context.Context, actionID string, status types.MitigationStatus, message, errCode string, completedAt *time.Time, durationMs int64) error {
	if strings.TrimSpace(actionID) == "" {
		return errors.New("action_id cannot be empty")
	}
	if !status.IsValid() {
		return fmt.Errorf("%w: invalid mitigation status %q", ErrInvalidMitigation, status)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin update tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var currentStatusStr string
	queryStatus := `SELECT status FROM mitigation_records WHERE action_id = ?;`
	err = tx.QueryRowContext(ctx, queryStatus, actionID).Scan(&currentStatusStr)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrMitigationNotFound
	}
	if err != nil {
		return fmt.Errorf("failed to query current status: %w", err)
	}

	currentStatus := types.MitigationStatus(currentStatusStr)
	if !CanMitigationTransition(currentStatus, status) {
		return fmt.Errorf("%w: cannot transition from %s to %s", ErrInvalidMitigationTransition, currentStatus, status)
	}

	nowStr := time.Now().UTC().Format(time.RFC3339Nano)
	var completedStr sql.NullString
	if completedAt != nil {
		completedStr = sql.NullString{
			String: completedAt.UTC().Format(time.RFC3339Nano),
			Valid:  true,
		}
	}
	var errCodeStr sql.NullString
	if errCode != "" {
		errCodeStr = sql.NullString{String: errCode, Valid: true}
	}

	updateSQL := `
	UPDATE mitigation_records SET
		status = ?,
		message = ?,
		error_code = ?,
		completed_at = COALESCE(?, completed_at),
		duration_ms = CASE WHEN ? > 0 THEN ? ELSE duration_ms END,
		updated_at = ?
	WHERE action_id = ?;
	`
	_, err = tx.ExecContext(ctx, updateSQL,
		string(status),
		message,
		errCodeStr,
		completedStr,
		durationMs,
		durationMs,
		nowStr,
		actionID,
	)
	if err != nil {
		return fmt.Errorf("failed to update mitigation record: %w", err)
	}

	return tx.Commit()
}

// MarkMitigationExecuting updates an existing mitigation to EXECUTING status.
func (s *SQLiteStore) MarkMitigationExecuting(ctx context.Context, actionID string, startedAt time.Time) error {
	return s.UpdateMitigationStatus(ctx, actionID, types.MitigationStatusExecuting, "mitigation execution started", "", nil, 0)
}

// MarkMitigationExecuted updates an existing mitigation to EXECUTED with completion details.
func (s *SQLiteStore) MarkMitigationExecuted(ctx context.Context, actionID string, message string, completedAt time.Time, durationMs int64) error {
	return s.UpdateMitigationStatus(ctx, actionID, types.MitigationStatusExecuted, message, "", &completedAt, durationMs)
}

// MarkMitigationFailed updates an existing mitigation to FAILED with error details.
func (s *SQLiteStore) MarkMitigationFailed(ctx context.Context, actionID string, message, errCode string, completedAt time.Time, durationMs int64) error {
	return s.UpdateMitigationStatus(ctx, actionID, types.MitigationStatusFailed, message, errCode, &completedAt, durationMs)
}

// MarkMitigationUnknown updates an existing mitigation to UNKNOWN_RECONCILIATION_REQUIRED.
func (s *SQLiteStore) MarkMitigationUnknown(ctx context.Context, actionID string, message, errCode string, updatedAt time.Time) error {
	return s.UpdateMitigationStatus(ctx, actionID, types.MitigationStatusUnknownReconciliationRequired, message, errCode, nil, 0)
}

// RecoverInFlightMitigations transitions any mitigation stranded in EXECUTING status to
// UNKNOWN_RECONCILIATION_REQUIRED upon startup/crash recovery.
// Returns the number of transitioned records.
func (s *SQLiteStore) RecoverInFlightMitigations(ctx context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrStoreClosed
	}

	if err := ctx.Err(); err != nil {
		return 0, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin recovery tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	nowStr := time.Now().UTC().Format(time.RFC3339Nano)
	recoveryMsg := " [RECOVERED ON RESTART: unconfirmed in-flight execution transitioned to UNKNOWN_RECONCILIATION_REQUIRED]"
	recoverSQL := `
	UPDATE mitigation_records SET
		status = 'UNKNOWN_RECONCILIATION_REQUIRED',
		message = message || ?,
		error_code = 'RESTART_RECONCILIATION_REQUIRED',
		updated_at = ?
	WHERE status = 'EXECUTING';
	`
	res, err := tx.ExecContext(ctx, recoverSQL, recoveryMsg, nowStr)
	if err != nil {
		return 0, fmt.Errorf("failed to execute recovery update: %w", err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit recovery tx: %w", err)
	}

	return affected, nil
}

// CountMitigations returns the total count of mitigation records stored.
func (s *SQLiteStore) CountMitigations(ctx context.Context) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return 0, ErrStoreClosed
	}

	if err := ctx.Err(); err != nil {
		return 0, err
	}

	var count int64
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM mitigation_records;").Scan(&count)
	return count, err
}

func scanMitigation(scanner rowScanner) (*StoredMitigation, error) {
	var (
		m            StoredMitigation
		actionType   string
		statusStr    string
		errCode      sql.NullString
		paramsJSON   sql.NullString
		startedStr   string
		completedStr sql.NullString
		simulatedInt int
		createdStr   string
		updatedStr   string
	)

	err := scanner.Scan(
		&m.ActionID,
		&m.DecisionID,
		&m.IncidentID,
		&m.NodeID,
		&actionType,
		&m.Target,
		&statusStr,
		&m.Mode,
		&m.Message,
		&errCode,
		&paramsJSON,
		&startedStr,
		&completedStr,
		&m.DurationMs,
		&simulatedInt,
		&createdStr,
		&updatedStr,
	)
	if err != nil {
		return nil, err
	}

	m.ActionType = types.MitigationActionType(actionType)
	m.Status = types.MitigationStatus(statusStr)
	if errCode.Valid {
		m.ErrorCode = errCode.String
	}
	if paramsJSON.Valid && paramsJSON.String != "" {
		_ = json.Unmarshal([]byte(paramsJSON.String), &m.Parameters)
	}
	m.StartedAt, _ = parseDBTime(startedStr)
	if completedStr.Valid && completedStr.String != "" {
		t, err := parseDBTime(completedStr.String)
		if err == nil {
			m.CompletedAt = &t
		}
	}
	m.Simulated = (simulatedInt == 1)
	m.CreatedAt, _ = parseDBTime(createdStr)
	m.UpdatedAt, _ = parseDBTime(updatedStr)

	return &m, nil
}

// ============================================================================
// ApprovalStore Implementation (Phase 6.5)
// ============================================================================

// RecordApproval transactionally validates and persists an approval record in PENDING status.
// Returns ErrDuplicateApproval if an approval with the same approval_id or decision_id already exists.
func (s *SQLiteStore) RecordApproval(ctx context.Context, app *StoredApproval) error {
	if app == nil {
		return ErrInvalidApproval
	}
	if err := app.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin tx for recording approval: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	query := `
	INSERT INTO approval_records (
		approval_id, decision_id, incident_id, action_id, node_id,
		action_type, target, policy_version, decision_fingerprint,
		status, requested_at, expires_at, requested_by,
		approved_by, rejected_by, reason, created_at, updated_at, consumed_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
	`

	var consumedStr sql.NullString
	if app.ConsumedAt != nil {
		consumedStr = sql.NullString{String: formatDBTime(*app.ConsumedAt), Valid: true}
	}

	_, err = tx.ExecContext(ctx, query,
		app.ApprovalID,
		app.DecisionID,
		app.IncidentID,
		app.ActionID,
		app.NodeID,
		string(app.ActionType),
		app.Target,
		app.PolicyVersion,
		app.DecisionFingerprint,
		string(app.Status),
		formatDBTime(app.RequestedAt),
		formatDBTime(app.ExpiresAt),
		app.RequestedBy,
		app.ApprovedBy,
		app.RejectedBy,
		app.Reason,
		formatDBTime(app.CreatedAt),
		formatDBTime(app.UpdatedAt),
		consumedStr,
	)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return fmt.Errorf("%w: %v", ErrDuplicateApproval, err)
		}
		return fmt.Errorf("failed to insert approval record: %w", err)
	}

	return tx.Commit()
}

// GetApproval retrieves an approval record by its unique ApprovalID.
func (s *SQLiteStore) GetApproval(ctx context.Context, approvalID string) (*StoredApproval, error) {
	if strings.TrimSpace(approvalID) == "" {
		return nil, errors.New("approval_id cannot be empty")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrStoreClosed
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	query := `
	SELECT approval_id, decision_id, incident_id, action_id, node_id,
	       action_type, target, policy_version, decision_fingerprint,
	       status, requested_at, expires_at, requested_by,
	       approved_by, rejected_by, reason, created_at, updated_at, consumed_at
	FROM approval_records
	WHERE approval_id = ?
	LIMIT 1;
	`
	row := s.db.QueryRowContext(ctx, query, approvalID)
	app, err := scanApproval(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrApprovalNotFound
	}
	if err != nil {
		return nil, err
	}
	return app, nil
}

// GetApprovalByDecisionID retrieves an approval record by its unique DecisionID.
func (s *SQLiteStore) GetApprovalByDecisionID(ctx context.Context, decisionID string) (*StoredApproval, error) {
	if strings.TrimSpace(decisionID) == "" {
		return nil, errors.New("decision_id cannot be empty")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrStoreClosed
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	query := `
	SELECT approval_id, decision_id, incident_id, action_id, node_id,
	       action_type, target, policy_version, decision_fingerprint,
	       status, requested_at, expires_at, requested_by,
	       approved_by, rejected_by, reason, created_at, updated_at, consumed_at
	FROM approval_records
	WHERE decision_id = ?
	LIMIT 1;
	`
	row := s.db.QueryRowContext(ctx, query, decisionID)
	app, err := scanApproval(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrApprovalNotFound
	}
	if err != nil {
		return nil, err
	}
	return app, nil
}

// ApproveApproval transitions a PENDING approval to APPROVED.
// Idempotent: if already APPROVED with matching approvedBy, returns nil.
func (s *SQLiteStore) ApproveApproval(ctx context.Context, approvalID, approvedBy string, now time.Time) error {
	if strings.TrimSpace(approvalID) == "" {
		return errors.New("approval_id cannot be empty")
	}
	if strings.TrimSpace(approvedBy) == "" {
		return errors.New("approved_by cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var (
		statusStr      string
		expiresStr     string
		currApprovedBy string
	)
	query := `SELECT status, expires_at, approved_by FROM approval_records WHERE approval_id = ?;`
	err = tx.QueryRowContext(ctx, query, approvalID).Scan(&statusStr, &expiresStr, &currApprovedBy)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrApprovalNotFound
	}
	if err != nil {
		return err
	}

	currStatus := types.ApprovalStatus(statusStr)
	expTime, err := parseDBTime(expiresStr)
	if err != nil {
		return fmt.Errorf("failed to parse expires_at: %w", err)
	}

	// Idempotent check
	if currStatus == types.ApprovalStatusApproved {
		return nil
	}

	// Check terminal states
	if currStatus.IsTerminal() {
		return fmt.Errorf("%w: approval %s is in terminal state %s", ErrInvalidApprovalTransition, approvalID, currStatus)
	}

	// Check expiration
	if now.After(expTime) || now.Equal(expTime) {
		_, _ = tx.ExecContext(ctx, `UPDATE approval_records SET status = 'EXPIRED', updated_at = ? WHERE approval_id = ?;`, formatDBTime(now), approvalID)
		_ = tx.Commit()
		return ErrApprovalExpired
	}

	// Transition PENDING -> APPROVED
	updateQuery := `
	UPDATE approval_records
	SET status = 'APPROVED', approved_by = ?, updated_at = ?
	WHERE approval_id = ? AND status = 'PENDING';
	`
	res, err := tx.ExecContext(ctx, updateQuery, approvedBy, formatDBTime(now), approvalID)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("%w: failed to transition to APPROVED", ErrInvalidApprovalTransition)
	}

	return tx.Commit()
}

// RejectApproval transitions a PENDING approval to REJECTED.
func (s *SQLiteStore) RejectApproval(ctx context.Context, approvalID, rejectedBy, reason string, now time.Time) error {
	if strings.TrimSpace(approvalID) == "" {
		return errors.New("approval_id cannot be empty")
	}
	if strings.TrimSpace(rejectedBy) == "" {
		return errors.New("rejected_by cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var statusStr string
	query := `SELECT status FROM approval_records WHERE approval_id = ?;`
	err = tx.QueryRowContext(ctx, query, approvalID).Scan(&statusStr)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrApprovalNotFound
	}
	if err != nil {
		return err
	}

	currStatus := types.ApprovalStatus(statusStr)
	if currStatus == types.ApprovalStatusRejected {
		return nil // Idempotent
	}
	if currStatus == types.ApprovalStatusApproved {
		return ErrCannotRejectApproved
	}
	if currStatus.IsTerminal() {
		return fmt.Errorf("%w: cannot reject approval in status %s", ErrInvalidApprovalTransition, currStatus)
	}

	updateQuery := `
	UPDATE approval_records
	SET status = 'REJECTED', rejected_by = ?, reason = ?, updated_at = ?
	WHERE approval_id = ? AND status = 'PENDING';
	`
	res, err := tx.ExecContext(ctx, updateQuery, rejectedBy, reason, formatDBTime(now), approvalID)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("%w: failed to transition to REJECTED", ErrInvalidApprovalTransition)
	}

	return tx.Commit()
}

// CancelApproval transitions a PENDING approval to CANCELLED.
func (s *SQLiteStore) CancelApproval(ctx context.Context, approvalID, reason string, now time.Time) error {
	if strings.TrimSpace(approvalID) == "" {
		return errors.New("approval_id cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var statusStr string
	query := `SELECT status FROM approval_records WHERE approval_id = ?;`
	err = tx.QueryRowContext(ctx, query, approvalID).Scan(&statusStr)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrApprovalNotFound
	}
	if err != nil {
		return err
	}

	currStatus := types.ApprovalStatus(statusStr)
	if currStatus == types.ApprovalStatusCancelled {
		return nil // Idempotent
	}
	if currStatus == types.ApprovalStatusConsumed {
		return fmt.Errorf("%w: cannot cancel already consumed approval", ErrInvalidApprovalTransition)
	}
	if currStatus.IsTerminal() {
		return fmt.Errorf("%w: cannot cancel approval in status %s", ErrInvalidApprovalTransition, currStatus)
	}

	updateQuery := `
	UPDATE approval_records
	SET status = 'CANCELLED', reason = ?, updated_at = ?
	WHERE approval_id = ? AND status IN ('PENDING', 'APPROVED');
	`
	res, err := tx.ExecContext(ctx, updateQuery, reason, formatDBTime(now), approvalID)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("%w: failed to transition to CANCELLED", ErrInvalidApprovalTransition)
	}

	return tx.Commit()
}

// ConsumeApproval atomically transitions an APPROVED approval to CONSUMED.
// Fails if the approval is expired, not approved, or already consumed.
func (s *SQLiteStore) ConsumeApproval(ctx context.Context, approvalID string, now time.Time) error {
	if strings.TrimSpace(approvalID) == "" {
		return errors.New("approval_id cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	// Atomic update: only succeeds if status == 'APPROVED' and expires_at > now
	nowStr := formatDBTime(now)
	updateQuery := `
	UPDATE approval_records
	SET status = 'CONSUMED', consumed_at = ?, updated_at = ?
	WHERE approval_id = ? AND status = 'APPROVED' AND expires_at > ?;
	`
	res, err := tx.ExecContext(ctx, updateQuery, nowStr, nowStr, approvalID, nowStr)
	if err != nil {
		return fmt.Errorf("failed to execute consumption update: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 1 {
		return tx.Commit()
	}

	// RowsAffected == 0: diagnose the specific failure cause
	var (
		statusStr  string
		expiresStr string
	)
	diagQuery := `SELECT status, expires_at FROM approval_records WHERE approval_id = ?;`
	err = tx.QueryRowContext(ctx, diagQuery, approvalID).Scan(&statusStr, &expiresStr)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrApprovalNotFound
	}
	if err != nil {
		return err
	}

	currStatus := types.ApprovalStatus(statusStr)
	if currStatus == types.ApprovalStatusConsumed {
		return ErrApprovalAlreadyConsumed
	}

	expTime, err := parseDBTime(expiresStr)
	if err == nil && (now.After(expTime) || now.Equal(expTime)) {
		_, _ = tx.ExecContext(ctx, `UPDATE approval_records SET status = 'EXPIRED', updated_at = ? WHERE approval_id = ?;`, nowStr, approvalID)
		_ = tx.Commit()
		return ErrApprovalExpired
	}

	return fmt.Errorf("%w: status %s cannot be consumed", ErrInvalidApprovalTransition, currStatus)
}

// ExpireStaleApprovals transitions any unconsumed approvals past their expiration time to EXPIRED.
func (s *SQLiteStore) ExpireStaleApprovals(ctx context.Context, now time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrStoreClosed
	}

	if err := ctx.Err(); err != nil {
		return 0, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	nowStr := formatDBTime(now)
	query := `
	UPDATE approval_records
	SET status = 'EXPIRED', updated_at = ?
	WHERE status IN ('PENDING', 'APPROVED') AND expires_at <= ?;
	`
	res, err := tx.ExecContext(ctx, query, nowStr, nowStr)
	if err != nil {
		return 0, fmt.Errorf("failed to expire stale approvals: %w", err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return affected, nil
}

// ListApprovals retrieves approval records matching an optional incidentID (or all if empty),
// ordered by requested_at ASC, bounded by limit.
func (s *SQLiteStore) ListApprovals(ctx context.Context, incidentID string, limit int) ([]*StoredApproval, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrStoreClosed
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if limit <= 0 {
		limit = 100
	}

	var (
		rows *sql.Rows
		err  error
	)
	if incidentID != "" {
		query := `
		SELECT approval_id, decision_id, incident_id, action_id, node_id,
		       action_type, target, policy_version, decision_fingerprint,
		       status, requested_at, expires_at, requested_by,
		       approved_by, rejected_by, reason, created_at, updated_at, consumed_at
		FROM approval_records
		WHERE incident_id = ?
		ORDER BY requested_at ASC
		LIMIT ?;
		`
		rows, err = s.db.QueryContext(ctx, query, incidentID, limit)
	} else {
		query := `
		SELECT approval_id, decision_id, incident_id, action_id, node_id,
		       action_type, target, policy_version, decision_fingerprint,
		       status, requested_at, expires_at, requested_by,
		       approved_by, rejected_by, reason, created_at, updated_at, consumed_at
		FROM approval_records
		ORDER BY requested_at ASC
		LIMIT ?;
		`
		rows, err = s.db.QueryContext(ctx, query, limit)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query approval records: %w", err)
	}
	defer rows.Close()

	var result []*StoredApproval
	for rows.Next() {
		app, err := scanApproval(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan approval record: %w", err)
		}
		result = append(result, app)
	}
	return result, rows.Err()
}

// CountApprovals returns the total count of approval records stored.
func (s *SQLiteStore) CountApprovals(ctx context.Context) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return 0, ErrStoreClosed
	}

	query := `SELECT COUNT(*) FROM approval_records;`
	var count int64
	err := s.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count approvals: %w", err)
	}

	return count, nil
}

func scanApproval(s scanner) (*StoredApproval, error) {
	var (
		app          StoredApproval
		actionType   string
		statusStr    string
		requestedStr string
		expiresStr   string
		createdStr   string
		updatedStr   string
		consumedStr  sql.NullString
	)

	err := s.Scan(
		&app.ApprovalID,
		&app.DecisionID,
		&app.IncidentID,
		&app.ActionID,
		&app.NodeID,
		&actionType,
		&app.Target,
		&app.PolicyVersion,
		&app.DecisionFingerprint,
		&statusStr,
		&requestedStr,
		&expiresStr,
		&app.RequestedBy,
		&app.ApprovedBy,
		&app.RejectedBy,
		&app.Reason,
		&createdStr,
		&updatedStr,
		&consumedStr,
	)
	if err != nil {
		return nil, err
	}

	app.ActionType = types.MitigationActionType(actionType)
	app.Status = types.ApprovalStatus(statusStr)
	app.RequestedAt, _ = parseDBTime(requestedStr)
	app.ExpiresAt, _ = parseDBTime(expiresStr)
	app.CreatedAt, _ = parseDBTime(createdStr)
	app.UpdatedAt, _ = parseDBTime(updatedStr)
	if consumedStr.Valid && consumedStr.String != "" {
		t, err := parseDBTime(consumedStr.String)
		if err == nil {
			app.ConsumedAt = &t
		}
	}

	return &app, nil
}

// ============================================================================
// VerificationStore Implementation (Phase 6.6)
// ============================================================================

// RecordVerification transactionally validates and persists a verification record in PENDING status.
func (s *SQLiteStore) RecordVerification(ctx context.Context, v *StoredVerification) error {
	if err := v.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	evidenceJSON := "{}"
	if len(v.Evidence) > 0 {
		b, err := json.Marshal(v.Evidence)
		if err == nil {
			evidenceJSON = string(b)
		}
	}

	query := `
	INSERT INTO verification_records (
		verification_id, incident_id, action_id, decision_id, node_id,
		metric_name, condition_type, recovery_threshold, comparator, status,
		required_observations, consecutive_healthy, total_observations,
		started_at, expires_at, completed_at, recovered_at,
		last_observation_at, last_observed_value, reason, evidence,
		created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
	`

	_, err := s.db.ExecContext(ctx, query,
		v.VerificationID,
		v.IncidentID,
		v.ActionID,
		v.DecisionID,
		v.NodeID,
		v.MetricName,
		v.ConditionType,
		v.RecoveryThreshold,
		v.Comparator,
		string(v.Status),
		v.RequiredObservations,
		v.ConsecutiveHealthy,
		v.TotalObservations,
		formatDBTime(v.StartedAt),
		formatDBTime(v.ExpiresAt),
		nullStringFromTimePtr(v.CompletedAt),
		nullStringFromTimePtr(v.RecoveredAt),
		nullStringFromTimePtr(v.LastObservationAt),
		v.LastObservedValue,
		v.Reason,
		evidenceJSON,
		formatDBTime(v.CreatedAt),
		formatDBTime(v.UpdatedAt),
	)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return ErrDuplicateVerification
		}
		return fmt.Errorf("failed to record verification: %w", err)
	}

	return nil
}

// GetVerification retrieves a verification record by its unique VerificationID.
func (s *SQLiteStore) GetVerification(ctx context.Context, verificationID string) (*StoredVerification, error) {
	if strings.TrimSpace(verificationID) == "" {
		return nil, errors.New("verification_id cannot be empty")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrStoreClosed
	}

	query := `
	SELECT verification_id, incident_id, action_id, decision_id, node_id,
	       metric_name, condition_type, recovery_threshold, comparator, status,
	       required_observations, consecutive_healthy, total_observations,
	       started_at, expires_at, completed_at, recovered_at,
	       last_observation_at, last_observed_value, reason, evidence,
	       created_at, updated_at
	FROM verification_records
	WHERE verification_id = ?;
	`
	row := s.db.QueryRowContext(ctx, query, verificationID)
	v, err := scanVerification(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrVerificationNotFound
	}
	if err != nil {
		return nil, err
	}
	return v, nil
}

// GetVerificationByActionID retrieves a verification record by its ActionID.
func (s *SQLiteStore) GetVerificationByActionID(ctx context.Context, actionID string) (*StoredVerification, error) {
	if strings.TrimSpace(actionID) == "" {
		return nil, errors.New("action_id cannot be empty")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrStoreClosed
	}

	query := `
	SELECT verification_id, incident_id, action_id, decision_id, node_id,
	       metric_name, condition_type, recovery_threshold, comparator, status,
	       required_observations, consecutive_healthy, total_observations,
	       started_at, expires_at, completed_at, recovered_at,
	       last_observation_at, last_observed_value, reason, evidence,
	       created_at, updated_at
	FROM verification_records
	WHERE action_id = ?
	ORDER BY started_at DESC LIMIT 1;
	`
	row := s.db.QueryRowContext(ctx, query, actionID)
	v, err := scanVerification(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrVerificationNotFound
	}
	if err != nil {
		return nil, err
	}
	return v, nil
}

// GetActiveVerification retrieves the active (PENDING) verification for a node and metric stream, or nil if none.
func (s *SQLiteStore) GetActiveVerification(ctx context.Context, nodeID, metricName string) (*StoredVerification, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrStoreClosed
	}

	query := `
	SELECT verification_id, incident_id, action_id, decision_id, node_id,
	       metric_name, condition_type, recovery_threshold, comparator, status,
	       required_observations, consecutive_healthy, total_observations,
	       started_at, expires_at, completed_at, recovered_at,
	       last_observation_at, last_observed_value, reason, evidence,
	       created_at, updated_at
	FROM verification_records
	WHERE node_id = ? AND metric_name = ? AND status = 'PENDING'
	ORDER BY started_at DESC LIMIT 1;
	`
	row := s.db.QueryRowContext(ctx, query, nodeID, metricName)
	v, err := scanVerification(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return v, nil
}

// UpdateVerificationProgress updates observations, consecutive healthy count, status, timestamps, and reason.
func (s *SQLiteStore) UpdateVerificationProgress(ctx context.Context, verificationID string, status types.VerificationStatus, consecutiveHealthy, totalObservations int, lastObservedValue float64, lastObsAt, completedAt, recoveredAt *time.Time, reason string, evidence map[string]string, now time.Time) error {
	if strings.TrimSpace(verificationID) == "" {
		return errors.New("verification_id cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var currStatusStr string
	queryCurr := `SELECT status FROM verification_records WHERE verification_id = ?;`
	err = tx.QueryRowContext(ctx, queryCurr, verificationID).Scan(&currStatusStr)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrVerificationNotFound
	}
	if err != nil {
		return err
	}

	currStatus := types.VerificationStatus(currStatusStr)
	if currStatus.IsTerminal() && currStatus != status {
		return ErrVerificationTerminal
	}
	if !currStatus.CanTransitionTo(status) {
		return fmt.Errorf("%w: cannot transition from %s to %s", ErrInvalidVerificationTransition, currStatus, status)
	}

	evidenceJSON := "{}"
	if len(evidence) > 0 {
		b, err := json.Marshal(evidence)
		if err == nil {
			evidenceJSON = string(b)
		}
	}

	var lastObsValPtr *float64
	if !math.IsNaN(lastObservedValue) && !math.IsInf(lastObservedValue, 0) {
		lastObsValPtr = &lastObservedValue
	}

	updateQuery := `
	UPDATE verification_records
	SET status = ?, consecutive_healthy = ?, total_observations = ?,
	    last_observed_value = ?, last_observation_at = ?,
	    completed_at = ?, recovered_at = ?, reason = ?, evidence = ?, updated_at = ?
	WHERE verification_id = ?;
	`
	_, err = tx.ExecContext(ctx, updateQuery,
		string(status),
		consecutiveHealthy,
		totalObservations,
		lastObsValPtr,
		nullStringFromTimePtr(lastObsAt),
		nullStringFromTimePtr(completedAt),
		nullStringFromTimePtr(recoveredAt),
		reason,
		evidenceJSON,
		formatDBTime(now),
		verificationID,
	)
	if err != nil {
		return fmt.Errorf("failed to update verification progress: %w", err)
	}

	return tx.Commit()
}

// ExpireStaleVerifications transitions any PENDING verifications past their expiration time to TIMED_OUT.
func (s *SQLiteStore) ExpireStaleVerifications(ctx context.Context, now time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrStoreClosed
	}

	query := `
	UPDATE verification_records
	SET status = 'TIMED_OUT', reason = 'verification deadline elapsed without recovery confirmation', updated_at = ?
	WHERE status = 'PENDING' AND expires_at <= ?;
	`
	res, err := s.db.ExecContext(ctx, query, formatDBTime(now), formatDBTime(now))
	if err != nil {
		return 0, fmt.Errorf("failed to expire stale verifications: %w", err)
	}
	return res.RowsAffected()
}

// RecoverInFlightVerifications checks PENDING verifications upon process startup/reboot.
// Records whose expiration deadline has passed are transitioned to TIMED_OUT.
// Records whose expiration deadline is in the future remain PENDING.
func (s *SQLiteStore) RecoverInFlightVerifications(ctx context.Context, now time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrStoreClosed
	}

	query := `
	UPDATE verification_records
	SET status = 'TIMED_OUT',
	    reason = 'verification expired during agent restart or offline window',
	    updated_at = ?
	WHERE status = 'PENDING' AND expires_at <= ?;
	`
	res, err := s.db.ExecContext(ctx, query, formatDBTime(now), formatDBTime(now))
	if err != nil {
		return 0, fmt.Errorf("failed to recover in-flight verifications: %w", err)
	}
	return res.RowsAffected()
}

// ListVerifications retrieves verification records matching an optional incidentID (or all if empty),
// ordered by started_at ASC, bounded by limit.
func (s *SQLiteStore) ListVerifications(ctx context.Context, incidentID string, limit int) ([]*StoredVerification, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrStoreClosed
	}

	if limit <= 0 {
		limit = 100
	}

	var rows *sql.Rows
	var err error
	if incidentID != "" {
		query := `
		SELECT verification_id, incident_id, action_id, decision_id, node_id,
		       metric_name, condition_type, recovery_threshold, comparator, status,
		       required_observations, consecutive_healthy, total_observations,
		       started_at, expires_at, completed_at, recovered_at,
		       last_observation_at, last_observed_value, reason, evidence,
		       created_at, updated_at
		FROM verification_records
		WHERE incident_id = ?
		ORDER BY started_at ASC
		LIMIT ?;
		`
		rows, err = s.db.QueryContext(ctx, query, incidentID, limit)
	} else {
		query := `
		SELECT verification_id, incident_id, action_id, decision_id, node_id,
		       metric_name, condition_type, recovery_threshold, comparator, status,
		       required_observations, consecutive_healthy, total_observations,
		       started_at, expires_at, completed_at, recovered_at,
		       last_observation_at, last_observed_value, reason, evidence,
		       created_at, updated_at
		FROM verification_records
		ORDER BY started_at ASC
		LIMIT ?;
		`
		rows, err = s.db.QueryContext(ctx, query, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*StoredVerification
	for rows.Next() {
		v, err := scanVerification(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, v)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

// CountVerifications returns the total count of verification records stored.
func (s *SQLiteStore) CountVerifications(ctx context.Context) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return 0, ErrStoreClosed
	}

	query := `SELECT COUNT(*) FROM verification_records;`
	var count int64
	err := s.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count verifications: %w", err)
	}
	return count, nil
}

func scanVerification(s scanner) (*StoredVerification, error) {
	var (
		v            StoredVerification
		statusStr    string
		startedStr   string
		expiresStr   string
		completedStr sql.NullString
		recoveredStr sql.NullString
		lastObsAtStr sql.NullString
		lastObsVal   sql.NullFloat64
		evidenceStr  string
		createdStr   string
		updatedStr   string
	)

	err := s.Scan(
		&v.VerificationID,
		&v.IncidentID,
		&v.ActionID,
		&v.DecisionID,
		&v.NodeID,
		&v.MetricName,
		&v.ConditionType,
		&v.RecoveryThreshold,
		&v.Comparator,
		&statusStr,
		&v.RequiredObservations,
		&v.ConsecutiveHealthy,
		&v.TotalObservations,
		&startedStr,
		&expiresStr,
		&completedStr,
		&recoveredStr,
		&lastObsAtStr,
		&lastObsVal,
		&v.Reason,
		&evidenceStr,
		&createdStr,
		&updatedStr,
	)
	if err != nil {
		return nil, err
	}

	v.Status = types.VerificationStatus(statusStr)
	v.StartedAt, _ = parseDBTime(startedStr)
	v.ExpiresAt, _ = parseDBTime(expiresStr)
	v.CreatedAt, _ = parseDBTime(createdStr)
	v.UpdatedAt, _ = parseDBTime(updatedStr)
	if completedStr.Valid && completedStr.String != "" {
		t, err := parseDBTime(completedStr.String)
		if err == nil {
			v.CompletedAt = &t
		}
	}
	if recoveredStr.Valid && recoveredStr.String != "" {
		t, err := parseDBTime(recoveredStr.String)
		if err == nil {
			v.RecoveredAt = &t
		}
	}
	if lastObsAtStr.Valid && lastObsAtStr.String != "" {
		t, err := parseDBTime(lastObsAtStr.String)
		if err == nil {
			v.LastObservationAt = &t
		}
	}
	if lastObsVal.Valid {
		val := lastObsVal.Float64
		v.LastObservedValue = &val
	}
	if evidenceStr != "" && evidenceStr != "{}" {
		_ = json.Unmarshal([]byte(evidenceStr), &v.Evidence)
	}

	return &v, nil
}

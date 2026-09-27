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

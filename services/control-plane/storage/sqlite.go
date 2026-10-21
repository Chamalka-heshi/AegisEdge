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

	// HookAfterPayloadInsert is an optional test hook called inside RecordBatch
	// after payload is inserted into cp_telemetry_payloads but before node activity is updated.
	// Returning an error aborts the transaction and rolls back, simulating node activity update failure.
	HookAfterPayloadInsert func() error
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

	CREATE TABLE IF NOT EXISTS cp_nodes (
		node_id           TEXT PRIMARY KEY NOT NULL,
		status            TEXT NOT NULL,
		last_seen         TEXT NOT NULL,
		registered_at     TEXT NOT NULL,
		agent_version     TEXT NOT NULL DEFAULT '',
		protocol_version  TEXT NOT NULL DEFAULT '',
		hostname          TEXT NOT NULL DEFAULT '',
		os                TEXT NOT NULL DEFAULT '',
		architecture      TEXT NOT NULL DEFAULT '',
		capabilities_json TEXT NOT NULL DEFAULT '[]',
		labels_json       TEXT NOT NULL DEFAULT '{}',
		sequence_number   INTEGER NOT NULL DEFAULT 0,
		created_at        TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at        TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_cp_nodes_reg_order
		ON cp_nodes(registered_at ASC, node_id ASC);

	CREATE INDEX IF NOT EXISTS idx_cp_nodes_last_seen
		ON cp_nodes(last_seen ASC);
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

	// Optional test hook for simulating failure after payload insert but before node activity update
	if s.HookAfterPayloadInsert != nil {
		if err := s.HookAfterPayloadInsert(); err != nil {
			_ = tx.Rollback()
			return false, fmt.Errorf("injected test failure after payload insert: %w", err)
		}
	}

	// Step 4: Atomically update registered-node activity within the same transaction.
	// If the node is not registered, the WHERE clause matches 0 rows and leaves cp_nodes untouched (no synthetic node created).
	queryNodeActivity := `
	UPDATE cp_nodes
	SET last_seen = ?,
	    sequence_number = CASE WHEN ? > sequence_number THEN ? ELSE sequence_number END,
	    updated_at = ?
	WHERE node_id = ?;
	`
	_, err = tx.ExecContext(ctx, queryNodeActivity,
		ingestedAtStr,
		batch.SequenceNumber,
		batch.SequenceNumber,
		ingestedAtStr,
		batch.NodeID,
	)
	if err != nil {
		return false, fmt.Errorf("failed to update node activity in batch transaction: %w", err)
	}

	// Step 5: Commit batch metadata, payload, and node activity atomically in a single fsync
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

// parseStoredTimestamp parses an RFC3339Nano or RFC3339 formatted timestamp string.
func parseStoredTimestamp(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err == nil {
		return t.UTC(), nil
	}
	t2, err2 := time.Parse(time.RFC3339, s)
	if err2 == nil {
		return t2.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("failed to parse timestamp %q: %w", s, err)
}

// RegisterNode idempotently enrolls a new node or updates mutable metadata on re-registration.
// Returns (true, nil) if newly enrolled.
// Returns (false, nil) if duplicate registration (with existing RegisteredAt and SequenceNumber preserved).
func (s *SQLiteStore) RegisterNode(ctx context.Context, record *NodeRecord) (bool, error) {
	if record == nil {
		return false, fmt.Errorf("%w: record cannot be nil", ErrInvalidMeta)
	}
	if strings.TrimSpace(record.NodeID) == "" {
		return false, fmt.Errorf("%w: node_id cannot be empty", ErrInvalidMeta)
	}
	if record.RegisteredAt.IsZero() {
		return false, fmt.Errorf("%w: registered_at cannot be zero", ErrInvalidMeta)
	}
	if record.LastSeen.IsZero() {
		record.LastSeen = record.RegisteredAt
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
	defer tx.Rollback()

	// Check if node already exists
	queryCheck := `
	SELECT status, registered_at, sequence_number, hostname, agent_version, protocol_version, os, architecture, capabilities_json, labels_json
	FROM cp_nodes WHERE node_id = ?;
	`
	var (
		existStatus, existRegAtStr, existHost, existAgentVer, existProtoVer, existOS, existArch, existCapsJSON, existLabelsJSON string
		existSeq                                                                                                                int64
	)
	err = tx.QueryRowContext(ctx, queryCheck, record.NodeID).Scan(
		&existStatus, &existRegAtStr, &existSeq,
		&existHost, &existAgentVer, &existProtoVer, &existOS, &existArch,
		&existCapsJSON, &existLabelsJSON,
	)

	if errors.Is(err, sql.ErrNoRows) {
		// New node registration
		capsJSON := "[]"
		if len(record.Capabilities) > 0 {
			b, err := json.Marshal(record.Capabilities)
			if err != nil {
				return false, fmt.Errorf("failed to marshal capabilities: %w", err)
			}
			capsJSON = string(b)
		}
		labelsJSON := "{}"
		if len(record.Labels) > 0 {
			b, err := json.Marshal(record.Labels)
			if err != nil {
				return false, fmt.Errorf("failed to marshal labels: %w", err)
			}
			labelsJSON = string(b)
		}

		statusStr := string(record.Status)
		if statusStr == "" {
			statusStr = string(types.NodeStatusHealthy)
			record.Status = types.NodeStatusHealthy
		}
		regAtStr := record.RegisteredAt.UTC().Format(time.RFC3339Nano)
		lastSeenStr := record.LastSeen.UTC().Format(time.RFC3339Nano)

		queryInsert := `
		INSERT INTO cp_nodes (
			node_id, status, last_seen, registered_at,
			agent_version, protocol_version, hostname, os, architecture,
			capabilities_json, labels_json, sequence_number,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
		`
		_, err = tx.ExecContext(ctx, queryInsert,
			record.NodeID,
			statusStr,
			lastSeenStr,
			regAtStr,
			record.AgentVersion,
			record.ProtocolVersion,
			record.Hostname,
			record.OS,
			record.Architecture,
			capsJSON,
			labelsJSON,
			record.SequenceNumber,
			regAtStr,
			lastSeenStr,
		)
		if err != nil {
			return false, fmt.Errorf("failed to insert new node record: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("failed to commit node registration: %w", err)
		}
		return true, nil
	} else if err != nil {
		return false, fmt.Errorf("failed to query existing node record: %w", err)
	}

	// Existing node duplicate registration:
	// Rules:
	// 1. Preserve original RegisteredAt
	// 2. Preserve original Status
	// 3. Preserve sequence_number (never regress)
	// 4. Update last_seen
	// 5. Update optional fields only if non-empty / non-zero
	existRegAt, parseErr := parseStoredTimestamp(existRegAtStr)
	if parseErr == nil {
		record.RegisteredAt = existRegAt
	}
	record.Status = types.NodeStatus(existStatus)
	if existSeq > record.SequenceNumber {
		record.SequenceNumber = existSeq
	}

	mergedHostname := existHost
	if record.Hostname != "" {
		mergedHostname = record.Hostname
	} else {
		record.Hostname = existHost
	}

	mergedAgentVer := existAgentVer
	if record.AgentVersion != "" {
		mergedAgentVer = record.AgentVersion
	} else {
		record.AgentVersion = existAgentVer
	}

	mergedProtoVer := existProtoVer
	if record.ProtocolVersion != "" {
		mergedProtoVer = record.ProtocolVersion
	} else {
		record.ProtocolVersion = existProtoVer
	}

	mergedOS := existOS
	if record.OS != "" {
		mergedOS = record.OS
	} else {
		record.OS = existOS
	}

	mergedArch := existArch
	if record.Architecture != "" {
		mergedArch = record.Architecture
	} else {
		record.Architecture = existArch
	}

	mergedCapsJSON := existCapsJSON
	if len(record.Capabilities) > 0 {
		b, err := json.Marshal(record.Capabilities)
		if err != nil {
			return false, fmt.Errorf("failed to marshal capabilities: %w", err)
		}
		mergedCapsJSON = string(b)
	} else {
		var caps []string
		if err := json.Unmarshal([]byte(existCapsJSON), &caps); err == nil {
			record.Capabilities = caps
		}
	}

	mergedLabelsJSON := existLabelsJSON
	if len(record.Labels) > 0 {
		b, err := json.Marshal(record.Labels)
		if err != nil {
			return false, fmt.Errorf("failed to marshal labels: %w", err)
		}
		mergedLabelsJSON = string(b)
	} else {
		var labels map[string]string
		if err := json.Unmarshal([]byte(existLabelsJSON), &labels); err == nil {
			record.Labels = labels
		}
	}

	lastSeenStr := record.LastSeen.UTC().Format(time.RFC3339Nano)

	queryUpdate := `
	UPDATE cp_nodes SET
		last_seen = ?,
		hostname = ?,
		agent_version = ?,
		protocol_version = ?,
		os = ?,
		architecture = ?,
		capabilities_json = ?,
		labels_json = ?,
		sequence_number = CASE WHEN ? > sequence_number THEN ? ELSE sequence_number END,
		updated_at = ?
	WHERE node_id = ?;
	`
	_, err = tx.ExecContext(ctx, queryUpdate,
		lastSeenStr,
		mergedHostname,
		mergedAgentVer,
		mergedProtoVer,
		mergedOS,
		mergedArch,
		mergedCapsJSON,
		mergedLabelsJSON,
		record.SequenceNumber,
		record.SequenceNumber,
		lastSeenStr,
		record.NodeID,
	)
	if err != nil {
		return false, fmt.Errorf("failed to update duplicate node record: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("failed to commit duplicate node update: %w", err)
	}

	return false, nil
}

// GetNode retrieves a registered node by node ID.
// Returns (nil, ErrNodeNotFound) if the node does not exist.
func (s *SQLiteStore) GetNode(ctx context.Context, nodeID string) (*NodeRecord, error) {
	if strings.TrimSpace(nodeID) == "" {
		return nil, fmt.Errorf("%w: node_id cannot be empty", ErrInvalidMeta)
	}

	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, ErrStoreClosed
	}
	s.mu.RUnlock()

	query := `
	SELECT node_id, status, last_seen, registered_at,
	       agent_version, protocol_version, hostname, os, architecture,
	       capabilities_json, labels_json, sequence_number
	FROM cp_nodes
	WHERE node_id = ?;
	`
	var (
		rec                                                    NodeRecord
		statusStr, lastSeenStr, regAtStr, capsJSON, labelsJSON string
	)
	err := s.db.QueryRowContext(ctx, query, nodeID).Scan(
		&rec.NodeID,
		&statusStr,
		&lastSeenStr,
		&regAtStr,
		&rec.AgentVersion,
		&rec.ProtocolVersion,
		&rec.Hostname,
		&rec.OS,
		&rec.Architecture,
		&capsJSON,
		&labelsJSON,
		&rec.SequenceNumber,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNodeNotFound
		}
		return nil, fmt.Errorf("failed to query node record: %w", err)
	}

	rec.Status = types.NodeStatus(statusStr)
	rec.LastSeen, _ = parseStoredTimestamp(lastSeenStr)
	rec.RegisteredAt, _ = parseStoredTimestamp(regAtStr)

	var unmarshalErrs []error
	if err := json.Unmarshal([]byte(capsJSON), &rec.Capabilities); err != nil {
		unmarshalErrs = append(unmarshalErrs, fmt.Errorf("capabilities: %w", err))
	}
	if err := json.Unmarshal([]byte(labelsJSON), &rec.Labels); err != nil {
		unmarshalErrs = append(unmarshalErrs, fmt.Errorf("labels: %w", err))
	}
	if len(unmarshalErrs) > 0 {
		return &rec, &CorruptNodeRecordError{
			NodeID: nodeID,
			Err:    errors.Join(unmarshalErrs...),
		}
	}

	return &rec, nil
}

// ListNodes returns all registered nodes ordered deterministically by registered_at ASC, node_id ASC.
func (s *SQLiteStore) ListNodes(ctx context.Context) ([]*NodeRecord, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, ErrStoreClosed
	}
	s.mu.RUnlock()

	query := `
	SELECT node_id, status, last_seen, registered_at,
	       agent_version, protocol_version, hostname, os, architecture,
	       capabilities_json, labels_json, sequence_number
	FROM cp_nodes
	ORDER BY registered_at ASC, node_id ASC;
	`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query nodes list: %w", err)
	}
	defer rows.Close()

	var nodes []*NodeRecord
	var corruptErrs []error

	for rows.Next() {
		var (
			rec                                                    NodeRecord
			statusStr, lastSeenStr, regAtStr, capsJSON, labelsJSON string
		)
		if err := rows.Scan(
			&rec.NodeID,
			&statusStr,
			&lastSeenStr,
			&regAtStr,
			&rec.AgentVersion,
			&rec.ProtocolVersion,
			&rec.Hostname,
			&rec.OS,
			&rec.Architecture,
			&capsJSON,
			&labelsJSON,
			&rec.SequenceNumber,
		); err != nil {
			return nil, fmt.Errorf("failed to scan node record row: %w", err)
		}

		rec.Status = types.NodeStatus(statusStr)
		rec.LastSeen, _ = parseStoredTimestamp(lastSeenStr)
		rec.RegisteredAt, _ = parseStoredTimestamp(regAtStr)

		var rowErr error
		if err := json.Unmarshal([]byte(capsJSON), &rec.Capabilities); err != nil {
			rowErr = fmt.Errorf("corrupt capabilities JSON: %w", err)
		}
		if err := json.Unmarshal([]byte(labelsJSON), &rec.Labels); err != nil {
			if rowErr != nil {
				rowErr = errors.Join(rowErr, fmt.Errorf("corrupt labels JSON: %w", err))
			} else {
				rowErr = fmt.Errorf("corrupt labels JSON: %w", err)
			}
		}

		if rowErr != nil {
			corruptErrs = append(corruptErrs, &CorruptNodeRecordError{
				NodeID: rec.NodeID,
				Err:    rowErr,
			})
		}
		nodes = append(nodes, &rec)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}
	if nodes == nil {
		nodes = []*NodeRecord{}
	}
	if len(corruptErrs) > 0 {
		return nodes, errors.Join(corruptErrs...)
	}
	return nodes, nil
}

// UpdateNodeHeartbeat updates an existing node's status, last_seen, and advances sequence_number monotonically.
// Returns ErrNodeNotFound if the node is not registered.
func (s *SQLiteStore) UpdateNodeHeartbeat(ctx context.Context, nodeID string, status types.NodeStatus, seqNum int64, heartbeatTime time.Time) error {
	if strings.TrimSpace(nodeID) == "" {
		return fmt.Errorf("%w: node_id cannot be empty", ErrInvalidMeta)
	}
	if heartbeatTime.IsZero() {
		return fmt.Errorf("%w: heartbeat_time cannot be zero", ErrInvalidMeta)
	}

	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return ErrStoreClosed
	}
	s.mu.RUnlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Check existence via SELECT so unchanged heartbeat (status & sequence identical) is NOT misidentified as missing node
	var dummy int
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM cp_nodes WHERE node_id = ?;`, nodeID).Scan(&dummy)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNodeNotFound
		}
		return fmt.Errorf("failed to verify node existence: %w", err)
	}

	heartbeatTimeStr := heartbeatTime.UTC().Format(time.RFC3339Nano)
	query := `
	UPDATE cp_nodes
	SET status = ?,
	    last_seen = ?,
	    sequence_number = CASE WHEN ? > sequence_number THEN ? ELSE sequence_number END,
	    updated_at = ?
	WHERE node_id = ?;
	`
	_, err = tx.ExecContext(ctx, query,
		string(status),
		heartbeatTimeStr,
		seqNum,
		seqNum,
		heartbeatTimeStr,
		nodeID,
	)
	if err != nil {
		return fmt.Errorf("failed to update node heartbeat: %w", err)
	}

	return tx.Commit()
}

// UpdateNodeTelemetryActivity advances last_seen and sequence_number monotonically for an existing node.
// Returns (true, nil) if an existing node record was updated, (false, nil) if the node is not registered.
func (s *SQLiteStore) UpdateNodeTelemetryActivity(ctx context.Context, nodeID string, seqNum int64, ingestedAt time.Time) (bool, error) {
	if strings.TrimSpace(nodeID) == "" {
		return false, fmt.Errorf("%w: node_id cannot be empty", ErrInvalidMeta)
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
	defer tx.Rollback()

	var dummy int
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM cp_nodes WHERE node_id = ?;`, nodeID).Scan(&dummy)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Node is not registered: return (false, nil) without fabricating a node
			return false, nil
		}
		return false, fmt.Errorf("failed to verify node existence: %w", err)
	}

	ingestedAtStr := ingestedAt.UTC().Format(time.RFC3339Nano)
	query := `
	UPDATE cp_nodes
	SET last_seen = ?,
	    sequence_number = CASE WHEN ? > sequence_number THEN ? ELSE sequence_number END,
	    updated_at = ?
	WHERE node_id = ?;
	`
	_, err = tx.ExecContext(ctx, query,
		ingestedAtStr,
		seqNum,
		seqNum,
		ingestedAtStr,
		nodeID,
	)
	if err != nil {
		return false, fmt.Errorf("failed to update node telemetry activity: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("failed to commit telemetry activity update: %w", err)
	}
	return true, nil
}

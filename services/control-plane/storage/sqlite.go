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

	// HookAfterHeartbeatUpdate is an optional test hook called inside UpdateNodeHeartbeat
	// after node existence and freshness checks are completed, before transaction commit.
	HookAfterHeartbeatUpdate func(nodeID string) error
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
		node_id              TEXT PRIMARY KEY NOT NULL,
		status               TEXT NOT NULL,
		last_seen            TEXT NOT NULL,
		registered_at        TEXT NOT NULL,
		agent_version        TEXT NOT NULL DEFAULT '',
		protocol_version     TEXT NOT NULL DEFAULT '',
		hostname             TEXT NOT NULL DEFAULT '',
		os                   TEXT NOT NULL DEFAULT '',
		architecture         TEXT NOT NULL DEFAULT '',
		capabilities_json    TEXT NOT NULL DEFAULT '[]',
		labels_json          TEXT NOT NULL DEFAULT '{}',
		sequence_number      INTEGER NOT NULL DEFAULT 0,
		heartbeat_sequence   INTEGER NOT NULL DEFAULT 0,
		metadata_version     INTEGER NOT NULL DEFAULT 0,
		metadata_updated_at  TEXT NOT NULL DEFAULT '',
		created_at           TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at           TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_cp_nodes_reg_order
		ON cp_nodes(registered_at ASC, node_id ASC);

	CREATE INDEX IF NOT EXISTS idx_cp_nodes_last_seen
		ON cp_nodes(last_seen ASC);
	`

	if _, err := s.db.Exec(schema); err != nil {
		return fmt.Errorf("failed to execute schema initialization: %w", err)
	}

	// Safe, idempotent migration for existing databases lacking separate sequence or metadata columns
	cols := make(map[string]bool)
	rows, err := s.db.Query(`PRAGMA table_info(cp_nodes);`)
	if err != nil {
		return fmt.Errorf("failed to check cp_nodes schema: %w", err)
	}
	for rows.Next() {
		var (
			cid           int
			name, colType string
			notNull, pk   int
			dfltVal       interface{}
		)
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dfltVal, &pk); err == nil {
			cols[name] = true
		}
	}
	rows.Close()

	if !cols["heartbeat_sequence"] {
		if _, err := s.db.Exec(`ALTER TABLE cp_nodes ADD COLUMN heartbeat_sequence INTEGER NOT NULL DEFAULT 0;`); err != nil {
			if !strings.Contains(err.Error(), "duplicate column") {
				return fmt.Errorf("failed to migrate cp_nodes for heartbeat_sequence: %w", err)
			}
		}
	}
	if !cols["metadata_version"] {
		if _, err := s.db.Exec(`ALTER TABLE cp_nodes ADD COLUMN metadata_version INTEGER NOT NULL DEFAULT 0;`); err != nil {
			if !strings.Contains(err.Error(), "duplicate column") {
				return fmt.Errorf("failed to migrate cp_nodes for metadata_version: %w", err)
			}
		}
	}
	if !cols["metadata_updated_at"] {
		if _, err := s.db.Exec(`ALTER TABLE cp_nodes ADD COLUMN metadata_updated_at TEXT NOT NULL DEFAULT '';`); err != nil {
			if !strings.Contains(err.Error(), "duplicate column") {
				return fmt.Errorf("failed to migrate cp_nodes for metadata_updated_at: %w", err)
			}
		}
		_, _ = s.db.Exec(`UPDATE cp_nodes SET metadata_updated_at = registered_at WHERE metadata_updated_at = '';`)
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

	collectedAtStr := formatTimestamp(batch.CollectedAt)
	ingestedAtStr := formatTimestamp(ingestedAt)

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
	// If the node is not registered, leave cp_nodes untouched (no synthetic node created).
	var existLastSeenStr string
	var existSeq int64
	err = tx.QueryRowContext(ctx, `SELECT last_seen, sequence_number FROM cp_nodes WHERE node_id = ?;`, batch.NodeID).Scan(&existLastSeenStr, &existSeq)
	if err == nil {
		existLastSeen, parseErr := parseStoredTimestamp(existLastSeenStr)
		if parseErr != nil {
			return false, &CorruptNodeRecordError{
				NodeID: batch.NodeID,
				Err:    fmt.Errorf("invalid last_seen timestamp %q: %w", existLastSeenStr, parseErr),
			}
		}
		targetLastSeenStr := existLastSeenStr
		if !ingestedAt.Before(existLastSeen) {
			targetLastSeenStr = formatTimestamp(ingestedAt)
		}
		targetSeq := existSeq
		if batch.SequenceNumber > existSeq {
			targetSeq = batch.SequenceNumber
		}
		queryNodeActivity := `
		UPDATE cp_nodes
		SET last_seen = ?,
		    sequence_number = ?,
		    updated_at = ?
		WHERE node_id = ?;
		`
		_, err = tx.ExecContext(ctx, queryNodeActivity,
			targetLastSeenStr,
			targetSeq,
			targetLastSeenStr,
			batch.NodeID,
		)
		if err != nil {
			return false, fmt.Errorf("failed to update node activity in batch transaction: %w", err)
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("failed to query node activity for batch transaction: %w", err)
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

// TimestampFormat defines the canonical zero-padded fixed-width UTC timestamp layout (30 characters)
// ensuring chronological sorting alignment across SQLite text columns.
const TimestampFormat = "2006-01-02T15:04:05.000000000Z"

func formatTimestamp(t time.Time) string {
	return t.UTC().Format(TimestampFormat)
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
	SELECT last_seen, status, registered_at, sequence_number, heartbeat_sequence, metadata_version, metadata_updated_at,
	       hostname, agent_version, protocol_version, os, architecture, capabilities_json, labels_json
	FROM cp_nodes WHERE node_id = ?;
	`
	var (
		existLastSeenStr, existStatus, existRegAtStr, existMetaTimeStr, existHost, existAgentVer, existProtoVer, existOS, existArch, existCapsJSON, existLabelsJSON string
		existSeq, existHbSeq, existMetaVer                                                                                                                          int64
	)
	err = tx.QueryRowContext(ctx, queryCheck, record.NodeID).Scan(
		&existLastSeenStr, &existStatus, &existRegAtStr, &existSeq, &existHbSeq, &existMetaVer, &existMetaTimeStr,
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
		regAtStr := formatTimestamp(record.RegisteredAt)
		lastSeenStr := formatTimestamp(record.LastSeen)
		metaTime := record.UpdatedAt
		if metaTime.IsZero() {
			metaTime = record.RegisteredAt
		}
		metaTimeStr := formatTimestamp(metaTime)
		record.UpdatedAt = metaTime

		queryInsert := `
		INSERT INTO cp_nodes (
			node_id, status, last_seen, registered_at,
			agent_version, protocol_version, hostname, os, architecture,
			capabilities_json, labels_json, sequence_number, heartbeat_sequence,
			metadata_version, metadata_updated_at,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
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
			record.HeartbeatSequence,
			record.MetadataVersion,
			metaTimeStr,
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
	existRegAt, parseErr := parseStoredTimestamp(existRegAtStr)
	if parseErr != nil {
		return false, &CorruptNodeRecordError{
			NodeID: record.NodeID,
			Err:    fmt.Errorf("invalid registered_at timestamp %q: %w", existRegAtStr, parseErr),
		}
	}
	record.RegisteredAt = existRegAt

	existLastSeen, parseErr := parseStoredTimestamp(existLastSeenStr)
	if parseErr != nil {
		return false, &CorruptNodeRecordError{
			NodeID: record.NodeID,
			Err:    fmt.Errorf("invalid last_seen timestamp %q: %w", existLastSeenStr, parseErr),
		}
	}

	existMetaTime, parseErr := parseStoredTimestamp(existMetaTimeStr)
	if parseErr != nil {
		existMetaTime = existRegAt
	}

	record.Status = types.NodeStatus(existStatus)
	if existSeq > record.SequenceNumber {
		record.SequenceNumber = existSeq
	}
	if existHbSeq > record.HeartbeatSequence {
		record.HeartbeatSequence = existHbSeq
	}

	reqMetaTime := record.UpdatedAt
	if reqMetaTime.IsZero() {
		reqMetaTime = record.RegisteredAt
	}
	reqMetaVer := record.MetadataVersion

	var shouldAdvanceMetadata bool
	if reqMetaVer > existMetaVer {
		shouldAdvanceMetadata = true
	} else if reqMetaVer < existMetaVer {
		shouldAdvanceMetadata = false
	} else {
		if reqMetaTime.After(existMetaTime) {
			shouldAdvanceMetadata = true
		} else if reqMetaTime.Before(existMetaTime) {
			shouldAdvanceMetadata = false
		} else {
			// Equal version and equal timestamp: idempotent merge of non-empty fields
			shouldAdvanceMetadata = true
		}
	}

	targetLastSeenStr := existLastSeenStr
	if record.LastSeen.After(existLastSeen) {
		targetLastSeenStr = formatTimestamp(record.LastSeen)
	} else {
		record.LastSeen = existLastSeen
	}

	targetMetaVer := existMetaVer
	targetMetaTimeStr := existMetaTimeStr
	if targetMetaTimeStr == "" {
		targetMetaTimeStr = formatTimestamp(existMetaTime)
	}

	mergedHostname := existHost
	mergedAgentVer := existAgentVer
	mergedProtoVer := existProtoVer
	mergedOS := existOS
	mergedArch := existArch
	mergedCapsJSON := existCapsJSON
	mergedLabelsJSON := existLabelsJSON

	if shouldAdvanceMetadata {
		targetMetaVer = reqMetaVer
		targetMetaTimeStr = formatTimestamp(reqMetaTime)
		record.MetadataVersion = targetMetaVer
		record.UpdatedAt = reqMetaTime

		if record.Hostname != "" {
			mergedHostname = record.Hostname
		} else {
			record.Hostname = existHost
		}
		if record.AgentVersion != "" {
			mergedAgentVer = record.AgentVersion
		} else {
			record.AgentVersion = existAgentVer
		}
		if record.ProtocolVersion != "" {
			mergedProtoVer = record.ProtocolVersion
		} else {
			record.ProtocolVersion = existProtoVer
		}
		if record.OS != "" {
			mergedOS = record.OS
		} else {
			record.OS = existOS
		}
		if record.Architecture != "" {
			mergedArch = record.Architecture
		} else {
			record.Architecture = existArch
		}
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
	} else {
		record.MetadataVersion = existMetaVer
		record.UpdatedAt = existMetaTime
		record.Hostname = existHost
		record.AgentVersion = existAgentVer
		record.ProtocolVersion = existProtoVer
		record.OS = existOS
		record.Architecture = existArch
		var caps []string
		if err := json.Unmarshal([]byte(existCapsJSON), &caps); err == nil {
			record.Capabilities = caps
		}
		var labels map[string]string
		if err := json.Unmarshal([]byte(existLabelsJSON), &labels); err == nil {
			record.Labels = labels
		}
	}

	targetSeq := existSeq
	if record.SequenceNumber > existSeq {
		targetSeq = record.SequenceNumber
	}
	targetHbSeq := existHbSeq
	if record.HeartbeatSequence > existHbSeq {
		targetHbSeq = record.HeartbeatSequence
	}

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
		sequence_number = ?,
		heartbeat_sequence = ?,
		metadata_version = ?,
		metadata_updated_at = ?,
		updated_at = ?
	WHERE node_id = ?;
	`
	_, err = tx.ExecContext(ctx, queryUpdate,
		targetLastSeenStr,
		mergedHostname,
		mergedAgentVer,
		mergedProtoVer,
		mergedOS,
		mergedArch,
		mergedCapsJSON,
		mergedLabelsJSON,
		targetSeq,
		targetHbSeq,
		targetMetaVer,
		targetMetaTimeStr,
		targetLastSeenStr,
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
	       capabilities_json, labels_json, sequence_number, heartbeat_sequence,
	       metadata_version, metadata_updated_at
	FROM cp_nodes
	WHERE node_id = ?;
	`
	var (
		rec                                                                 NodeRecord
		statusStr, lastSeenStr, regAtStr, capsJSON, labelsJSON, metaTimeStr string
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
		&rec.HeartbeatSequence,
		&rec.MetadataVersion,
		&metaTimeStr,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNodeNotFound
		}
		return nil, fmt.Errorf("failed to query node record: %w", err)
	}

	rec.Status = types.NodeStatus(statusStr)
	var unmarshalErrs []error
	var parseErr error
	rec.LastSeen, parseErr = parseStoredTimestamp(lastSeenStr)
	if parseErr != nil {
		unmarshalErrs = append(unmarshalErrs, fmt.Errorf("invalid last_seen timestamp %q: %w", lastSeenStr, parseErr))
	}
	rec.RegisteredAt, parseErr = parseStoredTimestamp(regAtStr)
	if parseErr != nil {
		unmarshalErrs = append(unmarshalErrs, fmt.Errorf("invalid registered_at timestamp %q: %w", regAtStr, parseErr))
	}
	if metaTimeStr != "" {
		rec.UpdatedAt, _ = parseStoredTimestamp(metaTimeStr)
	} else {
		rec.UpdatedAt = rec.RegisteredAt
	}

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
	       capabilities_json, labels_json, sequence_number, heartbeat_sequence,
	       metadata_version, metadata_updated_at
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
			rec                                                                 NodeRecord
			statusStr, lastSeenStr, regAtStr, capsJSON, labelsJSON, metaTimeStr string
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
			&rec.HeartbeatSequence,
			&rec.MetadataVersion,
			&metaTimeStr,
		); err != nil {
			return nil, fmt.Errorf("failed to scan node record row: %w", err)
		}

		rec.Status = types.NodeStatus(statusStr)
		var rowErr error
		var parseErr error
		rec.LastSeen, parseErr = parseStoredTimestamp(lastSeenStr)
		if parseErr != nil {
			rowErr = fmt.Errorf("invalid last_seen timestamp %q: %w", lastSeenStr, parseErr)
		}
		rec.RegisteredAt, parseErr = parseStoredTimestamp(regAtStr)
		if parseErr != nil {
			if rowErr != nil {
				rowErr = errors.Join(rowErr, fmt.Errorf("invalid registered_at timestamp %q: %w", regAtStr, parseErr))
			} else {
				rowErr = fmt.Errorf("invalid registered_at timestamp %q: %w", regAtStr, parseErr)
			}
		}
		if metaTimeStr != "" {
			rec.UpdatedAt, _ = parseStoredTimestamp(metaTimeStr)
		} else {
			rec.UpdatedAt = rec.RegisteredAt
		}

		if err := json.Unmarshal([]byte(capsJSON), &rec.Capabilities); err != nil {
			if rowErr != nil {
				rowErr = errors.Join(rowErr, fmt.Errorf("corrupt capabilities JSON: %w", err))
			} else {
				rowErr = fmt.Errorf("corrupt capabilities JSON: %w", err)
			}
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

// UpdateNodeHeartbeat updates an existing node's status, last_seen, and advances heartbeat_sequence monotonically.
// Returns ErrNodeNotFound if the node is not registered.
func (s *SQLiteStore) UpdateNodeHeartbeat(ctx context.Context, nodeID string, status types.NodeStatus, hbSeq int64, heartbeatTime time.Time) error {
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
	var existLastSeenStr, existStatus string
	var existHbSeq int64
	err = tx.QueryRowContext(ctx, `SELECT last_seen, status, heartbeat_sequence FROM cp_nodes WHERE node_id = ?;`, nodeID).Scan(&existLastSeenStr, &existStatus, &existHbSeq)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNodeNotFound
		}
		return fmt.Errorf("failed to verify node existence: %w", err)
	}

	existLastSeen, parseErr := parseStoredTimestamp(existLastSeenStr)
	if parseErr != nil {
		return &CorruptNodeRecordError{
			NodeID: nodeID,
			Err:    fmt.Errorf("invalid last_seen timestamp %q: %w", existLastSeenStr, parseErr),
		}
	}

	// Ordering policy within heartbeat sequence domain:
	// 1. Strictly newer timestamp (heartbeatTime.After(existLastSeen)): advance last_seen and status.
	// 2. Older timestamp (heartbeatTime.Before(existLastSeen)): do not regress last_seen or status.
	// 3. Equal timestamps (heartbeatTime.Equal(existLastSeen)):
	//    - If hbSeq >= existHbSeq: advance status.
	//    - If hbSeq < existHbSeq: do not regress status.
	var advanceStatusAndLastSeen bool
	if heartbeatTime.After(existLastSeen) {
		advanceStatusAndLastSeen = true
	} else if heartbeatTime.Equal(existLastSeen) && hbSeq >= existHbSeq {
		advanceStatusAndLastSeen = true
	}

	targetLastSeenStr := existLastSeenStr
	targetStatus := existStatus
	if advanceStatusAndLastSeen {
		targetLastSeenStr = formatTimestamp(heartbeatTime)
		targetStatus = string(status)
	}

	targetHbSeq := existHbSeq
	if hbSeq > existHbSeq {
		targetHbSeq = hbSeq
	}

	// Optional test hook for simulating concurrency or failure after heartbeat read/check
	if s.HookAfterHeartbeatUpdate != nil {
		if err := s.HookAfterHeartbeatUpdate(nodeID); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("injected test failure in heartbeat hook: %w", err)
		}
	}

	query := `
	UPDATE cp_nodes
	SET status = ?,
	    last_seen = ?,
	    heartbeat_sequence = ?,
	    updated_at = ?
	WHERE node_id = ?;
	`
	_, err = tx.ExecContext(ctx, query,
		targetStatus,
		targetLastSeenStr,
		targetHbSeq,
		targetLastSeenStr,
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

	var existLastSeenStr string
	var existSeq int64
	err = tx.QueryRowContext(ctx, `SELECT last_seen, sequence_number FROM cp_nodes WHERE node_id = ?;`, nodeID).Scan(&existLastSeenStr, &existSeq)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Node is not registered: return (false, nil) without fabricating a node
			return false, nil
		}
		return false, fmt.Errorf("failed to verify node existence: %w", err)
	}

	existLastSeen, parseErr := parseStoredTimestamp(existLastSeenStr)
	if parseErr != nil {
		return false, &CorruptNodeRecordError{
			NodeID: nodeID,
			Err:    fmt.Errorf("invalid last_seen timestamp %q: %w", existLastSeenStr, parseErr),
		}
	}
	targetLastSeenStr := existLastSeenStr
	if !ingestedAt.Before(existLastSeen) {
		targetLastSeenStr = formatTimestamp(ingestedAt)
	}

	targetSeq := existSeq
	if seqNum > existSeq {
		targetSeq = seqNum
	}

	query := `
	UPDATE cp_nodes
	SET last_seen = ?,
	    sequence_number = ?,
	    updated_at = ?
	WHERE node_id = ?;
	`
	_, err = tx.ExecContext(ctx, query,
		targetLastSeenStr,
		targetSeq,
		targetLastSeenStr,
		nodeID,
	)
	if err != nil {
		return false, fmt.Errorf("failed to update node telemetry activity: %w", err)
	}

	return true, tx.Commit()
}

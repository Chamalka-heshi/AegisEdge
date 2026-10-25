package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/services/control-plane/storage"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// ErrDuplicateBatch is returned when a batch with the same BatchID has already been ingested.
var ErrDuplicateBatch = errors.New("duplicate batch already ingested")

// IngestedBatch holds a validated telemetry batch along with its control-plane ingestion timestamp.
type IngestedBatch struct {
	Batch      types.TelemetryBatch `json:"batch"`
	IngestedAt time.Time            `json:"ingested_at"`
}

// IngestionResponse represents the deterministic JSON response for batch submission.
type IngestionResponse struct {
	Status     string     `json:"status"`
	BatchID    string     `json:"batch_id"`
	NodeID     string     `json:"node_id"`
	IngestedAt *time.Time `json:"ingested_at,omitempty"`
}

// ErrorResponse represents a structured error message.
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// DefaultMaxHistoryBatches is the conservative default capacity for in-memory payload history retention.
const DefaultMaxHistoryBatches = 1000

// IngestedMeta holds minimal metadata for a deduplicated batch without retaining full metric sample slices.
type IngestedMeta struct {
	NodeID         string    `json:"node_id"`
	BatchID        string    `json:"batch_id"`
	SequenceNumber int64     `json:"sequence_number"`
	IngestedAt     time.Time `json:"ingested_at"`
}

// IdempotencyKey identifies a telemetry batch uniquely by its node and batch identifiers.
// Using a typed struct rather than delimiter concatenation eliminates key ambiguity
// regardless of characters present in NodeID or BatchID.
type IdempotencyKey struct {
	NodeID  string
	BatchID string
}

// historyRingBuffer is an efficient, bounded circular ring buffer storing IngestedBatch payloads.
// It maintains items in strict FIFO arrival order with O(1) insertions, avoiding per-append slice shifting.
type historyRingBuffer struct {
	entries  []*IngestedBatch
	capacity int
	head     int // points to the oldest item
	count    int // current number of stored items
}

func newHistoryRingBuffer(capacity int) *historyRingBuffer {
	if capacity <= 0 {
		capacity = DefaultMaxHistoryBatches
	}
	return &historyRingBuffer{
		entries:  make([]*IngestedBatch, capacity),
		capacity: capacity,
		head:     0,
		count:    0,
	}
}

// push appends a new record to the ring buffer in O(1) time.
// If the buffer is full, the oldest record at head is overwritten and head advances.
func (r *historyRingBuffer) push(record *IngestedBatch) {
	if r.capacity <= 0 {
		return
	}
	if r.count < r.capacity {
		idx := (r.head + r.count) % r.capacity
		r.entries[idx] = record
		r.count++
	} else {
		r.entries[r.head] = record
		r.head = (r.head + 1) % r.capacity
	}
}

// snapshot returns a slice of all stored records in chronological order (oldest to newest).
func (r *historyRingBuffer) snapshot() []*IngestedBatch {
	if r.count == 0 {
		return []*IngestedBatch{}
	}
	out := make([]*IngestedBatch, r.count)
	firstChunk := r.capacity - r.head
	if firstChunk > r.count {
		firstChunk = r.count
	}
	copy(out, r.entries[r.head:r.head+firstChunk])
	if secondChunk := r.count - firstChunk; secondChunk > 0 {
		copy(out[firstChunk:], r.entries[:secondChunk])
	}
	return out
}

// len returns the current count of elements in the ring buffer.
func (r *historyRingBuffer) len() int {
	return r.count
}

// cap returns the maximum capacity of the ring buffer.
func (r *historyRingBuffer) cap() int {
	return r.capacity
}

// resize updates the ring buffer capacity. If the new capacity is smaller than count,
// the oldest entries are dropped, retaining the newest 'newCap' items in chronological order.
func (r *historyRingBuffer) resize(newCap int) error {
	if newCap <= 0 {
		return fmt.Errorf("max history size must be greater than zero, got %d", newCap)
	}
	if newCap == r.capacity {
		return nil
	}

	current := r.snapshot()
	newEntries := make([]*IngestedBatch, newCap)

	if len(current) > newCap {
		excess := len(current) - newCap
		copy(newEntries, current[excess:])
		r.count = newCap
	} else {
		copy(newEntries, current)
		r.count = len(current)
	}

	r.entries = newEntries
	r.capacity = newCap
	r.head = 0
	return nil
}

// ServerOption allows customizing Server parameters during construction.
type ServerOption func(*Server) error

// WithMaxHistory configures the maximum number of batch payloads to retain in memory.
func WithMaxHistory(maxHistory int) ServerOption {
	return func(s *Server) error {
		if maxHistory <= 0 {
			return fmt.Errorf("max history size must be greater than zero, got %d", maxHistory)
		}
		return s.history.resize(maxHistory)
	}
}

// DefaultStorageProbeTimeout is the default maximum duration for the storage readiness probe.
const DefaultStorageProbeTimeout = 2 * time.Second

// WithStorageProbeTimeout configures the maximum duration for storage readiness probes.
func WithStorageProbeTimeout(d time.Duration) ServerOption {
	return func(s *Server) error {
		if d <= 0 {
			return errors.New("storage probe timeout must be positive")
		}
		s.storageProbeTimeout = d
		return nil
	}
}

// WithIdempotencyStore configures the persistent idempotency store for deduplication.
func WithIdempotencyStore(store storage.IdempotencyStore) ServerOption {
	return func(s *Server) error {
		if store == nil {
			return errors.New("idempotency store cannot be nil")
		}
		s.store = store
		return nil
	}
}

// Server provides the HTTP ingestion endpoints for AegisEdge telemetry.
//
// NOTE (Phase 6.22B Durable Control-Plane Idempotency Architecture):
// Deduplication is scoped to typed composite struct (NodeID, BatchID) enforced
// by a durable SQLite database (or pluggable IdempotencyStore).
// The deduplication store guarantees atomic, restart-safe deduplication.
// Telemetry payload history (for listing batches) is stored in a genuine, thread-safe, bounded circular
// ring buffer (default 1,000 batches), completely decoupled from the persistent deduplication records.
type Server struct {
	mu                  sync.RWMutex
	store               storage.IdempotencyStore
	history             *historyRingBuffer
	nodes               map[string]*NodeRecord
	nodesList           []*NodeRecord
	logger              *slog.Logger
	maxPayloadSize      int64
	authConfig          AuthConfig
	replayCache         *ReplayCache
	initErr             error
	nodeLocks           *nodeLockManager
	storageProbeTimeout time.Duration
	shuttingDown        atomic.Bool
}

// nodeLock tracks active and pending operations for a specific node ID.
type nodeLock struct {
	mu   sync.Mutex
	refs int
}

// nodeLockManager provides fine-grained, reference-counted per-node operation serialization.
// It coordinates durable database writes and in-memory reconciliation for the same node
// without serializing operations across different nodes or holding the global server mutex across disk I/O.
type nodeLockManager struct {
	mu    sync.Mutex
	locks map[string]*nodeLock
}

func newNodeLockManager() *nodeLockManager {
	return &nodeLockManager{
		locks: make(map[string]*nodeLock),
	}
}

// Lock acquires operation serialization for the specified nodeID and returns a release function.
// Empty nodeIDs return a no-op release function.
func (m *nodeLockManager) Lock(nodeID string) func() {
	if nodeID == "" {
		return func() {}
	}

	m.mu.Lock()
	nl, ok := m.locks[nodeID]
	if !ok {
		nl = &nodeLock{}
		m.locks[nodeID] = nl
	}
	nl.refs++
	m.mu.Unlock()

	nl.mu.Lock()

	return func() {
		nl.mu.Unlock()

		m.mu.Lock()
		nl.refs--
		if nl.refs == 0 {
			delete(m.locks, nodeID)
		}
		m.mu.Unlock()
	}
}

func (s *Server) lockNode(nodeID string) func() {
	if s.nodeLocks == nil {
		return func() {}
	}
	return s.nodeLocks.Lock(nodeID)
}

// NewServer initializes a control-plane HTTP ingestion server with optional configuration.
// If no WithIdempotencyStore option is provided, an in-memory SQLite store is initialized by default.
func NewServer(logger *slog.Logger, opts ...ServerOption) *Server {
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}))
	}
	s := &Server{
		history:             newHistoryRingBuffer(DefaultMaxHistoryBatches),
		nodes:               make(map[string]*NodeRecord),
		nodesList:           make([]*NodeRecord, 0),
		logger:              logger,
		maxPayloadSize:      1024 * 1024, // 1MB payload limit
		authConfig:          DefaultAuthConfig(),
		replayCache:         NewReplayCache(DefaultMaxReplayKeys),
		nodeLocks:           newNodeLockManager(),
		storageProbeTimeout: DefaultStorageProbeTimeout,
	}
	for _, opt := range opts {
		if err := opt(s); err != nil {
			s.logger.Warn("invalid server option ignored", slog.Any("error", err))
		}
	}
	if s.store == nil {
		memStore, err := storage.OpenSQLite(":memory:")
		if err != nil {
			s.logger.Error("failed to initialize default in-memory sqlite store", slog.Any("error", err))
		} else {
			s.store = memStore
		}
	}

	// Rehydrate registered edge nodes from durable store before serving traffic
	if s.store != nil {
		nodes, err := s.store.ListNodes(context.Background())
		if err != nil {
			s.initErr = fmt.Errorf("failed to rehydrate node registry from durable store: %w", err)
			s.logger.Error("failed to rehydrate node registry from durable store", slog.Any("error", err))
		} else {
			for _, n := range nodes {
				s.nodes[n.NodeID] = n
				s.nodesList = append(s.nodesList, n)
			}
			s.logger.Info("rehydrated node registry from durable store", slog.Int("node_count", len(nodes)))
		}
	}

	// Warm in-memory history ring buffer from durable recent payloads if available
	if s.store != nil {
		recent, err := s.store.GetRecentBatches(context.Background(), s.history.cap())
		for _, item := range recent {
			s.history.push(&IngestedBatch{
				Batch:      item.Batch,
				IngestedAt: item.IngestedAt,
			})
		}
		if err != nil {
			if errors.Is(err, storage.ErrCorruptPayload) {
				s.logger.Warn("corrupted telemetry payloads detected during history rehydration; skipped invalid rows",
					slog.Any("error", err),
					slog.Int("recovered_batches", len(recent)),
				)
			} else {
				s.logger.Warn("failed to warm in-memory history cache from durable store",
					slog.Any("error", err),
				)
			}
		}
	}
	return s
}

// InitError returns any initialization error encountered during startup rehydration.
func (s *Server) InitError() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.initErr
}

// Close gracefully closes the server's underlying persistent storage.
// It invokes MarkShuttingDown() before closing storage so readiness endpoints (/readyz)
// immediately transition to not-ready during programmatic shutdown.
func (s *Server) Close() error {
	s.MarkShuttingDown()
	if s.store != nil {
		return s.store.Close()
	}
	return nil
}

// GetPayload retrieves a persisted telemetry batch by node ID and batch ID from the durable store.
func (s *Server) GetPayload(ctx context.Context, nodeID, batchID string) (*types.TelemetryBatch, error) {
	if s.store != nil {
		return s.store.GetPayload(ctx, nodeID, batchID)
	}
	return nil, storage.ErrStoreClosed
}

// SetAuthConfig sets the authentication configuration for the control plane.
func (s *Server) SetAuthConfig(cfg AuthConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.authConfig = cfg
	if s.replayCache == nil {
		s.replayCache = NewReplayCache(DefaultMaxReplayKeys)
	}
	if !cfg.Enabled {
		s.logger.Warn("control plane authentication is disabled (development mode)")
	} else {
		s.logger.Info("control plane authentication is enabled",
			slog.Bool("replay_protection", cfg.ReplayProtection),
			slog.Duration("max_clock_skew", cfg.MaxClockSkew),
		)
	}
}

// AuthConfig returns a copy of current authentication configuration.
func (s *Server) AuthConfig() AuthConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.authConfig
}

// MarkShuttingDown flags the server as shutting down, causing /readyz to return HTTP 503.
func (s *Server) MarkShuttingDown() {
	s.shuttingDown.Store(true)
}

// IsShuttingDown reports whether the server has begun graceful shutdown.
func (s *Server) IsShuttingDown() bool {
	return s.shuttingDown.Load()
}

// Routes constructs and returns the HTTP request multiplexer.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/api/v1/telemetry/batches", s.handleTelemetryBatches)
	mux.HandleFunc("/api/v1/nodes/register", s.handleNodeRegister)
	mux.HandleFunc("/api/v1/nodes/heartbeat", s.handleNodeHeartbeat)
	mux.HandleFunc("/api/v1/nodes/", s.handleNodes)
	mux.HandleFunc("/api/v1/nodes", s.handleNodes)
	return mux
}

// handleHealthz responds with basic service liveness status.
// Liveness indicates whether the control-plane process is alive and running.
// Unlike readiness (/readyz), /healthz does not execute deep database probes
// and will not report unhealthy on transient storage locks or slow connections.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		s.writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{
			Error:   "method_not_allowed",
			Message: "health check endpoint accepts GET only",
		})
		return
	}
	if s.initErr != nil {
		s.writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "unhealthy",
			"error":  s.initErr.Error(),
		})
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]string{
		"status": "healthy",
	})
}

// ReadinessResponse represents the structured health payload returned by /readyz.
type ReadinessResponse struct {
	Status string            `json:"status"` // "ready" or "not_ready"
	Checks map[string]string `json:"checks"` // e.g. {"storage": "healthy"} or {"storage": "unhealthy"}
	Error  string            `json:"error,omitempty"`
}

// handleReadyz responds with service readiness status.
// Readiness indicates whether the control plane is prepared to accept registrations,
// heartbeats, and telemetry batches. It verifies that startup initialization succeeded,
// graceful shutdown has not started, and the underlying storage probe succeeds within
// a bounded context timeout.
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		s.writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{
			Error:   "method_not_allowed",
			Message: "readiness endpoint accepts GET and HEAD only",
		})
		return
	}

	// 1. Startup initialization check
	if s.initErr != nil {
		s.writeReadiness(w, r.Method, http.StatusServiceUnavailable, ReadinessResponse{
			Status: "not_ready",
			Checks: map[string]string{
				"storage": "unhealthy",
			},
			Error: "server_uninitialized",
		})
		return
	}

	// 2. Graceful shutdown check (shed new traffic promptly)
	if s.shuttingDown.Load() {
		s.writeReadiness(w, r.Method, http.StatusServiceUnavailable, ReadinessResponse{
			Status: "not_ready",
			Checks: map[string]string{
				"storage": "healthy",
			},
			Error: "server_shutting_down",
		})
		return
	}

	// 3. Storage availability check
	if s.store == nil {
		s.writeReadiness(w, r.Method, http.StatusServiceUnavailable, ReadinessResponse{
			Status: "not_ready",
			Checks: map[string]string{
				"storage": "unhealthy",
			},
			Error: "storage_unavailable",
		})
		return
	}

	// 4. Bounded storage probe
	timeout := s.storageProbeTimeout
	if timeout <= 0 {
		timeout = DefaultStorageProbeTimeout
	}
	probeCtx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	if err := s.store.Ping(probeCtx); err != nil {
		s.logger.Warn("readiness storage probe failed", slog.Any("error", err))
		s.writeReadiness(w, r.Method, http.StatusServiceUnavailable, ReadinessResponse{
			Status: "not_ready",
			Checks: map[string]string{
				"storage": "unhealthy",
			},
			Error: "storage_unavailable",
		})
		return
	}

	// 5. All readiness checks passed
	s.writeReadiness(w, r.Method, http.StatusOK, ReadinessResponse{
		Status: "ready",
		Checks: map[string]string{
			"storage": "healthy",
		},
	})
}

func (s *Server) writeReadiness(w http.ResponseWriter, method string, statusCode int, resp ReadinessResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if method == http.MethodHead {
		return
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.logger.Error("failed to write readiness response", slog.Any("error", err))
	}
}

// handleTelemetryBatches processes batch ingestion (POST) and batch queries (GET).
func (s *Server) handleTelemetryBatches(w http.ResponseWriter, r *http.Request) {
	if s.initErr != nil {
		s.writeJSON(w, http.StatusServiceUnavailable, ErrorResponse{
			Error:   "service_uninitialized",
			Message: fmt.Sprintf("server failed initialization: %v", s.initErr),
		})
		return
	}
	switch r.Method {
	case http.MethodPost:
		s.handleIngestBatch(w, r)
	case http.MethodGet:
		s.handleListBatches(w, r)
	default:
		w.Header().Set("Allow", "GET, POST")
		s.writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{
			Error:   "method_not_allowed",
			Message: fmt.Sprintf("unsupported HTTP method %s", r.Method),
		})
	}
}

// handleIngestBatch validates, idempotently accepts, and stores an incoming batch.
func (s *Server) handleIngestBatch(w http.ResponseWriter, r *http.Request) {
	// 1. Enforce payload size limit (1MB)
	bodyBytes, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.maxPayloadSize))
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			s.writeJSON(w, http.StatusRequestEntityTooLarge, ErrorResponse{
				Error:   "payload_too_large",
				Message: fmt.Sprintf("request payload exceeds %d byte limit", s.maxPayloadSize),
			})
			return
		}
		s.writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "malformed_body",
			Message: fmt.Sprintf("failed to read request body: %v", err),
		})
		return
	}
	defer r.Body.Close()

	// 2. Authenticate Request
	authCtx, err := s.AuthenticateRequest(r, bodyBytes)
	if err != nil {
		s.writeAuthError(w, err)
		return
	}

	// 3. Decode JSON body
	var batch types.TelemetryBatch
	decoder := json.NewDecoder(bytes.NewReader(bodyBytes))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&batch); err != nil {
		s.writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "malformed_json",
			Message: fmt.Sprintf("failed to parse JSON payload: %v", err),
		})
		return
	}

	// 4. Domain validation
	if err := batch.Validate(); err != nil {
		s.writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "invalid_batch",
			Message: fmt.Sprintf("batch domain validation failed: %v", err),
		})
		return
	}

	// 5. Node impersonation protection (if authenticated and not admin)
	s.mu.RLock()
	authEnabled := s.authConfig.Enabled
	s.mu.RUnlock()
	if authEnabled && !authCtx.IsAdmin && authCtx.NodeID != batch.NodeID {
		s.writeJSON(w, http.StatusForbidden, ErrorResponse{
			Error:   "forbidden",
			Message: fmt.Sprintf("authenticated node %q cannot submit telemetry for node %q", authCtx.NodeID, batch.NodeID),
		})
		return
	}

	// 6. Atomic Durable Ingestion (both metadata and complete payload committed in single transaction)
	unlockNode := s.lockNode(batch.NodeID)
	defer unlockNode()

	ingestedAt := time.Now().UTC()

	isNew, err := s.store.RecordBatch(r.Context(), &batch, ingestedAt)
	if err != nil {
		s.logger.Error("failed to record batch in durable store",
			slog.String("batch_id", batch.BatchID),
			slog.String("node_id", batch.NodeID),
			slog.Any("error", err),
		)
		s.writeJSON(w, http.StatusInternalServerError, ErrorResponse{
			Error:   "internal_server_error",
			Message: "failed to persist telemetry batch",
		})
		return
	}

	if !isNew {
		// Duplicate batch: already accepted. Return 200 OK with already_accepted status.
		s.logger.Info("duplicate telemetry batch received (idempotent no-op)",
			slog.String("batch_id", batch.BatchID),
			slog.String("node_id", batch.NodeID),
			slog.Int64("sequence_number", batch.SequenceNumber),
		)
		s.writeJSON(w, http.StatusOK, IngestionResponse{
			Status:  "already_accepted",
			BatchID: batch.BatchID,
			NodeID:  batch.NodeID,
		})
		return
	}

	// New batch successfully persisted: append to bounded payload history
	record := &IngestedBatch{
		Batch:      batch,
		IngestedAt: ingestedAt,
	}
	s.mu.Lock()
	s.appendHistoryLocked(record)
	s.updateNodeTelemetryActivityLocked(batch.NodeID, batch.SequenceNumber, ingestedAt)
	s.mu.Unlock()

	s.logger.Info("telemetry batch accepted and ingested",
		slog.String("batch_id", batch.BatchID),
		slog.String("node_id", batch.NodeID),
		slog.Int64("sequence_number", batch.SequenceNumber),
		slog.Time("collected_at", batch.CollectedAt),
		slog.Time("ingested_at", ingestedAt),
	)

	s.writeJSON(w, http.StatusCreated, IngestionResponse{
		Status:     "accepted",
		BatchID:    batch.BatchID,
		NodeID:     batch.NodeID,
		IngestedAt: &ingestedAt,
	})
}

func (s *Server) appendHistoryLocked(record *IngestedBatch) {
	if s.history == nil {
		s.history = newHistoryRingBuffer(DefaultMaxHistoryBatches)
	}
	s.history.push(record)
}

// updateNodeTelemetryActivityLocked updates an existing node's LastSeen to ingestedAt
// and advances its SequenceNumber monotonically if the batch sequence number is greater.
// Must be called with s.mu held. If the node is not registered, no record is fabricated.
func (s *Server) updateNodeTelemetryActivityLocked(nodeID string, sequenceNumber int64, ingestedAt time.Time) {
	if s.nodes == nil {
		return
	}
	record, exists := s.nodes[nodeID]
	if !exists || record == nil {
		return
	}
	if !ingestedAt.Before(record.LastSeen) {
		record.LastSeen = ingestedAt
	}
	if sequenceNumber > record.SequenceNumber {
		record.SequenceNumber = sequenceNumber
	}
}

// handleListBatches returns ingested batches currently retained in the history window (admin only when auth enabled).
func (s *Server) handleListBatches(w http.ResponseWriter, r *http.Request) {
	authCtx, err := s.AuthenticateRequest(r, nil)
	if err != nil {
		s.writeAuthError(w, err)
		return
	}

	s.mu.RLock()
	authEnabled := s.authConfig.Enabled
	s.mu.RUnlock()

	if authEnabled && !authCtx.IsAdmin {
		s.writeJSON(w, http.StatusForbidden, ErrorResponse{
			Error:   "forbidden",
			Message: "administrative authorization required to list telemetry batches",
		})
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	s.writeJSON(w, http.StatusOK, s.history.snapshot())
}

// GetIngestedBatches returns a copy of all ingested batches currently retained in the history window in chronological order.
func (s *Server) GetIngestedBatches() []*IngestedBatch {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.history.snapshot()
}

// Count returns the number of uniquely ingested batch keys in the deduplication store.
func (s *Server) Count() int {
	if s.store != nil {
		c, err := s.store.Count(context.Background())
		if err == nil {
			return c
		}
	}
	return 0
}

// HistoryCount returns the current count of batch payloads retained in the history buffer.
func (s *Server) HistoryCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.history.len()
}

// MaxHistorySize returns the configured maximum payload history capacity.
func (s *Server) MaxHistorySize() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.history.cap()
}

// SetMaxHistorySize dynamically updates and enforces the history buffer capacity.
func (s *Server) SetMaxHistorySize(size int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.history.resize(size)
}

// HasIngestedBatch reports whether a batch for the specified node and batch ID has been ingested.
func (s *Server) HasIngestedBatch(nodeID, batchID string) bool {
	if s.store != nil {
		has, err := s.store.HasBatch(context.Background(), nodeID, batchID)
		if err == nil {
			return has
		}
	}
	return false
}

// IngestBatch programmatically ingests a validated telemetry batch.
// Used by the NATS consumer to share the same durable idempotency boundary as HTTP.
//
// Returns nil on successful first ingestion.
// Returns ErrDuplicateBatch if the batch has already been ingested (idempotent no-op).
func (s *Server) IngestBatch(ctx context.Context, batch *types.TelemetryBatch) error {
	if err := batch.Validate(); err != nil {
		return fmt.Errorf("batch validation failed: %w", err)
	}

	unlockNode := s.lockNode(batch.NodeID)
	defer unlockNode()

	ingestedAt := time.Now().UTC()

	isNew, err := s.store.RecordBatch(ctx, batch, ingestedAt)
	if err != nil {
		s.logger.Error("failed to record batch in durable store",
			slog.String("batch_id", batch.BatchID),
			slog.String("node_id", batch.NodeID),
			slog.Any("error", err),
		)
		return fmt.Errorf("failed to persist telemetry batch: %w", err)
	}

	if !isNew {
		s.logger.Info("duplicate batch ingestion (idempotent no-op via NATS consumer)",
			slog.String("batch_id", batch.BatchID),
			slog.String("node_id", batch.NodeID),
		)
		return ErrDuplicateBatch
	}

	record := &IngestedBatch{
		Batch:      *batch,
		IngestedAt: ingestedAt,
	}
	s.mu.Lock()
	s.appendHistoryLocked(record)
	s.updateNodeTelemetryActivityLocked(batch.NodeID, batch.SequenceNumber, ingestedAt)
	s.mu.Unlock()

	s.logger.Info("telemetry batch ingested via NATS consumer",
		slog.String("batch_id", batch.BatchID),
		slog.String("node_id", batch.NodeID),
		slog.Int64("sequence_number", batch.SequenceNumber),
		slog.Time("ingested_at", ingestedAt),
	)

	return nil
}

// writeJSON writes a structured JSON response with proper Content-Type header.
func (s *Server) writeJSON(w http.ResponseWriter, statusCode int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		s.logger.Error("failed to write JSON response", slog.Any("error", err))
	}
}

// NodeRecord represents an edge node registered in the control plane.
// Aliased from storage.NodeRecord for durable persistence and domain consistency.
type NodeRecord = storage.NodeRecord

// RegistrationResponse represents the JSON response for node enrollment.
type RegistrationResponse struct {
	Status       string    `json:"status"` // "registered" or "already_registered"
	NodeID       string    `json:"node_id"`
	RegisteredAt time.Time `json:"registered_at"`
}

// HeartbeatResponse represents the JSON response for node heartbeat.
type HeartbeatResponse struct {
	Status    string    `json:"status"` // "acknowledged"
	NodeID    string    `json:"node_id"`
	Timestamp time.Time `json:"timestamp"`
}

func isValidNodeID(id string) bool {
	id = strings.TrimSpace(id)
	if len(id) < 3 || len(id) > 128 {
		return false
	}
	for _, ch := range id {
		if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_') {
			return false
		}
	}
	lower := strings.ToLower(id)
	if strings.Contains(lower, "secret") || strings.Contains(lower, "token") || strings.Contains(lower, "password") {
		return false
	}
	return true
}

func (s *Server) writeAuthError(w http.ResponseWriter, err error) {
	errType := "unauthorized"
	if errors.Is(err, types.ErrInvalidSignature) {
		errType = "invalid_signature"
	} else if errors.Is(err, types.ErrExpiredTimestamp) {
		errType = "expired_timestamp"
	} else if errors.Is(err, types.ErrFutureTimestamp) {
		errType = "future_timestamp"
	} else if errors.Is(err, types.ErrReplayDetected) {
		errType = "replay_detected"
	} else if errors.Is(err, types.ErrMissingAuthHeaders) {
		errType = "missing_auth_headers"
	}

	s.writeJSON(w, http.StatusUnauthorized, ErrorResponse{
		Error:   errType,
		Message: err.Error(),
	})
}

func (s *Server) handleNodeRegister(w http.ResponseWriter, r *http.Request) {
	if s.initErr != nil {
		s.writeJSON(w, http.StatusServiceUnavailable, ErrorResponse{
			Error:   "service_uninitialized",
			Message: fmt.Sprintf("server failed initialization: %v", s.initErr),
		})
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		s.writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{
			Error:   "method_not_allowed",
			Message: "node registration endpoint accepts POST only",
		})
		return
	}

	bodyBytes, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.maxPayloadSize))
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			s.writeJSON(w, http.StatusRequestEntityTooLarge, ErrorResponse{
				Error:   "payload_too_large",
				Message: fmt.Sprintf("request payload exceeds %d byte limit", s.maxPayloadSize),
			})
			return
		}
		s.writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "malformed_body",
			Message: fmt.Sprintf("failed to read request body: %v", err),
		})
		return
	}
	defer r.Body.Close()

	authCtx, err := s.AuthenticateRequest(r, bodyBytes)
	if err != nil {
		s.writeAuthError(w, err)
		return
	}

	var reg types.NodeRegistration
	decoder := json.NewDecoder(bytes.NewReader(bodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&reg); err != nil {
		s.writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "malformed_json",
			Message: fmt.Sprintf("failed to parse registration payload: %v", err),
		})
		return
	}

	if err := reg.Validate(); err != nil {
		s.writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "invalid_registration",
			Message: fmt.Sprintf("registration domain validation failed: %v", err),
		})
		return
	}

	if !isValidNodeID(reg.NodeID) {
		s.writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "invalid_node_id",
			Message: fmt.Sprintf("node_id %q does not conform to character and length constraints", reg.NodeID),
		})
		return
	}

	s.mu.RLock()
	authEnabled := s.authConfig.Enabled
	s.mu.RUnlock()

	if authEnabled && !authCtx.IsAdmin && authCtx.NodeID != reg.NodeID {
		s.writeJSON(w, http.StatusForbidden, ErrorResponse{
			Error:   "forbidden",
			Message: fmt.Sprintf("authenticated node %q cannot register as node %q", authCtx.NodeID, reg.NodeID),
		})
		return
	}

	unlockNode := s.lockNode(reg.NodeID)
	defer unlockNode()

	now := time.Now().UTC()
	metaTime := now
	if reg.UpdatedAt != nil && !reg.UpdatedAt.IsZero() {
		metaTime = reg.UpdatedAt.UTC()
	} else if !reg.RegisteredAt.IsZero() {
		metaTime = reg.RegisteredAt.UTC()
	}

	regAt := now
	if !reg.RegisteredAt.IsZero() {
		regAt = reg.RegisteredAt.UTC()
	}

	record := &storage.NodeRecord{
		NodeID:          reg.NodeID,
		Status:          types.NodeStatusHealthy,
		LastSeen:        metaTime,
		RegisteredAt:    regAt,
		UpdatedAt:       metaTime,
		MetadataVersion: reg.MetadataVersion,
		AgentVersion:    reg.AgentVersion,
		ProtocolVersion: "v1",
		Hostname:        reg.Hostname,
		OS:              reg.OS,
		Architecture:    reg.Architecture,
		Capabilities:    reg.Capabilities,
		Labels:          reg.Labels,
	}

	// 1. Persist to durable store first (without holding s.mu across disk I/O)
	var isNew bool
	if s.store != nil {
		var err error
		isNew, err = s.store.RegisterNode(r.Context(), record)
		if err != nil {
			s.logger.Error("failed to persist node registration in durable store",
				slog.String("node_id", reg.NodeID),
				slog.Any("error", err),
			)
			s.writeJSON(w, http.StatusInternalServerError, ErrorResponse{
				Error:   "internal_server_error",
				Message: "failed to persist node registration",
			})
			return
		}
	} else {
		s.mu.RLock()
		_, inMemExists := s.nodes[reg.NodeID]
		s.mu.RUnlock()
		isNew = !inMemExists
	}

	// 2. Reconcile in-memory state under lock (only after successful durable storage)
	s.mu.Lock()
	defer s.mu.Unlock()

	if isNew {
		s.nodes[reg.NodeID] = record
		s.nodesList = append(s.nodesList, record)

		s.logger.Info("edge node successfully registered",
			slog.String("node_id", reg.NodeID),
			slog.Time("registered_at", record.RegisteredAt),
		)
		s.writeJSON(w, http.StatusCreated, RegistrationResponse{
			Status:       "registered",
			NodeID:       reg.NodeID,
			RegisteredAt: record.RegisteredAt,
		})
		return
	}

	// Duplicate registration: update existing in-memory copy with merged values from record
	existing, exists := s.nodes[reg.NodeID]
	if exists && existing != nil {
		existMetaTime := existing.UpdatedAt
		if existMetaTime.IsZero() {
			existMetaTime = existing.RegisteredAt
		}

		var shouldAdvanceMetadata bool
		if record.MetadataVersion > existing.MetadataVersion {
			shouldAdvanceMetadata = true
		} else if record.MetadataVersion < existing.MetadataVersion {
			shouldAdvanceMetadata = false
		} else {
			if record.UpdatedAt.After(existMetaTime) {
				shouldAdvanceMetadata = true
			} else if record.UpdatedAt.Before(existMetaTime) {
				shouldAdvanceMetadata = false
			} else {
				shouldAdvanceMetadata = true
			}
		}

		if shouldAdvanceMetadata {
			existing.MetadataVersion = record.MetadataVersion
			existing.UpdatedAt = record.UpdatedAt
			if record.Hostname != "" {
				existing.Hostname = record.Hostname
			}
			if record.AgentVersion != "" {
				existing.AgentVersion = record.AgentVersion
			}
			if record.ProtocolVersion != "" {
				existing.ProtocolVersion = record.ProtocolVersion
			}
			if record.OS != "" {
				existing.OS = record.OS
			}
			if record.Architecture != "" {
				existing.Architecture = record.Architecture
			}
			if len(record.Capabilities) > 0 {
				existing.Capabilities = record.Capabilities
			}
			if len(record.Labels) > 0 {
				existing.Labels = record.Labels
			}
		}

		if record.LastSeen.After(existing.LastSeen) {
			existing.LastSeen = record.LastSeen
		}
		if record.SequenceNumber > existing.SequenceNumber {
			existing.SequenceNumber = record.SequenceNumber
		}
		if record.HeartbeatSequence > existing.HeartbeatSequence {
			existing.HeartbeatSequence = record.HeartbeatSequence
		}
	} else {
		s.nodes[reg.NodeID] = record
		s.nodesList = append(s.nodesList, record)
		existing = record
	}

	s.logger.Info("duplicate node registration received (idempotent update)",
		slog.String("node_id", reg.NodeID),
	)
	s.writeJSON(w, http.StatusOK, RegistrationResponse{
		Status:       "already_registered",
		NodeID:       reg.NodeID,
		RegisteredAt: existing.RegisteredAt,
	})
}

func (s *Server) handleNodeHeartbeat(w http.ResponseWriter, r *http.Request) {
	if s.initErr != nil {
		s.writeJSON(w, http.StatusServiceUnavailable, ErrorResponse{
			Error:   "service_uninitialized",
			Message: fmt.Sprintf("server failed initialization: %v", s.initErr),
		})
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		s.writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{
			Error:   "method_not_allowed",
			Message: "node heartbeat endpoint accepts POST only",
		})
		return
	}

	bodyBytes, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.maxPayloadSize))
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			s.writeJSON(w, http.StatusRequestEntityTooLarge, ErrorResponse{
				Error:   "payload_too_large",
				Message: fmt.Sprintf("request payload exceeds %d byte limit", s.maxPayloadSize),
			})
			return
		}
		s.writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "malformed_body",
			Message: fmt.Sprintf("failed to read heartbeat body: %v", err),
		})
		return
	}
	defer r.Body.Close()

	authCtx, err := s.AuthenticateRequest(r, bodyBytes)
	if err != nil {
		s.writeAuthError(w, err)
		return
	}

	var hb types.Heartbeat
	decoder := json.NewDecoder(bytes.NewReader(bodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&hb); err != nil {
		s.writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "malformed_json",
			Message: fmt.Sprintf("failed to parse heartbeat payload: %v", err),
		})
		return
	}

	if err := hb.Validate(); err != nil {
		s.writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "invalid_heartbeat",
			Message: fmt.Sprintf("heartbeat domain validation failed: %v", err),
		})
		return
	}

	s.mu.RLock()
	authEnabled := s.authConfig.Enabled
	s.mu.RUnlock()

	if authEnabled && !authCtx.IsAdmin && authCtx.NodeID != hb.NodeID {
		s.writeJSON(w, http.StatusForbidden, ErrorResponse{
			Error:   "forbidden",
			Message: fmt.Sprintf("authenticated node %q cannot send heartbeat for node %q", authCtx.NodeID, hb.NodeID),
		})
		return
	}

	unlockNode := s.lockNode(hb.NodeID)
	defer unlockNode()

	now := time.Now().UTC()
	if !hb.Timestamp.IsZero() {
		now = hb.Timestamp.UTC()
	}

	// 1. Persist to durable store first (without holding s.mu across disk I/O)
	if s.store != nil {
		err := s.store.UpdateNodeHeartbeat(r.Context(), hb.NodeID, hb.Status, hb.SequenceNumber, now)
		if err != nil {
			if errors.Is(err, storage.ErrNodeNotFound) {
				s.writeJSON(w, http.StatusNotFound, ErrorResponse{
					Error:   "node_not_found",
					Message: fmt.Sprintf("node %q is not registered with control plane", hb.NodeID),
				})
				return
			}
			if errors.Is(err, storage.ErrCorruptNodeRecord) {
				s.logger.Error("corrupt node record encountered during heartbeat update",
					slog.String("node_id", hb.NodeID),
					slog.Any("error", err),
				)
				s.writeJSON(w, http.StatusInternalServerError, ErrorResponse{
					Error:   "corrupt_node_record",
					Message: "stored node record is corrupted",
				})
				return
			}
			s.logger.Error("failed to update node heartbeat in durable store",
				slog.String("node_id", hb.NodeID),
				slog.Any("error", err),
			)
			s.writeJSON(w, http.StatusInternalServerError, ErrorResponse{
				Error:   "internal_server_error",
				Message: "failed to persist node heartbeat",
			})
			return
		}
	} else {
		s.mu.RLock()
		_, exists := s.nodes[hb.NodeID]
		s.mu.RUnlock()
		if !exists {
			s.writeJSON(w, http.StatusNotFound, ErrorResponse{
				Error:   "node_not_found",
				Message: fmt.Sprintf("node %q is not registered with control plane", hb.NodeID),
			})
			return
		}
	}

	// 2. Reconcile in-memory state under lock (only after successful durable storage)
	s.mu.Lock()
	defer s.mu.Unlock()

	record, exists := s.nodes[hb.NodeID]
	if !exists {
		record = &storage.NodeRecord{
			NodeID:            hb.NodeID,
			Status:            hb.Status,
			LastSeen:          now,
			RegisteredAt:      now,
			HeartbeatSequence: hb.SequenceNumber,
		}
		s.nodes[hb.NodeID] = record
		s.nodesList = append(s.nodesList, record)
	} else {
		// Ordering policy within heartbeat domain:
		// 1. Strictly newer timestamp advances status and last_seen
		// 2. Equal timestamp uses heartbeat sequence tie-breaker
		if now.After(record.LastSeen) {
			record.LastSeen = now
			record.Status = hb.Status
		} else if now.Equal(record.LastSeen) && hb.SequenceNumber >= record.HeartbeatSequence {
			record.LastSeen = now
			record.Status = hb.Status
		}
		if hb.SequenceNumber > record.HeartbeatSequence {
			record.HeartbeatSequence = hb.SequenceNumber
		}
	}

	s.logger.Debug("node heartbeat acknowledged",
		slog.String("node_id", hb.NodeID),
		slog.String("status", string(hb.Status)),
		slog.Int64("sequence_number", record.SequenceNumber),
		slog.Int64("heartbeat_sequence", record.HeartbeatSequence),
	)

	s.writeJSON(w, http.StatusOK, HeartbeatResponse{
		Status:    "acknowledged",
		NodeID:    hb.NodeID,
		Timestamp: now,
	})
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	if s.initErr != nil {
		s.writeJSON(w, http.StatusServiceUnavailable, ErrorResponse{
			Error:   "service_uninitialized",
			Message: fmt.Sprintf("server failed initialization: %v", s.initErr),
		})
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		s.writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{
			Error:   "method_not_allowed",
			Message: "node query endpoint accepts GET only",
		})
		return
	}

	authCtx, err := s.AuthenticateRequest(r, nil)
	if err != nil {
		s.writeAuthError(w, err)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/v1/nodes")
	path = strings.TrimPrefix(path, "/")

	s.mu.RLock()
	authEnabled := s.authConfig.Enabled
	s.mu.RUnlock()

	if path == "" {
		// List all nodes: admin only
		if authEnabled && !authCtx.IsAdmin {
			s.writeJSON(w, http.StatusForbidden, ErrorResponse{
				Error:   "forbidden",
				Message: "administrative authorization required to list all nodes",
			})
			return
		}

		s.mu.RLock()
		defer s.mu.RUnlock()
		s.writeJSON(w, http.StatusOK, s.nodesList)
		return
	}

	// Single node query: path is node_id
	if authEnabled && !authCtx.IsAdmin && authCtx.NodeID != path {
		s.writeJSON(w, http.StatusForbidden, ErrorResponse{
			Error:   "forbidden",
			Message: fmt.Sprintf("authenticated node %q cannot inspect record for node %q", authCtx.NodeID, path),
		})
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	record, exists := s.nodes[path]
	if !exists {
		s.writeJSON(w, http.StatusNotFound, ErrorResponse{
			Error:   "node_not_found",
			Message: fmt.Sprintf("node %q not found in registry", path),
		})
		return
	}

	s.writeJSON(w, http.StatusOK, record)
}

// GetNode retrieves a registered node from memory.
func (s *Server) GetNode(nodeID string) (*NodeRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.nodes[nodeID]
	if !ok {
		return nil, false
	}
	cp := *rec
	return &cp, true
}

// GetNodes returns all registered nodes from memory.
func (s *Server) GetNodes() []*NodeRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*NodeRecord, len(s.nodesList))
	for i, n := range s.nodesList {
		cp := *n
		out[i] = &cp
	}
	return out
}

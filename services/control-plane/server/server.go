package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

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

// Server provides the HTTP ingestion endpoints for AegisEdge telemetry.
//
// NOTE (Phase 3 In-Memory Limitation):
// In Phase 3, this server stores ingested batches strictly in-memory.
// Accepted batches survive only for the lifetime of the control-plane process.
// If the process restarts, its accepted set is reset. The edge retains its
// local SQLite records until synchronization is confirmed. Persistent control-plane
// storage and brokers are scheduled for later phases.
type Server struct {
	mu             sync.RWMutex
	ingested       map[string]*IngestedBatch
	ingestedList   []*IngestedBatch
	nodes          map[string]*NodeRecord
	nodesList      []*NodeRecord
	logger         *slog.Logger
	maxPayloadSize int64
}

// NewServer initializes an in-memory control-plane HTTP ingestion server.
func NewServer(logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}))
	}
	return &Server{
		ingested:       make(map[string]*IngestedBatch),
		ingestedList:   make([]*IngestedBatch, 0),
		nodes:          make(map[string]*NodeRecord),
		nodesList:      make([]*NodeRecord, 0),
		logger:         logger,
		maxPayloadSize: 1024 * 1024, // 1MB payload limit
	}
}

// Routes constructs and returns the HTTP request multiplexer.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/api/v1/telemetry/batches", s.handleTelemetryBatches)
	mux.HandleFunc("/api/v1/nodes/register", s.handleNodeRegister)
	mux.HandleFunc("/api/v1/nodes/heartbeat", s.handleNodeHeartbeat)
	mux.HandleFunc("/api/v1/nodes/", s.handleNodes)
	mux.HandleFunc("/api/v1/nodes", s.handleNodes)
	return mux
}

// handleHealthz responds with basic service health status.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		s.writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{
			Error:   "method_not_allowed",
			Message: "health check endpoint accepts GET only",
		})
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]string{
		"status": "healthy",
	})
}

// handleTelemetryBatches processes batch ingestion (POST) and batch queries (GET).
func (s *Server) handleTelemetryBatches(w http.ResponseWriter, r *http.Request) {
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
	r.Body = http.MaxBytesReader(w, r.Body, s.maxPayloadSize)
	defer r.Body.Close()

	// 2. Decode JSON body
	var batch types.TelemetryBatch
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&batch); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			s.writeJSON(w, http.StatusRequestEntityTooLarge, ErrorResponse{
				Error:   "payload_too_large",
				Message: fmt.Sprintf("request payload exceeds %d byte limit", s.maxPayloadSize),
			})
			return
		}
		s.writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "malformed_json",
			Message: fmt.Sprintf("failed to parse JSON payload: %v", err),
		})
		return
	}

	// 3. Domain validation
	if err := batch.Validate(); err != nil {
		s.writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "invalid_batch",
			Message: fmt.Sprintf("batch domain validation failed: %v", err),
		})
		return
	}

	// 4. Idempotency Check & Ingestion
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.ingested[batch.BatchID]; exists {
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

	// Record new batch with control plane ingestion timestamp
	ingestedAt := time.Now().UTC()
	record := &IngestedBatch{
		Batch:      batch,
		IngestedAt: ingestedAt,
	}

	s.ingested[batch.BatchID] = record
	s.ingestedList = append(s.ingestedList, record)

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

// handleListBatches returns all ingested batches in memory.
func (s *Server) handleListBatches(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	s.writeJSON(w, http.StatusOK, s.ingestedList)
}

// GetIngestedBatches returns a copy of all ingested batches for testing/inspection.
func (s *Server) GetIngestedBatches() []*IngestedBatch {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]*IngestedBatch, len(s.ingestedList))
	copy(out, s.ingestedList)
	return out
}

// Count returns the number of uniquely ingested batches.
func (s *Server) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.ingested)
}

// IngestBatch programmatically ingests a validated telemetry batch.
// Used by the NATS consumer to share the same in-memory idempotency boundary as HTTP.
//
// Returns nil on successful first ingestion.
// Returns ErrDuplicateBatch if the batch has already been ingested (idempotent no-op).
//
// Limitation (Phase 4.3):
// Duplicate detection is idempotent during the lifetime of the current control-plane process.
// Persistent duplicate detection across control-plane restarts is NOT provided by the
// current in-memory ingestion boundary and is deferred to a future persistent ingestion layer.
func (s *Server) IngestBatch(_ context.Context, batch *types.TelemetryBatch) error {
	if err := batch.Validate(); err != nil {
		return fmt.Errorf("batch validation failed: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.ingested[batch.BatchID]; exists {
		s.logger.Info("duplicate batch ingestion (idempotent no-op via NATS consumer)",
			slog.String("batch_id", batch.BatchID),
			slog.String("node_id", batch.NodeID),
		)
		return ErrDuplicateBatch
	}

	ingestedAt := time.Now().UTC()
	record := &IngestedBatch{
		Batch:      *batch,
		IngestedAt: ingestedAt,
	}

	s.ingested[batch.BatchID] = record
	s.ingestedList = append(s.ingestedList, record)

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
type NodeRecord struct {
	NodeID          string            `json:"node_id"`
	Status          types.NodeStatus  `json:"status"`
	LastSeen        time.Time         `json:"last_seen"`
	RegisteredAt    time.Time         `json:"registered_at"`
	AgentVersion    string            `json:"agent_version,omitempty"`
	ProtocolVersion string            `json:"protocol_version,omitempty"`
	Hostname        string            `json:"hostname,omitempty"`
	OS              string            `json:"os,omitempty"`
	Architecture    string            `json:"architecture,omitempty"`
	Capabilities    []string          `json:"capabilities,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
	SequenceNumber  int64             `json:"sequence_number,omitempty"`
}

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

func (s *Server) handleNodeRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		s.writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{
			Error:   "method_not_allowed",
			Message: "node registration endpoint accepts POST only",
		})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, s.maxPayloadSize)
	defer r.Body.Close()

	var reg types.NodeRegistration
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&reg); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			s.writeJSON(w, http.StatusRequestEntityTooLarge, ErrorResponse{
				Error:   "payload_too_large",
				Message: fmt.Sprintf("request payload exceeds %d byte limit", s.maxPayloadSize),
			})
			return
		}
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

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	if existing, exists := s.nodes[reg.NodeID]; exists {
		// Idempotent duplicate registration: update last seen & metadata
		existing.LastSeen = now
		if reg.Hostname != "" {
			existing.Hostname = reg.Hostname
		}
		if reg.AgentVersion != "" {
			existing.AgentVersion = reg.AgentVersion
		}
		if len(reg.Capabilities) > 0 {
			existing.Capabilities = reg.Capabilities
		}
		if len(reg.Labels) > 0 {
			existing.Labels = reg.Labels
		}

		s.logger.Info("duplicate node registration received (idempotent update)",
			slog.String("node_id", reg.NodeID),
		)
		s.writeJSON(w, http.StatusOK, RegistrationResponse{
			Status:       "already_registered",
			NodeID:       reg.NodeID,
			RegisteredAt: existing.RegisteredAt,
		})
		return
	}

	record := &NodeRecord{
		NodeID:       reg.NodeID,
		Status:       types.NodeStatusHealthy,
		LastSeen:     now,
		RegisteredAt: now,
		AgentVersion: reg.AgentVersion,
		Hostname:     reg.Hostname,
		OS:           reg.OS,
		Architecture: reg.Architecture,
		Capabilities: reg.Capabilities,
		Labels:       reg.Labels,
	}

	s.nodes[reg.NodeID] = record
	s.nodesList = append(s.nodesList, record)

	s.logger.Info("edge node successfully registered",
		slog.String("node_id", reg.NodeID),
		slog.Time("registered_at", now),
	)

	s.writeJSON(w, http.StatusCreated, RegistrationResponse{
		Status:       "registered",
		NodeID:       reg.NodeID,
		RegisteredAt: now,
	})
}

func (s *Server) handleNodeHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		s.writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{
			Error:   "method_not_allowed",
			Message: "node heartbeat endpoint accepts POST only",
		})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, s.maxPayloadSize)
	defer r.Body.Close()

	var hb types.Heartbeat
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&hb); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			s.writeJSON(w, http.StatusRequestEntityTooLarge, ErrorResponse{
				Error:   "payload_too_large",
				Message: fmt.Sprintf("request payload exceeds %d byte limit", s.maxPayloadSize),
			})
			return
		}
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

	s.mu.Lock()
	defer s.mu.Unlock()

	record, exists := s.nodes[hb.NodeID]
	if !exists {
		s.writeJSON(w, http.StatusNotFound, ErrorResponse{
			Error:   "node_not_found",
			Message: fmt.Sprintf("node %q is not registered with control plane", hb.NodeID),
		})
		return
	}

	now := time.Now().UTC()
	record.LastSeen = now
	record.Status = hb.Status
	record.SequenceNumber = hb.SequenceNumber

	s.logger.Debug("node heartbeat acknowledged",
		slog.String("node_id", hb.NodeID),
		slog.String("status", string(hb.Status)),
		slog.Int64("sequence_number", hb.SequenceNumber),
	)

	s.writeJSON(w, http.StatusOK, HeartbeatResponse{
		Status:    "acknowledged",
		NodeID:    hb.NodeID,
		Timestamp: now,
	})
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		s.writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{
			Error:   "method_not_allowed",
			Message: "node query endpoint accepts GET only",
		})
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/v1/nodes")
	path = strings.TrimPrefix(path, "/")

	s.mu.RLock()
	defer s.mu.RUnlock()

	if path == "" {
		// List all nodes
		s.writeJSON(w, http.StatusOK, s.nodesList)
		return
	}

	// Single node query: path is node_id
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

package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

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
		logger:         logger,
		maxPayloadSize: 1024 * 1024, // 1MB payload limit
	}
}

// Routes constructs and returns the HTTP request multiplexer.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/api/v1/telemetry/batches", s.handleTelemetryBatches)
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

// writeJSON writes a structured JSON response with proper Content-Type header.
func (s *Server) writeJSON(w http.ResponseWriter, statusCode int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		s.logger.Error("failed to write JSON response", slog.Any("error", err))
	}
}

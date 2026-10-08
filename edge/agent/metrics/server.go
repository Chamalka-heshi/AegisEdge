package metrics

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/health"
)

// DefaultServerAddr is the default local loopback bind address for metrics exposition.
const DefaultServerAddr = "127.0.0.1:9091"

// Server provides a dedicated, read-only HTTP exposition server for Prometheus scrapers and health checks.
type Server struct {
	mu            sync.Mutex
	server        *http.Server
	ln            net.Listener
	addr          string
	reg           *Registry
	healthHandler *health.Handler
}

// NewServer constructs a Server exposing the given Registry.
// If addr is empty, DefaultServerAddr ("127.0.0.1:9091") is used.
func NewServer(addr string, reg *Registry) *Server {
	if addr == "" {
		addr = DefaultServerAddr
	}
	if reg == nil {
		reg = NewRegistry()
	}

	s := &Server{
		addr: addr,
		reg:  reg,
	}

	mux := http.NewServeMux()

	// 1. Prometheus metrics exposition endpoint
	mux.Handle("/metrics", reg.Handler())

	// 2. Local health check endpoint
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		hh := s.healthHandler
		s.mu.Unlock()
		if hh != nil {
			hh.Healthz(w, r)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	// 3. Local readiness check endpoint
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		hh := s.healthHandler
		s.mu.Unlock()
		if hh != nil {
			hh.Readyz(w, r)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	s.server = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}

	return s
}

// SetHealthTracker attaches a health.Tracker to provide /healthz and /readyz endpoints.
func (s *Server) SetHealthTracker(tracker health.Tracker) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if tracker != nil {
		s.healthHandler = health.NewHandler(tracker)
	} else {
		s.healthHandler = nil
	}
}

// Start binds to the configured network address and begins serving HTTP requests in the background.
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ln != nil {
		return errors.New("metrics server already started")
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.ln = ln

	go func() {
		_ = s.server.Serve(ln)
	}()

	return nil
}

// Stop gracefully shuts down the metrics server.
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ln == nil {
		return nil
	}
	err := s.server.Shutdown(ctx)
	s.ln = nil
	return err
}

// Addr returns the configured or active bind address of the server.
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln != nil {
		return s.ln.Addr().String()
	}
	return s.addr
}

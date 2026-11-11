// Package main implements a testable HTTP webhook receiver and mock metrics exporter
// for end-to-end validation of Prometheus -> Alertmanager -> Webhook delivery.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxRequestBodyBytes = 1 << 20 // 1 MiB bounded payload
	maxStoredEvents     = 1000    // Ring-buffer cap to avoid unbounded accumulation
	maxInstancesLimit   = 10      // Safety limit on multi-instance synthetic generation
	minAuthTokenLength  = 16      // Minimum required characters for valid Bearer token
)

// WebhookAlert represents an alert in the Alertmanager webhook payload.
type WebhookAlert struct {
	Status       string            `json:"status"`
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	StartsAt     time.Time         `json:"startsAt"`
	EndsAt       time.Time         `json:"endsAt"`
	GeneratorURL string            `json:"generatorURL"`
	Fingerprint  string            `json:"fingerprint"`
}

// WebhookMessage represents the Alertmanager webhook delivery payload.
type WebhookMessage struct {
	Version           string            `json:"version"`
	GroupKey          string            `json:"groupKey"`
	TruncatedAlerts   int               `json:"truncatedAlerts"`
	Status            string            `json:"status"`
	Receiver          string            `json:"receiver"`
	GroupLabels       map[string]string `json:"groupLabels"`
	CommonLabels      map[string]string `json:"commonLabels"`
	CommonAnnotations map[string]string `json:"commonAnnotations"`
	ExternalURL       string            `json:"externalURL"`
	Alerts            []WebhookAlert    `json:"alerts"`
}

// RecordedEvent records a webhook delivery with route tag and arrival timestamp.
type RecordedEvent struct {
	ID       int            `json:"id"`
	Route    string         `json:"route"`
	Received time.Time      `json:"received"`
	Payload  WebhookMessage `json:"payload"`
}

type ServerState struct {
	mu             sync.RWMutex
	metricValue    int64
	instances      int
	runID          string
	inhibitionMode bool
	inhibitionVal  int64
	events         []RecordedEvent
	nextEventID    int
}

func NewServerState() *ServerState {
	return &ServerState{
		metricValue:    0,
		instances:      1,
		runID:          "",
		inhibitionMode: false,
		inhibitionVal:  0,
		events:         make([]RecordedEvent, 0),
		nextEventID:    1,
	}
}

// requireAuth returns an http.HandlerFunc wrapper enforcing Bearer token authentication
// when authToken is non-empty. If authToken is empty, all requests pass through.
func requireAuth(authToken string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if authToken != "" {
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "missing authorization header"})
				return
			}
			parts := strings.SplitN(authHeader, " ", 2)
			if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") || parts[1] != authToken {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid bearer token"})
				return
			}
		}
		next(w, r)
	}
}

// NewHandler constructs and returns the HTTP handler mux for the webhook receiver and mock exporter without authentication.
func NewHandler(state *ServerState) http.Handler {
	return NewHandlerWithAuth(state, "")
}

// NewHandlerWithAuth constructs the HTTP handler mux enforcing Bearer token authentication on protected endpoints.
func NewHandlerWithAuth(state *ServerState, authToken string) http.Handler {
	mux := http.NewServeMux()

	// Health check (unauthenticated GET)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK\n"))
	})

	// Prometheus metrics exposition for synthetic test metrics (unauthenticated GET for scraper)
	mux.HandleFunc("/test-metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		state.mu.RLock()
		inhibitMode := state.inhibitionMode
		inhibitVal := state.inhibitionVal
		val := state.metricValue
		inst := state.instances
		runID := state.runID
		state.mu.RUnlock()

		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, "# HELP aegisedge_lifecycle_test_metric Synthetic metric for E2E alert lifecycle validation\n")
		_, _ = fmt.Fprintf(w, "# TYPE aegisedge_lifecycle_test_metric gauge\n")

		if inhibitMode {
			if runID != "" {
				_, _ = fmt.Fprintf(w, "aegisedge_lifecycle_test_metric{alert_family=\"lifecycle\",alert_type=\"warning\",tier=\"integration-test\",test_run_id=%q} %d\n", runID, inhibitVal)
				_, _ = fmt.Fprintf(w, "aegisedge_lifecycle_test_metric{alert_family=\"lifecycle\",alert_type=\"critical\",tier=\"integration-test\",test_run_id=%q} %d\n", runID, inhibitVal)
				_, _ = fmt.Fprintf(w, "aegisedge_lifecycle_test_metric{alert_family=\"unrelated\",alert_type=\"warning\",tier=\"integration-test\",test_run_id=%q} %d\n", runID, inhibitVal)
			} else {
				_, _ = fmt.Fprintf(w, "aegisedge_lifecycle_test_metric{alert_family=\"lifecycle\",alert_type=\"warning\",tier=\"integration-test\"} %d\n", inhibitVal)
				_, _ = fmt.Fprintf(w, "aegisedge_lifecycle_test_metric{alert_family=\"lifecycle\",alert_type=\"critical\",tier=\"integration-test\"} %d\n", inhibitVal)
				_, _ = fmt.Fprintf(w, "aegisedge_lifecycle_test_metric{alert_family=\"unrelated\",alert_type=\"warning\",tier=\"integration-test\"} %d\n", inhibitVal)
			}
			return
		}

		if inst > 1 {
			for i := 1; i <= inst; i++ {
				if runID != "" {
					_, _ = fmt.Fprintf(w, "aegisedge_lifecycle_test_metric{node=\"node-%c\",test_run_id=%q} %d\n", 'a'+i-1, runID, val)
				} else {
					_, _ = fmt.Fprintf(w, "aegisedge_lifecycle_test_metric{node=\"node-%c\"} %d\n", 'a'+i-1, val)
				}
			}
		} else {
			if runID != "" {
				_, _ = fmt.Fprintf(w, "aegisedge_lifecycle_test_metric{test_run_id=%q} %d\n", runID, val)
			} else {
				_, _ = fmt.Fprintf(w, "aegisedge_lifecycle_test_metric %d\n", val)
			}
		}
	})

	// Control API: update test metric value (protected by Bearer auth)
	mux.HandleFunc("/api/trigger", requireAuth(authToken, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		valStr := r.URL.Query().Get("val")
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid val: %v", err), http.StatusBadRequest)
			return
		}

		inst := 1
		if instStr := r.URL.Query().Get("instances"); instStr != "" {
			parsedInst, err := strconv.Atoi(instStr)
			if err != nil || parsedInst <= 0 || parsedInst > maxInstancesLimit {
				http.Error(w, fmt.Sprintf("invalid instances: must be an integer between 1 and %d", maxInstancesLimit), http.StatusBadRequest)
				return
			}
			inst = parsedInst
		}

		runID := strings.TrimSpace(r.URL.Query().Get("run_id"))

		state.mu.Lock()
		state.inhibitionMode = false
		state.metricValue = val
		state.instances = inst
		if runID != "" {
			state.runID = runID
		}
		currentRunID := state.runID
		state.mu.Unlock()

		log.Printf("[TRIGGER] metric updated to %d (instances=%d, run_id=%q)", val, inst, currentRunID)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"metricValue": val,
			"instances":   inst,
			"runId":       currentRunID,
		})
	}))

	// Control API: update simultaneous inhibition test metrics (protected by Bearer auth)
	mux.HandleFunc("/api/trigger/inhibition", requireAuth(authToken, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		valStr := r.URL.Query().Get("val")
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid val: %v", err), http.StatusBadRequest)
			return
		}
		runID := strings.TrimSpace(r.URL.Query().Get("run_id"))

		state.mu.Lock()
		state.inhibitionMode = true
		state.inhibitionVal = val
		if runID != "" {
			state.runID = runID
		}
		currentRunID := state.runID
		state.mu.Unlock()

		log.Printf("[TRIGGER-INHIBITION] simultaneous metrics updated to %d (run_id=%q)", val, currentRunID)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"inhibitionMode": true,
			"metricValue":    val,
			"runId":          currentRunID,
		})
	}))

	// Webhook handler factory with bounded read, validation, event cap, and Bearer auth
	handleWebhook := func(route string) http.HandlerFunc {
		return requireAuth(authToken, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
				return
			}
			r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
			body, err := io.ReadAll(r.Body)
			if err != nil {
				var maxBytesErr *http.MaxBytesError
				if errors.As(err, &maxBytesErr) || strings.Contains(strings.ToLower(err.Error()), "request body too large") {
					log.Printf("[WEBHOOK-ERR] payload exceeded maximum allowed size of %d bytes", maxRequestBodyBytes)
					http.Error(w, "Request entity too large", http.StatusRequestEntityTooLarge)
					return
				}
				http.Error(w, fmt.Sprintf("Failed to read body: %v", err), http.StatusBadRequest)
				return
			}
			var msg WebhookMessage
			if err := json.Unmarshal(body, &msg); err != nil {
				// Log diagnostic information only (error and body length) without logging body contents
				log.Printf("[WEBHOOK-ERR] json unmarshal failed: %v, body_bytes=%d", err, len(body))
				http.Error(w, "Invalid JSON", http.StatusBadRequest)
				return
			}

			// Validate essential Alertmanager webhook fields
			if msg.Version == "" || msg.Receiver == "" || msg.Status == "" {
				http.Error(w, "Invalid Alertmanager webhook payload: missing required fields", http.StatusBadRequest)
				return
			}

			state.mu.Lock()
			// Enforce bounded storage capacity (ring buffer eviction)
			if len(state.events) >= maxStoredEvents {
				state.events = state.events[1:]
			}
			event := RecordedEvent{
				ID:       state.nextEventID,
				Route:    route,
				Received: time.Now(),
				Payload:  msg,
			}
			state.nextEventID++
			state.events = append(state.events, event)
			state.mu.Unlock()

			log.Printf("[WEBHOOK-RECV] route=%s status=%s alerts_count=%d receiver=%s",
				route, msg.Status, len(msg.Alerts), msg.Receiver)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"received"}`))
		})
	}

	mux.HandleFunc("/alerts/default", handleWebhook("default"))
	mux.HandleFunc("/alerts/warning", handleWebhook("warning"))
	mux.HandleFunc("/alerts/critical", handleWebhook("critical"))

	// API to inspect and query received events (protected by Bearer auth)
	mux.HandleFunc("/api/events", requireAuth(authToken, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			state.mu.RLock()
			eventsCopy := make([]RecordedEvent, len(state.events))
			copy(eventsCopy, state.events)
			state.mu.RUnlock()

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(eventsCopy)
		case http.MethodDelete:
			state.mu.Lock()
			state.events = make([]RecordedEvent, 0)
			state.mu.Unlock()
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"cleared"}`))
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}))

	return mux
}

// loadAuthToken reads the Bearer token from the specified file path or WEBHOOK_AUTH_TOKEN_FILE env var.
// Returns an error if the path is provided but unreadable, or if the token is shorter than 16 characters.
func loadAuthToken(tokenFilePath string) (string, error) {
	if tokenFilePath == "" {
		tokenFilePath = os.Getenv("WEBHOOK_AUTH_TOKEN_FILE")
	}
	if tokenFilePath == "" {
		return "", nil
	}
	data, err := os.ReadFile(tokenFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to read auth token file at %s: %w", tokenFilePath, err)
	}
	token := strings.TrimSpace(string(data))
	if len(token) < minAuthTokenLength {
		return "", fmt.Errorf("auth token from %s is too short (minimum %d characters required, got %d)", tokenFilePath, minAuthTokenLength, len(token))
	}
	return token, nil
}

// ServerTimeouts holds HTTP server timeout parameters.
type ServerTimeouts struct {
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
}

// DefaultServerTimeouts defines the production-grade timeout configuration for the receiver.
var DefaultServerTimeouts = ServerTimeouts{
	ReadHeaderTimeout: 5 * time.Second,
	ReadTimeout:       10 * time.Second,
	WriteTimeout:      10 * time.Second,
	IdleTimeout:       60 * time.Second,
}

// NewServerWithTimeouts constructs an http.Server with custom timeout parameters.
func NewServerWithTimeouts(port int, handler http.Handler, timeouts ServerTimeouts) *http.Server {
	return &http.Server{
		Addr:              fmt.Sprintf("0.0.0.0:%d", port),
		Handler:           handler,
		ReadHeaderTimeout: timeouts.ReadHeaderTimeout,
		ReadTimeout:       timeouts.ReadTimeout,
		WriteTimeout:      timeouts.WriteTimeout,
		IdleTimeout:       timeouts.IdleTimeout,
	}
}

// NewServer creates an http.Server configured with production-grade timeouts.
func NewServer(port int, handler http.Handler) *http.Server {
	return NewServerWithTimeouts(port, handler, DefaultServerTimeouts)
}

func main() {
	port := flag.Int("port", 9095, "HTTP listen port")
	authTokenFile := flag.String("auth-token-file", "", "Path to file containing Bearer authentication token")
	flag.Parse()

	authToken, err := loadAuthToken(*authTokenFile)
	if err != nil {
		log.Fatalf("Fatal auth token configuration error: %v", err)
	}

	state := NewServerState()
	handler := NewHandlerWithAuth(state, authToken)
	server := NewServer(*port, handler)

	if authToken != "" {
		log.Printf("Starting AegisEdge Webhook Receiver on %s with Bearer auth enabled (token_length=%d)", server.Addr, len(authToken))
	} else {
		log.Printf("Starting AegisEdge Webhook Receiver on %s (unauthenticated mode)", server.Addr)
	}
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("Server exited with error: %v", err)
	}
}

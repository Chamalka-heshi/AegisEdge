package health

import (
	"encoding/json"
	"net/http"
	"time"
)

// Handler provides HTTP endpoints for liveness (/healthz) and readiness (/readyz).
type Handler struct {
	tracker Tracker
}

// NewHandler creates a Handler with the given health Tracker.
func NewHandler(tracker Tracker) *Handler {
	return &Handler{
		tracker: tracker,
	}
}

type healthzResponse struct {
	Status string       `json:"status"`
	State  ServiceState `json:"state"`
	Live   bool         `json:"live"`
}

type readyzResponse struct {
	Status    string                  `json:"status"`
	State     ServiceState            `json:"state"`
	Ready     bool                    `json:"ready"`
	Healthy   bool                    `json:"healthy"`
	Checks    map[CheckName]CheckInfo `json:"checks"`
	Timestamp time.Time               `json:"timestamp"`
}

// Healthz handles GET /healthz liveness probe.
func (h *Handler) Healthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	isLive := h.tracker.IsLive()
	state := h.tracker.State()

	resp := healthzResponse{
		State: state,
		Live:  isLive,
	}

	if isLive {
		resp.Status = "ok"
		w.WriteHeader(http.StatusOK)
	} else {
		resp.Status = "shutting_down"
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	if r.Method == http.MethodHead {
		return
	}

	_ = json.NewEncoder(w).Encode(resp)
}

// Readyz handles GET /readyz readiness probe.
func (h *Handler) Readyz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	snap := h.tracker.Snapshot()

	resp := readyzResponse{
		State:     snap.State,
		Ready:     snap.Ready,
		Healthy:   snap.Healthy,
		Checks:    snap.Checks,
		Timestamp: snap.Timestamp,
	}

	if snap.Ready {
		resp.Status = "ready"
		w.WriteHeader(http.StatusOK)
	} else {
		resp.Status = "not_ready"
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	if r.Method == http.MethodHead {
		return
	}

	_ = json.NewEncoder(w).Encode(resp)
}

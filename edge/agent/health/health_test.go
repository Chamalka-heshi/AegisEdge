package health_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/health"
)

// Scenario A: Initial state INITIALIZING
func TestScenarioA_InitialStateInitializing(t *testing.T) {
	tracker := health.NewTracker()
	if tracker.State() != health.StateInitializing {
		t.Fatalf("expected initial state %s, got %s", health.StateInitializing, tracker.State())
	}
	if !tracker.IsLive() {
		t.Fatalf("expected agent to be live during initializing")
	}
	if tracker.IsReady() {
		t.Fatalf("expected agent not to be ready during initializing")
	}
}

// Scenario B: Valid transition to READY
func TestScenarioB_ValidTransitionToReady(t *testing.T) {
	tracker := health.NewTracker()
	err := tracker.Transition(health.StateReady)
	if err != nil {
		t.Fatalf("expected valid transition to READY, got %v", err)
	}
	if tracker.State() != health.StateReady {
		t.Fatalf("expected state %s, got %s", health.StateReady, tracker.State())
	}
}

// Scenario C: Invalid transitions rejected
func TestScenarioC_InvalidTransitionsRejected(t *testing.T) {
	tracker := health.NewTracker()
	tracker.Shutdown() // Now in SHUTTING_DOWN

	// SHUTTING_DOWN -> READY should fail
	err := tracker.Transition(health.StateReady)
	if err != health.ErrInvalidTransition {
		t.Fatalf("expected ErrInvalidTransition, got %v", err)
	}

	// SHUTTING_DOWN -> INITIALIZING should fail
	err = tracker.Transition(health.StateInitializing)
	if err != health.ErrInvalidTransition {
		t.Fatalf("expected ErrInvalidTransition, got %v", err)
	}

	// Unknown state should fail
	err = tracker.Transition(health.ServiceState("INVALID_STATE"))
	if err != health.ErrInvalidTransition {
		t.Fatalf("expected ErrInvalidTransition for invalid state, got %v", err)
	}
}

// Scenario D: READY -> DEGRADED on required dependency failure
func TestScenarioD_ReadyToDegradedOnRequiredFailure(t *testing.T) {
	tracker := health.NewTracker()
	_ = tracker.RegisterCheck(health.CheckStorage, true)
	_ = tracker.SetCheckStatus(health.CheckStorage, health.StatusOk, "ready")
	_ = tracker.Transition(health.StateReady)

	if tracker.State() != health.StateReady {
		t.Fatalf("expected state READY, got %s", tracker.State())
	}

	// Storage fails
	_ = tracker.SetCheckStatus(health.CheckStorage, health.StatusFailed, "disk error")
	if tracker.State() != health.StateDegraded {
		t.Fatalf("expected automatic transition to DEGRADED, got %s", tracker.State())
	}
	if tracker.IsReady() {
		t.Fatalf("expected IsReady() to be false when degraded")
	}
}

// Scenario E: DEGRADED -> READY on recovery
func TestScenarioE_DegradedToReadyOnRecovery(t *testing.T) {
	tracker := health.NewTracker()
	_ = tracker.RegisterCheck(health.CheckStorage, true)
	_ = tracker.SetCheckStatus(health.CheckStorage, health.StatusFailed, "fail")
	_ = tracker.Transition(health.StateDegraded)

	if tracker.State() != health.StateDegraded {
		t.Fatalf("expected state DEGRADED, got %s", tracker.State())
	}

	// Storage recovers
	_ = tracker.SetCheckStatus(health.CheckStorage, health.StatusOk, "available")
	if tracker.State() != health.StateReady {
		t.Fatalf("expected recovery to READY, got %s", tracker.State())
	}
	if !tracker.IsReady() {
		t.Fatalf("expected IsReady() to be true after recovery")
	}
}

// Scenario F: Shutdown state explicit
func TestScenarioF_ShutdownStateExplicit(t *testing.T) {
	tracker := health.NewTracker()
	tracker.Shutdown()

	if tracker.State() != health.StateShuttingDown {
		t.Fatalf("expected state SHUTTING_DOWN, got %s", tracker.State())
	}
	if tracker.IsLive() {
		t.Fatalf("expected IsLive() to be false when shutting down")
	}
	if tracker.IsReady() {
		t.Fatalf("expected IsReady() to be false when shutting down")
	}
}

// Scenario G: Liveness independent from readiness
func TestScenarioG_LivenessIndependentFromReadiness(t *testing.T) {
	tracker := health.NewTracker()
	// StateInitializing
	if !tracker.IsLive() {
		t.Fatalf("expected live during INITIALIZING")
	}
	if tracker.IsReady() {
		t.Fatalf("expected not ready during INITIALIZING")
	}

	// Transition to DEGRADED
	_ = tracker.Transition(health.StateDegraded)
	if !tracker.IsLive() {
		t.Fatalf("expected live during DEGRADED")
	}
	if tracker.IsReady() {
		t.Fatalf("expected not ready during DEGRADED")
	}
}

// Scenario H: /healthz 200 while live but not ready
func TestScenarioH_Healthz200WhileLiveNotReady(t *testing.T) {
	tracker := health.NewTracker()
	handler := health.NewHandler(tracker)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	handler.Healthz(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for /healthz, got %d", rr.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode JSON response: %v", err)
	}
	if body["live"] != true {
		t.Fatalf("expected live=true, got %v", body["live"])
	}
}

// Scenario I: /readyz 503 while not ready
func TestScenarioI_Readyz503WhileNotReady(t *testing.T) {
	tracker := health.NewTracker() // starts INITIALIZING
	handler := health.NewHandler(tracker)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rr := httptest.NewRecorder()
	handler.Readyz(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable for /readyz when initializing, got %d", rr.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode JSON response: %v", err)
	}
	if body["ready"] != false {
		t.Fatalf("expected ready=false, got %v", body["ready"])
	}
}

// Scenario J: /readyz 200 when ready
func TestScenarioJ_Readyz200WhenReady(t *testing.T) {
	tracker := health.NewTracker()
	_ = tracker.RegisterCheck(health.CheckStorage, true)
	_ = tracker.SetCheckStatus(health.CheckStorage, health.StatusOk, "available")
	_ = tracker.Transition(health.StateReady)

	handler := health.NewHandler(tracker)
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rr := httptest.NewRecorder()
	handler.Readyz(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for /readyz when ready, got %d", rr.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode JSON response: %v", err)
	}
	if body["ready"] != true {
		t.Fatalf("expected ready=true, got %v", body["ready"])
	}
}

// Scenario K: POST /healthz -> 405
func TestScenarioK_PostHealthzMethodNotAllowed(t *testing.T) {
	tracker := health.NewTracker()
	handler := health.NewHandler(tracker)

	req := httptest.NewRequest(http.MethodPost, "/healthz", nil)
	rr := httptest.NewRecorder()
	handler.Healthz(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 Method Not Allowed, got %d", rr.Code)
	}
	if rr.Header().Get("Allow") != "GET, HEAD" {
		t.Fatalf("expected Allow: GET, HEAD header, got %s", rr.Header().Get("Allow"))
	}
}

// Scenario L: POST /readyz -> 405
func TestScenarioL_PostReadyzMethodNotAllowed(t *testing.T) {
	tracker := health.NewTracker()
	handler := health.NewHandler(tracker)

	req := httptest.NewRequest(http.MethodPost, "/readyz", nil)
	rr := httptest.NewRecorder()
	handler.Readyz(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 Method Not Allowed, got %d", rr.Code)
	}
	if rr.Header().Get("Allow") != "GET, HEAD" {
		t.Fatalf("expected Allow: GET, HEAD header, got %s", rr.Header().Get("Allow"))
	}
}

// Scenario M: Required dependency failure prevents readiness
func TestScenarioM_RequiredDependencyFailurePreventsReadiness(t *testing.T) {
	tracker := health.NewTracker()
	_ = tracker.RegisterCheck(health.CheckStorage, true)
	_ = tracker.RegisterCheck(health.CheckTelemetry, true)

	_ = tracker.SetCheckStatus(health.CheckStorage, health.StatusOk, "available")
	_ = tracker.SetCheckStatus(health.CheckTelemetry, health.StatusFailed, "buffer full")
	_ = tracker.Transition(health.StateReady)

	// Since CheckTelemetry is failed, state becomes DEGRADED and readiness is false
	if tracker.IsReady() {
		t.Fatalf("expected IsReady() to be false when required check failed")
	}
}

// Scenario N: Optional/disabled metrics does not fail readiness
func TestScenarioN_OptionalDisabledMetricsDoesNotFailReadiness(t *testing.T) {
	tracker := health.NewTracker()
	_ = tracker.RegisterCheck(health.CheckStorage, true)
	_ = tracker.RegisterCheck(health.CheckMetrics, false) // optional check

	_ = tracker.SetCheckStatus(health.CheckStorage, health.StatusOk, "available")
	_ = tracker.SetCheckStatus(health.CheckMetrics, health.StatusDegraded, "disabled")
	_ = tracker.Transition(health.StateReady)

	if !tracker.IsReady() {
		t.Fatalf("expected IsReady() to be true when optional check is degraded/disabled")
	}
}

// Scenario O: Unknown/invalid check name rejected
func TestScenarioO_UnknownCheckNameRejected(t *testing.T) {
	tracker := health.NewTracker()
	err := tracker.RegisterCheck(health.CheckName("disallowed_subsystem"), true)
	if err != health.ErrUnknownCheckName {
		t.Fatalf("expected ErrUnknownCheckName, got %v", err)
	}

	err = tracker.SetCheckStatus(health.CheckName("disallowed_subsystem"), health.StatusOk, "ok")
	if err != health.ErrUnknownCheckName {
		t.Fatalf("expected ErrUnknownCheckName on set status, got %v", err)
	}
}

// Scenario P: Arbitrary error strings not exposed
func TestScenarioP_ArbitraryErrorStringsSanitized(t *testing.T) {
	tracker := health.NewTracker()
	_ = tracker.RegisterCheck(health.CheckStorage, true)

	// Attempt to leak paths, SQL, tokens
	_ = tracker.SetCheckStatus(health.CheckStorage, health.StatusDegraded, "error opening C:\\data\\aegis.db: SELECT * token=123")
	check, ok := tracker.GetCheck(health.CheckStorage)
	if !ok {
		t.Fatalf("expected storage check to exist")
	}
	if check.Message != health.MsgDegraded {
		t.Fatalf("expected sanitized message %q, got %q", health.MsgDegraded, check.Message)
	}

	// Attempt to leak domain IDs (incident, decision, action, correlation)
	idLeaks := []string{
		"failed on inc-12345",
		"decision dec-98765 rejected",
		"action act-55443 timed out",
		"correlation corr-99881 broken",
		"approval appr-11223 pending",
		"$SECRET_ENV_VAR value",
		"KEY=value parameter",
	}

	for _, leak := range idLeaks {
		_ = tracker.SetCheckStatus(health.CheckStorage, health.StatusDegraded, leak)
		check, _ = tracker.GetCheck(health.CheckStorage)
		if check.Message != health.MsgDegraded {
			t.Fatalf("expected sanitized message %q for input %q, got %q", health.MsgDegraded, leak, check.Message)
		}
	}
}

// Scenario Q: Concurrent health updates safe
func TestScenarioQ_ConcurrentHealthUpdatesSafe(t *testing.T) {
	tracker := health.NewTracker()
	_ = tracker.RegisterCheck(health.CheckStorage, true)
	_ = tracker.RegisterCheck(health.CheckTelemetry, true)

	var wg sync.WaitGroup
	workers := 10
	iterations := 100

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				status := health.StatusOk
				if (id+j)%3 == 0 {
					status = health.StatusDegraded
				}
				_ = tracker.SetCheckStatus(health.CheckStorage, status, "updating")
				_ = tracker.SetCheckStatus(health.CheckTelemetry, health.StatusOk, "updating")
			}
		}(i)
	}

	wg.Wait()
}

// Scenario R: Concurrent HTTP reads safe
func TestScenarioR_ConcurrentHTTPReadsSafe(t *testing.T) {
	tracker := health.NewTracker()
	_ = tracker.RegisterCheck(health.CheckStorage, true)
	_ = tracker.SetCheckStatus(health.CheckStorage, health.StatusOk, "ok")
	_ = tracker.Transition(health.StateReady)

	handler := health.NewHandler(tracker)

	var wg sync.WaitGroup
	workers := 10
	iterations := 50

	for i := 0; i < workers; i++ {
		wg.Add(2)
		// Reader goroutine
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
				rr := httptest.NewRecorder()
				handler.Readyz(rr, req)
				if rr.Code != http.StatusOK && rr.Code != http.StatusServiceUnavailable {
					t.Errorf("unexpected status code: %d", rr.Code)
				}
			}
		}()
		// Writer goroutine
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				st := health.StatusOk
				if j%2 == 0 {
					st = health.StatusDegraded
				}
				_ = tracker.SetCheckStatus(health.CheckStorage, st, "ping")
			}
		}(i)
	}

	wg.Wait()
}

// Scenario S: Snapshot data bounded & deterministic
func TestScenarioS_SnapshotDataBoundedAndDeterministic(t *testing.T) {
	tracker := health.NewTracker()
	_ = tracker.RegisterCheck(health.CheckConfig, true)
	_ = tracker.SetCheckStatus(health.CheckConfig, health.StatusOk, "loaded")

	snap := tracker.Snapshot()
	if snap.State != health.StateInitializing {
		t.Fatalf("expected state INITIALIZING, got %s", snap.State)
	}
	if len(snap.Checks) != 1 {
		t.Fatalf("expected 1 check, got %d", len(snap.Checks))
	}

	// Mutating returned snapshot map should not affect tracker internal state
	delete(snap.Checks, health.CheckConfig)
	snap2 := tracker.Snapshot()
	if len(snap2.Checks) != 1 {
		t.Fatalf("internal map was mutated by caller")
	}
}

// Scenario T: Shutdown behavior tested
func TestScenarioT_ShutdownBehavior(t *testing.T) {
	tracker := health.NewTracker()
	_ = tracker.RegisterCheck(health.CheckStorage, true)
	_ = tracker.SetCheckStatus(health.CheckStorage, health.StatusOk, "ok")
	_ = tracker.Transition(health.StateReady)

	tracker.Shutdown()

	if tracker.IsLive() {
		t.Fatalf("expected IsLive() == false after shutdown")
	}
	if tracker.IsReady() {
		t.Fatalf("expected IsReady() == false after shutdown")
	}

	handler := health.NewHandler(tracker)

	// Liveness check should return 503
	reqH := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rrH := httptest.NewRecorder()
	handler.Healthz(rrH, reqH)
	if rrH.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for /healthz during shutdown, got %d", rrH.Code)
	}

	// Readiness check should return 503
	reqR := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rrR := httptest.NewRecorder()
	handler.Readyz(rrR, reqR)
	if rrR.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for /readyz during shutdown, got %d", rrR.Code)
	}
}

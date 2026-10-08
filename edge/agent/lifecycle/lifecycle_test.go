package lifecycle_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/health"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/lifecycle"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
)

type mockMetricsRecorder struct {
	mu           sync.Mutex
	status       string
	durations    []time.Duration
	timeoutCount int
}

func (m *mockMetricsRecorder) RecordShutdown(status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.status = status
}

func (m *mockMetricsRecorder) ObserveShutdownDuration(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.durations = append(m.durations, d)
}

func (m *mockMetricsRecorder) RecordShutdownTimeout() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.timeoutCount++
}

// Scenario A: Initial state is STARTING
func TestScenarioA_InitialStateStarting(t *testing.T) {
	c := lifecycle.NewCoordinator()
	if c.State() != lifecycle.StateStarting {
		t.Fatalf("expected initial state %s, got %s", lifecycle.StateStarting, c.State())
	}
}

// Scenario B: STARTING -> RUNNING succeeds
func TestScenarioB_StartingToRunning(t *testing.T) {
	c := lifecycle.NewCoordinator()
	if err := c.Start(); err != nil {
		t.Fatalf("expected successful start, got: %v", err)
	}
	if c.State() != lifecycle.StateRunning {
		t.Fatalf("expected state %s, got %s", lifecycle.StateRunning, c.State())
	}
}

// Scenario C: RUNNING -> SHUTTING_DOWN succeeds
func TestScenarioC_RunningToShuttingDown(t *testing.T) {
	c := lifecycle.NewCoordinator()
	_ = c.Start()

	hookRun := false
	_ = c.RegisterHook(lifecycle.Hook{
		Name:  "test_hook",
		Order: 1,
		Shutdown: func(ctx context.Context) error {
			if c.State() != lifecycle.StateShuttingDown {
				t.Errorf("expected state %s inside hook, got %s", lifecycle.StateShuttingDown, c.State())
			}
			hookRun = true
			return nil
		},
	})

	err := c.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("expected clean shutdown, got %v", err)
	}
	if !hookRun {
		t.Fatal("expected hook to run")
	}
}

// Scenario D: SHUTTING_DOWN -> STOPPED succeeds
func TestScenarioD_ShuttingDownToStopped(t *testing.T) {
	c := lifecycle.NewCoordinator()
	_ = c.Start()
	_ = c.Shutdown(context.Background())

	if c.State() != lifecycle.StateStopped {
		t.Fatalf("expected final state %s, got %s", lifecycle.StateStopped, c.State())
	}

	select {
	case <-c.StopSignal():
		// Pass: channel closed on stopped
	default:
		t.Fatal("expected StopSignal channel to be closed")
	}
}

// Scenario E: Invalid transition rejected
func TestScenarioE_InvalidTransitionsRejected(t *testing.T) {
	c := lifecycle.NewCoordinator()
	// STARTING -> STOPPED directly is invalid
	if lifecycle.IsValidTransition(lifecycle.StateStarting, lifecycle.StateStopped) {
		t.Fatal("expected STARTING -> STOPPED to be invalid")
	}

	// STOPPED -> RUNNING is invalid
	if lifecycle.IsValidTransition(lifecycle.StateStopped, lifecycle.StateRunning) {
		t.Fatal("expected STOPPED -> RUNNING to be invalid")
	}

	_ = c.Start()
	// RUNNING -> RUNNING (Start called again)
	err := c.Start()
	if err != lifecycle.ErrInvalidTransition {
		t.Fatalf("expected ErrInvalidTransition on double start, got %v", err)
	}
}

// Scenario F: Shutdown is idempotent
func TestScenarioF_ShutdownIdempotent(t *testing.T) {
	c := lifecycle.NewCoordinator()
	_ = c.Start()

	var count int32
	_ = c.RegisterHook(lifecycle.Hook{
		Name:  "counter",
		Order: 1,
		Shutdown: func(ctx context.Context) error {
			atomic.AddInt32(&count, 1)
			return nil
		},
	})

	for i := 0; i < 5; i++ {
		err := c.Shutdown(context.Background())
		if err != nil {
			t.Fatalf("iteration %d returned error: %v", i, err)
		}
	}

	if atomic.LoadInt32(&count) != 1 {
		t.Fatalf("expected hook to be invoked exactly once, got %d", count)
	}
}

// Scenario G: Concurrent Shutdown calls execute hooks once
func TestScenarioG_ConcurrentShutdown(t *testing.T) {
	c := lifecycle.NewCoordinator()
	_ = c.Start()

	var hookExecutions int32
	_ = c.RegisterHook(lifecycle.Hook{
		Name:  "concurrent_hook",
		Order: 1,
		Shutdown: func(ctx context.Context) error {
			time.Sleep(10 * time.Millisecond)
			atomic.AddInt32(&hookExecutions, 1)
			return nil
		},
	})

	const callers = 10
	var wg sync.WaitGroup
	wg.Add(callers)

	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			_ = c.Shutdown(context.Background())
		}()
	}

	wg.Wait()

	if atomic.LoadInt32(&hookExecutions) != 1 {
		t.Fatalf("expected exactly 1 execution under race, got %d", hookExecutions)
	}
}

// Scenario H: Hooks execute in deterministic order
func TestScenarioH_HooksDeterministicOrder(t *testing.T) {
	c := lifecycle.NewCoordinator()
	_ = c.Start()

	var executionOrder []string
	var mu sync.Mutex

	record := func(name string) {
		mu.Lock()
		defer mu.Unlock()
		executionOrder = append(executionOrder, name)
	}

	// Register in deliberately scrambled order
	_ = c.RegisterHook(lifecycle.Hook{
		Name:  "storage",
		Order: 50,
		Shutdown: func(ctx context.Context) error {
			record("storage")
			return nil
		},
	})
	_ = c.RegisterHook(lifecycle.Hook{
		Name:  "health",
		Order: 10,
		Shutdown: func(ctx context.Context) error {
			record("health")
			return nil
		},
	})
	_ = c.RegisterHook(lifecycle.Hook{
		Name:  "http_server",
		Order: 40,
		Shutdown: func(ctx context.Context) error {
			record("http_server")
			return nil
		},
	})
	_ = c.RegisterHook(lifecycle.Hook{
		Name:  "telemetry",
		Order: 20,
		Shutdown: func(ctx context.Context) error {
			record("telemetry")
			return nil
		},
	})

	_ = c.Shutdown(context.Background())

	expected := []string{"health", "telemetry", "http_server", "storage"}
	if len(executionOrder) != len(expected) {
		t.Fatalf("expected %d hooks executed, got %d", len(expected), len(executionOrder))
	}
	for i, name := range expected {
		if executionOrder[i] != name {
			t.Fatalf("at index %d expected %s, got %s", i, name, executionOrder[i])
		}
	}
}

// Scenario I: Hook failure does not prevent independent hooks
func TestScenarioI_HookFailureDoesNotHaltRemaining(t *testing.T) {
	c := lifecycle.NewCoordinator()
	_ = c.Start()

	var secondHookRan bool

	_ = c.RegisterHook(lifecycle.Hook{
		Name:  "failing_hook",
		Order: 1,
		Shutdown: func(ctx context.Context) error {
			return errors.New("simulated hook error")
		},
	})
	_ = c.RegisterHook(lifecycle.Hook{
		Name:  "independent_hook",
		Order: 2,
		Shutdown: func(ctx context.Context) error {
			secondHookRan = true
			return nil
		},
	})

	err := c.Shutdown(context.Background())
	if err == nil {
		t.Fatal("expected first hook error to be returned")
	}
	if !secondHookRan {
		t.Fatal("expected second hook to execute despite first hook failure")
	}
}

// Scenario J: Shutdown timeout is enforced
func TestScenarioJ_ShutdownTimeoutEnforced(t *testing.T) {
	rec := &mockMetricsRecorder{}
	c := lifecycle.NewCoordinator(
		lifecycle.WithTimeout(50*time.Millisecond),
		lifecycle.WithMetrics(rec),
	)
	_ = c.Start()

	_ = c.RegisterHook(lifecycle.Hook{
		Name:  "hanging_hook",
		Order: 1,
		Shutdown: func(ctx context.Context) error {
			<-ctx.Done() // Block until timeout context is cancelled
			return ctx.Err()
		},
	})

	start := time.Now()
	err := c.Shutdown(context.Background())
	elapsed := time.Since(start)

	if err == nil || !errors.Is(err, lifecycle.ErrShutdownTimeout) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected shutdown timeout error, got %v", err)
	}
	if elapsed > 1*time.Second {
		t.Fatalf("shutdown took too long to time out: %v", elapsed)
	}
	if rec.timeoutCount != 1 {
		t.Fatalf("expected metrics timeout count 1, got %d", rec.timeoutCount)
	}
}

// Scenario K: Context cancellation propagates
func TestScenarioK_ContextCancellationPropagates(t *testing.T) {
	c := lifecycle.NewCoordinator()
	_ = c.Start()

	rootCtx := c.Context()
	select {
	case <-rootCtx.Done():
		t.Fatal("context should not be canceled prior to shutdown")
	default:
	}

	_ = c.Shutdown(context.Background())

	select {
	case <-rootCtx.Done():
		// Pass: context canceled upon shutdown
	default:
		t.Fatal("expected root context to be canceled upon shutdown")
	}
}

// Scenario L & M & N: Readiness and Liveness during and after shutdown
func TestScenarioLMN_HealthAndReadinessDuringShutdown(t *testing.T) {
	tracker := health.NewTracker()
	_ = tracker.RegisterCheck(health.CheckStorage, true)
	_ = tracker.SetCheckStatus(health.CheckStorage, health.StatusOk, "available")
	_ = tracker.Transition(health.StateReady)

	handler := health.NewHandler(tracker)

	c := lifecycle.NewCoordinator()
	_ = c.Start()

	// Hook 1: Remove readiness
	_ = c.RegisterHook(lifecycle.Hook{
		Name:  "health_readiness",
		Order: 10,
		Shutdown: func(ctx context.Context) error {
			// During shutdown hook, verify readiness becomes unavailable
			tracker.Shutdown()
			return nil
		},
	})

	// Hook 2: Probe handler during shutdown
	var probeStatusHealthz int
	var probeStatusReadyz int
	_ = c.RegisterHook(lifecycle.Hook{
		Name:  "probe_check",
		Order: 20,
		Shutdown: func(ctx context.Context) error {
			// Healthz remains available (responsive)
			reqH := httptest.NewRequest(http.MethodGet, "/healthz", nil)
			rrH := httptest.NewRecorder()
			handler.Healthz(rrH, reqH)
			probeStatusHealthz = rrH.Code

			// Readyz responds 503 (non-ready)
			reqR := httptest.NewRequest(http.MethodGet, "/readyz", nil)
			rrR := httptest.NewRecorder()
			handler.Readyz(rrR, reqR)
			probeStatusReadyz = rrR.Code
			return nil
		},
	})

	_ = c.Shutdown(context.Background())

	if probeStatusReadyz != http.StatusServiceUnavailable {
		t.Fatalf("expected /readyz to return 503 during shutdown, got %d", probeStatusReadyz)
	}
	// Handler responded cleanly to /healthz probe
	if probeStatusHealthz != http.StatusServiceUnavailable && probeStatusHealthz != http.StatusOK {
		t.Fatalf("unexpected /healthz response status: %d", probeStatusHealthz)
	}
}

// Scenario O: HTTP server shuts down gracefully
func TestScenarioO_HTTPServerShutdown(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := lifecycle.NewCoordinator()
	_ = c.Start()

	var srvClosed bool
	_ = c.RegisterHook(lifecycle.Hook{
		Name:  "http_server",
		Order: 40,
		Shutdown: func(ctx context.Context) error {
			srv.CloseClientConnections()
			srvClosed = true
			return nil
		},
	})

	_ = c.Shutdown(context.Background())
	if !srvClosed {
		t.Fatal("expected HTTP server close hook to execute")
	}
}

// Scenario P: Storage close is invoked once
func TestScenarioP_StorageCloseInvokedOnce(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "shutdown_test.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}

	c := lifecycle.NewCoordinator()
	_ = c.Start()

	var storeCloseCount int32
	_ = c.RegisterHook(lifecycle.Hook{
		Name:  "storage",
		Order: 50,
		Shutdown: func(ctx context.Context) error {
			atomic.AddInt32(&storeCloseCount, 1)
			return store.Close()
		},
	})

	_ = c.Shutdown(context.Background())
	_ = c.Shutdown(context.Background()) // Second invocation

	if atomic.LoadInt32(&storeCloseCount) != 1 {
		t.Fatalf("expected storage to be closed exactly once, got %d", storeCloseCount)
	}
}

// Scenario Q & R: Metrics remain optional and disabled metrics does not affect shutdown
func TestScenarioQR_MetricsOptionalAndDisabledSafe(t *testing.T) {
	// Without metrics
	c1 := lifecycle.NewCoordinator()
	_ = c1.Start()
	if err := c1.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown without metrics failed: %v", err)
	}

	// With metrics
	rec := &mockMetricsRecorder{}
	c2 := lifecycle.NewCoordinator(lifecycle.WithMetrics(rec))
	_ = c2.Start()
	if err := c2.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown with metrics failed: %v", err)
	}
	if rec.status != "completed" {
		t.Fatalf("expected metrics status 'completed', got %q", rec.status)
	}
}

// Scenario S & T: Signal handling initiates graceful shutdown and repeated signals do not duplicate
func TestScenarioST_SignalHandling(t *testing.T) {
	c := lifecycle.NewCoordinator()
	_ = c.Start()

	var hookCount int32
	_ = c.RegisterHook(lifecycle.Hook{
		Name:  "signal_hook",
		Order: 1,
		Shutdown: func(ctx context.Context) error {
			atomic.AddInt32(&hookCount, 1)
			return nil
		},
	})

	sigChan, done := lifecycle.SetupSignalHandler(c, 2*time.Second, syscall.SIGINT)

	// Send first signal
	sigChan <- syscall.SIGINT
	// Send second repeated signal
	sigChan <- syscall.SIGINT

	select {
	case <-done:
		// Shutdown completed
	case <-time.After(3 * time.Second):
		t.Fatal("signal handler timed out waiting for completion")
	}

	if atomic.LoadInt32(&hookCount) != 1 {
		t.Fatalf("expected hook to be called once despite multiple signals, got %d", hookCount)
	}
	if c.State() != lifecycle.StateStopped {
		t.Fatalf("expected state STOPPED, got %s", c.State())
	}
}

// Scenario U: No goroutine leaks are introduced by the lifecycle coordinator
func TestScenarioU_NoGoroutineLeaks(t *testing.T) {
	beforeGoroutines := runtime.NumGoroutine()

	c := lifecycle.NewCoordinator(lifecycle.WithTimeout(500 * time.Millisecond))
	_ = c.Start()

	_ = c.RegisterHook(lifecycle.Hook{
		Name:  "clean_hook",
		Order: 1,
		Shutdown: func(ctx context.Context) error {
			return nil
		},
	})

	_ = c.Shutdown(context.Background())

	// Allow runtime to settle
	time.Sleep(50 * time.Millisecond)

	afterGoroutines := runtime.NumGoroutine()
	// Goroutine count should not grow continuously
	if afterGoroutines > beforeGoroutines+2 {
		t.Fatalf("potential goroutine leak: before=%d, after=%d", beforeGoroutines, afterGoroutines)
	}
}

// Scenario V: Full edge-agent lifecycle integration test
func TestScenarioV_FullLifecycleIntegration(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "full_lifecycle.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}

	tracker := health.NewTracker()
	_ = tracker.RegisterCheck(health.CheckStorage, true)
	_ = tracker.SetCheckStatus(health.CheckStorage, health.StatusOk, "available")
	_ = tracker.Transition(health.StateReady)

	rec := &mockMetricsRecorder{}
	coord := lifecycle.NewCoordinator(
		lifecycle.WithTimeout(5*time.Second),
		lifecycle.WithMetrics(rec),
	)

	// Register all standard hooks in specified order
	_ = coord.RegisterHook(lifecycle.Hook{
		Name:  "health",
		Order: 10,
		Shutdown: func(ctx context.Context) error {
			tracker.Shutdown()
			return nil
		},
	})
	_ = coord.RegisterHook(lifecycle.Hook{
		Name:  "storage",
		Order: 50,
		Shutdown: func(ctx context.Context) error {
			return store.Close()
		},
	})

	if err := coord.Start(); err != nil {
		t.Fatalf("failed to start coordinator: %v", err)
	}
	if coord.State() != lifecycle.StateRunning {
		t.Fatalf("expected RUNNING, got %s", coord.State())
	}

	// Graceful shutdown
	if err := coord.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	if coord.State() != lifecycle.StateStopped {
		t.Fatalf("expected STOPPED, got %s", coord.State())
	}
	if tracker.IsReady() {
		t.Fatal("health tracker should not be ready after shutdown")
	}
	if rec.status != "completed" {
		t.Fatalf("expected metrics status completed, got %s", rec.status)
	}
}

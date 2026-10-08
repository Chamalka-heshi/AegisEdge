package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"
)

// State defines the bounded runtime lifecycle states of the edge agent.
type State string

const (
	StateStarting     State = "STARTING"
	StateRunning      State = "RUNNING"
	StateShuttingDown State = "SHUTTING_DOWN"
	StateStopped      State = "STOPPED"
)

// DefaultShutdownTimeout is the default duration allocated for graceful shutdown.
const DefaultShutdownTimeout = 10 * time.Second

// Domain lifecycle errors.
var (
	ErrInvalidTransition   = errors.New("invalid lifecycle state transition")
	ErrShutdownTimeout     = errors.New("graceful shutdown timed out before all hooks completed")
	ErrAlreadyShuttingDown = errors.New("lifecycle is already shutting down or stopped")
	ErrNilShutdownHook     = errors.New("shutdown hook function cannot be nil")
	ErrEmptyHookName       = errors.New("shutdown hook name cannot be empty")
)

// IsValidState returns true if s is one of the four bounded lifecycle states.
func IsValidState(s State) bool {
	switch s {
	case StateStarting, StateRunning, StateShuttingDown, StateStopped:
		return true
	default:
		return false
	}
}

// IsValidTransition validates permitted lifecycle state movements.
func IsValidTransition(from, to State) bool {
	if !IsValidState(from) || !IsValidState(to) {
		return false
	}
	switch from {
	case StateStarting:
		return to == StateRunning || to == StateShuttingDown
	case StateRunning:
		return to == StateShuttingDown
	case StateShuttingDown:
		return to == StateStopped
	case StateStopped:
		return false
	default:
		return false
	}
}

// Hook represents a registered, ordered shutdown action.
type Hook struct {
	Name     string
	Order    int
	Shutdown func(ctx context.Context) error
}

// MetricsRecorder defines an optional metrics recording interface for lifecycle events.
type MetricsRecorder interface {
	RecordShutdown(status string)
	ObserveShutdownDuration(d time.Duration)
	RecordShutdownTimeout()
}

// Coordinator manages the edge agent runtime lifecycle and ordered shutdown sequence.
type Coordinator interface {
	State() State
	Start() error
	RegisterHook(hook Hook) error
	Shutdown(ctx context.Context) error
	Context() context.Context
	StopSignal() <-chan struct{}
}

type defaultCoordinator struct {
	mu             sync.RWMutex
	state          State
	hooks          []Hook
	defaultTimeout time.Duration
	rootCtx        context.Context
	rootCancel     context.CancelFunc
	shutdownDone   chan struct{}
	shutdownErr    error
	metrics        MetricsRecorder
}

// Option configures coordinator settings.
type Option func(*defaultCoordinator)

// WithTimeout sets the default shutdown timeout if none is provided in Shutdown(ctx).
func WithTimeout(d time.Duration) Option {
	return func(c *defaultCoordinator) {
		if d > 0 {
			c.defaultTimeout = d
		}
	}
}

// WithMetrics attaches an optional MetricsRecorder.
func WithMetrics(rec MetricsRecorder) Option {
	return func(c *defaultCoordinator) {
		c.metrics = rec
	}
}

// WithContext sets a parent context for the lifecycle coordinator.
func WithContext(ctx context.Context) Option {
	return func(c *defaultCoordinator) {
		if ctx != nil {
			c.rootCtx, c.rootCancel = context.WithCancel(ctx)
		}
	}
}

// NewCoordinator constructs a lifecycle coordinator starting in StateStarting.
func NewCoordinator(opts ...Option) Coordinator {
	ctx, cancel := context.WithCancel(context.Background())
	c := &defaultCoordinator{
		state:          StateStarting,
		defaultTimeout: DefaultShutdownTimeout,
		rootCtx:        ctx,
		rootCancel:     cancel,
		shutdownDone:   make(chan struct{}),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// State returns the current lifecycle state.
func (c *defaultCoordinator) State() State {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

// Start moves the coordinator from StateStarting to StateRunning.
func (c *defaultCoordinator) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !IsValidTransition(c.state, StateRunning) {
		return ErrInvalidTransition
	}
	c.state = StateRunning
	return nil
}

// RegisterHook adds an ordered shutdown hook. Cannot be registered once shutdown begins.
func (c *defaultCoordinator) RegisterHook(hook Hook) error {
	if hook.Name == "" {
		return ErrEmptyHookName
	}
	if hook.Shutdown == nil {
		return ErrNilShutdownHook
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state == StateShuttingDown || c.state == StateStopped {
		return ErrAlreadyShuttingDown
	}

	c.hooks = append(c.hooks, hook)
	return nil
}

// Context returns the root runtime context canceled as soon as shutdown begins.
func (c *defaultCoordinator) Context() context.Context {
	return c.rootCtx
}

// StopSignal returns a channel closed when shutdown has fully completed (state is StateStopped).
func (c *defaultCoordinator) StopSignal() <-chan struct{} {
	return c.shutdownDone
}

// Shutdown initiates ordered graceful shutdown. It is strictly idempotent; concurrent or
// subsequent calls wait for the active shutdown to conclude and receive the same outcome.
func (c *defaultCoordinator) Shutdown(ctx context.Context) error {
	c.mu.Lock()

	// If already stopped, return the cached result immediately
	if c.state == StateStopped {
		err := c.shutdownErr
		c.mu.Unlock()
		return err
	}

	// If already shutting down, release lock and await completion
	if c.state == StateShuttingDown {
		c.mu.Unlock()
		select {
		case <-c.shutdownDone:
			c.mu.RLock()
			defer c.mu.RUnlock()
			return c.shutdownErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// First invocation: transition to StateShuttingDown
	c.state = StateShuttingDown
	c.rootCancel() // Immediately cancel root context to stop in-flight / new work

	// Clone hooks slice so execution occurs outside the lock
	hooks := make([]Hook, len(c.hooks))
	copy(hooks, c.hooks)
	timeout := c.defaultTimeout
	rec := c.metrics
	c.mu.Unlock()

	// Sort hooks deterministically by Order ascending, then Name ascending
	sort.SliceStable(hooks, func(i, j int) bool {
		if hooks[i].Order != hooks[j].Order {
			return hooks[i].Order < hooks[j].Order
		}
		return hooks[i].Name < hooks[j].Name
	})

	// Establish bounded shutdown context
	shutdownCtx := ctx
	var cancel context.CancelFunc
	if shutdownCtx == nil {
		shutdownCtx, cancel = context.WithTimeout(context.Background(), timeout)
		defer cancel()
	} else if _, hasDeadline := shutdownCtx.Deadline(); !hasDeadline {
		shutdownCtx, cancel = context.WithTimeout(shutdownCtx, timeout)
		defer cancel()
	}

	startTime := time.Now()
	var hookErrors []error
	timedOut := false

	// Execute hooks in deterministic sequence
	for _, h := range hooks {
		if shutdownCtx.Err() != nil {
			timedOut = true
			hookErrors = append(hookErrors, ErrShutdownTimeout)
			break
		}

		if err := h.Shutdown(shutdownCtx); err != nil {
			hookErrors = append(hookErrors, fmt.Errorf("hook %s failed: %w", h.Name, err))
		}
	}

	if shutdownCtx.Err() == context.DeadlineExceeded && !timedOut {
		timedOut = true
		hookErrors = append(hookErrors, ErrShutdownTimeout)
	}

	// Record operational metrics
	duration := time.Since(startTime)
	if rec != nil {
		rec.ObserveShutdownDuration(duration)
		if timedOut {
			rec.RecordShutdownTimeout()
			rec.RecordShutdown("timed_out")
		} else if len(hookErrors) > 0 {
			rec.RecordShutdown("failed")
		} else {
			rec.RecordShutdown("completed")
		}
	}

	var finalErr error
	if len(hookErrors) > 0 {
		finalErr = hookErrors[0]
	}

	// Transition to StateStopped and signal completion
	c.mu.Lock()
	c.state = StateStopped
	c.shutdownErr = finalErr
	close(c.shutdownDone)
	c.mu.Unlock()

	return finalErr
}

// SetupSignalHandler registers signal notification for the specified OS signals
// (defaulting to os.Interrupt and syscall.SIGTERM) and triggers Coordinator.Shutdown
// upon reception. Returns the signal channel and a completion channel.
func SetupSignalHandler(c Coordinator, timeout time.Duration, signals ...os.Signal) (chan os.Signal, <-chan struct{}) {
	if len(signals) == 0 {
		signals = []os.Signal{os.Interrupt, syscall.SIGTERM}
	}
	sigChan := make(chan os.Signal, 2)
	signal.Notify(sigChan, signals...)
	done := make(chan struct{})

	go func() {
		defer close(done)
		_, ok := <-sigChan
		if !ok {
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		_ = c.Shutdown(ctx)
	}()

	return sigChan, done
}

package coordination

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/audit"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/health"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/metrics"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

const (
	DefaultHeartbeatInterval        = 10 * time.Second
	DefaultInitialReconnectInterval = 1 * time.Second
	DefaultMaxReconnectInterval     = 30 * time.Second
	DefaultBackoffFactor            = 2.0
	DefaultRequestTimeout           = 5 * time.Second
	DefaultAgentVersion             = "0.1.0"
)

// Config specifies the runtime coordination configuration.
type Config struct {
	NodeID                   string
	Hostname                 string
	OS                       string
	Architecture             string
	IPAddress                string
	AgentVersion             string
	Capabilities             []string
	Labels                   map[string]string
	HeartbeatInterval        time.Duration
	InitialReconnectInterval time.Duration
	MaxReconnectInterval     time.Duration
	BackoffFactor            float64
	RequestTimeout           time.Duration
}

// DefaultConfig generates a valid baseline Config for the given node ID.
func DefaultConfig(nodeID string) Config {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-host"
	}

	return Config{
		NodeID:                   nodeID,
		Hostname:                 hostname,
		OS:                       runtime.GOOS,
		Architecture:             runtime.GOARCH,
		AgentVersion:             DefaultAgentVersion,
		Capabilities:             []string{"telemetry", "anomaly_detection", "mitigation", "incident_fsm"},
		HeartbeatInterval:        DefaultHeartbeatInterval,
		InitialReconnectInterval: DefaultInitialReconnectInterval,
		MaxReconnectInterval:     DefaultMaxReconnectInterval,
		BackoffFactor:            DefaultBackoffFactor,
		RequestTimeout:           DefaultRequestTimeout,
	}
}

// Coordinator manages edge-to-control-plane registration, heartbeats, and safe offline autonomy.
type Coordinator struct {
	cfg           Config
	client        Client
	tracker       *Tracker
	metrics       metrics.Recorder
	healthTracker health.Tracker
	auditRec      audit.Recorder

	mu        sync.Mutex
	running   bool
	cancel    context.CancelFunc
	done      chan struct{}
	seqNumber int64

	currentBackoff time.Duration
	registered     bool
}

// NewCoordinator constructs a new Coordinator.
func NewCoordinator(
	cfg Config,
	client Client,
	tracker *Tracker,
	metricsRec metrics.Recorder,
	healthTracker health.Tracker,
	auditRec audit.Recorder,
) *Coordinator {
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if cfg.InitialReconnectInterval <= 0 {
		cfg.InitialReconnectInterval = DefaultInitialReconnectInterval
	}
	if cfg.MaxReconnectInterval <= 0 {
		cfg.MaxReconnectInterval = DefaultMaxReconnectInterval
	}
	if cfg.BackoffFactor < 1.0 {
		cfg.BackoffFactor = DefaultBackoffFactor
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = DefaultRequestTimeout
	}
	if cfg.AgentVersion == "" {
		cfg.AgentVersion = DefaultAgentVersion
	}
	if cfg.Hostname == "" {
		cfg.Hostname = "edge-node"
	}
	if metricsRec == nil {
		metricsRec = metrics.NoopRecorder{}
	}

	return &Coordinator{
		cfg:            cfg,
		client:         client,
		tracker:        tracker,
		metrics:        metricsRec,
		healthTracker:  healthTracker,
		auditRec:       auditRec,
		currentBackoff: cfg.InitialReconnectInterval,
	}
}

// Tracker returns the underlying StateTracker.
func (c *Coordinator) Tracker() *Tracker {
	return c.tracker
}

// Start launches the background coordination loop in a non-blocking goroutine.
func (c *Coordinator) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return errors.New("coordinator is already running")
	}

	coordCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.done = make(chan struct{})
	c.running = true

	go c.run(coordCtx)
	return nil
}

// Stop initiates graceful shutdown of the background coordination loop.
func (c *Coordinator) Stop(ctx context.Context) error {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return nil
	}
	c.cancel()
	doneCh := c.done
	c.mu.Unlock()

	c.tracker.Transition(ctx, StateStopping, "coordination stopping")

	select {
	case <-doneCh:
		c.mu.Lock()
		c.running = false
		c.mu.Unlock()
		return nil
	case <-ctx.Done():
		c.mu.Lock()
		c.running = false
		c.mu.Unlock()
		return ctx.Err()
	}
}

func (c *Coordinator) run(ctx context.Context) {
	defer close(c.done)

	slog.Info("coordination manager started", "node_id", c.cfg.NodeID)

	// Initial registration attempt
	c.attemptRegistration(ctx)

	ticker := time.NewTicker(c.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("coordination manager stopped", "node_id", c.cfg.NodeID)
			return

		case <-ticker.C:
			if !c.registered {
				c.attemptRegistration(ctx)
				continue
			}

			c.sendHeartbeat(ctx)
		}
	}
}

func (c *Coordinator) attemptRegistration(ctx context.Context) {
	c.tracker.Transition(ctx, StateConnecting, "initiating node registration")

	now := time.Now().UTC()
	if c.auditRec != nil {
		_ = c.auditRec.Record(ctx, audit.AuditEvent{
			EventType:  audit.EventTypeControlPlaneRegStarted,
			NodeID:     c.cfg.NodeID,
			IncidentID: "system-coordination",
			Result:     audit.ResultSuccess,
			Timestamp:  now,
		})
	}

	reg := &types.NodeRegistration{
		NodeID:       c.cfg.NodeID,
		Hostname:     c.cfg.Hostname,
		OS:           c.cfg.OS,
		Architecture: c.cfg.Architecture,
		IPAddress:    c.cfg.IPAddress,
		AgentVersion: c.cfg.AgentVersion,
		Capabilities: c.cfg.Capabilities,
		Labels:       c.cfg.Labels,
		RegisteredAt: now,
	}

	reqCtx, reqCancel := context.WithTimeout(ctx, c.cfg.RequestTimeout)
	resp, err := c.client.RegisterNode(reqCtx, reg)
	reqCancel()

	if err != nil {
		c.metrics.RecordControlPlaneRegistration("failed")
		if errors.Is(err, ErrAuthFailed) {
			c.metrics.RecordControlPlaneAuthFailure(err.Error())
			if c.auditRec != nil {
				_ = c.auditRec.Record(ctx, audit.AuditEvent{
					EventType:  audit.EventTypeControlPlaneAuthFailed,
					NodeID:     c.cfg.NodeID,
					IncidentID: "system-coordination",
					Result:     audit.ResultFailed,
					Reason:     err.Error(),
					Timestamp:  time.Now().UTC(),
				})
			}
		} else if c.auditRec != nil {
			_ = c.auditRec.Record(ctx, audit.AuditEvent{
				EventType:  audit.EventTypeControlPlaneRegFailed,
				NodeID:     c.cfg.NodeID,
				IncidentID: "system-coordination",
				Result:     audit.ResultFailed,
				Reason:     err.Error(),
				Timestamp:  time.Now().UTC(),
			})
		}

		c.tracker.Transition(ctx, StateDegraded, fmt.Sprintf("registration failed: %v", err))
		c.scheduleReconnect(ctx)
		return
	}

	c.registered = true
	c.currentBackoff = c.cfg.InitialReconnectInterval

	regStatus := "success"
	if resp != nil && resp.Status == "already_registered" {
		regStatus = "already_registered"
	}
	c.metrics.RecordControlPlaneRegistration(regStatus)

	if c.auditRec != nil {
		_ = c.auditRec.Record(ctx, audit.AuditEvent{
			EventType:  audit.EventTypeControlPlaneRegSuccess,
			NodeID:     c.cfg.NodeID,
			IncidentID: "system-coordination",
			Result:     audit.ResultSuccess,
			Reason:     regStatus,
			Timestamp:  time.Now().UTC(),
		})
	}

	c.tracker.Transition(ctx, StateConnected, "registration succeeded")
	slog.Info("node registered with control plane", "node_id", c.cfg.NodeID, "status", regStatus)
}

func (c *Coordinator) sendHeartbeat(ctx context.Context) {
	seq := atomic.AddInt64(&c.seqNumber, 1)

	nodeStatus := types.NodeStatusHealthy
	if c.healthTracker != nil && c.healthTracker.State() == health.StateDegraded {
		nodeStatus = types.NodeStatusDegraded
	}

	hb := &types.Heartbeat{
		NodeID:         c.cfg.NodeID,
		SequenceNumber: seq,
		Timestamp:      time.Now().UTC(),
		Status:         nodeStatus,
	}

	start := time.Now()
	reqCtx, reqCancel := context.WithTimeout(ctx, c.cfg.RequestTimeout)
	_, err := c.client.SendHeartbeat(reqCtx, hb)
	reqCancel()
	duration := time.Since(start)

	if err != nil {
		c.tracker.RecordHeartbeat(ctx, false, duration, err.Error())

		if errors.Is(err, ErrAuthFailed) {
			c.metrics.RecordControlPlaneAuthFailure(err.Error())
			if c.auditRec != nil {
				_ = c.auditRec.Record(ctx, audit.AuditEvent{
					EventType:  audit.EventTypeControlPlaneAuthFailed,
					NodeID:     c.cfg.NodeID,
					IncidentID: "system-coordination",
					Result:     audit.ResultFailed,
					Reason:     err.Error(),
					Timestamp:  time.Now().UTC(),
				})
			}
		}

		if errors.Is(err, ErrNodeNotFound) {
			slog.Warn("control plane reported node not found, re-registering", "node_id", c.cfg.NodeID)
			c.registered = false
			c.attemptRegistration(ctx)
			return
		}

		c.tracker.Transition(ctx, StateDegraded, fmt.Sprintf("heartbeat failed: %v", err))
		c.scheduleReconnect(ctx)
		return
	}

	// Successful heartbeat
	c.tracker.RecordHeartbeat(ctx, true, duration, "")
	c.currentBackoff = c.cfg.InitialReconnectInterval

	if c.tracker.State() != StateConnected {
		c.tracker.Transition(ctx, StateConnected, "heartbeat restored connection")
	}
}

func (c *Coordinator) scheduleReconnect(ctx context.Context) {
	c.metrics.RecordControlPlaneReconnect()

	backoff := c.currentBackoff
	if c.auditRec != nil {
		_ = c.auditRec.Record(ctx, audit.AuditEvent{
			EventType:  audit.EventTypeControlPlaneReconnectSched,
			NodeID:     c.cfg.NodeID,
			IncidentID: "system-coordination",
			Result:     audit.ResultSuccess,
			Reason:     fmt.Sprintf("reconnect backoff %v", backoff),
			Metadata: map[string]string{
				"backoff": backoff.String(),
			},
			Timestamp: time.Now().UTC(),
		})
	}

	// Increase backoff exponentially for next failure, capped at MaxReconnectInterval
	nextBackoff := time.Duration(float64(c.currentBackoff) * c.cfg.BackoffFactor)
	if nextBackoff > c.cfg.MaxReconnectInterval {
		nextBackoff = c.cfg.MaxReconnectInterval
	}
	c.currentBackoff = nextBackoff
}

package coordination

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/audit"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/health"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/metrics"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// fakeAuditRecorder records audit events in memory for assertions.
type fakeAuditRecorder struct {
	mu     sync.Mutex
	events []audit.AuditEvent
}

func (f *fakeAuditRecorder) Record(ctx context.Context, event audit.AuditEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, event)
	return nil
}

func (f *fakeAuditRecorder) Query() audit.QueryService {
	return nil
}

func (f *fakeAuditRecorder) Close() error {
	return nil
}

func (f *fakeAuditRecorder) countEvents(t audit.EventType) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, e := range f.events {
		if e.EventType == t {
			count++
		}
	}
	return count
}

func TestCoordination_A_StateModel(t *testing.T) {
	states := []State{
		StateDisconnected,
		StateConnecting,
		StateConnected,
		StateDegraded,
		StateStopping,
	}

	expectedStrings := []string{
		"DISCONNECTED",
		"CONNECTING",
		"CONNECTED",
		"DEGRADED",
		"STOPPING",
	}

	for i, s := range states {
		if s.String() != expectedStrings[i] {
			t.Errorf("expected state %d to have string %q, got %q", i, expectedStrings[i], s.String())
		}
	}

	if !StateConnected.IsConnected() {
		t.Errorf("expected StateConnected.IsConnected() to be true")
	}
	if StateDegraded.IsConnected() {
		t.Errorf("expected StateDegraded.IsConnected() to be false")
	}
	if !StateDegraded.IsDegraded() {
		t.Errorf("expected StateDegraded.IsDegraded() to be true")
	}
}

func TestCoordination_B_TrackerTransitionsAndHealth(t *testing.T) {
	ht := health.NewTracker()
	_ = ht.RegisterCheck(health.CheckStorage, true)
	_ = ht.RegisterCheck(health.CheckControlPlane, false) // Non-critical optional check!
	_ = ht.SetCheckStatus(health.CheckStorage, health.StatusOk, "storage ready")
	_ = ht.Transition(health.StateReady)

	metricRec := metrics.NewDefaultRecorder(nil)
	auditRec := &fakeAuditRecorder{}
	tracker := NewTracker("node-01", metricRec, ht, auditRec)

	if tracker.State() != StateDisconnected {
		t.Fatalf("expected initial state StateDisconnected, got %v", tracker.State())
	}

	ctx := context.Background()

	// 1. Transition to Connecting
	tracker.Transition(ctx, StateConnecting, "starting connection")
	if tracker.State() != StateConnecting {
		t.Errorf("expected StateConnecting, got %v", tracker.State())
	}
	// Readiness should still be true!
	if !ht.IsReady() {
		t.Errorf("agent must remain ready while control-plane is connecting")
	}

	// 2. Transition to Connected
	tracker.Transition(ctx, StateConnected, "connected")
	if tracker.State() != StateConnected {
		t.Errorf("expected StateConnected, got %v", tracker.State())
	}
	if tracker.LastConnected().IsZero() {
		t.Errorf("expected non-zero LastConnected timestamp")
	}
	if auditRec.countEvents(audit.EventTypeControlPlaneConnected) != 1 {
		t.Errorf("expected 1 EventTypeControlPlaneConnected audit event")
	}

	// 3. Transition to Degraded
	tracker.Transition(ctx, StateDegraded, "heartbeat timeout")
	if tracker.State() != StateDegraded {
		t.Errorf("expected StateDegraded, got %v", tracker.State())
	}
	if tracker.LastError() != "heartbeat timeout" {
		t.Errorf("expected error 'heartbeat timeout', got %q", tracker.LastError())
	}
	if auditRec.countEvents(audit.EventTypeControlPlaneDisconnected) != 1 {
		t.Errorf("expected 1 EventTypeControlPlaneDisconnected audit event")
	}
	// CRITICAL INVARIANT: Agent MUST remain READY even if control plane is Degraded!
	if !ht.IsReady() {
		t.Errorf("agent readiness must NOT fail when control-plane is degraded")
	}

	// 4. Record heartbeats
	tracker.RecordHeartbeat(ctx, true, 25*time.Millisecond, "")
	if tracker.LastHeartbeat().IsZero() {
		t.Errorf("expected non-zero LastHeartbeat timestamp")
	}
	if auditRec.countEvents(audit.EventTypeControlPlaneHeartbeatSuccess) != 1 {
		t.Errorf("expected 1 EventTypeControlPlaneHeartbeatSuccess")
	}

	tracker.RecordHeartbeat(ctx, false, 5*time.Millisecond, "network down")
	if auditRec.countEvents(audit.EventTypeControlPlaneHeartbeatFailed) != 1 {
		t.Errorf("expected 1 EventTypeControlPlaneHeartbeatFailed")
	}
}

func TestCoordination_C_ResolveNodeIdentity(t *testing.T) {
	dbPath := t.TempDir() + "/identity_test.db"
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("failed to open test store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// 1. Initial resolution with configured ID
	id1, err := ResolveNodeIdentity(ctx, store, "edge-cluster-node-99")
	if err != nil {
		t.Fatalf("unexpected error resolving identity: %v", err)
	}
	if id1 != "edge-cluster-node-99" {
		t.Errorf("expected edge-cluster-node-99, got %s", id1)
	}

	// 2. Re-resolve with empty configured ID should retrieve persisted ID
	id2, err := ResolveNodeIdentity(ctx, store, "")
	if err != nil {
		t.Fatalf("unexpected error re-resolving identity: %v", err)
	}
	if id2 != "edge-cluster-node-99" {
		t.Errorf("expected persisted identity edge-cluster-node-99, got %s", id2)
	}
}

func TestCoordination_D_HTTPClient_Endpoints(t *testing.T) {
	registeredNodes := make(map[string]*types.NodeRegistration)
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		switch r.URL.Path {
		case "/api/v1/nodes/register":
			var reg types.NodeRegistration
			if err := json.NewDecoder(r.Body).Decode(&reg); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if _, exists := registeredNodes[reg.NodeID]; exists {
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(RegistrationResponse{
					Status:  "already_registered",
					Message: "Node already registered",
				})
				return
			}
			registeredNodes[reg.NodeID] = &reg
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(RegistrationResponse{
				Status:  "registered",
				Message: "Node successfully registered",
			})

		case "/api/v1/nodes/heartbeat":
			var hb types.Heartbeat
			if err := json.NewDecoder(r.Body).Decode(&hb); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if _, exists := registeredNodes[hb.NodeID]; !exists {
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(map[string]string{
					"error": "Node not registered",
				})
				return
			}
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(HeartbeatResponse{
				Status:  "ok",
				Message: "Heartbeat accepted",
			})

		case "/api/v1/nodes/test-node":
			if reg, exists := registeredNodes["test-node"]; exists {
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(reg)
				return
			}
			w.WriteHeader(http.StatusNotFound)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := NewHTTPClient(server.URL, 2*time.Second)
	ctx := context.Background()

	// 1. Initial register
	regPayload := &types.NodeRegistration{
		NodeID:       "test-node",
		Hostname:     "test-host",
		RegisteredAt: time.Now().UTC(),
	}
	resp, err := client.RegisterNode(ctx, regPayload)
	if err != nil {
		t.Fatalf("failed to register node: %v", err)
	}
	if resp.Status != "registered" {
		t.Errorf("expected registered status, got %s", resp.Status)
	}

	// 2. Idempotent re-register
	resp2, err := client.RegisterNode(ctx, regPayload)
	if err != nil {
		t.Fatalf("failed to re-register node: %v", err)
	}
	if resp2.Status != "already_registered" {
		t.Errorf("expected already_registered status, got %s", resp2.Status)
	}

	// 3. Heartbeat success
	hbPayload := &types.Heartbeat{
		NodeID:         "test-node",
		SequenceNumber: 1,
		Timestamp:      time.Now().UTC(),
		Status:         types.NodeStatusHealthy,
	}
	hbResp, err := client.SendHeartbeat(ctx, hbPayload)
	if err != nil {
		t.Fatalf("failed to send heartbeat: %v", err)
	}
	if hbResp.Status != "ok" {
		t.Errorf("expected ok status, got %s", hbResp.Status)
	}

	// 4. Heartbeat unknown node -> ErrNodeNotFound
	hbUnknown := &types.Heartbeat{
		NodeID:         "non-existent-node",
		SequenceNumber: 1,
		Timestamp:      time.Now().UTC(),
		Status:         types.NodeStatusHealthy,
	}
	_, err = client.SendHeartbeat(ctx, hbUnknown)
	if err == nil || err != ErrNodeNotFound {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}

	// 5. Get node
	nodeResp, err := client.GetNode(ctx, "test-node")
	if err != nil {
		t.Fatalf("failed to get node: %v", err)
	}
	if nodeResp.NodeID != "test-node" {
		t.Errorf("expected node ID test-node, got %s", nodeResp.NodeID)
	}
}

func TestCoordination_E_FullLifecycleAndOfflineAutonomy(t *testing.T) {
	var heartbeatCount int64
	var registerCount int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/nodes/register":
			atomic.AddInt64(&registerCount, 1)
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(RegistrationResponse{
				Status:  "registered",
				Message: "Registered",
			})
		case "/api/v1/nodes/heartbeat":
			atomic.AddInt64(&heartbeatCount, 1)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(HeartbeatResponse{
				Status:  "ok",
				Message: "Heartbeat accepted",
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	ht := health.NewTracker()
	_ = ht.RegisterCheck(health.CheckStorage, true)
	_ = ht.RegisterCheck(health.CheckControlPlane, false) // Non-critical optional check
	_ = ht.SetCheckStatus(health.CheckStorage, health.StatusOk, "storage ready")
	_ = ht.Transition(health.StateReady)

	metricRec := metrics.NewDefaultRecorder(nil)
	auditRec := &fakeAuditRecorder{}
	tracker := NewTracker("autonomous-node-01", metricRec, ht, auditRec)

	cfg := DefaultConfig("autonomous-node-01")
	cfg.HeartbeatInterval = 50 * time.Millisecond
	cfg.RequestTimeout = 1 * time.Second

	client := NewHTTPClient(server.URL, 1*time.Second)
	coordinator := NewCoordinator(cfg, client, tracker, metricRec, ht, auditRec)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := coordinator.Start(ctx); err != nil {
		t.Fatalf("failed to start coordinator: %v", err)
	}

	// Wait for registration and at least 2 heartbeats
	time.Sleep(150 * time.Millisecond)

	if atomic.LoadInt64(&registerCount) < 1 {
		t.Errorf("expected at least 1 registration, got %d", registerCount)
	}
	if atomic.LoadInt64(&heartbeatCount) < 1 {
		t.Errorf("expected at least 1 heartbeat, got %d", heartbeatCount)
	}

	if tracker.State() != StateConnected {
		t.Errorf("expected StateConnected, got %v", tracker.State())
	}
	if !ht.IsReady() {
		t.Errorf("expected edge agent to be ready")
	}

	// Verify graceful stop
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()
	if err := coordinator.Stop(stopCtx); err != nil {
		t.Fatalf("coordinator stop failed: %v", err)
	}

	if tracker.State() != StateStopping {
		t.Errorf("expected StateStopping after Stop, got %v", tracker.State())
	}
}

func TestCoordination_F_OfflineOnStartupDoesNotBlock(t *testing.T) {
	// Point to an unavailable endpoint
	client := NewHTTPClient("http://127.0.0.1:54321", 100*time.Millisecond)

	ht := health.NewTracker()
	_ = ht.RegisterCheck(health.CheckStorage, true)
	_ = ht.RegisterCheck(health.CheckControlPlane, false)
	_ = ht.SetCheckStatus(health.CheckStorage, health.StatusOk, "storage ready")
	_ = ht.Transition(health.StateReady)

	metricRec := metrics.NewDefaultRecorder(nil)
	auditRec := &fakeAuditRecorder{}
	tracker := NewTracker("offline-node-01", metricRec, ht, auditRec)

	cfg := DefaultConfig("offline-node-01")
	cfg.HeartbeatInterval = 50 * time.Millisecond
	cfg.InitialReconnectInterval = 20 * time.Millisecond
	cfg.RequestTimeout = 50 * time.Millisecond

	coordinator := NewCoordinator(cfg, client, tracker, metricRec, ht, auditRec)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := coordinator.Start(ctx); err != nil {
		t.Fatalf("failed to start coordinator: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && tracker.State() != StateDegraded {
		time.Sleep(10 * time.Millisecond)
	}

	// INVARIANT: When control plane is unavailable, agent stays autonomous and READY!
	if tracker.State() != StateDegraded {
		t.Errorf("expected StateDegraded when offline, got %v", tracker.State())
	}
	if !ht.IsReady() {
		t.Errorf("CRITICAL: edge agent MUST remain READY offline even when control plane is unavailable")
	}

	// Clean stop
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer stopCancel()
	_ = coordinator.Stop(stopCtx)
}

func TestCoordination_G_ReconnectionAndRecovery(t *testing.T) {
	var online atomic.Bool
	online.Store(false)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !online.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "temporarily unavailable"})
			return
		}

		switch r.URL.Path {
		case "/api/v1/nodes/register":
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(RegistrationResponse{
				Status:  "registered",
				Message: "Registered",
			})
		case "/api/v1/nodes/heartbeat":
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(HeartbeatResponse{
				Status:  "ok",
				Message: "Heartbeat accepted",
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	ht := health.NewTracker()
	_ = ht.RegisterCheck(health.CheckStorage, true)
	_ = ht.RegisterCheck(health.CheckControlPlane, false)
	_ = ht.SetCheckStatus(health.CheckStorage, health.StatusOk, "ok")
	_ = ht.Transition(health.StateReady)

	metricRec := metrics.NewDefaultRecorder(nil)
	auditRec := &fakeAuditRecorder{}
	tracker := NewTracker("recovering-node-01", metricRec, ht, auditRec)

	cfg := DefaultConfig("recovering-node-01")
	cfg.HeartbeatInterval = 40 * time.Millisecond
	cfg.InitialReconnectInterval = 20 * time.Millisecond
	cfg.RequestTimeout = 50 * time.Millisecond

	client := NewHTTPClient(server.URL, 100*time.Millisecond)
	coordinator := NewCoordinator(cfg, client, tracker, metricRec, ht, auditRec)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := coordinator.Start(ctx); err != nil {
		t.Fatalf("failed to start coordinator: %v", err)
	}

	// Wait while server is offline
	time.Sleep(60 * time.Millisecond)
	if tracker.State() != StateDegraded {
		t.Errorf("expected StateDegraded while offline, got %v", tracker.State())
	}

	// Bring server online
	online.Store(true)

	// Wait for coordinator to reconnect and register
	time.Sleep(100 * time.Millisecond)

	if tracker.State() != StateConnected {
		t.Errorf("expected StateConnected after server came online, got %v", tracker.State())
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer stopCancel()
	_ = coordinator.Stop(stopCtx)
}

func TestCoordination_H_UnknownNodeReregistration(t *testing.T) {
	var registered atomic.Bool
	registered.Store(false)
	var registerCalls atomic.Int64
	var heartbeatCalls atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/nodes/register":
			registerCalls.Add(1)
			registered.Store(true)
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(RegistrationResponse{
				Status:  "registered",
				Message: "Registered",
			})
		case "/api/v1/nodes/heartbeat":
			heartbeatCalls.Add(1)
			if !registered.Load() {
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "node not found"})
				return
			}
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(HeartbeatResponse{
				Status:  "ok",
				Message: "Heartbeat accepted",
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	ht := health.NewTracker()
	_ = ht.RegisterCheck(health.CheckStorage, true)
	_ = ht.RegisterCheck(health.CheckControlPlane, false)
	_ = ht.SetCheckStatus(health.CheckStorage, health.StatusOk, "ok")
	_ = ht.Transition(health.StateReady)

	metricRec := metrics.NewDefaultRecorder(nil)
	auditRec := &fakeAuditRecorder{}
	tracker := NewTracker("rereg-node-01", metricRec, ht, auditRec)

	cfg := DefaultConfig("rereg-node-01")
	cfg.HeartbeatInterval = 40 * time.Millisecond
	cfg.RequestTimeout = 50 * time.Millisecond

	client := NewHTTPClient(server.URL, 100*time.Millisecond)
	coordinator := NewCoordinator(cfg, client, tracker, metricRec, ht, auditRec)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := coordinator.Start(ctx); err != nil {
		t.Fatalf("failed to start coordinator: %v", err)
	}

	// Wait for initial registration & at least 1 heartbeat
	time.Sleep(60 * time.Millisecond)
	if registerCalls.Load() < 1 {
		t.Fatalf("expected initial registration")
	}

	// Simulate control plane reboot/memory wipe: node is no longer registered
	registered.Store(false)

	// Wait for coordinator to send heartbeat, receive 404, and re-register
	time.Sleep(100 * time.Millisecond)

	if registerCalls.Load() < 2 {
		t.Errorf("expected coordinator to re-register upon 404, got %d registrations", registerCalls.Load())
	}
	if tracker.State() != StateConnected {
		t.Errorf("expected StateConnected after re-registration, got %v", tracker.State())
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer stopCancel()
	_ = coordinator.Stop(stopCtx)
}

func TestCoordination_I_AuditEventsComprehensive(t *testing.T) {
	auditRec := &fakeAuditRecorder{}
	tracker := NewTracker("audit-node-01", nil, nil, auditRec)
	ctx := context.Background()

	// 1. Connection events
	tracker.Transition(ctx, StateConnected, "connected")
	tracker.Transition(ctx, StateDegraded, "connection lost")

	// 2. Heartbeat events
	tracker.RecordHeartbeat(ctx, true, 10*time.Millisecond, "")
	tracker.RecordHeartbeat(ctx, false, 5*time.Millisecond, "timeout")

	if auditRec.countEvents(audit.EventTypeControlPlaneConnected) != 1 {
		t.Errorf("expected 1 EventTypeControlPlaneConnected")
	}
	if auditRec.countEvents(audit.EventTypeControlPlaneDisconnected) != 1 {
		t.Errorf("expected 1 EventTypeControlPlaneDisconnected")
	}
	if auditRec.countEvents(audit.EventTypeControlPlaneHeartbeatSuccess) != 1 {
		t.Errorf("expected 1 EventTypeControlPlaneHeartbeatSuccess")
	}
	if auditRec.countEvents(audit.EventTypeControlPlaneHeartbeatFailed) != 1 {
		t.Errorf("expected 1 EventTypeControlPlaneHeartbeatFailed")
	}
}

func TestCoordination_J_ConcurrencySafety(t *testing.T) {
	ht := health.NewTracker()
	_ = ht.RegisterCheck(health.CheckControlPlane, false)
	metricRec := metrics.NewDefaultRecorder(nil)
	auditRec := &fakeAuditRecorder{}
	tracker := NewTracker("concurrent-node-01", metricRec, ht, auditRec)

	var wg sync.WaitGroup
	const workers = 10
	const iterations = 50

	ctx := context.Background()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				if (id+j)%2 == 0 {
					tracker.Transition(ctx, StateConnected, "ok")
					tracker.RecordHeartbeat(ctx, true, time.Duration(j)*time.Millisecond, "")
				} else {
					tracker.Transition(ctx, StateDegraded, "warn")
					tracker.RecordHeartbeat(ctx, false, time.Duration(j)*time.Millisecond, "err")
				}
				_ = tracker.State()
				_ = tracker.LastConnected()
				_ = tracker.LastHeartbeat()
				_ = tracker.LastError()
			}
		}(i)
	}

	wg.Wait()
}

func TestCoordination_SecureClientEndToEnd(t *testing.T) {
	const secret = "coordination-test-secret-key-32c"
	const nodeID = "secure-node-01"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		ts := r.Header.Get(types.HeaderTimestamp)
		nonce := r.Header.Get(types.HeaderNonce)
		sig := r.Header.Get(types.HeaderSignature)
		headerNode := r.Header.Get(types.HeaderNodeID)

		if headerNode != nodeID {
			w.WriteHeader(http.StatusForbidden)
			return
		}

		target := r.URL.RequestURI()
		if target == "" {
			target = r.URL.Path
		}
		if !types.VerifySignature([]byte(secret), r.Method, target, headerNode, ts, nonce, sig, body) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		switch r.URL.Path {
		case "/api/v1/nodes/register":
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(RegistrationResponse{Status: "registered"})
		case "/api/v1/nodes/heartbeat":
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(HeartbeatResponse{Status: "acknowledged"})
		case "/api/v1/nodes/" + nodeID:
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(types.NodeRegistration{NodeID: nodeID})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := NewHTTPClient(server.URL, 1*time.Second)
	client.SetAuth(nodeID, secret)

	ctx := context.Background()

	// 1. Register
	reg := &types.NodeRegistration{
		NodeID:       nodeID,
		Hostname:     "secure-host",
		RegisteredAt: time.Now().UTC(),
	}
	regResp, err := client.RegisterNode(ctx, reg)
	if err != nil {
		t.Fatalf("RegisterNode failed: %v", err)
	}
	if regResp.Status != "registered" {
		t.Errorf("expected registered, got %s", regResp.Status)
	}

	// 2. Heartbeat
	hb := &types.Heartbeat{
		NodeID:         nodeID,
		SequenceNumber: 1,
		Timestamp:      time.Now().UTC(),
		Status:         types.NodeStatusHealthy,
	}
	hbResp, err := client.SendHeartbeat(ctx, hb)
	if err != nil {
		t.Fatalf("SendHeartbeat failed: %v", err)
	}
	if hbResp.Status != "acknowledged" {
		t.Errorf("expected acknowledged, got %s", hbResp.Status)
	}

	// 3. GetNode
	node, err := client.GetNode(ctx, nodeID)
	if err != nil {
		t.Fatalf("GetNode failed: %v", err)
	}
	if node.NodeID != nodeID {
		t.Errorf("expected %s, got %s", nodeID, node.NodeID)
	}
}

func TestCoordination_SecureClientRejectsBadSecret(t *testing.T) {
	const secret = "coordination-test-secret-key-32c"
	const nodeID = "secure-node-01"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		ts := r.Header.Get(types.HeaderTimestamp)
		nonce := r.Header.Get(types.HeaderNonce)
		sig := r.Header.Get(types.HeaderSignature)
		headerNode := r.Header.Get(types.HeaderNodeID)
		target := r.URL.RequestURI()
		if target == "" {
			target = r.URL.Path
		}
		if !types.VerifySignature([]byte(secret), r.Method, target, headerNode, ts, nonce, sig, body) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Configure client with bad secret
	client := NewHTTPClient(server.URL, 1*time.Second)
	client.SetAuth(nodeID, "wrong-secret-key-attacker")

	ctx := context.Background()

	reg := &types.NodeRegistration{
		NodeID:       nodeID,
		Hostname:     "secure-host",
		RegisteredAt: time.Now().UTC(),
	}
	_, err := client.RegisterNode(ctx, reg)
	if err == nil {
		t.Fatalf("expected error for bad secret, got nil")
	}
	if !errors.Is(err, ErrAuthFailed) {
		t.Errorf("expected error wrapping ErrAuthFailed, got %v", err)
	}

	hb := &types.Heartbeat{
		NodeID:         nodeID,
		SequenceNumber: 1,
		Timestamp:      time.Now().UTC(),
		Status:         types.NodeStatusHealthy,
	}
	_, err = client.SendHeartbeat(ctx, hb)
	if err == nil {
		t.Fatalf("expected error for bad secret on heartbeat, got nil")
	}
	if !errors.Is(err, ErrAuthFailed) {
		t.Errorf("expected error wrapping ErrAuthFailed, got %v", err)
	}
}

func TestCoordination_AuthFailureDegradesSafelyWithoutBlocking(t *testing.T) {
	// Server returns 401 Unauthorized for all incoming requests
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":   "unauthorized",
			"message": "invalid signature",
		})
	}))
	defer server.Close()

	client := NewHTTPClient(server.URL, 100*time.Millisecond)
	client.SetAuth("auth-node-01", "some-secret")

	ht := health.NewTracker()
	_ = ht.RegisterCheck(health.CheckStorage, true)
	_ = ht.RegisterCheck(health.CheckControlPlane, false)
	_ = ht.SetCheckStatus(health.CheckStorage, health.StatusOk, "storage ready")
	_ = ht.Transition(health.StateReady)

	metricRec := metrics.NewDefaultRecorder(nil)
	auditRec := &fakeAuditRecorder{}
	tracker := NewTracker("auth-node-01", metricRec, ht, auditRec)

	cfg := DefaultConfig("auth-node-01")
	cfg.HeartbeatInterval = 50 * time.Millisecond
	cfg.InitialReconnectInterval = 20 * time.Millisecond
	cfg.RequestTimeout = 50 * time.Millisecond

	coordinator := NewCoordinator(cfg, client, tracker, metricRec, ht, auditRec)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := coordinator.Start(ctx); err != nil {
		t.Fatalf("failed to start coordinator: %v", err)
	}

	// Poll until degraded
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && tracker.State() != StateDegraded {
		time.Sleep(10 * time.Millisecond)
	}

	// INVARIANT: When auth fails, coordinator enters StateDegraded, but edge agent MUST remain READY offline!
	if tracker.State() != StateDegraded {
		t.Errorf("expected StateDegraded after auth rejection, got %v", tracker.State())
	}
	if !ht.IsReady() {
		t.Errorf("CRITICAL: edge agent MUST remain READY offline even when control plane auth fails")
	}

	// Verify CONTROL_PLANE_AUTH_FAILED audit event was emitted
	if auditRec.countEvents(audit.EventTypeControlPlaneAuthFailed) == 0 {
		t.Errorf("expected at least 1 CONTROL_PLANE_AUTH_FAILED audit event, got %d", auditRec.countEvents(audit.EventTypeControlPlaneAuthFailed))
	}

	// Clean stop
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer stopCancel()
	if err := coordinator.Stop(stopCtx); err != nil {
		t.Fatalf("failed to stop coordinator: %v", err)
	}
}

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

type fakeClientWithCallbacks struct {
	mu          sync.Mutex
	onRegister  func(ctx context.Context, reg *types.NodeRegistration) (*RegistrationResponse, error)
	onHeartbeat func(ctx context.Context, hb *types.Heartbeat) (*HeartbeatResponse, error)
	onGetNode   func(ctx context.Context, nodeID string) (*types.NodeRegistration, error)
}

func (f *fakeClientWithCallbacks) RegisterNode(ctx context.Context, reg *types.NodeRegistration) (*RegistrationResponse, error) {
	f.mu.Lock()
	fn := f.onRegister
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, reg)
	}
	return &RegistrationResponse{Status: "registered"}, nil
}

func (f *fakeClientWithCallbacks) SendHeartbeat(ctx context.Context, hb *types.Heartbeat) (*HeartbeatResponse, error) {
	f.mu.Lock()
	fn := f.onHeartbeat
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, hb)
	}
	return &HeartbeatResponse{Status: "ok"}, nil
}

func (f *fakeClientWithCallbacks) GetNode(ctx context.Context, nodeID string) (*types.NodeRegistration, error) {
	f.mu.Lock()
	fn := f.onGetNode
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, nodeID)
	}
	return &types.NodeRegistration{NodeID: nodeID}, nil
}

func TestCoordination_RetryScheduling_ConsecutiveFailuresFollowBackoff(t *testing.T) {
	// Verifies:
	// 1. First failure schedules the expected initial retry delay.
	// 2. Consecutive failures follow the configured backoff factor and maximum.
	// 3. Delays are genuinely scheduled via timerFunc, not ignored or spun in a tight loop.
	var scheduledDelays []time.Duration
	var delaysMu sync.Mutex

	client := &fakeClientWithCallbacks{
		onRegister: func(ctx context.Context, reg *types.NodeRegistration) (*RegistrationResponse, error) {
			return nil, errors.New("temporary network outage")
		},
	}

	metricRec := metrics.NewDefaultRecorder(nil)
	auditRec := &fakeAuditRecorder{}
	tracker := NewTracker("backoff-node-01", metricRec, nil, auditRec)

	cfg := DefaultConfig("backoff-node-01")
	cfg.HeartbeatInterval = 10 * time.Second
	cfg.InitialReconnectInterval = 100 * time.Millisecond
	cfg.BackoffFactor = 2.0
	cfg.MaxReconnectInterval = 400 * time.Millisecond
	cfg.RequestTimeout = 50 * time.Millisecond

	coordinator := NewCoordinator(cfg, client, tracker, metricRec, nil, auditRec)

	timerTrigger := make(chan time.Time, 10)
	coordinator.SetTimerFuncForTest(func(d time.Duration) (<-chan time.Time, func() bool) {
		delaysMu.Lock()
		scheduledDelays = append(scheduledDelays, d)
		delaysMu.Unlock()
		return timerTrigger, func() bool { return true }
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := coordinator.Start(ctx); err != nil {
		t.Fatalf("failed to start coordinator: %v", err)
	}

	// Wait until at least 4 failures have been scheduled
	for i := 0; i < 4; i++ {
		// Verify that a delay was recorded before triggering the timer
		deadline := time.Now().Add(1 * time.Second)
		for {
			delaysMu.Lock()
			count := len(scheduledDelays)
			delaysMu.Unlock()
			if count > i {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for scheduled delay %d", i+1)
			}
			time.Sleep(1 * time.Millisecond)
		}
		// Trigger next attempt
		timerTrigger <- time.Now()
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer stopCancel()
	_ = coordinator.Stop(stopCtx)

	delaysMu.Lock()
	defer delaysMu.Unlock()

	if len(scheduledDelays) < 4 {
		t.Fatalf("expected at least 4 scheduled delays, got %d: %v", len(scheduledDelays), scheduledDelays)
	}

	// 1st retry delay must match InitialReconnectInterval
	if scheduledDelays[0] != 100*time.Millisecond {
		t.Errorf("expected 1st delay to be 100ms, got %v", scheduledDelays[0])
	}
	// 2nd retry delay must be initial * factor = 200ms
	if scheduledDelays[1] != 200*time.Millisecond {
		t.Errorf("expected 2nd delay to be 200ms, got %v", scheduledDelays[1])
	}
	// 3rd retry delay must be capped at MaxReconnectInterval = 400ms
	if scheduledDelays[2] != 400*time.Millisecond {
		t.Errorf("expected 3rd delay to be 400ms, got %v", scheduledDelays[2])
	}
	// 4th retry delay must remain capped at MaxReconnectInterval = 400ms
	if scheduledDelays[3] != 400*time.Millisecond {
		t.Errorf("expected 4th delay to be 400ms, got %v", scheduledDelays[3])
	}
}

func TestCoordination_RetryScheduling_RecoveryResetsBackoffAndResumesCadence(t *testing.T) {
	// Verifies:
	// 1. Failures increase backoff delay.
	// 2. Recovery on registration/heartbeat resets backoff to InitialReconnectInterval.
	// 3. While healthy, normal HeartbeatInterval is scheduled.
	var scheduledDelays []time.Duration
	var delaysMu sync.Mutex

	var shouldFail atomic.Bool
	shouldFail.Store(true)

	client := &fakeClientWithCallbacks{
		onRegister: func(ctx context.Context, reg *types.NodeRegistration) (*RegistrationResponse, error) {
			if shouldFail.Load() {
				return nil, errors.New("transient control plane outage")
			}
			return &RegistrationResponse{Status: "registered"}, nil
		},
		onHeartbeat: func(ctx context.Context, hb *types.Heartbeat) (*HeartbeatResponse, error) {
			return &HeartbeatResponse{Status: "ok"}, nil
		},
	}

	metricRec := metrics.NewDefaultRecorder(nil)
	tracker := NewTracker("recovery-node-01", metricRec, nil, nil)

	cfg := DefaultConfig("recovery-node-01")
	cfg.HeartbeatInterval = 5 * time.Second
	cfg.InitialReconnectInterval = 50 * time.Millisecond
	cfg.BackoffFactor = 2.0
	cfg.MaxReconnectInterval = 500 * time.Millisecond

	coordinator := NewCoordinator(cfg, client, tracker, metricRec, nil, nil)

	timerTrigger := make(chan time.Time, 10)
	coordinator.SetTimerFuncForTest(func(d time.Duration) (<-chan time.Time, func() bool) {
		delaysMu.Lock()
		scheduledDelays = append(scheduledDelays, d)
		delaysMu.Unlock()
		return timerTrigger, func() bool { return true }
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := coordinator.Start(ctx); err != nil {
		t.Fatalf("failed to start coordinator: %v", err)
	}

	// 1st attempt fails -> schedules 50ms
	deadline := time.Now().Add(1 * time.Second)
	for {
		delaysMu.Lock()
		count := len(scheduledDelays)
		delaysMu.Unlock()
		if count >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for 1st delay")
		}
		time.Sleep(1 * time.Millisecond)
	}

	// Bring control plane back online
	shouldFail.Store(false)
	timerTrigger <- time.Now()

	// Coordinator should now register successfully and schedule HeartbeatInterval (5s)
	deadline = time.Now().Add(1 * time.Second)
	for {
		delaysMu.Lock()
		count := len(scheduledDelays)
		delaysMu.Unlock()
		if count >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for 2nd delay (heartbeat interval)")
		}
		time.Sleep(1 * time.Millisecond)
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer stopCancel()
	_ = coordinator.Stop(stopCtx)

	delaysMu.Lock()
	defer delaysMu.Unlock()

	if scheduledDelays[0] != 50*time.Millisecond {
		t.Errorf("expected 1st delay to be 50ms, got %v", scheduledDelays[0])
	}
	if scheduledDelays[1] != 5*time.Second {
		t.Errorf("expected 2nd delay after recovery to be HeartbeatInterval (5s), got %v", scheduledDelays[1])
	}
	if backoff := coordinator.BackoffDuration(); backoff != 50*time.Millisecond {
		t.Errorf("expected backoff to be reset to 50ms upon recovery, got %v", backoff)
	}
}

func TestCoordination_RetryScheduling_CancellationExitsPromptly(t *testing.T) {
	// Verifies that when coordinator is waiting in backoff sleep,
	// cancelling context terminates the run loop immediately without waiting out the duration.
	timerStopped := make(chan struct{})
	timerBlocked := make(chan time.Time) // Never fires

	client := &fakeClientWithCallbacks{
		onRegister: func(ctx context.Context, reg *types.NodeRegistration) (*RegistrationResponse, error) {
			return nil, errors.New("offline")
		},
	}

	tracker := NewTracker("cancel-node-01", nil, nil, nil)
	cfg := DefaultConfig("cancel-node-01")
	cfg.InitialReconnectInterval = 1 * time.Hour // Huge duration

	coordinator := NewCoordinator(cfg, client, tracker, nil, nil, nil)
	coordinator.SetTimerFuncForTest(func(d time.Duration) (<-chan time.Time, func() bool) {
		return timerBlocked, func() bool {
			close(timerStopped)
			return true
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	if err := coordinator.Start(ctx); err != nil {
		t.Fatalf("failed to start coordinator: %v", err)
	}

	// Trigger cancellation while coordinator is waiting in 1-hour backoff
	start := time.Now()
	cancel()

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()
	if err := coordinator.Stop(stopCtx); err != nil {
		t.Fatalf("expected coordinator to stop promptly, got error: %v", err)
	}

	elapsed := time.Since(start)
	if elapsed > 1*time.Second {
		t.Errorf("coordinator took too long to exit on cancellation: %v", elapsed)
	}

	select {
	case <-timerStopped:
		// Timer stop func was invoked cleanly
	case <-time.After(100 * time.Millisecond):
		t.Errorf("expected timer cleanup/stop func to be called")
	}
}

func TestCoordination_RetryScheduling_OfflineTelemetryPreserved(t *testing.T) {
	// Verifies the core invariant:
	// EDGE OPERATES OFFLINE -> PERSISTS TO WAL -> RECONNECTS
	// While the control plane is failing and backoff is active,
	// the edge agent remains healthy/ready and local storage persists batches without blockage.
	dbPath := t.TempDir() + "/offline_durability.db"
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite store: %v", err)
	}
	defer store.Close()

	client := &fakeClientWithCallbacks{
		onRegister: func(ctx context.Context, reg *types.NodeRegistration) (*RegistrationResponse, error) {
			return nil, errors.New("control plane unreachable")
		},
	}

	ht := health.NewTracker()
	_ = ht.RegisterCheck(health.CheckStorage, true)
	_ = ht.RegisterCheck(health.CheckControlPlane, false)
	_ = ht.SetCheckStatus(health.CheckStorage, health.StatusOk, "sqlite ready")
	_ = ht.Transition(health.StateReady)

	metricRec := metrics.NewDefaultRecorder(nil)
	tracker := NewTracker("durability-node-01", metricRec, ht, nil)

	cfg := DefaultConfig("durability-node-01")
	cfg.HeartbeatInterval = 50 * time.Millisecond
	cfg.InitialReconnectInterval = 100 * time.Millisecond
	cfg.RequestTimeout = 50 * time.Millisecond

	coordinator := NewCoordinator(cfg, client, tracker, metricRec, ht, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := coordinator.Start(ctx); err != nil {
		t.Fatalf("failed to start coordinator: %v", err)
	}

	// While control plane is unreachable and coordinator is in degraded state:
	time.Sleep(50 * time.Millisecond)

	if tracker.State() != StateDegraded {
		t.Errorf("expected StateDegraded, got %v", tracker.State())
	}
	if !ht.IsReady() {
		t.Errorf("CRITICAL INVARIANT VIOLATION: Edge agent readiness must NOT fail when control plane is degraded")
	}

	// Edge persists local telemetry to WAL store during control-plane outage
	batch := &types.TelemetryBatch{
		BatchID:        "offline-batch-001",
		NodeID:         "durability-node-01",
		SequenceNumber: 1,
		CollectedAt:    time.Now().UTC(),
		Metrics: []types.MetricSample{
			{
				Name:      "system.cpu.utilization",
				Value:     42.5,
				Timestamp: time.Now().UTC(),
			},
		},
	}

	if err := store.PersistBatch(ctx, batch); err != nil {
		t.Fatalf("failed to store local batch offline: %v", err)
	}

	retrieved, err := store.GetBatch(ctx, "offline-batch-001")
	if err != nil {
		t.Fatalf("failed to retrieve local batch: %v", err)
	}
	if retrieved.BatchID != "offline-batch-001" {
		t.Errorf("expected offline-batch-001, got %s", retrieved.BatchID)
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer stopCancel()
	_ = coordinator.Stop(stopCtx)
}

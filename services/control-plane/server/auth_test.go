package server

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

const (
	testSharedSecret = "fleet-wide-super-secret-key-32chars"
	testAdminToken   = "adm-token-secure-999-super-secret"
)

func setupAuthServer() (*Server, *httptest.Server) {
	srv := NewServer(newDiscardLogger())
	srv.SetAuthConfig(AuthConfig{
		Enabled:          true,
		SharedSecret:     testSharedSecret,
		AdminToken:       testAdminToken,
		MaxClockSkew:     5 * time.Minute,
		ReplayProtection: true,
	})
	ts := httptest.NewServer(srv.Routes())
	return srv, ts
}

func signedRequest(t *testing.T, method, url, nodeID string, payload []byte) *http.Request {
	req, err := http.NewRequest(method, url, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	target := req.URL.RequestURI()
	if target == "" {
		target = req.URL.Path
	}
	headers, err := types.CreateAuthHeaders([]byte(testSharedSecret), method, target, nodeID, payload)
	if err != nil {
		t.Fatalf("failed to sign request: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return req
}

// 1. Public health check accessible without credentials
func TestAuth_PublicHealthzWithoutAuth(t *testing.T) {
	_, ts := setupAuthServer()
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK for /healthz without auth, got %d", resp.StatusCode)
	}
}

// 2. Valid node registration with valid HMAC signature succeeds (201 Created)
func TestAuth_NodeRegister_Success(t *testing.T) {
	_, ts := setupAuthServer()
	defer ts.Close()

	reg := types.NodeRegistration{
		NodeID:       "edge-node-alpha",
		Hostname:     "alpha-host",
		RegisteredAt: time.Now().UTC(),
	}
	body, _ := json.Marshal(reg)

	req := signedRequest(t, http.MethodPost, ts.URL+"/api/v1/nodes/register", reg.NodeID, body)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Errorf("expected 201 Created, got %d", resp.StatusCode)
	}
}

// 3. Missing auth headers when auth is enabled returns 401 Unauthorized
func TestAuth_MissingAuthHeaders(t *testing.T) {
	_, ts := setupAuthServer()
	defer ts.Close()

	reg := types.NodeRegistration{
		NodeID:       "edge-node-alpha",
		Hostname:     "alpha-host",
		RegisteredAt: time.Now().UTC(),
	}
	body, _ := json.Marshal(reg)

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/nodes/register", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized for missing auth headers, got %d", resp.StatusCode)
	}
}

// 4. Tampered body bytes breaks signature and returns 401 Unauthorized
func TestAuth_TamperedBody(t *testing.T) {
	_, ts := setupAuthServer()
	defer ts.Close()

	reg := types.NodeRegistration{
		NodeID:       "edge-node-alpha",
		Hostname:     "alpha-host",
		RegisteredAt: time.Now().UTC(),
	}
	body, _ := json.Marshal(reg)

	req := signedRequest(t, http.MethodPost, ts.URL+"/api/v1/nodes/register", reg.NodeID, body)

	// Tamper with body after signing
	tamperedBody := bytes.Replace(body, []byte("alpha-host"), []byte("hacked-host"), 1)
	req.Body = io.NopCloser(bytes.NewReader(tamperedBody))
	req.ContentLength = int64(len(tamperedBody))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized for tampered body, got %d", resp.StatusCode)
	}
}

// 5. Tampered HTTP method or path breaks signature and returns 401 Unauthorized
func TestAuth_TamperedMethodOrPath(t *testing.T) {
	_, ts := setupAuthServer()
	defer ts.Close()

	reg := types.NodeRegistration{
		NodeID:       "edge-node-alpha",
		Hostname:     "alpha-host",
		RegisteredAt: time.Now().UTC(),
	}
	body, _ := json.Marshal(reg)

	// Sign for /api/v1/nodes/register but send to /api/v1/nodes/heartbeat
	headers, _ := types.CreateAuthHeaders([]byte(testSharedSecret), http.MethodPost, "/api/v1/nodes/register", reg.NodeID, body)

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/nodes/heartbeat", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized for tampered path, got %d", resp.StatusCode)
	}
}

// 6. Expired timestamp returns 401 Unauthorized
func TestAuth_ExpiredTimestamp(t *testing.T) {
	_, ts := setupAuthServer()
	defer ts.Close()

	reg := types.NodeRegistration{
		NodeID:       "edge-node-alpha",
		Hostname:     "alpha-host",
		RegisteredAt: time.Now().UTC(),
	}
	body, _ := json.Marshal(reg)

	expiredTime := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339)
	nonce, _ := types.GenerateNonce()
	sig := types.SignRequest([]byte(testSharedSecret), http.MethodPost, "/api/v1/nodes/register", reg.NodeID, expiredTime, nonce, body)

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/nodes/register", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(types.HeaderNodeID, reg.NodeID)
	req.Header.Set(types.HeaderTimestamp, expiredTime)
	req.Header.Set(types.HeaderNonce, nonce)
	req.Header.Set(types.HeaderSignature, sig)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized for expired timestamp, got %d", resp.StatusCode)
	}
}

// 7. Future timestamp returns 401 Unauthorized
func TestAuth_FutureTimestamp(t *testing.T) {
	_, ts := setupAuthServer()
	defer ts.Close()

	reg := types.NodeRegistration{
		NodeID:       "edge-node-alpha",
		Hostname:     "alpha-host",
		RegisteredAt: time.Now().UTC(),
	}
	body, _ := json.Marshal(reg)

	futureTime := time.Now().UTC().Add(10 * time.Minute).Format(time.RFC3339)
	nonce, _ := types.GenerateNonce()
	sig := types.SignRequest([]byte(testSharedSecret), http.MethodPost, "/api/v1/nodes/register", reg.NodeID, futureTime, nonce, body)

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/nodes/register", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(types.HeaderNodeID, reg.NodeID)
	req.Header.Set(types.HeaderTimestamp, futureTime)
	req.Header.Set(types.HeaderNonce, nonce)
	req.Header.Set(types.HeaderSignature, sig)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized for future timestamp, got %d", resp.StatusCode)
	}
}

// 8. Replayed request returns 401 Unauthorized
func TestAuth_ReplayedRequest(t *testing.T) {
	_, ts := setupAuthServer()
	defer ts.Close()

	reg := types.NodeRegistration{
		NodeID:       "edge-node-alpha",
		Hostname:     "alpha-host",
		RegisteredAt: time.Now().UTC(),
	}
	body, _ := json.Marshal(reg)

	req1 := signedRequest(t, http.MethodPost, ts.URL+"/api/v1/nodes/register", reg.NodeID, body)
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatalf("first request failed: %v", err)
	}
	defer resp1.Body.Close()
	if resp1.StatusCode != http.StatusCreated {
		t.Fatalf("first request expected 201, got %d", resp1.StatusCode)
	}

	// Replay exact same request with same nonce & signature
	req2, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/nodes/register", bytes.NewReader(body))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set(types.HeaderNodeID, req1.Header.Get(types.HeaderNodeID))
	req2.Header.Set(types.HeaderTimestamp, req1.Header.Get(types.HeaderTimestamp))
	req2.Header.Set(types.HeaderNonce, req1.Header.Get(types.HeaderNonce))
	req2.Header.Set(types.HeaderSignature, req1.Header.Get(types.HeaderSignature))

	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("replay request failed: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized for replayed request, got %d", resp2.StatusCode)
	}
}

// 9. Concurrent replay requests with same nonce: exactly one succeeds
func TestAuth_ConcurrentNonceReplay(t *testing.T) {
	_, ts := setupAuthServer()
	defer ts.Close()

	reg := types.NodeRegistration{
		NodeID:       "edge-node-alpha",
		Hostname:     "alpha-host",
		RegisteredAt: time.Now().UTC(),
	}
	body, _ := json.Marshal(reg)

	tsStr := time.Now().UTC().Format(time.RFC3339)
	nonce, _ := types.GenerateNonce()
	sig := types.SignRequest([]byte(testSharedSecret), http.MethodPost, "/api/v1/nodes/register", reg.NodeID, tsStr, nonce, body)

	concurrency := 10
	var successes int32
	var replays int32
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/nodes/register", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(types.HeaderNodeID, reg.NodeID)
			req.Header.Set(types.HeaderTimestamp, tsStr)
			req.Header.Set(types.HeaderNonce, nonce)
			req.Header.Set(types.HeaderSignature, sig)

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusCreated {
				atomic.AddInt32(&successes, 1)
			} else if resp.StatusCode == http.StatusUnauthorized {
				atomic.AddInt32(&replays, 1)
			}
		}()
	}

	wg.Wait()

	if successes != 1 {
		t.Errorf("expected exactly 1 success out of concurrent replays, got %d", successes)
	}
	if replays != int32(concurrency-1) {
		t.Errorf("expected %d replays rejected with 401, got %d", concurrency-1, replays)
	}
}

// 10. Node Heartbeat with valid HMAC signature succeeds
func TestAuth_NodeHeartbeat_Success(t *testing.T) {
	_, ts := setupAuthServer()
	defer ts.Close()

	// Register node first
	reg := types.NodeRegistration{
		NodeID:       "edge-node-beta",
		Hostname:     "beta-host",
		RegisteredAt: time.Now().UTC(),
	}
	bodyReg, _ := json.Marshal(reg)
	reqReg := signedRequest(t, http.MethodPost, ts.URL+"/api/v1/nodes/register", reg.NodeID, bodyReg)
	respReg, err := http.DefaultClient.Do(reqReg)
	if err != nil || respReg.StatusCode != http.StatusCreated {
		t.Fatalf("node registration failed: %v", err)
	}
	respReg.Body.Close()

	// Heartbeat
	hb := types.Heartbeat{
		NodeID:         "edge-node-beta",
		SequenceNumber: 1,
		Timestamp:      time.Now().UTC(),
		Status:         types.NodeStatusHealthy,
	}
	bodyHb, _ := json.Marshal(hb)
	reqHb := signedRequest(t, http.MethodPost, ts.URL+"/api/v1/nodes/heartbeat", hb.NodeID, bodyHb)
	respHb, err := http.DefaultClient.Do(reqHb)
	if err != nil {
		t.Fatalf("heartbeat request failed: %v", err)
	}
	defer respHb.Body.Close()

	if respHb.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK for valid heartbeat, got %d", respHb.StatusCode)
	}
}

// 11. Node impersonation on registration returns 403 Forbidden
func TestAuth_NodeImpersonation_Registration(t *testing.T) {
	_, ts := setupAuthServer()
	defer ts.Close()

	reg := types.NodeRegistration{
		NodeID:       "node-victim",
		Hostname:     "victim-host",
		RegisteredAt: time.Now().UTC(),
	}
	body, _ := json.Marshal(reg)

	// Attacker signs as "node-attacker" in header, but attempts to register "node-victim"
	req := signedRequest(t, http.MethodPost, ts.URL+"/api/v1/nodes/register", "node-attacker", body)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden for registration impersonation, got %d", resp.StatusCode)
	}
}

// 12. Node impersonation on heartbeat returns 403 Forbidden
func TestAuth_NodeImpersonation_Heartbeat(t *testing.T) {
	_, ts := setupAuthServer()
	defer ts.Close()

	hb := types.Heartbeat{
		NodeID:         "node-victim",
		SequenceNumber: 1,
		Timestamp:      time.Now().UTC(),
		Status:         types.NodeStatusHealthy,
	}
	body, _ := json.Marshal(hb)

	// Attacker signs as "node-attacker" in header, but sends heartbeat for "node-victim"
	req := signedRequest(t, http.MethodPost, ts.URL+"/api/v1/nodes/heartbeat", "node-attacker", body)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden for heartbeat impersonation, got %d", resp.StatusCode)
	}
}

// 13. Node querying another node's record returns 403 Forbidden
func TestAuth_NodeImpersonation_SingleNodeQuery(t *testing.T) {
	_, ts := setupAuthServer()
	defer ts.Close()

	// Register node-b first
	regB := types.NodeRegistration{
		NodeID:       "node-b",
		Hostname:     "host-b",
		RegisteredAt: time.Now().UTC(),
	}
	bodyB, _ := json.Marshal(regB)
	reqReg := signedRequest(t, http.MethodPost, ts.URL+"/api/v1/nodes/register", "node-b", bodyB)
	r1, _ := http.DefaultClient.Do(reqReg)
	r1.Body.Close()

	// Node-a tries to query /api/v1/nodes/node-b
	req := signedRequest(t, http.MethodGet, ts.URL+"/api/v1/nodes/node-b", "node-a", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden when node-a queries node-b, got %d", resp.StatusCode)
	}
}

// 14. Node querying own record succeeds (200 OK)
func TestAuth_OwnNodeQuery_Success(t *testing.T) {
	_, ts := setupAuthServer()
	defer ts.Close()

	reg := types.NodeRegistration{
		NodeID:       "node-a",
		Hostname:     "host-a",
		RegisteredAt: time.Now().UTC(),
	}
	body, _ := json.Marshal(reg)
	reqReg := signedRequest(t, http.MethodPost, ts.URL+"/api/v1/nodes/register", "node-a", body)
	r1, _ := http.DefaultClient.Do(reqReg)
	r1.Body.Close()

	// Node-a queries /api/v1/nodes/node-a
	req := signedRequest(t, http.MethodGet, ts.URL+"/api/v1/nodes/node-a", "node-a", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK when node-a queries own record, got %d", resp.StatusCode)
	}
}

// 15. Listing all nodes requires Admin authorization (node credential returns 403 Forbidden)
func TestAuth_ListAllNodes_RequiresAdmin(t *testing.T) {
	_, ts := setupAuthServer()
	defer ts.Close()

	// 1. Node credentials: 403 Forbidden
	reqNode := signedRequest(t, http.MethodGet, ts.URL+"/api/v1/nodes", "node-a", nil)
	respNode, err := http.DefaultClient.Do(reqNode)
	if err != nil {
		t.Fatalf("node request failed: %v", err)
	}
	defer respNode.Body.Close()
	if respNode.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden for node credentials on /nodes, got %d", respNode.StatusCode)
	}

	// 2. Admin Bearer token: 200 OK
	reqAdmin, _ := http.NewRequest(http.MethodGet, ts.URL+"/api/v1/nodes", nil)
	reqAdmin.Header.Set("Authorization", "Bearer "+testAdminToken)
	respAdmin, err := http.DefaultClient.Do(reqAdmin)
	if err != nil {
		t.Fatalf("admin request failed: %v", err)
	}
	defer respAdmin.Body.Close()
	if respAdmin.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK for admin credentials on /nodes, got %d", respAdmin.StatusCode)
	}
}

// 16. Invalid Admin Bearer token returns 401 Unauthorized
func TestAuth_AdminBearer_InvalidToken(t *testing.T) {
	_, ts := setupAuthServer()
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/api/v1/nodes", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized for invalid Bearer token, got %d", resp.StatusCode)
	}
}

// 17. Listing telemetry batches requires Admin authorization
func TestAuth_ListTelemetryBatches_RequiresAdmin(t *testing.T) {
	_, ts := setupAuthServer()
	defer ts.Close()

	// 1. Node credentials: 403 Forbidden
	reqNode := signedRequest(t, http.MethodGet, ts.URL+"/api/v1/telemetry/batches", "node-a", nil)
	respNode, err := http.DefaultClient.Do(reqNode)
	if err != nil {
		t.Fatalf("node request failed: %v", err)
	}
	defer respNode.Body.Close()
	if respNode.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden for node credentials on /telemetry/batches, got %d", respNode.StatusCode)
	}

	// 2. Admin Bearer token: 200 OK
	reqAdmin, _ := http.NewRequest(http.MethodGet, ts.URL+"/api/v1/telemetry/batches", nil)
	reqAdmin.Header.Set("Authorization", "Bearer "+testAdminToken)
	respAdmin, err := http.DefaultClient.Do(reqAdmin)
	if err != nil {
		t.Fatalf("admin request failed: %v", err)
	}
	defer respAdmin.Body.Close()
	if respAdmin.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK for admin credentials on /telemetry/batches, got %d", respAdmin.StatusCode)
	}
}

// 18. Incomplete TLS configuration validation
func TestAuth_ValidateTLSConfig(t *testing.T) {
	// Both empty: ok (dev mode)
	if err := ValidateTLSConfig("", ""); err != nil {
		t.Errorf("expected nil error for empty TLS config, got %v", err)
	}

	// Incomplete TLS config: cert provided, key missing
	if err := ValidateTLSConfig("cert.pem", ""); err != types.ErrIncompleteTLSConfig {
		t.Errorf("expected ErrIncompleteTLSConfig when key is missing, got %v", err)
	}

	// Incomplete TLS config: key provided, cert missing
	if err := ValidateTLSConfig("", "key.pem"); err != types.ErrIncompleteTLSConfig {
		t.Errorf("expected ErrIncompleteTLSConfig when cert is missing, got %v", err)
	}

	// Non-existent files
	if err := ValidateTLSConfig("nonexistent-cert.pem", "nonexistent-key.pem"); err == nil {
		t.Errorf("expected error for non-existent TLS files")
	}

	// Valid accessible TLS files
	dir := t.TempDir()
	certFile, keyFile := generateTestCertificate(t, dir)

	if err := ValidateTLSConfig(certFile, keyFile); err != nil {
		t.Errorf("expected nil error for valid accessible TLS files, got %v", err)
	}

	// Corrupt / invalid certificate content
	corruptCertFile := filepath.Join(dir, "corrupt-cert.pem")
	_ = os.WriteFile(corruptCertFile, []byte("NOT-A-VALID-PEM-CERTIFICATE"), 0600)
	if err := ValidateTLSConfig(corruptCertFile, keyFile); err == nil {
		t.Errorf("expected error for corrupt TLS certificate file, got nil")
	}
}

// 19. Oversized payload returns 413 Payload Too Large
func TestAuth_OversizedPayload(t *testing.T) {
	_, ts := setupAuthServer()
	defer ts.Close()

	// 2MB oversized payload
	largeBytes := make([]byte, 2*1024*1024)
	req := signedRequest(t, http.MethodPost, ts.URL+"/api/v1/nodes/register", "node-large", largeBytes)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("expected 413 Payload Too Large, got %d", resp.StatusCode)
	}
}

// 20. Finding 1: Tampered Node ID header breaks cryptographic signature
func TestAuth_Finding1_NodeIdentityHeaderTampered(t *testing.T) {
	_, ts := setupAuthServer()
	defer ts.Close()

	reg := types.NodeRegistration{
		NodeID:       "node-alpha",
		Hostname:     "alpha-host",
		RegisteredAt: time.Now().UTC(),
	}
	body, _ := json.Marshal(reg)

	// Legitimate client signs for node-alpha
	req := signedRequest(t, http.MethodPost, ts.URL+"/api/v1/nodes/register", "node-alpha", body)

	// In-flight tampering: attacker alters node ID header to node-victim
	req.Header.Set(types.HeaderNodeID, "node-victim")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized for tampered node ID header, got %d", resp.StatusCode)
	}
}

// 21. Finding 1: Per-node credential isolation
func TestAuth_Finding1_PerNodeCredentials_Isolation(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	srv.SetAuthConfig(AuthConfig{
		Enabled: true,
		NodeSecrets: map[string]string{
			"node-alpha": "alpha-secret-key-at-least-16chars",
			"node-beta":  "beta-secret-key-at-least-16chars",
		},
		AdminToken:       testAdminToken,
		MaxClockSkew:     5 * time.Minute,
		ReplayProtection: true,
	})
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	regAlpha := types.NodeRegistration{
		NodeID:       "node-alpha",
		Hostname:     "alpha-host",
		RegisteredAt: time.Now().UTC(),
	}
	bodyAlpha, _ := json.Marshal(regAlpha)

	// 1. Node Alpha signs with Alpha's secret -> Success
	headersAlpha, _ := types.CreateAuthHeaders([]byte("alpha-secret-key-at-least-16chars"), http.MethodPost, "/api/v1/nodes/register", "node-alpha", bodyAlpha)
	reqAlpha, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/nodes/register", bytes.NewReader(bodyAlpha))
	reqAlpha.Header.Set("Content-Type", "application/json")
	for k, v := range headersAlpha {
		reqAlpha.Header.Set(k, v)
	}
	respAlpha, err := http.DefaultClient.Do(reqAlpha)
	if err != nil || respAlpha.StatusCode != http.StatusCreated {
		t.Fatalf("node-alpha registration with alpha secret expected 201, got %v (err: %v)", respAlpha.StatusCode, err)
	}
	respAlpha.Body.Close()

	// 2. Node Alpha tries to claim identity "node-beta" using Alpha's secret -> 401 Unauthorized
	regBeta := types.NodeRegistration{
		NodeID:       "node-beta",
		Hostname:     "beta-host",
		RegisteredAt: time.Now().UTC(),
	}
	bodyBeta, _ := json.Marshal(regBeta)
	headersForged, _ := types.CreateAuthHeaders([]byte("alpha-secret-key-at-least-16chars"), http.MethodPost, "/api/v1/nodes/register", "node-beta", bodyBeta)
	reqForged, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/nodes/register", bytes.NewReader(bodyBeta))
	reqForged.Header.Set("Content-Type", "application/json")
	for k, v := range headersForged {
		reqForged.Header.Set(k, v)
	}
	respForged, err := http.DefaultClient.Do(reqForged)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer respForged.Body.Close()
	if respForged.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized when signing for node-beta with alpha secret, got %d", respForged.StatusCode)
	}

	// 3. Node Alpha signs as node-alpha with Alpha secret, but tries to register body for node-beta -> 403 Forbidden
	headersCross, _ := types.CreateAuthHeaders([]byte("alpha-secret-key-at-least-16chars"), http.MethodPost, "/api/v1/nodes/register", "node-alpha", bodyBeta)
	reqCross, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/nodes/register", bytes.NewReader(bodyBeta))
	reqCross.Header.Set("Content-Type", "application/json")
	for k, v := range headersCross {
		reqCross.Header.Set(k, v)
	}
	respCross, err := http.DefaultClient.Do(reqCross)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer respCross.Body.Close()
	if respCross.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden for node ID mismatch between header and payload, got %d", respCross.StatusCode)
	}
}

// 22. Finding 1: SecretResolver dynamic credential lookup
func TestAuth_Finding1_SecretResolver(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	srv.SetAuthConfig(AuthConfig{
		Enabled: true,
		SecretResolver: func(nodeID string) ([]byte, error) {
			if nodeID == "node-dynamic" {
				return []byte("dynamic-secret-key-at-least-16"), nil
			}
			return nil, errors.New("node not recognized")
		},
		AdminToken:       testAdminToken,
		MaxClockSkew:     5 * time.Minute,
		ReplayProtection: true,
	})
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	reg := types.NodeRegistration{
		NodeID:       "node-dynamic",
		Hostname:     "dynamic-host",
		RegisteredAt: time.Now().UTC(),
	}
	body, _ := json.Marshal(reg)

	// Valid dynamic node
	headers, _ := types.CreateAuthHeaders([]byte("dynamic-secret-key-at-least-16"), http.MethodPost, "/api/v1/nodes/register", "node-dynamic", body)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/nodes/register", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created for dynamically resolved secret, got %v (err: %v)", resp.StatusCode, err)
	}
	resp.Body.Close()

	// Unknown node
	regUnknown := types.NodeRegistration{
		NodeID:       "node-unknown",
		Hostname:     "unknown-host",
		RegisteredAt: time.Now().UTC(),
	}
	bodyUnknown, _ := json.Marshal(regUnknown)
	headersUnk, _ := types.CreateAuthHeaders([]byte("some-arbitrary-key-16chars"), http.MethodPost, "/api/v1/nodes/register", "node-unknown", bodyUnknown)
	reqUnk, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/nodes/register", bytes.NewReader(bodyUnknown))
	reqUnk.Header.Set("Content-Type", "application/json")
	for k, v := range headersUnk {
		reqUnk.Header.Set(k, v)
	}
	respUnk, err := http.DefaultClient.Do(reqUnk)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer respUnk.Body.Close()
	if respUnk.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized for unknown node ID with resolver, got %d", respUnk.StatusCode)
	}
}

// 23. Finding 1: Query parameters bound to cryptographic signature
func TestAuth_Finding1_QueryParametersBound(t *testing.T) {
	_, ts := setupAuthServer()
	defer ts.Close()

	// Register node first
	reg := types.NodeRegistration{
		NodeID:       "node-query",
		Hostname:     "query-host",
		RegisteredAt: time.Now().UTC(),
	}
	body, _ := json.Marshal(reg)
	reqReg := signedRequest(t, http.MethodPost, ts.URL+"/api/v1/nodes/register", "node-query", body)
	r1, err := http.DefaultClient.Do(reqReg)
	if err != nil || r1.StatusCode != http.StatusCreated {
		t.Fatalf("failed to register node-query: %v", err)
	}
	r1.Body.Close()

	// Sign for /api/v1/nodes/node-query?status=active
	signedTarget := "/api/v1/nodes/node-query?status=active"
	headers, _ := types.CreateAuthHeaders([]byte(testSharedSecret), http.MethodGet, signedTarget, "node-query", nil)

	// Send to /api/v1/nodes/node-query?status=degraded (tampered query)
	reqTampered, _ := http.NewRequest(http.MethodGet, ts.URL+"/api/v1/nodes/node-query?status=degraded", nil)
	for k, v := range headers {
		reqTampered.Header.Set(k, v)
	}
	respTampered, err := http.DefaultClient.Do(reqTampered)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer respTampered.Body.Close()
	if respTampered.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized for tampered query parameter, got %d", respTampered.StatusCode)
	}
}

// 24. Finding 2: Invalid signature does NOT mutate replay cache
func TestAuth_Finding2_InvalidSignatureDoesNotMutateReplayCache(t *testing.T) {
	srv, ts := setupAuthServer()
	defer ts.Close()

	reg := types.NodeRegistration{
		NodeID:       "node-attacker",
		Hostname:     "attacker-host",
		RegisteredAt: time.Now().UTC(),
	}
	body, _ := json.Marshal(reg)

	initialCacheLen := srv.replayCache.Len()

	// Attacker sends request with invalid signature and arbitrary nonce
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/nodes/register", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(types.HeaderNodeID, reg.NodeID)
	req.Header.Set(types.HeaderTimestamp, time.Now().UTC().Format(time.RFC3339))
	req.Header.Set(types.HeaderNonce, "attacker-crafted-nonce-001")
	req.Header.Set(types.HeaderSignature, "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized, got %d", resp.StatusCode)
	}

	afterCacheLen := srv.replayCache.Len()
	if afterCacheLen != initialCacheLen {
		t.Errorf("CRITICAL SECURITY DEFECT: invalid signature mutated replay cache (before: %d, after: %d)", initialCacheLen, afterCacheLen)
	}
}

// 25. Finding 2: Attacker cannot pre-register nonce to deny legitimate node request
func TestAuth_Finding2_CannotPreRegisterNonceToDenyValidRequest(t *testing.T) {
	srv, ts := setupAuthServer()
	defer ts.Close()

	reg := types.NodeRegistration{
		NodeID:       "node-legit",
		Hostname:     "legit-host",
		RegisteredAt: time.Now().UTC(),
	}
	body, _ := json.Marshal(reg)

	targetNonce := "shared-target-nonce-12345"
	tsStr := time.Now().UTC().Format(time.RFC3339)

	// Step 1: Attacker attempts to front-run the nonce with invalid signature
	attackerReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/nodes/register", bytes.NewReader(body))
	attackerReq.Header.Set("Content-Type", "application/json")
	attackerReq.Header.Set(types.HeaderNodeID, "node-attacker")
	attackerReq.Header.Set(types.HeaderTimestamp, tsStr)
	attackerReq.Header.Set(types.HeaderNonce, targetNonce)
	attackerReq.Header.Set(types.HeaderSignature, "baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaad")

	respAttacker, err := http.DefaultClient.Do(attackerReq)
	if err != nil {
		t.Fatalf("attacker request failed: %v", err)
	}
	respAttacker.Body.Close()
	if respAttacker.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for attacker, got %d", respAttacker.StatusCode)
	}

	// Cache must remain untouched
	if srv.replayCache.Len() != 0 {
		t.Fatalf("cache should have 0 entries after failed attacker attempt, got %d", srv.replayCache.Len())
	}

	// Step 2: Legitimate node sends genuine valid request using targetNonce
	sig := types.SignRequest([]byte(testSharedSecret), http.MethodPost, "/api/v1/nodes/register", reg.NodeID, tsStr, targetNonce, body)
	legitReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/nodes/register", bytes.NewReader(body))
	legitReq.Header.Set("Content-Type", "application/json")
	legitReq.Header.Set(types.HeaderNodeID, reg.NodeID)
	legitReq.Header.Set(types.HeaderTimestamp, tsStr)
	legitReq.Header.Set(types.HeaderNonce, targetNonce)
	legitReq.Header.Set(types.HeaderSignature, sig)

	respLegit, err := http.DefaultClient.Do(legitReq)
	if err != nil {
		t.Fatalf("legit request failed: %v", err)
	}
	defer respLegit.Body.Close()

	if respLegit.StatusCode != http.StatusCreated {
		t.Errorf("CRITICAL: legitimate request was denied after attacker nonce front-run! Status: %d", respLegit.StatusCode)
	}
}

// 26. Finding 2: Replay cache capacity exhaustion fails closed
func TestAuth_Finding2_ReplayCacheCapacityExhaustionFailsClosed(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	srv.SetAuthConfig(AuthConfig{
		Enabled:          true,
		SharedSecret:     testSharedSecret,
		AdminToken:       testAdminToken,
		MaxClockSkew:     5 * time.Minute,
		ReplayProtection: true,
	})
	// Force tiny replay cache capacity limit of 2 entries
	srv.replayCache = NewReplayCache(2)

	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	reg1 := types.NodeRegistration{NodeID: "node-cap-1", Hostname: "host-1", RegisteredAt: time.Now().UTC()}
	b1, _ := json.Marshal(reg1)
	req1 := signedRequest(t, http.MethodPost, ts.URL+"/api/v1/nodes/register", reg1.NodeID, b1)
	r1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatalf("request 1 failed: %v", err)
	}
	if r1.StatusCode != http.StatusCreated {
		t.Fatalf("request 1 expected 201, got %d", r1.StatusCode)
	}
	r1.Body.Close()

	reg2 := types.NodeRegistration{NodeID: "node-cap-2", Hostname: "host-2", RegisteredAt: time.Now().UTC()}
	b2, _ := json.Marshal(reg2)
	req2 := signedRequest(t, http.MethodPost, ts.URL+"/api/v1/nodes/register", reg2.NodeID, b2)
	r2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("request 2 failed: %v", err)
	}
	if r2.StatusCode != http.StatusCreated {
		t.Fatalf("request 2 expected 201, got %d", r2.StatusCode)
	}
	r2.Body.Close()

	// Third request exceeds maximum capacity (2) and fails closed with 401
	reg3 := types.NodeRegistration{NodeID: "node-cap-3", Hostname: "host-3", RegisteredAt: time.Now().UTC()}
	b3, _ := json.Marshal(reg3)
	req3 := signedRequest(t, http.MethodPost, ts.URL+"/api/v1/nodes/register", reg3.NodeID, b3)
	r3, err := http.DefaultClient.Do(req3)
	if err != nil {
		t.Fatalf("request 3 failed: %v", err)
	}
	defer r3.Body.Close()

	if r3.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized when replay cache capacity is exhausted (fail closed), got %d", r3.StatusCode)
	}
}

// 27. Finding 3: Configuration validation rules
func TestAuth_Finding3_ConfigurationValidationRules(t *testing.T) {
	// 1. Production with Auth disabled -> error
	cfgProdNoAuth := AuthConfig{Enabled: false}
	if err := cfgProdNoAuth.Validate("127.0.0.1:8080", true); err == nil {
		t.Errorf("expected error for unauthenticated config in production")
	}

	// 2. Production without AdminToken -> error
	cfgProdNoAdmin := AuthConfig{
		Enabled:      true,
		SharedSecret: "strong-secret-key-32characters-long",
	}
	if err := cfgProdNoAdmin.Validate("127.0.0.1:8080", true); err == nil {
		t.Errorf("expected error for production config missing admin token")
	}

	// 3. Non-loopback with Auth disabled -> error
	nonLoopbackAddrs := []string{":8080", "0.0.0.0:8080", "192.168.1.100:8080"}
	for _, addr := range nonLoopbackAddrs {
		if err := cfgProdNoAuth.Validate(addr, false); err == nil {
			t.Errorf("expected error for unauthenticated config on non-loopback %s", addr)
		}
	}

	// 4. Loopback in dev with Auth disabled -> allowed
	loopbackAddrs := []string{"127.0.0.1:8080", "localhost:8080", "[::1]:8080"}
	for _, addr := range loopbackAddrs {
		if err := cfgProdNoAuth.Validate(addr, false); err != nil {
			t.Errorf("expected nil error for dev loopback %s, got %v", addr, err)
		}
	}

	// 5. Short shared secret (<16 chars) -> error
	cfgShortSecret := AuthConfig{
		Enabled:      true,
		SharedSecret: "short-key",
		AdminToken:   "valid-admin-token-16chars",
	}
	if err := cfgShortSecret.Validate("127.0.0.1:8080", false); err == nil {
		t.Errorf("expected error for short shared secret (<16 chars)")
	}

	// 6. Short admin token (<16 chars) -> error
	cfgShortAdmin := AuthConfig{
		Enabled:      true,
		SharedSecret: "strong-secret-key-32characters-long",
		AdminToken:   "short-token",
	}
	if err := cfgShortAdmin.Validate("127.0.0.1:8080", false); err == nil {
		t.Errorf("expected error for short admin token (<16 chars)")
	}

	// 7. Production with fleet shared secret but no per-node secrets -> error
	cfgProdFleetOnly := AuthConfig{
		Enabled:      true,
		SharedSecret: "strong-secret-key-32characters-long",
		AdminToken:   "strong-admin-token-32characters-long",
	}
	if err := cfgProdFleetOnly.Validate("127.0.0.1:8080", true); err == nil {
		t.Errorf("expected error for production config without per-node credentials")
	}

	// 8. Production without TLS cert/key -> error
	cfgProdNoTLS := AuthConfig{
		Enabled: true,
		NodeSecrets: map[string]string{
			"node-1": "strong-secret-key-32characters-long",
		},
		AdminToken: "strong-admin-token-32characters-long",
	}
	if err := cfgProdNoTLS.Validate("127.0.0.1:8080", true); err == nil {
		t.Errorf("expected error for production config without TLS")
	}

	// 9. Strong non-production non-loopback config with shared secret -> allowed
	cfgNonProdNonLoopback := AuthConfig{
		Enabled:      true,
		SharedSecret: "strong-secret-key-32characters-long",
		AdminToken:   "strong-admin-token-32characters-long",
	}
	if err := cfgNonProdNonLoopback.Validate("0.0.0.0:8080", false); err != nil {
		t.Errorf("expected nil error for valid strong non-prod non-loopback config, got %v", err)
	}

	// 10. Strong production config with per-node credentials and valid TLS -> allowed
	dir := t.TempDir()
	certFile, keyFile := generateTestCertificate(t, dir)
	cfgStrongProd := AuthConfig{
		Enabled: true,
		NodeSecrets: map[string]string{
			"node-1": "strong-secret-key-32characters-long",
		},
		AdminToken:  "strong-admin-token-32characters-long",
		TLSCertFile: certFile,
		TLSKeyFile:  keyFile,
	}
	if err := cfgStrongProd.Validate("0.0.0.0:8080", true); err != nil {
		t.Errorf("expected nil error for valid strong production config, got %v", err)
	}
}

// Helper to generate self-signed ECDSA certificate for TLS tests
func generateTestCertificate(t *testing.T, dir string) (string, string) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate ecdsa key: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"AegisEdge Test"},
		},
		NotBefore: time.Now().Add(-1 * time.Hour),
		NotAfter:  time.Now().Add(24 * time.Hour),

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	certPath := filepath.Join(dir, "server.crt")
	certOut, err := os.Create(certPath)
	if err != nil {
		t.Fatalf("failed to create cert file: %v", err)
	}
	defer certOut.Close()
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
		t.Fatalf("failed to write cert pem: %v", err)
	}

	keyPath := filepath.Join(dir, "server.key")
	keyOut, err := os.Create(keyPath)
	if err != nil {
		t.Fatalf("failed to create key file: %v", err)
	}
	defer keyOut.Close()
	b, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("failed to marshal private key: %v", err)
	}
	if err := pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: b}); err != nil {
		t.Fatalf("failed to write key pem: %v", err)
	}

	return certPath, keyPath
}

// 28. Finding 4: Actual TLS transport verification
func TestAuth_Finding4_ActualTLSTransport(t *testing.T) {
	tempDir := t.TempDir()
	certFile, keyFile := generateTestCertificate(t, tempDir)

	// Validate TLS configuration files
	if err := ValidateTLSConfig(certFile, keyFile); err != nil {
		t.Fatalf("ValidateTLSConfig failed: %v", err)
	}

	srv := NewServer(newDiscardLogger())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to bind listener: %v", err)
	}
	defer listener.Close()

	httpServer := &http.Server{
		Handler: srv.Routes(),
	}

	go func() {
		_ = httpServer.ServeTLS(listener, certFile, keyFile)
	}()
	defer httpServer.Close()

	serverURL := fmt.Sprintf("127.0.0.1:%d", listener.Addr().(*net.TCPAddr).Port)

	// 1. Plain HTTP request to TLS server MUST fail to negotiate application traffic (rejected by TLS listener)
	plainClient := &http.Client{Timeout: 2 * time.Second}
	plainResp, plainErr := plainClient.Get(fmt.Sprintf("http://%s/healthz", serverURL))
	if plainErr == nil {
		defer plainResp.Body.Close()
		body, _ := io.ReadAll(plainResp.Body)
		if plainResp.StatusCode == http.StatusOK {
			t.Errorf("plain HTTP request must NOT be served 200 OK by TLS server, got: %s", string(body))
		}
		if plainResp.StatusCode != http.StatusBadRequest {
			t.Errorf("expected 400 Bad Request from TLS listener for plain HTTP, got %d", plainResp.StatusCode)
		}
	}

	// 2. HTTPS request with TLS client negotiates genuine TLS connection and succeeds
	caCertBytes, _ := os.ReadFile(certFile)
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCertBytes)

	tlsClient := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs: caCertPool,
			},
		},
	}

	resp, err := tlsClient.Get(fmt.Sprintf("https://%s/healthz", serverURL))
	if err != nil {
		t.Fatalf("HTTPS request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK over TLS, got %d", resp.StatusCode)
	}
	if resp.TLS == nil || !resp.TLS.HandshakeComplete {
		t.Errorf("expected TLS handshake to be complete")
	}
}

// 29. Admin Bearer cannot fall through to node authentication
func TestAuth_AdminBearer_CannotFallThroughToNodeAuth(t *testing.T) {
	_, ts := setupAuthServer()
	defer ts.Close()

	reg := types.NodeRegistration{
		NodeID:       "node-alpha",
		Hostname:     "alpha-host",
		RegisteredAt: time.Now().UTC(),
	}
	body, _ := json.Marshal(reg)

	req := signedRequest(t, http.MethodPost, ts.URL+"/api/v1/nodes/register", reg.NodeID, body)
	// Inject invalid Bearer token
	req.Header.Set("Authorization", "Bearer invalid-admin-token-12345")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized for invalid Bearer token, got %d", resp.StatusCode)
	}
}

// 30. Real production server TLS lifecycle: startup, HTTPS handshake verification, and graceful shutdown
func TestAuth_Finding4_RealServerTLSStartup_AndGracefulShutdown(t *testing.T) {
	tempDir := t.TempDir()
	certFile, keyFile := generateTestCertificate(t, tempDir)

	srv := NewServer(newDiscardLogger())
	authCfg := AuthConfig{
		Enabled: true,
		NodeSecrets: map[string]string{
			"prod-node-01": "prod-secret-key-at-least-16chars",
		},
		AdminToken:       testAdminToken,
		MaxClockSkew:     5 * time.Minute,
		ReplayProtection: true,
		TLSCertFile:      certFile,
		TLSKeyFile:       keyFile,
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to bind listener: %v", err)
	}
	defer listener.Close()

	listenAddr := listener.Addr().String()

	// 1. Validate full production security posture
	if err := authCfg.Validate(listenAddr, true); err != nil {
		t.Fatalf("authConfig.Validate failed for valid production config: %v", err)
	}
	srv.SetAuthConfig(authCfg)

	httpServer := &http.Server{
		Addr:         listenAddr,
		Handler:      srv.Routes(),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	serverErrChan := make(chan error, 1)
	go func() {
		err := httpServer.ServeTLS(listener, certFile, keyFile)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrChan <- err
		}
	}()

	// 2. Client verification with trusted root CA pool
	caCertBytes, _ := os.ReadFile(certFile)
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCertBytes)

	tlsClient := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:            caCertPool,
				InsecureSkipVerify: false, // Explicitly enforce strict TLS certificate validation
			},
		},
	}

	// Sign a node registration request for prod-node-01
	reg := types.NodeRegistration{
		NodeID:       "prod-node-01",
		Hostname:     "prod-host",
		RegisteredAt: time.Now().UTC(),
	}
	body, _ := json.Marshal(reg)

	reqTarget := "/api/v1/nodes/register"
	headers, err := types.CreateAuthHeaders([]byte("prod-secret-key-at-least-16chars"), http.MethodPost, reqTarget, reg.NodeID, body)
	if err != nil {
		t.Fatalf("CreateAuthHeaders failed: %v", err)
	}

	httpsURL := fmt.Sprintf("https://%s%s", listenAddr, reqTarget)
	req, err := http.NewRequest(http.MethodPost, httpsURL, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("failed to create HTTPS request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := tlsClient.Do(req)
	if err != nil {
		t.Fatalf("HTTPS request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Errorf("expected 201 Created over TLS, got %d", resp.StatusCode)
	}
	if resp.TLS == nil || !resp.TLS.HandshakeComplete {
		t.Errorf("expected TLS handshake to be complete")
	}

	// 3. Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("graceful shutdown failed: %v", err)
	}
}

// 31. Unknown node IDs cannot register themselves in per-node provisioning mode
func TestAuth_Finding2_UnknownNodeCannotRegisterSelfWithArbitraryID_InPerNodeMode(t *testing.T) {
	srv := NewServer(newDiscardLogger())
	srv.SetAuthConfig(AuthConfig{
		Enabled: true,
		NodeSecrets: map[string]string{
			"authorized-node-01": "authorized-secret-key-16chars",
		},
		AdminToken:       testAdminToken,
		MaxClockSkew:     5 * time.Minute,
		ReplayProtection: true,
	})
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	// Rogue node generates its own arbitrary node ID and self-signed request with random key
	rogueID := "rogue-unknown-node-99"
	reg := types.NodeRegistration{
		NodeID:       rogueID,
		Hostname:     "rogue-host",
		RegisteredAt: time.Now().UTC(),
	}
	body, _ := json.Marshal(reg)

	headers, _ := types.CreateAuthHeaders([]byte("rogue-fabricated-key-16chars"), http.MethodPost, "/api/v1/nodes/register", rogueID, body)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/nodes/register", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized for unknown node self-registration attempt, got %d", resp.StatusCode)
	}
}

package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/services/control-plane/server"
	"github.com/Chamalka-heshi/AegisEdge/services/control-plane/testutil"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

const (
	regTestSecretAlpha = "secret-alpha-key-at-least-16chars"
	regTestSecretBeta  = "secret-beta-key-at-least-16chars"
	regTestAdminToken  = "admin-token-super-secure-32chars"
)

func setupAdversarialServer(t *testing.T) (*server.Server, *httptest.Server) {
	t.Helper()
	srv := server.NewServer(nil)
	srv.SetAuthConfig(server.AuthConfig{
		Enabled: true,
		NodeSecrets: map[string]string{
			"node-alpha": regTestSecretAlpha,
			"node-beta":  regTestSecretBeta,
		},
		AdminToken:       regTestAdminToken,
		MaxClockSkew:     5 * time.Minute,
		ReplayProtection: true,
	})
	ts := httptest.NewServer(srv.Routes())
	t.Cleanup(ts.Close)
	return srv, ts
}

// ---------------------------------------------------------------------------
// 2. Authentication Regression Tests (Tampering, Skew, Resolver, Keys)
// ---------------------------------------------------------------------------

func TestAdversarial_Auth_TamperedMethod(t *testing.T) {
	_, ts := setupAdversarialServer(t)

	body := []byte(`{"node_id":"node-alpha","hostname":"alpha-host"}`)
	// Sign for POST
	req := testutil.BuildSignedRequest(t, http.MethodPost, ts.URL+"/api/v1/nodes/register", "node-alpha", regTestSecretAlpha, body)
	// Mutate verb to PUT in-flight
	req.Method = http.MethodPut

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	testutil.AssertNoSecretDisclosure(t, regTestSecretAlpha, string(respBody))

	// PUT is either 401 (signature check fails because canonical verb differs) or 405 (method not allowed)
	if resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 401 or 405 for tampered HTTP method, got %d", resp.StatusCode)
	}
}

func TestAdversarial_Auth_TamperedQueryParams(t *testing.T) {
	_, ts := setupAdversarialServer(t)

	// Legitimate node queries own node info at /api/v1/nodes/node-alpha
	req := testutil.BuildSignedRequest(t, http.MethodGet, ts.URL+"/api/v1/nodes/node-alpha", "node-alpha", regTestSecretAlpha, nil)

	// Attacker appends query param to URL without re-signing
	req.URL.RawQuery = "injected=malicious"

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	testutil.AssertNoSecretDisclosure(t, regTestSecretAlpha, string(respBody))

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized for tampered query parameters, got %d", resp.StatusCode)
	}
}

func TestAdversarial_Auth_TamperedNonceOrTimestamp(t *testing.T) {
	_, ts := setupAdversarialServer(t)
	body := []byte(`{"node_id":"node-alpha","hostname":"alpha-host"}`)

	t.Run("TamperedNonce", func(t *testing.T) {
		req := testutil.BuildSignedRequest(t, http.MethodPost, ts.URL+"/api/v1/nodes/register", "node-alpha", regTestSecretAlpha, body)
		req.Header.Set(types.HeaderNonce, "00000000000000000000000000000000") // Mutated nonce

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("expected 401 Unauthorized for tampered nonce, got %d", resp.StatusCode)
		}
	})

	t.Run("TamperedTimestamp", func(t *testing.T) {
		req := testutil.BuildSignedRequest(t, http.MethodPost, ts.URL+"/api/v1/nodes/register", "node-alpha", regTestSecretAlpha, body)
		req.Header.Set(types.HeaderTimestamp, time.Now().UTC().Add(-1*time.Minute).Format(time.RFC3339)) // Mutated timestamp

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("expected 401 Unauthorized for tampered timestamp, got %d", resp.StatusCode)
		}
	})
}

func TestAdversarial_Auth_SecretResolverEdgeCases(t *testing.T) {
	srv := server.NewServer(nil)
	ts := httptest.NewServer(srv.Routes())
	t.Cleanup(ts.Close)

	t.Run("ResolverReturnsEmptySecret", func(t *testing.T) {
		srv.SetAuthConfig(server.AuthConfig{
			Enabled: true,
			SecretResolver: func(nodeID string) ([]byte, error) {
				return []byte(""), nil // empty secret
			},
			AdminToken: regTestAdminToken,
		})

		body := []byte(`{"node_id":"node-alpha","hostname":"alpha-host"}`)
		req := testutil.BuildSignedRequest(t, http.MethodPost, ts.URL+"/api/v1/nodes/register", "node-alpha", "some-dummy-secret-16", body)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		respBody, _ := io.ReadAll(resp.Body)
		testutil.AssertNoSecretDisclosure(t, "some-dummy-secret-16", string(respBody))

		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("expected 401 Unauthorized when resolver returns empty secret, got %d", resp.StatusCode)
		}
	})

	t.Run("ResolverReturnsError", func(t *testing.T) {
		srv.SetAuthConfig(server.AuthConfig{
			Enabled: true,
			SecretResolver: func(nodeID string) ([]byte, error) {
				return nil, errors.New("database lookup error")
			},
			AdminToken: regTestAdminToken,
		})

		body := []byte(`{"node_id":"node-alpha","hostname":"alpha-host"}`)
		req := testutil.BuildSignedRequest(t, http.MethodPost, ts.URL+"/api/v1/nodes/register", "node-alpha", regTestSecretAlpha, body)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("expected 401 Unauthorized when resolver errors, got %d", resp.StatusCode)
		}
	})
}

func TestAdversarial_Auth_EmptyAndUndersizedSecretsValidation(t *testing.T) {
	// 1. Shared secret < 16 chars rejected
	cfgShortShared := server.AuthConfig{
		Enabled:      true,
		SharedSecret: "short-secret",
	}
	if err := cfgShortShared.Validate("127.0.0.1:8080", false); err == nil {
		t.Errorf("expected Validate to reject short SharedSecret (<16 chars)")
	}

	// 2. NodeSecrets with short secret rejected
	cfgShortNode := server.AuthConfig{
		Enabled: true,
		NodeSecrets: map[string]string{
			"node-01": "short",
		},
	}
	if err := cfgShortNode.Validate("127.0.0.1:8080", false); err == nil {
		t.Errorf("expected Validate to reject short NodeSecret (<16 chars)")
	}

	// 3. AdminToken < 16 chars rejected
	cfgShortAdmin := server.AuthConfig{
		Enabled:      true,
		SharedSecret: "valid-shared-secret-16chars",
		AdminToken:   "short-admin",
	}
	if err := cfgShortAdmin.Validate("127.0.0.1:8080", false); err == nil {
		t.Errorf("expected Validate to reject short AdminToken (<16 chars)")
	}
}

// ---------------------------------------------------------------------------
// 3. Replay-Protection Tests
// ---------------------------------------------------------------------------

func TestAdversarial_Replay_InvalidSignatureDoesNotConsumeNonce(t *testing.T) {
	_, ts := setupAdversarialServer(t)
	reg := types.NodeRegistration{
		NodeID:       "node-alpha",
		Hostname:     "alpha-host",
		RegisteredAt: time.Now().UTC(),
	}
	body, _ := json.Marshal(reg)

	// 1. Attacker generates valid nonce and timestamp, but invalid signature
	nonce, _ := types.GenerateNonce()
	tsStr := time.Now().UTC().Format(time.RFC3339)
	badSig := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	reqBad, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/nodes/register", bytes.NewReader(body))
	reqBad.Header.Set("Content-Type", "application/json")
	reqBad.Header.Set(types.HeaderNodeID, "node-alpha")
	reqBad.Header.Set(types.HeaderTimestamp, tsStr)
	reqBad.Header.Set(types.HeaderNonce, nonce)
	reqBad.Header.Set(types.HeaderSignature, badSig)

	respBad, err := http.DefaultClient.Do(reqBad)
	if err != nil {
		t.Fatalf("first request failed: %v", err)
	}
	respBad.Body.Close()
	if respBad.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for bad signature, got %d", respBad.StatusCode)
	}

	// 2. Legitimate node now issues request reusing that exact same nonce with valid signature
	validSig := types.SignRequest([]byte(regTestSecretAlpha), http.MethodPost, "/api/v1/nodes/register", "node-alpha", tsStr, nonce, body)
	reqGood, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/nodes/register", bytes.NewReader(body))
	reqGood.Header.Set("Content-Type", "application/json")
	reqGood.Header.Set(types.HeaderNodeID, "node-alpha")
	reqGood.Header.Set(types.HeaderTimestamp, tsStr)
	reqGood.Header.Set(types.HeaderNonce, nonce)
	reqGood.Header.Set(types.HeaderSignature, validSig)

	respGood, err := http.DefaultClient.Do(reqGood)
	if err != nil {
		t.Fatalf("good request failed: %v", err)
	}
	defer respGood.Body.Close()

	if respGood.StatusCode != http.StatusCreated {
		t.Errorf("CRITICAL: legitimate request was denied because invalid request consumed nonce! got status %d", respGood.StatusCode)
	}
}

func TestAdversarial_Replay_ExpiredEntriesPruned(t *testing.T) {
	cache := server.NewReplayCache(10)

	// 1. Add an expired nonce
	expiredNonce := "expired-nonce-001"
	pastTime := time.Now().UTC().Add(-10 * time.Minute)
	if !cache.CheckAndAdd(expiredNonce, pastTime) {
		t.Fatalf("expected adding expired nonce to succeed initially")
	}

	// 2. Adding same nonce with future time should succeed because prior entry was expired
	futureTime := time.Now().UTC().Add(5 * time.Minute)
	if !cache.CheckAndAdd(expiredNonce, futureTime) {
		t.Errorf("expected expired nonce to be refreshable")
	}

	// 3. Adding it again with future time should fail (replay detected)
	if cache.CheckAndAdd(expiredNonce, futureTime) {
		t.Errorf("expected duplicate valid nonce to be rejected")
	}

	// 4. Add another distinct nonce with an expired timestamp
	anotherExpiredNonce := "expired-nonce-002"
	if !cache.CheckAndAdd(anotherExpiredNonce, pastTime) {
		t.Fatalf("expected adding second expired nonce to succeed")
	}

	// Cache now contains exactly 2 entries:
	// - "expired-nonce-001" with futureTime (active)
	// - "expired-nonce-002" with pastTime (expired)
	if cache.Len() != 2 {
		t.Fatalf("expected cache len 2 before prune, got %d", cache.Len())
	}

	// Explicit prune must remove exactly the 1 expired entry ("expired-nonce-002")
	pruned := cache.PruneExpired()
	if pruned != 1 {
		t.Errorf("expected exactly 1 expired entry pruned, got %d", pruned)
	}

	// Active entry must remain
	if cache.Len() != 1 {
		t.Errorf("expected cache len 1 after prune, got %d", cache.Len())
	}

	// Active entry must still trigger replay rejection
	if cache.CheckAndAdd(expiredNonce, futureTime) {
		t.Errorf("expected active nonce to still be rejected after pruning expired entries")
	}
}

func TestAdversarial_Replay_DeterministicTimeAndPruning(t *testing.T) {
	cache := server.NewReplayCache(10)

	baseTime := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	currentTime := baseTime
	cache.SetNowFunc(func() time.Time {
		return currentTime
	})

	nonce := "det-nonce-001"
	expiry := baseTime.Add(5 * time.Minute) // expires at 12:05

	// 1. Initial registration at 12:00 succeeds
	if !cache.CheckAndAdd(nonce, expiry) {
		t.Fatalf("expected initial nonce check to succeed")
	}

	// 2. Replay at 12:04 fails (before expiry)
	currentTime = baseTime.Add(4 * time.Minute)
	if cache.CheckAndAdd(nonce, expiry) {
		t.Errorf("expected replay at 12:04 to be rejected")
	}

	// 3. At 12:04, Len should be 1
	if cache.Len() != 1 {
		t.Errorf("expected cache len 1 at 12:04, got %d", cache.Len())
	}

	// 4. Advance time past expiry to 12:06: explicit prune removes it
	currentTime = baseTime.Add(6 * time.Minute)
	pruned := cache.PruneExpired()
	if pruned != 1 {
		t.Errorf("expected exactly 1 entry pruned at 12:06, got %d", pruned)
	}
	if cache.Len() != 0 {
		t.Errorf("expected cache len 0 after pruning, got %d", cache.Len())
	}

	// 5. Subsequent submission with new expiry after prune succeeds
	newExpiry := currentTime.Add(5 * time.Minute)
	if !cache.CheckAndAdd(nonce, newExpiry) {
		t.Errorf("expected new submission after expiry and prune to succeed")
	}
}

func TestAdversarial_Replay_CapacityCeilingNeverWeakensReplay(t *testing.T) {
	// Cache with small capacity limit of 4 entries
	maxCap := 4
	cache := server.NewReplayCache(maxCap)

	baseTime := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	cache.SetNowFunc(func() time.Time { return baseTime })

	futureExpiry := baseTime.Add(10 * time.Minute)

	// Fill cache to capacity (4 entries) with active nonces
	activeNonces := []string{"active-1", "active-2", "active-3", "active-4"}
	for _, n := range activeNonces {
		if !cache.CheckAndAdd(n, futureExpiry) {
			t.Fatalf("failed to add active nonce %s to capacity", n)
		}
	}

	if cache.Len() != maxCap {
		t.Fatalf("expected cache len %d, got %d", maxCap, cache.Len())
	}

	// Attempt to add a 5th novel nonce: must fail closed (return false)
	novelNonce := "novel-overflow"
	if cache.CheckAndAdd(novelNonce, futureExpiry) {
		t.Fatalf("CRITICAL SECURITY DEFECT: cache exceeded capacity ceiling without failing closed!")
	}

	// Ensure NONE of the active nonces can be replayed despite capacity saturation
	for _, n := range activeNonces {
		if cache.CheckAndAdd(n, futureExpiry) {
			t.Fatalf("CRITICAL SECURITY DEFECT: replay of %s was permitted under capacity saturation!", n)
		}
	}
}

// ---------------------------------------------------------------------------
// 4. Authorization and Impersonation Tests
// ---------------------------------------------------------------------------

func TestAdversarial_AuthZ_AdminBearerInvalid(t *testing.T) {
	_, ts := setupAdversarialServer(t)

	// Attacker provides wrong bearer token to list nodes
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/api/v1/nodes", nil)
	req.Header.Set(types.HeaderAuthorization, "Bearer wrong-token-12345")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized for invalid admin token, got %d", resp.StatusCode)
	}
}

func TestAdversarial_AuthZ_AdminEndpointsRejectNodeSignatures(t *testing.T) {
	_, ts := setupAdversarialServer(t)

	// 1. GET /api/v1/nodes (list all) with node signature -> 403 Forbidden
	reqList, _ := http.NewRequest(http.MethodGet, ts.URL+"/api/v1/nodes", nil)
	headers, _ := types.CreateAuthHeaders([]byte(regTestSecretAlpha), http.MethodGet, "/api/v1/nodes", "node-alpha", nil)
	for k, v := range headers {
		reqList.Header.Set(k, v)
	}

	respList, err := http.DefaultClient.Do(reqList)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer respList.Body.Close()
	if respList.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden for node signature on admin endpoint /nodes, got %d", respList.StatusCode)
	}

	// 2. GET /api/v1/telemetry/batches (list all) with node signature -> 403 Forbidden
	reqBatches, _ := http.NewRequest(http.MethodGet, ts.URL+"/api/v1/telemetry/batches", nil)
	headersBatches, _ := types.CreateAuthHeaders([]byte(regTestSecretAlpha), http.MethodGet, "/api/v1/telemetry/batches", "node-alpha", nil)
	for k, v := range headersBatches {
		reqBatches.Header.Set(k, v)
	}

	respBatches, err := http.DefaultClient.Do(reqBatches)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer respBatches.Body.Close()
	if respBatches.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden for node signature on admin endpoint /telemetry/batches, got %d", respBatches.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// 5. Request Validation and Resource Limits
// ---------------------------------------------------------------------------

func TestAdversarial_Validation_TrailingJSONData(t *testing.T) {
	_, ts := setupAdversarialServer(t)

	// Valid JSON followed by trailing garbage bytes
	body := []byte(`{"node_id":"node-alpha","hostname":"alpha-host"}{"extra":"data"}`)
	req := testutil.BuildSignedRequest(t, http.MethodPost, ts.URL+"/api/v1/nodes/register", "node-alpha", regTestSecretAlpha, body)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	// Should reject as malformed_json or invalid_registration rather than crashing or accepting
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for trailing JSON payload, got %d", resp.StatusCode)
	}
}

func TestAdversarial_Validation_SecretFileEdgeCases(t *testing.T) {
	tempDir := t.TempDir()

	// 1. Secrets file with trailing data after root JSON object
	t.Run("TrailingDataInSecretsFile", func(t *testing.T) {
		path := filepath.Join(tempDir, "trailing.json")
		content := `{"node-1": "secret-key-alpha-16chars"} trailing-garbage`
		_ = os.WriteFile(path, []byte(content), 0600)

		_, err := server.LoadNodeSecretsFile(path)
		if err == nil {
			t.Errorf("expected LoadNodeSecretsFile to reject trailing data after closing brace")
		}
	})

	// 2. Secrets file with non-string values
	t.Run("NonStringValueInSecretsFile", func(t *testing.T) {
		path := filepath.Join(tempDir, "bool_val.json")
		content := `{"node-1": true}`
		_ = os.WriteFile(path, []byte(content), 0600)

		_, err := server.LoadNodeSecretsFile(path)
		if err == nil {
			t.Errorf("expected LoadNodeSecretsFile to reject boolean value")
		}
	})
}

// ---------------------------------------------------------------------------
// 8. Concurrency and Robustness Tests
// ---------------------------------------------------------------------------

func TestAdversarial_Concurrency_MultipleNodesConcurrentAuth(t *testing.T) {
	_, ts := setupAdversarialServer(t)

	nodes := []struct {
		id     string
		secret string
	}{
		{"node-alpha", regTestSecretAlpha},
		{"node-beta", regTestSecretBeta},
	}

	concurrency := 20
	var wg sync.WaitGroup
	errChan := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		node := nodes[i%2]
		go func(nID, nSec string, iter int) {
			defer wg.Done()
			reg := types.NodeRegistration{
				NodeID:       nID,
				Hostname:     fmt.Sprintf("%s-host-%d", nID, iter),
				RegisteredAt: time.Now().UTC(),
			}
			body, _ := json.Marshal(reg)
			req := testutil.BuildSignedRequest(t, http.MethodPost, ts.URL+"/api/v1/nodes/register", nID, nSec, body)

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				errChan <- fmt.Errorf("request failed: %w", err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
				errChan <- fmt.Errorf("node %s iter %d expected 201/200, got %d", nID, iter, resp.StatusCode)
			}
		}(node.id, node.secret, i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("concurrency error: %v", err)
	}
}

func TestAdversarial_Concurrency_ContextCancellation(t *testing.T) {
	_, ts := setupAdversarialServer(t)

	body := []byte(`{"node_id":"node-alpha","hostname":"alpha-host"}`)
	req := testutil.BuildSignedRequest(t, http.MethodPost, ts.URL+"/api/v1/nodes/register", "node-alpha", regTestSecretAlpha, body)

	// Cancel context immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req = req.WithContext(ctx)

	_, err := http.DefaultClient.Do(req)
	if err == nil {
		t.Errorf("expected client request to fail with cancelled context, got nil error")
	}
}

func TestAdversarial_Lifecycle_HTTPRequestContextCancellation(t *testing.T) {
	// Verifies standard-library HTTP request-context cancellation behavior using an inline test handler.
	// NOTE: This test asserts that standard net/http server and client plumbing properly propagates
	// context cancellation upon client disconnect. It does NOT exercise production handlers or prove
	// production goroutine-leak freedom.
	requestReceived := make(chan struct{})
	serverSawContextDone := make(chan struct{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Signal deterministically that the handler has received the request
		close(requestReceived)

		// Wait for request context cancellation
		<-r.Context().Done()
		close(serverSawContextDone)
	}))
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/healthz", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}

	clientErrChan := make(chan error, 1)
	go func() {
		_, clientErr := http.DefaultClient.Do(req)
		clientErrChan <- clientErr
	}()

	// Deterministically wait for request to arrive at the server handler before cancelling context
	select {
	case <-requestReceived:
		// Request has arrived at the server handler
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for request to reach server handler")
	}

	// Cancel context to simulate client disconnect
	cancel()

	// Assert client unblocks immediately
	select {
	case err := <-clientErrChan:
		if err == nil {
			t.Errorf("expected client to report error on context cancel")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("client hung after context cancellation")
	}

	// Assert server observes context cancellation deterministically
	select {
	case <-serverSawContextDone:
		// Succeeded: server context was cancelled and unblocked
	case <-time.After(1 * time.Second):
		t.Fatalf("server did not receive context cancellation within deadline")
	}
}

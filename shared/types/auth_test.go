package types

import (
	"testing"
	"time"
)

func TestAuth_SignAndVerify_Success(t *testing.T) {
	secret := []byte("test-fleet-shared-secret-1234")
	method := "POST"
	requestTarget := "/api/v1/nodes/heartbeat?verbose=true"
	nodeID := "edge-01"
	timestamp := time.Now().UTC().Format(time.RFC3339)
	nonce := "a1b2c3d4e5f60718293a4b5c6d7e8f90"
	body := []byte(`{"node_id":"edge-01","sequence_number":10}`)

	sig := SignRequest(secret, method, requestTarget, nodeID, timestamp, nonce, body)
	if len(sig) != 64 {
		t.Fatalf("expected 64-char hex signature, got len %d: %s", len(sig), sig)
	}

	if !VerifySignature(secret, method, requestTarget, nodeID, timestamp, nonce, sig, body) {
		t.Errorf("expected valid signature verification to succeed")
	}

	// Also test CreateAuthHeaders
	headers, err := CreateAuthHeaders(secret, method, requestTarget, nodeID, body)
	if err != nil {
		t.Fatalf("CreateAuthHeaders failed: %v", err)
	}
	if headers[HeaderNodeID] != nodeID {
		t.Errorf("expected header %s to be %s, got %s", HeaderNodeID, nodeID, headers[HeaderNodeID])
	}
	if !VerifySignature(secret, method, requestTarget, headers[HeaderNodeID], headers[HeaderTimestamp], headers[HeaderNonce], headers[HeaderSignature], body) {
		t.Errorf("verification of CreateAuthHeaders failed")
	}
}

func TestAuth_VerifySignature_TamperResistance(t *testing.T) {
	secret := []byte("correct-secret")
	wrongSecret := []byte("wrong-secret")
	method := "POST"
	requestTarget := "/api/v1/nodes/heartbeat?verbose=true"
	nodeID := "edge-01"
	timestamp := time.Now().UTC().Format(time.RFC3339)
	nonce := "nonce-1"
	body := []byte(`{"node_id":"edge-01"}`)

	sig := SignRequest(secret, method, requestTarget, nodeID, timestamp, nonce, body)

	// 1. Wrong secret
	if VerifySignature(wrongSecret, method, requestTarget, nodeID, timestamp, nonce, sig, body) {
		t.Errorf("signature should fail with wrong secret")
	}

	// 2. Tampered method
	if VerifySignature(secret, "GET", requestTarget, nodeID, timestamp, nonce, sig, body) {
		t.Errorf("signature should fail with modified HTTP method")
	}

	// 3. Tampered request target (path or query)
	if VerifySignature(secret, method, "/api/v1/nodes/register", nodeID, timestamp, nonce, sig, body) {
		t.Errorf("signature should fail with modified path")
	}
	if VerifySignature(secret, method, "/api/v1/nodes/heartbeat?verbose=false", nodeID, timestamp, nonce, sig, body) {
		t.Errorf("signature should fail with modified query parameter")
	}
	if VerifySignature(secret, method, "/api/v1/nodes/heartbeat", nodeID, timestamp, nonce, sig, body) {
		t.Errorf("signature should fail when query parameters are stripped")
	}

	// 4. Tampered node ID
	if VerifySignature(secret, method, requestTarget, "edge-victim-node", timestamp, nonce, sig, body) {
		t.Errorf("signature should fail with modified node ID")
	}

	// 5. Tampered timestamp
	if VerifySignature(secret, method, requestTarget, nodeID, "2020-01-01T00:00:00Z", nonce, sig, body) {
		t.Errorf("signature should fail with modified timestamp")
	}

	// 6. Tampered nonce
	if VerifySignature(secret, method, requestTarget, nodeID, timestamp, "different-nonce", sig, body) {
		t.Errorf("signature should fail with modified nonce")
	}

	// 7. Tampered body
	tamperedBody := []byte(`{"node_id":"edge-02"}`)
	if VerifySignature(secret, method, requestTarget, nodeID, timestamp, nonce, sig, tamperedBody) {
		t.Errorf("signature should fail with modified body")
	}

	// 8. Empty or invalid inputs
	if VerifySignature(nil, method, requestTarget, nodeID, timestamp, nonce, sig, body) {
		t.Errorf("signature should fail with nil secret")
	}
	if VerifySignature(secret, method, requestTarget, nodeID, timestamp, nonce, "", body) {
		t.Errorf("signature should fail with empty signature")
	}
	if VerifySignature(secret, method, requestTarget, nodeID, timestamp, nonce, "invalid-hex", body) {
		t.Errorf("signature should fail with invalid hex signature")
	}
}

func TestAuth_VerifyAdminToken(t *testing.T) {
	adminToken := "super-secure-admin-token-xyz"

	// Valid Bearer
	if !VerifyAdminToken(adminToken, "Bearer super-secure-admin-token-xyz") {
		t.Errorf("valid admin token should succeed")
	}

	// Case-insensitive scheme
	if !VerifyAdminToken(adminToken, "bearer super-secure-admin-token-xyz") {
		t.Errorf("case-insensitive bearer prefix should succeed")
	}

	// Invalid token
	if VerifyAdminToken(adminToken, "Bearer wrong-token") {
		t.Errorf("wrong token should fail")
	}

	// Missing scheme
	if VerifyAdminToken(adminToken, "super-secure-admin-token-xyz") {
		t.Errorf("token without Bearer prefix should fail")
	}

	// Empty header or unconfigured
	if VerifyAdminToken(adminToken, "") {
		t.Errorf("empty header should fail")
	}
	if VerifyAdminToken("", "Bearer token") {
		t.Errorf("unconfigured admin token should fail")
	}
}

func TestAuth_GenerateNonce(t *testing.T) {
	n1, err := GenerateNonce()
	if err != nil {
		t.Fatalf("GenerateNonce failed: %v", err)
	}
	if len(n1) != 32 {
		t.Errorf("expected 32-char hex nonce (16 bytes), got %d", len(n1))
	}

	n2, err := GenerateNonce()
	if err != nil {
		t.Fatalf("GenerateNonce failed: %v", err)
	}
	if n1 == n2 {
		t.Errorf("consecutive nonces must be unique")
	}
}

func TestAuth_ParseTimestamp(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	// RFC3339
	rfcStr := now.Format(time.RFC3339)
	parsed, err := ParseTimestamp(rfcStr)
	if err != nil {
		t.Fatalf("ParseTimestamp RFC3339 failed: %v", err)
	}
	if !parsed.Equal(now) {
		t.Errorf("expected %v, got %v", now, parsed)
	}

	// Unix seconds
	unixStr := now.Format("1500000000") // test string
	parsedUnix, err := ParseTimestamp("1728450000")
	if err != nil {
		t.Fatalf("ParseTimestamp Unix failed: %v", err)
	}
	if parsedUnix.Unix() != 1728450000 {
		t.Errorf("expected unix timestamp 1728450000, got %d", parsedUnix.Unix())
	}
	_ = unixStr

	// Invalid
	if _, err := ParseTimestamp("not-a-timestamp"); err == nil {
		t.Errorf("expected error for invalid timestamp format")
	}
	if _, err := ParseTimestamp(""); err == nil {
		t.Errorf("expected error for empty timestamp")
	}
}

package types

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Security header constants for Edge-to-Control-Plane authentication.
const (
	HeaderNodeID        = "X-AegisEdge-Node-ID"
	HeaderTimestamp     = "X-AegisEdge-Timestamp"
	HeaderNonce         = "X-AegisEdge-Nonce"
	HeaderSignature     = "X-AegisEdge-Signature"
	HeaderAuthorization = "Authorization"
)

// Common authentication and authorization errors.
var (
	ErrMissingAuthHeaders  = errors.New("missing required authentication headers")
	ErrInvalidSignature    = errors.New("invalid request signature")
	ErrExpiredTimestamp    = errors.New("request timestamp has expired")
	ErrFutureTimestamp     = errors.New("request timestamp is too far in the future")
	ErrReplayDetected      = errors.New("request replay detected")
	ErrUnauthorized        = errors.New("unauthorized")
	ErrForbidden           = errors.New("forbidden: insufficient privileges or identity mismatch")
	ErrIncompleteTLSConfig = errors.New("incomplete TLS configuration: both certificate and key files are required")
)

// BuildCanonicalString constructs a deterministic canonical representation
// binding the HTTP method, request target (path + query), node ID, timestamp, nonce, and payload digest.
func BuildCanonicalString(method, requestTarget, nodeID, timestamp, nonce string, body []byte) string {
	hasher := sha256.New()
	hasher.Write(body)
	bodyDigest := hex.EncodeToString(hasher.Sum(nil))

	cleanMethod := strings.ToUpper(strings.TrimSpace(method))
	cleanTarget := strings.TrimSpace(requestTarget)
	cleanNodeID := strings.TrimSpace(nodeID)
	cleanTimestamp := strings.TrimSpace(timestamp)
	cleanNonce := strings.TrimSpace(nonce)

	return fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s",
		cleanMethod,
		cleanTarget,
		cleanNodeID,
		cleanTimestamp,
		cleanNonce,
		bodyDigest,
	)
}

// SignRequest computes a hex-encoded HMAC-SHA256 signature for the given request parameters.
func SignRequest(secret []byte, method, requestTarget, nodeID, timestamp, nonce string, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	canonical := BuildCanonicalString(method, requestTarget, nodeID, timestamp, nonce, body)
	mac.Write([]byte(canonical))
	return hex.EncodeToString(mac.Sum(nil))
}

// CreateAuthHeaders generates a fresh nonce and timestamp, signs the request with bound node ID, and returns header key-values.
func CreateAuthHeaders(secret []byte, method, requestTarget, nodeID string, body []byte) (map[string]string, error) {
	nonce, err := GenerateNonce()
	if err != nil {
		return nil, err
	}
	ts := time.Now().UTC().Format(time.RFC3339)
	sig := SignRequest(secret, method, requestTarget, nodeID, ts, nonce, body)

	return map[string]string{
		HeaderNodeID:    nodeID,
		HeaderTimestamp: ts,
		HeaderNonce:     nonce,
		HeaderSignature: sig,
	}, nil
}

// VerifySignature validates a hex-encoded HMAC-SHA256 signature using constant-time comparison.
func VerifySignature(secret []byte, method, requestTarget, nodeID, timestamp, nonce, signature string, body []byte) bool {
	if len(secret) == 0 || len(signature) == 0 {
		return false
	}

	expectedSig := SignRequest(secret, method, requestTarget, nodeID, timestamp, nonce, body)
	expectedBytes := []byte(expectedSig)
	providedBytes := []byte(strings.ToLower(strings.TrimSpace(signature)))

	if len(expectedBytes) != len(providedBytes) {
		return false
	}

	return subtle.ConstantTimeCompare(expectedBytes, providedBytes) == 1
}

// VerifyAdminToken checks a Bearer token header against the configured admin token using constant-time comparison.
func VerifyAdminToken(configuredToken, authHeader string) bool {
	cleanConfigured := strings.TrimSpace(configuredToken)
	if cleanConfigured == "" {
		return false
	}

	trimmedHeader := strings.TrimSpace(authHeader)
	if !strings.HasPrefix(strings.ToLower(trimmedHeader), "bearer ") {
		return false
	}

	token := strings.TrimSpace(trimmedHeader[7:])
	tokenBytes := []byte(token)
	configBytes := []byte(cleanConfigured)

	if len(tokenBytes) != len(configBytes) {
		return false
	}

	return subtle.ConstantTimeCompare(tokenBytes, configBytes) == 1
}

// GenerateNonce creates a cryptographically secure random 16-byte hex-encoded nonce.
func GenerateNonce() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate random nonce: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// ParseTimestamp attempts to parse a timestamp string as RFC3339 or Unix seconds.
func ParseTimestamp(tsStr string) (time.Time, error) {
	clean := strings.TrimSpace(tsStr)
	if clean == "" {
		return time.Time{}, errors.New("empty timestamp string")
	}

	if t, err := time.Parse(time.RFC3339, clean); err == nil {
		return t.UTC(), nil
	}

	// Try unix seconds
	var sec int64
	if _, err := fmt.Sscanf(clean, "%d", &sec); err == nil && sec > 0 {
		return time.Unix(sec, 0).UTC(), nil
	}

	return time.Time{}, fmt.Errorf("unrecognized timestamp format %q", clean)
}

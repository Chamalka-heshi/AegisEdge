package server

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

const (
	DefaultMaxClockSkew  = 5 * time.Minute
	DefaultMaxReplayKeys = 100000
)

// SecretResolver dynamically resolves the symmetric key for a given node ID.
type SecretResolver func(nodeID string) ([]byte, error)

// AuthConfig controls control-plane communication security and endpoint authorization.
type AuthConfig struct {
	Enabled          bool              // Enforce HMAC-SHA256 and Bearer authentication
	SharedSecret     string            // Fallback fleet-wide symmetric secret for HMAC-SHA256
	NodeSecrets      map[string]string // Static per-node secrets mapping (nodeID -> secret)
	SecretResolver   SecretResolver    // Dynamic per-node secret resolver
	AdminToken       string            // Bearer token required for administrative endpoints
	MaxClockSkew     time.Duration     // Maximum permitted timestamp skew against server clock
	ReplayProtection bool              // Enable nonce-based replay attack mitigation
	TLSCertFile      string            // Path to TLS certificate file (optional for local dev)
	TLSKeyFile       string            // Path to TLS private key file (optional for local dev)
}

// DefaultAuthConfig returns a secure baseline configuration.
func DefaultAuthConfig() AuthConfig {
	return AuthConfig{
		Enabled:          false, // Opt-in or configured via environment / flags
		MaxClockSkew:     DefaultMaxClockSkew,
		ReplayProtection: true,
	}
}

// AuthContext captures the validated security identity for an incoming HTTP request.
type AuthContext struct {
	Authenticated bool
	NodeID        string
	IsAdmin       bool
}

// ReplayCache provides a thread-safe, memory-bounded sliding-window cache for nonces.
type ReplayCache struct {
	mu         sync.Mutex
	seen       map[string]time.Time
	maxEntries int
}

// NewReplayCache constructs a ReplayCache with the specified capacity limit.
func NewReplayCache(maxEntries int) *ReplayCache {
	if maxEntries <= 0 {
		maxEntries = DefaultMaxReplayKeys
	}
	return &ReplayCache{
		seen:       make(map[string]time.Time),
		maxEntries: maxEntries,
	}
}

// CheckAndAdd verifies whether a nonce is novel. If novel, it is recorded with an expiration
// and returns true. If duplicate or if the cache is full and unprunable, it returns false.
func (c *ReplayCache) CheckAndAdd(nonce string, expiry time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().UTC()

	// Check for duplicate nonce
	if exp, exists := c.seen[nonce]; exists {
		if now.Before(exp) {
			return false // Replay detected!
		}
		// Expired existing entry can be refreshed
	}

	// Capacity management: prune expired entries if map is large
	if len(c.seen) >= c.maxEntries/2 {
		for k, exp := range c.seen {
			if now.After(exp) {
				delete(c.seen, k)
			}
		}
	}

	// Fail closed if still exceeding maximum capacity ceiling
	if len(c.seen) >= c.maxEntries {
		return false
	}

	c.seen[nonce] = expiry
	return true
}

// Len returns the current count of stored nonces.
func (c *ReplayCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.seen)
}

// resolveNodeSecret retrieves the appropriate symmetric secret for the claimed node ID.
func (s *Server) resolveNodeSecret(nodeID string, cfg AuthConfig) ([]byte, error) {
	if cfg.SecretResolver != nil {
		secret, err := cfg.SecretResolver(nodeID)
		if err != nil {
			return nil, fmt.Errorf("secret resolver failed for node: %w", err)
		}
		if len(secret) == 0 {
			return nil, errors.New("secret resolver returned empty secret")
		}
		return secret, nil
	}

	if len(cfg.NodeSecrets) > 0 {
		secret, ok := cfg.NodeSecrets[nodeID]
		if !ok || len(secret) == 0 {
			return nil, errors.New("no secret configured for node ID")
		}
		return []byte(secret), nil
	}

	if cfg.SharedSecret != "" {
		return []byte(cfg.SharedSecret), nil
	}

	return nil, errors.New("no authentication secret configured")
}

// AuthenticateRequest verifies request credentials, freshness, and integrity.
func (s *Server) AuthenticateRequest(r *http.Request, body []byte) (*AuthContext, error) {
	s.mu.RLock()
	cfg := s.authConfig
	replayCache := s.replayCache
	s.mu.RUnlock()

	if !cfg.Enabled {
		// Dev mode: unauthenticated
		return &AuthContext{
			Authenticated: false,
			NodeID:        r.Header.Get(types.HeaderNodeID),
		}, nil
	}

	// 1. Check for Admin Authorization (Bearer token)
	authHeader := r.Header.Get(types.HeaderAuthorization)
	if authHeader != "" {
		if cfg.AdminToken != "" && types.VerifyAdminToken(cfg.AdminToken, authHeader) {
			return &AuthContext{
				Authenticated: true,
				IsAdmin:       true,
			}, nil
		}
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(authHeader)), "bearer ") {
			return nil, types.ErrUnauthorized
		}
	}

	// 2. Node-level HMAC-SHA256 Authentication
	nodeID := strings.TrimSpace(r.Header.Get(types.HeaderNodeID))
	tsStr := strings.TrimSpace(r.Header.Get(types.HeaderTimestamp))
	nonce := strings.TrimSpace(r.Header.Get(types.HeaderNonce))
	sig := strings.TrimSpace(r.Header.Get(types.HeaderSignature))

	if nodeID == "" || tsStr == "" || nonce == "" || sig == "" {
		return nil, types.ErrMissingAuthHeaders
	}

	if !isValidNodeID(nodeID) {
		return nil, errors.New("invalid node identifier in security header")
	}

	// 3. Timestamp parsing and freshness validation
	reqTime, err := types.ParseTimestamp(tsStr)
	if err != nil {
		return nil, fmt.Errorf("malformed timestamp in header: %w", err)
	}

	now := time.Now().UTC()
	clockSkew := cfg.MaxClockSkew
	if clockSkew <= 0 {
		clockSkew = DefaultMaxClockSkew
	}

	if now.Sub(reqTime) > clockSkew {
		return nil, types.ErrExpiredTimestamp
	}
	if reqTime.Sub(now) > clockSkew {
		return nil, types.ErrFutureTimestamp
	}

	// 4. Resolve node secret (per-node secret or fallback shared secret)
	secretBytes, err := s.resolveNodeSecret(nodeID, cfg)
	if err != nil {
		return nil, types.ErrUnauthorized
	}

	// 5. Cryptographic HMAC-SHA256 signature verification BEFORE touching replay cache
	reqTarget := r.URL.RequestURI()
	if reqTarget == "" {
		reqTarget = r.URL.Path
	}

	valid := types.VerifySignature(secretBytes, r.Method, reqTarget, nodeID, tsStr, nonce, sig, body)
	if !valid {
		return nil, types.ErrInvalidSignature
	}

	// 6. Nonce replay mitigation (ONLY performed after signature verification succeeds)
	if cfg.ReplayProtection && replayCache != nil {
		expiry := reqTime.Add(clockSkew)
		if !replayCache.CheckAndAdd(nonce, expiry) {
			return nil, types.ErrReplayDetected
		}
	}

	return &AuthContext{
		Authenticated: true,
		NodeID:        nodeID,
		IsAdmin:       false,
	}, nil
}

// isLoopbackAddress checks whether an address string resolves to a loopback interface.
func isLoopbackAddress(addr string) bool {
	clean := strings.TrimSpace(addr)
	if clean == "" {
		return false // empty address binds to INADDR_ANY (0.0.0.0), not loopback
	}

	host := clean
	if h, _, err := net.SplitHostPort(clean); err == nil {
		host = h
	}

	host = strings.TrimSpace(host)
	if host == "" || host == "0.0.0.0" || host == "::" {
		return false // wildcard bind to all interfaces
	}

	if strings.EqualFold(host, "localhost") {
		return true
	}

	ip := net.ParseIP(host)
	if ip != nil && ip.IsLoopback() {
		return true
	}

	return false
}

// Validate enforces fail-safe configuration constraints based on the listen address and environment.
func (c AuthConfig) Validate(listenAddr string, isProduction bool) error {
	isLoopback := isLoopbackAddress(listenAddr)

	if isProduction {
		if !c.Enabled {
			return errors.New("authentication cannot be disabled in production")
		}
		if c.AdminToken == "" {
			return errors.New("admin token is required in production")
		}
		if len(c.NodeSecrets) == 0 && c.SecretResolver == nil {
			return errors.New("production environment requires per-node credentials (NodeSecrets or SecretResolver); fleet-wide shared secret is prohibited in production")
		}
		if c.TLSCertFile == "" || c.TLSKeyFile == "" {
			return errors.New("production environment requires TLS configuration (cert and key files)")
		}
	} else if !isLoopback {
		if !c.Enabled {
			return errors.New("authentication cannot be disabled on non-loopback listen address")
		}
		if c.AdminToken == "" {
			return errors.New("admin token is required on non-loopback listen address")
		}
	}

	if c.Enabled {
		if c.SharedSecret == "" && len(c.NodeSecrets) == 0 && c.SecretResolver == nil {
			return errors.New("authentication is enabled but neither shared secret nor node secrets are configured")
		}
		if c.SharedSecret != "" && len(c.SharedSecret) < 16 {
			return errors.New("shared secret must be at least 16 characters for cryptographic security")
		}
		for _, s := range c.NodeSecrets {
			if len(s) < 16 {
				return errors.New("per-node secrets must be at least 16 characters for cryptographic security")
			}
		}
		if c.AdminToken != "" && len(c.AdminToken) < 16 {
			return errors.New("admin token must be at least 16 characters for cryptographic security")
		}
	}

	if err := ValidateTLSConfig(c.TLSCertFile, c.TLSKeyFile); err != nil {
		return err
	}

	return nil
}

// ValidateTLSConfig ensures that TLS certificate and key files are provided together and are cryptographically valid.
func ValidateTLSConfig(certFile, keyFile string) error {
	cert := strings.TrimSpace(certFile)
	key := strings.TrimSpace(keyFile)

	if cert == "" && key == "" {
		return nil // TLS disabled (development mode)
	}

	if (cert != "" && key == "") || (cert == "" && key != "") {
		return types.ErrIncompleteTLSConfig
	}

	// Verify that files exist
	if _, err := os.Stat(cert); err != nil {
		return fmt.Errorf("TLS cert file inaccessible: %w", err)
	}
	if _, err := os.Stat(key); err != nil {
		return fmt.Errorf("TLS key file inaccessible: %w", err)
	}

	// Verify that the certificate and private key can be loaded and match
	if _, err := tls.LoadX509KeyPair(cert, key); err != nil {
		return fmt.Errorf("invalid TLS certificate or private key: %w", err)
	}

	return nil
}

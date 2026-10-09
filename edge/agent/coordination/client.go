package coordination

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

var (
	// ErrNodeNotFound indicates that the node is not registered on the control plane.
	ErrNodeNotFound = errors.New("node not registered on control plane")
	// ErrServerUnavailable indicates the control plane could not be reached or returned 5xx.
	ErrServerUnavailable = errors.New("control plane server is unavailable")
	// ErrValidationFailed indicates the control plane rejected the payload with 4xx.
	ErrValidationFailed = errors.New("control plane rejected payload validation")
	// ErrAuthFailed indicates the control plane rejected authentication (401/403).
	ErrAuthFailed = errors.New("control plane authentication failed")
)

const (
	defaultHTTPTimeout   = 5 * time.Second
	maxResponseBodyBytes = 1 << 20 // 1MB payload ceiling
)

// RegistrationResponse represents the JSON response for node registration.
type RegistrationResponse struct {
	Status  string `json:"status"`
	NodeID  string `json:"node_id,omitempty"`
	Message string `json:"message,omitempty"`
}

// HeartbeatResponse represents the JSON response for node heartbeat.
type HeartbeatResponse struct {
	Status  string `json:"status"`
	NodeID  string `json:"node_id,omitempty"`
	Message string `json:"message,omitempty"`
}

// Client defines the contract for edge-to-control-plane coordination HTTP calls.
type Client interface {
	RegisterNode(ctx context.Context, reg *types.NodeRegistration) (*RegistrationResponse, error)
	SendHeartbeat(ctx context.Context, hb *types.Heartbeat) (*HeartbeatResponse, error)
	GetNode(ctx context.Context, nodeID string) (*types.NodeRegistration, error)
}

// HTTPClient implements Client backed by standard library http.Client.
type HTTPClient struct {
	baseURL      string
	httpClient   *http.Client
	nodeID       string
	sharedSecret string
}

// NewHTTPClient creates an HTTPClient for the given control plane base URL.
func NewHTTPClient(baseURL string, timeout time.Duration) *HTTPClient {
	if timeout <= 0 {
		timeout = defaultHTTPTimeout
	}
	cleanURL := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	return &HTTPClient{
		baseURL: cleanURL,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

// SetAuth sets the credentials for request signing against the control plane.
func (c *HTTPClient) SetAuth(nodeID, sharedSecret string) {
	c.nodeID = nodeID
	c.sharedSecret = sharedSecret
}

// RegisterNode sends a node registration request to POST /api/v1/nodes/register.
func (c *HTTPClient) RegisterNode(ctx context.Context, reg *types.NodeRegistration) (*RegistrationResponse, error) {
	if reg == nil {
		return nil, errors.New("node registration cannot be nil")
	}
	if err := reg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid node registration: %w", err)
	}

	url := c.baseURL + "/api/v1/nodes/register"
	var resp RegistrationResponse
	if err := c.doPost(ctx, url, reg, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// SendHeartbeat sends a heartbeat to POST /api/v1/nodes/heartbeat.
func (c *HTTPClient) SendHeartbeat(ctx context.Context, hb *types.Heartbeat) (*HeartbeatResponse, error) {
	if hb == nil {
		return nil, errors.New("heartbeat cannot be nil")
	}
	if err := hb.Validate(); err != nil {
		return nil, fmt.Errorf("invalid heartbeat: %w", err)
	}

	url := c.baseURL + "/api/v1/nodes/heartbeat"
	var resp HeartbeatResponse
	if err := c.doPost(ctx, url, hb, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// GetNode retrieves node registration information from GET /api/v1/nodes/{id}.
func (c *HTTPClient) GetNode(ctx context.Context, nodeID string) (*types.NodeRegistration, error) {
	cleanID := strings.TrimSpace(nodeID)
	if cleanID == "" {
		return nil, errors.New("nodeID cannot be empty")
	}

	url := fmt.Sprintf("%s/api/v1/nodes/%s", c.baseURL, cleanID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	if c.sharedSecret != "" {
		effectiveNodeID := c.nodeID
		if effectiveNodeID == "" {
			effectiveNodeID = cleanID
		}
		target := req.URL.RequestURI()
		if target == "" {
			target = req.URL.Path
		}
		headers, err := types.CreateAuthHeaders([]byte(c.sharedSecret), http.MethodGet, target, effectiveNodeID, nil)
		if err == nil {
			for k, v := range headers {
				req.Header.Set(k, v)
			}
		}
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrServerUnavailable, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return nil, fmt.Errorf("%w: status %d", ErrAuthFailed, resp.StatusCode)
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNodeNotFound
	}
	if resp.StatusCode >= 500 {
		return nil, fmt.Errorf("%w: status %d", ErrServerUnavailable, resp.StatusCode)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%w: status %d", ErrValidationFailed, resp.StatusCode)
	}

	limitedReader := io.LimitReader(resp.Body, maxResponseBodyBytes)
	var node types.NodeRegistration
	if err := json.NewDecoder(limitedReader).Decode(&node); err != nil {
		return nil, fmt.Errorf("failed to parse response body: %w", err)
	}

	return &node, nil
}

func (c *HTTPClient) doPost(ctx context.Context, url string, payload interface{}, target interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal request payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	if c.sharedSecret != "" {
		effectiveNodeID := c.nodeID
		if effectiveNodeID == "" {
			if reg, ok := payload.(*types.NodeRegistration); ok {
				effectiveNodeID = reg.NodeID
			} else if hb, ok := payload.(*types.Heartbeat); ok {
				effectiveNodeID = hb.NodeID
			}
		}
		reqTarget := req.URL.RequestURI()
		if reqTarget == "" {
			reqTarget = req.URL.Path
		}
		headers, err := types.CreateAuthHeaders([]byte(c.sharedSecret), http.MethodPost, reqTarget, effectiveNodeID, data)
		if err == nil {
			for k, v := range headers {
				req.Header.Set(k, v)
			}
		}
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrServerUnavailable, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return fmt.Errorf("%w: status %d", ErrAuthFailed, resp.StatusCode)
	}
	if resp.StatusCode == http.StatusNotFound {
		return ErrNodeNotFound
	}
	if resp.StatusCode >= 500 {
		return fmt.Errorf("%w: status %d", ErrServerUnavailable, resp.StatusCode)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%w: status %d", ErrValidationFailed, resp.StatusCode)
	}

	if target != nil {
		limitedReader := io.LimitReader(resp.Body, maxResponseBodyBytes)
		if err := json.NewDecoder(limitedReader).Decode(target); err != nil {
			return fmt.Errorf("failed to parse response body: %w", err)
		}
	}
	return nil
}

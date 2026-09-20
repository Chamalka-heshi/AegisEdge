package sync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// Common synchronization errors.
var (
	ErrServerUnavailable = errors.New("control plane server is unavailable")
	ErrClientValidation  = errors.New("control plane rejected batch with 4xx validation error")
	ErrUnexpectedStatus  = errors.New("unexpected HTTP response status from control plane")
)

// SyncResult captures the outcome of a batch transmission attempt across one or more HTTP retries.
type SyncResult struct {
	BatchID      string
	NodeID       string
	Status       string // "accepted" or "already_accepted"
	HTTPAttempts int    // Transient HTTP attempt count during this single sync cycle
}

// Client defines the contract for synchronizing telemetry batches to the control plane.
type Client interface {
	SyncBatch(ctx context.Context, batch *types.TelemetryBatch) (*SyncResult, error)
}

// HTTPClientOption configures an HTTPClient.
type HTTPClientOption func(*HTTPClient)

// WithHTTPTimeout sets custom HTTP request timeout.
func WithHTTPTimeout(timeout time.Duration) HTTPClientOption {
	return func(c *HTTPClient) {
		c.httpClient.Timeout = timeout
	}
}

// WithMaxRetries sets the maximum number of transient HTTP retries per sync attempt.
func WithMaxRetries(retries int) HTTPClientOption {
	return func(c *HTTPClient) {
		c.maxRetries = retries
	}
}

// WithBaseBackoff sets the starting backoff duration for exponential retry backoff.
func WithBaseBackoff(backoff time.Duration) HTTPClientOption {
	return func(c *HTTPClient) {
		c.baseBackoff = backoff
	}
}

// WithLogger sets the logger for sync operations.
func WithLogger(logger *slog.Logger) HTTPClientOption {
	return func(c *HTTPClient) {
		if logger != nil {
			c.logger = logger
		}
	}
}

// HTTPClient implements Client using standard net/http with bounded transient retries.
type HTTPClient struct {
	endpointURL string
	httpClient  *http.Client
	maxRetries  int
	baseBackoff time.Duration
	logger      *slog.Logger
}

// NewHTTPClient creates a new HTTPClient pointing to the specified control plane URL.
func NewHTTPClient(endpointURL string, opts ...HTTPClientOption) *HTTPClient {
	c := &HTTPClient{
		endpointURL: endpointURL,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
		maxRetries:  3,
		baseBackoff: 50 * time.Millisecond,
		logger:      slog.Default(),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// SyncBatch transmits a TelemetryBatch to the control plane.
// If transient network or 5xx server errors occur, it retries up to maxRetries with exponential backoff.
//
// Both 201 Created ("accepted") and 200 OK ("already_accepted") are treated as successful sync outcomes.
// Note: Internal HTTP retries within this method do NOT mutate persistent database state.
func (c *HTTPClient) SyncBatch(ctx context.Context, batch *types.TelemetryBatch) (*SyncResult, error) {
	if batch == nil {
		return nil, errors.New("cannot sync nil batch")
	}

	payloadBytes, err := json.Marshal(batch)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal batch payload: %w", err)
	}

	url := fmt.Sprintf("%s/api/v1/telemetry/batches", c.endpointURL)

	var lastErr error
	httpAttempts := 0

	for attempt := 1; attempt <= c.maxRetries; attempt++ {
		httpAttempts = attempt

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payloadBytes))
		if err != nil {
			return nil, fmt.Errorf("failed to create http request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("%w: %v", ErrServerUnavailable, err)
			c.logger.Warn("transient http error during sync attempt",
				slog.String("batch_id", batch.BatchID),
				slog.Int("http_attempt", attempt),
				slog.Int("max_retries", c.maxRetries),
				slog.Any("error", err),
			)
		} else {
			respBody, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()

			switch resp.StatusCode {
			case http.StatusCreated:
				// 201 Created -> newly accepted
				return &SyncResult{
					BatchID:      batch.BatchID,
					NodeID:       batch.NodeID,
					Status:       "accepted",
					HTTPAttempts: httpAttempts,
				}, nil

			case http.StatusOK:
				// 200 OK -> idempotent duplicate ("already_accepted")
				// Requirement: Edge MUST treat already_accepted as a successful sync outcome!
				return &SyncResult{
					BatchID:      batch.BatchID,
					NodeID:       batch.NodeID,
					Status:       "already_accepted",
					HTTPAttempts: httpAttempts,
				}, nil

			case http.StatusBadRequest, http.StatusRequestEntityTooLarge, http.StatusMethodNotAllowed:
				// Non-retryable 4xx client errors
				return nil, fmt.Errorf("%w: status %d: %s", ErrClientValidation, resp.StatusCode, string(respBody))

			default:
				// 5xx or unexpected status -> transient server error
				lastErr = fmt.Errorf("%w: status %d: %s", ErrUnexpectedStatus, resp.StatusCode, string(respBody))
				c.logger.Warn("transient server error during sync attempt",
					slog.String("batch_id", batch.BatchID),
					slog.Int("status_code", resp.StatusCode),
					slog.Int("http_attempt", attempt),
				)
			}
		}

		// Exponential backoff before next retry
		if attempt < c.maxRetries {
			backoff := c.baseBackoff * time.Duration(1<<(attempt-1))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}
	}

	return nil, fmt.Errorf("sync failed after %d http attempts: %w", httpAttempts, lastErr)
}

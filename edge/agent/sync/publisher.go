package sync

import (
	"context"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// BatchPublisher defines the transport-agnostic contract for publishing
// an already-persisted TelemetryBatch to an upstream destination.
//
// Implementations must NOT own SQLite persistence. The publisher receives
// a fully validated, already-persisted batch and is responsible only for
// transport-level publishing.
//
// The architecture supports:
//   - HTTP BatchPublisher (existing Phase 3 HTTPClient implements Client, not BatchPublisher)
//   - NATS BatchPublisher (NATSPublisher)
//
// Only one transport is active per synchronization cycle. There is no
// automatic fallback between transports.
type BatchPublisher interface {
	// PublishBatch publishes a telemetry batch to the upstream transport.
	// Returns nil only after the upstream has confirmed receipt (e.g., JetStream PubAck).
	// Returns an error if the transport is unavailable or the broker does not acknowledge.
	PublishBatch(ctx context.Context, batch *types.TelemetryBatch) error

	// Close cleanly terminates the publisher's connections and resources.
	Close() error
}

package metrics

import (
	"sync"
	"time"
)

// Metric names for control-plane operational metrics.
const (
	MetricIngestedBatchesTotal         = "aegisedge_control_plane_ingested_batches_total"
	MetricStorageProbeDurationSeconds  = "aegisedge_control_plane_storage_probe_duration_seconds"
	MetricStorageProbeFailuresTotal    = "aegisedge_control_plane_storage_probe_failures_total"
	MetricAuthFailuresTotal            = "aegisedge_control_plane_auth_failures_total"
	MetricNATSExhaustedDeliveriesTotal = "aegisedge_control_plane_nats_exhausted_deliveries_total"
	MetricRegisteredNodesCount         = "aegisedge_control_plane_registered_nodes_count"
)

// Bounded label values and allowlists.
const (
	TransportHTTP = "http"
	TransportNATS = "nats"

	StatusAccepted  = "accepted"
	StatusDuplicate = "duplicate"

	ReasonInvalidSignature = "invalid_signature"
	ReasonExpiredTimestamp = "expired_timestamp"
	ReasonFutureTimestamp  = "future_timestamp"
	ReasonReplayDetected   = "replay_detected"
	ReasonMissingAuth      = "missing_auth"
	ReasonForbidden        = "forbidden"
	ReasonUnauthorized     = "unauthorized"
	ReasonUnknown          = "unknown"
)

var allowedAuthReasons = map[string]struct{}{
	ReasonInvalidSignature: {},
	ReasonExpiredTimestamp: {},
	ReasonFutureTimestamp:  {},
	ReasonReplayDetected:   {},
	ReasonMissingAuth:      {},
	ReasonForbidden:        {},
	ReasonUnauthorized:     {},
	ReasonUnknown:          {},
}

// Metrics encapsulates the control-plane operational metrics.
type Metrics struct {
	registry *Registry

	ingestedBatches         *CounterVec
	storageProbeDuration    *SingleHistogram
	storageProbeFailures    *SingleCounter
	authFailures            *CounterVec
	natsExhaustedDeliveries *FuncCounter
	registeredNodes         *FuncGauge

	mu                   sync.RWMutex
	natsExhaustedSource  func() float64
	registeredNodeSource func() float64
}

// NewMetrics initializes and registers all control-plane operational metrics.
func NewMetrics() *Metrics {
	r := NewRegistry()
	m := &Metrics{
		registry: r,
	}

	m.ingestedBatches = NewCounterVec(
		MetricIngestedBatchesTotal,
		"Total number of telemetry batches successfully ingested and committed into durable storage.",
		[]string{"transport", "status"},
	)
	// Pre-populate canonical label pairs to ensure series exist immediately
	m.ingestedBatches.WithLabelValues(TransportHTTP, StatusAccepted)
	m.ingestedBatches.WithLabelValues(TransportHTTP, StatusDuplicate)
	m.ingestedBatches.WithLabelValues(TransportNATS, StatusAccepted)
	m.ingestedBatches.WithLabelValues(TransportNATS, StatusDuplicate)
	_ = r.Register(m.ingestedBatches)

	m.storageProbeDuration = NewSingleHistogram(
		MetricStorageProbeDurationSeconds,
		"Latency of storage health readiness probes in seconds.",
		DefaultStorageProbeBuckets,
	)
	_ = r.Register(m.storageProbeDuration)

	m.storageProbeFailures = NewSingleCounter(
		MetricStorageProbeFailuresTotal,
		"Total number of storage health readiness probe failures.",
	)
	_ = r.Register(m.storageProbeFailures)

	m.authFailures = NewCounterVec(
		MetricAuthFailuresTotal,
		"Total number of control plane authentication and authorization failures.",
		[]string{"reason"},
	)
	// Pre-populate canonical reasons from ADR-0017 §7.1 allowlist
	for reason := range allowedAuthReasons {
		m.authFailures.WithLabelValues(reason)
	}
	_ = r.Register(m.authFailures)

	m.natsExhaustedDeliveries = NewFuncCounter(
		MetricNATSExhaustedDeliveriesTotal,
		"Total number of NATS JetStream messages that reached maximum delivery attempts.",
		func() float64 {
			m.mu.RLock()
			fn := m.natsExhaustedSource
			m.mu.RUnlock()
			if fn == nil {
				return 0
			}
			return fn()
		},
	)
	_ = r.Register(m.natsExhaustedDeliveries)

	m.registeredNodes = NewFuncGauge(
		MetricRegisteredNodesCount,
		"Total number of edge nodes currently registered in the control plane.",
		func() float64 {
			m.mu.RLock()
			fn := m.registeredNodeSource
			m.mu.RUnlock()
			if fn == nil {
				return 0
			}
			return fn()
		},
	)
	_ = r.Register(m.registeredNodes)

	return m
}

// Registry returns the underlying metric registry.
func (m *Metrics) Registry() *Registry {
	return m.registry
}

// RecordIngestedBatch increments the batch ingestion counter for committed batches.
func (m *Metrics) RecordIngestedBatch(transport, status string) {
	if m == nil {
		return
	}
	// Sanitize transport
	if transport != TransportHTTP && transport != TransportNATS {
		transport = "unknown"
	}
	// Sanitize status
	if status != StatusAccepted && status != StatusDuplicate {
		status = "unknown"
	}
	m.ingestedBatches.WithLabelValues(transport, status).Inc()
}

// RecordStorageProbeDuration records the duration of a storage health probe.
func (m *Metrics) RecordStorageProbeDuration(d time.Duration) {
	if m == nil {
		return
	}
	m.storageProbeDuration.Observe(d.Seconds())
}

// RecordStorageProbeDurationSeconds records the duration in seconds directly.
func (m *Metrics) RecordStorageProbeDurationSeconds(sec float64) {
	if m == nil {
		return
	}
	m.storageProbeDuration.Observe(sec)
}

// RecordStorageProbeFailure increments the storage probe failure counter.
func (m *Metrics) RecordStorageProbeFailure() {
	if m == nil {
		return
	}
	m.storageProbeFailures.Inc()
}

// SanitizeAuthReason maps an arbitrary auth error string to the bounded allowlist.
func SanitizeAuthReason(raw string) string {
	switch raw {
	case ReasonInvalidSignature, ReasonExpiredTimestamp, ReasonFutureTimestamp,
		ReasonReplayDetected, ReasonMissingAuth, ReasonForbidden, ReasonUnauthorized:
		return raw
	case "missing_auth_headers":
		return ReasonMissingAuth
	default:
		return ReasonUnknown
	}
}

// RecordAuthFailure increments the auth failure counter for a discrete sanitized reason.
func (m *Metrics) RecordAuthFailure(rawReason string) {
	if m == nil {
		return
	}
	reason := SanitizeAuthReason(rawReason)
	m.authFailures.WithLabelValues(reason).Inc()
}

// SetNATSExhaustedDeliveriesSource registers a dynamic source for the NATS exhausted deliveries counter.
func (m *Metrics) SetNATSExhaustedDeliveriesSource(fn func() float64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.natsExhaustedSource = fn
	m.mu.Unlock()
}

// SetRegisteredNodesSource registers a dynamic source for the registered nodes count gauge.
func (m *Metrics) SetRegisteredNodesSource(fn func() float64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.registeredNodeSource = fn
	m.mu.Unlock()
}

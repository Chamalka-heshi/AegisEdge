package metrics

import (
	"sync"
	"time"
)

// Metric names for control-plane operational metrics.
const (
	MetricIngestedBatchesTotal               = "aegisedge_control_plane_ingested_batches_total"
	MetricStorageProbeDurationSeconds        = "aegisedge_control_plane_storage_probe_duration_seconds"
	MetricStorageProbeFailuresTotal          = "aegisedge_control_plane_storage_probe_failures_total"
	MetricAuthFailuresTotal                  = "aegisedge_control_plane_auth_failures_total"
	MetricNATSExhaustedDeliveriesTotal       = "aegisedge_control_plane_nats_exhausted_deliveries_total"
	MetricRegisteredNodesCount               = "aegisedge_control_plane_registered_nodes_count"
	MetricNATSAdvisoryStreamMessages         = "aegisedge_control_plane_nats_advisory_stream_messages"
	MetricNATSAdvisoryStreamBytes            = "aegisedge_control_plane_nats_advisory_stream_bytes"
	MetricNATSAdvisoryPendingBacklog         = "aegisedge_control_plane_nats_advisory_pending_backlog"
	MetricNATSAdvisoryPendingAck             = "aegisedge_control_plane_nats_advisory_pending_ack"
	MetricNATSAdvisoryFetchErrorsTotal       = "aegisedge_control_plane_nats_advisory_fetch_errors_total"
	MetricNATSAdvisoryEvictionsTotal         = "aegisedge_control_plane_nats_advisory_evictions_total"
	MetricNATSQuarantinedTotal               = "aegisedge_control_plane_nats_quarantined_total"
	MetricNATSRecoveredTotal                 = "aegisedge_control_plane_nats_recovered_total"
	MetricNATSStalledUnacknowledgedSequences = "aegisedge_control_plane_nats_stalled_unacknowledged_sequences"
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

	ingestedBatches                    *CounterVec
	storageProbeDuration               *SingleHistogram
	storageProbeFailures               *SingleCounter
	authFailures                       *CounterVec
	natsExhaustedDeliveries            *FuncCounter
	registeredNodes                    *FuncGauge
	natsAdvisoryStreamMsgs             *FuncGauge
	natsAdvisoryStreamBytes            *FuncGauge
	natsAdvisoryPendingBacklog         *FuncGauge
	natsAdvisoryPendingAck             *FuncGauge
	natsAdvisoryFetchErrors            *FuncCounter
	natsAdvisoryEvictions              *FuncCounter
	natsQuarantined                    *FuncCounter
	natsRecovered                      *FuncCounter
	natsStalledUnacknowledgedSequences *FuncGauge

	mu                                      sync.RWMutex
	natsExhaustedSource                     func() float64
	registeredNodeSource                    func() float64
	natsAdvisoryStreamMsgsSource            func() float64
	natsAdvisoryStreamBytesSource           func() float64
	natsAdvisoryPendingBacklogSource        func() float64
	natsAdvisoryPendingAckSource            func() float64
	natsAdvisoryFetchErrorsSource           func() float64
	natsAdvisoryEvictionsSource             func() float64
	natsQuarantinedSource                   func() float64
	natsRecoveredSource                     func() float64
	natsStalledUnacknowledgedSequenceSource func() float64
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

	m.natsAdvisoryStreamMsgs = NewFuncGauge(
		MetricNATSAdvisoryStreamMessages,
		"Current number of advisory messages retained in the durable advisory stream.",
		func() float64 {
			m.mu.RLock()
			fn := m.natsAdvisoryStreamMsgsSource
			m.mu.RUnlock()
			if fn == nil {
				return 0
			}
			return fn()
		},
	)
	_ = r.Register(m.natsAdvisoryStreamMsgs)

	m.natsAdvisoryStreamBytes = NewFuncGauge(
		MetricNATSAdvisoryStreamBytes,
		"Current storage bytes utilized by the durable advisory stream.",
		func() float64 {
			m.mu.RLock()
			fn := m.natsAdvisoryStreamBytesSource
			m.mu.RUnlock()
			if fn == nil {
				return 0
			}
			return fn()
		},
	)
	_ = r.Register(m.natsAdvisoryStreamBytes)

	m.natsAdvisoryPendingBacklog = NewFuncGauge(
		MetricNATSAdvisoryPendingBacklog,
		"Current number of pending advisories awaiting consumer retrieval in JetStream.",
		func() float64 {
			m.mu.RLock()
			fn := m.natsAdvisoryPendingBacklogSource
			m.mu.RUnlock()
			if fn == nil {
				return 0
			}
			return fn()
		},
	)
	_ = r.Register(m.natsAdvisoryPendingBacklog)

	m.natsAdvisoryPendingAck = NewFuncGauge(
		MetricNATSAdvisoryPendingAck,
		"Current number of delivered advisories awaiting acknowledgment from the consumer.",
		func() float64 {
			m.mu.RLock()
			fn := m.natsAdvisoryPendingAckSource
			m.mu.RUnlock()
			if fn == nil {
				return 0
			}
			return fn()
		},
	)
	_ = r.Register(m.natsAdvisoryPendingAck)

	m.natsAdvisoryFetchErrors = NewFuncCounter(
		MetricNATSAdvisoryFetchErrorsTotal,
		"Total number of transport or protocol errors encountered while fetching from the advisory stream.",
		func() float64 {
			m.mu.RLock()
			fn := m.natsAdvisoryFetchErrorsSource
			m.mu.RUnlock()
			if fn == nil {
				return 0
			}
			return fn()
		},
	)
	_ = r.Register(m.natsAdvisoryFetchErrors)

	m.natsAdvisoryEvictions = NewFuncCounter(
		MetricNATSAdvisoryEvictionsTotal,
		"Total number of advisory stream sequences pruned under the DiscardOld retention policy.",
		func() float64 {
			m.mu.RLock()
			fn := m.natsAdvisoryEvictionsSource
			m.mu.RUnlock()
			if fn == nil {
				return 0
			}
			return fn()
		},
	)
	_ = r.Register(m.natsAdvisoryEvictions)

	m.natsQuarantined = NewFuncCounter(
		MetricNATSQuarantinedTotal,
		"Total number of messages successfully recorded in durable quarantine.",
		func() float64 {
			m.mu.RLock()
			fn := m.natsQuarantinedSource
			m.mu.RUnlock()
			if fn == nil {
				return 0
			}
			return fn()
		},
	)
	_ = r.Register(m.natsQuarantined)

	m.natsRecovered = NewFuncCounter(
		MetricNATSRecoveredTotal,
		"Total number of messages recovered through stream sequence or advisory recovery pipelines.",
		func() float64 {
			m.mu.RLock()
			fn := m.natsRecoveredSource
			m.mu.RUnlock()
			if fn == nil {
				return 0
			}
			return fn()
		},
	)
	_ = r.Register(m.natsRecovered)

	m.natsStalledUnacknowledgedSequences = NewFuncGauge(
		MetricNATSStalledUnacknowledgedSequences,
		"Current number of distinct unacknowledged stream sequences observed as stalled without authoritative exhaustion evidence.",
		func() float64 {
			m.mu.RLock()
			fn := m.natsStalledUnacknowledgedSequenceSource
			m.mu.RUnlock()
			if fn == nil {
				return 0
			}
			return fn()
		},
	)
	_ = r.Register(m.natsStalledUnacknowledgedSequences)

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

// SetNATSAdvisoryStreamMessagesSource registers a dynamic source for the advisory stream messages count gauge.
func (m *Metrics) SetNATSAdvisoryStreamMessagesSource(fn func() float64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.natsAdvisoryStreamMsgsSource = fn
	m.mu.Unlock()
}

// SetNATSAdvisoryStreamBytesSource registers a dynamic source for the advisory stream storage bytes gauge.
func (m *Metrics) SetNATSAdvisoryStreamBytesSource(fn func() float64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.natsAdvisoryStreamBytesSource = fn
	m.mu.Unlock()
}

// SetNATSAdvisoryPendingBacklogSource registers a dynamic source for the pending advisories backlog gauge.
func (m *Metrics) SetNATSAdvisoryPendingBacklogSource(fn func() float64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.natsAdvisoryPendingBacklogSource = fn
	m.mu.Unlock()
}

// SetNATSAdvisoryPendingAckSource registers a dynamic source for the unacknowledged advisories gauge.
func (m *Metrics) SetNATSAdvisoryPendingAckSource(fn func() float64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.natsAdvisoryPendingAckSource = fn
	m.mu.Unlock()
}

// SetNATSAdvisoryFetchErrorsSource registers a dynamic source for the advisory fetch errors counter.
func (m *Metrics) SetNATSAdvisoryFetchErrorsSource(fn func() float64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.natsAdvisoryFetchErrorsSource = fn
	m.mu.Unlock()
}

// SetNATSAdvisoryEvictionsSource registers a dynamic source for the advisory evictions counter.
func (m *Metrics) SetNATSAdvisoryEvictionsSource(fn func() float64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.natsAdvisoryEvictionsSource = fn
	m.mu.Unlock()
}

// SetNATSQuarantinedSource registers a dynamic source for the total quarantined records counter.
func (m *Metrics) SetNATSQuarantinedSource(fn func() float64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.natsQuarantinedSource = fn
	m.mu.Unlock()
}

// SetNATSRecoveredSource registers a dynamic source for the total recovered records counter.
func (m *Metrics) SetNATSRecoveredSource(fn func() float64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.natsRecoveredSource = fn
	m.mu.Unlock()
}

// SetNATSStalledUnacknowledgedSequencesSource registers a dynamic source for the stalled unacknowledged sequences gauge.
func (m *Metrics) SetNATSStalledUnacknowledgedSequencesSource(fn func() float64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.natsStalledUnacknowledgedSequenceSource = fn
	m.mu.Unlock()
}

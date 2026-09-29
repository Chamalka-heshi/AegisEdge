package incident

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// Common incident engine domain errors.
var (
	ErrNilAnomalySignal      = errors.New("anomaly signal cannot be nil")
	ErrNilIncident           = errors.New("incident cannot be nil")
	ErrInvalidIncidentStatus = errors.New("cannot register incident: status must be ANOMALY_DETECTED")
	ErrIncidentAlreadyActive = errors.New("an active incident already exists for this node and metric stream")
	ErrInvalidPolicyConfig   = errors.New("invalid incident policy configuration: M must be >= 1 and <= N")
	ErrStaleAnomalySignal    = errors.New("anomaly signal timestamp predates observation window")
	ErrNoActiveIncident      = errors.New("no active incident found for correlation key")
)

// IncidentEngine defines the boundary for local edge incident correlation and lifecycle management.
// It manages incident registration, state transitions according to the Incident FSM, and recovery.
type IncidentEngine interface {
	// Process evaluates an AnomalySignal against the correlation policy.
	// Returns:
	// - (*types.Incident, nil) if an incident was created or an active incident was updated.
	// - (nil, nil) if the anomaly observation was recorded but the correlation threshold was not reached.
	// - (nil, err) if validation failed or an error occurred during evaluation.
	Process(ctx context.Context, sig *types.AnomalySignal) (*types.Incident, error)

	// RegisterIncident registers an externally detected Incident into the local lifecycle manager.
	RegisterIncident(ctx context.Context, inc *types.Incident) (*types.Incident, error)

	// GetActiveIncident returns the active incident for the specified node and metric, or nil if none exists.
	GetActiveIncident(nodeID, metricName string) *types.Incident

	// TransitionActiveIncident transitions an active incident to a new valid status in accordance with the Incident FSM.
	TransitionActiveIncident(ctx context.Context, nodeID, metricName string, target types.IncidentStatus, now time.Time) (*types.Incident, error)

	// CloseActiveIncident removes the active incident from the local stream and resets observations (stream lifecycle cleanup).
	// If the incident is in MITIGATING or ESCALATED, it canonically transitions to RECOVERED;
	// if in ANOMALY_DETECTED, it cleans up the stream without forging an invalid transition.
	CloseActiveIncident(ctx context.Context, nodeID, metricName string, now time.Time) (*types.Incident, error)

	// ResolveIncident transitions an active incident to RECOVERED according to the canonical Incident FSM.
	// Returns ErrInvalidStateTransition if called directly on an incident in ANOMALY_DETECTED.
	ResolveIncident(ctx context.Context, nodeID, metricName string, now time.Time) (*types.Incident, error)

	// RecoverFromStore restores active incidents and recent correlation observations from SQLite into memory.
	RecoverFromStore(ctx context.Context) (int, int, error)
}

// PolicyConfig defines the operational correlation parameters for incident generation.
type PolicyConfig struct {
	// M is the minimum number of qualifying anomaly observations required to trigger an incident.
	// Defaults to 2 if <= 0.
	M int

	// N is the sliding observation window size (maximum observations tracked per metric stream).
	// Defaults to 3 if <= 0.
	N int

	// WindowDuration defines the maximum temporal horizon for correlating observations.
	// Signals older than this horizon relative to the latest seen signal are considered stale.
	// Defaults to 5 minutes if <= 0.
	WindowDuration time.Duration

	// RuleNamePrefix is prepended to the generated Incident.RuleName (e.g. "threshold_").
	// Defaults to "threshold_" if empty.
	RuleNamePrefix string

	// SeverityMap allows metric-specific overrides for IncidentSeverity.
	// If unconfigured or not matching, deterministic score-based severity mapping is used.
	SeverityMap map[string]types.IncidentSeverity

	// IDGenerator optionally overrides default UUIDv4 generation for deterministic testing.
	IDGenerator func() (string, error)

	// Store optionally provides durable persistence for incidents and observations in SQLite.
	Store storage.IncidentStore
}

// DefaultPolicyConfig returns standard recommended defaults: 2-of-3 observations within 5 minutes.
func DefaultPolicyConfig() PolicyConfig {
	return PolicyConfig{
		M:              2,
		N:              3,
		WindowDuration: 5 * time.Minute,
		RuleNamePrefix: "threshold_",
	}
}

// MapSeverity provides a deterministic, explainable mapping from AnomalyScore [0.0, 1.0]
// to standard IncidentSeverity enums:
// - Score >= 0.85 => CRITICAL
// - Score >= 0.60 => HIGH
// - Score >= 0.30 => MEDIUM
// - Score <  0.30 => LOW
func MapSeverity(score float64) types.IncidentSeverity {
	switch {
	case score >= 0.85:
		return types.SeverityCritical
	case score >= 0.60:
		return types.SeverityHigh
	case score >= 0.30:
		return types.SeverityMedium
	default:
		return types.SeverityLow
	}
}

// observation represents a single chronological anomaly event in the sliding window.
type observation struct {
	anomalyID  string
	detectedAt time.Time
}

// streamState maintains process-local correlation state for a single (NodeID, MetricName) key.
type streamState struct {
	nodeID         string
	metricName     string
	observations   []observation
	seenAnomalies  map[string]time.Time
	activeIncident *types.Incident
}

// LocalEngine implements the IncidentEngine interface using thread-safe memory with optional SQLite durability.
type LocalEngine struct {
	mu      sync.RWMutex
	cfg     PolicyConfig
	streams map[string]*streamState
}

// NewLocalEngine constructs and validates a new LocalEngine instance.
func NewLocalEngine(cfg PolicyConfig) (*LocalEngine, error) {
	if cfg.M <= 0 {
		cfg.M = 2
	}
	if cfg.N <= 0 {
		cfg.N = 3
	}
	if cfg.M > cfg.N {
		return nil, ErrInvalidPolicyConfig
	}
	if cfg.WindowDuration <= 0 {
		cfg.WindowDuration = 5 * time.Minute
	}
	if cfg.RuleNamePrefix == "" {
		cfg.RuleNamePrefix = "threshold_"
	}
	if cfg.IDGenerator == nil {
		cfg.IDGenerator = newUUIDv4
	}

	return &LocalEngine{
		cfg:     cfg,
		streams: make(map[string]*streamState),
	}, nil
}

// RecoverFromStore restores active incidents and recent correlation observations from SQLite into memory.
// Returns (activeIncidentsCount, observationsCount, error).
func (e *LocalEngine) RecoverFromStore(ctx context.Context) (int, int, error) {
	return e.RecoverFromStoreAt(ctx, time.Now().UTC())
}

// RecoverFromStoreAt restores state using an explicit reference time (useful for deterministic tests).
func (e *LocalEngine) RecoverFromStoreAt(ctx context.Context, refTime time.Time) (int, int, error) {
	if e.cfg.Store == nil {
		return 0, 0, nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// 1. Recover active incidents from SQLite
	activeIncidents, err := e.cfg.Store.ListActiveIncidents(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to recover active incidents from store: %w", err)
	}

	for _, inc := range activeIncidents {
		key := inc.NodeID + ":" + inc.TriggerMetric
		st, exists := e.streams[key]
		if !exists {
			st = &streamState{
				nodeID:        inc.NodeID,
				metricName:    inc.TriggerMetric,
				observations:  make([]observation, 0, e.cfg.N),
				seenAnomalies: make(map[string]time.Time),
			}
			e.streams[key] = st
		}
		st.activeIncident = inc
	}

	// 2. Recover recent observations within sliding window across all streams
	since := refTime.Add(-e.cfg.WindowDuration)
	recentObs, err := e.cfg.Store.ListAllRecentObservations(ctx, since, 500)
	if err != nil {
		return len(activeIncidents), 0, fmt.Errorf("failed to recover recent observations from store: %w", err)
	}

	obsCount := 0
	for _, o := range recentObs {
		key := o.NodeID + ":" + o.MetricName
		st, exists := e.streams[key]
		if !exists {
			st = &streamState{
				nodeID:        o.NodeID,
				metricName:    o.MetricName,
				observations:  make([]observation, 0, e.cfg.N),
				seenAnomalies: make(map[string]time.Time),
			}
			e.streams[key] = st
		}
		st.seenAnomalies[o.AnomalyID] = o.DetectedAt
		st.insertObservation(observation{
			anomalyID:  o.AnomalyID,
			detectedAt: o.DetectedAt,
		}, e.cfg.N, e.cfg.WindowDuration)
		obsCount++
	}

	return len(activeIncidents), obsCount, nil
}

// Process evaluates an AnomalySignal against the sliding M-of-N window.
// It is thread-safe, idempotent on AnomalyID, and strictly isolated per (NodeID, MetricName).
func (e *LocalEngine) Process(ctx context.Context, sig *types.AnomalySignal) (*types.Incident, error) {
	if sig == nil {
		return nil, ErrNilAnomalySignal
	}
	if err := sig.Validate(); err != nil {
		return nil, fmt.Errorf("invalid anomaly signal: %w", err)
	}

	correlationKey := sig.NodeID + ":" + sig.MetricName

	e.mu.Lock()
	defer e.mu.Unlock()

	st, exists := e.streams[correlationKey]
	if !exists {
		st = &streamState{
			nodeID:        sig.NodeID,
			metricName:    sig.MetricName,
			observations:  make([]observation, 0, e.cfg.N),
			seenAnomalies: make(map[string]time.Time),
		}
		e.streams[correlationKey] = st
	}

	// 1. Idempotency Check: check in-memory cache first
	if _, seen := st.seenAnomalies[sig.AnomalyID]; seen {
		// Return active incident if one exists, otherwise nil; do not increment observations
		return st.activeIncident, nil
	}

	// Check store-level idempotency if persistent store is configured
	if e.cfg.Store != nil {
		has, err := e.cfg.Store.HasObservation(ctx, sig.AnomalyID)
		if err != nil {
			return nil, fmt.Errorf("store idempotency check failed: %w", err)
		}
		if has {
			st.seenAnomalies[sig.AnomalyID] = sig.DetectedAt
			return st.activeIncident, nil
		}
	}

	// 2. Out-of-Order / Temporal Window Check
	if len(st.observations) > 0 {
		latest := st.observations[len(st.observations)-1].detectedAt
		// If signal detectedAt is older than latest - WindowDuration, reject as stale
		if sig.DetectedAt.Before(latest.Add(-e.cfg.WindowDuration)) {
			return nil, fmt.Errorf("%w: signal timestamp %v predates window boundary %v",
				ErrStaleAnomalySignal, sig.DetectedAt, latest.Add(-e.cfg.WindowDuration))
		}
	}

	// Prepare stored observation
	obs := &storage.StoredObservation{
		AnomalyID:       sig.AnomalyID,
		NodeID:          sig.NodeID,
		MetricName:      sig.MetricName,
		DetectedAt:      sig.DetectedAt,
		ObservedValue:   sig.ObservedValue,
		AnomalyScore:    sig.AnomalyScore,
		DetectionMethod: sig.DetectionMethod,
		Evidence:        copyMap(sig.Evidence),
		CreatedAt:       time.Now().UTC(),
	}

	// 3. Persistent Breach Handling: if an incident is already active, correlate to existing IncidentID
	if st.activeIncident != nil && isActiveStatus(st.activeIncident.Status) {
		updatedInc := *st.activeIncident
		updatedInc.UpdatedAt = sig.DetectedAt
		updatedInc.TriggerValue = sig.ObservedValue
		if updatedInc.Evidence != nil {
			ev := copyMap(updatedInc.Evidence)
			ev["latest_anomaly_id"] = sig.AnomalyID
			ev["latest_observed_value"] = strconv.FormatFloat(sig.ObservedValue, 'f', 4, 64)
			ev["latest_anomaly_score"] = strconv.FormatFloat(sig.AnomalyScore, 'f', 4, 64)
			updatedInc.Evidence = ev
		}

		if e.cfg.Store != nil {
			isNew, err := e.cfg.Store.PersistIncidentEvaluation(ctx, obs, &updatedInc)
			if err != nil {
				return nil, fmt.Errorf("persisting persistent anomaly evaluation failed: %w", err)
			}
			if !isNew {
				return st.activeIncident, nil
			}
		}

		*st.activeIncident = updatedInc
		st.seenAnomalies[sig.AnomalyID] = sig.DetectedAt
		st.insertObservation(observation{
			anomalyID:  sig.AnomalyID,
			detectedAt: sig.DetectedAt,
		}, e.cfg.N, e.cfg.WindowDuration)

		return st.activeIncident, nil
	}

	// 4. Inactive Incident: evaluate M-of-N sliding window
	// Build candidate observations to evaluate threshold
	candidateObs := append([]observation(nil), st.observations...)
	candidateObs = append(candidateObs, observation{
		anomalyID:  sig.AnomalyID,
		detectedAt: sig.DetectedAt,
	})
	sort.SliceStable(candidateObs, func(i, j int) bool {
		return candidateObs[i].detectedAt.Before(candidateObs[j].detectedAt)
	})

	cutoff := sig.DetectedAt.Add(-e.cfg.WindowDuration)
	validCount := 0
	for _, o := range candidateObs {
		if !o.detectedAt.Before(cutoff) {
			validCount++
		}
	}

	// 5. Evaluate M-of-N threshold
	if validCount < e.cfg.M {
		// Threshold not yet satisfied: persist observation alone if store configured
		if e.cfg.Store != nil {
			isNew, err := e.cfg.Store.PersistIncidentEvaluation(ctx, obs, nil)
			if err != nil {
				return nil, fmt.Errorf("persisting anomaly observation failed: %w", err)
			}
			if !isNew {
				return nil, nil
			}
		}

		st.seenAnomalies[sig.AnomalyID] = sig.DetectedAt
		st.insertObservation(observation{
			anomalyID:  sig.AnomalyID,
			detectedAt: sig.DetectedAt,
		}, e.cfg.N, e.cfg.WindowDuration)
		return nil, nil
	}

	// 6. Threshold satisfied: generate new Incident domain entity
	incidentID, err := e.cfg.IDGenerator()
	if err != nil {
		return nil, fmt.Errorf("failed to generate incident id: %w", err)
	}

	severity := MapSeverity(sig.AnomalyScore)
	if override, ok := e.cfg.SeverityMap[sig.MetricName]; ok && override.IsValid() {
		severity = override
	}

	// Populate evidence map preserving detector provenance
	evidence := make(map[string]string)
	for k, v := range sig.Evidence {
		evidence[k] = v
	}
	evidence["anomaly_id"] = sig.AnomalyID
	evidence["anomaly_score"] = strconv.FormatFloat(sig.AnomalyScore, 'f', 4, 64)
	evidence["deviation"] = strconv.FormatFloat(sig.Deviation, 'f', 4, 64)
	evidence["observed_value"] = strconv.FormatFloat(sig.ObservedValue, 'f', 4, 64)
	evidence["expected_value"] = strconv.FormatFloat(sig.ExpectedValue, 'f', 4, 64)
	evidence["detection_method"] = sig.DetectionMethod
	evidence["detector_version"] = sig.DetectorVersion
	if sig.CorrelationID != "" {
		evidence["correlation_id"] = sig.CorrelationID
	}

	inc := &types.Incident{
		IncidentID:    incidentID,
		NodeID:        sig.NodeID,
		RuleName:      e.cfg.RuleNamePrefix + sig.MetricName,
		Severity:      severity,
		Status:        types.StatusAnomalyDetected,
		Description:   fmt.Sprintf("Anomaly threshold exceeded for %s on node %s", sig.MetricName, sig.NodeID),
		TriggerMetric: sig.MetricName,
		TriggerValue:  sig.ObservedValue,
		Threshold:     sig.ExpectedValue,
		Evidence:      evidence,
		TriggeredAt:   sig.DetectedAt,
		UpdatedAt:     sig.DetectedAt,
		ResolvedAt:    nil,
		Mitigations:   nil, // Strict non-goal: no mitigations in Phase 5.5
	}

	if err := inc.Validate(); err != nil {
		return nil, fmt.Errorf("generated incident failed validation: %w", err)
	}

	// Persist observation and new incident atomically
	if e.cfg.Store != nil {
		isNew, err := e.cfg.Store.PersistIncidentEvaluation(ctx, obs, inc)
		if err != nil {
			return nil, fmt.Errorf("persisting new incident failed: %w", err)
		}
		if !isNew {
			return st.activeIncident, nil
		}
	}

	st.seenAnomalies[sig.AnomalyID] = sig.DetectedAt
	st.activeIncident = inc
	st.insertObservation(observation{
		anomalyID:  sig.AnomalyID,
		detectedAt: sig.DetectedAt,
	}, e.cfg.N, e.cfg.WindowDuration)

	return inc, nil
}

// RegisterIncident registers an externally detected Incident (such as mapped from an AnomalySignal)
// into the local incident lifecycle.
// It fails closed: nil incidents or incidents failing validation are rejected.
// Status must be ANOMALY_DETECTED.
// If an active incident with the same IncidentID already exists, it updates timestamps idempotently.
// If an active incident with a different IncidentID already exists, it returns ErrIncidentAlreadyActive.
func (e *LocalEngine) RegisterIncident(ctx context.Context, inc *types.Incident) (*types.Incident, error) {
	if inc == nil {
		return nil, ErrNilIncident
	}
	if err := inc.Validate(); err != nil {
		return nil, fmt.Errorf("invalid incident payload: %w", err)
	}
	if inc.Status != types.StatusAnomalyDetected {
		return nil, fmt.Errorf("%w: got %s", ErrInvalidIncidentStatus, inc.Status)
	}

	correlationKey := inc.NodeID + ":" + inc.TriggerMetric

	e.mu.Lock()
	defer e.mu.Unlock()

	st, exists := e.streams[correlationKey]
	if !exists {
		st = &streamState{
			nodeID:        inc.NodeID,
			metricName:    inc.TriggerMetric,
			observations:  make([]observation, 0, e.cfg.N),
			seenAnomalies: make(map[string]time.Time),
		}
		e.streams[correlationKey] = st
	}

	// Active incident protection
	if st.activeIncident != nil && isActiveStatus(st.activeIncident.Status) {
		if st.activeIncident.IncidentID == inc.IncidentID {
			// Idempotent re-registration of the same active incident
			st.activeIncident.UpdatedAt = inc.UpdatedAt
			st.activeIncident.TriggerValue = inc.TriggerValue
			return st.activeIncident, nil
		}
		return nil, fmt.Errorf("%w: existing active incident %s", ErrIncidentAlreadyActive, st.activeIncident.IncidentID)
	}

	// Register new active incident
	incCopy := *inc
	if inc.Evidence != nil {
		incCopy.Evidence = copyMap(inc.Evidence)
	}

	if e.cfg.Store != nil {
		isNew, err := e.cfg.Store.PersistIncidentEvaluation(ctx, nil, &incCopy)
		if err != nil {
			return nil, fmt.Errorf("failed to persist registered incident: %w", err)
		}
		if !isNew {
			return st.activeIncident, nil
		}
	}

	st.activeIncident = &incCopy
	return st.activeIncident, nil
}

// GetActiveIncident retrieves a copy of the active incident for a given stream, or nil if none.
func (e *LocalEngine) GetActiveIncident(nodeID, metricName string) *types.Incident {
	e.mu.RLock()
	defer e.mu.RUnlock()

	key := nodeID + ":" + metricName
	st, exists := e.streams[key]
	if !exists || st.activeIncident == nil || !isActiveStatus(st.activeIncident.Status) {
		return nil
	}

	cp := *st.activeIncident
	if st.activeIncident.Evidence != nil {
		cp.Evidence = make(map[string]string, len(st.activeIncident.Evidence))
		for k, v := range st.activeIncident.Evidence {
			cp.Evidence[k] = v
		}
	}
	return &cp
}

// TransitionActiveIncident transitions an active incident to a new valid status in accordance with the Incident FSM.
func (e *LocalEngine) TransitionActiveIncident(ctx context.Context, nodeID, metricName string, target types.IncidentStatus, now time.Time) (*types.Incident, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	key := nodeID + ":" + metricName
	st, exists := e.streams[key]
	if !exists || st.activeIncident == nil {
		return nil, ErrNoActiveIncident
	}

	cp := *st.activeIncident
	if err := cp.TransitionTo(target, now); err != nil {
		return nil, err
	}

	if e.cfg.Store != nil {
		if err := e.cfg.Store.UpdateIncidentStatus(ctx, cp.IncidentID, cp.Status, cp.UpdatedAt, cp.ResolvedAt); err != nil {
			return nil, fmt.Errorf("failed to persist incident status transition: %w", err)
		}
	}

	_ = st.activeIncident.TransitionTo(target, now)
	inc := st.activeIncident
	if target == types.StatusNormal {
		st.activeIncident = nil
		st.observations = nil
	} else if target == types.StatusRecovered {
		st.observations = nil
	}

	return inc, nil
}

// CloseActiveIncident resets the correlation stream and removes the active incident from memory as a lifecycle cleanup mechanism.
// It does NOT falsely claim a canonical RECOVERED transition when the incident is in ANOMALY_DETECTED,
// and it never automatically introduces MITIGATING or ESCALATED.
// If the incident is already in a state that permits RECOVERED (MITIGATING or ESCALATED), it applies that canonical transition.
// If the incident is in ANOMALY_DETECTED, it cleans up the active stream state without forging an invalid FSM transition.
func (e *LocalEngine) CloseActiveIncident(ctx context.Context, nodeID, metricName string, now time.Time) (*types.Incident, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	key := nodeID + ":" + metricName
	st, exists := e.streams[key]
	if !exists || st.activeIncident == nil {
		return nil, ErrNoActiveIncident
	}

	inc := st.activeIncident
	if inc.Status.CanTransitionTo(types.StatusRecovered) {
		if err := inc.TransitionTo(types.StatusRecovered, now); err != nil {
			return nil, err
		}
		if e.cfg.Store != nil {
			if err := e.cfg.Store.UpdateIncidentStatus(ctx, inc.IncidentID, inc.Status, inc.UpdatedAt, inc.ResolvedAt); err != nil {
				return nil, fmt.Errorf("failed to persist incident closure: %w", err)
			}
		}
	} else {
		// Non-canonical recovery: do not falsely claim RECOVERED status or automatically inject MITIGATING.
		// Simply update timestamp and clean up active stream state.
		inc.UpdatedAt = now
	}

	st.activeIncident = nil
	st.observations = nil
	return inc, nil
}

// ResolveIncident transitions an active incident to RECOVERED according to canonical FSM rules.
// Direct transition from ANOMALY_DETECTED is rejected with ErrInvalidStateTransition because
// the canonical FSM requires an incident to enter MITIGATING or ESCALATED prior to recovery.
// It never automatically introduces MITIGATING or ESCALATED.
func (e *LocalEngine) ResolveIncident(ctx context.Context, nodeID, metricName string, now time.Time) (*types.Incident, error) {
	return e.TransitionActiveIncident(ctx, nodeID, metricName, types.StatusRecovered, now)
}

// insertObservation adds an observation in chronological order and trims old entries.
func (st *streamState) insertObservation(obs observation, maxN int, windowDuration time.Duration) {
	st.observations = append(st.observations, obs)
	sort.SliceStable(st.observations, func(i, j int) bool {
		return st.observations[i].detectedAt.Before(st.observations[j].detectedAt)
	})

	// Prune entries older than latest - windowDuration
	if len(st.observations) > 0 {
		latest := st.observations[len(st.observations)-1].detectedAt
		cutoff := latest.Add(-windowDuration)
		validIdx := 0
		for i, o := range st.observations {
			if !o.detectedAt.Before(cutoff) {
				validIdx = i
				break
			}
		}
		st.observations = st.observations[validIdx:]
	}

	// Cap at maxN observations
	if len(st.observations) > maxN {
		st.observations = st.observations[len(st.observations)-maxN:]
	}
}

// isActiveStatus returns true if an incident status represents an active, unresolved incident.
func isActiveStatus(s types.IncidentStatus) bool {
	return s == types.StatusAnomalyDetected || s == types.StatusMitigating || s == types.StatusEscalated
}

func copyMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	cp := make(map[string]string, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}

// newUUIDv4 generates an RFC 4122 compliant UUIDv4 using standard crypto/rand.
func newUUIDv4() (string, error) {
	var b [16]byte
	_, err := rand.Read(b[:])
	if err != nil {
		return "", fmt.Errorf("crypto/rand read failed: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // RFC 4122 version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant 1
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4],
		b[4:6],
		b[6:8],
		b[8:10],
		b[10:16],
	), nil
}

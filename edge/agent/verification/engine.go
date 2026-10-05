package verification

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/incident"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// Engine defines the closed-loop incident verification and recovery interface.
type Engine interface {
	// StartVerification initiates closed-loop observation for a successfully executed mitigation.
	StartVerification(ctx context.Context, req VerificationRequest) (*VerificationResult, error)

	// ProcessSample evaluates a subsequent telemetry MetricSample against active verifications.
	// Returns (*VerificationResult, nil) if a verification was evaluated (whether pending or transitioned),
	// or (nil, nil) if no active verification matches the sample.
	ProcessSample(ctx context.Context, sample types.MetricSample) (*VerificationResult, error)

	// GetVerification retrieves a verification by VerificationID.
	GetVerification(ctx context.Context, verificationID string) (*VerificationResult, error)

	// GetActiveVerification retrieves the active PENDING verification for a node and metric stream.
	GetActiveVerification(nodeID, metricName string) *VerificationResult

	// CancelVerification cancels an active verification.
	CancelVerification(ctx context.Context, verificationID, reason string) error

	// FailVerification explicitly marks an active verification as NOT_RECOVERED.
	FailVerification(ctx context.Context, verificationID, reason string) error

	// ReconcileTimeouts evaluates all active verifications and transitions expired ones to TIMED_OUT.
	ReconcileTimeouts(ctx context.Context, now time.Time) (int64, error)

	// RecoverFromStore restores PENDING verifications from SQLite into memory and reconciles offline timeouts.
	RecoverFromStore(ctx context.Context) (int, int, error)

	// HandoffToIncidentEngine checks if the verification is RECOVERED and safely transitions the driving incident
	// through the existing IncidentEngine lifecycle methods if the FSM permits it.
	HandoffToIncidentEngine(ctx context.Context, verificationID string, eng incident.IncidentEngine) (*types.Incident, error)
}

type verificationKey struct {
	IncidentID string
	NodeID     string
	MetricName string
}

type streamKey struct {
	NodeID     string
	MetricName string
}

type verificationState struct {
	id                   string
	key                  verificationKey
	actionID             string
	decisionID           string
	condition            RecoveryCondition
	status               types.VerificationStatus
	requiredObservations int
	consecutiveHealthy   int
	totalObservations    int
	startedAt            time.Time
	expiresAt            time.Time
	completedAt          *time.Time
	recoveredAt          *time.Time
	lastObsAt            *time.Time
	lastObsVal           *float64
	reason               string
	evidence             map[string]string
	seenSamples          map[string]time.Time
}

func (s *verificationState) toResult() *VerificationResult {
	cpEvidence := make(map[string]string, len(s.evidence))
	for k, v := range s.evidence {
		cpEvidence[k] = v
	}
	return &VerificationResult{
		VerificationID:       s.id,
		Status:               s.status,
		IncidentID:           s.key.IncidentID,
		ActionID:             s.actionID,
		DecisionID:           s.decisionID,
		NodeID:               s.key.NodeID,
		MetricName:           s.key.MetricName,
		Observations:         s.totalObservations,
		ConsecutiveHealthy:   s.consecutiveHealthy,
		RequiredObservations: s.requiredObservations,
		StartedAt:            s.startedAt,
		ExpiresAt:            s.expiresAt,
		CompletedAt:          s.completedAt,
		RecoveredAt:          s.recoveredAt,
		Reason:               s.reason,
		Evidence:             cpEvidence,
	}
}

func (s *verificationState) toStored(now time.Time) *storage.StoredVerification {
	return &storage.StoredVerification{
		VerificationID:       s.id,
		IncidentID:           s.key.IncidentID,
		ActionID:             s.actionID,
		DecisionID:           s.decisionID,
		NodeID:               s.key.NodeID,
		MetricName:           s.key.MetricName,
		ConditionType:        string(s.condition.Type),
		RecoveryThreshold:    s.condition.RecoveryThreshold,
		Comparator:           s.condition.Comparator,
		Status:               s.status,
		RequiredObservations: s.requiredObservations,
		ConsecutiveHealthy:   s.consecutiveHealthy,
		TotalObservations:    s.totalObservations,
		StartedAt:            s.startedAt,
		ExpiresAt:            s.expiresAt,
		CompletedAt:          s.completedAt,
		RecoveredAt:          s.recoveredAt,
		LastObservationAt:    s.lastObsAt,
		LastObservedValue:    s.lastObsVal,
		Reason:               s.reason,
		Evidence:             s.evidence,
		CreatedAt:            s.startedAt,
		UpdatedAt:            now,
	}
}

// LocalEngine implements Engine for single-node edge verification.
type LocalEngine struct {
	mu            sync.RWMutex
	cfg           VerificationConfig
	activeStreams map[streamKey]*verificationState
	byID          map[string]*verificationState
}

// NewLocalEngine constructs a new LocalEngine instance.
func NewLocalEngine(cfg VerificationConfig) (*LocalEngine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid verification config: %w", err)
	}
	return &LocalEngine{
		cfg:           cfg,
		activeStreams: make(map[streamKey]*verificationState),
		byID:          make(map[string]*verificationState),
	}, nil
}

// SampleIdentityKey returns the canonical unique identity for a MetricSample.
// The primary identity is sample.SampleID.
// If sample.SampleID is omitted (e.g. synthetic or legacy callers), a fallback key
// based on node, metric, timestamp nanoseconds, and IEEE-754 value representation is used.
func SampleIdentityKey(sample types.MetricSample) string {
	if sID := strings.TrimSpace(sample.SampleID); sID != "" {
		return "sid:" + sID
	}
	// Fallback documentation: Legacy or synthetic samples lacking an explicit SampleID
	// fall back to node:metric:timestamp:bits to prevent unkeyed collision.
	return fmt.Sprintf("fallback:%s:%s:%d:%d", sample.NodeID, sample.Name, sample.Timestamp.UTC().UnixNano(), math.Float64bits(sample.Value))
}

// StartVerification initiates closed-loop observation for a successfully executed mitigation.
func (e *LocalEngine) StartVerification(ctx context.Context, req VerificationRequest) (*VerificationResult, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	sKey := streamKey{
		NodeID:     req.NodeID,
		MetricName: req.Condition.MetricName,
	}

	if existing, exists := e.activeStreams[sKey]; exists && !existing.status.IsTerminal() {
		return nil, fmt.Errorf("%w: node %s metric %s has active verification %s", ErrActiveVerificationExists, req.NodeID, req.Condition.MetricName, existing.id)
	}

	vID, err := e.cfg.IDGenerator()
	if err != nil {
		return nil, fmt.Errorf("failed to generate verification id: %w", err)
	}

	timeout := req.Timeout
	if timeout <= 0 {
		timeout = e.cfg.VerificationTimeout
	}
	if timeout > e.cfg.MaxVerificationTimeout {
		timeout = e.cfg.MaxVerificationTimeout
	}

	requiredObs := req.RequiredObservations
	if requiredObs <= 0 {
		requiredObs = e.cfg.RequiredConsecutiveObservations
	}

	now := e.cfg.Clock.Now()
	expiresAt := req.StartedAt.Add(timeout)

	st := &verificationState{
		id: vID,
		key: verificationKey{
			IncidentID: req.IncidentID,
			NodeID:     req.NodeID,
			MetricName: req.Condition.MetricName,
		},
		actionID:             req.ActionID,
		decisionID:           req.DecisionID,
		condition:            req.Condition,
		status:               types.VerificationStatusPending,
		requiredObservations: requiredObs,
		consecutiveHealthy:   0,
		totalObservations:    0,
		startedAt:            req.StartedAt,
		expiresAt:            expiresAt,
		reason:               "verification initiated; waiting for subsequent telemetry observations",
		evidence:             make(map[string]string),
		seenSamples:          make(map[string]time.Time),
	}

	if e.cfg.Store != nil {
		stored := st.toStored(now)
		if err := e.cfg.Store.RecordVerification(ctx, stored); err != nil {
			return nil, fmt.Errorf("persisting verification record failed: %w", err)
		}
	}

	e.activeStreams[sKey] = st
	e.byID[vID] = st

	return st.toResult(), nil
}

// ProcessSample evaluates a subsequent telemetry MetricSample against active verifications.
func (e *LocalEngine) ProcessSample(ctx context.Context, sample types.MetricSample) (*VerificationResult, error) {
	if strings.TrimSpace(sample.Name) == "" {
		return nil, ErrEmptyMetricName
	}
	if strings.TrimSpace(sample.NodeID) == "" {
		return nil, ErrEmptyNodeID
	}
	if math.IsNaN(sample.Value) || math.IsInf(sample.Value, 0) {
		return nil, ErrNaNMetricValue
	}
	if sample.Timestamp.IsZero() {
		return nil, errors.New("sample timestamp cannot be zero")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	sKey := streamKey{
		NodeID:     sample.NodeID,
		MetricName: sample.Name,
	}

	v, exists := e.activeStreams[sKey]
	if !exists {
		// No active verification for this stream
		return nil, nil
	}

	if v.status.IsTerminal() {
		return nil, ErrVerificationTerminal
	}

	now := e.cfg.Clock.Now()

	// 1. Timeout Check: sample timestamp or current clock after expiresAt
	if sample.Timestamp.After(v.expiresAt) || sample.Timestamp.Equal(v.expiresAt) || now.After(v.expiresAt) {
		v.status = types.VerificationStatusTimedOut
		v.completedAt = &v.expiresAt
		v.reason = "recovery not confirmed within configured verification window"
		delete(e.activeStreams, sKey)

		if e.cfg.Store != nil {
			_ = e.cfg.Store.UpdateVerificationProgress(
				ctx, v.id, v.status, v.consecutiveHealthy, v.totalObservations,
				sample.Value, v.lastObsAt, v.completedAt, nil, v.reason, v.evidence, now,
			)
		}
		return v.toResult(), nil
	}

	// 2. Ordering & Stale Telemetry Check: sample timestamp must not predate verification start
	if sample.Timestamp.Before(v.startedAt) {
		return nil, fmt.Errorf("%w: sample timestamp %s predates verification start %s", ErrStaleTelemetrySample, sample.Timestamp.UTC(), v.startedAt.UTC())
	}

	// 3. Duplicate Telemetry Suppression (Idempotency based on SampleID)
	sID := SampleIdentityKey(sample)
	if _, seen := v.seenSamples[sID]; seen {
		return nil, fmt.Errorf("%w: sample %s already evaluated", ErrDuplicateTelemetrySample, sID)
	}
	v.seenSamples[sID] = sample.Timestamp
	if v.evidence == nil {
		v.evidence = make(map[string]string)
	}
	if seenList, ok := v.evidence["seen_samples"]; ok && seenList != "" {
		v.evidence["seen_samples"] = seenList + "," + sID
	} else {
		v.evidence["seen_samples"] = sID
	}

	// 4. Recovery Condition Evaluation
	healthy, err := v.condition.Evaluate(sample.Value)
	if err != nil {
		return nil, fmt.Errorf("evaluating recovery condition failed: %w", err)
	}

	valCopy := sample.Value
	v.lastObsVal = &valCopy
	tsCopy := sample.Timestamp
	v.lastObsAt = &tsCopy
	v.totalObservations++

	if healthy {
		v.consecutiveHealthy++
		if v.consecutiveHealthy >= v.requiredObservations {
			v.status = types.VerificationStatusRecovered
			v.recoveredAt = &tsCopy
			v.completedAt = &tsCopy
			v.reason = "recovery condition satisfied by subsequent telemetry"

			if v.evidence == nil {
				v.evidence = make(map[string]string)
			}
			v.evidence["metric"] = sample.Name
			v.evidence["required_observations"] = strconv.Itoa(v.requiredObservations)
			v.evidence["healthy_observations"] = strconv.Itoa(v.consecutiveHealthy)
			v.evidence["last_value"] = strconv.FormatFloat(sample.Value, 'f', 4, 64)
			v.evidence["recovery_threshold"] = strconv.FormatFloat(v.condition.RecoveryThreshold, 'f', 4, 64)
			v.evidence["comparator"] = v.condition.Comparator

			delete(e.activeStreams, sKey)
		} else {
			v.reason = fmt.Sprintf("healthy observation %d of %d accepted", v.consecutiveHealthy, v.requiredObservations)
		}
	} else {
		// Unhealthy observation resets the consecutive counter!
		v.consecutiveHealthy = 0
		v.reason = fmt.Sprintf("unhealthy observation %f breached recovery threshold, streak reset", sample.Value)
	}

	if e.cfg.Store != nil {
		_ = e.cfg.Store.UpdateVerificationProgress(
			ctx, v.id, v.status, v.consecutiveHealthy, v.totalObservations,
			sample.Value, v.lastObsAt, v.completedAt, v.recoveredAt, v.reason, v.evidence, now,
		)
	}

	return v.toResult(), nil
}

// GetVerification retrieves a verification by VerificationID.
func (e *LocalEngine) GetVerification(ctx context.Context, verificationID string) (*VerificationResult, error) {
	if strings.TrimSpace(verificationID) == "" {
		return nil, errors.New("verification_id cannot be empty")
	}

	e.mu.RLock()
	st, exists := e.byID[verificationID]
	if exists {
		res := st.toResult()
		e.mu.RUnlock()
		return res, nil
	}
	e.mu.RUnlock()

	if e.cfg.Store != nil {
		stored, err := e.cfg.Store.GetVerification(ctx, verificationID)
		if err != nil {
			return nil, err
		}
		return &VerificationResult{
			VerificationID:       stored.VerificationID,
			Status:               stored.Status,
			IncidentID:           stored.IncidentID,
			ActionID:             stored.ActionID,
			DecisionID:           stored.DecisionID,
			NodeID:               stored.NodeID,
			MetricName:           stored.MetricName,
			Observations:         stored.TotalObservations,
			ConsecutiveHealthy:   stored.ConsecutiveHealthy,
			RequiredObservations: stored.RequiredObservations,
			StartedAt:            stored.StartedAt,
			ExpiresAt:            stored.ExpiresAt,
			CompletedAt:          stored.CompletedAt,
			RecoveredAt:          stored.RecoveredAt,
			Reason:               stored.Reason,
			Evidence:             stored.Evidence,
		}, nil
	}

	return nil, ErrVerificationNotFound
}

// GetActiveVerification retrieves the active PENDING verification for a node and metric stream.
func (e *LocalEngine) GetActiveVerification(nodeID, metricName string) *VerificationResult {
	e.mu.RLock()
	defer e.mu.RUnlock()

	sKey := streamKey{
		NodeID:     nodeID,
		MetricName: metricName,
	}

	v, exists := e.activeStreams[sKey]
	if !exists || v.status.IsTerminal() {
		return nil
	}
	return v.toResult()
}

// CancelVerification cancels an active verification.
func (e *LocalEngine) CancelVerification(ctx context.Context, verificationID, reason string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	v, exists := e.byID[verificationID]
	if !exists {
		return ErrVerificationNotFound
	}
	if v.status.IsTerminal() {
		return fmt.Errorf("%w: verification %s is already terminal (%s)", ErrVerificationTerminal, verificationID, v.status)
	}

	now := e.cfg.Clock.Now()
	v.status = types.VerificationStatusCancelled
	v.completedAt = &now
	v.reason = reason
	if v.reason == "" {
		v.reason = "verification cancelled by operator or caller"
	}

	delete(e.activeStreams, streamKey{NodeID: v.key.NodeID, MetricName: v.key.MetricName})

	if e.cfg.Store != nil {
		_ = e.cfg.Store.UpdateVerificationProgress(
			ctx, v.id, v.status, v.consecutiveHealthy, v.totalObservations,
			0, v.lastObsAt, v.completedAt, nil, v.reason, v.evidence, now,
		)
	}

	return nil
}

// FailVerification explicitly marks an active verification as NOT_RECOVERED.
func (e *LocalEngine) FailVerification(ctx context.Context, verificationID, reason string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	v, exists := e.byID[verificationID]
	if !exists {
		return ErrVerificationNotFound
	}
	if v.status.IsTerminal() {
		return fmt.Errorf("%w: verification %s is already terminal (%s)", ErrVerificationTerminal, verificationID, v.status)
	}

	now := e.cfg.Clock.Now()
	v.status = types.VerificationStatusNotRecovered
	v.completedAt = &now
	v.reason = reason
	if v.reason == "" {
		v.reason = "verification marked not recovered by caller"
	}

	delete(e.activeStreams, streamKey{NodeID: v.key.NodeID, MetricName: v.key.MetricName})

	if e.cfg.Store != nil {
		_ = e.cfg.Store.UpdateVerificationProgress(
			ctx, v.id, v.status, v.consecutiveHealthy, v.totalObservations,
			0, v.lastObsAt, v.completedAt, nil, v.reason, v.evidence, now,
		)
	}

	return nil
}

// ReconcileTimeouts evaluates all active verifications and transitions expired ones to TIMED_OUT.
func (e *LocalEngine) ReconcileTimeouts(ctx context.Context, now time.Time) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	var count int64
	for sKey, v := range e.activeStreams {
		if !v.status.IsTerminal() && (now.After(v.expiresAt) || now.Equal(v.expiresAt)) {
			v.status = types.VerificationStatusTimedOut
			v.completedAt = &v.expiresAt
			v.reason = "verification deadline elapsed without recovery confirmation"
			delete(e.activeStreams, sKey)
			count++

			if e.cfg.Store != nil {
				_ = e.cfg.Store.UpdateVerificationProgress(
					ctx, v.id, v.status, v.consecutiveHealthy, v.totalObservations,
					0, v.lastObsAt, v.completedAt, nil, v.reason, v.evidence, now,
				)
			}
		}
	}

	if e.cfg.Store != nil {
		_, _ = e.cfg.Store.ExpireStaleVerifications(ctx, now)
	}

	return count, nil
}

// RecoverFromStore restores PENDING verifications from SQLite into memory and reconciles offline timeouts.
func (e *LocalEngine) RecoverFromStore(ctx context.Context) (int, int, error) {
	if e.cfg.Store == nil {
		return 0, 0, nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	now := e.cfg.Clock.Now()
	records, err := e.cfg.Store.ListVerifications(ctx, "", 1000)
	if err != nil {
		return 0, 0, fmt.Errorf("listing verifications from store failed: %w", err)
	}

	restoredCount := 0
	timedOutCount := 0

	for _, rec := range records {
		if rec.Status == types.VerificationStatusPending {
			if now.After(rec.ExpiresAt) || now.Equal(rec.ExpiresAt) {
				// Expired during offline period
				completed := rec.ExpiresAt
				_ = e.cfg.Store.UpdateVerificationProgress(
					ctx, rec.VerificationID, types.VerificationStatusTimedOut,
					rec.ConsecutiveHealthy, rec.TotalObservations,
					0, rec.LastObservationAt, &completed, nil,
					"verification expired during offline or shutdown window", rec.Evidence, now,
				)
				timedOutCount++
			} else {
				// Active and valid: restore to memory
				cond := RecoveryCondition{
					Type:              ConditionType(rec.ConditionType),
					MetricName:        rec.MetricName,
					RecoveryThreshold: rec.RecoveryThreshold,
					Comparator:        rec.Comparator,
				}
				ev := make(map[string]string)
				for k, v := range rec.Evidence {
					ev[k] = v
				}
				st := &verificationState{
					id: rec.VerificationID,
					key: verificationKey{
						IncidentID: rec.IncidentID,
						NodeID:     rec.NodeID,
						MetricName: rec.MetricName,
					},
					actionID:             rec.ActionID,
					decisionID:           rec.DecisionID,
					condition:            cond,
					status:               types.VerificationStatusPending,
					requiredObservations: rec.RequiredObservations,
					consecutiveHealthy:   rec.ConsecutiveHealthy,
					totalObservations:    rec.TotalObservations,
					startedAt:            rec.StartedAt,
					expiresAt:            rec.ExpiresAt,
					lastObsAt:            rec.LastObservationAt,
					lastObsVal:           rec.LastObservedValue,
					reason:               rec.Reason,
					evidence:             ev,
					seenSamples:          make(map[string]time.Time),
				}
				if seenList, ok := ev["seen_samples"]; ok && seenList != "" {
					for _, key := range strings.Split(seenList, ",") {
						trimmed := strings.TrimSpace(key)
						if trimmed != "" {
							st.seenSamples[trimmed] = rec.StartedAt
						}
					}
				}
				sKey := streamKey{NodeID: rec.NodeID, MetricName: rec.MetricName}
				e.activeStreams[sKey] = st
				e.byID[rec.VerificationID] = st
				restoredCount++
			}
		}
	}

	return restoredCount, timedOutCount, nil
}

// HandoffToIncidentEngine checks if the verification is RECOVERED and safely transitions the driving incident
// through the existing IncidentEngine lifecycle methods if the FSM permits it.
func (e *LocalEngine) HandoffToIncidentEngine(ctx context.Context, verificationID string, eng incident.IncidentEngine) (*types.Incident, error) {
	if eng == nil {
		return nil, errors.New("incident engine cannot be nil")
	}

	res, err := e.GetVerification(ctx, verificationID)
	if err != nil {
		return nil, err
	}

	if res.Status != types.VerificationStatusRecovered {
		return nil, fmt.Errorf("cannot hand off verification with status %s: only RECOVERED verifications can be handed off", res.Status)
	}

	activeInc := eng.GetActiveIncident(res.NodeID, res.MetricName)
	if activeInc == nil {
		return nil, incident.ErrNoActiveIncident
	}

	if activeInc.IncidentID != res.IncidentID {
		return nil, fmt.Errorf("active incident id %s does not match verification incident id %s", activeInc.IncidentID, res.IncidentID)
	}

	now := e.cfg.Clock.Now()
	if res.RecoveredAt != nil {
		now = *res.RecoveredAt
	}

	// Safety Check: As per contract, verification handoff only transitions the incident if the incident is currently in MITIGATING.
	// If incident status is anything else (ANOMALY_DETECTED, ESCALATED, NORMAL), leave incident unchanged.
	// Never directly assign Incident.Status.
	if activeInc.Status != types.StatusMitigating {
		return activeInc, nil
	}

	return eng.ResolveIncident(ctx, res.NodeID, res.MetricName, now)
}

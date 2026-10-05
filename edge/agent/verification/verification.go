package verification

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/detector"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// Verification domain errors.
var (
	ErrNilVerificationRequest       = errors.New("verification request cannot be nil")
	ErrEmptyIncidentID              = errors.New("incident_id cannot be empty")
	ErrEmptyActionID                = errors.New("action_id cannot be empty")
	ErrEmptyDecisionID              = errors.New("decision_id cannot be empty")
	ErrEmptyNodeID                  = errors.New("node_id cannot be empty")
	ErrEmptyMetricName              = errors.New("metric_name cannot be empty")
	ErrInvalidMitigationStatus      = errors.New("verification requires mitigation status EXECUTED")
	ErrInvalidRecoveryCondition     = errors.New("invalid recovery condition")
	ErrUnsupportedRecoveryCondition = errors.New("unsupported recovery condition type")
	ErrNaNMetricValue               = errors.New("telemetry metric value cannot be NaN or Inf")
	ErrStaleTelemetrySample         = errors.New("telemetry sample predates verification start time")
	ErrDuplicateTelemetrySample     = errors.New("telemetry sample has already been processed for this verification")
	ErrVerificationNotFound         = errors.New("active verification not found")
	ErrActiveVerificationExists     = errors.New("an active verification already exists for this stream")
	ErrVerificationTerminal         = errors.New("verification is in terminal status and cannot accept observations")
	ErrNodeMismatch                 = errors.New("sample node_id does not match verification node_id")
	ErrMetricMismatch               = errors.New("sample metric_name does not match verification metric_name")
)

// ConditionType denotes the deterministic evaluation rule for telemetry recovery.
type ConditionType string

const (
	ConditionThresholdUpper ConditionType = "THRESHOLD_UPPER" // value <= RecoveryThreshold
	ConditionThresholdLower ConditionType = "THRESHOLD_LOWER" // value >= RecoveryThreshold
)

// IsValid checks whether the condition type is recognized.
func (c ConditionType) IsValid() bool {
	return c == ConditionThresholdUpper || c == ConditionThresholdLower
}

// RecoveryCondition specifies the deterministic predicate that subsequent telemetry must satisfy.
type RecoveryCondition struct {
	Type              ConditionType `json:"type"`
	MetricName        string        `json:"metric_name"`
	RecoveryThreshold float64       `json:"recovery_threshold"`
	Comparator        string        `json:"comparator"` // "<=" or ">="
}

// Validate verifies that the RecoveryCondition is structurally and numerically valid.
func (rc RecoveryCondition) Validate() error {
	if strings.TrimSpace(rc.MetricName) == "" {
		return ErrEmptyMetricName
	}
	if !rc.Type.IsValid() {
		return fmt.Errorf("%w: unknown type %q", ErrUnsupportedRecoveryCondition, rc.Type)
	}
	if math.IsNaN(rc.RecoveryThreshold) || math.IsInf(rc.RecoveryThreshold, 0) {
		return fmt.Errorf("%w: recovery threshold must be a finite number", ErrInvalidRecoveryCondition)
	}
	if rc.Type == ConditionThresholdUpper && rc.Comparator != "<=" {
		return fmt.Errorf("%w: THRESHOLD_UPPER requires comparator '<=', got %q", ErrInvalidRecoveryCondition, rc.Comparator)
	}
	if rc.Type == ConditionThresholdLower && rc.Comparator != ">=" {
		return fmt.Errorf("%w: THRESHOLD_LOWER requires comparator '>=', got %q", ErrInvalidRecoveryCondition, rc.Comparator)
	}
	return nil
}

// Evaluate tests whether an observed metric value satisfies the recovery condition.
func (rc RecoveryCondition) Evaluate(val float64) (bool, error) {
	if math.IsNaN(val) || math.IsInf(val, 0) {
		return false, ErrNaNMetricValue
	}
	switch rc.Type {
	case ConditionThresholdUpper:
		return val <= rc.RecoveryThreshold, nil
	case ConditionThresholdLower:
		return val >= rc.RecoveryThreshold, nil
	default:
		return false, ErrUnsupportedRecoveryCondition
	}
}

// NewThresholdUpperCondition creates a condition requiring value <= recoveryThreshold.
func NewThresholdUpperCondition(metricName string, recoveryThreshold float64) RecoveryCondition {
	return RecoveryCondition{
		Type:              ConditionThresholdUpper,
		MetricName:        metricName,
		RecoveryThreshold: recoveryThreshold,
		Comparator:        "<=",
	}
}

// NewThresholdLowerCondition creates a condition requiring value >= recoveryThreshold.
func NewThresholdLowerCondition(metricName string, recoveryThreshold float64) RecoveryCondition {
	return RecoveryCondition{
		Type:              ConditionThresholdLower,
		MetricName:        metricName,
		RecoveryThreshold: recoveryThreshold,
		Comparator:        ">=",
	}
}

// NewConditionFromThresholdRule extracts deterministic recovery thresholds from a detector.ThresholdRule.
// For upper breaches: uses rule.UpperRecoveryThreshold (or rule.UpperThreshold if nil).
// For lower breaches: uses rule.LowerRecoveryThreshold (or rule.LowerThreshold if nil).
func NewConditionFromThresholdRule(rule *detector.ThresholdRule, breachedUpper bool) (RecoveryCondition, error) {
	if rule == nil {
		return RecoveryCondition{}, errors.New("threshold rule cannot be nil")
	}
	if err := rule.Validate(); err != nil {
		return RecoveryCondition{}, fmt.Errorf("invalid threshold rule: %w", err)
	}

	if breachedUpper {
		if rule.UpperThreshold == nil {
			return RecoveryCondition{}, errors.New("threshold rule does not define upper threshold")
		}
		rec := *rule.UpperThreshold
		if rule.UpperRecoveryThreshold != nil {
			rec = *rule.UpperRecoveryThreshold
		}
		return NewThresholdUpperCondition(rule.MetricName, rec), nil
	}

	if rule.LowerThreshold == nil {
		return RecoveryCondition{}, errors.New("threshold rule does not define lower threshold")
	}
	rec := *rule.LowerThreshold
	if rule.LowerRecoveryThreshold != nil {
		rec = *rule.LowerRecoveryThreshold
	}
	return NewThresholdLowerCondition(rule.MetricName, rec), nil
}

// VerificationRequest initiates closed-loop recovery observation following successful mitigation.
type VerificationRequest struct {
	IncidentID           string                 `json:"incident_id"`
	ActionID             string                 `json:"action_id"`
	DecisionID           string                 `json:"decision_id"`
	NodeID               string                 `json:"node_id"`
	MitigationStatus     types.MitigationStatus `json:"mitigation_status"`
	Condition            RecoveryCondition      `json:"condition"`
	StartedAt            time.Time              `json:"started_at"`
	Timeout              time.Duration          `json:"timeout,omitempty"`
	RequiredObservations int                    `json:"required_observations,omitempty"`
}

// Validate verifies the request contract.
func (r *VerificationRequest) Validate() error {
	if r == nil {
		return ErrNilVerificationRequest
	}
	if strings.TrimSpace(r.IncidentID) == "" {
		return ErrEmptyIncidentID
	}
	if strings.TrimSpace(r.ActionID) == "" {
		return ErrEmptyActionID
	}
	if strings.TrimSpace(r.DecisionID) == "" {
		return ErrEmptyDecisionID
	}
	if strings.TrimSpace(r.NodeID) == "" {
		return ErrEmptyNodeID
	}
	if r.MitigationStatus != types.MitigationStatusExecuted {
		return fmt.Errorf("%w: got %s", ErrInvalidMitigationStatus, r.MitigationStatus)
	}
	if err := r.Condition.Validate(); err != nil {
		return err
	}
	if r.StartedAt.IsZero() {
		return errors.New("started_at timestamp cannot be zero")
	}
	return nil
}

// VerificationResult represents the immutable outcome or current snapshot of verification.
type VerificationResult struct {
	VerificationID       string                   `json:"verification_id"`
	Status               types.VerificationStatus `json:"status"`
	IncidentID           string                   `json:"incident_id"`
	ActionID             string                   `json:"action_id"`
	DecisionID           string                   `json:"decision_id"`
	NodeID               string                   `json:"node_id"`
	MetricName           string                   `json:"metric_name"`
	Observations         int                      `json:"observations"`
	ConsecutiveHealthy   int                      `json:"consecutive_healthy"`
	RequiredObservations int                      `json:"required_observations"`
	StartedAt            time.Time                `json:"started_at"`
	ExpiresAt            time.Time                `json:"expires_at"`
	CompletedAt          *time.Time               `json:"completed_at,omitempty"`
	RecoveredAt          *time.Time               `json:"recovered_at,omitempty"`
	Reason               string                   `json:"reason"`
	Evidence             map[string]string        `json:"evidence,omitempty"`
}

// AuditEvent represents a structured observability event emitted during verification.
type AuditEvent struct {
	EventType      string                   `json:"event_type"`
	VerificationID string                   `json:"verification_id"`
	IncidentID     string                   `json:"incident_id"`
	NodeID         string                   `json:"node_id"`
	MetricName     string                   `json:"metric_name"`
	Status         types.VerificationStatus `json:"status"`
	Timestamp      time.Time                `json:"timestamp"`
	Detail         string                   `json:"detail"`
}

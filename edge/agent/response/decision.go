package response

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// Common response decision domain errors.
var (
	ErrEmptyDecisionID  = errors.New("decision id cannot be empty")
	ErrEmptyIncidentID  = errors.New("incident id cannot be empty")
	ErrEmptyNodeID      = errors.New("node id cannot be empty")
	ErrEmptyTarget      = errors.New("action target cannot be empty")
	ErrInvalidTarget    = errors.New("action target is invalid or prohibited")
	ErrEmptyPolicyName  = errors.New("policy name cannot be empty")
	ErrInvalidPolicyVer = errors.New("policy version cannot be empty or invalid")
	ErrInvalidExecMode  = errors.New("invalid execution mode")
	ErrInvalidAuthClass = errors.New("invalid or empty authorization class")
	ErrZeroTimestamp    = errors.New("decision timestamp cannot be zero")
	ErrDecisionMismatch = errors.New("decision id does not match deterministic calculation")
)

// ExecutionMode designates the authorization and simulation scope of a proposed decision.
type ExecutionMode string

const (
	// ExecutionModeDryRun performs full policy evaluation and simulation without host mutations.
	ExecutionModeDryRun ExecutionMode = "DRY_RUN"

	// ExecutionModeAutoExecute authorizes automated execution of safe, allowlisted actions.
	ExecutionModeAutoExecute ExecutionMode = "AUTO_EXECUTE"

	// ExecutionModeApprovalRequired blocks execution until human operator authorization is recorded.
	ExecutionModeApprovalRequired ExecutionMode = "APPROVAL_REQUIRED"
)

// IsValid checks whether the execution mode is recognized.
func (m ExecutionMode) IsValid() bool {
	switch m {
	case ExecutionModeDryRun, ExecutionModeAutoExecute, ExecutionModeApprovalRequired:
		return true
	default:
		return false
	}
}

// AuthorizationClass defines the safety and authorization tier of a remediation action.
type AuthorizationClass string

const (
	// AuthClassAutoExecute authorizes automated execution when all safety constraints are satisfied.
	AuthClassAutoExecute AuthorizationClass = "AUTO_EXECUTE"

	// AuthClassApprovalRequired requires explicit human operator authorization before execution.
	// In Phase 6.2, actions requiring approval are never automatically authorized.
	AuthClassApprovalRequired AuthorizationClass = "APPROVAL_REQUIRED"

	// AuthClassForbidden marks an action that is permanently prohibited from execution on the edge host.
	AuthClassForbidden AuthorizationClass = "FORBIDDEN"
)

// IsValid checks whether the authorization class is recognized.
func (a AuthorizationClass) IsValid() bool {
	switch a {
	case AuthClassAutoExecute, AuthClassApprovalRequired, AuthClassForbidden:
		return true
	default:
		return false
	}
}

// ResponseDecision represents a candidate remediation decision produced by a deterministic ResponsePolicy.
// Note: It contains NO arbitrary command strings or shell payloads.
type ResponseDecision struct {
	DecisionID         string                     `json:"decision_id"`
	IncidentID         string                     `json:"incident_id"`
	NodeID             string                     `json:"node_id"`
	RuleID             string                     `json:"rule_id,omitempty"`
	ActionType         types.MitigationActionType `json:"action_type"`
	Target             string                     `json:"target"`
	Parameters         map[string]string          `json:"parameters,omitempty"`
	AuthorizationClass AuthorizationClass         `json:"authorization_class"`
	Reason             string                     `json:"reason"`
	PolicyName         string                     `json:"policy_name"`
	PolicyVersion      string                     `json:"policy_version"`
	ExecutionMode      ExecutionMode              `json:"execution_mode"`
	DecisionTimestamp  time.Time                  `json:"decision_timestamp"`
	EvidenceSummary    map[string]string          `json:"evidence_summary,omitempty"`
}

// ComputeDecisionID generates a stable, deterministic DecisionID using SHA-256 over key decision fields.
// This guarantees that evaluating the same incident under identical policy rules yields an identical identity.
func ComputeDecisionID(incidentID, policyName, policyVersion string, actionType types.MitigationActionType, target string) string {
	h := sha256.New()
	_, _ = h.Write([]byte(fmt.Sprintf("%s:%s:%s:%s:%s",
		strings.TrimSpace(incidentID),
		strings.TrimSpace(policyName),
		strings.TrimSpace(policyVersion),
		strings.TrimSpace(string(actionType)),
		strings.TrimSpace(target),
	)))
	return fmt.Sprintf("dec-%x", h.Sum(nil)[:12])
}

// Validate verifies the structural integrity of the ResponseDecision.
func (d *ResponseDecision) Validate() error {
	if strings.TrimSpace(d.DecisionID) == "" {
		return ErrEmptyDecisionID
	}
	if strings.TrimSpace(d.IncidentID) == "" {
		return ErrEmptyIncidentID
	}
	if strings.TrimSpace(d.NodeID) == "" {
		return ErrEmptyNodeID
	}
	if !d.ActionType.IsValid() || !IsAllowlisted(d.ActionType) {
		return ErrActionNotAllowlisted
	}
	if strings.TrimSpace(d.Target) == "" {
		return ErrEmptyTarget
	}
	if !IsValidTarget(d.Target) {
		return ErrInvalidTarget
	}
	if !d.AuthorizationClass.IsValid() {
		return ErrInvalidAuthClass
	}
	if strings.TrimSpace(d.PolicyName) == "" {
		return ErrEmptyPolicyName
	}
	if strings.TrimSpace(d.PolicyVersion) == "" {
		return ErrInvalidPolicyVer
	}
	if !d.ExecutionMode.IsValid() {
		return ErrInvalidExecMode
	}
	if d.DecisionTimestamp.IsZero() {
		return ErrZeroTimestamp
	}

	// Verify that DecisionID matches deterministic derivation
	expectedID := ComputeDecisionID(d.IncidentID, d.PolicyName, d.PolicyVersion, d.ActionType, d.Target)
	if d.DecisionID != expectedID {
		return fmt.Errorf("%w: got %s, want %s", ErrDecisionMismatch, d.DecisionID, expectedID)
	}

	return nil
}

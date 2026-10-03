package response

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// Common approval domain errors.
var (
	ErrApprovalNotApproved       = errors.New("approval is not in APPROVED status")
	ErrApprovalAlreadyConsumed   = errors.New("approval has already been consumed")
	ErrApprovalExpired           = errors.New("approval has expired")
	ErrApprovalRejected          = errors.New("approval has been rejected")
	ErrApprovalCancelled         = errors.New("approval has been cancelled")
	ErrApprovalDecisionMismatch  = errors.New("approval decision binding mismatch")
	ErrForbiddenNotOverridable   = errors.New("forbidden actions cannot be approved or overridden")
	ErrCannotRejectApproved      = errors.New("cannot reject an already approved approval")
	ErrNilApprovalStore          = errors.New("approval store cannot be nil")
	ErrEmptyRequestedBy          = errors.New("requested_by cannot be empty")
	ErrInvalidTTL                = errors.New("approval TTL must be positive")
	ErrApprovalNotFound          = storage.ErrApprovalNotFound
	ErrDuplicateApproval         = storage.ErrDuplicateApproval
	ErrInvalidApprovalTransition = storage.ErrInvalidApprovalTransition
)

// Approval status aliases from shared types.
const (
	ApprovalStatusPending   = types.ApprovalStatusPending
	ApprovalStatusApproved  = types.ApprovalStatusApproved
	ApprovalStatusConsumed  = types.ApprovalStatusConsumed
	ApprovalStatusRejected  = types.ApprovalStatusRejected
	ApprovalStatusExpired   = types.ApprovalStatusExpired
	ApprovalStatusCancelled = types.ApprovalStatusCancelled
)

// Clock abstracts system time to ensure 100% deterministic testing.
type Clock interface {
	Now() time.Time
}

// RealClock returns the current UTC time.
type RealClock struct{}

func (RealClock) Now() time.Time {
	return time.Now().UTC()
}

// OperatorApproval represents a strongly typed, decision-bound operator authorization record.
//
// OPERATOR IDENTITY SPECIFICATION (Phase 6.5):
// Operator identity is represented as application-level metadata in Phase 6.5. Strong authentication,
// cryptographic authorization, identity federation, and signed approval tokens are future work.
// Zero OAuth, JWT, SSO, or certificates are implemented in this phase.
type OperatorApproval struct {
	ApprovalID          string                     `json:"approval_id"`
	DecisionID          string                     `json:"decision_id"`
	IncidentID          string                     `json:"incident_id"`
	ActionID            string                     `json:"action_id"`
	NodeID              string                     `json:"node_id"`
	ActionType          types.MitigationActionType `json:"action_type"`
	Target              string                     `json:"target"`
	PolicyVersion       string                     `json:"policy_version"`
	DecisionFingerprint string                     `json:"decision_fingerprint"`
	Status              types.ApprovalStatus       `json:"status"`
	RequestedAt         time.Time                  `json:"requested_at"`
	ExpiresAt           time.Time                  `json:"expires_at"`
	RequestedBy         string                     `json:"requested_by"`
	ApprovedBy          string                     `json:"approved_by,omitempty"`
	RejectedBy          string                     `json:"rejected_by,omitempty"`
	Reason              string                     `json:"reason,omitempty"`
	CreatedAt           time.Time                  `json:"created_at"`
	UpdatedAt           time.Time                  `json:"updated_at"`
	ConsumedAt          *time.Time                 `json:"consumed_at,omitempty"`
}

// ComputeDecisionFingerprint computes a deterministic SHA-256 fingerprint over the decision's core parameters.
// This guarantees that any alteration to the target, action, node, incident, policy version, or parameters
// will invalidate the approval binding and fail closed.
func ComputeDecisionFingerprint(dec *ResponseDecision) string {
	if dec == nil {
		return ""
	}

	actionID := fmt.Sprintf("act-%s", dec.DecisionID[4:])
	if !strings.HasPrefix(dec.DecisionID, "dec-") {
		actionID = fmt.Sprintf("act-%s", dec.DecisionID)
	}

	h := sha256.New()

	// Write static identity fields
	_, _ = h.Write([]byte(fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s|",
		strings.TrimSpace(dec.DecisionID),
		strings.TrimSpace(dec.IncidentID),
		actionID,
		strings.TrimSpace(dec.NodeID),
		strings.TrimSpace(string(dec.ActionType)),
		strings.TrimSpace(dec.Target),
		strings.TrimSpace(dec.PolicyVersion),
		strings.TrimSpace(string(dec.AuthorizationClass)),
	)))

	// Write sorted parameters to ensure deterministic hashing
	keys := make([]string, 0, len(dec.Parameters))
	for k := range dec.Parameters {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		_, _ = h.Write([]byte(fmt.Sprintf("%s=%s;", k, dec.Parameters[k])))
	}

	return fmt.Sprintf("fp-%x", h.Sum(nil)[:16])
}

// ApprovalRequest provides parameters for requesting an operator approval.
type ApprovalRequest struct {
	Decision    *ResponseDecision
	RequestedBy string
	TTL         time.Duration
	Reason      string
}

// NewApprovalFromRequest constructs a new OperatorApproval bound to the candidate decision.
// Fails closed if the decision is forbidden, missing, or invalid.
func NewApprovalFromRequest(req ApprovalRequest, now time.Time) (*OperatorApproval, error) {
	dec := req.Decision
	if dec == nil {
		return nil, ErrNilDecision
	}
	if err := dec.Validate(); err != nil {
		return nil, fmt.Errorf("cannot create approval for invalid decision: %w", err)
	}

	// CRITICAL SAFETY INVARIANT: Operator approval can NEVER authorize a FORBIDDEN action.
	if dec.AuthorizationClass == AuthClassForbidden {
		return nil, ErrForbiddenNotOverridable
	}

	if strings.TrimSpace(req.RequestedBy) == "" {
		return nil, ErrEmptyRequestedBy
	}

	ttl := req.TTL
	if ttl <= 0 {
		ttl = 15 * time.Minute // default safe expiration window
	}

	actionID := fmt.Sprintf("act-%s", dec.DecisionID[4:])
	if !strings.HasPrefix(dec.DecisionID, "dec-") {
		actionID = fmt.Sprintf("act-%s", dec.DecisionID)
	}

	approvalID := fmt.Sprintf("app-%s", dec.DecisionID[4:])
	if !strings.HasPrefix(dec.DecisionID, "dec-") {
		approvalID = fmt.Sprintf("app-%s", dec.DecisionID)
	}

	fingerprint := ComputeDecisionFingerprint(dec)
	expiresAt := now.Add(ttl)

	return &OperatorApproval{
		ApprovalID:          approvalID,
		DecisionID:          dec.DecisionID,
		IncidentID:          dec.IncidentID,
		ActionID:            actionID,
		NodeID:              dec.NodeID,
		ActionType:          dec.ActionType,
		Target:              dec.Target,
		PolicyVersion:       dec.PolicyVersion,
		DecisionFingerprint: fingerprint,
		Status:              ApprovalStatusPending,
		RequestedAt:         now,
		ExpiresAt:           expiresAt,
		RequestedBy:         req.RequestedBy,
		Reason:              req.Reason,
		CreatedAt:           now,
		UpdatedAt:           now,
	}, nil
}

// ToStored converts OperatorApproval to the durable storage StoredApproval.
func (a *OperatorApproval) ToStored() *storage.StoredApproval {
	return &storage.StoredApproval{
		ApprovalID:          a.ApprovalID,
		DecisionID:          a.DecisionID,
		IncidentID:          a.IncidentID,
		ActionID:            a.ActionID,
		NodeID:              a.NodeID,
		ActionType:          a.ActionType,
		Target:              a.Target,
		PolicyVersion:       a.PolicyVersion,
		DecisionFingerprint: a.DecisionFingerprint,
		Status:              a.Status,
		RequestedAt:         a.RequestedAt,
		ExpiresAt:           a.ExpiresAt,
		RequestedBy:         a.RequestedBy,
		ApprovedBy:          a.ApprovedBy,
		RejectedBy:          a.RejectedBy,
		Reason:              a.Reason,
		CreatedAt:           a.CreatedAt,
		UpdatedAt:           a.UpdatedAt,
		ConsumedAt:          a.ConsumedAt,
	}
}

// StoredToApproval converts a durable storage record into domain OperatorApproval.
func StoredToApproval(s *storage.StoredApproval) *OperatorApproval {
	if s == nil {
		return nil
	}
	return &OperatorApproval{
		ApprovalID:          s.ApprovalID,
		DecisionID:          s.DecisionID,
		IncidentID:          s.IncidentID,
		ActionID:            s.ActionID,
		NodeID:              s.NodeID,
		ActionType:          s.ActionType,
		Target:              s.Target,
		PolicyVersion:       s.PolicyVersion,
		DecisionFingerprint: s.DecisionFingerprint,
		Status:              s.Status,
		RequestedAt:         s.RequestedAt,
		ExpiresAt:           s.ExpiresAt,
		RequestedBy:         s.RequestedBy,
		ApprovedBy:          s.ApprovedBy,
		RejectedBy:          s.RejectedBy,
		Reason:              s.Reason,
		CreatedAt:           s.CreatedAt,
		UpdatedAt:           s.UpdatedAt,
		ConsumedAt:          s.ConsumedAt,
	}
}

// ValidateApprovalForExecution performs the execution-time 14-point security validation gate.
// It fails closed if ANY check fails.
func ValidateApprovalForExecution(app *OperatorApproval, dec *ResponseDecision, inc *types.Incident, now time.Time) error {
	if app == nil {
		return errors.New("approval record is nil")
	}
	if dec == nil {
		return ErrNilDecision
	}
	if inc == nil {
		return ErrNilIncident
	}

	// 1. Status Check
	switch app.Status {
	case ApprovalStatusApproved:
		// Required state to proceed
	case ApprovalStatusPending:
		return ErrApprovalNotApproved
	case ApprovalStatusConsumed:
		return ErrApprovalAlreadyConsumed
	case ApprovalStatusRejected:
		return ErrApprovalRejected
	case ApprovalStatusCancelled:
		return ErrApprovalCancelled
	case ApprovalStatusExpired:
		return ErrApprovalExpired
	default:
		return fmt.Errorf("%w: unknown status %s", ErrApprovalNotApproved, app.Status)
	}

	// 2. Expiration Check (Evaluated at execution time)
	if now.After(app.ExpiresAt) || now.Equal(app.ExpiresAt) {
		return ErrApprovalExpired
	}

	// 3. Replay Protection: Ensure approval has not already been consumed
	if app.ConsumedAt != nil {
		return ErrApprovalAlreadyConsumed
	}

	// 4. Incident Binding Check
	if app.IncidentID != dec.IncidentID {
		return fmt.Errorf("%w: approval incident_id %q != decision incident_id %q", ErrApprovalDecisionMismatch, app.IncidentID, dec.IncidentID)
	}
	if dec.IncidentID != inc.IncidentID {
		return fmt.Errorf("%w: decision incident_id %q != incident incident_id %q", ErrApprovalDecisionMismatch, dec.IncidentID, inc.IncidentID)
	}

	// 5. DecisionID Binding Check
	if app.DecisionID != dec.DecisionID {
		return fmt.Errorf("%w: approval decision_id %q != decision decision_id %q", ErrApprovalDecisionMismatch, app.DecisionID, dec.DecisionID)
	}

	// 6. ActionID Binding Check
	expectedActionID := fmt.Sprintf("act-%s", dec.DecisionID[4:])
	if !strings.HasPrefix(dec.DecisionID, "dec-") {
		expectedActionID = fmt.Sprintf("act-%s", dec.DecisionID)
	}
	if app.ActionID != expectedActionID {
		return fmt.Errorf("%w: approval action_id %q != expected action_id %q", ErrApprovalDecisionMismatch, app.ActionID, expectedActionID)
	}

	// 7. NodeID Binding Check
	if app.NodeID != dec.NodeID {
		return fmt.Errorf("%w: approval node_id %q != decision node_id %q", ErrApprovalDecisionMismatch, app.NodeID, dec.NodeID)
	}
	if dec.NodeID != inc.NodeID {
		return fmt.Errorf("%w: decision node_id %q != incident node_id %q", ErrApprovalDecisionMismatch, dec.NodeID, inc.NodeID)
	}

	// 8. ActionType Binding Check
	if app.ActionType != dec.ActionType {
		return fmt.Errorf("%w: approval action_type %q != decision action_type %q", ErrApprovalDecisionMismatch, app.ActionType, dec.ActionType)
	}

	// 9. Target Binding Check
	if app.Target != dec.Target {
		return fmt.Errorf("%w: approval target %q != decision target %q", ErrApprovalDecisionMismatch, app.Target, dec.Target)
	}

	// 10. PolicyVersion Binding Check
	if app.PolicyVersion != dec.PolicyVersion {
		return fmt.Errorf("%w: approval policy_version %q != decision policy_version %q", ErrApprovalDecisionMismatch, app.PolicyVersion, dec.PolicyVersion)
	}

	// 11. Decision Fingerprint Verification
	currentFingerprint := ComputeDecisionFingerprint(dec)
	if app.DecisionFingerprint != currentFingerprint {
		return fmt.Errorf("%w: decision fingerprint mismatch (got %s, want %s)", ErrApprovalDecisionMismatch, currentFingerprint, app.DecisionFingerprint)
	}

	// 12. Authorization Class Verification: MUST be APPROVAL_REQUIRED
	if dec.AuthorizationClass != AuthClassApprovalRequired {
		return fmt.Errorf("approval validation only applies to APPROVAL_REQUIRED decisions, got %s", dec.AuthorizationClass)
	}

	// 13. Safety Perimeter: FORBIDDEN actions can NEVER be authorized
	if dec.AuthorizationClass == AuthClassForbidden {
		return ErrForbiddenNotOverridable
	}

	// 14. Target & Parameter Allowlist Verification
	if !IsAllowlisted(dec.ActionType) {
		return fmt.Errorf("action type %s is not allowlisted", dec.ActionType)
	}
	if !IsValidTarget(dec.Target) {
		return fmt.Errorf("target %s is invalid", dec.Target)
	}

	return nil
}

// ApprovalManager coordinates operator approval lifecycles, durable persistence, and execution-time validation.
type ApprovalManager struct {
	store storage.ApprovalStore
	clock Clock
}

// NewApprovalManager constructs a new ApprovalManager.
func NewApprovalManager(store storage.ApprovalStore, clock Clock) *ApprovalManager {
	if clock == nil {
		clock = RealClock{}
	}
	return &ApprovalManager{
		store: store,
		clock: clock,
	}
}

// RequestApproval creates and persists a new operator approval in PENDING status.
func (m *ApprovalManager) RequestApproval(ctx context.Context, req ApprovalRequest) (*OperatorApproval, error) {
	if m.store == nil {
		return nil, ErrNilApprovalStore
	}

	now := m.clock.Now()
	app, err := NewApprovalFromRequest(req, now)
	if err != nil {
		return nil, err
	}

	if err := m.store.RecordApproval(ctx, app.ToStored()); err != nil {
		return nil, err
	}

	return app, nil
}

// Approve records human operator approval, transitioning PENDING -> APPROVED.
func (m *ApprovalManager) Approve(ctx context.Context, approvalID, approvedBy string) error {
	if m.store == nil {
		return ErrNilApprovalStore
	}
	now := m.clock.Now()
	return m.store.ApproveApproval(ctx, approvalID, approvedBy, now)
}

// Reject records operator rejection, transitioning PENDING -> REJECTED.
func (m *ApprovalManager) Reject(ctx context.Context, approvalID, rejectedBy, reason string) error {
	if m.store == nil {
		return ErrNilApprovalStore
	}
	now := m.clock.Now()
	return m.store.RejectApproval(ctx, approvalID, rejectedBy, reason, now)
}

// Cancel cancels a pending or approved approval, transitioning to CANCELLED.
func (m *ApprovalManager) Cancel(ctx context.Context, approvalID, reason string) error {
	if m.store == nil {
		return ErrNilApprovalStore
	}
	now := m.clock.Now()
	return m.store.CancelApproval(ctx, approvalID, reason, now)
}

// GetApproval retrieves an approval record by ApprovalID.
func (m *ApprovalManager) GetApproval(ctx context.Context, approvalID string) (*OperatorApproval, error) {
	if m.store == nil {
		return nil, ErrNilApprovalStore
	}
	stored, err := m.store.GetApproval(ctx, approvalID)
	if err != nil {
		return nil, err
	}
	return StoredToApproval(stored), nil
}

// GetApprovalByDecisionID retrieves an approval record by DecisionID.
func (m *ApprovalManager) GetApprovalByDecisionID(ctx context.Context, decisionID string) (*OperatorApproval, error) {
	if m.store == nil {
		return nil, ErrNilApprovalStore
	}
	stored, err := m.store.GetApprovalByDecisionID(ctx, decisionID)
	if err != nil {
		return nil, err
	}
	return StoredToApproval(stored), nil
}

// ValidateForExecution loads the approval from durable storage and validates all 14 execution constraints.
func (m *ApprovalManager) ValidateForExecution(ctx context.Context, approvalID string, dec *ResponseDecision, inc *types.Incident) (*OperatorApproval, error) {
	if m.store == nil {
		return nil, ErrNilApprovalStore
	}

	stored, err := m.store.GetApproval(ctx, approvalID)
	if err != nil {
		return nil, err
	}

	app := StoredToApproval(stored)
	now := m.clock.Now()
	if err := ValidateApprovalForExecution(app, dec, inc, now); err != nil {
		return nil, err
	}

	return app, nil
}

// ConsumeApproval atomically consumes the approved approval record upon successful action dispatch.
func (m *ApprovalManager) ConsumeApproval(ctx context.Context, approvalID string) error {
	if m.store == nil {
		return ErrNilApprovalStore
	}
	now := m.clock.Now()
	return m.store.ConsumeApproval(ctx, approvalID, now)
}

// ExpireStaleApprovals expires any active approvals past their TTL.
func (m *ApprovalManager) ExpireStaleApprovals(ctx context.Context) (int64, error) {
	if m.store == nil {
		return 0, ErrNilApprovalStore
	}
	now := m.clock.Now()
	return m.store.ExpireStaleApprovals(ctx, now)
}

// ============================================================================
// Observability / Audit Events (Section 20)
// ============================================================================

// ApprovalEventType identifies observable operator approval lifecycle milestones.
type ApprovalEventType string

const (
	ApprovalEventRequested        ApprovalEventType = "APPROVAL_REQUESTED"
	ApprovalEventApproved         ApprovalEventType = "APPROVAL_APPROVED"
	ApprovalEventRejected         ApprovalEventType = "APPROVAL_REJECTED"
	ApprovalEventCancelled        ApprovalEventType = "APPROVAL_CANCELLED"
	ApprovalEventExpired          ApprovalEventType = "APPROVAL_EXPIRED"
	ApprovalEventConsumed         ApprovalEventType = "APPROVAL_CONSUMED"
	ApprovalEventValidationFailed ApprovalEventType = "APPROVAL_VALIDATION_FAILED"
)

// ApprovalAuditEvent documents an auditable operator action or state transition.
type ApprovalAuditEvent struct {
	EventType  ApprovalEventType `json:"event_type"`
	ApprovalID string            `json:"approval_id"`
	DecisionID string            `json:"decision_id"`
	IncidentID string            `json:"incident_id"`
	ActionID   string            `json:"action_id"`
	Actor      string            `json:"actor"`
	Details    string            `json:"details"`
	Timestamp  time.Time         `json:"timestamp"`
}

// NewApprovalAuditEvent constructs a structured audit event.
func NewApprovalAuditEvent(eventType ApprovalEventType, app *OperatorApproval, actor, details string, now time.Time) ApprovalAuditEvent {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	event := ApprovalAuditEvent{
		EventType: eventType,
		Actor:     actor,
		Details:   details,
		Timestamp: now,
	}
	if app != nil {
		event.ApprovalID = app.ApprovalID
		event.DecisionID = app.DecisionID
		event.IncidentID = app.IncidentID
		event.ActionID = app.ActionID
	}
	return event
}

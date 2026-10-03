package response_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/response"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// FakeClock provides deterministic time control for tests.
type FakeClock struct {
	current time.Time
}

func NewFakeClock(t time.Time) *FakeClock {
	return &FakeClock{current: t}
}

func (c *FakeClock) Now() time.Time {
	return c.current
}

func (c *FakeClock) Advance(d time.Duration) {
	c.current = c.current.Add(d)
}

func makeApprovalTestDecision(actionType types.MitigationActionType, target string, authClass response.AuthorizationClass) *response.ResponseDecision {
	now := time.Now().UTC()
	incidentID := "inc-test-001"
	policyName := "default_edge_response_policy"
	policyVer := "1.0.0"
	decID := response.ComputeDecisionID(incidentID, policyName, policyVer, actionType, target)

	return &response.ResponseDecision{
		DecisionID:         decID,
		IncidentID:         incidentID,
		NodeID:             "node-01",
		RuleID:             "rule-cpu-high",
		ActionType:         actionType,
		Target:             target,
		Parameters:         map[string]string{"throttle_percent": "50", "duration_sec": "60"},
		AuthorizationClass: authClass,
		Reason:             "testing operator approval boundary",
		PolicyName:         policyName,
		PolicyVersion:      policyVer,
		ExecutionMode:      response.ExecutionModeApprovalRequired,
		DecisionTimestamp:  now,
	}
}

func makeApprovalTestIncident(incidentID, nodeID string, status types.IncidentStatus) *types.Incident {
	now := time.Now().UTC()
	return &types.Incident{
		IncidentID:    incidentID,
		NodeID:        nodeID,
		RuleName:      "CpuSpike",
		Severity:      types.SeverityHigh,
		Status:        status,
		Description:   "CPU usage threshold breach",
		TriggerMetric: "cpu_usage_percent",
		TriggerValue:  92.5,
		Threshold:     80.0,
		TriggeredAt:   now,
		UpdatedAt:     now,
	}
}

// TEST: Decision Fingerprint computation determinism and sensitivity
func TestDecisionFingerprint_DeterminismAndSensitivity(t *testing.T) {
	dec1 := makeApprovalTestDecision(types.ActionSimulatedThrottle, "telemetry_generator", response.AuthClassApprovalRequired)
	fp1 := response.ComputeDecisionFingerprint(dec1)

	// Byte-for-byte determinism
	fp2 := response.ComputeDecisionFingerprint(dec1)
	if fp1 != fp2 {
		t.Fatalf("expected identical fingerprints, got %s vs %s", fp1, fp2)
	}

	// Sensitivity: Altering parameter value changes fingerprint
	decModifiedParam := makeApprovalTestDecision(types.ActionSimulatedThrottle, "telemetry_generator", response.AuthClassApprovalRequired)
	decModifiedParam.Parameters["throttle_percent"] = "75"
	fpParam := response.ComputeDecisionFingerprint(decModifiedParam)
	if fp1 == fpParam {
		t.Fatalf("fingerprint did not change when parameter was modified")
	}

	// Sensitivity: Altering target changes fingerprint
	decModifiedTarget := makeApprovalTestDecision(types.ActionSimulatedThrottle, "database_worker", response.AuthClassApprovalRequired)
	fpTarget := response.ComputeDecisionFingerprint(decModifiedTarget)
	if fp1 == fpTarget {
		t.Fatalf("fingerprint did not change when target was modified")
	}

	// Sensitivity: Nil decision returns empty fingerprint
	if response.ComputeDecisionFingerprint(nil) != "" {
		t.Fatalf("expected empty fingerprint for nil decision")
	}
}

// TEST: NewApprovalFromRequest validation and safety constraints
func TestNewApprovalFromRequest_SafetyConstraints(t *testing.T) {
	now := time.Now().UTC()
	dec := makeApprovalTestDecision(types.ActionSimulatedThrottle, "telemetry_generator", response.AuthClassApprovalRequired)

	// Valid request
	req := response.ApprovalRequest{
		Decision:    dec,
		RequestedBy: "operator_lead",
		TTL:         10 * time.Minute,
		Reason:      "manual throttle during incident",
	}
	app, err := response.NewApprovalFromRequest(req, now)
	if err != nil {
		t.Fatalf("NewApprovalFromRequest failed: %v", err)
	}
	if app.Status != response.ApprovalStatusPending {
		t.Fatalf("expected status PENDING, got %s", app.Status)
	}
	if app.DecisionID != dec.DecisionID || app.IncidentID != dec.IncidentID {
		t.Fatalf("mismatched decision binding in approval")
	}

	// CRITICAL SAFETY INVARIANT: FORBIDDEN action can NEVER receive an approval request
	forbiddenDec := makeApprovalTestDecision(types.ActionSimulatedRestart, "auth_service", response.AuthClassForbidden)
	forbiddenReq := response.ApprovalRequest{
		Decision:    forbiddenDec,
		RequestedBy: "rogue_operator",
		TTL:         10 * time.Minute,
	}
	_, err = response.NewApprovalFromRequest(forbiddenReq, now)
	if !errors.Is(err, response.ErrForbiddenNotOverridable) {
		t.Fatalf("expected ErrForbiddenNotOverridable for forbidden action, got %v", err)
	}

	// Empty requested_by rejected
	reqNoRequester := response.ApprovalRequest{
		Decision: dec,
	}
	_, err = response.NewApprovalFromRequest(reqNoRequester, now)
	if !errors.Is(err, response.ErrEmptyRequestedBy) {
		t.Fatalf("expected ErrEmptyRequestedBy, got %v", err)
	}
}

// TEST: ValidateApprovalForExecution 14-Point Security Validation Gate
func TestValidateApprovalForExecution_14PointSecurityGate(t *testing.T) {
	now := time.Now().UTC()
	dec := makeApprovalTestDecision(types.ActionSimulatedThrottle, "telemetry_generator", response.AuthClassApprovalRequired)
	inc := makeApprovalTestIncident(dec.IncidentID, dec.NodeID, types.StatusAnomalyDetected)

	req := response.ApprovalRequest{
		Decision:    dec,
		RequestedBy: "operator_lead",
		TTL:         10 * time.Minute,
	}
	baseApp, err := response.NewApprovalFromRequest(req, now)
	if err != nil {
		t.Fatalf("NewApprovalFromRequest failed: %v", err)
	}

	// 1. Pending approval must fail
	if err := response.ValidateApprovalForExecution(baseApp, dec, inc, now); !errors.Is(err, response.ErrApprovalNotApproved) {
		t.Fatalf("expected ErrApprovalNotApproved for PENDING status, got %v", err)
	}

	// Approve it for subsequent tests
	approvedApp := *baseApp
	approvedApp.Status = response.ApprovalStatusApproved
	approvedApp.ApprovedBy = "operator_alice"

	// 2. Valid approved approval MUST PASS
	if err := response.ValidateApprovalForExecution(&approvedApp, dec, inc, now); err != nil {
		t.Fatalf("expected valid approval to pass validation, got %v", err)
	}

	// 3. Expired approval: now >= ExpiresAt must fail
	futureTime := approvedApp.ExpiresAt.Add(time.Second)
	if err := response.ValidateApprovalForExecution(&approvedApp, dec, inc, futureTime); !errors.Is(err, response.ErrApprovalExpired) {
		t.Fatalf("expected ErrApprovalExpired, got %v", err)
	}

	// 4. Already consumed approval (ConsumedAt != nil) must fail
	consumedApp := approvedApp
	consumedTime := now.Add(time.Second)
	consumedApp.ConsumedAt = &consumedTime
	if err := response.ValidateApprovalForExecution(&consumedApp, dec, inc, now); !errors.Is(err, response.ErrApprovalAlreadyConsumed) {
		t.Fatalf("expected ErrApprovalAlreadyConsumed, got %v", err)
	}

	// 5. Rejected approval must fail
	rejectedApp := approvedApp
	rejectedApp.Status = response.ApprovalStatusRejected
	if err := response.ValidateApprovalForExecution(&rejectedApp, dec, inc, now); !errors.Is(err, response.ErrApprovalRejected) {
		t.Fatalf("expected ErrApprovalRejected, got %v", err)
	}

	// 6. Cancelled approval must fail
	cancelledApp := approvedApp
	cancelledApp.Status = response.ApprovalStatusCancelled
	if err := response.ValidateApprovalForExecution(&cancelledApp, dec, inc, now); !errors.Is(err, response.ErrApprovalCancelled) {
		t.Fatalf("expected ErrApprovalCancelled, got %v", err)
	}

	// 7. DecisionID mismatch must fail
	mismatchedDec := *dec
	mismatchedDec.DecisionID = "dec-different-uuid"
	if err := response.ValidateApprovalForExecution(&approvedApp, &mismatchedDec, inc, now); !errors.Is(err, response.ErrApprovalDecisionMismatch) {
		t.Fatalf("expected ErrApprovalDecisionMismatch for wrong DecisionID, got %v", err)
	}

	// 8. IncidentID mismatch must fail
	mismatchedIncDec := *dec
	mismatchedIncDec.IncidentID = "inc-different-uuid"
	if err := response.ValidateApprovalForExecution(&approvedApp, &mismatchedIncDec, inc, now); !errors.Is(err, response.ErrApprovalDecisionMismatch) {
		t.Fatalf("expected ErrApprovalDecisionMismatch for wrong IncidentID, got %v", err)
	}

	// 9. NodeID mismatch must fail
	mismatchedNodeDec := *dec
	mismatchedNodeDec.NodeID = "node-other"
	if err := response.ValidateApprovalForExecution(&approvedApp, &mismatchedNodeDec, inc, now); !errors.Is(err, response.ErrApprovalDecisionMismatch) {
		t.Fatalf("expected ErrApprovalDecisionMismatch for wrong NodeID, got %v", err)
	}

	// 10. ActionType mismatch must fail
	mismatchedTypeDec := *dec
	mismatchedTypeDec.ActionType = types.ActionSimulatedRestart
	if err := response.ValidateApprovalForExecution(&approvedApp, &mismatchedTypeDec, inc, now); !errors.Is(err, response.ErrApprovalDecisionMismatch) {
		t.Fatalf("expected ErrApprovalDecisionMismatch for wrong ActionType, got %v", err)
	}

	// 11. Target mismatch must fail
	mismatchedTargetDec := *dec
	mismatchedTargetDec.Target = "database_worker"
	if err := response.ValidateApprovalForExecution(&approvedApp, &mismatchedTargetDec, inc, now); !errors.Is(err, response.ErrApprovalDecisionMismatch) {
		t.Fatalf("expected ErrApprovalDecisionMismatch for wrong Target, got %v", err)
	}

	// 12. PolicyVersion mismatch must fail
	mismatchedVerDec := *dec
	mismatchedVerDec.PolicyVersion = "2.0.0"
	if err := response.ValidateApprovalForExecution(&approvedApp, &mismatchedVerDec, inc, now); !errors.Is(err, response.ErrApprovalDecisionMismatch) {
		t.Fatalf("expected ErrApprovalDecisionMismatch for wrong PolicyVersion, got %v", err)
	}

	// 13. Parameter tampering / Fingerprint mismatch must fail
	tamperedParamDec := *dec
	tamperedParamDec.Parameters = map[string]string{"throttle_percent": "99", "duration_sec": "60"}
	if err := response.ValidateApprovalForExecution(&approvedApp, &tamperedParamDec, inc, now); !errors.Is(err, response.ErrApprovalDecisionMismatch) {
		t.Fatalf("expected ErrApprovalDecisionMismatch for tampered parameters, got %v", err)
	}

	// 14. Authorization class not APPROVAL_REQUIRED (e.g. AUTO_EXECUTE) must fail
	autoExecDec := *dec
	autoExecDec.AuthorizationClass = response.AuthClassAutoExecute
	if err := response.ValidateApprovalForExecution(&approvedApp, &autoExecDec, inc, now); err == nil {
		t.Fatalf("expected failure when validating approval against AUTO_EXECUTE action")
	}
}

// TEST: ApprovalManager end-to-end lifecycle with deterministic FakeClock
func TestApprovalManager_EndToEndWithClock(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "approval_manager.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	startTime := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	clock := NewFakeClock(startTime)
	mgr := response.NewApprovalManager(store, clock)

	dec := makeApprovalTestDecision(types.ActionSimulatedThrottle, "telemetry_generator", response.AuthClassApprovalRequired)
	inc := makeApprovalTestIncident(dec.IncidentID, dec.NodeID, types.StatusAnomalyDetected)

	// 1. Request Approval
	req := response.ApprovalRequest{
		Decision:    dec,
		RequestedBy: "operator_bob",
		TTL:         10 * time.Minute,
		Reason:      "manual throttle during load surge",
	}
	app, err := mgr.RequestApproval(ctx, req)
	if err != nil {
		t.Fatalf("RequestApproval failed: %v", err)
	}
	if app.Status != response.ApprovalStatusPending {
		t.Fatalf("expected status PENDING, got %s", app.Status)
	}

	// Duplicate request with same DecisionID must fail
	_, err = mgr.RequestApproval(ctx, req)
	if !errors.Is(err, storage.ErrDuplicateApproval) {
		t.Fatalf("expected ErrDuplicateApproval, got %v", err)
	}

	// 2. Retrieve by DecisionID
	retrieved, err := mgr.GetApprovalByDecisionID(ctx, dec.DecisionID)
	if err != nil || retrieved.ApprovalID != app.ApprovalID {
		t.Fatalf("GetApprovalByDecisionID failed: %v", err)
	}

	// 3. Advance time 2 minutes and Approve
	clock.Advance(2 * time.Minute)
	if err := mgr.Approve(ctx, app.ApprovalID, "operator_supervisor"); err != nil {
		t.Fatalf("Approve failed: %v", err)
	}

	// Verify status is APPROVED
	approvedApp, err := mgr.GetApproval(ctx, app.ApprovalID)
	if err != nil || approvedApp.Status != response.ApprovalStatusApproved {
		t.Fatalf("expected APPROVED status, got %v (err: %v)", approvedApp, err)
	}

	// 4. ValidateForExecution at 5 minutes
	clock.Advance(3 * time.Minute)
	validated, err := mgr.ValidateForExecution(ctx, app.ApprovalID, dec, inc)
	if err != nil {
		t.Fatalf("ValidateForExecution failed: %v", err)
	}
	if validated.ApprovalID != app.ApprovalID {
		t.Fatalf("validated approval ID mismatch")
	}

	// 5. Consume approval
	if err := mgr.ConsumeApproval(ctx, app.ApprovalID); err != nil {
		t.Fatalf("ConsumeApproval failed: %v", err)
	}

	// Second consumption MUST FAIL
	if err := mgr.ConsumeApproval(ctx, app.ApprovalID); !errors.Is(err, storage.ErrApprovalAlreadyConsumed) {
		t.Fatalf("expected ErrApprovalAlreadyConsumed, got %v", err)
	}

	// ValidateForExecution on consumed approval MUST FAIL
	_, err = mgr.ValidateForExecution(ctx, app.ApprovalID, dec, inc)
	if !errors.Is(err, response.ErrApprovalAlreadyConsumed) {
		t.Fatalf("expected ErrApprovalAlreadyConsumed on consumed approval, got %v", err)
	}
}

// TEST: Expiration behavior with FakeClock
func TestApprovalManager_ExpirationWithClock(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "approval_expiration.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	startTime := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	clock := NewFakeClock(startTime)
	mgr := response.NewApprovalManager(store, clock)

	dec := makeApprovalTestDecision(types.ActionSimulatedThrottle, "telemetry_generator", response.AuthClassApprovalRequired)

	// Request with 5-minute TTL
	req := response.ApprovalRequest{
		Decision:    dec,
		RequestedBy: "operator_bob",
		TTL:         5 * time.Minute,
	}
	app, err := mgr.RequestApproval(ctx, req)
	if err != nil {
		t.Fatalf("RequestApproval failed: %v", err)
	}

	// Advance clock past expiration (6 minutes)
	clock.Advance(6 * time.Minute)

	// Attempting to approve must fail with ErrApprovalExpired
	if err := mgr.Approve(ctx, app.ApprovalID, "operator_supervisor"); !errors.Is(err, storage.ErrApprovalExpired) {
		t.Fatalf("expected ErrApprovalExpired, got %v", err)
	}

	// Record should now be marked EXPIRED
	got, err := mgr.GetApproval(ctx, app.ApprovalID)
	if err != nil || got.Status != response.ApprovalStatusExpired {
		t.Fatalf("expected status EXPIRED, got %s (err: %v)", got.Status, err)
	}
}

// TEST: Observability Audit Event generation
func TestApprovalAuditEvent_Generation(t *testing.T) {
	now := time.Now().UTC()
	dec := makeApprovalTestDecision(types.ActionSimulatedThrottle, "telemetry_generator", response.AuthClassApprovalRequired)
	app, err := response.NewApprovalFromRequest(response.ApprovalRequest{Decision: dec, RequestedBy: "operator_alice"}, now)
	if err != nil {
		t.Fatalf("NewApprovalFromRequest failed: %v", err)
	}

	event := response.NewApprovalAuditEvent(response.ApprovalEventApproved, app, "operator_supervisor", "authorized for test", now)
	if event.EventType != response.ApprovalEventApproved {
		t.Fatalf("unexpected event type: %s", event.EventType)
	}
	if event.ApprovalID != app.ApprovalID || event.DecisionID != dec.DecisionID {
		t.Fatalf("event does not match approval identity")
	}
	if event.Actor != "operator_supervisor" {
		t.Fatalf("unexpected actor: %s", event.Actor)
	}
}

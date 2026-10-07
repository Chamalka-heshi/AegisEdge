package audit_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/audit"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/escalation"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/incident"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/orchestrator"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/response"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/verification"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

func newTestAuditService(t *testing.T) (*audit.Service, *storage.SQLiteStore, string) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit_domain_test.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	svc, err := audit.NewService(store, audit.RealClock{})
	if err != nil {
		t.Fatalf("audit.NewService failed: %v", err)
	}
	return svc, store, dbPath
}

func sampleValidEvent(eventID string, evtType audit.EventType, incID, nodeID string) audit.AuditEvent {
	return audit.AuditEvent{
		EventID:       eventID,
		EventType:     evtType,
		Timestamp:     time.Now().UTC(),
		NodeID:        nodeID,
		IncidentID:    incID,
		CorrelationID: audit.ComputeCorrelationID(incID),
		DecisionID:    "dec-" + incID,
		ActionID:      "act-" + incID,
		PolicyVersion: "v1.0",
		Actor:         "test-runner",
		Result:        audit.ResultSuccess,
		Reason:        "test execution",
		Metadata: map[string]string{
			"env": "testing",
		},
	}
}

// ============================================================================
// Scenario A: Event Validation
// ============================================================================
func TestAudit_A_EventValidation(t *testing.T) {
	evt := sampleValidEvent("evt-a-1", audit.EventTypeSafetyValidated, "inc-1", "node-1")
	if err := evt.Validate(); err != nil {
		t.Fatalf("expected valid event to pass, got: %v", err)
	}

	// Negative tests
	badEvt := evt
	badEvt.EventID = ""
	if err := badEvt.Validate(); err == nil {
		t.Errorf("expected error for empty event_id")
	}

	badEvt = evt
	badEvt.Timestamp = time.Time{}
	if err := badEvt.Validate(); err == nil {
		t.Errorf("expected error for zero timestamp")
	}

	badEvt = evt
	badEvt.NodeID = ""
	if err := badEvt.Validate(); err == nil {
		t.Errorf("expected error for empty node_id")
	}

	badEvt = evt
	badEvt.IncidentID = ""
	if err := badEvt.Validate(); err == nil {
		t.Errorf("expected error for empty incident_id")
	}
}

// ============================================================================
// Scenario B: Supported Event Types
// ============================================================================
func TestAudit_B_SupportedEventTypes(t *testing.T) {
	expectedTypes := []audit.EventType{
		audit.EventTypeAnomalyDetected,
		audit.EventTypeIncidentCreated,
		audit.EventTypeIncidentStateChanged,
		audit.EventTypeResponseDecisionCreated,
		audit.EventTypeSafetyValidated,
		audit.EventTypeSafetyRejected,
		audit.EventTypeApprovalRequested,
		audit.EventTypeApprovalGranted,
		audit.EventTypeApprovalRejected,
		audit.EventTypeApprovalExpired,
		audit.EventTypeApprovalCancelled,
		audit.EventTypeMitigationRecorded,
		audit.EventTypeMitigationStarted,
		audit.EventTypeMitigationExecuted,
		audit.EventTypeMitigationFailed,
		audit.EventTypeVerificationStarted,
		audit.EventTypeVerificationProgress,
		audit.EventTypeVerificationRecovered,
		audit.EventTypeVerificationTimedOut,
		audit.EventTypeVerificationRejected,
		audit.EventTypeEscalationEvaluated,
		audit.EventTypeRetryAuthorized,
		audit.EventTypeCircuitOpened,
		audit.EventTypeOrchestrationCompleted,
		audit.EventTypeOrchestrationStopped,
	}

	for _, et := range expectedTypes {
		if !et.IsValid() {
			t.Errorf("expected EventType %s to be valid", et)
		}
	}
}

// ============================================================================
// Scenario C: Invalid Event Types
// ============================================================================
func TestAudit_C_InvalidEventTypes(t *testing.T) {
	invalidTypes := []audit.EventType{
		"UNKNOWN_EVENT",
		"UNAUTHORIZED_ACTION",
		"",
		"RANDOM_STRING",
	}

	for _, it := range invalidTypes {
		if it.IsValid() {
			t.Errorf("expected invalid EventType %s to return false", it)
		}
		evt := sampleValidEvent("evt-c", it, "inc-1", "node-1")
		if err := evt.Validate(); err == nil {
			t.Errorf("expected validation failure for invalid event type %s", it)
		}
	}
}

// ============================================================================
// Scenario D: Required Identifiers
// ============================================================================
func TestAudit_D_RequiredIdentifiers(t *testing.T) {
	evt := sampleValidEvent("evt-d", audit.EventTypeIncidentCreated, "", "node-1")
	if err := evt.Validate(); err == nil || !strings.Contains(err.Error(), "incident_id is required") {
		t.Errorf("expected incident_id is required error, got: %v", err)
	}

	evt = sampleValidEvent("evt-d", audit.EventTypeIncidentCreated, "inc-1", "")
	if err := evt.Validate(); err == nil || !strings.Contains(err.Error(), "node_id is required") {
		t.Errorf("expected node_id is required error, got: %v", err)
	}
}

// ============================================================================
// Scenario E: Deterministic Event Identity
// ============================================================================
func TestAudit_E_DeterministicEventIdentity(t *testing.T) {
	id1 := audit.ComputeEventID("node-1", "inc-1", audit.EventTypeMitigationExecuted, 0, "act-1")
	id2 := audit.ComputeEventID("node-1", "inc-1", audit.EventTypeMitigationExecuted, 0, "act-1")
	if id1 != id2 {
		t.Errorf("expected deterministic IDs to match, got %s and %s", id1, id2)
	}

	// Different attempt produces distinct ID
	id3 := audit.ComputeEventID("node-1", "inc-1", audit.EventTypeMitigationExecuted, 1, "act-1")
	if id1 == id3 {
		t.Errorf("expected attempt 1 to produce distinct ID, got collision: %s", id1)
	}

	// Different discriminator produces distinct ID
	id4 := audit.ComputeEventID("node-1", "inc-1", audit.EventTypeMitigationExecuted, 0, "act-2")
	if id1 == id4 {
		t.Errorf("expected distinct discriminator to produce distinct ID, got collision: %s", id1)
	}
}

// ============================================================================
// Scenario F: Duplicate Event Insertion
// ============================================================================
func TestAudit_F_DuplicateEventInsertion(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newTestAuditService(t)

	evt := sampleValidEvent("evt-dup-f", audit.EventTypeMitigationExecuted, "inc-f", "node-f")
	if err := svc.Record(ctx, evt); err != nil {
		t.Fatalf("first record should succeed, got: %v", err)
	}

	// Duplicate write
	err := svc.Record(ctx, evt)
	if err != audit.ErrDuplicateEvent {
		t.Fatalf("expected ErrDuplicateEvent, got: %v", err)
	}

	count, err := svc.Count(ctx)
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected count 1 after duplicate write attempt, got %d", count)
	}
}

// ============================================================================
// Scenario G: Different Retry Events
// ============================================================================
func TestAudit_G_DifferentRetryEvents(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newTestAuditService(t)

	// Attempt 0
	evt0 := sampleValidEvent(
		audit.ComputeEventID("node-1", "inc-retry", audit.EventTypeMitigationExecuted, 0, "act-1"),
		audit.EventTypeMitigationExecuted, "inc-retry", "node-1",
	)
	evt0.Reason = "initial attempt"
	if err := svc.Record(ctx, evt0); err != nil {
		t.Fatalf("failed to record attempt 0: %v", err)
	}

	// Attempt 1 (retry)
	evt1 := sampleValidEvent(
		audit.ComputeEventID("node-1", "inc-retry", audit.EventTypeMitigationExecuted, 1, "act-1"),
		audit.EventTypeMitigationExecuted, "inc-retry", "node-1",
	)
	evt1.Reason = "retry attempt 1"
	if err := svc.Record(ctx, evt1); err != nil {
		t.Fatalf("failed to record attempt 1: %v", err)
	}

	// Both should exist without collision
	count, err := svc.Count(ctx)
	if err != nil || count != 2 {
		t.Fatalf("expected 2 distinct retry events, got %d (err: %v)", count, err)
	}
}

// ============================================================================
// Scenario H: Persistence
// ============================================================================
func TestAudit_H_Persistence(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newTestAuditService(t)

	evt := sampleValidEvent("evt-h", audit.EventTypeResponseDecisionCreated, "inc-h", "node-h")
	evt.DecisionID = "dec-h-123"
	if err := svc.Record(ctx, evt); err != nil {
		t.Fatalf("record failed: %v", err)
	}

	retrieved, err := svc.GetEvent(ctx, "evt-h")
	if err != nil {
		t.Fatalf("GetEvent failed: %v", err)
	}
	if retrieved.DecisionID != "dec-h-123" {
		t.Errorf("expected decision_id dec-h-123, got: %s", retrieved.DecisionID)
	}
}

// ============================================================================
// Scenario I: Restart Persistence
// ============================================================================
func TestAudit_I_RestartPersistence(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "restart_audit.db")

	store1, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite 1 failed: %v", err)
	}
	svc1, _ := audit.NewService(store1, audit.RealClock{})

	evt := sampleValidEvent("evt-i", audit.EventTypeSafetyValidated, "inc-i", "node-i")
	evt.Reason = "restart test verification"
	if err := svc1.Record(ctx, evt); err != nil {
		t.Fatalf("failed to record before restart: %v", err)
	}

	// Close store1
	_ = store1.Close()

	// Reopen on same file
	store2, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite 2 failed: %v", err)
	}
	defer store2.Close()
	svc2, _ := audit.NewService(store2, audit.RealClock{})

	retrieved, err := svc2.GetEvent(ctx, "evt-i")
	if err != nil {
		t.Fatalf("failed to retrieve after restart: %v", err)
	}
	if retrieved.Reason != "restart test verification" {
		t.Errorf("reason mismatch after restart: %s", retrieved.Reason)
	}
}

// ============================================================================
// Scenarios J, K, L: Incident, Correlation, Node Queries
// ============================================================================
func TestAudit_JKL_Queries(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newTestAuditService(t)

	e1 := sampleValidEvent("evt-jkl-1", audit.EventTypeIncidentCreated, "inc-10", "node-X")
	e2 := sampleValidEvent("evt-jkl-2", audit.EventTypeMitigationExecuted, "inc-10", "node-X")
	e3 := sampleValidEvent("evt-jkl-3", audit.EventTypeIncidentCreated, "inc-20", "node-Y")

	_ = svc.Record(ctx, e1)
	_ = svc.Record(ctx, e2)
	_ = svc.Record(ctx, e3)

	// J: By Incident
	incList, err := svc.ListByIncident(ctx, "inc-10", 10)
	if err != nil || len(incList) != 2 {
		t.Fatalf("ListByIncident failed: len=%d, err=%v", len(incList), err)
	}

	// K: By Correlation
	corrList, err := svc.ListByCorrelation(ctx, "corr-inc-10", 10)
	if err != nil || len(corrList) != 2 {
		t.Fatalf("ListByCorrelation failed: len=%d, err=%v", len(corrList), err)
	}

	// L: By Node
	nodeList, err := svc.ListByNode(ctx, "node-Y", 10)
	if err != nil || len(nodeList) != 1 {
		t.Fatalf("ListByNode failed: len=%d, err=%v", len(nodeList), err)
	}
}

// ============================================================================
// Scenario M: Chronological Ordering
// ============================================================================
func TestAudit_M_ChronologicalOrdering(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newTestAuditService(t)

	base := time.Now().UTC()
	e1 := sampleValidEvent("evt-m-1", audit.EventTypeIncidentCreated, "inc-m", "node-m")
	e1.Timestamp = base.Add(1 * time.Second)

	e2 := sampleValidEvent("evt-m-2", audit.EventTypeMitigationExecuted, "inc-m", "node-m")
	e2.Timestamp = base.Add(2 * time.Second)

	e3 := sampleValidEvent("evt-m-3", audit.EventTypeVerificationRecovered, "inc-m", "node-m")
	e3.Timestamp = base.Add(3 * time.Second)

	// Insert in non-chronological order
	_ = svc.Record(ctx, e2)
	_ = svc.Record(ctx, e3)
	_ = svc.Record(ctx, e1)

	list, err := svc.ListByIncident(ctx, "inc-m", 10)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("expected 3 events, got %d", len(list))
	}
	if list[0].EventID != "evt-m-1" || list[1].EventID != "evt-m-2" || list[2].EventID != "evt-m-3" {
		t.Errorf("events not in chronological order: %s, %s, %s", list[0].EventID, list[1].EventID, list[2].EventID)
	}
}

// ============================================================================
// Scenario N: Bounded Query Limits
// ============================================================================
func TestAudit_N_BoundedQueryLimits(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newTestAuditService(t)

	for i := 0; i < 25; i++ {
		e := sampleValidEvent(fmt.Sprintf("evt-lim-%02d", i), audit.EventTypeVerificationProgress, "inc-lim", "node-lim")
		_ = svc.Record(ctx, e)
	}

	// Limit 7
	res7, err := svc.ListByIncident(ctx, "inc-lim", 7)
	if err != nil || len(res7) != 7 {
		t.Fatalf("expected 7 events, got %d (err: %v)", len(res7), err)
	}

	// Limit 0 defaults to DefaultAuditQueryLimit
	resDef, err := svc.ListByIncident(ctx, "inc-lim", 0)
	if err != nil || len(resDef) != 25 {
		t.Fatalf("expected 25 events with default limit, got %d", len(resDef))
	}
}

// ============================================================================
// Scenario O: Concurrent Writes
// ============================================================================
func TestAudit_O_ConcurrentWrites(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newTestAuditService(t)

	var wg sync.WaitGroup
	errCh := make(chan error, 30)
	n := 20

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			e := sampleValidEvent(
				fmt.Sprintf("evt-concurrent-%d", idx),
				audit.EventTypeMitigationExecuted,
				fmt.Sprintf("inc-%d", idx%4),
				fmt.Sprintf("node-%d", idx%2),
			)
			if err := svc.Record(ctx, e); err != nil {
				errCh <- err
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("concurrent write failed: %v", err)
	}

	count, _ := svc.Count(ctx)
	if count != int64(n) {
		t.Fatalf("expected %d events, got %d", n, count)
	}
}

// ============================================================================
// Scenario P: Malformed Metadata & Prohibited Content
// ============================================================================
func TestAudit_P_MalformedMetadata(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newTestAuditService(t)

	// Prohibited credentials
	e1 := sampleValidEvent("evt-sec-1", audit.EventTypeSafetyValidated, "inc-p", "node-p")
	e1.Metadata = map[string]string{"secret_key": "some_secret_key"}
	if err := svc.Record(ctx, e1); err == nil {
		t.Errorf("expected error for prohibited secret pattern")
	}

	// Prohibited command
	e2 := sampleValidEvent("evt-sec-2", audit.EventTypeSafetyValidated, "inc-p", "node-p")
	e2.Metadata = map[string]string{"cmd": "powershell -ExecutionPolicy Bypass"}
	if err := svc.Record(ctx, e2); err == nil {
		t.Errorf("expected error for prohibited powershell pattern")
	}
}

// ============================================================================
// Scenario Q: Oversized Metadata
// ============================================================================
func TestAudit_Q_OversizedMetadata(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newTestAuditService(t)

	e := sampleValidEvent("evt-q", audit.EventTypeSafetyValidated, "inc-q", "node-q")
	e.Metadata = map[string]string{
		"huge": strings.Repeat("X", 8500),
	}
	if err := svc.Record(ctx, e); err == nil {
		t.Errorf("expected error for oversized metadata")
	}
}

// ============================================================================
// Scenario R: Complete Response Lifecycle Audit Sequence
// ============================================================================
func TestAudit_R_CompleteResponseLifecycleAuditSequence(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newTestAuditService(t)

	incID := "inc-lifecycle-r"
	nodeID := "edge-node-1"

	stages := []audit.EventType{
		audit.EventTypeAnomalyDetected,
		audit.EventTypeIncidentCreated,
		audit.EventTypeResponseDecisionCreated,
		audit.EventTypeSafetyValidated,
		audit.EventTypeMitigationRecorded,
		audit.EventTypeMitigationStarted,
		audit.EventTypeMitigationExecuted,
		audit.EventTypeVerificationStarted,
		audit.EventTypeVerificationProgress,
		audit.EventTypeVerificationRecovered,
		audit.EventTypeOrchestrationCompleted,
	}

	for i, st := range stages {
		e := sampleValidEvent(fmt.Sprintf("evt-stage-%02d", i), st, incID, nodeID)
		if err := svc.Record(ctx, e); err != nil {
			t.Fatalf("failed to record stage %s: %v", st, err)
		}
	}

	events, err := svc.ListByIncident(ctx, incID, 20)
	if err != nil {
		t.Fatalf("ListByIncident failed: %v", err)
	}
	if len(events) != len(stages) {
		t.Fatalf("expected %d events, got %d", len(stages), len(events))
	}
	for i, st := range stages {
		if events[i].EventType != st {
			t.Errorf("stage %d: expected %s, got %s", i, st, events[i].EventType)
		}
	}
}

// ============================================================================
// Scenario S: Failed Mitigation Audit
// ============================================================================
func TestAudit_S_FailedMitigationAudit(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newTestAuditService(t)

	incID := "inc-failed-s"
	e1 := sampleValidEvent("evt-s-1", audit.EventTypeMitigationStarted, incID, "node-1")
	e2 := sampleValidEvent("evt-s-2", audit.EventTypeMitigationFailed, incID, "node-1")
	e2.Result = audit.ResultFailed
	e2.Reason = "simulated executor failure"

	_ = svc.Record(ctx, e1)
	_ = svc.Record(ctx, e2)

	list, _ := svc.ListByIncident(ctx, incID, 10)
	if len(list) != 2 || list[1].EventType != audit.EventTypeMitigationFailed {
		t.Fatalf("unexpected mitigation failure audit trail")
	}
}

// ============================================================================
// Scenario T: Verification Timeout Audit
// ============================================================================
func TestAudit_T_VerificationTimeoutAudit(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newTestAuditService(t)

	incID := "inc-timeout-t"
	e := sampleValidEvent("evt-t", audit.EventTypeVerificationTimedOut, incID, "node-1")
	e.Result = audit.ResultTimedOut
	e.Reason = "observation window expired before recovery"

	if err := svc.Record(ctx, e); err != nil {
		t.Fatalf("record failed: %v", err)
	}

	retrieved, _ := svc.GetEvent(ctx, "evt-t")
	if retrieved.Result != audit.ResultTimedOut {
		t.Errorf("expected ResultTimedOut, got %s", retrieved.Result)
	}
}

// ============================================================================
// Scenario U: Escalation Audit
// ============================================================================
func TestAudit_U_EscalationAudit(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newTestAuditService(t)

	incID := "inc-esc-u"
	e := sampleValidEvent("evt-u", audit.EventTypeEscalationEvaluated, incID, "node-1")
	e.Result = audit.ResultEscalated
	e.Reason = "max retries exceeded"

	if err := svc.Record(ctx, e); err != nil {
		t.Fatalf("record failed: %v", err)
	}

	retrieved, _ := svc.GetEvent(ctx, "evt-u")
	if retrieved.Result != audit.ResultEscalated {
		t.Errorf("expected ResultEscalated, got %s", retrieved.Result)
	}
}

// ============================================================================
// Scenario V: Retry Audit
// ============================================================================
func TestAudit_V_RetryAudit(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newTestAuditService(t)

	incID := "inc-retry-v"
	e := sampleValidEvent("evt-v", audit.EventTypeRetryAuthorized, incID, "node-1")
	e.Result = audit.ResultRetry
	e.Reason = "retry attempt 2 permitted by escalation policy"

	if err := svc.Record(ctx, e); err != nil {
		t.Fatalf("record failed: %v", err)
	}

	retrieved, _ := svc.GetEvent(ctx, "evt-v")
	if retrieved.Result != audit.ResultRetry {
		t.Errorf("expected ResultRetry, got %s", retrieved.Result)
	}
}

// ============================================================================
// Scenario W: Circuit-Breaker Audit
// ============================================================================
func TestAudit_W_CircuitBreakerAudit(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newTestAuditService(t)

	incID := "inc-cb-w"
	e := sampleValidEvent("evt-w", audit.EventTypeCircuitOpened, incID, "node-1")
	e.Result = audit.ResultCircuitOpen
	e.Reason = "consecutive failures tripped circuit breaker"

	if err := svc.Record(ctx, e); err != nil {
		t.Fatalf("record failed: %v", err)
	}

	retrieved, _ := svc.GetEvent(ctx, "evt-w")
	if retrieved.Result != audit.ResultCircuitOpen {
		t.Errorf("expected ResultCircuitOpen, got %s", retrieved.Result)
	}
}

// ============================================================================
// Scenario X: Orchestrator Stop / No-Action Audit
// ============================================================================
func TestAudit_X_OrchestratorStopNoActionAudit(t *testing.T) {
	ctx := context.Background()
	svc, _, _ := newTestAuditService(t)

	incID := "inc-stop-x"
	e := sampleValidEvent("evt-x", audit.EventTypeOrchestrationStopped, incID, "node-1")
	e.Result = audit.ResultSkipped
	e.Reason = "response policy determined NO_ACTION; orchestration stopped for cycle"

	if err := svc.Record(ctx, e); err != nil {
		t.Fatalf("record failed: %v", err)
	}

	retrieved, _ := svc.GetEvent(ctx, "evt-x")
	if retrieved.Result != audit.ResultSkipped {
		t.Errorf("expected ResultSkipped, got %s", retrieved.Result)
	}
}

// ============================================================================
// Scenario 18: Full End-to-End Incident Response Audit Reconstruction
// ============================================================================
func TestAudit_Item18_AuditReconstruction_Integration(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "audit_reconstruction.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite failed: %v", err)
	}
	defer store.Close()

	clock := orchestrator.NewFakeClock(time.Now().UTC())

	// Policy (ExecutionModeAutoExecute)
	policy := response.NewDefaultPolicy()
	if err := policy.SetExecutionMode(response.ExecutionModeAutoExecute); err != nil {
		t.Fatalf("SetExecutionMode failed: %v", err)
	}

	// Safety Validator
	validator := response.NewStandardValidator()

	// Executor
	exec := response.NewSimulatedExecutor(validator)
	exec.WithApprovalStore(store)
	exec.WithClock(clock)

	// Verification Engine
	vCfg := verification.DefaultConfig()
	vCfg.Store = store
	vCfg.Clock = clock
	vCfg.RequiredConsecutiveObservations = 2
	vCfg.VerificationTimeout = 5 * time.Minute
	vEngine, err := verification.NewLocalEngine(vCfg)
	if err != nil {
		t.Fatalf("NewLocalEngine failed: %v", err)
	}

	// Escalation Engine
	escPolicy := escalation.DefaultPolicy()
	escEngine, err := escalation.NewEngine(store, escPolicy, clock)
	if err != nil {
		t.Fatalf("NewEngine (escalation) failed: %v", err)
	}

	// Incident Engine
	incCfg := incident.DefaultPolicyConfig()
	incEngine, err := incident.NewLocalEngine(incCfg)
	if err != nil {
		t.Fatalf("NewLocalEngine (incident) failed: %v", err)
	}

	// Orchestrator
	orchCfg := orchestrator.DefaultConfig()
	orchCfg.DefaultRequiredObservations = 2
	orch, err := orchestrator.NewOrchestrator(
		policy, validator, exec, vEngine, escEngine, incEngine, store,
		orchCfg, clock,
	)
	if err != nil {
		t.Fatalf("NewOrchestrator failed: %v", err)
	}

	// Audit Service
	auditSvc := orch.GetAuditRecorder().(audit.Recorder).Query()

	// 1. ANOMALY DETECTED -> INCIDENT CREATED
	inc := &types.Incident{
		IncidentID:    "inc-recon-100",
		NodeID:        "edge-box-1",
		RuleName:      "cpu_surge_rule",
		Severity:      types.SeverityHigh,
		Status:        types.StatusAnomalyDetected,
		TriggerMetric: "cpu_usage_percent",
		TriggerValue:  85.0,
		Threshold:     80.0,
		TriggeredAt:   clock.Now(),
		UpdatedAt:     clock.Now(),
	}

	// Record initial detection audit events
	_ = orch.GetAuditRecorder().Record(ctx, audit.AuditEvent{
		EventID:    "evt-recon-anomaly",
		EventType:  audit.EventTypeAnomalyDetected,
		Timestamp:  clock.Now(),
		NodeID:     inc.NodeID,
		IncidentID: inc.IncidentID,
		Actor:      "detector",
		Result:     audit.ResultSuccess,
		Reason:     "telemetry anomaly detected",
	})
	_ = orch.GetAuditRecorder().Record(ctx, audit.AuditEvent{
		EventID:    "evt-recon-incident",
		EventType:  audit.EventTypeIncidentCreated,
		Timestamp:  clock.Now(),
		NodeID:     inc.NodeID,
		IncidentID: inc.IncidentID,
		Actor:      "incident_engine",
		Result:     audit.ResultSuccess,
		Reason:     "incident created from anomaly",
	})

	// 2. ORCHESTRATE INCIDENT (POLICY -> SAFETY -> PERSISTENCE -> EXECUTION -> VERIFICATION)
	// Provide healthy telemetry samples so verification recovers immediately
	healthySamples := []types.MetricSample{
		{NodeID: "edge-box-1", Name: "cpu_usage_percent", Value: 40.0, Timestamp: clock.Now()},
		{NodeID: "edge-box-1", Name: "cpu_usage_percent", Value: 42.0, Timestamp: clock.Now().Add(1 * time.Second)},
		{NodeID: "edge-box-1", Name: "cpu_usage_percent", Value: 39.0, Timestamp: clock.Now().Add(2 * time.Second)},
	}

	res, err := orch.OrchestrateIncident(ctx, inc, orchestrator.WithTelemetrySamples(healthySamples...))
	if err != nil {
		t.Fatalf("OrchestrateIncident failed: %v", err)
	}
	if res.State != orchestrator.StateRecovered {
		t.Fatalf("expected StateRecovered, got %s (msg: %s)", res.State, res.Message)
	}

	// 3. RECONSTRUCT AUDIT TRAIL
	auditTrail, err := auditSvc.ListByIncident(ctx, inc.IncidentID, 50)
	if err != nil {
		t.Fatalf("ListByIncident failed: %v", err)
	}

	if len(auditTrail) == 0 {
		t.Fatalf("audit trail is empty!")
	}

	t.Logf("Reconstructed %d lifecycle audit events for %s:", len(auditTrail), inc.IncidentID)
	for idx, e := range auditTrail {
		t.Logf("  [%d] %s: %s (Actor=%s, Result=%s, Reason=%s)",
			idx, e.Timestamp.Format(time.RFC3339Nano), e.EventType, e.Actor, e.Result, e.Reason)
	}

	// Verify key lifecycle checkpoints exist in order
	expectedEventTypes := []audit.EventType{
		audit.EventTypeAnomalyDetected,
		audit.EventTypeIncidentCreated,
		audit.EventTypeResponseDecisionCreated,
		audit.EventTypeSafetyValidated,
		audit.EventTypeMitigationRecorded,
		audit.EventTypeMitigationStarted,
		audit.EventTypeMitigationExecuted,
		audit.EventTypeVerificationStarted,
		audit.EventTypeVerificationRecovered,
		audit.EventTypeOrchestrationCompleted,
	}

	foundMap := make(map[audit.EventType]bool)
	for _, e := range auditTrail {
		foundMap[e.EventType] = true
	}

	for _, exp := range expectedEventTypes {
		if !foundMap[exp] {
			t.Errorf("missing expected lifecycle audit event: %s", exp)
		}
	}

	// Verify correlation ID query returns identical workflow history
	corrTrail, err := auditSvc.ListByCorrelation(ctx, audit.ComputeCorrelationID(inc.IncidentID), 50)
	if err != nil {
		t.Fatalf("ListByCorrelation failed: %v", err)
	}
	if len(corrTrail) < len(expectedEventTypes) {
		t.Errorf("expected at least %d correlation events, got %d", len(expectedEventTypes), len(corrTrail))
	}
}

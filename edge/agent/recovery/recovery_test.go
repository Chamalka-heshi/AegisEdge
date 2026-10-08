package recovery_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/audit"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/health"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/metrics"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/recovery"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// setupTestStore creates a real SQLite database for testing recovery interactions.
func setupTestStore(t *testing.T) *storage.SQLiteStore {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "recovery_test.db")
	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

// TestRecovery_A_CleanStartup_NoPriorState verifies clean startup when no prior checkpoint or in-flight records exist.
func TestRecovery_A_CleanStartup_NoPriorState(t *testing.T) {
	store := setupTestStore(t)
	ht := health.NewTracker()
	_ = ht.RegisterCheck(health.CheckRecovery, true)
	metricRec := metrics.NewDefaultRecorder(nil)
	auditRec, _ := audit.NewService(store, audit.RealClock{})

	mgr, err := recovery.NewManager(recovery.Config{
		NodeID:          "node-edge-1",
		Store:           store,
		HealthTracker:   ht,
		AuditRecorder:   auditRec,
		MetricsRecorder: metricRec,
	})
	if err != nil {
		t.Fatalf("failed to create recovery manager: %v", err)
	}

	ctx := context.Background()
	dec, err := mgr.RunRecovery(ctx)
	if err != nil {
		t.Fatalf("RunRecovery failed: %v", err)
	}

	if dec.State != recovery.StateRecoveryNotRequired {
		t.Errorf("expected StateRecoveryNotRequired, got %v", dec.State)
	}
	if dec.RequiresIntervention {
		t.Errorf("expected RequiresIntervention=false, got true")
	}
	if mgr.Tracker().State() != recovery.StateRecoveryNotRequired {
		t.Errorf("expected tracker state StateRecoveryNotRequired, got %v", mgr.Tracker().State())
	}
	if !mgr.Tracker().IsComplete() {
		t.Errorf("expected tracker IsComplete=true")
	}
	if mgr.Tracker().IsBlocked() {
		t.Errorf("expected tracker IsBlocked=false")
	}

	// Health check must be OK
	checkInfo, ok := ht.GetCheck(health.CheckRecovery)
	if !ok || checkInfo.Status != health.StatusOk {
		t.Errorf("expected recovery health status StatusOk, got ok=%v, status=%v", ok, checkInfo.Status)
	}

	// Verify checkpoint was persisted in SQLite
	latest, err := store.GetLatestCheckpoint(ctx, "node-edge-1")
	if err != nil {
		t.Fatalf("failed to retrieve latest checkpoint: %v", err)
	}
	if latest == nil {
		t.Fatalf("expected checkpoint to be persisted, got nil")
	}
	if latest.State != string(recovery.StateRecoveryNotRequired) {
		t.Errorf("expected persisted checkpoint state RECOVERY_NOT_REQUIRED, got %s", latest.State)
	}

	// Verify Prometheus metrics
	prom := metricRec.Registry().FormatPrometheus()
	if !strings.Contains(prom, `aegisedge_recovery_total{status="not_required"} 1`) {
		t.Errorf("expected not_required recovery metric, got:\n%s", prom)
	}
}

// TestRecovery_B_CleanRestart_AfterCleanShutdown verifies loading prior clean checkpoint.
func TestRecovery_B_CleanRestart_AfterCleanShutdown(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	// Seed previous clean checkpoint
	now := time.Now().UTC()
	prevCP := &storage.StoredCheckpoint{
		CheckpointID:          "cp-node-1-clean",
		NodeID:                "node-edge-1",
		State:                 string(recovery.StateRecoveryComplete),
		StartedAt:             now.Add(-1 * time.Hour),
		CompletedAt:           &now,
		RecoveryReason:        "previous clean shutdown",
		LastReconciledAttempt: 1,
		SchemaVersion:         9,
		CreatedAt:             now.Add(-1 * time.Hour),
		UpdatedAt:             now,
	}
	if err := store.SaveCheckpoint(ctx, prevCP); err != nil {
		t.Fatalf("failed to save prior checkpoint: %v", err)
	}

	ht := health.NewTracker()
	_ = ht.RegisterCheck(health.CheckRecovery, true)
	metricRec := metrics.NewDefaultRecorder(nil)

	mgr, err := recovery.NewManager(recovery.Config{
		NodeID:          "node-edge-1",
		Store:           store,
		HealthTracker:   ht,
		MetricsRecorder: metricRec,
	})
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	dec, err := mgr.RunRecovery(ctx)
	if err != nil {
		t.Fatalf("RunRecovery failed: %v", err)
	}

	if dec.State != recovery.StateRecoveryNotRequired {
		t.Errorf("expected StateRecoveryNotRequired on clean reboot, got %s", dec.State)
	}
	if dec.RequiresIntervention {
		t.Errorf("expected RequiresIntervention=false")
	}
}

// TestRecovery_C_InFlightMitigations_TransitionToUnknown_BlocksRecovery verifies that
// mitigations in EXECUTING before restart are marked UNKNOWN_RECONCILIATION_REQUIRED,
// causing recovery to enter RECOVERY_BLOCKED and preventing readiness.
func TestRecovery_C_InFlightMitigations_TransitionToUnknown_BlocksRecovery(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC()
	mit := &storage.StoredMitigation{
		ActionID:   "act-inflight-1",
		DecisionID: "dec-inflight-1",
		IncidentID: "inc-inflight-1",
		NodeID:     "node-edge-1",
		ActionType: types.ActionSimulatedRestart,
		Target:     "service-a",
		Status:     types.MitigationStatusExecuting,
		StartedAt:  now,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err := store.RecordMitigation(ctx, mit); err != nil {
		t.Fatalf("failed to record mitigation: %v", err)
	}

	ht := health.NewTracker()
	_ = ht.RegisterCheck(health.CheckRecovery, true)
	metricRec := metrics.NewDefaultRecorder(nil)
	auditRec, _ := audit.NewService(store, audit.RealClock{})

	mgr, err := recovery.NewManager(recovery.Config{
		NodeID:          "node-edge-1",
		Store:           store,
		HealthTracker:   ht,
		AuditRecorder:   auditRec,
		MetricsRecorder: metricRec,
	})
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	dec, err := mgr.RunRecovery(ctx)
	if err != nil {
		t.Fatalf("RunRecovery unexpectedly returned error: %v", err)
	}

	// Must be blocked
	if dec.State != recovery.StateRecoveryBlocked {
		t.Errorf("expected StateRecoveryBlocked, got %s", dec.State)
	}
	if !dec.RequiresIntervention {
		t.Errorf("expected RequiresIntervention=true")
	}
	if mgr.Tracker().State() != recovery.StateRecoveryBlocked {
		t.Errorf("expected tracker StateRecoveryBlocked, got %s", mgr.Tracker().State())
	}
	if !mgr.Tracker().IsBlocked() {
		t.Errorf("expected tracker IsBlocked=true")
	}
	if mgr.Tracker().IsComplete() {
		t.Errorf("expected tracker IsComplete=false")
	}

	// Health status must be Degraded
	checkInfo, ok := ht.GetCheck(health.CheckRecovery)
	if !ok || checkInfo.Status != health.StatusDegraded {
		t.Errorf("expected recovery health status StatusDegraded, got ok=%v, status=%v", ok, checkInfo.Status)
	}

	// Invariant: Mitigation must NOT be executed or approved automatically!
	// It must be transitioned to UNKNOWN_RECONCILIATION_REQUIRED.
	savedMit, err := store.GetMitigation(ctx, "act-inflight-1")
	if err != nil {
		t.Fatalf("failed to retrieve mitigation: %v", err)
	}
	if savedMit.Status != types.MitigationStatusUnknownReconciliationRequired {
		t.Errorf("expected mitigation status UNKNOWN_RECONCILIATION_REQUIRED, got %s", savedMit.Status)
	}

	// Check metrics
	prom := metricRec.Registry().FormatPrometheus()
	if !strings.Contains(prom, `aegisedge_recovery_total{status="blocked"} 1`) {
		t.Errorf("expected blocked recovery metric, got:\n%s", prom)
	}
	if !strings.Contains(prom, `aegisedge_recovery_blocked_total 1`) {
		t.Errorf("expected recovery blocked counter, got:\n%s", prom)
	}
	if !strings.Contains(prom, `aegisedge_recovery_reconciliation_required_total 1`) {
		t.Errorf("expected reconciliation required metric, got:\n%s", prom)
	}

	// Check audit logs
	events, err := store.ListAuditEventsByIncident(ctx, "inc-inflight-1", 10)
	if err != nil {
		t.Fatalf("failed to list audit events: %v", err)
	}
	hasBlockedAudit := false
	for _, e := range events {
		if e.EventType == string(audit.EventTypeRecoveryBlocked) && e.Result == audit.ResultBlocked {
			hasBlockedAudit = true
			break
		}
	}
	if !hasBlockedAudit {
		t.Errorf("expected recovery_blocked audit event in audit log, found: %d events", len(events))
	}
}

// TestRecovery_D_PreExistingUnknownMitigation_BlocksRecovery verifies that any pre-existing
// UNKNOWN_RECONCILIATION_REQUIRED mitigation immediately blocks recovery.
func TestRecovery_D_PreExistingUnknownMitigation_BlocksRecovery(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC()
	mit := &storage.StoredMitigation{
		ActionID:   "act-unknown-1",
		DecisionID: "dec-unknown-1",
		IncidentID: "inc-unknown-1",
		NodeID:     "node-edge-1",
		ActionType: types.ActionSimulatedRestart,
		Target:     "eth0",
		Status:     types.MitigationStatusUnknownReconciliationRequired,
		StartedAt:  now,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err := store.RecordMitigation(ctx, mit); err != nil {
		t.Fatalf("failed to record mitigation: %v", err)
	}

	ht := health.NewTracker()
	_ = ht.RegisterCheck(health.CheckRecovery, true)
	mgr, _ := recovery.NewManager(recovery.Config{
		NodeID:        "node-edge-1",
		Store:         store,
		HealthTracker: ht,
	})

	dec, err := mgr.RunRecovery(ctx)
	if err != nil {
		t.Fatalf("RunRecovery failed: %v", err)
	}

	if dec.State != recovery.StateRecoveryBlocked || !dec.RequiresIntervention {
		t.Errorf("expected blocked decision, got state=%s, intervention=%v", dec.State, dec.RequiresIntervention)
	}
}

// TestRecovery_E_PendingMitigations_ReconciledToSkipped verifies that mitigations in PENDING
// prior to reboot are safely marked SKIPPED rather than executed automatically.
func TestRecovery_E_PendingMitigations_ReconciledToSkipped(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC()
	mit := &storage.StoredMitigation{
		ActionID:   "act-pending-1",
		DecisionID: "dec-pending-1",
		IncidentID: "inc-pending-1",
		NodeID:     "node-edge-1",
		ActionType: types.ActionSimulatedRestart,
		Target:     "nginx",
		Status:     types.MitigationStatusPending,
		StartedAt:  now,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	_ = store.RecordMitigation(ctx, mit)

	ht := health.NewTracker()
	_ = ht.RegisterCheck(health.CheckRecovery, true)
	metricRec := metrics.NewDefaultRecorder(nil)
	auditRec, _ := audit.NewService(store, audit.RealClock{})

	mgr, err := recovery.NewManager(recovery.Config{
		NodeID:          "node-edge-1",
		Store:           store,
		HealthTracker:   ht,
		AuditRecorder:   auditRec,
		MetricsRecorder: metricRec,
	})
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	dec, err := mgr.RunRecovery(ctx)
	if err != nil {
		t.Fatalf("RunRecovery failed: %v", err)
	}

	if dec.State != recovery.StateRecoveryComplete {
		t.Errorf("expected StateRecoveryComplete, got %s", dec.State)
	}
	if dec.MitigationsReconciled != 1 {
		t.Errorf("expected 1 mitigation reconciled, got %d", dec.MitigationsReconciled)
	}
	if dec.RequiresIntervention {
		t.Errorf("expected RequiresIntervention=false")
	}

	// Verify mitigation is now SKIPPED
	savedMit, err := store.GetMitigation(ctx, "act-pending-1")
	if err != nil {
		t.Fatalf("failed to retrieve mitigation: %v", err)
	}
	if savedMit.Status != types.MitigationStatusSkipped {
		t.Errorf("expected status SKIPPED, got %s", savedMit.Status)
	}
	if savedMit.ErrorCode != "RESTART_ABORTED" {
		t.Errorf("expected error code RESTART_ABORTED, got %s", savedMit.ErrorCode)
	}
}

// TestRecovery_F_InFlightVerifications_ExpiredOnRestart verifies stale verifications expire safely.
func TestRecovery_F_InFlightVerifications_ExpiredOnRestart(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC()
	// Save verification that expired 10 minutes ago
	verif := &storage.StoredVerification{
		VerificationID:       "ver-1",
		IncidentID:           "inc-verif-1",
		ActionID:             "act-verif-1",
		DecisionID:           "dec-verif-1",
		NodeID:               "node-edge-1",
		MetricName:           "cpu_usage_percent",
		ConditionType:        "THRESHOLD_UPPER",
		RecoveryThreshold:    70.0,
		Comparator:           "<=",
		Status:               types.VerificationStatusPending,
		RequiredObservations: 3,
		StartedAt:            now.Add(-20 * time.Minute),
		ExpiresAt:            now.Add(-10 * time.Minute),
		Reason:               "verification initiated",
		CreatedAt:            now.Add(-20 * time.Minute),
		UpdatedAt:            now.Add(-20 * time.Minute),
	}
	_ = store.RecordVerification(ctx, verif)

	ht := health.NewTracker()
	_ = ht.RegisterCheck(health.CheckRecovery, true)

	mgr, _ := recovery.NewManager(recovery.Config{
		NodeID:        "node-edge-1",
		Store:         store,
		HealthTracker: ht,
	})

	dec, err := mgr.RunRecovery(ctx)
	if err != nil {
		t.Fatalf("RunRecovery failed: %v", err)
	}

	if dec.State != recovery.StateRecoveryComplete {
		t.Errorf("expected StateRecoveryComplete, got %s", dec.State)
	}
	if dec.VerificationsTimedOut != 1 {
		t.Errorf("expected 1 verification timed out, got %d", dec.VerificationsTimedOut)
	}

	// Verify verification record is TIMED_OUT
	vList, err := store.ListVerifications(ctx, "inc-verif-1", 10)
	if err != nil || len(vList) == 0 {
		t.Fatalf("failed to list verifications: %v", err)
	}
	if vList[0].Status != types.VerificationStatusTimedOut {
		t.Errorf("expected status TIMED_OUT, got %s", vList[0].Status)
	}
}

// TestRecovery_G_MultipleOperations_MixedReconciliation tests complex reconciliation.
func TestRecovery_G_MultipleOperations_MixedReconciliation(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC()
	// 2 PENDING mitigations
	for i := 1; i <= 2; i++ {
		_ = store.RecordMitigation(ctx, &storage.StoredMitigation{
			ActionID:   "act-mix-" + string(rune('0'+i)),
			DecisionID: "dec-mix-" + string(rune('0'+i)),
			IncidentID: "inc-mix-1",
			NodeID:     "node-edge-1",
			ActionType: types.ActionSimulatedRestart,
			Target:     "svc",
			Status:     types.MitigationStatusPending,
			StartedAt:  now,
			CreatedAt:  now,
			UpdatedAt:  now,
		})
	}

	// 1 Expired verification
	_ = store.RecordVerification(ctx, &storage.StoredVerification{
		VerificationID:       "ver-mix-1",
		IncidentID:           "inc-mix-1",
		ActionID:             "act-mix-1",
		DecisionID:           "dec-mix",
		NodeID:               "node-edge-1",
		MetricName:           "cpu_usage_percent",
		ConditionType:        "THRESHOLD_UPPER",
		RecoveryThreshold:    70.0,
		Comparator:           "<=",
		Status:               types.VerificationStatusPending,
		RequiredObservations: 3,
		StartedAt:            now.Add(-10 * time.Minute),
		ExpiresAt:            now.Add(-5 * time.Minute),
		CreatedAt:            now.Add(-10 * time.Minute),
		UpdatedAt:            now.Add(-10 * time.Minute),
	})

	ht := health.NewTracker()
	_ = ht.RegisterCheck(health.CheckRecovery, true)

	mgr, _ := recovery.NewManager(recovery.Config{
		NodeID:        "node-edge-1",
		Store:         store,
		HealthTracker: ht,
	})

	dec, err := mgr.RunRecovery(ctx)
	if err != nil {
		t.Fatalf("RunRecovery failed: %v", err)
	}

	if dec.State != recovery.StateRecoveryComplete {
		t.Errorf("expected StateRecoveryComplete, got %s", dec.State)
	}
	if dec.MitigationsReconciled != 2 {
		t.Errorf("expected 2 mitigations reconciled, got %d", dec.MitigationsReconciled)
	}
	if dec.VerificationsTimedOut != 1 {
		t.Errorf("expected 1 verification timed out, got %d", dec.VerificationsTimedOut)
	}
}

// failingStore implements storage failure injection.
type failingStore struct {
	recovery.StorageBackend
	getCheckpointErr error
}

func (f *failingStore) SaveCheckpoint(ctx context.Context, cp *storage.StoredCheckpoint) error {
	return nil
}

func (f *failingStore) GetLatestCheckpoint(ctx context.Context, nodeID string) (*storage.StoredCheckpoint, error) {
	if f.getCheckpointErr != nil {
		return nil, f.getCheckpointErr
	}
	return nil, nil
}

func (f *failingStore) ListCheckpoints(ctx context.Context, nodeID string, limit int) ([]*storage.StoredCheckpoint, error) {
	return nil, nil
}

func (f *failingStore) RecoverInFlightMitigations(ctx context.Context) (int64, error) {
	return 0, nil
}

func (f *failingStore) ListMitigationsByStatus(ctx context.Context, status types.MitigationStatus, limit int) ([]*storage.StoredMitigation, error) {
	return nil, nil
}

func (f *failingStore) UpdateMitigationStatus(ctx context.Context, actionID string, status types.MitigationStatus, message, errCode string, completedAt *time.Time, durationMs int64) error {
	return nil
}

func (f *failingStore) RecoverInFlightVerifications(ctx context.Context, now time.Time) (int64, error) {
	return 0, nil
}

func (f *failingStore) ListVerifications(ctx context.Context, incidentID string, limit int) ([]*storage.StoredVerification, error) {
	return nil, nil
}

// TestRecovery_H_StorageFailure_TriggersRecoveryFailed verifies handling of storage failures.
func TestRecovery_H_StorageFailure_TriggersRecoveryFailed(t *testing.T) {
	ht := health.NewTracker()
	_ = ht.RegisterCheck(health.CheckRecovery, true)
	metricRec := metrics.NewDefaultRecorder(nil)

	fs := &failingStore{
		getCheckpointErr: errors.New("simulated disk i/o corruption"),
	}

	mgr, err := recovery.NewManager(recovery.Config{
		NodeID:          "node-edge-1",
		Store:           fs,
		HealthTracker:   ht,
		MetricsRecorder: metricRec,
	})
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	dec, err := mgr.RunRecovery(context.Background())
	if err == nil {
		t.Fatalf("expected RunRecovery to return error on storage failure, got nil (dec=%v)", dec)
	}

	if mgr.Tracker().State() != recovery.StateRecoveryFailed {
		t.Errorf("expected tracker StateRecoveryFailed, got %s", mgr.Tracker().State())
	}

	checkInfo, ok := ht.GetCheck(health.CheckRecovery)
	if !ok || checkInfo.Status != health.StatusFailed {
		t.Errorf("expected recovery check StatusFailed, got ok=%v, status=%v", ok, checkInfo.Status)
	}

	prom := metricRec.Registry().FormatPrometheus()
	if !strings.Contains(prom, `aegisedge_recovery_total{status="failed"} 1`) {
		t.Errorf("expected failed recovery metric, got:\n%s", prom)
	}
	if !strings.Contains(prom, `aegisedge_recovery_failed_total 1`) {
		t.Errorf("expected recovery_failed_total counter, got:\n%s", prom)
	}
}

// TestRecovery_I_Tracker_StateTransitions tests state transition validation.
func TestRecovery_I_Tracker_StateTransitions(t *testing.T) {
	tr := recovery.NewTracker()
	if tr.State() != recovery.StateRecoveryLoading {
		t.Fatalf("expected initial state StateRecoveryLoading, got %s", tr.State())
	}

	// Loading -> Reconciling (valid)
	if err := tr.Transition(recovery.StateRecoveryReconciling); err != nil {
		t.Fatalf("expected valid transition Loading -> Reconciling: %v", err)
	}

	// Reconciling -> Complete (valid)
	if err := tr.Transition(recovery.StateRecoveryComplete); err != nil {
		t.Fatalf("expected valid transition Reconciling -> Complete: %v", err)
	}

	// Complete -> Loading (valid on restart/reload)
	if err := tr.Transition(recovery.StateRecoveryLoading); err != nil {
		t.Fatalf("expected valid transition Complete -> Loading: %v", err)
	}

	// Loading -> Blocked (valid)
	if err := tr.Transition(recovery.StateRecoveryBlocked); err != nil {
		t.Fatalf("expected valid transition Loading -> Blocked: %v", err)
	}

	// Blocked -> Complete (invalid without Reconciling)
	if err := tr.Transition(recovery.StateRecoveryComplete); err == nil {
		t.Fatal("expected error on invalid transition Blocked -> Complete")
	}

	// Blocked -> Reconciling (valid)
	if err := tr.Transition(recovery.StateRecoveryReconciling); err != nil {
		t.Fatalf("expected valid transition Blocked -> Reconciling: %v", err)
	}

	// Reconciling -> Failed (valid)
	if err := tr.Transition(recovery.StateRecoveryFailed); err != nil {
		t.Fatalf("expected valid transition Reconciling -> Failed: %v", err)
	}

	// Failed -> Blocked (invalid)
	if err := tr.Transition(recovery.StateRecoveryBlocked); err == nil {
		t.Fatal("expected error on invalid transition Failed -> Blocked")
	}
}

// TestRecovery_J_Tracker_ConcurrentSafety verifies thread-safe tracker access.
func TestRecovery_J_Tracker_ConcurrentSafety(t *testing.T) {
	tr := recovery.NewTracker()
	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = tr.State()
				_ = tr.IsComplete()
				_ = tr.IsBlocked()
				_ = tr.Reason()
				tr.SetReason("concurrency test")
			}
		}(i)
	}

	wg.Wait()
}

// TestRecovery_K_ManagerConfigValidation tests input validation during NewManager.
func TestRecovery_K_ManagerConfigValidation(t *testing.T) {
	store := setupTestStore(t)

	// Empty node ID
	_, err := recovery.NewManager(recovery.Config{
		NodeID: "",
		Store:  store,
	})
	if err == nil {
		t.Fatal("expected error on empty NodeID")
	}

	// Nil store
	_, err = recovery.NewManager(recovery.Config{
		NodeID: "node-1",
		Store:  nil,
	})
	if err == nil {
		t.Fatal("expected error on nil Store")
	}
}

// TestRecovery_L_EndToEnd_ReadinessBehavior tests integration with health readiness.
func TestRecovery_L_EndToEnd_ReadinessBehavior(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC()
	mit := &storage.StoredMitigation{
		ActionID:   "act-readiness-1",
		DecisionID: "dec-readiness-1",
		IncidentID: "inc-readiness-1",
		NodeID:     "node-edge-1",
		ActionType: types.ActionSimulatedRestart,
		Target:     "eth0",
		Status:     types.MitigationStatusExecuting,
		StartedAt:  now,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	_ = store.RecordMitigation(ctx, mit)

	ht := health.NewTracker()
	_ = ht.RegisterCheck(health.CheckStorage, true)
	_ = ht.RegisterCheck(health.CheckRecovery, true)

	// Set storage check OK
	_ = ht.SetCheckStatus(health.CheckStorage, health.StatusOk, "storage ready")

	mgr, _ := recovery.NewManager(recovery.Config{
		NodeID:        "node-edge-1",
		Store:         store,
		HealthTracker: ht,
	})

	dec, _ := mgr.RunRecovery(ctx)
	if dec.State != recovery.StateRecoveryBlocked {
		t.Fatalf("expected StateRecoveryBlocked, got %s", dec.State)
	}

	// Attempt transition to StateReady
	err := ht.Transition(health.StateReady)
	if err == nil && ht.IsReady() {
		t.Fatalf("readiness must NOT be true when recovery is blocked!")
	}
}

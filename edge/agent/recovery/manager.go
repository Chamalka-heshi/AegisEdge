package recovery

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Chamalka-heshi/AegisEdge/edge/agent/audit"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/health"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/metrics"
	"github.com/Chamalka-heshi/AegisEdge/edge/agent/storage"
	"github.com/Chamalka-heshi/AegisEdge/shared/types"
)

// StorageBackend defines the persistence subset required for persistent runtime recovery.
type StorageBackend interface {
	SaveCheckpoint(ctx context.Context, cp *storage.StoredCheckpoint) error
	GetLatestCheckpoint(ctx context.Context, nodeID string) (*storage.StoredCheckpoint, error)
	ListCheckpoints(ctx context.Context, nodeID string, limit int) ([]*storage.StoredCheckpoint, error)
	RecoverInFlightMitigations(ctx context.Context) (int64, error)
	ListMitigationsByStatus(ctx context.Context, status types.MitigationStatus, limit int) ([]*storage.StoredMitigation, error)
	UpdateMitigationStatus(ctx context.Context, actionID string, status types.MitigationStatus, message, errCode string, completedAt *time.Time, durationMs int64) error
	RecoverInFlightVerifications(ctx context.Context, now time.Time) (int64, error)
	ListVerifications(ctx context.Context, incidentID string, limit int) ([]*storage.StoredVerification, error)
}

// AuditRecorder defines the audit persistence subset required for recovery audit records.
type AuditRecorder interface {
	Record(ctx context.Context, event audit.AuditEvent) error
}

type noopAuditRecorder struct{}

func (noopAuditRecorder) Record(ctx context.Context, event audit.AuditEvent) error {
	return nil
}

// Decision represents the structured outcome of a recovery reconciliation evaluation.
type Decision struct {
	State                    State  `json:"state"`
	RequiresIntervention     bool   `json:"requires_intervention"`
	Reason                   string `json:"reason"`
	InFlightMitigationsFound int64  `json:"in_flight_mitigations_found"`
	MitigationsReconciled    int    `json:"mitigations_reconciled"`
	VerificationsTimedOut    int64  `json:"verifications_timed_out"`
	CheckpointID             string `json:"checkpoint_id"`
}

// Manager coordinates the restart recovery process according to the Phase 6.13 safety invariant:
// RESTART MUST NEVER BY ITSELF AUTHORIZE ACTION EXECUTION.
type Manager struct {
	nodeID    string
	store     StorageBackend
	tracker   Tracker
	health    health.Tracker
	auditRec  AuditRecorder
	metricRec metrics.Recorder
}

// Config specifies dependencies for the recovery Manager.
type Config struct {
	NodeID          string
	Store           StorageBackend
	Tracker         Tracker
	HealthTracker   health.Tracker
	AuditRecorder   AuditRecorder
	MetricsRecorder metrics.Recorder
}

// NewManager constructs a recovery Manager.
func NewManager(cfg Config) (*Manager, error) {
	if strings.TrimSpace(cfg.NodeID) == "" {
		return nil, fmt.Errorf("node_id cannot be empty")
	}
	if cfg.Store == nil {
		return nil, fmt.Errorf("storage backend cannot be nil")
	}
	if cfg.Tracker == nil {
		cfg.Tracker = NewTracker()
	}
	if cfg.AuditRecorder == nil {
		cfg.AuditRecorder = noopAuditRecorder{}
	}
	if cfg.MetricsRecorder == nil {
		cfg.MetricsRecorder = metrics.NoopRecorder{}
	}

	return &Manager{
		nodeID:    cfg.NodeID,
		store:     cfg.Store,
		tracker:   cfg.Tracker,
		health:    cfg.HealthTracker,
		auditRec:  cfg.AuditRecorder,
		metricRec: cfg.MetricsRecorder,
	}, nil
}

// Tracker returns the active recovery lifecycle tracker.
func (m *Manager) Tracker() Tracker {
	return m.tracker
}

// RunRecovery executes the startup reconciliation pipeline.
// Invariant: An interrupted operation must be reconciled safely before the agent becomes READY.
// The system MUST NOT interpret a restart as permission to automatically re-execute an incomplete or unknown mitigation.
func (m *Manager) RunRecovery(ctx context.Context) (*Decision, error) {
	startTime := time.Now().UTC()

	// 1. Audit recovery startup
	_ = m.auditRec.Record(ctx, audit.AuditEvent{
		EventType:  audit.EventTypeRecoveryStarted,
		NodeID:     m.nodeID,
		IncidentID: "system-recovery",
		Result:     audit.ResultSuccess,
		Reason:     "agent runtime restart recovery initiated",
		Timestamp:  startTime,
	})

	// 2. Load latest persisted runtime checkpoint
	latestCP, err := m.store.GetLatestCheckpoint(ctx, m.nodeID)
	if err != nil {
		m.failRecovery("failed to load latest runtime checkpoint: " + err.Error())
		return nil, fmt.Errorf("recovery load failed: %w", err)
	}

	if latestCP != nil {
		_ = m.auditRec.Record(ctx, audit.AuditEvent{
			EventType:  audit.EventTypeRecoveryStateLoaded,
			NodeID:     m.nodeID,
			IncidentID: "system-recovery",
			Result:     audit.ResultSuccess,
			Reason:     fmt.Sprintf("checkpoint %s loaded with state %s", latestCP.CheckpointID, latestCP.State),
			Metadata: map[string]string{
				"checkpoint_id": latestCP.CheckpointID,
				"state":         latestCP.State,
			},
			Timestamp: time.Now().UTC(),
		})
	}

	// 3. Reconcile in-flight mitigations (EXECUTING -> UNKNOWN_RECONCILIATION_REQUIRED)
	inFlightMitigations, err := m.store.RecoverInFlightMitigations(ctx)
	if err != nil {
		m.failRecovery("failed to recover in-flight mitigations: " + err.Error())
		return nil, fmt.Errorf("in-flight mitigation recovery failed: %w", err)
	}

	// 4. Check for any mitigations in UNKNOWN_RECONCILIATION_REQUIRED
	unknownMitigations, err := m.store.ListMitigationsByStatus(ctx, types.MitigationStatusUnknownReconciliationRequired, 100)
	if err != nil {
		m.failRecovery("failed to list unknown mitigations: " + err.Error())
		return nil, fmt.Errorf("listing unknown mitigations failed: %w", err)
	}

	if len(unknownMitigations) > 0 {
		// INVARIANT: An ambiguous unconfirmed execution CANNOT be automatically re-executed or treated as success.
		// Recovery enters RECOVERY_BLOCKED and readiness remains false.
		blockedReason := fmt.Sprintf("%d mitigation(s) in UNKNOWN_RECONCILIATION_REQUIRED require manual operator reconciliation", len(unknownMitigations))
		m.metricRec.RecordRecoveryReconciliationRequired()
		m.metricRec.RecordRecoveryBlocked()
		m.metricRec.RecordRecovery("blocked")

		_ = m.auditRec.Record(ctx, audit.AuditEvent{
			EventType:  audit.EventTypeRecoveryReconciliationRequired,
			NodeID:     m.nodeID,
			IncidentID: unknownMitigations[0].IncidentID,
			ActionID:   unknownMitigations[0].ActionID,
			DecisionID: unknownMitigations[0].DecisionID,
			Result:     audit.ResultBlocked,
			Reason:     blockedReason,
			Timestamp:  time.Now().UTC(),
		})

		_ = m.auditRec.Record(ctx, audit.AuditEvent{
			EventType:  audit.EventTypeRecoveryBlocked,
			NodeID:     m.nodeID,
			IncidentID: unknownMitigations[0].IncidentID,
			Result:     audit.ResultBlocked,
			Reason:     blockedReason,
			Timestamp:  time.Now().UTC(),
		})

		// Transition tracker: RECOVERY_LOADING -> RECOVERY_BLOCKED
		if err := m.tracker.Transition(StateRecoveryBlocked); err != nil {
			_ = m.tracker.Transition(StateRecoveryReconciling)
			_ = m.tracker.Transition(StateRecoveryBlocked)
		}
		m.tracker.SetReason(blockedReason)

		if m.health != nil {
			_ = m.health.SetCheckStatus(health.CheckRecovery, health.StatusDegraded, "recovery blocked: "+blockedReason)
		}

		cpID := fmt.Sprintf("cp-%s-%d", m.nodeID, startTime.UnixNano())
		cpRecord := &storage.StoredCheckpoint{
			CheckpointID:          cpID,
			NodeID:                m.nodeID,
			State:                 string(StateRecoveryBlocked),
			StartedAt:             startTime,
			RecoveryReason:        blockedReason,
			LastReconciledAttempt: 1,
			SchemaVersion:         9,
			CreatedAt:             startTime,
			UpdatedAt:             time.Now().UTC(),
		}
		_ = m.store.SaveCheckpoint(ctx, cpRecord)

		return &Decision{
			State:                    StateRecoveryBlocked,
			RequiresIntervention:     true,
			Reason:                   blockedReason,
			InFlightMitigationsFound: inFlightMitigations,
			CheckpointID:             cpID,
		}, nil
	}

	// 5. Inspect PENDING mitigations that were interrupted before starting
	pendingMitigations, err := m.store.ListMitigationsByStatus(ctx, types.MitigationStatusPending, 100)
	if err != nil {
		m.failRecovery("failed to list pending mitigations: " + err.Error())
		return nil, fmt.Errorf("listing pending mitigations failed: %w", err)
	}

	mitigationsReconciled := 0
	if len(pendingMitigations) > 0 {
		if err := m.tracker.Transition(StateRecoveryReconciling); err != nil {
			// ignore if already in reconciling
		}

		for _, p := range pendingMitigations {
			now := time.Now().UTC()
			reason := "interrupted by agent restart prior to execution"
			if err := m.store.UpdateMitigationStatus(
				ctx,
				p.ActionID,
				types.MitigationStatusSkipped,
				reason,
				"RESTART_ABORTED",
				&now,
				0,
			); err != nil {
				m.failRecovery("failed to reconcile pending mitigation: " + err.Error())
				return nil, fmt.Errorf("reconciling pending mitigation %s failed: %w", p.ActionID, err)
			}
			mitigationsReconciled++

			_ = m.auditRec.Record(ctx, audit.AuditEvent{
				EventType:  audit.EventTypeRecoveryReconciliationCompleted,
				NodeID:     m.nodeID,
				IncidentID: p.IncidentID,
				ActionID:   p.ActionID,
				DecisionID: p.DecisionID,
				Result:     audit.ResultSkipped,
				Reason:     reason,
				Timestamp:  time.Now().UTC(),
			})
		}
	}

	// 6. Inspect and expire timed-out verifications
	timedOutVerifs, err := m.store.RecoverInFlightVerifications(ctx, time.Now().UTC())
	if err != nil {
		m.failRecovery("failed to recover in-flight verifications: " + err.Error())
		return nil, fmt.Errorf("recovering in-flight verifications failed: %w", err)
	}

	// 7. Establish completion state
	cpID := fmt.Sprintf("cp-%s-%d", m.nodeID, startTime.UnixNano())
	now := time.Now().UTC()
	var finalState State
	var finalReason string

	if inFlightMitigations == 0 && mitigationsReconciled == 0 && timedOutVerifs == 0 &&
		(latestCP == nil || latestCP.State == string(StateRecoveryNotRequired) || latestCP.State == string(StateRecoveryComplete)) {
		finalState = StateRecoveryNotRequired
		finalReason = "clean startup: no in-flight or ambiguous operations found"
	} else {
		finalState = StateRecoveryComplete
		finalReason = fmt.Sprintf("recovery complete: %d in-flight, %d pending reconciled, %d verifications timed out",
			inFlightMitigations, mitigationsReconciled, timedOutVerifs)
	}

	// Advance tracker to final state
	if m.tracker.State() == StateRecoveryLoading {
		if err := m.tracker.Transition(finalState); err != nil {
			_ = m.tracker.Transition(StateRecoveryReconciling)
			_ = m.tracker.Transition(finalState)
		}
	} else if m.tracker.State() == StateRecoveryReconciling {
		if finalState == StateRecoveryNotRequired {
			_ = m.tracker.Transition(StateRecoveryComplete)
			finalState = StateRecoveryComplete
		} else {
			_ = m.tracker.Transition(finalState)
		}
	}
	m.tracker.SetReason(finalReason)

	// Persist checkpoint to SQLite WAL
	cpRecord := &storage.StoredCheckpoint{
		CheckpointID:          cpID,
		NodeID:                m.nodeID,
		State:                 string(finalState),
		StartedAt:             startTime,
		CompletedAt:           &now,
		RecoveryReason:        finalReason,
		LastReconciledAttempt: 1,
		SchemaVersion:         9,
		CreatedAt:             startTime,
		UpdatedAt:             now,
	}
	if err := m.store.SaveCheckpoint(ctx, cpRecord); err != nil {
		m.failRecovery("failed to save completion checkpoint: " + err.Error())
		return nil, fmt.Errorf("saving checkpoint failed: %w", err)
	}

	// Update health check
	if m.health != nil {
		_ = m.health.SetCheckStatus(health.CheckRecovery, health.StatusOk, "completed")
	}

	// Record audit completion
	_ = m.auditRec.Record(ctx, audit.AuditEvent{
		EventType:  audit.EventTypeRecoveryCompleted,
		NodeID:     m.nodeID,
		IncidentID: "system-recovery",
		Result:     audit.ResultSuccess,
		Reason:     finalReason,
		Metadata: map[string]string{
			"checkpoint_id": cpID,
			"state":         string(finalState),
		},
		Timestamp: now,
	})

	// Record metrics
	duration := time.Since(startTime)
	m.metricRec.ObserveRecoveryDuration(duration)
	if finalState == StateRecoveryNotRequired {
		m.metricRec.RecordRecovery("not_required")
	} else {
		m.metricRec.RecordRecovery("completed")
	}

	return &Decision{
		State:                    finalState,
		RequiresIntervention:     false,
		Reason:                   finalReason,
		InFlightMitigationsFound: inFlightMitigations,
		MitigationsReconciled:    mitigationsReconciled,
		VerificationsTimedOut:    timedOutVerifs,
		CheckpointID:             cpID,
	}, nil
}

func (m *Manager) failRecovery(reason string) {
	if m.tracker.State() == StateRecoveryLoading {
		_ = m.tracker.Transition(StateRecoveryFailed)
	} else if m.tracker.State() == StateRecoveryReconciling {
		_ = m.tracker.Transition(StateRecoveryFailed)
	}
	m.tracker.SetReason(reason)

	if m.health != nil {
		_ = m.health.SetCheckStatus(health.CheckRecovery, health.StatusFailed, "failed: "+reason)
	}

	m.metricRec.RecordRecoveryFailed()
	m.metricRec.RecordRecovery("failed")
}

package metrics

import (
	"strings"
	"time"
)

// Metric names as per Phase 6.10 specifications.
const (
	// Telemetry
	MetricTelemetryBatchesTotal       = "aegisedge_telemetry_batches_total"
	MetricTelemetryBatchesFailedTotal = "aegisedge_telemetry_batches_failed_total"

	// Anomaly Detection
	MetricAnomaliesDetectedTotal = "aegisedge_anomalies_detected_total"
	MetricDetectorErrorsTotal    = "aegisedge_detector_errors_total"
	MetricDetectorDurationSec    = "aegisedge_detector_duration_seconds"

	// Incidents
	MetricIncidentsCreatedTotal   = "aegisedge_incidents_created_total"
	MetricIncidentsRecoveredTotal = "aegisedge_incidents_recovered_total"
	MetricIncidentsEscalatedTotal = "aegisedge_incidents_escalated_total"
	MetricActiveIncidents         = "aegisedge_active_incidents"

	// Response Policy
	MetricResponseDecisionsTotal  = "aegisedge_response_decisions_total"
	MetricResponseNoActionTotal   = "aegisedge_response_no_action_total"
	MetricResponseRejectionsTotal = "aegisedge_response_rejections_total"

	// Safety Validation
	MetricSafetyValidationsTotal = "aegisedge_safety_validations_total"
	MetricSafetyRejectionsTotal  = "aegisedge_safety_rejections_total"

	// Operator Approval
	MetricApprovalsTotal = "aegisedge_approvals_total"

	// Mitigation Execution
	MetricMitigationsTotal      = "aegisedge_mitigations_total"
	MetricMitigationDurationSec = "aegisedge_mitigation_duration_seconds"

	// Telemetry Verification
	MetricVerificationsTotal      = "aegisedge_verifications_total"
	MetricVerificationDurationSec = "aegisedge_verification_duration_seconds"

	// Escalation & Circuit Breaker
	MetricEscalationsTotal          = "aegisedge_escalations_total"
	MetricRetriesTotal              = "aegisedge_retries_total"
	MetricCircuitBreakerEventsTotal = "aegisedge_circuit_breaker_events_total"

	// Response Orchestrator
	MetricOrchestrationsTotal      = "aegisedge_orchestrations_total"
	MetricOrchestrationDurationSec = "aegisedge_orchestration_duration_seconds"

	// Audit Trail
	MetricAuditEventsTotal      = "aegisedge_audit_events_total"
	MetricAuditWriteErrorsTotal = "aegisedge_audit_write_errors_total"
	MetricAuditDuplicatesTotal  = "aegisedge_audit_duplicates_total"

	// Shutdown & Lifecycle
	MetricShutdownsTotal        = "aegisedge_shutdowns_total"
	MetricShutdownDurationSec   = "aegisedge_shutdown_duration_seconds"
	MetricShutdownTimeoutsTotal = "aegisedge_shutdown_timeouts_total"

	// Persistent Runtime Recovery (Phase 6.13)
	MetricRecoveryTotal                       = "aegisedge_recovery_total"
	MetricRecoveryDurationSec                 = "aegisedge_recovery_duration_seconds"
	MetricRecoveryReconciliationRequiredTotal = "aegisedge_recovery_reconciliation_required_total"
	MetricRecoveryBlockedTotal                = "aegisedge_recovery_blocked_total"
	MetricRecoveryFailedTotal                 = "aegisedge_recovery_failed_total"
)

// Recorder defines the domain contract for recording lifecycle metrics.
type Recorder interface {
	// Telemetry
	RecordTelemetryBatch(success bool)

	// Anomaly Detection
	RecordAnomalyDetected(method string)
	RecordDetectorError(method string)
	ObserveDetectorDuration(method string, d time.Duration)

	// Incidents
	RecordIncidentCreated()
	RecordIncidentRecovered()
	RecordIncidentEscalated()
	SetActiveIncidents(count int)
	IncActiveIncidents()
	DecActiveIncidents()

	// Response Policy
	RecordResponseDecision(authClass string)
	RecordResponseNoAction()
	RecordResponseRejection()

	// Safety Validation
	RecordSafetyValidation(passed bool)
	RecordSafetyRejection(reason string)

	// Operator Approval
	RecordApproval(status string)

	// Mitigation Execution
	RecordMitigation(actionType string, status string)
	ObserveMitigationDuration(actionType string, d time.Duration)

	// Telemetry Verification
	RecordVerification(status string)
	ObserveVerificationDuration(d time.Duration)

	// Escalation & Circuit Breaker
	RecordEscalation(classification string)
	RecordRetry()
	RecordCircuitBreakerEvent(event string)

	// Response Orchestrator
	RecordOrchestration(outcome string)
	ObserveOrchestrationDuration(d time.Duration)

	// Audit Trail
	RecordAuditEvent(eventType string)
	RecordAuditWriteError()
	RecordAuditDuplicate()

	// Shutdown & Lifecycle
	RecordShutdown(status string)
	ObserveShutdownDuration(d time.Duration)
	RecordShutdownTimeout()

	// Persistent Runtime Recovery (Phase 6.13)
	RecordRecovery(status string)
	ObserveRecoveryDuration(d time.Duration)
	RecordRecoveryReconciliationRequired()
	RecordRecoveryBlocked()
	RecordRecoveryFailed()

	// Registry access
	Registry() *Registry
}

// DefaultRecorder implements Recorder backed by Registry.
type DefaultRecorder struct {
	reg *Registry

	// Telemetry
	telemetryBatches       *SingleCounter
	telemetryBatchesFailed *SingleCounter

	// Anomaly Detection
	anomaliesDetected *CounterVec
	detectorErrors    *CounterVec
	detectorDuration  *HistogramVec

	// Incidents
	incidentsCreated   *SingleCounter
	incidentsRecovered *SingleCounter
	incidentsEscalated *SingleCounter
	activeIncidents    *SingleGauge

	// Response Policy
	responseDecisions  *CounterVec
	responseNoAction   *SingleCounter
	responseRejections *SingleCounter

	// Safety Validation
	safetyValidations *CounterVec
	safetyRejections  *CounterVec

	// Operator Approval
	approvals *CounterVec

	// Mitigation Execution
	mitigations        *CounterVec
	mitigationDuration *HistogramVec

	// Telemetry Verification
	verifications        *CounterVec
	verificationDuration *SingleHistogram

	// Escalation & Circuit Breaker
	escalations          *CounterVec
	retries              *SingleCounter
	circuitBreakerEvents *CounterVec

	// Response Orchestrator
	orchestrations        *CounterVec
	orchestrationDuration *SingleHistogram

	// Audit Trail
	auditEvents      *CounterVec
	auditWriteErrors *SingleCounter
	auditDuplicates  *SingleCounter

	// Shutdown & Lifecycle
	shutdowns        *CounterVec
	shutdownDuration *SingleHistogram
	shutdownTimeouts *SingleCounter

	// Persistent Runtime Recovery (Phase 6.13)
	recovery                       *CounterVec
	recoveryDuration               *SingleHistogram
	recoveryReconciliationRequired *SingleCounter
	recoveryBlocked                *SingleCounter
	recoveryFailed                 *SingleCounter
}

// NewDefaultRecorder constructs and initializes all domain metrics in the provided Registry.
// If reg is nil, a new Registry is created.
func NewDefaultRecorder(reg *Registry) *DefaultRecorder {
	if reg == nil {
		reg = NewRegistry()
	}

	rec := &DefaultRecorder{
		reg: reg,

		// Telemetry
		telemetryBatches:       NewSingleCounter(MetricTelemetryBatchesTotal, "Total number of telemetry batches generated and processed."),
		telemetryBatchesFailed: NewSingleCounter(MetricTelemetryBatchesFailedTotal, "Total number of telemetry batches that failed persistence or generation."),

		// Anomaly Detection
		anomaliesDetected: NewCounterVec(MetricAnomaliesDetectedTotal, "Total number of anomaly signals detected by method.", []string{"method"}),
		detectorErrors:    NewCounterVec(MetricDetectorErrorsTotal, "Total number of detector evaluation errors by method.", []string{"method"}),
		detectorDuration:  NewHistogramVec(MetricDetectorDurationSec, "Latency distribution of anomaly detection by method in seconds.", []string{"method"}, DefaultDurationBuckets),

		// Incidents
		incidentsCreated:   NewSingleCounter(MetricIncidentsCreatedTotal, "Total number of new incidents correlated and created."),
		incidentsRecovered: NewSingleCounter(MetricIncidentsRecoveredTotal, "Total number of active incidents recovered."),
		incidentsEscalated: NewSingleCounter(MetricIncidentsEscalatedTotal, "Total number of incidents transitioned to ESCALATED."),
		activeIncidents:    NewSingleGauge(MetricActiveIncidents, "Current count of active incidents on the local node."),

		// Response Policy
		responseDecisions:  NewCounterVec(MetricResponseDecisionsTotal, "Total response policy decisions created by authorization class.", []string{"authorization_class"}),
		responseNoAction:   NewSingleCounter(MetricResponseNoActionTotal, "Total policy evaluations producing no action."),
		responseRejections: NewSingleCounter(MetricResponseRejectionsTotal, "Total decisions rejected by response policy or forbidden actions."),

		// Safety Validation
		safetyValidations: NewCounterVec(MetricSafetyValidationsTotal, "Total safety validation evaluations by outcome (passed or rejected).", []string{"outcome"}),
		safetyRejections:  NewCounterVec(MetricSafetyRejectionsTotal, "Total safety rejections categorized by bounded reason code.", []string{"reason"}),

		// Operator Approval
		approvals: NewCounterVec(MetricApprovalsTotal, "Total operator approval ticket lifecycle events by status.", []string{"status"}),

		// Mitigation Execution
		mitigations:        NewCounterVec(MetricMitigationsTotal, "Total simulated mitigation executions partitioned by action type and status.", []string{"action_type", "status"}),
		mitigationDuration: NewHistogramVec(MetricMitigationDurationSec, "Latency distribution of simulated mitigation execution in seconds.", []string{"action_type"}, DefaultDurationBuckets),

		// Telemetry Verification
		verifications:        NewCounterVec(MetricVerificationsTotal, "Total verification attempts partitioned by terminal status.", []string{"status"}),
		verificationDuration: NewSingleHistogram(MetricVerificationDurationSec, "Duration of verification observation cycles in seconds.", DefaultDurationBuckets),

		// Escalation & Circuit Breaker
		escalations:          NewCounterVec(MetricEscalationsTotal, "Total escalation engine evaluations by failure classification.", []string{"classification"}),
		retries:              NewSingleCounter(MetricRetriesTotal, "Total retry decisions authorized by escalation engine."),
		circuitBreakerEvents: NewCounterVec(MetricCircuitBreakerEventsTotal, "Circuit breaker state transition events (opened or reset).", []string{"event"}),

		// Response Orchestrator
		orchestrations:        NewCounterVec(MetricOrchestrationsTotal, "Total orchestration cycles by final outcome (completed, stopped, failed).", []string{"outcome"}),
		orchestrationDuration: NewSingleHistogram(MetricOrchestrationDurationSec, "Duration of complete incident response orchestration cycles in seconds.", DefaultDurationBuckets),

		// Audit Trail
		auditEvents:      NewCounterVec(MetricAuditEventsTotal, "Total audit events successfully recorded by allowlisted event type.", []string{"event_type"}),
		auditWriteErrors: NewSingleCounter(MetricAuditWriteErrorsTotal, "Total audit event persistence failures."),
		auditDuplicates:  NewSingleCounter(MetricAuditDuplicatesTotal, "Total duplicate audit events idempotently ignored."),

		// Shutdown & Lifecycle
		shutdowns:        NewCounterVec(MetricShutdownsTotal, "Total agent shutdown sequences initiated by terminal status.", []string{"status"}),
		shutdownDuration: NewSingleHistogram(MetricShutdownDurationSec, "Duration of graceful agent shutdown sequences in seconds.", DefaultDurationBuckets),
		shutdownTimeouts: NewSingleCounter(MetricShutdownTimeoutsTotal, "Total number of graceful shutdown attempts that exceeded the configured timeout."),

		// Persistent Runtime Recovery (Phase 6.13)
		recovery:                       NewCounterVec(MetricRecoveryTotal, "Total runtime recovery attempts by terminal status.", []string{"status"}),
		recoveryDuration:               NewSingleHistogram(MetricRecoveryDurationSec, "Duration of runtime recovery evaluation in seconds.", DefaultDurationBuckets),
		recoveryReconciliationRequired: NewSingleCounter(MetricRecoveryReconciliationRequiredTotal, "Total runtime recovery attempts discovering records requiring reconciliation."),
		recoveryBlocked:                NewSingleCounter(MetricRecoveryBlockedTotal, "Total runtime recovery attempts resulting in blocked readiness."),
		recoveryFailed:                 NewSingleCounter(MetricRecoveryFailedTotal, "Total runtime recovery attempts that encountered unrecoverable errors."),
	}

	// Register all metrics into registry
	_ = reg.Register(rec.telemetryBatches)
	_ = reg.Register(rec.telemetryBatchesFailed)
	_ = reg.Register(rec.anomaliesDetected)
	_ = reg.Register(rec.detectorErrors)
	_ = reg.Register(rec.detectorDuration)
	_ = reg.Register(rec.incidentsCreated)
	_ = reg.Register(rec.incidentsRecovered)
	_ = reg.Register(rec.incidentsEscalated)
	_ = reg.Register(rec.activeIncidents)
	_ = reg.Register(rec.responseDecisions)
	_ = reg.Register(rec.responseNoAction)
	_ = reg.Register(rec.responseRejections)
	_ = reg.Register(rec.safetyValidations)
	_ = reg.Register(rec.safetyRejections)
	_ = reg.Register(rec.approvals)
	_ = reg.Register(rec.mitigations)
	_ = reg.Register(rec.mitigationDuration)
	_ = reg.Register(rec.verifications)
	_ = reg.Register(rec.verificationDuration)
	_ = reg.Register(rec.escalations)
	_ = reg.Register(rec.retries)
	_ = reg.Register(rec.circuitBreakerEvents)
	_ = reg.Register(rec.orchestrations)
	_ = reg.Register(rec.orchestrationDuration)
	_ = reg.Register(rec.auditEvents)
	_ = reg.Register(rec.auditWriteErrors)
	_ = reg.Register(rec.auditDuplicates)
	_ = reg.Register(rec.shutdowns)
	_ = reg.Register(rec.shutdownDuration)
	_ = reg.Register(rec.shutdownTimeouts)
	_ = reg.Register(rec.recovery)
	_ = reg.Register(rec.recoveryDuration)
	_ = reg.Register(rec.recoveryReconciliationRequired)
	_ = reg.Register(rec.recoveryBlocked)
	_ = reg.Register(rec.recoveryFailed)

	return rec
}

func (r *DefaultRecorder) Registry() *Registry {
	return r.reg
}

// ----------------------------------------------------------------------------
// Domain Recording Methods (with Cardinality Safety sanitization)
// ----------------------------------------------------------------------------

func (r *DefaultRecorder) RecordTelemetryBatch(success bool) {
	if success {
		r.telemetryBatches.Inc()
	} else {
		r.telemetryBatchesFailed.Inc()
	}
}

func (r *DefaultRecorder) RecordAnomalyDetected(method string) {
	m := SanitizeMethod(method)
	r.anomaliesDetected.WithLabelValues(m).Inc()
}

func (r *DefaultRecorder) RecordDetectorError(method string) {
	m := SanitizeMethod(method)
	r.detectorErrors.WithLabelValues(m).Inc()
}

func (r *DefaultRecorder) ObserveDetectorDuration(method string, d time.Duration) {
	m := SanitizeMethod(method)
	r.detectorDuration.WithLabelValues(m).Observe(d.Seconds())
}

func (r *DefaultRecorder) RecordIncidentCreated() {
	r.incidentsCreated.Inc()
	r.activeIncidents.Inc()
}

func (r *DefaultRecorder) RecordIncidentRecovered() {
	r.incidentsRecovered.Inc()
	r.activeIncidents.Dec()
}

func (r *DefaultRecorder) RecordIncidentEscalated() {
	r.incidentsEscalated.Inc()
}

func (r *DefaultRecorder) SetActiveIncidents(count int) {
	if count < 0 {
		count = 0
	}
	r.activeIncidents.Set(float64(count))
}

func (r *DefaultRecorder) IncActiveIncidents() {
	r.activeIncidents.Inc()
}

func (r *DefaultRecorder) DecActiveIncidents() {
	r.activeIncidents.Dec()
}

func (r *DefaultRecorder) RecordResponseDecision(authClass string) {
	ac := SanitizeAuthClass(authClass)
	r.responseDecisions.WithLabelValues(ac).Inc()
}

func (r *DefaultRecorder) RecordResponseNoAction() {
	r.responseNoAction.Inc()
}

func (r *DefaultRecorder) RecordResponseRejection() {
	r.responseRejections.Inc()
}

func (r *DefaultRecorder) RecordSafetyValidation(passed bool) {
	outcome := "passed"
	if !passed {
		outcome = "rejected"
	}
	r.safetyValidations.WithLabelValues(outcome).Inc()
}

func (r *DefaultRecorder) RecordSafetyRejection(reason string) {
	res := SanitizeSafetyReason(reason)
	r.safetyRejections.WithLabelValues(res).Inc()
}

func (r *DefaultRecorder) RecordApproval(status string) {
	st := SanitizeApprovalStatus(status)
	r.approvals.WithLabelValues(st).Inc()
}

func (r *DefaultRecorder) RecordMitigation(actionType string, status string) {
	at := SanitizeActionType(actionType)
	st := SanitizeMitigationStatus(status)
	r.mitigations.WithLabelValues(at, st).Inc()
}

func (r *DefaultRecorder) ObserveMitigationDuration(actionType string, d time.Duration) {
	at := SanitizeActionType(actionType)
	r.mitigationDuration.WithLabelValues(at).Observe(d.Seconds())
}

func (r *DefaultRecorder) RecordVerification(status string) {
	st := SanitizeVerificationStatus(status)
	r.verifications.WithLabelValues(st).Inc()
}

func (r *DefaultRecorder) ObserveVerificationDuration(d time.Duration) {
	r.verificationDuration.Observe(d.Seconds())
}

func (r *DefaultRecorder) RecordEscalation(classification string) {
	cl := SanitizeEscalationClassification(classification)
	r.escalations.WithLabelValues(cl).Inc()
}

func (r *DefaultRecorder) RecordRetry() {
	r.retries.Inc()
}

func (r *DefaultRecorder) RecordCircuitBreakerEvent(event string) {
	ev := SanitizeCircuitEvent(event)
	r.circuitBreakerEvents.WithLabelValues(ev).Inc()
}

func (r *DefaultRecorder) RecordOrchestration(outcome string) {
	oc := SanitizeOrchestrationOutcome(outcome)
	r.orchestrations.WithLabelValues(oc).Inc()
}

func (r *DefaultRecorder) ObserveOrchestrationDuration(d time.Duration) {
	r.orchestrationDuration.Observe(d.Seconds())
}

func (r *DefaultRecorder) RecordAuditEvent(eventType string) {
	et := SanitizeAuditEventType(eventType)
	r.auditEvents.WithLabelValues(et).Inc()
}

func (r *DefaultRecorder) RecordAuditWriteError() {
	r.auditWriteErrors.Inc()
}

func (r *DefaultRecorder) RecordAuditDuplicate() {
	r.auditDuplicates.Inc()
}

func (r *DefaultRecorder) RecordShutdown(status string) {
	st := SanitizeShutdownStatus(status)
	r.shutdowns.WithLabelValues(st).Inc()
}

func (r *DefaultRecorder) ObserveShutdownDuration(d time.Duration) {
	r.shutdownDuration.Observe(d.Seconds())
}

func (r *DefaultRecorder) RecordShutdownTimeout() {
	r.shutdownTimeouts.Inc()
}

func (r *DefaultRecorder) RecordRecovery(status string) {
	st := SanitizeRecoveryStatus(status)
	r.recovery.WithLabelValues(st).Inc()
}

func (r *DefaultRecorder) ObserveRecoveryDuration(d time.Duration) {
	r.recoveryDuration.Observe(d.Seconds())
}

func (r *DefaultRecorder) RecordRecoveryReconciliationRequired() {
	r.recoveryReconciliationRequired.Inc()
}

func (r *DefaultRecorder) RecordRecoveryBlocked() {
	r.recoveryBlocked.Inc()
}

func (r *DefaultRecorder) RecordRecoveryFailed() {
	r.recoveryFailed.Inc()
}

// ============================================================================
// Label Cardinality Sanitization Policy
// ============================================================================

// SanitizeMethod maps anomaly detection methods to bounded discrete enums.
func SanitizeMethod(method string) string {
	switch strings.ToLower(strings.TrimSpace(method)) {
	case "threshold", "static_threshold", "static_threshold_detector":
		return "threshold"
	case "z_score", "zscore", "statistical", "rolling_statistical", "rolling_statistical_detector":
		return "z_score"
	case "isolation_forest", "ml", "isolationforest", "ml_isolation_forest", "ml_isolation_forest_detector":
		return "isolation_forest"
	default:
		return "unknown"
	}
}

// SanitizeAuthClass maps authorization classes to bounded discrete enums.
func SanitizeAuthClass(authClass string) string {
	switch strings.TrimSpace(authClass) {
	case "AUTO_EXECUTE":
		return "AUTO_EXECUTE"
	case "APPROVAL_REQUIRED":
		return "APPROVAL_REQUIRED"
	case "FORBIDDEN":
		return "FORBIDDEN"
	default:
		return "unknown"
	}
}

// SanitizeSafetyReason maps safety rejection reasons to bounded discrete enums.
func SanitizeSafetyReason(reason string) string {
	r := strings.ToLower(strings.TrimSpace(reason))
	switch {
	case strings.Contains(r, "param"):
		return "param_violation"
	case strings.Contains(r, "cooldown"):
		return "cooldown_active"
	case strings.Contains(r, "limit") || strings.Contains(r, "rate"):
		return "limit_exceeded"
	case strings.Contains(r, "action") || strings.Contains(r, "forbidden"):
		return "invalid_action"
	default:
		return "unknown"
	}
}

// SanitizeApprovalStatus maps operator approval states to bounded discrete enums.
func SanitizeApprovalStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "requested":
		return "requested"
	case "approved":
		return "approved"
	case "rejected":
		return "rejected"
	case "expired":
		return "expired"
	case "cancelled":
		return "cancelled"
	case "consumed":
		return "consumed"
	default:
		return "unknown"
	}
}

// SanitizeActionType maps mitigation action types to bounded allowlist enums.
func SanitizeActionType(actionType string) string {
	switch strings.TrimSpace(actionType) {
	case "SIMULATED_THROTTLE":
		return "SIMULATED_THROTTLE"
	case "SIMULATED_RESTART":
		return "SIMULATED_RESTART"
	case "SIMULATED_ISOLATE":
		return "SIMULATED_ISOLATE"
	case "SIMULATED_ALERT":
		return "SIMULATED_ALERT"
	default:
		return "unknown"
	}
}

// SanitizeMitigationStatus maps mitigation execution states to bounded discrete enums.
func SanitizeMitigationStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "started":
		return "started"
	case "executed":
		return "executed"
	case "failed":
		return "failed"
	case "skipped":
		return "skipped"
	case "unknown":
		return "unknown"
	default:
		return "unknown"
	}
}

// SanitizeVerificationStatus maps verification states to bounded discrete enums.
func SanitizeVerificationStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "started":
		return "started"
	case "progress":
		return "progress"
	case "recovered":
		return "recovered"
	case "timed_out", "timeout":
		return "timed_out"
	case "rejected":
		return "rejected"
	case "cancelled":
		return "cancelled"
	default:
		return "unknown"
	}
}

// SanitizeEscalationClassification maps escalation causes to bounded discrete enums.
func SanitizeEscalationClassification(classification string) string {
	switch strings.ToLower(strings.TrimSpace(classification)) {
	case "mitigation_failed":
		return "mitigation_failed"
	case "verification_timed_out":
		return "verification_timed_out"
	case "verification_rejected":
		return "verification_rejected"
	case "unknown_reconciliation_required", "unknown_reconciliation":
		return "unknown_reconciliation"
	case "cooldown_active":
		return "cooldown_active"
	case "retry_budget_exhausted":
		return "retry_budget_exhausted"
	case "circuit_breaker_open":
		return "circuit_breaker_open"
	default:
		return "unknown"
	}
}

// SanitizeCircuitEvent maps circuit breaker events to bounded discrete enums.
func SanitizeCircuitEvent(event string) string {
	switch strings.ToLower(strings.TrimSpace(event)) {
	case "opened":
		return "opened"
	case "reset":
		return "reset"
	default:
		return "unknown"
	}
}

// SanitizeOrchestrationOutcome maps orchestration terminal states to bounded discrete enums.
func SanitizeOrchestrationOutcome(outcome string) string {
	switch strings.ToLower(strings.TrimSpace(outcome)) {
	case "completed":
		return "completed"
	case "stopped":
		return "stopped"
	case "failed":
		return "failed"
	default:
		return "unknown"
	}
}

// validAuditTypes allowlist containing all 25 discrete event types from Phase 6.9.
var validAuditTypes = map[string]bool{
	"RESPONSE_DECISION_CREATED":         true,
	"RESPONSE_DECISION_NO_ACTION":       true,
	"APPROVAL_REQUESTED":                true,
	"APPROVAL_GRANTED":                  true,
	"APPROVAL_REJECTED":                 true,
	"APPROVAL_CANCELLED":                true,
	"APPROVAL_CONSUMED":                 true,
	"SAFETY_VALIDATED":                  true,
	"SAFETY_REJECTED":                   true,
	"MITIGATION_RECORDED":               true,
	"MITIGATION_STARTED":                true,
	"MITIGATION_EXECUTED":               true,
	"MITIGATION_FAILED":                 true,
	"MITIGATION_UNKNOWN":                true,
	"VERIFICATION_STARTED":              true,
	"VERIFICATION_PROGRESS":             true,
	"VERIFICATION_RECOVERED":            true,
	"VERIFICATION_TIMED_OUT":            true,
	"VERIFICATION_REJECTED":             true,
	"ESCALATION_EVALUATED":              true,
	"RETRY_AUTHORIZED":                  true,
	"CIRCUIT_OPENED":                    true,
	"CIRCUIT_RESET":                     true,
	"ORCHESTRATION_COMPLETED":           true,
	"ORCHESTRATION_STOPPED":             true,
	"RECOVERY_STARTED":                  true,
	"RECOVERY_STATE_LOADED":             true,
	"RECOVERY_RECONCILIATION_REQUIRED":  true,
	"RECOVERY_RECONCILIATION_COMPLETED": true,
	"RECOVERY_BLOCKED":                  true,
	"RECOVERY_FAILED":                   true,
	"RECOVERY_COMPLETED":                true,
}

// SanitizeAuditEventType maps audit event types strictly against allowlisted constants.
func SanitizeAuditEventType(eventType string) string {
	clean := strings.TrimPrefix(strings.TrimSpace(eventType), "EVENT_")
	if validAuditTypes[clean] {
		return clean
	}
	return "unknown"
}

// SanitizeShutdownStatus maps shutdown outcomes to bounded discrete enums.
func SanitizeShutdownStatus(status string) string {
	s := strings.ToLower(strings.TrimSpace(status))
	switch s {
	case "completed", "timed_out", "failed":
		return s
	default:
		return "unknown"
	}
}

// SanitizeRecoveryStatus maps recovery outcomes to bounded discrete enums.
func SanitizeRecoveryStatus(status string) string {
	s := strings.ToLower(strings.TrimSpace(status))
	switch s {
	case "completed", "not_required", "blocked", "failed":
		return s
	default:
		return "unknown"
	}
}

// ============================================================================
// No-Op Recorder Implementation
// ============================================================================

// NoopRecorder implements Recorder with no-op operations.
type NoopRecorder struct{}

func (NoopRecorder) RecordTelemetryBatch(bool)                       {}
func (NoopRecorder) RecordAnomalyDetected(string)                    {}
func (NoopRecorder) RecordDetectorError(string)                      {}
func (NoopRecorder) ObserveDetectorDuration(string, time.Duration)   {}
func (NoopRecorder) RecordIncidentCreated()                          {}
func (NoopRecorder) RecordIncidentRecovered()                        {}
func (NoopRecorder) RecordIncidentEscalated()                        {}
func (NoopRecorder) SetActiveIncidents(int)                          {}
func (NoopRecorder) IncActiveIncidents()                             {}
func (NoopRecorder) DecActiveIncidents()                             {}
func (NoopRecorder) RecordResponseDecision(string)                   {}
func (NoopRecorder) RecordResponseNoAction()                         {}
func (NoopRecorder) RecordResponseRejection()                        {}
func (NoopRecorder) RecordSafetyValidation(bool)                     {}
func (NoopRecorder) RecordSafetyRejection(string)                    {}
func (NoopRecorder) RecordApproval(string)                           {}
func (NoopRecorder) RecordMitigation(string, string)                 {}
func (NoopRecorder) ObserveMitigationDuration(string, time.Duration) {}
func (NoopRecorder) RecordVerification(string)                       {}
func (NoopRecorder) ObserveVerificationDuration(time.Duration)       {}
func (NoopRecorder) RecordEscalation(string)                         {}
func (NoopRecorder) RecordRetry()                                    {}
func (NoopRecorder) RecordCircuitBreakerEvent(string)                {}
func (NoopRecorder) RecordOrchestration(string)                      {}
func (NoopRecorder) ObserveOrchestrationDuration(time.Duration)      {}
func (NoopRecorder) RecordAuditEvent(string)                         {}
func (NoopRecorder) RecordAuditWriteError()                          {}
func (NoopRecorder) RecordAuditDuplicate()                           {}
func (NoopRecorder) RecordShutdown(string)                           {}
func (NoopRecorder) ObserveShutdownDuration(time.Duration)           {}
func (NoopRecorder) RecordShutdownTimeout()                          {}
func (NoopRecorder) RecordRecovery(string)                           {}
func (NoopRecorder) ObserveRecoveryDuration(time.Duration)           {}
func (NoopRecorder) RecordRecoveryReconciliationRequired()           {}
func (NoopRecorder) RecordRecoveryBlocked()                          {}
func (NoopRecorder) RecordRecoveryFailed()                           {}
func (NoopRecorder) Registry() *Registry                             { return nil }

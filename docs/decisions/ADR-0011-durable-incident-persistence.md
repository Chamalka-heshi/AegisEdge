# ADR-0011: Durable Edge Incident Persistence & Restart Recovery

**Status**: Accepted (Phase 5.5 Implementation)  
**Date**: 2026-09-27  
**Deciders**: AegisEdge Core Engineering Team  
**Supersedes**: None  
**Relates to**: ADR-0003 (Domain Event & Incident Contracts), ADR-0004 (Edge-Local Persistence & WAL Durability), ADR-0008 (NATS Resilience & Redelivery), ADR-0009 (Edge Anomaly Detection Architecture), ADR-0010 (Local Incident Engine Architecture)

---

## 1. Context

Through Phases 1 to 5.4, AegisEdge implemented an edge telemetry pipeline and local incident correlation engine governed by the invariant:
$$\textbf{PERSIST TELEMETRY FIRST} \longrightarrow \textbf{DETECT SECOND} \longrightarrow \textbf{CORRELATE THIRD}$$

In Phase 5.4, the Local Incident Engine was integrated to correlate discrete `types.AnomalySignal` observations into macro `types.Incident` domain objects using an $M$-of-$N$ sliding window policy and the canonical Incident FSM (`NORMAL` $\to$ `ANOMALY_DETECTED` $\to$ `MITIGATING` / `ESCALATED` $\to$ `RECOVERED` $\to$ `NORMAL`).

---

## 2. Problem Statement

While raw telemetry batches were durably stored in SQLite WAL storage (`telemetry_batches`), incident correlation state and active incident domain entities resided solely in process memory (`LocalEngine`).

Under process restart, device reboot, or crash:
1. Active incidents disappeared from memory, losing their lifecycle tracking and state.
2. In-flight $M$-of-$N$ correlation windows reset to zero (e.g., if 1 of 2 required anomalies had occurred, a crash caused the edge to lose this progress).
3. Processed `AnomalyID`s were lost, risking duplicate counting if duplicate or replayed signals were presented across restart boundaries.
4. Subsequent breaches generated fresh incident identities rather than maintaining lifecycle stability.

---

## 3. Decision

We introduce **Durable Incident Persistence** directly into the edge agent's local SQLite WAL storage layer via Migration v3. SQLite is established as the **durable source of truth**, while process memory serves as a thread-safe, reconstructed working cache.

The extended pipeline follows:
$$\text{Telemetry Generation} \longrightarrow \text{Validation} \longrightarrow \text{SQLite WAL Persistence} \longrightarrow \text{Anomaly Detector} \longrightarrow \text{AnomalySignal} \longrightarrow \text{Incident Engine} \longrightarrow \text{SQLite WAL Incident Persistence}$$

---

## 4. SQLite Schema (Migration v3)

Migration v3 extends the local SQLite database without altering existing tables (`telemetry_batches` and `schema_migrations`):

```sql
-- Formal Incident Records
CREATE TABLE IF NOT EXISTS incident_records (
    incident_id TEXT PRIMARY KEY,
    node_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    rule_name TEXT NOT NULL,
    severity TEXT NOT NULL,
    status TEXT NOT NULL,
    description TEXT NOT NULL,
    trigger_metric TEXT NOT NULL,
    trigger_value REAL NOT NULL,
    threshold REAL NOT NULL,
    evidence TEXT NOT NULL,
    triggered_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    resolved_at TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_incident_records_node_metric ON incident_records(node_id, metric_name);
CREATE INDEX IF NOT EXISTS idx_incident_records_status ON incident_records(status);

-- Correlation & Anomaly Observations
CREATE TABLE IF NOT EXISTS incident_observations (
    anomaly_id TEXT PRIMARY KEY,
    incident_id TEXT,
    node_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    detected_at TEXT NOT NULL,
    observed_value REAL NOT NULL,
    anomaly_score REAL NOT NULL,
    detection_method TEXT NOT NULL,
    evidence TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_incident_obs_node_metric_detected ON incident_observations(node_id, metric_name, detected_at);
CREATE INDEX IF NOT EXISTS idx_incident_obs_incident_id ON incident_observations(incident_id);
```

### Complete `types.Incident` Contract Field Mapping

Every field of the canonical `types.Incident` domain object is accounted for:

| Field | Type | Disposition | Rationale & Persistence Mechanism |
| :--- | :--- | :---: | :--- |
| `IncidentID` | `string` | **A. Persisted directly** | Primary key column `incident_id TEXT PRIMARY KEY`. |
| `NodeID` | `string` | **A. Persisted directly** | Column `node_id TEXT NOT NULL`. |
| `RuleName` | `string` | **A. Persisted directly** | Column `rule_name TEXT NOT NULL`. |
| `Severity` | `IncidentSeverity` | **A. Persisted directly** | Column `severity TEXT NOT NULL`. |
| `Status` | `IncidentStatus` | **A. Persisted directly** | Column `status TEXT NOT NULL`. |
| `Description` | `string` | **A. Persisted directly** | Column `description TEXT NOT NULL`. |
| `TriggerMetric` | `string` | **A. Persisted directly** | Column `trigger_metric TEXT NOT NULL`. |
| `TriggerValue` | `float64` | **A. Persisted directly** | Column `trigger_value REAL NOT NULL`. |
| `Threshold` | `float64` | **A. Persisted directly** | Column `threshold REAL NOT NULL`. |
| `Evidence` | `map[string]string` | **A. Persisted directly** | Serialized as JSON in `evidence TEXT NOT NULL`. Deserialized on read. |
| `TriggeredAt` | `time.Time` | **A. Persisted directly** | RFC 3339 Nano string in `triggered_at TEXT NOT NULL`. |
| `UpdatedAt` | `time.Time` | **A. Persisted directly** | RFC 3339 Nano string in `updated_at TEXT NOT NULL`. |
| `ResolvedAt` | `*time.Time` | **A. Persisted directly** | Nullable RFC 3339 Nano string in `resolved_at TEXT`. |
| `Mitigations` | `[]MitigationAction` | **C. Intentionally unused** | Autonomous remediation / mitigation execution is an explicit non-goal for Phase 5.x and is deferred to Phase 6. In Phase 5.5, `Mitigations` is always `nil`. |

---

## 5. Atomic Transaction Boundary

To prevent partial state corruption (e.g. observation committed without incident update, or incident advanced without durable evidence), all evaluation mutations occur within an atomic SQLite transaction:

```text
BEGIN TRANSACTION
    1. Check AnomalyID uniqueness in incident_observations (idempotency)
    2. If duplicate -> COMMIT (no-op)
    3. If Incident non-nil (created or updated):
       - If new -> INSERT INTO incident_records
       - If existing -> UPDATE incident_records
       - Set observation.incident_id = incident.IncidentID
    4. INSERT INTO incident_observations
COMMIT
```

If any validation, database constraint, or disk operation fails, the transaction issues a `ROLLBACK`, guaranteeing that zero partial or inconsistent lifecycle state is ever written to disk.

---

## 6. Anomaly Idempotency

- `anomaly_id` serves as the primary key in `incident_observations`.
- Duplicate submissions of the same `AnomalyID` (whether in-memory or following process restart) are caught by database index lookups and primary key constraints.
- Re-processing an already-seen `AnomalyID` does not increment the $M$-of-$N$ counter, does not alter existing `IncidentID`s, and creates no duplicate observation rows.

---

## 7. Incident Identity & Lifecycle Stability

- `IncidentID` is generated once upon initial incident qualification and remains immutable throughout the lifecycle of the active incident.
- When subsequent anomalies arrive during an active incident episode (`ANOMALY_DETECTED`, `MITIGATING`, `ESCALATED`), the engine updates the existing row in `incident_records` (`updated_at`, `trigger_value`, latest evidence) and reuses the existing `IncidentID`.
- Only after an incident transitions to `RECOVERED` or `NORMAL` does a future breach create a new incident with a distinct `IncidentID`.

---

## 8. Restart Recovery Mechanism

Upon edge agent startup:
1. Open SQLite database and execute pending migrations (Migration v3).
2. Invoke `RecoverFromStore(ctx)`:
   - Query all active incidents from `incident_records` (`WHERE status IN ('ANOMALY_DETECTED', 'MITIGATING', 'ESCALATED')`).
   - Query bounded recent observations from `incident_observations` (`WHERE detected_at >= now - WindowDuration`).
   - Reconstruct in-memory `streamState` (active incidents, observation windows, and seen anomaly IDs).
3. Resume autonomous pipeline execution seamlessly.

---

## 9. $M$-of-$N$ Durable Correlation

- The $M$-of-$N$ sliding window policy ($M=2, N=3$, 5-minute horizon) survives process restarts.
- Example: Anomaly A1 is processed before a crash (count 1 of 2). On restart, A1 is reloaded from SQLite into the correlation window. When Anomaly A2 arrives, the engine observes $M=2$ within the window and successfully creates the incident.

---

## 10. Correlation Window & Bounded Horizon

- Reconstructed observation state is strictly bounded by `WindowDuration` (default: 5 minutes).
- Observations older than `DetectedAt - WindowDuration` are excluded from correlation counts and pruned.
- Expired observations never contribute to new incident creation decisions.

---

## 11. Failure Isolation

The pipeline strictly isolates telemetry durability from incident persistence:
$$\textbf{PERSIST TELEMETRY FIRST} \longrightarrow \textbf{DETECT SECOND} \longrightarrow \textbf{PERSIST INCIDENT THIRD}$$

- **Incident Persistence Failure**: If writing to `incident_records` or `incident_observations` encounters an error, the incident error is logged. The previously persisted telemetry batch remains safe, confirmed, and durable in SQLite WAL storage, and upstream synchronization continues normally.
- Telemetry durability is never rolled back due to incident engine failures.

---

## 12. Concurrency Protection

- Thread safety is enforced by `sync.RWMutex` on the Incident Engine.
- At the storage layer, SQLite connection pooling (`SetMaxOpenConns(1)`) and transactional locks prevent concurrent goroutines from racing on incident creation or observation insertion.
- Verified under 40 concurrent workers processing duplicate and distinct anomaly signals without race conditions or duplicate incident creation.

---

## 13. Recovery Limitations

- The Phase 5.2/5.3 threshold detector emits only breach signals and resolves state silently via internal hysteresis.
- Phase 5.5 **does not invent synthetic recovery signals**.
- Automatic transition to `RECOVERED` remains dependent on future explicit recovery signal modeling or operator/actuation commands via `CloseActiveIncident`.

---

## 14. Consistency Guarantees & Clarifications

1. **Local Consistency & Crash Recovery**: We provide local ACID transactional consistency within the edge SQLite database. Incident state survives application/process restart and SQLite crash-recovery scenarios covered by the configured WAL durability semantics (`synchronous=NORMAL`).
2. **Physical Power-Loss Durability**: Physical power-loss durability depends on SQLite synchronous settings, filesystem behavior, storage hardware, and OS/device caches. In `synchronous=NORMAL`, uncheckpointed WAL frames could be lost upon sudden, unannounced hardware power cutoff. AegisEdge explicitly does not claim zero data loss, nor guaranteed survival of every physical power-loss scenario.
3. **No Distributed Claims**: We do not claim distributed exactly-once delivery, distributed transactions, or global consensus.
4. **No Cloud Persistence**: Incident durability is local to the edge device in this phase; control-plane incident replication is deferred to future transport phases.

---

## 15. Explicit Non-Goals

The following features were strictly excluded from Phase 5.5:
- Machine Learning (Isolation Forest, ONNX, Autoencoders).
- Autonomous remediation execution (no process kills, service restarts, or shell commands).
- Cloud incident replication or remote database storage.
- Dashboards, Prometheus, Grafana, alerts (Slack, email).
- NATS incident streams or HTTP incident ingestion endpoints.

---

## 16. Future Work

1. **Central Incident Transport**: Replicating durable edge incidents to the central control plane via NATS JetStream or HTTP sync.
2. **Explicit Recovery Events**: Modeling bidirectional recovery signals in detectors to automate transition to `RECOVERED`.
3. **Safe Allowlisted Remediation**: Executing allowlisted `MitigationAction` actuators downstream of incident qualification.

# AegisEdge

## Autonomous Edge AI Incident-Response Platform

AegisEdge is a distributed, fault-tolerant edge intelligence platform designed to detect system anomalies, execute safe autonomous incident mitigations at the edge, and maintain reliable telemetry synchronization with a central control plane across intermittent network partitions.

---

## Key Architectural Principles

1. **Edge-Local First**: Telemetry and incident events are committed to local edge storage before attempting upstream synchronization.
2. **Offline Resilience**: The edge agent continues monitoring and executing local mitigation policies even when disconnected from the control plane.
3. **Deterministic State Machine**: Incident detection, triage, and mitigation follow explicit, verifiable state transitions.
4. **Idempotent Synchronization**: Network partitions and retries with exponential backoff do not produce duplicate logical incidents or state corruption.
5. **Incremental Complexity**: Infrastructure (containers, message brokers, cloud deployments) is introduced only when technical requirements demand them, not prematurely.

---

## Repository Structure

```text
AegisEdge/
├── edge/
│   └── agent/              # Autonomous edge agent (telemetry collector, local buffer, actuator)
├── services/
│   └── control-plane/      # Central control plane (node registry, incident ingestion, fleet state)
├── shared/
│   └── types/              # Canonical domain types and data contracts
├── docs/
│   ├── architecture/       # System architecture diagrams and flow descriptions
│   └── decisions/          # Architecture Decision Records (ADRs)
├── scripts/                # Developer tooling, verification, and local runner scripts
├── .gitignore              # Workspace-wide ignore rules
├── go.work                 # Multi-module Go workspace
└── README.md
```

---

## Architectural Decision Records (ADRs)

Key architectural decisions are formally documented in `docs/decisions/`:

* [ADR-0001: Language and Initial Architecture Selection](docs/decisions/ADR-0001-language-and-initial-architecture.md)
* [ADR-0002: Edge-to-Control-Plane Communication and Offline Synchronization](docs/decisions/ADR-0002-edge-to-control-plane-communication-and-offline-sync.md)
* [ADR-0003: Domain Event and Incident Contracts](docs/decisions/ADR-0003-domain-event-and-incident-contracts.md)
* [ADR-0004: Edge-Local Persistence and WAL Durability](docs/decisions/ADR-0004-edge-local-persistence-and-wal-durability.md)
* [ADR-0005: Edge-to-Control-Plane HTTP Synchronization and Offline Recovery](docs/decisions/ADR-0005-edge-to-control-plane-http-synchronization.md)
* [ADR-0006: NATS Event-Driven Messaging Architecture and Ingestion Pipeline](docs/decisions/ADR-0006-nats-event-driven-messaging.md)
* [ADR-0007: NATS JetStream Application Integration](docs/decisions/ADR-0007-nats-jetstream-application-integration.md)
* [ADR-0008: NATS JetStream Resilience, Redelivery, and Recovery Design](docs/decisions/ADR-0008-nats-resilience-and-recovery.md)
* [ADR-0009: Edge Anomaly Detection Architecture & Design](docs/decisions/ADR-0009-edge-anomaly-detection.md)
* [ADR-0010: Edge Local Incident Engine Architecture](docs/decisions/ADR-0010-local-incident-engine.md)
* [ADR-0011: Durable Incident State Machine & Multi-Incident Concurrency](docs/decisions/ADR-0011-durable-incident-persistence.md)
* [ADR-0012: Autonomous Incident Response Architecture and Safety Design](docs/decisions/ADR-0012-autonomous-incident-response.md)
* [ADR-0013: Edge Agent Health, Liveness, and Readiness Model](docs/decisions/ADR-0013-edge-agent-health-readiness.md)
* [ADR-0014: Controlled Graceful Shutdown & Runtime Lifecycle Coordinator](docs/decisions/ADR-0014-graceful-shutdown-and-runtime-lifecycle.md)
* [ADR-0015: Persistent Runtime State & Safe Restart Recovery](docs/decisions/ADR-0015-persistent-runtime-state-and-safe-restart-recovery.md)
* [ADR-0016: Edge ↔ Control-Plane Coordination Layer](docs/decisions/ADR-0016-edge-control-plane-coordination.md)
* [ADR-0017: Secure Edge–Control-Plane Communication](docs/decisions/ADR-0017-secure-edge-control-plane-communication.md)

---

## Quickstart & Verification (Local Development)

### Prerequisites

* **Go** 1.22+ (verified on Go 1.27)
* **Git**

### Running the System (Phase 3 Distributed Sync)

#### 1. Start the Control Plane Server

```powershell
go run ./services/control-plane -port 8080
```

#### 2. Start the Edge Agent

```powershell
go run ./edge/agent -node-id edge-node-01 -control-plane-url http://localhost:8080 -interval 3s
```

#### 3. Offline-to-Online Demonstration

1. Start the edge agent while the control plane is stopped (`-once` or daemon mode).
2. Observe telemetry batches being generated and safely committed to SQLite with `sync_status = 'PENDING'`.
3. Start the control plane server on port 8080.
4. Observe the edge agent connecting, synchronizing all pending batches in `(NodeID, SequenceNumber)` order, and updating their SQLite records to `SYNCED`.

#### 4. Event-Driven Messaging via NATS JetStream (Phase 4.3 & 4.4)

A local NATS/JetStream development environment is available (see [`docs/development/local-nats.md`](docs/development/local-nats.md)). In Phase 4.3, full application-level NATS integration was completed (`NATS_ENABLED=true`), enabling `SQLite -> PENDING -> NATS JetStream -> PubAck -> PUBLISHED -> Consumer -> Idempotent Ingestion -> ACK`. Phase 4.4 formalizes the comprehensive resilience, redelivery, and failure recovery design in [`ADR-0008`](docs/decisions/ADR-0008-nats-resilience-and-recovery.md).

#### 5. Edge Anomaly Detection & Autonomous Response Architecture (Phases 5.1 – 6.15)

* **IMPLEMENTED**:
  * `ThresholdDetector`: Deterministic static threshold boundaries ($W=1$) with hysteresis bands and recovery thresholds (Phases 5.1–5.2).
  * `StatisticalDetector`: Deterministic rolling statistical baseline ($\mu, \sigma$, z-score) with bounded window eviction and zero-variance safety (Phase 5.4).
  * `IncidentEngine`: Local $M$-of-$N$ temporal correlation, deduplication, and canonical Incident FSM lifecycle (Phases 5.3–5.5).
  * `MLDetector`: Pure-Go Isolation Forest multivariate anomaly detection engine evaluating canonical 4-dimensional telemetry vectors (`cpu`, `memory`, `disk`, `temperature`) (Phase 5.5B, see [`ADR-0009 §30`](docs/decisions/ADR-0009-edge-anomaly-detection.md)).
  * `aegisedge-train`: Offline Isolation Forest training CLI with canonical 4D feature alignment, deterministic PRNG seed, and model serialization (Phase 5.5C).
  * `shared/ml`: Clean decoupled contracts for model manifests, canonical JSON SHA-256 integrity, runtime compatibility evaluation, and deployment lifecycle state machine (Phase 5.6A).
  * `edge/agent/modelstore`: Filesystem-backed edge model store and candidate staging boundary with multi-stage validation gates, safe bounded reads, Windows-compatible file operations, idempotency, and conflict rejection (Phase 5.6B).
  * `edge/agent/modelactivation`: Thread-safe runtime model activation and explicit rollback manager with pre-promotion validation pipeline, dynamic `RuntimeDetector` dispatching under `sync.RWMutex`, idempotency, conflict rejection, previous model disk retention in `previous/`, and repeated rollback swapping (Phases 5.6C & 5.6D).
  * `edge/agent/response`: Deterministic Response Policy Engine (`RuleBasedPolicy`), Fail-Closed Safety Validator (`StandardSafetyValidator`), Operator Approval Boundary & Lifecycle Manager (`ApprovalManager`), and Simulated Action Executor (`SimulatedExecutor`) providing safe, observable simulation and dry-run execution with idempotency, 14-point decision-binding security gate (binding `DecisionID`, `IncidentID`, `ActionID`, `NodeID`, `ActionType`, `Target`, `PolicyVersion`, and canonical parameters), single-use approval consumption, and zero real host mutation (Phases 6.2, 6.3, 6.5).
  * `edge/agent/storage`: Durable mitigation, approval, verification, escalation, audit trail, and runtime checkpoint persistence and startup crash recovery via SQLite WAL Migrations v4 (`mitigation_records`), v5 (`approval_records`), v6 (`verification_records`), v7 (`escalation_records` & `circuit_breaker_states`), v8 (`audit_events`), and v9 (`runtime_checkpoints`), providing local identity, duplicate suppression, fail-closed restart reconciliation, single-use atomic consumption, structured lifecycle audit history, and persistent recovery state (Phases 6.4, 6.5, 6.6, 6.7, 6.9, 6.13).
  * `edge/agent/verification`: Closed-loop incident verification engine (`LocalEngine`) evaluating post-mitigation telemetry against recovery conditions (`RecoveryCondition`, threshold hysteresis) with bounded observation timeouts, streak dynamics ($N$ consecutive healthy samples, reset on unhealthy sample while remaining `PENDING`), telemetry freshness boundary (`ErrStaleTelemetrySample`), SampleID duplicate suppression, multi-dimensional isolation, and safe incident FSM handoff where Verification `RECOVERED` does not automatically imply Incident `RECOVERED` (Phase 6.6).
  * `edge/agent/escalation`: Deterministic incident escalation and fail-closed circuit breaker engine (`LocalEngine`) enforcing "NO UNBOUNDED AUTONOMOUS REMEDIATION LOOP" by bounding retry decisions produced by policy, scoped to the node/incident pair `(NodeID, IncidentID)`, bounded retry decisions (`MaxAutomaticRetries = 3`, at most 3 retry decisions permitted after initial failure), cooldown enforcement (`RetryCooldown = 5m`), sliding failure window (`FailureWindow = 30m`), fail-closed circuit breaker (`CLOSED -> OPEN -> explicit operator reset -> CLOSED`), 7 failure classifications, deterministic SHA-256 escalation identity, SQLite WAL persistence (Migration v7), restart recovery, and safe handoff to `IncidentEngine.TransitionActiveIncident` (Phase 6.7).
  * `edge/agent/orchestrator`: Controlled incident response orchestrator (`DefaultOrchestrator`) connecting detection, incident correlation, response policy, safety validation, simulated mitigation, verification, escalation, and audit recording into a bounded, deterministic, fail-closed simulated incident-response workflow. Enforces bounded retry budgets (`MaxAutomaticRetries = 3`, bounding total executions to at most 4, backed by hard defense-in-depth ceiling `MaxExecutionCeiling = 4`; the hard ceiling of 4 agrees with the configured maximum of 3 automatic retries and cannot authorize an additional execution beyond the escalation policy), serialized execution per `IncidentID` via in-flight leader-follower tracking with wait groups, compile-time simulated-only actuation (`SIMULATED_*`, zero shell commands or real host modifications), synchronous and asynchronous telemetry processing, requirement that durable persistence precedes simulated actuation, detection of previously recorded logical attempts across supported restart-recovery paths to suppress duplicate orchestration where persisted state is available, and structured audit checkpoint recording (Phases 6.8 & 6.9).
  * `edge/agent/audit`: Structured, queryable audit trail for the complete incident-response lifecycle (`ANOMALY -> INCIDENT -> POLICY -> SAFETY -> APPROVAL -> MITIGATION -> VERIFICATION -> ESCALATION -> ORCHESTRATION`). Features 25 allowlisted event types, SHA-256 deterministic event identity with retry attempt isolation, correlation IDs (`corr-<IncidentID>`), fail-closed metadata sanitization (bounds max 32 keys, 8192 bytes, rejects credentials/scripts), bounded queries (default limit 100, max 1000), chronological ordering, non-fatal operational logging, and timeline reconstruction without real host actuation, cloud dependencies, or cryptographic non-repudiation (Phase 6.9).
  * `edge/agent/metrics`: Local, Prometheus-compatible operational metrics layer for the incident-response system. Features 27 metric families across 11 lifecycle categories, strict label cardinality enforcement (forbidden IDs, targets, errors, timestamps; fail-closed sanitization to "unknown"), pure standard library Go Prometheus-compatible text exposition (`text/plain; version=0.0.4; charset=utf-8`) at local read-only HTTP endpoint `127.0.0.1:9091/metrics`, and non-fatal process-lifetime in-memory recording that does not intentionally alter business decisions (Phase 6.10).
  * `edge/agent/health`: Health, liveness, and readiness subsystem (`Tracker`, `Handler`) distinguishing process liveness (`GET /healthz`) from subsystem readiness (`GET /readyz`). Enforces 4 bounded lifecycle states (`INITIALIZING`, `READY`, `DEGRADED`, `SHUTTING_DOWN`), a fixed allowlist of subsystem checks (`config`, `storage`, `telemetry`, `incident_engine`, `response_engine`, `metrics`), fail-closed information disclosure prevention (sanitizing paths, secrets, SQL), method validation (rejecting non-GET/HEAD with HTTP 405 `Allow: GET, HEAD`), and thread-safe in-memory tracking where probe handlers do not perform application-level disk or network I/O during request handling (Phase 6.11).
  * `edge/agent/lifecycle`: Controlled, graceful shutdown and runtime lifecycle coordinator (`Coordinator`, `SetupSignalHandler`) governing 4 bounded states (`STARTING`, `RUNNING`, `SHUTTING_DOWN`, `STOPPED`), root context cancellation, deterministic shutdown hook sequencing (`health` order 10 $\rightarrow$ `metrics_server` order 40 $\rightarrow$ `nats_publisher` order 45 $\rightarrow$ `storage` order 50), lock-free hook execution preventing deadlocks, idempotent shutdown execution, bounded timeout (`DefaultShutdownTimeout = 10s`, `AEGISEDGE_SHUTDOWN_TIMEOUT` / `-shutdown-timeout`), and shutdown metrics (`aegisedge_shutdowns_total`, `aegisedge_shutdown_duration_seconds`, `aegisedge_shutdown_timeouts_total`) without distributed shutdown protocols, external actuation, or shell commands (Phase 6.12, see [`ADR-0014`](docs/decisions/ADR-0014-graceful-shutdown-and-runtime-lifecycle.md)).
  * `edge/agent/recovery`: Persistent runtime state and safe restart recovery coordinator (`Manager`, `Tracker`) enforcing the core invariant: **RESTART MUST NEVER BY ITSELF AUTHORIZE ACTION EXECUTION**. Bounded recovery states (`RECOVERY_NOT_REQUIRED`, `RECOVERY_LOADING`, `RECOVERY_RECONCILING`, `RECOVERY_BLOCKED`, `RECOVERY_COMPLETE`, `RECOVERY_FAILED`), SQLite WAL checkpoints (Migration v9), fail-closed reconciliation of in-flight mitigations (`EXECUTING -> UNKNOWN_RECONCILIATION_REQUIRED`), blocked readiness on ambiguous actions (`503`), reconciliation of `PENDING` actions to `SKIPPED`, expiration of stale verifications to `TIMED_OUT`, required health check `recovery`, structured recovery audit events, and Prometheus recovery metrics without arbitrary shell commands or real host mutation (Phase 6.13, see [`ADR-0015`](docs/decisions/ADR-0015-persistent-runtime-state-and-safe-restart-recovery.md)).
  * `edge/agent/coordination`: Bounded, offline-first Edge ↔ Control-Plane coordination layer (`Coordinator`, `Tracker`, `HTTPClient`) enforcing the invariant: **EDGE OPERATES AUTONOMOUSLY OFFLINE $\longrightarrow$ CONTROL PLANE AVAILABLE $\longrightarrow$ EDGE REGISTERS / HEARTBEATS $\longrightarrow$ COORDINATION WHEN AVAILABLE $\longrightarrow$ EDGE REMAINS SAFE AND AUTONOMOUS IF LOST**. Persists durable node identity via SQLite WAL Migration v10 (`node_identity`), registers inventory and capabilities at `POST /api/v1/nodes/register`, broadcasts liveness and monotonic sequence numbers at `POST /api/v1/nodes/heartbeat`, tracks bounded connection lifecycle (`DISCONNECTED`, `CONNECTING`, `CONNECTED`, `DEGRADED`, `STOPPING`), implements exponential backoff reconnection (1s to 30s), registers non-critical health check `health.CheckControlPlane` (`required = false` preserving offline `/readyz` 200 OK), recovers in-memory control-plane restarts via automatic re-registration, records 8 dedicated audit events, and exposes 6 operational Prometheus metrics (`aegisedge_control_plane_*`) with graceful teardown at order 30 (Phase 6.14, see [`ADR-0016`](docs/decisions/ADR-0016-edge-control-plane-coordination.md)).
  * `shared/types`, `services/control-plane`, `edge/agent`: Secure Edge–Control-Plane communication layer enforcing **EDGE OPERATES AUTONOMOUSLY OFFLINE $\longrightarrow$ PERSISTS LOCALLY $\longrightarrow$ AUTHENTICATES WHEN AVAILABLE $\longrightarrow$ COORDINATES SECURELY $\longrightarrow$ REMAINS SAFE IF CP IS UNAVAILABLE**. Implements HMAC-SHA256 request signing over canonical strings binding HTTP method, route, timestamp, nonce, and payload digest. Provides constant-time verification, sliding-window anti-replay (5m clock skew window + thread-safe memory-bounded `ReplayCache`), granular authorization tiers (public, node-authenticated with anti-impersonation, admin Bearer token), fail-closed TLS configuration validation, auth failure metric `aegisedge_control_plane_auth_failures_total`, and `CONTROL_PLANE_AUTH_FAILED` audit logging without compromising offline edge readiness or autonomy (Phase 6.15, see [`ADR-0017`](docs/decisions/ADR-0017-secure-edge-control-plane-communication.md)).
* **DESIGNED**:
  * Autonomous Incident Response Architecture: Full response lifecycle model, operator approval ticketing, and automated telemetry verification (Phase 6.1 Design Only, see [ADR-0012](docs/decisions/ADR-0012-autonomous-incident-response.md)).
  * ML Model Distribution & Autonomous Rollback Architecture: Staged deployment, network transport, and autonomous policy-based rollback (Phase 5.5D Design Only; crash-consistent filesystem activation is not implemented, see [ADR-0009 §31](docs/decisions/ADR-0009-edge-anomaly-detection.md#31-phase-55d--ml-model-distribution--deployment-architecture)).
* **FUTURE IMPLEMENTATION**:
  * Real host actuators, production remediation, authenticated operator identities, cryptographic approval signatures, non-repudiation, tamper-proof audit logging, approval UI/API, and identity federation.
  * Ed25519 cryptographic signing, central model registry service, model distribution network transport, automated policy-driven rollback engine.
  * External operator notification infrastructure.

### Running Tests

To verify all 20 packages across the Go workspace:

```powershell
# Using the verification script
.\scripts\check.ps1

# Or directly via Go across all workspace modules
go test -v ./shared/types/... ./shared/ml/... ./edge/agent/... ./services/control-plane/...
```

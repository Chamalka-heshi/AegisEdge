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
* [ADR-0018: Security Regression & Adversarial Testing Suite](docs/decisions/ADR-0018-security-regression-testing.md)

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
  * `shared/types`, `services/control-plane`, `edge/agent`: Secure Edge–Control-Plane communication layer enforcing **EDGE OPERATES AUTONOMOUSLY OFFLINE $\longrightarrow$ PERSISTS LOCALLY $\longrightarrow$ AUTHENTICATES WHEN AVAILABLE $\longrightarrow$ COORDINATES SECURELY $\longrightarrow$ REMAINS SAFE IF CP IS UNAVAILABLE**. Implements HMAC-SHA256 request signing over canonical strings binding HTTP method, route, timestamp, nonce, and payload digest. Provides constant-time verification, per-node credential isolation with secure JSON file loading (`-node-secrets-file` and `NODE_SECRETS_FILE`) with duplicate key rejection and fail-closed validation, sliding-window anti-replay (5m clock skew window + thread-safe memory-bounded `ReplayCache`), granular authorization tiers (public, node-authenticated with anti-impersonation, admin Bearer token), fail-closed TLS configuration validation, auth failure metric `aegisedge_control_plane_auth_failures_total`, and `CONTROL_PLANE_AUTH_FAILED` audit logging without compromising offline edge readiness or autonomy (Phase 6.15 & 6.15.1, see [`ADR-0017`](docs/decisions/ADR-0017-secure-edge-control-plane-communication.md)).
  * `services/control-plane`: Operational storage readiness and decoupled lifecycle health checking (`Ping`, `/healthz`, `/readyz`). Implements bounded SQLite storage health probes (`SELECT 1` with 2-second timeout), explicit graceful shutdown signaling (`MarkShuttingDown()`, `Server.Close()`), and decoupled liveness probing preventing storage lock contention from triggering process kill signals (Phase 6.29 Stage 6).
* **DESIGNED**:
  * Autonomous Incident Response Architecture: Full response lifecycle model, operator approval ticketing, and automated telemetry verification (Phase 6.1 Design Only, see [ADR-0012](docs/decisions/ADR-0012-autonomous-incident-response.md)).
  * ML Model Distribution & Autonomous Rollback Architecture: Staged deployment, network transport, and autonomous policy-based rollback (Phase 5.5D Design Only; crash-consistent filesystem activation is not implemented, see [ADR-0009 §31](docs/decisions/ADR-0009-edge-anomaly-detection.md#31-phase-55d--ml-model-distribution--deployment-architecture)).
* **FUTURE IMPLEMENTATION**:
  * Real host actuators, production remediation, authenticated operator identities, cryptographic approval signatures, non-repudiation, tamper-proof audit logging, approval UI/API, and identity federation.
  * Ed25519 cryptographic signing, central model registry service, model distribution network transport, automated policy-driven rollback engine.
  * External operator notification infrastructure.

---

## Control-Plane Operational Health & Lifecycle Probes (`/healthz` and `/readyz`)

The control-plane HTTP server provides two decoupled health endpoints to distinguish process vitality from service readiness:

| Endpoint | Role | Evaluation Scope | Healthy Status | Unhealthy Status | Supported HTTP Methods |
| :--- | :--- | :--- | :---: | :---: | :--- |
| **`/healthz`** | **Liveness Probe** | Process and event loop vitality. Returns `200 OK` unless startup initialization failed. | `200 OK` | `503 Service Unavailable` | `GET` only (others return `405 Method Not Allowed`, `Allow: GET`) |
| **`/readyz`** | **Readiness Probe** | Ingress traffic readiness. Verifies initialization, storage responsiveness, and active lifecycle state. | `200 OK` | `503 Service Unavailable` | `GET`, `HEAD` (others return `405 Method Not Allowed`, `Allow: GET, HEAD`) |

### Endpoint Details & Behavioral Contracts

#### 1. Liveness (`GET /healthz`)
* **Purpose**: Confirms that the control-plane process is alive and handling HTTP requests.
* **Isolation from Storage**: `/healthz` **never** executes database queries or storage probes. A transient database lock or slow query will **not** cause `/healthz` to fail.
* **Startup Failure**: If startup rehydration encounters an unrecoverable failure (`s.initErr != nil`), `/healthz` returns `HTTP 503 Service Unavailable`:
  ```json
  {
    "status": "unhealthy",
    "error": "failed to rehydrate node registry from durable store: ..."
  }
  ```
* **Normal Operation**: Returns `HTTP 200 OK`:
  ```json
  {
    "status": "healthy"
  }
  ```

#### 2. Readiness (`GET /readyz`, `HEAD /readyz`)
* **Purpose**: Confirms that the control-plane instance is prepared to accept edge node registrations, heartbeats, and telemetry batch submissions.
* **Bounded Storage Probe**:
  * Executes a context-bounded storage health probe via `IdempotencyStore.Ping(ctx)`.
  * The probe duration is bounded by a per-request timeout defaulting to **`2 * time.Second`** (`DefaultStorageProbeTimeout`), preventing slow I/O or SQLite locks from hanging the readiness check.
  * The SQLite implementation issues a lightweight `SELECT 1;` query against the database connection.
  * **Important Scope Limitation**: The `SELECT 1` probe verifies basic database connection and engine responsiveness only; it **does not** guarantee future write transaction success, physical disk space availability, or successful ingestion.
* **Graceful Shutdown Integration**:
  * Upon receiving an OS termination signal (`SIGINT` / `SIGTERM`) or when `Server.Close()` is called, the server executes `MarkShuttingDown()`.
  * Once marked, `/readyz` immediately returns `HTTP 503 Service Unavailable`, allowing upstream proxies, load balancers, or edge clients to divert or pause traffic before the HTTP listener drains active connections.
* **Responses & Status Codes**:
  * **Ready (`HTTP 200 OK`)**: Initialization succeeded, shutdown has not begun, and the storage probe succeeded:
    ```json
    {
      "status": "ready",
      "checks": {
        "storage": "healthy"
      }
    }
    ```
  * **Storage Unavailable (`HTTP 503 Service Unavailable`)**: The storage probe failed, timed out, or the database connection was closed:
    ```json
    {
      "status": "not_ready",
      "checks": {
        "storage": "unhealthy"
      },
      "error": "storage_unavailable"
    }
    ```
  * **Server Uninitialized (`HTTP 503 Service Unavailable`)**: Startup rehydration failed or no store was configured:
    ```json
    {
      "status": "not_ready",
      "checks": {
        "storage": "unhealthy"
      },
      "error": "server_uninitialized"
    }
    ```
  * **Server Shutting Down (`HTTP 503 Service Unavailable`)**: Graceful shutdown has been initiated:
    ```json
    {
      "status": "not_ready",
      "checks": {
        "storage": "healthy"
      },
      "error": "server_shutting_down"
    }
    ```
* **`HEAD` Requests**: For `HEAD /readyz`, the server returns identical HTTP status codes (`200` or `503`) and `Content-Type: application/json` headers without transmitting a response body.

#### 3. Decoupling Liveness from Readiness
A readiness failure does **not** automatically imply a liveness failure:
* When storage experiences transient busy locks, checkpointing pauses, or slow disk writes, `/readyz` returns `503` so upstream traffic routers hold or redirect incoming edge traffic.
* If `/healthz` were coupled to storage probes, process supervisors would misinterpret transient storage latency as process failure and terminate the control plane (`SIGKILL`), interrupting in-flight SQLite WAL commits, escalating contention, and risking data loss across premature restarts.

#### 4. Operational Metrics (`GET /metrics`, `HEAD /metrics`)
* **Purpose**: Provides thread-safe, Prometheus-compatible (format version 0.0.4) operational metrics for the control plane without pulling external dependencies.
* **Exposition Format & Content-Type**:
  * Emits Prometheus standard text exposition format with `# HELP` and `# TYPE` declarations.
  * Responds with `Content-Type: text/plain; version=0.0.4; charset=utf-8`.
* **Fast-Path Performance Guarantees**:
  * Scraping `/metrics` executes zero SQLite queries and acquires no database locks.
  * Ingested batch counters, storage probe statistics, and auth failures use lock-free atomic operations or bounded RWMutex snapshots.
  * Dynamic gauges (e.g. `registered_nodes_count`) evaluate in-memory state under microsecond read locks.
  * Remains fully responsive (`200 OK`) even when the storage layer is degraded, unavailable, or when the server is in graceful shutdown.
* **Supported Methods**:
  * `GET`: Emits serialized metrics text payload.
  * `HEAD`: Returns `HTTP 200 OK` with identical headers and zero-length body.
  * Other methods return `HTTP 405 Method Not Allowed` with `Allow: GET, HEAD`.
* **Standard Control-Plane Metrics**:
  * `aegisedge_control_plane_ingested_batches_total{transport="http|nats", status="accepted|duplicate"}`: Monotonically increasing counter of batches committed into durable storage. Never incremented on validation or persistence error.
  * `aegisedge_control_plane_storage_probe_duration_seconds`: Histogram tracking the latency distribution of storage readiness `Ping` probes across sub-millisecond to multi-second buckets.
  * `aegisedge_control_plane_storage_probe_failures_total`: Counter tracking the count of storage health readiness probe failures or timeouts.
  * `aegisedge_control_plane_auth_failures_total{reason="..."}`: Counter tracking authentication and authorization rejections, strictly constrained to a bounded allowlist (`invalid_signature`, `expired_timestamp`, `future_timestamp`, `replay_detected`, `missing_auth`, `forbidden`, `unauthorized`, `unknown`).
  * `aegisedge_control_plane_nats_exhausted_deliveries_total`: Dynamic counter reflecting the total number of transient delivery exhaustion events observed by the NATS JetStream consumer.
  * `aegisedge_control_plane_registered_nodes_count`: Dynamic gauge reflecting the current count of edge nodes registered in the control-plane memory.
* **Production Security Consideration**:
  * Operational metrics should be protected in production by restricting access to authorized monitoring collectors (e.g., Prometheus scrapers) via network policies, internal listener binding, or reverse proxy authentication to prevent information disclosure.

### Practical Local Verification Examples (PowerShell)

In the local development environment, query the running control plane (e.g. `http://localhost:8080`) using `Invoke-WebRequest`:

#### A. Querying Liveness (`/healthz`)
```powershell
# Check process vitality (returns 200 OK under normal operation)
$liveness = Invoke-WebRequest -Uri "http://localhost:8080/healthz" -UseBasicParsing
Write-Host "Liveness Status:" $liveness.StatusCode
Write-Host "Liveness Body:"   $liveness.Content
```

#### B. Querying Readiness (`/readyz`)
```powershell
# Check traffic readiness (returns 200 OK when ready, 503 when degraded or draining)
try {
    $readiness = Invoke-WebRequest -Uri "http://localhost:8080/readyz" -UseBasicParsing
    Write-Host "Readiness Status:" $readiness.StatusCode
    Write-Host "Readiness Body:"   $readiness.Content
} catch {
    $resp = $_.Exception.Response
    $statusCode = [int]$resp.StatusCode
    $reader = New-Object System.IO.StreamReader($resp.GetResponseStream())
    $body = $reader.ReadToEnd()
    Write-Host "Readiness Not Ready (HTTP $statusCode): $body"
}
```

#### C. Querying Readiness via `HEAD` Method
```powershell
# Lightweight probe returning HTTP status and headers with zero payload body
$head = Invoke-WebRequest -Uri "http://localhost:8080/readyz" -Method HEAD -UseBasicParsing
Write-Host "HEAD Status:" $head.StatusCode
```

#### D. Scraping Operational Metrics (`/metrics`)
```powershell
# Scrape Prometheus text exposition format (returns 200 OK with version 0.0.4)
$metrics = Invoke-WebRequest -Uri "http://localhost:8080/metrics" -UseBasicParsing
Write-Host "Metrics Status:"       $metrics.StatusCode
Write-Host "Content-Type:"         $metrics.Headers["Content-Type"]
Write-Host "Metrics Body Preview:" ($metrics.Content -split "`n" | Select-Object -First 15 | Out-String)

# Lightweight HEAD probe
$metricsHead = Invoke-WebRequest -Uri "http://localhost:8080/metrics" -Method HEAD -UseBasicParsing
Write-Host "Metrics HEAD Status:"  $metricsHead.StatusCode
```

#### Interpreting Responses in Local Development
* **`HTTP 200 OK`**: The control-plane process is fully initialized, SQLite storage is responsive, and the instance is accepting edge registrations, heartbeats, and telemetry batches.
* **`HTTP 503 Service Unavailable`**: The instance should not receive incoming requests. Inspect the JSON `"error"` field:
  * `"storage_unavailable"`: Database probe failed or exceeded 2s timeout (e.g., file lock or closed pool).
  * `"server_uninitialized"`: Startup rehydration failed during launch.
  * `"server_shutting_down"`: Control plane received `SIGINT`/`SIGTERM` or `Close()` was called; instance is draining in-flight requests.

### Running Tests

To verify all 20 packages across the Go workspace:

```powershell
# Using the verification script
.\scripts\check.ps1

# Or directly via Go across all workspace modules
go test -v ./shared/types/... ./shared/ml/... ./edge/agent/... ./services/control-plane/...
```

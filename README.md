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

#### 5. Edge Anomaly Detection & Autonomous Response Architecture (Phases 5.1 – 6.5)

* **IMPLEMENTED**:
  * `ThresholdDetector`: Deterministic static threshold boundaries ($W=1$) with hysteresis bands and recovery thresholds (Phases 5.1–5.2).
  * `StatisticalDetector`: Deterministic rolling statistical baseline ($\mu, \sigma$, z-score) with bounded window eviction and zero-variance safety (Phase 5.4).
  * `IncidentEngine`: Local $M$-of-$N$ temporal correlation, deduplication, and canonical Incident FSM lifecycle (Phases 5.3–5.5).
  * `MLDetector`: Pure-Go Isolation Forest multivariate anomaly detection engine evaluating canonical 4-dimensional telemetry vectors (`cpu`, `memory`, `disk`, `temperature`) (Phase 5.5B, see [`ADR-0009 §30`](docs/decisions/ADR-0009-edge-anomaly-detection.md)).
  * `aegisedge-train`: Offline Isolation Forest training CLI with canonical 4D feature alignment, deterministic PRNG seed, and model serialization (Phase 5.5C).
  * `shared/ml`: Clean decoupled contracts for model manifests, canonical JSON SHA-256 integrity, runtime compatibility evaluation, and deployment lifecycle state machine (Phase 5.6A).
  * `edge/agent/modelstore`: Filesystem-backed edge model store and candidate staging boundary with multi-stage validation gates, safe bounded reads, Windows-compatible file operations, idempotency, and conflict rejection (Phase 5.6B).
  * `edge/agent/modelactivation`: Thread-safe runtime model activation and explicit rollback manager with pre-promotion validation pipeline, dynamic `RuntimeDetector` dispatching under `sync.RWMutex`, idempotency, conflict rejection, previous model disk retention in `previous/`, and repeated rollback swapping (Phases 5.6C & 5.6D).
  * `edge/agent/response`: Deterministic Response Policy Engine (`RuleBasedPolicy`), Fail-Closed Safety Validator (`StandardSafetyValidator`), Operator Approval Boundary & Lifecycle Manager (`ApprovalManager`), and Simulated Action Executor (`SimulatedExecutor`) providing safe, observable simulation and dry-run execution with idempotency, 14-point decision-binding security gate, single-use approval consumption, and zero real host mutation (Phases 6.2, 6.3, 6.5).
  * `edge/agent/storage`: Durable mitigation & approval persistence and startup crash recovery via SQLite WAL Migrations v4 (`mitigation_records`) and v5 (`approval_records`), providing local mitigation & approval identity, duplicate suppression, fail-closed restart reconciliation, and single-use atomic consumption (Phases 6.4 & 6.5).
* **DESIGNED**:
  * Autonomous Incident Response Architecture: Full response lifecycle model, operator approval ticketing, and automated telemetry verification (Phase 6.1 Design Only, see [ADR-0012](docs/decisions/ADR-0012-autonomous-incident-response.md)).
  * ML Model Distribution & Autonomous Rollback Architecture: Staged deployment, network transport, and autonomous policy-based rollback (Phase 5.5D Design Only; crash-consistent filesystem activation is not implemented, see [ADR-0009 §31](docs/decisions/ADR-0009-edge-anomaly-detection.md#31-phase-55d--ml-model-distribution--deployment-architecture)).
* **FUTURE IMPLEMENTATION**:
  * Real host actuators, production remediation, authenticated operator identities, cryptographic approval signatures, approval UI/API, identity federation, and automated telemetry-based post-action verification.
  * Ed25519 cryptographic signing, central model registry service, model distribution network transport, automated policy-driven rollback engine.

### Running Tests

To verify all 17 packages across the Go workspace:

```powershell
# Using the verification script
.\scripts\check.ps1

# Or directly via Go across all workspace modules
go test -v ./shared/types/... ./shared/ml/... ./edge/agent/... ./services/control-plane/...
```

# AegisEdge

**Autonomous Edge AI Incident-Response Platform**

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

### Running Tests
To verify all modules across the Go workspace:

```powershell
# Using the verification script
.\scripts\check.ps1

# Or directly via Go across all workspace modules
go test -v ./shared/types/... ./edge/agent/... ./services/control-plane/...
```
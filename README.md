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

---

## Quickstart & Verification (Local Development)

### Prerequisites
* **Go** 1.22+ (verified on Go 1.27)
* **Git**

### Running Tests
To verify all modules across the Go workspace:

```powershell
# Using the verification script
.\scripts\check.ps1

# Or directly via Go across all workspace modules
go test -v ./shared/types/... ./edge/agent/... ./services/control-plane/...
```
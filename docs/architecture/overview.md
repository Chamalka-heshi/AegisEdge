# AegisEdge Architectural Overview

## 1. System Vision
AegisEdge provides autonomous incident detection and mitigation on edge computing nodes while streaming consolidated telemetry and incident reports to a centralized control plane.

```mermaid
flowchart LR
    subgraph EdgeDevice ["Edge Node"]
        Collector["Metrics Collector\n(OS / Synthetic)"] --> Buffer["SQLite Local Store\n(WAL Mode)"]
        Collector --> RuleEngine["Incident Rule Engine"]
        RuleEngine --> Actuator["Simulated Actuator\n(Safe Non-destructive)"]
        Buffer --> SyncWorker["Upstream Sync Worker\n(Exponential Backoff)"]
    end

    subgraph CentralServer ["Central Control Plane"]
        APIServer["HTTP REST Ingestion"] --> IngestionHandler["Ingestion & Deduplication"]
        IngestionHandler --> StateStore["System Store\n(Repository Pattern)"]
        StateStore --> QueryAPI["Fleet Query & Incident API"]
    end

    SyncWorker -- "HTTP / REST\n(Idempotent batches)" --> APIServer
```

---

## 2. Component Responsibilities

### `edge/agent`
* **Collector**: Periodically gathers host metrics (CPU, memory, disk, network, synthetic metrics).
* **Local Storage**: Persists metrics and incident records into a local SQLite database using Write-Ahead Logging (WAL).
* **Incident Engine**: Evaluates metrics against threshold rules to detect anomalies in real time.
* **Actuator**: Executes mitigation actions locally. In Phase 1, **all actions are safely simulated** (recording the intended action, timestamp, and target without executing destructive shell commands or killing host processes).
* **Sync Worker**: Periodically queries un-synced batches from SQLite and sends them upstream over HTTP, respecting backoff and idempotency semantics.

### `services/control-plane`
* **API Server**: Exposes endpoints for device registration, telemetry ingestion, and incident logging.
* **Storage Layer**: Implements a clean repository interface (`Store`) enabling seamless transition between SQLite (for simple local dev) and PostgreSQL (for production).
* **Fleet State**: Tracks node heartbeats, liveness, and open incidents across the entire fleet.

### `shared/types`
* Contains canonical Go structs and JSON serialization logic shared between the edge agent and control plane.
* Prevents schema drift and ensures strict contract validation at compile time.

---

## 3. Safety and Non-Destructive Operation (Phase 1)
To ensure safety on developer workstations:
* No arbitrary shell commands are executed.
* No host processes are killed.
* No system services or network firewalls are modified.
* All mitigations in Phase 1 use the `SimulatedActuator` interface, which records simulated remediation operations into the audit trail for verification.

---

## 4. Deterministic Incident Lifecycle State Machine

The edge incident engine transitions through explicit, deterministic states:

```mermaid
stateDiagram-v2
    [*] --> NORMAL
    NORMAL --> ANOMALY_DETECTED : Threshold breach or anomaly detected
    ANOMALY_DETECTED --> MITIGATING : Safe mitigation action triggered
    MITIGATING --> RECOVERED : Telemetry returns to normal envelope
    MITIGATING --> ESCALATED : Mitigation failed or anomaly persists
    RECOVERED --> NORMAL : Incident closed, baseline restored
    ESCALATED --> [*] : Operator intervention required
```

* **`NORMAL`**: Baseline healthy operating state. Continuous metric collection.
* **`ANOMALY_DETECTED`**: Telemetry condition exceeded threshold for $N$ consecutive evaluation windows.
* **`MITIGATING`**: Autonomous mitigation policy is executing (safe simulated action in Phase 1).
* **`RECOVERED`**: Metrics returned to within safe limits following mitigation.
* **`ESCALATED`**: Mitigation unsuccessful or severity escalated, requiring central operator review.

---

## 5. Edge Local Durable Persistence Pipeline (Phase 2)

The edge agent enforces local persistence before considering any telemetry batch accepted, establishing the system's first durable buffering boundary:

```mermaid
flowchart TD
    TG["Telemetry Generator\n(Deterministic Synthetic)"] --> VAL["Domain Validation\n(types.TelemetryBatch.Validate)"]
    VAL --> WAL["SQLite Storage Engine\n(WAL Mode Transaction)"]
    WAL --> CONF["Confirmation Read\n(Verify Batch In SQLite Engine)"]
    CONF --> PEND["Locally Accepted Batch\n(Status: PENDING Sync)"]
```

### Core Durability Invariants:
1. **Local Persistence Precedes Ingestion**: Batches are committed to local SQLite WAL storage *before* any remote synchronization is attempted.
2. **Autonomous Offline Survivability**: The edge node does not require the control plane to start, run, or persist telemetry. If the control plane is offline, unreachable, or non-existent, the edge continues collecting and buffering data locally.
3. **Write-Ahead Logging (WAL) & Synchronous Tuning**:
   * Provides non-blocking concurrent reads while transactional writes are occurring.
   * Configured with `PRAGMA synchronous=NORMAL;` to balance write throughput and flash longevity with crash recovery.
   * A successful transaction commit means SQLite has accepted the batch according to its configured durability semantics. While `NORMAL` mode protects against database corruption across application crashes and clean reboots, transactions committed since the most recent checkpoint could be lost during an abrupt, catastrophic hardware power outage. The exact guarantees of physical persistence depend on SQLite configuration and underlying OS/filesystem write caching.
4. **Verification Read-Back**: An immediate read-back verifies that the batch was correctly written and is queryable in the local database engine before marking it accepted. It does **not** claim or prove physical persistence to non-volatile storage media.
5. **Per-Node Sequence Continuity**: On startup, the agent queries `MAX(sequence_number)` from the local database, ensuring monotonic sequence numbers across application restarts without collisions or resets.
6. **Idempotency Guarantee**: `batch_id` serves as a unique primary key. Duplicate batch insertions are detected and rejected (`ErrDuplicateBatch`), preventing duplicate records.
7. **Realistic Buffer Limits (No False Zero-Data-Loss Claims)**: Physical disk capacity on edge hardware is finite. Telemetry batches are persisted with status `PENDING` to await upstream sync. AegisEdge explicitly does **not** claim mathematically guaranteed zero data loss under indefinite network partitions.

---

## 6. Edge-to-Control-Plane Synchronization Pipeline (Phase 3)

Phase 3 introduces edge-to-control-plane synchronization over HTTP to prove the distributed recovery flow:

```mermaid
flowchart LR
    subgraph EdgeStore ["Edge Local Storage (SQLite WAL)"]
        PB["Pending Batches\n(Status: PENDING)"]
    end

    subgraph SyncerWorker ["Edge Syncer Worker"]
        PN["GetPendingNodes()"] --> PBNode["GetPendingBatchesByNode(NodeID)"]
        PBNode --> Client["HTTP Sync Client\n(Bounded Backoff Retries)"]
    end

    subgraph ControlPlane ["Control Plane Ingestion"]
        EP["POST /api/v1/telemetry/batches\n(1MB MaxBytesReader)"] --> VAL["Domain Validation"]
        VAL --> IDEM["Idempotency Filter\n(map by BatchID)"]
        IDEM -- "New" --> RES1["201 Created\nstatus: accepted"]
        IDEM -- "Duplicate" --> RES2["200 OK\nstatus: already_accepted"]
    end

    PB --> PN
    Client -- "HTTP POST" --> EP
    RES1 -- "Ack" --> Mark["MarkBatchSynced()\nStatus: SYNCED"]
    RES2 -- "Ack" --> Mark
    Client -- "Cycle Failed" --> Attempt["RecordSyncAttempt()\nattempt += 1, Status: PENDING\nHalt sequence for this node"]
```

### Core Synchronization Invariants:
1. **At-Least-Once Delivery + Idempotent Processing**: The system guarantees at-least-once delivery; exactly-once delivery is NOT claimed. Duplicates are handled idempotently via `BatchID`.
2. **Per-Node Sequence Ordering `(NodeID, SequenceNumber)`**: Sequence numbers are strictly per-node. Ordering is enforced per-node, never globally across different edge nodes.
3. **Failure Isolation Between Nodes**: If Node A sequence $N$ fails, subsequent batches for Node A are halted for that sync cycle to prevent out-of-order delivery, but Node B continues synchronizing independently.
4. **Retry Attempt Semantics**: The persisted `attempt` counter represents logical synchronization cycles and is incremented once per failed cycle, not per transient internal HTTP retry.
5. **Idempotent Duplicate Acceptance**: If the control plane returns HTTP 200 `already_accepted`, the edge marks the local batch as `SYNCED`.
6. **Timestamp Semantics**:
   * `collected_at`: Immutable edge timestamp when telemetry was captured.
   * `sent_at`: Recorded on the edge when transmission was attempted.
   * `ingested_at`: Appended by the control plane upon acceptance.
7. **Phase 3 Control Plane In-Memory Limitation**: Accepted batches survive for the lifetime of the control plane process. Process restarts reset the accepted set. Because the edge maintains records in SQLite until acknowledged, a control plane restart triggers safe re-synchronization without edge data loss. Durable control-plane storage will be introduced in later phases.

---

## 7. Event-Driven Messaging Architecture & Ingestion Pipeline (Phase 4 Design)

Phase 4 introduces an event-driven architecture using NATS to decouple edge devices from control-plane hostnames and enable multi-consumer telemetry streaming.

### Component Role Distinctions
To maintain architectural clarity throughout the platform, the messaging tiers are strictly distinguished:
* **NATS**: The underlying publish/subscribe messaging system responsible for subject-based routing, cluster topology, and connection multiplexing.
* **JetStream**: The durable streaming and persistence subsystem built into NATS, responsible for message storage, stream persistence, consumer offset tracking, deduplication windows (`Nats-Msg-Id`), and delivery acknowledgments (`Ack`/`Nak`/`Term`).
* **Consumer**: The application processing component (running within the control plane or auxiliary ingestor services) responsible for fetching messages from JetStream, validating envelopes and domain objects, enforcing application-level idempotency, executing business logic, and signaling message completion.

```mermaid
flowchart TD
    subgraph EdgeAgent ["Edge Node (AegisEdge Agent)"]
        TG["Telemetry Generator"] --> VAL["Domain Validation"]
        VAL --> WAL["SQLite WAL Storage Engine\n(Durable Local Boundary)"]
        WAL --> NP["NATS Publisher Worker\n(Wrapped Event Envelope)"]
    end

    subgraph Broker ["NATS Messaging Bus"]
        JS["JetStream Persistent Stream\naegisedge.v1.telemetry.*"]
    end

    subgraph ControlPlane ["Central Control Plane"]
        NC["NATS Consumer Group\n(Durable Pull Consumer)"] --> ENV["Envelope & Version Validation"]
        ENV --> DOM["Domain Validation (Validate())"]
        DOM --> IDEM["Idempotency Filter (BatchID)"]
        IDEM -- "New" --> COMMIT["Commit to System Store"]
        IDEM -- "Duplicate" --> NOOP["Log & Discard Duplicate"]
        COMMIT --> ACK["Message Ack()"]
        NOOP --> ACK
    end

    NP -- "Publish with Nats-Msg-Id: BatchID" --> JS
    JS -- "PubAck" --> ACKWAL["MarkBatchSynced() in SQLite"]
    JS -- "Deliver Message" --> NC
```

### Core Architecture Invariants (Phase 4):
1. **Persist Locally First, Publish Second**: SQLite remains the primary durability boundary for the edge node. NATS is an asynchronous distributed transport, not a replacement for local persistence. If NATS is unreachable or partitioned, batches remain safely stored in SQLite WAL according to configured SQLite durability semantics (`synchronous=NORMAL`), subject to local storage capacity limits and documented power loss guarantees.
2. **Canonical Event Envelope**: All NATS messages wrap payloads in a standardized metadata envelope (`event_id`, `event_type`, `schema_version`, `source_node_id`, `sequence_number`, `occurred_at`, `published_at`, `correlation_id`, `payload`).
3. **Event Identity & Deduplication**: For telemetry events, `event_id` is set identical to `batch_id`. NATS JetStream headers populate `Nats-Msg-Id: <batch_id>` for broker-side deduplication, while the control plane consumer enforces application-level idempotency by `batch_id`.
4. **Subject Hierarchy**: Formatted as `aegisedge.v1.<domain>.<node_id>` (e.g., `aegisedge.v1.telemetry.edge-node-01`). Supports per-node partitioning, subject authorization, and wildcard subscriptions (`aegisedge.v1.telemetry.*`).
5. **Delivery Semantics (At-Least-Once + Idempotent Processing)**: AegisEdge does not claim exactly-once delivery and instead uses at-least-once delivery with idempotent processing because retries, reconnects, and network or node failures can cause duplicate delivery.
6. **Per-Node Sequence Ordering**: Causality is strictly maintained per-node as `(NodeID, SequenceNumber)`. No global sequence synchronization across nodes is assumed or imposed. Node A failure isolates Node A sequence progression without blocking Node B.
7. **Transport Coexistence & Role Differentiation (No Automatic Fallback)**: The edge syncer adopts a pluggable `BatchPublisher` transport interface. At runtime, exactly one transport is active per sync worker cycle, avoiding split-brain database race conditions. Automatic runtime fallback from NATS to HTTP is **NOT implemented or implied**; when NATS connectivity is lost, telemetry remains safely buffered in local SQLite WAL according to configured durability semantics until NATS connectivity is restored. HTTP remains strictly a secondary, control-plane, and development/testing transport (for diagnostics `/healthz`, query APIs, initial device enrollment handshake, and lightweight local unit testing without an external NATS broker daemon), while NATS serves as the primary distributed streaming pipeline.


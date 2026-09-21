# ADR-0006: NATS Event-Driven Messaging Architecture and Ingestion Pipeline

## Status
Accepted (Phase 4.1 Architecture & Design)

---

## 1. Context

In AegisEdge Phase 2 and Phase 3, the edge node's local persistence and upstream synchronization foundations were established:
1. **Phase 2** proved local autonomous durability using pure-Go SQLite with Write-Ahead Logging (WAL) and `synchronous=NORMAL`.
2. **Phase 3** proved distributed synchronization semantics (at-least-once delivery, `BatchID` idempotency, per-node sequence ordering, and node failure isolation) using a temporary, direct HTTP transport (`POST /api/v1/telemetry/batches`).

While direct point-to-point HTTP verified offline recovery semantics, it has fundamental architectural limitations for a production-grade distributed incident-response platform:
* **Tight Point-to-Point Coupling**: The edge agent must know and reach a specific control-plane IP/hostname. Introducing redundant or regional control-plane workers requires external load balancers or client-side failover logic.
* **No Multi-Consumer Fan-Out**: Real-time telemetry cannot be observed simultaneously by multiple independent services (e.g., storage ingestors, streaming anomaly detectors, real-time telemetry dashboards, and audit loggers) without the control plane synchronously proxying payloads.
* **No Asynchronous Reverse Channel (Edge Commands)**: Direct client-initiated HTTP does not provide an efficient mechanism for the control plane to push reactive mitigation commands or policy updates to edge nodes without brittle long-polling or reverse tunnels.
* **Head-of-Line Blocking**: Synchronous HTTP request-response round-trips tie network socket lifecycles directly to control-plane thread scheduling.

To address these limitations, Phase 4 introduces **event-driven messaging via NATS**. 

This document establishes the architecture, event envelope contracts, subject hierarchy, delivery semantics, failure matrix, and transition strategy between Phase 3 HTTP and Phase 4 NATS before any integration code is implemented.

---

## 2. Core Design Principle

$$\textbf{PERSIST LOCALLY FIRST. PUBLISH SECOND.}$$

**NATS must NOT replace SQLite as the edge durability boundary.**

The operational invariant remains:
```text
Telemetry Generated
        ↓
Domain Validation (types.TelemetryBatch.Validate)
        ↓
Local SQLite WAL Persistence (status: PENDING)
        ↓
Durable Local Buffering Boundary
        ↓
NATS Publication (JetStream Stream)
        ↓
Control-Plane Consumer (Pull/Push Subscription)
        ↓
Envelope & Domain Validation
        ↓
Idempotent Processing (Deduplicated via BatchID)
        ↓
Consumer Acknowledgment (Ack)
```

### Component Role Distinctions
To maintain architectural clarity throughout the platform, the messaging tiers are strictly distinguished:
* **NATS**: The underlying high-performance publish/subscribe messaging system responsible for subject-based routing, cluster topology, and connection multiplexing.
* **JetStream**: The durable streaming and persistence subsystem built into NATS, responsible for message storage, stream persistence, consumer offset tracking, deduplication windows (`Nats-Msg-Id`), and delivery acknowledgments (`Ack`/`Nak`/`Term`).
* **Consumer**: The application processing component (running within the control plane or auxiliary ingestor services) responsible for fetching messages from JetStream, validating envelopes and domain objects, enforcing application-level idempotency, executing business logic, and signaling message completion.

### Durability Invariants:
1. **NATS Unavailability**: If NATS is unreachable, down, or disconnected, telemetry continues to be generated, validated, and committed to SQLite with `sync_status = 'PENDING'`. Telemetry remains buffered in SQLite according to configured SQLite durability semantics, subject to local storage capacity limits and documented SQLite crash recovery guarantees.
2. **Control-Plane Unavailability**: If the control plane consumer is offline, messages either buffer on the edge in SQLite (if NATS is down) or buffer in the NATS JetStream persistent stream (if NATS is up, subject to stream limits).
3. **Reconnection & Catch-Up**: When connectivity to NATS is restored, the edge syncer resumes publishing pending batches from SQLite strictly in `(NodeID, SequenceNumber)` order.
4. **No False Zero-Loss Claim**: NATS itself does not provide physical zero-loss guarantees under power outages or infinite partitions. Physical durability at the edge is governed solely by local SQLite WAL commits and physical storage limits.

---

## 3. Decisions

### Decision 1: Why NATS?
NATS (specifically NATS Core + JetStream) was chosen over competing messaging technologies (Kafka, RabbitMQ, MQTT) for technical reasons:
* **Single Lightweight Binary**: Written in Go, NATS has modest CPU and memory footprints relative to heavyweight enterprise message brokers, making it deployable at both resource-constrained edge gateways and centralized cloud clusters.
* **Built-In JetStream Persistence**: Provides at-least-once message persistence, consumer acknowledgments (`Ack`, `Nak`, `Term`), configurable deduplication windows (`Nats-Msg-Id`), and replay capabilities without the operational overhead of ZooKeeper or KRaft (Kafka).
* **Multi-Pattern Support**: Supports publish/subscribe (telemetry fan-out), request-reply (command dispatch), and key-value/object stores in a unified protocol.
* **Network Partition Resilience**: Native clustering with leaf nodes enables edge nodes to run local leaf brokers that automatically buffer and synchronize with upstream central clusters when WAN connectivity fluctuates.

---

### Decision 2: Event Envelope Specification

All events published to NATS must be wrapped in a canonical, strongly typed **Event Envelope**. The envelope standardizes metadata across diverse event types without polluting the domain-specific payloads.

#### Envelope Schema (JSON Specification)
```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "event_type": "aegisedge.telemetry.batch",
  "schema_version": "1.0.0",
  "source_node_id": "edge-node-01",
  "sequence_number": 42,
  "occurred_at": "2026-09-21T07:30:00.123456789Z",
  "published_at": "2026-09-21T07:30:00.150000000Z",
  "correlation_id": "7b1c4e90-c124-4f40-8ab3-61a7c5691122",
  "payload": { ... }
}
```

#### Field Responsibilities and Rationale:
| Field | Type | Required | Purpose & Technical Justification |
| :--- | :--- | :--- | :--- |
| `event_id` | `string` (UUIDv4) | Yes | Globally unique identifier for this message instance. For telemetry events, **`event_id` is set identical to `batch_id`**, preserving deterministic identity. |
| `event_type` | `string` | Yes | Dot-notated string discriminator (e.g., `aegisedge.telemetry.batch`, `aegisedge.incident.detected`). Allows consumers to multiplex handlers without inspecting payload bodies. |
| `schema_version`| `string` (SemVer) | Yes | Identifies the contract version (e.g., `"1.0.0"`). Enables non-breaking schema evolution and rejects unsupported payload structures at ingestion. |
| `source_node_id`| `string` | Yes | Unique edge node identifier that generated the event. Ensures provenance and partitions streams per device. |
| `sequence_number`| `int64` | Yes | Monotonically increasing counter per node. Provides per-node causal ordering and gap detection without relying on physical clock synchrony. |
| `occurred_at` | `time.Time` (RFC3339Nano)| Yes | Edge UTC timestamp when the domain event occurred (`collected_at` for telemetry). Immutable across all network retries. |
| `published_at` | `time.Time` (RFC3339Nano)| Yes | Edge UTC timestamp when the event was dispatched to NATS. Used to measure transmission delay and network lag. |
| `correlation_id`| `string` (UUIDv4) | No | Tracing identifier linking related causal events (e.g., Anomaly Detected $\rightarrow$ Incident Created $\rightarrow$ Mitigation Dispatched $\rightarrow$ Mitigated). |
| `payload` | `json.RawMessage` | Yes | The canonical domain object (e.g., `types.TelemetryBatch`, `types.Incident`). Preserves existing domain contracts without duplication. |

---

### Decision 3: Event Identity and Idempotency Strategy

#### Relationship between `BatchID` and `EventID`:
* **Decision**: For telemetry batch events, **`EventID = BatchID`**.
* **Rationale**: Introducing a separate random `EventID` independent of `BatchID` would create ambiguity during deduplication: if the edge republishes the same local SQLite batch across network reconnects, generating a new `EventID` would bypass broker-level deduplication. By setting `EventID = BatchID`, both the broker and the consumer have a single, stable identity for the logical telemetry unit.
* **Deduplication Layers**:
  1. **NATS JetStream Deduplication**: The edge publisher populates the NATS header `Nats-Msg-Id: <batch_id>`. JetStream streams configured with a deduplication window (e.g., 24 hours) automatically discard duplicate publications within that window.
  2. **Consumer-Side Store Deduplication**: The control plane consumer maintains an idempotency filter keyed on `BatchID` (in-memory for Phase 3/4.1, relational database primary key in later phases). If a duplicate event escapes the broker window, the consumer recognizes the existing `BatchID` and treats processing as an idempotent no-op.

---

### Decision 4: NATS Subject Hierarchy

Subjects are structured hierarchically to enable fine-grained routing, wildcard subscriptions, and role-based access control (RBAC):

$$\textbf{aegisedge.v1.}<\textit{domain}>.<\textit{node\_id}>$$

#### Subject Structure:
* **Root Namespace**: `aegisedge` (isolates platform messages).
* **API Version**: `v1` (enables major protocol upgrades on distinct subjects).
* **Domain Stream**: Category of events (`telemetry`, `incident`, `alert`, `command`).
* **Node Partition**: `<node_id>` (enables per-device filtering, targeted command delivery, and subject authorization).

#### Canonical Subjects:
| Subject Pattern | Direction | Purpose | Example |
| :--- | :--- | :--- | :--- |
| `aegisedge.v1.telemetry.<node_id>` | Edge $\rightarrow$ CP | Sequence-numbered telemetry batches | `aegisedge.v1.telemetry.edge-node-01` |
| `aegisedge.v1.incident.<node_id>` | Edge $\rightarrow$ CP | Incident state changes and anomaly detections | `aegisedge.v1.incident.edge-node-01` |
| `aegisedge.v1.alert.<node_id>` | Edge $\rightarrow$ CP | High-severity operator notifications | `aegisedge.v1.alert.edge-node-01` |
| `aegisedge.v1.command.<node_id>` | CP $\rightarrow$ Edge | Targeted mitigation commands dispatched to edge | `aegisedge.v1.command.edge-node-01` |

#### Consumer Subscription Patterns:
* **Global Telemetry Ingestor**: Subscribes to `aegisedge.v1.telemetry.*` (wildcard matches all nodes).
* **Fleet Command Router**: Subscribes to `aegisedge.v1.command.*`.
* **Single-Node Edge Listener**: Subscribes strictly to `aegisedge.v1.command.edge-node-01` (denying access to other nodes).
* **Audit Logger**: Subscribes to `aegisedge.v1.>` (full recursive wildcard).

---

### Decision 5: Delivery Semantics

$$\textbf{Target Semantic: At-Least-Once Delivery + Idempotent Processing}$$

* **Delivery Semantics Rationale**: AegisEdge does not claim exactly-once delivery and instead uses at-least-once delivery with idempotent processing because retries, reconnects, and network or node failures can cause duplicate delivery. In a distributed edge-cloud system subject to network partitions, transient disconnections, and process crashes, enforcing exactly-once delivery across the network would require distributed locks or two-phase coordination that violates edge local autonomy and resilience.
* **Publisher Protocol (Edge)**:
  1. Read pending batch from SQLite (`sync_status = 'PENDING'`).
  2. Publish envelope to NATS JetStream subject with header `Nats-Msg-Id: <batch_id>`.
  3. Await JetStream publication acknowledgment (`PubAck`) within context timeout (5s).
  4. If `PubAck` received: Execute `store.MarkBatchSynced(ctx, batchID, sentAt)`. Status becomes `SYNCED`.
  5. If publish errors or times out: Execute `store.RecordSyncAttempt(ctx, batchID, sentAt)`. Status remains `PENDING`. Halt subsequent publishes for this node in the current cycle.
* **Consumer Protocol (Control Plane)**:
  1. Pull event from JetStream consumer group.
  2. Parse and validate `EventEnvelope` structure and `schema_version`.
  3. Validate inner payload (`batch.Validate()`).
  4. Check idempotency:
     - If `batch_id` already ingested: Log idempotent duplicate, call `msg.Ack()`.
     - If new: Record batch in control-plane store, call `msg.Ack()`.
  5. If domain validation fails: Call `msg.Term()` (terminate delivery to prevent infinite poison-pill redelivery loops) and log error.
  6. If transient store failure occurs: Call `msg.NakWithDelay()` for bounded backoff redelivery.

---

### Decision 6: Per-Node Ordering Guarantees

* **Scope**: Sequence ordering is strictly scoped to the tuple:
  $$(NodeID, SequenceNumber)$$
* **No Global Ordering**: Different edge nodes operate on independent physical hardware with independent clocks. Imposing a global total order across nodes would require a centralized coordinator, introducing a single point of failure and bottleneck.
* **Failure Isolation**:
  * Edge Node A has pending batches $1, 2, 3$.
  * Edge Node B has pending batches $1, 2$.
  * If Node A batch $2$ fails during publication, Node A halts sequence $3$ to prevent sequence inversion.
  * Node B is completely unaffected and continues publishing batches $1$ and $2$ successfully.
* **Stream Partitioning**: In NATS JetStream, subject partitioning via `aegisedge.v1.telemetry.<node_id>` allows consumers to process distinct nodes in parallel while preserving strict FIFO ordering within each node's subject.

---

### Decision 7: Phase 3 HTTP vs. Phase 4 NATS Transition Strategy

#### Analysis of Options:
| Strategy | Description | Architectural Assessment | Decision |
| :--- | :--- | :--- | :--- |
| **Option A** | Permanent Fallback (HTTP active if NATS fails) | Creates split-brain risk, race conditions between sync loops competing for SQLite rows, duplicate configuration complexity, and confusing telemetry metrics. | **Rejected** |
| **Option B** | Immediate Deletion of Phase 3 HTTP | Discards working, tested offline-recovery code before NATS is proven; breaks lightweight testing harnesses (`httptest.Server`). | **Rejected** |
| **Option C** | Temporary Coexistence during Migration | Allows incremental development but risks dual-synchronization races if not strictly coordinated. | **Adopted with safeguards** |
| **Option D** | Architectural Role Differentiation | Assigns distinct responsibilities to HTTP and NATS based on their inherent strengths. | **Adopted as final architecture** |

#### Chosen Strategy: Pluggable Transport Abstraction + Role Differentiation (Option D + C)
1. **Pluggable Transport Architecture**: The edge `Syncer` will depend on a generic interface:
   ```go
   type BatchPublisher interface {
       PublishBatch(ctx context.Context, batch *types.TelemetryBatch) (*PublishResult, error)
   }
   ```
   * Phase 3 `sync.HTTPClient` satisfies this contract.
   * Phase 4 `nats.Publisher` will satisfy this contract.
2. **Single Active Transport Invariant**: At any given time, the edge agent's synchronization worker runs with **exactly one active transport** (configured via `AEGISEDGE_SYNC_TRANSPORT = "nats" | "http"`). This eliminates dual-worker race conditions on SQLite state.
3. **Role Differentiation**:
   * **NATS (Primary Asynchronous Event Bus)**: High-throughput telemetry streaming, multi-consumer fan-out, incident dissemination, real-time alerting, and bidirectional command dispatch.
   * **HTTP (Control-Plane Bootstrap & Inspection API)**:
     - Diagnostic inspection endpoints (`/healthz`, `/api/v1/telemetry/batches` query).
     - Initial device enrollment handshake (exchanging bootstrap secrets for NATS credentials).
     - Lightweight integration and unit testing without running an external NATS broker daemon.
     - Constrained edge environments where outbound TCP port 4222 is blocked by network firewalls.

> [!IMPORTANT]
> **No Automatic Runtime Fallback**: Automatic runtime fallback from NATS to HTTP (such as dynamically switching transport from NATS to HTTP when NATS connectivity is lost) is **NOT implemented or implied** by Phase 4.1. If NATS is the configured transport and becomes unreachable, the edge agent does not fail over to HTTP; instead, telemetry safely remains buffered locally in SQLite WAL according to configured durability semantics until NATS connectivity is restored. HTTP remains strictly a secondary, control-plane, bootstrap, and development/testing transport.

---

### Decision 8: Comprehensive Failure Matrix (14 Scenarios)

| # | Scenario | SQLite Impact | Event Status | Retry Behavior | Data Preservation & Durability Semantics | Recovery Mechanism |
| :- | :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | **NATS Unavailable** | None. Batch committed to WAL. | Remains `PENDING`. | Edge syncer exponential backoff. | Telemetry remains buffered in SQLite WAL according to configured SQLite durability semantics (`synchronous=NORMAL`), subject to local storage capacity and documented power loss limits. | When NATS connectivity restores, edge syncer resumes publishing pending batches in strict `(NodeID, SequenceNumber)` order. |
| 2 | **NATS starts after Edge** | None. Batches continue buffering in WAL. | Remains `PENDING`. | Syncer retries every `SyncInterval`. | Telemetry remains buffered in SQLite WAL according to configured SQLite durability semantics, subject to local storage capacity. | Edge detects broker availability on subsequent sync cycle and publishes pending queue in sequence order. |
| 3 | **NATS Broker Restarts** | None. Edge retains unacknowledged rows. | Remains `PENDING` until `PubAck`. | Client reconnects automatically. | Unacknowledged batches remain buffered in SQLite WAL with status `PENDING` according to configured durability semantics. | Client reconnect handler restores session; edge publishes pending batches and marks `SYNCED` upon receiving `PubAck`. |
| 4 | **CP Consumer Unavailable** | Marked `SYNCED` upon `PubAck`. | Durable in JetStream stream. | JetStream retains stream messages. | Telemetry is durably retained in NATS JetStream stream, subject to configured stream storage limits and retention policies. | When control-plane consumer comes online, it binds to durable stream consumer group and drains backlog. |
| 5 | **Consumer Disconnects Mid-Stream** | None. | Unacked in JetStream. | JetStream redelivers after `AckWait`. | Preserved in JetStream stream; unacknowledged message redelivered upon `AckWait` timeout. | Active or reconnected consumer receives redelivery; idempotency check on `BatchID` prevents duplicate state mutation. |
| 6 | **Duplicate Event Delivered** | None. | Processed idempotently. | None required. | Original telemetry is already durably recorded in control plane; duplicate message causes no state corruption. | Consumer detects existing `BatchID` in idempotency filter, logs duplicate, and returns immediate `Ack()`. |
| 7 | **Malformed JSON Payload** | None. | Rejected by consumer. | Terminated (`msg.Term()`). | Malformed payload rejected at ingestion boundary; terminated to prevent poison-pill crash loops. | Consumer calls `msg.Term()` (no redelivery), increments parse error metrics, and routes payload to dead-letter diagnostic logs. |
| 8 | **Invalid Schema Version** | None. | Rejected by consumer. | Terminated (`msg.Term()`). | Incompatible schema version rejected at contract validation; prevents corrupting downstream domain state. | Consumer calls `msg.Term()`, emits operator alert indicating schema mismatch, and retains raw payload in diagnostic log. |
| 9 | **Network Partition (Edge $\leftrightarrow$ NATS)** | None. Local telemetry ingestion continues. | Remains `PENDING`. | Exponential backoff up to `MaxBackoff`. | Telemetry remains buffered in SQLite WAL according to configured durability semantics, subject to local storage capacity. | Edge buffers until partition heals; syncer resumes publishing in strict per-node sequence order. |
| 10| **Edge Agent Process Crashes** | WAL uncheckpointed frames recovered on startup. | `PENDING` rows queried on restart. | Syncer reads sequence from SQLite. | Committed transactions recovered from SQLite WAL upon restart; uncommitted OS cache frames subject to `synchronous=NORMAL` durability limits. | Agent restarts, recovers database via SQLite WAL, queries un-synced rows, and resumes publishing in sequence order. |
| 11| **Control-Plane Process Crashes** | None on edge. | Preserved in JetStream. | JetStream redelivers unacked messages. | Preserved in JetStream persistent stream storage; edge retains unacknowledged batches in SQLite until `PubAck`. | Control plane process restarts, reconnects to NATS consumer group, and resumes draining stream. |
| 12| **NATS Publish Timeout** | `attempt` counter incremented by 1. | Remains `PENDING`. | Retried on next sync cycle. | Batch remains buffered in SQLite with status `PENDING` according to configured durability semantics; attempt counter incremented. | Syncer retries on subsequent cycle. If previous attempt reached broker, consumer-side `BatchID` idempotency suppresses duplicate. |
| 13| **Burst of Pending Telemetry** | Batches queued in SQLite WAL. | Batches processed in FIFO chunks. | Throttled by `limitPerNode`. | Backlog safely buffered in SQLite WAL up to local disk capacity; read queries throttled by `limitPerNode`. | Syncer fetches bounded pages (e.g. 50 batches per node per cycle) to prevent memory exhaustion and preserve ordering. |
| 14| **Out-of-Order Event Arrival** | None. | Evaluated by `sequence_number`. | None required. | All received events preserved in store; out-of-order sequence detected via `sequence_number`. | Consumer evaluates `(NodeID, SequenceNumber)`, ingests valid payload, and logs or alerts on sequence gap. |

---

### Decision 9: Future Extensibility (Incident & Autonomous Response)

The event-driven architecture is explicitly designed to support downstream autonomous response loops in subsequent phases without altering core transport contracts:

```text
Telemetry Event (aegisedge.v1.telemetry.<node_id>)
        ↓
Streaming Anomaly Detector
        ↓
Anomaly Detected Event (aegisedge.v1.anomaly.<node_id>)
        ↓
Incident Engine Evaluator
        ↓
Incident Created Event (aegisedge.v1.incident.<node_id>)
        ↓
Autonomous Mitigation Decider
        ↓
Mitigation Command (aegisedge.v1.command.<node_id>)
        ↓
Safe Edge Actuator (Simulated / Sandboxed)
        ↓
Mitigation Result Event (aegisedge.v1.mitigation.<node_id>)
```

#### Reserved Future Event Types:
* `aegisedge.telemetry.batch`: Periodic sensor/system metric payload.
* `aegisedge.anomaly.detected`: Statistical/ML threshold anomaly alert.
* `aegisedge.incident.created`: Formal incident lifecycle initiation.
* `aegisedge.incident.transition`: Incident state machine transition (`MITIGATING`, `RECOVERED`).
* `aegisedge.command.mitigation`: Allowlisted mitigation command dispatched to edge.
* `aegisedge.command.result`: Execution audit trail and mitigation outcome.

---

### Decision 10: Security Architecture (Specification for Future Phases)

While implementation of security primitives belongs to later phases, the architecture mandates:
1. **Transport Encryption**: All NATS TCP traffic must use TLS 1.3 with modern cipher suites. Plaintext TCP is strictly restricted to local testing environments.
2. **Decentralized Authentication (NKey & JWT)**: Edge nodes authenticate using NATS 2.0 public key credentials (NKey user seeds) without sharing master secrets.
3. **Subject-Level Authorization**:
   * Edge Node `edge-01` has publish permissions **only** to `aegisedge.v1.*.edge-01`.
   * Edge Node `edge-01` has subscribe permissions **only** to `aegisedge.v1.command.edge-01`.
   * Nodes cannot snoop or inject events into subjects belonging to other nodes.
4. **Payload Size Guards**: Max message payload enforced at 1MB at both NATS server configuration (`max_payload`) and consumer envelope unmarshaling.
5. **Replay & Injection Mitigation**: Messages must have non-zero `occurred_at`, strictly monotonic `sequence_number`, and valid `event_id` matching known registered nodes.

---

## 4. Alternatives Considered

1. **Apache Kafka**:
   * *Pros*: Industry standard for high-throughput enterprise event streaming.
   * *Cons*: Requires substantial memory and JVM operational overhead, complex clustering (ZooKeeper/KRaft), and heavy client dependencies. Completely unsuitable for resource-constrained edge gateways.
2. **MQTT (Mosquitto / EMQX)**:
   * *Pros*: Standard lightweight IoT protocol.
   * *Cons*: Lacks native distributed stream persistence (requires external databases for durable message replay), primitive clustering at cloud scale, and separate tooling required for pub/sub vs streaming.
3. **RabbitMQ**:
   * *Pros*: Sophisticated routing topologies via AMQP exchanges.
   * *Cons*: Erlang runtime operational complexity, higher memory footprint, and JetStream provides simpler, faster stream semantics in pure Go.
4. **Dual Active Synchronous Transport (Active-Active HTTP + NATS)**:
   * *Pros*: Redundant delivery paths.
   * *Cons*: Produces race conditions where both HTTP and NATS workers attempt to sync the same SQLite rows, resulting in database lock contention, duplicate network traffic, and conflicting retry state. Rejected in favor of single-active transport selection.

---

## 5. Consequences

### Positive
* Fully decouples the edge agent from specific control-plane hostnames and instances.
* Enables horizontal scaling of control-plane consumers using NATS JetStream consumer groups.
* Unlocks real-time multi-consumer fan-out (telemetry stream can feed storage, anomaly detection, and dashboards concurrently).
* Provides an architectural foundation for bidirectional command-and-control.
* Preserves 100% of Phase 2 local persistence and Phase 3 idempotency invariants.

### Negative / Trade-Offs
* Introduces an external infrastructure dependency (NATS server) for distributed operation.
* Requires managing NATS connection lifecycles, JetStream stream provisioning, and consumer group configurations in subsequent implementation phases.

---

## 6. Explicit Non-Goals (What is NOT Implemented in Phase 4.1)

In strict accordance with Phase 4.1 boundaries, the following were **deliberately NOT implemented**:
* No NATS Go library dependencies added to `go.mod`.
* No NATS daemon or Docker containers started.
* No Go publisher or consumer code written.
* No SQLite schema migrations or alterations.
* No removal or modification of existing Phase 3 HTTP synchronization.
* No Kubernetes, JetStream provisioning, or cloud deployment artifacts created.
* No ML models or anomaly detection logic added.

# ADR-0005: Edge-to-Control-Plane HTTP Synchronization and Offline Recovery

## Status
Accepted

## Context
In AegisEdge Phase 2, the edge node's local persistence foundation was established using pure-Go SQLite with Write-Ahead Logging (WAL) and `synchronous=NORMAL`. That established durable local buffering, proving the principle that:
$$\text{The edge node operates autonomously without an active control plane.}$$

Phase 3 introduces the first distributed capability of the platform: synchronizing telemetry buffered locally in SQLite to the central control plane after connectivity is restored.

We must define the communication transport, delivery semantics, ordering guarantees, failure isolation rules, and idempotency mechanisms that govern this synchronization pipeline.

---

## Decisions

### 1. Delivery Semantics: At-Least-Once Delivery + Idempotent Processing
The system explicitly implements **at-least-once delivery combined with idempotent server-side processing**.
* **No Exactly-Once Delivery Claim**: In distributed systems facing arbitrary network partitions and unacknowledged in-flight packets, true exactly-once network delivery is mathematically impossible without two-phase commit overhead that edge devices cannot sustain.
* **Guarantee**: Every telemetry batch generated at the edge is guaranteed to reach the control plane at least once, provided physical edge storage limits are not exceeded. Duplicate transmissions resulting from network acknowledgments lost in transit are deduplicated idempotently at the control plane using the stable `BatchID`.

### 2. Idempotent Ingestion and Duplicate Handling
* **Stable Idempotency Key**: Each `TelemetryBatch` is assigned an immutable `BatchID` (UUID) upon edge generation.
* **Control-Plane Ingestion**:
  * New `BatchID`: Control plane records the batch and responds with `HTTP 201 Created` (`{"status": "accepted", ...}`).
  * Known `BatchID`: Control plane treats the request as a duplicate no-op and responds with `HTTP 200 OK` (`{"status": "already_accepted", ...}`).
* **Edge Idempotent Acceptance**: When the edge syncer receives `HTTP 200 already_accepted`, it treats this as a **successful synchronization outcome** and immediately transitions the local SQLite record from `PENDING` to `SYNCED`.

### 3. Per-Node Sequence Ordering `(NodeID, SequenceNumber)`
* `SequenceNumber` is strictly scoped to `NodeID`. It is never treated as a global ordering mechanism across heterogeneous edge devices.
* Synchronization ordering is reasoned about as the tuple:
  $$(NodeID, SequenceNumber)$$
* For a single node, batches are processed in strict ascending sequence order ($1 \rightarrow 2 \rightarrow 3$). Different edge nodes synchronize independently and concurrently without artificial global interleaving constraints.

### 4. Failure Isolation Between Nodes
In multi-node deployments or multi-device edge environments:
* If Node A encounters a synchronization failure on sequence $N$, subsequent batches for Node A (sequence $N+1$) are halted for that sync cycle to prevent out-of-order delivery.
* **Node B is NOT blocked**: Failure isolation ensures that synchronization errors on Node A do not prevent Node B from continuing to synchronize its pending batches.

### 5. Retry Attempt Semantics: Logical Sync Cycles vs Transient HTTP Retries
We strictly distinguish an **individual HTTP request retry** from a **logical synchronization cycle**:
* **Transient HTTP Retries**: The HTTP client automatically retries transient 5xx or network errors up to $N$ times (default: 3) with exponential backoff within a single operation. These retries are transient and do not mutate the local database.
* **Logical Sync Cycle**: If all HTTP retries for a batch fail, the logical synchronization cycle fails. At that point, `RecordSyncAttempt` increments the persisted SQLite `attempt` counter **exactly once per cycle**.
* The persisted `attempt` field accurately reflects full synchronization cycle failures, not internal socket retries.

### 6. Persisted Sync State Machine
In Phase 3, the only persisted state transition is:
$$\text{PENDING} \longrightarrow \text{SYNCED}$$
* **Failed Sync $\neq$ Lost Data**: When a sync attempt fails (due to network timeout, server 5xx, or DNS refusal), the record **remains in `PENDING` status**. It is never deleted, skipped, or moved to a fatal terminal state.
* The edge retains all un-synced telemetry safely in SQLite until upstream acknowledgment is received.

### 7. Dual and Triple Timestamp Semantics
To prevent clock skew anomalies and maintain audit integrity:
* `collected_at`: Set when telemetry is collected at the edge. It is immutable and never overwritten.
* `sent_at`: Set by the edge syncer when a synchronization cycle is executed, recorded in SQLite and passed upstream.
* `ingested_at`: Set by the control plane using its synchronized UTC wall-clock time upon successfully accepting the batch.

### 8. Temporary Direct HTTP Transport
* Direct HTTP (`POST /api/v1/telemetry/batches`) is introduced as a temporary, lightweight transport for Phase 3 to validate offline buffering and idempotency semantics.
* Standard library `net/http` is used without external frameworks. Payloads are limited to 1MB via `http.MaxBytesReader`, and malformed payloads are rejected with `HTTP 400 Bad Request`.
* Message brokers (such as NATS) will be integrated in subsequent phases to support streaming pub/sub and distributed routing.

### 9. Phase 3 In-Memory Control Plane Limitation
* **In-Memory Cache**: In Phase 3, the control plane stores accepted batches strictly in memory (`map[string]*IngestedBatch`).
* **Process Lifetime**: Accepted batches survive only for the lifetime of the control plane process. Restarting the control plane resets its accepted set.
* **Safety**: Because the edge node retains its local SQLite records until synchronization is confirmed, a control-plane restart does not cause local edge data loss. If the control plane restarts, the edge can safely re-transmit pending batches.
* Durable control plane persistence (PostgreSQL / time-series storage) will be implemented in later phases.

---

## Consequences

### Positive
* Proves the complete offline $\rightarrow$ online synchronization loop under realistic edge network constraints.
* Prevents data loss during control plane outages and WAN disconnections.
* Protects against duplicate processing through deterministic `BatchID` idempotency.
* Preserves per-node FIFO ordering while preventing single-node failures from cascading across the fleet.

### Negative / Limitations
* In Phase 3, control plane restarts reset the deduplication cache because persistent central storage has not yet been introduced.
* Direct HTTP point-to-point connections require the edge agent to know the control plane URL, which will later be abstracted by message brokers.

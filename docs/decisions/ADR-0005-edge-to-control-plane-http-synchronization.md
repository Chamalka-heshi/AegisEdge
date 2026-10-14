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
* **Stable Idempotency Key**: Each `TelemetryBatch` is assigned an immutable `BatchID` (UUID) upon edge generation. Ingestion deduplication is scoped by the composite identity `(NodeID, BatchID)` (`nodeID/batchID`), ensuring distinct nodes submitting identical UUIDs cannot suppress one another.
* **Control-Plane Ingestion**:
  * New `(NodeID, BatchID)`: Control plane records the batch metadata, appends payload to bounded history, and responds with `HTTP 201 Created` (`{"status": "accepted", ...}`).
  * Known `(NodeID, BatchID)`: Control plane treats the request as a duplicate no-op and responds with `HTTP 200 OK` (`{"status": "already_accepted", ...}`).
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

### 9. Control Plane Durable Idempotency Architecture (Phase 6.22B Hardening)
* **At-Least-Once Transport Delivery + Durable Idempotent Ingestion**:
  The system explicitly implements at-least-once transport delivery (via HTTP or NATS JetStream) combined with server-side persistent idempotency. True exactly-once network delivery is mathematically impossible across distributed network partitions; instead, exactly-once processing outcome is achieved by atomically persisting idempotency metadata before network acknowledgment.
* **Durable Composite Idempotency Key `(NodeID, BatchID)`**:
  Deduplication is strictly scoped by the composite identity tuple `(NodeID, BatchID)` enforced via SQLite `PRIMARY KEY (node_id, batch_id)` in table `cp_ingested_batches`. Delimiter-concatenated strings are prohibited, preventing delimiter injection attacks and eliminating cross-node UUID collisions.
* **Decoupled Lightweight Metadata vs. Bounded In-Memory Cache**:
  * **Persistent Metadata**: The SQLite store retains lightweight metadata (`node_id`, `batch_id`, `sequence_number`, `collected_at`, `ingested_at`) in `cp_ingested_batches` for rapid indexing and deduplication checks.
  * **Bounded Inspection Cache**: Telemetry batch payloads for rapid in-memory inspection (`GET /api/v1/telemetry/batches`) are retained in a thread-safe circular ring buffer (`historyRingBuffer`, default: 1,000 batches). When capacity is reached, older entries in the cache are overwritten in $O(1)$ time.
  * **No Unsafe Eviction**: Naive sequence-watermark eviction, TTL expiration, or LRU deletion of deduplication keys is prohibited to ensure delayed or reordered retries are never re-accepted as novel batches.
* **Database Path & Persistence Configuration**:
  Configured via CLI flag `--db-path` and environment variable `AEGISEDGE_CP_DB_PATH` (default: `data/control-plane.db`). File-based persistence survives process crashes and restarts. Standard SQLite WAL checkpoints and backups apply.
* **Single-Instance SQLite Constraint**:
  The pure-Go SQLite storage engine (`modernc.org/sqlite`) operates in WAL mode with connection pooling serialized to single-writer access (`SetMaxOpenConns(1)`). This engine is designed for single control-plane process deployments or edge gateway setups. It does **not** support multiple independent control-plane processes concurrently mounting and writing to the same database file across network filesystems (NFS/SMB). Clustered multi-node control planes can evaluate distributed storage (e.g. NATS JetStream KV or PostgreSQL) in future phases.

### 10. Atomic Durable Payload Acceptance (Phase 6.22C Hardening)
* **Closing the Acceptance Gap**:
  Persisting only deduplication metadata while retaining batch payloads solely in volatile memory created an unrecoverable acceptance gap: if the control plane crashed after acknowledging an HTTP/NATS request, the edge node marked the batch `SYNCED` and discarded its retry buffer, while the control plane restarted with metadata recognizing the batch as a duplicate but having permanently lost all metric samples.
* **Atomic Two-Table Persistence Schema**:
  To close this gap, control-plane persistence couples metadata indexing and full payload storage within a single atomic SQLite transaction:
  1. `cp_ingested_batches`: Stores deduplication metadata (`node_id`, `batch_id`, `sequence_number`, `collected_at`, `ingested_at`) with `PRIMARY KEY (node_id, batch_id)`.
  2. `cp_telemetry_payloads`: Stores full serialized JSON telemetry payloads (`node_id`, `batch_id`, `sequence_number`, `collected_at`, `ingested_at`, `payload_json`) with `PRIMARY KEY (node_id, batch_id)` and foreign key constraint referencing `cp_ingested_batches(node_id, batch_id) ON DELETE CASCADE`.
* **Atomic Acceptance Guarantee**:
  * Both table inserts execute within `tx, err := s.db.BeginTx(ctx, nil)`.
  * If serialization fails, disk space is exhausted, or any table write encounters an error, `tx.Rollback()` is executed immediately. The batch is **never acknowledged** upstream (HTTP returns 500; NATS does not ACK).
  * Network acknowledgment (`HTTP 201 Created` or NATS ACK) is dispatched **strictly after** `tx.Commit()` succeeds.
  * A batch is treated as an idempotent duplicate (`HTTP 200 already_accepted`) only if the composite key committed successfully in a previous atomic transaction.
* **Durability Configuration (`PRAGMA synchronous=FULL`)**:
  In WAL mode, SQLite defaults to `synchronous=NORMAL`, which syncs WAL frames only during checkpoints. To ensure data survivability across abrupt power failure or host crashes, the control-plane SQLite store is configured with:
  ```sql
  PRAGMA journal_mode=WAL;
  PRAGMA synchronous=FULL;
  PRAGMA foreign_keys=ON;
  PRAGMA busy_timeout=5000;
  ```
  `synchronous=FULL` forces an fsync barrier to physical non-volatile storage on every transaction commit, guaranteeing payload durability before network acknowledgment.
* **Startup Rehydration and Durable Payload Retrieval**:
  * **Startup Warming**: On process startup, `NewServer` queries `store.GetRecentBatches` to warm the in-memory `historyRingBuffer` with up to its capacity in strict chronological FIFO order, ensuring `GET /api/v1/telemetry/batches` immediately reflects durable history without requiring new network traffic.
  * **Durable Retrieval**: Payloads evicted from the in-memory inspection ring buffer are never lost; they remain durably queryable via `store.GetPayload(ctx, nodeID, batchID)` throughout the lifecycle of the control-plane database.

---

## Consequences

### Positive
* Proves the complete offline $\rightarrow$ online synchronization loop under realistic edge network constraints.
* Prevents telemetry metric data loss during control plane outages, process crashes, and WAN disconnections.
* Closes the durable payload acceptance gap: telemetry payloads and idempotency metadata are committed atomically before network acknowledgment.
* Protects against duplicate processing through deterministic composite `(NodeID, BatchID)` durable idempotency that survives control-plane restarts.
* Decouples persistent deduplication and payload storage from in-memory inspection caches, preventing heap exhaustion while retaining full historical payloads.
* Preloads the in-memory inspection ring buffer upon startup, preserving continuous operational observability across restarts.
* Preserves per-node FIFO ordering while preventing single-node failures from cascading across the fleet.
* Eliminates cross-node batch suppression and delimiter ambiguity vulnerabilities.

### Negative / Limitations
* Embedded SQLite storage is limited to a single control-plane process instance; horizontal multi-instance scaling requires distributed storage (e.g. NATS JetStream KV or PostgreSQL).
* `PRAGMA synchronous=FULL` introduces an fsync disk round-trip per novel batch commit, incurring higher disk I/O latency than volatile memory acceptance. This trade-off is strictly necessary to guarantee durability before network acknowledgment.
* Direct HTTP point-to-point connections require the edge agent to know the control plane URL, which is abstracted when NATS event-driven transport is enabled.

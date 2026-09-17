# ADR-0004: Edge-Local Persistence and WAL Durability

## Status
Accepted

## Context
A primary architectural principle of AegisEdge is offline survivability: edge nodes must operate autonomously and continuously record system telemetry even when WAN connectivity is severed or the central control plane is unavailable.

To fulfill this requirement, the edge agent needs a local embedded storage engine acting as the system's first durable buffering boundary, satisfying:
1. **Local Durability Boundary**: Telemetry batches must be accepted and transactionally committed by the local database engine before the collection pipeline considers them locally accepted.
2. **Crash Resilience**: Process termination, unexpected application crashes, or clean OS restarts must not corrupt local storage.
3. **High Write Performance & Concurrency**: Telemetry writes should not block background query reads.
4. **Portability & Zero CGO Dependencies**: Edge nodes span diverse architectures (ARM64, AMD64) and OS environments. The storage engine should compile natively without external C toolchains or dynamic library dependencies.

## Decisions

### 1. SQLite with Pure-Go Driver (`modernc.org/sqlite`)
We selected SQLite using `modernc.org/sqlite` behind a clean repository interface (`storage.Store`).
* **Zero CGO**: Standard SQLite bindings (`mattn/go-sqlite3`) require a C compiler (GCC/MinGW) and CGO, creating cross-compilation hurdles for edge targets (such as embedded ARM Linux boards) and Windows developer machines. `modernc.org/sqlite` is 100% pure Go, compiling cleanly across all architectures with `CGO_ENABLED=0`.
* **Embedded & Serverless**: Operates within the edge agent process, eliminating external daemon processes, socket overhead, or separate database maintenance.

### 2. Write-Ahead Logging (WAL Mode)
We configure SQLite with Write-Ahead Logging (`PRAGMA journal_mode=WAL;`):
* **Concurrent Reads and Writes**: Writers append to a separate WAL journal file without locking out concurrent readers (such as future sync workers or local anomaly monitors).
* **Sequential Disk I/O**: WAL writes append sequentially to the log file, minimizing random disk seek latency on constrained edge storage (eMMC / SD cards / SSDs).
* **Crash Recovery Characteristics**: In the event of an application crash or process termination, uncommitted transactions in the journal are cleanly rolled back upon recovery without database file corruption.

### 3. Synchronous Tuning: `synchronous=NORMAL` vs. `FULL`
We explicitly configure `PRAGMA synchronous=NORMAL;`.
* **The Trade-off**:
  * In `synchronous=FULL`, SQLite forces an `fsync` of the WAL file to non-volatile disk on every transaction commit. While this provides maximum resilience against sudden power loss, it imposes severe I/O latency penalties and accelerates flash wear on edge hardware (e.g., SD cards, eMMC chips).
  * In `synchronous=NORMAL`, SQLite syncs the WAL file only during checkpoints, while continuing to flush writes to the OS.
* **Exact Durability Semantics**:
  * In `NORMAL` mode, committed transactions are protected against process crashes and clean system reboots, and the database file will not suffer structural corruption.
  * However, **we do NOT claim that `synchronous=NORMAL` guarantees committed data will survive every possible catastrophic sudden power-loss event**. If power is abruptly severed, transactions committed since the most recent checkpoint may be lost if they have not yet been flushed by the OS or drive controller. Furthermore, physical device caching (e.g., drives that reorder writes or ignore flush commands) dictates actual hardware behavior.
  * For edge telemetry ingestion, `NORMAL` is the intentional engineering choice balancing write throughput and flash longevity with crash recovery.

### 4. Local Persistence Precedes Acceptance
We enforce the invariant:
$$\text{Generate} \longrightarrow \text{Validate} \longrightarrow \text{Commit Transaction} \longrightarrow \text{Read-Back Verification} \longrightarrow \text{Locally Accepted}$$
* **Meaning of Successful Commit**: A successful commit means SQLite has accepted the transaction into its WAL journal according to its configured durability semantics (`synchronous=NORMAL`).
* **Meaning of Read-Back Verification**: The immediate read-back (`GetBatch`) verifies that the record was correctly written to SQLite and is queryable in the local database engine. It does **not** prove physical persistence to non-volatile storage media.
* If a write or commit fails, an error is surfaced immediately, and the batch is not marked as accepted.

### 5. Idempotent Ingestion and Sequence Continuity
* **Unique BatchID**: `batch_id` is the primary key in `telemetry_batches`. Attempted duplicate insertions return `ErrDuplicateBatch` and prevent duplicate logical rows.
* **Sequence Monotonicity**: On startup, the agent queries `MAX(sequence_number)` for its `node_id`, ensuring sequence numbers continue monotonically across process restarts without resets or collisions.

### 6. Explicit Bounded Buffering (No False Zero-Data-Loss Claims)
* Physical disk storage on edge devices is finite. While the storage layer commits accepted batches to SQLite according to its configured durability semantics, **AegisEdge explicitly does NOT claim zero data loss**.
* If an edge node remains disconnected from the control plane indefinitely, local disk capacity will eventually be exhausted, necessitating future FIFO eviction or throttling policies. Bounded retention policies and status management (`PENDING`, `SYNCING`, `SYNCED`, `FAILED`) prepare the schema for future sync and eviction policies.

## Consequences
* **Positive**: The edge agent starts and operates completely offline; local persistence establishes the system's first durable buffering boundary; and cross-compilation remains trivial with zero CGO dependencies.
* **Negative**: In `synchronous=NORMAL`, sudden hardware power loss prior to a checkpoint could result in the loss of recent batches committed since the last sync. This is an explicit, accepted engineering trade-off to protect edge flash media and maintain collection throughput.

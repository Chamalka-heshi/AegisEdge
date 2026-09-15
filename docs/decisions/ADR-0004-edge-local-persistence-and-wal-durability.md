# ADR-0004: Edge-Local Persistence and WAL Durability

## Status
Accepted

## Context
A primary architectural guarantee of AegisEdge is offline survivability: edge nodes must operate autonomously and continuously record system telemetry even when WAN connectivity is severed or the central control plane is unavailable.

To fulfill this requirement, the edge agent needs a local embedded storage engine that satisfies:
1. **Durability**: Telemetry batches must be reliably committed to non-volatile disk before they are acknowledged by the collection pipeline.
2. **Crash Resilience**: Unexpected power loss, OS restarts, or process termination must not corrupt local storage.
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
* **Crash Resilience**: In the event of an abrupt power outage, uncommitted transactions in the journal are cleanly rolled back or replayed upon recovery without file corruption.

### 3. Synchronous Tuning (`PRAGMA synchronous=NORMAL;`)
* In WAL mode, `synchronous=NORMAL` checkpoints the WAL file during sync operations while ensuring that the main database file is never corrupted by a system crash. This provides the optimal balance of high telemetry write throughput and crash durability for edge workloads.

### 4. Durability Precedes Acceptance
We enforce the invariant:
$$\text{Generate} \longrightarrow \text{Validate} \longrightarrow \text{Commit to WAL} \longrightarrow \text{Confirm Read} \longrightarrow \text{Locally Accepted}$$
* The edge agent never assumes telemetry is stored without explicit transaction confirmation from the database engine.
* If a write fails, an error is surfaced immediately, and the batch is not marked as accepted.

### 5. Idempotent Ingestion and Sequence Continuity
* **Unique BatchID**: `batch_id` is the primary key in `telemetry_batches`. Attempted duplicate insertions return `ErrDuplicateBatch` and prevent duplicate logical rows.
* **Sequence Monotonicity**: On startup, the agent queries `MAX(sequence_number)` for its `node_id`, ensuring sequence numbers continue monotonically across process restarts without resets or collisions.

### 6. Explicit Bounded Buffering (No False Zero-Data-Loss Claims)
* Physical disk storage on edge devices is finite. While the storage layer guarantees that accepted batches are durably persisted to SQLite, it does not claim mathematically guaranteed zero data loss if an edge node remains disconnected indefinitely and exhausts disk capacity. Bounded retention policies and status management (`PENDING`, `SYNCING`, `SYNCED`, `FAILED`) prepare the schema for future sync and eviction policies.

## Consequences
* **Positive**: The edge agent starts and operates completely offline; all telemetry is durably preserved across crashes; and cross-compilation remains trivial with zero CGO dependencies.
* **Negative**: SQLite in WAL mode maintains `-wal` and `-shm` auxiliary files alongside the database file, which must be ignored by version control (configured in `.gitignore`) and cleanly checkpointed upon shutdown.

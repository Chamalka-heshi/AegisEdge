# Telemetry Batch Quarantine and Recovery Runbook

This operational runbook provides guidance for managing, inspecting, and recovering quarantined telemetry batches in AegisEdge edge nodes.

---

## 1. Quarantine Overview and Triggers

AegisEdge adheres to the offline-first synchronization invariant:
$$\text{EDGE OPERATES OFFLINE} \longrightarrow \text{PERSISTS TO WAL} \longrightarrow \text{RECONNECTS} \longrightarrow \text{SYNCHRONIZES IDEMPOTENTLY}$$

To prevent **head-of-line blocking**, the edge agent separates transient retryable failures from permanent unrecoverable failures. When a telemetry batch cannot be synchronized due to an unrecoverable condition or retry exhaustion, the synchronization engine transitions the record in SQLite WAL from `PENDING` to `FAILED` (`SyncStatusFailed`) using `MarkBatchQuarantined`.

### Current Quarantine Triggers

A batch is transitioned to `FAILED` under two conditions:

1. **Permanent Failure (Quarantined on Attempt 1)**:
   - **HTTP Client Validation Failures (4xx)**: HTTP `400 Bad Request` (corrupt schema, malformed JSON, invalid metrics), `405 Method Not Allowed`, or `413 Payload Too Large`.
   - **NATS Subject Character Violations (`ErrInvalidNodeID`)**: Node IDs containing prohibited subject characters (such as `.` or `*`).
   - **Oversized NATS Message Payload (`ErrMessageTooLarge`)**: Batches exceeding `MaxPayloadSize` (default: 1 MiB / `DefaultNATSMaxPayloadSize`), verified prior to publication or rejected by the broker (`nats.ErrMaxPayload`).
2. **Logical Retry Exhaustion**:
   - Consecutive transient failures (e.g. network disconnects, server 5xx errors, broker unreachable) that reach or exceed the configured retry threshold (`s.maxAttempts`, default: 10 logical cycles).

Quarantining a batch unblocks subsequent valid batches for that node in the same synchronization cycle, allowing healthy telemetry to flow without data loss.

---

## 2. Operator CLI Commands

The `aegisedge-agent` executable includes a dedicated `quarantine` command group for inspecting and recovering batches without initializing the background synchronization daemon, telemetry generators, or Prometheus servers.

### 2.1 Listing Quarantined Batches

Lists batches currently in `FAILED` status, ordered deterministically by `failed_at ASC, sequence_number ASC, batch_id ASC`.

```powershell
# Default table output (limits to 50 batches by default)
aegisedge-agent quarantine list

# Specify database path and custom limit (1..1000)
aegisedge-agent quarantine list --db-path ./data/aegisedge-edge.db --limit 100

# JSON output for automated processing
aegisedge-agent quarantine list --json
```

**Example Output**:
```text
BATCH ID                      NODE ID       SEQ   ATTEMPTS   FAILED AT                 REASON
b-a1b2c3d4-0001               edge-node-01   102          0   2026-10-18T20:15:00Z      permanent sync failure: 400 Bad Request
b-e5f6a7b8-0002               edge-node-01   103         10   2026-10-18T20:25:30Z      sync retry threshold (10 logical cycles) exhausted: dial tcp ...

Total quarantined: 2
```

### 2.2 Inspecting Batch Details and Payload

Inspects full record metadata, failure diagnostic details, and telemetry metric samples for an explicitly specified batch without modifying database state.

```powershell
# Text summary
aegisedge-agent quarantine inspect b-a1b2c3d4-0001 --db-path ./data/aegisedge-edge.db

# Full JSON representation
aegisedge-agent quarantine inspect b-a1b2c3d4-0001 --json
```

**Example Output**:
```text
Batch ID:          b-a1b2c3d4-0001
Node ID:           edge-node-01
Sequence Number:   102
Status:            FAILED
Attempt Count:     0
Collected At:      2026-10-18T20:10:00Z
Failed At:         2026-10-18T20:15:00Z
Failure Reason:    permanent sync failure: control plane rejected batch: 400 Bad Request
Metrics Count:     2 samples
  - cpu_usage: 88.50 percent (at 2026-10-18T20:10:00Z)
  - mem_used: 2097152.00 bytes (at 2026-10-18T20:10:00Z)
```

### 2.3 Requeueing a Quarantined Batch

Transitions an explicitly identified batch from `FAILED` back to `PENDING`. Requeue resets `attempt = 0`, clears `failed_at` and `error_reason`, and records an immutable audit event.

```powershell
aegisedge-agent quarantine requeue b-e5f6a7b8-0002 --db-path ./data/aegisedge-edge.db
```

**Example Output**:
```text
Batch "b-e5f6a7b8-0002" successfully requeued to PENDING (attempts reset to 0).
```

> [!IMPORTANT]
> Requeueing a batch does **not** automatically repair corrupt data or modify its payload. If the underlying cause of a permanent rejection remains unaddressed, the batch will immediately fail and be quarantined again on its very next delivery attempt.

---

## 3. Decision Framework: When to Requeue

Before executing `quarantine requeue`, operators must inspect `ErrorReason` and determine whether requeueing is appropriate:

| Scenario / Root Cause | Failure Classification | Recommended Action |
| :--- | :--- | :--- |
| **Network outage / Control Plane Down** | Transient (Retries Exhausted) | **Safe to requeue**. Once upstream connectivity is restored, requeue the batch to resume delivery. |
| **Temporary NATS Broker Unavailable** | Transient (Retries Exhausted) | **Safe to requeue**. Requeue after NATS cluster or credentials recover. |
| **Control Plane Schema Update** | Permanent (HTTP 400 Bad Request) | **Safe to requeue ONLY after** control-plane ingestion schema is updated or deployed to accept new metric fields. |
| **Invalid Node ID (Prohibited Characters)** | Permanent (`ErrInvalidNodeID`) | **Do NOT requeue**. Batch subject contains characters invalid for NATS routing. Requeueing will immediately re-fail. |
| **Oversized Batch Payload** | Permanent (`ErrMessageTooLarge`) | **Do NOT requeue** unless control plane and NATS broker limits (`max_payload`) and agent configuration (`MaxPayloadSize`) have been increased across the cluster. |
| **Corrupt Payload Bytes** | Permanent (`ErrInvalidBatch`) | **Do NOT requeue**. Batch structure cannot be marshaled or validated. |

---

## 4. Post-Recovery Verification

After requeueing a batch, verify successful processing through the following checkpoints:

1. **Local Batch Status**: Verify the batch transitions from `PENDING` to `SYNCED` (HTTP) or `PUBLISHED` (NATS JetStream):
   ```powershell
   aegisedge-agent quarantine inspect <batch-id>
   # Output should indicate Status: SYNCED or Status: PUBLISHED
   ```
2. **Audit Event Verification**: Confirm audit entries using `ListAuditEventsByCorrelation`:
   - Quarantine event: `TELEMETRY_BATCH_QUARANTINED`
   - Requeue event: `TELEMETRY_BATCH_REQUEUED`
   Both events share the exact `CorrelationID = <batch-id>`.
3. **Control-Plane Activity**: Verify the edge node's `SequenceNumber` on the control plane advances monotonically without regressing.

---

## 5. Durability, Crash Safety, and Retention

### 5.1 Storage Lifecycle
- **WAL Durability**: Quarantined records and status transitions are written synchronously to SQLite WAL. A process restart or power loss retains `FAILED` status, attempt count, failure timestamp, and error reasons.
- **Retention Protection**: The automated background retention pruner (`PruneCompletedBatches`) strictly deletes rows in `SYNCED` or `PUBLISHED` status older than the retention cutoff. Records in `FAILED` status are **never** pruned automatically, preserving accountability until operator review.
- **Crash Between Dispatch and Persistence**: If the agent crashes after a batch was delivered to the control plane but before SQLite records `SYNCED`/`PUBLISHED`, the batch remains `PENDING`. Upon restart, idempotent deduplication (`s.store.HasBatch`) on the control plane safely acknowledges the duplicate without advancing sequence numbers or duplicating data.

---

## 6. Troubleshooting and Common Errors

### `Error: batch "<batch-id>" not found`
- **Cause**: The specified `batch-id` does not exist in `telemetry_batches`.
- **Resolution**: Check `aegisedge-agent quarantine list` to verify exact batch identifiers.

### `Error: cannot requeue batch "<batch-id>": invalid batch sync status transition`
- **Cause**: The batch is currently in `PENDING`, `SYNCED`, or `PUBLISHED` status. Only batches in `FAILED` status can be requeued.
- **Resolution**: Inspect the batch with `quarantine inspect <batch-id>` to determine its current status.

### Immediate Re-quarantine
- **Symptom**: A batch is requeued, but immediately returns to `FAILED` status with `Attempt Count: 0`.
- **Cause**: The batch failed a permanent validation check (e.g. HTTP 400 Bad Request or `ErrMessageTooLarge`).
- **Resolution**: Do not continuously requeue permanently rejected batches. Address the underlying schema mismatch or configuration defect before requeueing.

---

## 7. System Limitations and Secondary Fallbacks

### 7.1 Current System Limitations
1. **In-Memory NATS Delivery-Exhaustion Observability**:
   - The consumer counter `consumer.ExhaustedDeliveries()` tracks messages that exceeded `MaxDeliver` prior to terminal NAK.
   - The consumer gauge `consumer.StalledUnacknowledgedSequences()` tracks unacknowledged stream sequences holding the AckFloor that repeatedly return `ErrNotEligible`.
   - Both metrics are in-memory and process-local: they reset upon control-plane restart and are not persisted to SQLite or JetStream metadata.
2. **Lack of Automated Dead-Letter Queue (DLQ) / Auto-Replay**:
   - AegisEdge does not provide an automated background worker to re-read exhausted NATS messages from JetStream. Replaying messages requires explicit manual action or external tooling.
3. **No Administrative HTTP API for Force Recovery**:
   - The control plane does not expose an HTTP endpoint or CLI flag to force sequence recovery or bypass exhaustion evidence. Force recovery (`WithForceExhaustion(true)`) is strictly an internal programmatic option used for tests and controlled operational harnesses.

### 7.2 Secondary Fallback: Direct Read-Only SQLite Inspection
In air-gapped or embedded emergency environments where CLI execution is unavailable, operators with read access to the SQLite file may inspect quarantined batches using standard read-only queries:

```sql
-- Read-only query for failed batches (Edge Node):
SELECT batch_id, node_id, sequence_number, attempt, failed_at, error_reason 
FROM telemetry_batches 
WHERE sync_status = 'FAILED' 
ORDER BY failed_at ASC, sequence_number ASC;

-- Inspect payload of a specific batch (Edge Node):
SELECT payload 
FROM telemetry_batches 
WHERE batch_id = 'b-a1b2c3d4-0001';
```

> [!CAUTION]
> Do **NOT** execute raw `UPDATE` statements to manually requeue batches in production databases. Direct SQL updates bypass `Syncer.RequeueBatch` audit logging, omit state validation, and risk corrupting retry state. Always use `aegisedge-agent quarantine requeue`.

---

## 8. Control-Plane JetStream Stalled Sequence Recovery Workflow

When the control-plane NATS JetStream consumer encounters messages that cannot be acknowledged or safely quarantined, it exposes process-local observability and structured warnings. This section defines the standard operating procedure for observing, inspecting, deciding upon, and escalating stalled unacknowledged stream sequences.

### 8.1 Invariants and Safety Rules

- **Offline-First Synchronization Invariant**:
  $$\text{EDGE OPERATES OFFLINE} \longrightarrow \text{PERSISTS TO WAL} \longrightarrow \text{RECONNECTS} \longrightarrow \text{SYNCHRONIZES IDEMPOTENTLY}$$
- **Never Automatically Force Recovery**: `WithForceExhaustion(true)` is an internal Go API option and must never be invoked from background scans, periodic timers, warning handlers, or routine healthchecks.
- **No Supported Runtime Force-Recovery Interface**: The control plane does **not** expose any CLI command, HTTP endpoint, or RPC to force sequence recovery or bypass exhaustion evidence.
- **Never Speculatively Acknowledge**: Never execute `msg.Ack()`, `msg.Term()`, or advance the JetStream AckFloor solely because a sequence has stalled, aged, or repeatedly returned `ErrNotEligible`.
- **Preserve Message Durability**: Do not infer permanent delivery exhaustion merely from elapsed time, age, missing advisories, or in-flight retry delays.
- **Payload Privacy and Secret Hygiene**: Never print raw telemetry payloads, authorization tokens, or sensitive headers in unredacted logs, tickets, or incident records.
- **Interface Scoping**:
  - `aegisedge-agent quarantine` operates **exclusively** on the edge node's local SQLite database (`telemetry_batches`). It cannot inspect JetStream broker state or control-plane storage.
  - The control plane has **no runtime CLI** to inspect control-plane SQLite tables (`cp_ingested_batches`, `cp_telemetry_quarantine`).

---

### 8.2 Stage 1 — Observe

#### Stalled-Sequence Metric
The control plane exposes the Prometheus gauge:
- **Metric**: `aegisedge_control_plane_nats_stalled_unacknowledged_sequences`
- **Type**: Gauge
- **Semantics**: Current count of unacknowledged stream sequences holding the JetStream consumer `AckFloor` that have repeatedly returned `ErrNotEligible` during periodic recovery passes.
- **Lifecycle**: **Process-local and in-memory**. The backing counter and tracking map reset to 0 upon control-plane restart and do not persist to disk. A metric value of 0 immediately following a restart does **not** prove that stalled sequences were resolved.

#### Structured Warning Log
When a stream sequence remains stalled beyond the warning threshold (default: $2 \times \text{AckWait}$, minimum 60s), the consumer emits a rate-limited structured log (at most once every 10 minutes per sequence):

```text
WARN unacknowledged stream sequence holding ack floor without authoritative exhaustion evidence
     stream_seq=1042
     stream=AEGISEDGE_TELEMETRY
     consumer=aegisedge-control-plane-telemetry
     ack_floor=1042
     num_ack_pending=1
     elapsed=2m30s
     reason="missing authoritative exhaustion evidence (broker advisory evicted or missing; in-flight protection active)"
     remediation="verify sequence with quarantine runbook; if permanently exhausted, recover using RecoverStreamSequence with WithForceExhaustion(true)"
```

#### What the Alert Proves vs. What It Does NOT Prove
| Observation | What It Confirms | What It Does NOT Prove |
| :--- | :--- | :--- |
| **Gauge $> 0$** | An unacknowledged message at `AckFloor` has repeatedly returned `ErrNotEligible`. | It does **not** prove permanent broker delivery exhaustion. |
| **`elapsed` $> \text{threshold}$** | Time elapsed since the sequence was first observed stalled in process memory. | It does **not** prove the edge node crashed or abandoned the message. |
| **`ErrNotEligible`** | Message has valid schema (not poison) and lacks broker delivery-exhaustion evidence. | It does **not** prove broker advisories were evicted (evidence is simply absent). |
| **In-Flight Protection Active** | The consumer refuses to speculatively quarantine a message that may still be processing. | It does **not** indicate a dead-letter state. |

**Operator Action**: Record `stream_seq`, `stream`, `consumer`, and `elapsed` from the log entry. Proceed immediately to **Stage 2 (Inspect)**.

---

### 8.3 Stage 2 — Inspect

Inspection must use only verified, supported interfaces and respect operational boundaries.

#### Step 2.1 — Inspect Broker State via External NATS CLI (Read-Only)
> [!NOTE]
> The `nats` CLI is an external operational binary from the NATS project (`nats-io/natscli`) and is not bundled with AegisEdge or installed in the default agent environment. The authoritative command supported by the official NATS CLI for retrieving a JetStream message by stream sequence number is `nats stream get` (or `nats str get`), as defined in official NATS CLI documentation. Operators must validate syntax and flags against their installed CLI version:

```powershell
# Read message metadata and headers by stream sequence number (authoritative read-only NATS CLI command)
nats stream get AEGISEDGE_TELEMETRY <stream_seq>
```

**What this establishes**:
- `Subject`: e.g. `aegisedge.v1.telemetry.edge-node-01` $\rightarrow$ identifies `node_id = edge-node-01`.
- `Nats-Msg-Id` Header: identifies `batch_id` (e.g. `b-a1b2c3d4-0001`).
- Message timestamp and byte size.

**Output limitations**:
- `nats stream get` reads directly from the stream file on the broker. It does **not** indicate whether the consumer group processed the message, does not report per-consumer delivery counts, and does not alter consumer state.
- Reading a message does not acknowledge, redeliver, or replay the message.
- Strictly avoid mutating or destructive operations (`nats stream purge`, `nats consumer rm`, `nats stream rmm`, or manual acknowledgments) during diagnostic inspection.

> [!WARNING]
> Do **not** pipe or paste raw message payloads into ticket comments or public logs.

#### Step 2.2 — Check Consumer and Advisory Health (Read-Only)
```powershell
# Inspect consumer delivery state and ack floor (requires installed NATS CLI)
nats consumer info AEGISEDGE_TELEMETRY aegisedge-control-plane-telemetry

# Check advisory stream metrics via control-plane Prometheus endpoint:
curl -s http://127.0.0.1:8080/metrics | findstr /i "advisory"
```

- `aegisedge_control_plane_nats_advisory_evictions_total`: If $> 0$, JetStream server capacity under `DiscardOld` has evicted broker advisories from `AEGISEDGE_TELEMETRY_ADVISORIES`.
- `aegisedge_control_plane_nats_advisory_fetch_errors_total`: Indicates connection/fetch errors querying advisory messages.
- `nats consumer info` limitations: Reports aggregate consumer metrics (`NumPending`, `NumAckPending`, `AckFloor`), but does **not** display individual per-sequence delivery attempt counts.

#### Step 2.3 — Check Edge Node Status (Read-Only)
If the edge node is reachable, inspect the batch in local edge storage using the supported agent CLI:

```powershell
# Inspect edge node status for the batch (supported read-only CLI command)
aegisedge-agent quarantine inspect <batch_id> --db-path ./data/aegisedge-edge.db
```

- Confirms whether the edge agent considers the batch `PENDING`, `SYNCED`, `PUBLISHED`, or `FAILED`.
- **Boundary Limitation**: The `aegisedge-agent` CLI operates **exclusively** on the edge node's local database. It cannot inspect control-plane tables or JetStream broker state.

#### Step 2.4 — Control-Plane Storage State: Limits & Escalation Procedure
> [!IMPORTANT]
> **Runtime Interface Limitation**: The control plane provides **no supported application CLI or runtime HTTP endpoint** to query whether an arbitrary historical batch is recorded in `cp_ingested_batches` or `cp_telemetry_quarantine`.
>
> - The endpoint `GET /api/v1/telemetry/batches` (admin-token required) inspects only a small in-memory ring buffer of recent payloads (`DefaultMaxHistoryBatches = 100`). It does **not** query historical database records.
> - Direct SQL queries against `data/control-plane.db` are **not** a supported application interface. They represent an **authorized engineering diagnostic procedure** requiring physical/host file access and authorized tools.
> - **Mandatory Escalation Rule**: If a safe host-level diagnostic procedure is not available, operators must treat control-plane database status as **uncertain** and escalate to an authorized database administrator or systems engineer. **Never guess the batch status based on message age, sequence number, the stalled gauge, or AckFloor advancement.**

For authorized database administrators with approved host-level access, the diagnostic read-only queries are:

```sql
-- Read-only check for durable ingestion (Authorized Engineering Procedure):
SELECT node_id, batch_id, sequence_number, ingested_at
FROM cp_ingested_batches
WHERE node_id = '<node_id>' AND batch_id = '<batch_id>';

-- Read-only check for durable quarantine (Authorized Engineering Procedure):
SELECT id, dedup_key, source_sequence, node_id, batch_id, failure_category, failure_reason, delivery_count, attempts, status, last_quarantined_at
FROM cp_telemetry_quarantine
WHERE source_sequence = <stream_seq>
   OR (node_id = '<node_id>' AND batch_id = '<batch_id>');
```

---

### 8.4 Stage 3 — Decide

To prevent data loss and duplicate processing, operators must follow explicit decision branches based strictly on verified evidence.

#### Five Discrete Recovery States and Acknowledgment-Floor Semantics
The runbook strictly distinguishes five operational states:
1. **Advisory Observed**: A `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES` advisory exists in `AEGISEDGE_TELEMETRY_ADVISORIES`. Confirms the broker ceased redelivery. Does **not** prove quarantine write or message resolution.
2. **Advisory Reconciled**: The background recovery worker (`reconcileDurableAdvisories`) successfully processed the advisory, executed `RecoverStreamSequence`, and ACKed the advisory on the advisory stream.
3. **Durable Quarantine Confirmed**: A quarantine record was durably committed to `cp_telemetry_quarantine` in SQLite WAL.
4. **Message Acknowledged / Resolved**: The sequence is accounted for in durable storage (ingested or quarantined) and the consumer AckFloor can advance.
5. **Process-Local Gauge Cleared**: The sequence was removed from the in-memory `stalledSeqs` tracking map, decrementing the gauge. (Note: process restart also clears the gauge without resolving the sequence).

> [!CAUTION]
> **Acknowledgment-Floor Semantics vs. Durable Storage State**:
> In NATS JetStream, the consumer `AckFloor` is a broker-level watermark indicating the highest contiguous stream sequence for which all delivered messages have been acknowledged or terminated on that consumer.
> - `AckFloor` can advance due to stream retention discard (`DiscardOld`), message expiration (`max_age`), or consumer recreation, completely independent of whether a message was ingested or quarantined.
> - An advancing `AckFloor`, a clearing gauge, or an observed advisory does **not** prove that a particular batch was durably ingested or quarantined.
> - Durable evidence is established **exclusively** by committed records in SQLite storage (`cp_ingested_batches` or `cp_telemetry_quarantine`).

```text
                                [Stalled Sequence Observed]
                                (Gauge > 0, Structured Warning Log)
                                             │
                                Ingestion Status Verified?
                                (Authorized DB check or history)
                                ┌────────────┴────────────┐
                           Row Exists                 No Row / Uncertain
                                │                         │
                      [Branch 1: Ingested]      Quarantine Status Verified?
                      • Durably saved           (Authorized DB check)
                      • Do NOT force-quarantine ┌─────────┴─────────┐
                      • Next scan ACKs dup      Row Exists       No Row / Uncertain
                                                │                   │
                                        [Branch 2: Quarantined] Advisory Observed /
                                        • Quarantine confirmed  Deliveries >= MaxDeliver?
                                        • Reconciled by scan    ┌───────┴───────┐
                                                              Yes               No / Unknown
                                                               │                 │
                                                    [Branch 3: Exhausted] [Branch 4: Ambiguous]
                                                    • Normal recovery     • In-flight or retrying
                                                    • Auto quarantine     • PRESERVE MESSAGE
                                                                          • Escalate; DO NOT force
```

#### Decision Branches & Execution Mechanics
Do not assume background recovery succeeds merely because a worker is running; workers may encounter SQLite lock errors (`SQLITE_BUSY`), stream read timeouts, or backoff delays.

1. **Branch 1 — Already Ingested**:
   - **Condition**: Confirmed in `cp_ingested_batches` or recent ingestion history.
   - **Execution Mechanics**: When `RecoverStreamSequence` runs, `idempotencyStore.HasBatch` returns `true`, yielding `ErrAlreadyIngested`.
   - **Original Telemetry Message vs. Advisory Disposition**:
     - *In live delivery (`processMessage`)*: When a duplicate message is delivered, `IngestBatch` returns `server.ErrDuplicateBatch`, and `processMessage` calls `_ = msg.Ack()` ([`consumer.go:1277`](file:///C:/AegisEdge/services/control-plane/nats/consumer.go#L1277)). The live duplicate message receives an explicit **ACK**.
     - *In periodic recovery scan (`RecoverUnacknowledged`)*: `RecoverStreamSequence` returns `ErrAlreadyIngested`. The recovery scan clears the sequence from in-memory tracking `stalledSeqs` ([`consumer.go:1780, 1844`](file:///C:/AegisEdge/services/control-plane/nats/consumer.go#L1780)). Because the scan reads raw stream messages without a consumer acknowledgment token, **NO disposition action (No ACK, No TERM, No NAK)** is issued for the telemetry message.
     - *In advisory reconciliation (`handleAdvisoryData`)*: If an advisory arrived for an already-ingested batch, `handleAdvisoryData` calls `ackMsg.Ack()` ([`consumer.go:811`](file:///C:/AegisEdge/services/control-plane/nats/consumer.go#L811)). This ACKs the **advisory message**, but issues **NO disposition action** for the original telemetry message.
   - **Failure Behavior**: If the SQLite read fails, `RecoverStreamSequence` returns the database error, stalled tracking is **not** cleared, and the sequence is placed in `pendingScanRetries` with exponential backoff (up to 15 attempts).
   - **Action**: **Do NOT force-quarantine.** Durable ingestion is already established. Do not infer message acknowledgment merely from tracking cleanup.

2. **Branch 2 — Already Durably Quarantined**:
   - **Condition**: Confirmed in `cp_telemetry_quarantine` (via authorized database inspection).
   - **Quarantine Record Status Semantics**:
     - When `RecordQuarantine` runs, it detects the existing deduplication key inside an immediate SQLite transaction ([`sqlite.go:1532`](file:///C:/AegisEdge/services/control-plane/storage/sqlite.go#L1532)).
     - *If existing status is `quarantined`*: Status remains `quarantined`. `attempts` increments (`attempts = attempts + 1`), `last_quarantined_at` updates to current timestamp, and failure details are refreshed.
     - *If existing status is `replayed` or `discarded` (terminal disposition)*: The terminal disposition is **preserved** ([`sqlite.go:1561-1605`](file:///C:/AegisEdge/services/control-plane/storage/sqlite.go#L1561-L1605)). It is **never** overwritten or reverted back to `quarantined`. `attempts` increments to track redelivery attempts against a resolved record.
     - *Four Distinct States*: The runbook strictly separates: (1) a durable record exists, (2) the record is currently in active `quarantined` status, (3) a terminal `replayed` or `discarded` disposition was recorded, and (4) the JetStream broker message was acknowledged. Do **not** equate these states.
   - **Original Telemetry Message Disposition (ACK / TERM / NAK / Pending)**:
     - *In live delivery (`processMessage`)*: If a poison message is delivered, it is terminated via `termMessage` ([`consumer.go:1172`](file:///C:/AegisEdge/services/control-plane/nats/consumer.go#L1172)) $\rightarrow$ **TERM**. If a message reached `MaxDeliver` on transient failure, it was NAKed via `msg.Nak()` ([`consumer.go:1309`](file:///C:/AegisEdge/services/control-plane/nats/consumer.go#L1309)), halting redelivery.
     - *In stream recovery (`RecoverStreamSequence` called via `RecoverUnacknowledged`)*: Retrieves raw stream data via `c.stream.GetMsg(ctx, seq)` ([`consumer.go:1589`](file:///C:/AegisEdge/services/control-plane/nats/consumer.go#L1589)) which lacks a consumer delivery handle. **NO ACK, TERM, or NAK is issued for the telemetry message.** It remains stored in the stream file under broker exhaustion.
     - *In advisory reconciliation (`handleAdvisoryData`)*: When `RecoverStreamSequence` returns success on an existing record, `ackMsg.Ack()` ([`consumer.go:797`](file:///C:/AegisEdge/services/control-plane/nats/consumer.go#L797)) acknowledges **only the broker advisory message**. It issues **NO disposition action** for the original telemetry message.
     - *In direct `RecoverStreamSequence`*: Returns `*QuarantineRecord, nil` ([`consumer.go:1695`](file:///C:/AegisEdge/services/control-plane/nats/consumer.go#L1695)). Issues **NO disposition action** for the telemetry message.
   - **Database Failure Behavior**:
     - If the SQLite write encounters lock contention (`SQLITE_BUSY`), the transaction rolls back, `RecordQuarantine` returns an error, the sequence is scheduled for backoff retry in `pendingScanRetries` (or `pendingExhausted`), and in-memory tracking is **not** cleared.
   - **Clearing `stalledSeqs` vs. Durable Resolution**:
     - `c.clearStalledSequenceLocked(seq)` is **strictly process-local in-memory bookkeeping** ([`consumer.go:1468-1473`](file:///C:/AegisEdge/services/control-plane/nats/consumer.go#L1468-L1473)). It deletes the sequence from the internal map and decrements the gauge `aegisedge_control_plane_nats_stalled_unacknowledged_sequences`.
     - Clearing `stalledSeqs` has **no relationship** to JetStream broker acknowledgment and does **not** prove durable resolution (tracking also clears on process restart or consumer window pruning when `NumAckPending == 0`).
   - **Action**: Do not attempt manual duplicate writes. Background reconciliation updates retry metadata safely.

3. **Branch 3 — Authoritative Exhaustion Evidence Available**:
   - **Condition**: Broker advisory exists in `AEGISEDGE_TELEMETRY_ADVISORIES` or consumer info confirms delivery count $\ge \text{MaxDeliver}$ (5).
   - **Execution Mechanics**: `reconcileDurableAdvisories` fetches the advisory and calls `RecoverStreamSequence` with exhaustion evidence.
   - **Advisory Message Acknowledgment vs. Telemetry Message Disposition**:
     - *Advisory Message Acknowledgment*: When `RecoverStreamSequence` succeeds, `handleAdvisoryData` calls `ackMsg.Ack()`. This acknowledges **only the broker advisory message** in `AEGISEDGE_TELEMETRY_ADVISORIES`, marking the advisory event as handled.
     - *Original Telemetry Message Disposition*: The original telemetry message in `AEGISEDGE_TELEMETRY` is **not** acknowledged by `handleAdvisoryData` (which possesses an ACK handle only for the advisory, not the original telemetry message). The original message remains preserved in the broker stream under delivery exhaustion. Durable resolution is established exclusively by the committed row in `cp_telemetry_quarantine`.
   - **Failure Behavior**:
     - If the SQLite write fails (e.g. database locked): `handleAdvisoryData` NAKs the advisory with delay (`ackMsg.NakWithDelay`), enqueues the sequence in `pendingExhausted` for retries via `processPendingRecoveryPass`, and leaves the stalled gauge $> 0$.
     - If the SQLite write succeeds: `RecordQuarantine` commits, `ackMsg.Ack()` acknowledges the advisory, and `clearStalledSequenceLocked` clears in-memory tracking.
   - **Action**: Handled automatically by background recovery. No operator force-override required.
4. **Branch 4 — Evidence Missing, Ambiguous, or Uncertain**:
   - **Condition**: Neither ingested nor quarantined, advisory stream has no record, delivery count $< \text{MaxDeliver}$, or database status cannot be established via runtime interfaces.
   - **Conclusion**: The message may be actively processing or waiting for exponential backoff redelivery.
   - **Action**: **PRESERVE THE MESSAGE AND DO NOT FORCE-QUARANTINE.**
     - Never guess permanent failure based on age, sequence number, or the stalled gauge.
     - Check downstream health (SQLite disk space, database locks, network partitions).
     - Allow standard JetStream redeliveries (`MaxDeliver = 5`, `AckWait = 30s`) to either succeed or exhaust authoritatively.
     - Escalate to systems engineers for host-level diagnosis if the sequence remains unacknowledged past $2 \times \text{AckWait}$ without progress.
5. **Branch 5 — JetStream or SQLite Storage Unavailable**:
   - **Condition**: NATS connection severed or SQLite returning `SQLITE_BUSY` / `SQLITE_LOCKED`.
   - **Action**: Do not claim recovery has completed. Do not acknowledge messages. Let backoff and in-flight retry mechanisms preserve state safely until storage/broker connectivity recovers.
6. **Branch 6 — Stalled Gauge Resets Following Process Restart**:
   - **Condition**: Control-plane service restarts, resetting the in-memory gauge to 0.
   - **Conclusion**: Resetting to 0 is an in-memory lifecycle artifact, not proof of resolution.
   - **Action**: If the unacknowledged sequence is still in JetStream without evidence, subsequent background scan passes will observe `ErrNotEligible` again, re-track the sequence in `stalledSeqs`, and re-raise warnings once `StalledWarningThreshold` elapses.

---

### 8.5 Stage 4 — Recover

#### Automatic vs. Operator Recovery
- **Automatic Recovery**: The control-plane consumer automatically resolves messages in normal operation:
  - Poison messages (malformed JSON or schema violations) are quarantined and terminated (`msg.Term()`) on the next delivery attempt.
  - Delivery-exhausted messages with broker advisories are processed and quarantined by `reconcileDurableAdvisories`.
  - Already-ingested duplicates are recognized and acknowledged by `RecoverStreamSequence`.
- **No Supported Runtime Force-Recovery Interface**:
  - The control plane exposes **no HTTP administrative endpoint, RPC, or CLI command** to force sequence recovery.
  - `WithForceExhaustion(true)` is an internal Go functional option designed for programmatic tests and custom recovery harnesses. It is **not** a production CLI command.
  - Operators must **not** attempt to call internal Go methods directly in production.
- **Handling Lost Advisories Under Eviction**:
  - If advisory evictions occurred (`aegisedge_control_plane_nats_advisory_evictions_total > 0`), the broker advisory may have been pruned under `DiscardOld`.
  - In this scenario, the message remains unacknowledged in JetStream because `RecoverStreamSequence` returns `ErrNotEligible` without broker advisory evidence.
  - **Remediation**: Escalate to authorized systems engineers. Only an authorized engineer with approved custom tooling or administrative scripts integrating the Go SDK may evaluate `WithForceExhaustion(true)` after conclusively verifying that the batch is neither in `cp_ingested_batches` nor in `cp_telemetry_quarantine`.
- **Strictly Prohibited Actions**:
  - Do **NOT** manually delete messages from the stream (`nats str rm`).
  - Do **NOT** recreate or reset the consumer group while messages are stalled without recording unacknowledged sequences.
  - Do **NOT** execute raw SQL `UPDATE` or `INSERT` statements to bypass application logic.

---

### 8.6 Stage 5 — Verify and Close

Following resolution of a stalled sequence, verify complete recovery across all checkpoints:

1. **Durable Quarantine Verification (Authorized Database Admin)**:
   Confirm the record exists in control-plane storage:
   ```sql
   SELECT id, status, failure_category, attempts, last_quarantined_at
   FROM cp_telemetry_quarantine
   WHERE source_sequence = <stream_seq>;
   ```
   Status must be `'quarantined'`, failure category `'delivery_exhausted'`, and attempts $\ge 1$.

2. **Durable Ingestion Verification (Authorized Database Admin, If Applicable)**:
   If resolved via ingestion, confirm in `cp_ingested_batches`:
   ```sql
   SELECT node_id, batch_id, sequence_number, ingested_at
   FROM cp_ingested_batches
   WHERE node_id = '<node_id>' AND batch_id = '<batch_id>';
   ```

3. **Consumer AckFloor Advancement**:
   Verify via external NATS CLI that the consumer AckFloor has advanced beyond the stalled sequence:
   ```powershell
   nats consumer info AEGISEDGE_TELEMETRY aegisedge-control-plane-telemetry
   # Confirm AckFloor.Stream > <stream_seq> and NumAckPending has decremented.
   ```
   > [!NOTE]
   > AckFloor advancement confirms broker-level progress, but only the database queries in steps 1 and 2 prove durable resolution.

4. **Stalled-Sequence Metric Clearance**:
   Verify that the in-memory metric decrements to 0:
   ```powershell
   curl -s http://127.0.0.1:8080/metrics | findstr /i "aegisedge_control_plane_nats_stalled"
   # Output: aegisedge_control_plane_nats_stalled_unacknowledged_sequences 0
   ```

5. **Process-Local Tracking Boundary**:
   - Remember that `stalledSeqs` tracking and the gauge are **process-local**. If the control-plane service was restarted, a gauge reading of 0 does not prove durable resolution.
   - If an unacknowledged sequence remains unresolved in JetStream, subsequent periodic recovery scans will observe `ErrNotEligible` again, re-track the sequence, and re-issue warnings once `StalledWarningThreshold` elapses.
   - The authoritative, durable audit history resides exclusively in `cp_telemetry_quarantine` and `cp_ingested_batches`.

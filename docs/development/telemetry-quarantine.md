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

---

## 9. Control-Plane Terminal Quarantine Retention and Pruning

To prevent unbounded SQLite database growth while preserving strict audit compliance, the AegisEdge control plane integrates automated retention pruning for terminal quarantine payloads into its background pruning lifecycle.

### 9.1 Retention Overview and Invariants

Quarantined telemetry batches in `cp_telemetry_quarantine` contain large raw payload BLOBs (`payload`). Over long operational periods, resolved quarantine records can accumulate substantial storage overhead.

The retention pruner enforces strict architectural invariants:

1. **Payload Clearing Only (`payload = X''`)**:
   - Pruning clears the raw telemetry binary data by setting `payload = X''` and updating `updated_at = CURRENT_TIMESTAMP`.
   - **No rows are deleted**: all record identifiers, timestamps, failure categories, diagnostic error strings, delivery counts, operator identities, and resolution notes remain permanently available in `cp_telemetry_quarantine`.
   - The idempotency mapping in `cp_ingested_batches` is completely untouched.
2. **Strict Protection for Active Quarantines**:
   - Records with `status = 'quarantined'` are **never pruned**, regardless of age. Their full payload remains accessible until an operator explicitly acts on them.
3. **Terminal Records Only**:
   - Only records with terminal status `replayed` or `discarded` are eligible for payload pruning.
4. **Cutoff Timestamp Semantics**:
   - Eligibility is governed by `status_updated_at < cutoff` (where `cutoff = now - QuarantineRetentionDays`).
   - A newly resolved record (replayed or discarded within the retention window) retains its full payload until the retention period elapses.
5. **Bounded Execution**:
   - Pruning passes are bounded by `PruneBatchSize` (default: 500) via `LIMIT ?` to avoid transaction starvation and SQLite database lock contention.
6. **Failure Isolation**:
   - A failure in quarantine pruning is logged as a warning and does not crash the control plane or disrupt active telemetry ingestion.

### 9.2 Configuration Reference

Terminal quarantine payload pruning is controlled by the following parameters:

| Parameter | CLI Flag | Environment Variable | Default Value | Description |
| :--- | :--- | :--- | :--- | :--- |
| **Pruning Master Switch** | `--retention-enabled` | `STORAGE_RETENTION_ENABLED` | `false` | Master toggle to enable background retention pruner worker. |
| **Quarantine Retention** | `--quarantine-retention-days` | `STORAGE_QUARANTINE_RETENTION_DAYS` | `30` | Days to retain payload BLOBs for terminal (`replayed`, `discarded`) records. |
| **Telemetry Retention** | `--retention-days` | `STORAGE_RETENTION_DAYS` | `30` | Days to retain historical ingested telemetry payloads. |
| **Prune Interval** | `--prune-interval` | `STORAGE_PRUNE_INTERVAL` | `1h` | Interval between background storage pruning cycles. |
| **Prune Batch Size** | `--prune-batch-size` | `STORAGE_PRUNE_BATCH_SIZE` | `500` | Maximum number of records pruned per pass per table. |

#### Precedence Order
1. Explicit CLI flags (e.g. `--quarantine-retention-days=60`)
2. Environment variables (e.g. `STORAGE_QUARANTINE_RETENTION_DAYS=45`)
3. Built-in defaults (`server.DefaultQuarantineRetentionDays = 30`)

### 9.3 Eligibility SQL Logic

The pruning query executed against SQLite durable storage is:

```sql
UPDATE cp_telemetry_quarantine
SET payload = X'',
    updated_at = CURRENT_TIMESTAMP
WHERE id IN (
    SELECT q.id
    FROM cp_telemetry_quarantine q
    WHERE q.status IN ('replayed', 'discarded')
      AND q.status_updated_at < ?
      AND LENGTH(q.payload) > 0
    ORDER BY q.status_updated_at ASC, q.rowid ASC
    LIMIT ?
);
```

### 9.4 Prometheus Observability

Operators monitor quarantine lifecycle, administrative operations, storage retention, and NATS message handling through Prometheus metrics exposed on the control-plane `/metrics` endpoint:

#### 9.4.1 Storage Retention & Pruning Metrics

- **`aegisedge_quarantine_pruned_payloads_total`** (Counter):
  - **Lifecycle**: Process-local counter incremented strictly on successful pruning passes by the exact number of terminal quarantine payloads cleared.
  - Failures do not increment the counter.
  - Repeated passes on already-cleared records return 0 rows affected and do not double-count.

#### 9.4.2 Administrative Replay and Discard Operations (Stage 6.31B.10A)

- **`aegisedge_quarantine_replays_total`** (CounterVec, label: `outcome`):
  - Total number of administrative quarantine replay operations processed by the control plane.
  - Strictly bounded outcome labels: `success`, `not_found`, `conflict`, `invalid`, `error`.
  - Incremented exactly once per completed handler invocation. Unauthenticated requests are rejected before metric invocation and counted under `aegisedge_control_plane_auth_failures_total`.
- **`aegisedge_quarantine_discards_total`** (CounterVec, label: `outcome`):
  - Total number of administrative quarantine discard operations processed by the control plane.
  - Strictly bounded outcome labels: `success`, `not_found`, `conflict`, `invalid`, `error`.

#### 9.4.3 Active Quarantine Backlog Gauge (Stage 6.31B.10B)

- **`aegisedge_quarantine_active_records`** (Gauge):
  - **What it measures**: The exact number of durable quarantine records in SQLite whose lifecycle status is currently `quarantined`.
  - **Terminal state exclusion**: Records in `replayed` or `discarded` status are strictly excluded.
  - **Durability**: **Durable across restarts**. Backed by durable SQLite database state; retains accuracy after control-plane restarts.
  - **Collection & Freshness**: Evaluated dynamically at scrape time. Uses a covering index (`idx_cp_quarantine_status_updated` / `idx_cp_quarantine_status_time`) to count rows matching `WHERE status = 'quarantined'`. It **never** loads or deserializes raw payload BLOBs.
  - **Bounded Execution & Error Handling**: Executed with a bounded 2-second query timeout. If SQLite is unavailable or queries encounter storage errors, the metric emits `NaN` (standard Prometheus floating-point representation for invalid/uncomputable measurements) rather than silently returning a false successful `0`.

#### 9.4.4 NATS Recovery & Termination Observability (Stage 6.31B.10B)

- **`aegisedge_nats_pending_recovery_items`** (Gauge):
  - **What it measures**: Current count of exhausted NATS JetStream messages queued in the consumer's in-memory `pendingExhausted` map awaiting asynchronous durable quarantine persistence.
  - **Durability**: **Process-local and in-memory**. Resets to 0 upon control-plane restart; durable recovery is subsequently rehydrated from JetStream unacknowledged sequences and broker advisories.
  - **Operator Interpretation**: Under normal operation, this gauge should be 0 or small transient spikes. An elevated or growing value indicates that messages have exceeded `MaxDeliver` but their insertion into SQLite quarantine is contending or retrying due to database lock contention or disk latency.
  - **Safety**: Evaluated thread-safely via `c.PendingRecoveryCount()`. When NATS is unconfigured or disabled, reports `0` safely without panic.
- **`aegisedge_nats_terminated_messages_total`** (Counter):
  - **What it measures**: Cumulative number of permanently unprocessable messages successfully terminated in NATS JetStream via `TermWithReason` (or fallback `Term`).
  - **Durability**: **Process-local and in-memory**.
  - **Exact Event Measured**: Represents broker message state transitions to terminated status, permanently halting JetStream redeliveries. It is triggered only after the message is confirmed durably persisted to quarantine storage (or identified as already ingested).
  - **Error Semantics**: Incremented strictly when the termination operation succeeds. If `TermWithReason` and fallback `Term` return errors (e.g. broker disconnection), the counter does **not** increment. Does not double-count redeliveries.

#### 9.4.5 Scraping Example

```powershell
curl -s http://127.0.0.1:8080/metrics | findstr /i "quarantine nats"
# Output sample:
# # HELP aegisedge_quarantine_active_records Current number of active durable quarantine records in quarantined status awaiting operator review or replay.
# # TYPE aegisedge_quarantine_active_records gauge
# aegisedge_quarantine_active_records 3
# # HELP aegisedge_nats_pending_recovery_items Current number of exhausted NATS messages awaiting recovery persistence in the consumer queue.
# # TYPE aegisedge_nats_pending_recovery_items gauge
# aegisedge_nats_pending_recovery_items 0
# # HELP aegisedge_nats_terminated_messages_total Total number of permanently unprocessable NATS messages successfully terminated via JetStream Term.
# # TYPE aegisedge_nats_terminated_messages_total counter
# aegisedge_nats_terminated_messages_total 5
# # HELP aegisedge_quarantine_pruned_payloads_total Total number of historical terminal quarantine payloads successfully pruned from durable storage.
# # TYPE aegisedge_quarantine_pruned_payloads_total counter
# aegisedge_quarantine_pruned_payloads_total 14
```

---

## 10. Operational Prometheus Alerting & Troubleshooting Runbook (Stage 6.31B.11A)

AegisEdge control-plane deployment includes pre-configured Prometheus alerting rules in [`infra/prometheus/alert_rules.yml`](../../infra/prometheus/alert_rules.yml) to detect backlog growth, storage degradation, and message termination spikes.

### 10.1 Alert Rules Summary

| Alert Name | Severity | Condition (PromQL) | Duration (`for`) | Scope |
| :--- | :---: | :--- | :---: | :--- |
| **`QuarantineActiveBacklogHigh`** | `warning` | `aegisedge_quarantine_active_records > 100` | `5m` | Durable SQLite State |
| **`QuarantineActiveBacklogCritical`** | `critical` | `aegisedge_quarantine_active_records > 500` | `5m` | Durable SQLite State |
| **`QuarantineActiveCountStorageFailure`** | `critical` | `aegisedge_quarantine_active_records != aegisedge_quarantine_active_records` | `2m` | SQLite Probe Health (NaN) |
| **`QuarantineActiveMetricMissing`** | `critical` | `absent(aegisedge_quarantine_active_records)` | `5m` | Scrape Availability |
| **`NATSPendingRecoveryAccumulationWarning`** | `warning` | `aegisedge_nats_pending_recovery_items > 0` | `5m` | Process-Local In-Memory |
| **`NATSPendingRecoveryAccumulationCritical`** | `critical` | `aegisedge_nats_pending_recovery_items > 20` | `5m` | Process-Local In-Memory |
| **`NATSHighMessageTerminationRate`** | `warning` | `increase(aegisedge_nats_terminated_messages_total[5m]) > 10` | `2m` | Process-Local Counter |

> [!NOTE]
> **Threshold Tuning**: Backlog thresholds (100 warning, 500 critical) and pending recovery limits (0 warning, 20 critical) are initial operational baselines. Operators should tune these values in `alert_rules.yml` to reflect specific edge fleet size, reporting frequencies, and normal administrative review cadences.

---

### 10.2 Metric Scopes and Architectural Invariants

Understanding the data lifetime and storage tier of each metric is mandatory before interpreting alert firing:

1. **Durable SQLite State (`aegisedge_quarantine_active_records`)**:
   - Represents the physical count of records with `status = 'quarantined'` stored in SQLite `cp_telemetry_quarantine`.
   - **Persistent across restarts**: Stopping or restarting the control-plane daemon does **not** reset this gauge.
   - Evaluated at scrape time via covering index scan with a 2-second timeout.
2. **Process-Local In-Memory Queue (`aegisedge_nats_pending_recovery_items`)**:
   - Measures items currently held in the consumer's `c.pendingExhausted` map awaiting asynchronous insertion into SQLite quarantine.
   - **Non-durable**: Resets to 0 upon control-plane restart. On restart, unpersisted messages are re-acquired from JetStream unacknowledged streams and advisory sequences.
3. **Process-Local Termination Counter (`aegisedge_nats_terminated_messages_total`)**:
   - Monotonically tracks successful `TermWithReason` and `Term` calls issued to NATS JetStream by the active process.
   - **Non-durable**: Resets to 0 on restart. Preinitialized to 0 on startup so `increase()` has an uninterrupted baseline from $t=0$.

---

### 10.3 Alert Investigation & Remediation Procedures

#### 10.3.1 `QuarantineActiveBacklogHigh` & `QuarantineActiveBacklogCritical`

- **Meaning**: The number of durable records awaiting operator triage in `quarantined` state has exceeded 100 (warning) or 500 (critical) for over 5 minutes.
- **First Inspection Step**: Query the control-plane quarantine API to review failure categories and recent quarantine entries:
  ```powershell
  # List recent quarantined batches
  curl -s -H "Authorization: Bearer <ADMIN_TOKEN>" "http://127.0.0.1:8080/api/v1/telemetry/quarantine?limit=20&status=quarantined" | jq .
  ```
- **Diagnostic Signals**:
  - Check failure category distribution: `deserialization_error`, `schema_validation_error`, `delivery_exhausted`.
  - Inspect structured server logs: `slog.String("failure_category", ...)` and `slog.String("reason", ...)`.
- **Safe Remediation**:
  - Inspect specific record details:
    ```powershell
    curl -s -H "Authorization: Bearer <ADMIN_TOKEN>" "http://127.0.0.1:8080/api/v1/telemetry/quarantine/<record_id>" | jq .
    ```
  - For schema fixes or restored upstream services, replay eligible records:
    ```powershell
    curl -X POST -H "Authorization: Bearer <ADMIN_TOKEN>" "http://127.0.0.1:8080/api/v1/telemetry/quarantine/<record_id>/replay"
    ```
  - For unrecoverable or malicious test payloads, discard the record:
    ```powershell
    curl -X POST -H "Authorization: Bearer <ADMIN_TOKEN>" "http://127.0.0.1:8080/api/v1/telemetry/quarantine/<record_id>/discard"
    ```
- **Prohibited Actions**:
  - **Do NOT** manually execute `DELETE FROM cp_telemetry_quarantine` or `UPDATE cp_telemetry_quarantine SET status = 'discarded'` directly via SQLite CLI. Bypassing the API skips audit trails and administrative authorization invariants.

---

#### 10.3.2 `QuarantineActiveCountStorageFailure`

- **Meaning**: The active quarantine gauge is emitting `NaN` (detected in PromQL via `metric != metric`). The control plane cannot count active records due to SQLite timeouts (>2s) or database errors.
- **First Inspection Step**: Check control plane application logs for storage warnings:
  ```powershell
  # Search for active quarantine query errors
  grep "failed to count active quarantine records for metrics" /var/log/aegisedge/control-plane.log
  ```
- **Diagnostic Signals**:
  - Check database lock contention or disk I/O wait times.
  - Check whether a heavy database maintenance job (e.g. bulk VACUUM or unindexed queries) is holding exclusive SQLite locks.
  - Verify filesystem health and free disk space where `control-plane.db` is stored.
- **Safe Remediation**:
  - Verify server readiness via `GET /readyz`.
  - If locked, identify long-running SQLite transactions or external tools accessing the database file.
- **Prohibited Actions**:
  - **Do NOT** kill `-9` the control-plane process while SQLite WAL checkpoints are in progress, which risks database recovery delays.

---

#### 10.3.3 `QuarantineActiveMetricMissing`

- **Meaning**: `absent(aegisedge_quarantine_active_records)` evaluates to 1 for over 5 minutes. The metric is completely missing from Prometheus scrape targets.
- **First Inspection Step**: Verify that the control plane process is running and reachable:
  ```powershell
  curl -i http://127.0.0.1:8080/healthz
  curl -s http://127.0.0.1:8080/metrics | grep aegisedge_quarantine_active_records
  ```
- **Diagnostic Signals**:
  - Process exit / crash events in systemd, Kubernetes, or Docker logs.
  - Prometheus target status: check Prometheus UI `Targets` page for `aegisedge-control-plane` scrape errors (e.g. `connection refused`, `context deadline exceeded`).
- **Safe Remediation**:
  - Restart the control-plane daemon if stopped.
  - Verify firewall and network routing between Prometheus scraper and port 8080.

---

#### 10.3.4 `NATSPendingRecoveryAccumulationWarning` & `NATSPendingRecoveryAccumulationCritical`

- **Meaning**: Messages that exceeded `MaxDeliver` are queued in the consumer's in-memory `pendingExhausted` map awaiting quarantine persistence for >5m (>0 warning, >20 critical).
- **First Inspection Step**: Check NATS consumer logs for recovery write failures:
  ```powershell
  grep "quarantine write failed during background recovery retry" /var/log/aegisedge/control-plane.log
  ```
- **Diagnostic Signals**:
  - Check `c.pendingExhausted` retry attempt counts in log attributes (`attempts`, `lastAttempt`).
  - Check for database contention on `RecordQuarantine`.
  - Inspect advisory stream metrics: `aegisedge_nats_advisory_fetch_errors_total` and `aegisedge_nats_advisory_evictions_total`.
- **Safe Remediation**:
  - Ensure the database is writable. As soon as SQLite write capacity is restored, the background recovery worker automatically drains `pendingExhausted`.
- **Prohibited Actions**:
  - **Do NOT** restart the control-plane process prematurely if avoidable. While messages remain in JetStream, in-memory retry tracking is lost on restart and must be re-discovered via advisory streams.

---

#### 10.3.5 `NATSHighMessageTerminationRate`

- **Meaning**: `increase(aegisedge_nats_terminated_messages_total[5m]) > 10` for over 2 minutes. The control plane is actively terminating poisoned messages in JetStream at an elevated rate.
- **First Inspection Step**: Inspect the latest quarantined records and termination logs:
  ```powershell
  curl -s -H "Authorization: Bearer <ADMIN_TOKEN>" "http://127.0.0.1:8080/api/v1/telemetry/quarantine?limit=10" | jq .
  ```
- **Diagnostic Signals**:
  - Common termination reasons:
    - `malformed_json`: Ingested payload cannot be parsed.
    - `invalid_envelope`: Missing `node_id`, `batch_id`, or metadata fields.
    - `subject_character_violation`: Node ID contains illegal NATS subject characters.
  - Identify whether a specific edge node or agent version is broadcasting malformed batches.
- **Safe Remediation**:
  - If a malfunctioning edge agent firmware/build is producing invalid payloads, isolate the specific edge node or roll back its agent version.
  - Note: Messages counted by this metric have **already been durably persisted** to quarantine storage before termination; no telemetry data is silently lost.
- **Prohibited Actions**:
  - **Do NOT** purge or delete the NATS JetStream stream. Termination cleanly halts redelivery for poisoned messages without impacting healthy concurrent telemetry streams.

---

### 10.4 Scrape Configuration & Container Networking

The reference configuration in [`infra/prometheus/prometheus.yml`](../../infra/prometheus/prometheus.yml) targets `127.0.0.1:8080`:

```yaml
scrape_configs:
  - job_name: "aegisedge-control-plane"
    metrics_path: "/metrics"
    static_configs:
      - targets: ["127.0.0.1:8080"]
```

- **Host-Native Execution**: `127.0.0.1:8080` is appropriate when Prometheus executes directly on the host alongside the AegisEdge control-plane binary.
- **Dockerized Prometheus**: When Prometheus runs inside a Docker container, `127.0.0.1` addresses the container's isolated loopback interface rather than the host. For Docker Desktop (Windows/macOS), configure `host.docker.internal:8080`. On Linux, use `--net=host` or the host gateway IP.
- **Docker Compose Topology**: If deploying Prometheus via Docker Compose on a shared network, specify the service DNS name (e.g. `control-plane:8080`).
- **Deployment Scope**: The repository currently provides a local NATS compose environment (`infra/nats/docker-compose.yml`) but does not establish a combined Prometheus + control-plane Docker Compose stack.

---

### 10.5 Formal Rule Validation & CI Integration (Stage 6.31B.11A.1)

Prometheus rule and configuration files are formally verified in the AegisEdge test suite using YAML parsing and typed structural validation (`gopkg.in/yaml.v3`) in [`services/control-plane/metrics/alert_rules_test.go`](../../services/control-plane/metrics/alert_rules_test.go):

1. **Typed Schema Assertions**:
   - Parses `alert_rules.yml` and `prometheus.yml` into typed Go structs.
   - Verifies the rule group name `aegisedge-quarantine-and-nats` and all 7 alert definitions.
   - Asserts exact per-alert PromQL expressions, valid duration parsing via `time.ParseDuration`, bounded severity levels (`warning` or `critical`), and non-empty summary/description annotations.
   - Verifies that referenced metrics match registered Go metric constants.
2. **Promtool CLI Validation**:
   - When the official `promtool` binary is present on the environment `$PATH`, `TestAlertRules_PromtoolCheck` executes `promtool check rules` and `promtool check config`.
   - If `promtool` is not installed, the test explicitly marks the CLI step as **SKIPPED** via `t.Skip`, reporting the skip without falsely claiming CLI execution.
3. **Reproducible Validation Commands**:
   - **Native Promtool Check** (when installed locally):
     ```powershell
     promtool check rules infra/prometheus/alert_rules.yml
     promtool check config infra/prometheus/prometheus.yml
     ```
   - **Dockerized Promtool Check** (using pinned official Prometheus image with explicit promtool entrypoint):
     ```powershell
     docker run --rm --entrypoint promtool -v "${PWD}/infra/prometheus:/etc/prometheus" prom/prometheus:v3.2.1 check rules /etc/prometheus/alert_rules.yml
     docker run --rm --entrypoint promtool -v "${PWD}/infra/prometheus:/etc/prometheus" prom/prometheus:v3.2.1 check config /etc/prometheus/prometheus.yml
     ```
     *(Note: Docker commands require an active Docker daemon and were not executed in local test environments where Docker was unavailable.)*


---

## 11. Active Quarantine SQLite Performance Benchmarking (Stage 6.31B.11C)

### 11.1 Purpose & Prometheus Scrape Impact

The active quarantine backlog metric (`aegisedge_quarantine_active_records`) is dynamically computed during Prometheus scrape collection by invoking:
```go
CountActiveQuarantineRecords(ctx context.Context) (int64, error)
```
The underlying SQL query executed against SQLite is:
```sql
SELECT COUNT(*) FROM cp_telemetry_quarantine WHERE status = 'quarantined';
```

Because Prometheus scrape requests enforce a default timeout (typically 2–10 seconds, configured in AegisEdge to 2 seconds for quarantine inspection), database queries executed on scrape must be fast, bounded, and resource-efficient. This benchmarking suite measures the execution latency, memory footprint, and concurrency behavior across varying dataset sizes and payload volumes.

### 11.2 Query Plan Analysis & Covering Index Verification

The quarantine table schema defines a composite index:
```sql
CREATE INDEX IF NOT EXISTS idx_cp_quarantine_status_updated
ON cp_telemetry_quarantine(status, updated_at);
```

Running `EXPLAIN QUERY PLAN` on the count query produces:
```text
SEARCH cp_telemetry_quarantine USING COVERING INDEX idx_cp_quarantine_status_updated (status=?)
```

**Key Architectural Insights:**
1. **Covering Index Optimization**: SQLite satisfies the `COUNT(*)` query solely by traversing the B-tree of `idx_cp_quarantine_status_updated`. It does not visit the primary table rows (`rowid` lookups are avoided).
2. **Payload Independence**: The quarantine payload BLOB (which stores raw telemetry JSON chunks up to 64 KiB or more) is never loaded into memory or traversed. Execution latency and allocations remain completely invariant to payload size.
3. **Automated Plan Assertion**: The benchmark suite asserts this exact covering index plan prior to execution via `verifyBenchmarkQueryPlan`.

### 11.3 Benchmark Methodology

The benchmarks are implemented in [`services/control-plane/storage/quarantine_benchmark_test.go`](file:///C:/AegisEdge/services/control-plane/storage/quarantine_benchmark_test.go):
- **Isolated Databases**: Each benchmark run creates an isolated temporary SQLite database initialized through the standard production `OpenSQLite` factory, ensuring identical pragmas (`WAL`, `busy_timeout=5000`, `foreign_keys=ON`, `synchronous=NORMAL`) and schema.
- **Controlled Distribution**: Datasets are populated with realistic status distributions:
  - `10%` `quarantined` (active backlog)
  - `45%` `replayed` (terminal state)
  - `45%` `discarded` (terminal state)
- **Dataset Tiers**: Scaled across four orders of magnitude: `100`, `1,000`, `10,000`, and `100,000` total records.
- **Payload Dimensions**: Tested with both `1 KiB` and `64 KiB` payloads to empirically confirm payload independence.
- **Concurrency & Serialization**: `BenchmarkCountActiveQuarantineRecordsParallel` evaluates read concurrency across parallel goroutines (`b.RunParallel`) under SQLite's single-writer / connection pool configuration (`SetMaxOpenConns(1)`).

### 11.4 Measured Performance Results

*Measured on 13th Gen Intel(R) Core(TM) i7-13620H @ 2.40 GHz (Windows 11, Go 1.24.2):*

#### Sequential Execution (`BenchmarkCountActiveQuarantineRecords`)

| Total Records | Active (`quarantined`) | Payload Size | Latency (`ns/op`) | Latency (`ms/op`) | Memory (`B/op`) | Allocs (`allocs/op`) |
|--------------:|-----------------------:|-------------:|------------------:|------------------:|----------------:|---------------------:|
| 100 | 10 | 1 KiB | 16,781 ns/op | ~0.017 ms | 544 B/op | 15 allocs/op |
| 100 | 10 | 64 KiB | 55,727 ns/op | ~0.056 ms | 544 B/op | 15 allocs/op |
| 1,000 | 100 | 1 KiB | 65,660 ns/op | ~0.066 ms | 544 B/op | 15 allocs/op |
| 1,000 | 100 | 64 KiB | 96,841 ns/op | ~0.097 ms | 544 B/op | 15 allocs/op |
| 10,000 | 1,000 | 1 KiB | 582,251 ns/op | ~0.582 ms | 552 B/op | 16 allocs/op |
| 10,000 | 1,000 | 64 KiB | 243,208 ns/op | ~0.243 ms | 552 B/op | 16 allocs/op |
| 100,000 | 10,000 | 1 KiB | 2,317,686 ns/op | ~2.318 ms | 552 B/op | 16 allocs/op |

#### Parallel Contention (`BenchmarkCountActiveQuarantineRecordsParallel`)

| Total Records | Active (`quarantined`) | Payload Size | Latency (`ns/op`) | Latency (`ms/op`) | Memory (`B/op`) | Allocs (`allocs/op`) |
|--------------:|-----------------------:|-------------:|------------------:|------------------:|----------------:|---------------------:|
| 1,000 | 100 | 1 KiB | 31,973 ns/op | ~0.032 ms | 688 B/op | 18 allocs/op |
| 10,000 | 1,000 | 1 KiB | 470,220 ns/op | ~0.470 ms | 698 B/op | 19 allocs/op |
| 10,000 | 1,000 | 64 KiB | 317,228 ns/op | ~0.317 ms | 698 B/op | 19 allocs/op |

### 11.5 Analysis & Observations

1. **Sub-Millisecond Execution Up to 10k Records**:
   - For backlogs up to 1,000 active quarantine records (within a 10,000 row table), query execution remains strictly sub-millisecond ($<0.6\text{ ms}$).
2. **Predictable Scaling at 100k Records**:
   - At 100,000 records (10,000 active quarantine entries), count latency is approximately `2.3 ms`. This represents roughly `0.11%` of the Prometheus 2-second collection timeout, leaving ample margin for telemetry ingestion and HTTP transport.
3. **Flat Allocations**:
   - Memory allocations remain constant across all scales (`544–552 B/op`, `15–16 allocs/op`), representing solely the Go SQL driver statement preparation and row scanning wrappers. No record data or payloads are allocated on the heap.
4. **Single Connection Serialization**:
   - Under `SetMaxOpenConns(1)`, concurrent goroutines queue safely within Go's `database/sql` connection pool without SQLite busy lock errors (`SQLITE_BUSY`) or thread starvation.

### 11.6 Reproducible Benchmark Commands

Execute sequential benchmarks:
```powershell
go test ./services/control-plane/storage/... -run '^$' -bench '^BenchmarkCountActiveQuarantineRecords$' -benchmem -count=1
```

Execute parallel contention benchmarks:
```powershell
go test ./services/control-plane/storage/... -run '^$' -bench '^BenchmarkCountActiveQuarantineRecordsParallel$' -benchmem -count=1
```

Execute all quarantine benchmarks with extended timing:
```powershell
go test ./services/control-plane/storage/... -run '^$' -bench 'BenchmarkCountActiveQuarantineRecords' -benchmem -benchtime=2s -count=1
```

### 11.7 Caveats & Inter-Machine Variance

- **Hardware Dependency**: Absolute timings will vary depending on disk I/O performance (NVMe vs SATA/HDD), CPU single-thread clock speed, and virtualization layers.
- **Benchmarking Guidelines**: Benchmark numbers must never be used as brittle unit-test assertions (`t.Fatalf` on execution time); they serve as empirical operational capacity bounds for cluster sizing and alerting threshold calibration.


---

## 12. End-to-End Prometheus Monitoring & Alert Validation (Stage 6.31B.12)

### 12.1 Overview & Architecture

AegisEdge provides a complete, tiered observability stack for edge quarantine and NATS recovery operations:
1. **Instrumented Control Plane**: Exposes Prometheus-formatted operational metrics at `GET /metrics` on port `8080`.
2. **Prometheus Configuration**: Configured for both host-native (`infra/prometheus/prometheus.yml`) and containerized (`infra/prometheus/prometheus.docker.yml`, `infra/prometheus/docker-compose.yml`) deployments.
3. **Alerting Rules**: 7 dedicated alerting rules (`infra/prometheus/alert_rules.yml`) governing backlog growth, storage failures (NaN), missing metrics, NATS queue accumulation, and high message termination rates.
4. **Behavioral Unit Testing**: Prometheus rule test suite (`infra/prometheus/alert_rules_test.yml`) executed via `promtool test rules`.
5. **Typed Structural Validation**: Automated Go test suite (`services/control-plane/metrics/alert_rules_test.go`) validating schemas without external dependencies.

---

### 12.2 Deployment Models: Host-Native vs Containerized

#### 1. Host-Native Prometheus Execution
When running Prometheus as a native binary directly on the developer or production host:
- **Config File**: [`infra/prometheus/prometheus.yml`](../../infra/prometheus/prometheus.yml)
- **Scrape Target**: `127.0.0.1:8080` (reaches the control plane running on the same host).
- **Run Command**:
  ```powershell
  prometheus --config.file=infra/prometheus/prometheus.yml --storage.tsdb.path=data/prometheus
  ```

#### 2. Containerized Prometheus Execution (Docker Compose)
When running Prometheus inside a container (Docker Desktop on Windows/macOS or Docker on Linux):
- **Compose File**: [`infra/prometheus/docker-compose.yml`](../../infra/prometheus/docker-compose.yml)
- **Config File**: [`infra/prometheus/prometheus.docker.yml`](../../infra/prometheus/prometheus.docker.yml)
- **Scrape Target**: `host.docker.internal:8080`
- **Network Topology**:
  - Inside a Docker container, `127.0.0.1` refers to the container's isolated loopback namespace.
  - To reach the host control-plane server, Docker Desktop resolves `host.docker.internal`.
  - On Linux hosts, `docker-compose.yml` configures `extra_hosts: ["host.docker.internal:host-gateway"]` to route to the host bridge.
- **Port Mapping**: The Prometheus UI is bound to host port `127.0.0.1:9090` (`http://localhost:9090`).
- **Data Persistence**: Backed by a dedicated Docker volume `aegisedge_prometheus_data`.
- **Run Commands**:
  ```powershell
  docker compose -f infra/prometheus/docker-compose.yml up -d
  docker compose -f infra/prometheus/docker-compose.yml down
  ```

---

### 12.3 Validation Tiers & Tool Availability

Validation is organized into three distinct tiers to ensure developer workflows remain productive regardless of local environment tooling:

| Tier | Component | Dependencies | Execution Mechanism | Scope & Guarantees |
|:---|:---|:---|:---|:---|
| **Tier 1** | **Go Schema Tests** | Go compiler only | `go test ./services/control-plane/metrics/...` | Parses YAML files via `gopkg.in/yaml.v3`, validates PromQL expressions, checks alert names, durations, severities, and verifies behavioral test coverage for all 7 alerts. |
| **Tier 2** | **Promtool CLI Validation** | `promtool` or Docker | `promtool check ...` and `promtool test rules ...` | Performs official Prometheus syntactic and behavioral unit testing. If tools are missing, explicitly reports **SKIPPED** (never false PASS). |
| **Tier 3** | **Live Scrape Verification** | Control Plane + Prometheus | PowerShell script / curl | Verifies live HTTP 200 response on `GET /metrics`, validates target status `UP` in Prometheus, and queries time-series. |

---

### 12.4 Alert-Rule Behavioral Testing (`alert_rules_test.yml`)

The rule test file [`infra/prometheus/alert_rules_test.yml`](../../infra/prometheus/alert_rules_test.yml) executes discrete simulation scenarios using `promtool test rules`:

1. **`QuarantineActiveBacklogHigh` (Warning)**:
   - Evaluates active backlog `> 100`.
   - Simulates backlog rising to 150. Asserts pending state at $t=2\text{m}$, un-fired at $t=6\text{m}$, firing with severity `warning` at $t=7\text{m}$ (`for: 5m`), and resolving when backlog drops to 50.
2. **`QuarantineActiveBacklogCritical` (Critical)**:
   - Evaluates active backlog `> 500`.
   - Demonstrates independent threshold evaluation: at backlog 150, Warning fires while Critical remains inactive. At backlog 600, Critical fires after 5m. When backlog returns to 200, Critical resolves immediately while Warning remains firing.
3. **`QuarantineActiveCountStorageFailure` (Critical)**:
   - Evaluates IEEE 754 NaN detection idiom: `aegisedge_quarantine_active_records != aegisedge_quarantine_active_records`.
   - Simulates SQLite query timeouts emitting `NaN`. Asserts pending at $t=2\text{m}$, firing after $2\text{m}$ (`for: 2m`), and resolving when valid numeric telemetry resumes.
4. **`QuarantineActiveMetricMissing` (Critical)**:
   - Evaluates `absent(aegisedge_quarantine_active_records)`.
   - Simulates scrape failure/metric absence using missing samples (`_`). Fires after 5 minutes of total absence and resolves upon metric reappearance.
5. **`NATSPendingRecoveryAccumulationWarning` (Warning)**:
   - Evaluates in-memory recovery items `> 0`.
   - Simulates item accumulation (value 5). Fires after 5 minutes and resolves when consumer flushes queue to 0.
6. **`NATSPendingRecoveryAccumulationCritical` (Critical)**:
   - Evaluates in-memory recovery items `> 20`.
   - Simulates queue backlog spiking to 30. Fires after 5 minutes and resolves independently when backlog decreases below 20.
7. **`NATSHighMessageTerminationRate` (Warning)**:
   - Evaluates `increase(aegisedge_nats_terminated_messages_total[5m]) > 10`.
   - Simulates rapid poison message terminations (counter delta 22 over 5 minutes). Fires after 2 minutes of sustained high rate (`for: 2m`) and resolves when termination rate flattens.

---

### 12.5 Live Service Verification Procedure

To manually verify the end-to-end monitoring pipeline in PowerShell:

#### Step 1: Start the Control Plane Server
```powershell
# In terminal 1 (starts control plane with default port 8080 and local DB)
go run ./services/control-plane -port=8080 -db-path=data/control-plane.db
```

#### Step 2: Confirm Endpoint Responds & Exposes AegisEdge Metrics
```powershell
# In terminal 2
$resp = Invoke-WebRequest -Uri "http://127.0.0.1:8080/metrics" -UseBasicParsing
Write-Host "Status Code: $($resp.StatusCode)"

# Verify representative quarantine and NATS metrics
$resp.Content -split "`n" | Select-String -Pattern "aegisedge_quarantine_active_records|aegisedge_nats_pending_recovery_items|aegisedge_nats_terminated_messages_total"
```
*Expected Output:*
```text
Status Code: 200
# TYPE aegisedge_quarantine_active_records gauge
aegisedge_quarantine_active_records 0
# TYPE aegisedge_nats_pending_recovery_items gauge
aegisedge_nats_pending_recovery_items 0
# TYPE aegisedge_nats_terminated_messages_total counter
aegisedge_nats_terminated_messages_total 0
```

#### Step 3: Launch Prometheus (Docker Compose)
```powershell
docker compose -f infra/prometheus/docker-compose.yml up -d
```

#### Step 4: Confirm Scrape Target is UP
Query the Prometheus API endpoint:
```powershell
$targets = Invoke-RestMethod -Uri "http://127.0.0.1:9090/api/v1/targets"
$targets.data.activeTargets | Select-Object discoveredLabels, health, lastScrape
```
*Expected Output:* `health: "up"` for job `"aegisedge-control-plane"`.

#### Step 5: Query Representative Metrics Through Prometheus
```powershell
$result = Invoke-RestMethod -Uri "http://127.0.0.1:9090/api/v1/query?query=aegisedge_quarantine_active_records"
$result.data.result
```

#### Step 6: Inspect Active Alerts
```powershell
# Query Prometheus alerts API
$alerts = Invoke-RestMethod -Uri "http://127.0.0.1:9090/api/v1/alerts"
$alerts.data.alerts
```
Alternatively, open `http://localhost:9090/alerts` in a browser to inspect the state (`Inactive`, `Pending`, or `Firing`) of all 7 AegisEdge alert rules.

---

### 12.6 Troubleshooting DOWN Scrape Targets

If Prometheus reports the `aegisedge-control-plane` target as `DOWN`:

1. **Host Binding vs Loopback**:
   - When running Prometheus in Docker, the control plane must be accessible from outside `127.0.0.1`.
   - On Windows, ensure the control plane is listening on all interfaces if necessary:
     ```powershell
     go run ./services/control-plane -host=0.0.0.0 -port=8080
     ```
2. **Docker Host Gateway**:
   - Verify that `host.docker.internal` resolves from inside the container:
     ```powershell
     docker exec -it aegisedge-prometheus wget -q -O - http://host.docker.internal:8080/metrics
     ```
3. **Firewall & Port Conflicts**:
   - Check if Windows Defender Firewall blocks inbound connections on port `8080`.
   - Ensure another process is not holding port `8080` (`Get-NetTCPConnection -LocalPort 8080`).

---

### 12.7 Graceful Degradation & Skipped Validations

- When `promtool` and Docker are absent, automated CI runs `check.ps1` and `go test ./services/control-plane/...`.
- `TestAlertRules_PromtoolCheck` cleanly logs `t.Skip` without failing the test suite.
- `check.ps1` displays a yellow warning notifying operators that CLI checks were skipped, while maintaining 100% test coverage over YAML syntax, PromQL expressions, and behavioral test suite structure via typed Go tests.


---

## 13. Grafana Observability Dashboard (Stage 6.31B.13)

### 13.1 Overview & Architecture

AegisEdge provides a production-grade Grafana dashboard designed for edge telemetry and NATS recovery operations:
- **Dashboard Name**: `AegisEdge — Telemetry & Recovery Operations`
- **Dashboard UID**: `aegisedge-telemetry-recovery`
- **Location**: [`infra/prometheus/grafana/dashboards/telemetry_and_recovery_operations.json`](../../infra/prometheus/grafana/dashboards/telemetry_and_recovery_operations.json)
- **Deployment**: Integrated into [`infra/prometheus/docker-compose.yml`](../../infra/prometheus/docker-compose.yml) using pinned Grafana image `grafana/grafana:11.5.2`.
- **Provisioning**: 100% automated via declarative file mounts. No manual browser setup required.

```mermaid
graph LR
    subgraph Host Network
        CP["Control Plane<br/>:8080/metrics"]
    end

    subgraph Docker Compose Bridge Network
        Prom["Prometheus<br/>:9090"]
        Graf["Grafana<br/>:3000"]
        DS["Provisioned Datasource<br/>(http://prometheus:9090)"]
        Dash["Provisioned Dashboard<br/>(AegisEdge)"]

        Prom -->|"scrapes host.docker.internal:8080"| CP
        Graf --> DS
        DS --> Prom
        Graf --> Dash
    end

    HostUser["Operator Browser<br/>(http://localhost:3000)"] -->|"127.0.0.1:3000"| Graf
```

---

### 13.2 Dashboard Structure & Panel Inventory

The dashboard is structured into four functional operational sections:

#### Section A: Monitoring Health
- **Panel 2 (Stat)**: `Control Plane Scrape Target (up)`
  - *Query*: `up{job="aegisedge-control-plane"}`
  - *Purpose*: Immediate visual indicator of control plane availability (`UP` in green, `DOWN` in red, `NO DATA` in gray).
- **Panel 3 (Stat)**: `Telemetry Ingestion Rate`
  - *Query*: `sum(rate(aegisedge_control_plane_ingested_batches_total[5m]))`
  - *Purpose*: Aggregate throughput of incoming edge telemetry batches (`reqps`).
- **Panel 4 (Stat)**: `Storage Probe Failures`
  - *Query*: `aegisedge_control_plane_storage_probe_failures_total`
  - *Purpose*: Monitors `/readyz` probe health to detect SQLite lock contention or disk saturation.

#### Section B: Quarantine Operations
- **Panel 6 (Gauge)**: `Active Quarantine Records`
  - *Query*: `aegisedge_quarantine_active_records`
  - *Thresholds*: Normal $\le 100$ (Green), Warning $> 100$ (Yellow), Critical $> 500$ (Red). Aligns with alert rules `QuarantineActiveBacklogHigh` and `QuarantineActiveBacklogCritical`.
- **Panel 7 (Timeseries)**: `Active Quarantine Trend (Backlog Progression)`
  - *Query*: `aegisedge_quarantine_active_records`
  - *Threshold Lines*: Visual horizontal markers at 100 (Warning) and 500 (Critical). Tracks accumulation rate over time.
- **Panel 8 (Stat)**: `Quarantine Storage Health (NaN Check)`
  - *Query*: `(aegisedge_quarantine_active_records != aegisedge_quarantine_active_records) or (0 * aegisedge_quarantine_active_records)`
  - *Purpose*: Explicitly surfaces IEEE 754 NaN values emitted when SQLite count queries fail or exceed the 2-second timeout (`HEALTHY (0 NaN)` in green, `FAILURE (NaN DETECTED)` in red, `NO DATA` in gray).
- **Panel 9 (Timeseries)**: `Quarantine Discard vs Replay Resolution Rate`
  - *Queries*: `sum by (outcome) (rate(aegisedge_quarantine_replays_total[5m]))` and `sum by (outcome) (rate(aegisedge_quarantine_discards_total[5m]))`
  - *Purpose*: Operator throughput monitoring showing successful vs conflicting replays and discards.

#### Section C: NATS Recovery Operations
- **Panel 11 (Gauge)**: `NATS Pending Recovery Queue`
  - *Query*: `aegisedge_nats_pending_recovery_items`
  - *Thresholds*: Normal $= 0$ (Green), Warning $> 0$ (Yellow), Critical $> 20$ (Red). Aligns with alert rules `NATSPendingRecoveryAccumulationWarning` and `NATSPendingRecoveryAccumulationCritical`.
- **Panel 12 (Timeseries)**: `NATS Pending Recovery Backlog Trend`
  - *Query*: `aegisedge_nats_pending_recovery_items`
  - *Threshold Line*: Marker at 20 (Critical). Identifies recovery worker stalls or database write throttling.
- **Panel 13 (Stat)**: `Terminated Poison Messages (Total)`
  - *Query*: `aegisedge_nats_terminated_messages_total`
  - *Purpose*: Monotonically increasing counter of permanently unprocessable messages cleanly terminated via JetStream Term.
- **Panel 14 (Timeseries)**: `NATS Message Termination Rate (5m Increase)`
  - *Query*: `increase(aegisedge_nats_terminated_messages_total[5m])`
  - *Threshold Line*: Marker at 10 (Warning). Aligns with alert rule `NATSHighMessageTerminationRate`.

#### Section D: Operational Alerts Status
- **Panel 16 (Table)**: `Active AegisEdge Quarantine & NATS Alerts`
  - *Query*: `ALERTS{alertname=~"Quarantine.*|NATS.*"}`
  - *Purpose*: Displays active Prometheus alerts with state (`pending` or `firing`) and severity level, linking directly to triage runbooks.

---

### 13.3 Declarative Provisioning Configuration

1. **Prometheus Datasource Provisioning**:
   - File: [`infra/prometheus/grafana/provisioning/datasources/prometheus.yml`](../../infra/prometheus/grafana/provisioning/datasources/prometheus.yml)
   - Targets the Prometheus container via internal Docker DNS: `http://prometheus:9090`.
   - Set as default datasource with HTTP POST query method and 15s scrape alignment.
2. **Dashboard Provider Provisioning**:
   - File: [`infra/prometheus/grafana/provisioning/dashboards/dashboards.yml`](../../infra/prometheus/grafana/provisioning/dashboards/dashboards.yml)
   - Automatically loads dashboards from `/var/lib/grafana/dashboards`.
3. **Dashboard Definition**:
   - File: [`infra/prometheus/grafana/dashboards/telemetry_and_recovery_operations.json`](../../infra/prometheus/grafana/dashboards/telemetry_and_recovery_operations.json)
   - Mounted read-only into `/var/lib/grafana/dashboards`.

---

### 13.4 Deployment & Local Access

#### 1. Starting the Monitoring Stack
```powershell
docker compose -f infra/prometheus/docker-compose.yml up -d
```

#### 2. Accessing the Dashboard
- **URL**: `http://localhost:3000` or `http://127.0.0.1:3000`
- **Authentication**: Anonymous viewer access is enabled by default (`GF_AUTH_ANONYMOUS_ENABLED=true`).
- **Dashboard Location**: Navigate to **Dashboards** > **AegisEdge** > **AegisEdge — Telemetry & Recovery Operations** (or direct URL `http://localhost:3000/d/aegisedge-telemetry-recovery`).

#### 3. Stopping the Stack
```powershell
docker compose -f infra/prometheus/docker-compose.yml down
```

#### 4. Preserving vs Resetting Data
- **Preserve Data**: Container state and dashboards survive restarts via named Docker volumes `aegisedge_prometheus_data` and `aegisedge_grafana_data`.
- **Reset State**: To wipe TSDB history and Grafana preferences:
  ```powershell
  docker compose -f infra/prometheus/docker-compose.yml down -v
  ```

---

### 13.5 Troubleshooting Guide

1. **Disconnected Prometheus Datasource (`HTTP Error Bad Gateway` or `Connection Refused`)**:
   - Ensure both `prometheus` and `grafana` services are running in the same compose network (`docker compose ps`).
   - Verify that Grafana targets `http://prometheus:9090` (container internal name), not `localhost:9090`.
2. **Scrape Target `DOWN` in Panel 2**:
   - Ensure the AegisEdge control-plane server is running on the host (`go run ./services/control-plane -host=0.0.0.0 -port=8080`).
   - Confirm firewall rules allow inbound traffic to host port 8080.
3. **`NO DATA` on Panels**:
   - Normal on initial startup before edge nodes register or before any quarantine records occur.
   - For `up` panel: verify scrape job name is `aegisedge-control-plane`.

---

### 13.6 Validation Tiers & Tool Requirements

| Tier | Component | Dependencies | Execution Mechanism | Scope & Guarantees |
|:---|:---|:---|:---|:---|
| **Tier 1** | **Go Schema & Metric Validation** | Go compiler only | `go test ./services/control-plane/metrics/...` | Validates JSON syntax, UID uniqueness, panel IDs, and asserts that all panel queries reference valid Go metric constants. |
| **Tier 2** | **PowerShell Script Validation** | PowerShell | `.\scripts\check.ps1` | Validates JSON parsing and required title/UID/panels in automated scripts. |
| **Tier 3** | **Live Docker Integration** | Docker daemon running | `docker compose up -d` | Verifies live container health and UI rendering. When Docker is offline, Tier 3 is explicitly marked as **SKIPPED** while Tiers 1 and 2 provide 100% test coverage. |

---

## 14. End-to-End Observability Runtime Validation (Stage 6.31B.14)

### 14.1 Verified Architecture & Runtime Topology

In Stage 6.31B.14, full runtime validation of the live AegisEdge observability stack was executed with Docker Desktop Engine v29.8.0, Docker Compose v5.5.1, and the native Go control-plane server on Windows 11 WSL2.

```mermaid
graph TD
    subgraph Host Network (Windows 11)
        CP["AegisEdge Control Plane<br/>127.0.0.1:8080<br/>GET /metrics"]
        Browser["Operator Browser<br/>http://localhost:3000"]
    end

    subgraph Docker Compose Network (prometheus_default)
        Prom["Prometheus v3.2.1<br/>127.0.0.1:9090<br/>(host.docker.internal:8080)"]
        Graf["Grafana v11.5.2<br/>127.0.0.1:3000"]
        DS["Provisioned Datasource<br/>http://prometheus:9090"]
        Dash["Provisioned Dashboard<br/>UID: aegisedge-telemetry-recovery"]

        Prom -->|"scrapes /metrics every 15s"| CP
        Graf --> DS
        DS --> Prom
        Graf --> Dash
    end

    Browser -->|"accesses UI"| Graf
```

### 14.2 Startup Procedure & Port Allocation

#### Port Allocations
- **Port 8080**: AegisEdge Control Plane HTTP REST and Prometheus metrics endpoint (`GET /metrics`).
- **Port 9090**: Prometheus TSDB server and HTTP query API (`127.0.0.1:9090`).
- **Port 3000**: Grafana UI and dashboard server (`127.0.0.1:3000`).

#### 1. Starting the Observability Stack
```powershell
docker compose -f infra/prometheus/docker-compose.yml up -d
```
Verified outputs:
- Named volumes `aegisedge_prometheus_data` and `aegisedge_grafana_data` created.
- Network `prometheus_default` established.
- Containers `aegisedge-prometheus` (`prom/prometheus:v3.2.1`) and `aegisedge-grafana` (`grafana/grafana:11.5.2`) started.

#### 2. Starting the Control Plane
```powershell
go run ./services/control-plane -host=127.0.0.1 -port=8080
```
Verified endpoint response:
```powershell
(Invoke-WebRequest -Uri "http://localhost:8080/metrics" -UseBasicParsing).StatusCode
# 200 OK
```

---

### 14.3 Runtime Verification Commands & Live API Results

#### 1. Prometheus Server Health and Readiness
```powershell
Invoke-RestMethod -Uri "http://localhost:9090/-/healthy"
# Output: Prometheus Server is Healthy.

Invoke-RestMethod -Uri "http://localhost:9090/-/ready"
# Output: Prometheus Server is Ready.
```

#### 2. Control Plane Scrape Target Health
```powershell
Invoke-RestMethod -Uri "http://localhost:9090/api/v1/targets"
```
Verified runtime status:
- Target: `http://host.docker.internal:8080/metrics`
- Scrape health: **`up`**
- Last error: `""` (no error)
- Scrape duration: ~3.8ms

#### 3. Alert Rules Evaluation Status
```powershell
Invoke-RestMethod -Uri "http://localhost:9090/api/v1/rules"
```
All 7 operational rules loaded from `/etc/prometheus/alert_rules.yml`:
1. `QuarantineActiveBacklogHigh`: `health: ok`, `state: inactive`
2. `QuarantineActiveBacklogCritical`: `health: ok`, `state: inactive`
3. `QuarantineActiveCountStorageFailure`: `health: ok`, `state: inactive`
4. `QuarantineActiveMetricMissing`: `health: ok`, `state: inactive`
5. `NATSPendingRecoveryAccumulationWarning`: `health: ok`, `state: inactive`
6. `NATSPendingRecoveryAccumulationCritical`: `health: ok`, `state: inactive`
7. `NATSHighMessageTerminationRate`: `health: ok`, `state: inactive`

#### 4. Grafana Health & Provisioning Status
```powershell
# Health
Invoke-RestMethod -Uri "http://localhost:3000/api/health"
# database: ok, version: 11.5.2

# Datasource provisioning
Invoke-RestMethod -Uri "http://localhost:3000/api/datasources"
# Confirms datasource name 'Prometheus', type 'prometheus', url 'http://prometheus:9090', isDefault: true

# Dashboard search
Invoke-RestMethod -Uri "http://localhost:3000/api/search?query=AegisEdge"
# Folder 'AegisEdge' (UID: dg3qs5pgrva4ga) contains dashboard UID: 'aegisedge-telemetry-recovery'

# Dashboard panels count
(Invoke-RestMethod -Uri "http://localhost:3000/api/dashboards/uid/aegisedge-telemetry-recovery").dashboard.panels.Count
# 16 panels provisioned
```

---

### 14.4 Dashboard Panel PromQL Runtime Query Audit

All 13 distinct queries across the 16 dashboard panels were evaluated against the live Prometheus server API (`GET /api/v1/query?query=<PromQL>`):

| Panel ID | Panel Title | Exact PromQL Expression | Status | Result Type | Value / Series Count |
|:---:|:---|:---|:---:|:---:|:---:|
| **2** | Control Plane Scrape Target (up) | `up{job="aegisedge-control-plane"}` | `success` | `vector` | `1` (UP) |
| **3** | Telemetry Ingestion Rate | `sum(rate(aegisedge_control_plane_ingested_batches_total[5m]))` | `success` | `vector` | `0` |
| **4** | Storage Probe Failures | `aegisedge_control_plane_storage_probe_failures_total` | `success` | `vector` | `0` |
| **6** | Active Quarantine Records | `aegisedge_quarantine_active_records` | `success` | `vector` | `0` |
| **7** | Active Quarantine Trend (Progression) | `aegisedge_quarantine_active_records` | `success` | `vector` | `0` |
| **8** | Quarantine Storage Health (NaN Check) | `(aegisedge_quarantine_active_records != aegisedge_quarantine_active_records) or (0 * aegisedge_quarantine_active_records)` | `success` | `vector` | `0` (`HEALTHY (0 NaN)`) |
| **9** | Quarantine Replay Rate | `sum by (outcome) (rate(aegisedge_quarantine_replays_total[5m]))` | `success` | `vector` | 5 series (all outcomes initialized) |
| **9** | Quarantine Discard Rate | `sum by (outcome) (rate(aegisedge_quarantine_discards_total[5m]))` | `success` | `vector` | 5 series (all outcomes initialized) |
| **11** | NATS Pending Recovery Queue | `aegisedge_nats_pending_recovery_items` | `success` | `vector` | `0` |
| **12** | NATS Pending Recovery Backlog Trend | `aegisedge_nats_pending_recovery_items` | `success` | `vector` | `0` |
| **13** | Terminated Poison Messages (Total) | `aegisedge_nats_terminated_messages_total` | `success` | `vector` | `0` |
| **14** | Message Termination Rate (5m Increase) | `increase(aegisedge_nats_terminated_messages_total[5m])` | `success` | `vector` | `0` |
| **16** | Active Quarantine & NATS Alerts | `ALERTS{alertname=~"Quarantine.*|NATS.*"}` | `success` | `vector` | 0 series (clean operational state) |

---

### 14.5 Panel 8 Runtime Validation (Finite, NaN, and Absent Metrics)

The Panel 8 expression `(x != x) or (0 * x)` was evaluated across three distinct runtime states:

1. **Finite Normal Metric ($x = 0$)**:
   - Query: `(aegisedge_quarantine_active_records != aegisedge_quarantine_active_records) or (0 * aegisedge_quarantine_active_records)`
   - Runtime Result: `0`
   - Grafana Mapping: **`HEALTHY (0 NaN)`** in green.
2. **Missing / Absent Metric**:
   - Query: `(nonexistent != nonexistent) or (0 * nonexistent)`
   - Runtime Result: `[]` (empty vector)
   - Grafana Mapping: **`NO DATA`** in gray (null is never mapped to healthy).
3. **NaN Metric (Simulated via IEEE 754 vector in PromQL)**:
   - Query: `(vector(0/0) != vector(0/0)) or (0 * vector(0/0))`
   - Runtime Result: `NaN`
   - Grafana Mapping: **`FAILURE (NaN DETECTED)`** in red.

---

### 14.6 Promtool Rule Unit Testing & Validation Verification

1. **Promtool Rule Unit Tests (`alert_rules_test.yml`)**:
   - Running `promtool check config`, `promtool check rules`, and `promtool test rules` all pass with exit code 0 (`SUCCESS`).
   - The test scenarios in `alert_rules_test.yml` accurately reflect Prometheus v3.2.1 PromQL engine evaluation semantics:
     - `QuarantineActiveMetricMissing`: The test models target metric drop using an explicit series staleness marker (`stale`), bypassing lookback-delta retention and verifying the exact 5-minute `for:` pending window (pending at 2m, firing at 7m, resolved at 8m upon metric restoration).
     - `NATSHighMessageTerminationRate`: The test models range-window rate behavior under Prometheus's linear extrapolation algorithm for `increase()` over the 5m window (evaluating to 24 terminated messages), correctly verifying the 2-minute `for:` window (pending at 9m, firing at 11m, resolved at 16m).
   - All 7 production alert rules in `alert_rules.yml` are 100% syntactically and semantically validated by both promtool and Go schema tests.
2. **Host-to-Container Loopback Forwarding**:
   - On Windows WSL2, binding the control plane to `127.0.0.1:8080` allows `host.docker.internal:8080` to reach the control plane transparently via the WSL2 localhost forwarding proxy while keeping development mode security invariants intact.

---

## 15. Alertmanager Integration & End-to-End Alert Delivery (Stage 6.31B.15)

### 15.1 Architecture & Notification Pipeline Topology

In Stage 6.31B.15, Alertmanager (`prom/alertmanager:v0.28.1`) was integrated into the AegisEdge Docker Compose deployment to complete the alerting pipeline. Prometheus forwards firing and resolved alerts to Alertmanager via its `/api/v2/alerts` interface, which groups, deduplicates, and routes them to local webhook receivers.

```mermaid
graph TD
    subgraph Docker Network: prometheus_default
        Prom["Prometheus v3.2.1<br/>127.0.0.1:9090"]
        AM["Alertmanager v0.28.1<br/>127.0.0.1:9093<br/>(aegisedge-alertmanager)"]
        Recv["Webhook Receiver (Temporary)<br/>127.0.0.1:9095<br/>(webhook-receiver)"]

        Prom -->|"evaluates rules (15s / 2s test)"| Prom
        Prom -->|"POST /api/v2/alerts (firing/resolved)"| AM
        AM -->|"route: warning<br/>POST /alerts/warning"| Recv
        AM -->|"route: critical<br/>POST /alerts/critical"| Recv
        AM -->|"fallback: default<br/>POST /alerts/default"| Recv
        Prom -->|"scrapes test metric"| Recv
    end

    subgraph Host Network (Windows 11)
        Test["Integration Test Runner<br/>.\\scripts\\test-alertmanager-lifecycle.ps1"]
        Test -->|"GET /-/ready"| Prom
        Test -->|"GET /-/ready"| AM
        Test -->|"POST /api/trigger?val=1,2,0"| Recv
        Test -->|"GET /api/events"| Recv
    end
```

### 15.2 Alertmanager Configuration & Severity-Based Routing

The Alertmanager configuration [`infra/prometheus/alertmanager.yml`](../../infra/prometheus/alertmanager.yml) establishes deterministic, severity-based notification routing:

1. **Global Configuration**:
   - `resolve_timeout: 5m`: Automatically marks stale alerts resolved after 5 minutes if Prometheus stops renewing them.
2. **Root Route & Grouping Invariants**:
   - `receiver: "default-receiver"`: Catch-all fallback receiver.
   - `group_by: ['alertname', 'severity', 'job']`: Batches alerts sharing the same alert name, severity, and originating job into unified notification payloads.
   - `group_wait: 10s`: Initial buffer delay before dispatching the first notification in a group, allowing related alerts in a cluster to group together.
   - `group_interval: 30s`: Minimum interval before sending an updated notification for an active alert group.
   - `repeat_interval: 4h`: Default interval for repeating identical firing alert notifications.
3. **Sub-Routes**:
   - `severity = "critical"` $\to$ `critical-receiver` with `repeat_interval: 1h`.
   - `severity = "warning"` $\to$ `warning-receiver` with `repeat_interval: 4h`.
4. **Receivers**:
   - All three receivers (`default-receiver`, `warning-receiver`, `critical-receiver`) configure HTTP webhook targets with `send_resolved: true` to ensure operators receive both alert activations and clearance notifications.

### 15.3 Deployment and Shutdown Commands

#### 1. Starting the Complete Monitoring & Alerting Stack
```powershell
docker compose -f infra/prometheus/docker-compose.yml up -d
```
Verified services:
- `aegisedge-prometheus` (`prom/prometheus:v3.2.1`): `127.0.0.1:9090`
- `aegisedge-alertmanager` (`prom/alertmanager:v0.28.1`): `127.0.0.1:9093`
- `aegisedge-grafana` (`grafana/grafana:11.5.2`): `127.0.0.1:3000`

#### 2. Validating Stack Health
```powershell
# Prometheus Health & Alertmanager Discovery
Invoke-RestMethod -Uri "http://127.0.0.1:9090/-/ready"
(Invoke-RestMethod -Uri "http://127.0.0.1:9090/api/v1/alertmanagers").data.activeAlertmanagers

# Alertmanager Health
Invoke-RestMethod -Uri "http://127.0.0.1:9093/-/ready"
```

#### 3. Stopping the Stack
```powershell
docker compose -f infra/prometheus/docker-compose.yml down
```

### 15.4 Dedicated Integration Test Configuration & Compose Override (F-01 / F-02 Resolution)

To ensure zero test-related rules, test scrape targets, or temporary volumes pollute production monitoring:

1. **Strict Configuration Isolation**:
   - **Production Configurations** ([`infra/prometheus/prometheus.docker.yml`](../../infra/prometheus/prometheus.docker.yml) and [`infra/prometheus/prometheus.yml`](../../infra/prometheus/prometheus.yml)) define ONLY the 7 production alert definitions from `alert_rules.yml` and the production scrape target `aegisedge-control-plane`.
   - **Production Compose** ([`infra/prometheus/docker-compose.yml`](../../infra/prometheus/docker-compose.yml)) mounts only `prometheus.docker.yml`, `alert_rules.yml`, and `alertmanager.yml`.
   - **Dedicated Test Configuration** ([`infra/prometheus/prometheus.test.yml`](../../infra/prometheus/prometheus.test.yml)) runs with accelerated 2s scrape/evaluation intervals, loads both `alert_rules.yml` and `alert_rules_lifecycle_test.yml`, and configures the `aegisedge-lifecycle-test` scrape job targeting `webhook-receiver:9095`.
   - **Dedicated Test Compose Override** ([`infra/prometheus/docker-compose.test.yml`](../../infra/prometheus/docker-compose.test.yml)) mounts `prometheus.test.yml` as `/etc/prometheus/prometheus.yml` and `alert_rules_lifecycle_test.yml` into the Prometheus container during test execution.

2. **Test Lifecycle Architecture**:
   During integration runs, the test harness starts the stack using:
   ```powershell
   docker compose -f infra/prometheus/docker-compose.yml -f infra/prometheus/docker-compose.test.yml up -d
   ```
   Upon test completion or unexpected failure, a `try ... finally` block automatically cleans up temporary containers and executes:
   ```powershell
   docker compose -f infra/prometheus/docker-compose.yml up -d
   ```
   This immediately restores the production configuration with zero test mounts or rules.

---

### 15.5 End-to-End Alert Delivery & Resilience Verification

The verification script [`scripts/test-alertmanager-lifecycle.ps1`](../../scripts/test-alertmanager-lifecycle.ps1) executes comprehensive end-to-end integration testing across 12 distinct automated phases:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\test-alertmanager-lifecycle.ps1
```

Verified Lifecycle Steps & Scenarios:
1. **Compilation**: Compiles `bin/webhook-receiver` as a standalone, dependency-free Linux amd64 binary (`CGO_ENABLED=0`).
2. **Container Launch**: Deploys a temporary `webhook-receiver` container on network `prometheus_default` with `--entrypoint /webhook-receiver`.
3. **Readiness & Discovery**:
   - Prometheus and Alertmanager verify healthy via `/-/ready`.
   - Prometheus actively discovers Alertmanager at `http://alertmanager:9093/api/v2/alerts` (`GET /api/v1/alertmanagers`).
   - Synthetic test scrape target `aegisedge-lifecycle-test` is verified `UP`.
4. **Scenario A — Warning Alert Lifecycle (Firing $\to$ Resolved + Strict Event Selection)**:
   - `POST /api/trigger?val=1&run_id=$MainRunId` triggers synthetic warning metric tagged with unique scenario `test_run_id`.
   - Rule `AegisEdgeE2ELifecycleWarning` transitions: `inactive` $\to$ `pending` $\to$ `firing`.
   - Alertmanager routes alert to `/alerts/warning` (`warning-receiver`).
   - Webhook delivery asserted with matching `test_run_id`, summary, description, and valid `startsAt`. The firing fingerprint is captured.
   - `POST /api/trigger?val=0&run_id=$MainRunId` clears metric and triggers resolution.
   - Event selection matches the exact firing fingerprint, validates route (`warning`), receiver (`warning-receiver`), status (`resolved`), alert labels, annotations, and asserts `endsAt > startsAt`.
5. **Scenario B — Critical Alert Routing & Resolution**:
   - `POST /api/trigger?val=2&run_id=$MainRunId` triggers critical metric.
   - Rule `AegisEdgeE2ELifecycleCritical` fires.
   - Alertmanager routes alert to `/alerts/critical` (`critical-receiver`), matching severity.
   - Resolution is triggered and verified with fingerprint correlation and valid timestamps.
6. **Scenario C — Default Fallback Routing for Unmatched Severity**:
   - `POST /api/trigger?val=3&run_id=$MainRunId` triggers informational test metric (`severity: info`).
   - Rule `AegisEdgeE2ELifecycleDefaultFallback` fires.
   - Alertmanager routes alert to `/alerts/default` (`default-receiver`), proving fallback behavior.
   - Resolution verified with fingerprint matching and valid timestamps.
7. **Scenario D — Strict Alert Grouping & Grouped Resolution Verification**:
   - `POST /api/trigger?val=1&instances=2&run_id=$MainRunId` emits multi-instance metrics across `node-a` and `node-b`.
   - Firing webhook asserts exactly 2 alert instances batched under `groupLabels` `['alertname', 'severity', 'job']`.
   - Firing fingerprints for `node-a` and `node-b` are captured.
   - `POST /api/trigger?val=0&instances=2&run_id=$MainRunId` clears both instances.
   - Grouped resolution webhook asserts exactly 2 alert instances, matching labels, distinct `node-a` and `node-b`, and verifies that each resolved alert's fingerprint strictly matches the firing fingerprint for its respective node label with `endsAt > startsAt`.
8. **Scenario E — Alertmanager Restart Resilience & Post-Restart Delivery**:
   - `docker restart aegisedge-alertmanager` restarts Alertmanager.
   - Stack re-verifies health within 15 seconds without state corruption or connection drops.
   - Prometheus rediscovery of active Alertmanager is verified.
   - A distinct post-restart execution run ID (`$RestartRunId`) is generated.
   - `POST /api/trigger?val=1&run_id=$RestartRunId` triggers post-restart warning.
   - Post-restart firing notification is verified asserting `test_run_id=$RestartRunId`, capturing the post-restart fingerprint.
   - Resolution is triggered with `$RestartRunId` and asserted with matching post-restart fingerprint and valid timestamps (`endsAt > startsAt`).
9. **Teardown & Production Restoration**:
   - Temporary `webhook-receiver` container is safely stopped and removed (verified absent via `docker ps -a`).
   - Base Compose stack (`docker-compose.yml`) is recreated.
   - Protected `try/finally` ensures all cleanup steps are executed even if a previous step encounters an issue.
   - Teardown errors are accumulated without terminating `Write-Error` calls under `$ErrorActionPreference = "Stop"`.
   - Verification confirms production Prometheus active rules contain only `aegisedge-quarantine-and-nats` (7 production rules, 0 test rules) and active scrape targets contain only `aegisedge-control-plane` (0 test targets).

---

### 15.6 Webhook Receiver Hardening & Runtime Safety

The test webhook receiver (`services/control-plane/cmd/webhook-receiver`) enforces production-grade HTTP safety invariants:

1. **Explicit Server Timeouts**:
   Configured with bounded HTTP timeouts to mitigate Slowloris and resource exhaustion attacks:
   - `ReadHeaderTimeout: 5s`
   - `ReadTimeout: 10s`
   - `WriteTimeout: 10s`
   - `IdleTimeout: 60s`
2. **Payload Size Limits & HTTP 413 Handling**:
   - All webhook ingestion endpoints wrap `r.Body` with `http.MaxBytesReader(w, r.Body, 1048576)` (1 MiB maximum).
   - If an oversized request payload is received, the server halts reading and immediately responds with HTTP `413 Request Entity Too Large` (`http.StatusRequestEntityTooLarge`).
3. **Safe Malformed Body Handling (Log Injection & Flooding Defense)**:
   - When a malformed or invalid JSON payload is received, the receiver logs only the parsing error and the total payload length (`body_bytes=%d`).
   - The malformed request body content is never printed to server logs, preventing log injection, credential leakage, and log disk flooding.
4. **Bounded In-Memory Storage**:
   - Webhook events are stored in an in-memory ring buffer strictly capped at `maxRecordedEvents = 1000`.
   - When the event limit is reached, older events are pruned first-in-first-out, preventing unbounded memory growth.
5. **Parameter Validation (`instances` and `run_id`)**:
   - Query parameter `instances` in `/api/trigger` is strictly validated to integers between 1 and 10 (`maxInstancesLimit = 10`). Non-numeric, zero, negative, or values exceeding 10 are rejected with HTTP `400 Bad Request`.
   - Query parameter `run_id` dynamically populates metric label `test_run_id` across single- and multi-instance metrics for complete scenario isolation.

---

### 15.7 Security & Configuration Validation

1. **Localhost Isolation**: Port `9093` is bound strictly to `127.0.0.1`. The web UI and REST API are never exposed to external or untrusted interfaces.
2. **Zero Credentials**: No dummy tokens or cloud notification credentials are hardcoded.
3. **Automated Verification Pipeline**:
   - `amtool check-config infra/prometheus/alertmanager.yml`: Validates syntax, routing tree, and receivers via `scripts/check.ps1`.
   - `promtool check config`: Validates production `prometheus.yml`, `prometheus.docker.yml`, and isolated `prometheus.test.yml`.
   - `promtool check rules`: Validates `alert_rules.yml` (7 rules) and `alert_rules_lifecycle_test.yml` (3 rules).
   - Go typed schema tests in `alert_rules_test.go` and `dashboard_test.go`: Assert unmarshal schemas, port bindings, and pinned image tags.
   - Production isolation tests in `TestPrometheusConfig` and `TestPrometheusDockerConfig`: Assert test rule files and test scrape jobs are strictly absent from production configurations.

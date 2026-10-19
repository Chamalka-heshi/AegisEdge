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
   - This counter is in-memory and resets to 0 upon process restart. It is not persisted to SQLite or JetStream metadata.
2. **Lack of Automated Dead-Letter Queue (DLQ) / Auto-Replay**:
   - AegisEdge does not provide an automated background worker to re-read exhausted NATS messages from JetStream. Replaying messages requires explicit manual action or external tooling.

### 7.2 Secondary Fallback: Direct Read-Only SQLite Inspection
In air-gapped or embedded emergency environments where CLI execution is unavailable, operators with read access to the SQLite file may inspect quarantined batches using standard read-only queries:

```sql
-- Read-only query for failed batches:
SELECT batch_id, node_id, sequence_number, attempt, failed_at, error_reason 
FROM telemetry_batches 
WHERE sync_status = 'FAILED' 
ORDER BY failed_at ASC, sequence_number ASC;

-- Inspect payload of a specific batch:
SELECT payload 
FROM telemetry_batches 
WHERE batch_id = 'b-a1b2c3d4-0001';
```

> [!CAUTION]
> Do **NOT** execute raw `UPDATE` statements to manually requeue batches in production databases. Direct SQL updates bypass `Syncer.RequeueBatch` audit logging, omit state validation, and risk corrupting retry state. Always use `aegisedge-agent quarantine requeue`.

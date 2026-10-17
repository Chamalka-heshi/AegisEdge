# Local NATS & JetStream Development Environment

This document describes how to start, inspect, validate, and manage the local NATS and JetStream environment for AegisEdge Phase 4 development.

---

## 1. Architectural Overview & Boundaries

In accordance with [ADR-0006 (NATS Event-Driven Messaging)](../decisions/ADR-0006-nats-event-driven-messaging.md):
* **SQLite WAL remains the edge durability boundary**: The edge node commits telemetry locally first before publishing to NATS.
* **NATS is the asynchronous event transport**: Decouples edge nodes from control-plane hostnames.
* **JetStream provides durable streaming & replay**: Preserves published messages across consumer restarts and supports at-least-once delivery with `Nats-Msg-Id` deduplication.
* **Phase 4.2 scope**: Infrastructure and broker environment preparation only. Application publishers, consumers, and Go client dependencies are implemented in subsequent phases.

---

## 2. Configuration & Pinned Versions

* **NATS Server Version**: Pinned to **`2.10.26`** (`nats:2.10.26-alpine` in Docker).
* **Ports Exposed (Localhost Only)**:
  * `127.0.0.1:4222`: Standard NATS client TCP protocol.
  * `127.0.0.1:8222`: HTTP monitoring, metrics, and healthcheck.
* **JetStream Storage Volume**:
  * Docker: Named volume `aegisedge_nats_data` mounted to container path `/data`.
  * Store directory: `/data/jetstream`.
  * Resource limits: `max_mem: 512MB`, `max_file: 2GB`.

---

## 3. Telemetry Stream Specification

The AegisEdge telemetry ingestion stream is defined in [`infra/nats/streams/telemetry-stream.json`](../../infra/nats/streams/telemetry-stream.json):

| Property | Value | Architectural Justification |
| :--- | :--- | :--- |
| **Stream Name** | `AEGISEDGE_TELEMETRY` | Root stream for edge telemetry batches. |
| **Subjects** | `aegisedge.v1.telemetry.*` | Covers all edge node telemetry streams (e.g. `aegisedge.v1.telemetry.edge-node-01`). |
| **Storage** | `file` | Persisted to disk in `/data/jetstream` to survive broker restarts. |
| **Retention** | `limits` | Bounded time-series retention; discards oldest when limits reached. |
| **Max Age** | `7d` (168 hours) | Prevents unbounded disk accumulation during local testing. |
| **Max Bytes** | `512MB` | Local development storage quota. |
| **Max Msg Size** | `1MB` | Enforces the 1MB payload ceiling specified in ADR-0005 and ADR-0006. |
| **Discard Policy** | `old` | Preserves newest telemetry by evicting oldest expired records. |
| **Duplicate Window**| `24h` | NATS JetStream deduplication window matching `Nats-Msg-Id: <batch_id>`. |
| **Replicas** | `1` | Single-node broker for local development. |

---

## 4. Running NATS via Docker Compose (Recommended)

When Docker Desktop (with WSL2 backend or hardware virtualization) is available:

### Start NATS
```powershell
docker compose -f infra/nats/docker-compose.yml up -d
```

### Inspect Container & Health
```powershell
docker compose -f infra/nats/docker-compose.yml ps
```

### Inspect Server Logs
```powershell
docker compose -f infra/nats/docker-compose.yml logs -f
```

### Stop NATS (Preserving Storage Volume)
```powershell
docker compose -f infra/nats/docker-compose.yml down
```

### Stop NATS & Delete Storage Volume
```powershell
docker compose -f infra/nats/docker-compose.yml down -v
```

---

## 5. Running NATS via Standalone Binary (Alternative)

If Docker or hardware virtualization is not available on the host machine:

1. Download the official NATS Server v2.10.26 binary for Windows:
   * Source: `https://github.com/nats-io/nats-server/releases/tag/v2.10.26`
   * Binary: `nats-server.exe`
2. Start the server using the repository configuration:
   ```powershell
   .\nats-server.exe -c infra/nats/nats-server.conf
   ```
3. To stop the server, press `Ctrl+C`.

---

## 6. Monitoring & Healthcheck Endpoints

NATS exposes an embedded HTTP monitoring server on port `8222`:

* **Healthcheck**: `GET http://127.0.0.1:8222/healthz`
  * Returns: `{"status": "ok"}`
* **Server Details**: `GET http://127.0.0.1:8222/varz`
  * Returns: server version, uptime, memory, connections, and CPU stats.
* **JetStream Details**: `GET http://127.0.0.1:8222/jsz`
  * Returns: JetStream status, storage paths, active streams, and consumers.
* **Connection Details**: `GET http://127.0.0.1:8222/connz`

---

## 7. Running the Validation Script

Verify the health, ports, and JetStream status of the running server:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\check-nats.ps1
```

---

## 8. Local Development vs. Production Durability

| Aspect | Local Development Environment | Production AegisEdge Architecture |
| :--- | :--- | :--- |
| **Topology** | Single standalone broker node | 3-node or 5-node NATS cluster with Raft consensus |
| **Storage** | Single named Docker volume / local directory | Distributed SSDs with filesystem replication |
| **Stream Replication** | `num_replicas: 1` | `num_replicas: 3` (replicated across failure domains) |
| **Security** | Plaintext TCP, no authentication, localhost bound | TLS 1.3 mutual authentication, decentralized NKey/JWT |
| **Durability Boundary**| SQLite WAL on edge + local Docker volume | SQLite WAL on edge + clustered JetStream persistence |

---

## 9. Security Note

* This environment is configured strictly for **local development and testing**.
* Plaintext TCP and unauthenticated connections are used solely on `127.0.0.1`.
* Do NOT expose port 4222 or 8222 to untrusted network interfaces without configuring TLS and NKey authentication.

---

## 10. Consumer Poison-Message Containment & Reliability (Phase 6.26A)

The control-plane telemetry consumer implements strict error classification, bounded retry backoff, and poison-message containment to uphold the core invariant:
$$\text{EDGE OPERATES OFFLINE} \longrightarrow \text{PERSISTS TO WAL} \longrightarrow \text{RECONNECTS} \longrightarrow \text{SYNCHRONIZES IDEMPOTENTLY}$$

### Failure Classification & Handling

| Category | Triggers / Errors | Consumer Action | Rationale |
| :--- | :--- | :--- | :--- |
| **Permanent / Poison** | Malformed JSON envelope, envelope schema validation failure, invalid batch domain payload, or storage `ErrInvalidMeta` | `msg.TermWithReason(reason)` (or `msg.Term()`) | Message content is structurally unprocessable. Immediate termination prevents poison-message redelivery loops. |
| **Duplicate Delivery** | Composite key collision (`storage.ErrDuplicateBatch`) | `msg.Ack()` | Edge sync retries are idempotent; duplicates are acknowledged to drain them from the queue. |
| **Transient Error** | Database busy/locked, I/O timeout, network disruption | `msg.NakWithDelay(backoff)` | Bounded exponential backoff gives downstream storage time to recover. |
| **Delivery Exhaustion** | Delivery count reaches `MaxDeliver` on transient failure | Structured `CRITICAL` log + `msg.Nak()` (NOT `Term()`) | Prevents silent data loss for valid telemetry during outages; leaves message preserved in stream. |
| **Graceful Shutdown** | Consumer context canceled (`c.ctx.Err() != nil`) | No ACK / No NAK / No Term | Leaves in-flight message available for next process instance. |

### Consumer Configuration Defaults

The consumer uses explicit durability tuning parameters (configurable via `ConsumerConfig`):
* `MaxDeliver: 5` (finite delivery attempts before halting redelivery)
* `AckWait: 30s` (server acknowledgement timeout window)
* `MaxAckPending: 1000` (concurrency limit)
* `RetryBackoff: 100ms` (base delay for transient retries, capped at 10s with jitter)

### Stream Retention, Undelivered Messages & Replay Limitations

> [!IMPORTANT]
> **Stream retention is not an automated Dead-Letter Queue (DLQ), and replay is not implemented.**
>
> 1. **Messages no longer delivered to the durable consumer**:
>    * **Permanently Poison Messages**: When a message has malformed JSON, invalid envelope schema, invalid batch domain payload, or storage `ErrInvalidMeta`, the consumer calls `TermWithReason` (or `Term`). JetStream permanently removes the message from the consumer's delivery schedule. It will **never** be redelivered to this consumer group.
>    * **Transiently Exhausted Messages**: When a message fails due to transient storage/network errors and exceeds `MaxDeliver` (default: 5), the consumer issues a final `Nak()`. JetStream halts redelivery to this consumer and emits a `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AEGISEDGE_TELEMETRY.<consumer>` advisory event.
>    * **Stream Storage Preservation**: In both cases, the message physically remains in the `AEGISEDGE_TELEMETRY` stream on disk for up to 7 days (`max_age: 7d`, `max_bytes: 512MB`), ensuring raw records are not lost from the broker during outages.
>
> 2. **Inspecting vs. Replaying Messages**:
>    * **Inspection**: Operators can inspect stream contents and read specific messages using the NATS CLI without affecting consumer state.
>    * **Replay / Redrive Limitation**: **AegisEdge does not currently implement or verify automated message replay, redrive, or dead-letter queue extraction.** Reading a message with `nats msg get` is strictly a read-only operation and does **not** redeliver or replay the message to the consumer. Replaying an exhausted message requires manual operator re-publication or custom tooling outside the current control-plane scope.

### Operator Diagnostic Commands

When investigating poison messages, transient delivery exhaustion, or advisory alerts, operators can use the standard NATS CLI for diagnostic inspection:

```powershell
# 1. View overall consumer progress, ack state, and pending redeliveries
nats consumer report AEGISEDGE_TELEMETRY

# 2. Inspect detailed consumer configuration, redelivery counts, and stream state
nats consumer info AEGISEDGE_TELEMETRY aegisedge-control-plane-telemetry

# 3. Interactively page through stored messages and metadata in the stream (read-only)
nats stream view AEGISEDGE_TELEMETRY

# 4. Read a specific raw message payload and headers by stream sequence number (read-only; does not replay)
nats msg get AEGISEDGE_TELEMETRY <sequence_number>

# 5. Monitor real-time broker advisory events, including MaxDeliver exhaustion alerts
nats event --advisory
```

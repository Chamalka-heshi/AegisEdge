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

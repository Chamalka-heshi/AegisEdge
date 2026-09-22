# ADR-0007: NATS JetStream Application Integration

**Status**: Accepted  
**Date**: 2026-09-22  
**Deciders**: AegisEdge engineering team  
**Supersedes**: None  
**Relates to**: ADR-0006 (NATS/JetStream Architecture & Design)

## Context

Phase 4.2 provisioned the `AEGISEDGE_TELEMETRY` JetStream stream.
Phase 4.3 implements the first application-level integration:

    SQLite-persisted telemetry → NATS JetStream → Consumer → Idempotent Processing → ACK

This ADR documents the design decisions for this integration layer.

## Durability Boundaries

The system has **distinct** durability boundaries that must not be conflated:

| Boundary | Scope | Guarantee |
|----------|-------|-----------|
| **SQLite WAL** | Edge agent local storage | Durable persistence according to configured SQLite pragmas. Subject to local storage capacity. |
| **NATS JetStream** | Transport-level durable streaming | At-least-once delivery with file-backed persistence. Subject to stream retention limits. |
| **Consumer Processing** | Control-plane ingestion | In-memory idempotency boundary. Duplicate detection is NOT persistent across process restarts. |
| **JetStream ACK** | Delivery acknowledgement | Explicit ACK only after successful consumer processing. |

## Key Design Decisions

### 1. Persist Locally First, Publish Second

The core invariant from ADR-0006 is preserved:

```
SQLite persistence FIRST → NATS publication SECOND
```

The NATS publisher receives an **already-persisted** `TelemetryBatch`.
A publish failure does not result in data loss because the batch remains in SQLite with status `PENDING`.

### 2. Event Identity: EventID == BatchID

Every `TelemetryEvent` envelope uses the originating `BatchID` as its `EventID`.
On retry, the **exact same** `EventID` is reused — no new event ID is generated.

This enables:
- **Broker-level deduplication** via the `Nats-Msg-Id` header (JetStream dedup window).
- **Consumer-level idempotency** via the `EventID` lookup in the ingestion map.

### 3. At-Least-Once Delivery with Idempotent Processing

AegisEdge does **not** claim exactly-once delivery.

The system uses at-least-once delivery with idempotent processing because retries, reconnects, and failures can cause duplicate delivery.

Duplicate deliveries are handled at two levels:
1. **JetStream dedup window** (24 hours) — prevents duplicate stream storage via `Nats-Msg-Id`.
2. **Consumer in-memory idempotency** — prevents duplicate logical ingestion within the current process lifetime.

**Limitation**: Persistent duplicate detection across control-plane restarts is NOT provided by the current in-memory ingestion boundary. This is deferred to a future persistent ingestion layer.

### 4. Sync Status Semantics

| Status | Meaning | Set When |
|--------|---------|----------|
| `PENDING` | Not yet delivered to any upstream | Initial state after SQLite persistence |
| `SYNCING` | Reserved for future use | — |
| `SYNCED` | Delivered via HTTP transport | HTTP 2xx response from control-plane |
| `PUBLISHED` | Delivered via NATS JetStream | JetStream PubAck received |
| `FAILED` | Reserved for future use | — |

`PUBLISHED` requires a **confirmed JetStream PubAck**, not merely a successful NATS client publish call.

### 5. Transport Selection (No Automatic Fallback)

Transport is selected by configuration, not by runtime fallback:

| `NATS_ENABLED` | Transport Used |
|-----------------|---------------|
| `false` (default) | HTTP (existing Phase 3 `HTTPClient`) |
| `true` | NATS JetStream (`NATSPublisher`) |

The same batch is **never** sent through both transports automatically.
There is no automatic HTTP fallback when NATS is unavailable.

### 6. Consumer ACK Semantics

| Scenario | Action |
|----------|--------|
| Valid event, successful processing | ACK |
| Duplicate event (already ingested) | ACK (idempotent, no duplicate logical insertion) |
| Processing failure | NAK (allows JetStream redelivery) |
| Invalid envelope or payload | NAK |

ACK is issued **only after** successful ingestion.

### 7. SQLite Migration v2

An additive schema migration adds the `published_at` column:

```sql
ALTER TABLE telemetry_batches ADD COLUMN published_at TEXT;
```

This is a non-destructive, forward-compatible change.

## Consequences

### Positive

- First real NATS JetStream integration validates the ADR-0006 architecture.
- Clear separation between SQLite durability, NATS transport, and consumer processing.
- Phase 3 HTTP transport remains fully operational and unmodified.
- All 56+ existing tests continue to pass.

### Negative / Limitations

- Consumer idempotency is in-memory only — restarts lose duplicate detection state.
- No automatic transport fallback — operational simplicity at the cost of resilience.
- Single-node NATS — not clustered in this phase.

### Risks

- JetStream dedup window (24h) may be insufficient for very long outages.
  Mitigation: Consumer-level idempotency provides a second dedup layer.
- In-memory ingestion map grows unbounded during long-running processes.
  Mitigation: Acceptable for Phase 4.3; persistent layer planned for future phases.

## Not Implemented (Deferred)

The following are explicitly **not** part of Phase 4.3:

- Persistent duplicate detection across restarts
- NATS authentication / TLS
- NATS clustering / multi-node
- Automatic HTTP fallback when NATS fails
- Dead-letter queue for permanently invalid messages
- Kubernetes / Docker deployment
- Dashboard / monitoring integration

# ADR-0003: Domain Event and Incident Contracts

## Status
Accepted

## Context
In a distributed edge-cloud system like AegisEdge, telemetry data and incident events originate on edge nodes that periodically disconnect from the central control plane. When designing the shared domain models (`shared/types`), several distributed systems challenges emerge:
1. When connectivity drops, edge devices must continue recording events independently without a central ID-generation service (e.g., database auto-incrementing IDs or centralized Snowflake counters).
2. Network timeouts cause upstream sync workers to retry dispatches, risking duplicate processing and phantom incident duplication.
3. Edge clocks frequently drift or lack real-time NTP sync, making wall-clock timestamps unreliable for establishing strict global causal ordering.
4. Autonomous mitigation actions must not introduce arbitrary remote code execution (RCE) or uncontrollable shell vulnerabilities.

## Decisions

### 1. Edge-Generated Stable Identifiers (UUIDv4)
* All primary domain entities (`BatchID`, `IncidentID`, `ActionID`) are assigned globally unique identifiers (UUIDv4) at the moment of creation on the edge node.
* **No Central ID Service**: Relying on a central coordination service or database sequence would require edge nodes to be online to create records, violating the edge-first design requirement.
* **Stability Across Retries**: Once generated, an entity's ID is immutable. If a telemetry batch or incident report times out and is retried 5 times over an intermittent cellular uplink, the payload retains its original `BatchID` / `IncidentID`.

### 2. Idempotent Ingestion via Stable IDs
* The central control plane uses the immutable, edge-generated IDs as idempotency keys (e.g., relational primary keys or upsert constraints).
* Duplicate network deliveries resulting from network retries or connection drops after server-side processing are safely treated as no-ops (`ON CONFLICT (id) DO NOTHING`). Duplicate delivery will never spawn duplicate logical incidents, duplicate metrics, or redundant operator notifications.

### 3. Monotonic Sequence Numbers for Per-Node Causality
* Each edge node maintains a strictly increasing, 64-bit integer sequence counter (`SequenceNumber`) for heartbeats and telemetry batches.
* **Why Not Timestamps Alone?**: Physical clocks suffer from clock skew, drift, daylight saving adjustments, and NTP backwards steps. Two events with identical or reversed timestamps can occur on edge devices.
* **Sequence Numbers Provide Causal Ordering**: By pairing `(NodeID, SequenceNumber)`, the control plane can:
  - Deterministically order incoming batches from a specific node.
  - Detect missing batches (gaps in sequence) caused by dropped packets or disk queue eviction.
  - Reject stale or out-of-order replayed packets.

### 4. Dual-Timestamp Semantic Model
* We explicitly avoid treating edge timestamps as a source of global truth.
* Each event captures:
  - `collected_at` / `triggered_at`: The edge node's local UTC time when the event occurred, preserving relative local intervals.
  - `ingested_at`: Appended by the control plane upon receipt to measure network delay and pipeline lag.
* Analytical and aggregation queries acknowledge both timestamps to account for clock drift.

### 5. Explicit Allowlisted Mitigations vs. Arbitrary Commands
* `MitigationAction` explicitly restricts `ActionType` to a strictly validated, allowlisted enum (`SIMULATED_THROTTLE`, `SIMULATED_RESTART`, `SIMULATED_ISOLATE`, `SIMULATED_ALERT`).
* **Why Commands are Prohibited**: Permitting free-form shell commands (e.g., `cmd: "systemctl restart foo"`) introduces catastrophic security risks (command injection, accidental privilege escalation) and makes formal verification impossible.
* In Phase 1, all mitigations are safely simulated by the actuator to prevent unintended workstation or host mutations during development and testing.

## Consequences
* **Positive**: The edge operates completely autonomously when offline; retries are safely idempotent; ordering is robust against clock drift; and security is enforced at the domain contract boundary.
* **Negative**: Control plane storage requires index lookups on UUIDs for deduplication, and the edge must persist monotonic sequence state locally. These are necessary and acceptable trade-offs for distributed reliability.

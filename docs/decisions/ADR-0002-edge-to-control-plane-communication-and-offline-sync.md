# ADR-0002: Edge-to-Control-Plane Communication and Offline Synchronization

## Status
Accepted

## Context
In an edge computing environment, network partitions, WAN latency spikes, intermittent cell/satellite uplinks, and central control plane outages are **normal operating conditions, not rare exceptions**.

If an edge agent relies on synchronous cloud roundtrips or in-memory queues to report telemetry or trigger incident response:
1. Network outages cause unbounded in-memory queue growth, eventually crashing the edge process via OOM (Out Of Memory).
2. Critical local anomalies cannot be mitigated if the edge waits for cloud authorization.
3. Network blips cause dropped data or duplicate event processing on reconnect.

Therefore, the edge-to-control-plane communication layer must be explicitly designed for partition tolerance and at-least-once synchronization semantics.

## Decisions

### 1. Edge-Local Persistence Before Synchronization
All telemetry samples and incident events are committed to local SQLite storage *before* any network dispatch is attempted.
* The telemetry ingestion pipeline writes to the local database transactionally.
* An independent background synchronization worker reads un-synced records from the local store and attempts upstream transmission.
* Decoupling collection from synchronization ensures that a stalled network connection never blocks metric collection or local incident evaluation.

### 2. Autonomous Operation During Disconnection
The edge agent is fully autonomous:
* Local rule evaluation and anomaly detection run continuously regardless of network state.
* Local autonomous mitigations execute locally and log audit trails to local storage.
* The edge does not block waiting for control plane confirmation before mitigating a critical condition (e.g. thermal or memory emergencies).

### 3. Synchronization Retries with Truncated Exponential Backoff and Jitter
When the sync worker attempts to push batches to the control plane and encounters a network timeout, connection refusal, or 5xx server response:
* It enters a backoff state: $\text{interval} = \min(\text{base} \times 2^{\text{retry}} + \text{jitter}, \text{max\_interval})$.
* Jitter prevents the "thundering herd" problem where hundreds of reconnected edge devices hit the control plane simultaneously.
* Unsent records remain safely stored in SQLite until upstream acknowledgment (HTTP 200/201) is received.

### 4. Idempotency and Deduplication
Because networks can fail *after* the control plane processes a request but *before* the edge receives the HTTP 200 response, retries will inevitably produce duplicate deliveries.
* **Stable Unique Identifiers**: Every incident, mitigation action, and telemetry batch is assigned a stable unique identifier (UUID v4) at the moment of generation on the edge.
* **Idempotent Ingestion**: The control plane uses these unique IDs to deduplicate incoming events (e.g., `INSERT ... ON CONFLICT (id) DO NOTHING`). Duplicate delivery will never create duplicate logical incidents or repeated alerts.

### 5. Sequence Tracking and Telemetry Batches
* Each edge node maintains a monotonically increasing batch sequence number per stream.
* The control plane can identify missing intervals or out-of-order deliveries caused by multi-path routing or retry reordering.

### 6. Clock Skew and Timestamp Integrity
Edge hardware clocks frequently drift, reset to epoch upon power cycles, or suffer NTP synchronization delays.
* **Edge Timestamps**: Telemetry and incident records capture the edge's local monotonic timestamp and UTC wall-clock time (`collected_at`).
* **Ingestion Timestamps**: The control plane appends its own synchronized UTC timestamp (`ingested_at`) upon receiving the payload.
* Analytical queries rely on both timestamps to evaluate latency, clock drift, and true event ordering.

### 7. Realistic Storage Limits and Bounded Buffering
We do not claim mathematically guaranteed zero data loss under indefinite disconnection.
* Physical edge storage is finite. A prolonged network partition could fill the edge disk.
* The edge SQLite buffer implements configurable retention policies (e.g., maximum buffer size in megabytes or maximum retention days) using a FIFO eviction strategy for low-priority telemetry, while prioritizing critical incident audit logs.

## Consequences
* **Positive**: The edge agent can survive arbitrary network outages, continue protecting local workloads, and catch up with the control plane upon reconnection without data corruption.
* **Negative**: Requires additional local disk I/O and deduplication logic on the control plane, which is an intentional and necessary engineering trade-off for partition-tolerant systems.

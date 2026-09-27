# ADR-0010: Edge Local Incident Engine Architecture & Correlation Policy

**Status**: Accepted (Phase 5.4 Implementation)  
**Date**: 2026-09-27  
**Deciders**: AegisEdge Core Engineering Team  
**Supersedes**: None  
**Relates to**: ADR-0003 (Domain Event & Incident Contracts), ADR-0004 (Edge-Local Persistence & WAL Durability), ADR-0008 (NATS Resilience & Redelivery), ADR-0009 (Edge Anomaly Detection Architecture)

---

## 1. Context & Operational Motivation

Through Phases 1 through 5.3, AegisEdge established an offline-first telemetry collection pipeline governed by the core invariant:
$$\textbf{PERSIST FIRST. DETECT SECOND.}$$

Telemetry samples are captured, validated, and durably committed to SQLite WAL storage before anomaly detection evaluates deviations. In Phase 5.2 and 5.3, the deterministic `ThresholdDetector` was integrated to emit structured `types.AnomalySignal` objects upon detecting metric deviations.

However, an anomaly observation is not an incident:
$$\textbf{AnomalySignal} \neq \textbf{Incident}$$

Raw anomaly observations are noisy, discrete, and high-frequency. A transient spike (such as brief CPU contention during log compaction or process startup) must not instantly trigger macro operational incident management, operator paging, or downstream remediation. Conversely, persistent or multi-observation breaches must be correlated into a coherent, single incident lifecycle with deterministic state tracking.

ADR-0010 defines the architecture, correlation policy, lifecycle FSM integration, and reliability boundaries of the **Local Incident Engine** (`edge/agent/incident`).

---

## 2. Core Architectural Invariants

### Invariant 1: The Multi-Stage Edge Intelligence Pipeline
The edge intelligence pipeline flows through strictly decoupled, sequential stages:
$$\text{Telemetry Collection} \longrightarrow \text{SQLite WAL Persistence} \longrightarrow \text{Anomaly Detection} \longrightarrow \text{Incident Engine} \longrightarrow \text{Incident FSM} \longrightarrow (\text{Future Autonomous Remediation})$$

1. **Telemetry Collection**: Periodic metric gathering (`types.MetricSample`).
2. **SQLite WAL Persistence**: Immutable durability boundary (`storage.Store.PersistBatch`).
3. **Anomaly Detection**: Mathematical observation of threshold or statistical breaches (`types.AnomalySignal`).
4. **Incident Engine**: State-managed correlation and qualification (`incident.IncidentEngine.Process`).
5. **Incident FSM**: Domain state lifecycle (`types.IncidentStatus`).
6. **Autonomous Remediation**: Downstream allowlisted actuation (**Deferred to future phase; strictly prohibited in Phase 5.4**).

### Invariant 2: Complete Decoupling of Correlation from Remediation
$$\textbf{CORRELATION } \neq \textbf{ REMEDIATION}$$
The Incident Engine is solely responsible for incident correlation, classification, and lifecycle management. It does NOT execute shell commands, terminate processes, modify firewall rules, restart system services, or interact with external cloud/Kubernetes APIs.

---

## 3. Key Architectural Decisions

### 3.1 Domain Distinction: AnomalySignal vs. Incident
* **AnomalySignal**: Identifies a single mathematical or rule-based observation at an exact instant in time (`AnomalyID`, observation-level). Emitted by detectors.
* **Incident**: Identifies a qualified, operational condition requiring formal state management across time (`IncidentID`, lifecycle-level). Managed by the Incident Engine.

A single anomaly observation does not automatically open an incident; multiple anomaly observations over an extended period may belong to a single ongoing incident.

### 3.2 Correlation Policy: Configurable $M$-of-$N$ Sliding Window
To prevent false-positive incident flapping from transient anomalies, the Incident Engine implements an explicit, deterministic $M$-of-$N$ sliding window policy:
* **$M$ (Threshold)**: Minimum qualifying anomaly observations required to transition into an incident (default: $M = 2$).
* **$N$ (Window Size)**: Maximum number of chronological observations tracked per stream (default: $N = 3$).
* **Temporal Horizon (`WindowDuration`)**: Observations older than the sliding window horizon (default: 5 minutes) relative to the latest signal are pruned.

An incident is created if and only if at least $M$ valid anomaly observations exist within the active sliding window.

### 3.3 Correlation Key & Strict Stream Isolation
Correlation state is strictly partitioned using a composite stream key:
$$\text{Key} = \text{NodeID} + \text{":"} + \text{MetricName}$$

* **Per-Node Isolation**: `Node-A:cpu_usage_percent` and `Node-B:cpu_usage_percent` maintain completely distinct counters and window queues.
* **Per-Metric Isolation**: `Node-A:cpu_usage_percent` and `Node-A:memory_usage_percent` maintain completely distinct counters and window queues.
* No global or cross-metric correlation is performed at the edge layer.

### 3.4 Incident Identity & Lifecycle Stability
* **Stable IncidentID**: When $M$-of-$N$ conditions are satisfied, a new `types.Incident` is generated with an immutable `IncidentID` (UUIDv4).
* **Persistent Breach Correlation**: When additional anomaly observations occur while an incident remains in an active state (`ANOMALY_DETECTED`, `MITIGATING`, `ESCALATED`), the engine updates the existing incident (`UpdatedAt`, `TriggerValue`, evidence) and returns the **same `IncidentID`**. It never spawns duplicate incident entities for an ongoing breach.
* **Post-Recovery Re-breach**: If an incident is recovered/closed, subsequent breaches begin a fresh evaluation lifecycle and receive a new `IncidentID`.

### 3.5 Idempotent Ingestion via AnomalyID
To guard against duplicated signal delivery or replayed evaluation steps:
* Each correlation stream tracks seen `AnomalyID`s within a bounded cache.
* If an `AnomalySignal` with an already-seen `AnomalyID` is processed a second time, the engine treats it as a duplicate no-op.
* Duplicate deliveries **never increment the $M$-of-$N$ observation counter** and never create duplicate incidents.

### 3.6 Out-of-Order & Temporal Window Policy
* The engine evaluates temporal sequence using the immutable `DetectedAt` timestamp of the `AnomalySignal`.
* Signals arriving with timestamps within the sliding temporal window `[Latest - WindowDuration, Latest]` are inserted chronologically into the observation queue.
* Signals arriving with timestamps strictly older than `Latest - WindowDuration` are rejected as stale (`ErrStaleAnomalySignal`), preventing out-of-date anomalies from corrupting current operational state.

### 3.7 Deterministic Severity Mapping
ADR-0009 enforced strict decoupling of mathematical `AnomalyScore` $\in [0.0, 1.0]$ from operational `IncidentSeverity`. The Incident Engine maps normalized scores to domain severities using deterministic, explainable thresholds:

| Normalized AnomalyScore | Domain IncidentSeverity | Operational Interpretation |
| :--- | :--- | :--- |
| $\ge 0.85$ | `CRITICAL` | Severe deviation; immediate operational risk |
| $\ge 0.60$ | `HIGH` | Substantial breach beyond safety margin |
| $\ge 0.30$ | `MEDIUM` | Moderate anomaly exceeding primary threshold |
| $< 0.30$ | `LOW` | Minor or boundary anomaly |

Custom overrides can also be configured per metric name via `PolicyConfig.SeverityMap`.

### 3.8 Domain FSM Integration
The Incident Engine operates strictly through the canonical Incident Finite State Machine (`types.IncidentStatus`):
$$\text{NORMAL} \xrightarrow{\text{Breach } (M\text{-of-}N)} \text{ANOMALY\_DETECTED} \xrightarrow{} \text{MITIGATING} \mid \text{ESCALATED} \xrightarrow{} \text{RECOVERED} \xrightarrow{} \text{NORMAL}$$

* The engine enforces FSM validity using `inc.TransitionTo(next, now)`.
* Invalid transitions (e.g. `ANOMALY_DETECTED` directly to `RECOVERED` without entering `MITIGATING` or `ESCALATED`) are strictly rejected with `types.ErrInvalidStateTransition`.

### 3.9 Recovery Signal Limitations
* The Phase 5.2/5.3 `ThresholdDetector` implements internal hysteresis to clear its active breach state when metrics return to nominal bounds, but currently emits only breach signals.
* The Incident Engine **does not invent synthetic recovery signals**.
* When formal recovery events are modeled in a future phase, or when an operator/remediation actuator invokes `CloseActiveIncident`, the engine transitions the incident through the appropriate FSM steps (`RECOVERED`) and clears the stream's active incident pointer.

### 3.10 Process-Local State & Restart Semantics
* For Phase 5.4, the Incident Engine holds its correlation windows and active incident references in thread-safe, process-local memory (`LocalEngine`).
* **Restart Semantics**: Upon process restart, the in-memory observation sliding window and active incident references are reset.
* **Durability Guarantee**: Telemetry durability is unaffected; all raw telemetry remains durably recorded in the SQLite WAL store.
* **Future Durability Direction**: Durable incident persistence (local SQLite incident table and central synchronization) is scheduled for a dedicated future persistence phase.

### 3.11 Error Isolation
The Incident Engine is an observer and coordinator downstream of storage:
* If the incident engine fails or an `AnomalyHandler` returns an error, **telemetry persistence is never rolled back or invalidated**.
* The telemetry batch remains in SQLite WAL storage with `PENDING` status, and synchronization with the central control plane continues unimpeded.

---

## 4. Consequences & Trade-offs

### Positive
1. **Explainable Incident Creation**: $M$-of-$N$ threshold correlation eliminates single-observation noise.
2. **Stable Incident Identity**: Eliminates incident thrashing by preserving `IncidentID` throughout an active breach episode.
3. **Strict Stream Isolation**: Completely isolates nodes and metric channels from cross-contamination.
4. **Idempotency & Replay Safety**: Prevents replay or duplicate signal delivery from skewing incident counts.
5. **Robust Error Isolation**: Engine failures can never poison local telemetry storage or sync transport.

### Negative & Deferred Work
1. **Process-Local Volatility**: In-memory incident correlation state does not survive agent process restarts. (Addressed in future Incident Persistence phase).
2. **One-Way Detection Signals**: Current detector emits only positive breaches, deferring automated recovery detection to future bidirectional detector enhancements.
3. **No Autonomous Remediation**: Mitigations are populated as domain descriptors only; execution of remediation actions is strictly deferred to future phases.

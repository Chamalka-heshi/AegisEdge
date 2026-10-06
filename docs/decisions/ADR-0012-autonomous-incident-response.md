# ADR-0012: Autonomous Incident Response Architecture and Safety Design

**Status**: Accepted (Phase 6.1 Architecture & Safety Design; Phase 6.2 Policy & Validator; Phase 6.3 Simulated Action Executor; Phase 6.4 Durable Mitigation Persistence & Recovery; Phase 6.5 Operator Approval; Phase 6.6 Closed-Loop Incident Verification & Recovery Implemented)
**Date**: 2026-10-02  
**Deciders**: AegisEdge Core Engineering Team  
**Supersedes**: None  
**Relates to**: ADR-0003 (Domain Event & Incident Contracts), ADR-0004 (Edge-Local Persistence & WAL Durability), ADR-0009 (Edge Anomaly Detection Architecture), ADR-0010 (Local Incident Engine Architecture), ADR-0011 (Durable Incident Persistence)

---

## 1. Context & Operational Motivation

Through Phases 1 through 5.6D, AegisEdge established an offline-first telemetry ingestion, anomaly detection, incident correlation, and ML model lifecycle engine governed by the sequential pipeline:
$$\textbf{PERSIST TELEMETRY FIRST} \longrightarrow \textbf{DETECT SECOND} \longrightarrow \textbf{CORRELATE THIRD}$$

At the conclusion of Phase 5.6D, edge nodes:
1. Durably record local telemetry batches in SQLite WAL storage (`telemetry_batches`).
2. Detect mathematical threshold, statistical, and Isolation Forest ML breaches (`types.AnomalySignal`).
3. Correlate multi-signal breach sequences across time using an $M$-of-$N$ sliding window policy (`types.Incident`).
4. Manage candidate staging, active runtime switching, and explicit rollback of ML models locally without control-plane dependency.

However, incident detection and correlation alone do not resolve host anomalies. Remote edge nodes (e.g. industrial gateways, substations, wind turbines, cell towers) frequently experience extended WAN network partitions lasting hours or days. When an edge node experiences a critical resource breach during a network severance (such as runaway telemetry generation saturating memory or CPU thrashing), waiting for central cloud operator intervention is impossible.

### The Existential Risk of Unrestricted Remediation
While autonomous response is necessary, unconstrained remediation is catastrophic:
1. **The "Cure Worse Than the Disease" Dilemma**: An unconstrained remediation system executing arbitrary scripts or process terminations can kill mission-critical control daemons, corrupt filesystems, disable network interfaces, or render the node permanently unbootable.
2. **The Hallucination Danger of Generative AI / Unbounded Commands**: Passing arbitrary command strings (`exec.Command("sh", "-c", cmd)`) or AI-generated commands to a host shell introduces catastrophic security and operational failure modes: prompt injection, non-deterministic execution, and arbitrary host destruction.
3. **Flapping & Remediation Loops**: Without strict cooldowns, idempotency, and state machine integration, automated remediation can trigger cascading restart storms that consume battery reserves, corrupt local databases, and exhaust hardware write cycles.

Phase 6.1 establishes the formal **architecture, safety model, and design specification** for safe, deterministic, allowlisted autonomous response.

> [!IMPORTANT]
> **PHASE 6.1 IS ARCHITECTURE AND SAFETY DESIGN ONLY.**  
> No host mutation, no arbitrary shell commands, no real process manipulation, no database migrations, and no remote actuators are implemented in this phase.

---

## 2. Core Safety Invariants

The central, non-negotiable architectural principles of AegisEdge Autonomous Response are:

### Invariant 1: Detection Must Not Directly Execute Actions
$$\textbf{DETECTION MUST NOT DIRECTLY EXECUTE ACTIONS.}$$
An anomaly signal or incident must never trigger host operations directly. It must pass through an explicit response-policy engine and an independent, fail-closed safety boundary before any action can be authorized.

### Invariant 2: The Canonical Response Pipeline
$$\textbf{INCIDENT [IMPLEMENTED]} \longrightarrow \textbf{RESPONSE POLICY [DESIGNED]} \longrightarrow \textbf{SAFETY VALIDATION [DESIGNED]} \longrightarrow \textbf{DRY-RUN / SIMULATION [DESIGNED]} \longrightarrow \textbf{OPERATOR APPROVAL [DESIGNED]} \longrightarrow \textbf{CONTROLLED ACTION EXECUTION [DESIGNED]} \longrightarrow \textbf{VERIFY RESULT [DESIGNED]} \longrightarrow \textbf{UPDATE INCIDENT [DESIGNED]} \longrightarrow \textbf{RECORD AUDIT EVENT [DESIGNED]}$$

```mermaid
flowchart TD
    TEL["1. Telemetry Capture & WAL Persistence\n[IMPLEMENTED]"] --> DET["2. Local Anomaly Detectors\n(Threshold / Statistical / ML) [IMPLEMENTED]"]
    DET --> INC["3. Incident Engine Correlation\n(M-of-N Window & FSM) [IMPLEMENTED]"]
    INC --> POL["4. Deterministic Response Policy\n(Candidate Decision Selection) [DESIGNED - Phase 6.2]"]
    POL --> VAL{"5. Safety Validator (Fail-Closed)\n(Allowlist, Limits, Cooldowns) [DESIGNED - Phase 6.2]"}

    VAL -- "REJECT" --> AUD_REJ["Record Rejection Audit Event\n(Incident transitions to ESCALATED) [DESIGNED]"]
    VAL -- "ALLOW" --> MODE{"6. Execution Mode"}

    MODE -- "DRY_RUN / SIMULATE" --> SIM["Simulated Execution (Zero Host Side Effects)\n(types.MitigationAction) [DESIGNED - Phase 6.3]"]
    MODE -- "APPROVAL_REQUIRED" --> QUEUE["Operator Approval Ticket\n[DESIGNED / FUTURE - Phase 6.5]"]
    MODE -- "AUTO_EXECUTE" --> EXEC["Controlled Action Execution\n(Scoped Non-Shell Handlers) [DESIGNED / FUTURE - Phase 6.5]"]

    QUEUE -- "Operator Approved" --> EXEC
    QUEUE -- "Expired / Denied" --> AUD_REJ

    SIM --> VER["7. Verify Result via Telemetry\n(Observe Metric Conditions) [DESIGNED / FUTURE - Phase 6.5]"]
    EXEC --> VER

    VER --> UPD["8. Update Incident Lifecycle\n(Append MitigationAction to Incident) [DESIGNED / FUTURE - Phase 6.4]"]
    UPD --> AUD["9. Record Audit Event\n(Local WAL Storage & Upstream Sync) [DESIGNED / FUTURE - Phase 6.4]"]
```

---

## 3. Component Architecture & Layer Boundaries

The response architecture comprises decoupled conceptual components:

```
┌────────────────────────────────────────────────────────┐
│ 1. Incident Engine (edge/agent/incident) [CURRENT]     │
│    - Correlates AnomalySignals into types.Incident     │
│    - Manages Incident FSM lifecycle                    │
│    - Strictly detects; NEVER decides host commands     │
└───────────────────────────┬────────────────────────────┘
                            │ Evaluates
                            ▼
┌────────────────────────────────────────────────────────┐
│ 2. Response Policy Engine (edge/agent/response) [FUTURE]│
│    - Pure deterministic policy evaluation              │
│    - Evaluates incident severity, rule, and cooldowns  │
│    - Proposes candidate ResponseDecision               │
└───────────────────────────┬────────────────────────────┘
                            │ Evaluates
                            ▼
┌────────────────────────────────────────────────────────┐
│ 3. Fail-Closed Safety Validator [FUTURE]               │
│    - Enforces closed ActionAllowlist                   │
│    - Validates target, parameter bounds, cooldowns     │
│    - Rejects any out-of-bounds or malformed decision   │
└───────────────────────────┬────────────────────────────┘
                            │ Authorizes
                            ▼
┌────────────────────────────────────────────────────────┐
│ 4. Execution Mode Dispatcher [FUTURE]                  │
│    - DRY_RUN: Executes simulation; zero host mutations │
│    - APPROVAL_REQUIRED: Holds ticket for operator auth │
│    - AUTO_EXECUTE: Dispatches allowlisted handler      │
└───────────────────────────┬────────────────────────────┘
                            │ Dispatches
                            ▼
┌────────────────────────────────────────────────────────┐
│ 5. Controlled Action Executor [FUTURE]                 │
│    - Strongly typed Go handlers (no shell wrappers)    │
│    - Strict execution timeout via context.Context      │
│    - Idempotent execution tokens (ActionID)            │
└───────────────────────────┬────────────────────────────┘
                            │ Observes
                            ▼
┌────────────────────────────────────────────────────────┐
│ 6. Verification Engine [FUTURE]                        │
│    - Evaluates post-execution telemetry metrics        │
│    - Confirms whether breach cleared (not just exit 0) │
└───────────────────────────┬────────────────────────────┘
                            │ Commits
                            ▼
┌────────────────────────────────────────────────────────┐
│ 7. Audit & Persistence Layer (Migration v4) [FUTURE]   │
│    - Transactionally commits MitigationAction to WAL   │
│    - Reconciles ambiguous crashes upon restart         │
└────────────────────────────────────────────────────────┘
```

---

## 4. Allowlisted Actions & Negative Boundaries

AegisEdge divides all potential system actions into three rigid categories:

```
┌────────────────────────────────────────────────────────────────────────────┐
│ 1. SIMULATED ACTIONS (Conceptual Baseline — Implementation Deferred to Phase 6.3) │
│    - SIMULATED_THROTTLE — controlled simulation of a throttling response          │
│    - SIMULATED_RESTART  — controlled simulation of a restart response             │
│    - SIMULATED_ISOLATE  — controlled simulation of an isolation response           │
│    - SIMULATED_ALERT    — controlled alert simulation                             │
├────────────────────────────────────────────────────────────────────────────┤
│ 2. CONTROLLED REAL ACTIONS (Future Scoped Actuators — Phase 6.5)           │
│    - Scoped process cgroup cpu.max throttling                              │
│    - Internal application worker goroutine restart                         │
│    - Controlled loopback firewall rule via pre-compiled binary template    │
├────────────────────────────────────────────────────────────────────────────┤
│ 3. FORBIDDEN ACTIONS (Permanently Prohibited Across All Phases)            │
│    - Arbitrary command-line strings via /bin/sh, /bin/bash, powershell.exe │
│    - Unrestricted filesystem mutation ("rm -rf ...", file deletion)        │
│    - Unrestricted process termination ("kill -9 ...", arbitrary PID kills) │
│    - Raw network command strings ("iptables ...", "ufw ...", "curl | sh")  │
│    - AI/LLM-generated natural language commands                            │
└────────────────────────────────────────────────────────────────────────────┘
```
*Note: Actual simulation behavior will be implemented in Phase 6.3.*

### Negative Boundary Specification
The response engine must **NEVER** accept, construct, or execute:
- Any arbitrary shell string: `"rm -rf /"`, `"kill -9 1234"`, `"powershell.exe -Command ..."`.
- Any command containing chaining/redirection metacharacters: `|`, `;`, `&`, `` ` ``, `$()`, `>`, `<`.
- Any unvetted dynamic script loaded from network or filesystem.

---

## 5. Action Risk Classification

To ensure appropriate operational governance, actions are categorized by operational risk:

| Risk Category | Permitted Action Types | Authorization Default | Impact on Host |
| :--- | :--- | :---: | :--- |
| **LOW RISK** | `SIMULATED_ALERT`, local log annotation, internal telemetry rate throttling | `AUTO_EXECUTE` or `DRY_RUN` | Zero disruption; non-destructive; reversible. |
| **CONTROLLED / APPROVAL REQUIRED** | `SIMULATED_RESTART`, `SIMULATED_ISOLATE`, future real service restart, future cgroup throttle | `APPROVAL_REQUIRED` (default) | Potential transient disruption; requires operator review or strict policy override. |
| **FORBIDDEN** | Arbitrary shell execution, raw process kills, arbitrary filesystem writes, arbitrary firewall mutation | **PERMANENTLY REJECTED** | Catastrophic host failure risk; structurally blocked by compiler types and validator. |

> [!NOTE]
> The exact classification thresholds and policy bindings must be finalized against the actual actuator implementations in Phase 6.3–6.5.

---

## 6. Fail-Closed Safety Validator

The `SafetyValidator` sits between policy decisions and execution authorization. It acts as an absolute gatekeeper:

$$\text{ValidateAction}(\text{Incident}, \text{ProposedAction}) \longrightarrow \textbf{ALLOW} \;\Big|\; \textbf{REJECT}$$

### Validation Checklist (All Must Pass):
1. **Allowlist Membership**: `ActionType` matches compile-time enum `types.MitigationActionType.IsValid()`.
2. **Incident Association**: `decision.IncidentID == incident.IncidentID` and `decision.NodeID == incident.NodeID`.
3. **Target Validity**: `Target` is non-empty, contains no shell metacharacters, and conforms to target allowlist.
4. **Parameter Bounds**: Numerical and duration parameters reside strictly within configured minimum/maximum safety bounds (e.g. `throttle_percent` $\in [1, 100]$, `duration_sec` $\in [1, 3600]$).
5. **Action Risk Level**: Action classification matches node execution policy.
6. **Approval Status**: If action is classified as `APPROVAL_REQUIRED`, a cryptographically valid approval token must be present and unexpired.
7. **Idempotency & Deduplication**: No identical action is currently `PENDING` or `EXECUTING`, and action has not already completed for this decision.
8. **Cooldown Verification**: Node and metric cooldown timers have elapsed since previous mitigation.
9. **Incident State**: Incident must be in `ANOMALY_DETECTED` or `MITIGATING`; incidents in `NORMAL`, `RECOVERED`, or `ESCALATED` reject new automated actions.
10. **Policy Version Integrity**: `PolicyName` and `PolicyVersion` are registered and recognized.
11. **Timeout Configured**: Bounded execution context timeout is specified ($> 0$ and $\le 30\text{s}$).

### Fail-Closed Directive
If validation cannot positively confirm that every single check passes:
$$\textbf{REJECT}$$
The validator must **never** execute on "best effort" or assume missing parameters are benign. Any validation error causes immediate rejection and transitions the driving incident to `ESCALATED`.

---

## 7. Human Approval Boundary

For actions carrying operational risk (service restarts, network isolation, host state adjustments), human-in-the-loop authorization is required:

### Three Authorization Classes:
1. **AUTOMATICALLY ALLOWED**: Low-risk, non-destructive allowlisted actions (`SIMULATED_ALERT`, bounded `SIMULATED_THROTTLE`).
2. **APPROVAL REQUIRED**: Potentially disruptive actions (`SIMULATED_RESTART`, future host service restarts).
3. **FORBIDDEN**: Permanently rejected; cannot be authorized by policy or human operator.

### Required Approval Ticket Specification:
When an action requires approval, the system generates an immutable approval ticket:
- `TicketID`: Unique identifier for the approval request.
- `ActionID`: Stable identity of the proposed action.
- `IncidentID`: Stable identity of the driving incident.
- `ActionType` & `Target`: Exactly what would be executed.
- `Parameters`: Immutable copy of proposed parameters.
- `ProposedAt`: Timestamp of ticket creation.
- `ExpiresAt`: Deterministic expiration deadline (e.g., 15 minutes). If expired, the ticket becomes invalid (`EXPIRED`).
- `ApproverIdentity`: Identifier of authorized human operator or signed control-plane service (future work).
- `ApprovedAt`: Timestamp of approval signature.
- **Future Anti-Tampering Binding**: Future approval mechanisms should cryptographically bind approval to the exact proposed action: the approval signature should bind to the exact hash of `(TicketID, ActionID, ActionType, Target, Parameters)` so that approving a ticket can never authorize a modified or substituted action.
- **Implementation Status**: Cryptographic approval/signature implementation is deferred to Phase 6.5. Cryptographic identity, public key infrastructure, and operator UI are future implementation milestones and are not implemented in Phase 6.1.

---

## 8. Dry-Run / Simulation Mode

Dry-run simulation is an architectural first-class operational mode:

For every proposed action, the system evaluates:
1. *What action would have been executed?* (e.g. `SIMULATED_THROTTLE`)
2. *Against what target?* (e.g. `"telemetry_generator"`)
3. *Why was it chosen?* (e.g. `"CPU usage exceeded 90% threshold"`)
4. *Which policy produced it?* (e.g. `"default_edge_response_policy" v1.0.0`)
5. *Which safety rules approved it?* (e.g. all 11 validator gates passed)
6. *Would approval have been required?* (e.g. `APPROVAL_REQUIRED = true`)

### Invariants of Simulation:
- Simulation executes the full pipeline up to execution, but invokes **zero host side effects**.
- Simulation reuses existing `SIMULATED_*` action types.
- The result is recorded with status `EXECUTED` or `SKIPPED` with explicit metadata `Message: "[DRY-RUN] Simulated execution succeeded"`.
- **Non-Equivalence**: Simulation verifies policy, parameter structure, and safety rules. It does **not** prove that a future real OS actuator will succeed (e.g., OS permission errors, locked processes).

---

## 9. Action Execution Lifecycle State Machine

Discrete actions transition through an explicit lifecycle that is decoupled from, but drives, the macro `types.IncidentStatus`:

```mermaid
stateDiagram-v2
    [*] --> PROPOSED
    PROPOSED --> VALIDATED : SafetyValidator Passes
    PROPOSED --> REJECTED : SafetyValidator Fails / Unsafe

    VALIDATED --> WAITING_APPROVAL : Approval Required
    VALIDATED --> EXECUTING : Auto-Execute / Simulation Allowed

    WAITING_APPROVAL --> APPROVED : Operator Approves within Deadline
    WAITING_APPROVAL --> EXPIRED : Approval Deadline Exceeded
    WAITING_APPROVAL --> CANCELLED : Incident Resolves Before Approval

    APPROVED --> EXECUTING : Dispatcher Starts Action
    APPROVED --> CANCELLED : Incident Resolves Before Execution

    EXECUTING --> SUCCEEDED : Actuator Finishes (Exit 0 / Clean Return)
    EXECUTING --> FAILED : Actuator Returns Error
    EXECUTING --> UNKNOWN_RECONCILIATION_REQUIRED : Timeout / Process Crash

    SUCCEEDED --> VERIFIED : Post-Action Telemetry Confirms Resolution
    SUCCEEDED --> VERIFICATION_FAILED : Metric Remains in Breach

    REJECTED --> [*]
    EXPIRED --> [*]
    CANCELLED --> [*]
    FAILED --> [*]
    VERIFIED --> [*]
    VERIFICATION_FAILED --> [*]
    UNKNOWN_RECONCILIATION_REQUIRED --> [*]
```

### Mapping to Existing Domain Contracts:
In `shared/types/types.go`, `types.MitigationStatus` currently defines:
- `PENDING`: Encompasses `PROPOSED`, `VALIDATED`, `WAITING_APPROVAL`, `APPROVED`.
- `EXECUTING`: Action handler currently running.
- `EXECUTED`: Action handler completed cleanly.
- `FAILED`: Action handler returned error or safety rejected.
- `SKIPPED`: Action intentionally bypassed (cooldown active, dry-run skipped).
- `UNKNOWN_RECONCILIATION_REQUIRED`: Ambiguous timeout or post-execution crash.

*Future Extension Note*: In Phase 6.3–6.4, `types.MitigationStatus` may be enriched with explicit intermediate states (`WAITING_APPROVAL`, `VERIFIED`, `VERIFICATION_FAILED`, `EXPIRED`, `CANCELLED`) to provide finer-grained auditability without breaking the existing FSM.

---

## 10. Idempotency & Stable Identity

To prevent duplicate execution across network retries, duplicate anomaly deliveries, and process crashes, AegisEdge enforces an identity hierarchy:

$$\textbf{AnomalyID} \longrightarrow \textbf{IncidentID} \longrightarrow \textbf{DecisionID} \longrightarrow \textbf{ActionID}$$

1. **`AnomalyID`**: Discrete point-in-time observation signal.
2. **`IncidentID`**: Stable UUIDv4 identifying the continuous operational incident.
3. **`DecisionID`**: Deterministic SHA-256 digest:
   $$\text{DecisionID} = \text{SHA256}(\text{IncidentID} \mathbin{\Vert} \text{PolicyName} \mathbin{\Vert} \text{PolicyVersion} \mathbin{\Vert} \text{ActionType} \mathbin{\Vert} \text{Target})[:12]$$
4. **`ActionID`**: Unique execution token (`act-<uuid>`) representing the concrete execution attempt.

### At-Least-Once Request Handling with Idempotent Action Semantics
AegisEdge recognizes the **Critical Consistency Window**:
$$\text{Executor succeeds on host} \longrightarrow \textbf{[CRASH / REBOOT]} \longrightarrow \text{Result not committed to SQLite}$$
Upon agent restart, the system detects an unconfirmed in-flight action:
- It **does not blindly re-execute**.
- The action transitions to `UNKNOWN_RECONCILIATION_REQUIRED`.
- The incident transitions to `ESCALATED`.
- Subsequent evaluation requests for the same `DecisionID` within the cooldown period are idempotently suppressed.

---

## 11. Cooldown & Repeat Suppression (Anti-Flapping)

Automated remediation can trigger catastrophic oscillation (restart loops, flapping network interfaces). AegisEdge enforces four layers of defense:

1. **Per-Action Cooldown**: After an action executes, the same action type cannot execute on the same target until a configured cooldown period elapses (default: 5 minutes).
2. **Maximum Actions per Incident**: An active incident episode is capped at a strict maximum number of automated mitigations (default: 3). Once reached, automated responses cease and the incident transitions to `ESCALATED`.
3. **Circuit Breaker**: If 3 consecutive actions fail or fail verification across a metric stream, the response engine trips the circuit breaker for that stream, blocking all automated actions for 30 minutes.
4. **Operator Escalation**: Any breach of limits automatically halts automation and requests manual operator intervention.

---

## 12. Verification Semantics

A successful action execution does **not** equal incident resolution:

$$\textbf{"Command returned exit code 0" } \neq \textbf{ "Incident is resolved."}$$

For example:
- A service restart command may return exit code 0 while the service crashes immediately upon initialization.
- A throttle command may succeed while CPU consumption remains pegged due to an unthrottled thread.

### The Two-Stage Verification Protocol:
1. **Actuator Execution Verification**: Verifies that the actuator completed cleanly without errors (`SUCCEEDED`).
2. **Telemetry Condition Verification**: 
   - The edge agent continues ingesting telemetry samples following execution.
   - The verification engine monitors the driving metric (e.g. `cpu_usage_percent`) across a post-mitigation observation window (e.g. 3 consecutive nominal samples).
   - If the metric returns below recovery threshold: status becomes `VERIFIED`, and incident transitions `MITIGATING` $\to$ `RECOVERED`.
   - If the metric remains anomalous after the verification deadline: status becomes `VERIFICATION_FAILED`, and incident transitions `MITIGATING` $\to$ `ESCALATED`.

---

## 13. Comprehensive Failure Matrix (Scenarios A through T)

| Scenario | Failure Mode | System Behavior | Action Executes? | Retry Allowed? | Operator Action | Audit Requirement |
| :---: | :--- | :--- | :---: | :---: | :--- | :--- |
| **A** | **Policy Missing** | No matching policy rule found for metric/severity | **NO** | No | Alerted if severity critical | Logged as `NO_POLICY_MATCHED` |
| **B** | **Unknown Action Type** | ActionType not recognized in `MitigationActionType` | **NO** | **NO** | Incident escalated to operator | Logged as `REJECTED_UNKNOWN_ACTION_TYPE` |
| **C** | **Invalid Target** | Target empty, regex mismatch, or contains shell tokens | **NO** | **NO** | Incident escalated to operator | Logged as `REJECTED_INVALID_TARGET` |
| **D** | **Safety Validator Failure** | Parameter out of bounds, internal validator panic/error | **NO** | **NO** | Incident escalated to operator | Logged as `REJECTED_SAFETY_VIOLATION` |
| **E** | **Approval Required but Unavailable** | Action requires human approval; no operator online | **NO** | No | Ticket queued in SQLite | Logged as `APPROVAL_REQUIRED_PENDING` |
| **F** | **Approval Expired** | Approval deadline elapsed before operator signature | **NO** | No | Operator notified of expiration | Logged as `APPROVAL_EXPIRED` |
| **G** | **Incident Resolved Before Execution** | Incident transitioned to `RECOVERED` before dispatch | **NO** | No | None; auto-cleared | Logged as `CANCELLED_INCIDENT_RECOVERED` |
| **H** | **Duplicate Action Request** | ActionID/DecisionID already executed or executing | **NO** | No | None; idempotent filter | Logged as `DUPLICATE_SUPPRESSED` |
| **I** | **Executor Unavailable** | Target subsystem down, internal handler unregistered | **NO** | Yes (max 2) | Escalated if retries fail | Logged as `EXECUTOR_UNAVAILABLE` |
| **J** | **Executor Timeout** | Execution context exceeded deadline (default: 5s) | Ambiguous | **NO** | Action marked `UNKNOWN_RECONCILIATION_REQUIRED`; escalated | Logged as `FAILED_TIMEOUT` |
| **K** | **Executor Reports Failure** | Actuator returned explicit error / non-zero exit | Attempted | **NO** | Incident escalated to operator | Logged as `FAILED_EXECUTION_ERROR` |
| **L** | **Execution Succeeds but Write Fails** | Process killed after actuation but before SQLite write | Executed | **NO** | Recovery reconciles on startup; escalated | Logged as `UNKNOWN_RECONCILIATION_REQUIRED` |
| **M** | **Verification Fails** | Actuator succeeded, but telemetry metric remains bad | Executed | **NO** | Incident escalated to operator | Logged as `VERIFICATION_FAILED` |
| **N** | **Repeated Execution Attempts** | Action attempted maximum times (e.g. 3) | **NO** | **NO** | Circuit breaker tripped; operator paged | Logged as `LIMIT_EXCEEDED_CIRCUIT_TRIPPED` |
| **O** | **Node Restart During Action** | Process crash during in-flight action | Interrupted | **NO** | On startup, marked `UNKNOWN_RECONCILIATION_REQUIRED` | Logged as `CRASH_RECONCILIATION_REQUIRED` |
| **P** | **Network Partition** | Edge completely offline from control plane | **YES** (if allowlisted) | Per policy | Buffered locally; synced on reconnect | Full local audit record buffered in SQLite WAL |
| **Q** | **Control Plane Unavailable** | Remote registry or approval server offline | **YES** (local policy) | Local policy | Queues remote tickets; runs local | Logged locally in WAL |
| **R** | **Telemetry Unavailable After Action** | Sensor failure prevents verification | Executed | **NO** | Incident marked `ESCALATED` due to unverified state | Logged as `VERIFICATION_TELEMETRY_UNAVAILABLE` |
| **S** | **Policy Version Mismatch** | Local policy config hash does not match registry | **NO** | **NO** | Falls back to default safe policy; alerts operator | Logged as `POLICY_VERSION_MISMATCH` |
| **T** | **Unsafe/Malformed Action Payload** | Payload contains shell injection chars (`|`, `;`, `&`, `$`) | **NO** | **NO** | Security incident escalated immediately | Logged as `SECURITY_VIOLATION_MALFORMED_PAYLOAD` |

---

## 14. Security Boundary & Model Integrity

### Security Principles:
1. **Trusted Response Policy**: Response policies are immutable, compile-time configurations or cryptographically signed policy bundles.
2. **Zero Shell Interpolation**: No shell command strings (`exec.Command("sh", "-c", ...)`) are permitted. All actuators invoke typed Go functions.
3. **Payload Sanitization**: All action parameters are strongly typed (`int`, `float`, enum). Free-form string parameters are strictly validated against alphanumeric allowlists.
4. **Least Privilege**: The edge agent process runs under dedicated OS user credentials with strictly bounded Linux capabilities (`CAP_NET_ADMIN` only if isolation required).
5. **Auditable Response Events**: Every decision, safety check, and execution outcome is designed to be committed to local SQLite WAL storage and streamed upstream via NATS JetStream when connected. (Cryptographic tamper-evidence is future work deferred to subsequent security milestones.)

---

## 15. The AI / ML Boundary

AegisEdge strictly segregates Machine Learning / AI capabilities from Operational Response Execution:

```
┌────────────────────────────────────────────────────────┐
│ MACHINE LEARNING LAYER (Isolation Forest, etc.)        │
│                                                        │
│ PERMITTED OUTPUTS:                                     │
│  - AnomalyScore in [0.0, 1.0]                          │
│  - AnomalySignal mathematical domain event             │
│  - Normalized feature split path & depth evidence      │
│  - Confidence metric and detector metadata             │
│                                                        │
│ PROHIBITED OUTPUTS:                                    │
│  - NO arbitrary host shell commands                    │
│  - NO arbitrary action types                           │
│  - NO unrestricted file paths or targets               │
│  - NO shell scripts or execution wrappers              │
│  - NO network interface or firewall configurations     │
└───────────────────────────┬────────────────────────────┘
                            │ Produces mathematical signal only
                            ▼
┌────────────────────────────────────────────────────────┐
│ INCIDENT & RESPONSE POLICY LAYER                       │
│  - Maps mathematical signal to Incident domain entity  │
│  - Evaluates deterministic, compile-time policy rules  │
│  - SafetyValidator enforces rigid allowlist and bounds │
└────────────────────────────────────────────────────────┘
```

The response system remains **strictly deterministic and policy-driven**, even when driven by machine learning detection models.

---

## 16. Observability & Structured Audit Events

In future implementation phases, the response subsystem will emit structured domain events for every lifecycle milestone:

1. `incident.detected`: Incident correlated from anomaly signals.
2. `response.proposed`: Policy selected candidate action.
3. `response.validated`: Safety validator passed or rejected decision.
4. `response.approval_requested`: Approval ticket created for operator.
5. `response.approved` / `response.denied` / `response.expired`: Approval resolution.
6. `response.execution_started`: Actuator commenced execution.
7. `response.execution_completed`: Actuator finished successfully.
8. `response.execution_failed`: Actuator returned error or timed out.
9. `response.verification_started`: Post-action telemetry monitoring started.
10. `response.verified` / `response.verification_failed`: Operational resolution status.
11. `response.escalated`: Incident escalated to human operators.

---

## 17. Current vs. Future Implementation Roadmap

| Subsystem / Capability | Current Status (Phase 6.6) | Target Implementation Phase |
| :--- | :---: | :--- |
| **Incident Correlation & Lifecycle FSM** | **CURRENTLY IMPLEMENTED** | Phase 5.3–5.5 |
| **MitigationAction & ActionType Contracts** | **CURRENTLY IMPLEMENTED** | `shared/types/types.go` |
| **Response Architecture & Safety Specification** | *DESIGNED / DOCUMENTED* | Phase 6.1 (ADR-0012) |
| **Deterministic Response Policy Engine** | **CURRENTLY IMPLEMENTED** | Phase 6.2 (`edge/agent/response/policy.go`) |
| **Fail-Closed Safety Validator** | **CURRENTLY IMPLEMENTED** | Phase 6.2 (`edge/agent/response/validator.go`) |
| **Simulated Action Executor & Dry-Run Mode** | **CURRENTLY IMPLEMENTED** | Phase 6.3 (`edge/agent/response/executor.go`) |
| **Durable Mitigation Persistence (Migration v4)** | **CURRENTLY IMPLEMENTED** | Phase 6.4 (`edge/agent/storage/sqlite.go`) |
| **Operator Approval & Safety Gate (Migration v5)** | **CURRENTLY IMPLEMENTED** | Phase 6.5 (`edge/agent/response/approval.go`, `edge/agent/storage/sqlite.go`) |
| **Automated Telemetry Verification Engine (Migration v6)** | **CURRENTLY IMPLEMENTED** | Phase 6.6 (`edge/agent/verification/engine.go`, `edge/agent/storage/sqlite.go`) |
| **Real Host Actuators & Production Remediation** | *DESIGNED (Future Implementation)* | Future Phase |

---

## 18. Alternatives Considered

1. **Direct Detector-to-Actuator Coupling**:
   - *Design*: Allow the anomaly detector to directly call an OS command when an anomaly is observed.
   - *Reason for Rejection*: Violates pipeline separation of concerns. Causes alert flapping and cascading restarts. Gives statistical/ML algorithms authority over host operations without safety checks.
2. **Unrestricted Shell Script Execution**:
   - *Design*: Accept shell script paths or raw command strings from configuration or central server.
   - *Reason for Rejection*: Massive security vulnerability. Introduces arbitrary code execution, command injection, privilege escalation, and unmaintainable failure modes.
3. **AI / LLM-Driven Dynamic Remediation Commands**:
   - *Design*: Pass incident telemetry to an LLM to generate dynamic remediation commands.
   - *Reason for Rejection*: Non-deterministic, unpredictable, prone to hallucinations, prompt injections, and catastrophic host destruction on edge devices.
4. **Cloud-Only Centralized Remediation Orchestration**:
   - *Design*: Require all responses to be decided and commanded remotely from the cloud control plane.
   - *Reason for Rejection*: Fails completely during WAN network partitions. Leaves unmanned remote edge devices unprotected during critical resource saturation.

---

## 19. Explicit Limitations of Phase 6.4

1. **Durable Persistence & Simulated Execution Only**: Phase 6.4 implements durable SQLite WAL persistence (`mitigation_records` via Migration v4) and startup crash recovery for simulated mitigation decisions.
2. **Zero Real Host Mutation**: Real host actuators remain strictly excluded. Zero process termination, zero service restarts, zero CPU throttling, zero network isolation, zero firewall modifications, zero filesystem remediation, and zero shell commands are executed.
3. **Startup Recovery Semantics**: If the agent process crashes while a mitigation is marked `EXECUTING`, upon restart the recovery routine transitions the record to `UNKNOWN_RECONCILIATION_REQUIRED`. The system never infers that an external action completed merely because the agent process restarted. Transition of the driving incident to `ESCALATED` upon ambiguous mitigation state is designed future orchestration and is not performed by the local storage layer in this phase.
4. **Local Durability Boundary**: Mitigation records are persisted through SQLite transactions using the project's configured WAL durability boundary. This provides the application's local durable persistence boundary under SQLite's configured semantics, but does not guarantee survival of every sudden-power-loss, storage-device, or hardware failure scenario. AegisEdge maintains strict non-claims: no zero-data-loss claim, no arbitrary physical power-loss guarantee, no hardware failure guarantee, no distributed exactly-once execution, and no distributed transaction guarantee.
5. **No Operator UI or Remote Approval APIs**: Approval ticketing workflows and cryptographic authorization signatures remain deferred to Phase 6.5. Actions classified as `APPROVAL_REQUIRED` are not auto-authorized and are marked as `SKIPPED`.
6. **No Real Host Actuators or Telemetry Verification**: Concrete host manipulation actuators and automated closed-loop telemetry verification belong to Phase 6.5.

---

## 20. Explicit Limitations of Phase 6.5

1. **Operator Approval & Simulation Only**: Phase 6.5 introduces the operator approval lifecycle (`PENDING -> APPROVED -> CONSUMED`), deterministic decision fingerprint binding (`ComputeDecisionFingerprint`), 14-point fail-closed execution-time authorization gate, durable SQLite WAL persistence (Migration v5 `approval_records`), and atomic single-use consumption. Real host actuators remain completely excluded and execution operates strictly against `SimulatedExecutor`.
2. **Deterministic Decision Fingerprint Binding**: The approval decision fingerprint binds exactly eight canonical fields: `DecisionID`, `IncidentID`, `ActionID`, `NodeID`, `ActionType`, `Target`, `PolicyVersion`, and canonical sorted `Parameters`. No timestamps, random nonces, or volatile fields are included. At execution time, `ValidateApprovalForExecution` enforces explicit equality on `IncidentID`, `ActionID`, `NodeID`, `PolicyVersion`, `Target`, `ActionType`, `DecisionID`, and `DecisionFingerprint`.
3. **Explicit Non-Claims**:
   - **No Cryptographic Signatures**: Approvals are not digitally signed using asymmetric keys (e.g., Ed25519, RSA).
   - **No Authenticated Operator Identity**: Operator identities (`approved_by`, `requested_by`, `rejected_by`) are plain application-level metadata strings; no RBAC, OAuth2/OIDC, mTLS, or identity verification is performed.
   - **No Non-Repudiation**: Without cryptographic signing, approvals provide operational safety binding but cannot guarantee cryptographic non-repudiation.
   - **No Tamper-Proof Audit Logging**: Approval records are stored in local SQLite WAL tables; cryptographic hash chaining and tamper-evident storage are deferred to future security milestones.
4. **Zero Real Host Mutation**: Real host actuators remain strictly excluded. Zero process termination, zero service restarts, zero CPU throttling, zero network isolation, zero firewall modifications, zero filesystem remediation, and zero shell commands are executed.
5. **Local Durability Boundary**: Approval records and mitigation records are persisted through SQLite transactions using the project's configured WAL durability boundary (`synchronous=NORMAL`). This provides local crash recovery across process restarts, but does not guarantee survival of arbitrary physical power-loss or storage hardware failure. AegisEdge maintains explicit non-claims: no zero-data-loss claim, no arbitrary power-loss guarantee, no hardware failure guarantee, no distributed exactly-once execution, and no distributed transaction guarantee.
6. **No Direct Incident FSM Mutation**: Mitigation approvals and executions operate on `mitigation_records` and `approval_records`. They do NOT mutate `IncidentStatus` to `RECOVERED` or alter the incident state machine.

---

## 21. Phase 6.6 Closed-Loop Incident Verification & Recovery Design & Invariants

Phase 6.6 introduces the closed-loop incident verification subsystem (`edge/agent/verification/engine.go`) situated between action execution and incident recovery.

### 1. Fundamental Principle: Execution Success $\neq$ Incident Recovery
$$\textbf{"Execution success does not equal recovery."}$$
A successful mitigation execution (e.g. exit code 0, simulated completion) merely confirms that the actuator completed its configured invocation. It does NOT prove that host health was restored. Recovery must be established exclusively from subsequent empirical telemetry evidence.

### 2. Causality Non-Claim
$$\textbf{"Recovery confirmation means that the configured recovery condition was satisfied by subsequent telemetry; it does not prove that the mitigation caused the recovery."}$$
The verification engine confirms temporal correlation between mitigation execution and telemetry stabilization; external environmental shifts, parallel processes, or natural metric dissipation could also account for the recovery. AegisEdge makes no causal proof claims.

### 3. Verification Lifecycle State Machine
Verification records transition through explicit deterministic states:
- `PENDING`: Verification active, observing subsequent telemetry stream.
- `RECOVERED`: Required consecutive healthy observations met before timeout. (Terminal)
- `NOT_RECOVERED`: Explicit terminal non-recovery decision (e.g. operator/supervisor rejection or hard policy limit). Normal telemetry evaluation on an unhealthy sample resets the consecutive streak to 0 while status remains `PENDING`. (Terminal)
- `TIMED_OUT`: Observation window expired without meeting required consecutive healthy observations. (Terminal)
- `CANCELLED`: Explicitly cancelled due to incident escalation, supervisor override, or supersede. (Terminal)

### 4. Streak Dynamics & Hysteresis Semantics
- **Consecutive Observation Count**: Recovery requires $N$ consecutive healthy samples (default: 3).
- **Streak Reset**: Any unhealthy telemetry sample resets the consecutive healthy counter to zero (`observed_count = 0`), requiring a fresh streak while remaining in `PENDING`.
- **Hysteresis Recovery Threshold**: When evaluating threshold rules, the engine tests `sample.Value <= UpperRecoveryThreshold` (or `>= LowerRecoveryThreshold`), preventing flapping around the trigger threshold. Statistical (z-score) and ML recovery conditions are unsupported in this phase and reserved for future implementation.

### 5. Freshness & Idempotent Ordering Guarantees
- **Freshness Boundary**: Telemetry samples with timestamps prior to mitigation execution completion are rejected as stale (`ErrStaleTelemetrySample`).
- **SampleID Duplicate Suppression**: Telemetry `SampleID` is the primary deduplication identity. Re-submitting an already processed `SampleID` is rejected with `ErrDuplicateTelemetrySample`. Different `SampleID`s with the same timestamp or value are evaluated normally. Processed `SampleID`s are persisted across restarts to ensure idempotency.
- **Multi-Dimensional Isolation**: Verifications are strictly isolated by `(NodeID, IncidentID, ActionID, MetricName)`.

### 6. SQLite WAL Durability & Crash Recovery (Migration v6)
- **Schema**: Dedicated `verification_records` table with indexes on incident, action, status, and node/metric.
- **Crash Recovery**: On agent restart, `RecoverFromStore` scans pending verifications. Records whose deadline elapsed during downtime are reconciled to `TIMED_OUT`. Unexpired records remain `PENDING` and resume observation in-memory without losing prior streak progress or seen sample identity history.

### 7. Authoritative Incident Engine Handoff Invariants
- **Authority**: Verification `RECOVERED` does not automatically imply Incident `RECOVERED`. The existing `IncidentEngine` FSM remains authoritative.
- **Canonical Incident FSM**:
  $$\textbf{NORMAL} \longrightarrow \textbf{ANOMALY\_DETECTED} \longrightarrow \textbf{MITIGATING / ESCALATED} \longrightarrow \textbf{RECOVERED} \longrightarrow \textbf{NORMAL}$$
  Phase 6.6 does NOT introduce `CONFIRMED`, `CLOSED`, or any new Incident states.
- **Strict Handoff Invariant**: `HandoffToIncidentEngine` only resolves the incident via the existing `IncidentEngine.ResolveIncident` method if the incident status is currently `MITIGATING`.
- **Non-Mitigating Safety**: If the incident is in any other status (`ANOMALY_DETECTED`, `ESCALATED`, or `NORMAL`), the incident remains completely unchanged and returns without error. Incident status is never directly assigned.

### 8. Strict Scope & Safety Boundaries
- **Zero Real Host Actuation**: All executions remain simulated.
- **Simulation Boundary Maintained**: No OS commands, shell processes, or container mutations.
- **Durability Non-Claims**: Local WAL crash recovery under `synchronous=NORMAL`; no claims of zero-data-loss, hardware failure resilience, or distributed consensus.

---

## 22. Phase 6.7 Deterministic Incident Escalation & Circuit Breaker Design & Invariants

Phase 6.7 introduces the deterministic escalation and failure-handling engine (`edge/agent/escalation`) to enforce the critical safety invariant:
$$\textbf{"NO UNBOUNDED AUTONOMOUS REMEDIATION LOOP."}$$

Phase 6.7 bounds the retry decisions produced by the escalation policy and prevents the escalation engine from authorizing retries beyond the configured budget.

### 1. Architectural Pipeline & Orchestration Boundary
Phase 6.7 establishes an explicit architectural boundary between domain decision evaluation and mitigation re-execution:

```
Mitigation / Verification Failure
        ↓
Escalation Engine
        ↓
Retry Decision OR Escalation Decision
        ↓
[ORCHESTRATION BOUNDARY - Future / Not Implemented]
        ↓
Future/next orchestration layer initiates bounded retry
        OR
Operator escalation
```

> [!IMPORTANT]
> **Orchestration Component Boundary**: The orchestration component that consumes retry decisions and re-triggers mitigation is future/not implemented. Phase 6.7 does NOT implement the complete autonomous closed-loop retry execution loop; it implements the deterministic escalation domain services that evaluate failures and authorize bounded retry decisions (`DecisionActionRetry`) or trigger escalation (`DecisionActionEscalate`).

### 2. Failure Classifications
Phase 6.7 formalizes 7 deterministic failure classifications:
- `MITIGATION_FAILED`: The simulated mitigation action failed during execution.
- `VERIFICATION_TIMED_OUT`: The closed-loop verification observation window elapsed without satisfying recovery criteria.
- `VERIFICATION_REJECTED`: Verification observed immediate negative regression or explicit policy rejection, requiring immediate escalation.
- `UNKNOWN_RECONCILIATION_REQUIRED`: The actuator or execution engine reached an unknown state, requiring immediate operator investigation; autonomous retry decisions are strictly forbidden.
- `COOLDOWN_ACTIVE`: An autonomous retry decision was evaluated while the cooldown timer has not yet expired; execution is held.
- `RETRY_BUDGET_EXHAUSTED`: The maximum number of automatic mitigation retry decisions has been reached; autonomous retry decisions cease.
- `CIRCUIT_BREAKER_OPEN`: Repeated failures within the sliding window reached the failure threshold, tripping the circuit breaker to `OPEN`.

### 3. Circuit Breaker Semantics
- **State Transition**: strictly `CLOSED` $\rightarrow$ `OPEN` $\rightarrow$ explicit operator reset $\rightarrow$ `CLOSED`.
- **Fail-Closed**: Once the circuit breaker is `OPEN`, ALL autonomous remediation retry decisions on the node and incident are strictly prohibited.
- **Explicit Reset**: Resetting the breaker requires operator identity (`reset_by`). No automated timer resets `OPEN` back to `CLOSED` (no half-open state in Phase 6.7).
- **Restart Survival**: The `OPEN` state is persisted durably in the `circuit_breaker_states` SQLite table and survives process crashes and restarts.

### 4. Deterministic Identity Formula
Escalation identity is computed via SHA-256 over:
$$\text{IncidentID} \mid \text{NodeID} \mid \text{ActionID} \mid \text{FailureClassification} \mid \text{PolicyVersion} \mid \text{Attempt}$$
This contains zero random bytes and zero timestamps, guaranteeing identical, idempotent IDs across concurrent evaluations and retries.

### 5. SQLite WAL Persistence (Migration v7)
- `escalation_records`: Durably logs every escalation and retry attempt with classification, attempt counter, status, reason, evidence, and timestamps.
- `circuit_breaker_states`: Durably tracks breaker state (`CLOSED`/`OPEN`), failure count, tripped timestamp, and operator reset details.

### 6. Authoritative Incident Engine Handoff
- **Authority**: The canonical Incident FSM (`NORMAL -> ANOMALY_DETECTED -> MITIGATING / ESCALATED -> RECOVERED -> NORMAL`) remains authoritative.
- Escalation engine calls `IncidentEngine.TransitionActiveIncident(..., types.StatusEscalated, now)`.
- It never directly mutates `Incident.Status` and never bypasses IncidentEngine validation.
- If the incident is already `ESCALATED`, handoff is a safe idempotent no-op.
- If the incident is in `RECOVERED` or `NORMAL`, the handoff does not forge invalid transitions.

### 7. Retry Semantics & Scope
- **Retry Semantics**: Phase 6.7 permits at most three retry decisions after the initial failed attempt. For `MaxAutomaticRetries = 3`, this defines:
  $$\text{1 initial mitigation attempt} + \text{up to 3 automatic retry decisions} = \text{maximum 4 mitigation executions}$$
  if the future orchestration layer honors all retry decisions. The invariant currently enforced by Phase 6.7 is that it cannot issue more than the configured retry budget; Phase 6.7 does not claim that four executions have actually been performed by Phase 6.7.
- **Circuit Breaker Scope**: The Phase 6.7 circuit breaker is scoped to the node/incident pair `(NodeID, IncidentID)`. Opening the circuit for Incident A on Node 1 does not affect Incident B on Node 1, nor Incident A on Node 2.
- **Integration Boundary**: Phase 6.7 provides `EvaluateFailure`, `EvaluateMitigationResult`, and `EvaluateVerificationResult` as domain services. Automatic end-to-end chaining across separate pipeline packages is designed for subsequent orchestration phases.

### 8. Current vs. Future Implementation Matrix

| Capability | Current Status | Description |
| :--- | :---: | :--- |
| **Escalation Domain Model** | **CURRENTLY IMPLEMENTED** | Typed failure classifications, decisions, and policy structures |
| **Deterministic Failure Classification** | **CURRENTLY IMPLEMENTED** | 7 domain classifications mapping execution/verification outcomes |
| **Retry Budget Evaluation** | **CURRENTLY IMPLEMENTED** | Bounds retry decisions to `MaxAutomaticRetries` |
| **Cooldown Evaluation** | **CURRENTLY IMPLEMENTED** | Evaluates cooldown elapsed state; suppresses premature failure count increments |
| **Sliding Failure Window** | **CURRENTLY IMPLEMENTED** | Tracks failures within configurable window |
| **Circuit Breaker** | **CURRENTLY IMPLEMENTED** | Scoped to `(NodeID, IncidentID)`, trips to `OPEN`, explicit operator reset |
| **Durable Escalation State (Migration v7)** | **CURRENTLY IMPLEMENTED** | SQLite WAL tables `escalation_records` and `circuit_breaker_states` |
| **Deterministic Escalation Identity** | **CURRENTLY IMPLEMENTED** | SHA-256 ID over tuple; zero random bytes |
| **Restart Reconstruction** | **CURRENTLY IMPLEMENTED** | Restores failure counts, cooldown timestamps, and `OPEN` state from DB |
| **IncidentEngine Escalation Handoff** | **CURRENTLY IMPLEMENTED** | Invokes canonical `TransitionActiveIncident` preserving Incident FSM |
| **Bounded Retry Decisions** | **CURRENTLY IMPLEMENTED** | Produces `DecisionActionRetry` with timestamp and budget tracking |
| **Automatic Orchestration of RETRY Decisions** | *FUTURE / NOT IMPLEMENTED* | Chaining `DecisionActionRetry` into automatic mitigation re-execution |
| **Full End-to-End Autonomous Retry Loop** | *FUTURE / NOT IMPLEMENTED* | Autonomous loop driving mitigation, verification, and retry orchestration |
| **Real Host Actuation** | *FUTURE / NOT IMPLEMENTED* | Actuation remains strictly simulated (zero OS commands, processes, containers) |
| **External Operator Notification** | *FUTURE / NOT IMPLEMENTED* | PagerDuty, Webhooks, Slack, or email operator alerting |
| **Real Infrastructure Remediation** | *FUTURE / NOT IMPLEMENTED* | No live cloud, cluster, or host remediation |

### 9. Failure Handling & Resilience Matrix (16 Conditions)

| # | Failure / Edge Condition | Engine Behavior | Circuit Breaker Impact | Incident FSM Impact |
| :---: | :--- | :--- | :--- | :--- |
| **1** | **Mitigation Failure** | **RETRY** — bounded retry decision permitted; actual re-execution is performed by the orchestration layer. | Remains `CLOSED` unless threshold reached | Remains `MITIGATING` |
| **2** | **Verification Timeout** | **RETRY** — bounded retry decision permitted; actual re-execution is performed by the orchestration layer. | Remains `CLOSED` unless threshold reached | Remains `MITIGATING` |
| **3** | **Verification Rejection** | **ESCALATE** — escalation decision generated and IncidentEngine handoff may occur (`VERIFICATION_REJECTED`). | Remains `CLOSED` (isolated policy event) | Transitions to `ESCALATED` |
| **4** | **Cooldown Active** | Returns `DecisionActionNone` and `COOLDOWN_ACTIVE`; does NOT increment failure count. | Unchanged | Unchanged |
| **5** | **Retry Budget Exhausted** | **ESCALATE** — escalation decision generated and IncidentEngine handoff may occur (`RETRY_BUDGET_EXHAUSTED`). | Trips to `OPEN` | Transitions to `ESCALATED` |
| **6** | **Circuit Open** | **ESCALATE** — autonomous remediation blocked; returns `CIRCUIT_BREAKER_OPEN`. | Remains `OPEN` until operator reset | Transitions to `ESCALATED` |
| **7** | **Duplicate Failure** | Idempotently recognized by deterministic SHA-256 `EscalationID`. | Unchanged | Unchanged |
| **8** | **Restart During Cooldown** | Cooldown timestamp reconstructed from SQLite; holds retry decisions until expiry. | Unchanged | Unchanged |
| **9** | **Restart While Circuit Open** | Breaker `OPEN` state reconstructed from SQLite; blocks remediation decisions immediately. | Remains `OPEN` | Unchanged |
| **10** | **Stale Failure for Recovered Incident** | Handoff checks canonical FSM; `RECOVERED -> ESCALATED` is rejected. | Unchanged | FSM rejects transition; stays `RECOVERED` |
| **11** | **Stale Failure for Normal Incident** | Handoff checks canonical FSM; `NORMAL -> ESCALATED` is rejected. | Unchanged | FSM rejects transition; stays `NORMAL` |
| **12** | **Unknown Mitigation State** | **ESCALATE** — escalation decision generated (`UNKNOWN_RECONCILIATION_REQUIRED`); retry decision forbidden. | Remains `CLOSED` | Transitions to `ESCALATED` |
| **13** | **Corrupted Escalation Record** | Store validation rejects malformed payload with `ErrInvalidEscalation`. | Unchanged | Unchanged |
| **14** | **Concurrent Duplicate Escalation** | Mutex serializes evaluation; unique index in SQLite prevents duplicate records. | Consistently trips if threshold reached | Transitions once idempotently |
| **15** | **SQLite Storage Unavailable** | Fail-closed: storage errors reject evaluation; no unpersisted retry decisions permitted. | Fails closed | Holds active state |
| **16** | **Invalid Policy Config** | `Validate()` rejects negative cooldown, negative retries, or zero window at init. | Engine initialization fails closed | Unchanged |

### 10. Strict Safety Boundaries & Non-Claims
- **Zero Real Host Actuation**: All executions remain simulated.
- **Simulation Boundary Maintained**: No OS commands, shell processes, or container mutations.
- **Durability Non-Claims**: Local WAL crash recovery under `synchronous=NORMAL`; no claims of zero-data-loss, hardware failure resilience, or distributed consensus.
- **No Unbounded Loop Invariant**: Phase 6.7 bounds the retry decisions produced by the escalation policy and prevents the escalation engine from authorizing retries beyond the configured budget.

---

## 23. Verification Status

As of Phase 6.8, all 20 packages across the Go workspace compile, pass static analysis (`go vet`), and pass all automated unit and integration tests (`go test -count=1 -p 1 ./...`) cleanly.

---

## 24. Phase 6.8: Controlled Incident Response Orchestrator

### 1. Objective & Architectural Purpose
Phase 6.8 implements the controlled orchestration layer (`edge/agent/orchestrator`) that connects the decoupled response pipeline components into a bounded, deterministic, fail-closed incident response workflow:
$$\text{TELEMETRY} \longrightarrow \text{ANOMALY DETECTION} \longrightarrow \text{INCIDENT CORRELATION} \longrightarrow \text{RESPONSE POLICY} \longrightarrow \text{SAFETY VALIDATION} \longrightarrow \text{MITIGATION EXECUTION} \longrightarrow \text{VERIFICATION} \longrightarrow \text{ESCALATION} \longrightarrow \text{ORCHESTRATION DECISION}$$

### 2. Orchestration State Model
The orchestrator maintains an orchestration-specific state model completely decoupled from the canonical `IncidentStatus` FSM:
- `PENDING`: Initial state prior to cycle execution.
- `RUNNING`: Orchestration cycle in active progress.
- `WAITING_APPROVAL`: Policy classified action as `APPROVAL_REQUIRED`; awaiting externally supplied operator authorization. Zero mitigation executes.
- `EXECUTING`: Validated candidate action currently undergoing simulated execution.
- `VERIFYING`: Action completed with status `EXECUTED`; closed-loop recovery observation is actively tracking subsequent telemetry.
- `RETRY_AUTHORIZED`: Escalation engine evaluated a mitigation or verification failure and authorized a bounded retry (`DecisionActionRetry`).
- `ESCALATED`: Escalation engine returned `DecisionActionEscalate` or circuit tripped; incident safely handed off to `IncidentEngine.TransitionActiveIncident(..., StatusEscalated)`.
- `RECOVERED`: Telemetry verification satisfied recovery condition; incident safely resolved via `IncidentEngine.ResolveIncident(...)`.
- `FAILED`: Cycle terminated due to safety rejection, persistence failure, or exhausted retry/cooldown constraints.
- `CANCELLED`: Operation stopped due to context cancellation.

### 3. Bounded Retry & Hard Ceiling Semantics
- **Reused Budget**: The orchestrator strictly reuses the Phase 6.7 retry budget (`MaxAutomaticRetries = 3`), permitting at most 1 initial execution + up to 3 retry decisions = maximum 4 mitigation executions.
- **No Independent Retries**: The orchestrator never independently initiates a retry. A retry occurs if and only if `EscalationEngine.Evaluate*` produces `DecisionActionRetry` and `CanRetry` confirms no cooldown or tripped circuit breaker.
- **Defense-in-Depth Ceiling**: A hard safety ceiling (`MaxExecutionCeiling = 4`) protects against programming errors or corrupted loop counters without overriding the configured escalation policy. The hard ceiling of 4 is defense-in-depth and agrees with the configured maximum of 3 automatic retries. It cannot authorize an additional execution beyond the escalation policy.
- **Cooldown & Breaker Respect**: If `CanRetry` returns false due to active cooldown or an `OPEN` circuit breaker, execution halts immediately without incrementing the failure count.

### 4. Approval & Safety Validation Boundaries
- **Approval Gate**: For `APPROVAL_REQUIRED` actions, the orchestrator halts in `WAITING_APPROVAL` unless a valid approval record is provided. The orchestrator never automatically approves its own requests.
- **Execution Validation**: If an approval is provided, `ValidateApprovalForExecution` enforces the 14-point decision-binding gate (fingerprint, timestamps, action, target, incident, node). Expired, tampered, or mismatched approvals fail closed.
- **Authoritative Pre-Execution Validation**: Every execution attempt crosses the `SafetyValidator` boundary immediately before execution. No action executes following a failed safety check.
- **Persistence Precedes Actuation**: Durable persistence precedes simulated actuation. The orchestrator requires the mitigation record to be successfully persisted before invoking the executor. If persistence fails, the executor is not invoked, orchestration fails closed, and no retry is fabricated locally.

### 5. Idempotency & Concurrency Control
- **Per-Incident In-Flight Gate**: Internal mutex and `inFlight map[string]*inFlightOrchestration` ensure that concurrent invocations for the same `IncidentID` are serialized. Exactly one leader executes the workflow, and all concurrent callers await the leader and observe the identical logical result.
- **Incident Stream Isolation**: Concurrent orchestrations on different incidents execute concurrently without state interference.
- **Restart Recovery & Deduplication**: Uses durable SQLite mitigation and verification records to detect previously recorded logical attempts across supported restart-recovery paths and suppress duplicate orchestration where persisted state is available.

### 6. Failure Safety Matrix (20 Conditions)

| # | Failure / Edge Condition | Execution Occurs? | Persistence Changes? | Retry Permitted? | IncidentEngine Called? | Resulting Orchestration State |
| :---: | :--- | :---: | :---: | :---: | :---: | :---: |
| **1** | Policy returns NO_ACTION | No | No | No | No | `FAILED` / Stopped (Incident state unchanged) |
| **2** | Policy returns FORBIDDEN | No | No | No | No | `FAILED` (`ErrForbiddenAction`) |
| **3** | Safety validation fails | No | No | No | No | `FAILED` (`ErrSafetyValidationFailed`) |
| **4** | Approval missing | No | Yes (Pending ticket in store) | No | No | `WAITING_APPROVAL` (`ErrApprovalMissing`) |
| **5** | Approval expired | No | Yes (Escalation failure logged) | No | No | `FAILED` (`ErrApprovalInvalid`) |
| **6** | Approval fingerprint mismatch | No | Yes (Escalation failure logged) | No | No | `FAILED` (`ErrApprovalInvalid`) |
| **7** | Persistence failure | No | No | No | No | `FAILED` (`ErrPersistenceFailed`) |
| **8** | Executor failure | Yes (Simulated) | Yes (`mitigation_records` FAILED) | If budget & cooldown permit | If ESCALATE decided | `RETRY_AUTHORIZED` or `ESCALATED` |
| **9** | Context cancellation | No | No | No | No | `CANCELLED` (`ErrOrchestrationCancelled`) |
| **10** | Verification timeout | No | Yes (`verification_records` TIMED_OUT) | If budget & cooldown permit | If ESCALATE decided | `RETRY_AUTHORIZED` or `ESCALATED` |
| **11** | Verification rejection | No | Yes (`verification_records` NOT_RECOVERED) | No (`VERIFICATION_REJECTED`) | Yes (`StatusEscalated`) | `ESCALATED` |
| **12** | Escalation says NONE | No | No | No | No | `FAILED` |
| **13** | Escalation says RETRY | Yes (Next attempt) | Yes (Next attempt logged) | Yes (Bounded attempt) | No | `RETRY_AUTHORIZED` -> `EXECUTING` |
| **14** | Escalation says ESCALATE | No | Yes (`escalation_records` ESCALATED) | No | Yes (`StatusEscalated`) | `ESCALATED` |
| **15** | Retry budget exhausted | No | Yes (Circuit trips to OPEN) | No | Yes (`StatusEscalated`) | `ESCALATED` |
| **16** | Circuit breaker OPEN | No | Yes (Escalation failure logged) | No | Yes (`StatusEscalated`) | `ESCALATED` (`ErrCircuitBreakerOpen`) |
| **17** | Duplicate orchestration request | No | No (Returns cached result) | No | No | Cached Terminal State |
| **18** | Concurrent duplicate requests | Exactly 1 Leader | Leader persists | Leader manages | Leader hands off | All callers receive identical result |
| **19** | Stale / NORMAL incident | No | No | No | No | `RECOVERED` (Suppressed) |
| **20** | Unknown reconciliation state | No | No | No | No | `FAILED` (`ErrUnknownReconciliation`) |

### 7. Critical Safety Invariants
1. **INVARIANT 1**: Zero arbitrary host command execution (`os/exec`, `exec.Command`, PowerShell, shell, SSH, Docker CLI, kubectl, AWS CLI prohibited).
2. **INVARIANT 2**: No retry occurs unless `EscalationEngine` authorizes `DecisionActionRetry`.
3. **INVARIANT 3**: Retry count cannot exceed configured budget (`MaxAutomaticRetries = 3`).
4. **INVARIANT 4**: Safety validation is required before every execution attempt.
5. **INVARIANT 5**: Approval-required actions cannot bypass approval.
6. **INVARIANT 6**: Execution success does not imply incident recovery.
7. **INVARIANT 7**: Verification must evaluate subsequent telemetry evidence.
8. **INVARIANT 8**: Canonical Incident FSM is owned exclusively by `IncidentEngine`.
9. **INVARIANT 9**: Durable persistence precedes simulated actuation. The orchestrator requires the mitigation record to be successfully persisted before invoking the executor. If persistence fails: executor is not invoked, orchestration fails closed, and no retry is fabricated locally.
10. **INVARIANT 10**: No unbounded autonomous remediation loop (`MaxExecutionCeiling = 4` defense-in-depth, strictly enforcing at most 4 total executions: 1 initial + up to 3 retries).
11. **INVARIANT 11**: No real host, container, or cloud actuation; actuation remains strictly simulated (`SIMULATED_*`).

### 8. Current vs. Future Implementation Matrix

| Capability | Current Status | Description |
| :--- | :---: | :--- |
| **Controlled Response Orchestrator** | **CURRENTLY IMPLEMENTED** | Coordinates full cycle from policy to escalation in `edge/agent/orchestrator` |
| **Deterministic Policy & Validator Wiring** | **CURRENTLY IMPLEMENTED** | Connects `ResponsePolicy`, `SafetyValidator`, and `SimulatedExecutor` |
| **Approval Workflow Integration** | **CURRENTLY IMPLEMENTED** | Halts at `WAITING_APPROVAL`; validates externally provided approval records |
| **Durable Mitigation Persistence** | **CURRENTLY IMPLEMENTED** | Persists execution lifecycle into SQLite WAL `mitigation_records` |
| **Closed-Loop Verification Integration** | **CURRENTLY IMPLEMENTED** | Ingests subsequent telemetry; confirms recovery on $N$ healthy samples |
| **Bounded Retry Orchestration** | **CURRENTLY IMPLEMENTED** | Re-executes attempts up to `MaxAutomaticRetries` under `DecisionActionRetry` |
| **Fail-Closed Circuit & Cooldown Handling** | **CURRENTLY IMPLEMENTED** | Halts execution if breaker is OPEN or cooldown is active |
| **Leader-Follower Concurrency Gate** | **CURRENTLY IMPLEMENTED** | Serializes duplicate concurrent requests into a single logical execution |
| **Real Host Actuators** | *FUTURE / NOT IMPLEMENTED* | Shell, systemctl, reboot, iptables, process termination |
| **Cloud / Container Remediation** | *FUTURE / NOT IMPLEMENTED* | Docker, Kubernetes, AWS, GCP, Azure actuators |
| **External Operator Alerting** | *FUTURE / NOT IMPLEMENTED* | PagerDuty, Webhooks, Slack, email notifications |
| **Authenticated Operator Identity** | *FUTURE / NOT IMPLEMENTED* | Cryptographic signatures, mTLS, JWT, SSO, non-repudiation |
| **Signed Approval Tokens** | *FUTURE / NOT IMPLEMENTED* | Cryptographically signed approval manifests |
| **Advanced Statistical / ML Verification** | *FUTURE / NOT IMPLEMENTED* | Dynamic statistical streak baselining and multivariate verification |
| **Distributed Multi-Node Orchestration** | *FUTURE / NOT IMPLEMENTED* | Raft/Paxos consensus across distributed edge clusters |

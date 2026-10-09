# ADR-0017: Secure Edge–Control-Plane Communication

**Status**: Accepted (Phase 6.15)  
**Date**: 2026-10-09  
**Deciders**: AegisEdge Core Engineering Team  
**Supersedes**: None  
**Relates to**: ADR-0002 (Edge-to-Control-Plane Communication), ADR-0005 (HTTP Synchronization), ADR-0012 (Autonomous Incident Response Architecture), ADR-0013 (Edge Agent Health and Readiness Model), ADR-0014 (Graceful Shutdown & Runtime Lifecycle), ADR-0015 (Persistent Runtime State & Recovery), ADR-0016 (Edge-Control-Plane Coordination Layer)

---

## 1. Context & Problem Statement

Phase 6.14 introduced node identity persistence, dynamic enrollment, heartbeat synchronization, and lifecycle coordination between edge agents and the central control plane. However, communication across this boundary operated unauthenticated and without cryptographic integrity verification or replay protection:

1. **Spoofing & Impersonation**: A malicious or misconfigured client could submit telemetry batches, register rogue nodes, or send fabricated heartbeats claiming to be any valid `node_id`.
2. **Replay Attacks**: Valid registration or heartbeat payloads could be captured in transit and replayed indefinitely to deceive the control plane regarding edge liveness and status.
3. **Information Disclosure & Privilege Escalation**: Any connected edge node could query the full inventory of all other edge nodes (`GET /api/v1/nodes`) and read all accepted telemetry batches (`GET /api/v1/telemetry/batches`).
4. **Offline Autonomy Invariant**: While security controls are critical when network communication occurs, **authentication or control plane outages must never compromise edge autonomy**. If the control plane rejects an edge agent's credentials or becomes unreachable, the edge node must continue local telemetry sampling, anomaly detection, incident FSM transitions, and safe mitigations without interruption.

Phase 6.15 introduces a secure, authenticated communication layer based on HMAC-SHA256 request signing, sliding-window replay defense, granular endpoint authorization, and transport security, strictly adhering to standard library cryptography and offline-first autonomy.

---

## 2. Core Invariant

```
EDGE OPERATES AUTONOMOUSLY OFFLINE
               ↓
        PERSISTS LOCALLY
               ↓
AUTHENTICATES WHEN COMMUNICATION IS AVAILABLE
               ↓
      COORDINATES SECURELY
               ↓
  REMAINS SAFE IF CP IS UNAVAILABLE
```

> [!IMPORTANT]
> **OFFLINE-FIRST SAFETY GUARANTEE:**  
> Authentication failures (HTTP 401/403) or control plane outages **MUST NOT** block local telemetry generation, SQLite WAL persistence, anomaly detection, incident response, or agent readiness (`/readyz`). The edge coordinator transitions to `StateDegraded`, records `CONTROL_PLANE_AUTH_FAILED` audit events, increments failure metrics, and schedules exponential backoff while local operations proceed uninterrupted.

---

## 3. Cryptographic Architecture & Threat Model

### 3.1 Standard Library Cryptography

In accordance with strict security standards, AegisEdge avoids custom cryptographic protocols. The implementation relies exclusively on Go standard library primitives:
- `crypto/hmac` and `crypto/sha256`: Hash-based message authentication.
- `crypto/subtle.ConstantTimeCompare`: Side-channel and timing attack mitigation for signatures and admin tokens.
- `crypto/rand`: Cryptographically secure pseudorandom number generation for 16-byte nonces.

### 3.2 Canonical Request String Construction

To guarantee request integrity and prevent tampering of methods, routes, query parameters, node identity, headers, or payloads, HMAC-SHA256 signatures are calculated over a deterministic canonical representation:

```
CANONICAL_STRING =
    METHOD + "\n" +
    REQUEST_TARGET + "\n" +
    NODE_ID + "\n" +
    TIMESTAMP + "\n" +
    NONCE + "\n" +
    HEX_SHA256_OF_BODY
```

Where:
- `METHOD`: Normalized uppercase HTTP verb (e.g., `POST`, `GET`).
- `REQUEST_TARGET`: Request URI path including raw query parameters (e.g., `/api/v1/nodes/alpha?status=active`).
- `NODE_ID`: Claimed edge node identifier (e.g., `edge-node-01`), cryptographically binding the claimed identity to the signature.
- `TIMESTAMP`: UTC timestamp formatted as RFC3339 (e.g., `2026-10-09T05:00:00Z`).
- `NONCE`: Cryptographically random 16-byte hex-encoded string (32 characters).
- `HEX_SHA256_OF_BODY`: Lowercase hexadecimal SHA-256 digest of the raw request payload (for empty or GET bodies, the SHA-256 digest of 0 bytes: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`).

### 3.3 Security Headers & Credential Management

Authenticated edge requests supply four standardized HTTP headers:
- `X-AegisEdge-Node-ID`: The claimed node identifier.
- `X-AegisEdge-Timestamp`: The request generation timestamp in RFC3339 format.
- `X-AegisEdge-Nonce`: Unique per-request nonce.
- `X-AegisEdge-Signature`: Hex-encoded HMAC-SHA256 digest computed using the provisioned symmetric key.

#### Credential Isolation Models
1. **Per-Node Credentials (Recommended)**: The control plane supports static per-node secret mappings (`NodeSecrets map[string]string`) or dynamic lookup callbacks (`SecretResolver func(nodeID string) ([]byte, error)`). Each node holds an independent key ($\ge 16$ characters). A compromised node cannot forge signatures for any other node.
2. **Fleet Shared Secret (Legacy / Flat Fleet)**: If a single fleet-wide secret is configured (`SharedSecret`), all nodes share one symmetric key. 
   - *Threat Model Limitation*: A compromised node possessing the fleet secret can forge signatures claiming other node identities unless per-node keys are provisioned. For production environments with multi-tenant or untrusted edge hardware, per-node secrets MUST be used.

---

## 4. Anti-Replay Mitigation & DoS Hardening

To prevent adversaries from capturing and replaying valid signed requests, the Control Plane enforces dual sliding-window validation with strict order of operations:

1. **Verification Order & DoS Defense**:
   - Verification occurs in strict pipeline order:
     1. Validate bounded request size and required security headers.
     2. Validate timestamp freshness against clock skew window.
     3. Resolve symmetric key for the claimed `node_id`.
     4. **Cryptographic HMAC-SHA256 signature verification** in constant time. Return `ErrInvalidSignature` immediately without touching the replay cache if invalid.
     5. **Atomic Check and Add to Nonce Cache (`ReplayCache`)**: Only valid, authenticated nonces are recorded.
   - *Security Benefit*: Unauthenticated attackers sending malformed signatures CANNOT consume replay cache memory, cannot induce premature cache eviction, and cannot front-run or pre-register nonces to deny service to legitimate nodes.

2. **Timestamp Freshness & Clock Skew Window**:
   - The server verifies: `|ServerTime - RequestTime| <= MaxClockSkew` (default: 5 minutes).
   - Requests with expired timestamps are rejected with HTTP 401 (`expired_timestamp`).
   - Requests too far in the future are rejected with HTTP 401 (`future_timestamp`).

3. **Bounded In-Memory Nonce Cache (`ReplayCache`)**:
   - A thread-safe, mutex-protected `ReplayCache` stores nonces mapped to expiration timestamps (`RequestTime + MaxClockSkew`).
   - Novel nonces are recorded atomically under lock; re-used nonces are rejected with HTTP 401 (`replay_detected`).
   - Once entries pass their expiration timestamp, they are pruned during capacity reviews.
   - To prevent memory exhaustion denial-of-service, the cache enforces a maximum capacity ceiling (default: 100,000 entries) and fails closed if capacity is saturated.

---

## 5. Granular Endpoint Authorization & Anti-Impersonation

The Control Plane categorizes endpoints into discrete privilege tiers:

| Endpoint | Method | Required Authorization | Access Rule |
| :--- | :--- | :--- | :--- |
| `/healthz` | `GET` | Public | Unrestricted (service liveness check) |
| `/api/v1/nodes/register` | `POST` | Node HMAC or Admin Bearer | `authCtx.NodeID == payload.NodeID` |
| `/api/v1/nodes/heartbeat` | `POST` | Node HMAC or Admin Bearer | `authCtx.NodeID == payload.NodeID` |
| `/api/v1/telemetry/batches` | `POST` | Node HMAC or Admin Bearer | `authCtx.NodeID == payload.NodeID` |
| `/api/v1/nodes/{id}` | `GET` | Node HMAC or Admin Bearer | Allowed if `authCtx.IsAdmin` OR `authCtx.NodeID == id` |
| `/api/v1/nodes` (list all) | `GET` | Admin Bearer token only | HTTP 403 Forbidden for edge node credentials |
| `/api/v1/telemetry/batches` (list) | `GET` | Admin Bearer token only | HTTP 403 Forbidden for edge node credentials |

### Anti-Impersonation Defense
- On registration, the Control Plane ensures `Header(X-AegisEdge-Node-ID) == Payload(NodeID)`.
- On heartbeats, the Control Plane ensures `Header(X-AegisEdge-Node-ID) == Payload(NodeID)`.
- On single node queries, a node with ID `node-a` is forbidden from inspecting records belonging to `node-b`.
- Mismatches return HTTP 403 Forbidden with clear error messages.
- Admin Bearer tokens are validated with constant-time equality and cannot fall through to edge node evaluation.

---

## 6. Transport Layer Security (TLS) & Fail-Safe Configuration

### 6.1 Fail-Safe Configuration Validation (`AuthConfig.Validate`)
To eliminate accidental unauthenticated deployments, the server enforces strict startup validation:
1. **Production & Non-Loopback Enforcement**: When binding to non-loopback network addresses (e.g. `0.0.0.0`, public IPs, wildcard `":8080"`) or when `ENV=production`, authentication cannot be disabled (`AuthEnabled` MUST be `true`) and `AdminToken` is mandatory.
2. **Key Strength Enforcement**: Symmetric secrets (`SharedSecret`, `NodeSecrets`) and `AdminToken` must be at least 16 characters in length.
3. **Safe Error Messages**: Configuration error messages report the violation policy without logging raw credentials or secret values.

### 6.2 Transport Layer Security (TLS) Transport & Confidentiality
For deployments spanning untrusted networks, the control plane supports HTTPS via TLS certificates and keys:
- `-tls-cert` / `TLS_CERT_FILE`
- `-tls-key` / `TLS_KEY_FILE`

> [!IMPORTANT]
> **PAYLOAD CONFIDENTIALITY NOTICE:**  
> HMAC-SHA256 provides message integrity, origin authenticity, and replay protection, but does **NOT** encrypt payloads or headers. Non-loopback production deployments **MUST** terminate TLS (HTTPS) to prevent passive eavesdropping of telemetry data, node metadata, and authentication nonces in transit.

#### Fail-Closed Validation
Partial TLS configurations (providing a certificate without a private key, or vice-versa) are strictly rejected at startup via `ValidateTLSConfig`, returning `ErrIncompleteTLSConfig` and exiting immediately.

#### Development Mode Compatibility
When running locally on loopback addresses (`127.0.0.1`, `localhost`, `[::1]`) outside production, unauthenticated development mode is permitted for local developer ergonomics, logging explicit warnings.

---

## 7. Metrics & Explainability Audit Trail

### 7.1 Prometheus Metrics
- `aegisedge_control_plane_auth_failures_total`: Total number of control plane authentication failures partitioned by sanitized discrete reason:
  - `invalid_signature`
  - `expired_timestamp`
  - `future_timestamp`
  - `replay_detected`
  - `forbidden`
  - `missing_auth`
  - `unauthorized`
  - `unknown`

### 7.2 Durable Audit Trail
When an edge node encounters an authentication failure (HTTP 401 or 403), the edge Coordinator logs a structured, non-repudiable audit event:
- `EventType`: `CONTROL_PLANE_AUTH_FAILED`
- `Result`: `failed`
- `IncidentID`: `system-coordination`
- `Reason`: Sanitized HTTP error description (excluding credentials or signature secrets).

---

## 8. Verification & Quality Gates

The implementation is verified through:
1. `shared/types/auth_test.go`: Unit tests for canonical string derivation, signature signing, constant-time verification, timestamp parsing, nonce generation, and admin token matching.
2. `services/control-plane/server/auth_test.go`: 19 end-to-end HTTP integration tests covering public endpoints, authenticated requests, tampered payloads/verbs/routes, expired/future timestamps, replay detection, concurrent replay race conditions, anti-impersonation, admin authorization boundaries, and TLS validation.
3. `edge/agent/coordination/coordination_test.go`: Client-side request signing, invalid secret handling, and safe offline degradation.

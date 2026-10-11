# ADR-0018: Security Regression & Adversarial Testing Suite

**Status**: Accepted (Phase 6.16)  
**Date**: 2026-10-11  
**Deciders**: AegisEdge Core Engineering Team  
**Supersedes**: None  
**Relates to**: ADR-0002 (Edge-to-Control-Plane Communication), ADR-0005 (HTTP Synchronization), ADR-0012 (Autonomous Incident Response Architecture), ADR-0016 (Edge-Control-Plane Coordination Layer), ADR-0017 (Secure Edge–Control-Plane Communication)

---

## 1. Context & Problem Statement

Phase 6.15 and 6.15.1 established HMAC-SHA256 request authentication, canonical string derivation binding node identities and request targets, per-node credential isolation, sliding-window anti-replay, and strict fail-closed production validation.

However, complex distributed security architectures require continuous, deterministic adversarial regression suites to prevent subtle security regressions during future development. Furthermore, security boundaries, testing limits, and operational assumptions must be documented transparently without overstating testing guarantees.

Phase 6.16 introduces a deterministic security regression suite that rigorously exercises adversarial attack vectors, replay defenses, resource boundary saturations, CLI vs environment precedence, and offline-first autonomy invariants.

---

## 2. Security Properties Covered by Automated Tests

The Phase 6.16 test suite covers the following security properties:

### 2.1 Cryptographic Request Integrity & Binding
- **Method Tampering**: Requests signed with one HTTP verb (e.g. `POST`) cannot be altered in flight to another verb (`PUT`, `GET`), returning HTTP `401 Unauthorized` or `405 Method Not Allowed`.
- **Query Parameter Binding**: Tampering with or appending query strings (e.g. `?injected=malicious`) invalidates the signature, returning HTTP `401 Unauthorized`.
- **Node Identity Binding**: Tampering with the `X-AegisEdge-Node-ID` header invalidates the signature, returning HTTP `401 Unauthorized`.
- **Payload Integrity**: Modifying any byte of the request payload invalidates the SHA-256 body digest and HMAC signature, returning HTTP `401 Unauthorized`.
- **Nonce & Timestamp Integrity**: Mutating either the nonce or timestamp header after signing breaks verification, returning HTTP `401 Unauthorized`.

### 2.2 Freshness & Anti-Replay Mitigation
- **Single-Use Nonce Acceptance**: Valid nonces are accepted exactly once.
- **Duplicate Nonce Rejection**: Resending an identical nonce within the clock skew window returns HTTP `401 Unauthorized` (`replay_detected`).
- **No Nonce Pre-Registration / Cache Pollution**: Requests with invalid HMAC signatures do **NOT** mutate the replay cache. Attackers cannot pre-register nonces or front-run legitimate nodes to cause denial of service.
- **Cache Capacity Ceiling**: The memory-bounded `ReplayCache` enforces an upper capacity limit (`DefaultMaxReplayKeys = 100,000`) and fails closed when saturated, preventing memory exhaustion attacks.

### 2.3 Credential Isolation & Anti-Impersonation
- **Per-Node Isolation**: Each node possesses a dedicated symmetric key. Node Alpha cannot forge requests for Node Beta.
- **Anti-Impersonation Checks**: Even if a node signs with its valid key, registration (`POST /api/v1/nodes/register`) and heartbeat (`POST /api/v1/nodes/heartbeat`) endpoints enforce that `Header(NodeID) == Payload(NodeID)`. Mismatches return HTTP `403 Forbidden`.
- **Single-Node Inspection Isolation**: A node authenticated as `node-alpha` is forbidden from querying `/api/v1/nodes/node-beta`, returning HTTP `403 Forbidden`.
- **Zero Shared-Secret Fallback**: When per-node credentials (`NodeSecrets` or `SecretResolver`) are configured, failed node lookups or resolver errors return HTTP `401 Unauthorized` immediately and **never fall through to `SharedSecret`**.
- **Production Shared-Secret Prohibition**: In production (`ENV=production` / `ENVIRONMENT=production`), non-empty `SharedSecret` is strictly rejected at startup (`Validate`), failing closed before listener creation.

### 2.4 Resource Limits & Malformed Input Handling
- **Request Body Bounding**: Requests exceeding `MaxRequestBodySize` (1 MB) return HTTP `413 Payload Too Large`.
- **Malformed & Trailing JSON**: Trailing garbage bytes and malformed syntax fail safely with HTTP `400 Bad Request` without server crashes or panics.
- **Node Secrets File Loader**: Rejects duplicate keys, non-object roots, invalid node IDs, short secrets ($<16$ chars), non-string values, and trailing data.

### 2.5 Offline-First Durability Invariants
- **Failure Non-Destructiveness**: When control-plane authentication fails (HTTP 401/403) or the server is unavailable (503), the edge agent continues local operations uninterrupted.
- **Zero Telemetry Loss**: Unsynchronized telemetry batches remain safely in SQLite WAL (`sync_status = 'PENDING'`). Local data is **never dropped, deleted, or corrupted** on sync failure.
- **Sequence Preservation**: When a batch fails, sequence progression for that specific node is halted to prevent sequence gaps, while other nodes synchronize independently.

---

## 3. Replay-Cache Architecture & Architectural Limitations

> [!WARNING]
> **MULTI-REPLICA LIMITATION NOTICE:**  
> The current `ReplayCache` is thread-safe and memory-bounded within the **local process memory**.  
> In multi-replica active-active control-plane deployments operating behind a stateless round-robin load balancer, a captured request could theoretically be replayed against a different control-plane instance within the clock skew window.  
> **Production Recommendation**: Deployments utilizing multiple active control-plane instances MUST either:
> 1. Use stateful L7 load balancing with consistent hashing / session affinity on `X-AegisEdge-Node-ID`, OR
> 2. Introduce a distributed shared cache (e.g. Redis) for synchronized nonce deduplication across cluster nodes.

---

## 4. Test Scope vs. Full Penetration Testing

> [!IMPORTANT]
> **ASSURANCE BOUNDARY DISCLAIMER:**  
> The presence of passing automated unit and integration tests does **NOT** prove that the system is invulnerable, nor does it substitute for an independent third-party penetration test.  
> Automated tests verify conformance against known, modelled threat vectors under controlled test harness conditions. They do not evaluate:
> - Physical host compromise or cold-boot hardware memory extraction.
> - Kernel-level race conditions or zero-day vulnerabilities in the underlying Go runtime or OS network stack.
> - Side-channel attacks beyond constant-time byte comparisons (e.g. CPU power or electromagnetic side channels).
> - Advanced network adversary capabilities outside the TLS trust boundary (e.g. compromised Root CAs or BGP hijacking).

---

## 5. Verification Summary

The regression suite is verified across:
1. `services/control-plane/server/security_regression_test.go`: Adversarial protocol tampering, query string mutation, replay defense integrity, admin authorization tiers, trailing data rejection, and concurrent multi-node authentication.
2. `services/control-plane/server/auth_test.go`: Core authentication, per-node credential isolation, duplicate key rejection, and production gate checks.
3. `services/control-plane/main_test.go`: CLI vs environment variable configuration precedence and fail-closed startup validation.
4. `edge/agent/sync/offline_regression_test.go`: Offline persistence, sequence gap prevention, and non-destructive failure handling.

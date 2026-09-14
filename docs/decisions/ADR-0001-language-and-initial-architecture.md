# ADR-0001: Language and Initial Architecture Selection

## Status
Accepted

## Context
AegisEdge is designed as an autonomous edge AI incident-response platform. The system consists of two primary operational domains:
1. **Edge Node Fleet**: Devices deployed in constrained, remote, or heterogeneous network environments that must monitor local system telemetry, detect anomalies, execute safe autonomous mitigations, and survive network partitions.
2. **Central Control Plane**: A central orchestration server that ingests telemetry batches, registers nodes, tracks fleet health, and manages incident history.

When starting this project, key decisions regarding programming languages, storage engines, communication protocols, and infrastructure dependencies must be established to ensure high performance, low resource consumption, maintainability, and rapid local iteration.

## Decisions

### 1. Go for Edge Agent and Control Plane
We selected **Go** for both the Edge Agent and the Control Plane.
* **Static Binary Compilation**: Go compiles into self-contained static binaries with zero external runtime dependencies (no JVM, no Python interpreter required on edge devices). This dramatically simplifies deployment to edge environments.
* **Minimal Resource Footprint**: Go processes exhibit negligible idle memory usage (<15–25 MB RSS) and low CPU overhead, preserving edge hardware resources for host workloads.
* **First-Class Concurrency**: Goroutines and channels provide safe, clean abstractions for running concurrent metric collectors, local storage writers, and upstream synchronization workers without callback nesting or async event loop complexity.
* **Single Toolchain Across Agent and Server**: Using Go across both domains allows sharing canonical domain types (`shared/types`) and reduces cognitive load during development.

### 2. SQLite for Edge-Local Persistence
We selected **SQLite** (operating with Write-Ahead Logging / WAL mode) as the edge-local storage engine.
* **Zero Configuration & In-Process**: SQLite requires no background daemon, eliminating service management overhead or external failure modes on edge nodes.
* **Crash Resilience & ACID Guarantees**: Under power drops or abrupt edge restarts, SQLite WAL mode ensures transactional durability and prevents database corruption.
* **Local Buffer**: SQLite serves as a durable FIFO/ring-buffer queue. When network connectivity to the control plane is severed, telemetry and incident audit records accumulate safely on local disk without memory bloat.

### 3. REST / JSON for Initial Edge-to-Cloud Communication
We selected standard **HTTP/1.1 REST with JSON payloads** for Phase 1.
* **Simplicity & Debuggability**: Payloads can be inspected cleanly using standard tools (`curl`, Postman, browser dev tools, plain loggers) without requiring binary deserialization tooling or schema compiler setup.
* **Ubiquitous Network Traversal**: HTTP/1.1 easily traverses firewalls, NATs, and corporate proxies commonly encountered by edge nodes.
* **Future Upgrade Path**: Once domain models and streaming patterns stabilize, high-throughput binary protocols (such as gRPC or NATS) can be introduced with measurable performance benchmarks.

### 4. Deferral of React / TypeScript Frontend
The operator dashboard (React + TypeScript) is intentionally deferred until the core backend vertical slice (Edge Agent $\rightarrow$ Control Plane API) is functioning and verified with automated tests.
* **Rationale**: Building a UI before API contracts are proven leads to frequent schema churn and wasted UI rework. Establishing working data contracts in Go first ensures the frontend can be built against a stable, tested REST API.

### 5. Intentional Deferral of Kafka, Kubernetes, AWS, and NATS
We explicitly avoid introducing complex distributed infrastructure (Apache Kafka, Kubernetes/K3s, AWS cloud resources, NATS message bus) in Phase 1.
* **Rationale**:
  - A distributed system must be fundamentally correct as standalone binaries communicating over network protocols before introducing orchestration layers.
  - Adding Kubernetes introduces YAML maintenance, pod networking, ingress controllers, and resource overhead that obscure core application logic.
  - Kafka requires dedicated broker management, Zookeeper/KRaft, and heavy resource allocations that are unnecessary for local vertical-slice verification.
  - Deferring cloud infrastructure keeps the entire platform 100% runnable and testable locally on a developer machine with zero cloud bills.

## Alternatives Considered and Trade-offs

| Component | Selected | Alternative | Why Alternative Was Not Chosen |
| :--- | :--- | :--- | :--- |
| **Edge Language** | Go | Rust | Rust offers exceptional memory safety and smaller binaries, but has a steeper learning curve and slower compilation cycles. Go provides sufficient memory safety, fast builds, and simple code easily explainable in technical interviews. |
| **Edge Language** | Go | Python | Python requires an installed interpreter (~50-100MB), has higher RAM usage, suffers from GIL contention during concurrent telemetry polling, and introduces packaging complexity on bare-metal edge nodes. |
| **Edge Storage** | SQLite | Flat Files / JSONL | Flat files lack transactional locking, make atomic queue popping difficult under crashes, and risk partial writes during abrupt power cuts. |
| **Communication** | HTTP REST | gRPC (Protobuf) | gRPC adds protoc toolchain overhead, binary inspection friction, and proxy configuration complexity before API contracts have stabilized. |

## Consequences
* **Positive**: The system can be compiled, run, and unit-tested on any standard developer machine in seconds without Docker or cloud prerequisites.
* **Negative**: HTTP/JSON has slightly higher network serialization overhead than Protobuf, which will be measured and addressed in future optimization milestones.

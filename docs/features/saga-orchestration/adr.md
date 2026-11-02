# ADR-0001: Polyglot Execution Runtime for Saga Orchestration

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-11-02 |
| Deciders | Core Architecture Guild |
| Related ADRs | [ADR-0001 (Tech Stack)](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/tech-stack/ADR-0001-polyglot-execution-runtime.md) |
| Related System | [Saga Orchestration Feature](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/saga-orchestration) |

---

## 1. Context

Saga orchestration requires executing multi-phase distributed workflows with automated compensating transactions when any intermediate step fails. Monolithic architectures often suffer from performance bottlenecks or safety risks when balancing I/O concurrency with zero-jitter rollback math.

---

## 2. Decision Outcome

**Chosen option:** **Tri-Tier Polyglot Engine**

- **Go 1.23 Layer:** Orchestrates saga execution graphs, handles gRPC/HTTP traffic, and manages distributed timeouts.
- **Rust 1.81 Layer:** Enforces saga state machine transitions and guarantees memory-safe outbox operations.
- **C++20 Layer:** Executes deterministic compensation formulas and financial ledger calculations.

### Inter-Process & FFI Architecture

```
Go Orchestrator (gRPC / HTTP)
       │
       ▼ (cgo / FFI)
Rust State Machine Core (Invariants & Outbox)
       │
       ▼ (cxx Bridge)
C++ Deterministic Rollback Kernel (Math / Compensations)
```

# ADR-0004: Continuous Leaderless Crash Recovery, Atomic Takeover Protocol & Poison Guard

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-11-03 |
| Deciders | Lead Systems Architect & Core Architecture Guild |
| Related Docs | [Business Logic & Decision Trees](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/crash-recovery/business-logic.md) · [Diagrams](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/crash-recovery/diagrams.md) · [Failure Cases & Mitigations](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/crash-recovery/failure-cases.md) |
| Target Feature | [Crash Recovery Feature](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/src/features/crash-recovery) |

---

## 1. Context & Problem Statement

Distributed transaction engines manage asynchronous, long-running multi-service sagas. In cloud environments, worker pods are routinely evicted, rescheduled, terminated by Out-Of-Memory (OOM) killers, or temporarily partitioned from the database during network blips or failovers.

When a worker holding a 30-minute lease on a saga crashes:
1. **Permanent Transaction Freeze**: Sagas in `RUNNING` or `COMPENSATING` remain assigned to the dead worker's identity (`lease_owner = 'worker-xxx'`). Standard poller loops skip these sagas, leaving customer transactions stranded indefinitely.
2. **StatefulSet Identity Collisions**: If a StatefulSet pod restarts with the identical name (`worker-0`), a naive recovery model sees `worker_id == me`, leading the restarted process to attempt renewals on sagas for which it has no in-memory state.
3. **Split-Brain Zombie Dispatches**: If a worker is partitioned from the database but remains connected to downstream payment APIs, and another worker takes over the lease, both workers may attempt downstream calls simultaneously.
4. **Poison Pill Crash Loops**: A corrupted step output or payload that causes worker panics will repeatedly crash every recovering worker in the cluster, turning a single bad transaction into a cluster-wide denial-of-service.

---

## 2. Decision Drivers & Architecture Invariants

* **Continuous Leaderless Recovery (Invariant R1)**: Recovery is not a boot-only phase. Every active worker periodically queries for expired leases without requiring a centralized coordinator or Raft cluster.
* **Atomic CAS with Expiry Re-Check (Invariant R2)**: Lease takeover occurs via a single atomic `UPDATE` statement that verifies `fencing_token = :observed_token AND lease_until < NOW()`.
* **Incarnation Identity Separation (Invariant R3)**: Owner identity combines `pod_name` with a unique boot-time UUID (`<pod>_<uuid>`), guaranteeing identity nonces are never reused across restarts.
* **Monotonic Self-Fencing (Invariant R4)**: Workers monitor a monotonic clock; if a worker fails to renew its lease before `lease_until - margin`, it immediately halts all outbound dispatches.
* **Step Reconciliation Boundaries (Invariants R5-R7)**:
  - `PENDING` steps were never dispatched downstream; recovery never queries downstream or marks them failed.
  - `DISPATCHED` steps are classified as `UNKNOWN` and re-dispatched with the identical forward idempotency key.
  - `NOT_FOUND` from a downstream query is never treated as proof of failure.
* **Poison Pill Quarantine (Invariant R10)**: Takeover counts are incremented and persisted atomically before processing. Sagas exceeding `max_takeovers` (default: 5) transition to `PAUSED_POISON`.

---

## 3. Considered Architectural Options

### Option 1: Centralized Coordinator (ZooKeeper / Raft Leader)
* **Architecture**: A designated leader pod monitors worker heartbeats and reassigns orphaned sagas.
* **Cons**:
  - Introduces a single point of failure and split-brain risk during network partitions.
  - Adds external infrastructure complexity.
* **Verdict**: **Rejected**.

### Option 2: Redis-Distributed Locks for Leases
* **Architecture**: Leases stored in Redis with TTLs; expired keys trigger recovery events.
* **Cons**:
  - Asynchronous Redis replication loses lease states on failover.
  - Cannot enforce atomic CAS checks inside PostgreSQL database transactions.
* **Verdict**: **Rejected**.

### Option 3: PostgreSQL-Native Leaderless Continuous Takeover (Selected)
* **Architecture**: Continuous autonomous sweeper polling PostgreSQL using `SELECT ... FOR UPDATE SKIP LOCKED` on expired leases, combined with atomic CAS takeovers and monotonic fencing tokens.
* **Pros**:
  - Zero external dependencies beyond existing PostgreSQL ACID datastore.
  - Provable consistency via database transactional boundaries.
  - Complete immunity to split-brain double ownership.
* **Verdict**: **Accepted**.

---

## 4. Consequences & Operational Guidelines

* **Database Load**: A partial index on `(lease_until) WHERE current_state IN ('CREATED', 'RUNNING', 'ROLLING_FORWARD', 'COMPENSATING')` ensures sweep queries execute in under 1ms without table scans.
* **Poller Separation**: Standard poller queries now exclusively claim unassigned work (`lease_owner = ''`), while `CrashRecoveryService` claims expired owned leases (`lease_owner != '' AND lease_until < NOW()`).

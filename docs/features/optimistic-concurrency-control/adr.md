# ADR-0006: Optimistic Concurrency Control with Aggregate Root Versioning & Ownership Epoch Fencing

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-11-04 |
| Deciders | Core Architecture Guild |
| Related Docs | [Business Logic Decision Tree v2](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/optimistic-concurrency-control/business-logic.md) · [Diagrams](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/optimistic-concurrency-control/diagrams.md) · [Failure Cases](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/optimistic-concurrency-control/failure-cases.md) |
| Target Feature | [Optimistic Concurrency Control](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/src/features/optimistic-concurrency-control) |

---

## 1. Context & Problem Statement

Distributed sagas experience concurrent mutations from multiple ingress channels:
- External client updates (e.g., cancellations, parameter modifications).
- Asynchronous step worker replies arriving over Kafka.
- Scheduled timer expirations (deadlines, step retry delays).
- Crash-recovery sweepers scanning for orphaned sagas.

Using coarse pessimistic row locks held across network RPCs causes severe database connection pool exhaustion and lock wait timeouts. Conversely, naive optimistic concurrency control (checking only an integer `version = version + 1`) introduces critical vulnerabilities:
1. **Zombie Worker Overwrites**: A worker paused by a GC stall or network delay wakes up, re-reads the latest version, re-evaluates against the newly modified state, and commits an invalid transition using the new version number.
2. **Orphaned Child Row Updates**: Mutating child `saga_step_executions` rows without updating or locking the parent `saga_instances` row allows concurrent workers to make conflicting branch decisions based on stale state snapshots.
3. **Dropped Engine Inputs on CAS Conflict Exhaustion**: When retrying a CAS conflict on an internal event (such as a worker reply), treating retry exhaustion as a dropped event leaves the saga stuck forever.

---

## 2. Decision Outcome

**Chosen Option:** **Dual-Token Atomic CAS (Version + Ownership Epoch) with Aggregate Root Version Bumping and Bounded Exponential Jitter Retries.**

### Core Invariants & Implementation:
* **Composite Guard (OC-01):** State updates must match both the expected monotonic version AND the worker's active lease epoch:
  ```sql
  UPDATE saga_instances
  SET 
      status = :new_status,
      version = version + 1,
      updated_at = NOW()
  WHERE 
      id = :saga_id 
      AND tenant_id = :tenant_id
      AND version = :expected_version 
      AND epoch = :worker_epoch;
  ```
* **Aggregate Root Inviolability (OC-02):** Child step mutations and outbox emissions commit in the exact same database transaction that increments the root version.
* **Pre-Retry Idempotency Verification (OC-05):** Before re-executing an engine decision following a CAS failure, the worker inspects the inbox to verify whether the input was already processed by the winning writer.
* **Durability on Exhaustion (OC-06):** Exhausting CAS retry attempts never drops engine-internal replies; the message remains unacknowledged in Kafka or re-queued with a delay.

---

## 3. Rejected Alternatives

1. **Long-Lived Pessimistic Row Locks (`SELECT FOR UPDATE` held during RPCs):**
   * *Reason for Rejection:* Severe risk of database connection starvation, cascading timeouts, and distributed deadlocks across service boundaries.
2. **Distributed Redis Locks (Redlock):**
   * *Reason for Rejection:* Vulnerable to clock drift, JVM/Go runtime GC pauses, and split-brain network partitions (Kleppmann analysis). Fails correctness standards for financial transaction state machines.

---

## 4. Operational & Performance Consequences

* **Throughput at Low Contention:** Near zero lock contention latency overhead for sequential pipelines.
* **High Contention Behavior:** Hot workflows with >50 parallel branch completions switch to partitioned single-worker queue serialization rather than OCC CAS loops to prevent CAS thrashing.


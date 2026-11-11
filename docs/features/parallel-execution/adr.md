# ADR-0010: High-Throughput Fork-Join DAG Orchestration, Join Barrier Semantics & Partial Branch Compensation

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-11-07 |
| Deciders | Lead Systems Architect & Core Architecture Guild |
| Related Docs | [Business Logic & Decision Tree v2](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/parallel-execution/business-logic.md) · [Diagrams](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/parallel-execution/diagrams.md) · [Failure Cases & Mitigations](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/parallel-execution/failure-cases.md) |
| Target Feature | [Parallel Execution Feature](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/src/features/parallel-execution) |

---

## 1. Context & Problem Statement

While sequential workflows enforce deterministic linear progression, complex distributed workflows (e.g. multi-supplier flight/hotel booking, concurrent KYC/AML identity verification, parallel warehouse pick-list reserving) demand **parallel execution** to minimize end-to-end latency. 

Naive parallel implementations create severe production hazards:
1. **The In-Flight Drain Race**: If Branch 1 fails while Branch 2 and Branch 3 are still in flight (`DISPATCHED`), initiating immediate compensation calculates targets on a stale snapshot. If Branch 2 completes 50ms later, its side effect is orphaned downstream and never compensated.
2. **Ambiguous Join Resolutions**: If two parallel branches complete simultaneously on separate worker pods, concurrent CAS updates to the join barrier can cause both workers to attempt next-step dispatches or double-advance the saga.
3. **Irreversible Mutations Inside Parallel Groups**: Dispatching an irreversible side effect (e.g., sending an irrevocable payment or dispatching physical courier) inside a parallel group where another sibling branch can fail triggers unrecoverable state corruption.
4. **Hot-Row CAS Thrashing**: 50 concurrent branch replies attempting to update a single `completed_steps` JSONB array on the parent saga row causes intense lock contention, 409 retry storms, and throughput collapse.

---

## 2. Real-World Use Cases: When to Use vs. When to Reject

### When to Use
* **Multi-Party Travel Bookings (Reserve-Then-Confirm)**: Concurrently reserving flights, hotels, and rental cars with provisional hold timeouts. If any required hold fails, remaining provisional holds are cancelled.
* **Concurrent Identity & Fraud Screening**: Fan-out to credit bureau, biometric verification, and sanctions blacklist simultaneously; evaluate join policy when all complete.
* **Price Shopping / Failover Aggregation (`FIRST_SUCCESS`)**: Quoting 5 different liquidity providers concurrently; the first valid quote wins, and the remaining 4 are cancelled.

### When NOT to Use (Rejection Boundaries)
* **Strict Dependent Chains ($N \to N+1$)**: Where Step 2 requires the authoritative output of Step 1 (use [`sequential-execution`](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/sequential-execution)).
* **Shared Entity Resource Contention**: Concurrently debiting the exact same inventory SKU or customer account balance in parallel causes massive database row-lock contention.

---

## 3. Analytical Numbers & Capacity Model

| Metric | Specification Target | Analytical Limit & Derivation |
|---|---|---|
| **Max Fan-Out per Group** | $50 \text{ concurrent branches}$ | Bounded by PostgreSQL transaction write sizes and Kafka message fan-out limits. |
| **Join Evaluation Latency** | $p50 < 6 \text{ ms}, \quad p99 < 25 \text{ ms}$ | Atomic CAS on `saga_parallel_groups` row; zero cross-table table lock queries. |
| **Memory Footprint per Group** | $\sim 14 \text{ KB}$ | PostgreSQL row metadata + branch state tracking array. |
| **Write Amplification on Fork** | $1 \text{ group row} + K \text{ branch rows} + K \text{ outbox rows}$ | Batched into chunks of 50 inside a single ACID database transaction. |
| **Join Quorum Throughput** | $1,200 \text{ joins/sec/shard}$ | Optimized via branch-isolated execution rows and single join barrier CAS. |

---

## 4. Architectural Trade-offs & Considered Options

| Architecture Option | Failure Resilience | CAS Contention | Operational Complexity | Verdict |
|---|---|---|---|---|
| **Option 1: In-Memory Goroutine Fan-Out (`sync.WaitGroup`)** | ❌ Fatal: Worker crash wipes in-memory branches; state stranded permanently. | ✅ Zero DB contention. | ❌ Zero restartability or durable lease recovery. | **Rejected** |
| **Option 2: Distributed Actor Model (Akka / ProtoActor)** | ⚠️ Complex cluster rebalancing; split-brain risk during network partitions. | ⚠️ In-memory state tracking requires event sourcing snapshots. | ❌ Requires maintaining dedicated actor cluster infrastructure. | **Rejected** |
| **Option 3: Relational Branch Journaling with Atomic Join Barrier (Selected)** | ✅ 100% durable; any node crash is safely recovered via leaderless sweeper. | ✅ Decoupled: branch results write to independent rows; join CAS evaluated once. | ✅ Standard PostgreSQL tables, indexes, and outbox relay. | **Adopted** |

---

## 5. Decision Drivers & Architecture Invariants (PX-01 – PX-12)

* **PX-01 (Atomic Fork Dispatch)**: The group header, all branch execution rows, and all outbox dispatch records commit in ONE atomic database transaction.
* **PX-02 (Branch-Level Idempotency)**: Every branch execution carries an independent idempotency key:
  $$\text{Key}_{\text{branch}} = \text{idem:fwd:} \langle \text{tenant\_id} \rangle : \langle \text{saga\_id} \rangle : \langle \text{group\_id} \rangle : \langle \text{branch\_id} \rangle$$
* **PX-03 (Pre-Compensation Drain Barrier)**: When a REQUIRED branch fails, the group enters `DRAINING`. Compensation of succeeded branches is strictly barred until all sibling branches reach a terminal state (`SUCCEEDED`, `FAILED`, or `TIMED_OUT`).
* **PX-04 (Irreversible Side Effect Prohibition)**: Workflow definition validation strictly rejects any definition placing an irreversible action (e.g. non-refundable wire) inside a parallel group.
* **PX-05 (Single Join Decision)**: The join condition (`ALL_REQUIRED`, `FIRST_SUCCESS`, `N_OF_M`) is evaluated in the same transaction as the branch completion. The join is decided exactly once via atomic CAS on `join_evaluated_at IS NULL`.
* **PX-06 (Late Reply Quarantine)**: Replies arriving after a group has already been cancelled or resolved trigger idempotent no-ops, or reactive compensation if the branch was executed downstream.

---

## 6. Relational Schema & DDL

```sql
CREATE TYPE join_policy_type AS ENUM ('ALL_REQUIRED', 'FIRST_SUCCESS', 'N_OF_M', 'ALL_SETTLED');
CREATE TYPE branch_role_type AS ENUM ('REQUIRED', 'OPTIONAL', 'ALTERNATIVE');
CREATE TYPE parallel_group_status AS ENUM ('DISPATCHED', 'DRAINING', 'SUCCEEDED', 'FAILED', 'CANCELLED');

-- Master parallel group tracking table
CREATE TABLE IF NOT EXISTS saga_parallel_groups (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id VARCHAR(64) NOT NULL,
    saga_id VARCHAR(64) NOT NULL,
    group_id VARCHAR(64) NOT NULL,
    join_policy join_policy_type NOT NULL,
    required_success_count INT NOT NULL DEFAULT 0,
    status parallel_group_status NOT NULL DEFAULT 'DISPATCHED',
    join_evaluated_at TIMESTAMPTZ,
    version INT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_saga_group UNIQUE (tenant_id, saga_id, group_id),
    CONSTRAINT fk_group_saga FOREIGN KEY (saga_id) REFERENCES saga_instances(id) ON DELETE CASCADE
);

-- Individual branch execution rows (eliminates hot-row parent contention)
CREATE TABLE IF NOT EXISTS saga_parallel_branches (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id VARCHAR(64) NOT NULL,
    group_id UUID NOT NULL REFERENCES saga_parallel_groups(id) ON DELETE CASCADE,
    branch_id VARCHAR(64) NOT NULL,
    role branch_role_type NOT NULL,
    idempotency_key VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'DISPATCHED',
    attempt_count INT NOT NULL DEFAULT 1,
    output_payload JSONB,
    completed_at TIMESTAMPTZ,

    CONSTRAINT uq_branch_key UNIQUE (tenant_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_group_branch_poll 
    ON saga_parallel_branches (tenant_id, group_id, status);
```


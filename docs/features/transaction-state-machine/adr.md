# ADR-0002: Formal Transaction State Machine Architecture & Invariant Enforcement

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-11-02 |
| Deciders | Chief Strategist & Core Architecture Guild |
| Related Docs | [Business Logic & Decision Trees](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/transaction-state-machine/business-logic.md) · [System Diagrams](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/transaction-state-machine/diagrams.md) · [Failure Cases & Mitigations](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/transaction-state-machine/failure-cases.md) |
| Target Feature | [Transaction State Machine Feature](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/src/features/transaction-state-machine) |

---

## 1. Context & Problem Statement

In distributed saga orchestration, multi-step workflows cross service and network boundaries where node failures, transient network partitions, downstream timeouts, and out-of-order asynchronous webhooks are routine operating conditions.

Without a formally verified, deterministic finite state machine (FSM) backed by persistent storage invariants, distributed transaction engines encounter severe production failure modes:
1. **Split-Brain Zombie Workers**: A worker paused during a 30-second GC pause wakes up and commits state changes, overwriting work completed by a replacement worker.
2. **Double Charging / Phantom Effects**: Network timeouts (e.g. HTTP 504 Gateway Timeout) prompt naive blind retries that charge customer credit cards or book airline seats twice.
3. **Deadlock / Lost Update Anomalies**: Concurrent asynchronous updates (e.g., an automated recovery sweeper competing with a late 3D-Secure payment webhook) clobber state without optimistic validation.
4. **Resurrection of Terminal Sagas**: Late arriving webhooks or re-drive attempts overwrite `CANCELLED` or `COMPLETED` transactions, reviving completed flows and dispensing goods or wire transfers for free.
5. **Regulatory & Audit Blind Spots**: State transitions occurring without synchronous, tamper-evident audit records leave engineers unable to explain financial rollback discrepancies to auditors.

---

## 2. Decision Drivers & Architecture Axioms

* **Data Integrity Axiom (AX-2 / AX-13)**: Every state mutation must satisfy database storage gate constraints (`CHECK`, `UNIQUE`, `FOREIGN KEY`), preventing illegal states from ever persisting regardless of application bugs.
* **Terminal Immutability Invariant**: States `COMPLETED`, `CANCELLED`, `COMPENSATED`, and `FAILED` are strictly terminal. No incoming event or operator action can transition out of a terminal state.
* **Monotonic Fencing Tokens (Invariant 2 & Case 25)**: Every worker claim acquires an incremented fencing token; any commit with a token strictly less than the active token is hard rejected.
* **Optimistic Concurrency Control (OCC - Invariant 6)**: Updates must compare-and-swap (CAS) on `version = version + 1`, failing fast with `HTTP 409 Conflict` on race conditions.
* **5 Mandatory Identifier Roles**: Ingress and persistence must isolate internal surrogate UUIDv7, external opaque `public_id` (`sag_...`), ownership `tenant_id`, natural `source` + `external_id`, and causation `correlation_id`.
* **Zero Magic Strings & Pure Domain Boundary**: FSM rules must be pure mathematical functions taking `(CurrentState, Event) -> (NextState, Error)` with zero network, disk, or memory side-effects.

---

## 3. Considered Architectural Options

### Option 1: Ad-hoc Status Column on Monolithic Sagas Table
* **Architecture**: A single string column `status VARCHAR(32)` updated via blind SQL `UPDATE sagas SET status = ? WHERE id = ?`.
* **Pros**: Trivially fast to implement; minimal initial boilerplate.
* **Cons**:
  - Zero OCC: lost update races between webhooks and workers are guaranteed.
  - Zero DB gate: application typos can write invalid state strings.
  - No transition audit trail: impossible to reconstruct chronology or prove compliance.
  - Fails under split-brain workers.
* **Verdict**: **Rejected (High data corruption risk)**.

### Option 2: Pure Event-Sourced Log with Dynamic Read-Model Projections
* **Architecture**: Every transition is appended as an immutable event to an event store; current state is folded/projected in memory upon retrieval.
* **Pros**: Native historical auditability; event replay capabilities.
* **Cons**:
  - High read latency on sagas with hundreds of steps.
  - Complex projection snapshotting and caching requirements.
  - Fails simple SQL unique constraints across in-flight states without distributed locks.
  - Heavy infrastructure footprint requiring dedicated event store engines.
* **Verdict**: **Rejected (Unjustified complexity and latency overhead for operational state)**.

### Option 3: Hexagonal Stateless FSM Engine with Database-Gated Relational CAS & Synchronous Audit (Chosen)
* **Architecture**:
  - Pure domain state machine and rule matrix in Go (`domain/rules/`, `domain/machines/`).
  - Storage gate backed by PostgreSQL `saga_instance_states` (compound PK `(tenant_id, id)`), `saga_state_transitions` lookup matrix, and `state_transition_audit` log.
  - Single atomic database transaction boundary (`BEGIN ... COMMIT`) executing CAS state update and audit row append together.
* **Pros**:
  - Microsecond in-memory evaluation (4,740 ns/op; ~210,000 transitions/sec).
  - Mathematical state safety guaranteed by both application FSM and relational database constraints.
  - Native protection against zombie workers via monotonic fencing tokens.
  - Immediate linearizability with zero external coordinator dependencies.
* **Verdict**: **Accepted as Architectural Standard**.

---

## 4. Decision Outcome & System Contract

### 4.1 State Hierarchy & Allowed Transitions

```
                             ┌─────────────────┐
                             │     CREATED     │
                             └────────┬────────┘
                                      │
                 ┌────────────────────┴────────────────────┐
                 │ Event: ORCHESTRATOR_CLAIM               │ Event: CLIENT_ABORT
                 ▼                                         ▼
        ┌─────────────────┐                       ┌─────────────────┐
        │     RUNNING     │                       │    CANCELLED    │ [TERMINAL]
        └────────┬────────┘                       └─────────────────┘
                 │
   ┌─────────────┼─────────────────────────┬─────────────────────────┐
   │             │                         │                         │
   │ DAG_COMPL   │ RATE_LIMITED            │ ASYNC_SUSPEND           │ INDETERMINATE_ERROR /
   ▼             ▼                         ▼                         │ BUSINESS_FAILURE
┌───────────┐ ┌───────────────┐   ┌─────────────────┐                ▼
│ COMPLETED │ │THROTTLED_WAIT │   │     WAITING     │        ┌─────────────────┐
└───────────┘ └───────┬───────┘   └────────┬────────┘        │   RECONCILING   │
 [TERMINAL]           │                    │                 └────────┬────────┘
                      │ TIMER_FIRED        │ WEBHOOK_APPR             │
                      └──────────┐         └────────┐                 ├────────────────────────┐
                                 ▼                  ▼                 │ PROBE_OK / NOT_FOUND   │ PROBE_BUDGET_EXPIRED
                        ┌─────────────────┐         │                 ▼                        ▼
                        │     RUNNING     │◄────────┘        ┌─────────────────┐      ┌─────────────────┐
                        └────────┬────────┘                  │     RUNNING     │      │      STUCK      │
                                 │                           └─────────────────┘      └────────┬────────┘
                                 │ BUSINESS_FAILURE                                            │
                                 ▼                                                             ├──────────────────────┐
                        ┌─────────────────┐                                                    │ OPERATOR_REDRIVE     │ OPERATOR_WRITEOFF
                        │  COMPENSATING   │◄───────────────────────────────────────────────────┘                      ▼
                        └────────┬────────┘                                                                  ┌─────────────────┐
                                 │                                                                           │     FAILED      │ [TERMINAL]
            ┌────────────────────┼────────────────────┐                                                      └─────────────────┘
            │ STEP_OK (Undo)     │ COMPENSATION_HALT  │ COMPENSATION_DONE
            ▼                    ▼                    ▼
   ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
   │  COMPENSATING   │  │      STUCK      │  │   COMPENSATED   │ [TERMINAL]
   └─────────────────┘  └─────────────────┘  └─────────────────┘
```

### 4.2 Optimistic Concurrency & Audit Contract

State transitions are committed exclusively via the following parameterized query inside an explicit ACID transaction:

```sql
-- Step 1: Atomic CAS Update
UPDATE saga_instance_states
SET current_state = $1, version = version + 1, fencing_token = $2, updated_at = $3
WHERE tenant_id = $4 AND id = $5 AND version = $6 AND current_state = $7
RETURNING id, tenant_id, current_state, version, fencing_token, updated_at;

-- Step 2: Atomic Durable Audit Append (Same Transaction)
INSERT INTO state_transition_audit (
    id, public_id, tenant_id, saga_id, correlation_id, 
    from_state, to_state, version_before, version_after, 
    fencing_token, reason, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);
```

If the `UPDATE` returns zero affected rows:
1. The transaction executes an immediate `ROLLBACK`.
2. The service loads the current database row.
3. If the record's version has advanced, it returns typed error `appErrors.NewConflictError("stale version detected: reload required")` mapping to HTTP status `409 Conflict`.
4. If the record is in a terminal state, all future transitions are permanently rejected.

---

## 5. Consequences & Trade-Offs

### Positive Consequences
* **Absolute Terminal Safety**: Impossible for network blips, late callbacks, or human error to revive dead sagas.
* **Predictable Crash Recovery**: Monotonic fencing tokens ensure that dead worker processes waking up after network isolation fail fast with zero database mutations.
* **Strict Hexagonal Boundary**: Handlers interact strictly with `transactionStateMachine.Service` via `index.go`, maintaining 100% decoupling from concrete SQL drivers or storage implementations.
* **Complete Auditability**: Every state hop has a durable chronological trail capturing timestamp, reason, version hop, and trace correlation ID.

### Negative Consequences & Operational Mitigations
* **High-Contention Retries**: Sagas experiencing frequent concurrent events (e.g. parallel DAG branches) can experience CAS retries.
  - *Mitigation*: The engine sorts parallel resource branches and applies randomized exponential jitter (50ms–300ms) before retrying CAS transitions.
* **Storage Growth**: Append-only transition audit logs expand proportionally to the transaction volume.
  - *Mitigation*: Daily partitioned tables with cold-storage archival to Parquet/S3 Glacier after 90 days.

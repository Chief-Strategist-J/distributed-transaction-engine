# Parallel Execution — Failure Cases, Contention Races & Mitigations

This document analyzes failure cases, concurrent race conditions, join deadlocks, and mitigations for the Parallel (Fork-Join) Execution Engine.

---

## 1. Failure Modes & Resilience Matrix

| Failure ID | Scenario / Trigger | Direct Impact | Invariants Challenged | Architectural Mitigation & Detection |
|---|---|---|---|---|
| **FAIL-PX-001** | **The In-Flight Drain Race**: Branch 2 fails; engine immediately constructs a compensation plan while Branch 1 is still in flight (`DISPATCHED`). Branch 1 completes 50ms later with `SUCCEEDED`. | Branch 1 is omitted from the compensation plan, leaving an active, unrefunded hold downstream. | **PX-03** | **Mitigation:** Pre-compensation drain barrier. When any REQUIRED branch fails, the group enters `DRAINING`. Compensation is strictly blocked until all branches reach terminal states (`SUCCEEDED`, `FAILED`, `TIMED_OUT`). |
| **FAIL-PX-002** | **Simultaneous Join Completion Race**: Branch 1 and Branch 2 both finish within 1ms on different worker nodes, satisfying an `ALL_REQUIRED` join policy simultaneously. | Both workers evaluate the join condition, win CAS, and dispatch duplicate next-step commands. | **PX-05** | **Mitigation:** Atomic CAS on join evaluation: `UPDATE saga_parallel_groups SET status = 'SUCCEEDED', join_evaluated_at = NOW() WHERE id = :id AND join_evaluated_at IS NULL`. Exactly one worker updates 1 row; the second receives 0 rows and exits. |
| **FAIL-PX-003** | **Irreversible Side Effect Inside Failing Group**: A workflow definition places an irreversible bank payout in Branch 1 and an inventory hold in Branch 2. Branch 2 fails. | The engine attempts reverse compensation, but the bank wire cannot be reversed, leaving the system in a corrupted state. | **PX-04** | **Mitigation:** Static workflow registration gate. Definitions containing irreversible steps inside a parallel group are rejected with `422 Unprocessable Entity: IRREVERSIBLE_STEP_IN_PARALLEL_GROUP`. |
| **FAIL-PX-004** | **Branch Output Namespace Collision**: Branch 1 (Car Rental) and Branch 2 (Hotel) both output a key named `reservation_id` to the parent context. | Non-deterministic overwrite; whichever worker commits last overwrites the other's booking reference. | **PX-09** | **Mitigation:** Namespaced context isolation. Branch outputs are strictly namespaced: `context[group_id][branch_id][entry_name]`. Direct mutation of root context entries by branches is blocked. |
| **FAIL-PX-005** | **Hot-Row Lock Contention Storm (50 Branches)**: 50 parallel branches complete within a 10ms window and attempt `UPDATE saga_instances SET ...` on the root saga row. | Severe row-lock wait queues, connection pool starvation, and repeated 409 conflict retries. | **Section 3** | **Mitigation:** Isolated branch execution rows (`saga_parallel_branches`). Each branch worker writes exclusively to its own unique row. The root group row is updated only once when the join barrier condition is satisfied. |

---

## 2. In-Depth Failure Drill: The Drain Barrier Interception

```mermaid
sequenceDiagram
    autonumber
    participant B1 as Slow Worker (Flight Hold)
    participant B2 as Fast Worker (Car Rental - Fails)
    participant Engine as Parallel Engine
    participant DB as PostgreSQL 18
    participant Comp as Compensation Engine

    B2-->>Engine: Car Rental FAILED (No vehicles)
    Engine->>DB: UPDATE saga_parallel_branches SET status='FAILED' WHERE branch='car'
    
    rect rgb(255, 235, 235)
        Note over Engine, DB: Drain Barrier Activated (PX-03)
        Engine->>DB: UPDATE saga_parallel_groups SET status = 'DRAINING' WHERE group_id = 'grp-1'
        Note over Engine: Flight hold is still DISPATCHED; Compensation is HELD!
    end

    Note over B1: Flight Hold arrives 50ms later
    B1-->>Engine: Flight Hold SUCCEEDED (ref: 'FL-90')
    Engine->>DB: UPDATE saga_parallel_branches SET status='SUCCEEDED' WHERE branch='flight'

    rect rgb(240, 248, 255)
        Note over Engine, Comp: All branches settled; Safe to execute reverse compensation
        Engine->>DB: UPDATE saga_parallel_groups SET status = 'FAILED'
        Engine->>Comp: Trigger Compensation for Flight Hold 'FL-90'
    end
```

---

## 3. Operator Runbook: Resolving Stuck Drain Groups

When a parallel group remains in `DRAINING` longer than 60 seconds (alert `ParallelGroupDrainTimeout`):

1. **Identify the lagging branch:**
   ```sql
   SELECT b.branch_id, b.status, b.idempotency_key, b.attempt_count
   FROM saga_parallel_branches b
   JOIN saga_parallel_groups g ON b.group_id = g.id
   WHERE g.status = 'DRAINING' AND b.status = 'DISPATCHED';
   ```
2. **Force-terminate the hanging branch:**
   `POST /api/v1/sagas/{id}/groups/{groupId}/branches/{branchId}/force-timeout`
   The branch transitions to `TIMED_OUT`, resolving the drain barrier and allowing compensation to unwind remaining branches.


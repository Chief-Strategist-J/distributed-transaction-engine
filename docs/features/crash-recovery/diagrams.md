# Crash Recovery — Architectural & Visual Diagrams

This document details the architectural blueprints, component models, and sequence workflows for the **Continuous Leaderless Crash Recovery Engine** in the Distributed Transaction Engine platform, supporting [ADR-0004](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/crash-recovery/adr.md) and [Business Logic Decision Tree v2](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/crash-recovery/business-logic.md).

---

## 1. High-Level Architecture (HLD): Continuous Leaderless Recovery Fleet

Crash recovery operates continuously across all running worker replicas without a centralized master or consensus cluster (ZooKeeper/Raft). Workers discover orphaned sagas directly via indexed database queries.

```mermaid
flowchart TB
    subgraph Fleet ["Orchestrator Worker Fleet (K8s Replicas)"]
        W1["Worker Pod 1 (Incarnation: pod1_u1)"]
        W2["Worker Pod 2 (Incarnation: pod2_u2)"]
        W3["Dead / Partitioned Pod (pod3_u3)"]
    end

    subgraph DatabaseLayer ["PostgreSQL 18 Primary"]
        Sagas[("saga_instances<br/>lease_owner, lease_until, fencing_token")]
        Steps[("saga_step_executions<br/>status: PENDING | DISPATCHED | UNKNOWN")]
        Audit[("saga_recovery_audit_log")]
    end

    subgraph TakeoverProtocol ["Leaderless Takeover Protocol"]
        W1 -->|1. Polling: lease_until < NOW() SKIP LOCKED| Sagas
        W1 -->|2. Atomic CAS: fencing_token=token+1, owner=pod1_u1| Sagas
        W2 -->|Concurrent claim attempt on same row| Sagas
        Sagas -->>|0 rows affected: lock contention resolved| W2
    end

    subgraph DownstreamServices ["Dispatched Step Handlers"]
        W1 -->|3. Re-dispatch UNKNOWN with same idempotency key| PartnerAPI["Payment / Inventory Services"]
        PartnerAPI -->|Deduplicate & return authoritative outcome| W1
    end

    subgraph PoisonQuarantine ["Poison Pill Circuit Breaker"]
        W1 -->|takeover_count >= 5| Quarantine["PAUSED_POISON State"]
        Quarantine -->|Emit P1 Alert| PagerDuty["Alertmanager / On-Call"]
    end
```

---

## 2. Low-Level Component & Service Engine (LLD)

```mermaid
classDiagram
    class RecoverySweeper {
        -RecoveryRepositoryPort repo
        -RateLimiter rampUpLimiter
        -string workerIncarnationID
        +StartSweeperLoop(ctx) void
        +ScanAndClaimOrphans(ctx, batchSize) int
        +ReconcileSaga(ctx, sagaID, currentToken) error
    }

    class RecoveryRepositoryPort {
        <<interface>>
        +FindExpiredLeases(ctx, limit) ([]SagaLeaseRecord, error)
        +AttemptAtomicTakeover(ctx, sagaID, observedToken, newOwner, newExpiry) (bool, int64, error)
        +IncrementTakeoverCounter(ctx, sagaID) (int, error)
        +MarkPoisonPill(ctx, sagaID, reason) error
        +GetNonTerminalSteps(ctx, sagaID) ([]StepExecutionRecord, error)
    }

    class SelfFencingMonitor {
        -time.Time leaseExpiresAt
        -time.Duration safetyMargin
        +CanDispatch() bool
        +HaltOutboundRPC() void
    }

    class StepReconciliationDispatcher {
        +ReconcileStep(ctx, step) error
        +ProbeDownstream(ctx, step) (StepOutcome, error)
        +ReDispatchForward(ctx, step) error
    }

    RecoverySweeper --> RecoveryRepositoryPort : invokes
    RecoverySweeper --> StepReconciliationDispatcher : coordinates
    RecoverySweeper ..> SelfFencingMonitor : verifies
```

---

## 3. Atomic Lease Takeover Sequence with Monotonic Fencing

```mermaid
sequenceDiagram
    autonumber
    participant DeadPod as Dead Worker (pod-3_uuidA)
    participant DB as PostgreSQL 18
    participant Sweeper as Healthy Worker (pod-1_uuidB)
    participant Downstream as Downstream Partner API

    Note over DeadPod: Pod-3 suffers OOM-kill or net-partition mid-saga
    Note over DB: Time passes: lease_until (30s) expires
    
    Sweeper->>DB: SELECT id, fencing_token FROM saga_instances WHERE lease_until < NOW() FOR UPDATE SKIP LOCKED LIMIT 10
    DB-->>Sweeper: Return saga 'tx-99', fencing_token=14
    
    rect rgb(240, 248, 255)
        Note over Sweeper, DB: Invariant R2: Atomic CAS with Expiry Re-Check
        Sweeper->>DB: UPDATE saga_instances SET lease_owner='pod-1_uuidB', lease_until=NOW()+30s, fencing_token=15, takeover_count=takeover_count+1 WHERE id='tx-99' AND fencing_token=14 AND lease_until < NOW()
        DB-->>Sweeper: 1 Row Affected (Takeover Succeeded)
    end

    Sweeper->>DB: SELECT * FROM saga_step_executions WHERE saga_id='tx-99' AND status NOT IN ('SUCCEEDED', 'COMPENSATED')
    DB-->>Sweeper: Step 2 status = 'DISPATCHED' (In-Flight when crashed)

    Note over Sweeper: Invariant R6: Re-dispatch with identical idempotency key
    Sweeper->>Downstream: POST /charge (idem:fwd:tenant1:tx-99:step2:0)
    Downstream-->>Sweeper: HTTP 200 (Already executed: auth_id='auth-881')
    
    Sweeper->>DB: UPDATE saga_step_executions SET status='SUCCEEDED', output='{"auth_id":"auth-881"}' WHERE id='step-2' AND fencing_token=15
    Sweeper->>DB: Commit next steps / proceed normal orchestration
```

---

## 4. Zombie Worker Interception & Monotonic Self-Fencing

```mermaid
sequenceDiagram
    autonumber
    participant Zombie as Zombie Worker (pod-3_uuidA)
    participant DB as PostgreSQL 18
    participant Sweeper as Active Worker (pod-1_uuidB)
    participant Downstream as Downstream Gateway

    Note over Zombie: Suffers 40s Stop-the-World GC Pause
    Note over DB: Lease expires at sec 30; Sweeper takes over at sec 32 (token: 14 -> 15)
    
    Note over Zombie: Zombie wakes up at sec 40!
    
    rect rgb(255, 235, 235)
        Note over Zombie: Invariant R4: Monotonic Self-Fencing Check
        Zombie->>Zombie: Check clock: now() > lease_until - margin
        alt Self-Fencing Halts Dispatch (Early Guard)
            Zombie-->>Zombie: Lease lapsed! Abort outbound calls immediately.
        else Clock Skew / Missed Local Guard
            Zombie->>Downstream: POST /charge (Outbound mutation)
            Downstream-->>Zombie: 200 OK
            Zombie->>DB: UPDATE saga_instances SET version=version+1 WHERE id='tx-99' AND fencing_token=14
            DB-->>Zombie: 0 Rows Affected (Rejected! Fencing token is 15)
            Note over Zombie: Zombie state write denied; database remains consistent!
        end
    end
```

---

## 5. Poison Pill Quarantine Sequence (Invariant R10)

```mermaid
sequenceDiagram
    autonumber
    participant Sweeper as Recovery Sweeper
    participant DB as PostgreSQL 18
    participant Alert as Prometheus Alertmanager

    Sweeper->>DB: Takeover saga 'tx-666' (takeover_count incremented to 5)
    
    rect rgb(255, 240, 240)
        Note over Sweeper: Invariant R10: Poison Threshold Reached (count >= 5)
        Sweeper->>DB: UPDATE saga_instances SET status = 'PAUSED_POISON', reason = 'EXCEEDED_MAX_RECOVERIES' WHERE id = 'tx-666'
        Sweeper->>DB: INSERT INTO saga_recovery_audit_log (saga_id, event, details) VALUES ('tx-666', 'QUARANTINE_POISON', 'Repeated crashes during recovery')
        Sweeper->>Alert: Emit Metric: saga_quarantine_poison_total{saga_id='tx-666'}
    end

    Note over Sweeper: Sweeper skips step execution; saga quarantined from crashing fleet
```

---

## 6. Recovery Lifecycle State Machine

```mermaid
stateDiagram-v2
    [*] --> ORPHANED: Worker Crashes / Lease Expires
    
    ORPHANED --> TAKEN_OVER: Atomic CAS (token=token+1, owner=new_worker)
    
    TAKEN_OVER --> PAUSED_POISON: takeover_count >= 5 (Quarantined)
    
    TAKEN_OVER --> RECONCILING_STEPS: takeover_count < 5
    
    RECONCILING_STEPS --> RUNNING: PENDING steps scheduled / DISPATCHED probed
    RECONCILING_STEPS --> COMPENSATING: Dispatched step confirmed terminal failure
    RECONCILING_STEPS --> ROLLING_FORWARD: Failure occurred after Pivot step
    
    PAUSED_POISON --> RUNNING: Operator Manual Intervention (Reset Counter)
    PAUSED_POISON --> ABORTED: Operator Manual Abandon
```


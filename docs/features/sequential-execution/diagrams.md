# Sequential Saga Execution — Architectural & Visual Diagrams

This document contains high-level and low-level architectural diagrams for the **Sequential Execution Engine** in the Distributed Transaction Engine platform, supporting [ADR-0005](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/sequential-execution/adr.md) and [Business Logic Tree v2](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/sequential-execution/business-logic.md).

---

## 1. High-Level Architecture (HLD): Hexagonal Outbox Model

The sequential execution runtime utilizes a hexagonal architecture pattern where the orchestrator engine is decoupled from transport infrastructure via PostgreSQL tables, providing zero dual-write vulnerability and persistent auditability.

```mermaid
flowchart TB
    subgraph IngressLayer ["Ingress & Client Layer"]
        Client["Client / External Ingress"] -->|POST /api/v1/tenants/{tenant}/sagas/sequential| API["API Gateway / Router"]
        API -->|Invoke Service| Service["SequentialExecutionService"]
    end

    subgraph CoreEngine ["Domain Core (Hexagonal Center)"]
        Service -->|Evaluate Transitions| Machine["SequentialExecutionMachine"]
        Machine -->|Enforce Invariants SQ-01..15| Rules["SequentialExecutionRules"]
    end

    subgraph PortsAdapters ["Infrastructure Ports & Adapters"]
        Service -->|Persist State & Outbox| RepoPort["SequentialExecutionRepositoryPort"]
        RepoPort -->|PostgreSQL Adapter| PGAdapter["PostgresSequentialExecutionRepositoryAdapter"]
        Service -->|Emit Commands| DispPort["SequentialDispatcherPort"]
        DispPort -->|Outbox Adapter| OutboxAdapter["TransactionalOutboxDispatcherAdapter"]
    end

    subgraph StorageLayer ["PostgreSQL ACID Datastore"]
        PGAdapter -->|Atomic CAS UPDATE| DB_Instances[("saga_instances")]
        PGAdapter -->|Append Immutable Entry| DB_Context[("saga_context_entries")]
        PGAdapter -->|Schedule Due Time| DB_Timers[("saga_timers")]
        OutboxAdapter -->|Atomic INSERT with CAS| DB_Outbox[("transactional_outbox")]
    end

    subgraph TransportLayer ["Asynchronous Message Plane"]
        DB_Outbox -->|WAL Log Tailing / Debezium| OutboxRelay["Outbox Relay Daemon"]
        OutboxRelay -->|Publish Command| KafkaBroker["Kafka Step Command Topics"]
        KafkaBroker -->|Consume Command| Microservice["Downstream Step Worker"]
        Microservice -->|Acknowledge / Complete| ReplyQueue["Reply Event Topic"]
        ReplyQueue -->|Idempotent Ingestion| InboxConsumer["Orchestrator Reply Consumer"]
        InboxConsumer -->|Dispatch Event| Service
    end

    subgraph BackgroundDaemons ["Engine Resiliency Fleet"]
        TimerPoller["Durable Timer Poller"] -->|Poll due_at <= NOW| DB_Timers
        TimerPoller -->|Trigger Timeout Event| Service
        Sweeper["Crash Recovery Sweeper"] -->|Takeover Expired Leases| DB_Instances
    end
```

---

## 2. Low-Level Component & Service Engine (LLD)

This diagram details the internal package layout, class relationships, and repository query interfaces governing the feature.

```mermaid
classDiagram
    class SequentialExecutionService {
        -repo SequentialExecutionRepositoryPort
        -workflowRepo WorkflowDefinitionsRepositoryPort
        -dispatcher SequentialDispatcherPort
        -machine SequentialExecutionMachine
        -tracer DefaultTracer
        +StartSequentialSaga(ctx, input) SequentialSagaState
        +HandleStepCompletion(ctx, tenantId, sagaId, stepIndex, isSuccess, output, isRetryable) error
        +HandleStepTimeout(ctx, tenantId, sagaId, stepIndex, attempt) error
        +GetSaga(ctx, tenantId, sagaId) SequentialSagaState
    }

    class SequentialExecutionMachine {
        +EvaluateStepSuccess(phase, currentIndex, totalSteps, isPivot, pivotReached) SequentialTransitionDecision
        +EvaluateStepFailure(phase, currentIndex, pivotReached, isRetryable) SequentialTransitionDecision
        +EvaluateStepUnknown(phase, currentIndex, attempt) SequentialTransitionDecision
    }

    class SequentialExecutionRules {
        +NextForwardStepIndex(currentIndex, totalSteps) (int, bool)
        +NextCompensatingStepIndex(currentIndex) (int, bool)
        +CanCompensate(phase, pivotReached, nature) bool
        +DetermineNextPhaseOnFailure(pivotReached) ExecutionPhase
        +ValidateStepOrdering(natures) error
    }

    class SequentialExecutionRepositoryPort {
        <<interface>>
        +CreateSequentialSaga(ctx, saga) error
        +GetSequentialSaga(ctx, tenantId, sagaId) SequentialSagaState
        +UpdateSagaCAS(ctx, tenantId, sagaId, expectedVersion, ownerEpoch, newState, nextStepIndex, nextPhase, pivotReached, output) error
        +SaveStepContext(ctx, entry) error
        +ScheduleTimer(ctx, timer) error
        +CancelTimer(ctx, tenantId, sagaId, stepIndex, kind) error
    }

    class PostgresSequentialExecutionRepositoryAdapter {
        -db *sql.DB
        +CreateSequentialSaga(ctx, saga) error
        +GetSequentialSaga(ctx, tenantId, sagaId) SequentialSagaState
        +UpdateSagaCAS(ctx, tenantId, sagaId, ...) error
    }

    SequentialExecutionService --> SequentialExecutionMachine
    SequentialExecutionService --> SequentialExecutionRepositoryPort
    SequentialExecutionMachine --> SequentialExecutionRules
    PostgresSequentialExecutionRepositoryAdapter ..|> SequentialExecutionRepositoryPort
```

---

## 3. Comprehensive State Machine Transition Model (FSM)

The sequential saga finite state machine manages forward progression, unknown outcome resolution, pre-pivot reverse compensation, and post-pivot roll-forward execution.

```mermaid
stateDiagram-v2
    [*] --> CREATED: Client POST /sagas/sequential
    CREATED --> RUNNING: Dispatch Step 0

    state RUNNING {
        [*] --> DISPATCHED
        DISPATCHED --> WAITING_STEP: Outbox Row Committed
        WAITING_STEP --> RETRY_SCHEDULED: Transient Failure & Under Max Retries
        RETRY_SCHEDULED --> DISPATCHED: Durable Timer Fired (Same Idempotency Key)
        WAITING_STEP --> RESOLVING_UNKNOWN: Network Drop / HTTP 504 Timeout
        
        state RESOLVING_UNKNOWN {
            [*] --> PROBING_DOWNSTREAM
            PROBING_DOWNSTREAM --> PROBE_SUCCEEDED: Remote Executed
            PROBING_DOWNSTREAM --> PROBE_NOT_FOUND: Remote Not Executed
            PROBING_DOWNSTREAM --> ESCALATE_ATTENTION: Remote Unreachable
        }
        
        PROBE_SUCCEEDED --> SUCCEEDED
        WAITING_STEP --> SUCCEEDED: 200 OK / Success Reply
    }

    SUCCEEDED --> RUNNING: Advance to Step i+1 [hasMore == true]
    SUCCEEDED --> COMPLETED: Final Step Succeeded [hasMore == false]

    RUNNING --> COMPENSATING: Step Failure (pivotReached == false)
    PROBE_NOT_FOUND --> COMPENSATING: Failure Verified (pivotReached == false)

    RUNNING --> ROLL_FORWARD: Step Failure (pivotReached == true)
    PROBE_NOT_FOUND --> ROLL_FORWARD: Failure Verified (pivotReached == true)

    state COMPENSATING {
        [*] --> DISPATCH_COMPENSATION
        DISPATCH_COMPENSATION --> WAITING_COMPENSATION: Outbox Row Committed
        WAITING_COMPENSATION --> STEP_COMPENSATED: Compensated OK
        STEP_COMPENSATED --> DISPATCH_COMPENSATION: Advance to Step i-1 [i > 0]
        WAITING_COMPENSATION --> COMPENSATION_RETRY: Error & Under Retry Limit
        COMPENSATION_RETRY --> DISPATCH_COMPENSATION: Backoff Timer Fired
        WAITING_COMPENSATION --> NEEDS_ATTENTION: Compensation Retries Exhausted
    }

    STEP_COMPENSATED --> FAILED_COMPENSATED: Step 0 Compensated Finished

    state ROLL_FORWARD {
        [*] --> RETRY_STEP
        RETRY_STEP --> RUNNING: Retry Succeeded
        RETRY_STEP --> NEEDS_ATTENTION: Max Retries Exhausted
    }

    state NEEDS_ATTENTION {
        [*] --> AWAITING_OPERATOR
        AWAITING_OPERATOR --> RUNNING: Operator Fixed & Retried
        AWAITING_OPERATOR --> COMPENSATING: Operator Approved Force-Rollback
        AWAITING_OPERATOR --> COMPLETED_MANUALLY: Operator Reconciled
        AWAITING_OPERATOR --> ABANDONED: Operator Wrote-Off
    }

    COMPLETED --> [*]
    FAILED_COMPENSATED --> [*]
    COMPLETED_MANUALLY --> [*]
    ABANDONED --> [*]
```

---

## 4. End-to-End Happy Path Forward Execution Sequence

This sequence traces step $i$ completing, context being recorded, and step $i+1$ being atomically dispatched via the transactional outbox.

```mermaid
sequenceDiagram
    autonumber
    participant Worker as Downstream Worker (Step i)
    participant Ingress as Reply Ingestion Service
    participant Engine as SequentialExecutionService
    participant FSM as SequentialExecutionMachine
    participant DB as PostgreSQL (Single ACID Tx)
    participant OutboxRelay as Outbox Poller / Relay
    participant NextWorker as Downstream Worker (Step i+1)

    Worker->>Ingress: POST /replies (saga_id, step_i, status=SUCCEEDED, output)
    Ingress->>Engine: HandleStepCompletion(tenant, saga_id, step_i, success=true, output)
    
    Engine->>DB: SELECT * FROM saga_instances WHERE tenant_id=... AND id=... FOR UPDATE
    DB-->>Engine: Returns Saga Instance (state=WAITING_STEP, step_index=i, version=v, epoch=e)
    
    Engine->>FSM: EvaluateStepSuccess(FORWARD, currentIndex=i, totalSteps=N, isPivot, pivotReached)
    FSM-->>Engine: Decision(NextStep=i+1, NextState=WAITING_STEP, Commands=[DISPATCH_STEP])
    
    Note over Engine,DB: Invariant SQ-03: Single Atomic Database Transaction
    Engine->>DB: BEGIN TRANSACTION
    Engine->>DB: UPDATE saga_instances SET current_step_index=i+1, version=v+1 WHERE version=v AND owner_epoch=e
    Engine->>DB: INSERT INTO saga_context_entries (tenant_id, saga_id, producer_step=i, entry_name, entry_value)
    Engine->>DB: DELETE FROM saga_timers WHERE saga_id=... AND step_index=i AND timer_kind='STEP_TIMEOUT'
    Engine->>DB: INSERT INTO saga_timers (tenant_id, saga_id, step_index=i+1, due_at=NOW()+timeout, timer_kind='STEP_TIMEOUT')
    Engine->>DB: INSERT INTO transactional_outbox (topic, event_type, idempotency_key, payload)
    Engine->>DB: COMMIT TRANSACTION
    
    Note over DB,NextWorker: Asynchronous Relay & Dispatch
    OutboxRelay->>DB: SELECT * FROM transactional_outbox WHERE status='PENDING' FOR UPDATE SKIP LOCKED
    OutboxRelay->>NextWorker: Emit Command via Kafka/HTTP (Idempotency Key: idemp_fwd_step_{i+1}_att_1)
    OutboxRelay->>DB: UPDATE transactional_outbox SET status='PROCESSED'
```

---

## 5. Ambiguous Timeout & Unknown Resolution Sequence

Demonstrates strict enforcement of **Invariant SQ-05 (Timeout $\neq$ Failure)**. Network timeouts trigger active probing rather than speculative compensation.

```mermaid
sequenceDiagram
    autonumber
    participant TimerPoller as Durable Timer Daemon
    participant Engine as SequentialExecutionService
    participant DB as PostgreSQL
    participant Downstream as Downstream Microservice
    participant FSM as SequentialExecutionMachine

    TimerPoller->>DB: SELECT * FROM saga_timers WHERE timer_state='PENDING' AND due_at <= NOW()
    DB-->>TimerPoller: Returns Step Timeout Timer (saga_id, step_index=i, attempt=1)
    TimerPoller->>Engine: HandleStepTimeout(tenant, saga_id, step_index=i, attempt=1)

    Engine->>DB: UPDATE saga_instances SET current_state='RESOLVING_UNKNOWN' WHERE id=... AND owner_epoch=...
    
    Note over Engine,Downstream: Invariant SQ-05: Probe Downstream by Deterministic Key
    Engine->>Downstream: GET /operations/status?idempotency_key=idemp_fwd_tenant_saga_i_att_1
    
    alt Case 1: Downstream Confirms Remote Execution Succeeded
        Downstream-->>Engine: 200 OK {status: "EXECUTED", result: {authCode: "A-99"}}
        Engine->>Engine: Treat as Step Success
        Engine->>DB: Persist Context & Advance to Step i+1
    else Case 2: Downstream Confirms No Execution (NOT_FOUND)
        Downstream-->>Engine: 404 Not Found / 200 OK {status: "NOT_FOUND"}
        Engine->>Engine: Verified Clean Failure (No Side Effects)
        alt Pre-Pivot Phase
            Engine->>FSM: EvaluateStepFailure(FORWARD, i, pivotReached=false)
            FSM-->>Engine: NextPhase=COMPENSATING, Start at Step i-1
            Engine->>DB: Transition to COMPENSATING
        else Post-Pivot Phase
            Engine->>FSM: EvaluateStepFailure(FORWARD, i, pivotReached=true)
            FSM-->>Engine: NextPhase=ROLL_FORWARD, Retry Step i
            Engine->>DB: Transition to ROLL_FORWARD
        end
    else Case 3: Downstream Endpoint Unreachable / Gateway Timeout Continues
        Downstream--xEngine: 504 Gateway Timeout / Socket Hangup
        Engine->>DB: UPDATE saga_instances SET current_state='NEEDS_ATTENTION'
        Engine->>Engine: Alert On-Call Engineers (No Automatic Rollback)
    end
```

---

## 6. Pre-Pivot Reverse Compensation Flowchart & Sequence

Illustrates **Invariants SQ-08 and SQ-09**: when Step 2 fails before the pivot point, completed Steps 1 and 0 are rolled back in exact reverse order ($1 \to 0$).

```mermaid
sequenceDiagram
    autonumber
    participant Engine as SequentialExecutionService
    participant DB as PostgreSQL
    participant Relay as Outbox Relay
    participant Worker1 as Step 1 Compensator (e.g. Release Hold)
    participant Worker0 as Step 0 Compensator (e.g. Invalidate Auth)

    Note over Engine,DB: Pre-Pivot Failure at Step 2
    Engine->>DB: BEGIN TRANSACTION
    Engine->>DB: UPDATE saga_instances SET execution_phase='COMPENSATING', current_step_index=1, current_state='COMPENSATING'
    Engine->>DB: INSERT INTO transactional_outbox (topic, event_type='COMPENSATE_STEP', idemp_key='idemp_comp_saga_step_1_att_1')
    Engine->>DB: COMMIT TRANSACTION

    Relay->>Worker1: Dispatch Compensation Command (Step 1)
    Worker1-->>Engine: Compensation Acknowledged (Step 1 SUCCEEDED)

    Note over Engine,DB: Step 1 Unwound, Now Unwind Step 0
    Engine->>DB: BEGIN TRANSACTION
    Engine->>DB: UPDATE saga_instances SET current_step_index=0
    Engine->>DB: INSERT INTO transactional_outbox (topic, event_type='COMPENSATE_STEP', idemp_key='idemp_comp_saga_step_0_att_1')
    Engine->>DB: COMMIT TRANSACTION

    Relay->>Worker0: Dispatch Compensation Command (Step 0)
    Worker0-->>Engine: Compensation Acknowledged (Step 0 SUCCEEDED)

    Note over Engine,DB: All Compensable Ancestors Unwound
    Engine->>DB: UPDATE saga_instances SET current_state='FAILED_COMPENSATED'
```

---

## 7. Post-Pivot Failure Roll-Forward & Operator Escalation

Enforces **Invariant SQ-08 (The Pivot Point of No Return)**: after a pivot step succeeds, failure in downstream steps triggers persistent retries and human escalation, never rollback.

```mermaid
sequenceDiagram
    autonumber
    participant Step3Worker as Fulfillment Worker (Step 3, Post-Pivot)
    participant Engine as SequentialExecutionService
    participant DB as PostgreSQL
    participant Ops as Support Operator Console

    Note over Engine: Step 2 (Payment Capture - PIVOT) already SUCCEEDED
    Step3Worker--xEngine: Step 3 Permanently Failed (Warehouse API Unreachable)
    
    Engine->>DB: SELECT * FROM saga_instances WHERE id=...
    DB-->>Engine: pivot_reached=TRUE, execution_phase='FORWARD'

    Note over Engine: Invariant SQ-08: Reverse Compensation Forbidden!
    Engine->>DB: BEGIN TRANSACTION
    Engine->>DB: UPDATE saga_instances SET execution_phase='ROLL_FORWARD', current_state='ROLLING_FORWARD'
    Engine->>DB: INSERT INTO saga_timers (kind='RETRY_BACKOFF', due_at=NOW()+exponential_backoff)
    Engine->>DB: COMMIT TRANSACTION

    Note over Engine: Unbounded Retries Exhausted (Attempt 10)
    Engine->>DB: UPDATE saga_instances SET current_state='NEEDS_ATTENTION'
    
    Ops->>DB: View Saga in Operator Console (Flags: PIVOT_REACHED, POST_PIVOT_FAILURE)
    Ops->>Ops: Manually create fulfillment ticket with warehouse supplier
    Ops->>Engine: POST /api/v1/sagas/{id}:resolve (Action: COMPLETE_MANUALLY, Reason: "Ticket W-9912 logged")
    Engine->>DB: UPDATE saga_instances SET current_state='COMPLETED_MANUALLY'
```

---

## 8. Monotonic Epoch Fencing & Zombie Worker Prevention

Demonstrates **Invariants SQ-02 and SQ-11**: worker crash recovery, lease stealing, and fencing off zombie workers using monotonic epochs.

```mermaid
sequenceDiagram
    autonumber
    participant Worker1 as Worker Pod 1 (Initial Owner)
    participant DB as PostgreSQL
    participant Worker2 as Worker Pod 2 (Recovery Sweeper)
    participant Downstream as Downstream Service

    Worker1->>DB: Acquire Lease (owner='worker-1', owner_epoch=10, lease_until=12:00:00)
    
    Note over Worker1: Worker 1 suffers severe GC freeze or network partition (12:00:00 - 12:00:30)
    
    Note over Worker2,DB: Lease Expires at 12:00:00
    Worker2->>DB: Crash Recovery Sweeper scans expired leases (lease_until < NOW())
    Worker2->>DB: UPDATE saga_instances SET owner='worker-2', owner_epoch=11, lease_until=12:01:00 WHERE id=... AND owner_epoch=10
    DB-->>Worker2: 1 Row Affected (Takeover Successful)

    Worker2->>Downstream: Dispatch Step i Command (owner_epoch=11)

    Note over Worker1: Worker 1 GC pause ends (Zombie Resume)
    Worker1->>DB: UPDATE saga_instances SET current_state='WAITING_STEP', version=v+1 WHERE id=... AND owner_epoch=10
    
    Note over DB,Worker1: Monotonic Fencing: Epoch 10 != Current Epoch 11
    DB-->>Worker1: 0 Rows Affected (CAS Fencing Rejection)
    Worker1->>Worker1: Detect Ownership Lost -> Terminate In-Memory Routine Immediately
```


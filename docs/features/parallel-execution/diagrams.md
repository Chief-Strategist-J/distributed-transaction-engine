# Parallel Execution — Architectural & Visual Diagrams

This document contains architectural blueprints, component models, and execution workflows for the **High-Throughput Parallel (Fork-Join) Execution Engine**, supporting [ADR-0010](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/parallel-execution/adr.md) and [Business Logic Decision Tree v2](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/parallel-execution/business-logic.md).

---

## 1. High-Level Architecture (HLD): Parallel Fork-Join Engine

```mermaid
flowchart TB
    subgraph IngressGateway ["Saga Engine Ingress"]
        Orchestrator["Saga Orchestrator Core"] -->|Evaluate Step: PARALLEL_GROUP| ForkEngine["ParallelForkEngine"]
    end

    subgraph AtomicFork ["Atomic PostgreSQL Fork Transaction (PX-01)"]
        ForkEngine -->|1. INSERT Group Header| DB_Group[("saga_parallel_groups<br/>status: DISPATCHED, join: ALL_REQUIRED")]
        ForkEngine -->|2. Bulk INSERT N Branches| DB_Branches[("saga_parallel_branches<br/>status: DISPATCHED (N rows)")]
        ForkEngine -->|3. Bulk INSERT Outbox Commands| DB_Outbox[("transactional_outbox<br/>N dispatch commands")]
    end

    subgraph AsynchronousFabric ["Asynchronous Dispatch Fabric"]
        DB_Outbox --> Relay["Outbox Relay"]
        Relay -->|Fan-Out Dispatch| KafkaTopics["Kafka Worker Topics"]
        KafkaTopics --> Worker1["Branch 1: Hotel Booking"]
        KafkaTopics --> Worker2["Branch 2: Flight Booking"]
        KafkaTopics --> Worker3["Branch 3: Car Rental"]
    end

    subgraph JoinBarrierLayer ["Join Barrier & Drain Synchronization"]
        Worker1 -->|Reply: SUCCEEDED| JoinConsumer["Branch Outcome Consumer"]
        Worker2 -->|Reply: SUCCEEDED| JoinConsumer
        Worker3 -->|Reply: FAILED| JoinConsumer
        JoinConsumer -->|Evaluate Join Policy (PX-05)| JoinEvaluator["JoinBarrierEvaluator"]
        JoinEvaluator -->|All Required Met?| DB_Group
        JoinEvaluator -->|Required Branch Failed -> Enter DRAINING| DrainBarrier["Pre-Compensation Drain Barrier (PX-03)"]
    end
```

---

## 2. Low-Level Component Model (LLD)

```mermaid
classDiagram
    class ForkJoinEnginePort {
        <<interface>>
        +DispatchParallelGroup(ctx, tenantID, sagaID, groupDef) error
        +RecordBranchOutcome(ctx, tenantID, groupID, branchID, outcome, payload) error
        +EvaluateJoinBarrier(ctx, tenantID, groupID) (JoinDecision, error)
    }

    class ParallelRepositoryPort {
        <<interface>>
        +CreateGroupAndBranchesAtomic(ctx, group, branches, outboxEvents) error
        +UpdateBranchStatusCAS(ctx, branchID, expectedVersion, outcome, payload) (bool, error)
        +LockAndEvaluateGroup(ctx, groupID) (*GroupAggregate, error)
        +TransitionGroupToDraining(ctx, groupID, failureReason) error
    }

    class JoinBarrierEvaluator {
        +Evaluate(group, branches) JoinEvaluationResult
        +CheckQuorum(branches, requiredCount) bool
        +CheckAlternatives(branches, altSets) bool
    }

    class ParallelExecutionService {
        -ParallelRepositoryPort repo
        -JoinBarrierEvaluator evaluator
        -OutboxDispatcherPort outbox
        +HandleBranchReply(ctx, reply) error
    }

    ForkJoinEnginePort <|.. ParallelExecutionService
    ParallelExecutionService --> ParallelRepositoryPort : invokes
    ParallelExecutionService --> JoinBarrierEvaluator : delegates
```

---

## 3. Atomic Fork & Join Synchronization Sequence

```mermaid
sequenceDiagram
    autonumber
    participant Engine as Saga Core
    participant DB as PostgreSQL 18
    participant Relay as Outbox Relay
    participant B1 as Branch 1 (Hotel)
    participant B2 as Branch 2 (Flight)

    Note over Engine: Step requires concurrent execution of Hotel + Flight
    Engine->>DB: BEGIN TRANSACTION
    Engine->>DB: INSERT INTO saga_parallel_groups (group_id='grp-1', policy='ALL_REQUIRED')
    Engine->>DB: INSERT INTO saga_parallel_branches (branch='hotel'), (branch='flight')
    Engine->>DB: INSERT INTO transactional_outbox (Dispatch hotel), (Dispatch flight)
    Engine->>DB: COMMIT TRANSACTION
    
    par Concurrent Outbox Relay
        Relay->>B1: Dispatch hotel hold (idem:fwd:grp-1:hotel:0)
    and
        Relay->>B2: Dispatch flight hold (idem:fwd:grp-1:flight:0)
    end

    B1-->>Engine: Hotel SUCCEEDED (hold_id: 'H-901')
    Engine->>DB: UPDATE saga_parallel_branches SET status='SUCCEEDED' WHERE branch='hotel'
    Engine->>DB: Evaluate Join Barrier: Flight still DISPATCHED -> NO-OP (Wait)

    B2-->>Engine: Flight SUCCEEDED (booking_ref: 'FL-44')
    Engine->>DB: UPDATE saga_parallel_branches SET status='SUCCEEDED' WHERE branch='flight'
    
    rect rgb(240, 255, 240)
        Note over Engine, DB: Join Barrier Satisfied (ALL_REQUIRED)
        Engine->>DB: UPDATE saga_parallel_groups SET status='SUCCEEDED', join_evaluated_at=NOW() WHERE group_id='grp-1' AND join_evaluated_at IS NULL
        DB-->>Engine: 1 Row Affected (Join Won)
    end
    Engine->>DB: Advance saga to next sequential phase
```

---

## 4. Failure Drain Barrier Sequence (Pre-Compensation Safety)

```mermaid
sequenceDiagram
    autonumber
    participant B1 as Branch 1 (Hotel - Dispatched)
    participant B2 as Branch 2 (Flight - Fails)
    participant Engine as Parallel Engine
    participant DB as PostgreSQL 18
    participant Comp as Compensation Engine

    B2-->>Engine: Flight FAILED (No seats available)
    Engine->>DB: UPDATE saga_parallel_branches SET status='FAILED' WHERE branch='flight'
    
    rect rgb(255, 235, 235)
        Note over Engine, DB: Invariant PX-03: Pre-Compensation Drain Barrier
        Engine->>DB: UPDATE saga_parallel_groups SET status = 'DRAINING' WHERE group_id = 'grp-1'
        Note over Engine: Compensation of Hotel hold is BLOCKED while Hotel is DISPATCHED!
    end

    Note over B1: 45ms later: Hotel returns 200 OK
    B1-->>Engine: Hotel SUCCEEDED (hold_id: 'H-901')
    Engine->>DB: UPDATE saga_parallel_branches SET status='SUCCEEDED' WHERE branch='hotel'
    
    rect rgb(240, 248, 255)
        Note over Engine, Comp: All branches now terminal! Safe to unwind
        Engine->>DB: UPDATE saga_parallel_groups SET status = 'FAILED'
        Engine->>Comp: Trigger Compensation for Hotel (hold_id: 'H-901')
    end
```

---

## 5. Parallel Group Lifecycle State Machine

```mermaid
stateDiagram-v2
    [*] --> DISPATCHED: Atomic Group & Branches Persisted (PX-01)
    
    DISPATCHED --> SUCCEEDED: Join Policy Met (ALL_REQUIRED / N_OF_M / FIRST_SUCCESS)
    DISPATCHED --> DRAINING: Any REQUIRED Branch Fails (Awaiting In-Flight Siblings)
    
    DRAINING --> FAILED: All In-Flight Branches Settle (Trigger Reverse DAG)
    
    DISPATCHED --> CANCELLED: Saga Cancel Requested (Drain & Compensate)
    DRAINING --> CANCELLED: External Cancel Arrives During Drain
    
    SUCCEEDED --> [*]
    FAILED --> [*]
    CANCELLED --> [*]
```


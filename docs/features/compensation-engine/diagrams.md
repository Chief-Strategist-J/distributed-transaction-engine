# Compensation Engine — Architectural & Visual Diagrams

This document contains architectural blueprints, component models, and sequence workflows for the **Compensation Engine** in the Distributed Transaction Engine platform, supporting [ADR-0005](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/compensation-engine/adr.md) and [Business Logic Decision Tree v2](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/compensation-engine/business-logic.md).

---

## 1. High-Level Architecture (HLD): Hexagonal Outbox Model

The Compensation Engine operates as an isolated domain core interacting exclusively through persistence and dispatch ports. This guarantees transactional atomicity across state updates, outbox dispatches, and residual ledgering.

```mermaid
flowchart TB
    subgraph IngressLayer ["Ingress & Trigger Layer"]
        Trigger["Failure / Deadline / Cancel Event"] -->|Trigger Reversal| Service["CompensationEngineService"]
        Admin["Operator Console"] -->|Manual Override / Residual Ack| API["Intervention API"]
        API --> Service
    end

    subgraph CoreDomain ["Compensation Domain Core (Hexagonal Center)"]
        Service -->|Enforce Invariants CE-01..15| Rules["CompensationRulesEvaluator"]
        Service -->|Topological Sort & Dependency Resolution| DAGBuilder["ReverseDAGPlanner"]
        Service -->|Evaluate Pivot Invariant| PivotGuard["PivotBoundaryGuard"]
    end

    subgraph PortsAdapters ["Infrastructure Ports & Adapters"]
        Service -->|Persist State & Residuals| RepoPort["CompensationRepositoryPort"]
        RepoPort -->|PostgreSQL Adapter| PGAdapter["PostgresCompensationRepositoryAdapter"]
        Service -->|Emit Compensation Commands| DispPort["CompensationDispatcherPort"]
        DispPort -->|Outbox Adapter| OutboxAdapter["TransactionalOutboxCompensationAdapter"]
    end

    subgraph StorageLayer ["PostgreSQL 18 Datastore"]
        PGAdapter -->|Atomic CAS UPDATE| DB_Comp[("saga_compensations")]
        PGAdapter -->|Edge Traversal| DB_Deps[("saga_compensation_dependencies")]
        PGAdapter -->|Record Non-Reversible Delta| DB_Residuals[("saga_compensation_residuals")]
        OutboxAdapter -->|INSERT Outbox Event with CAS| DB_Outbox[("transactional_outbox")]
    end

    subgraph TransportLayer ["Asynchronous Messaging Plane"]
        DB_Outbox -->|Debezium / Outbox Poller| Relay["Outbox Relay Daemon"]
        Relay -->|Publish Reverse Command| Kafka["Kafka Compensation Command Topics"]
        Kafka -->|Execute Undo Logic| Worker["Step Compensation Worker"]
        Worker -->|Emit Outcome Event| ReplyBus["Compensation Reply Topic"]
        ReplyBus -->|Idempotent Ingestion| InboxConsumer["Compensation Reply Consumer"]
        InboxConsumer -->|Update Step State| Service
    end

    subgraph BackgroundFleet ["Self-Healing & Timer Fleets"]
        TimerPoller["Durable Timer Poller"] -->|Retry Backoff Expiry| Service
        Sweeper["Crash Recovery Sweeper"] -->|Reclaim Expired Leases| PGAdapter
    end
```

---

## 2. Low-Level Component & Service Engine (LLD)

This class diagram illustrates the domain entity relationships, ports, and hexagonal adapter contracts governing the compensation subsystem.

```mermaid
classDiagram
    class EnginePort {
        <<interface>>
        +InitiateCompensation(ctx, tenantID, sagaID, reason) error
        +ProcessWorkerOutcome(ctx, tenantID, sagaID, stepID, outcome, payload) error
        +ResolveLateSuccess(ctx, tenantID, sagaID, stepID, forwardOutput) error
    }

    class RepositoryPort {
        <<interface>>
        +GetSagaPivotStatus(ctx, tenantID, sagaID) (bool, error)
        +StoreCompensationPlan(ctx, tenantID, sagaID, items) error
        +GetExecutableSteps(ctx, tenantID, sagaID) ([]CompensationPlanItem, error)
        +UpdateStepStatusCAS(ctx, tenantID, sagaID, stepID, expectedVersion, epoch, outcome, output) (bool, error)
        +RecordResidual(ctx, tenantID, sagaID, stepID, compID, amount, currency, reason) error
    }

    class DispatcherPort {
        <<interface>>
        +DispatchCompensationCommand(ctx, tenantID, sagaID, item) error
    }

    class CompensationEngineService {
        -RepositoryPort repo
        -DispatcherPort dispatcher
        -ReverseDAGPlanner planner
        -PivotBoundaryGuard pivotGuard
        +InitiateCompensation(ctx, tenantID, sagaID, reason) error
        +ProcessWorkerOutcome(ctx, tenantID, sagaID, stepID, outcome, payload) error
        +HandleBarrierResolution(ctx, tenantID, sagaID) error
    }

    class ReverseDAGPlanner {
        +BuildPlan(forwardHistory, workflowGraph) []CompensationPlanItem
        +PruneNonExecutedSteps(plan) []CompensationPlanItem
        +DeriveTopologicalBatches(plan) [][]CompensationPlanItem
    }

    class PivotBoundaryGuard {
        +CanCompensate(sagaState) bool
        +RouteToRollForward(sagaID) error
    }

    class CompensationPlanItem {
        +UUID ID
        +string StepID
        +UUID ForwardExecutionID
        +string IdempotencyKey
        +map Payload
        +StepCompensationOutcome Status
        +int Version
        +int64 Epoch
    }

    EnginePort <|.. CompensationEngineService
    CompensationEngineService --> RepositoryPort : invokes
    CompensationEngineService --> DispatcherPort : invokes
    CompensationEngineService --> ReverseDAGPlanner : delegates
    CompensationEngineService --> PivotBoundaryGuard : checks
    CompensationEngineService ..> CompensationPlanItem : orchestrates
```

---

## 3. End-to-End Reverse Compensation Sequence (With Drain Barrier)

```mermaid
sequenceDiagram
    autonumber
    participant Engine as Compensation Engine
    participant Barrier as Drain Barrier
    participant DB as PostgreSQL 18
    participant Outbox as Transactional Outbox
    participant Relay as Outbox Relay
    participant Worker_B as Worker B (Inventory)
    participant Worker_A as Worker A (Payment Auth)

    Note over Engine: Failure in Step 3 detected
    Engine->>DB: Check Pivot Status (CE-02)
    DB-->>Engine: Pivot NOT Reached (Safe)
    
    rect rgb(240, 248, 255)
        Note over Engine, Barrier: Pre-Compensation Drain Barrier (CE-01)
        Engine->>Barrier: Check in-flight (DISPATCHED) steps
        Barrier-->>Engine: Step 2 in-flight (network timeout pending)
        Engine->>Engine: Await terminal resolution or force UNKNOWN
        Barrier-->>Engine: All forward branches terminal
    end

    Engine->>Engine: ReverseDAGPlanner.BuildPlan() from forward outputs (CE-03)
    
    Engine->>DB: BEGIN TX
    Engine->>DB: INSERT INTO saga_compensations (items)
    Engine->>DB: INSERT INTO transactional_outbox (Dispatch Comp B)
    Engine->>DB: COMMIT TX (CAS version bump)

    Relay->>Worker_B: Dispatch Comp B (idem:comp:tx-1:inv:0)
    Worker_B-->>Engine: StepOutcome: SUCCEEDED
    
    Engine->>DB: UPDATE saga_compensations SET status='SUCCEEDED' WHERE step_id='inv'
    Note over Engine: Unlocks dependent prerequisite: Step A (Payment)
    
    Engine->>DB: BEGIN TX
    Engine->>DB: INSERT INTO transactional_outbox (Dispatch Comp A)
    Engine->>DB: COMMIT TX
    
    Relay->>Worker_A: Dispatch Comp A (idem:comp:tx-1:pay:0)
    Worker_A-->>Engine: StepOutcome: ALREADY_COMPENSATED (CE-06)
    
    Engine->>DB: UPDATE saga_compensations SET status='ALREADY_COMPENSATED'
    Engine->>DB: UPDATE saga_instances SET status='COMPENSATED' (Absorbing terminal)
```

---

## 4. Post-Pivot Failure: Transition to Durable Roll-Forward

When a failure strikes after the pivot transaction has committed, backward compensation is illegal. The system switches permanently to forward recovery.

```mermaid
sequenceDiagram
    autonumber
    participant Engine as Saga Orchestrator
    participant DB as PostgreSQL 18
    participant PivotGuard as Pivot Boundary Guard
    participant Alert as Prometheus / PagerDuty
    participant Worker as Fulfillment Worker

    Note over Engine: Step 3 (Fulfillment) fails with 500 Internal Error
    Engine->>PivotGuard: Evaluate CanCompensate(saga_id)
    PivotGuard->>DB: SELECT pivot_succeeded FROM saga_instances WHERE id = 'tx-42'
    DB-->>PivotGuard: pivot_succeeded = TRUE (Payment Captured)
    
    rect rgb(255, 235, 235)
        Note over PivotGuard, Engine: CE-02 Violation Intercepted
        PivotGuard-->>Engine: REJECT COMPENSATION (Illegal post-pivot rollback)
        Engine->>DB: UPDATE saga_instances SET status = 'ROLLING_FORWARD'
        Engine->>Alert: Emit Alert: SagaRollingForwardWarning (P2)
    end

    loop Exponential Backoff with Full Jitter
        Engine->>Worker: Retry Step 3 (idem:fwd:tx-42:step-3:attempt-N)
        Worker-->>Engine: HTTP 503 Retry-After 15s
    end

    Worker-->>Engine: Step 3 SUCCEEDED
    Engine->>DB: UPDATE saga_instances SET status = 'COMPLETED'
```

---

## 5. Late Forward Reply Resolution (CE-08 Reactive Compensation)

```mermaid
sequenceDiagram
    autonumber
    participant Worker as Slow Worker (Hotel Reservation)
    participant Engine as Compensation Engine
    participant DB as PostgreSQL 18
    participant Recon as Reconciliation Ledger

    Note over Engine: Step timed out after 30s; marked UNKNOWN
    Note over Engine: Compensation ran; saga marked COMPENSATED
    
    Worker->>Engine: Late Reply: "Hotel Room 402 Booked" (SUCCEEDED)
    Engine->>DB: Read saga status
    DB-->>Engine: status = 'COMPENSATED'
    
    rect rgb(255, 245, 230)
        Note over Engine, Recon: CE-08 Reactive Rollback Protocol
        Engine->>DB: INSERT INTO saga_compensations (step_id='hotel', status='PENDING')
        Engine->>Worker: Dispatch Reactive Compensation (idem:comp:tx-1:hotel:reactive)
        Worker-->>Engine: Confirmed Hotel Room 402 Cancelled
        Engine->>DB: UPDATE saga_compensations SET status = 'SUCCEEDED'
        Engine->>Recon: Append entry to audit_log (Late reconciliation resolved)
    end
```

---

## 6. Compensation Unit State Machine (Full Lifecycle)

```mermaid
stateDiagram-v2
    [*] --> PENDING: Reverse Plan Generated
    
    PENDING --> DRAINING_FORWARD: Waiting on In-Flight Barrier (CE-01)
    DRAINING_FORWARD --> DISPATCHED: Barrier Resolved & In-Degree == 0
    
    PENDING --> DISPATCHED: Prerequisites Satisfied
    
    DISPATCHED --> SUCCEEDED: Worker Confirms Normal Undo
    DISPATCHED --> ALREADY_COMPENSATED: Provider Confirms Idempotent Prior Undo
    DISPATCHED --> NOTHING_TO_COMPENSATE: Forward Step Was Proven No-Op
    
    DISPATCHED --> FAILED_RETRYABLE: Transient Error (Network/503/429)
    FAILED_RETRYABLE --> DISPATCHED: Jittered Timer Expiration (Attempt < Max)
    
    DISPATCHED --> FAILED_TERMINAL: Retries Exhausted -> Escalate to DLQ
    DISPATCHED --> NOT_COMPENSABLE: External Provider Disallows Undo (CE-10)
    
    NOT_COMPENSABLE --> COMPENSATION_STUCK: Record in Residual Ledger
    FAILED_TERMINAL --> COMPENSATION_STUCK: Alert Operator
    
    SUCCEEDED --> [*]
    ALREADY_COMPENSATED --> [*]
    NOTHING_TO_COMPENSATE --> [*]
    COMPENSATION_STUCK --> [*]: Manual Intervention / Force Ack
```

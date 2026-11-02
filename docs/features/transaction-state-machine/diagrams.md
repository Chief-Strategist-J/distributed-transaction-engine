# Transaction State Machine — Visual Architecture & Process Flow Diagrams

This document illustrates how the transaction state machine works, featuring both intuitive diagrams for business and non-technical stakeholders and in-depth engineering sequence diagrams.

---

## 1. Non-Technical / Business Overview: How an Order Travels Through the System

Imagine an e-commerce order or a travel booking. The transaction engine works like a smart digital traffic controller. It guarantees that an order either completely finishes or completely cancels, with zero money or inventory lost.

### The Plain-English Journey of a Transaction

```mermaid
flowchart TD
    Start(["Customer Places Order"]):::startClass --> CREATED["CREATED<br/><i>Order intent registered; zero money taken</i>"]

    CREATED -->|"Customer cancels immediately"| CANCELLED["CANCELLED<br/><b>[TERMINAL]</b><br/><i>No charges made</i>"]:::terminalFail
    CREATED -->|"Worker claims order"| RUNNING["RUNNING<br/><i>Executing payments, stock, bookings</i>"]:::activeClass

    RUNNING -->|"All steps succeed cleanly"| COMPLETED["COMPLETED<br/><b>[TERMINAL]</b><br/><i>Order finalized & shipped</i>"]:::terminalSuccess

    RUNNING -->|"Needs external approval (e.g. Bank SMS)"| WAITING["WAITING<br/><i>Paused awaiting callback</i>"]:::waitClass
    WAITING -->|"Approval received"| RUNNING
    WAITING -->|"Rejected or timed out"| COMPENSATING

    RUNNING -->|"Partner busy (HTTP 429 Too Many Requests)"| THROTTLED["THROTTLED WAIT<br/><i>Lease released; sleep timer set</i>"]:::waitClass
    THROTTLED -->|"Timer expires"| RUNNING

    RUNNING -->|"Network dropped (Unknown bank outcome)"| RECONCILING["RECONCILING<br/><i>Probing status to avoid double-charging</i>"]:::waitClass
    RECONCILING -->|"Transaction confirmed"| RUNNING
    RECONCILING -->|"Partner permanently unavailable"| STUCK

    RUNNING -->|"Step fails (e.g. Card declined, out of stock)"| COMPENSATING["COMPENSATING<br/><i>Automatically undoing prior steps & refunding</i>"]:::rollbackClass
    
    COMPENSATING -->|"All refunds & holds released"| COMPENSATED["COMPENSATED<br/><b>[TERMINAL]</b><br/><i>Customer fully refunded</i>"]:::terminalSuccess
    COMPENSATING -->|"Partner refund API down (Max retries reached)"| STUCK["STUCK<br/><i>Quarantined safely; on-call SRE alerted</i>"]:::alertClass

    STUCK -->|"Engineer fixes outage & clicks Re-drive"| COMPENSATING
    STUCK -->|"Permanent accounting write-off approved"| FAILED["FAILED<br/><b>[TERMINAL]</b><br/><i>Escalated to financial ledger</i>"]:::terminalFail

    classDef startClass fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#0d47a1;
    classDef activeClass fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20;
    classDef waitClass fill:#fff8e1,stroke:#f57f17,stroke-width:2px,color:#e65100;
    classDef rollbackClass fill:#fbe9e7,stroke:#d84315,stroke-width:2px,color:#bf360c;
    classDef alertClass fill:#ffebee,stroke:#c62828,stroke-width:3px,color:#b71c1c;
    classDef terminalSuccess fill:#f1f8e9,stroke:#33691e,stroke-width:2px,color:#1b5e20;
    classDef terminalFail fill:#efebe9,stroke:#4e342e,stroke-width:2px,color:#3e2723;
```

### Business State Plain-English Glossary

* **CREATED**: The customer just clicked "Submit". The order intent is registered, but no money has been taken and no inventory held yet.
* **RUNNING**: The engine is actively coordinating steps (e.g., deducting points, charging credit card, notifying warehouse).
* **WAITING**: The engine paused safely while waiting for an external event (such as a 3D-Secure SMS bank authorization code or identity check).
* **THROTTLED WAIT**: The bank or partner asked us to slow down. The engine steps back without burning server resources until the timer expires.
* **RECONCILING**: A network hiccup occurred and we aren't sure if the bank processed the charge or not. Instead of guessing or double-charging, the engine pauses to safely check the bank's records first.
* **COMPENSATING**: A step failed (e.g., card declined or hotel room sold out). The engine is actively undoing any earlier steps in reverse order (releasing holds, issuing refunds).
* **STUCK**: A partner service broke and won't respond to refunds after multiple attempts. The order is quarantined safely so an on-call engineer can inspect it without risk of runaway loops.
* **COMPLETED (Terminal)**: Success! The transaction finished 100% cleanly.
* **CANCELLED (Terminal)**: The user cancelled before execution started. Zero charges were made.
* **COMPENSATED (Terminal)**: Rollback succeeded 100%. The customer was refunded and all held resources were freed.
* **FAILED (Terminal)**: The transaction was formally written off by an authorized operator.

---

## 2. Technical Hexagonal Component Architecture

```mermaid
graph TD
    subgraph INGRESS["Delivery & Ingress Layer"]
        Router["TransactionStateMachineRouter<br/><i>Mounts HTTP Endpoints</i>"]
        Handler["TransactionStateMachineHandler<br/><i>Closed-Schema Parsing, W3C Traceparent, RFC Envelope</i>"]
        Router --> Handler
    end

    subgraph CORE["Application Core & Domain Layer"]
        Facade["Public Facade<br/><i>features/transaction-state-machine/index.go</i>"]
        Service["TransactionStateMachineService<br/><i>Orchestrator: Fencing Tokens & Optimistic CAS Guard</i>"]
        FSM["TransactionStateMachine<br/><i>domain/machines: Pure Mathematical FSM</i>"]
        Port["TransactionStateMachineRepositoryPort<br/><i>infrastructure/ports</i>"]
        Adapter["PostgresTransactionStateMachineAdapter<br/><i>infrastructure/adapters</i>"]
        
        Facade --> Service
        Service -->|"ComputeNextState(State, Event)"| FSM
        Service -->|"UpdateStateCAS(ctx, ...)"| Port
        Port -.->|"Satisfied By"| Adapter
    end

    subgraph STORAGE["Storage Gate Layer (PostgreSQL 14+)"]
        StatesTable[("saga_instance_states<br/><b>PK:</b> tenant_id, id<br/><b>UK:</b> tenant_id, public_id<br/><b>UK:</b> tenant_id, source, external_id")]
        AuditTable[("state_transition_audit<br/><b>PK:</b> tenant_id, id<br/><b>FK:</b> tenant_id, saga_id")]
        TransitionsTable[("saga_state_transitions<br/><b>PK:</b> from_state, to_state")]
        
        StatesTable -->|"Lookup matrix constraint"| TransitionsTable
        AuditTable -->|"Foreign Key Constraint"| StatesTable
    end

    Handler --> Facade
    Adapter -->|"SQL Parameterized FLOW_*"| StatesTable
    Adapter -->|"Atomic Single TX Insert"| AuditTable
```

---

## 3. Engineering State Machine Transition Topology

```mermaid
stateDiagram-v2
    [*] --> CREATED: Initialize Transaction

    CREATED --> RUNNING: ORCHESTRATOR_CLAIM
    CREATED --> CANCELLED: CLIENT_ABORT

    RUNNING --> RUNNING: STEP_SUCCESS (Intermediate DAG Step)
    RUNNING --> COMPLETED: DAG_COMPLETED (All Steps Succeeded)
    RUNNING --> COMPENSATING: BUSINESS_FAILURE
    RUNNING --> THROTTLED_WAIT: RATE_LIMITED (HTTP 429)
    RUNNING --> RECONCILING: INDETERMINATE_ERROR (HTTP 504 / TCP Drop)
    RUNNING --> WAITING: ASYNC_SUSPEND (Waiting for Callback/Webhook)

    WAITING --> RUNNING: WEBHOOK_APPROVED
    WAITING --> COMPENSATING: WEBHOOK_REJECTED / WEBHOOK_TIMEOUT

    THROTTLED_WAIT --> RUNNING: THROTTLE_TIMER_FIRED

    RECONCILING --> RUNNING: PROBE_SUCCEEDED / PROBE_NOT_FOUND (Safe to Retry)
    RECONCILING --> COMPENSATING: BUSINESS_FAILURE
    RECONCILING --> STUCK: PROBE_BUDGET_EXPIRED (Downstream Down)

    COMPENSATING --> COMPENSATING: COMPENSATION_STEP_OK (Semantic Undo)
    COMPENSATING --> COMPENSATED: COMPENSATION_DONE (All Undone)
    COMPENSATING --> STUCK: COMPENSATION_HALT (Downstream 503 Max Attempts)

    STUCK --> COMPENSATING: OPERATOR_REDRIVE (SRE Triage Fix)
    STUCK --> FAILED: OPERATOR_WRITEOFF (Financial Write-Off)

    COMPLETED --> [*]: TERMINAL IMMUTABLE
    CANCELLED --> [*]: TERMINAL IMMUTABLE
    COMPENSATED --> [*]: TERMINAL IMMUTABLE
    FAILED --> [*]: TERMINAL IMMUTABLE
```

---

## 4. Concurrency Safety: Compare-And-Swap (CAS) Sequence Diagram

```mermaid
sequenceDiagram
    autonumber
    actor Client as Ingress / Worker
    participant Handler as TransactionStateMachineHandler
    participant Svc as TransactionStateMachineService
    participant FSM as TransactionStateMachine (Domain)
    participant Repo as PostgresRepositoryAdapter
    participant DB as PostgreSQL Database

    Client->>Handler: POST /api/v1/sagas/transitions<br/>(sagaId, event, expectedVersion=2, fencingToken=10)
    Handler->>Svc: TransitionState(ctx, input)
    
    rect rgb(240, 248, 255)
        note over Svc,FSM: Domain Validation & State Computation
        Svc->>Repo: GetState(ctx, tenantId, sagaId)
        Repo->>DB: SELECT * FROM saga_instance_states WHERE tenant_id=$1 AND id=$2
        DB-->>Repo: Row(current_state='RUNNING', version=2, fencing_token=10)
        Repo-->>Svc: SagaStateRecord

        Svc->>Svc: Guard 1: Verify expectedVersion (2 == 2)
        Svc->>Svc: Guard 2: Verify fencingToken (10 >= 10)
        
        Svc->>FSM: ComputeNextState('RUNNING', 'DAG_COMPLETED')
        FSM-->>Svc: 'COMPLETED'
    end

    rect rgb(255, 250, 240)
        note over Svc,DB: Atomic Database Commit Boundary
        Svc->>Repo: UpdateStateCAS(ctx, 'RUNNING', 'COMPLETED', expectedVersion=2, fencingToken=10)
        Repo->>DB: BEGIN TRANSACTION
        Repo->>DB: UPDATE saga_instance_states SET current_state='COMPLETED', version=3<br/>WHERE tenant_id=$1 AND id=$2 AND version=2 AND current_state='RUNNING'
        alt Rows Affected == 1 (Success)
            Repo->>DB: INSERT INTO state_transition_audit (from='RUNNING', to='COMPLETED', ver=3...)
            Repo->>DB: COMMIT TRANSACTION
            DB-->>Repo: Transaction Committed
            Repo-->>Svc: Updated SagaStateRecord (version=3)
            Svc-->>Handler: Updated Record
            Handler-->>Client: HTTP 200 OK + ApiResponse Envelope
        else Rows Affected == 0 (Concurrent Collision / Stale Version)
            Repo->>DB: ROLLBACK TRANSACTION
            Repo-->>Svc: ConflictError("optimistic concurrency conflict: state or version mismatch")
            Svc-->>Handler: ApplicationError(409, "CONFLICT")
            Handler-->>Client: HTTP 409 Conflict (Client Retries with Backoff)
        end
    end
```

---

## 5. Zombie Worker Fencing Sequence

```mermaid
sequenceDiagram
    autonumber
    participant WorkerA as Worker A (Zombie - Slow GC Pause)
    participant WorkerB as Worker B (Active Lease Holder)
    participant Engine as Transaction State Machine
    participant DB as PostgreSQL

    Note over WorkerA: Worker A pauses for 30s during GC pause.<br/>Lease expires at T=10s.
    Note over WorkerB: Worker B discovers expired lease at T=11s.<br/>Claims saga with incremented fencing token.
    WorkerB->>Engine: Claim Lease (fencingToken=43)
    Engine->>DB: UPDATE saga_instance_states SET fencing_token=43, version=version+1
    DB-->>Engine: Committed (fencing_token=43, version=5)
    
    Note over WorkerA: Worker A wakes up from GC pause at T=32s.<br/>Attempts to commit step result with stale token 42.
    WorkerA->>Engine: Commit Step (fencingToken=42, expectedVersion=4)
    Engine->>Engine: Check: incoming token (42) < current record token (43)
    Engine-->>WorkerA: ERROR 409: "stale fencing token: rejected by newer execution owner"
    Note over WorkerA: Worker A aborts without corrupting state.
```

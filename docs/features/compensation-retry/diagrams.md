# Compensation Retry — Architecture & System Diagrams

## 1. High-Level Architecture (HLD)

```mermaid
graph TD
    subgraph "Core Engine"
        CE["Compensation Engine<br/>(Reverse DAG Execution)"]
        CRS["Compensation Retry Service<br/>(Time-Horizon & Ambiguity Logic)"]
        FSM["Compensation Step FSM<br/>(PENDING -> RETRY_SCHEDULED -> DONE / NEEDS_ATTENTION)"]
    end

    subgraph "Storage & Fencing Barrier"
        PG["PostgreSQL ACID Store<br/>(compensation_retries, step_compensations)"]
        FT["Fencing Barrier Gate<br/>(fencing_leases, Statement 1)"]
        OB["Transactional Outbox<br/>(outbox_events)"]
    end

    subgraph "Downstream Targets & Escalation"
        GW["Downstream Payment/Supplier Gateway"]
        SQ["Gateway Status Query API"]
        OPS["SRE / Finance Escalation Ladder<br/>(Level 1 / 2 / 3 Alerts)"]
    end

    CE --> CRS
    CRS --> FSM
    CRS -- "Fenced Barrier Mutation" --> FT
    FT --> PG
    CRS -- "Emit Redrive Command" --> OB
    OB --> GW
    GW -. "Timeout / 504" .-> CRS
    CRS -- "CR-02 Ambiguity Check" --> SQ
    CRS -- "Deadline Risk / Horizon Exceeded" --> OPS
```

---

## 2. Low-Level Architecture (LLD)

```mermaid
classDiagram
    class CompensationRetryService {
        -RepositoryPort repo
        -CompensationRetryRules rules
        -CompensationRetryMachine machine
        -Tracer tracer
        +EvaluateCompensationResult(ctx, input) (CompensationDecision, error)
        +ScheduleRedrive(ctx, input) error
        +VerifyAmbiguousAttempt(ctx, input) (VerificationResult, error)
        +GetStepRetryJournal(ctx, tenantID, sagaID, stepID) (CompensationRetryRecord, error)
        +PauseStepType(ctx, stepType, reason) error
        +ResumeStepType(ctx, stepType) error
    }

    class CompensationRetryRules {
        +ClassifyResult(statusCode, errCode, responseBody) CompensationOutcomeClass
        +CalculateBackoff(attempt, baseMillis, maxMillis, retryAfterHeader) time.Duration
        +EvaluateBusinessHorizon(now, firstAttempt, deadline, maxAttempts) HorizonDecision
        +DetermineEscalationLevel(now, deadline, leadTimeMinutes) EscalationLevel
    }

    class CompensationRetryMachine {
        +Transition(current, event) (StepCompensationState, error)
        +CanRetry(state) bool
        +IsTerminal(state) bool
    }

    class RepositoryPort {
        <<interface>>
        +RecordAttempt(ctx, record) error
        +UpdateStepCompensationState(ctx, tenantID, sagaID, stepID, state) error
        +GetActiveRetries(ctx, tenantID, limit) ([]CompensationRetryRecord, error)
        +PauseStepType(ctx, stepType, reason) error
        +IsStepTypePaused(ctx, stepType) (bool, error)
    }

    class MemoryCompensationRetryAdapter {
        -sync.RWMutex mu
        -map[string]CompensationRetryRecord records
        -map[string]bool pausedTypes
    }

    class PostgresCompensationRetryAdapter {
        -DBExecutor db
    }

    CompensationRetryService --> RepositoryPort
    CompensationRetryService --> CompensationRetryRules
    CompensationRetryService --> CompensationRetryMachine
    RepositoryPort <|.. MemoryCompensationRetryAdapter
    RepositoryPort <|.. PostgresCompensationRetryAdapter
```

---

## 3. Compensation Step State Machine (FSM)

```mermaid
stateDiagram-v2
    [*] --> PENDING_COMPENSATION
    PENDING_COMPENSATION --> DISPATCHED: Dispatch Rollback
    
    DISPATCHED --> COMPENSATED: Explicit Success / Verified
    DISPATCHED --> VERIFYING_AMBIGUOUS: Timeout / 504 Gateway Timeout
    DISPATCHED --> RETRY_SCHEDULED: Transient 503 / 429 Rate Limited
    DISPATCHED --> STEP_TYPE_PAUSED: 400 Bad Schema / Credential Revoked
    DISPATCHED --> BUSINESS_ALTERNATIVE: NOT_COMPENSABLE (Window Closed / Shipped)
    
    VERIFYING_AMBIGUOUS --> COMPENSATED: Status Query Confirmed
    VERIFYING_AMBIGUOUS --> RETRY_SCHEDULED: Status Query Returns NOT_FOUND
    
    RETRY_SCHEDULED --> DISPATCHED: Redrive Timer Fires (Within Horizon)
    RETRY_SCHEDULED --> FROZEN_BREAKER: Downstream Circuit Breaker Opened
    FROZEN_BREAKER --> RETRY_SCHEDULED: Circuit Breaker Closed
    
    RETRY_SCHEDULED --> NEEDS_ATTENTION: Horizon Exhausted / Deadline Imminent
    STEP_TYPE_PAUSED --> RETRY_SCHEDULED: Defect Fixed / Credentials Rotated
    
    COMPENSATED --> [*]
    BUSINESS_ALTERNATIVE --> [*]
    NEEDS_ATTENTION --> [*]
```

---

## 4. Sequence Diagram 1: Ambiguity Verification & Status Query

```mermaid
sequenceDiagram
    autonumber
    participant W as Compensation Worker
    participant CRS as CompensationRetryService
    participant SQ as Gateway Status Query API
    participant PG as PostgreSQL Storage

    W->>CRS: EvaluateCompensationResult(StatusCode=504, Err="Gateway Timeout")
    Note over CRS: Classified as AMBIGUOUS (CR-02)
    CRS->>SQ: QueryStatusByCompensationKey(Key="idk_refund_998877")
    alt Status Query Returns: SUCCESS (Refund already processed)
        SQ-->>CRS: Status=REFUNDED, Reference="ref_bank_12345"
        CRS->>PG: Mark Step State = COMPENSATED (Zero double refund!)
        CRS-->>W: Action = DONE (Prevent redundant re-send)
    else Status Query Returns: NOT_FOUND
        SQ-->>CRS: Status=NOT_FOUND (Refund was not processed)
        CRS->>PG: Mark Step State = RETRY_SCHEDULED, Schedule Jittered Redrive
        CRS-->>W: Action = RETRY (Safe to re-send with same idempotency key)
    end
```

---

## 5. Sequence Diagram 2: Jittered Backoff & Dependency Breaker Interplay

```mermaid
sequenceDiagram
    autonumber
    participant W as Compensation Worker
    participant CRS as CompensationRetryService
    participant CB as Dependency Circuit Breaker
    participant PG as PostgreSQL Storage

    W->>CRS: EvaluateCompensationResult(StatusCode=503, Err="Service Unavailable")
    CRS->>CB: CheckBreakerStatus("dep_payment_gateway")
    alt Circuit Breaker is OPEN
        CB-->>CRS: State = OPEN
        Note over CRS: CR-05: Outage Freezes Budget without consuming attempts
        CRS->>PG: Mark Step State = FROZEN_BREAKER (Freeze backoff timer)
        CRS-->>W: Action = FREEZE_PENDING_BREAKER
    else Circuit Breaker is CLOSED
        CB-->>CRS: State = CLOSED
        Note over CRS: Calculate Full Jitter Backoff (Floor >= Base/2)
        CRS->>PG: Mark Step State = RETRY_SCHEDULED (next_attempt_at = NOW + JitterDelay)
        CRS-->>W: Action = RETRY (Delay = 3,450ms)
    end
```

---

## 6. Sequence Diagram 3: Redrive Execution & Pre-Deadline Escalation Ladder

```mermaid
sequenceDiagram
    autonumber
    participant TIMER as Durable Redrive Scheduler
    participant CRS as CompensationRetryService
    participant PG as PostgreSQL Storage
    participant OPS as Alerting / Escalation Ladder
    participant OB as Transactional Outbox

    TIMER->>CRS: FireScheduledCompensationRetry(TenantID, SagaID, StepID, AttemptID=3)
    Note over CRS: CR-07: Fenced verification of Step-Level State
    CRS->>PG: SELECT state, attempt_id, deadline FROM step_compensations WHERE step_id=:id
    alt Late Success Arrived or Manual Resolution Recorded (CR-10)
        PG-->>CRS: State = COMPENSATED
        CRS-->>TIMER: Discard Redrive (Step already completed)
    else Remaining Time < LeadTimeMinutes
        PG-->>CRS: State = RETRY_SCHEDULED, Deadline - NOW < 30min
        CRS->>OPS: Fire Level 2 Pager Alert ("Refund Deadline Imminent! Remaining: 25min")
        CRS->>OB: Append Redrive Command to outbox_events with Priority=CRITICAL
        CRS-->>TIMER: Redrive Dispatched with Pre-Deadline Escalation
    end
```


# Retry Policies — Architecture Diagrams

This document illustrates the High-Level Architecture, Component Topology, Finite State Machine, and Sequence Diagrams for the Deterministic Retry Policies feature.

---

## 1. High-Level Design (HLD)

The Retry Policies engine operates as the authoritative decision gate between the step execution dispatcher and downstream services.

```mermaid
graph TD
    SagaEngine["Saga Orchestrator Engine"] -->|"1. Dispatch Step Command"| RetryEngine["Retry Policy Engine"]
    
    subgraph Core ["Retry Engine Decision Pipeline"]
        RetryEngine -->|"2. Check Circuit Breaker"| Breaker["Dependency Circuit Breaker"]
        Breaker -->|"3. Consume Token"| Limiter["Tenant / Dependency Token Bucket"]
        Limiter -->|"4. Execute Dispatch"| HTTPClient["Protocol Dispatcher (gRPC / HTTP)"]
        HTTPClient -->|"5. Return Status / Error"| Classifier["Multidimensional Error Classifier"]
        Classifier -->|"6. Compute Jitter"| Backoff["Backoff Calculator (Full / Equal Jitter)"]
    end
    
    subgraph Storage ["Durable Persistence Tier (PostgreSQL)"]
        RetryEngine -->|"7. Persist Attempt & Timers"| DB[("step_retries & durable_timers Table")]
    end
    
    HTTPClient -->|"8. Outbound Request"| Downstream["Downstream Microservice / Gateway"]
```

---

## 2. Low-Level Component Architecture (LLD)

Hexagonal architecture separating classification rules, backoff calculators, circuit breakers, and persistence adapters.

```mermaid
classDiagram
    class RetryPolicyHandler {
        +EvaluateRetry(w, r)
        +GetDependencyStatus(w, r)
    }

    class RetryPolicyService {
        -repo RepositoryPort
        -classifier ErrorClassifier
        -backoffCalculator BackoffCalculator
        -breakerManager CircuitBreakerManager
        -tracer Tracer
        +ProcessStepFailure(ctx, req) RetryDecision
        +ScheduleRetryTimer(ctx, stepID, delay) error
    }

    class ErrorClassifier {
        +Classify(err, status, attempt, capabilities) ErrorClass
        +IsDeterministic(fingerprintHistory) bool
    }

    class BackoffCalculator {
        +CalculateDelay(strategy, attempt, base, cap, floor, retryAfter) Duration
        +ParseRetryAfter(header) (Duration, bool)
    }

    class CircuitBreakerManager {
        +AllowRequest(dependencyID) bool
        +RecordSuccess(dependencyID)
        +RecordFailure(dependencyID)
        +GetState(dependencyID) BreakerState
    }

    class RepositoryPort {
        <<interface>>
        +RecordAttempt(ctx, attemptRecord) error
        +ScheduleDurableTimer(ctx, timerRecord) error
        +GetAttemptHistory(ctx, stepID) []AttemptRecord
    }

    RetryPolicyHandler --> RetryPolicyService
    RetryPolicyService --> ErrorClassifier
    RetryPolicyService --> BackoffCalculator
    RetryPolicyService --> CircuitBreakerManager
    RetryPolicyService --> RepositoryPort
```

---

## 3. Finite State Machine (FSM)

Lifecycle of a retry attempt from dispatch to resolution.

```mermaid
stateDiagram-v2
    [*] --> DISPATCHING: Attempt Recorded in DB
    
    DISPATCHING --> SUCCEEDED: 2xx Success / Contract Succeeded
    DISPATCHING --> TRANSIENT_FAIL: 429 / 503 / DNS Refused
    DISPATCHING --> AMBIGUOUS_FAIL: 500 / 502 / 504 / Socket Drop
    DISPATCHING --> TERMINAL_REJECT: 400 / 422 / Business Rejection
    
    TRANSIENT_FAIL --> RETRY_SCHEDULED: Budget Available & Delay < Deadline
    TRANSIENT_FAIL --> BUDGET_EXHAUSTED: Max Attempts / Max Elapsed Time
    
    AMBIGUOUS_FAIL --> STATUS_QUERYING: Idempotent=False & StatusQuery=True
    AMBIGUOUS_FAIL --> RETRY_SCHEDULED: Idempotent=True
    AMBIGUOUS_FAIL --> NEEDS_ATTENTION: Idempotent=False & StatusQuery=False
    
    STATUS_QUERYING --> SUCCEEDED: Target Confirms EXECUTED
    STATUS_QUERYING --> RETRY_SCHEDULED: Target Confirms NOT_FOUND
    
    RETRY_SCHEDULED --> DISPATCHING: Durable Timer Fires & Breaker Closed
    
    BUDGET_EXHAUSTED --> COMPENSATING: Pre-Pivot Step
    BUDGET_EXHAUSTED --> NEEDS_ATTENTION: Post-Pivot Step (Irreversible)
    
    TERMINAL_REJECT --> COMPENSATING: Pre-Pivot Step
    TERMINAL_REJECT --> NEEDS_ATTENTION: Post-Pivot Step
    
    SUCCEEDED --> [*]
    COMPENSATING --> [*]
    NEEDS_ATTENTION --> [*]
```

---

## 4. Sequence Diagram 1: Transient 503 with Jittered Backoff & `Retry-After`

```mermaid
sequenceDiagram
    autonumber
    actor Engine as Saga Engine
    participant Svc as Retry Policy Service
    participant Calc as Backoff Calculator
    participant DB as PostgreSQL
    participant Target as Inventory Service

    Engine->>Svc: ExecuteStep(Step 2, Attempt 1, IdempotencyKey)
    Svc->>DB: INSERT step_retries (Attempt 1, State=DISPATCHING)
    Svc->>Target: POST /api/v1/inventory/reserve (Key: ik_step_2)
    Target-->>Svc: HTTP 503 Service Unavailable (Retry-After: 4)
    
    Svc->>Calc: CalculateDelay(Attempt 1, Base=500ms, RetryAfter=4s)
    Calc-->>Svc: Delay = 4.250s (Max(Computed, 4s) + Jitter)
    
    Svc->>DB: UPDATE step_retries SET outcome='TRANSIENT_NOT_EXECUTED', next_attempt_at=NOW()+4.25s
    Svc->>DB: INSERT durable_timers (fire_at=NOW()+4.25s, action='DISPATCH_STEP_2')
    Svc-->>Engine: RETRY_SCHEDULED (Delay: 4.25s)
    
    Note over Svc,DB: 4.25 Seconds Elapse (Timer Fires)
    
    DB->>Svc: DurableTimerTrigger(Step 2, Attempt 2)
    Svc->>Target: POST /api/v1/inventory/reserve (Key: ik_step_2, FrozenPayload)
    Target-->>Svc: HTTP 200 OK ({"holdId": "hld_999"})
    Svc->>DB: UPDATE step_retries SET outcome='SUCCEEDED'
    Svc-->>Engine: STEP_SUCCEEDED
```

---

## 5. Sequence Diagram 2: Ambiguous 504 on Non-Idempotent Step with Status Query

```mermaid
sequenceDiagram
    autonumber
    actor Engine as Saga Engine
    participant Svc as Retry Policy Service
    participant Classifier as Error Classifier
    participant Target as Legacy Core Banking API

    Engine->>Svc: ExecuteStep(Step 3: Transfer Funds, Non-Idempotent)
    Svc->>Target: POST /transfers (TransferID: tx_888)
    Note over Target: Target processes debit, but gateway times out!
    Target-->>Svc: HTTP 504 Gateway Timeout
    
    Svc->>Classifier: Classify(504, Capability: Idempotent=False, StatusQuery=True)
    Classifier-->>Svc: AMBIGUOUS (Requires Status Query Before Blind Retry)
    
    Svc->>Target: GET /transfers/status?clientRef=tx_888
    Target-->>Svc: HTTP 200 OK ({"status": "COMMITTED", "amount": 1000.00})
    
    Note over Svc: Mutation verified as COMMITTED without issuing a blind duplicate transfer!
    Svc-->>Engine: STEP_SUCCEEDED (Result: Committed)
```

---

## 6. Sequence Diagram 3: Dependency Circuit Breaker & Budget Freezing

```mermaid
sequenceDiagram
    autonumber
    participant Svc as Retry Policy Service
    participant Breaker as Circuit Breaker (Payment Gateway)
    participant DB as PostgreSQL
    participant Target as External Payment Gateway

    Note over Breaker: 5 Consecutive Failures Detected -> State: OPEN
    
    Svc->>Breaker: CheckPermission("payment_gateway")
    Breaker-->>Svc: REJECTED_BREAKER_OPEN (Cooldown: 60s)
    
    Svc->>DB: FreezeStepRetryClock(StepID, Target="payment_gateway")
    Note over Svc,DB: Step attempt counter & elapsed time are FROZEN. Zero budget consumed!
    
    Note over Breaker: 60s Cooldown Expires -> State: HALF_OPEN
    Svc->>Breaker: ProbeRequest()
    Breaker-->>Svc: ALLOWED_PROBE
    Svc->>Target: Probe Health
    Target-->>Svc: HTTP 200 OK
    Breaker->>Breaker: State: CLOSED
    
    Svc->>DB: UnfreezeAndRampRelease(Target="payment_gateway")
    Svc->>Target: Dispatch Postponed Retries (Jitter Spaced)
```


# Dead-Letter Queue (DLQ) — Architectural & Visual Diagrams

This document illustrates the architectural blueprints, component relationships, and human disposition workflows for the **Transactional Dead-Letter Queue (DLQ)** subsystem, supporting [ADR-0008](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/dead-letter-queue/adr.md) and [Business Logic Decision Tree v2](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/dead-letter-queue/business-logic.md).

---

## 1. High-Level Architecture (HLD): Transactional DLQ Engine

```mermaid
flowchart TB
    subgraph EngineCore ["Saga Execution Core"]
        Worker["Orchestrator Step Worker"]
        Evaluator["Failure & Fingerprint Evaluator"]
        Worker --> Evaluator
    end

    subgraph RelationalAdmission ["PostgreSQL Atomic Admission (DQ-01)"]
        Evaluator -->|Atomic BEGIN ... COMMIT| DB_Saga[("saga_instances<br/>status: NEEDS_ATTENTION")]
        Evaluator -->|Atomic INSERT| DB_DLQ[("saga_dlq_records<br/>status: PENDING_REVIEW")]
        Evaluator -->|Atomic INSERT| DB_Outbox[("transactional_outbox<br/>event: DLQ_ENQUEUED")]
    end

    subgraph StreamingPlane ["Notification Streaming Plane (DQ-02)"]
        DB_Outbox --> Relay["Outbox Relay"]
        Relay --> KafkaDLQ["Kafka dlq-notifications Topic"]
        KafkaDLQ --> SlackPager["Ops Slack / PagerDuty Dispatcher"]
    end

    subgraph HumanGovernance ["Dual-Custody Human Governance Portal"]
        Op1["Operator 1 (Submitter)"] -->|POST /api/v1/dlq/{id}/submit-disposition| DLQService["DLQ Intervention Service"]
        Op2["Operator 2 (Approver)"] -->|POST /api/v1/dlq/{id}/approve| DLQService
        DLQService -->|Verify Check Constraint: op1 != op2| DB_DLQ
        DLQService -->|Trigger Replay with Original Key| Worker
    end
```

---

## 2. Low-Level Component Model (LLD)

```mermaid
classDiagram
    class DLQAdmissionPort {
        <<interface>>
        +AdmitToDLQ(ctx, tenantID, sagaID, stepID, err, payload) error
        +CheckSystemicOutage(ctx, fingerprint, window) (bool, error)
    }

    class DLQDispositonPort {
        <<interface>>
        +SubmitDisposition(ctx, recordID, submitter, proposedAction, reason) error
        +ApproveDisposition(ctx, recordID, approver) error
        +ReplayRecord(ctx, recordID) error
    }

    class DLQService {
        -DLQRepositoryPort repo
        -OutboxDispatcherPort outbox
        -FingerprintCalculator hasher
        +Admit(ctx, admissionReq) error
        +SubmitAction(ctx, actionReq) error
        +Approve(ctx, approveReq) error
    }

    class FingerprintCalculator {
        +Compute(err, stepType, schemaVersion) string
        +HashPayload(payload) string
    }

    DLQAdmissionPort <|.. DLQService
    DLQDispositonPort <|.. DLQService
    DLQService --> FingerprintCalculator : computes
```

---

## 3. Dual-Custody Disposition Sequence Flow ($> \$1,000$ Financial Action)

```mermaid
sequenceDiagram
    autonumber
    participant Op1 as Operator 1 (Submitter)
    participant API as DLQ Intervention API
    participant DB as PostgreSQL 18
    participant Op2 as Lead Architect (Approver)
    participant Engine as Saga Core Engine
    participant Partner as Downstream Gateway

    Note over DB: Record exists with money_at_stake = $4,500.00
    Op1->>API: POST /api/v1/dlq/rec-99/submit-disposition (action='FORCE_COMPLETE', reason='Offline bank wire verified')
    API->>DB: UPDATE saga_dlq_records SET status='AWAITING_SECOND_APPROVAL', submitted_by='alice@company.com' WHERE id='rec-99'
    DB-->>API: Status updated
    API-->>Op1: 202 Accepted (Pending second approver)

    rect rgb(255, 245, 230)
        Note over Op2, API: Dual-Custody Check (DQ-05 / chk_dual_custody)
        Op1->>API: POST /api/v1/dlq/rec-99/approve (Alice attempts self-approval)
        API->>DB: UPDATE ... approved_by='alice@company.com'
        DB-->>API: ERROR: Check constraint violation (submitted_by != approved_by)
        API-->>Op1: 403 Forbidden (Self-approval rejected)
    end

    Op2->>API: POST /api/v1/dlq/rec-99/approve (Bob approves)
    API->>DB: UPDATE saga_dlq_records SET status='FORCE_COMPLETED', approved_by='bob@company.com' WHERE id='rec-99'
    DB-->>API: 1 Row Affected (Approved!)

    API->>Engine: Re-activate saga (saga_id='tx-12', status='COMPLETED')
    Engine->>DB: UPDATE saga_instances SET status='COMPLETED'
```

---

## 4. Replay Execution Flow (Invariant DQ-04: Original Idempotency Key)

```mermaid
sequenceDiagram
    autonumber
    participant Operator as Operator
    participant API as DLQ API
    participant DB as PostgreSQL 18
    participant Engine as Saga Engine
    participant Partner as Downstream Bank API

    Operator->>API: POST /api/v1/dlq/rec-99/replay
    API->>DB: Query original idempotency key: idem:fwd:tenant1:tx-12:step-3:0
    API->>DB: UPDATE saga_dlq_records SET status='REPLAY_DISPATCHED'
    
    rect rgb(240, 248, 255)
        Note over API, Engine: Original Idempotency Key Reused
        API->>Engine: DispatchStep(idem_key='idem:fwd:tenant1:tx-12:step-3:0')
        Engine->>Partner: POST /v1/payout (Idempotency-Key: idem:fwd:tenant1:tx-12:step-3:0)
        Partner-->>Engine: HTTP 200 (Partner returns cached/idempotent result)
    end

    Engine->>DB: UPDATE saga_instances SET status='RUNNING' (Saga Resumed)
```

---

## 5. DLQ Record Lifecycle State Machine

```mermaid
stateDiagram-v2
    [*] --> PENDING_REVIEW: Admitted to DLQ (Atomic with saga NEEDS_ATTENTION)

    PENDING_REVIEW --> AWAITING_SECOND_APPROVAL: Monetary Action Submitted (> $1,000)
    PENDING_REVIEW --> REPLAY_DISPATCHED: Low-Risk Replay Approved (Original Idempotency Key)
    PENDING_REVIEW --> DISMISSED_IRRELEVANT: Verified No-Op / False Positive

    AWAITING_SECOND_APPROVAL --> FORCE_COMPLETED: Second Approver Authorizes (Dual Custody)
    AWAITING_SECOND_APPROVAL --> FORCE_ABORTED: Second Approver Rejects -> Transition to Compensate
    AWAITING_SECOND_APPROVAL --> PENDING_REVIEW: Approver Requests Clarification

    REPLAY_DISPATCHED --> [*]: Replay Succeeded -> Saga Completes
    FORCE_COMPLETED --> [*]: Saga Marked Completed with Residual Entry
    FORCE_ABORTED --> [*]: Saga Transitioned to FAILED_ABORTED
    DISMISSED_IRRELEVANT --> [*]
```


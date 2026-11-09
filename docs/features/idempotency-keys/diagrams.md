# Idempotency Keys — Architecture Diagrams

This document illustrates the High-Level Architecture, Low-Level Component Wiring, State Machine, and Sequence Diagrams for the Authoritative Idempotency Keys engine.

---

## 1. High-Level Design (HLD)

The Idempotency Keys engine acts as an authoritative, transaction-scoped gate for all state-changing API operations. It coordinates between the API Delivery Layer, the pure Domain Rules, PostgreSQL storage, and downstream execution engines (e.g. Saga Orchestrator).

```mermaid
graph TD
    Client["API Client / Partner / Mobile App"] -->|"1. HTTP Request + Idempotency-Key"| Gateway["API Gateway / Chassis Router"]
    Gateway -->|"2. Authenticate & Extract Scope"| ScopeExtract["Scope Extractor (tenant, principal, op)"]
    ScopeExtract -->|"3. Ingest Request"| Handler["Idempotency Handler"]
    
    subgraph Engine ["Idempotency Engine Boundary"]
        Handler -->|"4. Normalize & Fingerprint"| Hasher["Canonical Request Hasher (SHA-256)"]
        Handler -->|"5. Evaluate Intent"| Service["Idempotency Service"]
        Service -->|"6. Pure Decision Logic"| Rules["Domain Rules Engine"]
        Service -->|"7. Atomic Tx Coordination"| RepoPort["Repository Port"]
    end
    
    subgraph Storage ["Durable Persistence Tier (PostgreSQL)"]
        RepoPort -->|"8. Lock / Insert / Transition"| DB[("idempotency_keys Table")]
    end
    
    subgraph Downstream ["Execution Engine"]
        Service -->|"9. Execute Side Effect"| SagaEngine["Saga Orchestrator / Domain Service"]
    end
    
    Service -->|"10. Return Response Snapshot"| Gateway
    Gateway -->|"11. Replayed / Fresh Response"| Client
```

---

## 2. Low-Level Component Architecture (LLD)

Hexagonal architecture decoupling HTTP routing, pure domain rules, state machine transitions, and database persistence.

```mermaid
classDiagram
    class IdempotencyKeyHandler {
        +IngestOperation(w, r)
        -setStandardHeaders(w, r)
        -writeSuccess(w, r, status, data)
        -writeFailure(w, r, err)
    }

    class IdempotencyKeyService {
        -repo RepositoryPort
        -tracer Tracer
        -logger Logger
        +ExecuteIdempotent(ctx, req, executionFn) Result
        +LookupKey(ctx, scope, key) Record
    }

    class IdempotencyRules {
        +ComputeCanonicalFingerprint(op, params, body) string
        +ValidateScope(scope) error
        +EvaluateStateTransition(current, event) Result
        +DeriveDownstreamKey(rootKey, stepID) string
    }

    class RepositoryPort {
        <<interface>>
        +AcquireKey(ctx, scope, key, fingerprint, lease) Result
        +CommitSuccess(ctx, id, snapshot, resourceRef) error
        +CommitRejection(ctx, id, snapshot) error
        +ReleaseKey(ctx, id) error
        +FindExpiredLeases(ctx, limit) []Record
    }

    class PostgresIdempotencyRepositoryAdapter {
        -db DBExecutor
        +AcquireKey()
        +CommitSuccess()
        +CommitRejection()
    }

    class MemoryIdempotencyRepositoryAdapter {
        -mu sync.RWMutex
        -records map[string]Record
        +AcquireKey()
        +CommitSuccess()
    }

    IdempotencyKeyHandler --> IdempotencyKeyService
    IdempotencyKeyService --> IdempotencyRules
    IdempotencyKeyService --> RepositoryPort
    RepositoryPort <|.. PostgresIdempotencyRepositoryAdapter
    RepositoryPort <|.. MemoryIdempotencyRepositoryAdapter
```

---

## 3. Finite State Machine (FSM)

Authoritative lifecycle states of an idempotency key record.

```mermaid
stateDiagram-v2
    [*] --> IN_PROGRESS: Insert in Tx 1 / Single-Tx
    
    IN_PROGRESS --> COMPLETED: 2xx Success / 202 Saga Started
    IN_PROGRESS --> REJECTED_FINAL: 4xx Business Rejection (e.g. Insufficient Funds)
    IN_PROGRESS --> [*]: Pre-write Validation / 429 Rate Limit / Tx Rollback (Clean Release)
    IN_PROGRESS --> UNKNOWN: Lease Expired / Ambiguous Timeout (Crash)
    
    COMPLETED --> COMPLETED: Replay Original Response (Idempotent-Replayed: true)
    REJECTED_FINAL --> REJECTED_FINAL: Replay Original Rejection (Idempotent-Replayed: true)
    
    UNKNOWN --> COMPLETED: External Lookup Confirms Effect Committed
    UNKNOWN --> IN_PROGRESS: External Lookup Confirms Effect Absent (New Lease)
    UNKNOWN --> [*]: Manual / Escalation Resolution
```

---

## 4. Sequence Diagram 1: Fresh Request Execution (Single-Tx Model)

Client sends an operation with an idempotency key for the first time.

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant API as API Handler
    participant Svc as Idempotency Service
    participant Rules as Domain Rules
    participant DB as PostgreSQL
    participant Domain as Domain Business Logic

    Client->>API: POST /api/v1/payments (Idempotency-Key: ik_1001)
    API->>Svc: ExecuteIdempotent(Scope, Key, Payload)
    Svc->>Rules: ComputeCanonicalFingerprint(Operation, Payload)
    Rules-->>Svc: sha256_hash_abc
    
    Svc->>DB: BEGIN TRANSACTION
    Svc->>DB: INSERT INTO idempotency_keys (scope, key, fingerprint, state='IN_PROGRESS')
    DB-->>Svc: Insert OK (Row Lock Acquired)
    
    Svc->>Domain: ExecutePaymentOperation()
    Domain-->>Svc: PaymentSuccess(resourceId="pay_999", amount=250.00)
    
    Svc->>DB: UPDATE idempotency_keys SET state='COMPLETED', response_snapshot={status:201, body:...}
    Svc->>DB: COMMIT TRANSACTION
    
    Svc-->>API: Response(HTTP 201, Snapshot)
    API-->>Client: HTTP 201 Created (Payment Confirmation)
```

---

## 5. Sequence Diagram 2: Concurrent In-Flight Collision (Case B)

Two identical client requests arrive simultaneously before the first completes.

```mermaid
sequenceDiagram
    autonumber
    actor ClientA as Client (Req 1)
    actor ClientB as Client (Req 2 - Retry)
    participant API as API Handler
    participant Svc as Idempotency Service
    participant DB as PostgreSQL

    ClientA->>API: POST /api/v1/payments (Key: ik_1001)
    API->>Svc: Ingest(ik_1001)
    Svc->>DB: BEGIN Tx 1
    Svc->>DB: INSERT INTO idempotency_keys (state='IN_PROGRESS')
    DB-->>Svc: Row Locked by Tx 1
    
    ClientB->>API: POST /api/v1/payments (Key: ik_1001)
    API->>Svc: Ingest(ik_1001)
    Svc->>DB: INSERT INTO idempotency_keys (state='IN_PROGRESS')
    Note over DB,Svc: Blocked waiting on Tx 1 row lock OR Unique Violation
    
    alt Short In-Progress Mode (Non-blocking)
        DB-->>Svc: Unique Key Violation (State = IN_PROGRESS, Lease Valid)
        Svc-->>API: Conflict Error
        API-->>ClientB: HTTP 409 Conflict (Retry-After: 2)
    else Bounded Wait Mode
        Note over Svc,DB: Waits up to 2 seconds for Tx 1 to COMMIT
        Svc->>DB: Tx 1 COMMITS (State = COMPLETED)
        DB-->>Svc: Reads COMPLETED record
        Svc-->>API: Stored Snapshot
        API-->>ClientB: HTTP 201 Created (Idempotent-Replayed: true)
    end
```

---

## 6. Sequence Diagram 3: Duplicate Replay with Re-Authorization (Case C)

Client retries an operation after network drop; response is re-authorized and replayed.

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant API as API Handler
    participant Svc as Idempotency Service
    participant Rules as Domain Rules
    participant DB as PostgreSQL

    Client->>API: POST /api/v1/payments (Key: ik_1001, Token: UserAuth)
    API->>Svc: ExecuteIdempotent(Scope, Key, Payload)
    Svc->>Rules: ComputeCanonicalFingerprint(Operation, Payload)
    Rules-->>Svc: sha256_hash_abc
    
    Svc->>DB: SELECT * FROM idempotency_keys WHERE scope=:scope AND key='ik_1001'
    DB-->>Svc: Record(State='COMPLETED', Fingerprint='sha256_hash_abc', Snapshot={status:201, body:...})
    
    Svc->>Rules: VerifyFingerprintMatch('sha256_hash_abc', 'sha256_hash_abc')
    Rules-->>Svc: MATCH
    
    Svc->>API: Re-authorize Caller (Verify active token permissions)
    API-->>Svc: Authorized
    
    Svc-->>API: ReplaySnapshot(Status: 201, Body: ..., Replayed: true)
    API-->>Client: HTTP 201 Created<br/>Idempotent-Replayed: true<br/>{"orderId": "pay_999", "status": "COMPLETED"}
```

---

## 7. Sequence Diagram 4: Fingerprint Tamper Detection (HTTP 422 Unprocessable)

Client reuses an existing idempotency key with modified financial details.

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant API as API Handler
    participant Svc as Idempotency Service
    participant Rules as Domain Rules
    participant DB as PostgreSQL

    Client->>API: POST /api/v1/payments (Key: ik_1001, Amount: 5000.00)
    API->>Svc: ExecuteIdempotent(Scope, Key, Amount: 5000)
    Svc->>Rules: ComputeCanonicalFingerprint(Payload[5000])
    Rules-->>Svc: sha256_hash_MODIFIED
    
    Svc->>DB: SELECT * FROM idempotency_keys WHERE scope=:scope AND key='ik_1001'
    DB-->>Svc: Record(State='COMPLETED', Fingerprint='sha256_hash_ORIGINAL_250')
    
    Svc->>Rules: VerifyFingerprintMatch('sha256_hash_ORIGINAL_250', 'sha256_hash_MODIFIED')
    Rules-->>Svc: MISMATCH (Payload Tampered)
    
    Svc-->>API: IntegrityConflictError(422, "idempotency key reused with different payload")
    API-->>Client: HTTP 422 Unprocessable Entity<br/>{"error": {"code": "FINGERPRINT_MISMATCH"}}
```


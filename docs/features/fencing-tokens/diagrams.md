# Fencing Tokens — Architecture & System Diagrams

## 1. High-Level Architecture (HLD)

```mermaid
graph TD
    subgraph "Execution Nodes"
        W1["Worker Node 1 (Active Holder)<br/>Holds Token: 42"]
        W2["Worker Node 2 (Takeover Worker)<br/>Acquires Token: 43"]
    end

    subgraph "Authoritative ACID Store (PostgreSQL)"
        FL["fencing_leases<br/>(resource_id, holder_id, epoch, expires_at)"]
        SI["saga_instances<br/>(id, state, fencing_token)"]
        OB["outbox_events<br/>(id, payload, fencing_token)"]
        FA["fencing_audits<br/>(rejected_holder, attempted_token, current_token)"]
    end

    subgraph "Downstream Boundary"
        RELAY["Outbox Publisher Daemon"]
        EXT["Downstream APIs / Payment Gateways"]
    end

    W1 -- "1. Statement 1: Fenced Check (T=42)" --> SI
    W2 -- "Lease Expired: Atomic CAS Takeover (T=43)" --> FL
    W1 -. "Zombie Write (T=42 < 43: REJECT 0 Rows)" .-> SI
    SI -- "Audit Violation" --> FA
    W2 -- "Committed Writes (T=43)" --> OB
    OB --> RELAY --> EXT
```

---

## 2. Low-Level Architecture (LLD)

```mermaid
classDiagram
    class FencingTokensService {
        -RepositoryPort repo
        -FencingRules rules
        -FencingMachine machine
        -Tracer tracer
        +AcquireLease(ctx, input) (FencingLease, error)
        +RenewLease(ctx, input) (FencingLease, error)
        +ReleaseLease(ctx, input) error
        +ValidateAndFence(ctx, input) (FencingDecision, error)
        +RecordViolation(ctx, violation) error
    }

    class FencingMachine {
        +Transition(current, event) (LeaseState, error)
        +CanRenew(state) bool
        +CanRelease(state) bool
    }

    class FencingRules {
        +EvaluateFencingBarrier(storedToken, incomingToken) FencingDecision
        +ComputeLeaseExpiry(now, durationMs) time.Time
        +ClassifyRejection(storedToken, incomingToken, rowsAffected) ViolationType
    }

    class FencingRepositoryPort {
        <<interface>>
        +AcquireOrTakeover(ctx, lease) (FencingLease, error)
        +Renew(ctx, resourceID, holderID, epoch, durationMs) (FencingLease, error)
        +Release(ctx, resourceID, holderID, epoch) error
        +ExecuteFencedBarrier(ctx, tenantID, resourceID, token) (int64, int64, error)
        +RecordAudit(ctx, audit) error
    }

    class FencingPostgresAdapter {
        -DBExecutor db
        +AcquireOrTakeover(...)
        +Renew(...)
        +Release(...)
        +ExecuteFencedBarrier(...)
        +RecordAudit(...)
    }

    class FencingMemoryAdapter {
        -sync.RWMutex mu
        -map[string]FencingLease leases
        -map[string]int64 storedTokens
        -[]FencingAudit audits
    }

    FencingTokensService --> FencingRepositoryPort
    FencingTokensService --> FencingRules
    FencingTokensService --> FencingMachine
    FencingRepositoryPort <|.. FencingPostgresAdapter
    FencingRepositoryPort <|.. FencingMemoryAdapter
```

---

## 3. Lease & Fencing Token State Machine (FSM)

```mermaid
stateDiagram-v2
    [*] --> UNALLOCATED
    UNALLOCATED --> ACQUIRED: Acquire (Epoch = 1)
    
    ACQUIRED --> RENEWED: Heartbeat Renew (Epoch = T)
    RENEWED --> RENEWED: Heartbeat Renew (Epoch = T)
    
    RENEWED --> RELEASED: Graceful Release (Epoch = T, released_at = NOW)
    ACQUIRED --> RELEASED: Graceful Release (Epoch = T, released_at = NOW)
    
    RENEWED --> EXPIRED: Heartbeat Missed (NOW > expires_at)
    ACQUIRED --> EXPIRED: Heartbeat Missed (NOW > expires_at)
    
    EXPIRED --> ACQUIRED: Takeover by New Worker (Epoch = T + 1)
    RELEASED --> ACQUIRED: Re-acquired (Epoch = T + 1)
    
    EXPIRED --> ZOMBIE_REJECTED: Stale Worker Awakes & Attempts Write (T < T_latest)
    ZOMBIE_REJECTED --> [*]
```

---

## 4. Sequence Diagram 1: Atomic Acquisition and Token Issuance

```mermaid
sequenceDiagram
    autonumber
    participant W as Worker Node
    participant SVC as FencingTokensService
    participant PG as PostgreSQL (Authoritative Store)

    W->>SVC: AcquireLease(Tenant, ResourceID, WorkerID, Duration=15s)
    SVC->>PG: UPDATE fencing_leases SET holder_id=:worker_id, epoch=epoch+1, expires_at=NOW()+interval WHERE resource_id=:res AND (expires_at < NOW() OR released_at IS NOT NULL) RETURNING epoch, public_id
    alt Lease Available / Expired
        PG-->>SVC: Return epoch = 42, public_id = "fnl_..."
        SVC-->>W: Lease Granted (Token: 42, Expires: NOW() + 15s)
    else Lease Still Active By Another Node
        PG-->>SVC: 0 Rows Affected
        SVC-->>W: Error (ERR_LEASE_BUSY, Current Holder Active)
    end
```

---

## 5. Sequence Diagram 2: Fenced Protected Mutation Protocol

```mermaid
sequenceDiagram
    autonumber
    participant W as Active Worker (Token: 42)
    participant TX as DB Transaction
    participant SI as saga_instances Table
    participant ST as saga_step_executions Table
    participant OB as outbox_events Table

    W->>TX: BEGIN Transaction
    Note over W,TX: FT-04: Statement 1 is ALWAYS the Fencing Barrier
    W->>TX: UPDATE saga_instances SET fencing_token=42 WHERE id=:saga_id AND fencing_token <= 42 RETURNING fencing_token
    TX-->>W: RowsAffected=1, StoredToken=42 (SUCCESS: Gate Passed)
    
    Note over W,TX: Subsequent mutations depend on Statement 1
    W->>TX: UPDATE saga_instances SET state='RUNNING', current_step='step_payment'
    W->>TX: INSERT INTO saga_step_executions (step_id, status) VALUES ('step_payment', 'COMPLETED')
    W->>TX: INSERT INTO outbox_events (topic, payload, fencing_token) VALUES ('payment.command', '...', 42)
    W->>TX: COMMIT Transaction
    TX-->>W: 200 OK (State, Step, and Outbox Safely Committed)
```

---

## 6. Sequence Diagram 3: Zombie Worker Interception & Quarantine

```mermaid
sequenceDiagram
    autonumber
    participant W1 as Worker 1 (Old Holder, Token: 42)
    participant W2 as Worker 2 (New Holder, Token: 43)
    participant PG as PostgreSQL
    participant AUD as fencing_audits Table

    Note over W1: GC Pause for 20s... Lease Expires
    W2->>PG: AcquireLease (Atomic Epoch Increment)
    PG-->>W2: Lease Granted (Token: 43)
    W2->>PG: Execute Fenced Mutation (Sets fencing_token = 43)
    PG-->>W2: Committed

    Note over W1: Worker 1 Wakes Up! Still believes it holds Token 42
    W1->>PG: BEGIN Transaction
    W1->>PG: Statement 1: UPDATE saga_instances SET fencing_token=42 WHERE id=:saga_id AND fencing_token <= 42
    Note over PG: Check fails: stored fencing_token is 43 (43 <= 42 is FALSE)
    PG-->>W1: RowsAffected = 0 (Query: SELECT fencing_token -> 43)
    
    Note over W1: FT-08: Stale Holder Detected!
    W1->>PG: ROLLBACK Transaction
    W1->>AUD: INSERT INTO fencing_audits (attempted=42, current=43, rejected_holder='worker-1', type='STALE_TOKEN')
    Note over W1: Worker 1 halts execution on this saga immediately
```


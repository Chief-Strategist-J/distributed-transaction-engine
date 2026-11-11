# Optimistic Concurrency Control — Architecture Diagrams & Workflows

## 1. Dual-Token CAS Transition Sequence

```mermaid
sequenceDiagram
    autonumber
    participant W1 as Worker 1 (Epoch 10)
    participant W2 as Worker 2 (Stale Epoch 9)
    participant DB as PostgreSQL 18
    participant Inbox as Inbox Store

    W1->>DB: SELECT status, version, epoch FROM saga_instances WHERE id = 'tx-1'
    DB-->>W1: status='RUNNING', version=3, epoch=10

    Note over W1: Computes state transition to 'COMPLETED'
    
    W2->>DB: UPDATE saga_instances SET status='FAILED', version=version+1 WHERE id='tx-1' AND version=3 AND epoch=9
    DB-->>W2: 0 Rows Affected (Epoch mismatch - Rejected)
    Note over W2: Stale lease holder stopped by epoch fence!

    W1->>DB: BEGIN TRANSACTION
    W1->>DB: INSERT INTO inbox_messages (msg_id) VALUES ('msg-45')
    W1->>DB: UPDATE saga_instances SET status='COMPLETED', version=4 WHERE id='tx-1' AND version=3 AND epoch=10
    DB-->>W1: 1 Row Affected (CAS Succeeded)
    W1->>DB: INSERT INTO outbox_events (event_id, type) VALUES ('evt-99', 'SAGA_COMPLETED')
    W1->>DB: COMMIT
```

---

## 2. OCC Conflict Retry & Inbox Re-Check Loop

```mermaid
flowchart TD
    A["Receive Input Event / Reply"] --> B["Read Current State from DB (version V0, epoch E0)"]
    B --> C["Check Inbox: Has msg_id already been applied?"]
    C -- "YES" --> D["Acknowledge & Discard (Idempotent No-Op)"]
    C -- "NO" --> E["Evaluate Pure Transition Function"]
    E --> F["Attempt CAS: UPDATE ... WHERE version = V0 AND epoch = E0"]
    F --> G{"Rows Affected == 1?"}
    G -- "YES" --> H["Commit Outbox Events & Ack Message"]
    G -- "NO" --> I["Diagnose Failure Reason"]
    I --> J{"Is Epoch Mismatched?"}
    J -- "YES" --> K["Yield Lease: Worker no longer owns saga"]
    J -- "NO" --> L{"Retry Count < Max?"}
    L -- "YES" --> M["Exponential Backoff with Full Jitter"]
    M --> B
    L -- "NO" --> N["Leave Message Unacknowledged / Re-queue with Delay (OC-06)"]
```

---

## 3. Contention Strategy Boundary (OCC vs Partition Serialization)

```mermaid
graph TD
    subgraph Low Contention
        A[Sequential Sagas / Low Branch Count] --> B[OCC with Dual-Token CAS]
        B --> C[Optimistic parallel processing]
    end

    subgraph High Contention
        D[Fan-out > 50 Parallel Branches] --> E[Kafka Partitioned Worker Affinity]
        E --> F[Single Worker Processes All Step Replies for Saga Key]
        F --> G[Eliminates CAS Thrashing]
    end
```


# Transactional Outbox — Business Logic Decision Tree

```
TRANSACTIONAL OUTBOX (BUSINESS DECISION TREE)
│
├── 1. ATOMIC LOCAL WRITE TIER
│   └── Trigger: Saga Orchestrator Mutates State (e.g. Saga Created, Step Succeeded, Saga Compensated)
│       ├── Action: Open Single ACID PostgreSQL Transaction
│       │   ├── Action A: Mutate Domain Table (e.g. UPDATE saga_instance SET state = 'COMPLETED')
│       │   └── Action B: Insert Outbox Event Record:
│       │       │   INSERT INTO outbox_events (
│       │       │       id, tenant_id, aggregate_type, aggregate_id,
│       │       │       event_type, payload, status, created_at
│       │       │   ) VALUES (
│       │       │       uuid_generate_v4(), :tenant_id, 'SAGA', :saga_id,
│       │       │       'SAGA_COMPLETED', :eventPayload, 'PENDING', NOW()
│       │       │   );
│       │
│       └── Action: Commit PostgreSQL Transaction
│           ├── Commit Fails ──> Entire state mutation and outbox record roll back together
│           │   └── State: WRITE_FAILED ──> [Zero Discrepancy, Client Receives Error]
│           └── Commit Succeeds
│               └── State: OUTBOX_COMMITTED ──> [Event Atomically Stored in DB]
│
├── 2. EVENT RELAY & POLLING TIER
│   └── Trigger: Asynchronous Outbox Relay Worker (CDC Debezium OR Polling Relayer)
│       ├── Action: Query Unsent Outbox Events with Batching & Concurrency Lock:
│       │   └── SQL: SELECT id, aggregate_id, event_type, payload FROM outbox_events
│       │            WHERE status = 'PENDING'
│       │            ORDER BY created_at ASC LIMIT 100
│       │            FOR UPDATE SKIP LOCKED;
│       │
│       ├── Check: Are there pending outbox records?
│       │   ├── NO ──> Worker sleeps for Polling Interval (e.g. 50ms) ──> Loop
│       │   └── YES ──> State: BATCH_CLAIMED
│
└── 3. KAFKA DISPATCH & CONFIRMATION TIER
    └── State: BATCH_CLAIMED
        └── Loop: For each claimed outbox record:
            ├── Action: Construct Kafka Producer Record:
            │   ├── Topic: `dist-tx.events.v1`
            │   ├── Partition Key: aggregate_id (Preserves strict ordering per Saga)
            │   └── Headers: TenantID, EventType, TraceParent Context
            │
            └── Action: Produce Message to Kafka Cluster with `acks=all`:
                ├── Result: KAFKA ACKNOWLEDGED (Message written to min.insync.replicas)
                │   ├── Action: UPDATE outbox_events SET status='PUBLISHED', published_at=NOW() WHERE id=:id;
                │   └── State: PUBLISHED ──> [TERMINAL: Event Guaranteed in Stream]
                │
                └── Result: KAFKA ERROR / TIMEOUT (Broker Unavailable)
                    ├── Action: Do NOT mark event as published (Leave as 'PENDING')
                    ├── Action: Release Lock and Apply Exponential Backoff
                    └── State: RELAY_RETRY ──> [Will be Re-polled by Next Relay Loop]
```

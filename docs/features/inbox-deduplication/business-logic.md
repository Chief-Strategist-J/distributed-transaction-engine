# Inbox Deduplication — Business Logic Decision Tree

```
INBOX DEDUPLICATION (BUSINESS DECISION TREE)
│
├── 1. MESSAGE ARRIVAL & SIGNATURE COMPUTATION TIER
│   └── Trigger: Kafka Consumer Worker Fetches Message Record from Topic
│       ├── Check: Does Message Contain Unique Message ID / Outbox Event ID in Headers?
│       │   ├── NO ──> Action: Synthesize Deterministic Key:
│       │   │   └── MessageKey = SHA256(KafkaTopic || Partition || Offset)
│       │   └── YES ──> Set MessageKey = InboundHeader.MessageID
│       │
│       └── State: MESSAGE_IDENTIFIED
│
├── 2. TWO-TIER DEDUPLICATION CHECK
│   └── State: MESSAGE_IDENTIFIED
│       ├── Tier 2A: High-Throughput Redis Bloom Filter / Fast-Path Key
│       │   ├── Check: Redis.EXISTS("inbox:" + MessageKey) == 1?
│       │   │   ├── YES (Likely Duplicate) ──> Fall through to Tier 2B for DB Ground-Truth Check
│       │   │   └── NO (Guaranteed Fresh or Cache Miss) ──> Proceed to Tier 2B
│       │
│       └── Tier 2B: PostgreSQL ACID Unique Constraint Boundary
│           └── Action: Open Local DB Transaction
│               ├── SQL: INSERT INTO inbox_events (message_id, tenant_id, status, received_at)
│               │        VALUES (:MessageKey, :TenantID, 'PROCESSING', NOW())
│               │        ON CONFLICT (message_id) DO NOTHING;
│               │
│               ├── Check: Did the INSERT statement return 1 row?
│               │   ├── NO (RowsAffected == 0 — Ground-Truth Duplicate Detected!)
│               │   │   ├── Action: Rollback Local DB Transaction
│               │   │   ├── Action: Commit Kafka Offset (Acknowledge Message to Prevent Re-delivery Loop)
│               │   │   └── State: DUPLICATE_SKIPPED ──> [TERMINAL: Zero Side-Effects Applied]
│               │   │
│               │   └── YES (RowsAffected == 1 — First-Time Arrival Confirmed)
│               │       └── State: INBOX_CLAIMED
│
└── 3. MESSAGE PROCESSING & STATE FLUSH TIER
    └── State: INBOX_CLAIMED
        ├── Action: Invoke Registered Business Domain Handler within DB Transaction
        │   ├── Handler Outcome: SUCCESS
        │   │   ├── Action: UPDATE inbox_events SET status='PROCESSED', processed_at=NOW()
        │   │   ├── Action: Commit DB Transaction
        │   │   ├── Action: Set Redis Fast-Path Cache ("inbox:" + MessageKey, TTL=7 Days)
        │   │   └── Action: Commit Kafka Consumer Offset
        │   │       └── State: COMPLETED ──> [TERMINAL: Exactly-Once Side-Effect Guaranteed]
        │   │
        │   └── Handler Outcome: FAILURE / UNHANDLED PANIC
        │       ├── Action: Rollback DB Transaction (Deletes or Resets inbox_events row)
        │       ├── Action: Do NOT Commit Kafka Offset (Allow Re-poll or DLQ Routing)
        │       └── State: HANDLER_FAILED ──> [Retry Scheduled via Consumer Group Backoff]
```

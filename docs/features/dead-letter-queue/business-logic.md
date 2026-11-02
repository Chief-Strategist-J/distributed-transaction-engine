# Dead-Letter Queue — Business Logic Decision Tree

```
DEAD-LETTER QUEUE (BUSINESS DECISION TREE)
│
├── 1. ADMISSION & QUARANTINE ROUTING TIER
│   └── Trigger: Message/Saga Step Fails Permanently (Max Retries Breached / Unrecoverable Error)
│       ├── Action: Intercept Payload, Stack Trace, Headers, and Correlation ID
│       ├── Check: Is Failure Classified as POISON_PILL (Schema Corruption, Malformed Byte Stream)?
│       │   ├── YES ──> Action: Tag metadata with priority='CRITICAL_POISON_PILL'
│       │   └── NO ──> Action: Tag metadata with priority='RETRY_EXHAUSTED'
│       │
│       └── Action: Atomic Dual-Write to DLQ Storage:
│           ├── Store 1: Append to PostgreSQL `dead_letter_records` table
│           └── Store 2: Produce to Kafka DLQ Topic (`dist-tx.saga.dead-letter.v1`)
│               ├── Write Result: SUCCESS
│               │   ├── Action: Emit Metric `tx_dlq_messages_total{reason=...}`
│               │   └── Action: Fire Prometheus Alert Rule `DeadLetterQueueNotEmpty`
│               │       └── State: QUARANTINED
│               └── Write Result: FAILURE
│                   └── Action: Write to Local Disk Emergency Spillway & Panic Pod (Preserve Zero Loss)
│
├── 2. OPERATIONAL TRIAGE & INSPECTION TIER
│   └── State: QUARANTINED
│       └── Trigger: SRE / Platform Engineer Queries DLQ via Management Dashboard / API
│           ├── Check: Caller has 'dlq:admin' Permission?
│           │   ├── NO ──> [REJECT: HTTP 403 Forbidden]
│           │   └── YES ──> Action: Return Paginated Records with Decoded Payloads and Failure Reason
│           │       └── Operator Decision Point:
│           │           ├── Option A: Transient Upstream Fixed ──> Select: REPLAY (Re-drive)
│           │           ├── Option B: Corrupted Payload Needs Patch ──> Select: EDIT_AND_REPLAY
│           │           └── Option C: Unrecoverable Bad Business Data ──> Select: DISMISS (Purge/Archive)
│
└── 3. RE-DRIVE EXECUTION & RESOLUTION TIER
    ├── Branch: REPLAY / EDIT_AND_REPLAY
    │   ├── Action: If EDIT, Validate Modified Payload Schema against Workflow Definition
    │   ├── Action: Re-inject Record into Ingress Topic / Saga Inbox with New Retry Budget
    │   └── Action: Update `dead_letter_records` SET status='REDRIVEN', resolved_by=OperatorID
    │       └── State: RESOLVED ──> [TERMINAL: Removed from DLQ, Resumed in Engine]
    │
    └── Branch: DISMISS (Purge/Archive)
        ├── Action: Mandatory Entry of Operator Reason & Ticket Reference (Audit Compliance)
        └── Action: Update `dead_letter_records` SET status='DISMISSED', resolved_by=OperatorID
            └── State: ARCHIVED ──> [TERMINAL: Written to Cold Audit Archive, Removed from Active Alerting]
```

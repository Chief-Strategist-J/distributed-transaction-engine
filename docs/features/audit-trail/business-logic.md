# Audit Trail — Business Logic Decision Tree

```
AUDIT TRAIL (BUSINESS DECISION TREE)
│
├── 1. EVENT INGESTION & INTERCEPTION TIER
│   └── Trigger: Internal State Mutation / External Operator Action Emitted
│       ├── Check: Is Event Payload Structured & Valid against W3C/CloudEvents Schema?
│       │   ├── NO ──> [ABORT: Reject to Dead-Letter Audit Ingestion with SchemaValidationError]
│       │   └── YES ──> Check: Is Actor Identity & Tenant Context Present?
│       │       ├── NO ──> Action: Assign System Identity (Actor: "SYSTEM_KERNEL", Tenant: "GLOBAL_SYSTEM")
│       │       └── YES ──> Action: Extract Actor Context (Principal ID, IP Address, Tenant ID, Trace ID)
│       │
│       └── Action: Generate Monotonic Sequence Identifier (Epoch Millis + Monotonic Sequence Counter)
│           └── State: EVENT_BUFFERED
│
├── 2. CRYPTOGRAPHIC INTEGRITY & IMMUTABILITY TIER
│   └── State: EVENT_BUFFERED
│       ├── Action: Fetch Previous Event's Cryptographic Hash for Tenant Partition (H_prev)
│       │   ├── Check: Is Tenant Ledger Chain Initial (Genesis Block)?
│       │   │   ├── YES ──> Set H_prev = SHA256("GENESIS_BLOCK_SEED_" + TenantID)
│       │   │   └── NO ──> Set H_prev = Last Committed Hash from immutable_audit_log table
│       │   │
│       │   └── Action: Compute Current Node Hash:
│       │       └── H_curr = SHA256(H_prev || SequenceID || Timestamp || TenantID || ActorID || Action || Payload)
│       │
│       └── Action: Atomic Append to PostgreSQL Write-Optimized Partition
│           ├── Transaction: INSERT INTO immutable_audit_log (sequence_id, prev_hash, curr_hash, payload)
│           ├── Result: SUCCESS
│           │   └── Action: Asynchronously forward event to Long-term Cold Storage (WORM / S3 Object Lock)
│           │       └── State: COMMITTED ──> [TERMINAL: Event Cryptographically Anchored]
│           └── Result: FAILURE (e.g. Unique Constraint or Concurrent Insertion Conflict)
│               └── Action: Re-read tail hash, recalculate H_curr, and retry with Exponential Backoff
│                   └── State: RETRYING
│
└── 3. COMPLIANCE VERIFICATION & AUDIT QUERY TIER
    └── Trigger: Compliance Officer / Auditor Requests Chain Verification
        ├── Check: Does Auditor Possess 'audit:read' RBAC Permission?
        │   ├── NO ──> [REJECT: HTTP 403 Forbidden]
        │   └── YES ──> Action: Stream Audit Events Ordered by sequence_id ASC
        │       └── Loop: Validate Hash Chain Linkage
        │           ├── Condition: Computed SHA256 == Stored curr_hash AND Stored prev_hash == Prior curr_hash
        │           │   └── Status: VALID ──> Continue to Next Node
        │           └── Condition: Hash Mismatch Detected!
        │               └── Action: Trip TAMPER_EVIDENT Security Alarm
        │                   └── State: COMPROMISED ──> [TERMINAL: Lock Tenant Audit Ledger & Page Security Lead]
```

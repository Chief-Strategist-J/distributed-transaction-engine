# Transaction State Machine — Business Logic Decision Tree

```
TRANSACTION STATE MACHINE (BUSINESS DECISION TREE)
│
├── 1. REGISTRATION & INITIALIZATION TIER
│   └── Trigger: Client Submits New Transaction/Saga Intent
│       ├── Check: Is Idempotency Key Present in Request Header?
│       │   ├── NO ──> [REJECT: HTTP 400 Bad Request — Missing Idempotency-Key]
│       │   └── YES ──> Check: Does Active Key Exist in Redis / Ledger DB?
│       │       ├── YES (Key in-flight) ──> [ABORT: HTTP 409 Conflict — Transaction Already Processing]
│       │       ├── YES (Key completed) ──> [TERMINAL: Return Cached Previous Response via HTTP 200]
│       │       └── NO (New Unique Transaction)
│       │           └── Action: Write Saga Intent Record to PostgreSQL (version=1)
│       │               └── State: CREATED
│       │                   ├── Event: Client issues Cancel before Step 1 starts
│       │                   │   └── Action: Transition to CANCELLED
│       │                   │       └── State: CANCELLED ──> [TERMINAL: Zero Side-Effects Executed]
│       │                   └── Event: Orchestrator acquires execution lease
│       │                       └── Action: Transition to RUNNING
│       │
├── 2. FORWARD DAG EXECUTION TIER
│   └── State: RUNNING (Executing Forward Steps in Topological Order)
│       ├── Step Type: Synchronous Local/Remote Service (e.g. Reserve Inventory)
│       │   ├── Execution: Invoke Microservice via gRPC/HTTP with Timeout
│       │   │   ├── Result: SUCCESS (HTTP 200 / gRPC OK)
│       │   │   │   ├── Action: Atomic UPDATE saga_step SET status='SUCCEEDED'
│       │   │   │   └── Check: Are there more pending steps in workflow DAG?
│       │   │   │       ├── YES ──> Action: Dispatch Next Step ──> [Loop in State: RUNNING]
│       │   │   │       └── NO ──> Action: Execute Pivot Transaction Commit
│       │   │   │           └── State: COMPLETED ──> [TERMINAL: Ledger Finalized, Outbox Event Emitted]
│       │   │   │
│       │   │   └── Result: NON-RETRYABLE FAILURE (e.g. Insufficient Balance, HTTP 422)
│       │   │       └── Action: Halt forward execution & Transition to Rollback
│       │   │           └── State: COMPENSATING (Transition to Tier 4)
│       │   │
│       │   └── Result: TRANSIENT NETWORK FAILURE (e.g. TCP Timeout, HTTP 503)
│       │       ├── Check: Has step retry budget been exhausted?
│       │       │   ├── NO ──> Action: Exponential Backoff with Full Jitter ──> [Retry in State: RUNNING]
│       │       │   └── YES ──> Action: Mark step failed & Transition to Rollback
│       │       │       └── State: COMPENSATING (Transition to Tier 4)
│       │
│       └── Step Type: Asynchronous External Callback (e.g. 3D-Secure Bank SMS / Manual Approval)
│           └── Action: Suspend execution worker thread, release CPU, and register timer
│               └── State: WAITING (Awaiting Webhook or Async Event)
│                   ├── Event: Valid Webhook Received with Signature
│                   │   ├── Check: Payload status == 'APPROVED'
│                   │   │   └── Action: Resume worker thread ──> State: RUNNING
│                   │   └── Check: Payload status == 'REJECTED'
│                   │       └── Action: Transition to COMPENSATING
│                   └── Event: Timer Expiration (SLA Timeout Exceeded, e.g. 30 minutes)
│                       └── Action: Cancel waiting token & Transition to COMPENSATING
│
└── 3. COMPENSATION & RECOVERY TIER
    └── State: COMPENSATING (Executing Compensating Actions in Strict Reverse Order)
        ├── Action: Fetch all SUCCEEDED steps from database in reverse execution sequence
        │   ├── Loop: For each step, call registered Compensating Action (Semantic Undo)
        │   │   ├── Result: SUCCESS (e.g. Release Reserved Funds)
        │   │   │   └── Action: Mark step compensated, advance to next previous step
        │   │   └── Result: FAILURE (e.g. Third-Party Refund Endpoint Returns 500)
        │   │       ├── Check: Retry count < Max Allowed (default: 5 attempts)
        │   │       │   └── Action: Retry with jittered backoff ──> [Loop in State: COMPENSATING]
        │   │       └── Check: Retry count >= Max Allowed
        │   │           └── Action: Trip Circuit Breaker, Alert On-Call SRE, Quarantine Saga
        │   │               └── State: STUCK (Awaiting Manual Human Intervention)
        │   │                   ├── Event: Operator fixes downstream dependency & triggers Re-drive
        │   │                   │   └── Action: Resume compensation loop ──> State: COMPENSATING
        │   │                   └── Event: Operator declares permanent financial write-off / poison pill
        │   │                       └── Action: Write reconciliation audit log & abort
        │   │                           └── State: FAILED ──> [TERMINAL: Escalate to Accounting]
        │   │
        │   └── Check: Have all compensable steps been successfully undone?
        │       └── YES ──> Action: Mark saga as completely cleaned up
        │           └── State: COMPENSATED ──> [TERMINAL: Clean Rollback, Outbox Event Emitted]
```

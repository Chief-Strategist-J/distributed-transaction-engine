# Compensation Engine — Business Logic Decision Tree

```
COMPENSATION ENGINE (BUSINESS DECISION TREE)
│
├── 1. TRIGGER & TOPOLOGICAL INVERSION TIER
│   └── Trigger: Forward Step Fails Permanently OR Orchestrator Aborts Saga
│       ├── Action: Acquire Global Saga Distributed Mutex (Redis Lease + Fencing Token)
│       │   ├── Lock Acquisition Fails ──> [ABORT: Concurrency Collision — Worker Yields Execution]
│       │   └── Lock Acquired
│       │       └── Action: Atomic Transition saga_instance.state = 'COMPENSATING'
│       │
│       └── Action: Query All Recorded Forward Steps with status='SUCCEEDED' from PostgreSQL
│           ├── Check: Are there any succeeded steps recorded?
│           │   ├── NO ──> Action: Mark saga state = 'FAILED'
│           │   │   └── State: FAILED ──> [TERMINAL: Zero Steps Executed, Nothing to Compensate]
│           │   └── YES ──> Action: Invert Forward Execution Graph (Reverse Topological Order: Step N down to Step 1)
│           │       └── State: COMPENSATION_PIPELINE_BUILT
│
├── 2. REVERSE COMPENSATION EXECUTION TIER
│   └── State: COMPENSATION_PIPELINE_BUILT
│       └── Loop: For each completed step in reverse order (Step i)
│           ├── Check: Is step marked as 'PIVOT' transaction (Irreversible, e.g. Final Settlement)?
│           │   ├── YES ──> [PANIC / CRITICAL: Pivot step cannot be compensated! Escalate to STUCK]
│           │   └── NO ──> Check: Does step provide registered Compensating Action definition?
│           │       ├── NO ──> [FAIL: Missing Compensating Action — Flag as Configuration Poison Pill]
│           │       └── YES ──> Action: Prepare Compensating Payload with Inverted Parameters & Original Step ID
│           │           └── Execution: Dispatch Semantic Undo to Target Service via gRPC/HTTP
│           │               ├── Result: HTTP 200 / gRPC OK (Compensated Successfully)
│           │               │   ├── Action: Atomic UPDATE saga_step SET compensation_status='SUCCEEDED'
│           │               │   └── Check: Are there remaining completed steps to undo?
│           │               │       ├── YES ──> Advance pointer to Step i-1 ──> [Continue Loop]
│           │               │       └── NO ──> State: ALL_STEPS_COMPENSATED
│           │               │
│           │               ├── Result: IDEMPOTENT ALREADY COMPENSATED (HTTP 200 or 409 Flagged)
│           │               │   └── Action: Treat as SUCCESS and advance pointer to Step i-1
│           │               │
│           │               └── Result: FAILURE (e.g. Network Drop, 5xx Server Error, Downstream Down)
│           │                   └── Action: Handover to Compensation Retry Subsystem (Tier 3)
│
└── 3. ESCALATION & COMPLETION TIER
    ├── Branch: ALL_STEPS_COMPENSATED
    │   └── Action: Atomic UPDATE saga_instance SET state='COMPENSATED', completed_at=NOW()
    │       └── Action: Emit outbox event 'SAGA_COMPENSATED' to Kafka Topic
    │           └── State: COMPENSATED ──> [TERMINAL: Full Reversible Consistency Achieved]
    │
    └── Branch: UNRECOVERABLE COMPENSATION FAILURE (Retry Exhaustion)
        └── Action: Atomic UPDATE saga_instance SET state='STUCK', requires_manual_intervention=TRUE
            └── Action: Push Alert to Opsgenie / PagerDuty & Emit DLQ Notification
                └── State: STUCK ──> [TERMINAL: Quarantined Awaiting Manual Operator Resolution]
```

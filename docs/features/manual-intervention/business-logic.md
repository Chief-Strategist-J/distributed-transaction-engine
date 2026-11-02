# Manual Intervention — Business Logic Decision Tree

```
MANUAL INTERVENTION (BUSINESS DECISION TREE)
│
├── 1. TRIGGER & OPERATOR AUTHORIZATION TIER
│   └── Trigger: Operator Initiates Action on Stuck Saga via CLI (`engine-cli`) or Web UI
│       ├── Check: Does Operator have Valid OAuth Token with 'admin:saga:intervene' Role?
│       │   ├── NO ──> [REJECT: HTTP 403 Forbidden — Unauthorized Operator]
│       │   └── YES ──> Action: Verify Target Saga State in PostgreSQL:
│       │       ├── Check: Is saga in a valid intervention state ('STUCK', 'FAILED', 'WAITING')?
│       │       │   ├── NO (Saga is already 'COMPLETED' or active 'RUNNING')
│       │       │   │   └── [REJECT: HTTP 409 Conflict — Cannot Intervene on Active/Completed Saga]
│       │       │   └── YES ──> State: INTERVENTION_LOCKED
│
├── 2. ACTION SELECTION & PRE-CONDITION VALIDATION TIER
│   └── State: INTERVENTION_LOCKED
│       ├── Action: Acquire Distributed Mutex Lock on Target Saga (Prevent Concurrent Worker Interference)
│       └── Evaluation: Determine Selected Operator Action:
│           │
│           ├── Action A: FORCE_RETRY (Re-drive Failed Step)
│           │   ├── Action: Validate Downstream Dependency Health via Diagnostic Ping
│           │   ├── Check: Is Service Responding?
│           │   │   ├── NO ──> Warn Operator with Override Prompt
│           │   │   └── YES ──> Reset Step Failure Counter, Reset State to 'RUNNING' / 'COMPENSATING'
│           │   └── Action: Release Lock & Enqueue into Orchestrator Immediate Queue
│           │
│           ├── Action B: FORCE_COMPENSATE (Manually Trigger Rollback)
│           │   ├── Action: Mark Current Failed Step as 'SKIPPED_BY_OPERATOR'
│           │   ├── Action: Transition Saga State to 'COMPENSATING'
│           │   └── Action: Dispatch Execution to Compensation Engine Tier
│           │
│           ├── Action C: FORCE_COMPLETE (Override Terminal Success)
│           │   ├── Requirement: Mandatory External Approval Ticket ID & Rationale Input
│           │   ├── Action: Mark Current Step as 'MANUALLY_OVERRIDDEN_SUCCESS'
│           │   ├── Action: Inject Operator Synthetic Output Payload
│           │   └── Action: Advance Saga to Next Step or 'COMPLETED'
│           │
│           └── Action D: FORCE_TERMINATE (Permanent Write-Off)
│               ├── Action: Set Saga State to 'FAILED_TERMINATED'
│               └── Action: Release all Held Resource Reservations / Leases
│
└── 3. AUDIT RECORDING & EVENT EMISSION TIER
    └── Action: Append Entry to `manual_intervention_audit_log`:
        │       (saga_id, operator_id, action_type, reason, old_state, new_state, timestamp)
        ├── Action: Emit Immutable Audit Event to Kafka Topic `dist-tx.audit.manual-actions`
        └── Action: Release Distributed Mutex Lock on Saga
            └── State: INTERVENTION_COMPLETED ──> [TERMINAL: Saga Dispatched or Resolved]
```

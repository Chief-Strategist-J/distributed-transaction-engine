# Saga Orchestration — Business Logic Decision Tree

```
SAGA ORCHESTRATION (BUSINESS DECISION TREE)
│
├── 1. WORKFLOW INSTANTIATION & PARSING TIER
│   └── Trigger: Engine Receives Valid Transaction Request
│       ├── Action: Fetch Workflow Definition DAG from Registry by Name & Version
│       ├── Check: Is Workflow Definition Valid, Active, and Non-Deprecated?
│       │   ├── NO ──> [REJECT: HTTP 400 Bad Request — Invalid Workflow Definition]
│       │   └── YES ──> Action: Instantiate Saga Execution Context in PostgreSQL
│       │       ├── Record: Insert into `saga_instance` with state='CREATED', version=1
│       │       └── Record: Insert initial step nodes into `saga_step` with status='PENDING'
│       │           └── State: SAGA_INITIALIZED
│
├── 2. FORWARD GRAPH TRAVERSAL TIER
│   └── State: SAGA_INITIALIZED or STEP_SUCCEEDED
│       ├── Action: Identify Next Executable Steps in DAG (Dependencies Satisfied)
│       ├── Check: Are there runnable steps remaining?
│       │   ├── YES ──> Action: Dispatch Steps:
│       │   │   ├── Single Step ──> Dispatch via Sequential Execution Tier
│       │   │   └── Multiple Sibling Steps ──> Dispatch via Parallel Execution Tier
│       │   │
│       │   └── NO ──> Check: Have all workflow steps completed with status='SUCCEEDED'?
│       │       └── YES ──> Action: Mark Saga Instance as Successfully Finished
│       │           ├── Action: Atomic UPDATE saga_instance SET state='COMPLETED', completed_at=NOW()
│       │           ├── Action: Write Event to Transactional Outbox (`SAGA_COMPLETED`)
│       │           └── State: COMPLETED ──> [TERMINAL: Saga Finalized Successfully]
│
├── 3. FAILURE HANDLING & COMPENSATION TRIGGER TIER
│   └── Trigger: Any Forward Step Emits Permanent Failure or Exhausts Retries
│       ├── Action: Halt All In-Flight Forward Steps in DAG (Send Cancellation Tokens)
│       ├── Action: Atomic UPDATE saga_instance SET state='COMPENSATING'
│       │
│       └── Action: Handover Execution Context to Compensation Engine:
│           ├── Compensation Engine Undoes All Preceding Succeeded Steps in Reverse Order
│           ├── Check: Did Compensation Engine Complete All Undos Successfully?
│           │   ├── YES ──> Action: Atomic UPDATE saga_instance SET state='COMPENSATED'
│           │   │   └── State: COMPENSATED ──> [TERMINAL: Clean Rollback Emitted to Outbox]
│           │   └── NO ──> Action: Atomic UPDATE saga_instance SET state='STUCK'
│           │       └── State: STUCK ──> [TERMINAL: Quarantined Awaiting Manual Intervention]
```

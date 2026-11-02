# Parallel Execution — Business Logic Decision Tree

```
PARALLEL EXECUTION (BUSINESS DECISION TREE)
│
├── 1. FORK & FAN-OUT DISPATCH TIER
│   └── Trigger: Orchestrator Resolves Workflow DAG Node with Multiple Concurrent Siblings
│       ├── Example: Step 2A (Book Flight) and Step 2B (Book Hotel) have No Inter-Dependencies
│       ├── Action: Spawn Worker Goroutines / Async Kafka Tasks for Each Parallel Branch
│       ├── Action: Initialize Distributed Barrier / Sync Construct in Redis / PostgreSQL:
│       │   └── Set `barrier:saga_{id}:step_group_{group_id}` ExpectedCount = N
│       └── State: PARALLEL_FANOUT_ACTIVE
│
├── 2. INDEPENDENT BRANCH EXECUTION TIER
│   └── State: PARALLEL_FANOUT_ACTIVE
│       └── For Each Parallel Branch (Branch k):
│           ├── Action: Execute Remote Service Call with Isolated Idempotency Key
│           ├── Result: SUCCESS (HTTP 200 / gRPC OK)
│           │   ├── Action: Atomic UPDATE saga_step SET status='SUCCEEDED' WHERE id = :branch_step_id
│           │   └── Action: Decrement Shared Redis Barrier Counter (Redis.DECR)
│           │
│           └── Result: FAILURE (e.g. Flight Sold Out / Network Error)
│               ├── Action: Atomic UPDATE saga_step SET status='FAILED', error = :error_msg
│               └── Action: Atomically Set Shared Barrier Flag `has_failure = TRUE`
│                   └── Action: Signal Short-Circuit Cancellation to Other Sibling Goroutines (Context Cancel)
│
└── 3. JOIN & BARRIER SYNCHRONIZATION TIER
    └── Trigger: Barrier Counter Reaches Zero OR Short-Circuit Failure Triggered
        ├── Check: Did All Parallel Branches Complete with status='SUCCEEDED'?
        │   ├── YES (All Succeeded)
        │   │   ├── Action: Aggregate Output Payloads from All Branches into Shared Saga Context
        │   │   └── Action: Advance Orchestrator Pointer to Next Sequential Stage
        │   │       └── State: JOIN_SUCCEEDED ──> [Proceed to Next Forward Step]
        │   │
        │   └── NO (One or More Branches Failed)
        │       ├── Action: Halt Forward Execution Immediately
        │       └── Action: Identify Partially Completed Branches (status='SUCCEEDED')
        │           ├── Action: Submit Only Succeeded Branches to Compensation Engine
        │           └── Action: Mark Branches that were Cancelled Before Execution as 'ABORTED'
        │               └── State: PARALLEL_COMPENSATING ──> [Handover to Compensation Engine]
```

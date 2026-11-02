# Sequential Execution — Business Logic Decision Tree

```
SEQUENTIAL EXECUTION (BUSINESS DECISION TREE)
│
├── 1. PIPELINE INITIALIZATION TIER
│   └── Trigger: Saga Orchestrator Initiates Linear Step Pipeline (Step 1 -> Step 2 -> ... -> Step N)
│       ├── Action: Validate Sequence Order and Dependency Outputs (Step i requires Output of Step i-1)
│       ├── Action: Set CurrentStepIndex = 1
│       └── State: PIPELINE_ACTIVE
│
├── 2. STEP DISPATCH & OUTPUT PIPING TIER
│   └── State: PIPELINE_ACTIVE
│       └── Loop: While CurrentStepIndex <= TotalSteps:
│           ├── Action: Extract Step Definition for CurrentStepIndex
│           ├── Action: Resolve Input Context:
│           │   ├── Merge Initial Saga Payload with Accumulated Outputs from Steps 1 .. CurrentStepIndex-1
│           │   └── Validate Schema of Resolved Step Input Payload
│           │
│           ├── Action: Atomically UPDATE saga_step SET status='RUNNING' WHERE step_index = CurrentStepIndex
│           │
│           └── Execution: Dispatch Step to Service Worker via gRPC / HTTP
│               ├── Result: SUCCESS (Status Code 200 / gRPC OK)
│               │   ├── Action: Atomic UPDATE saga_step SET status='SUCCEEDED', output_payload = :result
│               │   ├── Action: Append Step Output to Saga Execution Context Memory
│               │   └── Action: Increment CurrentStepIndex = CurrentStepIndex + 1
│               │       └── Continue Loop to Next Step
│               │
│               └── Result: FAILURE (Timeout / Business Rejection / Max Retries Breached)
│                   ├── Action: Atomic UPDATE saga_step SET status='FAILED', error_payload = :error
│                   └── Action: Halt Pipeline Immediately! Do NOT Dispatch CurrentStepIndex + 1
│                       └── State: SEQUENTIAL_PIPELINE_BROKEN ──> Advance to Tier 3
│
└── 3. SEQUENTIAL ROLLBACK HANDOVER TIER
    ├── Branch: All Steps Succeeded (CurrentStepIndex > TotalSteps)
    │   └── Action: Mark Saga Instance as COMPLETED
    │       └── State: PIPELINE_COMPLETED ──> [TERMINAL: Output Returned to Client]
    │
    └── Branch: SEQUENTIAL_PIPELINE_BROKEN
        └── Action: Calculate Compensable Range:
            │   CompensableSteps = [CurrentStepIndex-1 down to 1]
            └── Action: Handover CompensableSteps to Compensation Engine
                └── State: ROLLBACK_ENGAGED ──> [Reverse Execution of Succeeded Steps Only]
```

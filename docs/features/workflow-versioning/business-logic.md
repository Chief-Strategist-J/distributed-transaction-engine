# Workflow Versioning — Business Logic Decision Tree

```
WORKFLOW VERSIONING (BUSINESS DECISION TREE)
│
├── 1. VERSION RESOLUTION & PINNING TIER
│   └── Trigger: Client Submits New Transaction for Workflow `OrderProcessing`
│       ├── Check: Did Request Explicitly Specify a Workflow Version (e.g. `version="v2.1"`)?
│       │   ├── YES ──> Action: Query Definition for Specified Version
│       │   │   ├── Version Not Found or Status == 'DEPRECATED_RETIRED'
│       │   │   │   └── [REJECT: HTTP 400 Bad Request — Requested Workflow Version Inactive]
│       │   │   └── Version Found ──> Pin Saga to Explicit Version
│       │   │
│       │   └── NO ──> Action: Query Latest Active Version from Cache (`workflow:{id}:latest`)
│       │       └── Pin Saga to Latest Active Version (e.g. `v2.4`)
│       │
│       └── Action: Store Version Immutably in Saga Record:
│           └── PostgreSQL: `INSERT INTO saga_instance (id, workflow_id, workflow_version, ...) ...`
│               └── State: SAGA_VERSION_PINNED
│
├── 2. RUNTIME ISOLATION & IN-FLIGHT MIGRATION TIER
│   └── State: SAGA_VERSION_PINNED
│       └── Execution Rule: Saga MUST ALWAYS Execute Against Its Pinned Version
│           ├── Scenario A: New Workflow Version `v3.0` Deployed While Saga is `RUNNING` on `v2.4`
│           │   ├── Action: Orchestrator Continues Evaluating Steps using Cached `v2.4` DAG Spec
│           │   └── Guarantee: Zero In-Flight Step Breakage or Schema Incompatibility
│           │
│           └── Scenario B: Critical Hotfix / Security Patch Migration Required for In-Flight Sagas
│               ├── Trigger: SRE Initiates Controlled Saga Migration Tooling
│               ├── Check: Does Target Version `v2.4.1` Share Topological Equivalence with `v2.4`?
│               │   ├── NO (Structural Divergence in Steps)
│               │   │   └── [REJECT: Cannot Hot-Migrate structurally divergent DAG — Must drain naturally]
│               │   └── YES (Only Step Retry Policy or Minor Config Patched)
│               │       └── Action: Atomic UPDATE saga_instance SET workflow_version = 'v2.4.1'
│               │           └── State: MIGRATION_COMMITTED
│
└── 3. DEPRECATION & SUNSET LIFECYCLE TIER
    └── Trigger: Operator Deprecates Old Workflow Version (e.g. `v1.0`)
        ├── Step 1: Transition Status to 'DEPRECATED_DRAINING':
        │   ├── Rule: Reject all NEW saga creation requests targeting `v1.0` (HTTP 410 Gone)
        │   └── Rule: Allow existing in-flight `v1.0` sagas to run until natural completion
        │
        ├── Step 2: Periodic Scan for Active Sagas on `v1.0`:
        │   ├── Active Sagas Remaining > 0 ──> Continue Draining Mode
        │   └── Active Sagas Remaining == 0
        │       └── Step 3: Transition Status to 'RETIRED':
        │           └── Action: Evict from Redis Active Registry Cache
        │               └── State: RETIRED ──> [TERMINAL: Version Archived for Historical Audit Only]
```

# Workflow Definitions — Business Logic Decision Tree

```
WORKFLOW DEFINITIONS (BUSINESS DECISION TREE)
│
├── 1. DEFINITION SUBMISSION & SCHEMA VALIDATION TIER
│   └── Trigger: Engineer / Operator Submits New Workflow Definition (YAML / JSON)
│       ├── Check: Does Document Adhere to Structural JSONSchema / Spec?
│       │   ├── Properties: `workflow_id`, `version`, `steps`, `timeout_seconds`, `retry_policy`
│       │   ├── NO ──> [REJECT: HTTP 400 Bad Request — Schema Validation Errors]
│       │   └── YES ──> State: SCHEMA_VALIDATED
│       │
│       └── Check: Tenant Namespace & Authorization:
│           ├── Check: Does Submitting Principal have 'workflow:write' Permission for Tenant?
│           │   ├── NO ──> [REJECT: HTTP 403 Forbidden]
│           │   └── YES ──> State: AUTHORIZED_FOR_REGISTRATION
│
├── 2. GRAPH INTEGRITY & ACYCLIC VALIDATION TIER
│   └── State: AUTHORIZED_FOR_REGISTRATION
│       ├── Action: Parse Steps and Build Directed Dependency Graph:
│       │   ├── Check 1: Cycle Detection (Depth-First Search / Tarjan's Strongly Connected Components)
│       │   │   ├── Cycle Detected (e.g. Step A -> Step B -> Step A)
│       │   │   │   └── [REJECT: HTTP 422 Unprocessable — Workflow Graph Contains Cycles (Must be DAG)]
│       │   │   └── Graph is Acyclic (Valid DAG)
│       │   │
│       │   ├── Check 2: Orphaned Nodes Check
│       │   │   ├── Any Step depends on non-existent `step_id`?
│       │   │   │   └── [REJECT: HTTP 422 Unprocessable — Broken Dependency Reference]
│       │   │   └── All Dependencies Valid
│       │   │
│       │   └── Check 3: Compensability Verification
│       │       ├── Rule: Every Non-Pivot Forward Step MUST Have a Corresponding Compensating Action Defined
│       │       ├── Check: Does Step have `compensation_action` specified?
│       │       │   ├── NO AND Step is not marked `is_pivot=true`
│       │       │   │   └── [REJECT: HTTP 422 Unprocessable — Non-Pivot Step Missing Compensation]
│       │       │   └── YES ──> State: GRAPH_VERIFIED
│
└── 3. IMMUTABLE REGISTRATION & ACTIVATION TIER
    └── State: GRAPH_VERIFIED
        ├── Action: Compute Semantic Canonical Hash of Workflow Spec (SHA256)
        ├── Action: Check for Existing Version in PostgreSQL `workflow_definitions`:
        │   ├── If Version Exists with Different Hash: Reject (Versions are Immutable!)
        │   └── If New Version:
        │       ├── Action: Insert into `workflow_definitions` (workflow_id, version, spec, hash, created_at)
        │       ├── Action: Update Redis Workflow Registry Cache
        │       └── State: ACTIVE ──> [TERMINAL: Workflow Ready for Saga Instantiation]
```

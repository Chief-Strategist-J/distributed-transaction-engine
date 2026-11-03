# Workflow Definitions — Formal Business Logic Decision Trees, Invariants & Edge Cases

## 1. Architectural Overview & Workflow Decision Tree

```
WORKFLOW DEFINITIONS (BUSINESS DECISION TREE)
│
├── 1. DEFINITION SUBMISSION & SCHEMA VALIDATION TIER [P0]
│   └── Real-World: Platform developer submits new YAML/JSON payment pipeline via CLI / CI/CD pipeline
│       └── Trigger: Engineer / Operator Submits New Workflow Definition
│           ├── Check: Does Document Adhere to Structural JSONSchema / Closed Specification?
│           │   ├── Properties: `tenantId`, `name`, `version`, `steps`, `source`, `externalId`, `correlationId`
│           │   ├── Validation: Name matches regex ^[a-zA-Z0-9_\-\.]{3,128}$?
│           │   │   └── NO ──> [REJECT: HTTP 400 Bad Request — Invalid Workflow Name Pattern]
│           │   ├── Validation: Step count >= 1 and timeout_seconds between 1 and 86400?
│           │   │   └── NO ──> [REJECT: HTTP 400 Bad Request — Step Bounds Constraint Violation]
│           │   ├── Validation: Step ID format valid ^[a-zA-Z0-9_\-]{2,64}$ and unique across workflow?
│           │   │   └── NO ──> [REJECT: HTTP 400 Bad Request — Duplicate or Malformed Step ID]
│           │   └── YES ──> State: SCHEMA_VALIDATED
│           │
│           └── Check: Tenant Namespace & Authorization:
│               ├── Check: Does Submitting Principal have 'workflow:write' Permission for Tenant?
│               │   ├── NO ──> [REJECT: HTTP 403 Forbidden — Insufficient Tenant Scopes]
│               │   └── YES ──> State: AUTHORIZED_FOR_REGISTRATION
│
├── 2. GRAPH INTEGRITY & ACYCLIC VALIDATION TIER [P0]
│   └── Real-World: Microservices DAG with branching and parallel join (e.g. Flight Seat + Hotel -> Payment)
│       └── State: AUTHORIZED_FOR_REGISTRATION
│           ├── Action: Parse Steps and Build Directed Dependency Graph (Adjacency List + In-Degree Array)
│           │   ├── Check 1: Orphaned Dependency Check
│           │   │   ├── Any Step depends on a non-existent stepId?
│           │   │   │   └── YES ──> [REJECT: HTTP 400 Bad Request — Step References Missing Dependency]
│           │   │   └── NO (All Dependencies Exist in Workflow)
│           │   │
│           │   ├── Check 2: Cycle Detection via Kahn's Topological Sort (BFS Queue)
│           │   │   ├── Queue initialized with steps having in-degree == 0
│           │   │   ├── Traverse neighbors, decrement in-degrees, collect topological order
│           │   │   ├── Length of Topological Order == Total Step Count?
│           │   │   │   ├── NO (Cycle Detected: e.g. Step A -> Step B -> Step A or Self-Loop)
│           │   │   │   │   └── [REJECT: HTTP 409 Conflict — Cyclic Dependency Detected in DAG]
│           │   │   │   └── YES ──> Graph is Mathematically Acyclic (Strict DAG Verified)
│           │   │
│           │   └── Check 3: Richardson Compensability & Single-Pivot Invariant
│           │       ├── Check: Count of steps marked `is_pivot = true`
│           │       │   ├── Count > 1
│           │       │   │   └── [REJECT: HTTP 400 Bad Request — Workflow Cannot Have Multiple Pivots]
│           │       │   └── Count <= 1 (Valid Pivot Assignment)
│           │       │
│           │       └── Check: Compensability Verification for Every Step
│           │           ├── Step is marked `is_pivot = false` (Pre-Pivot Compensable Action)
│           │           │   ├── Missing `compensation_action` or `compensation_endpoint`?
│           │           │   │   └── YES ──> [REJECT: HTTP 400 Bad Request — Non-Pivot Step Missing Compensation]
│           │           │   │   └── NO ──> State: COMPENSABILITY_VERIFIED
│           │           └── Step is marked `is_pivot = true` (Point of No Return)
│           │               ├── Declares `compensation_action` or `compensation_endpoint`?
│           │               │   └── YES ──> [REJECT: HTTP 400 Bad Request — Pivot Step Cannot Have Compensation]
│           │               └── NO ──> State: GRAPH_VERIFIED
│
└── 3. IMMUTABLE REGISTRATION & ACTIVATION TIER [P0 / P1]
    └── Real-World: Versioning a live production billing workflow without clobbering 10k running sagas
        └── State: GRAPH_VERIFIED
            ├── Action: Serialize Canonical Step Array to Deterministic JSON
            ├── Action: Compute Semantic Spec Hash = SHA256(CanonicalPayload)
            │
            ├── Action: Verify Storage Gate Invariants against PostgreSQL `workflow_definitions`:
            │   ├── Check: Does `(tenant_id, name, version)` already exist?
            │   │   └── YES ──> [REJECT: HTTP 409 Conflict — Workflow Version Already Exists]
            │   ├── Check: Does `(tenant_id, name, spec_hash)` already exist under another version?
            │   │   └── YES ──> [REJECT: HTTP 409 Conflict — Identical Spec Already Registered]
            │   └── NO (Unique Version and Unique Spec Hash)
            │
            └── Action: Atomic SQL Transaction (Unit of Work):
                ├── Mint Internal UUIDv7 `id` and External `public_id` (`wfd_[0-9a-zA-Z]{26}`)
                ├── INSERT INTO `workflow_definitions` (id, public_id, tenant_id, spec_hash, status='ACTIVE'...)
                ├── INSERT INTO `workflow_definition_steps` for each step node with foreign key to definition
                ├── COMMIT TRANSACTION
                └── State: ACTIVE ──> [TERMINAL: Workflow Ready for Dynamic Saga Instantiation]
```

---

## 2. The 10 Invariants — Real-Life Rationale & Enforcement Trees

### Invariant 1: Directed Acyclic Topology (Zero Cycles Invariant)
* **Real-World Rationale**: A cyclic dependency creates an infinite loop where Step A waits for Step B, which waits for Step A. Workers freeze, connection pools starve, and memory exhausts.
* **Enforcement Gate**: Domain Rules via Kahn's Algorithm + Unit Tests.
```
INVARIANT 1: DIRECTED ACYCLIC GRAPH (ZERO CYCLES)
├── Input: Step Definitions Array
├── Algorithm: Kahn's BFS Topological Sort
│   ├── If len(TopologicalOrder) == len(Steps) ──> [PASS: Valid DAG]
│   └── If len(TopologicalOrder) < len(Steps) ──> [FAIL: Cyclic Dependency Error]
└── Storage Guard: Step dependencies foreign-key graph verified before saga execution
```

### Invariant 2: Richardson Compensability Invariant (No Orphan Mutations)
* **Real-World Rationale**: If a forward step reserves inventory or locks a seat without a matching cancellation hook, any subsequent failure leaves the business with permanently lost resources.
* **Enforcement Gate**: Domain Validation + Database CHECK constraint `workflow_definition_steps_pivot_comp_ck`.
```
INVARIANT 2: RICHARDSON COMPENSABILITY INVARIANT
├── Step is Non-Pivot (`is_pivot = false`):
│   ├── `compensation_action` != '' AND `compensation_endpoint` != '' ──> [PASS]
│   └── `compensation_action` == '' OR `compensation_endpoint` == '' ──> [REJECT: Missing Compensation]
└── Step is Pivot (`is_pivot = true`):
    ├── `compensation_action` == '' AND `compensation_endpoint` == '' ──> [PASS: Point of No Return]
    └── Declares compensation ──> [REJECT: Pivot Cannot Rollback]
```

### Invariant 3: Single Pivot Invariant (Deterministic Point-of-No-Return)
* **Real-World Rationale**: Having multiple pivot points creates an architectural paradox. If Pivot 1 succeeds and Pivot 2 fails, Pivot 1 cannot be rolled back, violating the Saga transaction contract.
* **Enforcement Gate**: Domain Validation in `ValidateCreateWorkflowDefinitionInput`.
```
INVARIANT 3: SINGLE PIVOT INVARIANT
├── Count steps with `is_pivot == true`:
│   ├── count == 0 ──> [PASS: Pure Compensable Saga]
│   ├── count == 1 ──> [PASS: Richardson Canonical Saga]
│   └── count > 1  ──> [REJECT: HTTP 400 — Multiple Pivots Prohibited]
```

### Invariant 4: Cryptographic Spec Immutability (Zero In-Place Mutations)
* **Real-World Rationale**: If an operator modifies a workflow definition in-place while 10,000 sagas are mid-flight, workers deserialize incompatible payloads and crash.
* **Enforcement Gate**: SHA-256 hash lock + SQL UNIQUE constraints.
```
INVARIANT 4: SPEC IMMUTABILITY
├── Compute Canonical SHA256(spec) -> spec_hash
├── SQL Check: UNIQUE (tenant_id, name, version)
└── SQL Check: UNIQUE (tenant_id, name, spec_hash)
    ├── Any violation ──> [REJECT: HTTP 409 Conflict — Versions Are Append-Only]
    └── Unique ──> [PASS: Append Immutable Record]
```

### Invariant 5: Five Mandatory Identifier Roles
* **Real-World Rationale**: Blending internal database keys with external IDs leaks database topologies and breaks multi-tenant data isolation.
* **Enforcement Gate**: Schema definitions & PostgreSQL primary/unique constraints.
```
INVARIANT 5: FIVE IDENTIFIER ROLES
├── 1. `id`: Internal UUIDv7 surrogate key (never exposed to external clients)
├── 2. `public_id`: Opaque prefixed string `wfd_[0-9a-zA-Z]{26}` for REST URLs and logs
├── 3. `tenant_id`: Mandatory ownership partition key leading all composite indexes
├── 4. `source` + `external_id`: Idempotent upsert natural key from upstream Git/CI systems
└── 5. `correlation_id`: W3C distributed trace and audit correlation UUID
```

---

## 3. The 10 Core Edge Cases — Real-Life Scenarios & Mitigations

### Edge Case 1: Self-Referential Direct Loop (`step_a -> step_a`)
* **Scenario**: An author configures retry logic by having `step_a` list `step_a` as its dependency.
* **Decision**: Enqueue check sees `inDegree == 1`. Queue is empty at start. Immediate `HTTP 409 Conflict`.

### Edge Case 2: Multi-Hop Indirect Circular Chain (`A -> B -> C -> A`)
* **Scenario**: Three microservices form an indirect cyclic dependency.
* **Decision**: All nodes remain at `inDegree >= 1`. Zero nodes dequeued. Caught and rejected before persistence.

### Edge Case 3: Disconnected Island Subgraphs (`A -> B` and `C -> D`)
* **Scenario**: A single workflow contains two completely disjoint sub-DAGs.
* **Decision**: Both zero-degree roots (`A` and `C`) enter the queue simultaneously. Both branches are resolved in parallel; valid DAG verified.

### Edge Case 4: Deep Sequential Linear Chain (50+ Steps)
* **Scenario**: Massive enterprise payroll batch process.
* **Decision**: Iterative BFS queue executes in O(V+E) time without recursive stack frames. Benchmark passes in 0.20 ms.

### Edge Case 5: Diamond Parallel Fan-Out and Join (`Start -> A & B -> Join`)
* **Scenario**: Flight and Hotel booked simultaneously, joining at the Payment step.
* **Decision**: In-degree of `Join` is 2. `Join` is only dequeued when both `A` and `B` have completed traversal. Valid parallel DAG.

### Edge Case 6: Duplicate Version Re-Submission with Same Payload
* **Scenario**: CI/CD pipeline re-runs a deploy job with identical YAML.
* **Decision**: Caught by `UNIQUE (tenant_id, name, version)` and `spec_hash`. Returns `HTTP 409 Conflict`.

### Edge Case 7: Duplicate Version Re-Submission with Modified Payload (Tampering)
* **Scenario**: An operator modifies a step timeout under Version 1 without bumping to Version 2.
* **Decision**: Version collision detected; rejects update. Workflow definitions are strictly immutable.

### Edge Case 8: Identical Payload Submitted under New Version (Spec Duplication)
* **Scenario**: Author creates Version 2 with the exact same steps as Version 1.
* **Decision**: Caught by `UNIQUE (tenant_id, name, spec_hash)`. Rejects duplicate registration.

### Edge Case 9: Non-Pivot Step Declaring Empty Rollback Endpoint
* **Scenario**: Step 2 (inventory reservation) omits the compensation URL.
* **Decision**: Rejected at Ingress Gate (Schema validation) and Storage Gate (`CHECK` constraint).

### Edge Case 10: Pivot Step Declaring Compensation Endpoint
* **Scenario**: Author attempts to define a rollback action for a credit card charge step marked `is_pivot = true`.
* **Decision**: Rejection: A pivot transaction is the point-of-no-return; rollbacks past this point must be modeled as separate forward dispute sagas.

# Saga Orchestration Runtime — Business Logic Decision Tree v3 (Production)

> **Legend**: `──>` outcome · `[HALT: CODE]` terminal condition · `◆` real-world failure mode · `⚠` explicit architectural policy choice

---

## 1. The Production Saga Execution Decision Tree

```
SAGA RUNTIME EXECUTION & COMPENSATION — DECISION TREE v3
│
├── 0. INGRESS & INSTANTIATION TIER
│   └── Trigger: POST /api/v1/sagas/execute
│       ├── 0.1 Input & Tenant Validation
│       │   ├── Missing `tenantId`, `workflowName`, `externalId` ──> [HALT: 400 — E_INVALID_INPUT]
│       │   └── `workflowVersion` < 1 ──> [HALT: 400 — E_INVALID_VERSION]
│       │
│       ├── 0.2 Workflow Definition Resolution
│       │   ├── Lookup (tenant_id, workflowName, workflowVersion) in `workflow_definitions`
│       │   ├── Workflow Not Found ──> [HALT: 404 — E_WORKFLOW_NOT_FOUND]
│       │   ├── Workflow status == 'DEPRECATED' or 'ARCHIVED' ──> [HALT: 409 — E_WORKFLOW_INACTIVE]
│       │   └── Pinned Immutable Snapshot: (id, version, spec_hash) ──> State: SNAPSHOT_FROZEN
│       │
│       ├── 0.3 Journal Initialization
│       │   ├── Generate UUIDv7 internal `id` and opaque public `sga_...`
│       │   ├── Seed `saga_step_executions` for every step in definition (`StepStatePending`)
│       │   ├── Derive unique `idempotency_key` per step (`idemp_<saga_id>_<step_id>`)
│       │   └── Insert `saga_instances` with state `CREATED` ──> State: INSTANTIATED
│       │
│       └── State: READY_FOR_DISPATCH
│
├── 1. DYNAMIC IN-DEGREE RESOLUTION & FORWARD DISPATCH TIER
│   └── State: READY_FOR_DISPATCH
│       ├── 1.1 Transition Saga to RUNNING
│       │   └── CAS update: version = version + 1, current_state = 'RUNNING'
│       │
│       ├── 1.2 Root Discovery
│       │   ├── Compute in-degree for all steps from pinned DAG
│       │   └── Filter steps with dependencies == [] ──> Initial Ready Set
│       │
│       ├── 1.3 Dispatch Loop
│       │   ├── For each step in Ready Set:
│       │   │   ├── Mark step `DISPATCHED`, increment `attempt_count`, record `started_at`
│       │   │   ├── Invoke Action Endpoint with `Idempotency-Key` and `step_input`
│       │   │   │
│       │   │   ├── Action Responds 2xx OK:
│       │   │   │   ├── Record `step_output`, `completed_at`
│       │   │   │   ├── Mark step `SUCCEEDED`
│       │   │   │   ├── Add step to `completed_steps` and `succeeded_steps`
│       │   │   │   └── Re-evaluate: Decrement in-degree of downstream steps
│       │   │   │       └── Any dependent reaching in_degree == 0 ──> Append to Ready Set
│       │   │   │
│       │   │   └── Action Responds 5xx / Network Timeout:
│       │   │       ├── Under max_retries?
│       │   │       │   ├── YES ──> Wait exponential backoff with full jitter, retry
│       │   │       │   └── NO ──> Mark step `FAILED`, record `error_message`
│       │   │       └── Trigger State: ABORT_FORWARD_EXECUTION
│       │   │
│       │   └── All Steps in DAG reach `SUCCEEDED`?
│       │       ├── YES ──> Update saga state to `COMPLETED` ──> [TERMINAL: SAGA_COMPLETED]
│       │       └── NO ──> Continue loop or transition to compensation
│       │
│       └── State: ABORT_FORWARD_EXECUTION
│
└── 2. REVERSE TOPOLOGICAL COMPENSATION TIER
    └── State: ABORT_FORWARD_EXECUTION
        ├── 2.1 Freeze Forward Execution
        │   ├── Immediately halt dispatch of remaining `PENDING` steps
        │   └── CAS update saga state to `COMPENSATING`
        │
        ├── 2.2 Reverse Compensation Plan Derivation
        │   ├── Retrieve topological sort order of pinned DAG
        │   ├── Filter executed steps: step ∈ `succeeded_steps` AND is_pivot == false
        │   └── Reverse order: Traverse from nearest predecessor back to initial root
        │
        ├── 2.3 Rollback Execution
        │   ├── For each step in Reverse Chain:
        │   │   ├── Mark step `COMPENSATING`
        │   │   ├── Invoke `compensation_endpoint` with `Idempotency-Key` and `step_input`
        │   │   │   ├── 2xx OK ──> Mark step `COMPENSATED`, record `completed_at`
        │   │   │   └── Failure ──> Retry with capped backoff (compensations MUST NOT give up)
        │   │   └── ◆ If compensation fails permanently ──> Enqueue to Dead Letter Queue (DLQ)
        │   │
        │   └── All compensable steps reach `COMPENSATED`
        │       ├── CAS update saga state to `FAILED`
        │       └── [TERMINAL: SAGA_FAILED_COMPENSATED]
```

---

## 2. Core Architectural Invariants (S1–S12)

| ID | Invariant Name | Rule & Failure Mitigation |
|---|---|---|
| **S1** | **Definition Pinning** | Sagas freeze immutable foreign-key pointers `(workflow_definition_id, workflow_version, spec_hash)`. Mid-flight definition updates cannot alter active sagas. |
| **S2** | **Zero Cross-Tenant Leak** | All database queries and lock operations enforce composite keys `(tenant_id, id)`. |
| **S3** | **Dynamic In-Degree Fanout** | Steps with `dependencies == []` must execute concurrently without synthetic barriers. |
| **S4** | **Strict Reverse Rollback** | Compensation executes in exact reverse topological order of executed steps. Unstarted steps are never compensated. |
| **S5** | **Pivot Inviolability** | Pivot steps represent the point of no return. Sagas never compensate a pivot step; if a pivot succeeds, subsequent steps must be retried until completion. |
| **S6** | **Idempotent Step Dispatch** | Every step execution transmits an opaque `Idempotency-Key: idemp_<saga_id>_<step_id>`. Downstream services must ignore duplicate dispatches. |
| **S7** | **Optimistic Concurrency (CAS)** | All state changes enforce `version = version + 1 WHERE version = current_version`, rejecting split-brain updates. |
| **S8** | **Full Jitter Retry** | Network retries calculate $t_{wait} = \text{random}(0, \min(t_{max}, t_{base} \times 2^{attempt}))$. Prevents thundering herds. |
| **S9** | **Step Timeout Clamping** | Per-step execution timeouts must be $\le$ total saga timeout. |
| **S10** | **Crash Recovery Journal** | Every step transition is written to disk before network dispatch. If a node crashes, a recovery worker reads the journal and resumes in-flight steps safely. |
| **S11** | **W3C Distributed Tracing** | Root spans and child step spans propagate W3C `traceparent` headers through all HTTP/gRPC calls. |
| **S12** | **Structured Loki Audit Logs** | Every transition logs structured JSON to stdout including `timestamp`, `level`, `service`, `tenant_id`, `trace_id`, and `span_id`. |

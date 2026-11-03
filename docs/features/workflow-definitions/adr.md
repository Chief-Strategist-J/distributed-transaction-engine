# ADR-0003: Declarative Workflow Definitions, DAG Cycle Detection & Immutable Semantic Versioning

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-11-03 |
| Deciders | Lead Systems Architect & Core Architecture Guild |
| Related Docs | [Business Logic & Decision Trees](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/workflow-definitions/business-logic.md) · [Rule 0 Scaffolding](file:///home/btpl-lap-22/live/System-Architect/llm-obs-policies/rules/folder-structure/0-package-folder-writing-rule.md) · [Rule 5 Data Contract](file:///home/btpl-lap-22/live/System-Architect/llm-obs-policies/rules/folder-structure/05-db-writing-rules.md) |
| Target Feature | [Workflow Definitions Feature](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/src/features/workflow-definitions) |

---

## 1. Context & Problem Statement

A distributed saga orchestrator coordinates multi-service transactions involving disparate downstream microservices (payments, inventories, shipping, notifications). To execute these sagas reliably without manual intervention or data corruption, the orchestrator cannot rely on ad-hoc or hardcoded step lists.

Allowing dynamic or unverified workflows directly into the execution engine introduces critical operational hazards:
1. **Cyclic Deadlocks**: Mutual step dependencies (e.g. Step A requires Step B, and Step B requires Step A) cause the orchestrator loop to block indefinitely or consume unbounded CPU/goroutines.
2. **Uncompensable Financial Drift**: If a forward step fails after non-pivot steps have mutated downstream state without declaring an inverse compensating action, the saga cannot cleanly rollback, resulting in permanent financial discrepancies.
3. **Multiple Pivot Ambiguity**: A saga can have at most one pivot transaction (the point of no return). Workflows with multiple pivot steps create indeterminate rollback targets when later pivots fail.
4. **Silent Workflow Definition Mutation**: Modifying step definitions under an active version causes in-flight sagas to execute incompatible steps, causing version mismatch panics and corrupted states.
5. **Dangling/Orphan References**: Steps pointing to undeclared dependency keys lead to runtime panics during forward traversal.

---

## 2. Decision Drivers & Architecture Invariants

* **Contract-First Inviolability (Rule 0 Guardrail 2)**: No implementation orchestrator may run until the workflow definition DAG specification is registered and validated as an immutable contract.
* **Pure Acyclic Invariant (Kahn's / Tarjan's Theorem)**: A workflow is strictly a Directed Acyclic Graph (DAG). Any cycle or self-loop must be rejected at submission time with `HTTP 409 Conflict` / `HTTP 422 Unprocessable`.
* **Richardson Compensability Invariant**: Every forward step preceding the pivot transaction MUST declare a corresponding compensating action and endpoint. The pivot step itself must not declare compensation.
* **Single Pivot Rule**: A workflow definition can declare at most one pivot step.
* **Cryptographic Spec Immutability**: Every workflow spec is canonically serialized and hashed using SHA-256 (`spec_hash`). Re-registering the same version with modified steps is rejected; re-registering an identical spec under a new version is rejected.
* **Three-Gate Data Contract (Rule 5)**: Ingress validation (`schema`), domain logic (`rules`), and storage constraints (`CHECK`, `UNIQUE`, `FOREIGN KEY`) must all reject invalid DAGs.

---

## 3. Considered Architectural Options

### Option 1: Hardcoded Code-Based Workflows in Go
* **Architecture**: Workflows defined as Go structs or code pipelines directly inside the orchestrator binary.
* **Pros**: Simple compile-time type safety; zero database queries to fetch definitions.
* **Cons**:
  - Requires full service re-compilation and zero-downtime rolling deploys to add or modify a workflow.
  - Zero dynamic multi-tenant isolation.
  - In-flight sagas cannot easily pin to their original definition version across deployments.
  - Fails contract-first design principles.
* **Verdict**: **Rejected**.

### Option 2: Scripted Turing-Complete Workflow DSL (Lua / JavaScript / Python)
* **Architecture**: Workflows defined as dynamic scripts with arbitrary conditional statements, loops, and custom logic executed inside an embedded engine (e.g., Otto, LuaVM).
* **Pros**: Highly flexible for end users; supports dynamic while-loops and conditionals.
* **Cons**:
  - Turing-complete scripts cannot be statically validated for cycles or termination (Halting Problem).
  - High risk of resource exhaustion, infinite loops, and arbitrary code execution vulnerabilities.
  - Impossible to mathematically guarantee reversible compensations across distributed boundaries.
* **Verdict**: **Rejected (High security and operational hazard)**.

### Option 3: Declarative Directed Acyclic Graph (DAG) with Topological Validation & Cryptographic Version Hashing (Chosen)
* **Architecture**: Pure declarative JSON/YAML schemas parsed into strongly typed step definitions. Verified via Kahn's topological sort algorithm, checked for compensability, and stored in PostgreSQL with SHA-256 checksum locks.
* **Pros**:
  - Provable acyclic termination and deterministic execution order.
  - O(V + E) validation time with zero runtime allocation overhead.
  - Complete version immutability—in-flight sagas pin to an exact immutable version and `spec_hash`.
  - Full relational integrity: foreign keys ensure steps cannot be orphaned.
* **Verdict**: **Accepted**.

---

## 4. Detailed Technical Specification

### 4.1 Pure Domain DAG Validation Algorithm

The topology validator implements Kahn's algorithm in `domain/rules/workflow-definitions.rules.go`:
```
function ValidateDAGTopology(steps):
    Initialize inDegree map for all V steps to 0
    Initialize adjList map for all V steps to empty list
    
    For each step in steps:
        For each dep in step.Dependencies:
            If dep not in stepMap: return ERROR_ORPHAN_DEPENDENCY
            adjList[dep].append(step.StepID)
            inDegree[step.StepID]++
            
    queue = [step for step in inDegree if inDegree[step] == 0]
    topologicalOrder = []
    
    While queue is not empty:
        curr = queue.pop()
        topologicalOrder.append(curr)
        For neighbor in adjList[curr]:
            inDegree[neighbor]--
            If inDegree[neighbor] == 0: queue.append(neighbor)
            
    If len(topologicalOrder) != len(steps):
        return ERROR_CYCLIC_DEPENDENCY
        
    Validate that at most one pivot step exists.
    Return topologicalOrder
```

### 4.2 Database Relational Schema

Schema defined in `0002_create_workflow_definitions_tables.sql`:
* `workflow_definitions`:
  - `id UUID`, `public_id VARCHAR(32)`, `tenant_id VARCHAR(64)`
  - `name VARCHAR(128)`, `version BIGINT`
  - `spec_hash CHAR(64)` (SHA-256)
  - `CONSTRAINT workflow_definitions_tenant_name_ver_uk UNIQUE (tenant_id, name, version)`
  - `CONSTRAINT workflow_definitions_spec_hash_uk UNIQUE (tenant_id, name, spec_hash)`
* `workflow_definition_steps`:
  - `id UUID`, `workflow_id UUID`, `step_id VARCHAR(64)`
  - `is_pivot BOOLEAN`, `action_type VARCHAR(64)`, `action_endpoint VARCHAR(256)`
  - `compensation_action VARCHAR(64)`, `compensation_endpoint VARCHAR(256)`
  - `timeout_seconds INT`, `max_retries INT`, `dependencies JSONB`
  - `CONSTRAINT workflow_definition_steps_pivot_comp_ck CHECK ((is_pivot = TRUE AND compensation_action = '') OR (is_pivot = FALSE AND compensation_action <> ''))`

---

## 5. Consequences & Trade-offs

### Positive Consequences
* **Deterministic Execution**: The orchestrator receives a pre-sorted topological order, eliminating runtime resolution ambiguity.
* **Audit & Version Stability**: Historical sagas retain an immutable pointer to the exact workflow version and hash under which they started.
* **Fast Failure**: Malformed graphs, missing compensations, and cyclic graphs fail at ingress before touching runtime saga instances.

### Negative Consequences / Risks
* **No Dynamic Looping**: True iterative loops (e.g. while condition X do Y) are disallowed by design. Mitigated by designing iterations as fan-out parallel child sagas.
* **Schema Strictness**: Adding even a minor non-breaking annotation to a step requires minting a new version because the `spec_hash` is cryptographically enforced.


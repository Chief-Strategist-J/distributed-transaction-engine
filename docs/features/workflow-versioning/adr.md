# ADR-0011: Immutable Workflow Specification Pinning, Deterministic Version Rollout & In-Flight Drain Boundaries

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-11-07 |
| Deciders | Lead Systems Architect & Core Architecture Guild |
| Related Docs | [Business Logic & Decision Tree v2](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/workflow-versioning/business-logic.md) · [Diagrams](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/workflow-versioning/diagrams.md) · [Failure Cases & Mitigations](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/workflow-versioning/failure-cases.md) |
| Target Feature | [Workflow Versioning Feature](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/src/features/workflow-versioning) |

---

## 1. Context & Problem Statement

Distributed multi-step transactions can run for hours, days, or even weeks (e.g., cross-border B2B trade financing, multi-day escrow holds, KYC compliance approvals). During this lifecycle, engineering teams routinely deploy new workflow definitions, modify step schemas, deprecate third-party integrations, and update compensation logic.

Deploying naive workflow versioning models produces catastrophic production bugs:
1. **The In-Flight Mutation Hazard**: Modifying an active workflow DAG mid-execution. A saga starts under v1.0 (where Step 3 is Payment Auth), but before Step 3 runs, v2.0 deploys (where Step 3 is changed to Fraud Verification). The executing worker attempts to decode Step 3 output using v2.0 schemas, leading to unhandled deserialization crashes and corrupted state machines.
2. **Non-Deterministic Replay Failures**: If an engine utilizes event-sourced replaying without specification pinning, replaying historical events against newly deployed code executes a different code branch, corrupting historical ledgers.
3. **Rollout Version Oscillation**: A canary deployment routes 10% of new sagas to v2.0. If a client retries an ambiguous start request, naive resolution assigns the retry to v1.0, creating duplicate sagas across conflicting schema versions.
4. **External Un-Pinning**: A saga definition pins v1.0, but steps read live database fee schedules or external feature flags that change dynamically, silently violating version determinism.

---

## 2. Real-World Use Cases: When to Use vs. When to Reject

### When to Use
* **Long-Running Financial & Compliance Sagas (Hours to 30 Days)**: Workflows spanning multiple days must remain pinned to their exact initiation specification, even if 20 new application releases deploy while the saga is active.
* **Zero-Downtime Blue/Green Engine Deployments**: Rolling new orchestrator code pods without terminating or corrupting in-flight sagas.
* **Contractual Multi-Tenant SLA Version Pinning**: Enterprise tenants contractually requiring frozen API contracts until certified on new versions.

### When NOT to Use (Rejection Boundaries)
* **Stateless Microservice CRUD Operations**: Sub-second HTTP endpoints where no asynchronous state is held across deployments.
* **Emergency Security Hotfixes to In-Flight Sagas**: If a zero-day vulnerability requires mutating an active step immediately, the saga must be intercepted via [`manual-intervention`](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/manual-intervention), never through implicit version swapping.

---

## 3. Analytical Numbers & Capacity Model

| Metric | Specification Target | Analytical Limit & Derivation |
|---|---|---|
| **Spec Bundle Storage per Version** | $\sim 45 \text{ KB}$ | Serialized DAG JSON schema + step action bindings + timeout rules. |
| **Active Concurrent Versions per Cluster** | Up to $100 \text{ versions}$ | Multi-version worker capability matrix supported concurrently. |
| **Version Resolution Latency** | $p50 < 1.2 \text{ ms}, \quad p99 < 5 \text{ ms}$ | In-memory Redis bundle cache + immutable PostgreSQL lookup. |
| **Spec Cache Hit Ratio** | $> 99.8\%$ | Static immutable bundles cached locally in worker memory (`sync.Map`). |
| **Deprecation Grace Window** | $30 \text{ to } 90 \text{ days}$ | In-flight sagas drain naturally before old bundle removal. |

---

## 4. Architectural Trade-offs & Considered Options

| Versioning Strategy | Determinism & Safety | Operational Overhead | Schema Evolution Agility | Verdict |
|---|---|---|---|---|
| **Option 1: Live In-Flight Graph Patching (Temporal Style)** | ⚠️ Complex developer branching (`if version >= 2 ...`); high risk of replay non-determinism. | ⚠️ High cognitive burden on engineers writing migration branches in code. | ✅ Immediate updates to running workflows. | **Rejected** |
| **Option 2: Infrastructure Forking (Separate Clusters)** | ✅ Complete physical isolation per version. | ❌ Massive cloud infrastructure cost and operational sprawl. | ❌ Complex ingress traffic routing. | **Rejected** |
| **Option 3: Immutable Spec Pinning & Capability-Aware Workers (Selected)** | ✅ 100% deterministic; zero in-flight mutations; historical reproducibility. | ✅ Low operational burden: immutable bundle registry in DB; workers load versions dynamically. | ✅ Seamless canary rollouts and gradual drainage. | **Adopted** |

---

## 5. Decision Drivers & Architecture Invariants (WV-01 – WV-16)

* **WV-01 (Specification Pinning)**: Every saga instance permanently freezes an immutable reference to `(workflow_definition_id, workflow_version, spec_hash)` at initiation.
* **WV-02 (Immutable Spec Hashing)**: Workflow definitions are cryptographically hashed (`spec_sha256`). Any change to a step, action, timeout, or schema generates a new version number and hash.
* **WV-03 (Idempotent Start Version Consistency)**: Saga initiation idempotency records store the resolved version. A retried start request always resolves to the exact same version as the original attempt.
* **WV-04 (Zero Live External Dependencies)**: All fee schedules, timeout limits, and step bindings must be packaged inside the version bundle or snapshotted into `saga_instances.context` at initiation.
* **WV-05 (Capability-Aware Worker Ownership)**: An orchestrator worker pod only claims leases on sagas whose definition versions are supported by its loaded worker binaries.
* **WV-06 (Dual-Custody Version Promotion)**: Promoting a version to `ACTIVE` or blocking a version requires approval from two authorized operators.

---

## 6. Relational Schema & DDL

```sql
CREATE TYPE version_lifecycle_status AS ENUM ('DRAFT', 'CANARY', 'ACTIVE', 'DEPRECATED', 'BLOCKED');

-- Immutable workflow definition bundles
CREATE TABLE IF NOT EXISTS workflow_version_registry (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id VARCHAR(64) NOT NULL,
    version INT NOT NULL,
    spec_sha256 CHAR(64) NOT NULL,
    status version_lifecycle_status NOT NULL DEFAULT 'DRAFT',
    canary_percentage INT NOT NULL DEFAULT 0,
    definition_bundle JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    promoted_at TIMESTAMPTZ,
    deprecated_at TIMESTAMPTZ,

    CONSTRAINT uq_workflow_version UNIQUE (workflow_id, version),
    CONSTRAINT chk_canary_bounds CHECK (canary_percentage >= 0 AND canary_percentage <= 100)
);

-- Active saga binding pinning table
ALTER TABLE saga_instances 
    ADD COLUMN IF NOT EXISTS workflow_version INT NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS spec_sha256 CHAR(64),
    ADD CONSTRAINT fk_saga_version FOREIGN KEY (workflow_id, workflow_version) 
        REFERENCES workflow_version_registry (workflow_id, version);

CREATE INDEX IF NOT EXISTS idx_workflow_version_lookup 
    ON workflow_version_registry (workflow_id, status);
```


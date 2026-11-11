# Workflow Versioning — Architectural & Visual Diagrams

This document illustrates the architectural blueprints, component relationships, and rollout workflows for the **Immutable Workflow Versioning Subsystem**, supporting [ADR-0011](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/workflow-versioning/adr.md) and [Business Logic Decision Tree v2](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/workflow-versioning/business-logic.md).

---

## 1. High-Level Architecture (HLD): Immutable Version Registry & Worker Engine

```mermaid
flowchart TB
    subgraph RegistryLayer ["Workflow Version Registry (PostgreSQL 18)"]
        V1_Bundle[("v1.0 Bundle<br/>spec_hash: 0a8f...<br/>status: DEPRECATED")]
        V2_Bundle[("v2.0 Bundle<br/>spec_hash: 99c1...<br/>status: ACTIVE")]
    end

    subgraph ClientIngress ["Client Ingress & Rollout Engine"]
        ClientReq["Client POST /sagas"] --> Router["Rollout Version Resolver"]
        Router -->|1. Resolve Active Version (v2.0)| RegistryLayer
        Router -->|2. Pin Immutable Spec Hash| Starter["Saga Initiation Service"]
    end

    subgraph SagaStorage ["Saga Storage & Pinning (WV-01)"]
        Starter -->|INSERT with workflow_version=2, spec_hash='99c1...'| SagaTable[("saga_instances")]
    end

    subgraph HeterogeneousWorkers ["Capability-Aware Worker Fleet (WV-16)"]
        SagaV1["Older Saga Tx-10 (Pinned to v1.0)"] -->|Lease Claimed by| WorkerA["Worker Pod A (Capabilities: v1.0, v2.0)"]
        SagaV2["Newer Saga Tx-20 (Pinned to v2.0)"] -->|Lease Claimed by| WorkerB["Worker Pod B (Capabilities: v2.0)"]
    end
```

---

## 2. Low-Level Component Model (LLD)

```mermaid
classDiagram
    class WorkflowRegistryPort {
        <<interface>>
        +RegisterVersion(ctx, workflowID, version, specBundle) (string, error)
        +PromoteVersion(ctx, workflowID, version, targetStatus) error
        +GetActiveVersion(ctx, workflowID) (*WorkflowVersionRecord, error)
        +GetVersionByHash(ctx, workflowID, specHash) (*WorkflowVersionRecord, error)
    }

    class VersionResolverPort {
        <<interface>>
        +ResolveVersionForEntity(ctx, workflowID, entityID) (*WorkflowVersionRecord, error)
    }

    class CapabilityRegistry {
        -[]int supportedVersions
        +CanExecute(workflowID, version) bool
        +RegisterCapability(version) void
    }

    class VersionedSagaOrchestrator {
        -WorkflowRegistryPort registry
        -CapabilityRegistry capabilities
        +ExecuteStep(ctx, sagaInstance, stepIndex) error
        +ValidateStepSchema(ctx, sagaInstance, stepPayload) bool
    }

    VersionedSagaOrchestrator --> WorkflowRegistryPort : queries
    VersionedSagaOrchestrator --> CapabilityRegistry : validates
```

---

## 3. Zero-Downtime Blue/Green Rollout Sequence with In-Flight Drain

```mermaid
sequenceDiagram
    autonumber
    participant Op as Release Engineer
    participant Registry as Version Registry
    participant WorkerFleet as Worker Fleet
    participant OldSaga as In-Flight Saga (Pinned to v1.0)
    participant NewSaga as New Saga (Pinned to v2.0)

    Op->>Registry: Publish Version v2.0 (spec_hash: '99c1...', status: 'ACTIVE')
    Registry-->>Op: 201 Created (v1.0 transitioned to DEPRECATED)
    
    rect rgb(240, 248, 255)
        Note over WorkerFleet, OldSaga: In-Flight Sagas Drain on Pinned Version (WV-01)
        WorkerFleet->>OldSaga: Execute Step 4 using v1.0 Definition
        OldSaga-->>WorkerFleet: Step 4 SUCCEEDED (v1.0 contracts preserved)
    end

    rect rgb(240, 255, 240)
        Note over WorkerFleet, NewSaga: New Sagas Execute on Active Version
        WorkerFleet->>NewSaga: Initiate Saga Tx-99 on v2.0 Definition
        NewSaga-->>WorkerFleet: Step 1 SUCCEEDED (v2.0 contracts enforced)
    end
```

---

## 4. Idempotent Start Version Consistency (Invariant WV-11)

```mermaid
sequenceDiagram
    autonumber
    participant Client as Client API
    participant Engine as Saga Engine
    participant DB as PostgreSQL 18
    participant Registry as Version Registry

    Note over Registry: At T0: v1.0 is ACTIVE. Canary rollout promoted to v2.0 at T1.
    
    Client->>Engine: POST /sagas (Idempotency-Key: 'idem-42', at T0)
    Engine->>Registry: Resolve Version -> v1.0
    Engine->>DB: INSERT INTO idempotency_keys (key, version) VALUES ('idem-42', 1)
    Note over Engine: Network drop occurs before HTTP 201 response reaches Client
    
    Note over Registry: Release Engineer promotes v2.0 to ACTIVE at T1
    
    Client->>Engine: Retry POST /sagas (Idempotency-Key: 'idem-42', at T2)
    Engine->>DB: Query idempotency_keys WHERE key = 'idem-42'
    DB-->>Engine: Key found! (Pinned to version: 1)
    
    rect rgb(240, 248, 255)
        Note over Engine: Invariant WV-11 Enforced
        Engine->>Engine: Return existing v1.0 saga instance!
        Note over Engine: Client retry does NOT flip to v2.0!
    end
```

---

## 5. Workflow Version Lifecycle State Machine

```mermaid
stateDiagram-v2
    [*] --> DRAFT: Bundle Registered & Spec Hashed
    
    DRAFT --> CANARY: Promotion with Canary Percentage (e.g., 10%)
    CANARY --> ACTIVE: 100% Rollout Approved (Dual Custody)
    
    ACTIVE --> DEPRECATED: Newer Version Promoted to ACTIVE
    DEPRECATED --> ARCHIVED: Zero In-Flight Sagas on Version (30-day drain)
    
    ACTIVE --> BLOCKED: Security Defect Discovered (Immediate Ingress Halt)
    CANARY --> BLOCKED: Canary Defect Discovered
    
    BLOCKED --> [*]
    ARCHIVED --> [*]
```


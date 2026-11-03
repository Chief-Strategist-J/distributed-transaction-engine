# Workflow Definitions — Visual Architecture, Internal Engine Mechanics & Process Flow Diagrams

This document illustrates the internal architecture, mathematical graph algorithms, data contracts, and end-to-end execution flow of the **Workflow Definitions subsystem**.

---

## 1. Non-Technical / Business Overview: The Blueprint of a Distributed Workflow

Before an airline can book a seat, charge a credit card, or reserve a hotel room, the engine must know the exact rules of engagement: what steps run first, which run in parallel, and who cleans up if a payment fails.

```mermaid
flowchart TD
    Submit(["Developer / Operator Submits Workflow JSON/YAML"]):::startClass --> Gate1{"Gate 1: Schema Check<br/><i>Are step names, timeouts & URLs valid?</i>"}

    Gate1 -->|"Invalid Syntax or Missing Endpoint"| RejectBadReq["REJECT: HTTP 400 Bad Request<br/><i>Syntax errors returned to caller</i>"]:::terminalFail
    Gate1 -->|"Valid Spec"| Gate2{"Gate 2: Graph Cycle Check<br/><i>Does Kahn's DAG Algorithm find any infinite loops?</i>"}

    Gate2 -->|"Loop / Cycle Detected"| RejectConflict["REJECT: HTTP 409 Conflict<br/><i>Cyclic dependency error returned</i>"]:::terminalFail
    Gate2 -->|"Clean Acyclic DAG"| Gate3{"Gate 3: Compensability Check<br/><i>Does every non-pivot forward step have a rollback action?</i>"}

    Gate3 -->|"Missing Undo Action"| RejectUnproc["REJECT: HTTP 422 Unprocessable<br/><i>Uncompensable step rejected</i>"]:::terminalFail
    Gate3 -->|"All Rollbacks Present"| HashCompute["Compute Semantic SHA-256 Hash<br/><i>Cryptographic fingerprint of entire spec</i>"]:::processClass

    HashCompute --> Gate4{"Gate 4: Immutability Check<br/><i>Does this version or hash already exist in PostgreSQL?</i>"}
    Gate4 -->|"Spec Hash or Version Collision"| RejectImmut["REJECT: HTTP 409 Conflict<br/><i>Versions are immutable once stored</i>"]:::terminalFail
    Gate4 -->|"New Unique Version"| PersistDB[("PostgreSQL Atomic Commit<br/><i>Insert workflow_definitions & steps</i>")]:::dbClass

    PersistDB --> Done(["TERMINAL: Active Immutable Blueprint Ready for Saga Engine"]):::terminalSuccess

    classDef startClass fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#0d47a1;
    classDef processClass fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20;
    classDef terminalSuccess fill:#f1f8e9,stroke:#33691e,stroke-width:2px,color:#1b5e20;
    classDef terminalFail fill:#ffebee,stroke:#c62828,stroke-width:2px,color:#b71c1c;
    classDef dbClass fill:#fff3e0,stroke:#e65100,stroke-width:2px,color:#e65100;
```

---

## 2. Hexagonal Subsystem Topology & Internal Component Flow

The subsystem strictly adheres to the stateless hexagonal core pattern:

```mermaid
graph LR
    subgraph INGRESS["API / Delivery Ingress Layer"]
        Router["workflow-definitions.router.go<br/>(Path routing & method filtering)"]
        Handler["workflow-definitions.handler.go<br/>(Headers, JSON serialization, RFC envelopes)"]
    end

    subgraph CORE["Domain Core & Hexagonal Boundary"]
        Schema["contracts.schema.go<br/>(Input parsing & SHA-256 spec hashing)"]
        Types["contracts.types.go<br/>(Domain entities & DTO models)"]
        Rules["domain/rules/workflow-definitions.rules.go<br/>(Kahn's DAG cycle detection & pivot validator)"]
        Service["service/workflow-definitions.service.go<br/>(Orchestration & UUIDv7/public_id minting)"]
    end

    subgraph INFRA["Infrastructure Persistence Layer"]
        Port["infrastructure/ports/repository.port.go<br/>(Abstract driven port interface)"]
        MemAdapter["infrastructure/adapters/repository.adapter.go<br/>(In-memory CAS test adapter)"]
        PGAdapter["infrastructure/adapters/postgres.adapter.go<br/>(Production relational SQL engine)"]
        Queries["infrastructure/queries/queries.go<br/>(Named parameterized FLOW_* SQL queries)"]
        DB[("PostgreSQL 14+<br/>• workflow_definitions<br/>• workflow_definition_steps")]
    end

    Router --> Handler
    Handler --> Service
    Service --> Schema
    Service --> Rules
    Service --> Port
    Port -.->|Implements| MemAdapter
    Port -.->|Implements| PGAdapter
    PGAdapter --> Queries
    PGAdapter --> DB
```

---

## 3. Internal Mechanics: Kahn's DAG Cycle Detection Algorithm

The mathematical core of graph validation uses Kahn's algorithm (Topological BFS) to guarantee that incoming workflows contain zero circular dependencies and compute the exact execution order.

```mermaid
flowchart TD
    Step1["1. Build In-Degree Table & Adjacency List<br/><i>For each step: calculate count of prerequisite dependencies</i>"] --> Step2["2. Enqueue All Zero In-Degree Steps<br/><i>Steps with 0 prerequisites can execute immediately</i>"]
    
    Step2 --> Loop{"Is Queue Empty?"}
    
    Loop -->|"No (Pop Step S)"| Process["Append S to TopologicalOrder<br/>For each neighbor N dependent on S:<br/>Decrement inDegree[N] by 1"]
    Process --> CheckZero{"Has inDegree[N] reached 0?"}
    CheckZero -->|"Yes"| Enqueue["Enqueue N"]
    CheckZero -->|"No"| Loop
    Enqueue --> Loop

    Loop -->|"Yes (Queue Empty)"| Verify{"Length of TopologicalOrder == Total Steps?"}
    Verify -->|"Equal (No remaining cycles)"| Valid["Valid DAG<br/><i>Topological order sorted & returned</i>"]:::terminalSuccess
    Verify -->|"Not Equal (Unresolved circular nodes)"| Cycle["REJECT: Circular Cycle Detected<br/><i>e.g. A depends on B and B depends on A</i>"]:::terminalFail

    classDef terminalSuccess fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20;
    classDef terminalFail fill:#ffebee,stroke:#c62828,stroke-width:2px,color:#b71c1c;
```

---

## 4. End-to-End Registration & Verification Sequence Diagram

This sequence represents what happens when a client invokes `POST /api/v1/workflow-definitions`:

```mermaid
sequenceDiagram
    autonumber
    actor Client as Ingress Client / CLI
    participant Router as WorkflowDefinitionsRouter
    participant Handler as WorkflowDefinitionsHandler
    participant Svc as WorkflowDefinitionsService
    participant Rules as Domain Rules (DAG & Compensability)
    participant Schema as Contracts (Spec Hashing)
    participant Repo as PostgresRepositoryAdapter
    participant DB as PostgreSQL Database

    Client->>Router: POST /api/v1/workflow-definitions<br/>[x-tenant-id: tenant_prod, Body: Workflow JSON]
    Router->>Handler: RegisterWorkflow(http.ResponseWriter, http.Request)
    Handler->>Handler: Validate transport headers & decode JSON body
    Handler->>Svc: RegisterWorkflow(ctx, CreateWorkflowDefinitionInput)

    rect rgb(240, 248, 255)
        Note over Svc,Rules: Domain Verification Tier
        Svc->>Schema: ValidateCreateWorkflowDefinitionInput(input)
        Schema-->>Svc: Input OK (Field boundaries valid)
        
        Svc->>Rules: ValidateDAGTopology(steps)
        Note over Rules: Executes Kahn's Algorithm (BFS):<br/>• Cycle Detection<br/>• Orphan Dependency Verification<br/>• Single-Pivot Placement Invariant
        Rules-->>Svc: ValidationResult (IsValid=true, TopoOrder=['reserve', 'book', 'charge'])
        
        Svc->>Schema: ComputeSpecHash(name, steps)
        Note over Schema: Serializes canonical JSON & computes SHA-256
        Schema-->>Svc: specHash (e.g. 'e3b0c44298fc1c14...')
    end

    rect rgb(255, 250, 240)
        Note over Svc,DB: Atomic Storage Gate Tier
        Svc->>Repo: GetBySpecHash(ctx, tenant_prod, name, specHash)
        Repo->>DB: SELECT id FROM workflow_definitions WHERE tenant_id=$1 AND spec_hash=$2
        DB-->>Repo: No Rows Found (Unique)
        
        Svc->>Repo: GetByNameAndVersion(ctx, tenant_prod, name, version=1)
        Repo->>DB: SELECT id FROM workflow_definitions WHERE tenant_id=$1 AND name=$2 AND version=$3
        DB-->>Repo: No Rows Found (Unique)

        Svc->>Svc: Mint UUIDv7 ID and Public ID ('wfd_018d9f1c...')
        Svc->>Repo: Save(ctx, WorkflowDefinitionInstance)
        
        Repo->>DB: BEGIN TRANSACTION
        Repo->>DB: INSERT INTO workflow_definitions (id, public_id, tenant_id, spec_hash...)
        Repo->>DB: INSERT INTO workflow_definition_steps (workflow_id, step_id, is_pivot, action_type...)
        Repo->>DB: COMMIT TRANSACTION
        DB-->>Repo: Transaction Committed Successfully
        Repo-->>Svc: Saved Successfully
    end

    Svc-->>Handler: WorkflowDefinitionInstance
    Handler-->>Client: HTTP 201 Created<br/>ApiResponse Envelope (success: true, data: {publicId: 'wfd_...'})
```

---

## 5. Diamond Dependency Resolution (Parallel Fan-Out & Join)

Workflows frequently split into concurrent branches and join before reaching the critical pivot point:

```mermaid
flowchart TD
    StartStep["Step 1: Ingest & Validate Order<br/><b>is_pivot=false</b><br/><i>Rollback: Cancel Intent</i>"]:::nodeStyle
    
    StartStep --> BranchA["Step 2A: Reserve Flight Seat<br/><b>is_pivot=false</b><br/><i>Rollback: Cancel Seat</i>"]:::branchStyle
    StartStep --> BranchB["Step 2B: Book Hotel Room<br/><b>is_pivot=false</b><br/><i>Rollback: Cancel Room</i>"]:::branchStyle

    BranchA --> JoinPivot["Step 3: Charge Payment Card<br/><b>is_pivot=true [PIVOT TRANSACTION]</b><br/><i>Zero compensation (point of no return)</i>"]:::pivotStyle
    BranchB --> JoinPivot

    JoinPivot --> PostPivot["Step 4: Dispatch Ticket & Vouchers<br/><b>is_pivot=false, Retriable</b><br/><i>Idempotent repeated retries</i>"]:::postPivotStyle

    classDef nodeStyle fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#0d47a1;
    classDef branchStyle fill:#fff8e1,stroke:#f57f17,stroke-width:2px,color:#e65100;
    classDef pivotStyle fill:#ffebee,stroke:#c62828,stroke-width:3px,color:#b71c1c;
    classDef postPivotStyle fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20;
```

### Compensability Boundary Rules
1. **Pre-Pivot Steps (Steps 1, 2A, 2B)**: Compensable. If Step 2B fails, Step 2A is rolled back via its declared `CANCEL_SEAT` action, and Step 1 via `Cancel Intent`.
2. **Pivot Step (Step 3)**: The point of no return. If Step 3 succeeds, the saga *must* finish forward. Rollback is impossible once money is settled.
3. **Post-Pivot Steps (Step 4)**: Must be idempotent and retriable until success.


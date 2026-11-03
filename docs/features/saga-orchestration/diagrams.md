# Saga Orchestration Runtime — Architectural & Visual Diagrams

---

## 1. Hexagonal Runtime Architecture

```mermaid
flowchart TD
    subgraph Ingress [API Delivery & Observability]
        Client["Client / Upstream Gateway"]
        Mux["Observability Middleware (W3C Tracing & Loki Logging)"]
        Handler["SagaHandler (HTTP REST /v1/sagas)"]
    end

    subgraph Core [Domain & Application Core]
        Svc["SagaOrchestrationService"]
        Rules["Saga Execution Rules (In-Degree & Reverse Topo)"]
        StateM["Transaction State Machine (CAS FSM)"]
    end

    subgraph Ports [Infrastructure Ports]
        RepoPort["SagaRepositoryPort"]
        WfPort["WorkflowDefinitionsRepositoryPort"]
        DispPort["StepDispatcherPort"]
    end

    subgraph Adapters [Infrastructure Adapters]
        PgRepo["Postgres / Memory Saga Adapter"]
        PgWf["Postgres / Memory Workflow Adapter"]
        HttpDisp["HTTP Step Dispatcher Adapter"]
    end

    subgraph Storage [Databases & Downstream Services]
        PG[("PostgreSQL: saga_instances & saga_step_executions")]
        Downstream["Downstream Microservices (REST / gRPC)"]
    end

    Client -->|HTTP POST /api/v1/sagas/execute| Mux
    Mux --> Handler
    Handler --> Svc
    Svc --> Rules
    Svc --> StateM
    Svc --> RepoPort
    Svc --> WfPort
    Svc --> DispPort
    RepoPort --> PgRepo
    WfPort --> PgWf
    DispPort --> HttpDisp
    PgRepo --> PG
    PgWf --> PG
    HttpDisp --> Downstream
```

---

## 2. Dynamic In-Degree Parallel Fan-Out

```mermaid
graph TD
    subgraph Workflow DAG Definition
        R1["reserve_flight (In-Degree: 0)"]
        R2["reserve_hotel (In-Degree: 0)"]
        P["charge_card [PIVOT] (In-Degree: 2)"]
        C["send_confirmation (In-Degree: 1)"]

        R1 --> P
        R2 --> P
        P --> C
    end

    subgraph Parallel Runtime Traversal
        T0["1. FindInitialExecutableSteps()"] -->|In-Degree == 0| DispatchR["Dispatch R1 & R2 Concurrently"]
        DispatchR -->|R1 SUCCEEDED: In-Degree P: 2 -> 1| W1["Wait for R2"]
        DispatchR -->|R2 SUCCEEDED: In-Degree P: 1 -> 0| DispatchP["Dispatch charge_card [PIVOT]"]
        DispatchP -->|P SUCCEEDED: In-Degree C: 1 -> 0| DispatchC["Dispatch send_confirmation"]
        DispatchC -->|C SUCCEEDED| Terminal["Saga Completed"]
    end
```

---

## 3. Reverse Compensation Sequence Diagram

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant Engine as SagaOrchestrationService
    participant Flight as Flight Service
    participant Hotel as Hotel Service
    participant Payment as Payment Gateway
    participant Journal as PostgreSQL Journal

    Client->>Engine: POST /api/v1/sagas/execute (Flight + Hotel + Payment)
    Engine->>Journal: INSERT saga_instance (CREATED -> RUNNING)

    par Parallel Root Step Dispatch
        Engine->>Flight: POST /reserve (Idempotency-Key)
        Engine->>Hotel: POST /book (Idempotency-Key)
    and
        Flight-->>Engine: 200 OK (Reservation Confirmed)
        Engine->>Journal: UPDATE step reserve_flight -> SUCCEEDED
    and
        Hotel-->>Engine: 200 OK (Room Held)
        Engine->>Journal: UPDATE step book_hotel -> SUCCEEDED
    end

    Note over Engine: Both dependencies satisfied. In-degree for payment reaches 0.

    Engine->>Payment: POST /charge (Amount: $500)
    Payment-->>Engine: 500 Internal Server Error (Card Declined / Network Timeout)
    Engine->>Journal: UPDATE step charge_payment -> FAILED

    Note over Engine: Failure encountered before pivot. Trigger reverse compensation.

    Engine->>Journal: UPDATE saga_instance -> COMPENSATING

    Note over Engine: Step 1: Compensate nearest predecessor (book_hotel)
    Engine->>Hotel: POST /cancel (Undo Room Hold)
    Hotel-->>Engine: 200 OK (Room Released)
    Engine->>Journal: UPDATE step book_hotel -> COMPENSATED

    Note over Engine: Step 2: Compensate initial root (reserve_flight)
    Engine->>Flight: POST /cancel (Undo Reservation)
    Flight-->>Engine: 200 OK (Seat Released)
    Engine->>Journal: UPDATE step reserve_flight -> COMPENSATED

    Engine->>Journal: UPDATE saga_instance -> FAILED
    Engine-->>Client: HTTP 500 (Saga Compensated and Rolled Back)
```

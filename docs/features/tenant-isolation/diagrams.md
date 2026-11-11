# Tenant Isolation — Architectural & Visual Diagrams

This document illustrates the multi-tier defense-in-depth isolation blueprints, component models, and sequence workflows for the **Multi-Tenant Isolation Subsystem**, supporting [ADR-0009](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/tenant-isolation/adr.md) and [Business Logic Decision Tree v2](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/tenant-isolation/business-logic.md).

---

## 1. High-Level Architecture (HLD): Multi-Tier Defense-in-Depth

```mermaid
flowchart TB
    subgraph IngressLayer ["Ingress & Identity Gateway"]
        ClientA["Tenant A Client"] -->|JWT (sub: userA, tenant: org_1)| Gateway["API Gateway / Middleware"]
        ClientB["Attacker (JWT: org_2, X-Tenant: org_1)"] --> Gateway
        Gateway -->|Extract Identity-Derived Tenant (TI-01)| Authenticator["Token Claims Extractor"]
        Authenticator -->|Inject TenantContext: org_1| ContextFilter["Tenant Context Scope"]
    end

    subgraph ServiceLayer ["Orchestrator Service Layer"]
        ContextFilter --> Service["MultiTenantSagaService"]
        Service -->|1. Atomic Quota Reservation (TI-08)| QuotaManager["Atomic Quota Guard"]
        Service -->|2. App-Level Predicate: WHERE tenant_id = 'org_1'| RepoPort["TenantAwareRepository"]
    end

    subgraph DatabaseLayer ["PostgreSQL 18 Multi-Tenant Layer"]
        RepoPort -->|BEGIN TRANSACTION| DB_Conn["Pooled DB Connection"]
        DB_Conn -->|SET LOCAL app.current_tenant = 'org_1' (TI-03)| RLS_Engine["PostgreSQL RLS Engine"]
        RLS_Engine -->|FORCE ROW LEVEL SECURITY (TI-04)| Table_Sagas[("saga_instances<br/>PRIMARY KEY (tenant_id, id)")]
        RLS_Engine -->|WITH CHECK Policy: Blocks writes to other tenants| Table_Sagas
        DB_Conn -->|COMMIT TRANSACTION (auto-clears SET LOCAL)| PgBouncer["PgBouncer Pool"]
    end
```

---

## 2. Low-Level Component Model (LLD)

```mermaid
classDiagram
    class TenantContext {
        +string TenantID
        +string PrincipalID
        +[]string Roles
        +IsAuthenticated() bool
    }

    class TenantSecurityMiddleware {
        +ExtractAndVerify(ctx, httpReq) (TenantContext, error)
        +SanitizeCrossTenantHeader(ctx, claims, requestedTenant) (string, error)
    }

    class QuotaRepositoryPort {
        <<interface>>
        +AcquireQuotaSlot(ctx, tenantID) (bool, error)
        +ReleaseQuotaSlot(ctx, tenantID) error
    }

    class TenantRepositoryPort {
        <<interface>>
        +SetTransactionTenant(ctx, tenantID) error
        +CreateSaga(ctx, tenantID, saga) error
        +GetSagaByID(ctx, tenantID, sagaID) (*SagaInstance, error)
    }

    class MultiTenantSagaService {
        -TenantRepositoryPort repo
        -QuotaRepositoryPort quota
        +StartSaga(ctx, req) (*SagaInstance, error)
        +QuerySaga(ctx, sagaID) (*SagaInstance, error)
    }

    MultiTenantSagaService --> TenantRepositoryPort : invokes
    MultiTenantSagaService --> QuotaRepositoryPort : enforces
    TenantSecurityMiddleware ..> TenantContext : produces
    MultiTenantSagaService ..> TenantContext : consumes
```

---

## 3. Transaction-Scoped RLS Execution & Connection Pool Recycling

```mermaid
sequenceDiagram
    autonumber
    participant App as Service Worker
    participant Pool as PgBouncer Connection Pool
    participant PG as PostgreSQL 18
    participant NextReq as Next Tenant Request (Tenant B)

    App->>Pool: Borrow Connection
    Pool-->>App: Connection #42

    App->>PG: BEGIN TRANSACTION;
    rect rgb(240, 248, 255)
        Note over App, PG: Transaction-Scoped Context (TI-03)
        App->>PG: SET LOCAL app.current_tenant = 'tenant-A';
        App->>PG: SELECT * FROM saga_instances WHERE id = 'tx-100';
        Note over PG: RLS applies USING (tenant_id = 'tenant-A')
        PG-->>App: Return Tenant A row
        App->>PG: COMMIT;
        Note over PG: SET LOCAL automatically resets on COMMIT!
    end

    App->>Pool: Return Connection #42
    
    rect rgb(255, 245, 230)
        Note over NextReq, PG: Zero Connection Poisoning
        NextReq->>Pool: Borrow Connection #42
        NextReq->>PG: BEGIN;
        NextReq->>PG: SELECT current_setting('app.current_tenant', true);
        PG-->>NextReq: NULL / Empty (Clean slate!)
    end
```

---

## 4. Atomic Quota Reservation Sequence (Eliminating Check-Then-Act Race)

```mermaid
sequenceDiagram
    autonumber
    participant Client as 50 Concurrent Requests
    participant App as Orchestrator Worker
    participant DB as PostgreSQL 18

    Client->>App: 50 Parallel POST /sagas (Quota = 10 active)
    
    rect rgb(240, 255, 240)
        Note over App, DB: Atomic In-Database Slot Decrement
        App->>DB: UPDATE tenant_quotas SET current_active_sagas = current_active_sagas + 1 WHERE tenant_id = 'org_1' AND current_active_sagas < max_active_sagas RETURNING current_active_sagas;
        DB-->>App: Exactly 10 requests return 1 row affected (Granted)
        DB-->>App: 40 requests return 0 rows affected (Quota Exceeded)
    end

    App-->>Client: 10 Sagas Started (201 Created)
    App-->>Client: 40 Sagas Rejected (429 Too Many Requests / Quota Exceeded)
```

---

## 5. Tenant Status Lifecycle & Suspension State Machine

```mermaid
stateDiagram-v2
    [*] --> ACTIVE: Tenant Provisioned
    
    ACTIVE --> SUSPENDED_BILLING: Payment Method Failed
    ACTIVE --> SUSPENDED_ABUSE: High Anomaly Rate / DDoS Probe
    
    SUSPENDED_BILLING --> ACTIVE: Balance Paid
    SUSPENDED_ABUSE --> ACTIVE: Manual Security Clearance
    
    state SUSPENDED_BILLING {
        [*] --> BLOCK_INGRESS
        BLOCK_INGRESS --> DRAIN_IN_FLIGHT: Existing Sagas Continue Settling
    }

    state SUSPENDED_ABUSE {
        [*] --> HARD_FREEZE
        HARD_FREEZE --> HALT_OUTBOUND: Immediate Outbox & Timer Freeze
    }
```


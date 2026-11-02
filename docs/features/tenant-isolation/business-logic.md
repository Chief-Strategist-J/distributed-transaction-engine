# Tenant Isolation — Business Logic Decision Tree

```
TENANT ISOLATION (BUSINESS DECISION TREE)
│
├── 1. INGRESS EXTRACTION & TENANT IDENTIFICATION TIER
│   └── Trigger: Request Enters Ingress API / Kafka Consumer Message
│       ├── Action: Extract Tenant Identifier from Inbound Context:
│       │   ├── HTTP / gRPC: From JWT Claim (`tenant_id`), Verified API-Key, or Header `X-Tenant-ID`
│       │   └── Kafka Message: From CloudEvents Header `ce_tenantid`
│       │
│       ├── Check: Is Tenant ID Valid, Non-Empty, and Present in `registered_tenants` Table?
│       │   ├── NO ──> [REJECT: HTTP 401 Unauthorized / Drop Kafka Event with Security Alert]
│       │   └── YES ──> Check: Is Tenant Account Status == 'ACTIVE'?
│       │       ├── Status == 'SUSPENDED' ──> [REJECT: HTTP 403 Forbidden — Tenant Suspended for Billing/Abuse]
│       │       └── Status == 'ACTIVE' ──> State: TENANT_AUTHENTICATED
│
├── 2. RESOURCE FAIRNESS & RATE LIMITING TIER
│   └── State: TENANT_AUTHENTICATED
│       ├── Action: Check Tenant Dynamic Token Bucket in Redis (`rate_limit:{tenant_id}`):
│       │   ├── Check: Does Tenant have Available Tokens?
│       │   │   ├── NO ──> [REJECT: HTTP 429 Too Many Requests — Tenant Quota Breached]
│       │   │   └── YES ──> Decrement Token Bucket
│       │
│       └── Action: Check Tenant Concurrent In-Flight Saga Limit:
│           ├── Check: ActiveSagasForTenant < MaxConcurrentSagas (Tier Quota)?
│           │   ├── NO ──> [QUEUE / THROTTLE: Delay Ingestion or Return 429 Retry-After]
│           │   └── YES ──> State: TENANT_AUTHORIZED_FOR_COMPUTE
│
└── 3. STORAGE & DATABASE ISOLATION TIER
    └── State: TENANT_AUTHORIZED_FOR_COMPUTE
        ├── Action: Bind Tenant Context to PostgreSQL Database Connection:
        │   │
        │   ├── Strategy A: Row-Level Security (RLS) Policy Enforcement
        │   │   └── Execute: `SET LOCAL app.current_tenant_id = :tenant_id;`
        │   │       └── PostgreSQL Kernel automatically filters all SELECT, UPDATE, DELETE queries
        │   │
        │   └── Strategy B: Schema-Per-Tenant or Explicit Compound Primary Keys
        │       └── Constraint: Every query MUST contain `WHERE tenant_id = :tenant_id`
        │           ├── Check: Does Query Mutation Match Tenant Context?
        │           │   ├── Cross-Tenant ID in Body ──> [PANIC: Immediate Security Exception!]
        │           │   └── Matching ID ──> Execute Mutation
        │
        └── Action: Partition Isolation in Messaging & Caching:
            ├── Redis Keys Prefixed: `{tenant_id}:saga:{saga_id}`
            └── Kafka Headers Tagged: Message routed to Tenant Dedicated or Shared Partition with Tenant Key
                └── State: ISOLATION_GUARANTEED ──> [TERMINAL: Zero Cross-Tenant Data Leaks]
```

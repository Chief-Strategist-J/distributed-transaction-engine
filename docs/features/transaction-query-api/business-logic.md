# Transaction Query API — Business Logic Decision Tree

```
TRANSACTION QUERY API (BUSINESS DECISION TREE)
│
├── 1. INGRESS AUTHENTICATION & QUERY VALIDATION TIER
│   └── Trigger: Client Submits Query Request (`GET /api/v1/transactions` or `GET /api/v1/transactions/{id}`)
│       ├── Check: Is Authentication Valid and Caller Authorized for `tx:read` Scope?
│       │   ├── NO ──> [REJECT: HTTP 401 Unauthorized / HTTP 403 Forbidden]
│       │   └── YES ──> Action: Extract Client `tenant_id`
│       │
│       ├── Check: Query Type:
│       │   ├── Type A: Point Lookup by Transaction ID (`GET /api/v1/transactions/{id}`)
│       │   │   └── Action: Validate UUIDv4 Format ──> Proceed to Tier 2 (Single Lookup)
│       │   │
│       │   └── Type B: Filtered Search / List Query (`GET /api/v1/transactions?status=...&from=...`)
│       │       ├── Action: Validate Date Range (Max Query Window: 30 Days)
│       │       └── Action: Enforce Pagination Constraints (Default: limit=20, max_limit=100)
│       │           └── Proceed to Tier 3 (Range Search)
│
├── 2. POINT LOOKUP & DUAL-LAYER CACHE TIER
│   └── Trigger: Point Lookup by Transaction ID
│       ├── Layer 1: High-Speed Cache (Redis) Lookup
│       │   ├── Check: Does Redis Key `tx:{tenant_id}:{id}` Exist?
│       │   │   ├── YES (Cache Hit — Active or Recently Completed Saga)
│       │   │   │   └── Action: Deserialize JSON Payload & Inject Header `X-Cache: HIT`
│       │   │   │       └── State: RETURN_CACHE ──> [TERMINAL: HTTP 200 OK]
│       │   │   └── NO (Cache Miss) ──> Proceed to Database Layer
│       │
│       └── Layer 2: PostgreSQL Ground-Truth Read Replica Lookup
│           ├── SQL: SELECT * FROM saga_instance WHERE id = :id AND tenant_id = :tenant_id;
│           ├── Check: Does Record Exist?
│           │   ├── NO ──> [REJECT: HTTP 404 Not Found — Transaction ID Does Not Exist]
│           │   └── YES ──> Action: Query Child Steps (`saga_step`) Ordered by `step_index ASC`
│           │       ├── Action: If State is Terminal ('COMPLETED', 'FAILED'), Populate Redis Cache (TTL=3600s)
│           │       └── State: RETURN_DB ──> [TERMINAL: HTTP 200 OK with Execution DAG]
│
└── 3. PAGINATED RANGE SEARCH TIER
    └── Trigger: Filtered List Query
        ├── Action: Execute Query on Read Replica with Covered Indexes:
        │   └── SQL: SELECT id, state, created_at, completed_at FROM saga_instance
        │            WHERE tenant_id = :tenant_id
        │              AND (:status IS NULL OR state = :status)
        │              AND created_at >= :from_time AND created_at <= :to_time
        │            ORDER BY created_at DESC LIMIT :limit OFFSET :offset;
        │
        ├── Action: Generate Keyset / Cursor Pagination Tokens (`next_cursor`)
        └── Action: Return JSON Response with Metadata
            └── State: RETURN_PAGE ──> [TERMINAL: HTTP 200 OK with Results Array]
```

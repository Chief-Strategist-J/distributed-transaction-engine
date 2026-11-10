Accepting `X-Tenant-ID` as a tenant source puts tenant selection in the **client's hands**. A user with a valid JWT for tenant A sends `X-Tenant-ID: B`. If any code path prefers the header (and a "fallback" path always appears eventually), that user reads and writes tenant B's sagas.

The tenant must come from the **authenticated credential**: the JWT claim, or the tenant the API key was issued to. A header may only *select* among tenants the principal is verified to belong to. [Certain]

Five more errors, ranked by damage:

**1. Row-level security as described has several holes.** [Certain]
I disagree on five specific points:
- **`SET LOCAL` outside a transaction does nothing.** PostgreSQL warns and discards it, so autocommit statements run with **no tenant context**.
- **Session-level `SET` on pooled connections leaks.** The next request on that connection runs as the previous tenant.
- **Owners and superusers bypass RLS** unless the table forces it, and roles with the bypass attribute ignore it entirely.
- **INSERT and UPDATE need a `WITH CHECK` policy.** A `USING` filter alone doesn't stop writing a row *into another tenant*.
- **Views, `SECURITY DEFINER` functions, and materialized views** run with their owner's rights by default and return every tenant's rows.

**2. "Drop Kafka event with security alert" is data loss, and the tenant header is producer-controlled.** [Certain]
I disagree because an unknown tenant is often an **onboarding race** (the registry hasn't propagated yet), not an attack. A `ce_tenantid` header that disagrees with the key, payload, or aggregate owner is an integrity failure.
Here's what I'd do instead: quarantine in both cases, verify consistency across header, key, payload, and aggregate, and never drop.

**3. "PANIC" on a cross-tenant ID turns an attack probe into a pod crash.** [Certain]
I disagree because anyone can send another tenant's IDs in a request body and crash your services repeatedly. Return **404** (not 403, which confirms the resource exists), record a security event, and keep serving.

**4. The concurrent-saga limit is check-then-act.** [Certain]
Reading the active count and then creating a saga lets 20 simultaneous requests all see 9 of 10 and all proceed. The quota must be an **atomic reservation in the same transaction** as saga creation.

**5. "Suspended → reject" says nothing about sagas already running, and "zero cross-tenant leaks" is a claim no isolation scheme can make.** [Certain]
A tenant suspended mid-saga still has payments to settle, compensations to run, refunds owed, and scheduled jobs pending. Suspension semantics must be defined per reason (billing vs. abuse).
Leaks also happen outside the database: logs, traces, metrics labels, search aggregations, error messages, DLQ views, exports, signed URLs, sequential IDs. Isolation is **defense in depth**, verified continuously.

```
TENANT ISOLATION (SAGA ENGINE) — BUSINESS LOGIC DECISION TREE (v2)
Legend: [C] Certain   [L] Likely (verify per stack/version)   [G] Guessing (business/security must decide)
│
├── 0. SCOPE
│   ├── Isolation dimensions: data read, data write, side effects (emails, payments, webhooks), compute fairness,
│   │   operator access, observability
│   ├── Model: defense in depth — at least two independent controls on every data path
│   └── Claim: "no known leak paths, continuously tested" — never "zero leaks guaranteed"
│
├── 1. INVARIANTS
│   ├── TI-01 Tenant is derived from authenticated identity; client-supplied values only select among verified memberships [C]
│   ├── TI-02 Every unit of work (request, message, saga step, timer, compensation, job) carries an explicit tenant context [C]
│   ├── TI-03 Tenant context is transaction-scoped in the database; absence of context denies access [C]
│   ├── TI-04 RLS is forced, has USING and WITH CHECK policies, and application roles cannot bypass it [C]
│   ├── TI-05 Every query also carries an explicit tenant predicate; keys and foreign keys include tenant [C]
│   ├── TI-06 Cross-tenant references are answered as "not found" and recorded; never crash [C]
│   ├── TI-07 Unknown or inconsistent tenant on messages → quarantine, never drop, never process [C]
│   ├── TI-08 Quotas are enforced atomically with the action they limit [C]
│   ├── TI-09 Side effects use the tenant's own configured endpoints and credentials only [C]
│   └── TI-10 Operator cross-tenant access is explicit, scoped, time-bound, and audited [C]
│
├── 2. TENANT RESOLUTION  (TI-01)
│   ├── 2.1 HTTP / gRPC from clients
│   │   ├── JWT: tenant claim validated (issuer, audience, signature); multi-tenant users → membership list
│   │   ├── Requested tenant (path or header) → must be in verified memberships → else 404/403 + security event
│   │   ├── API key → tenant bound at issuance (lookup), never from request
│   │   └── Gateway strips client-supplied internal tenant headers
│   ├── 2.2 Service-to-service
│   │   └── Tenant from internal signed token minted by gateway/token service; plain headers not trusted
│   ├── 2.3 Kafka messages (TI-07)
│   │   ├── Tenant from envelope; must equal key prefix, payload tenant, and aggregate's owning tenant
│   │   ├── Mismatch → quarantine (INTEGRITY) + security alert
│   │   └── Unknown tenant → quarantine (may be onboarding race) → reprocess after registry propagation
│   ├── 2.4 Saga engine internal inputs (replies, timers, compensations, interventions)
│   │   └── Tenant from the saga record (authoritative); reply tenant must match saga tenant
│   ├── 2.5 Scheduled jobs / background work
│   │   └── Tenant from schedule/job record; global jobs iterate tenants with per-tenant context per unit
│   └── Edge cases
│       ├── E2.1 Header overrides JWT tenant — cross-tenant access — SECURITY [C]
│       ├── E2.2 Valid JWT for tenant A, path id belongs to tenant B — must be 404, not data — SECURITY [C]
│       ├── E2.3 Message dropped because tenant not yet replicated — LOSS [C]
│       └── E2.4 Reply from step worker carries different tenant than saga — accepted — CORRUPT/SECURITY [C]
│
├── 3. TENANT STATUS & LIFECYCLE
│   ├── Status source: authoritative registry; cached with short TTL + invalidation push on change [L: seconds]
│   ├── ACTIVE → normal
│   ├── SUSPENDED (by reason, business-defined) [G]
│   │   ├── New commands: rejected (403 with reason code)
│   │   ├── In-flight sagas: continue to completion | pause at next safe point | compensate (per reason)
│   │   ├── Money owed TO tenant (refunds, payouts): continue unless legal hold
│   │   ├── Scheduled jobs: paused
│   │   └── Inbound events from partners for this tenant: accepted and processed or parked (never dropped)
│   ├── OFFBOARDING → no new work; in-flight drained or compensated; data export; then deletion workflow
│   ├── OFFBOARDED → id tombstoned forever; events for it quarantined/discarded by policy with record
│   └── Edge cases
│       ├── E3.1 Suspended tenant still active for cache TTL — commands accepted after suspension [C]
│       ├── E3.2 Suspension stops refunds owed to tenant's customers — LOSS for third parties [C]
│       └── E3.3 Tenant id reused after offboarding — new tenant sees old tenant's residual data — SECURITY [C]
│
├── 4. PER-UNIT TENANT CONTEXT  (TI-02)
│   ├── Context object created per request/message/step/timer; passed explicitly
│   ├── No thread-local/goroutine-global reuse across units in worker pools
│   ├── Propagated into: outbox rows, step commands, audit records, logs (opaque id), cache keys, search queries
│   └── E4.1 Worker lane reuses previous record's tenant context — writes into wrong tenant — SECURITY [C]
│
├── 5. DATABASE ISOLATION
│   ├── 5.1 Row-level security (TI-03, TI-04)
│   │   ├── Policies: USING (read/update/delete visibility) AND WITH CHECK (insert/update target) on every tenant table
│   │   ├── Table-level forced RLS so table owners are also subject to policies [C]
│   │   ├── Application roles: not table owners, no superuser, no RLS bypass attribute [C]
│   │   ├── Tenant setting set per transaction (transaction-scoped) at start of every transaction
│   │   ├── Policy treats missing/empty setting as "no rows" (deny), never "all rows"
│   │   ├── Pooled connections: transaction-scoped setting only; never session-level setting
│   │   ├── Views: created with invoker rights where supported (PostgreSQL 15+) or avoided for tenant data [C]
│   │   ├── Security-definer functions: avoided for tenant data or enforce tenant internally
│   │   ├── Materialized views: not covered by RLS → tenant-filtered by construction or not exposed [C]
│   │   └── Migrations/maintenance roles: separate, audited; never used by application paths
│   ├── 5.2 Explicit predicates & keys (TI-05)
│   │   ├── Every query includes tenant predicate (second control)
│   │   ├── Primary keys and unique constraints include tenant (or ids globally unique + tenant column)
│   │   ├── Foreign keys include tenant (prevents linking rows across tenants)
│   │   └── Query lint / repository layer refuses tenant-less queries on tenant tables
│   ├── 5.3 Schema-per-tenant / database-per-tenant variants
│   │   ├── Search path set per transaction; never shared session state
│   │   └── Cross-schema joins forbidden in application roles
│   ├── 5.4 Cross-tenant references (TI-06)
│   │   ├── Lookup returns nothing under RLS → respond 404
│   │   ├── Mutation referencing foreign tenant id → rejected (FK + RLS), 404, security event
│   │   └── Never crash/panic on detection
│   └── Edge cases
│       ├── E5.1 Transaction-scoped setting used in autocommit statement — no context — empty results or, with
│       │         a permissive default, all rows [C]
│       ├── E5.2 Session-level setting on pooled connection — next request sees previous tenant — SECURITY [C]
│       ├── E5.3 Application connects as table owner — RLS not applied — SECURITY [C]
│       ├── E5.4 Missing WITH CHECK — insert/update moves row into another tenant — SECURITY [C]
│       ├── E5.5 Reporting view owned by admin — returns all tenants — SECURITY [C]
│       └── E5.6 Panic on cross-tenant id — attacker crashes pods repeatedly — OUTAGE [C]
│
├── 6. NON-DATABASE STORES
│   ├── Cache: keys prefixed by tenant; cache wrapper rejects unprefixed keys; values never shared across tenants
│   ├── Search: tenant filter enforced by query layer for all query types (incl. aggregations, suggestions)
│   ├── Object storage: tenant-prefixed paths; signed URLs scoped to object and short-lived; no listing across prefixes
│   ├── Kafka: shared topics carry multiple tenants → consumers enforce tenant per record (§2.3); dedicated topics per policy
│   ├── DLQ / quarantine / intervention tooling: tenant-scoped views; masked payloads
│   ├── Exports & reports: generated per tenant; recipient bound to tenant
│   └── Edge cases
│       ├── E6.1 Search aggregation counts include other tenants — volume leak — SECURITY [C]
│       ├── E6.2 Signed URL for tenant A object guessable/long-lived and shared — data exposure [C]
│       └── E6.3 Cache key collision (same saga id format across tenants) without prefix — wrong tenant data [C]
│
├── 7. SIDE EFFECTS  (TI-09)
│   ├── Emails, webhooks, payment provider calls use configuration of the saga's tenant only
│   ├── Webhook URLs validated at configuration (ownership verification, no internal addresses)
│   ├── Provider credentials stored per tenant; selection by saga tenant, never by request input
│   └── Edge cases
│       ├── E7.1 Webhook for tenant A's event delivered to tenant B's endpoint (config lookup bug) — data leak [C]
│       ├── E7.2 Payment executed with platform's or another tenant's provider account — funds misrouted [C]
│       └── E7.3 Tenant webhook URL points to internal network — server-side request forgery — SECURITY [C]
│
├── 8. FAIRNESS & QUOTAS  (TI-08)
│   ├── Ingress rate limits per tenant (token bucket); limiter store outage → policy: fail open for low-risk reads,
│   │   fail closed for expensive/monetary commands [G]
│   ├── Concurrent saga quota: reservation counter incremented in the same transaction as saga creation
│   │   (decremented on terminal state); over quota → 429 with Retry-After or queued per tier
│   ├── Engine-internal fairness: per-tenant shares in step dispatch, timers, outbox relay, retries, compensations
│   ├── Priority: money-at-stake and deadline-critical work within each tenant's share
│   └── Edge cases
│       ├── E8.1 Count-then-create race — tenant exceeds quota under burst [C]
│       ├── E8.2 Ingress limited but one tenant's retries flood dependencies — others starve [C]
│       └── E8.3 Quota counter drifts (missed decrement on crash) — tenant blocked forever [C]
│           └── Handling: reconciliation of counters against actual non-terminal sagas
│
├── 9. OPERATOR & PLATFORM ACCESS  (TI-10)
│   ├── Platform tooling runs without tenant context only for platform-level data
│   ├── Tenant data access by support: impersonation session (tenant, reason, ticket, expiry) → audited
│   ├── Bulk cross-tenant operations: explicit scope list, approvals, per-tenant audit records
│   └── E9.1 Admin console queries all tenants by default — over-exposure — SECURITY [C]
│
├── 10. OBSERVABILITY LEAKAGE
│   ├── Logs/traces: opaque tenant ids, no payload data; access scoped
│   ├── Metrics: tenant labels only for top-N/VIP (cardinality) — no per-tenant business values exposed broadly
│   ├── Error responses: no other tenants' identifiers or data in messages
│   └── E10.1 Error message includes "saga X belongs to tenant B" — existence + ownership leak [C]
│
├── 11. IDENTIFIERS & SIDE CHANNELS
│   ├── Non-sequential, non-guessable public ids (no tenant volume inference)
│   ├── Existence probing: 404 for both "missing" and "other tenant" (TI-06)
│   ├── Timing: authorization check before expensive lookups to reduce timing differences [L]
│   └── E11.1 Sequential saga ids — competitor infers tenant volumes — business leak [C]
│
├── 12. TENANT LIFECYCLE EDGE CASES
│   ├── Onboarding: registry replicated to all consumers before ACTIVE (avoid E2.3)
│   ├── Tenant merge/split: new tenant ids; data moved via governed migration; sagas in flight completed on original tenant
│   ├── Offboarding with in-flight sagas: drain/compensate; refunds owed completed; schedules deleted
│   └── E12.1 Tenant split moves half the sagas mid-flight — replies arrive for old tenant — mismatch quarantines [C]
│
└── 13. FEATURE TEST SCENARIOS
    ├── JWT tenant A + header tenant B → 404/403; no B data
    ├── API key of A used with B's saga id in path → 404
    ├── Kafka message with header tenant ≠ payload tenant → quarantined, alert
    ├── Unknown tenant message during onboarding → quarantined, reprocessed after registration
    ├── Statement executed without transaction → no rows (deny), never all rows
    ├── Pooled connection reused across tenants → no context leakage
    ├── Application role as owner → test fails CI (role must not own tables)
    ├── Insert row with other tenant id → rejected by WITH CHECK
    ├── Reporting view → only caller's tenant rows (invoker rights)
    ├── Cross-tenant id in body → 404 + security event; pod keeps running
    ├── 20 concurrent saga creations at quota 10 → exactly 10 created
    ├── Suspended (billing) tenant → new commands rejected; in-flight refunds complete
    ├── Webhook for A never sent to B's endpoint; internal-address webhook rejected at config
    ├── Search aggregation from A → no B counts
    ├── Worker lane alternating tenants → no context bleed
    └── Offboarded tenant id never reissued
```

[Certain] The core corrections follow from documented PostgreSQL behavior and common isolation failures:
- `SET LOCAL` has no effect outside a transaction, and session settings persist on pooled connections.
- Table owners bypass RLS unless it's forced.
- `WITH CHECK` is required to constrain inserts and updates.
- Views run with owner rights by default before PostgreSQL 15's invoker option.
- Materialized views aren't covered by RLS.
- Client headers are forgeable, and check-then-act quotas race.

[Likely] Cache TTLs for tenant status and timing-mitigation measures are defaults.

[Guessing] Suspension semantics by reason, limiter failure policy, and tenant merge and split handling are business and security decisions.

Two decisions are yours:
1. **Remove `X-Tenant-ID` as a tenant source (TI-01).** Derive tenant only from credentials and verified memberships. This is the cheapest fix and closes the most direct cross-tenant path.
2. **Harden RLS with all four controls (§5.1):** forced RLS, non-owner application roles, `WITH CHECK` policies, and transaction-scoped tenant context that denies on absence. Add explicit tenant predicates and tenant-scoped foreign keys as the second layer.
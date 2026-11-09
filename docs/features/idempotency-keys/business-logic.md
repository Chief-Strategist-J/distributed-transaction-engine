# Idempotency Keys — Business Logic Decision Tree

```

IDEMPOTENCY KEYS — BUSINESS LOGIC DECISION TREE (v2)
Legend: [C] Certain   [L] Likely (sound default)   [G] Guessing (business/API owner must decide)
│
├── 0. MEANING & SCOPE
│   ├── A key means: "one logical operation intended by this caller" — every retry of that intent reuses the key;
│   │   a new intent always uses a new key
│   ├── Applies to: state-changing operations (create payment, start saga, submit order, refund)
│   │   Not to: reads (side-effect free), health checks
│   ├── Outcome contract: same key + same request → same outcome (success OR business rejection), returned again
│   └── E0.1 Key treated as a lock only (not an outcome record) — retries after completion re-execute [C]
│
├── 1. INVARIANTS
│   ├── IK-01 The authoritative idempotency record commits in the SAME transaction as the operation's effect [C]
│   ├── IK-02 Uniqueness is enforced by the database on (scope, key) — not by a cache or a lock with TTL [C]
│   ├── IK-03 Scope = (tenant, principal/API client, operation) [C]
│   ├── IK-04 A key is released only when the operation is KNOWN not to have executed [C]
│   ├── IK-05 Fingerprint covers semantic request content in canonical form [C]
│   ├── IK-06 Final outcomes — success and business rejection — are stored and replayed [C]
│   ├── IK-07 Every replay is re-authorized for the current caller [C]
│   ├── IK-08 Retention ≥ the longest client/partner retry horizon for that operation [C]
│   └── IK-09 Downstream effects receive deterministic keys derived from the request key [C]
│
├── 2. ADMISSION & VALIDATION
│   ├── Check 2.1: Operation requires a key?
│   │   ├── YES and header missing
│   │   │   ├── Public/mobile/partner API → 400 (missing key) [C]
│   │   │   └── Partner webhooks/integrations that cannot send keys → server-derived key from a natural
│   │   │       business identifier (partner event id, partner order reference) [G: per integration]
│   │   └── NO → normal processing
│   │
│   ├── Check 2.2: Key format
│   │   ├── Length 1–128, allowed characters, high entropy expected (UUID recommended, not mandated) [L]
│   │   └── Invalid → 400 (no record created)
│   │
│   ├── Check 2.3: Scope (IK-03)
│   │   ├── tenant id (from authenticated context, never from body/header)
│   │   ├── principal id or API client id (who made the request)
│   │   └── operation id (stable name, e.g., "payments.create" — not raw URL path)
│   │
│   ├── Check 2.4: Fingerprint (IK-05)
│   │   ├── Include: operation id, API version, path parameters, semantically relevant query parameters,
│   │   │   semantically relevant headers (currency, locale if it changes behaviour), canonical body
│   │   ├── Canonical body: parsed → normalized (sorted keys, normalized numbers, trimmed whitespace,
│   │   │   normalized unicode) → serialized deterministically → hashed
│   │   ├── Exclude: client timestamps, request ids, tracing headers, SDK nonces, auth tokens
│   │   └── Edge cases
│   │       ├── E2.4a Raw-bytes hash — retry with reordered JSON keys → false 422 — client stuck [C]
│   │       ├── E2.4b Fingerprint without query/version — different requests match → wrong replay [C]
│   │       └── E2.4c Body contains client-generated timestamp → every retry "differs" → 422 [C]
│   │           └── Handling: excluded fields declared per operation
│   │
│   └── Edge cases
│       ├── E2.1 Scope by tenant only — two users collide; user B gets user A's response — SECURITY [C]
│       └── E2.2 Key accepted on read endpoints — record table fills with noise; no benefit [C]
│
├── 3. AUTHORITATIVE RECORD  (PostgreSQL, same database as the operation's effect)
│   ├── Unique: (tenant, principal/client, operation, key)
│   ├── Fields: fingerprint, state, resource reference (e.g., saga id / payment id), outcome class,
│   │   stored response (status, selected headers, body or reference), created at, completed at, expires at,
│   │   lease owner + lease expiry (only for §4.3 model)
│   └── States: IN_PROGRESS | COMPLETED | REJECTED_FINAL | UNKNOWN
│
├── 4. EXECUTION DECISION TREE
│   └── Request admitted (key, scope, fingerprint)
│       │
│       ├── Step 4.1: Look up record by (scope, key)
│       │
│       ├── Case A: No record
│       │   ├── Preferred model (single transaction) — operation completes in one DB transaction
│       │   │   ├── Begin tx → insert record (IN_PROGRESS) → perform effect (e.g., create saga row + outbox)
│       │   │   │   → set record COMPLETED with response snapshot + resource reference → commit
│       │   │   ├── Concurrent identical request → its insert waits on the unique entry → after first commits
│       │   │   │   → sees COMPLETED → replay (Case C)
│       │   │   └── Commit failure (known) → nothing persisted → client may retry with same key
│       │   │
│       │   ├── Long/async model (saga accepted, completes later)
│       │   │   ├── Same single transaction creates the saga and the record; response = 202 Accepted + saga id
│       │   │   ├── Record COMPLETED immediately (the operation "start saga" is complete)
│       │   │   └── Replays return the same saga id; client polls saga status separately
│       │   │
│       │   └── External-call model (effect not in this database, e.g., synchronous provider call) [L]
│       │       ├── Tx 1: insert record IN_PROGRESS with lease → commit
│       │       ├── Call provider with derived idempotency key (IK-09)
│       │       ├── Tx 2: record outcome (COMPLETED / REJECTED_FINAL) with response
│       │       └── Crash between → lease expires → recovery queries provider by derived key before any re-execution
│       │
│       ├── Case B: Record IN_PROGRESS
│       │   ├── Fingerprint mismatch → 422 (key reused for different request)
│       │   ├── Lease valid (or single-tx model: other tx still open) → 409 with Retry-After
│       │   │   (or bounded wait for completion [L: ≤ 2–5 s] then replay)
│       │   └── Lease expired (external-call model) → recovery:
│       │       ├── Look up effect by derived key (provider/saga) → found → mark COMPLETED → replay
│       │       ├── Confirmed not executed → re-execute under new lease (same derived key)
│       │       └── Cannot determine → UNKNOWN (Case E)
│       │
│       ├── Case C: Record COMPLETED
│       │   ├── Fingerprint match → re-authorize caller (IK-07) → replay stored response
│       │   └── Fingerprint mismatch → 422
│       │
│       ├── Case D: Record REJECTED_FINAL (business rejection stored)
│       │   ├── Fingerprint match → replay the same rejection (even if conditions changed since) [C]
│       │   └── Fingerprint mismatch → 422
│       │
│       ├── Case E: Record UNKNOWN
│       │   ├── Resolve: query downstream by resource reference / derived key
│       │   │   ├── Executed → COMPLETED → replay
│       │   │   ├── Not executed → re-execute (same derived keys)
│       │   │   └── Still unknown → 409/503 with Retry-After; escalate after budget
│       │   └── Never released to "new" automatically (IK-04)
│       │
│       └── Edge cases
│           ├── E4.1 Redis lock TTL expires during slow execution — retry executes again — double effect [C]
│           ├── E4.2 Redis failover loses lock/record — retry executes again — double effect [C]
│           ├── E4.3 Record persisted after effect in a separate step — crash between — retry duplicates [C]
│           ├── E4.4 Two instances in different regions accept the same key — two effects [C]
│           │   └── Handling: keys handled in the tenant's home region; cross-region requests forwarded
│           └── E4.5 Client sends same key with a different principal (shared credentials or stolen key) — scope prevents cross-user replay; logged [C]
│
├── 5. OUTCOME CLASSIFICATION  (what is stored, what is released)
│   ├── 2xx success → COMPLETED (store response)
│   ├── 202 accepted (saga started) → COMPLETED (store saga id + accepted response)
│   ├── 4xx business rejection (insufficient funds, limit exceeded, item unavailable) → REJECTED_FINAL (store)
│   ├── 400 malformed / schema validation (request never accepted) → no record (or record discarded in same tx)
│   ├── 401/403 authentication/authorization failure → no record (outcome depends on credentials, not intent)
│   ├── 409 business conflict (expected version mismatch) → REJECTED_FINAL [G: per operation; usually store]
│   ├── 429 rate limited before execution → no record (known not executed)
│   ├── 5xx where effect KNOWN not executed (failed before any write, tx rolled back) → no record / released
│   ├── 5xx / timeout where outcome AMBIGUOUS → UNKNOWN (never released)
│   └── Edge cases
│       ├── E5.1 Insufficient-funds rejection not stored — retry later succeeds under same key — conflicting outcomes [C]
│       ├── E5.2 Timeout to saga engine treated as "not executed" — key deleted — second saga — double charge [C]
│       └── E5.3 403 stored — after permission granted, retry with same key still forbidden forever [C]
│
├── 6. INTEGRATION WITH THE SAGA ENGINE
│   ├── Saga start is itself idempotent: saga table unique on (scope, key) — second line of defence
│   ├── Record's resource reference = saga id; replay returns saga id and status link
│   ├── Derived keys (IK-09): saga's step idempotency keys = f(saga id, step); provider keys = f(request key, effect)
│   ├── Request key never reused by the saga for unrelated effects
│   └── Edge cases
│       ├── E6.1 Saga engine generates a new saga per request regardless of key — duplicates if record path fails [C]
│       └── E6.2 Provider receives a fresh random key on each attempt — provider-side dedupe impossible [C]
│
├── 7. FAST-PATH CACHE  (optional)
│   ├── Caches COMPLETED / REJECTED_FINAL records only, written AFTER the database commit
│   ├── Cache key includes full scope (IK-03); value includes fingerprint
│   ├── Cache hit → fingerprint check → re-authorize → replay
│   ├── Cache miss or cache down → database path (never treated as "new" by itself)
│   ├── Never used as a lock or as the source of truth (IK-02)
│   └── Edge cases
│       ├── E7.1 Cache entry with longer TTL than DB record — replay after record expired — inconsistent [C]
│       └── E7.2 Cache keyed without principal — cross-user replay — SECURITY [C]
│
├── 8. RETENTION & EXPIRY  (IK-08)
│   ├── Retention per operation ≥ longest retry horizon:
│   │   mobile offline queue, partner retry schedule, batch resubmission window [G: e.g., 7–30 days for payments]
│   ├── Expired records removed by background job (never during in-progress or UNKNOWN states)
│   ├── Same key after expiry → treated as new operation (documented in API contract)
│   └── Edge cases
│       ├── E8.1 24 h retention, mobile client retries after 2 days offline — duplicate order — CORRUPT [C]
│       └── E8.2 UNKNOWN records expired by cleanup — later retry executes again — double effect [C]
│
├── 9. RESPONSE REPLAY SEMANTICS
│   ├── Replay returns the ORIGINAL outcome (status, body), not current resource state [C]
│   │   └── Resource's current state available via its own GET endpoint
│   ├── Replay marker header (e.g., "idempotent-replayed: true") so clients can distinguish
│   ├── Stored responses: size limit [L]; large bodies stored as reference to resource, rebuilt on replay
│   ├── Sensitive data: store minimal response (ids, statuses); exclude secrets/one-time tokens; PII encrypted
│   └── Edge cases
│       ├── E9.1 Response contained a one-time secret (API key shown once) — replay reveals it again or loses it [C]
│       │   └── Handling: operation-specific policy: replay without secret + instructions, or secret retrievable once
│       └── E9.2 Replay returns current state instead of original — client logic sees a different outcome than first attempt [C]
│
├── 10. SECURITY
│   ├── Re-authorization on replay (IK-07): caller must still be allowed to see the resource
│   ├── Scope prevents cross-principal replay (IK-03)
│   ├── Rate limits on key creation per principal (prevent record-table flooding)
│   ├── Keys are not secrets but must not be predictable across users (scope makes guessing harmless)
│   └── Edge cases
│       ├── E10.1 Revoked user replays a stored response containing customer data — SECURITY [C]
│       └── E10.2 Attacker floods unique keys to grow storage — DEGRADE [C]
│
├── 11. CLIENT CONTRACT  (documented and enforced in SDKs)
│   ├── Generate key once per user intent (e.g., per "Pay" button press), persist it with the pending request
│   ├── Retry with the SAME key on network errors, timeouts, 409 (in progress), 5xx
│   ├── Do NOT retry with the same key after changing the request (new intent → new key)
│   ├── On 422 (key reused with different payload) → client bug; do not auto-retry
│   └── Edge cases
│       ├── E11.1 Client generates a new key on each retry — duplicates — CORRUPT [C]
│       ├── E11.2 Client reuses one key for all requests of a session — second operation replays first — LOSS [C]
│       └── E11.3 Client retries 409 in a tight loop — load — Retry-After honoured by SDK [C]
│
└── 12. FEATURE TEST SCENARIOS
    ├── Same key + same request twice → one saga; second gets replayed response with replay marker
    ├── Same key + JSON with reordered fields → treated as same request (canonical fingerprint)
    ├── Same key + different amount → 422
    ├── Concurrent identical requests on two instances → one effect; other waits or gets 409 then replay
    ├── Saga engine times out after committing → record UNKNOWN/COMPLETED via lookup; retry does not create second saga
    ├── Failure before any write (validation, 429) → no record; retry executes normally
    ├── Insufficient funds → rejection stored; retry after top-up still returns the rejection
    ├── User B uses user A's key → treated as a different scope; no access to A's response
    ├── Replay after user's access revoked → 403
    ├── Mobile retry after 3 days within retention → replay; after retention → new operation (documented)
    ├── Cache outage → database path; correctness unchanged
    ├── External provider call crash after send → lease expiry → provider lookup by derived key → no double charge
    └── Cleanup job skips IN_PROGRESS and UNKNOWN records
```

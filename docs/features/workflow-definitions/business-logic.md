# Workflow Definition Registry & Validation — Business Logic Decision Tree v3 (Production)

> **Legend**: `──>` outcome · `[REJECT: HTTP — CODE]` validation halt · `◆` real-world failure mode · `⚠` explicit architectural policy choice

---

## 1. The Production Registration Decision Tree

```
WORKFLOW DEFINITION REGISTRY — DECISION TREE v3 (PRODUCTION)
│
├── 0. INGRESS & TRANSPORT TIER
│   └── Trigger: POST /v1/tenants/{tenant_id}/workflows[?dry_run=true]
│       │
│       ├── 0.1 Registry availability
│       │   ├── DB primary unreachable / failover / registry read-only
│       │   │   └── [REJECT: 503 — E_REGISTRY_UNAVAILABLE + Retry-After]
│       │   └── ◆ Postgres failover takes 30–60s; CI must retry, not fail the pipeline
│       │
│       ├── 0.2 Content-Type ∈ {application/json, application/yaml}
│       │   └── NO ──> [REJECT: 415 — E_UNSUPPORTED_MEDIA_TYPE]
│       │
│       ├── 0.3 Encoding
│       │   ├── Content-Encoding gzip/br ──> cap DECOMPRESSED size, abort stream past cap
│       │   │   └── ◆ 10 KB gzip expands to 10 GB of zeros (decompression bomb)
│       │   ├── Not valid UTF-8 / contains NUL ──> [REJECT: 400 — E_INVALID_ENCODING]
│       │   └── Leading UTF-8 BOM ──> strip once OR reject ⚠ (some JSON parsers choke)
│       │
│       ├── 0.4 Size
│       │   ├── Content-Length > MAX (e.g. 1 MiB) ──> [REJECT: 413 — E_PAYLOAD_TOO_LARGE]
│       │   ├── Chunked / no Content-Length ──> streaming byte counter
│       │   └── Enforced at BOTH edge proxy AND service (proxy config drifts)
│       │
│       ├── 0.5 Authentication
│       │   ├── Missing / malformed / expired token ──> [REJECT: 401 — E_UNAUTHENTICATED]
│       │   ├── Wrong audience (token minted for another service) ──> 401
│       │   ├── Clock-skew tolerance ≤ 60s
│       │   └── Client-sent X-Tenant-Id / X-User-Id / X-Forwarded-User ──> STRIPPED at edge
│       │       └── ◆ Internal service trusts the header → any caller impersonates any tenant
│       │
│       ├── 0.6 Tenant resolution
│       │   ├── Path tenant_id MUST ∈ token tenant claims
│       │   ├── Body tenant_id present AND ≠ path ──> [REJECT: 403 — E_TENANT_MISMATCH]
│       │   ├── Tenant not visible to principal ──> [REJECT: 404 — E_NOT_FOUND]  (no enumeration)
│       │   ├── Tenant SUSPENDED / DELETING / MIGRATING ──> [REJECT: 423 — E_TENANT_LOCKED]
│       │   └── ◆ Tenant mid-migration between shards: write lands on old shard, silently lost
│       │
│       ├── 0.7 Authorization
│       │   ├── Lacks `workflow:write` ──> [REJECT: 403 — E_FORBIDDEN]
│       │   ├── dry_run still requires `workflow:write` or `workflow:validate`
│       │   │   └── ◆ Open dry-run = free oracle for probing which actions/secrets exist
│       │   └── Service accounts scoped to a workflow_id prefix ⚠
│       │
│       ├── 0.8 Rate & quota
│       │   ├── Per-tenant + per-principal rate ──> [REJECT: 429 — E_RATE_LIMITED + Retry-After]
│       │   ├── Workflows per tenant ≥ MAX ──> [REJECT: 403 — E_QUOTA_WORKFLOWS]
│       │   ├── Versions per workflow ≥ MAX ──> [REJECT: 403 — E_QUOTA_VERSIONS]
│       │   └── ◆ CI bug registers a version per commit → 40k versions in a month
│       │
│       ├── 0.9 Idempotency-Key header
│       │   ├── Same key + same body ──> replay stored response
│       │   ├── Same key + different body ──> [REJECT: 422 — E_IDEMPOTENCY_KEY_REUSED]
│       │   └── Key TTL 24h, scoped to (tenant, principal)
│       │
│       └── State: AUTHORIZED_FOR_PARSE
│
├── 1. SAFE PARSING TIER
│   └── State: AUTHORIZED_FOR_PARSE
│       │
│       ├── 1.1 Top-level shape
│       │   ├── Empty / whitespace / null / scalar / array ──> [REJECT: 400 — E_PARSE_ROOT]
│       │   └── Multi-document YAML stream ──> [REJECT: 400 — E_PARSE_MULTIDOC]
│       │
│       ├── 1.2 JSON-specific
│       │   ├── NaN / Infinity ──> reject (Python's json accepts them by default)
│       │   ├── Trailing commas / comments (JSON5) ──> reject
│       │   ├── Integers > 2^53 ──> reject or require string (JS clients corrupt silently)
│       │   └── Duplicate keys at ANY depth ──> [REJECT: 400 — E_DUPLICATE_KEY]
│       │       └── ◆ "compensation_action":"refund" … "compensation_action":null → last one wins
│       │
│       ├── 1.3 YAML-specific
│       │   ├── Safe loader; explicit tags (!!python/object, !Ref, custom) ──> reject
│       │   ├── Anchors/aliases disabled ⚠ (or expansion cap ≤ 10× input)
│       │   │   └── ◆ "Billion laughs": 1 KB YAML OOM-kills the validator pod
│       │   ├── Merge keys `<<:` ──> reject (non-obvious override semantics)
│       │   ├── YAML 1.2 core schema — NO 1.1 implicit typing:
│       │   │   ├── ◆ `country: NO` → false              (Norway problem)
│       │   │   ├── ◆ `version: 1.10` → float 1.1        (collides with 1.1)
│       │   │   ├── ◆ `mode: 0755` → int 493             (octal)
│       │   │   ├── ◆ `run_at: 2026-01-01` → date object (breaks serialization)
│       │   │   └── ◆ `owner: ~` → null
│       │   ├── Non-string keys (`1: x`, `true: y`) ──> reject
│       │   └── Tabs in indentation ──> reject with line:col
│       │
│       ├── 1.4 Structural limits
│       │   ├── Nesting depth > 32 ──> [REJECT: 400 — E_TOO_DEEP]
│       │   ├── Any string > 64 KiB ──> reject
│       │   └── Any array > 10k elements ──> reject
│       │
│       ├── 1.5 Character hygiene (identifiers AND human-readable fields)
│       │   ├── Control chars (except \n \t in descriptions) ──> reject
│       │   ├── Bidi overrides U+202A–202E, U+2066–2069 ──> reject
│       │   │   └── ◆ "Trojan Source": what reviewers see ≠ what executes
│       │   └── Zero-width chars (U+200B, mid-string U+FEFF) ──> reject
│       │
│       ├── 1.6 spec_version
│       │   ├── Missing ──> [REJECT: 400 — E_SPEC_VERSION_MISSING]   (never default it)
│       │   ├── Unknown ──> [REJECT: 400 — E_SPEC_VERSION_UNSUPPORTED]
│       │   └── Sunset ──> [REJECT: 400 — E_SPEC_VERSION_SUNSET + migration link]
│       │
│       ├── 1.7 JSONSchema 2020-12 (ruleset selected by spec_version)
│       │   ├── additionalProperties: false at EVERY level
│       │   │   └── ◆ `retry_polcy:` typo ignored → default policy on a payment step
│       │   ├── Strict types, no coercion ("300" ≠ 300, "true" ≠ true)
│       │   ├── null vs absent: schema states which fields accept null
│       │   └── Errors: collect ALL, sort by JSON pointer, cap 100, deterministic
│       │       ├── Never echo raw values of secret-ish fields in error messages
│       │       └── [REJECT: 400 — E_SCHEMA_INVALID, errors[]: {pointer, code, message}]
│       │
│       └── State: SCHEMA_VALIDATED
│
├── 2. FIELD SEMANTICS TIER
│   └── State: SCHEMA_VALIDATED
│       │
│       ├── 2.1 Identifiers (workflow_id, step_id)
│       │   ├── ASCII only ^[a-z][a-z0-9-]{2,62}$
│       │   │   └── ◆ Latin "approve" vs Cyrillic "аpprove" — visually identical, different steps
│       │   ├── Lowercase only (no `Order-Flow` vs `order-flow` dupes)
│       │   ├── Reserved (system, admin, internal, start, end, root, _*) ──> [REJECT: 422 — E_RESERVED_ID]
│       │   ├── step_id unique ──> [REJECT: 422 — E_DUPLICATE_STEP_ID]
│       │   └── workflow_id ≠ any step_id ⚠ (ambiguous expression refs)
│       │
│       ├── 2.2 version
│       │   ├── String, strict SemVer 2.0, no `v` prefix, no leading zeros, ≤ 64 chars
│       │   ├── Build metadata `+sha` ──> reject (equal precedence → silent collision)
│       │   ├── Prerelease `-rc.1` ──> registrable, BLOCKED from prod activation ⚠
│       │   └── Lower than latest ──> allowed (hotfix line), never auto-activated
│       │
│       ├── 2.3 Timeouts
│       │   ├── Workflow timeout_seconds: int, 1 ≤ t ≤ 2,592,000 (30d)
│       │   ├── Step timeout REQUIRED, 1 ≤ t ≤ workflow timeout
│       │   │   └── NO ──> [REJECT: 422 — E_STEP_TIMEOUT_EXCEEDS_WORKFLOW]
│       │   └── ◆ No step timeout → hung downstream holds the saga for days
│       │
│       ├── 2.4 retry_policy (workflow default + per-step override)
│       │   ├── max_attempts 1..20, INCLUDES first attempt (document it — off-by-one magnet)
│       │   ├── initial_backoff_ms > 0, multiplier 1.0..10.0, max_backoff_ms REQUIRED
│       │   ├── jitter REQUIRED
│       │   │   └── ◆ 50k sagas retry in lockstep when downstream recovers → re-kills it
│       │   ├── retryable_errors: explicit allowlist; never retry 4xx except 408/429
│       │   ├── Retry-on-timeout for step WITHOUT idempotency key ──> [REJECT: 422 — E_UNSAFE_RETRY]
│       │   │   └── ◆ Charge times out, retried, customer charged twice
│       │   └── Honor downstream Retry-After ⚠
│       │
│       ├── 2.5 Action references (action, compensation_action, reconcile_action)
│       │   ├── Version-pinned: `inventory.reserve@3` ✔   `@latest` / unpinned ✘
│       │   │   └── ◆ Action owner ships breaking v4 → every @latest workflow breaks at once
│       │   ├── Not in Action Registry ──> [REJECT: 422 — E_UNKNOWN_ACTION]
│       │   ├── DEPRECATED ──> WARN;  DISABLED ──> [REJECT: 422 — E_ACTION_DISABLED]
│       │   ├── Principal lacks `action:invoke` ──> [REJECT: 403 — E_ACTION_FORBIDDEN]
│       │   │   └── ◆ Confused deputy: tenant A wires tenantB.export_customers; orchestrator runs it as system
│       │   └── Action scope ∈ {same tenant, explicitly shared, platform-global}
│       │
│       ├── 2.6 Input mappings & expressions
│       │   ├── Sandboxed language only (CEL / JSONata); no eval, no code templates
│       │   ├── Allowed roots: input, steps.<id>.output, workflow.meta — NOT env, NOT raw secrets
│       │   ├── Expression ≤ 4 KiB, AST nodes ≤ N, no unbounded iteration
│       │   ├── Static type check vs action input schema ──> [REJECT: 422 — E_TYPE_MISMATCH]
│       │   │   └── ◆ amount "10.00" (string) into action expecting integer cents
│       │   └── Referenced field ∉ source step OUTPUT schema ──> [REJECT: 422 — E_UNKNOWN_FIELD]
│       │
│       ├── 2.7 Secrets
│       │   ├── Reference only `secret://<tenant>/<name>`; tenant segment == request tenant
│       │   ├── `..`, `%2e%2e`, `//` ──> [REJECT: 422 — E_SECRET_REF_INVALID]
│       │   ├── Must exist at registration (value may rotate later)
│       │   └── Inline credentials (AKIA…, sk_live_…, -----BEGIN) ──> reject  [Likely: catches most, not all]
│       │
│       ├── 2.8 Outbound URLs (HTTP / webhook steps)
│       │   ├── https only; no userinfo (https://user:pass@host)
│       │   ├── Resolves to private / loopback / link-local / 169.254.169.254 ──> reject
│       │   ├── Re-check at CALL time (DNS rebinding defeats registration-time check)
│       │   └── ◆ SSRF to cloud metadata → node IAM credentials exfiltrated
│       │
│       ├── 2.9 Concurrency & fan-out
│       │   ├── max_parallelism ≥ 1 (0 = deadlock)
│       │   └── Dynamic map step: max_items REQUIRED, ≤ platform cap
│       │       └── ◆ Map over customer list → 2M child steps from one saga
│       │
│       └── State: FIELDS_VERIFIED
│
├── 3. GRAPH INTEGRITY TIER
│   └── State: FIELDS_VERIFIED
│       │
│       ├── 3.1 Build G from depends_on (normalize: absent = null = []; dedupe; sort)
│       │
│       ├── 3.2 References
│       │   ├── depends_on → missing step ──> [REJECT: 422 — E_BROKEN_DEPENDENCY]
│       │   │   └── Include "did you mean …" (edit distance ≤ 2)
│       │   └── Self-dependency ──> [REJECT: 422 — E_SELF_DEPENDENCY]
│       │
│       ├── 3.3 Acyclicity — ITERATIVE Kahn's (no recursion)
│       │   ├── Zero roots ──> [REJECT: 422 — E_NO_ENTRY_STEP]  (implies cycle; clearer message)
│       │   ├── Leftover nodes ──> iterative Tarjan; report SCCs (cap 10, path ≤ 20)
│       │   │   └── [REJECT: 422 — E_CYCLE, cycles: [[a,b,c,a], …]]
│       │   └── ◆ Recursive DFS on 5,000-step generated chain → stack overflow → crash loop
│       │
│       ├── 3.4 Connectivity — DECIDED: REJECT
│       │   ├── Count WEAKLY connected components (ignore direction)
│       │   ├── Multiple roots in ONE component ──> VALID (parallel start)
│       │   ├── Multiple sinks ──> VALID
│       │   ├── Components > 1 ──> [REJECT: 422 — E_DISCONNECTED_GRAPH, components: [[…],[…]]]
│       │   │   ├── Fix A: add an explicit join step
│       │   │   └── Fix B: split into two workflows
│       │   └── ◆ Copy-paste leftover `legacy-notify` (no edges) fires on every saga forever
│       │
│       ├── 3.5 Size & shape limits
│       │   ├── steps ≤ 500, edges ≤ 5,000, longest path ≤ 100, fan-out ≤ 50, fan-in ≤ 50
│       │   └── Exceeded ──> [REJECT: 422 — E_GRAPH_LIMIT {limit, actual, max}]
│       │
│       ├── 3.6 Redundant edges (A→C while A→B→C)
│       │   └── VALID + WARN (never auto-remove: changes the hash)
│       │
│       ├── 3.7 Data-flow safety
│       │   ├── steps.X.output used in S requires X ∈ ancestors(S)
│       │   │   ├── NO ──> [REJECT: 422 — E_NON_ANCESTOR_REFERENCE]
│       │   │   └── ◆ C reads B, depends only on A. Green in tests (B is fast). Null in prod under load.
│       │   └── Ref to a step on a skippable branch must be null-safe
│       │       └── NO ──> [REJECT: 422 — E_SKIPPABLE_REFERENCE]
│       │
│       ├── 3.8 Conditional branches (if `when:` is supported)
│       │   ├── Fan-in join policy REQUIRED: all_success | all_done | any_success
│       │   ├── SKIPPED propagates to descendants unless the join policy absorbs it
│       │   │   └── ◆ Join waits on a skipped branch → saga hangs until workflow timeout
│       │   ├── Conditions reference ancestors only
│       │   └── Statically always-false condition (dead step) ──> WARN
│       │
│       ├── 3.9 Timeout feasibility
│       │   ├── Critical path = Σ(step_timeout × attempts + Σ capped backoff); waits at max_wait
│       │   └── > workflow timeout ──> [REJECT: 422 — E_TIMEOUT_INFEASIBLE] (per-tenant override)
│       │
│       └── State: GRAPH_VERIFIED
│
├── 4. SAGA SEMANTICS TIER
│   └── State: GRAPH_VERIFIED
│       │
│       ├── 4.1 Every step exactly one class: COMPENSABLE | PIVOT | RETRIABLE
│       │   └── Missing / ambiguous ──> [REJECT: 422 — E_STEP_CLASS]
│       │
│       ├── 4.2 Pivot rules
│       │   ├── Pivots ≤ 1 ──> NO ──> [REJECT: 422 — E_MULTIPLE_PIVOTS]
│       │   ├── ancestors(P):   COMPENSABLE or RETRIABLE
│       │   ├── descendants(P): RETRIABLE only
│       │   ├── concurrent(P):  RETRIABLE only ──> NO ──> [REJECT: 422 — E_PIVOT_CONCURRENCY]
│       │   │   └── ◆ charge_card ∥ reserve_inventory: charged, inventory fails, saga can't refund
│       │   ├── Pivot on conditional branch ──> [REJECT: 422] ⚠
│       │   └── Pivot MUST have: provider idempotency key  OR  reconcile_action
│       │       ├── NEITHER ──> [REJECT: 422 — E_PIVOT_UNSAFE]
│       │       └── ◆ Gateway 504: retry = double charge; compensate = refund a charge that may not exist
│       │
│       ├── 4.3 Step outcome contract (what the orchestrator must report)
│       │   ├── SUCCEEDED ──────────────> compensate on rollback
│       │   ├── UNKNOWN (timeout/lost) ─> reconcile first, else compensate
│       │   ├── FAILED_PERMANENT (no effect) ──> no compensation
│       │   └── FAILED_PARTIAL (effect partly applied) ──> compensate
│       │
│       ├── 4.4 Compensation actions
│       │   ├── Idempotent: declared AND confirmed in Action Registry metadata
│       │   ├── Safe no-op when forward effect is absent
│       │   │   └── ◆ refund(order) for a charge that never happened must return OK, not 500 forever
│       │   ├── Inputs: forward step's I/O + its ancestors' outputs; forward output MAY be null
│       │   ├── Itself RETRIABLE; no compensation-of-compensation
│       │   ├── Own timeout + retry + time budget OUTSIDE workflow timeout
│       │   │   └── ◆ Workflow times out at 30m → compensation still has to run
│       │   ├── Exhausted ──> DLQ + page on-call; saga = COMPENSATION_FAILED (distinct from FAILED)
│       │   └── Order: reverse topological; parallel branches compensate in parallel;
│       │       X compensates only after ALL descendants of X are compensated
│       │
│       ├── 4.5 Irreversible-effect steps (email, SMS, push, 3rd-party webhooks)
│       │   ├── MUST be RETRIABLE and AFTER the pivot ⚠
│       │   └── ◆ "Order confirmed" email sent, inventory then fails → customer told yes, then no
│       │
│       ├── 4.6 RETRIABLE steps
│       │   ├── Idempotency key derived from (saga_id, step_id) — never random per attempt
│       │   └── Permanent error ──> DLQ + human (NOT compensation — it's past the point of no return)
│       │
│       ├── 4.7 Long waits / human approval
│       │   ├── max_wait REQUIRED
│       │   ├── on_expiry REQUIRED: compensate | continue_default | escalate ⚠
│       │   └── ◆ Approval waits 6 months; the approver left the company
│       │
│       ├── 4.8 All-RETRIABLE workflow ──> VALID + WARN (only recourse on failure is DLQ)
│       │
│       └── State: SAGA_VERIFIED
│
├── 5. CANONICALIZATION & IMMUTABLE REGISTRATION TIER
│   └── State: SAGA_VERIFIED
│       │
│       ├── 5.0 dry_run=true ──> 200 + warnings + would-be hash; NO writes, NO outbox
│       │
│       ├── 5.1 Canonical form — semantically equal ⇒ byte-equal
│       │   ├── YAML vs JSON of same content ──> same hash
│       │   ├── steps[] sorted by step_id (array order isn't semantic)
│       │   │   └── ◆ Engineer reorders steps for readability → 409 on an identical workflow
│       │   ├── depends_on sorted, deduped, absent/null/[] → []
│       │   ├── Numbers canonical per RFC 8785 (1.0 → 1, 1e3 → 1000)
│       │   ├── Strings NFC (precomposed é vs e + combining accent)
│       │   ├── Comments, whitespace, key order dropped
│       │   └── Defaults NOT applied (hash = user intent; code defaults change)
│       │
│       ├── 5.2 hash = SHA-256(spec_version ‖ canonicalizer_version ‖ canonical_bytes)
│       │   └── Canonicalizer bug fix ⇒ bump canonicalizer_version; old hashes remain valid
│       │
│       ├── 5.3 Stored artifacts
│       │   ├── raw_document  — exact submitted bytes (audit)
│       │   ├── canonical_spec — hashed form
│       │   ├── resolved_spec  — defaults applied (what EXECUTES)
│       │   └── ◆ JSONB reorders keys & drops dupes → can't prove what was submitted without raw
│       │
│       ├── 5.4 SINGLE TRANSACTION
│       │   ├── Quota re-check INSIDE txn (SELECT … FOR UPDATE on tenant counter)
│       │   │   └── ◆ 20 parallel CI jobs pass the quota check outside txn → quota exceeded by 19
│       │   ├── INSERT … ON CONFLICT (tenant_id, workflow_id, version) DO NOTHING RETURNING
│       │   ├── Inserted ──────────────────> 201 Created
│       │   ├── Conflict, same hash ───────> 200 OK (idempotent replay)
│       │   ├── Conflict, different hash ──> [REJECT: 409 — E_VERSION_IMMUTABLE {existing_hash}]
│       │   ├── Tombstoned version ────────> [REJECT: 409 — E_VERSION_RETIRED]
│       │   ├── INSERT audit_log (principal, tenant, hash, request_id, source_ip, user_agent)
│       │   └── INSERT outbox (definition_registered)
│       │
│       ├── 5.5 Commit succeeded, response lost (pod killed / LB timeout)
│       │   └── Client retries ──> 200 via same-hash path. Correct by design, no special code.
│       │
│       ├── 5.6 Cache propagation (outbox relay: at-least-once, may reorder)
│       │   ├── Key (tenant, workflow_id, version, hash) — immutable ⇒ order-insensitive
│       │   ├── Duplicate events ──> idempotent set
│       │   ├── Redis down ──> relay retries; readers fall through to Postgres
│       │   └── Cold start ──> single-flight loading (no stampede on Postgres)
│       │
│       ├── 5.7 Multi-region
│       │   ├── Writes only in tenant's home region
│       │   └── Activate right after register ──> read from PRIMARY
│       │       └── ◆ Replica lag 2s → activate returns 404 for a version that exists
│       │
│       └── State: REGISTERED (not executable)
│
├── 6. ACTIVATION TIER
│   └── State: REGISTERED
│       │
│       ├── 6.1 POST …/versions/{v}:activate {expected_current, mode, reason}
│       │   ├── Requires `workflow:activate`
│       │   └── Two-person rule in prod: registrant ≠ activator ⚠
│       │
│       ├── 6.2 Re-validate AT ACTIVATION (world changed since registration)
│       │   ├── Action now DISABLED ──> [REJECT: 422 — E_ACTION_DISABLED]
│       │   ├── Secret deleted ──> [REJECT: 422 — E_SECRET_MISSING]
│       │   ├── Principal lost `action:invoke` ──> [REJECT: 403]
│       │   ├── spec_version sunset ──> [REJECT: 422]
│       │   └── Prerelease in prod tenant ──> [REJECT: 422 — E_PRERELEASE_IN_PROD]
│       │
│       ├── 6.3 Pointer: compare-and-swap, generation++
│       │   ├── expected_current mismatch ──> [REJECT: 409 — E_ACTIVE_VERSION_CHANGED]
│       │   ├── Already active ──> 200 no-op
│       │   └── Target < current ──> requires rollback=true + reason (audited)
│       │
│       ├── 6.4 Rollout modes
│       │   ├── full
│       │   ├── canary(%) ──> sticky on BUSINESS key (order_id), not saga_id
│       │   │   └── ◆ Client retries same order, hits v1 then v2 → two sagas for one order
│       │   └── Max pointer propagation staleness documented (e.g. ≤ 5s)
│       │
│       ├── 6.5 Instantiation contract
│       │   ├── Resolve active pointer ONCE at saga start; pin (version, hash)
│       │   ├── In-flight sagas never switch versions, even on rollback
│       │   └── Cache hash ≠ Postgres hash ──> trust Postgres, evict, alert
│       │
│       ├── 6.6 Kill switch :deactivate
│       │   ├── New instantiations ──> [REJECT: 409 — E_WORKFLOW_INACTIVE]
│       │   └── In-flight: continue (default) | pause ⚠ — NEVER mass auto-compensate
│       │
│       ├── 6.7 Retirement / deletion
│       │   ├── In-flight > 0 ──> [REJECT: 409 — E_IN_FLIGHT_INSTANCES]
│       │   ├── Retention not elapsed ──> [REJECT: 409]
│       │   └── Leave tombstone; version number burned forever
│       │
│       └── State: ACTIVE ──> [TERMINAL: Ready for Saga Instantiation]
│
└── X. CROSS-CUTTING
    ├── X.1 Action Registry coupling
    │   └── Action version referenced by ANY version with in-flight sagas is undeletable
    │       └── ◆ Team deletes refund@2 → 3,000 in-flight sagas can no longer compensate
    ├── X.2 Validation wall-clock budget (e.g. 5s) ──> [REJECT: 422 — E_VALIDATION_TIMEOUT]
    ├── X.3 Determinism: same input ⇒ same errors, warnings, hash on every pod
    ├── X.4 Ruleset versioned by spec_version; tightened rules apply to NEW registrations only
    ├── X.5 Error contract {code, message, pointer, docs_url}; codes stable forever
    ├── X.6 Metrics: outcomes by code, validation p99, cache hit rate, outbox lag
    └── X.7 Audit retention ≥ longest saga lifetime + compliance window
```

---

## 2. Invariants (I1–I16)

- **I1: Directed Acyclic Graph** [Certain] A workflow graph must be mathematically acyclic (`len(topological_order) == len(steps)` via Kahn's BFS).
- **I2: Richardson Compensability** [Certain] Every pre-pivot non-pivot step must declare a non-empty `compensation_action` and `compensation_endpoint`.
- **I3: Single Pivot Bound** [Certain] A workflow definition can declare at most one step marked `is_pivot = true`.
- **I4: Spec Immutability** [Certain] Any modification under an existing `(tenant_id, name, version)` triggers an immutable conflict rejection.
- **I5: Five Mandatory Identifier Roles** [Certain] Strict separation of `id` (UUIDv7), `public_id` (`wfd_...`), `tenant_id`, `source + external_id`, and `correlation_id`.
- **I6: Closed Schema** [Certain] Undeclared attributes at any level trigger `additionalProperties: false` rejections.
- **I7: Weak Graph Connectivity** [Certain] More than one weakly connected component is rejected (`E_DISCONNECTED_GRAPH`). Multiple roots inside one connected component are valid parallel starts.
- **I8: Monotonic Timeouts** [Certain] Step timeouts must be explicitly declared and cannot exceed the total workflow timeout.
- **I9: Zero Unsafe Retries** [Certain] Steps configured with retry-on-timeout without an idempotency key or provider reconciliation action are rejected (`E_UNSAFE_RETRY`).
- **I10: Ancestor Data Flow Lineage** [Certain] A step can only read output mappings from steps in its explicit `ancestors(S)`.
- **I11: Action Registry Coupling** [Certain] Any action version referenced by a workflow version with in-flight sagas cannot be deleted from the Action Registry.
- **I12: RFC 8785 Canonical Hashing** [Certain] Two semantically equal definitions produce the exact same SHA-256 hash regardless of formatting, step order, or YAML/JSON syntax.
- **I13: Independent Compensation Budget** [Certain] Compensation actions carry an independent timeout and retry budget outside the expired forward workflow timeout.
- **I14: Monotonic Generation Activation** [Certain] The active-version pointer generation only ever increments under CAS concurrency control.
- **I15: Strict Tenant Partitioning** [Certain] `tenant_id` is part of every primary key, index, cache key, secret path, and outbox event.
- **I16: Pivot Concurrency & Safety** [Certain] Steps running concurrently with the pivot must be strictly retriable without side-effects. The pivot must carry an idempotency key or `reconcile_action`.

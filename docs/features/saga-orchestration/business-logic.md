# Saga Orchestration Runtime — Business Logic Decision Tree v4 (Production)

> **Legend**: `──>` outcome · `[HALT: CODE]` terminal condition · `[TERMINAL: STATE]` terminal state · `◆` real-world failure mode · `⚠` explicit architectural policy choice

---

## 1. The Production Saga Execution Decision Tree v4

```
SAGA RUNTIME — EXECUTION & COMPENSATION DECISION TREE v4 (PRODUCTION)
Legend: ──> outcome  [HALT: HTTP — CODE]  [TERMINAL: STATE]  ◆ real failure  ⚠ policy
│
├── 0. INGRESS & IDEMPOTENT INSTANTIATION TIER
│   └── Trigger: POST /v1/tenants/{tenant_id}/sagas {workflow_id, external_id, input, version?}
│       │
│       ├── 0.1 AuthN / AuthZ / tenant  (same rules as registry Tier 0)
│       │   ├── Body tenant ≠ path/token tenant ──> [HALT: 403 — E_TENANT_MISMATCH]
│       │   ├── Lacks `saga:start` on workflow ──> [HALT: 403 — E_FORBIDDEN]
│       │   └── Tenant SUSPENDED / DELETING ──> [HALT: 423 — E_TENANT_LOCKED]
│       │
│       ├── 0.2 Request validation
│       │   ├── Missing workflow_id / external_id ──> [HALT: 400 — E_INVALID_INPUT]
│       │   ├── external_id ≤ 256 chars, printable ASCII ──> else 400
│       │   ├── Body ≤ MAX_INPUT (e.g. 256 KiB) ──> [HALT: 413 — E_PAYLOAD_TOO_LARGE]
│       │   └── input vs workflow INPUT schema ──> [HALT: 422 — E_INPUT_SCHEMA]
│       │       └── ◆ Saga starts without customer_id; step 2 charges, step 4 crashes on null
│       │
│       ├── 0.3 Version resolution
│       │   ├── version omitted (default) ──> ACTIVE pointer; canary sticky on external_id
│       │   ├── version given ──> requires `saga:start_pinned` ⚠ (tests / replays only)
│       │   │   └── ◆ Client hard-codes v3; your rollback to v2 never reaches them
│       │   ├── Workflow deactivated ──> [HALT: 409 — E_WORKFLOW_INACTIVE]
│       │   ├── Not found / not visible ──> [HALT: 404 — E_WORKFLOW_NOT_FOUND]
│       │   └── Load resolved_spec BY HASH (cache used only if hash matches Postgres)
│       │
│       ├── 0.4 Business-key dedup — UNIQUE (tenant_id, workflow_id, external_id)
│       │   ├── New ──> continue
│       │   ├── Exists, same input hash ──> 200 OK + existing saga (whatever its state)
│       │   ├── Exists, different input ──> [HALT: 409 — E_EXTERNAL_ID_CONFLICT]
│       │   ├── Exists, terminal failure ──> still 200/409 — NEVER auto-restart ⚠
│       │   │   (retry = caller sends a new external_id, deliberately)
│       │   └── ◆ Double-tap on "Pay" / gateway retry → two sagas, two charges
│       │
│       ├── 0.5 Admission control
│       │   ├── Tenant running sagas ≥ quota ──> [HALT: 429 — E_TENANT_CONCURRENCY + Retry-After]
│       │   └── Global backpressure (queue depth, DB load) ──> [HALT: 503 + Retry-After]
│       │
│       ├── 0.6 SINGLE TRANSACTION (dedup via INSERT … ON CONFLICT inside it)
│       │   ├── INSERT saga_instances (id UUIDv7, public_id sga_…, tenant, workflow_id,
│       │   │     version, spec_hash, external_id, input_hash, state=CREATED,
│       │   │     deadline_at = db_now() + timeout, fencing_token = 0)
│       │   ├── INSERT saga_step_executions per STATIC step (PENDING)
│       │   │   └── Dynamic map children are created at runtime, not here
│       │   ├── INSERT work item (saga_id, run_at = db_now())
│       │   └── INSERT outbox (saga_created)
│       │
│       └── 202 Accepted {saga_id, status_url}  ← never execute in the request thread
│           └── ◆ Inline execution: LB 60s timeout, client retries, pod restarts mid-saga
│
├── 1. OWNERSHIP, LEASING & SCHEDULING TIER
│   └── Poll: SELECT … WHERE run_at ≤ db_now() FOR UPDATE SKIP LOCKED LIMIT n
│       │
│       ├── 1.1 Lease acquisition
│       │   ├── UPDATE saga SET lease_owner = w, lease_until = db_now() + L,
│       │   │     fencing_token = fencing_token + 1
│       │   │     WHERE id = ? AND (lease_until < db_now() OR lease_owner = w)
│       │   ├── 0 rows ──> owned elsewhere ──> skip silently (not an error)
│       │   └── Heartbeat every L/3; renewal fails ──> worker STOPS issuing calls immediately
│       │
│       ├── 1.2 Fencing
│       │   ├── Every state write: … AND fencing_token = mine  (stale owner rejected)
│       │   ├── Outbound calls carry fencing token header ⚠ (if downstream supports it)
│       │   └── ◆ Worker A GC-pauses 40s, lease expires, B takes over, A wakes and
│       │         overwrites B's state with stale SUCCEEDED
│       │
│       ├── 1.3 Time
│       │   └── Leases, deadlines, next_attempt_at use DATABASE time only
│       │       └── ◆ Node clock 3 min fast "expires" every lease it sees → double ownership
│       │
│       ├── 1.4 Fairness
│       │   ├── Per-tenant concurrent-step cap; round-robin tenants in poll
│       │   └── ◆ One tenant's 2M-saga backfill starves every other tenant for hours
│       │
│       └── 1.5 Retry waits are DURABLE timers (next_attempt_at), never sleep()
│           └── ◆ sleep(backoff) pins a worker; restart loses the retry; saga hangs to deadline
│
├── 2. FORWARD DISPATCH TIER
│   └── CAS saga CREATED ──> RUNNING  (CAS loss ──> already started elsewhere ──> stop)
│       │
│       ├── 2.1 Guards — checked EVERY iteration
│       │   ├── db_now() ≥ deadline_at ──> Tier 7.1
│       │   ├── cancel_requested ──> Tier 7.2
│       │   ├── paused ──> release lease, stop
│       │   └── state ∉ {RUNNING, ROLLING_FORWARD} ──> stop dispatching
│       │
│       ├── 2.2 Ready set — DERIVED from journal each time, no in-memory counters
│       │   ├── ready(S) ⇔ S = PENDING ∧ deps(S) satisfy S.join_policy
│       │   ├── `when` evaluates false ──> S = SKIPPED; propagate per join policy
│       │   └── ◆ In-memory in-degree lost on crash → step runs twice, or never
│       │
│       ├── 2.3 Dispatch protocol — ORDER MATTERS
│       │   ├── a) CAS step PENDING|RETRY_WAIT ──> DISPATCHED, attempt++,
│       │   │      dispatched_at = db_now(), fencing check — COMMIT before network I/O
│       │   ├── b) Evaluate input mappings (pinned spec)
│       │   │      └── Evaluation error ──> FAILED_PERMANENT (E_MAPPING), no effect
│       │   ├── c) Call action with:
│       │   │      Idempotency-Key: idem:{saga_id}:{step_id}:fwd  (stable across ALL attempts)
│       │   │      traceparent, X-Saga-Id, X-Tenant-Id,
│       │   │      deadline = min(step timeout, saga deadline_at)
│       │   └── d) Client-side timeout = step timeout (never trust downstream to time out)
│       │
│       ├── 2.4 Concurrency
│       │   ├── All ready steps in parallel, up to max_parallelism ∧ tenant cap ∧ action bulkhead
│       │   └── Completions write STEP rows; saga row only on saga-level transitions
│       │       └── ◆ 50 parallel completions CAS the saga row → retry storm, lost updates
│       │
│       └── Completion check
│           └── All steps ∈ {SUCCEEDED, SKIPPED} ──> [TERMINAL: COMPLETED]
│
├── 3. RESPONSE CLASSIFICATION TIER
│   │
│   ├── 3.1 2xx
│   │   ├── Output fails action OUTPUT schema ──> FAILED_PARTIAL (effect happened ⇒ compensable)
│   │   ├── Output > MAX (e.g. 256 KiB) ──> blob store + pointer, or FAILED_PARTIAL ⚠
│   │   ├── CAS DISPATCHED ──> SUCCEEDED (+ output, completed_at, fencing)
│   │   │   └── CAS loss (step already terminal) ──> discard, log "late response"
│   │   ├── 202 Accepted (async action) ──> AWAITING_CALLBACK, deadline = step timeout
│   │   │   ├── Callback authenticated (per-step HMAC token)
│   │   │   └── Duplicate / late callbacks idempotent
│   │   └── ──> back to 2.2
│   │
│   ├── 3.2 Retryable, request NOT processed
│   │   ├── Connect refused, DNS failure, 429, 503 + Retry-After
│   │   ├── next_attempt_at = db_now() + max(full_jitter, Retry-After), capped
│   │   ├── attempts < max ──> RETRY_WAIT
│   │   └── exhausted ──> FAILED_PERMANENT (no effect)  [or UNKNOWN if any earlier attempt was ambiguous]
│   │
│   ├── 3.3 Permanent, request rejected: 400, 404, 409, 422, 501
│   │   ├── FAILED_PERMANENT (no effect — action contract MUST guarantee this) ⚠
│   │   ├── 409 "idempotency key reused with different payload" ──> FAILED_PERMANENT + page (bug)
│   │   └── 401 / 403 = platform credential problem, not business failure
│   │       └── PAUSE saga + alert; do NOT compensate ⚠
│   │       └── ◆ Rotated API key → 40k sagas mass-refund over a config mistake
│   │
│   ├── 3.4 AMBIGUOUS — effect may have happened
│   │   ├── Read timeout, reset after send, 500, 502, 504
│   │   │   (500 is ambiguous unless action contract declares otherwise ⚠)
│   │   ├── Step = UNKNOWN; retry with SAME key ──> downstream dedups ──> resolves to 3.1/3.3
│   │   └── Still UNKNOWN after retries ──> 3.5
│   │       └── ◆ Shipping API 504s but created the label; v3 marks FAILED, never cancels; parcel ships
│   │
│   ├── 3.5 Reconciliation
│   │   ├── reconcile_action: "did idem key X take effect?"
│   │   │   ├── Yes ──> SUCCEEDED (output from reconcile)
│   │   │   ├── No ──> FAILED_PERMANENT (nothing to undo)
│   │   │   └── Can't tell ──> stays UNKNOWN
│   │   └── UNKNOWN is treated as "maybe happened" ⇒ INCLUDED in compensation
│   │
│   ├── 3.6 Idempotency-key expiry
│   │   ├── Downstream key TTL (e.g. Stripe ~24h) > max first-attempt→last-retry span
│   │   └── Next attempt would exceed TTL ──> do NOT re-dispatch ──> reconcile only ⚠
│   │       └── ◆ Saga recovered 30h after an outage; key expired; retry = second charge
│   │
│   └── 3.7 Step failure ──> Tier 4
│
├── 4. FAILURE ROUTING TIER — pivot status decides direction, not the error
│   │
│   ├── 4.1 Pivot status at time of failure
│   │   ├── No pivot / pivot not dispatched / pivot FAILED_PERMANENT ──> BACKWARD (4.3)
│   │   ├── Pivot SUCCEEDED ──> FORWARD-ONLY (4.2) — compensate NOTHING
│   │   │   └── ◆ v3: charge OK, ship fails, inventory released → paid, no stock, no refund
│   │   ├── Pivot UNKNOWN ──> reconcile pivot FIRST; block both directions
│   │   │   └── Unresolvable ──> NEEDS_OPERATOR + page
│   │   └── Pivot DISPATCHED (in flight) ──> wait for it to settle, then re-route
│   │
│   ├── 4.2 Forward-only (post-pivot)
│   │   ├── Saga ──> ROLLING_FORWARD
│   │   ├── Retry budget measured in TIME (e.g. 72h), not attempts ⚠
│   │   ├── Exhausted / FAILED_PERMANENT ──> step STUCK, saga STUCK_FORWARD
│   │   │   └── DLQ + page; operator: retry | patch input & retry | mark done manually
│   │   └── Independent post-pivot branches keep running
│   │
│   └── 4.3 Backward (pre-pivot)
│       └── CAS RUNNING ──> COMPENSATING ──> Tier 5
│
├── 5. DRAIN TIER — settle in-flight work BEFORE planning compensation
│   │
│   ├── 5.1 PENDING ──> NOT_STARTED (never dispatched, never compensated)
│   ├── 5.2 RETRY_WAIT ──> cancel timer
│   │   ├── Any prior attempt ambiguous ──> UNKNOWN
│   │   └── Otherwise ──> FAILED_PERMANENT (no effect)
│   ├── 5.3 DISPATCHED ──> wait up to step timeout; never abandon
│   │   ├── Late 2xx ──> SUCCEEDED ⇒ joins compensation set
│   │   └── Timeout ──> UNKNOWN ⇒ joins compensation set
│   │   └── ◆ v3 plans from succeeded_steps; B succeeds 200 ms later; never compensated
│   ├── 5.4 AWAITING_CALLBACK ──> send cancel if supported; else wait to deadline ──> UNKNOWN
│   └── 5.5 Compensation set = COMPENSABLE ∩ {SUCCEEDED, FAILED_PARTIAL, UNKNOWN}
│
├── 6. COMPENSATION TIER
│   │
│   ├── 6.1 Ordering
│   │   ├── X eligible ⇔ every executed descendant of X ∈ {COMPENSATED, NOT_REQUIRED}
│   │   ├── Independent branches compensate IN PARALLEL
│   │   └── ◆ "Exact reverse topo order" serializes 30 independent releases → rollback 30× slower
│   │
│   ├── 6.2 Call
│   │   ├── Idempotency-Key: idem:{saga_id}:{step_id}:comp  — DIFFERENT from :fwd
│   │   │   └── ◆ Same key on same payments API → refund deduped as charge replay → no refund
│   │   ├── Input: forward input + forward OUTPUT (null if UNKNOWN) + ancestors' outputs
│   │   │   └── ◆ Refund needs charge_id; v3 passes step_input only
│   │   └── Lease + fencing checks identical to forward
│   │
│   ├── 6.3 Responses
│   │   ├── 2xx ──> COMPENSATED
│   │   ├── "Nothing to undo" (for UNKNOWN step) ──> NOT_REQUIRED (success)
│   │   ├── Retryable / ambiguous ──> retry, same :comp key, time-based budget
│   │   ├── Permanent 4xx ──> COMPENSATION_FAILED (bug or data drift → human)
│   │   └── Budget exhausted ──> COMPENSATION_FAILED + DLQ + page
│   │
│   ├── 6.4 Own time budget — NOT bound by saga deadline_at
│   │   └── ◆ Saga timed out at 30m; compensation must still run at minute 31
│   │
│   ├── 6.5 Partial compensation failure
│   │   ├── Independent branches continue
│   │   └── Ancestors of the failed step: WAIT (conservative) ⚠ or proceed
│   │
│   └── 6.6 Outcome
│       ├── All ∈ {COMPENSATED, NOT_REQUIRED} ──> [TERMINAL: FAILED_COMPENSATED]
│       └── Any COMPENSATION_FAILED ──> COMPENSATION_STUCK (non-terminal, never auto-closes)
│           └── ◆ Dashboard shows "failed, rolled back"; card hold still on customer's account
│
├── 7. DEADLINE, CANCELLATION & OPERATOR TIER
│   │
│   ├── 7.1 Saga deadline (durable timer at deadline_at)
│   │   ├── Before pivot ──> failure ──> 4.3
│   │   ├── After pivot ──> NO compensation; alert; keep rolling forward ⚠
│   │   └── Steps in flight at deadline ──> UNKNOWN unless a response arrives
│   │
│   ├── 7.2 Cancel: POST …/sagas/{id}:cancel  (`saga:cancel`)
│   │   ├── Terminal ──> [HALT: 409 — E_SAGA_TERMINAL]
│   │   ├── Pivot SUCCEEDED / UNKNOWN / in flight ──> [HALT: 409 — E_PAST_POINT_OF_NO_RETURN]
│   │   ├── Already COMPENSATING ──> 200 no-op
│   │   └── Else CAS cancel_requested ──> worker drains (Tier 5) ──> compensates
│   │       └── [TERMINAL: CANCELLED]  (separate from FAILED for reporting)
│   │       └── ◆ Customer cancels while charge is in flight — must drain, not race
│   │
│   ├── 7.3 Pause / resume — per saga, per workflow, per action
│   │   └── Finishes in-flight calls, schedules nothing new
│   │
│   └── 7.4 Operator actions (audited; two-person for bulk ⚠)
│       ├── retry-step, patch-input-and-retry (post-pivot only)
│       ├── mark-compensated-manually (with evidence reference)
│       └── force-terminal (reason required)
│
├── 8. CRASH RECOVERY TIER
│   │
│   ├── 8.1 Detection: lease_until < db_now() ──> eligible for takeover (1.1)
│   ├── 8.2 Step state on takeover
│   │   ├── PENDING ──> normal scheduling
│   │   ├── DISPATCHED ──> UNKNOWN (call may or may not have left) ──> re-dispatch SAME key
│   │   ├── RETRY_WAIT ──> honor next_attempt_at
│   │   ├── SUCCEEDED, downstream not yet evaluated ──> recompute ready set (idempotent)
│   │   └── COMPENSATING ──> re-issue with same :comp key
│   ├── 8.3 Rebuild ALL state from journal; trust nothing in memory or cache
│   ├── 8.4 Poison saga: worker crashes N times (e.g. 3) on it ──> PAUSED_POISON + alert
│   │   └── ◆ 2 MB output OOMs the decoder; every worker that picks it up dies; fleet crash-loops
│   └── 8.5 Region failover ⚠
│       ├── Same keys, same fencing semantics in DR region
│       └── Async replication loses last N seconds ──> dispatches not journaled
│           ⇒ duplicate dispatch is absorbed ONLY by idempotency keys  [Certain]
│
├── 9. DOWNSTREAM PROTECTION TIER
│   │
│   ├── 9.1 Circuit breaker PER ACTION (shared across sagas & tenants)
│   │   ├── Open ──> RETRY_WAIT until half-open; does NOT consume attempts
│   │   └── ◆ Shipping API down 2h → every saga exhausts retries → 80k compensations in 10 min
│   ├── 9.2 Per-action concurrency bulkhead + rate limit
│   ├── 9.3 Separate cap for compensation traffic (refund APIs rate-limit too)
│   ├── 9.4 Retry budget: retries ≤ X% of first attempts per action per minute
│   └── 9.5 Mass-failure brake: > N sagas/min of one workflow entering COMPENSATING
│       ──> auto-pause workflow + page ⚠  [Likely: prevents most storm damage]
│
├── 10. TERMINAL STATES & LIFECYCLE
│   │
│   ├── Terminal (absorbing — enforced by DB trigger, no transition out):
│   │   ├── COMPLETED          — all steps SUCCEEDED / SKIPPED
│   │   ├── FAILED_COMPENSATED — failed pre-pivot, fully undone
│   │   └── CANCELLED          — cancelled pre-pivot, fully undone
│   ├── Needs-human (never auto-close; age-based SLA alerts):
│   │   └── COMPENSATION_STUCK, STUCK_FORWARD, NEEDS_OPERATOR, PAUSED_POISON
│   ├── Completion event via outbox (at-least-once; consumers dedupe on saga_id + state)
│   └── Retention: purge step input/output after N days; keep skeleton + audit
│
└── X. CROSS-CUTTING
    ├── X.1 Never log step input/output or downstream error bodies verbatim
    │   └── ◆ Loki keeps 30 days of cardholder names and addresses from "transition" logs
    ├── X.2 error_message ≤ 2 KiB, scrubbed, + structured {code, http_status, action, attempt}
    ├── X.3 saga_events append-only, written in the SAME txn as each state change
    ├── X.4 Metrics: sagas per state, AGE of oldest non-terminal per state, UNKNOWN count per
    │        action, compensation rate per workflow, lease takeovers/min, breaker state
    └── X.5 Public ID opaque; UUIDv7 never exposed (leaks creation time and volume)
```

---

## 2. Core Architectural Invariants (S1–S18)

| ID | Invariant Name & Definition | Evolutionary Change from v3 |
|---|---|---|
| **S1** | Saga pinned to (workflow_id, version, spec_hash); version comes from the active pointer unless pinned by a privileged caller | Version source fixed |
| **S2** | tenant_id in every key, query, lock, cache key, and event | Same |
| **S3** | All ready steps dispatch concurrently, within the parallelism, tenant, and action caps | Was roots only |
| **S4** | X is compensated only after all of its executed descendants; independent branches compensate in parallel; NOT_STARTED steps never compensated | “Exact reverse topo” removed |
| **S5** | Once the pivot has SUCCEEDED, no step is ever compensated; failures roll forward | Now actually enforced by Tier 4 |
| **S6** | Forward key …:fwd and compensation key …:comp differ, and each is stable across all attempts | Fixes refund dedupe bug |
| **S7** | Step state is CAS’d on step rows; the saga row is touched only on saga transitions; all writes are fenced | Fixes hot row and split-brain |
| **S8** | Full jitter, plus Retry-After honored, plus per-action circuit breaker and retry budget | Extended |
| **S9** | Step timeout ≤ saga deadline; compensation has its own budget | Extended |
| **S10** | DISPATCHED is committed before I/O; on recovery, DISPATCHED means UNKNOWN | Makes “safely” true |
| **S11** | W3C tracing propagated | Same |
| **S12** | Structured logs contain IDs and codes, never payloads | PII fix |
| **S13** | (tenant, workflow_id, external_id) unique; a duplicate start never creates a second saga | New |
| **S14** | UNKNOWN is treated as “maybe happened” and is always reconciled or compensated, never dropped | New |
| **S15** | The compensation set is computed only after all in-flight steps are settled | New |
| **S16** | At most one fenced owner per saga at a time; all time comes from the DB | New |
| **S17** | No retry is sent after the downstream idempotency-key TTL; reconcile instead | New |
| **S18** | Terminal states are absorbing; needs-human states never auto-close | New |


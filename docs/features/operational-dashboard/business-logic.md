Your stream takes the tenant from a **query parameter** (`?tenant={id}`) and checks authorization **once, at connect time**. That combination leaks in two ways:
- **Cross-tenant streams.** Any authenticated user who edits `tenant=` subscribes to another tenant's live transaction feed. Streams don't pass back through your per-request tenant checks.
- **Revoked access keeps flowing.** SSE and WebSocket connections live for hours. An operator whose role is removed, or whose session expired, keeps receiving live data until the connection drops.

The stream's scope must be **derived from the principal's verified authorization**. Revocation must **terminate or narrow** open streams. [Certain]

Five more errors, ranked by damage:

**1. Live counts built from transition events drift and lie.** [Certain]
I disagree because transition events are delivered at least once, are ordered only per saga, and are missed during gateway restarts and client reconnects. Incrementing and decrementing "active sagas" from them accumulates duplicates and misses until the number is simply wrong.
Here's what I'd do instead: compute headline numbers from **authoritative state** (periodic snapshots or Prometheus series), and use the event stream only for **detail and deltas**, deduplicated by saga version, with resume-by-event-ID on reconnect.

**2. A three-colour DAG (green/blue/red) misleads operators into dangerous actions.** [Certain]
I disagree because UNKNOWN (timed out, may have executed), COMPENSATING, PAUSED, RETRY_SCHEDULED, and NEEDS_ATTENTION all collapse into "blue" or "red." An operator seeing **red** on a step that is actually UNKNOWN will compensate around an executed charge, the exact failure your intervention tree exists to prevent.

**3. Action gating is described as hiding buttons in the UI.** [Certain]
I disagree because "disable action buttons if role lacks `sre:admin`" protects nothing: the POST endpoint must authorize on the server. With cookie sessions it also needs CSRF protection and an idempotency key against double-clicks. The "impact analysis" must be **computed server-side and bound to a state snapshot**, so the operator confirms exactly what will execute.

**4. The headline metrics compute the wrong things.** [Certain]
- **"Failure rate %"** mixes business rejections with system failures.
- **"P99 latency"** averaged across instances isn't a P99. Quantiles must come from merged histogram buckets.
- **"DLQ pending count"** without oldest age, deadlines, and money at stake can't drive prioritization.

**5. A broken stream looks like a quiet system.** [Certain]
When Kafka or the gateway stalls, the dashboard keeps showing the last numbers, or zero activity. Operators then act on data minutes or hours old.
Here's what I'd do instead: every tile shows **data age** and switches to UNKNOWN when it goes stale, and actions are disabled while the view is stale.

```
OPERATIONAL DASHBOARD (SAGA ENGINE) — BUSINESS LOGIC DECISION TREE (v2)
Legend: [C] Certain   [L] Likely (sound default)   [G] Guessing (owner must decide)
│
├── 0. PURPOSE & AUDIENCES
│   ├── Operators/SRE: platform-wide health, stuck/unknown sagas, DLQ triage entry, interventions
│   ├── Support: tenant-scoped saga lookup (impersonation session), timelines, customer-impact view
│   ├── Tenant admins: their own tenant's transaction health (no internals)
│   └── Views: Overview (aggregates) → Lists (filtered sagas) → Saga detail (DAG + timeline) → Action (via intervention flow)
│
├── 1. INVARIANTS
│   ├── DB-01 Stream scope is derived from the principal's authorization, never from request parameters [C]
│   ├── DB-02 Long-lived streams re-validate authorization and terminate on revocation/expiry [C]
│   ├── DB-03 Headline numbers come from authoritative sources; event streams provide detail and deltas only [C]
│   ├── DB-04 Every displayed value carries its data age; stale data is shown as UNKNOWN, never as current [C]
│   ├── DB-05 The visual state model is complete; UNKNOWN is never rendered as FAILED or SUCCEEDED [C]
│   ├── DB-06 All actions are authorized server-side and executed only via the manual intervention flow [C]
│   ├── DB-07 Action previews are server-computed and bound to a state snapshot version [C]
│   ├── DB-08 Live payloads contain identifiers and statuses only; details fetched on drill-down with masking [C]
│   ├── DB-09 Per-client backpressure: a slow client never degrades the gateway or other clients [C]
│   └── DB-10 Sensitive views and every action are audited [C]
│
├── 2. SESSION & STREAM AUTHORIZATION  (DB-01, DB-02)
│   ├── 2.1 Connect
│   │   ├── Authenticate (session cookie with CSRF-safe design or bearer token)
│   │   ├── Origin check for WebSocket; same-site cookies [C]
│   │   ├── Scope resolution
│   │   │   ├── Tenant admin → own tenant only
│   │   │   ├── Support → tenant from active impersonation session (reason, ticket, expiry)
│   │   │   └── Operator → platform aggregates; tenant detail only via explicit scope selection that is authorized and audited
│   │   └── Requested filters validated against scope (TQL subset; transaction-query-api rules)
│   ├── 2.2 During the stream
│   │   ├── Re-authorize at intervals [L: ≤ 5 min] and on revocation push (role removed, session revoked, impersonation ended)
│   │   ├── Session/token expiry → stream closed with reason; client re-authenticates
│   │   └── Scope narrowing (lost tenant access) → stream closed (not silently filtered)
│   └── Edge cases
│       ├── E2.1 ?tenant=B edited by tenant A user — live cross-tenant feed — SECURITY [C]
│       ├── E2.2 Operator removed from role, keeps receiving stream for hours — SECURITY [C]
│       ├── E2.3 Impersonation session expires while stream open — continues — SECURITY [C]
│       └── E2.4 WebSocket without origin check — cross-site hijacking with cookie auth — SECURITY [C]
│
├── 3. STREAM PROTOCOL
│   ├── 3.1 Transport
│   │   ├── SSE (server → client updates) preferred for one-way feeds; WebSocket if client must send frequent messages [L]
│   │   ├── Heartbeats [L: every 15 s] to keep proxies/load balancers from idling out connections
│   │   └── HTTP/1.1 browsers limit concurrent connections per origin (SSE counts) → use HTTP/2 or one multiplexed stream per tab [C]
│   ├── 3.2 Snapshot + delta
│   │   ├── On connect: snapshot of current view (aggregates + first page of list) with snapshot version
│   │   ├── Then deltas: each with monotonically increasing event id per stream
│   │   ├── Saga-level deltas carry saga version; client/gateway apply only newer versions (dedupe, reorder-safe)
│   │   └── Reconnect: client sends last event id → gateway resumes if within retention window, else new snapshot
│   ├── 3.3 Throttling & coalescing
│   │   ├── Aggregates pushed at fixed cadence [L: 1–2 s]
│   │   ├── List updates coalesced per saga within window (latest state only)
│   │   └── Per-view cap on rows streamed (e.g., top 100 by priority); "more changes" indicator beyond
│   ├── 3.4 Per-client backpressure (DB-09)
│   │   ├── Bounded send buffer per connection; overflow → drop intermediate deltas and send resync marker
│   │   └── Persistent overflow → close connection with reason (client reconnects with snapshot)
│   └── Edge cases
│       ├── E3.1 Slow browser tab buffers unbounded events — gateway memory exhaustion — OUTAGE [C]
│       ├── E3.2 Reconnect without resume — missed transitions — list shows stale states [C]
│       ├── E3.3 Gateway deploy drops all streams — synchronized reconnect storm [C]
│       │   └── Handling: randomized reconnect delay (server-provided retry hint), graceful drain on deploy
│       └── E3.4 Load balancer idle timeout shorter than event gaps — silent disconnects [C]
│
├── 4. DATA SOURCES & CORRECTNESS  (DB-03, DB-04)
│   ├── Aggregates (counts by state, rates, latencies)
│   │   ├── From metrics backend (recording rules) or authoritative state snapshots — not from event arithmetic
│   │   ├── Latency quantiles from merged histogram buckets, never averaged per-instance quantiles [C]
│   │   └── Each aggregate has source timestamp → data age
│   ├── Live transitions
│   │   ├── From engine transition notifications (outbox-published) — deduped by (saga id, version)
│   │   └── Used for list/detail freshness, not for counters
│   ├── Periodic reconciliation of live view vs authoritative state (e.g., every 30–60 s) → corrects drift [L]
│   ├── Source health
│   │   ├── Metrics backend stale / Kafka notification lag / gateway consumer down → affected tiles UNKNOWN
│   │   └── Overall banner when any source exceeds staleness threshold
│   └── Edge cases
│       ├── E4.1 Active count from +1/-1 on events — duplicates drift upwards; restart resets to zero — wrong [C]
│       ├── E4.2 Notification consumer stopped — dashboard shows "0 transitions" as calm system [C]
│       └── E4.3 Averaged P99 across pods — understated tail latency — wrong prioritization [C]
│
├── 5. METRIC DEFINITIONS  (what the overview must show)
│   ├── Sagas by state (RUNNING, WAITING, RETRY_SCHEDULED, UNKNOWN, COMPENSATING, NEEDS_ATTENTION, PAUSED, terminal)
│   ├── Outcome rates split: completed / business-rejected / failed (system) / compensated — not one "failure %"
│   ├── UNKNOWN outcomes: count, oldest age, money at stake (highest priority tile)
│   ├── NEEDS_ATTENTION / DLQ: count, oldest age, nearest business deadline, money at stake, by owner team
│   ├── Step latency: p50/p95/p99 per step type (histogram-based)
│   ├── Pipeline health: outbox lag (time), relay status, dispatch rate vs reply rate, timer lateness
│   ├── Compensations: in progress, failed, approaching deadlines
│   └── Per-tenant top-N (operators) with fairness/noisy-neighbour indicators
│
├── 6. VISUAL SEMANTICS  (DB-05)
│   ├── Complete palette with legend and icons (not colour-only, for accessibility):
│   │   SUCCEEDED, RUNNING/DISPATCHED, WAITING, RETRY_SCHEDULED, UNKNOWN (distinct, warning), REJECTED,
│   │   FAILED (not executed), COMPENSATING, COMPENSATED, COMPENSATION_FAILED, SKIPPED, NOT_REACHED, PAUSED, NEEDS_ATTENTION
│   ├── Pivot step marked; post-pivot steps visually distinguished (compensation not possible)
│   ├── Parallel groups shown with join policy and branch roles
│   ├── Times shown in viewer's zone with UTC on hover; durations computed from server timestamps
│   ├── Staleness overlay on any element older than threshold
│   └── Edge cases
│       ├── E6.1 UNKNOWN rendered red as FAILED — operator compensates around executed payment — LOSS [C]
│       └── E6.2 Colour-only states — misread by colour-blind operators — wrong actions [C]
│
├── 7. DRILL-DOWN
│   ├── GET saga detail (transaction-query-api): authorization (tenant + scope), masking by entitlement
│   ├── Not visible or nonexistent → 404 (indistinguishable)
│   ├── Just created, not yet projected → PENDING (with consistency token where applicable), not 404
│   ├── Detail content
│   │   ├── DAG with full state model, attempts, outcome classes, timers, compensations, deadlines
│   │   ├── Error information: classes and sanitized messages; raw payloads only for unmask-entitled roles, audited
│   │   ├── Evidence links: worker status-query results, provider references (restricted), reconciliation anomalies
│   │   └── Links to traces/logs by saga id (operators)
│   └── Edge cases
│       ├── E7.1 Raw error payload shows other customers' data from provider error messages — leak [C]
│       └── E7.2 Detail served from cache while stream shows newer state — conflicting views [C]
│           └── Handling: detail shows its own as-of version; mismatch prompts refresh
│
├── 8. ACTIONS  (DB-06, DB-07)
│   ├── Available actions computed server-side from saga state + caller's authorization matrix (intervention tree)
│   ├── Flow
│   │   ├── Step 1 Operator selects action → server PAUSES saga if required (intervention entry)
│   │   ├── Step 2 Server computes preview: affected steps, commands to be sent, compensations, money, customer-visible effects,
│   │   │          deadlines — bound to snapshot version
│   │   ├── Step 3 Fact-finding results displayed (executed / not executed / undetermined per step)
│   │   ├── Step 4 Confirmation with re-authentication for money actions; approval request if second approver needed
│   │   ├── Step 5 Submit with intervention id (idempotency) + snapshot version + CSRF token
│   │   └── Step 6 Server re-validates snapshot; mismatch → re-preview required
│   ├── UI behaviour: actions disabled while view stale or stream disconnected; disabled state still enforced server-side
│   └── Edge cases
│       ├── E8.1 Buttons hidden but endpoint unprotected — any user calls it directly — SECURITY [C]
│       ├── E8.2 Double-click submits redrive twice — duplicate actions — prevented by intervention id [C]
│       ├── E8.3 Operator acts on 10-minute-old view; saga already compensated — wrong action — snapshot mismatch blocks [C]
│       ├── E8.4 Preview computed in browser from displayed data — diverges from real effect [C]
│       └── E8.5 Cookie-authenticated POST without CSRF protection — forged actions from malicious page — SECURITY [C]
│
├── 9. FAILURE & DEGRADATION
│   ├── Stream disconnected → banner "live updates paused", data age counting up, actions disabled
│   ├── Metrics backend down → aggregate tiles UNKNOWN; lists still available via query API
│   ├── Notification lag → list updates delayed; lag shown
│   ├── Query API degraded → drill-down shows error with retry; no stale detail presented as current
│   └── Gateway overload → shed streaming clients by priority (operators on incident first) [G]
│
├── 10. SCALE & PERFORMANCE
│   ├── Shared ingestion: one set of consumers per gateway cluster (not per connection); fan-out in memory with filters
│   ├── Pre-aggregated views for overview; per-client streaming of only matching sagas (bounded)
│   ├── Sampling of live transition detail when rates exceed thresholds (aggregates remain exact)
│   ├── Connection limits per principal and per tenant
│   └── E10.1 Each dashboard tab starts its own Kafka consumer — consumer group explosion — broker load [C]
│
├── 11. AUDIT  (DB-10)
│   ├── Stream sessions: who, scope, impersonation, duration
│   ├── Drill-down views of sensitive sagas / unmasked payloads
│   └── All actions (via intervention audit) with snapshot version and preview shown to operator
│
└── 12. FEATURE TEST SCENARIOS
    ├── Tenant user edits tenant parameter → scope unchanged (derived from authorization)
    ├── Role revoked mid-stream → stream closed within re-auth interval
    ├── Impersonation expires → stream closed
    ├── Reconnect with last event id → missed deltas replayed or fresh snapshot
    ├── Slow client → resync marker, no gateway memory growth
    ├── Gateway deploy → staggered reconnects, no storm
    ├── Duplicate transition events → list state correct (version-based)
    ├── Notification consumer stopped → tiles show UNKNOWN with data age
    ├── P99 tile matches histogram-merged quantile from metrics backend
    ├── UNKNOWN step rendered distinctly; compensate action not offered until resolved
    ├── Action endpoint called without role → 403 regardless of UI
    ├── Double-submit action → single intervention
    ├── Stale snapshot on confirm → re-preview required
    ├── CSRF attempt → rejected
    └── Many tabs per operator → shared ingestion, connection limits enforced
```

[Certain] The core corrections follow from how live dashboards fail:
- Request parameters can't define authorization scope.
- Long-lived connections outlive authorization checks.
- Event-derived counters drift under at-least-once delivery and reconnects.
- Averaged quantiles are wrong.
- Collapsed state palettes cause wrong interventions.
- UI-only gating and missing CSRF protection let anyone execute actions.
- Silent staleness looks like calm.

[Likely] Heartbeat intervals, re-authorization cadence, coalescing windows, and reconciliation intervals are defaults to tune.

[Guessing] Which audiences see which tiles, load-shedding priorities, and sampling thresholds for live detail are owner decisions.

Two decisions are yours:
1. **Derive stream scope from authorization and enforce revocation on open streams (DB-01, DB-02).** This closes the live cross-tenant leak and the "removed operator still watching" gap.
2. **Adopt the complete state model in the UI (DB-05), with server-computed, snapshot-bound action previews (DB-07).** The dashboard is where operators decide to compensate or redrive. If it shows UNKNOWN as FAILED, every safeguard in the intervention and compensation trees is bypassed by a misread colour.
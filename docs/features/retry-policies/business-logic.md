# Retry Policies — Business Logic Decision Tree

```
RETRY POLICIES (SAGA STEPS) — BUSINESS LOGIC DECISION TREE (v2)
Legend: [C] Certain   [L] Likely (verify per protocol/proxy/library)   [G] Guessing (step owner must decide)
│
├── 0. POLICY MODEL  (declared per step at definition time)
│   ├── Step capability
│   │   ├── idempotent_by_key: worker dedupes on the step idempotency key (YES/NO)
│   │   ├── status_query: worker can answer "what happened to key K" (YES/NO)
│   │   └── effect_type: NONE (read) | HOLD | COMPENSABLE | IRREVERSIBLE
│   ├── Saga position: COMPENSABLE step | PIVOT | RETRIABLE (post-pivot)
│   ├── Budgets: max attempts AND max elapsed time AND deadline caps (§4)
│   ├── Backoff: algorithm, base, cap, floor, jitter (§7)
│   ├── Dependency id (for shared rate limits and breaker, §5–6)
│   └── Classification overrides (worker-specific outcome codes, §2.2)
│
├── 1. INVARIANTS
│   ├── RP-01 Every retry of a step reuses the SAME idempotency key and the SAME frozen inputs [C]
│   ├── RP-02 A blind retry is allowed only if the request was not sent, or the worker is idempotent by key [C]
│   ├── RP-03 Ambiguous outcomes are resolved (status query / same-key retry) before any failure decision [C]
│   ├── RP-04 Budget exhaustion never implies "not executed" unless the last outcome proves it [C]
│   ├── RP-05 Own rate limiting or breaker state never converts into a step failure — only postponement [C]
│   ├── RP-06 Dependency-wide failures pause retries for that dependency; they don't consume per-step budgets [C]
│   ├── RP-07 Retry timers are durable; attempts recorded before dispatch [C]
│   ├── RP-08 No retry is dispatched after the step's effective deadline [C]
│   ├── RP-09 Exactly one retry layer per call (no hidden client/mesh retries underneath) [C]
│   └── RP-10 Post-pivot steps retry until success or human decision; never compensated [C]
│
├── 2. CLASSIFICATION  (produce exactly one class per attempt)
│   │
│   ├── 2.1 Was the request sent to the target?
│   │   ├── NOT SENT (provably): DNS failure, connection refused, TLS handshake failure, own client-side limiter,
│   │   │   proxy-generated "no healthy upstream / upstream connect failure" before forwarding [L: verify proxy signals]
│   │   │   └── Class NOT_SENT → safe to retry for every step type
│   │   └── SENT or UNKNOWN whether sent → continue
│   │
│   ├── 2.2 Did the worker return an explicit contract outcome? (preferred over protocol status)
│   │   ├── SUCCEEDED / ALREADY_DONE(with result) → SUCCESS path
│   │   ├── REJECTED(reason) → REJECTED
│   │   ├── NOT_EXECUTED_RETRYABLE (e.g., overloaded before processing) → TRANSIENT_NOT_EXECUTED
│   │   ├── IN_PROGRESS → WAIT (re-check later via status query)
│   │   └── No explicit outcome (gateway/proxy/transport error) → 2.3
│   │
│   ├── 2.3 Protocol-status mapping (when no contract outcome)
│   │   ├── TRANSIENT_NOT_EXECUTED (target refused before processing)
│   │   │   ├── HTTP 429, HTTP 503 from target with Retry-After [L: by contract]
│   │   │   ├── gRPC RESOURCE_EXHAUSTED (rate/quota), UNAVAILABLE when generated before forwarding [L]
│   │   │   └── HTTP 408 (server timed out waiting for request) [L: usually not processed; verify per service]
│   │   ├── AMBIGUOUS (may have executed)
│   │   │   ├── HTTP 500, 502, 504; gRPC DEADLINE_EXCEEDED, UNAVAILABLE after forwarding, UNKNOWN, INTERNAL
│   │   │   └── Transport: connection reset / broken pipe / read timeout AFTER request bytes were sent
│   │   ├── REJECTED (deterministic business or contract failure)
│   │   │   ├── HTTP 400, 422; gRPC INVALID_ARGUMENT, FAILED_PRECONDITION, OUT_OF_RANGE
│   │   │   └── Same input will fail again
│   │   ├── CONFLICT_CHECK
│   │   │   ├── HTTP 409 / gRPC ALREADY_EXISTS on a RETRY (attempt > 1) → fetch by key → likely ALREADY_DONE
│   │   │   ├── HTTP 409 / gRPC ALREADY_EXISTS on FIRST attempt → business conflict → REJECTED
│   │   │   └── gRPC ABORTED (concurrency conflict) → TRANSIENT_NOT_EXECUTED (retry)
│   │   ├── CREDENTIAL
│   │   │   ├── HTTP 401 / UNAUTHENTICATED → refresh credentials once → retry; repeat → REJECTED + security alert
│   │   │   └── HTTP 403 / PERMISSION_DENIED → REJECTED (optional short wait if policy propagation is known to lag [G])
│   │   ├── CAUSAL_WAIT
│   │   │   └── HTTP 404 / NOT_FOUND for a resource expected to exist soon (created by an earlier step or
│   │   │       another service) → bounded delayed retries → then REJECTED
│   │   ├── INTEGRITY
│   │   │   └── gRPC DATA_LOSS, contradictory outcomes for same key → stop, NEEDS_ATTENTION
│   │   └── CANCELLED (own cancellation) → see saga cancellation rules (outcome unknown if sent)
│   │
│   ├── 2.4 Deterministic-error detection
│   │   ├── Same error fingerprint (class + normalized message) on N consecutive attempts [L: N = 3]
│   │   │   for AMBIGUOUS/5xx → likely a bug, not transience → stop retrying → resolve outcome → NEEDS_ATTENTION
│   │   └── E2.4a Retrying a deterministic 500 for hours — wasted budget, load, delayed resolution [C]
│   │
│   ├── 2.5 Request-specific vs dependency-wide
│   │   ├── Same dependency failing for most calls in window → DEPENDENCY_DOWN → §6 (pause)
│   │   └── Only this request failing while others succeed → request-specific class as above
│   │
│   └── Edge cases
│       ├── E2.1 504 from gateway treated as transient → blind retry of non-idempotent payment → double charge [C]
│       ├── E2.2 ALREADY_EXISTS after retry treated as failure → successful step compensated [C]
│       ├── E2.3 401 from expired token treated as business rejection → saga compensated unnecessarily [C]
│       ├── E2.4 404 because target's read replica lags → step rejected → valid saga cancelled [C]
│       └── E2.5 Proxy and upstream both emit 503 indistinguishably → treat as AMBIGUOUS (safe default) [C]
│
├── 3. ACTION BY CLASS × STEP CAPABILITY
│   ├── NOT_SENT → retry (backoff, budget)
│   ├── TRANSIENT_NOT_EXECUTED → retry (backoff, honour Retry-After)
│   ├── AMBIGUOUS
│   │   ├── idempotent_by_key = YES → retry with same key (worker returns original outcome if executed)
│   │   ├── idempotent_by_key = NO, status_query = YES → query status by key:
│   │   │   ├── EXECUTED → SUCCESS path (with stored result)
│   │   │   ├── NOT_FOUND (within worker's dedupe/record window) → retry
│   │   │   ├── IN_PROGRESS → wait and re-query
│   │   │   └── query fails → retry query (not the operation)
│   │   └── idempotent_by_key = NO, status_query = NO → UNKNOWN → NEEDS_ATTENTION (no blind retry, no compensation)
│   ├── REJECTED → no retry → saga decision:
│   │   ├── Pre-pivot step → compensation of executed steps
│   │   └── Pivot/post-pivot step → NEEDS_ATTENTION (roll-forward impossible without decision)
│   ├── ALREADY_DONE → SUCCESS path
│   ├── CREDENTIAL → one refresh-and-retry, then REJECTED
│   ├── CAUSAL_WAIT → delayed retries within causal budget [L: minutes], then REJECTED
│   ├── DEPENDENCY_DOWN → pause dependency (§6); step stays RETRY_SCHEDULED with clock frozen
│   ├── INTEGRITY → NEEDS_ATTENTION
│   └── Edge cases
│       ├── E3.1 Ambiguous retry of non-idempotent worker without status query → duplicate effect [C]
│       └── E3.2 Status query hits target's lagging replica → false NOT_FOUND → duplicate [C]
│           └── Handling: workers answer status queries from authoritative storage
│
├── 4. BUDGETS & DEADLINES
│   ├── 4.1 Budget = BOTH max attempts AND max elapsed time since first dispatch
│   ├── 4.2 Effective deadline = minimum of:
│   │   ├── step deadline
│   │   ├── saga deadline
│   │   ├── hold/reservation expiry this step depends on
│   │   ├── provider idempotency window (retry after it may double-execute)
│   │   └── business validity of the request (valid_until: price quote, cutoff time)
│   ├── 4.3 No dispatch scheduled beyond the effective deadline (RP-08)
│   ├── 4.4 Exhaustion outcome (RP-04)
│   │   ├── Last class NOT_SENT / TRANSIENT_NOT_EXECUTED (proven not executed) → FAILED_NOT_EXECUTED
│   │   │   └── Pre-pivot → compensation | Post-pivot → NEEDS_ATTENTION
│   │   ├── Last class AMBIGUOUS → UNKNOWN → resolution ladder (status query / provider records / human)
│   │   └── Post-pivot RETRIABLE step → NEEDS_ATTENTION (never compensation) (RP-10)
│   └── Edge cases
│       ├── E4.1 5 attempts × 200 ms backoff — budget gone in 3 s of a 2-minute blip — needless failures [C]
│       ├── E4.2 Retry after provider's idempotency window — provider executes again — double effect [C]
│       ├── E4.3 Retry after hold expired — confirm against expired hold — rejection after payment [C]
│       └── E4.4 Exhaustion treated as failure while last error was a 504 — compensation around executed step [C]
│
├── 5. RATE LIMITING & AMPLIFICATION CONTROL
│   ├── Per-dependency retry token bucket (protects the target) — retries ≤ a fraction of normal traffic [L: 10–20%]
│   ├── Per-tenant fairness within that bucket (one tenant can't consume all retry capacity)
│   ├── Limiter denial → postpone (RETRY_SCHEDULED with next eligible time) — never a failure (RP-05)
│   ├── Single retry layer (RP-09): HTTP/gRPC client retries disabled or limited to NOT_SENT cases; mesh retries off
│   │   for state-changing calls
│   └── Edge cases
│       ├── E5.1 Limiter "fail fast" → step FAILED → compensation for a healthy request [C]
│       ├── E5.2 Mesh retries a timed-out POST underneath the engine — duplicate execution invisible to engine [C]
│       └── E5.3 Per-tenant buckets only — 1,000 tenants each retrying in budget overwhelm one dependency [C]
│
├── 6. DEPENDENCY BREAKER INTERPLAY  (RP-06)
│   ├── Breaker per dependency: opens on error/ambiguous rate or latency breach
│   ├── Open → no dispatches to that dependency; all RETRY_SCHEDULED steps for it have budgets FROZEN
│   ├── Half-open → limited probes (real low-risk requests or health endpoint per contract)
│   ├── Closed → ramped release of postponed retries (jittered, rate-limited) (§10)
│   ├── Breaker open longer than step deadlines → deadline policy decides (§4.2), not the breaker
│   └── E6.1 Outage burns every saga's budget → mass compensation/attention when dependency returns minutes later [C]
│
├── 7. BACKOFF COMPUTATION
│   ├── Algorithms
│   │   ├── Exponential full jitter: random(floor, min(cap, base × 2^attempt)) — floor > 0 [L: ≥ base/2]
│   │   ├── Exponential equal jitter: half + random(0, half), half = min(cap, base × 2^attempt)/2
│   │   └── Linear: min(cap, base × attempt) + jitter (never without jitter)
│   ├── Overflow protection: exponent capped (attempt clamped) before computing 2^attempt
│   ├── Retry-After (429 and 503)
│   │   ├── Parse both seconds and HTTP-date formats; reject negative/invalid → use computed backoff
│   │   ├── Delay = max(computed, Retry-After)
│   │   └── If delay pushes beyond effective deadline → decide now (§4.4), don't schedule a useless retry
│   ├── Per-dependency minimum spacing between retries of the same key
│   └── Edge cases
│       ├── E7.1 Full jitter without floor → zero-delay retries → hammering [C]
│       ├── E7.2 Linear without jitter → synchronized retries from thousands of sagas [C]
│       ├── E7.3 Retry-After of hours accepted blindly → step waits past hold expiry/deadline [C]
│       └── E7.4 2^attempt overflow at high attempt counts → negative/huge delays [C]
│
├── 8. SCHEDULING & DISPATCH OF A RETRY  (RP-01, RP-07)
│   ├── In the same transaction: record attempt outcome, compute next time, set RETRY_SCHEDULED, insert durable timer
│   ├── Timer fires (owner-executed, idempotent) → re-check: step still RETRY_SCHEDULED? deadline not passed?
│   │   breaker closed? limiter token available? → dispatch with same key + frozen inputs + per-attempt deadline
│   ├── Attempt counter incremented and persisted BEFORE dispatch (survives crashes)
│   ├── Late reply from previous attempt arriving after retry scheduled → apply (same key) → cancel timer
│   └── Edge cases
│       ├── E8.1 In-memory timing wheel — restart loses all scheduled retries — sagas stuck [C]
│       ├── E8.2 Inputs re-resolved at retry time (context changed) — same key, different payload — worker conflict [C]
│       └── E8.3 Timer fires after step already resolved by late reply — duplicate dispatch — guarded by state check [C]
│
├── 9. COMPENSATION & POST-PIVOT RETRY POLICIES
│   ├── Compensation steps
│   │   ├── Own idempotency key (saga, step, "compensate"); same classification rules
│   │   ├── Longer budgets; retry until success or NEEDS_ATTENTION
│   │   ├── REJECTED compensation (e.g., refund window closed) → NEEDS_ATTENTION with evidence
│   │   └── AMBIGUOUS compensation → status query before re-sending (refund twice is a real loss)
│   ├── Post-pivot RETRIABLE steps (RP-10)
│   │   ├── Retry indefinitely with capped backoff while within business deadline
│   │   └── Beyond deadline → NEEDS_ATTENTION (business escalation), never compensation
│   └── E9.1 Compensation retried blindly after timeout — double refund — LOSS [C]
│
├── 10. RECOVERY AFTER OUTAGES
│   ├── Postponed retries released in waves (rate-limited, re-jittered across the release window)
│   ├── Priority: earliest deadlines first (holds about to expire, T0 sagas)
│   └── E10.1 All postponed retries fire at breaker close — thundering herd — dependency fails again [C]
│
└── 11. FEATURE TEST SCENARIOS
    ├── Connection refused before send → retried immediately with backoff (any step)
    ├── 504 on idempotent payment step → retried with same key → provider returns original result, one charge
    ├── 504 on non-idempotent step with status query → query finds EXECUTED → success, no retry
    ├── 504 on non-idempotent step without status query → UNKNOWN → NEEDS_ATTENTION, no compensation
    ├── ALREADY_EXISTS on attempt 2 → fetched → treated as success
    ├── 401 expired token → refreshed once → succeeds
    ├── 404 from replica lag → causal retries → succeeds within causal budget
    ├── 422 validation → no retry → pre-pivot compensation
    ├── Same 500 fingerprint three times → stop, resolve outcome, NEEDS_ATTENTION
    ├── Dependency outage 20 minutes → budgets frozen; no compensations; ramped release on recovery
    ├── Limiter denial → postponed, never failed
    ├── Retry-After beyond hold expiry → decided immediately per deadline policy
    ├── Engine restart with 10k scheduled retries → all fire on schedule (durable timers)
    ├── Late success from attempt 1 after attempt 2 scheduled → applied; attempt 2 cancelled
    ├── Compensation refund times out → status query → no double refund
    └── Post-pivot step failing past deadline → NEEDS_ATTENTION, not compensation
```

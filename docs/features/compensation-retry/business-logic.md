Your retry loop re-sends a compensation after a **network timeout** without first asking whether the earlier attempt went through. For a refund, that is a double refund. The provider may have processed the first request and lost only the response.

A timed-out compensation is **ambiguous**. It must be verified (a status query by its compensation key) or retried with **the same idempotency key** within the provider's deduplication window. It must never be blindly re-dispatched. [Certain]

Four more errors, ranked by damage:

**1. "Exhausted → STUCK, terminal, dead-letter" abandons a real customer effect.** [Certain]
I disagree because the customer is still charged and the booking still exists. Ten attempts with sub-second-to-minutes backoff also exhaust during an ordinary provider maintenance window.
Here's what I'd do instead: compensation retries run on a **time horizon** (hours to days), bounded by the **business deadline** (refund window, free-cancellation cutoff). Exhaustion leads to a non-terminal NEEDS_ATTENTION with an escalation ladder that starts **before** the deadline, not after it.

**2. "Explicitly uncompensable" merges three different situations.** [Certain]
I disagree because each needs a different response:
- **400 bad schema** is your engine's contract defect. Fix it once and resume every affected compensation; escalating each one individually is wrong.
- **Auth revoked** is a credential problem. Rotate or refresh and resume.
- **Already shipped / window closed** is a business NOT_COMPENSABLE. It needs a business alternative (voucher, partial refund), not a stuck flag.

**3. The timer checks the saga state but not the step state.** [Certain]
I disagree because a late reply may have already marked this step COMPENSATED while the saga is still COMPENSATING (other steps pending). The timer fires and re-dispatches anyway.
Here's what I'd do instead: re-check the **step's** compensation state and attempt ID. A Redis step lock doesn't help here; the fenced database transition does.

**4. Tenant-share throttling delays refunds that are about to miss their deadline.** [Certain]
I disagree because clamping a large tenant's backoff to the maximum interval can push a refund past its window, turning a free cancellation into a fee.
Here's what I'd do instead: prioritize by **deadline and cost first**, then fairness. Protect downstreams with **per-dependency** rate limits, not per-tenant penalties.

Two smaller defects remain. Full jitter starting at zero produces immediate re-sends, and an in-memory timing wheel loses every scheduled retry on restart.

```
COMPENSATION RETRY — BUSINESS LOGIC DECISION TREE (v2)
Legend: [C] Certain   [L] Likely (verify per provider)   [G] Guessing (business must decide)
│
├── 0. STANCE
│   ├── A compensation must end in exactly one disposition: COMPENSATED | NOTHING_TO_COMPENSATE |
│   │   COMPENSATED_PARTIAL (residual recorded) | ALTERNATIVE_APPLIED | WRITTEN_OFF (approved)
│   ├── "Stuck" is a status, never a disposition — the customer effect still exists
│   └── Retry horizon is business-driven (deadlines), not a fixed attempt count
│
├── 1. INVARIANTS
│   ├── CR-01 Every retry uses the SAME compensation idempotency key and the SAME frozen inputs [C]
│   ├── CR-02 Ambiguous attempts are verified (status query) before any re-send; re-send only with same key inside provider window [C]
│   ├── CR-03 Retry budgets are time-based and capped by the compensation's business deadline [C]
│   ├── CR-04 Exhaustion never ends the compensation; it escalates it (non-terminal) [C]
│   ├── CR-05 Dependency-wide unavailability freezes budgets; it doesn't consume them [C]
│   ├── CR-06 Engine/contract defects and credential failures pause the step type for a fix, then resume all affected [C]
│   ├── CR-07 Timer firing re-checks the STEP state and attempt id before dispatch [C]
│   ├── CR-08 Scheduling is durable; transitions are fenced database writes; dispatch only via outbox [C]
│   ├── CR-09 Priority = earliest business deadline / highest cost of delay first [C]
│   └── CR-10 Manual actions (support refunded by hand) stop automated retries until verified [C]
│
├── 2. CLASSIFY EACH COMPENSATION ATTEMPT
│   ├── Explicit contract outcome from target (preferred)
│   │   ├── COMPENSATED (with details: amount, fee, reference) → DONE
│   │   ├── ALREADY_COMPENSATED (same key seen before) → DONE (use original details)
│   │   ├── NOTHING_TO_COMPENSATE (forward never took effect / hold already expired) → DONE
│   │   ├── PARTIALLY_COMPENSATED → DONE with residual
│   │   ├── NOT_COMPENSABLE(reason: shipped, consumed, window closed, non-refundable) → BUSINESS_ALTERNATIVE
│   │   ├── REJECTED(business rule) → BUSINESS_ALTERNATIVE
│   │   ├── RETRYABLE_NOT_EXECUTED (target busy, lock conflict, maintenance) → RETRY
│   │   └── IN_PROGRESS (target processing asynchronously, e.g., refund pending at bank) → WAIT_AND_POLL
│   ├── No explicit outcome → protocol/transport mapping
│   │   ├── Not sent (connection refused, DNS, TLS failure, own limiter) → RETRY
│   │   ├── 429 / quota → RATE_LIMITED (honour Retry-After)
│   │   ├── 400 / schema / contract violation → CONTRACT_DEFECT (CR-06)
│   │   ├── 401 / 403 with auth/credential reason → CREDENTIAL (CR-06)
│   │   ├── 404 for the forward reference → verify: was forward effect real? → NOTHING_TO_COMPENSATE or INTEGRITY
│   │   ├── 409 → never assumed success; fetch status → map to one of the explicit outcomes above
│   │   ├── 500/502/504, deadline exceeded, reset after send → AMBIGUOUS (§4)
│   │   └── Breaker open / dependency-wide failure → DEPENDENCY_DOWN (§9)
│   └── Edge cases
│       ├── E2.1 409 "cannot cancel" treated as already compensated — effect remains, saga closed — LOSS [C]
│       ├── E2.2 Schema 400 escalated per compensation — 5,000 human tasks for one bug — OUTAGE (ops) [C]
│       └── E2.3 Asynchronous refund (PENDING at bank) treated as failure and re-sent — double refund [C]
│
├── 3. ACTION PER CLASS
│   ├── DONE → record details → release dependents in compensation plan → no further retries
│   ├── RETRY → schedule (§7) within budget/deadline
│   ├── RATE_LIMITED → schedule at max(backoff, Retry-After) unless beyond deadline → escalate early (§6)
│   ├── WAIT_AND_POLL → poll status on target's expected processing cadence (e.g., bank batch windows) [L]
│   ├── AMBIGUOUS → §4 resolution
│   ├── CONTRACT_DEFECT → pause all compensations of this step type; alert owning team; after fix deploy → resume all
│   ├── CREDENTIAL → pause step type for that target; credential rotation/refresh; resume
│   ├── BUSINESS_ALTERNATIVE → alternative path (voucher, partial refund, finance transfer) as its own idempotent action [G]
│   ├── DEPENDENCY_DOWN → freeze (§9)
│   └── INTEGRITY → stop; NEEDS_ATTENTION immediately
│
├── 4. AMBIGUITY RESOLUTION  (CR-02)
│   ├── Step 1 Status query by compensation key
│   │   ├── COMPENSATED / PENDING → DONE / WAIT_AND_POLL (no re-send)
│   │   ├── NOT_FOUND → re-send with SAME key (only within provider idempotency window)
│   │   └── Query unavailable → retry the query (not the compensation)
│   ├── Step 2 No status query available
│   │   ├── Within provider idempotency window → re-send with SAME key (provider dedupes)
│   │   └── Outside window → no re-send; reconcile against provider records / statements → else NEEDS_ATTENTION
│   └── Edge cases
│       ├── E4.1 Re-send after timeout without key — double refund — LOSS [C]
│       ├── E4.2 Re-send with same key after provider window expired — provider executes again — double refund [C]
│       └── E4.3 Status query against provider's eventually consistent API — false NOT_FOUND shortly after send [L]
│           └── Handling: minimum delay before status query after ambiguous send; repeat query before concluding
│
├── 5. BUDGETS & HORIZONS  (CR-03, CR-04)
│   ├── Budget dimensions
│   │   ├── Elapsed time since first attempt (primary) [G: e.g., up to 24–72 h for refunds]
│   │   ├── Attempt count (secondary safety cap, high) [L]
│   │   └── Business deadline (refund window, free-cancellation cutoff, hold expiry, chargeback window)
│   ├── Effective horizon = min(elapsed budget, business deadline − escalation lead time)
│   ├── Hold/reservation compensations: if hold expiry passed → verify → NOTHING_TO_COMPENSATE (no retries needed)
│   ├── Horizon reached → NEEDS_ATTENTION (non-terminal) with full evidence; automated retries may continue at low rate [L]
│   └── Edge cases
│       ├── E5.1 10 attempts exhausted in minutes during provider maintenance — mass escalations [C]
│       ├── E5.2 Retries continue past refund window — every retry rejected — customer never refunded — LOSS [C]
│       └── E5.3 Terminal STUCK state — no retries, no late success processing — LOSS [C]
│
├── 6. ESCALATION LADDER
│   ├── Level 1 (early): owner notified when attempts span X% of horizon or first AMBIGUOUS unresolved > T [L]
│   ├── Level 2 (deadline risk): page when remaining time to business deadline < lead time [G]
│   ├── Level 3 (deadline missed): finance/business process engaged (fee absorbed, goodwill credit)
│   ├── Customer communication: "refund pending" with expected timing when compensation delayed beyond promise [G]
│   ├── Human task contains: forward outcome, compensation attempts, target responses, status-query results,
│   │   deadlines, monetary impact, allowed actions
│   └── E6.1 Paging only after exhaustion — deadline already missed — avoidable fees/disputes [C]
│
├── 7. SCHEDULING & DISPATCH  (CR-07, CR-08)
│   ├── Attempt outcome, next due time, and durable timer written in one fenced transaction
│   ├── Timer fires → re-check in one transaction:
│   │   ├── Saga still COMPENSATING (or COMPENSATING_NEEDS_ATTENTION)
│   │   ├── Step compensation state still RETRY_SCHEDULED for this attempt id
│   │   ├── No manual resolution recorded (CR-10)
│   │   ├── Dependency not frozen; deadline not passed
│   │   └── → record new attempt id → outbox command (same key, frozen inputs, per-attempt timeout)
│   ├── Any check fails → discard timer (record why)
│   └── Edge cases
│       ├── E7.1 Late success already recorded, timer re-dispatches — duplicate compensation (same key absorbs if supported) [C]
│       ├── E7.2 In-memory scheduler restart — all pending compensations silently stop — LOSS [C]
│       └── E7.3 Inputs recomputed at retry (e.g., refund amount from current order total) — different payload under same key [C]
│
├── 8. BACKOFF
│   ├── Exponential with jitter and floor: random(floor, min(cap, base × 2^n)), floor > 0, exponent clamped
│   ├── Cap appropriate to horizon [L: minutes, not seconds, for long horizons]
│   ├── Retry-After honoured (seconds or date); capped by deadline
│   ├── Target calendars: provider maintenance windows and settlement/batch windows respected (no futile retries) [L]
│   └── E8.1 Zero-floor jitter → instant re-send → hammering a struggling provider [C]
│
├── 9. DEPENDENCY OUTAGES  (CR-05)
│   ├── Breaker per target opens → all compensations to that target frozen (budgets paused, deadlines still tracked)
│   ├── Deadline approaching while frozen → escalation ladder proceeds (§6) — outage doesn't hide deadlines
│   ├── Breaker closes → release in priority order (CR-09), rate-limited, jittered
│   └── E9.1 Outage drains every budget → thousands of escalations once provider returns [C]
│
├── 10. PRIORITIZATION & FAIRNESS
│   ├── Order: earliest business deadline → highest cost of delay (fee steps, amount) → age
│   ├── Per-dependency rate limits protect targets
│   ├── Tenant fairness applied within the same deadline band (never delays a near-deadline compensation)
│   └── E10.1 Large tenant's refunds deprioritized past their windows — fees charged — LOSS [C]
│
├── 11. DEFECT & CREDENTIAL PAUSES  (CR-06)
│   ├── Step type paused (not individual compensations escalated)
│   ├── Single incident for the defect; affected compensation count tracked
│   ├── After fix: controlled resume (priority order, rate-limited); deadlines checked first
│   └── E11.1 Defect fix deployed but paused step type never resumed — compensations silently waiting [C]
│       └── Handling: pause records owner + expiry; resume is explicit and monitored
│
├── 12. INTERACTION WITH THE COMPENSATION PLAN
│   ├── A retrying compensation blocks only compensations that depend on it
│   ├── Independent compensations continue
│   └── E12.1 Strict sequential plan — one retrying refund blocks hold releases that then expire with fees [C]
│
├── 13. OPERATOR & MANUAL ACTIONS  (CR-10)
│   ├── Manual refund done outside the system → record it (with reference) → automated retries stop → verify with provider
│   ├── Force-resolve requires evidence; second approver for money
│   ├── Operator cancel of retries → disposition must be set (WRITTEN_OFF / ALTERNATIVE_APPLIED)
│   └── E13.1 Support refunds manually while automated retry still scheduled — double refund — LOSS [C]
│
├── 14. COMPLETION
│   ├── Record disposition, actual amounts/fees, references, attempts, delays vs deadline
│   ├── Hand back to compensation engine (dependents released)
│   └── Customer/finance notified with actual outcome (residuals included)
│
└── 15. FEATURE TEST SCENARIOS
    ├── Refund times out, provider processed it → status query → DONE, no second refund
    ├── Refund times out, provider did not process → NOT_FOUND → re-sent with same key → single refund
    ├── Provider maintenance 2 hours → compensations frozen, no escalations, resumed in deadline order
    ├── Refund window ends in 6 hours, provider failing → early page at lead time
    ├── 409 "already shipped" → status fetched → NOT_COMPENSABLE → alternative path
    ├── Schema defect affecting 5,000 compensations → one pause, one incident, bulk resume after fix
    ├── Credential expired at provider → step type paused → rotated → resumed
    ├── Late success recorded before timer fires → timer discarded
    ├── Engine restart with 10k scheduled compensation retries → all resume on schedule
    ├── Support refunded manually → automated retries stop; verification recorded
    ├── Hold already expired at supplier → NOTHING_TO_COMPENSATE, no retries
    ├── Asynchronous bank refund PENDING → polled, not re-sent
    └── Large tenant flood → near-deadline compensations of all tenants still served first
```

[Certain] The core corrections follow from how compensations fail:
- Timeouts hide whether a refund executed.
- Idempotency windows end.
- Terminal stuck states abandon real customer effects.
- Defects and credentials need fix-and-resume, not per-item escalation.
- Timers must re-check step-level state.
- In-memory schedulers lose work.

[Likely] Provider status-query consistency, maintenance and batch windows, and Retry-After behavior vary by provider.

[Guessing] Retry horizons, escalation lead times, customer messaging, and business alternatives per compensation type are business decisions.

Two decisions are yours:
1. **Replace the 10-attempt budget with deadline-bound time horizons and an escalation ladder that fires before deadlines (§5–6).** Refunds and cancellations have business windows. Missing them costs real money; running out of attempts doesn't.
2. **Require a status query by compensation key for every refundable or cancellable target (§4).** Without it, every ambiguous compensation becomes a choice between a possible double refund and a possible missed refund.
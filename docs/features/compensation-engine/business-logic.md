Compensating only steps marked `SUCCEEDED` leaves behind the effects that are most likely to exist and least likely to be noticed:
- the **failing step itself**, when its "failure" was a timeout after it actually executed;
- steps still **in flight** (dispatched, retrying, or unknown) when compensation began, which complete *afterwards*;
- **late successes** from parallel branches.

Your "no succeeded steps → FAILED, nothing to compensate" branch is the worst case. A saga whose only step timed out after charging the card is closed as FAILED with the charge still in place. [Certain]

Compensation must start from **every step whose forward outcome isn't proven not-executed**. Steps whose outcome is still open get resolved *before* the compensation plan is built.

Four more errors, ranked by damage:

**1. Discovering the pivot mid-loop means the engine entered compensation it should never have entered.** [Certain]
I disagree because once a pivot (final settlement, shipment) has succeeded, the saga can only move **forward**. Steps *before* the pivot must not be undone either, or you get a settled payment for an order whose inventory you just released.
Here's what I'd do instead: check the pivot at **entry**. If it was reached, take the roll-forward path; compensation is never built.

**2. "Inverted parameters" is not how business reversal works.** [Certain]
I disagree because a refund isn't "charge with amount × −1." It needs the **capture reference from the forward step's output**, the amount actually captured (partial captures, fees, FX), and the provider's refund rules.
Here's what I'd do instead: compensation inputs come from the **recorded forward outcome**, never from inverting forward inputs.

**3. A 409 response treated as "already compensated" can mean the opposite.** [Certain]
I disagree because a 409 frequently means "can't cancel: already shipped / already checked in / window closed." Treating it as success leaves the effect in place and closes the saga as compensated.
Here's what I'd do instead: workers return explicit outcomes. ALREADY_COMPENSATED, NOTHING_TO_COMPENSATE, and NOT_COMPENSABLE are different results with different actions.

**4. Strict sequential reverse with stop-on-failure, ending in a terminal STUCK, freezes everything behind one failure.** [Certain]
I disagree because one refund stuck at a provider blocks releasing an unrelated hotel hold, which then may expire with fees.
Here's what I'd do instead: compensate in **dependency order** (independent compensations run concurrently). A failed compensation escalates **that step**, others continue, and the saga stays in a non-terminal NEEDS_ATTENTION state with defined resolutions.

The missing-compensation-definition check also belongs at definition registration. By runtime, the forward effects already exist.

```
COMPENSATION ENGINE — BUSINESS LOGIC DECISION TREE (v2)
Legend: [C] Certain   [L] Likely (sound default)   [G] Guessing (business must decide)
│
├── 0. ENTRY DECISION
│   └── Trigger: pre-pivot permanent failure | customer/operator cancel | saga deadline | parallel group failure
│       ├── Check 0.1: Has the PIVOT step succeeded (or is its outcome UNKNOWN)?
│       │   ├── Succeeded → NO compensation → ROLL_FORWARD path (retry remaining steps / business escalation)
│       │   ├── UNKNOWN → resolve pivot outcome first (status query, provider records) → re-evaluate
│       │   └── Not reached / proven not executed → continue
│       ├── Check 0.2: Is compensation already in progress or finished for this saga?
│       │   ├── Yes → merge trigger (record reason); do not start a second run
│       │   └── No → enter COMPENSATING (§2)
│       └── Edge cases
│           ├── E0.1 Cancel arrives after pivot — compensation of pre-pivot steps on a settled payment — CORRUPT [C]
│           │   └── Handling: post-pivot cancel = separate business process (refund/return), defined per saga type [G]
│           └── E0.2 Two triggers at once (failure + deadline) — two compensation runs — double refunds [C]
│
├── 1. INVARIANTS
│   ├── CE-01 Compensation never starts while any relevant forward outcome is unresolved [C]
│   ├── CE-02 Compensation never crosses a succeeded pivot [C]
│   ├── CE-03 Compensation inputs come from recorded forward OUTCOMES, not inverted forward inputs [C]
│   ├── CE-04 Every compensation has a stable idempotency key: (saga, step, forward execution id, "compensate") [C]
│   ├── CE-05 Ambiguous compensation outcomes are verified before re-sending [C]
│   ├── CE-06 Workers return explicit compensation outcomes; protocol status alone is never trusted [C]
│   ├── CE-07 A compensation failure escalates that step only; independent compensations continue [C]
│   ├── CE-08 Late forward successes are always compensated, even after the saga closed [C]
│   ├── CE-09 Every compensation definition exists and is validated at definition registration [C]
│   └── CE-10 The final outcome records what could NOT be reversed (residuals) [C]
│
├── 2. SINGLE ENTRY & OWNERSHIP
│   ├── Transition RUNNING/NEEDS_ATTENTION → COMPENSATING by conditional update (expected state + owner epoch)
│   │   in the saga database — 0 rows → another owner handled it → stop
│   ├── Ownership/fencing enforced where the state lives (database), not only in an external lock
│   ├── Record trigger reason(s), triggering actor, time
│   └── Edge cases
│       ├── E2.1 Redis lease expires, second worker starts compensating — duplicate compensations [C]
│       │   └── Handling: fencing epoch checked on every DB transition and carried in compensation commands
│       └── E2.2 Lock acquired but DB transition fails — engine believes it owns compensation while saga is RUNNING [C]
│
├── 3. PRE-COMPENSATION RESOLUTION  (CE-01)
│   └── For every forward step / branch
│       ├── PENDING (never dispatched) → NOT_EXECUTED (cancel any scheduled dispatch/timer)
│       ├── RETRY_SCHEDULED → cancel timer → previous attempt may have executed → status query → EXECUTED | NOT_EXECUTED
│       ├── DISPATCHED (awaiting reply) → send cancel request (idempotent) and/or wait for reply until bounded deadline
│       │   ├── Reply SUCCEEDED / EXECUTED_DESPITE_CANCEL → EXECUTED
│       │   ├── CANCELLED / NOT_EXECUTED → NOT_EXECUTED
│       │   └── No answer → status query → still unknown → step UNKNOWN
│       ├── UNKNOWN → resolution ladder (status query → same-key probe → provider records)
│       ├── SUCCEEDED → EXECUTED
│       ├── REJECTED / FAILED_PERMANENT with "not executed" contract → NOT_EXECUTED
│       └── Any step remaining UNKNOWN after budget → saga NEEDS_ATTENTION (compensation of others may proceed
│           only if they don't depend on the unknown step — §5 dependencies)
│   └── Edge cases
│       ├── E3.1 Compensating while forward command still in flight — compensation arrives first, worker rejects it,
│       │         forward then executes — orphan effect [C]
│       ├── E3.2 "Failed" step that timed out after executing — excluded from compensation — customer charged — LOSS [C]
│       └── E3.3 Parallel branch still running when sibling failed — branch completes later — must be included [C]
│
├── 4. COMPENSATION SET  (per step, from resolved forward state and effect type)
│   ├── EXECUTED + COMPENSABLE → COMPENSATE
│   ├── EXECUTED + HOLD → RELEASE (or verify already expired → NOTHING_TO_COMPENSATE)
│   ├── EXECUTED + READ-ONLY / no side effect → SKIP
│   ├── EXECUTED + IRREVERSIBLE (should not exist pre-pivot; definition error) → RESIDUAL + NEEDS_ATTENTION
│   ├── PARTIALLY EXECUTED (2 of 3 items) → COMPENSATE actual recorded outcome only
│   ├── NOT_EXECUTED → nothing
│   ├── PIVOT / post-pivot → never in set (CE-02)
│   ├── SKIPPED by operator with recorded effect → per recorded effect
│   └── Edge cases
│       ├── E4.1 Hold released twice (explicit release + expiry) — supplier error on second — treat "not found" as released [L]
│       └── E4.2 Partial forward effect compensated as full — refunding more than captured — LOSS [C]
│
├── 5. COMPENSATION PLAN
│   ├── 5.1 Order
│   │   ├── Default: reverse order of forward completion
│   │   ├── Declared dependencies override default (e.g., reverse transfer before releasing source funds)
│   │   ├── Independent compensations (parallel siblings, unrelated resources) run concurrently
│   │   └── Holds released early (cheap, time-sensitive) unless a dependency forbids
│   ├── 5.2 Inputs (CE-03)
│   │   ├── Forward outcome references (booking id, capture id, reservation id, actual amount, currency, fees)
│   │   ├── Original customer/business context needed by target (reason codes)
│   │   └── Never "negated" inputs
│   ├── 5.3 Keys (CE-04) and deadlines per compensation (refund windows, free-cancellation cutoffs)
│   ├── 5.4 Definition validation happens at registration (CE-09); runtime missing definition = engine bug → NEEDS_ATTENTION
│   └── Edge cases
│       ├── E5.1 Strict reverse order when dependency requires different order — inconsistent intermediate state [C]
│       ├── E5.2 Refund computed from requested amount instead of captured amount — wrong refund — CORRUPT financial [C]
│       └── E5.3 FX changed since forward step — refund in original currency vs settlement currency ambiguity [G: business rule]
│
├── 6. EXECUTION & OUTCOMES  (per compensation)
│   ├── Dispatch via the orchestrator outbox with compensation key; same command topic/key as the forward command
│   │   (ordering with forward command of the same step)
│   ├── Outcome contract (CE-06)
│   │   ├── COMPENSATED → record (with details: amount refunded, fee charged) → next per plan
│   │   ├── ALREADY_COMPENSATED (same compensation key seen) → treat as COMPENSATED (record original result)
│   │   ├── NOTHING_TO_COMPENSATE (forward never took effect at target / hold expired) → record → next
│   │   ├── NOT_COMPENSABLE (shipped, checked in, consumed, window closed) → business alternative (§8)
│   │   ├── REJECTED (business rule at target) → business alternative (§8)
│   │   ├── PARTIALLY_COMPENSATED (refund only part, fee retained) → record residual → continue
│   │   ├── RETRYABLE (target overloaded, not executed) → retry with backoff (same key)
│   │   └── AMBIGUOUS (timeout, 5xx, reset after send) → status query by compensation key → never blind re-send (CE-05)
│   └── Edge cases
│       ├── E6.1 409 treated as "already compensated" while meaning "cannot cancel" — effect remains — CORRUPT [C]
│       ├── E6.2 Refund retried after timeout without status query — double refund — LOSS [C]
│       └── E6.3 Compensation succeeds at target but reply lost — retry with same key returns original result [C]
│
├── 7. CONFLICTS WITH LATER CHANGES
│   ├── Before compensating, target checks current resource state against the forward outcome
│   ├── Cases
│   │   ├── Resource modified after forward step (customer changed dates, support partially refunded manually)
│   │   │   → compensate the delta still attributable to this saga; record manual actions as residual context
│   │   ├── Resource consumed by another process (reserved stock later picked/shipped)
│   │   │   → NOT_COMPENSABLE → business alternative
│   │   └── Resource already reversed by another saga/process → NOTHING_TO_COMPENSATE (verify reference)
│   └── E7.1 Blind compensation overwrites later legitimate changes (cancels a booking the customer re-confirmed) — CORRUPT [C]
│
├── 8. FAILURE, ALTERNATIVES & ESCALATION  (CE-07)
│   ├── Per-step escalation: step compensation state = NEEDS_ATTENTION; other compensations continue if independent
│   ├── Business alternatives (declared per step type) [G]
│   │   ├── Refund not possible → store credit / voucher / bank transfer via finance process
│   │   ├── Cancellation not possible → resell / partial refund / customer offered alternative
│   │   └── Each alternative is itself an idempotent, audited action
│   ├── Retry budgets for RETRYABLE/AMBIGUOUS outcomes: long; budgets frozen while target dependency is down
│   ├── Human task: full evidence (forward outcome, compensation attempts, target responses, deadlines, customer impact)
│   ├── Saga state while any step NEEDS_ATTENTION: COMPENSATING_NEEDS_ATTENTION (non-terminal)
│   └── Edge cases
│       ├── E8.1 One stuck refund blocks releasing other holds — holds expire with fees or block inventory [C]
│       ├── E8.2 Terminal "STUCK" state — no further automated handling, late successes ignored — LOSS [C]
│       └── E8.3 Escalation without deadline information — human acts after refund window closes [C]
│
├── 9. LATE FORWARD REPLIES & REOPENING  (CE-08)
│   ├── Late SUCCEEDED for a step resolved NOT_EXECUTED or excluded → add to set → compensate
│   ├── Saga already terminal (COMPENSATED) → governed reopen: COMPENSATING (that step only) → record reason
│   ├── Late SUCCEEDED for a pivot after compensation completed — critical: pivot executed, pre-pivot undone
│   │   → NEEDS_ATTENTION (business decision: re-execute pre-pivot steps or reverse pivot via business process)
│   └── E9.1 Late booking confirmation after saga compensated and customer notified — double booking billed — CORRUPT [C]
│
├── 10. DEADLINES & COST-AWARE URGENCY
│   ├── Each compensation carries its business deadline (free cancellation until T, refund allowed until T)
│   ├── Plan prioritizes by earliest deadline among independent compensations
│   ├── Cost changes over time (cancellation fee after T) recorded in residuals if deadline missed
│   └── E10.1 Compensations queued behind a slow one miss free-cancellation windows — avoidable fees — LOSS [C]
│
├── 11. COMPLETION OUTCOMES & COMMUNICATION  (CE-10)
│   ├── FULLY_COMPENSATED — every executed effect reversed with no residual
│   ├── COMPENSATED_WITH_RESIDUALS — reversed with fees, partial refunds, non-retractable notifications, alternatives used
│   ├── PARTIALLY_COMPENSATED_ESCALATED — some steps resolved by humans/alternatives; record final dispositions
│   ├── Emit outcome event via outbox with residual details (amounts, fees, alternatives)
│   ├── Customer communication reflects real outcome (refund amount, timing, fees) — from residual record
│   └── E11.1 "Full reversible consistency achieved" announced while fees/notifications remain — misleading customers and reports [C]
│
├── 12. TRIGGERS DURING COMPENSATION
│   ├── Repeat cancel / deadline / failure triggers → merged (idempotent)
│   ├── Operator pause → no new compensation dispatches; replies still recorded
│   ├── Operator force actions (mark compensated with evidence, choose alternative) → audited, second approver for money
│   └── E12.1 Operator marks step compensated without evidence while target still holds effect — LOSS [C]
│
├── 13. NESTED SAGAS & PARALLEL GROUPS
│   ├── Sub-saga step: compensation = trigger sub-saga's own compensation; wait for its final outcome
│   ├── Parallel groups: compensate executed branches concurrently (independent), after their forward outcomes are final
│   └── E13.1 Parent marks compensated while sub-saga still compensating — premature completion [C]
│
└── 14. FEATURE TEST SCENARIOS
    ├── Only step 1 (payment) timed out after charging → resolved EXECUTED → refunded (not "FAILED, nothing to do")
    ├── Pivot succeeded, then cancel → no compensation; post-pivot business process invoked
    ├── Step 3 in flight when step 4 rejected → cancel request → executed despite cancel → compensated
    ├── Refund uses capture id and captured amount (partial capture) from forward outcome
    ├── 409 "already shipped" → NOT_COMPENSABLE → alternative offered; saga not marked compensated
    ├── Refund timeout → status query → found refunded → no second refund
    ├── One refund stuck → hotel hold still released; saga COMPENSATING_NEEDS_ATTENTION
    ├── Late booking success after saga COMPENSATED → reopened → booking cancelled
    ├── Free-cancellation deadline approaching → that compensation prioritized
    ├── Customer modified booking after forward step → compensation applies only saga's attributable part
    ├── Duplicate cancel triggers → single compensation run
    ├── Two engine instances race → fencing epoch allows one
    ├── Sub-saga compensation awaited before parent completes
    └── Final event and customer message include fees/residuals
```

[Certain] The core corrections follow from saga semantics:
- "Failed" and "in flight" steps can still have executed.
- Compensation cannot cross a pivot.
- Business reversals need the forward outcome, not negated inputs.
- 409 responses are ambiguous.
- Strict sequential compensation lets one failure block unrelated reversals.
- Terminal stuck states drop late successes.

[Likely] Hold-release behavior on already-expired holds varies by supplier.

[Guessing] Business alternatives when compensation isn't possible (vouchers, partial refunds, resell), FX handling, and post-pivot cancellation processes are business decisions per saga type.

Two decisions are yours:
1. **Make pre-compensation resolution mandatory (§3).** No compensation plan is built while any forward outcome is unknown or in flight. This closes the most expensive class of saga bugs: charged-but-cancelled.
2. **Define the explicit compensation outcome contract with every target service (§6), plus a business alternative for NOT_COMPENSABLE per step (§8).** Without those, the engine has to guess at 409s and escalate every non-reversible case to a human with no predefined answer.
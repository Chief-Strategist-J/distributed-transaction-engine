# Parallel Execution — Business Logic Decision Tree

```
PARALLEL EXECUTION (FORK / JOIN) — BUSINESS LOGIC DECISION TREE (v2)
Legend: [C] Certain   [L] Likely (sound default)   [G] Guessing (business must decide)
│
├── 0. BUSINESS MODEL OF A PARALLEL GROUP  (decided per group at design time)
│   │
│   ├── 0.1 Branch role
│   │   ├── REQUIRED      — group cannot succeed without it (flight)
│   │   ├── OPTIONAL      — nice to have; failure does not fail the group (insurance add-on)
│   │   └── ALTERNATIVE   — member of an alternative set; one success is enough (hotel A | hotel B | hotel C)
│   │
│   ├── 0.2 Branch effect type
│   │   ├── HOLD          — tentative reservation with expiry; released automatically if not confirmed
│   │   ├── COMPENSABLE   — real effect that has a defined business reversal (cancel booking with full refund)
│   │   └── IRREVERSIBLE  — cannot be reversed (non-refundable purchase, payout, email) → NEVER inside a group that can fail
│   │
│   ├── 0.3 Join policy
│   │   ├── ALL_REQUIRED  — every REQUIRED branch succeeds (+ each ALTERNATIVE set has ≥ 1 success)
│   │   ├── FIRST_SUCCESS — first success wins; remaining branches cancelled/released (price shopping, failover suppliers)
│   │   ├── N_OF_M        — at least N successes (multi-supplier fulfilment, quorum checks)
│   │   └── ALL_SETTLED   — wait for every branch to reach a final outcome; decide on the combined result
│   │
│   ├── 0.4 Failure policy (when join policy cannot be met)
│   │   ├── RELEASE_AND_FAIL     — release holds / compensate successes → group FAILED
│   │   ├── TRY_ALTERNATIVES     — dispatch next alternative before failing
│   │   ├── PARTIAL_ACCEPT       — accept what succeeded if business allows (customer confirmation may be required)
│   │   └── ROLL_FORWARD         — only when an irreversible effect already happened (should not occur if 0.2 respected)
│   │
│   └── 0.5 Recommended pattern for multi-party bookings: RESERVE-THEN-CONFIRM [C]
│       ├── Phase A (parallel): HOLD every required/alternative resource with expiry ≥ group deadline + margin
│       ├── Join A: all required holds succeeded?
│       │   ├── YES → Phase B
│       │   └── NO → release successful holds (or let them expire) → TRY_ALTERNATIVES / RELEASE_AND_FAIL
│       ├── Phase B: (payment authorization/capture as its own step, then) CONFIRM holds (parallel or sequential)
│       └── Confirm failures after payment → business process (rebooking/refund), never silent rollback
│
├── 1. INVARIANTS
│   ├── PX-01 Join decision is derived from durable branch states, evaluated in the same transaction as each branch outcome [C]
│   ├── PX-02 Each branch has its own stable idempotency key: (saga, group, branch, logical execution) [C]
│   ├── PX-03 Fork is atomic: group + all branch records + all dispatch commands created together, or none [C]
│   ├── PX-04 Timeout / lost reply = UNKNOWN, resolved before any join or compensation decision that depends on it [C]
│   ├── PX-05 Cancel is a request, not a fact; only never-dispatched branches are NOT_EXECUTED [C]
│   ├── PX-06 Join is decided exactly once; later replies never re-decide it, they trigger follow-up handling [C]
│   ├── PX-07 Branches are independent: no branch reads another branch's output or competes for the same resource [C]
│   ├── PX-08 Irreversible effects never run inside a group that can fail [C]
│   └── PX-09 Branch outputs are namespaced; aggregation is explicit and validated after join [C]
│
├── 2. DEFINITION VALIDATION  (before the group can be used)
│   ├── Every branch: role, effect type, timeout, retry policy, cancel capability, compensation/release action
│   ├── ALTERNATIVE sets: ordering (preference) and whether alternatives run concurrently or sequentially on failure
│   ├── Independence check (PX-07)
│   │   ├── No input binding from a sibling's output
│   │   ├── No shared consumable resource (same credit line, same inventory pool, same budget) across siblings
│   │   └── Hidden dependency examples to reject: hotel dates depend on confirmed flight times; insurance price depends on trip total
│   ├── Effect check (PX-08): any IRREVERSIBLE branch → move out of group (after join) or redesign as HOLD
│   ├── HOLD expiry ≥ group deadline + confirmation time + safety margin [L]
│   ├── Max branches per group [L: e.g., ≤ 50 direct; larger fan-outs batched]
│   └── Edge cases
│       ├── E2.1 Two branches debit the same wallet concurrently — overdraft or one rejected — group logic wrong [C]
│       ├── E2.2 Branch secretly needs sibling output → reads stale/default → wrong booking [C]
│       └── E2.3 Hold expiry shorter than slowest sibling's timeout — first hold expires before join — confirm fails [C]
│
├── 3. FORK
│   ├── 3.1 Preconditions
│   │   ├── Previous step/group fully resolved (no UNKNOWN upstream)
│   │   ├── Inputs for every branch resolved from saga inputs / earlier steps (frozen at fork time)
│   │   └── Group deadline computed (now + group budget, capped by saga deadline)
│   ├── 3.2 Atomic fork (PX-03) — one transaction
│   │   ├── Create group record: policy, deadline, state = FANOUT_ACTIVE
│   │   ├── Create branch records: role, effect type, idempotency key, frozen inputs, state = PENDING
│   │   ├── For each branch to start now: state = DISPATCHED + dispatch command (outbox) + branch timeout timer
│   │   │   (FIRST_SUCCESS with sequential alternatives: dispatch first preference only)
│   │   └── Group deadline timer
│   └── Edge cases
│       ├── E3.1 Branches spawned as in-process goroutines — orchestrator crash loses them — group stuck [C]
│       ├── E3.2 Fork partially committed (some branch commands sent, others not) — join waits forever [C]
│       └── E3.3 Fork retried after crash creates a second set of branches — double bookings [C]
│           └── Handling: group identity = (saga, group definition id); creation idempotent
│
├── 4. BRANCH STATE MACHINE
│   ├── PENDING → DISPATCHED → SUCCEEDED | REJECTED | FAILED_RETRYABLE | UNKNOWN
│   ├── FAILED_RETRYABLE → RETRY_SCHEDULED → DISPATCHED (same idempotency key)
│   ├── UNKNOWN → (resolved by status query / same-key retry) SUCCEEDED | NOT_EXECUTED | REJECTED | NEEDS_ATTENTION
│   ├── PENDING → NOT_EXECUTED (cancelled before dispatch)
│   ├── DISPATCHED/RETRY_SCHEDULED/UNKNOWN → CANCEL_REQUESTED → CANCELLED (confirmed not executed) |
│   │                                                          EXECUTED_DESPITE_CANCEL (→ needs release/compensation) |
│   │                                                          CANCEL_UNKNOWN (→ resolve)
│   ├── SUCCEEDED → RELEASE_PENDING / COMPENSATION_PENDING → RELEASED / COMPENSATED | RELEASE_FAILED / COMPENSATION_FAILED
│   └── SUCCEEDED → CONFIRMED (reserve-then-confirm phase B)
│
├── 5. BRANCH OUTCOME HANDLING  (each in one transaction with join evaluation, PX-01)
│   ├── SUCCEEDED → record namespaced output → evaluate join (§6)
│   ├── REJECTED (business: sold out, price changed, not eligible)
│   │   ├── REQUIRED → join cannot be met unless alternative exists → evaluate join / failure policy
│   │   ├── OPTIONAL → recorded; group unaffected
│   │   └── ALTERNATIVE → next alternative dispatched (if sequential) or set evaluated (if concurrent)
│   ├── FAILED_RETRYABLE → retry within branch budget and group deadline; else treat as REJECTED-equivalent for join
│   │   (but only if worker confirms NOT executed; otherwise UNKNOWN)
│   ├── UNKNOWN (timeout, lost reply, ambiguous error)
│   │   ├── Resolve: status query by idempotency key → same-key retry → provider records → NEEDS_ATTENTION
│   │   └── Join evaluation treats UNKNOWN as "not yet final" (PX-04)
│   └── Edge cases
│       ├── E5.1 Price changed on hold (supplier returns different price) — treat as REJECTED or as SUCCEEDED-with-condition
│       │         per business rule; never auto-accept higher price [G: business rule]
│       ├── E5.2 Partial success inside a branch (2 of 3 rooms held) — branch must report atomic outcome or explicit partial;
│       │         partial treated per business rule [G]
│       └── E5.3 Branch succeeds after its own timeout fired — reply applies if branch still UNKNOWN; else late-reply rules (§9)
│
├── 6. JOIN EVALUATION  (decided once, PX-06)
│   ├── Always computed from all branch states under a lock on the group row (serializes concurrent replies)
│   │
│   ├── ALL_REQUIRED
│   │   ├── Every REQUIRED SUCCEEDED and every ALTERNATIVE set has ≥ 1 SUCCEEDED → JOIN_SUCCEEDED
│   │   ├── Any REQUIRED final-negative (REJECTED / confirmed NOT_EXECUTED after retries) and no alternative → JOIN_FAILED
│   │   ├── Any ALTERNATIVE set exhausted (all members final-negative) → JOIN_FAILED
│   │   └── Otherwise → keep waiting (includes any UNKNOWN on a branch that matters)
│   │
│   ├── FIRST_SUCCESS
│   │   ├── First SUCCEEDED → JOIN_SUCCEEDED (winner recorded) → cancel/release all others (§7)
│   │   ├── All final-negative → JOIN_FAILED
│   │   └── Two successes arrive together → lock decides winner by arrival order or preference rank (declared) → loser released
│   │
│   ├── N_OF_M
│   │   ├── Successes ≥ N → JOIN_SUCCEEDED → extra in-flight branches: cancel or keep per policy (over-fulfilment rule)
│   │   ├── Successes + (still possible) < N → JOIN_FAILED early (no need to wait)
│   │   └── Otherwise wait
│   │
│   ├── ALL_SETTLED
│   │   ├── Wait until every branch final (UNKNOWN must resolve) or group deadline
│   │   └── Business decision function on the combined outcome (accept / partial accept / fail)
│   │
│   ├── On JOIN_SUCCEEDED
│   │   ├── Validate combined result (§11) → if invalid → treat as JOIN_FAILED with release
│   │   └── Advance to next step/phase (e.g., payment, then confirm holds)
│   │
│   └── Edge cases
│       ├── E6.1 Two replies processed concurrently each see "last one pending" — join decided twice — double advance [C]
│       │   └── Handling: group row lock + join decision recorded once (state transition FANOUT_ACTIVE → JOINED_*)
│       ├── E6.2 Join succeeds while an OPTIONAL branch is still UNKNOWN — proceed; resolve optional later (release if executed and unwanted)
│       └── E6.3 Group deadline reached with REQUIRED branch UNKNOWN — cannot decide safely → resolve UNKNOWN first
│                 (extend deadline within hold expiry) or NEEDS_ATTENTION
│
├── 7. SHORT-CIRCUIT & SIBLING CANCELLATION
│   ├── When to short-circuit
│   │   ├── ALL_REQUIRED: a REQUIRED branch final-negative with no alternative → stop siblings
│   │   ├── FIRST_SUCCESS: winner found → stop the rest
│   │   └── Never short-circuit on UNKNOWN (PX-04)
│   ├── Per sibling state
│   │   ├── PENDING (never dispatched) → NOT_EXECUTED (no remote action)
│   │   ├── RETRY_SCHEDULED → cancel the retry timer → but previous attempt may have executed → status query → NOT_EXECUTED | release
│   │   ├── DISPATCHED / UNKNOWN → send cancel request (idempotent) → outcomes:
│   │   │   ├── CANCELLED (not executed) → done
│   │   │   ├── EXECUTED_DESPITE_CANCEL → release hold / compensate
│   │   │   └── CANCEL_UNKNOWN → resolve via status query
│   │   └── SUCCEEDED → release (HOLD) or compensate (COMPENSABLE)
│   ├── Worker without cancel capability → wait for outcome, then release/compensate
│   └── Edge cases
│       ├── E7.1 Local context cancelled, remote booking completes later — marked ABORTED — orphan booking + charge [C]
│       ├── E7.2 Cancel request overtakes the dispatch command (different paths) — worker cancels nothing, then executes [C]
│       │   └── Handling: worker records "cancelled key" tombstone; later command with that key is refused
│       └── E7.3 Hold released while customer is still being shown the offer — UI shows unavailable item [L]
│
├── 8. GROUP FAILURE → RELEASE / COMPENSATION
│   ├── 8.1 Set to undo
│   │   ├── Every branch SUCCEEDED or EXECUTED_DESPITE_CANCEL or UNKNOWN-resolved-as-executed
│   │   ├── Exclude NOT_EXECUTED, CANCELLED, REJECTED
│   │   └── Plus earlier saga steps per saga failure policy (group failure ≠ automatic saga failure if alternatives exist)
│   ├── 8.2 Order
│   │   ├── Siblings undone in parallel (they are independent by PX-07)
│   │   ├── Group's undo completes before undoing steps that ran BEFORE the group
│   │   └── HOLD branches: explicit release preferred; expiry is the safety net, not the plan
│   ├── 8.3 Undo outcomes per branch: RELEASED/COMPENSATED | REJECTED (e.g., cancellation window passed) | UNKNOWN
│   │   └── REJECTED or exhausted → NEEDS_ATTENTION (customer-impacting); group stays FAILED_PENDING_UNDO
│   └── Edge cases
│       ├── E8.1 Compensating a branch still in flight — compensation arrives before booking — supplier rejects compensation,
│       │         booking then completes [C]
│       │         └── Handling: never compensate a branch until its forward outcome is final
│       ├── E8.2 Second branch fails while first undo is running — undo set recomputed under group lock [C]
│       └── E8.3 Compensation has a cost (cancellation fee) — business must know before choosing RELEASE_AND_FAIL vs PARTIAL_ACCEPT [G]
│
├── 9. LATE REPLIES  (after the join was decided, PX-06)
│   ├── Group JOIN_SUCCEEDED
│   │   ├── Late duplicate SUCCEEDED of a counted branch → ignore (same key)
│   │   ├── Late SUCCEEDED of a cancelled/losing branch (FIRST_SUCCESS, N_OF_M over-fulfilment) → release/compensate it
│   │   └── Late REJECTED of an already-SUCCEEDED branch → contradiction → NEEDS_ATTENTION
│   ├── Group JOIN_FAILED (undo in progress or done)
│   │   ├── Late SUCCEEDED → add to undo set → release/compensate
│   │   └── Late REJECTED → record only
│   └── Edge cases
│       ├── E9.1 Late success after group fully undone and saga closed — reopen undo for that branch [C]
│       └── E9.2 Late success arrives after hold already expired at supplier — release returns "not found" → treat as released [L]
│
├── 10. TIMEOUTS & DEADLINES
│   ├── Branch timeout → UNKNOWN (not failure)
│   ├── Group deadline (business: how long the customer/offer can wait)
│   │   ├── Pre-join: stop dispatching new alternatives; cancel in-flight; resolve UNKNOWNs; then fail or partial-accept
│   │   └── Deadline must be < shortest HOLD expiry among succeeded branches (holds must still be valid at confirm)
│   ├── Hold expiry tracking per branch → confirm phase rejects expired holds → re-hold or fail per policy
│   └── Edge cases
│       ├── E10.1 Slow sibling delays join until first hold expires — confirm fails — group fails late [C]
│       │   └── Handling: join deadline derived from earliest hold expiry, not a fixed timeout
│       └── E10.2 Retry of slow branch continues after group deadline — executes after group failed — orphan booking [C]
│           └── Handling: commands carry valid_until = group deadline; workers refuse expired commands
│
├── 11. OUTPUT AGGREGATION & POST-JOIN VALIDATION  (PX-09)
│   ├── Outputs stored per branch (flight.confirmation, hotel.confirmation) — never merged into one flat map
│   ├── Aggregation is explicit and deterministic (preference order for alternatives; stable ordering for lists)
│   ├── Post-join business validation on the combined result
│   │   ├── Consistency: hotel dates cover flight arrival/departure; same traveller details; same currency or converted
│   │   ├── Totals: combined price ≤ quoted/approved budget; price change tolerance per business rule
│   │   └── Policy: customer reconfirmation required if total changed beyond threshold [G]
│   └── Edge cases
│       ├── E11.1 Two branches return the same output field name — overwrite — wrong confirmation shown to customer [C]
│       └── E11.2 Each branch valid, combination invalid (dates mismatch) — join must fail or ask customer [C]
│
├── 12. CONCURRENCY HAZARDS SPECIFIC TO FORK/JOIN
│   ├── Duplicate replies (at-least-once) → branch idempotent transition (state already final → no-op)
│   ├── Simultaneous replies from different branches → serialized by group row lock (PX-01)
│   ├── Reply and timeout for same branch at once → compare-and-set on branch state; one wins
│   ├── Two orchestrator instances evaluating the same group → ownership/epoch check on saga
│   └── E12.1 Barrier counter (Redis DECR) incremented/decremented outside the branch transaction — premature or missing join [C]
│
├── 13. LARGE FAN-OUT  (tens to thousands of branches)
│   ├── Dispatch in waves with concurrency limit per downstream service (no burst of N commands at once)
│   ├── Join state maintained as counts per branch state in the group row, updated in the same transaction as each
│   │   branch transition (derived from durable state, not a separate counter)
│   ├── N_OF_M early decisions to avoid waiting for stragglers
│   ├── Partial progress visible (x of N done)
│   └── Edge cases
│       ├── E13.1 1,000 simultaneous holds against one supplier — supplier rate limits — mass REJECTED misread as sold out [C]
│       │   └── Handling: rate-limited waves; distinguish RATE_LIMITED (retry) from SOLD_OUT (business rejection)
│       └── E13.2 Group row lock contention with many concurrent replies — throughput collapse [L]
│           └── Handling: batch reply application per group; or sharded counters reconciled under lock at decision time
│
├── 14. NESTED GROUPS
│   ├── A branch can itself be a sub-group (parallel inside parallel) or a sequential sub-flow
│   ├── Sub-group's final outcome (after its own join/undo) is the branch outcome for the parent
│   ├── Parent cancellation propagates as cancel requests into sub-groups (same PX-05 rules)
│   └── E14.1 Parent decides join while sub-group still undoing — parent outcome premature [C]
│       └── Handling: sub-group reports final outcome only after its undo completes
│
└── 15. FEATURE TEST SCENARIOS
    ├── Flight + hotel holds both succeed → join → payment → confirm both
    ├── Flight sold out (REQUIRED, no alternative) → hotel hold released → group FAILED, no charge
    ├── Hotel A sold out → hotel B (ALTERNATIVE) dispatched → succeeds → join succeeds
    ├── Insurance (OPTIONAL) fails → trip proceeds without insurance
    ├── Duplicate SUCCESS reply for flight → join not advanced early; hotel still awaited
    ├── Crash after flight SUCCEEDED commit → join still evaluated correctly on recovery
    ├── Hotel branch times out → UNKNOWN → status query finds booked → counted as success (no compensation)
    ├── Flight rejected while hotel in flight → cancel request → hotel executed despite cancel → hotel released
    ├── FIRST_SUCCESS among 3 suppliers → two late successes → both released
    ├── N_OF_M impossible after 3 rejections → early fail, in-flight branches cancelled
    ├── Slow branch delays join past earliest hold expiry → join deadline prevents confirm on expired hold
    ├── Combined validation fails (dates mismatch) → group fails with release; customer informed
    ├── Price increased on one hold beyond tolerance → customer reconfirmation path
    ├── Late success after group fully undone → that booking released
    └── 500-branch fan-out → waves respect supplier rate limits; RATE_LIMITED not treated as SOLD_OUT
```

# Manual Intervention — Business Logic Decision Tree

```
MANUAL INTERVENTION — BUSINESS LOGIC DECISION TREE (v2)
Legend: [C] Certain   [L] Likely (sound default)   [G] Guessing (business must decide)
│
├── 0. PRINCIPLES
│   ├── Establish facts first (what executed where), then decide (what the business wants)
│   ├── Humans decide what the system cannot; they never assert facts without evidence
│   ├── Every action is an engine transition: fenced, atomic with audit, dispatched only via outbox
│   └── No action fabricates data that downstream steps or customers will rely on
│
├── 1. INVARIANTS
│   ├── MI-01 Every intervention starts with PAUSE (fenced state transition); workers stop dispatching for that saga [C]
│   ├── MI-02 Outcome-changing actions require per-step outcome evidence (executed / not executed) [C]
│   ├── MI-03 No synthetic outputs; outputs come from the target system or verified records [C]
│   ├── MI-04 Compensation never skips an executed step and never crosses a succeeded pivot [C]
│   ├── MI-05 Money-affecting actions need a second approver who is not the requester [C]
│   ├── MI-06 Audit record commits in the same transaction as the state change [C]
│   ├── MI-07 One active intervention per saga; operator actions are idempotent by intervention id [C]
│   ├── MI-08 Termination requires a disposition for every executed effect [C]
│   └── MI-09 Retries use the original idempotency key; new keys only via corrective action [C]
│
├── 2. ENTRY
│   ├── 2.1 Eligible sagas
│   │   ├── NEEDS_ATTENTION (DLQ, unresolved unknown, compensation failure)
│   │   ├── RUNNING/WAITING beyond expected duration or deadline (stuck detection)
│   │   ├── COMPENSATING_NEEDS_ATTENTION
│   │   └── COMPLETED / COMPENSATED → no engine override; use business processes (refund, rebooking) as NEW sagas
│   ├── 2.2 PAUSE (MI-01)
│   │   ├── Conditional transition: current state + epoch → PAUSED_FOR_INTERVENTION (records previous state)
│   │   ├── Timers suspended; no new dispatches; in-flight replies still ingested and recorded
│   │   └── 0 rows → saga changed meanwhile → re-read and re-present
│   └── Edge cases
│       ├── E2.1 Intervention without pause — worker timer fires and retries while operator compensates — CORRUPT [C]
│       └── E2.2 Late reply arrives during pause — recorded; shown to operator before any action [C]
│
├── 3. AUTHORIZATION
│   ├── Matrix: action type × money at stake × tenant/region × saga type [G]
│   │   ├── PAUSE/RESUME, RETRY_STEP → operator
│   │   ├── RESOLVE_AS_EXECUTED / NOT_EXECUTED → operator + evidence; money ≥ threshold → second approver
│   │   ├── CORRECTIVE_ACTION, COMPENSATE, ROLL_FORWARD, CHOOSE_ALTERNATIVE → operator + second approver if money
│   │   ├── COMPLETE_MANUALLY → second approver always
│   │   └── TERMINATE_WITH_WRITE_OFF → finance approver above threshold
│   ├── Ticket reference validated against ticketing system (exists, open, linked to saga/customer)
│   ├── Self-approval forbidden (MI-05); approvals expire [L: hours]
│   ├── Operators scoped to tenants/regions they are allowed to act on
│   ├── Break-glass: personal, time-bound, post-review mandatory
│   └── Edge cases
│       ├── E3.1 Single admin role force-completes high-value saga with fake ticket — fraud/LOSS [C]
│       └── E3.2 Approval granted for one action reused for another — approval bound to exact action + parameters [C]
│
├── 4. FACT-FINDING  (mandatory before outcome-changing actions, MI-02)
│   ├── System assembles per step
│   │   ├── Engine view: state, attempts, outcome class, idempotency key, outputs
│   │   ├── Target view: status query by idempotency key (executed / not found / in progress)
│   │   ├── Provider/reconciliation view: references, settlement status, duplicate detection
│   │   └── Compensation view: compensation attempts and target status
│   ├── Per step verdict: EXECUTED | NOT_EXECUTED | IN_PROGRESS | UNDETERMINED
│   ├── UNDETERMINED blocks outcome-changing actions on that step (only PAUSE, RETRY status query, escalate)
│   ├── Operator may attach external evidence (provider support confirmation) → verdict recorded with evidence
│   └── Edge cases
│       ├── E4.1 Operator assumes failed step did nothing — skips its compensation — customer charged — LOSS [C]
│       └── E4.2 Status query hits lagging replica — false NOT_EXECUTED — duplicate on retry [C]
│
├── 5. ACTION CATALOG  (offered only when preconditions hold)
│   │
│   ├── 5.1 PAUSE / RESUME
│   │   └── Resume restores previous state; timers recalculated from now (not fired all at once)
│   │
│   ├── 5.2 RETRY_STEP
│   │   ├── Preconditions: verdict NOT_EXECUTED, or target idempotent by key; saga still expects step;
│   │   │   within business validity and provider idempotency window
│   │   ├── Effect: step RETRY_SCHEDULED with SAME key (MI-09); failure counter policy: new budget recorded explicitly
│   │   └── Dependency health check is advisory only (not evidence of step success)
│   │
│   ├── 5.3 RESOLVE_STEP_AS_EXECUTED
│   │   ├── Preconditions: verdict EXECUTED with evidence
│   │   ├── Output taken from target (status query result / provider record) — never typed in (MI-03)
│   │   └── Effect: step SUCCEEDED; saga continues (or completes) via normal transitions
│   │
│   ├── 5.4 RESOLVE_STEP_AS_NOT_EXECUTED
│   │   ├── Preconditions: verdict NOT_EXECUTED with evidence
│   │   └── Effect: step NOT_EXECUTED; follow-up decision required (retry, alternative, compensate, roll forward)
│   │
│   ├── 5.5 CORRECTIVE_ACTION (wrong inputs)
│   │   ├── Preconditions: original verdict NOT_EXECUTED (or original compensated first)
│   │   ├── Effect: original attempt SUPERSEDED (key retired); new command with new key and corrected inputs
│   │   └── Inputs validated against schema and business rules; approver (MI-05)
│   │
│   ├── 5.6 CHOOSE_ALTERNATIVE
│   │   ├── Only alternatives declared in saga definition (other supplier, voucher, partial fulfilment)
│   │   └── Executed as new steps with their own keys
│   │
│   ├── 5.7 COMPENSATE
│   │   ├── Preconditions: pivot not succeeded (MI-04); every step verdict EXECUTED or NOT_EXECUTED (no UNDETERMINED)
│   │   ├── Compensation set = all EXECUTED compensable steps INCLUDING the current one if executed
│   │   └── Effect: saga COMPENSATING via compensation engine (no "skip" of executed steps)
│   │
│   ├── 5.8 ROLL_FORWARD
│   │   ├── Preconditions: pivot succeeded
│   │   └── Effect: remaining steps retried / alternatives / business process; never compensation of pre-pivot steps
│   │
│   ├── 5.9 COMPLETE_MANUALLY
│   │   ├── Preconditions: every required effect verified EXECUTED with real outputs; nothing pending
│   │   ├── Use case: engine lost track but business outcome achieved
│   │   └── Effect: saga COMPLETED with "manually completed" marker; outputs from verified data
│   │
│   ├── 5.10 TERMINATE_WITH_WRITE_OFF
│   │   ├── Preconditions: every EXECUTED effect has a disposition — compensated, residual accepted, or written off (amount)
│   │   ├── Holds/reservations released (verified)
│   │   ├── Finance approval above threshold; customer communication prepared
│   │   └── Effect: saga TERMINATED_WITH_DISPOSITION (terminal) with full residual record
│   │
│   └── Edge cases
│       ├── E5.1 FORCE_COMPLETE with synthetic booking reference — later steps and customer use a fake reference — CORRUPT [C]
│       ├── E5.2 FORCE_TERMINATE leaving captured payment — money kept for cancelled order — LOSS [C]
│       ├── E5.3 Retry with "reset counter" but new key generated by tooling — double execution [C]
│       ├── E5.4 Compensate after pivot via manual action — settled payment with released inventory — CORRUPT [C]
│       └── E5.5 Operator types output JSON for a step — schema-valid but false — CORRUPT [C]
│
├── 6. PREVIEW & CONFIRMATION
│   ├── Before execution show: steps affected, commands to be sent, compensations to run, money movements,
│   │   customer-visible effects (emails/notifications), deadlines impacted
│   ├── Confirmation requires re-authentication for money actions
│   └── E6.1 Operator unaware that compensation will email the customer — unwanted communication [C]
│
├── 7. EXECUTION MECHANICS
│   ├── One transaction: verify paused state + epoch → apply transition → write audit record (MI-06) →
│   │   outbox commands/events → intervention record
│   ├── Intervention id makes the action idempotent (double-click, retried API call) (MI-07)
│   ├── No direct calls from tooling to target services or providers
│   ├── Saga leaves PAUSED_FOR_INTERVENTION only through the chosen action's transition
│   └── E7.1 Tooling calls provider API directly — action invisible to engine; reconciliation finds orphan [C]
│
├── 8. CONCURRENCY
│   ├── One active intervention per saga (intervention record unique on saga while open)
│   ├── Second operator sees "intervention in progress by X" with details
│   ├── Late replies during pause update facts → pending approvals invalidated if facts changed
│   └── E8.1 Approval given on facts that changed (late success arrived) — wrong action executed [C]
│       └── Handling: approval bound to a fact snapshot version; mismatch → re-approval required
│
├── 9. BULK INTERVENTIONS  (many sagas, same cause)
│   ├── Selection by fingerprint/cause; preview totals (sagas, money, customers)
│   ├── Each saga still passes its own preconditions and fact-finding
│   ├── Progressive execution (sample → batch → all) with stop-on-failure threshold
│   ├── Approval for the bulk action with explicit caps (count, money)
│   └── E9.1 Bulk force-complete across sagas with mixed real outcomes — some never executed — LOSS [C]
│
├── 10. DEADLINES & CUSTOMER COMMUNICATION
│   ├── Intervention queue sorted by business deadline and money at stake
│   ├── Customer messages derived from actual outcome and residuals (not from operator free text alone)
│   └── E10.1 Refund window passes while saga waits for approver — fee/loss [C]
│       └── Handling: approver escalation tied to deadlines
│
├── 11. AUDIT CONTENT  (MI-06)
│   ├── intervention id, saga id, action, parameters, operator, approver(s), ticket, reason
│   ├── fact snapshot (verdicts + evidence references) at approval and at execution
│   ├── previous state, new state, commands emitted, money affected
│   └── emitted as audit event via outbox (same transaction)
│
└── 12. FEATURE TEST SCENARIOS
    ├── Intervention without pause → impossible (tooling requires pause first)
    ├── Timed-out payment step verified EXECUTED → compensation includes it (no "skipped" charge)
    ├── Step verdict UNDETERMINED → compensate/complete blocked; only retry status/escalate offered
    ├── Complete manually with all outputs from target records → saga completed; no typed outputs accepted
    ├── Terminate with captured payment and no disposition → blocked
    ├── Retry step → same idempotency key; target returns original result if executed
    ├── Corrective action → original superseded, new key, approver required
    ├── Pivot succeeded → compensate option not offered; roll-forward offered
    ├── Requester tries to approve own action → rejected
    ├── Late success arrives after approval → approval invalidated, re-review required
    ├── Two operators open same saga → second blocked with details
    ├── Bulk action over 200 sagas → progressive, per-saga preconditions, caps enforced
    ├── Crash during intervention commit → no partial state; audit and state consistent
    └── Double-click on "execute" → single action (intervention id)
```

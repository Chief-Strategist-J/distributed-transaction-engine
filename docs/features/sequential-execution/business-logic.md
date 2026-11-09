# Sequential Execution — Business Logic Decision Tree

```
SEQUENTIAL SAGA EXECUTION — PRODUCTION SYSTEM DESIGN TREE (v2)
Legend  : [C] Certain   [L] Likely (sound default, tune it)   [G] Guessing (needs your inputs)
Severity: LOSS | CORRUPT | OUTAGE | DEGRADE | COMPLIANCE | SECURITY
Edge node format: Scenario → Impact → Handling → Detection
│
├── 0. SYSTEM CONTEXT & COMPONENT MAP
│   ├── C-ORC   Orchestrator service — stateless workers executing a persisted state machine
│   ├── C-SST   Saga store (PostgreSQL) — instances, steps, attempts, context entries, timers, ownership
│   ├── C-DEF   Definition registry — versioned saga definitions (steps, types, timeouts, schemas, policies)
│   ├── C-OBX   Orchestrator outbox — step commands and saga events published atomically with transitions
│   ├── C-CMD   Command topics — one per step-handler service (communication tree §5)
│   ├── C-INB   Reply consumer + inbox — idempotent reply ingestion keyed by (saga, step, attempt)
│   ├── C-TMR   Durable timer scheduler — step timeouts, retry backoffs, saga deadlines (DB-based, owner-executed)
│   ├── C-WRK   Step workers (other services) — idempotent by step idempotency key; expose status query by key
│   ├── C-CMP   Compensation engine — same state machine, compensation phase
│   ├── C-CTL   Operator console via control plane — cancel, pause, resume, retry, skip, force-resolve (audited)
│   ├── C-API   Client API — start saga (idempotent), status, cancel; notifications on completion
│   └── C-MON   Monitoring — states, stuck sagas, unknown outcomes, compensation failures, human tasks
│
├── 1. INVARIANTS  (with enforcement)
│   ├── SQ-01 No saga state lives only in memory [C]
│   │   └── Enforced by: every transition persisted before any side effect is triggered; workers stateless
│   ├── SQ-02 Every transition is a conditional update (compare-and-set) scoped by saga id, step, expected state, owner epoch [C]
│   │   └── Enforced by: state-machine library is the only writer; zero rows → abort and re-read
│   ├── SQ-03 A step command is dispatched only via outbox in the same tx as its DISPATCHED transition [C]
│   │   └── Enforced by: no direct RPC/Kafka calls from orchestrator code paths for step execution
│   ├── SQ-04 Every step execution carries a deterministic idempotency key; workers dedupe on it [C]
│   │   └── Enforced by: key = (saga id, step index, step definition id, logical execution number); attempts reuse it
│   ├── SQ-05 Timeout ≠ failure: unknown outcomes are resolved before any compensation decision [C]
│   │   └── Enforced by: UNKNOWN state with mandatory resolution path (§8)
│   ├── SQ-06 Every waiting state has a durable timer [C]
│   │   └── Enforced by: transition into any WAITING/DISPATCHED/RETRY state inserts a timer row in the same tx
│   ├── SQ-07 Steps are typed COMPENSABLE | PIVOT | RETRIABLE and ordered compensable → pivot → retriable [C]
│   │   └── Enforced by: definition validation at registration (C-DEF)
│   ├── SQ-08 After the pivot succeeds, the saga only moves forward [C]
│   │   └── Enforced by: compensation engine refuses to compensate pivot or post-pivot steps; failures → retry or human
│   ├── SQ-09 Compensation is idempotent, retried to completion or escalated, and only for steps whose effect is known or presumed executed [C]
│   │   └── Enforced by: compensation key per (saga, step); compensation phase state machine
│   ├── SQ-10 Late replies are handled by explicit policy per step [C]
│   │   └── Enforced by: reply handler for terminal/compensating sagas (§6.4)
│   ├── SQ-11 One owner per saga at a time [C]
│   │   └── Enforced by: ownership lease + epoch on saga row; transitions check epoch
│   ├── SQ-12 In-flight sagas run on the definition version they started with [C]
│   │   └── Enforced by: definition version pinned on instance; old versions retained until drained
│   ├── SQ-13 Step outputs are namespaced and immutable [C]
│   │   └── Enforced by: context entries keyed by (saga, step index, field); later steps read explicitly declared inputs
│   ├── SQ-14 Every human/operator action is authorized, reasoned, and audited [C]
│   │   └── Enforced by: C-CTL only path (authorization tree AZ-09; audit tree T-ADMIN)
│   └── SQ-15 One active saga per business key (when the business requires it) [C]
│       └── Enforced by: unique constraint on (saga type, business key) for non-terminal instances
│
├── 2. DATA MODEL  (C-SST, described in words)
│   ├── 2.1 saga_instance
│   │   ├── saga id, saga type, definition version, business key, tenant, correlation id
│   │   ├── state, current step index, phase (FORWARD | COMPENSATING | ROLL_FORWARD), pivot reached (bool)
│   │   ├── owner id, owner epoch, lease expiry
│   │   ├── created/updated times, saga deadline, cancel requested (bool + reason)
│   │   └── row version (optimistic concurrency)
│   ├── 2.2 saga_step
│   │   ├── (saga id, step index) unique; step definition id; type (COMPENSABLE/PIVOT/RETRIABLE)
│   │   ├── state; idempotency key; attempt count; last attempt id; last outcome class; reason codes
│   │   ├── dispatched at, resolved at, step deadline
│   │   └── compensation state; compensation key; compensation attempts
│   ├── 2.3 saga_step_attempt (append-only)
│   │   └── attempt id, step, dispatched at, outcome, reply id, error class (sanitized)
│   ├── 2.4 saga_context_entry
│   │   ├── (saga id, producer step index, name) unique; value (encrypted if PII); schema version; size
│   │   └── Large values → claim-check reference
│   ├── 2.5 saga_timer
│   │   └── (saga id, kind, step index, attempt) → due time (DB clock), owner partition, state
│   └── 2.6 saga_history (append-only)
│       └── every transition: from, to, cause (reply/timer/operator/cancel), actor, epoch, time
│
├── 3. STATE MACHINES
│   ├── 3.1 Saga states
│   │   ├── CREATED → RUNNING
│   │   ├── RUNNING → WAITING_STEP(i) → RUNNING (next step) | RESOLVING_UNKNOWN(i) | COMPENSATING | ROLL_FORWARD
│   │   ├── RESOLVING_UNKNOWN(i) → RUNNING | COMPENSATING | ROLL_FORWARD | NEEDS_ATTENTION
│   │   ├── COMPENSATING → COMPENSATED (terminal) | NEEDS_ATTENTION
│   │   ├── ROLL_FORWARD → COMPLETED (terminal) | NEEDS_ATTENTION
│   │   ├── RUNNING → COMPLETED (terminal)
│   │   ├── Any non-terminal → PAUSED → previous state (operator)
│   │   ├── Any non-terminal → CANCEL_REQUESTED → (pre-pivot) COMPENSATING | (post-pivot) ROLL_FORWARD or REJECT_CANCEL
│   │   └── NEEDS_ATTENTION → (operator decision) RUNNING | COMPENSATING | ROLL_FORWARD | COMPLETED_MANUALLY | ABANDONED_WITH_APPROVAL
│   │
│   ├── 3.2 Step states
│   │   ├── PENDING → DISPATCHED (attempt n) → SUCCEEDED | REJECTED | FAILED_RETRYABLE | FAILED_PERMANENT | UNKNOWN | INVALID_OUTPUT
│   │   ├── FAILED_RETRYABLE → RETRY_SCHEDULED → DISPATCHED (attempt n+1, same idempotency key)
│   │   ├── UNKNOWN → (resolved) SUCCEEDED | NOT_EXECUTED | FAILED_PERMANENT | still UNKNOWN → NEEDS_ATTENTION
│   │   ├── SKIPPED (operator, approved) | NOT_REACHED (saga ended before dispatch)
│   │   └── Compensation sub-states: NOT_REQUIRED | COMPENSATION_PENDING | COMPENSATING | COMPENSATED |
│   │                                COMPENSATION_FAILED | COMPENSATION_UNKNOWN
│   │
│   └── 3.3 Transition rules
│       ├── Every transition: conditional on (saga id, expected state, epoch, row version) — SQ-02
│       ├── Every transition writes history row + (if applicable) outbox command + timer rows in ONE tx
│       └── E3.1 Transition without expected-state condition — duplicate dispatch, lost replies — CORRUPT [C]
│
├── 4. DEFINITION & INITIALIZATION
│   ├── 4.1 Definition validation (at registration, not at runtime)
│   │   ├── Steps linear and non-empty; each step: handler, command type, input schema, output schema
│   │   ├── Input bindings: each input field bound to (initial payload field | output of step j < i); no implicit merging
│   │   ├── Types ordered: all COMPENSABLE before PIVOT; at most one PIVOT; RETRIABLE after PIVOT (SQ-07)
│   │   ├── Each COMPENSABLE step defines a compensation command + its timeout/retry policy
│   │   ├── Each step defines: timeout, retry policy, failure-class mapping, late-reply policy, status-query support
│   │   ├── Saga deadline and NEEDS_ATTENTION ownership (team/queue)
│   │   └── Edge cases
│   │       ├── E4.1a Irreversible step (email, payout) declared COMPENSABLE — fake compensation — CORRUPT [C]
│   │       ├── E4.1b Input bound to a field no prior step produces — runtime failure mid-saga after effects — CORRUPT [C]
│   │       ├── E4.1c Zero-step or single-step definition — orchestration overhead without value / degenerate paths untested [C]
│   │       └── E4.1d Worker without status-query support for a step with external effects — UNKNOWN unresolvable — OUTAGE (flows) [C]
│   │           └── Handling: registration rejects external-effect steps lacking status query or provider reconciliation
│   │
│   ├── 4.2 Instance creation (C-API)
│   │   ├── Client sends start with business key + client idempotency key
│   │   ├── Check: existing non-terminal saga for business key? (SQ-15)
│   │   │   ├── Same client idempotency key → return existing saga id (idempotent start)
│   │   │   ├── Different key → reject (409) or queue per business policy
│   │   │   └── None → create in one tx: instance (CREATED, pinned definition version), steps (PENDING), history,
│   │   │       first transition to RUNNING, first step dispatch (§5), deadline timer
│   │   ├── Initial payload validated against definition input schema before creation
│   │   ├── Authorization of the initiator (authorization tree §5) and tenant scoping
│   │   └── Edge cases
│   │       ├── E4.2a Client retries start after timeout without idempotency key — two sagas, double effects — CORRUPT [C]
│   │       ├── E4.2b Two sagas for same order (concurrent requests) — conflicting compensations — CORRUPT [C]
│   │       └── E4.2c Start accepted while target dependency is down — saga immediately enters retries — load on recovery [C]
│   │           └── Handling: admission check optional; retries bounded with jitter (§9)
│   │
│   └── 4.3 Admission & limits
│       ├── Per-saga-type concurrency caps; per-tenant caps (multi-tenant tree §7)
│       └── E4.3a Burst of 100k saga starts — command topics and workers flooded — DEGRADE/OUTAGE [L]
│
├── 5. STEP DISPATCH PROTOCOL
│   ├── 5.1 Steps (one tx)
│   │   ├── Verify ownership (epoch) and saga state RUNNING, step i PENDING or RETRY_SCHEDULED
│   │   ├── Resolve inputs from declared bindings (immutable context entries); validate against step input schema
│   │   │   └── Invalid → classify as DEFINITION/DATA defect → NEEDS_ATTENTION (no dispatch) (E5.1a)
│   │   ├── Transition step → DISPATCHED (attempt n, attempt id), saga → WAITING_STEP(i)
│   │   ├── Insert outbox command: idempotency key (SQ-04), attempt id, saga id, step index, inputs, deadline (valid_until),
│   │   │   reply-to, correlation/causation, trace context (tracing tree TR-02)
│   │   ├── Insert step timeout timer (SQ-06)
│   │   └── Commit
│   │
│   ├── 5.2 Sync vs async dispatch decision
│   │   ├── Default: async command + reply (survives orchestrator restarts; no thread held)
│   │   ├── Sync RPC only when: step reliably < 1–2 s, worker idempotent by key, and the orchestrator records
│   │   │   DISPATCHED before calling and persists outcome after (crash between = UNKNOWN path) [L]
│   │   └── E5.2a Sync call inside a DB transaction — long transactions, locks held across network — OUTAGE [C]
│   │
│   ├── 5.3 Crash-point matrix
│   │   ├── Crash before commit → nothing dispatched; timer absent; recovery re-evaluates step (still PENDING)
│   │   ├── Crash after commit, before relay publishes → outbox delivers later (at-least-once)
│   │   ├── Crash after worker executed, before reply consumed → reply arrives later → inbox applies
│   │   ├── Reply lost / worker crashed before replying → timeout timer fires → UNKNOWN resolution (§8)
│   │   └── DB failover during commit → ambiguous → recovery reads state; worker idempotency absorbs re-dispatch
│   │
│   └── Edge cases
│       ├── E5.1a Input validation fails at step i after steps 1..i−1 executed — mid-saga dead end — NEEDS_ATTENTION + compensation decision [C]
│       ├── E5.1b Command delivered to worker after its valid_until — worker rejects as EXPIRED (not executed) [C]
│       ├── E5.1c Worker receives same command twice (relay replay) — idempotency key absorbs (SQ-04) [C]
│       └── E5.1d New idempotency key generated per attempt — retries execute effect again — CORRUPT [C]
│
├── 6. REPLY HANDLING
│   ├── 6.1 Ingestion (C-INB): inbox keyed by reply id; correlate by (saga id, step index, attempt id); one tx with transition
│   ├── 6.2 Matching rules
│   │   ├── Reply for current step + current attempt → apply outcome (§7)
│   │   ├── Reply for current step + OLDER attempt → same idempotency key → treat as authoritative outcome of the
│   │   │   logical execution (worker deduped); apply if step not yet resolved, else record
│   │   ├── Reply for a step already resolved → duplicate → record only (verify consistent outcome; mismatch → incident)
│   │   ├── Reply for future step index (impossible) → INTEGRITY incident
│   │   └── Reply for unknown saga → quarantine (DLQ tree) + alert
│   ├── 6.3 Output handling
│   │   ├── Validate output against step output schema
│   │   │   ├── Valid → write context entries (namespaced, SQ-13) → SUCCEEDED
│   │   │   └── Invalid → INVALID_OUTPUT: effect happened but output unusable → NEEDS_ATTENTION (never treat as "not executed")
│   │   └── PII outputs encrypted (encryption tree); large outputs claim-checked
│   ├── 6.4 Late replies (SQ-10) — saga no longer waiting on this step
│   │   ├── Saga COMPENSATING and late reply says SUCCEEDED for a compensable step → add step to compensation set
│   │   ├── Saga COMPENSATED (terminal) and late SUCCEEDED arrives → start follow-up compensation for that step
│   │   │   (saga reopened into COMPENSATING via governed transition) or NEEDS_ATTENTION if not compensable
│   │   ├── Saga COMPLETED and late reply for a resolved step → record only
│   │   └── Late REJECTED/FAILED → record only
│   └── Edge cases
│       ├── E6.1 Reply processed by two orchestrator instances — double transition — guarded by SQ-02/SQ-11 [C]
│       ├── E6.2 Worker sends SUCCEEDED then (after bug) FAILED for same key — contradictory outcomes — INTEGRITY [C]
│       └── E6.3 Reply arrives before DISPATCHED commit visible (fast worker, replica read) — "unknown step" — misclassified [C]
│           └── Handling: reply handler reads primary; brief retry before quarantine
│
├── 7. OUTCOME CLASSIFICATION & DECISION
│   └── On resolved outcome of step i (type T)
│       ├── SUCCEEDED
│       │   ├── T = PIVOT → set pivot reached = true (no compensation beyond this point)
│       │   ├── i = last step → saga COMPLETED → notify client (§18)
│       │   └── else → dispatch step i+1 (§5) in the same tx as the outcome transition
│       ├── REJECTED (business decision, deterministic)
│       │   ├── Never retried
│       │   ├── Pre-pivot → COMPENSATING with compensation set = succeeded compensable steps (§12)
│       │   └── Post-pivot → impossible by design for RETRIABLE steps (must not reject) → NEEDS_ATTENTION
│       ├── FAILED_RETRYABLE → retry policy (§9) → RETRY_SCHEDULED (same idempotency key)
│       ├── FAILED_PERMANENT (worker states effect NOT executed)
│       │   ├── Pre-pivot → COMPENSATING
│       │   └── Post-pivot → NEEDS_ATTENTION (roll-forward impossible without intervention)
│       ├── UNKNOWN (timeout, connection lost, ambiguous error) → §8 resolution FIRST (SQ-05)
│       ├── INVALID_OUTPUT → NEEDS_ATTENTION (effect assumed executed)
│       └── EXPIRED (worker rejected stale command, not executed)
│           ├── Re-dispatch with new deadline if business still valid (same key semantics declared per step)
│           └── Else treat as REJECTED
│
│   └── Edge cases
│       ├── E7.1 Timeout treated as FAILED — compensation around an executed effect — CORRUPT financial [C]
│       ├── E7.2 Business rejection retried — repeated declined charges, provider fraud flags — CORRUPT [C]
│       └── E7.3 Worker returns "error" for both "not executed" and "executed but reply failed" — unclassifiable — CORRUPT [C]
│           └── Handling: worker contract distinguishes NOT_EXECUTED vs UNKNOWN vs REJECTED explicitly
│
├── 8. UNKNOWN OUTCOME RESOLUTION  (SQ-05)
│   ├── 8.1 Ladder
│   │   ├── Step 1 Query worker status by idempotency key (EXECUTED with output | NOT_FOUND | IN_PROGRESS)
│   │   │   ├── EXECUTED → treat as SUCCEEDED (validate output) → continue
│   │   │   ├── IN_PROGRESS → extend wait with timer (bounded)
│   │   │   ├── NOT_FOUND (and worker guarantees dedupe window covers the command) → NOT_EXECUTED → retry with same key
│   │   │   └── Query fails → retry query with backoff
│   │   ├── Step 2 If no status query: retry command with SAME idempotency key (worker dedupes; returns original outcome)
│   │   ├── Step 3 External provider effects: reconcile via provider records (payment reference derived from key)
│   │   └── Step 4 Budget exhausted → NEEDS_ATTENTION with full evidence (never auto-compensate around UNKNOWN)
│   │
│   ├── 8.2 Dedupe window rule
│   │   ├── Worker/provider idempotency window ≥ max time from first dispatch to last retry/query (DLQ tree DL-09)
│   │   └── E8.2a Retry after provider window expired — provider executes again — double effect — CORRUPT financial [C]
│   │
│   └── Edge cases
│       ├── E8.1 NOT_FOUND because worker purged dedupe records — misread as not executed — double execution [C]
│       ├── E8.2 Status query hits a lagging replica — false NOT_FOUND — double execution [C]
│       │   └── Handling: worker answers status queries from primary/authoritative store
│       └── E8.3 Unknown outcome on PIVOT step — the most dangerous case — must resolve before any direction is chosen [C]
│
├── 9. RETRY POLICY  (per step)
│   ├── Failure classes from worker contract + orchestrator transport errors (retry tree §3 taxonomy)
│   ├── Transport/environment errors → retry with backoff + jitter; budget in TIME and attempts
│   ├── Dependency outage signalled broadly → pause dispatch for that step type (backpressure), don't burn budgets
│   ├── Post-pivot RETRIABLE steps → retry until success; exhaust → NEEDS_ATTENTION (never compensate)
│   ├── Retry timers durable (C-TMR), executed by saga owner
│   └── Edge cases
│       ├── E9.1 Retries without jitter across thousands of sagas — synchronized load spikes — OUTAGE [C]
│       ├── E9.2 Retry budget burned during a 20-minute dependency outage — mass NEEDS_ATTENTION — OUTAGE (ops) [C]
│       │   └── Handling: dependency-level pause (breaker) suspends retry clocks for affected step types
│       └── E9.3 Retry with mutated inputs (re-resolved context changed) — same key, different payload — worker conflict [C]
│           └── Handling: inputs frozen at first dispatch and stored with the step
│
├── 10. TIMEOUTS, DEADLINES & TIMERS
│   ├── 10.1 Step timeout → UNKNOWN (if dispatched) | re-dispatch (if never delivered — not knowable → UNKNOWN anyway)
│   ├── 10.2 Saga deadline → business decision: pre-pivot → cancel + compensate; post-pivot → escalate, keep rolling forward
│   ├── 10.3 Timer mechanics
│   │   ├── Due times from DB clock (retry tree F7.2); fired by owner of saga partition; idempotent firing
│   │   ├── Timer vs reply race: both processed through CAS; whichever transitions first wins; loser no-ops
│   │   └── Timers cancelled in the same tx as the transition that makes them obsolete
│   └── Edge cases
│       ├── E10.1 In-memory timers — lost on restart — sagas wait forever — OUTAGE (stuck) [C]
│       ├── E10.2 Timer fires after reply already applied — stale timer causes UNKNOWN on resolved step — guarded by CAS [C]
│       └── E10.3 Clock jump on orchestrator host — early/late timeouts — guarded by DB clock [C]
│
├── 11. CONTEXT MANAGEMENT  (SQ-13)
│   ├── Outputs stored per (step, name); immutable after step SUCCEEDED
│   ├── Step inputs reference explicit bindings; no "merge all outputs"
│   ├── Size limits per entry and per saga [L]; claim-check for large outputs
│   ├── PII fields encrypted per subject (encryption tree); context purge on retention
│   ├── Schema versions recorded per entry; definition upgrades never reinterpret old entries silently
│   └── Edge cases
│       ├── E11.1 Flat merge — step 4 output "id" overwrites step 2 "id" — wrong downstream inputs — CORRUPT [C]
│       ├── E11.2 Context grows with each retry attempt's partial outputs — bloat [C]
│       │   └── Handling: only SUCCEEDED outputs written; attempts logged separately without payloads
│       └── E11.3 Context holds card data / tokens — saga store becomes PCI scope — COMPLIANCE [C]
│           └── Handling: store provider tokens/references only
│
├── 12. FAILURE → COMPENSATION
│   ├── 12.1 Entry conditions
│   │   ├── Pre-pivot REJECTED / FAILED_PERMANENT / approved cancel / saga deadline pre-pivot
│   │   └── Never entered while any step is UNKNOWN (SQ-05) — resolve first
│   │
│   ├── 12.2 Compensation set
│   │   ├── Include: COMPENSABLE steps with outcome SUCCEEDED (incl. resolved-from-UNKNOWN as executed)
│   │   ├── Include: current step if resolved as executed
│   │   ├── Exclude: NOT_EXECUTED, REJECTED, NOT_REACHED, SKIPPED (unless skip recorded an effect)
│   │   ├── Exclude: PIVOT and post-pivot steps (SQ-08) — if pivot reached, this is ROLL_FORWARD territory
│   │   └── Order: reverse of success order
│   │
│   ├── 12.3 Compensation execution (per step, same dispatch protocol as §5)
│   │   ├── Compensation idempotency key = (saga, step, "compensate")
│   │   ├── Outcomes: COMPENSATED | REJECTED (e.g., refund window closed) | FAILED_RETRYABLE | UNKNOWN
│   │   ├── FAILED_RETRYABLE → retry to budget; UNKNOWN → status query ladder (§8 applied to compensation)
│   │   ├── REJECTED / exhausted → COMPENSATION_FAILED → NEEDS_ATTENTION (continue compensating other steps? per definition)
│   │   └── All compensated → saga COMPENSATED (terminal) → notify client
│   │
│   ├── 12.4 Isolation during compensation
│   │   ├── Semantic locks/pending states released only after compensation succeeds
│   │   └── Other sagas may have read intermediate state (communication tree §7.4) — countermeasures per resource
│   │
│   └── Edge cases
│       ├── E12.1 Compensation for a step that never executed (e.g., refund without capture) — CORRUPT financial [C]
│       ├── E12.2 Late success of an earlier step during compensation — step missed — CORRUPT [C]
│       │   └── Handling: §6.4 adds it to compensation set
│       ├── E12.3 Compensation order dependency violated (release funds before reversing transfer) — CORRUPT [C]
│       ├── E12.4 Compensation itself has side effects needing notification (customer refund email) — handled as forward effect of compensation step [C]
│       └── E12.5 Pivot failed with UNKNOWN, operator forces compensation — if pivot actually executed — CORRUPT [C]
│           └── Handling: forced decisions require evidence attachment and second approver (SQ-14)
│
├── 13. CANCELLATION, PAUSE & MANUAL INTERVENTION
│   ├── 13.1 User cancel
│   │   ├── Pre-pivot → CANCEL_REQUESTED → wait for in-flight step resolution → COMPENSATING
│   │   ├── Post-pivot → reject cancel or convert to business reversal process (new saga) [G: per business]
│   │   └── E13.1a Cancel processed while step in flight → compensation starts before step resolves — CORRUPT [C]
│   ├── 13.2 Operator pause/resume
│   │   ├── PAUSED stops new dispatches and timers advance only on resume (retry clocks frozen)
│   │   └── In-flight replies still ingested and recorded
│   ├── 13.3 NEEDS_ATTENTION workflow
│   │   ├── Task with full evidence: history, attempts, worker status queries, provider records, context (masked)
│   │   ├── Allowed actions: retry step (same key), mark resolved as executed/not executed (with evidence), skip step (approved),
│   │   │   start compensation, start roll-forward, complete manually, abandon (approved, with business record)
│   │   └── Every action via C-CTL, two-person rule for financial outcomes
│   └── Edge cases
│       ├── E13.1 Operator retries with a new idempotency key "to force it" — double effect — CORRUPT [C]
│       │   └── Handling: tooling cannot mint new keys; re-execution requires explicit "new logical execution" approval
│       └── E13.2 NEEDS_ATTENTION queue unowned — sagas stuck for weeks — LOSS (business) [C]
│
├── 14. CONCURRENCY & OWNERSHIP
│   ├── 14.1 Saga ownership
│   │   ├── Sagas hashed to orchestrator partitions; partition owner holds lease + epoch (consumer groups tree CG-01)
│   │   ├── Every transition verifies epoch (SQ-11)
│   │   └── Lease expiry → new owner bumps epoch → resumes from persisted state
│   ├── 14.2 Business-key exclusivity (SQ-15)
│   ├── 14.3 Resource contention across sagas — semantic locks owned by the resource's aggregate (not the orchestrator)
│   └── Edge cases
│       ├── E14.1 Zombie orchestrator (GC pause, partition) dispatches after losing lease — duplicate commands — guarded by epoch [C]
│       ├── E14.2 Two sagas reserve the last unit — oversell — resource aggregate decides atomically [C]
│       └── E14.3 Hot business key with frequent sagas — serialized by SQ-15 — queueing policy defined [C]
│
├── 15. VERSIONING & DEPLOYS
│   ├── 15.1 Definition versions immutable; instances pinned (SQ-12); new starts use latest
│   ├── 15.2 Worker command contracts versioned (schema evolution tree); orchestrator sends version expected by pinned definition
│   ├── 15.3 Removing a step handler only after no in-flight sagas reference it (drain report)
│   ├── 15.4 Orchestrator code deploys: state machine interpretation backward compatible with persisted states
│   └── Edge cases
│       ├── E15.1 Definition edited in place — in-flight sagas jump steps or re-run steps — CORRUPT [C]
│       ├── E15.2 New version renames context fields — old sagas' later steps can't find inputs — NEEDS_ATTENTION storm [C]
│       └── E15.3 Worker drops support for old command version while old sagas still in flight — OUTAGE (flows) [C]
│
├── 16. THROUGHPUT, FAIRNESS & BACKPRESSURE
│   ├── Per-step-type dispatch rate limits matching worker capacity
│   ├── Fair scheduling across tenants and saga types (no starvation)
│   ├── Worker saturation → pause dispatch for that step type (don't fail sagas)
│   ├── Timer firing rate limits after outages (ramped resume)
│   └── Edge cases
│       ├── E16.1 Thousands of sagas resume simultaneously after outage — thundering herd on workers — OUTAGE [C]
│       └── E16.2 One tenant's bulk sagas starve others — DEGRADE [C]
│
├── 17. DURABILITY & RECOVERY
│   ├── Saga store HA with synchronous replication for T0 sagas
│   ├── Restore from backup = DANGER: steps already executed may appear PENDING → re-dispatch with same keys
│   │   (workers dedupe within windows) → reconcile every non-terminal saga against worker status before resuming
│   ├── Kafka outage: dispatches accumulate in outbox; replies delayed; timers must not fire UNKNOWN storms
│   │   └── Handling: pipeline-health-aware timers (extend while command/reply pipeline degraded)
│   └── Edge cases
│       ├── E17.1 Restored saga store resumes blindly — double payments outside idempotency windows — CORRUPT financial [C]
│       └── E17.2 Reply topic retention shorter than orchestrator outage — replies lost — UNKNOWN storm [C]
│           └── Handling: retention ≥ max outage; status-query resolution as fallback
│
├── 18. CLIENT INTERACTION
│   ├── Start returns saga id immediately (ACCEPTED), not final output (CQRS tree §3.2)
│   ├── Status endpoint: state, current step (business wording), last update, ETA if known
│   ├── Completion/compensation notifications via events and user channels
│   ├── Short sagas may offer bounded wait for completion with token-based status
│   └── E18.1 Client polls aggressively — status endpoint load — rate limits and push notifications [C]
│
├── 19. RETENTION, PRIVACY & AUDIT
│   ├── Terminal sagas archived after retention; history kept per audit class
│   ├── PII in context encrypted and erasable (encryption tree)
│   ├── Every operator action and forced resolution in audit trail
│   └── E19.1 Saga history purged before dispute window closes — no evidence — COMPLIANCE [C]
│
├── 20. OBSERVABILITY
│   ├── Sagas by type × state; oldest non-terminal age; deadline breaches
│   ├── Steps: dispatch rate, latency by step, outcome class distribution, attempts per step
│   ├── UNKNOWN: count, age, resolution path outcomes (must trend to 0)
│   ├── Compensation: started, succeeded, failed, unknown; COMPENSATION_FAILED must be 0 or owned
│   ├── NEEDS_ATTENTION: queue size, age, owner
│   ├── Late replies by type; integrity conflicts (must be 0)
│   ├── Ownership: epoch conflicts, lease expirations
│   ├── Timers: due backlog, firing lateness
│   └── Absence alerts on all of the above
│
├── 21. RUNBOOKS
│   ├── RB-01 UNKNOWN spike on a step → check worker/provider health → status-query ladder → no manual compensation
│   ├── RB-02 Compensation failures → identify rejection reasons → business decision → manual resolution via C-CTL
│   ├── RB-03 Stuck sagas (no timer, no progress) → verify timer rows/owner leases → re-arm via tooling
│   ├── RB-04 Saga store restore → freeze dispatch → reconcile non-terminal sagas with workers → resume per saga
│   ├── RB-05 Definition defect discovered → stop new starts on that version → migrate/resolve in-flight via NEEDS_ATTENTION
│   └── RB-06 Dependency outage → pause dispatch for step type → ramped resume → watch UNKNOWN counts
│
└── 22. TEST MATRIX  (fault injection at every crash point)
    ├── Crash after DISPATCHED commit, before relay → command delivered once after recovery
    ├── Worker executes, reply lost → timeout → status query finds EXECUTED → saga continues (no compensation)
    ├── Worker never received command → status NOT_FOUND → retry same key → single execution
    ├── Pivot step times out with provider success → resolved as executed → roll forward, never compensate
    ├── Business rejection at step 3 → compensate steps 2 then 1; step 3 not compensated
    ├── Late success of step 2 after compensation started → step 2 added to compensation set
    ├── Compensation rejected (refund window closed) → NEEDS_ATTENTION with evidence
    ├── Two orchestrator instances on same saga (forced) → epoch blocks one; single dispatch
    ├── Timer and reply race → exactly one transition
    ├── Cancel while step in flight → waits for resolution, then compensates
    ├── Definition v2 deployed → v1 sagas finish on v1
    ├── Context names collide across steps → impossible (namespaced bindings)
    ├── Dependency outage 30 min → dispatch paused; no retry budget burn; ramped resume
    ├── Saga store restored from backup (staging) → reconciliation prevents re-execution
    ├── 100k concurrent sagas → fairness and rate limits hold; no worker overload
    └── Operator force-resolve without evidence/second approver → rejected
```

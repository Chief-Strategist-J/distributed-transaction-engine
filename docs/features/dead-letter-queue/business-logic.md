DEAD-LETTER QUEUE (SAGA ENGINE) — BUSINESS LOGIC DECISION TREE (v2)
Legend: [C] Certain   [L] Likely (sound default)   [G] Guessing (business must decide)
│
├── 0. WHAT THE DLQ IS
│   ├── A DLQ record = a saga step / message that requires a human or system DISPOSITION
│   ├── The saga is never abandoned: its state becomes NEEDS_ATTENTION and stays non-terminal until disposition
│   ├── Admitted
│   │   ├── Poison: unreadable/invalid payload, schema/contract violation
│   │   ├── Definition defect: missing binding, invalid step input, engine bug
│   │   ├── Exhausted with known NOT_EXECUTED outcome
│   │   ├── Exhausted with UNKNOWN outcome (ambiguous) — flagged distinctly
│   │   ├── Business rejection that the saga definition routes to humans (post-pivot, compensation not possible)
│   │   ├── Compensation failure / NOT_COMPENSABLE without automated alternative
│   │   └── Integrity: contradictory outcomes, same id with different content
│   └── Never admitted (handled elsewhere)
│       ├── Dependency outages (pause, not quarantine)
│       ├── Own rate limiting / breaker states
│       └── Transient errors still within budget
│
├── 1. INVARIANTS
│   ├── DQ-01 The DLQ record and the saga/step transition to NEEDS_ATTENTION commit in ONE database transaction [C]
│   ├── DQ-02 The database is the source of truth; any DLQ topic is a notification emitted via outbox [C]
│   ├── DQ-03 Every record carries the last outcome class; UNKNOWN is never treated as NOT_EXECUTED [C]
│   ├── DQ-04 Replay is an engine action under current saga state, with the ORIGINAL idempotency key [C]
│   ├── DQ-05 Payloads are never edited; changes happen through approved corrective actions with new identity [C]
│   ├── DQ-06 Every record ends with an explicit business disposition, recorded with actor, reason, evidence [C]
│   ├── DQ-07 Money-impacting actions require a second approver [C]
│   ├── DQ-08 Actions on a record are serialized (one action at a time, compare-and-set on record state) [C]
│   ├── DQ-09 Systemic failures are handled as one incident with a paused step type, not N records [C]
│   └── DQ-10 Customer data is masked by default; unmasking is role-gated and audited [C]
│
├── 2. ADMISSION
│   ├── 2.1 One transaction (DQ-01)
│   │   ├── Insert DLQ record: saga id, step index, attempt ids, idempotency key, outcome class, failure class,
│   │   │   error fingerprint, sanitized message, references to stored payload (not a free copy), schema version,
│   │   │   business deadline(s), money at stake, tenant, owner team
│   │   ├── Saga → NEEDS_ATTENTION (or step-level attention if other steps can proceed)
│   │   ├── Cancel pending timers for that step
│   │   └── Outbox notification (DQ-02)
│   ├── 2.2 Systemic check (DQ-09)
│   │   ├── Same fingerprint across many sagas in a short window [L: > 20 in 5 min]
│   │   │   → pause that step type (no further dispatches), open one incident, link records
│   │   └── Otherwise → individual record
│   ├── 2.3 Admission failure
│   │   ├── Transaction fails → nothing changes; the step stays in its prior state; retried by engine
│   │   └── Never "spill to local disk" or crash the worker
│   └── Edge cases
│       ├── E2.1 PostgreSQL written, Kafka notification fails — notification later via outbox (no loss) [C]
│       ├── E2.2 Ephemeral pod disk spillway — lost on reschedule — LOSS [C]
│       ├── E2.3 UNKNOWN outcome admitted as "retry exhausted" — later replay double-executes [C]
│       └── E2.4 Schema bug sends 10,000 sagas to DLQ — 10,000 pages — one incident instead [C]
│
├── 3. RECORD STATES & DISPOSITIONS
│   ├── States: QUARANTINED → TRIAGED → ACTION_IN_PROGRESS → RESOLVED
│   │           (any) → ESCALATED (deadline risk, money threshold)
│   ├── Dispositions (DQ-06)
│   │   ├── RETRIED_SUCCEEDED — same-key retry completed the step
│   │   ├── RESOLVED_AS_EXECUTED — evidence shows step happened; saga advanced
│   │   ├── RESOLVED_AS_NOT_EXECUTED — evidence shows step did not happen; saga decision applied
│   │   ├── CORRECTED — superseded by approved corrective action
│   │   ├── SAGA_COMPENSATED / SAGA_ROLLED_FORWARD / SAGA_COMPLETED_MANUALLY
│   │   └── WRITTEN_OFF — approved business loss, with amount and reason
│   └── Edge cases
│       ├── E3.1 "Dismissed" without saga disposition — saga stuck, effect untracked — LOSS [C]
│       └── E3.2 Record resolved while saga still NEEDS_ATTENTION — inconsistent states [C]
│           └── Handling: record resolution and saga transition in one transaction
│
├── 4. TRIAGE
│   ├── 4.1 Access (DQ-10)
│   │   ├── Roles: viewer (masked), operator (masked + actions), approver, unmask (case-bound, audited)
│   │   └── Every view and action audited
│   ├── 4.2 What the operator sees
│   │   ├── Saga timeline: steps, outcomes, attempts, compensation states
│   │   ├── Outcome class and evidence: worker status-query results, provider references
│   │   ├── Deadlines: hold expiry, refund window, customer promise
│   │   ├── Money at stake and customer impact
│   │   ├── Fingerprint group (other records with same cause)
│   │   └── Allowed actions given current saga state (only valid options offered)
│   ├── 4.3 Routing: owner team by step type and failure class; business owner for rejections/write-offs
│   └── Edge cases
│       ├── E4.1 Decoded payloads shown to all admins — PII exposure — COMPLIANCE [C]
│       └── E4.2 Operator acts without seeing that saga was already compensated — wrong action [C]
│
├── 5. ACTIONS  (offered only when preconditions hold)
│   │
│   ├── 5.1 RETRY (same identity)
│   │   ├── Preconditions
│   │   │   ├── Root cause addressed (fix deployed / data fixed upstream / dependency healthy)
│   │   │   ├── Saga and step still in the state expecting this step (not compensated/cancelled/resolved)
│   │   │   ├── Outcome class NOT_EXECUTED, or worker idempotent by key (same-key retry safe),
│   │   │   │   or UNKNOWN resolved first via status query
│   │   │   ├── Within business validity (valid_until, hold expiry) and provider idempotency window
│   │   │   └── Ordering: no later step for the saga already executed
│   │   ├── Execution: engine transition → step RETRY_SCHEDULED → dispatch via outbox with SAME key
│   │   └── Outcome feeds normal saga flow; record resolved on step resolution
│   │
│   ├── 5.2 RESOLVE AS EXECUTED / NOT EXECUTED (evidence-based)
│   │   ├── Evidence attached (provider reference, worker status, reconciliation report)
│   │   ├── EXECUTED → record output (validated) → saga advances
│   │   └── NOT_EXECUTED → saga decision: retry, alternative, compensate (pre-pivot), escalate (post-pivot)
│   │
│   ├── 5.3 CORRECTIVE ACTION (replaces "edit and replay")
│   │   ├── For commands with wrong inputs
│   │   │   ├── Confirm original NOT executed (status query/evidence) — otherwise compensate/reconcile original first
│   │   │   ├── Mark original attempt SUPERSEDED (its key can never be retried again)
│   │   │   ├── New command with new logical execution id → new idempotency key
│   │   │   ├── Inputs validated against step schema and business rules
│   │   │   └── Reason, ticket, approver recorded (DQ-07 for money)
│   │   ├── For business facts (events from producers)
│   │   │   └── Never edited; producer issues a correction event (same aggregate, next version)
│   │   └── For poison payloads (unreadable)
│   │       └── Fix producer/consumer code; replay same bytes after fix — never hand-patch bytes
│   │
│   ├── 5.4 SAGA-LEVEL DECISION
│   │   ├── Pre-pivot → compensate (compensation engine)
│   │   ├── Post-pivot → roll forward (retry, alternative) or business process
│   │   └── Manual completion with evidence (approved)
│   │
│   ├── 5.5 WRITE-OFF (replaces "dismiss")
│   │   ├── Preconditions: no unaccounted executed effects (or residuals listed), saga disposition set
│   │   ├── Amount, reason, approver (DQ-07)
│   │   └── Finance notified for monetary write-offs
│   │
│   ├── 5.6 BULK ACTIONS (by fingerprint)
│   │   ├── Preview: affected records, saga states, deadlines, money at stake
│   │   ├── Progressive: sample → subset → all; rate-limited; stop on failure threshold
│   │   └── Each record still passes its own preconditions (5.1)
│   │
│   └── Edge cases
│       ├── E5.1 Replay into ingress topic — other consumers re-process; ordering ignored — CORRUPT [C]
│       ├── E5.2 Replay forward step after saga compensated — booking after refund — CORRUPT financial [C]
│       ├── E5.3 Edited payload under same key — worker ignores edit or rejects — confusion, wrong outcome [C]
│       ├── E5.4 Edited payload under new key while original actually executed — double effect — LOSS [C]
│       ├── E5.5 Replay after hold expired — confirm fails or rebooks at new price — CORRUPT [C]
│       └── E5.6 Replay of old-schema payload with current code — upcasting needed — failure loops [C]
│
├── 6. REPLAY SAFETY CHECKLIST  (evaluated by the system, not by the operator's memory)
│   ├── Saga state allows the step
│   ├── Step state is the dead-lettered attempt (no newer attempt/resolution)
│   ├── Outcome class safe (NOT_EXECUTED or same-key safe or resolved)
│   ├── Deadlines and windows valid
│   ├── Step type not paused (systemic incident resolved)
│   ├── Payload readable with current schema (upcasters available)
│   └── Approver present if money-impacting
│
├── 7. SYSTEMIC INCIDENTS  (DQ-09)
│   ├── Paused step type → no new dispatches; affected sagas NEEDS_ATTENTION grouped
│   ├── Fix → validation on sample → bulk retry (5.6) → unpause
│   └── E7.1 Step type paused and forgotten — sagas wait indefinitely [C]
│       └── Handling: pause has owner + review deadline; visible on saga dashboards
│
├── 8. PRIORITIZATION & ALERTING
│   ├── Priority: earliest business deadline → money at stake → customer impact → age
│   ├── Alerts
│   │   ├── Any record with money ≥ threshold or deadline within lead time → page owner [G: thresholds]
│   │   ├── New fingerprint → notify owner team
│   │   ├── Oldest unresolved record age beyond SLA → escalate
│   │   └── Integrity class → page immediately
│   └── E8.1 Single "DLQ not empty" alert — permanent noise — real deadline misses ignored [C]
│
├── 9. CONCURRENCY  (DQ-08)
│   ├── Record actions via compare-and-set on record state + saga transition
│   ├── Two operators acting simultaneously → one succeeds; other sees updated state
│   └── E9.1 Two operators replay the same record — duplicate dispatch [C]
│
├── 10. DATA PROTECTION & RETENTION
│   ├── Payload stored once (reference); diagnostics sanitized
│   ├── Masked views by default (DQ-10)
│   ├── Retention: records kept until disposition + audit retention; payload subject to erasure rules
│   └── E10.1 Stack traces containing customer data stored and shown — COMPLIANCE [C]
│
└── 11. FEATURE TEST SCENARIOS
    ├── Admission writes record + saga NEEDS_ATTENTION atomically; notification via outbox
    ├── Admission DB failure → step unchanged, retried; no local spill, no crash
    ├── Timeout-exhausted step admitted as UNKNOWN → replay blocked until status query resolves
    ├── Replay offered only while saga still expects the step; blocked after compensation
    ├── Replay uses original idempotency key; worker returns original result if executed
    ├── Wrong amount in command → corrective action: original confirmed not executed, superseded, new key, approved
    ├── Attempt to edit payload directly → not possible
    ├── Producer event with wrong data → correction event path, not DLQ edit
    ├── Write-off without saga disposition → blocked
    ├── 2,000 records same fingerprint → one incident, step type paused, bulk retry after fix
    ├── Hold expiring in 2 hours → record prioritized and paged
    ├── Two operators act at once → only one action applied
    ├── Viewer role sees masked payload; unmask requires case and is audited
    └── Old-schema payload replay → upcast succeeds or replay blocked with reason
Your schedules have **no time zone and no DST policy**, so business schedules fire at the wrong time and sometimes twice:
- **No zone:** a 5-field cron evaluated in server time means "daily at midnight" for an Indian customer runs at 05:30 IST, and for a US customer it moves by an hour twice a year.
- **Fall-back:** with local zones, 01:30 happens twice. Each occurrence has different epoch seconds, so your idempotency keys differ and the **monthly billing run executes twice**.
- **Spring-forward:** 02:30 doesn't exist, so that day's run is **silently skipped**. [Certain]

Every schedule needs an explicit time zone and a declared rule for nonexistent and repeated local times.

Four more errors, ranked by damage:

**1. Your retry backoff changes the idempotency key, so an ambiguous dispatch creates a duplicate saga.** [Certain]
I disagree because the key is built from `NextRunAtEpochSeconds`, and on `DISPATCH_FAILED` you push `next_run_at` forward by 10 s. If the first dispatch actually started the saga (a timeout after the orchestrator committed), the retry computes a **new** key, and a second saga starts.
Here's what I'd do instead: the key identifies the **logical occurrence** (the scheduled fire time), never the retry time. Retry state goes in separate fields.

**2. Dispatch and schedule advancement aren't atomic, and the row lock is held across an API call or not held at all.** [Certain]
I disagree because `FOR UPDATE SKIP LOCKED` either stays open across orchestrator API calls (long transactions, bloat) or autocommits and claims nothing. The "dispatch, then update `next_run_at`" sequence then double-fires after a crash.
Here's what I'd do instead: **one transaction** that records the occurrence (unique per schedule and occurrence time), creates the saga or writes a start command to the outbox, and advances the schedule. Drop the Redis leader lock entirely.

**3. Missed runs are silently dropped, and overlapping runs aren't handled.** [Certain]
I disagree because if the scheduler is down for 3 hours, `next_run_at <= NOW()` fires once and the intervening occurrences vanish. That's fine for a digest email and wrong for daily billing.
A recurring job whose previous saga is still running also needs a rule.
Here's what I'd do instead: every schedule declares a **misfire policy** (run all missed, run once, skip) and an **overlap policy** (allow, skip, queue, cancel previous).

**4. Schedules run with the creator's authority forever.** [Certain]
I disagree because a user who created a recurring payout and later lost their role, or left the company, keeps triggering it. Their authority was checked once, at registration.
Here's what I'd do instead: **re-authorize at every firing**, under a run-as principal, and auto-pause schedules whose owner is removed.

```
SCHEDULED EXECUTION — BUSINESS LOGIC DECISION TREE (v2)
Legend: [C] Certain   [L] Likely (sound default)   [G] Guessing (business must decide)
│
├── 0. SCHEDULE MODEL  (every schedule declares all of these)
│   ├── Type: ONE_OFF (local datetime + zone) | RECURRING_CRON (expression + zone) | RECURRING_CALENDAR
│   │   (business rules cron can't express: last business day, every 2nd Tuesday, every 30 days from start)
│   ├── Time zone: IANA zone (e.g., Asia/Kolkata) — mandatory, no server default
│   ├── DST policy: nonexistent time → RUN_AT_NEXT_VALID | SKIP; repeated time → RUN_FIRST | RUN_SECOND (never both)
│   ├── Misfire policy: RUN_ALL_MISSED (bounded) | RUN_ONCE | SKIP_MISSED
│   ├── Overlap policy: ALLOW | SKIP_IF_RUNNING | QUEUE_ONE | CANCEL_PREVIOUS (business-defined)
│   ├── Max lateness: occurrence older than this when fired → EXPIRED (not run) [G]
│   ├── Active window: start date, end date or max occurrences
│   ├── Payload binding: FROZEN at registration | RESOLVED at fire time (from referenced entities)
│   ├── Workflow version policy: PINNED version | LATEST at fire time
│   └── Run-as principal: owner identity + required permission for the action
│
├── 1. INVARIANTS
│   ├── SE-01 Every occurrence has a stable identity: (schedule id, logical occurrence instant) [C]
│   ├── SE-02 Firing = one transaction: occurrence record + saga start (or outbox start command) + schedule advance [C]
│   ├── SE-03 Saga start idempotency key is derived from SE-01, never from retry or wall time [C]
│   ├── SE-04 Occurrences are computed in the schedule's time zone with declared DST rules [C]
│   ├── SE-05 Next occurrence is computed from the last LOGICAL occurrence, not from "now" [C]
│   ├── SE-06 Authorization is re-checked at each firing [C]
│   ├── SE-07 Misfire, overlap, and lateness behaviour are explicit per schedule [C]
│   ├── SE-08 Claiming uses database leases; no lock is held across calls to other services [C]
│   └── SE-09 Schedule edits never change occurrences already fired or in flight [C]
│
├── 2. REGISTRATION & VALIDATION
│   ├── Authorization: caller may create schedules for this action/tenant; quotas per tenant [G]
│   ├── Time zone valid (IANA); local datetime interpreted in that zone
│   ├── ONE_OFF
│   │   ├── Instant ≥ now + minimum lead [L: seconds] and ≤ maximum horizon [G]
│   │   ├── Local time nonexistent (DST gap) → reject with explanation or apply declared policy
│   │   └── Local time ambiguous (DST overlap) → require explicit choice (first/second)
│   ├── RECURRING_CRON
│   │   ├── Syntax valid; dialect declared (5-field vs 6-field with seconds)
│   │   ├── Never-firing expressions (e.g., Feb 30) → reject
│   │   ├── Minimum interval [G: per action] — too-frequent schedules rejected
│   │   └── Preview: next N occurrences in zone shown to the user (catches DST/month-end surprises)
│   ├── RECURRING_CALENDAR
│   │   ├── Month-end rules (31st in short months → last day | skip) explicit
│   │   └── Business calendar (holidays) reference and roll rule (previous/next business day)
│   ├── Payload validated against workflow input schema (FROZEN) or references validated (RESOLVED)
│   └── Edge cases
│       ├── E2.1 Client sends local time without zone — server assumes UTC — off by hours [C]
│       ├── E2.2 "Every 30 days" expressed as monthly cron — business expectation broken [C]
│       ├── E2.3 Thousands of tenants schedule at exactly 00:00 — load spike (§11) [C]
│       └── E2.4 Schedule created for action the user can no longer perform later — handled at firing (SE-06)
│
├── 3. OCCURRENCE COMPUTATION  (SE-04, SE-05)
│   ├── Compute next local occurrence from last logical occurrence → convert to instant in zone
│   ├── DST gap (nonexistent local time) → declared policy (RUN_AT_NEXT_VALID default) [L]
│   ├── DST overlap (repeated local time) → declared policy (RUN_FIRST default) — exactly one occurrence [C]
│   ├── Zone rule changes (government changes DST/offset) → zone database updated; future occurrences recomputed;
│   │   already-recorded occurrences unchanged
│   └── Edge cases
│       ├── E3.1 Repeated hour fires twice with different epoch keys — duplicate billing [C]
│       ├── E3.2 Nonexistent hour skipped silently — missed daily settlement [C]
│       └── E3.3 Next computed from "now" after a delay — schedule drifts (09:00 becomes 09:07 forever) [C]
│
├── 4. CLAIMING DUE OCCURRENCES  (SE-08)
│   ├── Scheduler workers own shards of schedules (hash of schedule id) with DB leases/epochs
│   ├── Due query per shard: active schedules with next occurrence ≤ now, ordered by due time and priority
│   ├── Claim in a short transaction (lease on the schedule row) or fire directly in §5 transaction
│   ├── Batch size and tick sized to peak due rate [L]
│   └── Edge cases
│       ├── E4.1 Redis leader with 5 s lease; leader paused 6 s — two leaders fire same occurrences [C]
│       └── E4.2 FOR UPDATE held while calling orchestrator API — long transactions — DEGRADE [C]
│
├── 5. FIRING  (SE-02, SE-03, SE-06)
│   ├── One transaction
│   │   ├── Insert occurrence record (unique: schedule id + occurrence instant) → conflict = already fired → skip
│   │   ├── Re-check schedule still active (not paused/deleted/expired) and within active window
│   │   ├── Authorization re-check for run-as principal and action (SE-06)
│   │   │   └── Denied → occurrence DENIED; schedule paused; owner notified
│   │   ├── Lateness check: now − occurrence instant > max lateness → occurrence EXPIRED (not run)
│   │   ├── Overlap check (§7)
│   │   ├── Resolve payload (FROZEN copy or RESOLVED from current entities, validated)
│   │   ├── Start saga in same database (idempotent by occurrence key) OR write outbox "start saga" command
│   │   │   with key = f(schedule id, occurrence instant)
│   │   ├── Advance schedule: last occurrence = this occurrence; next occurrence computed (§3)
│   │   └── Commit
│   ├── Ambiguous commit (connection lost) → re-read: occurrence record present? → fired; else fire again (same key)
│   └── Edge cases
│       ├── E5.1 Key derived from retried timestamp — ambiguous dispatch then retry — duplicate saga [C]
│       ├── E5.2 Saga started, schedule advance failed (separate step) — next tick fires again — duplicate (without SE-01) [C]
│       └── E5.3 RESOLVED payload references deleted entity — occurrence FAILED_TO_START with reason; policy: skip/pause [G]
│
├── 6. MISFIRES  (scheduler downtime, backlog)  (SE-07)
│   ├── Detect: multiple occurrences ≤ now for a schedule
│   ├── RUN_ALL_MISSED → fire each missed occurrence in order (each own key), up to max catch-up count [G];
│   │   remaining → EXPIRED with record
│   ├── RUN_ONCE → fire the most recent missed occurrence; mark older as SKIPPED_MISFIRE
│   ├── SKIP_MISSED → mark all missed as SKIPPED_MISFIRE; next future occurrence scheduled
│   ├── Lateness rule still applies per occurrence
│   └── Edge cases
│       ├── E6.1 Daily billing with SKIP_MISSED default — missed day never billed — revenue LOSS [C]
│       └── E6.2 RUN_ALL after a week outage on per-minute schedule — 10,000 sagas burst — OUTAGE [C]
│
├── 7. OVERLAP  (previous occurrence's saga still running)
│   ├── ALLOW → start anyway (saga must tolerate concurrency)
│   ├── SKIP_IF_RUNNING → occurrence SKIPPED_OVERLAP
│   ├── QUEUE_ONE → keep one pending occurrence; fire when previous finishes (older pending replaced)
│   ├── CANCEL_PREVIOUS → request cancellation of running saga (saga's cancel semantics apply), then start
│   └── E7.1 Two concurrent runs of a monthly payout saga — double payouts [C]
│
├── 8. LATENESS & EXPIRY
│   ├── Max lateness per schedule (morning digest useless at 15:00; payout still needed late)
│   ├── EXPIRED occurrences recorded and visible to owner; optional notification
│   └── E8.1 Time-sensitive action executed hours late (promotion email after promotion ended) — CORRUPT business [C]
│
├── 9. FAILURES
│   ├── Firing transaction fails (DB error) → nothing recorded → retried next tick with same occurrence key
│   ├── Outbox start command delivery delayed → saga starts late; lateness evaluated at firing, not at saga start [L]
│   ├── Saga outcome tracked on occurrence (STARTED → SUCCEEDED/FAILED/COMPENSATED)
│   ├── Consecutive failed occurrences ≥ N → schedule auto-paused + owner notified [G: N]
│   └── E9.1 Schedule keeps firing a failing workflow every minute — repeated compensations/charges [C]
│
├── 10. LIFECYCLE OPERATIONS
│   ├── Pause → no firing; occurrences during pause handled by misfire policy on resume (explicit choice at resume)
│   ├── Resume → misfire policy applied; next occurrence recomputed from last logical occurrence
│   ├── Edit (SE-09) → takes effect from next unfired occurrence; in-flight sagas untouched; change audited
│   ├── Delete → no new occurrences; in-flight sagas continue (or cancel per explicit choice)
│   ├── Owner removed / permission revoked → schedules auto-paused (SE-06) pending reassignment
│   ├── Tenant offboarded → schedules deleted; no firing
│   └── Edge cases
│       ├── E10.1 Edit of amount while occurrence firing — half old, half new — handled by transaction isolation [C]
│       └── E10.2 Resume after long pause fires every missed occurrence unexpectedly — explicit resume choice required [C]
│
├── 11. LOAD SHAPING
│   ├── Popular instants (00:00, top of hour) → due backlog spikes
│   ├── Spread: optional per-schedule jitter window where business allows [G] (e.g., reports within 00:00–00:30)
│   ├── Priority: schedules with business deadlines and money impact first
│   ├── Per-tenant fairness and per-workflow rate limits on saga starts
│   └── E11.1 100 jobs per second capacity vs 50,000 at midnight — last ones 8 minutes late, beyond lateness — EXPIRED [C]
│
└── 12. FEATURE TEST SCENARIOS
    ├── Daily 00:00 Asia/Kolkata → fires at 18:30 UTC previous day
    ├── America/New_York 01:30 on fall-back day → fires once (RUN_FIRST)
    ├── 02:30 on spring-forward day → fires at next valid time (or skipped per policy), never silently lost
    ├── 31st monthly in February → per month-end rule
    ├── Ambiguous dispatch timeout → retry uses same occurrence key → single saga
    ├── Crash after saga start before advance → impossible (same transaction); re-fire detected by occurrence record
    ├── 3-hour outage, daily billing RUN_ALL_MISSED → each missed day fired once in order
    ├── Hourly digest SKIP_MISSED after outage → missed recorded as skipped; next hour runs
    ├── Previous payout saga still running → overlap policy applied (no concurrent payout)
    ├── Owner loses permission → next firing denied; schedule paused; owner notified
    ├── Max lateness exceeded → EXPIRED, not run
    ├── 5 consecutive failures → schedule auto-paused
    ├── Edit schedule → in-flight saga unaffected; next occurrence uses new settings
    ├── Two scheduler workers → each occurrence fired once (unique occurrence record)
    └── Midnight burst → prioritized and rate-limited; deadline-critical schedules first
```

[Certain] The core corrections follow from time and concurrency fundamentals:
- DST creates nonexistent and repeated local times.
- Keys derived from retry times change under retries.
- Non-atomic dispatch-then-advance double-fires after crashes.
- Computing next runs from "now" drifts and loses missed occurrences.
- Short Redis leases overlap during pauses.
- Authority checked once becomes stale.

[Likely] Tick intervals, batch sizes, and default DST policies are sensible starting points.

[Guessing] Misfire policy, overlap policy, maximum lateness, catch-up limits, and auto-pause thresholds are business decisions per schedule type.

Two decisions are yours:
1. **Make time zone, DST policy, misfire policy, and overlap policy mandatory fields on every schedule (§0).** Defaults hidden in code are how billing runs get skipped or doubled twice a year.
2. **Key every saga start by the logical occurrence, and fire inside one transaction that records the occurrence and advances the schedule (SE-01 to SE-03).** That single change removes duplicate runs from crashes, retries, and overlapping schedulers.
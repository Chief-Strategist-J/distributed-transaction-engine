Retrying after a conflict re-applies the same input to a state where **another writer may already have applied it**.

Two workers both receive the reply "step 3 succeeded" (at-least-once delivery). Worker A wins the CAS. Worker B re-reads, sees the new version, recomputes, and applies "step 3 succeeded" **again**: it dispatches step 4 a second time, or writes a duplicate output. OCC prevented the lost update and caused a duplicate effect. [Certain]

A conflict retry must first ask **"has this input already been applied?"** (inbox, attempt ID) before re-deciding. Then it must re-decide against the new state, which may make the input a late or irrelevant reply.

Four more errors, ranked by damage:

**1. "Exhausted → 409 / requeue" can drop an engine input.** [Certain]
I disagree because for engine-internal transitions (replies, timers) there is no HTTP client to receive a 409. If the reply's offset is committed or the timer marked fired after three conflicts, a step outcome is **lost** and the saga waits forever.
Here's what I'd do instead: exhaustion leaves the input **unacknowledged and durably pending**, retried later and escalated as contention. Only client-originated commands return 409.

**2. OCC doesn't stop a stale owner.** [Certain]
I disagree because a zombie worker that re-reads gets the **latest** version and then wins the CAS. A version check catches concurrent edits, not illegitimate writers.
Here's what I'd do instead: put the **ownership epoch** in the same CAS (version AND epoch), as in your fencing tree.

**3. "RowsAffected == 0" means four different things, and only one should retry.** [Certain]
I disagree because zero rows can mean the version moved (retry), the epoch moved (stop: you're no longer the owner), the row is missing (error), or a tenant/state precondition failed (re-decide, don't loop).
Here's what I'd do instead: diagnose from the **same statement**, returning the current version, epoch, and state.

**4. The version guards the saga row but not its children.** [Certain]
I disagree because if a reply handler updates `saga_step` rows without bumping `saga_instance.version`, another worker decides from stale step data and its CAS still succeeds.
Here's what I'd do instead: every mutation of the aggregate (steps, timers, context) bumps the root version or locks the root row.

Hot sagas also need a different strategy. A parallel fan-out with 50 replies landing at once makes OCC retries thrash. Those sagas should be **serialized** (single owner per saga, or a row lock), not retried.

```
OPTIMISTIC CONCURRENCY CONTROL (SAGA ENGINE) — BUSINESS LOGIC DECISION TREE (v2)
Legend: [C] Certain   [L] Likely (sound default)   [G] Guessing (owner must decide)
│
├── 0. WHERE OCC APPLIES
│   ├── Saga aggregate transitions (engine inputs: replies, timers, operator actions)
│   ├── Client-driven edits (user changes saga parameters) via client-supplied expected version
│   └── Not suitable alone for: hot aggregates with many concurrent inputs, global counters (§2 alternatives)
│
├── 1. INVARIANTS
│   ├── OC-01 The CAS checks version AND ownership epoch [C]
│   ├── OC-02 Every mutation of the aggregate (root, steps, timers, context) increments the root version or locks the root [C]
│   ├── OC-03 Decisions are pure functions of (loaded state, input); no side effects before commit [C]
│   ├── OC-04 Side effects leave only via outbox rows written in the same transaction as the CAS [C]
│   ├── OC-05 Before re-deciding after a conflict, check whether the input was already applied [C]
│   ├── OC-06 Conflict exhaustion never drops an input; the input stays durably pending [C]
│   ├── OC-07 Zero-row results are diagnosed from the same statement; only version conflicts retry [C]
│   ├── OC-08 Reads used for decisions come from the primary [C]
│   └── OC-09 Client-facing version conflicts are returned to the client, never auto-retried with the client's intent [C]
│
├── 2. CONCURRENCY STRATEGY DECISION  (per saga type / operation)
│   ├── Check: Expected concurrent inputs per saga?
│   │   ├── Low (sequential steps, occasional timer/reply overlap) → OCC (this tree)
│   │   ├── High (parallel fan-out replies, frequent timers) → serialize per saga:
│   │   │   ├── Single owner per saga partition processes inputs sequentially (preferred) [C]
│   │   │   └── Or pessimistic row lock on saga root for each transition (short transactions) [C]
│   │   └── Client edits (UI/API) → OCC with client-supplied expected version (OC-09)
│   └── Edge cases
│       ├── E2.1 OCC on 50-branch fan-out — most replies conflict repeatedly — throughput collapse [C]
│       └── E2.2 Pessimistic lock held across network calls — lock waits, timeouts — OUTAGE [C]
│
├── 3. READ PHASE
│   ├── Read from primary (OC-08): root (state, version, epoch), steps, context needed for decision
│   ├── Read inside the same transaction as the write (consistent snapshot of children) [L]
│   ├── Capture input identity (reply id / timer id / operator intervention id)
│   └── Edge cases
│       ├── E3.1 Read from replica — stale version — constant conflicts (livelock) [C]
│       └── E3.2 Children read in a separate transaction — decision on mixed snapshot — CORRUPT [C]
│
├── 4. DECISION PHASE  (OC-03)
│   ├── Input: (loaded state, input)
│   ├── Output: new root state, child changes, outbox rows (commands/events), timer changes, input-applied marker
│   ├── No calls to other services, no Kafka sends, no emails — only planned outputs
│   └── Edge cases
│       ├── E4.1 Decision dispatches a command before CAS — conflict retry dispatches again — duplicate execution [C]
│       └── E4.2 Decision uses wall clock for business rules — retries produce different decisions [C]
│           └── Handling: decision time captured once per input (or DB time in the transaction)
│
├── 5. WRITE PHASE  (one transaction)
│   ├── Step 1 CAS on root: set new state, version = version + 1
│   │          WHERE id = saga AND version = loaded version AND owner epoch = my epoch
│   │          RETURNING current version, epoch, state (or diagnose on 0 rows — §6)
│   ├── Step 2 Child writes (steps, context entries, timers)
│   ├── Step 3 Outbox rows (OC-04)
│   ├── Step 4 Input-applied marker (inbox record for reply id / timer fired record)
│   ├── Step 5 Commit
│   └── Edge cases
│       ├── E5.1 Child writes before CAS — another writer's children interleave — inconsistent aggregate [C]
│       ├── E5.2 ORM ignores affected-row count — conflicting write treated as success — CORRUPT [C]
│       └── E5.3 Whole payload rewritten each transition — large write amplification, conflicts on unrelated fields [L]
│
├── 6. ZERO-ROW DIAGNOSIS  (OC-07)
│   ├── Single statement returns current (version, epoch, state, tenant) for the id
│   ├── Classification
│   │   ├── Epoch differs → NOT_OWNER → stop processing this saga (re-acquire ownership before anything else)
│   │   ├── Version differs, epoch same → VERSION_CONFLICT → §7
│   │   ├── Row missing → DATA_ERROR (deleted/never existed) → quarantine input
│   │   ├── Tenant mismatch → SECURITY/INTEGRITY → quarantine + alert
│   │   └── State precondition failed (if included in CAS) → STATE_CONFLICT → re-decide (not a blind loop)
│   └── E6.1 All zero-row results retried — not-owner workers fight owner; deleted rows retried forever [C]
│
├── 7. CONFLICT HANDLING  (VERSION_CONFLICT)
│   ├── Step 1 Rollback
│   ├── Step 2 Re-read latest state (primary)
│   ├── Step 3 Input already applied? (inbox / attempt id / timer fired marker) (OC-05)
│   │   ├── YES → acknowledge input; done (no new decision)
│   │   └── NO → Step 4
│   ├── Step 4 Re-decide against new state
│   │   ├── Input still relevant (e.g., reply for current step) → new decision → write phase
│   │   ├── Input now late (saga moved on: compensating, cancelled, completed) → late-input policy
│   │   │   (e.g., late success → add to compensation set)
│   │   └── Input obsolete (timer for resolved step) → acknowledge, no change
│   ├── Step 5 Retry budget
│   │   ├── Backoff with jitter: random(floor, min(cap, base × 2^n)) [L: floor 5–10 ms, cap ~200 ms]
│   │   ├── Attempts [L: 5–10] — conflicts usually resolve quickly
│   │   └── Exhausted (OC-06)
│   │       ├── Engine input → leave unacknowledged / reschedule durably; contention alert
│   │       └── Client command → 409 to client with current version
│   └── Edge cases
│       ├── E7.1 Re-applying a reply already applied by the winner — duplicate next-step dispatch — CORRUPT [C]
│       ├── E7.2 Exhaustion acknowledges the reply — step outcome lost — saga stuck — LOSS [C]
│       ├── E7.3 Re-decision ignores that saga was cancelled meanwhile — forward step dispatched after cancel — CORRUPT [C]
│       └── E7.4 Sleep inside an open transaction — locks/snapshots held during backoff [C]
│
├── 8. CLIENT EDITS  (OC-09)
│   ├── Client reads saga (with version) → edits → submits with expected version
│   ├── Server CAS with client's expected version (plus authorization, tenant)
│   ├── Conflict → 409 with current state/version; client re-presents to user (no silent merge)
│   ├── Engine progress also bumps version → user edits during active processing conflict by design
│   └── Edge cases
│       ├── E8.1 Server retries client edit automatically on new version — overwrites engine's progress with stale intent — CORRUPT [C]
│       └── E8.2 Edits allowed on fields engine already acted on (amount after payment) — business rule must forbid by state [G]
│
├── 9. AGGREGATE BOUNDARY  (OC-02)
│   ├── Saga root + steps + context + timers = one consistency boundary
│   ├── Any writer to children must take the root CAS (or root lock)
│   ├── Read-only side tables (history, audit) may be appended without bumping version, but in the same transaction
│   └── E9.1 Reply handler updates step row without root CAS — concurrent decision uses stale step — CORRUPT [C]
│
├── 10. MULTI-AGGREGATE TRANSACTIONS
│   ├── Avoid: saga transitions should not update other aggregates (resources) in the same transaction
│   ├── If unavoidable: fixed lock order (by aggregate type then id), short transactions
│   ├── Deadlock detected → rollback → retry as conflict (budgeted)
│   └── E10.1 Saga + inventory row updated together in different orders by different workers — deadlocks [C]
│
├── 11. ADDITIONAL EDGE CASES
│   ├── Version overflow → 64-bit integer [C]
│   ├── Migration of existing rows without version → initialize to 0 (not NULL) [C]
│   ├── Bulk operator action across many sagas → each saga CAS independently; partial failures reported per saga
│   ├── Replay/redrive of old input → OC-05 check prevents re-application
│   └── Clock: updated_at informational only, never used for conflict decisions
│
└── 12. FEATURE TEST SCENARIOS
    ├── Two workers apply same reply concurrently → one applies, other sees "already applied" and acknowledges
    ├── Reply and timer for same step race → one transition; other re-decides as obsolete
    ├── Zombie worker with old epoch re-reads latest version → CAS fails on epoch → stops
    ├── Row deleted → DATA_ERROR, input quarantined, no retry loop
    ├── Conflict exhaustion on engine reply → reply still pending; processed later; alert raised
    ├── Saga cancelled between read and CAS → re-decision treats reply as late input
    ├── Step row updated by handler → root version incremented → concurrent decision conflicts correctly
    ├── Client edit with stale version → 409 with current version; no auto-merge
    ├── 50 parallel replies on serialized saga → processed sequentially without conflicts
    ├── Decision code calling external service → blocked by design (no side effects before commit)
    ├── Backoff never occurs inside an open transaction
    └── Rows with NULL version after migration → initialized; first CAS succeeds
```

[Certain] The core corrections follow from OCC semantics in a saga engine:
- Conflicts mean another writer acted, possibly applying the same input.
- Version checks don't establish ownership.
- Zero rows has several causes.
- Child rows outside the version boundary allow stale decisions.
- Dropping an input after retries loses outcomes.

[Likely] Retry counts, backoff values, and the choice between serialization and row locks for hot sagas are defaults to tune.

[Guessing] Which saga fields clients may edit, and in which states, is a business decision.

Two decisions are yours:
1. **Add the input-already-applied check and the ownership epoch to every conflict path (OC-01, OC-05).** Together they stop duplicate next-step dispatches and zombie writes, the two failures OCC alone can't prevent.
2. **Serialize hot sagas (§2) instead of relying on OCC retries.** Parallel fan-out sagas will otherwise spend most of their time conflicting with themselves.
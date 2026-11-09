# Inbox Deduplication — Business Logic Decision Tree

```
INBOX DEDUPLICATION — BUSINESS LOGIC DECISION TREE (v2)
Legend: [C] Certain   [L] Likely (sound default)   [G] Guessing (business/owner must decide)
│
├── 0. SCOPE — WHAT THE INBOX GUARANTEES (AND WHAT IT DOES NOT)
│   ├── Guarantees
│   │   └── A given (handler, message identity) is applied at most once for effects committed in the SAME database
│   │       transaction as the inbox record — combined with redelivery, effectively once for those effects [C]
│   ├── Does NOT guarantee (needs other mechanisms)
│   │   ├── External side effects (payments, emails, HTTP calls) → §4 model M2
│   │   ├── Effects in other stores (search, cache, second database) → §4 model M3
│   │   ├── Ordering / stale events (older version arriving after newer) → version guard (§10)
│   │   └── Business duplicates with DIFFERENT message ids (double submit, upstream retry minting new ids)
│   │       → business idempotency key / natural key in the handler
│   └── E0.1 Inbox treated as "exactly-once system" — teams skip version guards and effect idempotency — CORRUPT [C]
│
├── 1. INVARIANTS
│   ├── IB-01 Dedup identity is the producer-assigned message identity, never a transport position [C]
│   ├── IB-02 Uniqueness is (handler identity, message identity) — never message identity alone [C]
│   ├── IB-03 Inbox record and every effect in the same store commit in ONE transaction [C]
│   ├── IB-04 A duplicate is acknowledged only after its content is confirmed identical [C]
│   ├── IB-05 The consumer position never moves past a message that is neither applied, held, nor quarantined [C]
│   ├── IB-06 Dedup retention ≥ the longest time between first and last possible delivery of the same message [C]
│   ├── IB-07 Any cache is an accelerator for known duplicates only; the database decides "new" [C]
│   └── IB-08 Attempt counts and failure history live outside the rolled-back business transaction [C]
│
├── 2. MESSAGE IDENTITY RESOLUTION
│   └── Trigger: record fetched
│       ├── Check 2.1: Producer message identity present (event_id in envelope/headers)?
│       │   ├── YES → validate format (non-empty, expected type/length) → identity = event_id
│       │   │   └── Invalid format → quarantine (contract defect) — never fall back silently
│       │   └── NO → Check 2.2
│       │
│       ├── Check 2.2: Stream policy for missing identity (declared per topic)
│       │   ├── T0 / financial streams → REJECT: quarantine + key hold + producer incident (no synthesized identity)
│       │   ├── Streams with versioned aggregates → identity = (aggregate type, aggregate id, aggregate version)
│       │   │   (stable across replays/re-emits; duplicates of the same version absorbed) [C]
│       │   └── Legacy/unkeyed streams (last resort) → position identity =
│       │       (cluster id, topic, topic incarnation id, partition, offset) [C]
│       │       └── Accepts: catches transport redelivery only; replays/re-emits NOT caught (documented risk)
│       │
│       ├── Check 2.3: Payload-hash identity?
│       │   └── Only if the stream declares "identical payload = same fact" [G]
│       │       └── Otherwise forbidden: two legitimate identical events (two identical purchases) would be merged — LOSS
│       │
│       ├── Check 2.4: Scope
│       │   ├── Handler identity = (service, handler name, handler generation) — generation changes only for rebuilds (§13)
│       │   └── Tenant included if message ids are only unique per tenant [G: producer contract]
│       │
│       └── Edge cases
│           ├── E2.1 Offset-based key after topic recreation — new messages skipped as duplicates — LOSS [C]
│           ├── E2.2 Offset-based key with relay replay — same event at new offset applied twice — CORRUPT [C]
│           ├── E2.3 Producer generates event_id at relay time (new id on each relay retry) — duplicates not caught — CORRUPT [C]
│           │   └── Handling: event_id must be created in the producer's business transaction (outbox row)
│           ├── E2.4 Two producers use overlapping id spaces (sequential integers) — cross-stream collisions — LOSS [C]
│           │   └── Handling: identity namespaced by (source stream / aggregate type) unless ids are globally unique
│           └── E2.5 Header carrying event_id stripped by an intermediary — falls into 2.2 — policy decides, never silent
│
├── 3. INBOX RECORD DESIGN
│   ├── Unique key: (handler identity, message identity) — IB-02
│   ├── Fields: content hash (canonical payload), aggregate id + version (if present), source topic/partition/offset,
│   │   topic incarnation id, received at (DB clock), producer event time (for retention partitioning)
│   ├── No "PROCESSING" status in the single-transaction model (row exists only if effects committed)
│   └── Edge cases
│       ├── E3.1 Global unique on message id only — second handler skips its own work — LOSS [C]
│       ├── E3.2 Inbox row committed before effects with status PROCESSING — crash leaves it — every redelivery skipped — LOSS [C]
│       └── E3.3 Content hash computed over non-canonical bytes (field order, whitespace) — false integrity alarms [C]
│
├── 4. PROCESSING MODEL  (declared per handler)
│   │
│   ├── M1 SINGLE TRANSACTION (all effects in the inbox's database)
│   │   ├── Step 1 Begin transaction
│   │   ├── Step 2 Insert inbox row (conflict-ignore); read affected rows
│   │   │   ├── 0 rows → duplicate path (§5) → rollback
│   │   │   └── 1 row → continue
│   │   ├── Step 3 Version guard / business validation (§10)
│   │   ├── Step 4 Apply effects (business tables) + outbox rows for follow-up events
│   │   ├── Step 5 Store consumer position for this partition (if positions are DB-stored)
│   │   ├── Step 6 Commit
│   │   └── Edge cases
│   │       ├── E4.1a Network call made inside the transaction — locks held across network latency — contention/timeouts [C]
│   │       ├── E4.1b Unique-violation raised as exception (instead of conflict-ignore) — transaction aborted mid-way,
│   │       │         error noise, retries [C]
│   │       └── E4.1c Commit outcome ambiguous (connection lost at commit) — redelivery decides: if committed, inbox
│   │                 row exists → duplicate path; if not, applies — correct by construction [C]
│   │
│   ├── M2 EXTERNAL SIDE EFFECTS (payment, email, partner API)
│   │   ├── Step 1 Transaction A: insert inbox row + insert effect INTENT row(s) (each with idempotency key derived
│   │   │          from (handler, message identity, effect name)) + store position → commit
│   │   ├── Step 2 Intent executor calls the external system with the intent's idempotency key
│   │   ├── Step 3 Transaction B: record intent outcome (SUCCEEDED / REJECTED / UNKNOWN)
│   │   ├── UNKNOWN → query by idempotency key / retry with same key (never a new key)
│   │   └── Edge cases
│   │       ├── E4.2a External call before inbox commit, then rollback — redelivery calls again — double charge [C]
│   │       ├── E4.2b External provider ignores idempotency keys — retries duplicate — needs query-before-act
│   │       │         or human review for unknown outcomes [C]
│   │       ├── E4.2c Provider idempotency window shorter than intent retry horizon — late retry double-executes [C]
│   │       └── E4.2d Message duplicate arrives while intent still executing — inbox says duplicate (correct);
│   │                 intent continues once [C]
│   │
│   └── M3 MULTI-STORE (DB + search/cache)
│       ├── Authoritative effects in DB with inbox (M1); derived stores updated via outbox/fan-out with versions
│       └── E4.3a Inbox in DB, search write done directly in handler — inbox committed, search failed (or reverse)
│                 — permanent divergence [C]
│
├── 5. DUPLICATE PATH  (inbox insert affected 0 rows)
│   ├── Step 1 Read existing inbox row (content hash, source position)
│   ├── Step 2 Compare content hash (IB-04)
│   │   ├── Equal → TRUE DUPLICATE → acknowledge (position may advance) → count
│   │   └── Different → INTEGRITY FAILURE (same identity, different content)
│   │       ├── Do not apply; quarantine + hold aggregate key (if keyed) + alert producer owner
│   │       └── Causes: producer bug, id reuse, split-brain producer, restore at producer
│   └── Edge cases
│       ├── E5.1 Duplicate acknowledged without content check — corrupted second version silently ignored — CORRUPT [C]
│       └── E5.2 Existing row from a previous handler generation (rebuild) — wrong scope — §13
│
├── 6. CONCURRENCY
│   ├── Same message processed concurrently (zombie consumer after rebalance, overlapping workers)
│   │   ├── Second insert waits on the first transaction's uncommitted unique entry [C]
│   │   ├── First commits → second gets 0 rows → duplicate path
│   │   └── First rolls back → second inserts → processes (correct)
│   ├── Lock ordering: inbox insert first, then business rows, in a fixed order — avoids deadlocks with other handlers
│   ├── Lock wait timeout on inbox insert → treat as transient (retry), not duplicate
│   └── Edge cases
│       ├── E6.1 Lock timeout treated as "duplicate" — message acknowledged unprocessed — LOSS [C]
│       ├── E6.2 Batch with keys in different order across consumers — deadlocks [C]
│       │   └── Handling: sort batch by inbox key before inserting
│       └── E6.3 Zombie applies after losing ownership — inbox prevents double apply, but ordering may break — version guard needed (§10)
│
├── 7. FAILURE HANDLING & POSITION SAFETY  (IB-05, IB-08)
│   ├── Failure classification
│   │   ├── Transient (DB lock timeout, connection reset) → retry in place (same partition blocked briefly) with backoff
│   │   ├── Dependency outage → pause partition (do not burn attempts)
│   │   ├── Permanent (validation, contract, handler bug) → after attempt budget → quarantine + hold key
│   │   └── Crash during handler (process dies) → persisted attempt count detects crash loops
│   ├── Attempt tracking
│   │   ├── Attempts recorded in a separate short transaction BEFORE processing (survives rollback/crash) — IB-08
│   │   └── Over budget at pickup → quarantine without executing
│   ├── Position rules
│   │   ├── Position advances only after: applied (committed), duplicate (confirmed), held, or quarantined (committed)
│   │   ├── Parallel lanes: position = lowest offset not yet in one of those states (contiguity)
│   │   └── Never commit a later offset while an earlier one is unresolved
│   └── Edge cases
│       ├── E7.1 Failed message left uncommitted while later offsets committed — failed message skipped forever — LOSS [C]
│       ├── E7.2 Attempt counter stored in the inbox row (rolled back with the transaction) — infinite retries — OUTAGE [C]
│       ├── E7.3 Poison message blocks partition indefinitely — all later messages delayed — DEGRADE [C]
│       │   └── Handling: attempt budget → quarantine + key hold (partition flows for other keys)
│       └── E7.4 Rollback of handler also rolls back the "duplicate" decision — no issue (re-evaluated next delivery) [C]
│
├── 8. FAST-PATH CACHE  (optional; IB-07)
│   ├── Decision: add only if duplicate volume is high enough that DB duplicate checks matter [G: measure]
│   ├── Correct usage
│   │   ├── Exact keys (handler identity + message identity), never Bloom filters for skipping
│   │   ├── Written strictly AFTER the DB transaction commits
│   │   ├── Cache hit → treat as known duplicate (content check optional if hash cached alongside)
│   │   ├── Cache miss → normal DB path (miss never means "new" by itself)
│   │   └── Cache TTL ≤ DB inbox retention
│   ├── Failure behavior: cache unavailable → bypass (DB path), never fail processing
│   └── Edge cases
│       ├── E8.1 Cache written before commit, transaction rolls back — later delivery skipped as duplicate — LOSS [C]
│       ├── E8.2 Bloom filter positive used to skip — false positive drops a new message — LOSS [C]
│       ├── E8.3 Cache key without handler scope — another handler's success skips this handler — LOSS [C]
│       └── E8.4 Cache eviction/failover — more DB checks, no correctness impact [C]
│
├── 9. RETENTION & PRUNING  (IB-06)
│   ├── Window = max of: topic retention, producer re-emit horizon, reconciliation re-publish horizon,
│   │   DLQ redrive window, manual replay into the same handler generation [G: per stream]
│   ├── Prune by producer event time (identical on every redelivery), not by received time
│   ├── Table partitioning must keep uniqueness enforceable
│   │   ├── Partition key = producer event time bucket included in the unique key (same value on every redelivery) [C]
│   │   └── Never partition by received time (duplicate in new partition accepted)
│   ├── Purge horizon = min(window, oldest unprocessed message time) — never prune while consumers lag behind it
│   └── Edge cases
│       ├── E9.1 Inbox pruned after 7 days; producer re-emits a 10-day-old event — applied again — CORRUPT [C]
│       │   └── Handling: window per IB-06, or version guard as permanent second layer (§10)
│       ├── E9.2 Uniqueness only per partition (partitioned by received date) — cross-day duplicates accepted — CORRUPT [C]
│       └── E9.3 Producer event time missing → bucket by received time → falls into E9.2 → require event time in envelope [C]
│
├── 10. INTERACTION WITH ORDERING (inbox + version guard)
│   ├── Order of checks inside the transaction: inbox insert → version decision → effects
│   ├── Version decision (aggregate-scoped streams)
│   │   ├── version = last + 1 → apply
│   │   ├── version ≤ last → stale/duplicate → acknowledge (inbox row kept to remember this identity)
│   │   ├── version > last + 1 → gap → hold key (do not apply); inbox row NOT committed (rollback) so later delivery re-evaluates
│   │   └── same version, different content → integrity (§5)
│   └── Edge cases
│       ├── E10.1 Inbox alone accepts an older event never seen before (v5 after v6) — regression — CORRUPT [C]
│       └── E10.2 Inbox row committed for a held (gap) event — later redelivery treated as duplicate, never applied — LOSS [C]
│
├── 11. BATCH PROCESSING
│   ├── Batch = multiple messages in one transaction (throughput)
│   ├── Same identity twice within a batch → second insert returns 0 rows → duplicate path (correct)
│   ├── One failing message rolls back the whole batch → retry batch without the failing message (bisect) or process singly
│   ├── Position advanced only to the end of the committed contiguous batch
│   └── Edge cases
│       ├── E11.1 Whole batch retried forever because of one poison message — partition stalls — OUTAGE [C]
│       └── E11.2 Batch spanning messages of the same aggregate with a gap in the middle — partial apply must stop at gap [C]
│
├── 12. TOPIC, CLUSTER & TOPOLOGY CHANGES
│   ├── Event-id identity survives: partition increases, topic migrations, mirroring, cluster failover [C]
│   ├── Position identity (2.2 last resort) breaks on all of them → include cluster id + topic incarnation; review on every change
│   └── E12.1 DR failover to mirrored cluster — offsets differ — position identities differ — duplicates re-applied [C]
│
├── 13. REPLAYS, REBUILDS & REDRIVES
│   ├── Rebuild into a NEW projection → new handler generation → empty inbox scope for that generation
│   ├── Live handler is never given a fresh inbox to "allow replay" (would re-apply everything)
│   ├── DLQ redrive into the same handler → same identity → inbox decides (held events never had committed inbox rows)
│   ├── Producer re-emit (same event_id) → absorbed as duplicate (if within window)
│   └── E13.1 Inbox truncated to force reprocessing in production — mass duplicate effects — CORRUPT [C]
│
├── 14. VOLUME & TABLE HEALTH  (feature-relevant limits)
│   ├── One inbox row per (handler, message) → size = message rate × handlers × retention window
│   ├── Time-ordered identities improve index locality [L]
│   ├── Pruning in partition drops (by producer event time bucket), not row-by-row deletes at high volume
│   └── E14.1 Inbox insert latency grows with bloated index — handler throughput collapses — DEGRADE [C]
│
└── 15. FEATURE TEST SCENARIOS
    ├── Same event delivered twice (same offset) → applied once
    ├── Same event re-emitted at a new offset → applied once (event-id identity)
    ├── Topic recreated, offsets restart → new messages applied (no false duplicates)
    ├── Same event_id with different content → not applied; integrity alert; key held
    ├── Two handlers in one service consume the same event → both apply their own effect
    ├── Crash after effects, before commit → redelivery applies once
    ├── Crash after commit, before position stored externally → redelivery detected as duplicate
    ├── Message 100 fails, 101–150 succeed → 100 never skipped (held/quarantined or partition blocked)
    ├── Handler with payment call → intent + idempotency key; redelivery doesn't double charge
    ├── Concurrent zombie and new owner process same message → single application
    ├── Lock timeout on inbox insert → retried, not acknowledged
    ├── Cache written after commit only; cache outage → processing continues via DB
    ├── Event older than inbox window re-emitted → version guard rejects as stale
    ├── v7 arrives with gap → held; inbox row not committed; later delivery applies after v6
    └── Batch with one poison message → others committed; poison quarantined; position safe
```

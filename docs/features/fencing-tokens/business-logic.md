Your fencing check `fencing_token < :fencing_token` rejects the **legitimate leaseholder** on its second write.

The first write stores token 42. The holder's next write also carries 42, and `42 < 42` is false, so zero rows are affected. Your tree then classifies the rightful owner as a zombie and aborts it. Every saga gets exactly one transition per lease. [Certain]

The check must accept **equal** tokens (current holder) and reject only **lower** ones (stale holders).

Four more errors, ranked by damage:

**1. Tokens from Redis `INCRBY` can go backwards or repeat after failover.** [Certain]
I disagree because Redis replication is asynchronous. A failover can lose recent increments, so the counter rewinds and **issues a token that was already issued**.
- **Duplicate token:** two holders both pass an "equal-is-allowed" check, which is exactly the split brain fencing exists to prevent.
- **Lower token:** the rightful new holder is rejected everywhere, which is an outage.

Here's what I'd do instead: issue the token **in the same atomic operation that grants the lease**, from the same durable store that enforces it (a PostgreSQL lease row whose epoch increments on acquisition), so issuance is unique and survives failover.

**2. Fencing one row doesn't fence the work.** [Certain]
I disagree because the zombie also writes step rows, timers, and outbox rows, and may call workers directly. Only writes **inside the same transaction, gated by the fenced check performed first**, are protected.
External calls (RPC, Redis, search, providers) can't check your token at all. Commands must leave only via the outbox written in the fenced transaction, never by direct call from the worker.

**3. Lease and token are acquired as separate steps.** [Certain]
"CAS the lease, then INCRBY a counter" is two operations. A crash or race between them yields a lease with no token, or a token with no lease, and nothing ties a token to the lease it supposedly represents.

**4. Rows with a NULL `fencing_token` fail every comparison.** [Certain]
`NULL < 42` evaluates to unknown, so zero rows are affected. Every pre-existing saga becomes unwritable the moment fencing is deployed.

```
FENCING TOKENS — BUSINESS LOGIC DECISION TREE (v2)
Legend: [C] Certain   [L] Likely (sound default)   [G] Guessing (owner must decide)
│
├── 0. PURPOSE & BOUNDARY
│   ├── Protects: writes to stores that CHECK the token atomically with the write (saga database)
│   ├── Does NOT protect by itself
│   │   ├── External effects (provider APIs, emails) → idempotency keys + outbox from fenced transactions
│   │   ├── Stores that can't check tokens (cache, search, message brokers) → version guards / outbox fan-out
│   │   └── Decisions made on stale reads → harmless only if every resulting write is fenced
│   └── E0.1 Fencing assumed to make zombie workers harmless while they still call workers directly — duplicate commands [C]
│
├── 1. INVARIANTS
│   ├── FT-01 Lease grant and token issuance are ONE atomic operation in ONE durable authority [C]
│   ├── FT-02 Tokens are unique and strictly increasing per resource across failovers, restarts, and restores [C]
│   ├── FT-03 Protected stores accept token ≥ stored token and reject token < stored token [C]
│   ├── FT-04 The fenced check is the FIRST statement of the transaction; every other write in it depends on it [C]
│   ├── FT-05 A token is constant for the whole lease term (renewal never changes it) [C]
│   ├── FT-06 Lease expiry is judged by the authority's clock, never the holder's [C]
│   ├── FT-07 Outbound commands are emitted only from fenced transactions (outbox) [C]
│   ├── FT-08 A rejected holder stops all work for that resource and must re-acquire and re-read before acting again [C]
│   ├── FT-09 Lease records and stored tokens are never deleted or reset to lower values [C]
│   └── FT-10 Stored tokens are NOT NULL, initialized to 0 [C]
│
├── 2. LEASE & TOKEN ISSUANCE
│   ├── 2.1 Authority choice
│   │   ├── Same database as the protected data (PostgreSQL lease row) — preferred [C]
│   │   │   └── Atomic acquisition: one conditional update on the lease row:
│   │   │       holder = me, epoch = epoch + 1, expires = authority now + lease duration
│   │   │       WHERE resource = R AND (expired OR released) → returns new epoch (= token)
│   │   ├── Consensus store (linearizable) — acceptable if tokens are its revision numbers [L]
│   │   └── Asynchronously replicated cache — forbidden for token issuance
│   ├── 2.2 Granularity
│   │   ├── Per partition (worker owns a shard of sagas): token = partition epoch; each saga write checks partition epoch
│   │   └── Per resource (single saga/step lease): token = resource epoch
│   ├── 2.3 Uniqueness & monotonicity (FT-02)
│   │   ├── Epoch increments only inside the acquisition update (no separate counter)
│   │   ├── Lease row never deleted (FT-09); resource re-creation reuses or continues epoch
│   │   └── Restore from backup: lease rows and protected data restored together (same database) — consistent
│   └── Edge cases
│       ├── E2.1 Cache counter loses increments after failover — duplicate/lower tokens — split brain or outage [C]
│       ├── E2.2 Lock acquired in one system, token from another — token not tied to lease — meaningless fencing [C]
│       ├── E2.3 Lease row deleted on release and recreated with epoch 0 — old zombie holds higher token and wins [C]
│       └── E2.4 Two concurrent acquisitions on expired lease — conditional update lets exactly one succeed [C]
│
├── 3. LEASE LIFECYCLE
│   ├── 3.1 Acquire → token T bound to the lease (FT-01)
│   ├── 3.2 Renew
│   │   ├── Conditional update: expires = authority now + duration WHERE resource = R AND holder = me AND epoch = T
│   │   ├── Token unchanged (FT-05)
│   │   ├── 0 rows → lease lost → stop work (FT-08)
│   │   └── Renewal interval ≤ duration / 3 [L]
│   ├── 3.3 Expiry
│   │   ├── Judged by authority clock (FT-06)
│   │   ├── Expired lease remains valid for writes until another holder acquires (epoch unchanged) —
│   │   │   the holder must still self-fence (§8) to avoid acting on a lease it can no longer renew
│   │   └── New holder's acquisition increments epoch → previous holder's writes now rejected
│   ├── 3.4 Release (graceful shutdown, rebalance)
│   │   ├── Finish or abort in-flight fenced transactions FIRST
│   │   ├── Mark released WHERE holder = me AND epoch = T (epoch unchanged; next acquirer increments)
│   │   └── Local state discarded after release
│   └── Edge cases
│       ├── E3.1 Renewal increments the token — holder's own in-flight transactions rejected — self-inflicted failures [C]
│       ├── E3.2 Holder judges validity by its own timer after a long pause — acts on lost lease — writes rejected (good),
│       │         but external effects already sent if not routed via outbox (bad) [C]
│       └── E3.3 Release before in-flight transaction commits — new holder and old transaction overlap — fence rejects old (correct) [C]
│
├── 4. TOKEN PROPAGATION
│   ├── Token bound to the unit of work (per saga processing task), not to a global worker variable
│   ├── Every fenced transaction carries T; every outbox command records T (for diagnostics and target-side checks)
│   ├── Token never accepted from external callers (only issued by authority)
│   └── E4.1 Long-running task reuses the token of a previous lease after re-acquisition — stale token, rejected writes [C]
│
├── 5. PROTECTED WRITE PROTOCOL  (FT-03, FT-04, FT-10)
│   ├── Step 1 Begin transaction
│   ├── Step 2 Fenced check (first statement):
│   │   ├── Partition model: lease row for partition P has holder = me AND epoch = T (lock the row in share mode
│   │   │   or verify with conditional update) — OR —
│   │   └── Resource model: update resource SET token = T WHERE id = R AND token ≤ T (accept equal) → returns state
│   ├── Step 3 0 rows → §6 (rejection) → rollback
│   ├── Step 4 All further writes of the transaction (state, steps, timers, history, outbox rows) proceed
│   ├── Step 5 Commit
│   └── Edge cases
│       ├── E5.1 Strict "<" comparison — current holder rejected after first write — false zombie — saga stalls [C]
│       ├── E5.2 Fence check placed after other writes — those writes committed by a zombie before check? No: same tx rolls back;
│       │         but writes in OTHER transactions/sessions without check are unprotected [C]
│       ├── E5.3 NULL stored token — every comparison unknown — all writes rejected after rollout [C]
│       └── E5.4 Separate read to diagnose rejection races with new holder — wrong diagnosis [C]
│           └── Handling: single statement returning stored token/holder alongside the result
│
├── 6. REJECTION HANDLING  (0 rows from fenced check)
│   ├── Diagnose from the same statement's returned values
│   │   ├── Stored token > T (or lease epoch > T) → STALE HOLDER (zombie)
│   │   │   ├── Abort the unit of work; cancel local timers and in-memory work for that resource/partition
│   │   │   ├── Do NOT retry the write with a refreshed token
│   │   │   ├── Optionally re-acquire (if allowed) → re-read full state → re-decide from scratch
│   │   │   └── Record fencing violation (expected occasionally; spikes indicate pauses/partitions)
│   │   ├── Resource not found → data error (not a zombie) → normal error handling
│   │   └── Token matches but business precondition failed (expected state mismatch) → concurrency/state conflict
│   │       → re-read and re-decide (not a zombie)
│   └── Edge cases
│       ├── E6.1 Zombie retries with a new token without re-reading — applies decisions made on stale state — CORRUPT [C]
│       └── E6.2 State-precondition failures misclassified as zombies — healthy worker aborts repeatedly — saga stalls [C]
│
├── 7. TARGETS THAT CANNOT CHECK TOKENS  (FT-07)
│   ├── Step workers / external services
│   │   ├── Commands emitted only via outbox rows written in fenced transactions → zombie cannot emit
│   │   ├── Commands carry idempotency keys (duplicates harmless) and T
│   │   └── Targets that can, keep "max token seen per saga/resource" and reject lower tokens [L]
│   ├── Cache / search / derived stores → fed from committed state (fan-out with versions), never written by zombie path
│   ├── Message broker → only via outbox relay (which has its own ownership fencing)
│   └── Edge cases
│       ├── E7.1 Worker sends RPC directly after a GC pause — duplicate step execution — CORRUPT [C]
│       └── E7.2 Zombie writes cache entry directly — stale data served — CORRUPT (derived) [C]
│
├── 8. SELF-FENCING (LOCAL SAFETY)
│   ├── Before any action with external visibility, check remaining lease time ≥ safety margin [L: ≥ 2× expected action time]
│   ├── Every operation deadline < remaining lease time
│   ├── Renewal failure → stop starting new work immediately; finish nothing that isn't fenced
│   └── E8.1 Worker starts a 60-second operation with 5 seconds left on lease — completes after new holder took over [C]
│
├── 9. TIMING PARAMETERS
│   ├── Lease duration > worst-case pause (GC, VM stall, network hiccup) + renewal interval [L: tens of seconds]
│   ├── Too short → ownership flapping, repeated aborts; too long → slow takeover after crash
│   ├── Authority clock only (FT-06); host clock skew irrelevant to correctness
│   └── E9.1 Lease shorter than routine GC pauses — constant takeovers — throughput collapse [C]
│
├── 10. FAILURE SCENARIOS
│   ├── Worker stop-the-world pause → lease expires → new holder (T+1) → old worker resumes → writes with T rejected ✓
│   ├── Network partition (worker ↔ database) → renewals fail → worker self-fences; new holder acquires ✓
│   ├── Authority (database) failover with synchronous replication → lease rows and tokens preserved ✓
│   ├── Authority failover with async replication → recent acquisitions may be lost → tokens may be reissued
│   │   → use synchronous replication for the lease table's database, or treat failover as full ownership reset [C]
│   ├── Backup restore → lease and data rewind together → all holders must re-acquire after restore [C]
│   ├── Clock jump on worker → no effect on expiry (authority clock) ✓
│   └── Split brain at authority (two primaries) → fencing cannot help → prevented by database HA fencing [C]
│
└── 11. FEATURE TEST SCENARIOS
    ├── Holder performs 10 consecutive transitions with same token → all succeed (equal accepted)
    ├── Worker paused past lease; new holder acquires T+1 → old worker's write rejected; no state change
    ├── Rejected worker does not retry with new token without re-reading state
    ├── Renewal keeps token unchanged; in-flight transaction with T commits
    ├── Release then re-acquire by another worker → epoch increases, never resets
    ├── Lease row deletion attempted → prevented (policy/constraint)
    ├── Existing rows with no stored token after rollout → initialized to 0; first fenced write succeeds
    ├── Two workers race to acquire expired lease → exactly one succeeds
    ├── Zombie attempts direct RPC → architecture prevents (commands only via fenced outbox)
    ├── State precondition mismatch → classified as conflict, not zombie
    ├── Database failover (synchronous) → tokens continue increasing; no duplicates
    └── Worker with 5 s lease left skips starting a long external action
```

[Certain] The core corrections follow from fencing semantics:
- Strict less-than rejects the current holder.
- Counters on asynchronously replicated caches can rewind and reissue tokens.
- Lease and token must be granted atomically.
- Only transactions gated by the fenced check are protected.
- NULL comparisons fail.
- Resetting epochs hands control back to stale holders.

[Likely] Lease durations, renewal ratios, and target-side token checks are defaults that depend on your runtimes' pause behavior and target capabilities.

[Guessing] Whether the lease database runs synchronous replication is an infrastructure decision with latency cost.

Two decisions are yours:
1. **Move token issuance into the lease grant itself, in PostgreSQL (FT-01, FT-02).** Drop the Redis counter. This removes duplicate and rewound tokens, and it makes "equal is allowed" safe.
2. **Forbid direct outbound calls from saga workers (FT-07).** Fencing protects only what the database checks. Every command must leave through the outbox written in a fenced transaction, or a paused worker will still execute steps twice.
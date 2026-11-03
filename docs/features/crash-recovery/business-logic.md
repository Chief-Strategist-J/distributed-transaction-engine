Two things are wrong at the root of this tree. First, recovery only runs when a pod boots. A saga orphaned by a worker that hangs, gets cut off from the database, or loses its lease while staying alive is never reclaimed until some unrelated pod happens to restart. Second, tier 3 will compensate healthy sagas.

## Critical bugs (fix before adding edge cases)

**1. PENDING steps are reconciled as if they were in flight.** [Certain] I disagree because, under runtime S10, `DISPATCHED` is committed before any network call. A PENDING step was therefore never sent. Querying the downstream service for it returns NOT_FOUND, your tree marks the step FAILED, and the saga starts compensating even though nothing went wrong. Here's what I'd do instead: never query for PENDING steps; just schedule them normally. The risk in your approach is that every crash turns into spurious refunds and releases.

**2. NOT_FOUND is treated as FAILED.** [Certain] I disagree because NOT_FOUND for an in-flight step doesn't prove failure. The request may still be sitting in a queue, in a network retry, in an async pipeline, or behind replica lag. Here's what I'd do instead: re-send the request with the same idempotency key. If the original arrives later, the downstream deduplicates it. The risk: you mark the step FAILED and compensate, the original charge lands two seconds later, and it is never refunded.

**3. Only "the last step" is reconciled.** [Certain] I disagree because your sagas are DAGs, and six steps can be DISPATCHED at the moment of the crash. Here's what I'd do instead: reconcile every non-terminal step, independently, without one blocking the others. The risk: five of those six steps stay in flight forever.

**4. The takeover CAS doesn't re-check that the lease has expired.** [Certain] I disagree because heartbeats renew `heartbeat_at` without bumping `fencing_token`. If the owner renews between your scan and your CAS, the token still matches and you steal a live saga, leaving two owners. Here's what I'd do instead: add `AND lease_until < db_now()` to the UPDATE itself. The risk: double dispatch under normal load, not just during failures.

**5. Pod ID as lease owner.** [Certain] I disagree because StatefulSet pods restart with the same name. A new process sees `worker_id = me` and keeps renewing leases for sagas it has no in-memory state for, so nobody ever recovers them. Here's what I'd do instead: use an owner identity of pod name plus a UUID generated at boot. The risk: sagas silently stuck after every pod restart, with no error anywhere.

Other gaps, all [Certain]:

- **Liveness probe tied to the database.** Combined with "connection fails → crash loop", a 45-second DB failover becomes a fleet-wide restart storm. When the DB comes back, every pod reconnects at once and can knock it over again.
- **Redis leases can't fence Postgres writes.** Redis also loses data on failover, so a lease stored there can vanish and let two workers own the same saga.
- **The scan misses some states.** It only covers RUNNING and COMPENSATING, so sagas that crashed in CREATED or ROLLING_FORWARD never recover.
- **A step marked SUCCEEDED by reconciliation has no output.** Downstream steps and compensations need that output; a refund needs the `charge_id`.
- **The outbox CLAIMED status never expires.** If the relay dies mid-batch, those events are lost forever. Republishing an older claimed row after a newer one breaks per-saga ordering.
- **Recovery ignores conditions that changed during the outage.** Deadlines that passed, cancels requested, and pauses set while the pod was down are not checked.
- **No ramp-up after mass recovery.** If 400k sagas resume in the same second, your downstream services get a self-inflicted outage.
- **Nothing makes the old owner stop.** The original worker, if alive but partitioned from the database, keeps calling downstream services.

---

## Revised tree

Everything below is [Certain] unless tagged otherwise. State names match runtime v4.

```
CRASH RECOVERY — DECISION TREE v2 (PRODUCTION)
Legend: ──> outcome  [STATE]  ◆ real failure  ⚠ policy
Principle: recovery is NOT a boot phase. Every worker reclaims expired leases continuously.
│
├── 0. PROCESS LIFECYCLE TIER
│   │
│   ├── 0.1 Startup
│   │   ├── incarnation_id = pod_name + boot UUID (never reused across restarts)
│   │   │   └── ◆ StatefulSet `worker-0` restarts, matches lease_owner, "renews" leases
│   │   │         for sagas it has no state for; nobody ever recovers them
│   │   ├── Postgres pool: bounded; pods × pool ≤ max_connections − headroom
│   │   ├── Redis: optional; never required to start or to be correct ⚠
│   │   ├── Journal schema version compatible with this binary?
│   │   │   └── NO (journal written by newer schema) ──> readiness=false, take no leases, alert
│   │   │       └── ◆ Deploy rollback: old code meets AWAITING_CALLBACK, unknown enum → wrong branch
│   │   └── Binary supports every spec_version with in-flight sagas?
│   │       └── NO ──> exclude those sagas from this worker's polling + alert
│   │
│   ├── 0.2 Probes
│   │   ├── Liveness: event loop responsive ONLY — no DB / Redis check
│   │   ├── Readiness: DB reachable AND schema compatible
│   │   └── ◆ Liveness on DB: 45s failover → all pods restart-loop → reconnect storm
│   │         when DB returns → DB falls over again
│   │
│   ├── 0.3 DB unreachable at runtime ──> backoff + jitter reconnect; never exit the process
│   │
│   └── 0.4 Graceful shutdown (SIGTERM)
│       ├── Stop polling for new work
│       ├── Let in-flight calls finish until (grace period − margin)
│       ├── Journal results, then RELEASE leases (lease_until = db_now())
│       ├── Calls unfinished at margin ──> leave DISPATCHED (becomes UNKNOWN on takeover)
│       └── ◆ No release on deploy → every rolling update stalls each saga one full lease TTL
│
├── 1. CONTINUOUS ORPHAN DETECTION TIER  (every worker, every T seconds, leaderless)
│   │
│   ├── 1.1 Candidate query
│   │   SELECT id, fencing_token FROM saga_instances
│   │   WHERE state IN ('CREATED','RUNNING','ROLLING_FORWARD','COMPENSATING')
│   │     AND lease_until < db_now()
│   │   ORDER BY lease_until            -- oldest orphans first
│   │   LIMIT :batch
│   │   FOR UPDATE SKIP LOCKED
│   │   ├── Partial index ON (lease_until) WHERE state is non-terminal
│   │   ├── ◆ Missing CREATED / ROLLING_FORWARD → those sagas never recover
│   │   └── ◆ No partial index → full scan of 200M historical sagas every 10s → DB CPU pegged
│   │
│   ├── 1.2 Capacity gate (before claiming)
│   │   ├── owned_sagas(me) ≥ MAX_OWNED ──> claim nothing this round
│   │   ├── Claim rate ≤ R per worker per second
│   │   └── ◆ Lone surviving pod claims 400k orphans after zone loss → OOM → leases expire → repeat
│   │
│   ├── 1.3 Tenant fairness: round-robin tenants within each batch
│   │
│   └── 1.4 Never reclaimed (human states):
│       PAUSED, PAUSED_POISON, NEEDS_OPERATOR, COMPENSATION_STUCK, STUCK_FORWARD
│
├── 2. TAKEOVER PROTOCOL TIER
│   │
│   ├── 2.1 Atomic CAS — single statement
│   │   UPDATE saga_instances
│   │   SET lease_owner    = :my_incarnation,
│   │       lease_until    = db_now() + :L,
│   │       fencing_token  = fencing_token + 1,
│   │       takeover_count = takeover_count + 1
│   │   WHERE id = :id
│   │     AND fencing_token = :observed_token
│   │     AND lease_until < db_now()          ← REQUIRED expiry re-check at write time
│   │   RETURNING fencing_token
│   │   ├── 0 rows ──> owner renewed or another worker won ──> skip (normal)
│   │   │   └── ◆ Without expiry re-check: renewal doesn't bump token → live saga stolen
│   │   └── 1 row ──> [SAGA_RECLAIMED] with token T'
│   │
│   ├── 2.2 saga_events row in same txn: {prev_owner, new_owner, reason, token}
│   │
│   ├── 2.3 Poison guard
│   │   ├── takeover_count persisted BEFORE any processing (done in 2.1)
│   │   ├── takeover_count > N (e.g. 5) within window ──> PAUSED_POISON, release lease, page
│   │   └── ◆ 2 MB step output crashes every worker during rebuild; fleet crash-loops one by one
│   │
│   └── 2.4 Lease authority = Postgres only ⚠
│       └── ◆ Redis lease lost in Redis failover (async replication) → two owners;
│             Redis tokens can't be checked in Postgres WHERE clauses
│
├── 3. OLD-OWNER SELF-FENCING TIER  (the half of the split nobody draws)
│   │
│   ├── 3.1 Heartbeat every L/3:
│   │   UPDATE … SET lease_until = db_now() + L
│   │   WHERE id = :id AND lease_owner = :me AND fencing_token = :mine
│   ├── 3.2 0 rows ──> lease lost ──> abort all work for that saga immediately
│   ├── 3.3 DB unreachable ──> self-fence at (last successful renewal + L − margin),
│   │   measured on MONOTONIC clock ──> stop ALL outbound calls for ALL owned sagas
│   │   └── ◆ Worker partitioned from DB, not from payments API, keeps charging while
│   │         new owner re-dispatches
│   ├── 3.4 Every journal write includes `AND fencing_token = :mine`
│   └── 3.5 Responses arriving after self-fence ──> discard (new owner resolves via same key)
│
├── 4. SAGA TRIAGE TIER  (after takeover, before touching steps)
│   │
│   ├── 4.1 Load pinned spec BY spec_hash from Postgres (never cache-only, never "latest")
│   │   └── Not found ──> NEEDS_OPERATOR + page (violates registry I1/I11)
│   │
│   ├── 4.2 Rebuild state from journal (step rows + saga_events); discard all cached state
│   │
│   ├── 4.3 Conditions that changed DURING the outage — evaluate in order:
│   │   ├── cancel_requested ──> runtime 7.2
│   │   ├── deadline_at passed ──> runtime 7.1 (pre-pivot: compensate; post-pivot: continue + alert)
│   │   ├── workflow / action paused ──> honor, release lease
│   │   └── retry timers due during outage ──> eligible, subject to ramp-up (Tier 6)
│   │
│   └── 4.4 Route by saga state
│       ├── CREATED ──> start normally (runtime Tier 2)
│       ├── RUNNING / ROLLING_FORWARD ──> Tier 5, then forward
│       └── COMPENSATING ──> Tier 5, then compensation (drain rules still apply)
│
├── 5. STEP RECONCILIATION TIER — EVERY non-terminal step, independently
│   │   ◆ 6 steps DISPATCHED at crash; "last step" logic reconciles 1; 5 stuck forever
│   │
│   ├── 5.1 PENDING  (never sent — DISPATCHED is committed before I/O)
│   │   ├── NO downstream query
│   │   └── Schedule normally
│   │   └── ◆ v1 queries, gets NOT_FOUND, marks FAILED → healthy saga compensates
│   │
│   ├── 5.2 DISPATCHED  (call may or may not have left the pod) ──> UNKNOWN
│   │   ├── db_now() − first_dispatched_at ≥ downstream key TTL − margin?
│   │   │   ├── YES ──> NO re-dispatch ──> reconcile_action only (5.3) ⚠
│   │   │   │   └── ◆ 30h outage, Stripe key expired, re-dispatch = second charge
│   │   │   └── NO ──> re-dispatch with SAME :fwd key (dedup IS the query)
│   │   │       ├── 2xx + output ──> SUCCEEDED, output journaled
│   │   │       ├── Permanent 4xx ──> FAILED_PERMANENT (no effect)
│   │   │       └── Ambiguous again ──> UNKNOWN, durable retry (runtime 3.4)
│   │   └── Action has no dedup AND no reconcile_action ──> NEEDS_OPERATOR
│   │       (registry 4.2 should have rejected this definition)
│   │
│   ├── 5.3 reconcile_action outcomes
│   │   ├── EXECUTED + output ──> SUCCEEDED
│   │   ├── EXECUTED, no output ──> SUCCEEDED; any descendant / compensation needing
│   │   │   that output ──> fetch via action, else NEEDS_OPERATOR
│   │   ├── FAILED, no effect ──> FAILED_PERMANENT
│   │   ├── FAILED, partial effect ──> FAILED_PARTIAL (compensable)
│   │   ├── NOT_FOUND ──> NOT proof of failure
│   │   │   ├── Request may be queued / in network retry / async / behind replica lag
│   │   │   ├── Within TTL ──> re-dispatch same key (late original then deduped)
│   │   │   ├── Past TTL ──> re-query after delay, bounded; then UNKNOWN
│   │   │   └── ◆ NOT_FOUND → FAILED → compensate; original lands 2s later; never refunded
│   │   └── UNREACHABLE ──> UNKNOWN, durable probe timer, time-bounded
│   │       └── Budget exceeded:
│   │           ├── Non-pivot pre-pivot step ──> include in compensation ("maybe happened")
│   │           └── Pivot ──> NEEDS_OPERATOR + page (direction can't be chosen)
│   │
│   ├── 5.4 RETRY_WAIT
│   │   ├── Previous attempt definitively resolved ──> keep next_attempt_at
│   │   └── Previous attempt ambiguous ──> treat as 5.2
│   │
│   ├── 5.5 AWAITING_CALLBACK
│   │   ├── Callbacks are journaled by ANY pod, not only the owner ⚠
│   │   │   └── ◆ Callback arrives while owner is dead → 404 → provider stops retrying → lost
│   │   ├── Callback journaled during outage ──> apply it
│   │   └── Not journaled, callback deadline passed ──> poll status if supported, else UNKNOWN
│   │
│   ├── 5.6 COMPENSATING step ──> re-issue with SAME :comp key; never re-run forward
│   │
│   ├── 5.7 SUCCEEDED / SKIPPED / COMPENSATED / NOT_REQUIRED / FAILED_PERMANENT ──> trusted
│   │
│   └── 5.8 Non-blocking: one UNREACHABLE step never blocks independent branches
│
├── 6. RESUME & RAMP-UP TIER
│   │
│   ├── 6.1 Ready set re-derived from journal (runtime 2.2) — never from pre-crash memory
│   │
│   ├── 6.2 Slow start after mass recovery ⚠
│   │   ├── Trigger: reclaims/min > threshold
│   │   ├── Dispatch rate ramps 10% → 100% over K minutes
│   │   ├── Per-action breakers start HALF-OPEN
│   │   └── ◆ 400k sagas resume in one second → 429 storm → retry budgets burned
│   │         → mass compensation
│   │
│   ├── 6.3 Priority order ⚠
│   │   COMPENSATING > ROLLING_FORWARD > RUNNING > CREATED
│   │   (finish undoing money movement before starting new money movement)
│   │
│   └── 6.4 Hand to normal scheduler ──> [RECOVERY_COMPLETE] (per saga)
│
├── 7. OUTBOX RELAY RECOVERY TIER  (separate continuous process)
│   │
│   ├── 7.1 Claims are LEASES (claimed_by, claimed_until), not a CLAIMED flag
│   │   └── ◆ Relay dies holding 5k CLAIMED rows; flag never resets; events lost forever
│   ├── 7.2 Expired claims reclaimed with CAS + expiry re-check (same as 2.1)
│   ├── 7.3 Delivery: publish ──> mark SENT; crash between ⇒ duplicate (accepted)
│   │   ├── event_id from outbox row on every message ──> consumers dedupe
│   │   └── Kafka idempotent producer dedupes within ONE producer session only
│   ├── 7.4 Ordering
│   │   ├── Partition key = saga_id
│   │   ├── Per saga, publish strictly in outbox sequence; later rows wait on earlier ones
│   │   ├── Consumers also drop seq ≤ last_seen (defense in depth)
│   │   └── ◆ Reclaimed "saga_running" published after "saga_completed" → order shown
│   │         "in progress" forever
│   ├── 7.5 Kafka down ──> rows stay PENDING, relay backs off, alert on oldest-pending age
│   └── 7.6 Unpublishable event (size, serialization) ──> PARKED + page
│       └── Blocks only later events of the SAME saga, not the partition
│
├── 8. CATASTROPHIC SCENARIOS
│   │
│   ├── 8.1 Postgres failover, synchronous replica
│   │   └── No journal loss; workers reconnect and renew; renewal gap > L ⇒ normal takeover
│   │
│   ├── 8.2 Postgres failover, ASYNC replica (last N seconds lost)
│   │   ├── Journal regresses: SUCCEEDED / DISPATCHED steps reappear as PENDING
│   │   │   └── Re-dispatch is safe ONLY because idempotency keys are deterministic
│   │   ├── fencing_token regresses ──> a stale worker's token can match again
│   │   │   └── Mitigation: token = (cluster_epoch, counter); bump epoch on every promotion ⚠
│   │   └── ◆ Saga row created in last 2s is gone; client holds a 202 for a saga that doesn't
│   │         exist; client retry with same external_id recreates it (dedup key saves you)
│   │
│   ├── 8.3 Point-in-time restore
│   │   └── 8.2 at scale: bump epoch, pause all workflows, reconcile every non-terminal
│   │       saga via reconcile_actions, then resume with ramp-up ⚠
│   │
│   ├── 8.4 Redis total loss ──> no correctness impact; single-flight cold reload
│   │
│   ├── 8.5 Full region loss ──> DR region: same journal, epoch bumped, ramp-up mandatory
│   │
│   ├── 8.6 Split brain across regions
│   │   └── Only one Postgres primary accepts fenced writes; calls the stale side made before
│   │       self-fencing are absorbed by idempotency keys  [Likely — depends on 3.3 margin]
│   │
│   └── 8.7 Downstream lost ITS idempotency store
│       └── Re-dispatch may double-execute ──> for pivot / money steps, reconcile FIRST ⚠
│
├── 9. QUARANTINE & HUMAN HANDOFF
│   ├── Human states: lease released, excluded from scans (1.4)
│   ├── Runbook actions (audited): retry-reconcile, set-step-outcome-with-evidence,
│   │   force-compensate (pre-pivot only), resume
│   └── Age-based SLA alerts per human state
│
└── X. OBSERVABILITY & VERIFICATION
    ├── Takeovers/min by reason (expiry, release, poison); oldest orphan age
    ├── UNKNOWN steps per action; reconcile outcome mix; NOT_FOUND rate
    ├── Self-fence events per pod; heartbeat renewal p99 (alert > L/3)
    ├── Outbox pending count, oldest pending age, duplicate-publish rate
    └── Chaos suite (staging, every release):
        SIGKILL mid-dispatch · SIGKILL mid-compensation · DB failover · worker↔DB partition
        · clock skew · deploy rollback · Redis loss · Kafka outage · mass reclaim
```

## Invariants

| ID | Invariant |
|---|---|
| R1 | Recovery is continuous and leaderless; a pod booting is not a special event |
| R2 | A takeover CAS re-checks lease expiry against DB time inside the same statement |
| R3 | Owner identity is a per-incarnation value and is never reused across restarts |
| R4 | An owner that cannot renew stops all outbound calls before `lease_until − margin` (monotonic clock) |
| R5 | PENDING steps are never queried downstream, never failed by recovery, and never compensated |
| R6 | A DISPATCHED step found during recovery is UNKNOWN, never FAILED |
| R7 | NOT_FOUND from a downstream service is never treated as proof of failure |
| R8 | Every non-terminal step is reconciled, independently and non-blocking |
| R9 | Recovery never re-dispatches past the downstream service's idempotency-key TTL |
| R10 | The takeover counter is persisted before processing; a saga exceeding N takeovers is quarantined |
| R11 | Liveness never depends on Postgres, Redis, or Kafka |
| R12 | Fencing tokens include a cluster epoch that is bumped on every DB promotion or restore |
| R13 | Outbox delivery is at-least-once, ordered per saga, and deduplicated by consumers via `event_id` |
| R14 | Mass recovery ramps up its dispatch rate; it never resumes at full speed |

## Decisions you have to make

1. **Lease timing.** [Likely] L = 30 s, heartbeat every 10 s, self-fence margin 5 s is a sane starting point. The margin must exceed your p99 GC pause plus DB round-trip, so measure it before trusting it.
2. **Cluster epoch in fencing tokens.** [Certain] You need this if your Postgres HA uses async replicas. With synchronous replication only, you can skip it.
3. **Callback ingestion by any pod.** [Likely] This needs a separate stateless endpoint that writes only to step rows and never takes the saga lease.
4. **Ramp-up threshold and curve.** [Guessing] Size this from your slowest critical downstream's rate limit, not from your own capacity.
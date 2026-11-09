# Fencing Tokens — Failure Cases, Edge Conditions & Chaos Matrix

## 1. Classification Tiers

| Tier | Name | Scope & Severity | Invariants Enforced |
|---|---|---|---|
| Tier 0 | Protocol & Syntax Boundary | Malformed tokens, negative durations, empty identifiers | Input Validation Gate |
| Tier 1 | Ingress & Lease Acquisition | Concurrent lease contention, already-held leases, zero-increment bypass | FT-01, FT-02 |
| Tier 2 | Mutation Barrier Gate | First-statement barrier checks, strictly monotonic comparisons, null protection | FT-03, FT-04, FT-10 |
| Tier 3 | Zombie Interception & Quarantining | GC pause stale writes, split-brain write suppression, audit isolation | FT-08, FT-09 |
| Tier 4 | Distributed Heartbeat & Self-Fencing | Renewal timeout, clock desynchronization, remaining lease safety margin | FT-05, FT-06 |
| Tier 5 | Downstream Egress Protection | RPC bypass prevention, outbox transactional pairing | FT-07 |
| Tier 6 | Storage Infrastructure & Chaos | Primary database crash/failover, replication lag, restore rollback | FT-02, FT-09 |

---

## 2. Comprehensive Edge Cases Matrix

### Tier 0: Protocol & Syntax Boundary
- **E0.1: Direct Remote Procedure Call Bypass**
  - *Trigger*: Developer attempts to call third-party HTTP gateway directly from saga worker thread instead of using transactional outbox.
  - *Consequence*: If the worker experiences a GC pause after the RPC but before committing state, a zombie duplicate call occurs with no DB barrier protection.
  - *Mitigation*: Architecture policy bans direct HTTP clients in saga step handlers; commands must be appended to `outbox_events` within the fenced transaction.
- **E0.2: Negative or Zero Lease Duration**
  - *Trigger*: Client requests `lease_duration_ms = 0` or negative.
  - *Mitigation*: Schema validation rejects with `VALIDATION_FAILED` (minimum duration: 1,000 ms).

### Tier 1: Ingress & Lease Acquisition
- **E2.1: Cache Counter Failover Inconsistency**
  - *Trigger*: Token generation offloaded to asynchronously replicated cache (e.g., Redis). Cache primary crashes before replica receives `INCRBY`.
  - *Consequence*: Counter rewinds. New worker receives a token lower than or equal to an existing token, violating monotonicity.
  - *Mitigation*: Tokens are strictly generated from PostgreSQL sequence/epoch increments inside the atomic lease acquisition SQL update.
- **E2.2: Decoupled Lock and Token Issuance**
  - *Trigger*: Worker acquires lock in one system and requests token from another.
  - *Consequence*: Crash between acquisition and token issuance creates orphaned lock or unattached token.
  - *Mitigation*: FT-01 enforces atomic issuance: `UPDATE fencing_leases ... RETURNING epoch`.
- **E2.3: Lease Row Deletion and Epoch Reset**
  - *Trigger*: Cleanup routine deletes expired lease rows and recreates them with `epoch = 0`.
  - *Consequence*: Stale zombie holding `epoch = 42` wakes up and clobbers new worker's `epoch = 1` writes.
  - *Mitigation*: Hard SQL constraints prohibit `DELETE` on `fencing_leases`. Released leases retain their current epoch and only increment.
- **E2.4: Concurrent Acquisition Race on Expired Lease**
  - *Trigger*: Two standby workers simultaneously observe `expires_at < NOW()` and attempt takeover.
  - *Mitigation*: Atomic conditional update with row lock guarantees exactly one worker updates the row; the other receives 0 affected rows.

### Tier 2: Mutation Barrier Gate
- **E5.1: Strict Less-Than (`<`) Comparison Hazard**
  - *Trigger*: Engine evaluates `stored_token < :token` instead of `<=`.
  - *Consequence*: Step 1 sets `stored_token = 42`. On Step 2, the same valid holder presents `token = 42`. `42 < 42` evaluates to FALSE, causing the engine to misclassify its legitimate owner as a zombie and halt the saga.
  - *Mitigation*: FT-03 strictly mandates `fencing_token <= :token` (accepting equal tokens for the current holder).
- **E5.2: Inverted Barrier Placement**
  - *Trigger*: Developer executes step mutation before the fencing check inside the transaction.
  - *Mitigation*: While rollback protects the DB on failure, FT-04 requires the fencing gate to be Statement 1 to fail fast and prevent unnecessary lock acquisitions.
- **E5.3: NULL Stored Token Rollout Hazard**
  - *Trigger*: Database migration adds `fencing_token` column without `DEFAULT 0 NOT NULL`.
  - *Consequence*: In SQL, `NULL <= 42` evaluates to `UNKNOWN` (falsy in `WHERE`), causing 0 rows affected and blocking every existing transaction.
  - *Mitigation*: Column declared `fencing_token BIGINT NOT NULL DEFAULT 0`.
- **E5.4: Read-Then-Update Race During Diagnosis**
  - *Trigger*: Worker reads token, compares in application memory, then executes mutation.
  - *Mitigation*: Application-layer checks are strictly advisory. Storage-layer atomic CAS update is the final gate.

### Tier 3: Zombie Interception & Quarantining
- **E6.1: Zombie Retries Without State Refresh**
  - *Trigger*: Worker rejected with 0 rows fetches a new token and immediately retries the write.
  - *Consequence*: Worker commits stale decisions made before the pause over newly committed state.
  - *Mitigation*: FT-08 mandates immediate worker abort. The worker must cancel in-memory work, re-acquire lease, and reload the entire saga state before executing.
- **E6.2: State-Precondition Misclassification**
  - *Trigger*: Worker's update returns 0 rows due to `state = 'RUNNING'` mismatch rather than a token violation.
  - *Mitigation*: Engine queries `fencing_token` on 0 rows. If `stored_token > token`, it logs `STALE_TOKEN`. If `stored_token == token`, it logs `CONCURRENCY_CONFLICT`.

### Tier 4: Distributed Heartbeat & Self-Fencing
- **E3.1: Heartbeat Renewal Increments Token**
  - *Trigger*: Renewal routine increments epoch on every heartbeat.
  - *Consequence*: In-flight transactions carrying earlier token $T$ are rejected by their own worker.
  - *Mitigation*: FT-05 enforces `epoch = :current_epoch` during renewal.
- **E8.1: Work Dispatched With Insufficient Lease Headroom**
  - *Trigger*: Worker starts a 10-second external execution when the lease expires in 2 seconds.
  - *Consequence*: Lease expires mid-flight, allowing takeover before the operation completes.
  - *Mitigation*: Self-fencing check requires remaining lease duration $\ge 2 \times$ expected step latency.

### Tier 5 & 6: Infrastructure & Chaos Matrix
- **E10.1: Stop-the-World GC Pause (20s)**
  - *Scenario*: Worker 1 pauses for 20s. Lease duration is 15s.
  - *Behavior*: Worker 2 takes over at $t=15.1\text{s}$ (Token 43). Worker 1 wakes at $t=20\text{s}$ and attempts write with Token 42.
  - *Outcome*: Mutation rejected (0 rows affected). Rollback executed. `fencing_audits` logs `STALE_TOKEN`.
- **E10.2: Primary Database HA Failover**
  - *Scenario*: Primary crashes immediately after granting Token 42.
  - *Behavior*: Synchronous PostgreSQL replication guarantees replica contains the updated `epoch = 42`.
  - *Outcome*: Failover promote retains epoch 42; subsequent acquisition grants Token 43. No token rewind.


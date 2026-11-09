# ADR-0011: Monotonic Fencing Tokens, Atomic In-Storage Lease Epochs & Zombie Write Quarantine

## Status
ACCEPTED

## Context
In a distributed transaction engine, workers execute asynchronous multi-step sagas concurrently. While lease mechanisms (such as heartbeat leases in crash recovery) guarantee single-worker ownership during normal operations, they fail under routine distributed systems phenomena:
1. **Stop-the-World Garbage Collection Pauses**: A worker holding a lease pauses for 20 seconds. Its lease expires in the database. A standby worker detects the expired lease, reclaims ownership, and advances the saga to a new state. When the paused worker wakes up, it resumes executing its stale in-flight step and writes the outdated result back to the database.
2. **Asymmetric Network Partitions**: A worker can communicate with downstream third-party payment gateways but cannot reach the primary engine database to renew its lease. The cluster reassigns the saga. Both workers now concurrently dispatch operations on the same saga instance.
3. **Clock Skew and Desynchronization**: If worker nodes rely on local clocks to judge lease expiration, NTP adjustments or drift cause workers to act on leases that the database already deemed expired.
4. **Asynchronous Cache Invalidation Hazards**: Storing fencing counters in asynchronously replicated in-memory data grids (e.g., Redis `INCRBY`) introduces counter rewinds upon node failover. If a failover loses recent increments, a new worker is granted a duplicate or lower token than a zombie worker, enabling split-brain state clobbering.

Without cryptographic or strictly monotonic barriers enforced at the point of storage mutation, zombie workers will execute stale writes, overwrite newer state transitions, and produce phantom executions.

## Decision Drivers
- **Atomic Issuance & Single Authority (FT-01)**: Lease acquisition and fencing token increment must occur in a single atomic SQL statement in the same durable ACID datastore where state mutations are persisted.
- **Strict Monotonicity & Failover Survivability (FT-02)**: Fencing tokens must strictly increase per resource and never rewind or duplicate across restarts, crash takeovers, or replication failovers.
- **Non-Decreasing Acceptance Invariant (FT-03)**: Protected writes must accept tokens that are equal to or greater than the stored token (`token >= stored_token`) and reject tokens that are lower (`token < stored_token`). Strict less-than (`<`) is forbidden as it causes self-rejection of the valid leaseholder on subsequent steps.
- **Transaction-Leading Fencing Barrier (FT-04)**: The fenced token check must be the very first statement inside a database mutation transaction. Downstream state, step journals, timers, and transactional outbox rows depend entirely on the success of this check.
- **Authority Clock Expiry (FT-06)**: Lease expirations must be evaluated exclusively against the database engine's authoritative monotonic clock (`NOW()`), eliminating worker clock drift.
- **Gated Command Emission via Outbox (FT-07)**: Direct remote procedure calls from workers to external systems are prohibited. External effects must be enqueued via the transactional outbox inside the fenced transaction to guarantee that zombies cannot execute duplicate remote calls.

## Considered Options

### Option 1: Distributed Lock with External Redis Counter (`INCRBY`)
- *Mechanism*: Acquire a distributed lock via Redis Redlock and generate a token via Redis `INCRBY(fencing_key, 1)`.
- *Downsides*: Two-phase coordination hazard. If the node crashes between lock grant and counter increment, tokens desynchronize. Furthermore, during Redis Sentinel or Cluster failover, asynchronous replication lag causes counter rewinds, reissuing duplicate tokens and re-introducing split-brain mutations.

### Option 2: Pure Optimistic Concurrency Control (OCC) Version Checks
- *Mechanism*: Rely solely on integer `version = version + 1` CAS on the saga instance row.
- *Downsides*: OCC detects concurrent writes but does not track worker lease ownership. If Worker A pauses at Step 2 and Worker B finishes Step 2 (advancing version 2 to 3) and begins Step 3, Worker A waking up could theoretically match a subsequent step version or trigger repeated false-conflict rollbacks without recognizing that its lease was revoked.

### Option 3 (Selected): In-Storage Monotonic Lease Epochs with First-Statement Fencing Gates
- *Mechanism*: Maintain an authoritative `fencing_leases` table in PostgreSQL.
  - Lease acquisition/takeover is executed via an atomic conditional SQL `UPDATE`:
    ```sql
    UPDATE fencing_leases
    SET holder_id = :worker_id,
        epoch = epoch + 1,
        acquired_at = NOW(),
        expires_at = NOW() + (:duration_ms || ' milliseconds')::INTERVAL,
        released_at = NULL,
        updated_at = NOW()
    WHERE tenant_id = :tenant_id
      AND resource_id = :resource_id
      AND (expires_at < NOW() OR released_at IS NOT NULL)
    RETURNING epoch, public_id;
    ```
  - Renewal maintains the same token (`epoch = :current_epoch`) and strictly checks `holder_id = :worker_id AND epoch = :current_epoch`.
  - Every saga mutation begins with:
    ```sql
    UPDATE saga_instances
    SET fencing_token = :token, updated_at = NOW()
    WHERE tenant_id = :tenant_id AND id = :saga_id AND fencing_token <= :token
    RETURNING fencing_token;
    ```
  - If 0 rows are affected, the worker halts immediately, writes a violation audit record, and aborts in-memory execution.

## Invariants & Design Contracts
- **FT-01**: Lease grant and token issuance are ONE atomic operation in ONE durable authority.
- **FT-02**: Tokens are unique and strictly increasing 64-bit integers per resource.
- **FT-03**: Protected tables accept `token >= stored_token` and reject `token < stored_token`.
- **FT-04**: The fenced gate is executed as statement #1 of the transaction before any state, step, or outbox mutation.
- **FT-05**: Renewals never modify or increment the epoch.
- **FT-06**: Lease duration is evaluated against database `NOW()`.
- **FT-07**: Outbound actions must use transactional outbox rows committed in the fenced transaction.
- **FT-08**: A rejected worker stops immediately without retrying the write using a refreshed token without a complete re-read.
- **FT-09**: Lease rows are never deleted; epochs never decrement.
- **FT-10**: Stored fencing tokens are `NOT NULL`, initialized to 0.

## Consequences

### Positive
- Total immunity against split-brain mutations caused by GC stalls, virtualization pauses, or network partitions.
- Failover safety: because lease epochs live in PostgreSQL, synchronous database replication preserves token monotonicity across HA failover.
- Clean rejection diagnosis: distinguishing between zombie workers (`stored_token > token`) and concurrency precondition conflicts (`stored_token == token` but status mismatch).

### Negative
- Increased transaction latency: every mutation transaction requires executing the fencing barrier update.
- Strict requirement on outbox usage: workers cannot perform fire-and-forget external HTTP calls directly; all external interactions must traverse the outbox pipeline.


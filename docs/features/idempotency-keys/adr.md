# ADR-0009: Authoritative Transaction-Scoped Idempotency Keys, Canonical Fingerprinting, Concurrent In-Flight Locking & Replay Semantics

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-11-09 |
| Deciders | Lead Systems Architect & Core Architecture Guild |
| Related Docs | [Business Logic & Decision Tree v2](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/idempotency-keys/business-logic.md) · [Diagrams](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/idempotency-keys/diagrams.md) · [Failure Cases & Mitigations](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/idempotency-keys/failure-cases.md) |
| Target Feature | [Idempotency Keys Feature](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/src/features/idempotency-keys) |

---

## 1. Context & Problem Statement

Distributed API callers (mobile clients, web applications, microservices, and external B2B partner integrations) execute critical state-changing operations across unreliable network boundaries. Typical operations include initiating payment authorizations, submitting saga workflows, reserving warehouse inventory, executing wallet transfers, or requesting order cancellations.

In distributed computing, networks are inherently unreliable (Fallacy #1 of Distributed Computing). Network timeouts, connection drops, reverse proxy resets (HTTP 502/504), and client-side retries inevitably cause requests to be received multiple times. When an operation is not idempotent, retries produce catastrophic consequences:
* **Duplicate Financial Debits**: Submitting the same credit card charge or account transfer twice, causing customer overdrafts and financial liability.
* **Orphaned Sagas & Resource Leaks**: Initiating two parallel saga executions for a single checkout intent, reserving double inventory while charging only once.
* **Corrupted Inventory & Accounting**: Decrementing warehouse stock levels or ledger entries on every transient network timeout retry.

Common naive implementations of idempotency introduce fatal production vulnerabilities:
1. **The Distributed Lock/Cache Fallacy (Redis Lock Anti-Pattern)**: Using Redis with a TTL as the idempotency record. If the operation takes longer than the lock TTL or Redis fails over, a client retry executes again concurrently, causing double execution (E4.1, E4.2). Furthermore, Redis keys lack transactional atomicity with the database effect (IK-01).
2. **The Lock-Only Mistake (Missing Outcome Snapshots)**: Treating an idempotency key merely as a mutex. Once the first request completes, the key is unlocked/deleted; a subsequent retry sees "no lock" and re-executes the operation from scratch (E0.1).
3. **The Raw Payload Hash Trap (Key Reordering Collisions)**: Hashing raw request body bytes. A client retry that serializes identical JSON fields in a different order generates a different hash, erroneously triggering a 422 Conflict or double execution (E2.4a).
4. **The Cross-Principal Security Breach (Tenant-Only Scoping)**: Scoping keys solely by `(tenant_id, key)`. If User A and User B coincidentally generate the same UUID or natural key, User B receives User A's private response or payment receipt (E2.1, E10.1).
5. **The Non-Persistent Failure Flaw**: Forgetting to record business rejections (e.g., insufficient funds or account suspended). If conditions change between retries, a rejected operation suddenly succeeds on retry under the same key, violating the immutable intent contract (E5.1).

---

## 2. Decision Drivers & Architecture Invariants (IK-01 – IK-09)

The Idempotency Keys engine is governed by nine fundamental invariants defined in the business logic contract:

* **IK-01 (Authoritative Transactional Atomicity)**: The idempotency record MUST commit in the exact same database transaction as the operation's business mutation (e.g., creating the saga row and outbox record). No idempotency record may exist without the effect, and no effect may commit without the record.
* **IK-02 (RDBMS-Enforced Uniqueness)**: Uniqueness MUST be enforced by PostgreSQL via a composite unique index on `(tenant_id, principal_id, operation_id, idempotency_key)`—never by a distributed cache, Redis mutex, or volatile in-memory map.
* **IK-03 (Strict Four-Dimensional Scope)**: Scope is defined strictly as:
  $$\text{Scope} = (\text{tenant\_id}, \text{principal\_id}, \text{operation\_id}, \text{idempotency\_key})$$
  Tenant ID and Principal ID are derived strictly from authenticated claims, never from client-controlled request bodies or unauthenticated query parameters.
* **IK-04 (Conservative Retention & Release)**: A key is released to "new" ONLY when the operation is authoritatively known NOT to have executed (e.g., pre-write schema validation failures, rate limit 429 rejections, or clean transaction rollbacks). If an outcome is ambiguous (HTTP 500/504), the key remains in `UNKNOWN` and is never released.
* **IK-05 (Canonical Semantic Fingerprinting)**: Request fingerprints are generated using SHA-256 over a canonical representation of the operation: operation name, API version, path parameters, semantically relevant query parameters, and canonical JSON body (sorted keys, normalized numbers, trimmed whitespace). Dynamic client nonces, timestamps, and tracing headers are strictly excluded.
* **IK-06 (Immutable Outcome Snapshots & Replay)**: Final outcomes—both 2xx successes and 4xx business rejections—are captured as immutable response snapshots (HTTP status code, selected headers, response body payload). Subsequent retries replay the exact snapshot with an `Idempotent-Replayed: true` header.
* **IK-07 (Re-Authorization on Replay)**: Every replayed response is re-authenticated and re-authorized against the current caller's active token before the stored snapshot is returned, preventing revoked users from accessing historical data (E10.1).
* **IK-08 (Durable Retention Horizon)**: Authoritative idempotency records must be retained for at least the maximum client retry horizon (e.g., 7 to 30 days for payment operations). Expired records are pruned asynchronously by background workers, never during active request processing.
* **IK-09 (Deterministic Downstream Key Derivation)**: Downstream effects and external service calls MUST receive deterministic idempotency keys derived as a pure mathematical function of the root request key:
  $$\text{Key}_{\text{step}} = \text{HMAC-SHA256}(\text{RootKey}, \text{step\_id} \parallel \text{attempt})$$

---

## 3. Considered Options & Alternatives

### Option A: Redis Distributed Mutex (Redlock / SETNX with TTL)
* **Description**: Acquire a Redis key on request arrival; release on completion or expire after 60 seconds.
* **Pros**: Sub-millisecond latency; offloads locking from PostgreSQL.
* **Cons**:
  * **Violates IK-01**: Cannot be part of the PostgreSQL transaction boundary. If Redis crashes or the DB rollback occurs, state diverges.
  * **TTL Expiry Race (E4.1)**: If downstream takes 61 seconds, the lock expires; a concurrent retry double-executes.
  * **No Durable Snapshots**: Cannot reliably store long-term (30-day) response snapshots without high RAM cost.
* **Verdict**: **Rejected**.

### Option B: Gateway-Level Reverse Proxy Caching (NGINX / Envoy)
* **Description**: Cache responses at the API gateway based on `Idempotency-Key` HTTP header.
* **Pros**: Zero backend compute on duplicate requests.
* **Cons**:
  * **No In-Flight Concurrency Control**: Multiple concurrent requests with the same key bypass cache and execute in parallel.
  * **No Fingerprint Tamper Detection**: Replays outdated responses even if the client altered the body payload.
  * **No Transactional Guarantee**: Gateway caches responses even if the downstream database transaction later rolled back.
* **Verdict**: **Rejected**.

### Option C: Database-Authoritative Dual-Phase State Machine (Chosen)
* **Description**: PostgreSQL authoritative table `idempotency_keys` storing scope, canonical fingerprint, state (`IN_PROGRESS`, `COMPLETED`, `REJECTED_FINAL`, `UNKNOWN`), response snapshots, and lease metadata.
* **Pros**:
  * Guaranteed transactional atomicity with domain entities via PostgreSQL ACID semantics.
  * Uniqueness enforced by PostgreSQL B-tree index.
  * Safe in-flight concurrency management: concurrent requests either wait on DB lock or receive HTTP 409 with `Retry-After`.
  * Tamper detection via canonical SHA-256 fingerprinting.
* **Cons**: Additional database write and row-lock overhead per state-changing request.
* **Verdict**: **Accepted**.

---

## 4. Execution State Machine

```
                      +-------------------+
                      |   [Non-Existent]  |
                      +---------+---------+
                                |
                   Insert (Tx 1 / Single-Tx)
                                |
                                v
                      +-------------------+
                      |    IN_PROGRESS    |
                      +----+---------+----+
                           |         |
              Success /    |         | Business Rejection
              202 Accepted |         | (e.g. 402, 422)
                           v         v
                     +-----------+ +----------------+
                     | COMPLETED | | REJECTED_FINAL |
                     +-----+-----+ +-------+--------+
                           |               |
              Replay Exact |  Replay Exact |
              Response (2xx|  Response (4xx|
              + Replay Hdr)|  + Replay Hdr)|
                           v               v
                     +------------------------------+
                     |       Caller Terminal        |
                     +------------------------------+
```

When an unhandled crash or network partition occurs during external execution:
```
IN_PROGRESS (Lease Expired) ---> UNKNOWN (Requires Recovery Sweep via Derived Key)
```

---

## 5. Consequences & Operational Trade-offs

### Positive Consequences
* **Absolute Duplicate Prevention**: Eliminates double payments, duplicate saga orchestrations, and leaked resources under all network failure modes.
* **Zero Client Disruption**: Legitimate retries following timeouts or dropped connections transparently receive the original response snapshot.
* **Tamper Proof**: Reusing an idempotency key with a modified payload is immediately rejected with HTTP 422 Unprocessable Entity, preventing accidental payload collision bugs.
* **Strict Tenant & Principal Isolation**: Guarantees that clients can never view or intercept another user's replayed response.

### Negative Consequences & Mitigations
* **Database Write Amplification**: Each idempotent operation requires row inserts and updates in `idempotency_keys`.
  * *Mitigation*: Partition `idempotency_keys` by `tenant_id` and prune expired rows via daily batch workers.
* **Response Snapshot Storage Overhead**:
  * *Mitigation*: Responses over 64 KB are stored as references or stripped of non-essential debug metadata.


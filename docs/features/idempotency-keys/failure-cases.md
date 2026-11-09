# Idempotency Keys — Failure Modes, Edge Cases & Mitigation Matrix

This document establishes the comprehensive failure taxonomy, edge-case analysis (E0.1 – E11.3), and chaos engineering mitigation strategies for the Authoritative Idempotency Keys feature.

---

## 1. Failure Taxonomy Across 6 Tiers

| Tier | Failure Class | Root Cause | Impact | Mitigation Strategy |
|---|---|---|---|---|
| **Tier 1** | **Admission & Validation** | Missing header, key length > 128, malformed characters, read endpoint tagging. | Invalid client request admitted or storage polluted with read locks. | Reject with HTTP 400 immediately; enforce strict key schema; strictly forbid keys on GET/HEAD/OPTIONS. |
| **Tier 2** | **Concurrency & Storage** | Identical request arrives while original is IN_PROGRESS; lock acquisition collision. | Potential race condition or double execution if non-atomic. | PostgreSQL composite unique index on `(tenant_id, principal_id, operation_id, idempotency_key)`; return HTTP 409 with `Retry-After`. |
| **Tier 3** | **Fingerprint & Integrity** | Reusing key with modified body (amount, currency), or JSON key reordering anomaly. | Financial corruption or false 422 rejection. | Canonical JSON serialization (sorted keys, normalized numbers, stripped dynamic nonces/timestamps) before SHA-256 hashing. Mismatch returns HTTP 422. |
| **Tier 4** | **Execution & Dual-Write** | Worker crashes after effect commits but before idempotency record updates, or vice versa. | Desynchronized state; lost outcome snapshot. | Single-Tx pattern (IK-01): idempotency record commits in the EXACT same database transaction as the effect. |
| **Tier 5** | **Lease Expiry & Ambiguity** | Crash during external synchronous call; worker partition. | Operation outcome unknown (ambiguous timeout). | State transitions to `UNKNOWN`. Fencing tokens and derived downstream keys (`IK-09`) allow recovery sweep to query provider before re-executing. Never release to "new" automatically. |
| **Tier 6** | **Security & Access Control** | User B attempts to replay User A's response, or revoked user replays cached invoice. | Information disclosure; unauthorized data access. | Four-dimensional scope isolation (IK-03); mandatory re-authorization of caller token on replay (IK-07). |

---

## 2. Exhaustive Edge Cases Analysis (E0.1 – E11.3)

### E0.1: Key Treated as Lock Only (Missing Outcome Record)
* **Scenario**: Developer implements idempotency using a Redis mutex. When request completes, lock is deleted.
* **Failure**: Client network dropped right before receiving response. Client retries with same key; sees no lock and re-executes.
* **Mitigation**: Keys are never locks; they are durable outcome records storing HTTP status, headers, and body snapshots.

### E2.1: Scope by Tenant Only (Cross-User Collision)
* **Scenario**: Two users in the same company independently submit an order with a simple key (e.g. `order-1`).
* **Failure**: User B receives User A's order confirmation, credit card receipt, and shipping address.
* **Mitigation**: Enforce 4D Scope: `(tenant_id, principal_id, operation_id, idempotency_key)`. User B's scope is distinct.

### E2.4a: Raw-Bytes Hashing (False 422 Conflicts)
* **Scenario**: Mobile client sends `{"amount": 100, "currency": "USD"}`. On retry, client SDK serializes as `{"currency": "USD", "amount": 100}`.
* **Failure**: Byte comparison detects difference; returns HTTP 422 Unprocessable Entity; client stuck.
* **Mitigation**: Canonical JSON normalization: parse JSON into AST, sort dictionary keys alphabetically, normalize numerical representations, hash canonical bytes.

### E2.4c: Dynamic Client Timestamps in Body
* **Scenario**: Client payload includes `"clientTimestamp": "2026-11-09T08:24:00Z"`. On retry 2 seconds later, timestamp changes.
* **Failure**: Fingerprint diverges on every retry, producing false 422 errors.
* **Mitigation**: Maintain operation-specific exclusion lists that strip non-semantic fields (`clientTimestamp`, `nonce`, `requestId`) prior to hashing.

### E4.1: Redis Lock TTL Expiry During Slow Execution
* **Scenario**: Payment gateway takes 35 seconds to respond. Redis lock had 30s TTL.
* **Failure**: Lock expires at $T=30$. Retry arrives at $T=31$ and starts a second payment authorization.
* **Mitigation**: Zero Redis locking. PostgreSQL transaction holds the row lock until commit, or explicit lease renewal is used.

### E5.1: Business Rejection Not Stored
* **Scenario**: User attempts payment with insufficient funds. Server returns HTTP 402, but does not record it under the idempotency key.
* **Failure**: User transfers money into account, and client retries with the same key. The second call succeeds.
* **Impact**: Violates the fundamental definition of idempotency (same intent must produce same outcome).
* **Mitigation**: All 4xx business rejections (`REJECTED_FINAL`) are durably stored and replayed with HTTP 402 on subsequent retries.

### E5.2: Timeout Treated as "Not Executed"
* **Scenario**: Network timeout talking to payment gateway. Orchestrator deletes idempotency key thinking it failed safely.
* **Failure**: Downstream actually charged the card. Client retries with same key; orchestrator charges the card a second time.
* **Mitigation**: Ambiguous timeouts transition to `UNKNOWN`, never deleted. Recovery queries provider using derived key (IK-09).

### E10.1: Revoked Principal Replays Stored Response
* **Scenario**: Employee generates an idempotency key to view a confidential financial report. Employee permissions are revoked 10 minutes later.
* **Failure**: Ex-employee replays request with same idempotency key; server blindly returns cached 200 OK snapshot with sensitive data.
* **Mitigation**: Mandatory Re-Authorization (IK-07): before returning replayed snapshot, evaluate active caller authorization. If revoked, return HTTP 403 Forbidden.

---

## 3. Chaos Scenarios & Automated Recovery Matrix

```
+-------------------------------------------------------------------------------+
| Scenario 1: Split-Brain In-Flight Race                                        |
| Injection: 50 concurrent goroutines submit identical (scope, key) requests.    |
| Assertion: Exactly 1 executes; 49 receive 409 Retry-After OR 200 Replayed.    |
| Zero duplicate records created in PostgreSQL.                                 |
+-------------------------------------------------------------------------------+
| Scenario 2: Canonical Payload Tamper Attempt                                  |
| Injection: Request 1 sends amount 100.00; Request 2 sends amount 200.00.       |
| Assertion: Request 2 immediately rejected with HTTP 422 (FINGERPRINT_MISMATCH).|
| Target entity remains at original state (100.00).                             |
+-------------------------------------------------------------------------------+
| Scenario 3: Database Rollback (Zero Orphaned Records)                         |
| Injection: Domain logic throws error before commit.                          |
| Assertion: PostgreSQL rolls back transaction. No idempotency record exists.   |
| Subsequent retry is permitted to execute fresh.                              |
+-------------------------------------------------------------------------------+
| Scenario 4: Replay Authorization Revocation                                   |
| Injection: Valid key replayed with revoked/invalid Bearer token.             |
| Assertion: Server returns HTTP 401/403. Snapshot is NOT leaked.               |
+-------------------------------------------------------------------------------+
```


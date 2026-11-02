# Failure Cases, Chaos Scenarios & Defensive Mitigations

This document details the failure modes, edge-case chaos behaviors, real-world blast radii, and architectural safety mitigations enforced by the `transaction-state-machine` engine.

---

## 1. Classification of Failure Modes

| Category | Typical Cause | Blast Radius | Defense Mechanism | Resulting State |
|---|---|---|---|---|
| **Concurrency Split-Brain** | Slow worker GC pause, network partition | Double debit, duplicate side-effect | Monotonic fencing token (`>=`) + CAS version | Stale worker aborted (`409 Conflict`) |
| **Downstream Outage** | Third-party payment gateway 503/timeout | Worker thread pool starvation, retry storm | Circuit breaker fast-fail + exponential jitter | `COMPENSATING` or `STUCK` |
| **Downstream Rate Limit** | HTTP 429 Too Many Requests | IP ban, connection quota exhausted | Parse `Retry-After`, suspend worker lease | `THROTTLED_WAIT` |
| **Network Partition / Drop** | TCP connection reset, HTTP 504 Gateway Timeout | Incomplete state, unknown outcome | Mark indeterminate, probe downstream status | `RECONCILING` |
| **Terminal State Mutate** | Late webhook arrival after user cancellation | Reviving dead orders, shipping free items | Terminal state lockout predicate | Unconditional rejection (`409 Conflict`) |
| **Cross-Tenant Breach** | Buggy SQL missing tenant filter | Confidentiality breach, data clobber | Compound PK/UK `(tenant_id, id)` + RLS | Isolated error (`404 / 409`) |
| **Permanent Rollback Block** | Downstream compensation service unavailable | Unresolved financial ledger, memory leak | Max retry budget trip to triage queue | `STUCK` (Awaiting Operator) |

---

## 2. Deep-Dive Failure Scenarios & Mitigations

### Failure Scenario 1: The Zombie Worker (Network Partition & Lease Expiry)
* **Real-World Problem**: An engine node executing a $10,000 corporate payroll batch suffers a 35-second stop-the-world garbage collection pause. The cluster assumes the node dead after its 10-second lease expires and reassigns the transaction to Node B. Node B successfully debits the account and completes the step. Node A then resumes execution, completely unaware that time passed, and prepares to flush its stale in-memory state.
* **Blast Radius Without Defense**: Double-execution of payroll, duplicate credit transactions, corrupted version sequence.
* **Architectural Defense**:
  1. Node B claimed the task with incremented monotonic fencing token (Token = 42).
  2. Node A attempts to commit using its original token (Token = 41).
  3. `TransactionStateMachineService` checks: `input.FencingToken (41) < currentRecord.FencingToken (42)`.
  4. The engine halts Node A with `409 Conflict`, rolling back Node A's transaction before any database write occurs.

---

### Failure Scenario 2: Indeterminate Outcome on Payment Gateway Timeout
* **Real-World Problem**: The engine dispatches an HTTP request to Stripe/Adyen to capture $500. After 30 seconds, an intermediate cloud proxy drops the connection and returns `HTTP 504 Gateway Timeout`. The engine has no idea whether the bank charged the customer or failed before processing.
* **Blast Radius Without Defense**:
  - If the engine immediately retries: The bank charges the card twice ($1,000 total).
  - If the engine immediately compensates/refunds: The customer is refunded $500 while their bank was never charged, losing money for the business.
* **Architectural Defense**:
  1. The engine refuses blind retries or premature rollbacks.
  2. The state transitions to `RECONCILING` via `INDETERMINATE_ERROR`.
  3. A dedicated reconciliation probe worker uses the stable `X-Operation-ID` (SHA-256 hash of `tenant + saga + step + attempt`) to query the partner's status endpoint.
  4. Only when the partner definitively reports `FOUND_SUCCESS` does the engine advance to `RUNNING`. If `NOT_FOUND`, it safely retries.

---

### Failure Scenario 3: The Late Webhook Zombie Resurrection
* **Real-World Problem**: A shopper cancels an e-commerce order on their mobile phone. The cancellation succeeds, transitioning the saga to terminal state `CANCELLED`. Ten minutes later, a delayed asynchronous webhook from a legacy banking provider arrives with payload `{"status": "APPROVED"}`.
* **Blast Radius Without Defense**: The webhook handler updates the order state from `CANCELLED` to `RUNNING` or `COMPLETED`, triggering warehouse pick-and-pack systems to ship items to a user who already received a refund.
* **Architectural Defense**:
  1. The domain rules engine implements the `IsTerminal(state)` invariant guard:
     ```go
     if rules.IsTerminal(current) {
         return current, errors.NewConflictError("cannot transition from terminal state")
     }
     ```
  2. The database table enforces this via relational constraint `saga_state_transitions`.
  3. The late webhook is rejected with `HTTP 409 Conflict` and logged in audit as a stale discard without touching domain state.

---

### Failure Scenario 4: Cascading Compensation Outage (Circuit Breaker to `STUCK`)
* **Real-World Problem**: A flight booking fails, triggering automatic rollback to refund the hotel reservation. However, the hotel's partner API is completely down (HTTP 503 Service Unavailable).
* **Blast Radius Without Defense**: The rollback thread retries every 500ms endlessly, consuming connection pools, exhausting background worker threads, and hiding the failed state from site reliability engineers.
* **Architectural Defense**:
  1. The engine executes compensation retries with full random exponential jitter.
  2. If retries hit the max limit (default: 10 attempts), the engine trips the compensation circuit breaker.
  3. The transaction transitions to `STUCK` via event `COMPENSATION_HALT`.
  4. Automated background threads halt immediately. An SRE on-call alert fires with the exact step and error context.
  5. Once the hotel API is restored, an SRE issues a controlled `OPERATOR_REDRIVE` event that safely resumes the rollback loop from the exact point of interruption.

---

### Failure Scenario 5: Database Failover & Connection Reset
* **Real-World Problem**: An AWS RDS Multi-AZ or cloud database primary instance crashes during the exact millisecond a worker is committing a step transition. The TCP socket drops with SQLSTATE `08006` / `57P01`.
* **Blast Radius Without Defense**: The worker assumes the transaction died and restarts from scratch, potentially duplicating side-effects.
* **Architectural Defense**:
  1. The PostgreSQL adapter intercepts connection loss.
  2. The worker reconnects to the newly promoted primary replica and executes an idempotent query by `(tenant_id, id)`.
  3. If the row already has the target version, the transition succeeded before the crash; the worker simply proceeds forward.
  4. If the row has the old version, the ACID transaction rolled back cleanly; the worker safely replays the CAS transition.

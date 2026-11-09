# Sequential Saga Execution — Failure Cases, Production Chaos & Mitigations

This document details the failure taxonomy, chaos scenarios, risk mitigations, and blast radius calculations for the **Sequential Execution Engine**, adhering to [ADR-0005](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/sequential-execution/adr.md) and [Business Logic Tree v2](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/sequential-execution/business-logic.md).

---

## 1. Production Failure Taxonomy Matrix

Each scenario is mapped across severity tiers: **LOSS** (financial/data loss), **CORRUPT** (inconsistent state), **OUTAGE** (engine/service downtime), **DEGRADE** (latency spike), **COMPLIANCE** (regulatory/audit violation), and **SECURITY** (unauthorized access or data exposure).

| Ref ID | Scenario / Failure Trigger | Severity | Primary Risk & Blast Radius | Handling & Mitigation Protocol | Invariant Enforced |
|---|---|---|---|---|---|
| **FC-01** | **Crash between DB commit and network dispatch** | **LOSS** | Database marks step `WAITING_STEP`, but downstream RPC is never called. Transaction hangs indefinitely. | Step commands are enqueued into `transactional_outbox` in the exact same database transaction as the state transition. External outbox relay daemon guarantees at-least-once delivery. | **SQ-03** |
| **FC-02** | **Post-Pivot step failure triggered compensation** | **CORRUPT** | Payment was captured (Pivot). Subsequent delivery step fails. Engine attempts to refund payment and cancel authorization, leaving external goods or funds in an orphaned state. | Domain machine verifies `pivot_reached == true`. Any reverse compensation attempt is permanently rejected. Engine switches to `ROLL_FORWARD` with durable retries or escalates to `NEEDS_ATTENTION`. | **SQ-07, SQ-08** |
| **FC-03** | **Network Timeout (HTTP 504) treated as failure** | **CORRUPT** | Downstream payment service completed the charge, but the response was lost to network drop. Engine marks step `FAILED` and starts rolling back, resulting in double-debit or ghost charges. | Invariant `Timeout ≠ Failure`. Engine transitions to `RESOLVING_UNKNOWN` and issues an active status probe keyed by the deterministic forward idempotency key before deciding to compensate or advance. | **SQ-05** |
| **FC-04** | **Zombie worker split-brain update** | **CORRUPT** | Worker 1 pauses due to a 30s GC freeze. Worker 2 steals the expired lease and dispatches Step 3. Worker 1 wakes up and attempts to complete Step 2. | Monotonic `owner_epoch`. Every database CAS update asserts `WHERE owner_epoch = :my_epoch`. Worker 1's update hits 0 rows affected, aborts, and Worker 1 yields execution. | **SQ-02, SQ-11** |
| **FC-05** | **Poison Pill Payload causes crash loop** | **OUTAGE** | A malformed step output payload causes a nil pointer panic during unmarshaling. Every worker that takes over the lease panics and crashes, knocking down the worker cluster. | Worker traps panics using deferred recovery handlers, records the panic message to `saga_step_attempt`, increments `takeover_count`, and parks the saga in `PAUSED_POISON` when `takeover_count >= 5`. | **SQ-01, SQ-14** |
| **FC-06** | **Late reply arrives for already compensated saga** | **CORRUPT** | Step 1 reply arrives 10 minutes late, after Step 2 failed and Step 1 was already reversed. Late reply attempts to advance the saga to Step 2. | Reply consumer verifies saga phase. If phase is `COMPENSATING` or `FAILED_COMPENSATED`, the reply is dropped as an out-of-order duplicate and routed to an audit DLQ topic. | **SQ-10** |
| **FC-07** | **Context Key Collision / Mutation** | **CORRUPT** | Step 2 overwrites a context dictionary entry produced by Step 1 with incompatible data, causing Step 3 to fail validation. | Immutable, append-only context journal in `saga_context_entries`. Unique constraint on `(tenant_id, saga_id, producer_step_index, entry_name)`. Direct in-place mutation is rejected at the DB constraint layer. | **SQ-13** |
| **FC-08** | **Cardholder data (PCI-DSS) or PII in central logs** | **COMPLIANCE** | Raw step input payloads containing customer credit card numbers or SSNs are logged to stdout during debugging. | Observability interceptor scrubs all log outputs, recording only opaque identifiers (`public_id`, `step_index`, `tenant_id`). Payloads are encrypted at rest with AES-GCM or offloaded to Claim-Check storage. | **SQ-13, SQ-14** |
| **FC-09** | **Idempotency Key Reuse across Forward and Reverse** | **LOSS** | Compensation calls use the same idempotency key as the forward call (`idemp_123`). Downstream gateway returns the cached 200 OK from the forward charge instead of issuing a refund. | Idempotency keys strictly enforce prefix namespaces: `idemp_fwd_...` vs `idemp_comp_...`. Downstream gateways treat them as distinct operations. | **SQ-04, SQ-09** |
| **FC-10** | **Step Timeout timer lost during pod eviction** | **DEGRADE** | In-memory timer drops when pod restarts. Saga remains in `WAITING_STEP` forever. | Durable timer records persisted in `saga_timers`. Periodic database sweeper scans `due_at <= NOW()` and wakes up the saga regardless of pod lifecycle events. | **SQ-06** |

---

## 2. Deep-Dive Chaos Scenarios & Formal Mitigations

### 2.1 The Post-Pivot Disaster
#### The Failure Event
Consider a high-value e-commerce checkout saga with 4 sequential steps:
1. `Step 0 [COMPENSABLE]`: Reserve Inventory
2. `Step 1 [COMPENSABLE]`: Apply Discount Voucher
3. `Step 2 [PIVOT]`: Authorize & Capture Credit Card (\$2,500)
4. `Step 3 [RETRIABLE]`: Notify Logistics & Generate Shipping Label

Step 2 (Credit Card Capture) succeeds. Step 3 (Notify Logistics) experiences a 500 Internal Server Error from the shipping carrier's API. A naive orchestration engine evaluates Step 3 as failed, flags the saga as failed, and triggers reverse compensation:
- It compensates Step 1 (re-enables voucher).
- It compensates Step 0 (releases reserved inventory back to public stock).
- It leaves Step 2 (Credit Card Capture) untouched because the shipping failure occurred downstream!

#### The Catastrophic Result
The customer's credit card was charged \$2,500, but their inventory was sold to another customer, and the order was canceled. The customer files a chargeback, resulting in dispute fees, financial loss, and severe brand damage.

#### Formal Mitigation
1. **Definition Validation Rule (`ValidateStepOrdering`)**: Workflow definitions strictly prohibit placing any compensable step after a pivot step.
2. **Runtime Pivot Latch (`pivot_reached`)**: When Step 2 transitions to `SUCCEEDED`, the database column `pivot_reached` is latched to `TRUE`.
3. **Machine Transition Rule (`EvaluateStepFailure`)**:
   ```go
   if pivotReached {
       return SequentialTransitionDecision{
           NextPhase: contracts.PhaseRollForward,
           NextSagaState: "ROLLING_FORWARD",
           Commands: []SequentialCommand{{Type: CommandScheduleRetry, StepIndex: currentIndex}},
       }, nil
   }
   ```
4. **Enforcement**: Reverse compensation is physically impossible once `pivot_reached = TRUE`. The engine forces infinite backoff retries on Step 3 or parks the transaction in `NEEDS_ATTENTION` for human logistics dispatch.

---

### 2.2 The Dual-Write Worker Crash
#### The Failure Event
In naive microservice architectures, an orchestrator updates its database:
```go
// ANTI-PATTERN:
saga.CurrentStepIndex = 1
db.Save(saga)
kafkaProducer.Send("step-commands", step1Payload)
```
If the orchestrator pod suffers an Out-Of-Memory (OOM) kill or AWS EC2 hardware failure immediately after `db.Save(saga)`, the database reflects that Step 1 is in-flight. However, the Kafka command was never dispatched. Because the orchestrator relies on the worker's asynchronous reply to proceed, the transaction freezes indefinitely.

#### Formal Mitigation
1. **Transactional Outbox Table**: Command dispatch is coupled with the database CAS update:
   ```sql
   BEGIN;
   UPDATE saga_instances SET current_step_index = 1, current_state = 'WAITING_STEP' WHERE id = '...';
   INSERT INTO transactional_outbox (topic, event_type, idempotency_key, payload) 
   VALUES ('step-commands', 'DISPATCH_STEP', 'idemp_fwd_saga_1_att_1', '...');
   COMMIT;
   ```
2. **Decoupled Outbox Relay**: An independent daemon tails the PostgreSQL Write-Ahead Log (WAL) or executes `SELECT ... FOR UPDATE SKIP LOCKED` on the outbox table. If the orchestrator pod crashes, the outbox row remains safely stored in PostgreSQL, and the relay publishes the message upon worker recovery.

---

### 2.3 The Ambiguous 504 Timeout Dilemma
#### The Failure Event
Step 1 dispatches a funds reservation request to a core banking service. The core banking service takes 32 seconds to process the debit due to database lock congestion. The orchestrator's HTTP client or gateway client has a 30-second timeout. At second 30, the orchestrator receives an HTTP 504 Gateway Timeout.

If the orchestrator treats HTTP 504 as `FAILED`:
- It initiates compensation for Step 0.
- Meanwhile, at second 32, the core banking service completes the funds reservation!
- The funds are locked in the bank, but the saga is marked failed. The customer's funds are permanently stranded in an unlinked escrow account.

#### Formal Mitigation
1. **Invariant SQ-05 Enforcement**: Network timeouts and socket resets transition the step to `UNKNOWN`, never `FAILED`.
2. **Active Status Probing**: The orchestrator enters `RESOLVING_UNKNOWN` and polls the downstream banking service using the deterministic idempotency key (`idemp_fwd_...`).
3. **Deterministic Classification**:
   - If the downstream service responds `TRANSACTION_NOT_FOUND`, it confirms that the transaction was never committed. The orchestrator can now safely compensate.
   - If the downstream service responds `COMMITTED`, the orchestrator marks the step as `SUCCEEDED` and advances to the next step.
   - If the downstream service continues to time out past maximum probing thresholds, the saga enters `NEEDS_ATTENTION` to prevent unassisted rollback.

---

### 2.4 Zombie Worker Split-Brain
#### The Failure Event
1. Worker Pod A is executing Saga 101.
2. Worker Pod A experiences a 45-second Stop-The-World Go Garbage Collection pause or hypervisor CPU freeze.
3. The lease on Saga 101 (`lease_until = 12:00:00`) expires.
4. The Crash Recovery Sweeper detects the expired lease at 12:00:01 and reassigns Saga 101 to Worker Pod B.
5. Worker Pod B increments the step index to Step 2 and dispatches commands.
6. Worker Pod A's GC pause ends at 12:00:30. Worker Pod A is unaware that 30 seconds have elapsed, believes it is still the owner, and attempts to write completion outputs for Step 1.

#### Formal Mitigation
1. **Monotonic Fencing Tokens (`owner_epoch`)**:
   - Worker Pod A acquired the saga with `owner_epoch = 10`.
   - When Worker Pod B stole the lease, it executed:
     ```sql
     UPDATE saga_instances 
     SET lease_owner = 'worker-B', 
         owner_epoch = owner_epoch + 1, -- now 11
         lease_until = NOW() + INTERVAL '30 seconds'
     WHERE id = 'saga-101' AND owner_epoch = 10;
     ```
2. **Fenced CAS Updates**:
   - Worker Pod A attempts to save its result:
     ```sql
     UPDATE saga_instances 
     SET current_state = 'WAITING_STEP', version = 2 
     WHERE id = 'saga-101' AND owner_epoch = 10;
     ```
   - Because `owner_epoch` in the database is now 11, the query updates 0 rows.
   - Worker Pod A detects the zero-row return, halts execution immediately, and drops in-memory state.

---

### 2.5 Poison Pill Step Output & Cluster Crash Loops
#### The Failure Event
A downstream service returns a valid HTTP 200 OK, but the JSON payload contains a field with an unexpected type (e.g. an array where an object was expected) that triggers an unhandled nil pointer dereference or panic inside the orchestrator's output parser.
1. Worker Pod 1 consumes the message, panics, and crashes.
2. Kubernetes restarts Worker Pod 1.
3. Crash recovery detects the incomplete saga and reassigns it to Worker Pod 2.
4. Worker Pod 2 reads the identical payload, panics, and crashes.
5. Within 60 seconds, every worker in the Kubernetes cluster is caught in a crash loop, resulting in a complete platform outage (**OUTAGE**).

#### Formal Mitigation
1. **Deferred Recovery Panic Wrapper**:
   ```go
   defer func() {
       if r := recover(); r != nil {
           metrics.IncrementPanicCounter()
           s.repo.RecordTakeoverFailure(ctx, tenantID, sagaID, fmt.Sprintf("%v", r))
       }
   }()
   ```
2. **Atomic Takeover & Poison Quarantine**:
   - Each recovery takeover increments `takeover_count` in PostgreSQL.
   - If `takeover_count > max_takeovers` (default: 5), the saga transitions automatically to `PAUSED_POISON`.
   - In `PAUSED_POISON`, the saga is permanently ignored by automated poller loops until an engineer patches the schema or manually resolves the payload via the operator console.

---

## 3. Automated Chaos Verification Test Scenarios

The following automated test scenarios are required in the continuous integration pipeline:

```
+---------------------------------------------------------------------------------------+
| TEST 1: Dual-Write Outbox Atomicity                                                   |
| Trigger: Inject SIGKILL to orchestrator process during StepCompletion execution.     |
| Assert: Upon boot, no step commands are lost; outbox relay picks up pending records.  |
+---------------------------------------------------------------------------------------+
| TEST 2: Post-Pivot Failure Roll-Forward                                               |
| Trigger: Force Step 3 (Post-Pivot) to return terminal 500 error.                      |
| Assert: Machine refuses transition to COMPENSATING; saga enters ROLLING_FORWARD.      |
+---------------------------------------------------------------------------------------+
| TEST 3: Ambiguous Timeout Probing                                                     |
| Trigger: Mock downstream service to drop connection on Step 1 dispatch.              |
| Assert: Saga enters RESOLVING_UNKNOWN; probes downstream status; completes safely.    |
+---------------------------------------------------------------------------------------+
| TEST 4: Zombie Worker Fencing                                                         |
| Trigger: Simulate 60s freeze on Worker A; steal lease with Worker B; resume Worker A. |
| Assert: Worker A CAS fails with 0 rows affected; Worker A yields without corruption.   |
+---------------------------------------------------------------------------------------+
| TEST 5: Poison Pill Isolation                                                         |
| Trigger: Inject malformed corrupt JSON into step output.                              |
| Assert: Engine does not crash loop; saga parks in PAUSED_POISON after 5 attempts.      |
+---------------------------------------------------------------------------------------+
```


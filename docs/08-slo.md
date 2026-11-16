# SLOs, SLIs, Error Budgets & Verification Metrics
**System:** Distributed Transaction Engine (`distributed-transaction-engine`)  
**Repository:** [https://github.com/Chief-Strategist-J/distributed-transaction-engine](https://github.com/Chief-Strategist-J/distributed-transaction-engine)  
**Tracking Issue:** [TODO.md](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/TODO.md#L32) (Section 1: Architecture artifacts -> `docs/08-slo.md`)

---

## 1. Executive Summary & SLO Philosophy
In a distributed transaction engine orchestrating financial and business-critical operations (Saga, Outbox, Idempotency, Fencing, and Reconciliation), generic infrastructure metrics (such as CPU, memory, or raw HTTP 200 counts) are insufficient. 

This engine enforces **Transaction-Correctness First** SLOs. Our SLIs measure exact state transitions, idempotency guarantees, outbox delivery latency, and automatic crash recovery within strict bounds.

---

## 2. Core SLI / SLO Catalog

### 2.1 Correctness & Integrity SLOs (Tier 0 — Zero Tolerance)

#### SLO-1: Exactly-Once Downstream Side-Effect Guarantee
* **Description:** No downstream service shall be invoked more than once per logical step execution with divergent parameters or without a matching idempotency key.
* **SLI Formula:**
  $$\text{Deduplication Ratio} = 1 - \left( \frac{\text{Unintended Duplicate Downstream Executions}}{\text{Total Downstream Dispatches}} \right)$$
* **Objective:** **100% (Zero tolerance for duplicate side effects)**
* **Alert Trigger:** Any event (> 0) triggers immediate P1 paging alert.

#### SLO-2: Terminal State Consistency (Saga Completion Guarantee)
* **Description:** Every initiated saga must reach a valid, deterministic terminal state (`COMPLETED`, `COMPENSATED`, or `CANCELLED`). Sagas must never remain permanently stalled in intermediate states (`RUNNING`, `COMPENSATING`) without operator notification.
* **SLI Formula:**
  $$\text{Terminal Completion Ratio} = \left( \frac{\text{Sagas Reaching Final Valid State}}{\text{Total Initiated Sagas}} \right) \times 100\%$$
* **Objective:** **≥ 99.999%** over any rolling 30-day window.
* **Alert Trigger:** Any saga stuck in intermediate state longer than its defined timeout (default: 15 minutes) transitions to `STUCK` and fires an alert.

#### SLO-3: Cryptographic Audit Trail Immutability
* **Description:** Every state transition recorded in the audit trail must verify against the prior row's HMAC-SHA256 checksum without hash chain breakage.
* **Objective:** **100% valid verification** across 100% of audit ledger segments.

---

### 2.2 Latency & Performance SLOs (Tier 1)

#### SLO-4: Step Dispatch Latency
* **Description:** The time elapsed from when a step becomes ready (previous step committed or timer fired) to when the HTTP/gRPC dispatch request is emitted to the downstream participant.
* **SLI Formula:** Wall-clock duration measured by OpenTelemetry span `saga.step.dispatch`.
* **Objective:**
  * **p90 < 25 ms**
  * **p99 < 50 ms**
* **Error Budget:** 1% of dispatches exceeding 50 ms in a rolling 1-hour window.

#### SLO-5: End-to-End Saga Completion Latency
* **Description:** Duration from saga submission (`POST /v1/sagas`) to terminal state confirmation for standard 3-step sequential sagas.
* **Objective:**
  * **p95 < 1,500 ms**
  * **p99 < 5,000 ms**

#### SLO-6: Transaction Query API Latency
* **Description:** Response time for state queries (`GET /v1/sagas/{id}`) across tables containing up to 50,000,000 saga history rows.
* **Objective:**
  * **p99 < 100 ms** with index plan verified via `EXPLAIN ANALYZE`.

---

### 2.3 Event Fabric & Outbox Delivery SLOs (Tier 1)

#### SLO-7: Transactional Outbox Relay Lag
* **Description:** Time elapsed from the insertion of a message into the PostgreSQL `outbox_events` table to broker ACK (Apache Kafka / Redpanda).
* **SLI Formula:**
  $$\text{Lag} = t_{\text{broker\_ack}} - t_{\text{outbox\_created}}$$
* **Objective:**
  * **p95 < 200 ms**
  * **p99 < 500 ms** under sustained load of 5,000 events/second.

#### SLO-8: Ingress Request Idempotency Hit Latency
* **Description:** Latency for returning a cached response when a client retries with an existing `Idempotency-Key`.
* **Objective:**
  * **p99 < 15 ms** (served directly from index / memory tier).

---

### 2.4 Availability & Disaster Recovery SLOs (Tier 2)

#### SLO-9: Engine Ingress Availability
* **Description:** Availability of the saga initiation and status query endpoints.
* **SLI Formula:**
  $$\text{Availability} = \left( \frac{\text{HTTP 2xx, 4xx (Valid Client Requests)}}{\text{Total Ingress Requests}} \right) \times 100\%$$
* **Objective:** **≥ 99.95%** over any rolling 30-day window.

#### SLO-10: Crash Recovery Time Objective (RTO)
* **Description:** Elapsed time from an abrupt process termination (`kill -9`) of a coordinator replica to the acquisition of the monotonic fencing lease by a surviving replica and resumption of execution.
* **Objective:** **RTO < 60 seconds** (automatic lease failover with zero operator intervention).

#### SLO-11: Recovery Point Objective (RPO)
* **Description:** Maximum acceptable duration of state loss upon failure.
* **Objective:** **RPO = 0 seconds** (all step intents are strictly journaled to PostgreSQL before dispatch).

---

## 3. Multi-Window Multi-Burn-Rate Alerting Policy
In accordance with Google SRE best practices, alerts are evaluated using two rolling windows to prevent fatigue while catching fast burns immediately:

| Severity | Burn Rate | % Budget Consumed | 1-Hour Window | 6-Hour Window | Response Action |
|---|---|---|---|---|---|
| **P1 — Critical** | **14.4x** | 2% in 1 hour | Alert if budget burn > 2% | Alert if budget burn > 2% | PagerDuty on-call wake-up |
| **P2 — Warning** | **6.0x** | 5% in 6 hours | - | Alert if budget burn > 5% | Ticket created, next-business-day triage |
| **P3 — Low** | **1.0x** | 10% in 3 days | - | Alert if 3-day burn > 10% | Slack notification to architecture channel |

---

## 4. Expected Targets vs. Actual Results

As required by [TODO.md](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/TODO.md), every acceptance criterion and benchmark must record expected targets alongside measured empirical actual results.

| Metric / Drill | Acceptance Target | Actual Result | Verification Harness / Link |
|---|---|---|---|
| **Saga Crash Recovery (WP-2 / WP-10)** | Sagas resume after `kill -9` without duplicate side effects; RTO < 60s | *Pending Execution* | [tests/chaos/saga_crash_recovery_test.go](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/tests/chaos/saga_crash_recovery_test.go) |
| **Outbox Relay Lag (WP-5)** | p99 < 500 ms @ 5k msgs/s | *Pending Execution* | `tests/load/k6/` |
| **Idempotency Deduplication (WP-7)** | 100 concurrent requests with identical key -> 1 effect | *Pending Execution* | [docs/features/idempotency-keys/business-logic.md](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/idempotency-keys/business-logic.md) |
| **Contention Curve (WP-9)** | CAS version conflict resolution with bounded jitter | *Pending Execution* | `docs/09-benchmarks.md` |
| **Query API Latency (WP-15)** | p99 < 100 ms at 50M rows | *Pending Execution* | [docs/features/transaction-query-api/business-logic.md](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/transaction-query-api/business-logic.md) |
| **Audit Trail Chain Check (WP-14)** | 100% hash consistency across all sealed segments | *Verified in Spec* | [docs/features/audit-trail/business-logic.md](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/audit-trail/business-logic.md) |

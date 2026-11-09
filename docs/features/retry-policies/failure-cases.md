# Retry Policies — Failure Modes, Edge Cases & Mitigation Matrix

This document establishes the comprehensive failure taxonomy, edge-case analysis (E2.1 – E10.1), and chaos engineering mitigation strategies for the Deterministic Retry Policies feature.

---

## 1. Failure Taxonomy Across 6 Tiers

| Tier | Failure Class | Root Cause | Impact | Mitigation Strategy |
|---|---|---|---|---|
| **Tier 1** | **Classification & Signals** | Gateway 504 or proxy 503 treated as non-executed; 401 treated as terminal business rejection. | Double charge or unnecessary saga compensation. | Authoritative mapping matrix (§2); check `NOT_SENT` provability; refresh tokens once on 401; query status by key before retrying non-idempotent steps (RP-03). |
| **Tier 2** | **Backoff & Timing** | Zero-delay jitter floor; missing exponent cap causing integer overflow; naive linear spacing. | Downstream server hammering; negative sleep durations; synchronized thundering herds. | Enforce minimum floor ($\ge \frac{\text{Base}}{2}$); clamp exponent attempt at $\le 30$; mandate Full/Equal Jitter on all algorithms. |
| **Tier 3** | **Budget & Deadlines** | Short retry budget burning out during a transient deploy; ignoring downstream `Retry-After`. | Valid sagas aborted prematurely; holds expiring mid-retry. | Dual budget (both max attempts AND max elapsed time); enforce effective deadline = $\min(\text{step}, \text{saga}, \text{hold\_expiry}, \text{idempotency\_window})$ (RP-08). |
| **Tier 4** | **Amplification & Breaker** | Unbounded client/mesh retries; limiter fail-fast; uncoordinated retry bursts on recovery. | Downstream cascade failure; healthy requests dropped; recovery thundering herd. | Single retry layer (RP-09); rate limiter denials postpone (`RETRY_SCHEDULED`) without failing (RP-05); circuit breaker freezes budgets (RP-06); ramped wave release on recovery. |
| **Tier 5** | **Storage & Durability** | In-memory retry scheduling; attempt counters incremented after network call; late replies ignored. | Lost retries on pod restart; duplicate dispatch on worker crash; duplicate execution. | Durable database timers; increment attempt counter before network dispatch (RP-07); state re-check upon timer fire. |
| **Tier 6** | **Position & Post-Pivot** | Attempting backward compensation on an irreversible step after retry budget exhaustion. | Severe financial desynchronization; corrupt inventory ledgers. | Strict post-pivot rule (RP-10): post-pivot steps retry indefinitely within business deadline or escalate to human intervention (`NEEDS_ATTENTION`), never compensated. |

---

## 2. Exhaustive Edge Cases Analysis (E2.1 – E10.1)

### E2.1: Gateway 504 Treated as Transient Blind Retry (Double Charge)
* **Scenario**: Payment gateway takes 15 seconds to charge customer card. Reverse proxy returns HTTP 504 Gateway Timeout.
* **Failure**: Orchestrator blindly retries step. Payment gateway executes a second charge.
* **Mitigation**: Invariant RP-02: Blind retries forbidden on non-idempotent steps. Invariant RP-03: Ambiguous timeouts must resolve status via query before any re-execution or failure decision.

### E2.2: ALREADY_EXISTS on Retry Treated as Failure
* **Scenario**: Attempt 1 succeeds in DB, but TCP connection resets before ack is received. Attempt 2 receives `HTTP 409 / gRPC ALREADY_EXISTS`.
* **Failure**: Engine treats 409 as a conflict error and triggers reverse compensation around an already-succeeded step.
* **Mitigation**: Classification Rule §2.3: `ALREADY_EXISTS` on attempt > 1 is treated as `ALREADY_DONE` and converted to `SUCCESS`.

### E4.1: Short Attempt Budget in Transient Blips
* **Scenario**: Step configured with 5 attempts, fixed 200ms delay. Microservice does a 45-second rolling pod restart.
* **Failure**: All 5 attempts exhaust within 1.5 seconds. Saga fails and compensates while dependency is healthy 30 seconds later.
* **Mitigation**: Dual budget: max attempts (e.g. 10) AND max elapsed time (e.g. 5 minutes). Jittered backoff expands delays across the expected outage window.

### E6.1: Dependency Outage Burns All Saga Budgets
* **Scenario**: Payment partner has a 15-minute scheduled maintenance outage. 5,000 active sagas retry repeatedly.
* **Failure**: All 5,000 sagas exhaust their retry limits, triggering mass compensation and manual alerts.
* **Mitigation**: Circuit Breaker Interplay (RP-06): When breaker opens, dependency clock and attempt budgets are completely FROZEN. Zero budget consumed during dependency-wide outages.

### E7.1: Full Jitter Without Floor (Downstream Hammering)
* **Scenario**: Developer uses `Random(0, Base * 2^Attempt)`. The random generator yields `0` on multiple iterations.
* **Failure**: Rapid-fire tight loop hammers the recovering downstream service with 0ms delays.
* **Mitigation**: Backoff Rule §7: Enforce non-zero floor: $\text{Sleep} = \text{Random}(\text{Floor}, \text{CalculatedCap})$ where $\text{Floor} \ge \frac{\text{Base}}{2}$.

### E7.4: Integer Exponent Overflow at High Attempt Counts
* **Scenario**: Worker retries indefinitely for post-pivot step. At attempt 64, `1 << 64` overflows 64-bit integer into negative values or 0.
* **Failure**: Backoff suddenly becomes negative or resets to zero, causing infinite tight-loop execution.
* **Mitigation**: Clamp attempt counter before computing shift: $\min(\text{Attempt}, 30)$.

### E10.1: Thundering Herd on Breaker Recovery
* **Scenario**: Circuit breaker transitions from OPEN to CLOSED with 10,000 sagas queued in `RETRY_SCHEDULED`.
* **Failure**: All 10,000 sagas immediately dispatch at once, instantly crashing the newly recovered service.
* **Mitigation**: Recovery Rule §10: Postponed retries are released in rate-limited waves, re-jittered across the release window, prioritized by earliest expiring deadlines.

---

## 3. Chaos Engineering Scenarios & Automated Recovery Matrix

```
+-------------------------------------------------------------------------------+
| Scenario 1: Rolling Downstream Pod Restart (Transient 503 Spike)              |
| Injection: Mock downstream service returns 503 with Retry-After: 2 for 5 sec. |
| Assertion: Sagas enter RETRY_SCHEDULED, respect 2s header, and succeed on     |
| attempt 2. Zero premature compensations triggered.                           |
+-------------------------------------------------------------------------------+
| Scenario 2: Deterministic 500 Bug Trap                                        |
| Injection: Target returns identical 500 error fingerprint on 3 attempts.      |
| Assertion: Engine terminates retry loop after attempt 3; escalates to         |
| NEEDS_ATTENTION. CPU and network bandwidth protected.                         |
+-------------------------------------------------------------------------------+
| Scenario 3: Circuit Breaker Trip & Budget Freeze                              |
| Injection: Inject 10 consecutive downstream network timeouts.                |
| Assertion: Breaker trips to OPEN. Subsequent sagas targeting dependency have  |
| budgets frozen (attempt_count unchanged). Resumes cleanly on HALF_OPEN probe.|
+-------------------------------------------------------------------------------+
| Scenario 4: Irreversible Post-Pivot Step Failure                              |
| Injection: Step 3 (Post-Pivot Capture) fails with 500 past deadline.          |
| Assertion: Backward compensation is strictly blocked. State escalates to      |
| NEEDS_ATTENTION with audit trail preserved.                                  |
+-------------------------------------------------------------------------------+
```


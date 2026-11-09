# ADR-0010: Deterministic Step Retry Policies, Multidimensional Error Classification, Jittered Backoff & Dependency Breaker Interplay

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-11-09 |
| Deciders | Lead Systems Architect & Core Architecture Guild |
| Related Docs | [Business Logic & Decision Tree v2](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/retry-policies/business-logic.md) · [Diagrams](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/retry-policies/diagrams.md) · [Failure Cases & Mitigations](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/retry-policies/failure-cases.md) |
| Target Feature | [Retry Policies Feature](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/src/features/retry-policies) |

---

## 1. Context & Problem Statement

Distributed saga orchestrators coordinate multi-step workflows across heterogeneous downstream microservices, third-party payment gateways, message brokers, and legacy external APIs. In real-world networks, transient perturbations—such as rolling deployments, TCP socket resets, ephemeral proxy timeouts (HTTP 502/504), rate-limit bursts (HTTP 429), and garbage collection pauses—are standard daily operational events.

A naive or unprincipled retry strategy introduces catastrophic operational failure modes:
1. **The Blind Retry Disaster (Duplicate Operations)**: Retrying a step blindly after an ambiguous HTTP 504 gateway timeout when the target worker is non-idempotent. The first request actually executed; the blind retry executes a second time, charging the customer's credit card twice or double-shipping physical goods (E2.1, E3.1).
2. **The Thundering Herd & Retry Storm**: When an upstream service stumbles, retrying immediately without jitter or with uncoordinated linear backoff synchronizes thousands of concurrent sagas, converting a minor 500ms blip into a catastrophic cascade outage that knocks out the downstream service for hours (E7.2, E10.1).
3. **The Premature Compensation Trap**: Treating transient network drops as permanent failures. Retrying 5 times with 200ms fixed delays burns through the entire step retry budget in 3 seconds during a 2-minute maintenance window, prematurely triggering reverse compensation, cancelling valid customer orders, and incurring refund fees (E4.1).
4. **The Infinite Poison Pill Loop**: Retrying deterministic business rejections (HTTP 400 Bad Request, HTTP 422 Unprocessable Entity, invalid card numbers) or deterministic bugs (500s that crash on identical AST inputs), wasting server CPU, DB storage, and worker threads indefinitely (E2.4a).
5. **The Post-Pivot Compensation Disaster**: Attempting to compensate an irreversible step (e.g. captured credit card settlement) when retries fail, violating core ACID and financial integrity invariants (RP-10).

---

## 2. Decision Drivers & Architecture Invariants (RP-01 – RP-10)

The Retry Policies engine is strictly governed by ten core architectural invariants:

* **RP-01 (Frozen Intent & Idempotency Key Immutability)**: Every retry of a step MUST reuse the exact same idempotency key and the exact same frozen request payload. Modifying inputs between attempts under the same key is strictly prohibited.
* **RP-02 (Guarded Blind Retry)**: A blind retry is permitted ONLY if either:
  1. The request was provably `NOT_SENT` (e.g., local DNS failure, client-side rate limit rejection, connection refused before bytes were written).
  2. The target worker is explicitly registered as `idempotent_by_key = true`.
  Blind retries of non-idempotent workers are strictly forbidden.
* **RP-03 (Authoritative Ambiguity Resolution)**: When an attempt ends in an ambiguous state (HTTP 500, 502, 504, TCP read timeout), the engine MUST resolve whether the mutation occurred (via status query or same-key query) before any failure or compensation decision is made.
* **RP-04 (Exhaustion Invariant)**: Budget exhaustion never implies "not executed". If the final attempt was ambiguous, the saga MUST NOT transition to `FAILED` or compensate; it must transition to `UNKNOWN` or `NEEDS_ATTENTION`.
* **RP-05 (Downstream Protection Postponement)**: Client-side rate-limit throttling or circuit breaker trip NEVER converts into a step failure. It postpones the attempt (`RETRY_SCHEDULED`) without consuming the step attempt budget.
* **RP-06 (Dependency Outage Budget Freezing)**: When a downstream dependency experiences an outage (circuit breaker open), all scheduled retries targeting that dependency have their elapsed time and attempt budgets frozen.
* **RP-07 (Durable Step Timers & Atomic Attempt Recording)**: Retry timers MUST be persisted to durable database storage before dispatch. Attempt counters are incremented and committed before the network call occurs, surviving worker crashes.
* **RP-08 (Effective Deadline Cap)**: No retry may be dispatched or scheduled past the step's effective deadline, which is the minimum of step deadline, saga deadline, hold/reservation expiry, and provider idempotency window.
* **RP-09 (Single Retry Layer)**: Exactly one retry layer exists per step. Underlying HTTP/gRPC clients and service meshes (e.g. Istio/Envoy) must have retries disabled for state-changing POST/PUT mutations.
* **RP-10 (Post-Pivot Irreversibility)**: Post-pivot steps are strictly non-compensable. They retry indefinitely within business validity or escalate to human intervention (`NEEDS_ATTENTION`).

---

## 3. Multidimensional Error Classification Matrix

The engine maps all network, protocol, and contract signals into seven authoritative classification tiers:

| Tier / Class | Conditions | Capability / Position | Action |
|---|---|---|---|
| **NOT_SENT** | DNS failure, Connection refused, TLS handshake error, Client limiter drop | Any step | Safe to retry immediately with jittered backoff. |
| **TRANSIENT_NOT_EXECUTED** | HTTP 429, HTTP 503 with `Retry-After`, gRPC `RESOURCE_EXHAUSTED` before execution | Any step | Schedule retry; strictly honor `Retry-After` header. |
| **AMBIGUOUS** | HTTP 500/502/504, gRPC `DEADLINE_EXCEEDED`, TCP read timeout after bytes sent | `idempotent_by_key = true` | Retry with identical idempotency key. |
| **AMBIGUOUS** | HTTP 500/502/504, gRPC `DEADLINE_EXCEEDED`, TCP read timeout after bytes sent | `idempotent = false`, `status_query = true` | Execute status query by key. If executed -> SUCCESS; if not found -> retry. |
| **AMBIGUOUS** | HTTP 500/502/504, gRPC `DEADLINE_EXCEEDED`, TCP read timeout after bytes sent | `idempotent = false`, `status_query = false` | Transition to `UNKNOWN` -> `NEEDS_ATTENTION`. Zero blind retry, zero compensation. |
| **REJECTED** | HTTP 400, 422, gRPC `INVALID_ARGUMENT`, business conflict | Pre-pivot step | Zero retry -> Halt step and trigger reverse compensation. |
| **REJECTED** | HTTP 400, 422, gRPC `INVALID_ARGUMENT`, business conflict | Post-pivot step | Escalate to `NEEDS_ATTENTION`. Backward compensation is prohibited. |
| **CREDENTIAL** | HTTP 401 `UNAUTHENTICATED` | Any step | Refresh credential token once -> retry. If fails again -> `REJECTED`. |
| **CAUSAL_WAIT** | HTTP 404 on resource created by upstream step (read replica lag) | Any step | Bounded delayed retries within causal budget (e.g. 2 minutes) -> then `REJECTED`. |
| **DETERMINISTIC_500** | Identical error fingerprint (class + message) on 3 consecutive attempts | Any step | Stop retrying -> Escalate to `NEEDS_ATTENTION` (bug, not transience). |

---

## 4. Backoff Mathematics & Jitter Algorithms

To break synchronization across competing workers, backoff intervals are calculated using full jitter with an enforced non-zero floor:

### 1. Full Jitter (Default)
$$\text{Sleep} = \text{Random}(\text{Floor}, \min(\text{Cap}, \text{Base} \cdot 2^{\min(\text{Attempt}, 30)}))$$
Enforcing $\text{Floor} \ge \frac{\text{Base}}{2}$ ensures the engine never hammers downstream services with zero-delay loops (E7.1). Exponent overflow is mitigated by capping the attempt count at 30 before shifting.

### 2. Equal Jitter
$$\text{Half} = \frac{\min(\text{Cap}, \text{Base} \cdot 2^{\min(\text{Attempt}, 30)})}{2}$$
$$\text{Sleep} = \text{Half} + \text{Random}(0, \text{Half})$$

### 3. Header-Aware Spacing
When downstream returns `Retry-After` (HTTP 429/503 in either seconds or RFC 7231 HTTP-date):
$$\text{EffectiveDelay} = \max(\text{ComputedSleep}, \text{RetryAfterSeconds})$$
If $\text{Now} + \text{EffectiveDelay} > \text{EffectiveDeadline}$, the attempt is cancelled immediately rather than scheduled into useless execution.

---

## 5. Consequences & Operational Trade-offs

### Positive Consequences
* **Zero Double Operations**: Eliminates blind retries of non-idempotent operations after ambiguous timeouts.
* **Resilience Under Downstream Outages**: Dependency token buckets and circuit breaker integration prevent engine retries from overwhelming degraded dependencies.
* **Elimination of Premature Compensation**: Extended elapsed-time budgets and transient classification prevent unnecessary reverse compensation.

### Negative Consequences & Mitigations
* **Increased Step Latency During Degradation**: Sagas wait durably rather than failing immediately.
  * *Mitigation*: Effective deadlines cap total wait time, and circuit breaker metrics surface degraded dependencies to operators.
* **Durable Storage Writes**: Recording attempt outcomes and timers in PostgreSQL requires row updates per attempt.
  * *Mitigation*: Batching timer updates and pruning completed attempts via background sweepers.


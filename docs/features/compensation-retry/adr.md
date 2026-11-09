# ADR-0012: Time-Horizon Compensation Retries, Ambiguity Verification, and Non-Terminal Escalation Ladders

## Status
ACCEPTED

## Context
When a distributed transaction fails after its pivot step, or fails before pivot and triggers rollback, the engine invokes reverse compensation actions (`compensation-engine`). Unlike forward business steps that can simply be aborted on failure, compensation steps represent real-world customer effects (e.g., credit card refunds, flight seat cancellations, inventory releases, tax reversals).

Existing naive compensation retry models suffer from five fatal flaws:
1. **Blind Re-send on Network Timeouts**: A network timeout or HTTP 504 on a refund request is ambiguous—the payment gateway may have processed the charge reversal and dropped only the acknowledgment. Blindly retrying causes catastrophic double refunds.
2. **Fixed Attempt Count Burnout**: Setting a hard cap of 10 retries with sub-second backoff burns the entire retry budget during an ordinary 15-minute downstream provider maintenance window, stranding transactions in a terminal dead-end while the customer remains unrefunded.
3. **Merging Distinct Uncompensable Failures**: Conflating schema/contract defects (engine bugs), expired credentials, and business-ineligible states (e.g., flight already departed) into a single "uncompensable" flag produces either thousands of redundant operator tickets or silent customer loss.
4. **Race Conditions in Step State Verification**: When an asynchronous compensation acknowledgment arrives late, a scheduled redrive timer that checks only the parent saga state (which remains `COMPENSATING` while other steps run) will blindly dispatch a duplicate compensation call.
5. **Tenant Throttling Overriding Business Deadlines**: Naive tenant-bandwidth throttling clamps large tenants to maximum intervals, pushing refunds past supplier-mandated free-cancellation deadlines and incurring non-refundable fees.

## Decision Drivers
- **Idempotency Key & Frozen Input Invariant (CR-01)**: Every retry must present the exact same compensation idempotency key and frozen payload.
- **Mandatory Ambiguity Verification (CR-02)**: Timeouts and 5xx errors must trigger a status query by compensation key prior to any re-send.
- **Deadline-Bound Time Horizons (CR-03)**: Retry horizons must be governed by business deadlines (refund windows, cancellation cutoffs), not arbitrary attempt counts.
- **Non-Terminal Escalation (CR-04)**: Reaching the end of a retry horizon transitions the compensation to `NEEDS_ATTENTION` with an escalation ladder (early warning, deadline alert, finance engagement); it must never be silently dropped.
- **Circuit Breaker Budget Freezing (CR-05)**: When a downstream dependency's circuit breaker opens, all compensation retries targeted at it are frozen without consuming attempt budgets.
- **Step-Level Fenced Verification (CR-07)**: Redrive execution must verify the specific step's compensation state and attempt ID within a fenced database transaction.

## Considered Options

### Option 1: Fixed 10-Attempt Immediate Backoff with Dead-Letter Abandonment
- *Mechanism*: Retry 10 times with exponential backoff; if still failing, mark `FAILED_COMPENSATION` and route to dead-letter queue.
- *Downsides*: Causes double refunds on timeouts; exhausts budgets in minutes during provider outages; abandons customers with no resolution path.

### Option 2: Pure Manual Escalation on First Failure
- *Mechanism*: Any compensation step failure immediately halts automated processing and pages on-call SREs.
- *Downsides*: Generates catastrophic alert fatigue during routine transient network glitches.

### Option 3 (Selected): Time-Horizon Retries with Ambiguity Verification, Jittered Backoff, and Pre-Deadline Escalation Ladder
- *Mechanism*:
  1. Multidimensional error classification: `DONE`, `RETRY`, `RATE_LIMITED`, `WAIT_AND_POLL`, `AMBIGUOUS`, `CONTRACT_DEFECT`, `CREDENTIAL`, `BUSINESS_ALTERNATIVE`, `DEPENDENCY_DOWN`.
  2. For `AMBIGUOUS` (5xx, timeouts), execute status query by key. Only re-send if `NOT_FOUND` within provider deduplication window.
  3. Dynamic backoff: Full jitter with guaranteed floor $\ge \text{Base}/2$, honouring `Retry-After`.
  4. Priority dispatch: Compensations prioritized by earliest business deadline, then cost of delay.
  5. Escalation ladder: Level 1 (early warning at $50\%$ horizon), Level 2 (page before deadline), Level 3 (finance escalation).

## Invariants & Design Contracts
- **CR-01**: Same compensation key and frozen inputs across all retries.
- **CR-02**: Ambiguous outcomes verified before re-send.
- **CR-03**: Horizons bounded by business deadlines.
- **CR-04**: Horizon exhaustion yields `NEEDS_ATTENTION` (non-terminal).
- **CR-05**: Dependency outages freeze retry budgets.
- **CR-06**: Contract defects and credential failures pause the step type in bulk for fix-and-resume.
- **CR-07**: Timers verify step-level state and attempt ID in a fenced transaction.
- **CR-08**: Durable scheduling in PostgreSQL; dispatch only via transactional outbox.
- **CR-09**: Earliest deadline and highest cost of delay dispatched first.
- **CR-10**: External manual resolutions halt automated retries immediately.

## Consequences

### Positive
- Zero double refunds from network timeouts.
- Immunity against provider maintenance windows consuming retry budgets.
- Transparent operational visibility with actionable escalation alerts before financial deadlines pass.

### Negative
- Increased state tracking complexity: step retries require tracking business deadlines, attempt IDs, and verification statuses.
- Requirement for status query adapters per downstream payment/supplier gateway.


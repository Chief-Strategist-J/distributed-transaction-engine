# Compensation Retry — Failure Cases, Edge Conditions & Chaos Matrix

## 1. Classification Tiers

| Tier | Name | Scope & Severity | Invariants Enforced |
|---|---|---|---|
| Tier 0 | Protocol & Boundary | Malformed identifiers, negative intervals, missing deadline timestamps | Ingress Validation Gate |
| Tier 1 | Ambiguity & Deduplication | Network timeouts, HTTP 504s, status query discrepancies, duplicate refunds | CR-01, CR-02 |
| Tier 2 | Time-Horizon & Deadlines | Window cutoffs, pre-deadline alerting, non-terminal escalation | CR-03, CR-04 |
| Tier 3 | Dependency & Circuit Breakers | Downstream 503 storm, provider maintenance, frozen retry budgets | CR-05 |
| Tier 4 | Defect & Credential Pauses | Schema validation 400s, revoked OAuth tokens, bulk pause/resume | CR-06 |
| Tier 5 | Fenced Redrive & Step Verification | Late-arriving successes, stale timer firings, manual override interception | CR-07, CR-08, CR-10 |
| Tier 6 | Priority & Fairness | High-volume tenant retry floods, nearest deadline prioritization | CR-09 |

---

## 2. Comprehensive Edge Cases Matrix

### Tier 1: Ambiguity & Deduplication
- **E2.1: 409 Conflict Misinterpreted as Completed Compensation**
  - *Trigger*: Downstream provider returns HTTP 409 Conflict with message "order already locked".
  - *Hazard*: Naively assuming 409 means "already compensated" closes the saga while customer funds remain unrefunded.
  - *Mitigation*: HTTP 409 triggers an explicit status query or maps to `NEEDS_ATTENTION`; it is never assumed to be successful.
- **E4.1: Re-send After Timeout Without Idempotency Key**
  - *Trigger*: Gateway timeout on POST `/refunds`; retry dispatches new request without original key or with fresh key.
  - *Hazard*: Catastrophic double refund: the supplier executed the first call and executes the second.
  - *Mitigation*: CR-01 enforces immutable compensation idempotency keys and frozen input payloads across all attempts.
- **E4.2: Re-send After Provider Idempotency Window Expires**
  - *Trigger*: Downstream gateway deduplication cache expires after 24 hours; engine retries at hour 25.
  - *Hazard*: Provider treats request as brand new, issuing a duplicate refund.
  - *Mitigation*: Engine checks provider idempotency TTL. If elapsed time exceeds TTL, automated re-send is forbidden; routes to `NEEDS_ATTENTION`.
- **E4.3: Eventually Consistent Provider Status Query Race**
  - *Trigger*: Status query executed 100ms after a 504 timeout returns `NOT_FOUND` due to internal provider replication lag.
  - *Mitigation*: Status queries are delayed by a minimum settle window (e.g., 2,000ms) and repeated before concluding an attempt was never received.

### Tier 2: Time-Horizon & Deadlines
- **E5.1: Rapid Attempt Burnout During Provider Maintenance**
  - *Trigger*: Payment gateway undergoes scheduled 30-minute maintenance; engine burns 10 attempts in 2 minutes.
  - *Hazard*: Entire retry budget exhausted while outage was purely transient.
  - *Mitigation*: CR-03 establishes time-horizon retries (e.g., 24 hours for refunds) rather than tight attempt counts.
- **E5.2: Retries Continuing Past Non-Refundable Cutoff**
  - *Trigger*: Airline ticket cancellation allows free cancellation until 2 hours prior to departure; engine retries past the cutoff.
  - *Hazard*: Provider rejects cancellation; airline fee is levied against merchant.
  - *Mitigation*: Horizon is bounded by `min(budget_duration, business_deadline)`. At $T = \text{deadline} - \text{lead_time}$, Level 2 alert pages human operator.
- **E5.3: Silent Abandonment via Terminal STUCK State**
  - *Trigger*: Retry attempts exhausted; engine flags saga as terminal without human notification.
  - *Hazard*: Customer never receives refund; chargeback filed with penalty fees.
  - *Mitigation*: CR-04 mandates that horizon exhaustion transitions to `NEEDS_ATTENTION` and triggers the Level 3 finance escalation ladder.

### Tier 3: Dependency & Circuit Breakers
- **E9.1: Dependency Outage Draining Tenant Retry Quotas**
  - *Trigger*: Third-party hotel API is down for 3 hours; all pending cancellations increment attempt counters.
  - *Mitigation*: CR-05 freezes retry counters when the target circuit breaker trips to `OPEN`.

### Tier 4: Defect & Credential Pauses
- **E2.2: Schema Defect Spawning Thousands of Individual Alerts**
  - *Trigger*: Engine deploys bug formatting decimal amounts with commas instead of dots; provider returns 400 Bad Request.
  - *Hazard*: 5,000 individual saga tickets created for an identical root-cause bug.
  - *Mitigation*: CR-06 pauses the compensation step type globally (`PAUSED_CONTRACT_DEFECT`) with a single high-priority incident; once patched, all affected compensations resume in bulk.
- **E11.1: Credential Rotation Deadlock**
  - *Trigger*: API secret revoked; engine continuously retries and gets 401 Unauthorized.
  - *Mitigation*: Step type is paused (`PAUSED_CREDENTIAL`); resumes automatically upon token refresh.

### Tier 5 & 6: Fenced Redrive & Step Verification
- **E7.1: Late Acknowledgment Race With Redrive Timer**
  - *Trigger*: Attempt 1 times out; bank completes refund 10 seconds later via webhook; redrive timer fires at 15 seconds.
  - *Mitigation*: CR-07 requires the timer to inspect the step row within a fenced transaction; finding state `COMPENSATED` causes the timer to gracefully discard without re-sending.
- **E10.1: Large Tenant Flooding Retry Queue**
  - *Trigger*: High-volume tenant initiates 50,000 cancellations, starving other tenants.
  - *Mitigation*: Priority scheduling orders by `earliest_deadline ASC, cost_of_delay DESC`, ensuring near-deadline refunds of smaller tenants are never delayed.
- **E13.1: Support Desk Manual Refund Collision**
  - *Trigger*: Customer calls support; agent issues manual refund in provider dashboard; automated engine redrive fires 5 minutes later.
  - *Mitigation*: CR-10 records manual resolutions and halts automated retries immediately.


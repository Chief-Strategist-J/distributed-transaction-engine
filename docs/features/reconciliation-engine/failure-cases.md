# Reconciliation Engine — Failure Cases, Risk Mitigations & Runbooks

This document outlines failure modes, financial race conditions, and mitigation strategies for the Bi-Directional Financial Reconciliation Engine.

---

## 1. Failure Modes & Resilience Matrix

| Failure ID | Scenario / Trigger | Financial / Operational Impact | Invariants Challenged | Architectural Mitigation & Detection |
|---|---|---|---|---|
| **FAIL-RC-001** | **Premature Reversal on Pending Settlement**: Reconciliation executes 2 hours after a payment is authorized. Partner API shows no settled transaction because banking batch settlement takes 24 hours. | Engine flags payment as missing, assumes failure, and triggers an automated refund or cancellation while the customer is charged. | **RC-02, E1.1** | **Mitigation:** Settlement window quarantine. Transactions are exempt from absence-based anomalies until the registered provider settlement period (e.g. 72 hours for ACH, 24 hours for Visa/Mastercard) has elapsed AND the provider's settlement report file is marked complete. |
| **FAIL-RC-002** | **Single-Observation Double Refund Leak**: Engine detects external capture on a saga whose state is `FAILED`. An automated refund is immediately dispatched. However, an automated compensation refund was already in flight at the bank. | Two refunds are issued for a single customer charge, causing direct monetary loss to the platform. | **RC-03, E2.1** | **Mitigation:** Two-phase stability confirmation. The anomaly must be re-confirmed on a subsequent run after a 6-hour delay. The engine queries the pending compensation queue to ensure no refund is currently `IN_FLIGHT` before constructing the remediation saga. |
| **FAIL-RC-003** | **Systemic Feed Drop Bulk-Remediation Catastrophe**: Upstream payment processor API returns a truncated daily CSV with 50% fewer records due to a partner pipeline outage. | Engine detects 5,000 "missing external settlements" and initiates bulk cancellations or refunds. | **RC-08, E5.1** | **Mitigation:** Systemic anomaly circuit breaker. If the anomaly rate exceeds 5% of the total records in a batch, all automated remediations are immediately halted, the run transitions to `HALTED_SYSTEMIC_ANOMALY`, and a P1 on-call incident is triggered. |
| **FAIL-RC-004** | **Zero-Tolerance Alert Storm on FX/Fees**: Transactions with legitimate interchange fees ($2.90 + 30¢) or currency conversion rounding ($0.004) are compared against zero tolerance. | Thousands of false-positive P3 alerts flood on-call engineers, burying genuine financial leakage anomalies. | **RC-05, E4.1** | **Mitigation:** Dynamic tolerance rules. Amounts are evaluated against registered fee matrix tables: $|\text{Internal} - \text{External} - \text{Fee}_{\text{expected}}| \le 0.05$. Variances within tolerance are automatically classified as `DISMISSED_WITHIN_TOLERANCE`. |
| **FAIL-RC-005** | **Overlapping Runs Double-Action Race**: A daily settlement run exceeds 30 minutes; a scheduled cron triggers a second runner. | Both runs detect the same discrepancies and trigger duplicate remediation sagas. | **RC-04, E3.1** | **Mitigation:** Database lease lock on `reconciliation_runs` table with unique constraint on `(tenant_id, provider_id, window_start, window_end)`. All remediations use deterministic idempotency keys: `idem:recon:<tenant>:<anomaly_id>`. |
| **FAIL-RC-006** | **Disruptive Tenant Account Auto-Lock**: Internal accounting detects a $15,000 discrepancy caused by a partner reporting delay and automatically freezes the tenant's transaction processing. | Severe customer business outage, contractual SLA breaches, and merchant churn. | **RC-06, Bug 3** | **Mitigation:** Tiered non-disruptive response. Discrepancies above threshold trigger payout holds on merchant settlement balances but never block live payment authorization or saga execution without manual executive approval. |

---

## 2. In-Depth Failure Drill: Systemic Feed Failure Circuit Breaker

```mermaid
sequenceDiagram
    autonumber
    participant Feed as Partner Report Ingest
    participant Engine as Reconciliation Engine
    participant CB as Circuit Breaker
    participant Alert as PagerDuty / On-Call
    participant DB as PostgreSQL 18

    Feed->>Engine: Ingest Daily Report (5,000 records; normal is 50,000)
    Engine->>DB: Query expected internal sagas (50,000 records)
    Engine->>Engine: Match internal -> external
    Note over Engine: Discrepancy count: 45,000 (90% anomaly rate!)
    
    Engine->>CB: EvaluateAnomalyRate(45000, 50000)
    
    rect rgb(255, 230, 230)
        Note over CB, Alert: RC-08 Circuit Breaker TRIPPED (> 5% threshold)
        CB-->>Engine: HALT AUTOMATED REMEDIATIONS
        Engine->>DB: UPDATE reconciliation_runs SET status = 'HALTED_SYSTEMIC_ANOMALY'
        Engine->>Alert: Trigger P1 Incident: SystemicPartnerFeedDropDetected
        Note over Engine: Zero auto-refunds or auto-cancellations sent!
    end
```

---

## 3. Operator Runbook for Orphan External Charges

When an anomaly of class `ORPHAN_EXTERNAL_CHARGE` is confirmed:

1. **Query confirmed unresolved orphan charges:**
   ```sql
   SELECT id, external_reference, external_amount, currency, last_confirmed_at 
   FROM reconciliation_anomalies 
   WHERE anomaly_class = 'ORPHAN_EXTERNAL_CHARGE' 
     AND resolution = 'UNRESOLVED'
     AND observation_count >= 2;
   ```
2. **Investigate external reference in provider dashboard (Stripe/Adyen):**
   - Verify whether charge corresponds to a customer shopping cart or failed saga.
3. **Trigger corrective action:**
   - **Link to existing saga and mark succeeded:**
     `POST /api/v1/reconciliation/anomalies/{id}/resolve-link` with `{"saga_id": "tx-1234"}`
   - **Issue manual refund via engine:**
     `POST /api/v1/reconciliation/anomalies/{id}/resolve-refund`


# ADR-0007: Bi-Directional Financial Reconciliation, Windowed Settlement Matching & Idempotent Remediation Engine

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-11-05 |
| Deciders | Lead Systems Architect & Core Architecture Guild |
| Related Docs | [Business Logic & Decision Tree v2](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/reconciliation-engine/business-logic.md) · [Diagrams](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/reconciliation-engine/diagrams.md) · [Failure Cases & Mitigations](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/reconciliation-engine/failure-cases.md) |
| Target Feature | [Reconciliation Engine Feature](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/src/features/reconciliation-engine) |

---

## 1. Context & Problem Statement

Distributed transaction engines record saga lifecycles and communicate with external financial institutions, payment gateways (Stripe, Adyen), bank APIs, and logistics networks. Even with the Transactional Outbox, Inbox Deduplication, and Idempotent Reverse Compensation, state drift inevitably occurs across distributed network boundaries due to:
1. **Orphaned External Charges**: Downstream partner captures funds, but the orchestrator crashes before recording the external reference ID. The saga terminates as `FAILED`, leaving settled customer funds unrecorded internally.
2. **Delayed Settlements & Bank Clearing (T+1/T+2)**: Banking rails do not settle instantly. Reconciling immediately against live partner APIs incorrectly classifies pending transactions as missing ("phantom settlement").
3. **Foreign Exchange (FX) & Interchange Fee Variances**: A $100.00 charge incurs a $2.90 interchange fee and 0.5% FX markup, returning an external settlement of $96.60. Naive zero-tolerance matching flags every transaction as corrupt.
4. **Duplicate External Executions**: Transient partner timeouts followed by blind retries generate duplicate captures at the gateway with two distinct provider reference IDs for a single saga.
5. **Harmful Automated Interventions**: Automatically locking customer tenant accounts or issuing duplicate refunds based on a single transient API mismatch creates customer outages and financial double-refund leaks.

---

## 2. Decision Drivers & Architecture Invariants (RC-01 – RC-12)

* **RC-01 (Mandatory Bi-Directional Matching)**: Reconciliation runs across two symmetrical vectors:
  - **Internal $\to$ External**: Every internal transaction in `COMPLETED` or `COMPENSATED` status must match an external provider settlement record.
  - **External $\to$ Internal**: Every external debit/credit on bank/gateway settlement reports must map to a valid internal saga aggregate.
* **RC-02 (Settlement Window Quarantines)**: Transactions within their provider settlement grace period (e.g. 72 hours for ACH/SEPA) are classified as `PENDING_SETTLEMENT` and never flagged as discrepancies.
* **RC-03 (Two-Phase Anomaly Confirmation)**: An automated remediation is never triggered on a single observation. An anomaly must be confirmed as stable across two consecutive reconciliation runs separated by a cool-down delay (default: 6 hours).
* **RC-04 (Deterministic Idempotent Remediation)**: All corrective actions dispatch via the saga engine using deterministic idempotency keys:
  $$\text{Key}_{\text{remed}} = \text{idem:recon:} \langle \text{tenant\_id} \rangle : \langle \text{run\_id} \rangle : \langle \text{anomaly\_id} \rangle$$
* **RC-05 (Dynamic Tolerance Rule Matching)**: Balance matching evaluates against registered fee schedules, rounding rules, and currency conversion bands rather than raw zero-dollar parity:
  $$|\Delta_{\text{amount}}| \le \text{Fee}_{\text{expected}} + \text{Tolerance}_{\text{currency}}$$
* **RC-06 (No Automated Customer Account Locking)**: Anomalies exceeding financial thresholds flag high-priority operator reviews and payout holds; they never automatically lock active tenant processing.
* **RC-07 (Append-Only Discrepancy Journal)**: Reconciliation runs, ledger deltas, and audit anomalies are strictly append-only in PostgreSQL.
* **RC-08 (Systemic Outage Suppression)**: If anomaly rates exceed 5% of a batch, all automated remediations are halted immediately; the engine classifies the event as an upstream provider report outage.

---

## 3. Relational Schema & DDL

```sql
CREATE TYPE recon_run_type AS ENUM ('OPERATIONAL', 'SETTLEMENT', 'PERIOD_CLOSE');
CREATE TYPE recon_status AS ENUM ('RUNNING', 'COMPLETED', 'HALTED_SYSTEMIC_ANOMALY', 'FAILED');
CREATE TYPE anomaly_class AS ENUM (
    'ORPHAN_EXTERNAL_CHARGE',
    'ORPHAN_INTERNAL_RECORD',
    'AMOUNT_MISMATCH',
    'DUPLICATE_EXTERNAL_CAPTURE',
    'STATUS_DIVERGENCE'
);
CREATE TYPE anomaly_resolution AS ENUM ('UNRESOLVED', 'AUTO_REMEDIATED', 'MANUAL_ADJUSTMENT', 'DISMISSED_WITHIN_TOLERANCE');

-- Master table for reconciliation batches
CREATE TABLE IF NOT EXISTS reconciliation_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id VARCHAR(64) NOT NULL,
    provider_id VARCHAR(64) NOT NULL,
    run_type recon_run_type NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    status recon_status NOT NULL DEFAULT 'RUNNING',
    total_records_processed INT NOT NULL DEFAULT 0,
    anomalies_detected INT NOT NULL DEFAULT 0,
    remediations_dispatched INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

-- Individual anomaly ledger
CREATE TABLE IF NOT EXISTS reconciliation_anomalies (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id VARCHAR(64) NOT NULL,
    run_id UUID NOT NULL REFERENCES reconciliation_runs(id) ON DELETE CASCADE,
    saga_id VARCHAR(64),
    external_reference VARCHAR(128),
    anomaly_class anomaly_class NOT NULL,
    internal_amount NUMERIC(18, 4),
    external_amount NUMERIC(18, 4),
    discrepancy_delta NUMERIC(18, 4),
    currency VARCHAR(3) NOT NULL,
    first_observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_confirmed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    observation_count INT NOT NULL DEFAULT 1,
    resolution anomaly_resolution NOT NULL DEFAULT 'UNRESOLVED',
    resolution_details JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_recon_anomaly UNIQUE (tenant_id, anomaly_class, saga_id, external_reference)
);

CREATE INDEX IF NOT EXISTS idx_recon_unresolved 
    ON reconciliation_anomalies (tenant_id, resolution, anomaly_class) 
    WHERE resolution = 'UNRESOLVED';
```

---

## 4. Hexagonal Architecture Ports & Go Contracts

```go
package reconciliation

import (
	"context"
	"time"
	"github.com/google/uuid"
)

type AnomalyClass string

const (
	AnomalyOrphanExternalCharge     AnomalyClass = "ORPHAN_EXTERNAL_CHARGE"
	AnomalyOrphanInternalRecord     AnomalyClass = "ORPHAN_INTERNAL_RECORD"
	AnomalyAmountMismatch           AnomalyClass = "AMOUNT_MISMATCH"
	AnomalyDuplicateExternalCapture AnomalyClass = "DUPLICATE_EXTERNAL_CAPTURE"
	AnomalyStatusDivergence         AnomalyClass = "STATUS_DIVERGENCE"
)

type SettlementRecord struct {
	ExternalRef string
	ProviderID  string
	Amount      float64
	Currency    string
	SettledAt   time.Time
}

type EngineReconPort interface {
	ExecuteSettlementRun(ctx context.Context, tenantID, providerID string, windowStart, windowEnd time.Time, externalFeed []SettlementRecord) (*ReconciliationSummary, error)
	ConfirmStableAnomaly(ctx context.Context, anomalyID uuid.UUID) (bool, error)
	DispatchAutoRemediation(ctx context.Context, anomalyID uuid.UUID) error
}

type ReconciliationRepositoryPort interface {
	CreateRun(ctx context.Context, run *ReconciliationRun) error
	GetInternalSettledSagas(ctx context.Context, tenantID, providerID string, start, end time.Time) ([]InternalSagaRecord, error)
	UpsertAnomaly(ctx context.Context, anomaly *ReconciliationAnomaly) error
	GetStableAnomaliesForRemediation(ctx context.Context, tenantID string, minObservations int, minAge time.Duration) ([]ReconciliationAnomaly, error)
	UpdateAnomalyResolution(ctx context.Context, anomalyID uuid.UUID, resolution string, details map[string]interface{}) error
}
```


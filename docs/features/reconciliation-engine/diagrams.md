# Reconciliation Engine — Architectural & Visual Diagrams

This document contains architectural diagrams and execution workflows for the **Bi-Directional Financial Reconciliation Engine**, supporting [ADR-0007](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/reconciliation-engine/adr.md) and [Business Logic Decision Tree v2](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/reconciliation-engine/business-logic.md).

---

## 1. High-Level Architecture (HLD): Bi-Directional Ingestion Pipeline

```mermaid
flowchart TB
    subgraph ExternalSources ["External Providers & Banking Rails"]
        StripeReport["Stripe Settlement CSV / Feed"]
        BankFeed["Bank BAI2 / MT940 CAMT.053"]
        AdyenFeed["Adyen Financial Journal"]
    end

    subgraph IngestionAdapters ["Ingestion & Normalization Layer"]
        StripeAdapter["StripeSettlementParser"]
        BankAdapter["BankReportParser"]
        StripeReport --> StripeAdapter
        BankFeed --> BankAdapter
        AdyenFeed --> StripeAdapter
    end

    subgraph InternalSources ["Internal Transaction Datastores"]
        SagaStore[("PostgreSQL 18: saga_instances")]
        LedgerStore[("Accounting Ledger DB")]
    end

    subgraph ReconciliationCore ["Reconciliation Engine Core"]
        Matcher["Bi-Directional Matching Engine"]
        ToleranceRules["Fee & FX Tolerance Evaluator"]
        StabilityTracker["Two-Phase Anomaly Tracker"]
        
        Matcher --> ToleranceRules
        ToleranceRules --> StabilityTracker
    end

    IngestionAdapters -->|Normalized Settlement Records| Matcher
    InternalSources -->|Internal Completed/Failed Records| Matcher

    subgraph StorageAndAudit ["Audit & Anomaly Datastore"]
        StabilityTracker --> DB_Runs[("reconciliation_runs")]
        StabilityTracker --> DB_Anomalies[("reconciliation_anomalies")]
    end

    subgraph RemediationLayer ["Automated Remediation"]
        StabilityTracker -->|Stable Anomaly Confirmed (count >= 2)| RemediationDispatcher["Remediation Sagas Dispatcher"]
        RemediationDispatcher -->|Dispatch Corrective Outbox Event| SagaEngine["Saga Orchestrator Core"]
        StabilityTracker -->|Anomaly Rate > 5%| CircuitBreaker["Systemic Outage Circuit Breaker"]
        CircuitBreaker -->|P1 Alert| PagerDuty["On-Call & Finance Operations"]
    end
```

---

## 2. Low-Level Component Model (LLD)

```mermaid
classDiagram
    class EngineReconPort {
        <<interface>>
        +ExecuteSettlementRun(ctx, tenantID, providerID, windowStart, windowEnd, externalFeed) ReconciliationSummary
        +ConfirmStableAnomaly(ctx, anomalyID) bool
        +DispatchAutoRemediation(ctx, anomalyID) error
    }

    class ReconciliationRepositoryPort {
        <<interface>>
        +CreateRun(ctx, run) error
        +GetInternalSettledSagas(ctx, tenantID, providerID, start, end) []InternalSagaRecord
        +UpsertAnomaly(ctx, anomaly) error
        +GetStableAnomaliesForRemediation(ctx, tenantID, minObservations, minAge) []ReconciliationAnomaly
        +UpdateAnomalyResolution(ctx, anomalyID, resolution, details) error
    }

    class BiDirectionalMatcher {
        +MatchInternalToExternal(internal, external) []Discrepancy
        +MatchExternalToInternal(external, internal) []Discrepancy
        +ApplyTolerances(discrepancies, rules) []ReconciliationAnomaly
    }

    class SystemicDriftDetector {
        +EvaluateBatchThreshold(total, anomalies) bool
        +HaltAutomatedRemediations() void
    }

    class RemediationDispatcher {
        +BuildRemediationPayload(anomaly) SagaStartRequest
        +DispatchCorrectiveSaga(ctx, req) error
    }

    EngineReconPort <|.. ReconciliationService
    ReconciliationService --> ReconciliationRepositoryPort : persists
    ReconciliationService --> BiDirectionalMatcher : matches
    ReconciliationService --> SystemicDriftDetector : validates
    ReconciliationService --> RemediationDispatcher : triggers
```

---

## 3. Bi-Directional Matching Sequence Flow

```mermaid
sequenceDiagram
    autonumber
    participant Cron as Settlement Scheduler
    participant Recon as Reconciliation Engine
    participant DB as PostgreSQL 18
    participant Partner as External Gateway (Stripe)

    Cron->>Recon: Trigger Daily Settlement Run (Window: T-24h to T)
    Recon->>Partner: Download Authoritative Settlement Feed
    Partner-->>Recon: 10,000 External Settlement Records
    
    Recon->>DB: Query Internal Sagas within Window
    DB-->>Recon: 9,996 Internal Settled Records

    rect rgb(240, 248, 255)
        Note over Recon: Phase 1: Internal -> External Verification
        Recon->>Recon: Verify all 9,996 internal records exist in partner feed
        Note over Recon: Result: 1 internal charge missing from external report (Orphan Internal)
    end

    rect rgb(255, 245, 230)
        Note over Recon: Phase 2: External -> Internal Verification
        Recon->>Recon: Verify all 10,000 external records have matching internal sagas
        Note over Recon: Result: 5 external charges have no internal saga (Orphan External)
    end

    Recon->>DB: Record Anomalies in 'reconciliation_anomalies'
    Recon->>DB: Complete run record in 'reconciliation_runs'
```

---

## 4. Two-Phase Stable Anomaly Confirmation & Remediation

```mermaid
sequenceDiagram
    autonumber
    participant Run1 as Recon Run 1 (Hour 00:00)
    participant DB as PostgreSQL 18
    participant Run2 as Recon Run 2 (Hour 06:00)
    participant Engine as Saga Core Engine
    participant Gateway as External Payment Gateway

    Run1->>DB: Detect Orphan External Charge (ref='ch_9912', amount=$50.00)
    Run1->>DB: INSERT INTO reconciliation_anomalies (observation_count=1, resolution='UNRESOLVED')
    Note over DB: First observation logged. NO ACTION TAKEN.
    
    Note over Run2, DB: 6-Hour Cool-down Elapsed...
    
    Run2->>DB: Query pending anomalies (min_age=6h, min_obs=1)
    Run2->>Gateway: Active Status Probe (Query ch_9912)
    Gateway-->>Run2: Charge settled, no refund recorded
    
    rect rgb(240, 255, 240)
        Note over Run2, Engine: Anomaly Confirmed Stable (RC-03)!
        Run2->>DB: UPDATE reconciliation_anomalies SET observation_count=2, resolution='AUTO_REMEDIATED'
        Run2->>Engine: Dispatch Remediation Saga (idem:recon:tenant1:run2:anom-ch9912)
        Engine->>Engine: Start IngestOrphanChargeWorkflow(ch_9912, $50.00)
    end
```

---

## 5. Anomaly Lifecycle State Machine

```mermaid
stateDiagram-v2
    [*] --> OBSERVED_ONCE: Discrepancy Found in Run
    
    OBSERVED_ONCE --> RESOLVED_TRANSIENT: Cleared in Next Run (Within Settlement Lag)
    OBSERVED_ONCE --> CONFIRMED_STABLE: Re-confirmed after 6h Cooldown
    
    CONFIRMED_STABLE --> DISMISSED_TOLERANCE: Matches registered Fee/FX variance
    CONFIRMED_STABLE --> AUTO_REMEDIATED: Low Risk Anomaly (< $500, Known Pattern)
    CONFIRMED_STABLE --> MANUAL_REVIEW_REQUIRED: High Value (> $500) or Duplicate Capture
    
    AUTO_REMEDIATED --> [*]
    DISMISSED_TOLERANCE --> [*]
    MANUAL_REVIEW_REQUIRED --> MANUAL_ADJUSTED: Operator Corrects / Offsets Ledger
    MANUAL_ADJUSTED --> [*]
```


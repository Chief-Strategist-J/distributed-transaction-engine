# Reconciliation Engine — Business Logic Decision Tree

```
RECONCILIATION ENGINE (BUSINESS DECISION TREE)
│
├── 1. RECONCILIATION BATCH SCAN TIER
│   └── Trigger: Periodic Cron Schedule (e.g. Every Hour at Minute 0) OR Manual Ops Trigger
│       ├── Action: Acquire Distributed Reconciliation Leader Lock in Redis (TTL=1800s)
│       │   ├── Lock Held by Another Worker ──> [ABORT: Another Reconciliation Job In-Flight]
│       │   └── Lock Acquired
│       │       └── Action: Define Time Window Boundary:
│       │           └── WindowStart = NOW() - 25 Hours, WindowEnd = NOW() - 1 Hour
│       │
│       └── Action: Query PostgreSQL Engine Ledger for Sagas Completed within Window
│           └── State: RECONCILIATION_SCANNING
│
├── 2. THREE-WAY DATA CROSS-CHECK TIER
│   └── State: RECONCILIATION_SCANNING
│       └── Loop: For Each Completed Saga in Ledger:
│           ├── Step 1: Query Internal Engine Transaction Status & Step Outcomes
│           ├── Step 2: Query External Third-Party Systems (Bank API, Payment Gateway, Inventory DB)
│           │   ├── Third-Party Query Returns: COMMITTED (Record Exists, Amount Matches)
│           │   │   └── Match with Engine Status == 'COMPLETED':
│           │   │       └── Status: BALANCED ──> Advance to Next Saga
│           │   │
│           │   ├── Third-Party Query Returns: COMMITTED (Record Exists) BUT Engine Status == 'COMPENSATED'
│           │   │   └── Anomaly: GHOST TRANSACTION (Downstream was not refunded/undone)
│           │   │       └── State: ANOMALY_DETECTED ──> Handover to Tier 3
│           │   │
│           │   ├── Third-Party Query Returns: NOT_FOUND BUT Engine Status == 'COMPLETED'
│           │   │   └── Anomaly: PHANTOM SETTLEMENT (Engine assumed success, external failed)
│           │   │       └── State: ANOMALY_DETECTED ──> Handover to Tier 3
│           │   │
│           │   └── Step 3: Check Numerical Drift (e.g. Fee discrepancy, rounding delta > $0.01)
│           │       └── State: ANOMALY_DETECTED ──> Handover to Tier 3
│
└── 3. ANOMALY RESOLUTION & ESCALATION TIER
    └── State: ANOMALY_DETECTED
        ├── Action: Write Anomaly Record to `reconciliation_anomalies` Table
        ├── Check: Can Anomaly Be Auto-Resolved (e.g. Issue Delayed Refund / Resend Webhook)?
        │   ├── YES (Configured for Safe Auto-Correction)
        │   │   └── Action: Dispatch Compensating/Correcting Action via Engine Internal API
        │   │       ├── Outcome: SUCCESS ──> Update Anomaly status='AUTO_RESOLVED'
        │   │       └── Outcome: FAILED ──> Escalate to Manual Review
        │   │
        │   └── NO (Financial Mismatch / Irreversible Drift)
        │       └── Action: Create Critical Incident Alert in PagerDuty
        │           ├── Action: Lock Tenant Account if Drift Exceeds Risk Threshold (e.g. > $10,000)
        │           └── Action: Generate Financial Reconciliation Discrepancy Report for Finance Team
        │               └── State: QUARANTINED_FOR_AUDIT ──> [TERMINAL: Awaiting Manual Human Settlement]
```

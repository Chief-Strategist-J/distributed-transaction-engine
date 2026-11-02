# Crash Recovery — Business Logic Decision Tree

```
CRASH RECOVERY (BUSINESS DECISION TREE)
│
├── 1. POD STARTUP & KERNEL INITIALIZATION TIER
│   └── Trigger: Distributed Transaction Engine Pod Boots / Reboots (SIGKILL Recovery)
│       ├── Action: Connect to PostgreSQL 18 & Redis Cluster
│       │   ├── Connection Fails ──> [CRASH LOOP: Liveness Probe Fails, Kubelet Restarts Pod]
│       │   └── Connection Ready
│       │       └── Action: Acquire Leadership Lease or Worker Node Partition Slot via Redis/Postgres
│       │           └── State: RECOVERY_BOOTSTRAP
│
├── 2. ORPHANED & EXPIRED SAGA RECLAMATION TIER
│   └── State: RECOVERY_BOOTSTRAP
│       └── Action: Scan PostgreSQL for Sagas in Non-Terminal States ('RUNNING', 'COMPENSATING')
│           │       WHERE worker_heartbeat_at < (NOW() - Interval '30 seconds')
│           ├── Check: Are there any expired orphaned saga leases?
│           │   ├── NO ──> State: CLEAN_STATE ──> [Enter Normal Event Consumer Mode]
│           │   └── YES ──> Action: Fetch list of orphaned Saga IDs
│           │       └── Loop: For each orphaned saga:
│           │           ├── Action: Attempt Atomic CAS Lease Takeover:
│           │           │   UPDATE saga_instance SET worker_id = CurrentPodID, heartbeat_at = NOW(),
│           │           │   fencing_token = fencing_token + 1 WHERE id = SagaID AND fencing_token = StoredToken
│           │           ├── Result: CAS Fails (Another live worker claimed it first) ──> Skip to next
│           │           └── Result: CAS Succeeds ──> State: SAGA_RECLAIMED
│
├── 3. STATE RECONCILIATION & REPLAY TIER
│   └── State: SAGA_RECLAIMED
│       ├── Action: Inspect Last Persisted saga_step and Outbox Ledger for Reclaimed Saga
│       │   ├── Condition: Last Step has status='PENDING' or 'IN_FLIGHT'
│       │   │   ├── Action: Query Downstream Service via Step Idempotency Key (Inspect actual remote state)
│       │   │   ├── Remote Status: COMMITTED/EXECUTED ──> Update DB step status='SUCCEEDED'
│       │   │   ├── Remote Status: NOT_FOUND/FAILED ──> Mark DB step status='FAILED'
│       │   │   └── Remote Status: INDETERMINATE / UNREACHABLE ──> Increment Retry or Schedule Probe
│       │   │
│       │   ├── Condition: Saga in State 'RUNNING' with valid next steps
│       │   │   └── Action: Rebuild in-memory DAG and Resume Forward Execution Tier
│       │   │
│       │   └── Condition: Saga in State 'COMPENSATING'
│       │       └── Action: Rebuild in-memory reverse graph and Resume Compensation Engine Tier
│       │
│       └── Action: Scan Transactional Outbox for unsent messages (status='PENDING' / 'CLAIMED')
│           └── Action: Re-publish pending messages to Kafka and update status='SENT'
│               └── State: RECOVERY_COMPLETE ──> [Handover to Active Execution Scheduler]
```

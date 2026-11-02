# Scheduled Execution — Business Logic Decision Tree

```
SCHEDULED EXECUTION (BUSINESS DECISION TREE)
│
├── 1. SCHEDULE REGISTRATION & VALIDATION TIER
│   └── Trigger: User / Client Defines Scheduled or Delayed Transaction
│       ├── Check: Schedule Type:
│       │   ├── Type A: ONE_OFF_DELAYED (Run at Timestamp T in future)
│       │   │   └── Check: Is Target Timestamp > NOW() + 5 seconds?
│       │   │       ├── NO ──> [REJECT: Scheduled time must be in future]
│       │   │       └── YES ──> Valid
│       │   └── Type B: RECURRING_CRON (e.g. `0 0 * * *`)
│       │       └── Check: Parse Standard 5-Field Cron Expression
│       │           ├── Invalid Cron Syntax ──> [REJECT: HTTP 400 Bad Request]
│       │           └── Valid Cron Syntax ──> Compute Next Execution Timestamp (NextRunAt)
│       │
│       └── Action: Atomic INSERT into `scheduled_jobs` Table with status='SCHEDULED'
│           └── State: JOB_PERSISTED
│
├── 2. DISTRIBUTED TIMEWHEEL & SCANNING TIER
│   └── Trigger: Engine Scheduler Ticks (Every 1000ms via High-Resolution Timer)
│       ├── Action: Acquire Distributed Scheduler Leader Mutex in Redis (Lease: 5s)
│       │   ├── Lease Acquisition Fails ──> [Worker Yields to Current Leader]
│       │   └── Lease Acquired
│       │       └── Action: Query Due Jobs from PostgreSQL:
│       │           │   SELECT id, workflow_name, payload, cron_expr FROM scheduled_jobs
│       │           │   WHERE status = 'SCHEDULED' AND next_run_at <= NOW()
│       │           │   ORDER BY next_run_at ASC LIMIT 100 FOR UPDATE SKIP LOCKED;
│       │
├── 3. SAGA TRIGGERING & INTERVAL ADVANCEMENT TIER
│   └── Loop: For Each Claimed Due Job:
│       ├── Action: Generate Idempotency Key:
│       │   └── ScheduleIdempotencyKey = "sched:" || JobID || ":" || NextRunAtEpochSeconds
│       │
│       ├── Action: Trigger New Saga Instance via Core Orchestrator API
│       │   ├── Result: SAGA_DISPATCHED
│       │   │   ├── Check: Is Job Recurring (Cron)?
│       │   │   │   ├── YES ──> Compute Subsequent Run Timestamp:
│       │   │   │   │   └── UPDATE scheduled_jobs SET next_run_at = NextComputedTime, last_run_at = NOW()
│       │   │   │   └── NO (One-Off) ──>
│       │   │   │       └── UPDATE scheduled_jobs SET status = 'COMPLETED', completed_at = NOW()
│       │   │   └── State: SCHEDULE_PROCESSED ──> [TERMINAL: Job Dispatched to Engine]
│       │   │
│       │   └── Result: DISPATCH_FAILED (Database / Queue Error)
│       │       └── Action: Release Job Lock & Apply 10s Retry Backoff to next_run_at
```

# Optimistic Concurrency Control — Business Logic Decision Tree

```
OPTIMISTIC CONCURRENCY CONTROL (BUSINESS DECISION TREE)
│
├── 1. READ & VERSION CAPTURE TIER
│   └── Trigger: Engine Worker Reads Saga Record to Execute Next Mutation
│       ├── Action: SELECT id, state, version, payload FROM saga_instance WHERE id = :id;
│       ├── Action: Store In-Memory Copy: CurrentVersion = Record.version
│       └── State: STATE_LOADED
│
├── 2. BUSINESS EXECUTION & ATOMIC CAS MUTATION TIER
│   └── State: STATE_LOADED
│       ├── Action: Compute Business Logic & Determine New State
│       └── Action: Execute Atomic Compare-And-Swap (CAS) Update in PostgreSQL:
│           │
│           └── SQL: UPDATE saga_instance
│                    SET state = :newState,
│                        payload = :newPayload,
│                        version = version + 1,
│                        updated_at = NOW()
│                    WHERE id = :id AND version = :CurrentVersion;
│
├── 3. OUTCOME EVALUATION & CONFLICT RESOLUTION TIER
│   └── Trigger: PostgreSQL Returns Affected Row Count
│       ├── Check: Did Update Affect Exactly 1 Row (RowsAffected == 1)?
│       │   ├── YES ──> Commit Transaction
│       │   │   └── State: WRITE_COMMITTED ──> [TERMINAL: Version Incremented Smoothly]
│       │   │
│       │   └── NO (RowsAffected == 0 — Concurrent Conflict Detected!)
│       │       └── Action: Rollback Local DB Transaction
│       │           ├── Check: Has Current Attempt Exceeded Max OCC Retries (Default: 3 Attempts)?
│       │           │   ├── YES ──> Action: Abort Update Due to High Contention
│       │           │   │   └── State: OCC_CONFLICT_EXHAUSTED ──> [ABORT: HTTP 409 Conflict / Requeue Saga]
│       │           │   │
│       │           │   └── NO ──> Action: Apply Full Jitter Delay:
│       │           │       │       Sleep = Random(10ms, 50ms * 2^Attempt)
│       │           │       └── Action: Re-read Saga Record from DB (Get Latest Version)
│       │           │           └── Loop Back to Tier 1 with Attempt = Attempt + 1
```

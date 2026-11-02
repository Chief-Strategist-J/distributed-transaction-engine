# Fencing Tokens — Business Logic Decision Tree

```
FENCING TOKENS (BUSINESS DECISION TREE)
│
├── 1. TOKEN GENERATION & LEASE ACQUISITION TIER
│   └── Trigger: Worker Process Seeks Leadership Lease to Execute Saga / Step
│       ├── Action: Request Distributed Lock Lease from Central Authority (Redis / Postgres)
│       ├── Check: Is Lock Free OR Previous Worker Lease Expired (Heartbeat Timeout)?
│       │   ├── NO ──> [REJECT: Lock Busy — Yield Execution to Current Leaseholder]
│       │   └── YES ──> Action: Acquire Lease via Atomic CAS
│       │       └── Action: Atomically Increment Global Fencing Token Counter:
│       │           └── NewFencingToken = Redis.INCRBY(fencing_key, 1)  [Monotonically Increasing 64-bit Integer]
│       │
│       └── Action: Bind NewFencingToken to In-Memory Worker Context
│           └── State: TOKEN_ACQUIRED
│
├── 2. TRANSACTIONAL PROPAGATION & SUBMISSION TIER
│   └── State: TOKEN_ACQUIRED
│       ├── Action: Worker prepares Database Mutation or Outbound Remote Command
│       └── Action: Inject `fencing_token = NewFencingToken` into Query / RPC Payload
│           │
│           └── Target System: PostgreSQL Storage Engine
│               └── Query Execution:
│                   UPDATE saga_instance
│                   SET state = NewState, fencing_token = :fencing_token, updated_at = NOW()
│                   WHERE id = :saga_id AND fencing_token < :fencing_token;
│
├── 3. PERSISTENCE BARRIER & STALE WRITE REJECTION TIER
│   └── Trigger: Database Executes Fencing Validation Query
│       ├── Check: Did the UPDATE statement affect exactly 1 row (RowsAffected == 1)?
│       │   ├── YES ──> Commit Mutation
│       │   │   └── State: MUTATION_COMMITTED ──> [TERMINAL: Token Validated & Advanced]
│       │   │
│       │   └── NO (RowsAffected == 0)
│       │       └── Action: Detect Cause:
│       │           ├── Check: Does Database record contain fencing_token >= :fencing_token?
│       │           │   ├── YES ──> [DETECTED: ZOMBIE WRITE HAZARD! Worker experienced Stop-The-World GC/Net Split]
│       │           │   │   └── Action: Immediately Abort Worker Context
│       │           │   │       ├── Action: Release Local Resources, Cancel Thread, Log Fencing Violation
│       │           │   │       └── State: ZOMBIE_QUARANTINED ──> [TERMINAL: Data Corruption Prevented]
│       │           │   │
│       │           │   └── NO ──> Record Not Found or Other Precondition Failed ──> Raise Database Error
```

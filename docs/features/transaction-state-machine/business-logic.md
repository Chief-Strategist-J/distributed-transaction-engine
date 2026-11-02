# Transaction State Machine — Business Logic Decision Trees

## 1. Architectural Overview & State Decision Tree

```
TRANSACTION STATE MACHINE (BUSINESS DECISION TREE)
│
├── 1. REGISTRATION & INITIALIZATION TIER [P0]
│   └── Real-World: Mobile shopper taps 'Pay $500' twice on flaky 4G network; ingress proxy duplicates POST
│       └── Trigger: Client Submits Transaction / Saga Intent
│           ├── Check: Is Idempotency-Key header present in request?
│           │   ├── NO ──> [REJECT: HTTP 400 Bad Request — Missing Idempotency-Key]
│           │   └── YES ──> Check: Tenant-scoped Idempotency Key in Redis / PostgreSQL?
│           │       ├── YES (In-Flight / Leased) ──> [ABORT: HTTP 409 Conflict — Transaction Already In-Flight]
│           │       ├── YES (Terminal / Completed) ──> [TERMINAL: Return Cached Previous Response via HTTP 200]
│           │       └── NO (New Unique Intent)
│           │           └── Action: Atomic INSERT INTO saga_instance & outbox (version=1, state='CREATED')
│           │               └── State: CREATED
│           │                   ├── Event: Client Abort / Cancellation before Worker Claim
│           │                   │   └── Action: CAS transition to CANCELLED (version=version+1)
│           │                   │       └── State: CANCELLED ──> [TERMINAL: Zero Side-Effects Executed]
│           │                   └── Event: Orchestrator acquires execution lease + monotonic fencing token
│           │                       └── Action: CAS transition to RUNNING (lease_expires_at = NOW() + TTL)
│           │                           └── State: RUNNING
│
├── 2. FORWARD DAG EXECUTION TIER [P0 / P1]
│   └── Real-World: Multi-vendor checkout workflow (deduct points -> debit card -> reserve warehouse stock)
│       └── State: RUNNING (Executing Forward Steps in Topological Sequence)
│           ├── Step Type: Synchronous Remote Service (e.g. gRPC/HTTP Inventory Reservation)
│           │   ├── Pre-Flight: Generate Stable Operation ID = SHA256(tenant_id, saga_id, step_id, attempt_no)
│           │   ├── Execution: Invoke Microservice with Deadline & Distributed Tracing Context
│           │   │   ├── Result: SUCCESS (HTTP 200 / gRPC OK)
│           │   │   │   ├── Action: Atomic UPDATE saga_step SET status='SUCCEEDED' + saga_instance version bump
│           │   │   │   └── Check: Are all steps in DAG completed?
│           │   │   │       ├── YES ──> Action: Execute Pivot Step Commit & Enqueue Completion Outbox Event
│           │   │   │       │   └── State: COMPLETED ──> [TERMINAL: Transaction Finalized]
│           │   │   │       └── NO ──> Action: Resolve next dependent DAG nodes ──> [Loop in State: RUNNING]
│           │   │   │
│           │   │   ├── Result: NON-RETRYABLE BUSINESS FAILURE (e.g. HTTP 422 Insufficient Balance)
│           │   │   │   └── Action: Halt forward execution, mark step FAILED, trigger compensation
│           │   │   │       └── State: COMPENSATING (Transition to Tier 3)
│           │   │   │
│           │   │   ├── Result: DOWNSTREAM RATE LIMIT (HTTP 429 Too Many Requests) [P1]
│           │   │   │   └── Action: Read Retry-After header, suspend worker, set delayed resume trigger
│           │   │   │       └── State: THROTTLED_WAIT ──> Timer Expiration ──> State: RUNNING
│           │   │   │
│           │   │   └── Result: INDETERMINATE TIMEOUT / NETWORK PARTITION (e.g. TCP Drop, HTTP 504) [P0]
│           │   │       └── Action: Mark step INDETERMINATE (DO NOT immediately retry side-effect)
│           │   │           └── State: RECONCILING
│           │   │               ├── Step Status Probe: Query downstream partner with Operation ID
│           │   │               │   ├── Probe Result: OPERATION_FOUND_SUCCESS ──> Mark Step SUCCEEDED ──> State: RUNNING
│           │   │               │   ├── Probe Result: OPERATION_NOT_FOUND ──> Safe to Retry with Backoff
│           │   │               │   │   ├── Check: Retry Budget Remaining?
│           │   │               │   │   │   ├── YES ──> Exponential Jittered Backoff ──> State: RUNNING
│           │   │               │   │   │   └── NO ──> Exhausted Budget ──> State: COMPENSATING
│           │   │               │   └── Probe Result: DOWNSTREAM_UNAVAILABLE
│           │   │               │       └── Action: Exponential backoff probing until probe budget expires
│           │   │               │           └── Budget Expired ──> State: STUCK (Operator Triage)
│           │   │
│           │   └── Step Type: Asynchronous Callback / Webhook (e.g. 3D-Secure SMS, External KYC) [P1]
│           │       └── Action: Suspend execution lease, write pending callback token to inbox
│           │           └── State: WAITING (Awaiting Webhook or Async Timer)
│           │               ├── Event: Valid Webhook Received (Signed + Tenant Verified)
│           │               │   ├── Check: Is webhook sequence > current step sequence?
│           │               │   │   ├── YES ──> Transition payload to step outcome
│           │               │   │   │   ├── Payload status == 'APPROVED' ──> Resume lease ──> State: RUNNING
│           │               │   │   │   └── Payload status == 'REJECTED' ──> Transition to COMPENSATING
│           │               │   │   └── NO (Stale/Duplicate Webhook) ──> Return 200 OK without state mutation
│           │               ├── Event: Timer Expiration (SLA Timeout Exceeded)
│           │               │   └── Action: Invalidate wait token, mark step EXPIRED ──> State: COMPENSATING
│           │               └── Event: Duplicate Out-of-Order Webhook
│           │                   └── Action: De-duplicate against Transactional Inbox ──> Discard duplicate
│           │
│           └── Step Type: Long-Running Resource Reservation (e.g. Flight/Hotel Hold) [P1]
│               └── Action: Register reservation heartbeat thread
│                   ├── Heartbeat fires before expiration: Extend reservation lease downstream
│                   └── Heartbeat fails / Reservation expires: Trigger automatic rollback to COMPENSATING
│
├── 3. COMPENSATION & ROLLBACK TIER [P0 / P1]
│   └── Real-World: Debit succeeded but inventory warehouse burned down; customer card must be refunded cleanly
│       └── State: COMPENSATING (Executing Compensating Actions in Strict Reverse Topological Order)
│           ├── Action: Fetch all SUCCEEDED steps from database in reverse execution sequence
│           │   ├── Loop: For each step, invoke registered Compensating Action (Semantic Undo)
│           │   │   ├── Pre-Flight: Transmit original stable Operation ID to guarantee idempotent undo
│           │   │   ├── Result: SUCCESS (e.g. Released Reserved Inventory / Refund Emitted)
│           │   │   │   └── Action: Mark step COMPENSATED, advance to next prior step
│           │   │   └── Result: RETRYABLE FAILURE (e.g. Downstream partner returns HTTP 500/503)
│           │   │       ├── Check: Compensation retry count < Max Allowed (default: 10 attempts)
│           │   │       │   └── Action: Full jitter backoff retry ──> [Loop in State: COMPENSATING]
│           │   │       └── Check: Compensation retry count >= Max Allowed
│           │   │           └── Action: Trip circuit breaker, halt automated recovery, alert SRE
│           │   │               └── State: STUCK (Quarantined, Awaiting Operator Manual Intervention)
│           │   │                   ├── Event: Operator resolves outage & issues Re-drive [P1]
│           │   │                   │   ├── Guard: Validate expected version == current version
│           │   │                   │   └── Action: Clear error counter, resume loop ──> State: COMPENSATING
│           │   │                   └── Event: Operator declares permanent financial write-off [P2]
│           │   │                       └── Action: Append manual audit trail & transition to terminal failure
│           │   │                           └── State: FAILED ──> [TERMINAL: Escalate to Accounting / GL]
│           │   │
│           │   └── Check: Have all compensable steps completed semantic undo?
│           │       └── YES ──> Action: Mark saga fully compensated & publish rollback outbox event
│           │           └── State: COMPENSATED ──> [TERMINAL: Clean Rollback Finalized]
│
└── 4. RECOVERY & SWEEP TIER (Background Worker Loop) [P1]
    └── Real-World: Kubernetes pod running orchestrator gets OOM-killed mid-execution; orphaned transaction recovered
        └── Trigger: Periodic recovery ticker (every 5000ms)
            └── Action: SELECT * FROM saga_instance WHERE state NOT IN (TERMINAL) AND lease_expires_at < NOW()
                ├── Lock: Obtain advisory lock with monotonic fencing token increment
                └── Check: Is transaction state RUNNING or COMPENSATING?
                    ├── YES ──> Reassign worker lease, increment fencing token, resume execution
                    └── NO (Stuck in WAITING/RECONCILING) ──> Re-check external probe endpoint
```

---

## 2. The 15 Invariants — Real-Life Rationale & Enforcement Trees [P0–P3]

```
INVARIANT ENFORCEMENT & REAL-LIFE BUSINESS LOGIC TREES
│
├── INVARIANT 1: ONE IDEMPOTENCY SCOPE MAPS TO ONE LOGICAL TRANSACTION [P0]
│   ├── Real-World Incident: High-latency network prompts buyer to click 'Submit Order' 4 times in 2 seconds
│   ├── Why Needed: Without single mapping, 4 distinct sagas spawn, billing customer 4x for 1 physical item
│   └── Enforcement Logic:
│       └── INGRESS: Check (tenant_id, idempotency_key)
│           ├── Exists in DB Ledger ──> Reject duplicate / return cached outcome
│           └── Not found ──> Atomic INSERT INTO idempotency_ledger WITH UNIQUE CONSTRAINT ──> Proceed
│
├── INVARIANT 2: ONE STEP CANNOT HAVE TWO VALID CONCURRENT EXECUTION OWNERS [P0]
│   ├── Real-World Incident: Worker A pauses for 30s during major Java Full-GC; Cluster assumes dead and assigns Worker B
│   ├── Why Needed: Worker A wakes up and executes simultaneously with Worker B, issuing double debit to partner bank
│   └── Enforcement Logic:
│       └── DISPATCH: Worker acquires task with monotonic fencing_token (e.g. token = 42)
│           └── Worker commits result:
│               ├── DB current_fencing_token == 42 ──> Commit allowed (token remains valid)
│               └── DB current_fencing_token > 42 (Worker B took over) ──> Hard Reject commit; Worker A aborts
│
├── INVARIANT 3: EVERY EXTERNAL OPERATION HAS A STABLE OPERATION ID [P0]
│   ├── Real-World Incident: Payment gateway times out after 10s; engine retries with new random UUID
│   ├── Why Needed: Payment gateway treats second call as new transaction and charges credit card twice
│   └── Enforcement Logic:
│       └── PRE-FLIGHT: Compute stable ID = SHA256(tenant_id + saga_id + step_id + attempt_no)
│           └── Inject into HTTP/gRPC header: `X-Operation-ID: <stable_hash>`
│               └── Downstream uses header to deduplicate across network blips
│
├── INVARIANT 4: EVERY RETRY IS SAFE AGAINST DUPLICATE SIDE EFFECTS [P0]
│   ├── Real-World Incident: Airline seat booking call drops TCP during ACK packet; seat was allocated
│   ├── Why Needed: Retrying blindly attempts to book a second seat, failing or causing double billing
│   └── Enforcement Logic:
│       └── RETRY DECISION: Network Error Encountered
│           ├── Action: Query downstream partner status with stable Operation ID first
│           ├── Status == FOUND ──> Mark completed; DO NOT re-invoke side effect
│           └── Status == NOT_FOUND ──> Safe to retry with same Operation ID
│
├── INVARIANT 5: EVERY STATE TRANSITION HAS A DURABLE AUDIT RECORD [P0]
│   ├── Real-World Incident: Financial regulator demands exact timeline of why a $2M settlement rolled back
│   ├── Why Needed: In-memory logging lost on container restart; unable to prove compliance or debug financial discrepancies
│   └── Enforcement Logic:
│       └── TRANSITION: Before memory state changes
│           └── Atomic local DB transaction:
│               ├── UPDATE saga_instance SET state = :next_state
│               └── INSERT INTO state_transition_audit (saga_id, from_state, to_state, timestamp, actor)
│
├── INVARIANT 6: EVERY STATE TRANSITION USES OPTIMISTIC CONCURRENCY [P0]
│   ├── Real-World Incident: Orchestrator sweeper and webhook worker update saga state in the exact same millisecond
│   ├── Why Needed: Blind updates overwrite each other, causing lost updates and corrupted saga status
│   └── Enforcement Logic:
│       └── CAS UPDATE:
│           └── `UPDATE saga_instance SET state=:next, version=version+1 WHERE id=:id AND version=:expected_version`
│               ├── Rows affected == 1 ──> Success; proceed
│               └── Rows affected == 0 ──> Stale state detected; abort and reload
│
├── INVARIANT 7: NO FORWARD EXECUTION PROCEEDS AFTER COMPENSATION BEGINS [P1]
│   ├── Real-World Incident: Flight reservation fails; rollback starts to refund hotel, but car-rental step finishes delayed
│   ├── Why Needed: Forward steps execute while rollback is active, leaving orphaned resources reserved forever
│   └── Enforcement Logic:
│       └── FORWARD DISPATCH CHECK:
│           ├── State == 'COMPENSATING' OR State == 'STUCK' ──> HARD BLOCK; reject forward step scheduling
│           └── State == 'RUNNING' ──> Schedule forward step
│
├── INVARIANT 8: UNKNOWN OUTCOMES MUST BE RECONCILED BEFORE UNSAFE RETRIES [P0]
│   ├── Real-World Incident: HTTP 504 Gateway Timeout while issuing $10,000 wire transfer
│   ├── Why Needed: Retrying immediately without verifying state results in duplicate $10,000 wires
│   └── Enforcement Logic:
│       └── TIMEOUT ENCOUNTERED:
│           └── Transition step to `INDETERMINATE` ──> Enter `RECONCILING`
│               └── Polling probe queries partner status until definitively resolved (Found vs Not Found)
│
├── INVARIANT 9: COMPENSATION MUST BE INDEPENDENTLY IDEMPOTENT [P1]
│   ├── Real-World Incident: Refund API returns 500 on first try but actually refunded; compensation retried
│   ├── Why Needed: Non-idempotent refund fails on retry or double-refunds customer balance
│   └── Enforcement Logic:
│       └── COMPENSATE CALL: Send stable compensation key
│           ├── Partner returns: 'ALREADY_REFUNDED' / 200 OK ──> Treated as SUCCESS
│           └── Partner refunds balance ──> Treated as SUCCESS
│
├── INVARIANT 10: TERMINAL STATES CANNOT BE SILENTLY OVERWRITTEN [P0]
│   ├── Real-World Incident: Late webhook arrives 10 minutes after saga was CANCELLED by user
│   ├── Why Needed: Webhook marks saga COMPLETED, reviving dead transaction and shipping goods for free
│   └── Enforcement Logic:
│       └── CAS GUARD:
│           ├── Current state IN ('COMPLETED', 'CANCELLED', 'COMPENSATED', 'FAILED')
│           └── Reject ALL incoming state transition events unconditionally
│
├── INVARIANT 11: DATABASE STATE AND OUTBOX CREATION ARE ATOMIC [P0]
│   ├── Real-World Incident: Saga marks COMPLETED in DB, but Kafka producer crashes before event published
│   ├── Why Needed: Downstream notification service never fires; customer never receives order confirmation email
│   └── Enforcement Logic:
│       └── SINGLE DB TRANSACTION:
│           └── BEGIN;
│               ├── UPDATE saga_instance SET state = 'COMPLETED';
│               ├── INSERT INTO transactional_outbox (event_type, payload);
│               └── COMMIT;
│
├── INVARIANT 12: DUPLICATE EVENTS CANNOT CAUSE DUPLICATE BUSINESS EFFECTS [P1]
│   ├── Real-World Incident: Kafka rebalances consumer group; 3D-Secure payment authorization message processed twice
│   ├── Why Needed: Order gets approved twice, dispatching two shipping labels
│   └── Enforcement Logic:
│       └── TRANSACTIONAL INBOX:
│           └── INSERT INTO transactional_inbox (tenant_id, event_id) VALUES (:tenant, :event_id)
│               ├── Duplicate key error ──> ACK and discard event immediately
│               └── Success ──> Execute event business logic
│
├── INVARIANT 13: EXPIRED LEASES CANNOT AUTHORIZE STALE WORKERS [P1]
│   ├── Real-World Incident: Worker network cable disconnected for 2 minutes, lease expires; cable reconnected
│   ├── Why Needed: Stale worker flushes stale state to DB, overwriting work done by replacement worker
│   └── Enforcement Logic:
│       └── DB WRITE GUARD:
│           └── Include `WHERE lease_expires_at >= NOW() AND fencing_token = :worker_token`
│               └── If lease expired ──> 0 rows affected; throw StaleWorkerLeaseException
│
├── INVARIANT 14: WORKFLOW VERSIONS ARE IMMUTABLE FOR EXISTING TRANSACTIONS [P3]
│   ├── Real-World Incident: DevOps deploys Workflow v2 with Step 3 removed; running v1 transactions crash at Step 3
│   ├── Why Needed: Changing running DAG definitions corrupts in-flight execution pointers
│   └── Enforcement Logic:
│       └── SAGA INITIALIZATION:
│           └── Pin `workflow_definition_version = 'v1.4'` in saga row
│               └── Worker executes DAG strictly using pinned version snapshot
│
└── INVARIANT 15: EVERY UNRESOLVED TRANSACTION IS DISCOVERABLE BY RECOVERY WORKERS [P1]
    ├── Real-World Incident: Orchestrator node suffers hardware kernel panic; 500 active sagas freeze in limbo
    ├── Why Needed: Customers charged but orders never complete; zero alerts triggered because process died
    └── Enforcement Logic:
        └── RECOVERY SWEEPER:
            └── Indexed query runs every 5 seconds:
                └── `SELECT * FROM saga_instance WHERE state NOT IN (TERMINAL) AND lease_expires_at < NOW()`
                    └── Unassigned/frozen transactions claimed and resumed automatically
```

---

## 3. All 25 Edge Cases — Real-Life Scenario & State Machine Decision Trees

```
25 EDGE CASES REAL-LIFE DECISION TREES
│
├── CASE 1: CLOCK SKEW BETWEEN WORKERS [P0]
│   ├── Real-World Incident: Worker A clock is 15 seconds behind Worker B due to broken NTP daemon
│   ├── Why Needed: Worker A incorrectly thinks its lease is valid and overwrites Worker B
│   └── Decision Logic:
│       └── Worker evaluates lease validity:
│           ├── Local Clock Check ──> BANNED; workers never evaluate timestamps locally
│           └── DB Evaluation ──> Lease comparison evaluated exclusively on DB server: `NOW() AT TIME ZONE 'UTC'`
│
├── CASE 2: DATABASE FAILOVER DURING STATE TRANSITION [P0]
│   ├── Real-World Incident: AWS RDS Multi-AZ failover triggers mid-commit of step transition
│   ├── Why Needed: Connection drops; orchestrator doesn't know if state committed or rolled back
│   └── Decision Logic:
│       └── Connection dropped with SQLSTATE 08006 / 57P01:
│           ├── Orchestrator reconnects to new Primary replica
│           └── Queries state by idempotency_key:
│               ├── Record committed ──> Continue forward execution
│               └── Record missing ──> Safely replay CAS transition
│
├── CASE 3: REDIS EVICTION OF IDEMPOTENCY ENTRY [P0]
│   ├── Real-World Incident: Flash sale floods Redis; maxmemory-eviction policy evicts active idempotency keys
│   ├── Why Needed: Subsequent duplicate requests bypass Redis cache and re-execute financial transactions
│   └── Decision Logic:
│       └── Check Idempotency:
│           ├── Redis Cache Miss ──> DO NOT ASSUME NEW TRANSACTION
│           └── Fallback to PostgreSQL: `SELECT status FROM idempotency_keys WHERE key = :key`
│               ├── Found in DB ──> Restore Redis cache, return existing status
│               └── Not in DB ──> Safe to initialize new transaction
│
├── CASE 4: POSTGRESQL DEADLOCK DURING CONCURRENT UPDATES [P0]
│   ├── Real-World Incident: Sagas X and Y update Inventory Rows A and B in opposing sequence simultaneously
│   ├── Why Needed: PostgreSQL terminates one transaction with 40P01 Deadlock Detected
│   └── Decision Logic:
│       └── SQLSTATE 40P01 Caught:
│           ├── Prevention: Engine sorts resource locks lexicographically before locking
│           └── Mitigation: Roll back sub-transaction, apply full random jitter (50ms - 300ms), and retry CAS
│
├── CASE 5: WORKFLOW DEFINITION CHANGES MID-EXECUTION [P1]
│   ├── Real-World Incident: Engineering releases v2 of onboarding saga renaming 'verify_phone' to 'verify_mfa'
│   ├── Why Needed: In-flight sagas waiting on phone webhook fail to find step in code definition and halt
│   └── Decision Logic:
│       └── Saga Execution:
│           └── Read `saga.workflow_definition_version`
│               ├── Version == 'v1' ──> Route to v1 DAG Handler
│               └── Version == 'v2' ──> Route to v2 DAG Handler
│
├── CASE 6: RETRY STORM DURING DOWNSTREAM OUTAGE [P0]
│   ├── Real-World Incident: Card processor suffers 30s outage; 5,000 workers retry synchronously every 1s
│   ├── Why Needed: Downstream processor gets completely DOS'd and cannot recover
│   └── Decision Logic:
│       └── Downstream returns 500 / Timeout:
│           ├── Check circuit breaker for downstream:
│           │   ├── OPEN ──> Fast-fail immediately without network call ──> State: COMPENSATING
│           │   └── CLOSED ──> Compute backoff = MIN(max_backoff, base * 2^attempt) * rand(0, 1)
│           └── Add to jittered retry queue
│
├── CASE 7: RETRY BUDGET EXHAUSTED GLOBALLY [P0]
│   ├── Real-World Incident: System-wide network disruption consumes 100,000 retries across 10,000 tenants
│   ├── Why Needed: Exhausts database connection pools and worker CPU threads
│   └── Decision Logic:
│       └── Retry Request:
│           ├── Check Global Cluster Token Bucket:
│           │   ├── Tokens Available ──> Decrement bucket ──> Dispatch retry
│           │   └── Bucket Empty ──> Shed load; bypass retry ──> Transition directly to COMPENSATING
│
├── CASE 8: DOWNSTREAM RATE LIMIT (HTTP 429) [P1]
│   ├── Real-World Incident: Twilio SMS returns HTTP 429 'Too Many Requests' with 'Retry-After: 45'
│   ├── Why Needed: Worker thread holding connection and retrying burns compute and gets IP banned
│   └── Decision Logic:
│       └── HTTP 429 Intercepted:
│           ├── Parse `Retry-After` header value (e.g. 45s)
│           ├── Transition step to `THROTTLED_WAIT`
│           ├── Release worker CPU execution lease
│           └── Set wake-up timer: `NOW() + INTERVAL '45 seconds'`
│
├── CASE 9: LARGE SAGA WITH THOUSANDS OF STEPS [P2]
│   ├── Real-World Incident: Bulk payroll transaction processing 5,000 employee salary steps in one saga
│   ├── Why Needed: Saga row state payload exceeds PostgreSQL 1GB TOAST limit and worker crashes on OOM
│   └── Decision Logic:
│       └── Saga DAG Evaluation:
│           ├── Steps > 100 ──> Enable chunked execution mode
│           ├── Step outputs written to individual `saga_step` rows (not one giant JSON blob)
│           └── Payloads > 100KB offloaded to S3 object storage; only hash/URI stored in DB
│
├── CASE 10: LONG-RUNNING TRANSACTION EXCEEDING RETENTION [P2]
│   ├── Real-World Incident: Real estate escrow saga remains in WAITING state for 90 days awaiting title deed
│   ├── Why Needed: Operational DB bloats with millions of ancient pending rows, degrading query performance
│   └── Decision Logic:
│       └── Transaction Age > 30 Days:
│           ├── Move heavy audit payload to cold archive storage (Parquet / S3 Glacier)
│           └── Retain lightweight transaction stub in operational DB for callback routing
│
├── CASE 11: TENANT ISOLATION VIOLATION [P0]
│   ├── Real-World Incident: Multi-tenant SaaS orchestrator bug queries `WHERE saga_id = :id` without `tenant_id`
│   ├── Why Needed: Tenant A views, cancels, or mutates Tenant B's confidential payment workflow
│   └── Decision Logic:
│       └── Every Query Execution:
│           ├── Enforce PostgreSQL Row-Level Security: `SET LOCAL app.current_tenant_id = :tenant_id`
│           └── Every SQL statement includes mandatory compound key: `WHERE tenant_id = :t AND id = :id`
│
├── CASE 12: AUTHORIZATION REVOKED DURING EXECUTION [P1]
│   ├── Real-World Incident: Corporate admin revokes API credentials of employee mid-way through 5-step wire
│   ├── Why Needed: Remaining steps execute unauthorized transactions after security revocation
│   └── Decision Logic:
│       └── Step Boundary:
│           ├── Check Revocation Cache (Redis bitset / JWT revocation list)
│           │   ├── Revoked ──> Halt forward DAG; transition immediately to COMPENSATING
│           │   └── Valid ──> Continue forward execution
│
├── CASE 13: DUPLICATE OPERATOR RE-DRIVE [P1]
│   ├── Real-World Incident: Frustrated SRE double-clicks 'Re-drive Stuck Saga' button in internal admin dashboard
│   ├── Why Needed: Two concurrent recovery routines dispatch duplicate compensation or execution steps
│   └── Decision Logic:
│       └── Operator Action Received:
│           ├── Check: `re_drive_idempotency_token`
│           │   ├── Exists in cache ──> Reject HTTP 409 Conflict
│           │   └── New ──> Proceed to optimistic state check (Case 14)
│
├── CASE 14: OPERATOR RE-DRIVE WITH STALE STATE [P1]
│   ├── Real-World Incident: Operator clicks 'Retry Step' on screen opened 20 minutes ago; worker already recovered it
│   ├── Why Needed: Operator command overwrites newly completed state back to running
│   └── Decision Logic:
│       └── Re-drive Execution:
│           └── Execute CAS: `UPDATE saga_instance SET state='RUNNING' WHERE id=:id AND version=:ui_version`
│               ├── Rows affected == 0 ──> [ABORT: HTTP 409 Conflict — Saga state has mutated; please refresh]
│               └── Rows affected == 1 ──> Success; dispatch worker
│
├── CASE 15: POISON-PILL WORKFLOW DEFINITION [P2]
│   ├── Real-World Incident: Developer deploys DAG with infinite circular dependency (Step A -> Step B -> Step A)
│   ├── Why Needed: Workers loop forever, consuming all queue capacity and CPU across the cluster
│   └── Decision Logic:
│       └── Workflow Registration:
│           ├── Run Tarjan's Cycle Detection Algorithm on DAG
│           │   ├── Cycle Detected ──> REJECT deployment with schema validation error
│           │   └── Acyclic (Valid DAG) ──> Approve workflow definition deployment
│
├── CASE 16: MALFORMED OR OVERSIZED WEBHOOK [P1]
│   ├── Real-World Incident: Third-party webhook partner has bug and sends 50MB malformed XML instead of JSON
│   ├── Why Needed: Ingress worker parser crashes with Out-Of-Memory error, bringing down API gateway
│   └── Decision Logic:
│       └── Webhook Ingress Gateway:
│           ├── Check Content-Length > 1MB ──> Reject HTTP 413 Payload Too Large
│           ├── JSON Schema Validation Fails ──> Route to Dead-Letter Queue (DLQ); return HTTP 400
│           └── Valid ──> Push to Transactional Inbox
│
├── CASE 17: PROVIDER CHANGES API BEHAVIOR [P2]
│   ├── Real-World Incident: Downstream KYC provider changes response from `{"status": "OK"}` to `{"approved": true}`
│   ├── Why Needed: Unchecked JSON parser defaults missing field to null/false, falsely failing transactions
│   └── Decision Logic:
│       └── Response Ingestion:
│           ├── Strict Schema Validation (Pydantic / Zod)
│           │   ├── Schema Validation Error ──> Transition step to `INDETERMINATE` (NOT failed)
│           │   └── Notify Engineering: API contract drift detected
│
├── CASE 18: COMPENSATION SERVICE UNAVAILABLE INDEFINITELY [P1]
│   ├── Real-World Incident: Downstream ledger service database corrupted; rollback refund endpoint returns 503 for 6 hours
│   ├── Why Needed: Rollback loop retries endlessly, clogging worker queues and hiding broken state
│   └── Decision Logic:
│       └── Compensation Failure Loop:
│           ├── Retry Count >= Max Allowed (10 attempts)
│           └── Transition State to `STUCK`
│               ├── Halt automated retry thread
│               ├── Set `operator_lock = FALSE`
│               └── Trigger P1 PagerDuty incident to On-Call SRE team
│
├── CASE 19: OUTBOX BACKLOG [P1]
│   ├── Real-World Incident: Kafka broker slow; outbox table accumulates 500,000 unpublished events
│   ├── Why Needed: Database disk fills up; downstream systems experience hours of message delay
│   └── Decision Logic:
│       └── Outbox Dispatcher:
│           ├── Sharded poller workers partitioned by `hash(saga_id) % num_workers`
│           └── Polling query: `SELECT * FROM transactional_outbox WHERE status='PENDING' FOR UPDATE SKIP LOCKED LIMIT 100`
│               └── Enables parallel publication without lock contention
│
├── CASE 20: EVENT ORDERING VIOLATION [P0]
│   ├── Real-World Incident: Network routes Webhook 2 ('ORDER_COMPLETED') faster than Webhook 1 ('PAYMENT_CONFIRMED')
│   ├── Why Needed: Saga attempts to complete before payment is marked received, crashing state machine
│   └── Decision Logic:
│       └── Event Arrival:
│           ├── Check: `event.sequence_number == step.expected_sequence_number`
│           │   ├── MATCH ──> Apply event payload to state machine
│           │   └── FUTURE (Out-of-Order) ──> Store in Inbox Buffer; start 60s timer
│           └── Predecessor arrives ──> Apply predecessor, then dequeue and apply buffered event
│
├── CASE 21: DELAYED SUCCESS AFTER TRANSACTION TIMEOUT [P0]
│   ├── Real-World Incident: Payment API times out after 30s; orchestrator starts rollback; payment succeeds at 32s
│   ├── Why Needed: Customer gets refunded while their credit card was actually charged downstream
│   └── Decision Logic:
│       └── Timeout Occurs:
│           ├── Step status = `INDETERMINATE`
│           └── Mandatory Status Reconciliation Query sent to provider:
│               ├── Provider shows SUCCESS ──> Convert step to SUCCEEDED; continue forward
│               └── Provider shows NOT FOUND / CANCELLED ──> Safe to proceed with rollback
│
├── CASE 22: PARTIAL DATABASE TRANSACTION COMMIT [P0]
│   ├── Real-World Incident: App code updates `saga_step` in DB 1, then crashes before updating `saga_instance` in DB 2
│   ├── Why Needed: Step shows SUCCEEDED but parent saga still shows CREATED; state corrupted permanently
│   └── Decision Logic:
│       └── Single PostgreSQL ACID Boundary:
│           └── `BEGIN;`
│               ├── `UPDATE saga_instance SET state = :next;`
│               ├── `UPDATE saga_step SET status = :status;`
│               ├── `INSERT INTO transactional_outbox ...;`
│               └── `COMMIT;` (All succeed or all roll back together)
│
├── CASE 23: EXTERNAL OPERATION SUCCEEDS AFTER CANCELLATION [P1]
│   ├── Real-World Incident: User cancels order while card charge in flight; cancellation completes, but charge settles 1s later
│   ├── Why Needed: Customer cancelled order but their bank account was charged with zero product delivered
│   └── Decision Logic:
│       └── Client issues Cancel:
│           ├── Check in-flight external operations:
│           │   ├── Query partner status: Did in-flight operation execute?
│           │   │   ├── YES ──> Transition to COMPENSATING to undo settled charge
│           │   │   └── NO ──> Invalidate client token; transition cleanly to CANCELLED
│
├── CASE 24: RESOURCE RESERVATION EXPIRES BEFORE PAYMENT [P1]
│   ├── Real-World Incident: Concert ticket held for 10 minutes; 3D-Secure SMS takes 11 minutes; payment succeeds
│   ├── Why Needed: Payment is captured for a seat that was released and sold to another customer
│   └── Decision Logic:
│       └── Reservation Heartbeat Protocol:
│           ├── Active payment step sends periodic heartbeat extending reservation TTL
│           └── If reservation TTL expires before payment capture:
│               └── Payment capture MUST abort; refund issued immediately via COMPENSATING
│
└── CASE 25: MANUAL INTERVENTION CONFLICTS WITH AUTOMATED RECOVERY [P1]
    ├── Real-World Incident: SRE fixing stuck transaction in terminal while automated sweeper simultaneously re-drives it
    ├── Why Needed: SRE manual SQL updates clobbered by background automated worker thread
    └── Decision Logic:
        └── SRE Initiates Triage:
            ├── Set `UPDATE saga_instance SET operator_lock = TRUE WHERE id = :id`
            ├── Background recovery sweepers query: `WHERE lease_expires_at < NOW() AND operator_lock = FALSE`
            │   └── Sweeper skips locked transaction completely
            └── SRE finishes triage ──> Releases `operator_lock = FALSE`
```

---

## 4. Phase-Gated Engineering Delivery Roadmap Tree (P0–P3)

```
PHASE-GATED IMPLEMENTATION ROADMAP TREE
│
├── [P0] FOUNDATIONAL SAFETY, CONCURRENCY & RECONCILIATION
│   ├── Invariants Enforced: #1, #2, #3, #4, #5, #6, #8, #10, #11
│   ├── Edge Cases Covered: #1, #2, #3, #4, #6, #7, #11, #20, #21, #22
│   ├── Real-World Focus: Eliminate phantom debits, double charges, split-brain workers, and DB deadlocks
│   └── Architecture Components:
│       ├── PostgreSQL unique constraints & Redis fast-path idempotency ledger
│       ├── Monotonic fencing tokens with DB-managed lease expiry
│       ├── Optimistic concurrency control (CAS version checks on all transitions)
│       ├── Out-of-band downstream reconciliation probe loop (`RECONCILING` state)
│       ├── Transactional Outbox table committed atomically with domain state
│       ├── Row-Level Security (RLS) tenant isolation policies
│       └── Lexicographical lock acquisition ordering (deadlock prevention)
│
├── [P1] FAULT RECOVERY, INBOX ORCHESTRATION & OPERATOR CONTROL
│   ├── Invariants Enforced: #7, #9, #12, #13, #15
│   ├── Edge Cases Covered: #5, #8, #12, #13, #14, #16, #18, #19, #23, #24, #25
│   ├── Real-World Focus: Automated crash recovery, re-drive tooling, out-of-order webhook buffering, and SRE triage
│   └── Architecture Components:
│       ├── Background recovery sweeper thread discovering expired leases (`NOW() > lease_expires_at`)
│       ├── Idempotent reverse compensation engine with circuit breaker trip to `STUCK`
│       ├── Transactional Inbox pattern with monotonic sequence buffering
│       ├── Downstream rate-limiting throttle (`HTTP 429 Retry-After` suspension)
│       ├── Operator control plane (guarded re-drive CAS, `operator_lock` isolation)
│       └── Resource reservation heartbeat leases
│
├── [P2] OPERATIONAL HARDENING, PAYLOAD OFF-LOAD & SCALE
│   ├── Edge Cases Covered: #9, #10, #15, #17
│   ├── Real-World Focus: High throughput scaling, multi-megabyte payloads, poison pill defense, and cold archival
│   └── Architecture Components:
│       ├── Outbox bulk publisher with `FOR UPDATE SKIP LOCKED`
│       ├── Large saga state pagination & offloading oversized payloads (>100KB) to S3
│       ├── Partitioned cold-data archival pipeline for completed sagas
│       ├── Poison-pill AST workflow definition validator and DAG cycle detection
│       └── Strict Pydantic/Zod contract drift response parsers
│
└── [P3] FORMAL VERIFICATION & EVOLUTION
    ├── Invariant Enforced: #14
    ├── Real-World Focus: Mathematical proofs of linearizability and zero-deadlock state space under chaos failure
    └── Architecture Components:
        ├── Immutable workflow definition snapshot pinning per transaction
        ├── Formal TLA+ / TLC state machine specification and model checking
        └── Cross-region multi-primary failover verification drills
```

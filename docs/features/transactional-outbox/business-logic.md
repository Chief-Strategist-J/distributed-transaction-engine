# Transactional Outbox — Business Logic Decision Tree

```
TRANSACTIONAL OUTBOX (SAGA ORCHESTRATOR) — BUSINESS LOGIC DECISION TREE (v2)
Legend: [C] Certain   [L] Likely (sound default)   [G] Guessing (owner must decide)
│
├── 0. RELAY MODE DECISION  (one mode per outbox table — never both)
│   ├── Check: Log-based CDC available and operated (logical replication, connector)?
│   │   ├── YES → MODE A: CDC relay (commit order from WAL; no status column; insert + delete pattern) [recommended]
│   │   └── NO  → MODE B: polling relay with lease claims (this tree §5)
│   └── E0.1 CDC connector reading the table while a poller also publishes it — every event twice, unpredictable order [C]
│
├── 1. INVARIANTS
│   ├── OX-01 Saga state change and its outbox rows commit in ONE transaction [C]
│   ├── OX-02 Every outbox row carries a gapless per-saga sequence assigned under the saga row lock [C]
│   ├── OX-03 Rows of the same partition key are published strictly in sequence order, one in flight at a time [C]
│   ├── OX-04 A failed row blocks only its own key; later rows of that key never overtake it [C]
│   ├── OX-05 No database transaction is open while sending to Kafka [C]
│   ├── OX-06 A row is marked published only after broker acknowledgement, and only by the lease owner [C]
│   ├── OX-07 Payload is serialized, validated, and size-checked BEFORE the transaction commits [C]
│   ├── OX-08 event_id is created in the transaction and never regenerated [C]
│   ├── OX-09 Non-retriable publish errors park the key and alert; they never loop forever [C]
│   └── OX-10 Pending rows are never deleted by cleanup [C]
│
├── 2. OUTBOX RECORD  (fields, in words)
│   ├── event_id (unique, created in transaction)
│   ├── destination topic (saga lifecycle events topic, or a step handler's command topic)
│   ├── partition key (saga id for lifecycle events; declared per destination for commands)
│   ├── aggregate type + aggregate id (saga id) + aggregate sequence (gapless per saga, OX-02)
│   ├── event/command type, schema reference, payload bytes (already serialized)
│   ├── headers captured at insert: tenant, correlation id, causation id (transition / triggering reply),
│   │   trace context from the orchestrator span, idempotency key (commands)
│   ├── created at (DB time; informational only — never used for ordering)
│   └── MODE B only: status (PENDING | IN_FLIGHT | PUBLISHED | PARKED), lease owner, lease expiry,
│       attempt count, last error class, published at
│
├── 3. ATOMIC WRITE  (orchestrator transition)
│   ├── Step 1 Begin transaction
│   ├── Step 2 Lock saga row; check expected state + owner epoch (conditional transition)
│   │   └── 0 rows → another instance owns/advanced the saga → rollback, re-read
│   ├── Step 3 Apply saga state change (+ step rows, timers)
│   ├── Step 4 For each message the transition emits:
│   │   ├── Assign sequence = saga's next sequence (increment on saga row) — OX-02
│   │   ├── Serialize with registered schema; validate envelope; check size ≤ limit (OX-07)
│   │   └── Insert outbox row (event_id generated now — OX-08)
│   ├── Step 5 Commit
│   │   ├── Success → OUTBOX_COMMITTED
│   │   ├── Explicit failure → nothing persisted → caller may retry the transition
│   │   └── Ambiguous (connection lost at commit) → re-read saga state:
│   │       ├── Transition present → committed (do not repeat)
│   │       └── Transition absent → not committed (retry)
│   └── Edge cases
│       ├── E3.1 Sequence from a database sequence object — gaps on rollback — consumers see false gaps [C]
│       ├── E3.2 Serialization/size check at relay instead of here — state committed, event unpublishable — saga stuck [C]
│       ├── E3.3 One transition emits many rows (parallel fork of 200 branch commands) — large transaction; relay must
│       │         publish in sequence per key, other keys in parallel [C]
│       ├── E3.4 Saga start retried by client after ambiguous error — second saga — duplicate effects [C]
│       │   └── Handling: start is idempotent by (saga type, business key, client idempotency key)
│       └── E3.5 Outbox insert done in a separate transaction or after commit — dual write — lost or phantom events [C]
│
├── 4. MODE A — CDC RELAY  (feature-specific rules)
│   ├── Insert outbox row and delete it in the same transaction (WAL keeps the insert; table stays small)
│   ├── Ordering: WAL commit order; per saga, commits are serialized by the saga row lock → sequence order preserved
│   ├── Routing: destination topic + partition key taken from row fields
│   ├── No status column, no confirmation updates
│   └── Edge cases
│       ├── E4.1 Status updates on rows under CDC — unexpected update events — confusion/duplicates [C]
│       ├── E4.2 Connector cannot convert a row (bad payload) — connector stops — all sagas stall [C]
│       │   └── Handling: OX-07 prevents bad rows; connector failure is an incident (no error tolerance)
│       └── E4.3 Connector slot lost — events in gap never published — sagas wait on replies that never come [C]
│           └── Handling: durable audit of event ids per saga; re-emit gap from saga store (same event_id)
│
├── 5. MODE B — POLLING RELAY
│   │
│   ├── 5.1 Relay topology (OX-03)
│   │   ├── Relay workers own disjoint key shards: shard = hash(partition key) mod S
│   │   ├── Each shard owned by exactly one worker at a time (lease/epoch on shard)
│   │   └── Throughput scales with shards, ordering preserved per key
│   │
│   ├── 5.2 Selection (per owned shard)
│   │   ├── For each key in the shard: the lowest-sequence PENDING row, only if that key has no IN_FLIGHT row
│   │   │   and no PARKED row (OX-03, OX-04)
│   │   ├── Order by sequence per key; fair round-robin across keys/tenants (§9)
│   │   ├── Batch limit per poll [L: 100–500 rows]
│   │   └── Index supports "pending rows by shard, key, sequence" (partial index on PENDING)
│   │
│   ├── 5.3 Claim (short transaction A)
│   │   ├── Set status IN_FLIGHT, lease owner = worker, lease expiry = now + lease [L: 30 s]
│   │   │   WHERE status = PENDING (conditional)
│   │   ├── Commit immediately (OX-05)
│   │   └── E5.3a Lock held during sends — long transactions, vacuum blocked, table bloat — DEGRADE→OUTAGE [C]
│   │
│   ├── 5.4 Send (no transaction open)
│   │   ├── Produce with idempotent producer, acknowledgement from all in-sync replicas
│   │   ├── Per key: send next row only after previous row of that key is acknowledged
│   │   ├── Different keys may be in flight concurrently
│   │   └── Headers copied from row (never generated from relay's own context)
│   │
│   ├── 5.5 Confirm (short transaction B, per acknowledged row)
│   │   ├── Set PUBLISHED, published at — WHERE event_id = row AND lease owner = me AND status = IN_FLIGHT (OX-06)
│   │   ├── 0 rows → lease lost (another worker reclaimed) → stop sending this key; duplicates possible (acceptable)
│   │   └── Next row of the key becomes eligible
│   │
│   ├── 5.6 Failure handling per row (OX-04, OX-09)
│   │   ├── Retriable (broker unavailable, timeout, not enough replicas, leader not available)
│   │   │   ├── Release: status back to PENDING (or keep IN_FLIGHT until lease expiry), attempt + 1, backoff
│   │   │   ├── STOP this key: later rows of the same key are not sent this round
│   │   │   └── Producer instance reset on sequence/epoch errors before next send [L]
│   │   ├── Non-retriable (record too large, topic authorization failure, unknown topic, invalid record)
│   │   │   ├── Status PARKED with error class; key blocked (no later rows for this saga published)
│   │   │   └── Alert owner; saga marked as blocked by publication failure (visible on saga)
│   │   └── Unknown outcome (timeout after send) → treat as retriable; resend later (duplicate possible, same event_id)
│   │
│   ├── 5.7 Lease expiry & reclaim
│   │   ├── Reclaim IN_FLIGHT rows with expired lease → PENDING (same shard owner only)
│   │   ├── Lease ≫ worst-case send time to avoid reclaiming rows still being sent
│   │   └── E5.7a Reclaim while original worker still sending — two sends, possibly reordered with next row — CORRUPT order [C]
│   │       └── Handling: shard ownership with epoch; reclaim only by current shard owner; confirm fenced by owner
│   │
│   ├── 5.8 Wake-up & latency
│   │   ├── Commit-time notification (database notify) wakes relay immediately; polling interval as fallback [L]
│   │   └── E5.8a 50 ms tight polling on a large table without proper index — constant scans — DEGRADE [C]
│   │
│   └── 5.9 Crash points
│       ├── Crash after claim, before send → lease expires → reclaimed → sent (no loss)
│       ├── Crash after send, before confirm → lease expires → resent → duplicate (same event_id; consumers dedupe)
│       ├── Crash during confirm → same as above
│       └── DB unavailable at confirm → stop all sends (cannot confirm) → resume when DB back
│
├── 6. KAFKA DISPATCH RULES
│   ├── Producer: idempotence on; acks from all in-sync replicas; bounded in-flight requests (≤ 5 with idempotence)
│   ├── Destination topics pre-created with durable settings; relay never auto-creates topics
│   ├── Partition key bytes identical across all producers of that topic (same serializer and partitioner)
│   └── Edge cases
│       ├── E6.1 Relay continues to next row of same key after a failed row — reorder — CORRUPT [C]
│       ├── E6.2 Topic auto-created with default partitions/replication — ordering/durability broken — LOSS risk [C]
│       └── E6.3 Wrong cluster/environment configured — events published where nobody consumes — sagas stall [C]
│
├── 7. DUPLICATES — WHAT CONSUMERS MUST ASSUME
│   ├── At-least-once publication: same event_id may appear more than once, always in sequence order per key
│   ├── Consumers dedupe by (handler, event_id) and check per-saga sequence (gapless)
│   └── E7.1 Consumers assume exactly-once from outbox — double execution of step commands — CORRUPT [C]
│
├── 8. SAGA-SPECIFIC DESTINATIONS
│   ├── Saga lifecycle events (created, step succeeded, completed, compensated) → saga events topic, key = saga id
│   ├── Step commands → the step handler's command topic, key = saga id (or target aggregate id if the handler needs
│   │   per-resource ordering) [G: per handler contract]
│   ├── Compensation commands → same command topic as forward commands for that handler (ordering with forward command
│   │   of the same saga guaranteed by same key)
│   └── Edge cases
│       ├── E8.1 Commands and lifecycle events on one topic — every subscriber sees commands; wrong consumers act [C]
│       ├── E8.2 Forward command and compensation keyed differently — compensation can overtake forward command [C]
│       └── E8.3 Parallel branch commands keyed by saga id on one partition — hot partition for huge fan-outs [L]
│           └── Handling: key by branch target when branches are independent and ordering across them not required
│
├── 9. FAIRNESS ACROSS SAGAS AND TENANTS
│   ├── Selection round-robins across keys and tenants; per-tenant cap per batch
│   ├── A saga with thousands of pending rows does not starve others
│   └── E9.1 FIFO by created time — one tenant's bulk sagas delay everyone's step commands — DEGRADE [C]
│
├── 10. CLEANUP & RETENTION
│   ├── MODE A: rows deleted in the same transaction (no cleanup job)
│   ├── MODE B: delete PUBLISHED rows older than retention window (partition drops by created day are allowed
│   │   only for partitions with zero PENDING / IN_FLIGHT / PARKED rows — OX-10)
│   ├── Durable per-saga record of emitted event ids and sequences kept in the saga store (for re-emit after loss)
│   └── Edge cases
│       ├── E10.1 No cleanup — table/index growth — polling query slows — saga latency grows — OUTAGE eventually [C]
│       └── E10.2 Cleanup deletes PARKED rows — saga's blocked event lost — saga stuck forever — LOSS [C]
│
├── 11. RELAY OUTAGE — EFFECT ON SAGAS
│   ├── Commands not delivered → step timers would fire and mark steps UNKNOWN
│   ├── Relay health must be visible to the orchestrator: while relay/publication is degraded, step timeouts are
│   │   extended (no UNKNOWN storm caused by the relay itself)
│   ├── On recovery: publish in sequence per key; fairness across sagas; ramped rate
│   └── E11.1 Relay down 20 min — thousands of steps marked UNKNOWN and resolved needlessly — load storm [C]
│
└── 12. FEATURE TEST SCENARIOS
    ├── Saga transition commits → outbox row present with next sequence; rollback → no row
    ├── Ambiguous commit → re-read decides; no duplicate transition or saga
    ├── Two relay workers → each key published by one worker only; per-saga order preserved
    ├── Event 1 of saga X fails (broker timeout), event 2 pending → event 2 not sent until event 1 succeeds
    ├── Record too large → row PARKED, saga X blocked and visible; other sagas unaffected
    ├── Relay crash after send before confirm → resend with same event_id; consumer dedupes
    ├── Lease expiry reclaim while original worker alive → fenced; no reordering
    ├── Long transaction commits event with earlier created time → ordering by sequence unaffected
    ├── Forward command then compensation for same saga → delivered in that order to handler
    ├── 500-branch fork → per-branch keys published in parallel; each key in order
    ├── Tenant flood of 50k rows → other tenants' commands still published within latency budget
    ├── Cleanup with PARKED rows present → not deleted
    ├── Relay down 20 minutes → no UNKNOWN storm; ordered catch-up after recovery
    └── CDC mode → insert+delete, no status updates, ordering by commit
```

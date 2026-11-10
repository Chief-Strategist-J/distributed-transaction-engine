# TODO — Distributed Transaction Engine (Senior/Staff System Architect prep)

Topics: Saga, Outbox, Inbox, Idempotency, Fencing, Reconciliation.
Primary company fit: **Stripe** (payments correctness, idempotency, ledgers), **Google** (distributed systems depth, Spanner-style thinking), **Anthropic** (reliable billing/metering, agent workflow durability), Neo4j (secondary: transactional guarantees, Raft).

Current state: scaffold + 25 feature `context.yml`; only `src/features/saga-orchestration/index.go` has code (~550 LOC Go total). Rust/C++ modules are stubs.

Legend: [ ] todo · every WP has an **AC** (acceptance criteria) — a WP is not done until its AC is demonstrated by a test, benchmark, or written artifact.

---

## 0. Definition of done for "interview-ready"
- [ ] Can whiteboard the full design in 35 minutes: requirements -> API -> data model -> flow -> failure modes -> scale -> trade-offs
- [ ] Can answer "what breaks at 100x?" with numbers from your own benchmarks (section 5)
- [ ] Every non-trivial decision has an ADR containing at least two rejected alternatives
- [ ] A 10-minute recorded walkthrough of the repo (practice for the behavioral "tell me about a system you designed")
- [ ] At least 8 failure drills automated (section 4) and runnable via `scripts/test`

## 1. Architecture artifacts (write in `docs/`)
- [ ] `docs/00-requirements.md`
  - Functional: start saga, query, compensate, manual intervene, pause/resume, cancel, versioned workflows
  - Non-functional with numbers: e.g. 5k saga starts/s, p99 step dispatch < 50 ms, saga max duration 30 d, RPO 0 for saga state, RTO < 60 s, 99.95% availability
  - Explicit non-goals (e.g. not a general workflow engine, no cross-saga transactions)
  - **AC:** every NFR has an owner metric and an alert
- [ ] `docs/01-c4-context-container-component.md` — C4 L1-L3, one Mermaid diagram each
- [ ] `docs/02-data-model.md` — ERD for `saga`, `saga_step`, `saga_event` (append-only), `outbox`, `inbox`, `idempotency_key`, `lease`, `audit`; indexes, partition keys, retention per table
- [ ] `docs/03-state-machine.md` — transition table with guards, who may trigger each transition, and the idempotency of each transition
- [ ] `docs/04-consistency-and-isolation.md` — which anomalies are possible (dirty read, lost update, non-repeatable read between steps) and the countermeasure for each (semantic lock, commutative ops, pessimistic view, reread value)
- [ ] `docs/05-capacity-model.md` — back-of-envelope: rows/day x bytes = GB/day; write amplification (state + outbox + audit); IOPS; partition count; cost per million sagas
- [ ] `docs/06-threat-model.md` — STRIDE; replay attacks on idempotency keys, cross-tenant key collisions, operator abuse of manual intervention, poisoned workflow definitions
- [ ] `docs/07-runbooks/` — stuck saga, outbox lag, DLQ flood, reconciliation drift, failed deploy mid-saga
- [ ] `docs/08-slo.md` — SLIs (step success ratio, saga completion within SLA, outbox lag), error budget policy, burn-rate alerts (1h/6h multi-window)
- [ ] ADRs (each: context, options, decision, consequences, revisit-when):
  - [ ] ADR-001 Orchestration vs choreography
  - [ ] ADR-002 Saga vs 2PC/XA vs TCC vs event sourcing
  - [ ] ADR-003 Outbox relay: polling vs logical replication/CDC vs transaction log tailing
  - [ ] ADR-004 Idempotency store: Postgres unique constraint vs Redis (+ failure semantics of Redis loss)
  - [ ] ADR-005 State store: relational rows vs event-sourced log + snapshots
  - [ ] ADR-006 Leasing/fencing: DB lease vs etcd/ZooKeeper vs Redis (Redlock critique — read Kleppmann's article)
  - [ ] ADR-007 Workflow versioning: pin-to-start-version vs patching (compare Temporal "patching")
  - [ ] ADR-008 Multi-tenancy: shared tables + tenant_id vs schema vs DB per tenant
  - [ ] ADR-009 Build vs buy (Temporal, Cadence, AWS Step Functions, Camunda) — be honest about when you'd buy

## 2. Work packages (implementation)

### WP-1 State machine (`transaction-state-machine`)
- [ ] States: CREATED, RUNNING, WAITING, COMPENSATING, COMPLETED, COMPENSATED, FAILED, STUCK, CANCELLED
- [ ] Transition table as data; illegal transition returns typed error
- [ ] **AC:** property-based test (rapid/gopter): random event sequences never reach an illegal state; replaying the event log reproduces the same state (determinism)

### WP-2 Orchestrator core (`saga-orchestration`, `workflow-definitions`)
- [ ] Persist step intent BEFORE dispatch; persist result AFTER; resume from last persisted point
- [ ] Declarative workflow definition (YAML/JSON) with validation (no cycles, every step has a compensation or is marked non-compensable/pivot)
- [ ] Identify the **pivot transaction** and retriable steps (Richardson classification: compensable / pivot / retriable)
- [ ] **AC:** kill -9 the orchestrator at every step boundary in a test harness (fault injection point between each persist and each dispatch); saga always finishes or compensates exactly once effect-wise

### WP-3 Execution modes (`sequential-execution`, `parallel-execution`)
- [ ] Sequential: reverse-order compensation
- [ ] Parallel: fan-out/fan-in, join policy (all / quorum / first), compensation of completed branches only
- [ ] **AC:** table-driven tests for every combination of branch failure x timing; no compensation for steps that never ran

### WP-4 Compensation (`compensation-engine`, `compensation-retry`, `retry-policies`)
- [ ] Compensations must be idempotent and retriable until success; define "semantic undo" (refund != reverse charge) per step
- [ ] Error taxonomy: retryable / non-retryable / unknown-outcome; unknown-outcome triggers query-then-decide
- [ ] Exponential backoff with full jitter; retry budget per tenant; circuit breaker per downstream
- [ ] **AC:** chaos test where compensation fails N times then succeeds; final state correct; retry storm bounded by budget

### WP-5 Transactional outbox (`transactional-outbox`)
- [ ] Business write + outbox insert in one local transaction
- [ ] Relay: poll with `FOR UPDATE SKIP LOCKED`, batch, publish, mark sent; then implement CDC variant
- [ ] Ordering per aggregate: single relay worker per key or ordered partitioning
- [ ] **AC:** crash relay between publish and mark-sent -> duplicate published, consumer dedupes (proves at-least-once + idempotent); measure relay lag at 1k/5k/20k msgs/s

### WP-6 Inbox/dedup (`inbox-deduplication`)
- [ ] `INSERT ... ON CONFLICT DO NOTHING` on message id inside the same tx as the side effect
- [ ] Retention/pruning strategy; what happens when dedupe window is shorter than max redelivery delay
- [ ] **AC:** concurrent duplicate delivery test (100 goroutines same message id) -> exactly one effect

### WP-7 Idempotency keys (`idempotency-keys`) — Stripe-critical
- [ ] Semantics modeled on Stripe API: key scoped per account; same key + same request -> replay stored response (including errors, except 5xx/conflicts policy you define); same key + different params -> 400/422
- [ ] Concurrent request with same key while first in flight -> 409 or wait; define and test
- [ ] Recovery points: store progress phases so a retry resumes mid-flow (Stripe blog "Designing robust and predictable APIs with idempotency" — implement the "atomic phases" idea)
- [ ] Key TTL (e.g. 24 h) with documented consequences; key length/charset limits
- [ ] **AC:** test matrix (same/different payload x in-flight/completed/failed x client timeout/retry); fingerprint = canonical hash of method+path+body

### WP-8 Fencing and leases (`fencing-tokens`)
- [ ] Monotonic token issued with each lease; store rejects writes with token < max seen
- [ ] Simulate GC pause / network delay: old leader wakes and writes -> rejected
- [ ] **AC:** deterministic test reproducing the Kleppmann "unsafe lock" scenario and showing fencing fixes it

### WP-9 Concurrency control (`optimistic-concurrency-control`)
- [ ] Version column, `UPDATE ... WHERE version = $n`, bounded retry with jitter
- [ ] Compare with `SELECT FOR UPDATE`, advisory locks, serializable isolation (and its retry requirement)
- [ ] **AC:** contention benchmark (1, 10, 100 writers per saga) with throughput/abort curves

### WP-10 Recovery and timers (`crash-recovery`, `scheduled-execution`)
- [ ] Heartbeat + lease expiry; recovery scanner claims orphaned sagas with SKIP LOCKED
- [ ] Timer wheel or DB-polled `due_at` index; no double fire across replicas; clock skew tolerance (use DB time, not app time)
- [ ] Recovery rate limiting to avoid thundering herd after outage
- [ ] **AC:** kill half the replicas mid-load; all sagas progress; no duplicate step execution beyond at-least-once bound

### WP-11 Reconciliation (`reconciliation-engine`)
- [ ] Compare saga outcome vs. downstream source of truth (payments ledger, inventory) on schedule; classify: match / missing / mismatch / orphan
- [ ] Auto-repair policy per class; everything else to manual queue
- [ ] Metrics: drift count, time-to-detect, time-to-repair
- [ ] **AC:** inject divergence in a fake downstream; reconciler detects and repairs within SLA

### WP-12 DLQ and manual intervention (`dead-letter-queue`, `manual-intervention`)
- [ ] Use `messaging/policies/dlq_policy.yaml`; DLQ record keeps original payload, headers, error, attempts
- [ ] Operator actions aligned with `docs/features/manual-intervention/business-logic.md`: FORCE_RETRY, FORCE_COMPENSATE, FORCE_COMPLETE (with approval ticket + synthetic payload), FORCE_TERMINATE, all audited
- [ ] AC: an operator action cannot bypass RBAC (`admin:saga:intervene`); every action produces an immutable audit row in `manual_intervention_audit_log`

### WP-13 Versioning (`workflow-versioning`)
- [ ] In-flight sagas pinned to starting version; new version only for new sagas; deprecation + drain procedure
- [ ] **AC:** deploy v2 while 1k v1 sagas are mid-flight; all v1 finish on v1 semantics

### WP-14 Tenancy, authN/Z, audit, tracing (`tenant-isolation`, `authentication-authorization`, `audit-trail`, `distributed-tracing`)
- [ ] tenant_id in every PK/index and every query (enforce with Postgres RLS); per-tenant rate limits and quotas
- [x] mTLS/JWT between services; scopes for operator vs. service vs. read-only (v2: 5-layer authorization, SPIFFE mesh mapping, webhook replay protection, fail-closed revocation)
- [x] Hash-chained audit log (each row includes hash of previous); verification job (v2: outbox capture, keyed HMAC-SHA256, quarantine, signed checkpoints, segment sealing)
- [x] W3C trace context carried through outbox -> broker -> consumer; one trace per saga (v2: W3C traceparent parse/inject, async link registry, adaptive tail sampling, LEVEL E deep sessions, span budget)
- [ ] **AC:** noisy-neighbour test: tenant A floods; tenant B p99 unaffected beyond a stated bound

### WP-15 Query API + dashboard (`transaction-query-api`, `operational-dashboard`)
- [ ] Saga timeline, filter by state/tenant/age, stuck list, SLA breaches, cursor pagination
- [ ] **AC:** query p99 < 100 ms at 50M saga rows (index plan shown via EXPLAIN ANALYZE)

### WP-16 Infrastructure
- [ ] Fill `database/migrations/sql` (forward + rollback, expand/contract pattern for zero-downtime), `messaging/schemas`, `messaging/topics/topics.yaml`
- [ ] Graceful shutdown: stop claiming, finish/handoff in-flight steps, release leases
- [ ] Verify `deploy/helm|k8s|argocd|terraform` actually render/plan; add PDB, HPA on queue depth, readiness gates
- [ ] Decide purpose for `rust-modules` (hash-chain verifier / canonical fingerprinting) and `cpp-modules` or delete

## 3. Deep-dive concepts to master (be able to teach each in 5 minutes)
- [ ] Exactly-once is at-least-once + idempotence; why "exactly-once delivery" is impossible (Two Generals) and what Kafka EOS actually guarantees
- [ ] 2PC blocking problem, coordinator failure, 3PC limits, Paxos Commit; why Spanner uses 2PC over Paxos groups + TrueTime
- [ ] Spanner: TrueTime, commit-wait, external consistency, how it avoids saga-style compensation inside one database
- [ ] Percolator / Calvin as alternative distributed transaction designs
- [ ] Isolation levels and anomalies: read committed, snapshot, serializable, write skew; Postgres SSI
- [ ] CAP, PACELC, linearizability vs serializability vs strict serializability
- [ ] Raft/Paxos basics (leader election, log replication, safety), etcd leases
- [ ] Lamport/vector clocks, hybrid logical clocks
- [ ] Double-entry ledger design (Stripe-style): immutable entries, balanced transactions, derived balances, reconciliation against bank files
- [ ] Money handling: integer minor units, currency, rounding, FX, never floats
- [ ] Rate limiting and load shedding (Stripe's four limiter types: request rate, concurrent requests, fleet usage, worker utilization — read their blog)
- [ ] Online schema migrations without downtime (Stripe's "Online migrations at scale")

## 4. Failure drills (automate under `scripts/test/chaos/`)
- [ ] D1 crash between DB commit and publish
- [ ] D2 crash after step side effect, before result persisted
- [ ] D3 duplicate + reordered message delivery
- [ ] D4 compensation permanently fails -> STUCK + page
- [ ] D5 split brain: two orchestrators own one saga (fencing)
- [ ] D6 poison message blocks partition
- [ ] D7 clock skew on timers
- [ ] D8 downstream timeout with unknown outcome -> query-before-compensate
- [ ] D9 thundering herd after outage
- [ ] D10 DB failover mid-transaction (client retries safely?)
- [ ] D11 idempotency store lost (Redis flush) -> what is the safety net? (answer: DB uniqueness on the effect)
- [ ] D12 deploy rollback with incompatible schema/workflow version
- **AC:** each drill has expected behavior in writing, an automated reproduction, and a runbook link

## 5. Experiments to run and record (numbers go in `docs/09-benchmarks.md`)
- [ ] Saga throughput vs steps per saga (3, 10, 30) and vs parallelism
- [ ] Outbox lag percentiles: polling interval x batch size matrix; CDC comparison
- [ ] Contention curve for optimistic concurrency
- [ ] Postgres table bloat/autovacuum behavior on outbox (high churn) — partitioning by day + drop old partitions
- [ ] Recovery time after killing N% replicas
- [ ] Cost per million sagas on your chosen infra
- **AC:** for each: hypothesis, setup, result, bottleneck identified, what you'd change

## 6. Company-specific mapping

### Stripe
- [ ] Re-derive the PaymentIntent lifecycle; explain why it is a state machine and how webhooks + polling reconcile
- [ ] Implement a minimal ledger service (double-entry) on top of this engine as a sample saga: authorize -> capture -> payout -> refund
- [ ] Read: Stripe idempotency blog, "Online migrations at scale", rate limiter blog, API versioning blog ("APIs as infrastructure")
- [ ] Practice API design rounds: resources, errors (stable error codes), pagination (cursor), versioning, webhooks (signing, retries, ordering not guaranteed), idempotency
- [ ] Practice Stripe-style "integration" and "bug squash" exercises: read unfamiliar code, find a defect with tests, communicate clearly
- [ ] Be ready to argue user-impact and correctness over cleverness: "money must not be created or destroyed"

### Google
- [ ] Papers (read + summarize in 1 page each): Spanner, Chubby, Paxos Made Simple, Raft, MapReduce, Bigtable, Borg, Dapper, Megastore, Percolator, Zanzibar (authorization — pairs with `tenant-isolation`)
- [ ] SRE: SLOs, error budgets, toil, postmortems, cascading failures chapter, load shedding, graceful degradation
- [ ] System design format: clarify requirements, estimate, API, data, high-level, deep-dive on 2 components, bottlenecks, evolve. Practice with 45-min timer, 1/week
- [ ] Behavioral ("Googleyness & leadership"): 6 STAR stories — ambiguity, conflict, failure, influence without authority, mentoring, ownership
- [ ] Know trade-off language: latency vs consistency, simplicity vs flexibility, build vs buy, operational burden

### Neo4j
- [ ] Transactions in Neo4j: ACID on a single instance, causal clustering with Raft, causal consistency bookmarks; how would this saga engine talk to Neo4j (idempotent MERGE writes, bookmarks for read-your-writes)
- [ ] Explain why distributed transactions across Neo4j and another store need saga/outbox (link to `knowledge-graph-platform`)

### Anthropic
- [ ] Metering/billing for token usage as a saga/ledger problem (usage events -> aggregate -> invoice; idempotent, auditable, reconcilable)
- [ ] Durable agent workflows: long-running tool-use loops with checkpointing, resume after crash, idempotent tool calls, human-in-the-loop approval (maps to `manual-intervention`)
- [ ] Reliability under load: rate limiting per tenant/model, queueing vs shedding, graceful degradation, capacity planning for GPU-bound services
- [ ] Read Anthropic engineering posts on agents ("Building effective agents"), MCP spec, and the Responsible Scaling Policy; prepare a genuine answer to "why Anthropic" tied to safety and reliability

## 7. Mock interview bank (answer each aloud, 10 min, then critique against the "strong answer" notes)
1. Design a payment system that never double-charges. *Strong: idempotency keys at API and at effect, state machine, outbox, reconciliation against processor, ledger, retries with unknown-outcome handling.*
2. Design a distributed job scheduler with exactly-once-effect semantics. *Strong: leases + fencing, idempotent jobs, timer index, recovery scanner.*
3. Design a ledger with auditability. *Strong: append-only, double entry, balance invariants, snapshots, reconciliation, corrections via reversing entries.*
4. A saga is stuck for 3 days — walk through debugging. *Strong: query API, audit trail, trace, classify cause, safe manual action.*
5. How do you migrate a live billion-row table with no downtime? *Strong: expand/contract, dual write, backfill, verify, cutover, rollback.*
6. Design Stripe webhooks. *Strong: at-least-once, signature, exponential retry up to 3 days, ordering not guaranteed, idempotent handlers, replay UI.*
7. Design a rate limiter. *Strong: token bucket, distributed counters, Redis Lua, per-tenant, fail-open vs fail-closed, headers.*
8. Design a metering pipeline for LLM token usage. *Strong: event capture, dedupe, windowed aggregation, late events, reconciliation, invoices.*
9. How would you make agent tool calls safe to retry? *Strong: idempotency keys per call, checkpointing, classify read vs write tools, approvals for irreversible actions.*
10. Compare your engine to Temporal. *Strong: event-sourced history + deterministic replay vs state rows; trade-offs honest.*
11. Cross-region active-active for this engine. *Strong: conflict domains, home-region per tenant, avoid multi-master on saga state.*
12. What is the hardest bug this design could hide? *Strong: unknown-outcome side effects and non-idempotent compensations.*

## 8. Reading list (ordered)
1. DDIA (Kleppmann) ch. 5-9, 11 — finish with exercises
2. Garcia-Molina & Salem, "Sagas" (1987)
3. microservices.io: Saga, Transactional Outbox, Idempotent Consumer
4. Stripe engineering blog: idempotency, online migrations, rate limiters, API design
5. Spanner paper + "Spanner, TrueTime and the CAP theorem"
6. Kleppmann: "How to do distributed locking"
7. Pat Helland: "Life beyond Distributed Transactions", "Data on the Outside vs Data on the Inside"
8. Temporal docs: workflow determinism, event history
9. Google SRE book chs. on SLOs, cascading failures, handling overload

## 9. Weekly cadence suggestion
- Mon/Tue: implement a WP (AC first, then code)
- Wed: write the related ADR/doc
- Thu: run failure drill or benchmark, record numbers
- Fri: 45-min timed mock design + self-critique
- Weekend: one paper + one behavioral story rehearsal

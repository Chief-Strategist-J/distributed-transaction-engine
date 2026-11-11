# ADR-0008: Transactional Dead-Letter Queue (DLQ), Human Intervention State Machine & Dual-Custody Disposition

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-11-06 |
| Deciders | Lead Systems Architect & Core Architecture Guild |
| Related Docs | [Business Logic & Decision Tree v2](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/dead-letter-queue/business-logic.md) · [Diagrams](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/dead-letter-queue/diagrams.md) · [Failure Cases & Mitigations](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/dead-letter-queue/failure-cases.md) |
| Target Feature | [Dead-Letter Queue Feature](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/src/features/dead-letter-queue) |

---

## 1. Context & Problem Statement

In mission-critical transactional saga architectures (e.g., core payment captures, banking ledger postings, multi-service order provisioning), transient errors are mitigated by exponential retries and circuit breakers. However, a significant category of errors cannot be healed automatically:
- **Contract & Schema Incompatibilities**: Payloads failing JSON Schema / Protobuf decoding due to upstream version skew.
- **Permanent Business Rejections**: Downstream payment gateway returning `402 Card Expired` or `422 Unprocessable Entity` where automatic retries will never succeed.
- **Post-Pivot Compensation Deadlocks**: An unrecoverable failure occurring after an irreversible pivot step (e.g., card debited, but physical dispatch warehouse API permanently down and refund rejected).
- **Poison Messages**: Repeatedly crashing workers upon deserialization.

In standard streaming pipelines (e.g. plain Kafka or RabbitMQ), a common anti-pattern is blindly dumping failed messages into a raw Kafka DLQ topic. In a distributed transaction engine, this creates catastrophic failures:
1. **Saga Orphanage**: The message sits on an unread Kafka topic while the parent saga state machine remains frozen in `RUNNING` or `COMPENSATING` forever.
2. **Missing Transactional Context**: The Kafka DLQ message loses the database lease epoch, forward execution ID, and tenant financial balance state.
3. **Unfettered Replay Hazards**: Operators blindly hitting "replay" on a Kafka topic republish events without verifying whether the original saga was already manually refunded or cancelled, resulting in duplicate financial debits.
4. **Unauthorized Financial Alterations**: A single operator modifying payloads or forcing completions without peer review (dual custody) creates internal fraud vectors.

---

## 2. Real-World Use Cases: When to Use vs. When to Reject

### When to Use
* **Multi-Service Financial Settlements (Stripe / Adyen Style)**: When an unrecoverable discrepancy occurs after customer card charge (e.g., merchant payout fails permanently), the saga transitions to `NEEDS_ATTENTION` and enqueues a DLQ record for human financial reconciliation.
* **Complex B2B Supply Chain Fulfillment**: Physical inventory reserved, but carrier label generation fails due to address validation errors. Operator reviews, amends shipping address, and triggers replay.
* **Post-Pivot Reversal Failures**: Airline API rejects refund because departure is within 2 hours. Requires manual negotiation by support desk.

### When NOT to Use (Rejection Boundaries)
* **High-Throughput Analytics & Telemetry**: Event ingestion (>50,000 events/sec) where dropped events can be sampled or discarded without ledger corruption.
* **Transient Network Drops / Partner Outages**: Downtime in downstream services must be handled via Circuit Breakers and Backoff Timers (`WAITING_RETRY`), never routed to DLQ.
* **Low-Latency Trading / In-Memory State**: Sub-millisecond order books cannot incur PostgreSQL disk I/O and manual review workflows.

---

## 3. Analytical Numbers & Capacity Model

| Metric | Target Specification | Engineering Derivation & Analytical Limits |
|---|---|---|
| **Max Ingress DLQ Rate** | $500 \text{ records/sec}$ | Sized for catastrophic downstream partner outage ($10\%$ of $5,000 \text{ sagas/s}$ baseline). |
| **Admission Transaction Latency** | $p50 < 4 \text{ ms}, \quad p99 < 15 \text{ ms}$ | DLQ record inserted in same local PostgreSQL transaction as saga state update. |
| **Write Amplification per Admission** | $3.5\times$ base payload | 1x `saga_dlq_records` + 1x `saga_instances` update + 1x `transactional_outbox` + 0.5x index updates. |
| **Payload Storage per Record** | $4 \text{ KB}$ (Clamped) | Payloads $>16 \text{ KB}$ offloaded to MinIO/S3 claim check; pointer stored in DB. |
| **Storage per 100,000 DLQ Records** | $\sim 580 \text{ MB}$ | Table data ($400 \text{ MB}$) + B-Tree indexes ($180 \text{ MB}$). |
| **P99 Replay Dispatch Latency** | $< 50 \text{ ms}$ | Read from DB + CAS on record + outbox enqueue. |
| **Systemic Incident Threshold** | $\ge 20 \text{ errors/5 min}$ with same fingerprint | Auto-pauses step type to prevent 10,000+ flood. |

---

## 4. Decision Drivers & Architecture Invariants (DQ-01 – DQ-12)

* **DQ-01 (Single Database Transaction Admission)**: The insertion of the DLQ record and the saga/step state transition to `NEEDS_ATTENTION` commit within the exact same database transaction.
* **DQ-02 (Database is System of Record)**: The primary PostgreSQL table `saga_dlq_records` is the authoritative store. Any Kafka DLQ topic is strictly an informational notification feed emitted via the Transactional Outbox.
* **DQ-03 (Immutability of Historical Payloads)**: The original failed payload is immutable and cryptographically hashed (`payload_sha256`). Any corrected payload provided by an operator is stored as an explicit, separate versioned patch.
* **DQ-04 (Original Idempotency Key Replay)**: Automated or manual retries from DLQ MUST use the original forward or reverse idempotency key to prevent double execution at the downstream provider.
* **DQ-05 (Dual-Custody Four-Eyes Principle)**: Any disposition involving monetary liability or manual completion above $\$1,000.00$ requires approval from two distinct authenticated operators (`operator_submitter != operator_approver`).
* **DQ-06 (Automated Systemic Fingerprint Quarantine)**: When identical error fingerprints exceed 20 records within 5 minutes across a tenant or workflow type, the engine automatically pauses that step definition rather than flooding operators with 10,000 duplicate DLQ items.

---

## 5. Architectural Trade-offs & Considered Options

| Architectural Option | Consistency & Durability | Operational Ergonomics | Replay Safety | Verdict |
|---|---|---|---|---|
| **Option 1: Raw Kafka DLQ Topic** | ❌ Dual-write vulnerability; saga state in DB diverges from Kafka. | ❌ Requires custom Kafka CLI scripts; zero search/filter by tenant or amount. | ❌ High risk of duplicate financial transactions on uncoordinated replay. | **Rejected** |
| **Option 2: Redis-Backed DLQ** | ❌ Ephemeral; memory exhaustion risk under partner outage; data loss on failover. | ⚠️ Fast key-value lookups, but lacks relational filtering across tenants and dates. | ⚠️ Lacks transactional CAS guarantees with saga state table. | **Rejected** |
| **Option 3: Relational DLQ with Outbox Notifications & Dual Custody (Selected)** | ✅ 100% ACID consistency; zero saga state drift; RPO = 0. | ✅ SQL queryability, tenant partitioning, audit logging, and web UI integration. | ✅ Guaranteed idempotency key reuse and atomic CAS state transitions. | **Adopted** |

---

## 6. Relational Schema & DDL

```sql
CREATE TYPE dlq_record_status AS ENUM (
    'PENDING_REVIEW',
    'AWAITING_SECOND_APPROVAL',
    'REPLAY_DISPATCHED',
    'FORCE_COMPLETED',
    'FORCE_ABORTED',
    'DISMISSED_IRRELEVANT'
);

CREATE TABLE IF NOT EXISTS saga_dlq_records (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id VARCHAR(64) NOT NULL,
    saga_id VARCHAR(64) NOT NULL,
    step_id VARCHAR(64) NOT NULL,
    step_index INT NOT NULL,
    attempt_count INT NOT NULL,
    idempotency_key VARCHAR(128) NOT NULL,
    failure_class VARCHAR(64) NOT NULL,
    error_fingerprint VARCHAR(64) NOT NULL,
    error_message TEXT NOT NULL,
    payload_sha256 CHAR(64) NOT NULL,
    payload_claim_check_url TEXT,
    payload JSONB NOT NULL,
    status dlq_record_status NOT NULL DEFAULT 'PENDING_REVIEW',
    money_at_stake NUMERIC(18, 4) DEFAULT 0.0000,
    currency VARCHAR(3) DEFAULT 'USD',
    submitted_by VARCHAR(128),
    approved_by VARCHAR(128),
    disposition_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_saga_dlq UNIQUE (tenant_id, saga_id, step_id, attempt_count),
    CONSTRAINT chk_dual_custody CHECK (
        (status != 'FORCE_COMPLETED' AND status != 'FORCE_ABORTED') 
        OR (money_at_stake < 1000.00) 
        OR (submitted_by IS NOT NULL AND approved_by IS NOT NULL AND submitted_by != approved_by)
    )
);

CREATE INDEX IF NOT EXISTS idx_dlq_review 
    ON saga_dlq_records (tenant_id, status, created_at) 
    WHERE status IN ('PENDING_REVIEW', 'AWAITING_SECOND_APPROVAL');

CREATE INDEX IF NOT EXISTS idx_dlq_fingerprint 
    ON saga_dlq_records (tenant_id, error_fingerprint, created_at);
```


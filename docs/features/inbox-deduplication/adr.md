# ADR-0007: Atomic Inbox Deduplication, Natural Identity Resolution & Integrity-Checked Reply Ingestion

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-11-09 |
| Deciders | Lead Systems Architect & Core Architecture Guild |
| Related Docs | [Business Logic Tree v2](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/inbox-deduplication/business-logic.md) · [Diagrams](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/inbox-deduplication/diagrams.md) · [Failure Cases](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/inbox-deduplication/failure-cases.md) · [Sequential Execution ADR-0005](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/sequential-execution/adr.md) |
| Target Feature | [Inbox Deduplication Feature](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/src/features/inbox-deduplication) |

---

## 1. Context & Problem Statement

Distributed messaging platforms (Kafka, RabbitMQ, SQS) provide at-least-once message delivery guarantees. Network blips, broker rebalances, consumer restarts, and upstream retries routinely deliver identical messages multiple times to downstream consumers.

Within the Distributed Transaction Engine, reply messages from external microservices (e.g. payment confirmations, inventory release acknowledgments) drive state transitions in saga execution. If an engine processes duplicate reply messages:
1. **Double Progression / Counter Corruption**: A duplicate completion reply could cause a step machine to increment step counters twice or trigger parallel downstream dispatches prematurely.
2. **Ghost Side Effects**: If an idempotent step worker successfully processes an order and emits a completion event, consumer redelivery could trigger duplicate secondary effects (such as shipping notifications or loyalty points issuance).
3. **Integrity Failures from Replay Attacks / Producer Id Reuse**: If an upstream service reuses an identical `event_id` for two materially different business payloads (e.g. two distinct purchases), a naive deduplication store that acknowledges duplicates solely based on key presence will silently drop the second legitimate transaction, causing silent data loss.

---

## 2. Decision Drivers & Architecture Invariants (IB-01 – IB-08)

The inbox deduplication feature is governed by eight non-negotiable invariants:

* **IB-01 (Producer Message Identity)**: Deduplication identity is anchored strictly to the producer-assigned message identity (`external_id` / `event_id`), never to transport-level offsets or partition coordinates.
* **IB-02 (Composite Handler Uniqueness)**: Uniqueness is scoped to `(tenant_id, handler_name, handler_generation, external_id)`. Two independent consumers in the same service processing the same event must each apply their own local effects.
* **IB-03 (Single-Transaction Atomicity)**: In the primary processing model (M1), the inbox record insertion and business entity mutations are committed within the **exact same ACID database transaction**. No network RPCs occur while holding this transaction.
* **IB-04 (Content Hash Integrity Verification)**: A duplicate message is acknowledged as a safe duplicate only if its SHA-256 canonical payload hash matches the stored hash. If the hashes differ, processing is blocked, the key is held, and an alert is raised (`INTEGRITY_MISMATCH`).
* **IB-05 (Monotonic Position Progression)**: The consumer offset never advances past a message that is neither committed, held, nor quarantined.
* **IB-06 (Retention Horizon)**: Inbox records are retained for a window exceeding the maximum possible redelivery and replay horizon. Table partitioning is keyed by `producer_event_time`, never by local reception time.
* **IB-07 (Cache as Accelerator Only)**: Any fast-path in-memory or Redis cache serves solely to accelerate known duplicate identification. The relational database remains the sole authoritative gatekeeper for determining new messages.
* **IB-08 (Isolated Attempt Counters)**: Failure histories and attempt counters are tracked in an isolated context that survives application rollbacks, preventing infinite crash loops.

---

## 3. Considered Architectural Options

### Option 1: Transport Offset Deduplication
* **Architecture**: Track processed Kafka `(topic, partition, offset)` tuples in memory or Redis.
* **Cons**:
  - Offsets change when topics are re-mirrored, migrated, or repartitioned.
  - Re-emitted events with new offsets bypass deduplication completely.
* **Verdict**: **Rejected**.

### Option 2: Pre-Execution Distributed Lock / Cache Only
* **Architecture**: Check Redis with key `inbox:{msg_id}` before invoking the handler.
* **Cons**:
  - Dual-write race: if the application crashes after marking Redis but before saving DB state, the message is permanently dropped on redelivery.
* **Verdict**: **Rejected**.

### Option 3: Relational Inbox Table with Single Transaction Atomicity (Selected)
* **Architecture**: Insert an inbox record (`INSERT ... ON CONFLICT DO NOTHING`) inside the local business transaction. If 0 rows are affected, verify the content hash and acknowledge without re-executing business effects.
* **Pros**:
  - Mathematically provable idempotency across crashes and network drops.
  - Zero dual-write window.
  - Detection of payload corruption and identity reuse via SHA-256 hash checks.
* **Verdict**: **Adopted**.

---

## 4. Entity-Relationship & Storage DDL

```sql
CREATE TABLE IF NOT EXISTS inbox_messages (
    id UUID NOT NULL,
    public_id VARCHAR(32) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    source VARCHAR(64) NOT NULL,
    external_id VARCHAR(128) NOT NULL,
    correlation_id UUID NOT NULL,
    handler_name VARCHAR(128) NOT NULL,
    handler_generation INT NOT NULL DEFAULT 1,
    content_hash CHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'APPLIED',
    attempt_count INT NOT NULL DEFAULT 1,
    producer_event_time TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_inbox_messages PRIMARY KEY (tenant_id, id),
    CONSTRAINT inbox_messages_tenant_id_id_uk UNIQUE (tenant_id, id),
    CONSTRAINT inbox_messages_public_id_uk UNIQUE (tenant_id, public_id),
    CONSTRAINT inbox_messages_dedup_uk UNIQUE (tenant_id, handler_name, handler_generation, external_id),
    CONSTRAINT inbox_messages_public_id_ck CHECK (public_id ~ '^inb_[0-9a-zA-Z]{26}$'),
    CONSTRAINT inbox_messages_handler_name_ck CHECK (handler_name = btrim(handler_name) AND handler_name <> ''),
    CONSTRAINT inbox_messages_external_id_ck CHECK (external_id = btrim(external_id) AND external_id <> ''),
    CONSTRAINT inbox_messages_hash_ck CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT inbox_messages_status_ck CHECK (status IN ('APPLIED', 'HELD', 'QUARANTINED')),
    CONSTRAINT inbox_messages_attempt_ck CHECK (attempt_count >= 1),
    CONSTRAINT inbox_messages_generation_ck CHECK (handler_generation >= 1)
);

CREATE INDEX IF NOT EXISTS idx_inbox_messages_prune 
    ON inbox_messages (tenant_id, producer_event_time);
```

---

## 5. Consequences & Trade-offs

### Positive
- **Guaranteed Single Effect**: Business state changes and inbox presence are inseparable.
- **Tamper Detection**: Prevents message identity collisions from silently destroying new business data.
- **Rebuild Support**: Handler generations (`handler_generation`) permit replay and projection rebuilds without truncating operational history.

### Negative
- **Database Write Overhead**: Every consumed event requires a database insert and unique index evaluation.
- **Lock Contention**: Concurrent arrivals of identical messages serialize on the unique constraint index lock.


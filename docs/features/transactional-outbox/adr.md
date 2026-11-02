# ADR-0003: Dual-Tier Datastore and KRaft Event Fabric for Transactional Outbox

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-11-02 |
| Deciders | Core Architecture Guild |
| Related ADRs | [ADR-0002](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/tech-stack/ADR-0002-dual-tier-datastore-strategy.md), [ADR-0003](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/tech-stack/ADR-0003-kraft-event-fabric.md) |
| Related System | [Transactional Outbox Feature](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/transactional-outbox) |

---

## 1. Context

Dual writes across a database and message queue inevitably suffer from partial failure: if a transaction commits to the database but message publishing fails, downstream consumers never execute; if the message publishes first but the database rollback occurs, downstream systems process phantom data. 

The Transactional Outbox pattern guarantees at-least-once message delivery by committing business mutations and outbox records inside the same local database transaction.

---

## 2. Decision Outcome

**Chosen option:** **PostgreSQL 18 ACID Outbox + Apache Kafka 4.4 KRaft Partitioned Log**

- **Database:** PostgreSQL 18 table `outbox_events` with JSONB payloads and sequential sequence IDs.
- **Message Broker:** Apache Kafka 4.4 in KRaft mode with key-based hashing on `transaction_id`.

### Service Endpoints & Database Credentials

| Property | Value |
|---|---|
| Database Server | `db:5432` (Host: `127.0.0.1:5432`) |
| Database Name | `dist_tx_engine` |
| Username | `postgres` |
| Password | `supersecretpgpassword` |
| Kafka Broker | `kafka:9092` (Host: `127.0.0.1:9092`) |
| Kafka Mode | KRaft (ZooKeeper-less) |

### Outbox Table Schema

```sql
CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox_events(status, created_at) WHERE status = 'PENDING';
```

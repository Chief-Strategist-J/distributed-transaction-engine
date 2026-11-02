# ADR-0002: Dual-Tier Datastore Strategy

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-11-02 |
| Deciders | Core Architecture Guild |
| Related ADRs | [ADR-0001](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/tech-stack/ADR-0001-polyglot-execution-runtime.md), [ADR-0003](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/tech-stack/ADR-0003-kraft-event-fabric.md) |
| Related System | [Transactional Outbox](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/transactional-outbox/adr.md) |

---

## 1. Context

Distributed sagas require two fundamentally incompatible storage profiles:
1. **Strict Durability & Audit Immutability:** Transaction states, outbox events, and compensating logs must survive catastrophic process failure with strict ACID semantics.
2. **Sub-millisecond Idempotency Deduplication:** Fast duplicate request rejection and ephemeral lock acquisition require memory-speed operations.

---

## 2. Decision Drivers

| Driver | Why It Matters |
|---|---|
| ACID Compliance & Point-In-Time Recovery | Zero data loss during unexpected node crashes |
| Idempotency Check Latency (< 500µs) | Prevent distributed double-execution without querying disk |
| Operational Simplicity | Standardized tooling with managed cloud offerings (Cloud SQL / Memorystore) |

---

## 3. Considered Options

| Option | Pros | Cons |
|---|---|---|
| **Option A: Single Store (Pure PostgreSQL 18)** | Unified backup, ACID guarantees | Disk I/O contention on high-frequency idempotency key checks |
| **Option B: Single Store (Pure Redis 7.4)** | Sub-millisecond reads and writes | Risk of transaction state loss on power failure; complex WAL persistence |
| **Option C: Dual-Tier (PostgreSQL 18 + Redis 7.4)** | ACID log durability on disk + sub-millisecond memory deduplication | Requires handling cache-invalidation edge cases |

---

## 4. Decision Outcome

**Chosen option:** **Option C: Dual-Tier Datastore**

- **Primary Tier (PostgreSQL 18):** System of record for `transactions`, `saga_steps`, and `outbox_events`.
- **Cache Tier (Redis 7.4):** In-memory cache for idempotency keys (TTL: 86400s) and distributed fencing tokens.

### Service Credentials & Access

```env
# PostgreSQL 18
POSTGRES_DB=dist_tx_engine
POSTGRES_USER=postgres
POSTGRES_PASSWORD=supersecretpgpassword
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5432
DATABASE_URL=postgres://postgres:supersecretpgpassword@127.0.0.1:5432/dist_tx_engine?sslmode=disable

# Redis 7.4
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
REDIS_URL=redis://127.0.0.1:6379/0
```

---

## 5. Architectural Diagram

```mermaid
sequenceDiagram
    participant API as API Handler
    participant Redis as Redis 7.4 (Memory)
    participant PG as PostgreSQL 18 (Disk)
    
    API->>Redis: SETNX idempotency:{key}
    alt Key Exists
        Redis-->>API: Conflict (409 Duplicate)
    else Key Acquired
        Redis-->>API: OK (Token granted)
        API->>PG: BEGIN Transaction
        API->>PG: INSERT INTO transactions ...
        API->>PG: INSERT INTO outbox_events ...
        API->>PG: COMMIT
        PG-->>API: Transaction Committed
    end
```

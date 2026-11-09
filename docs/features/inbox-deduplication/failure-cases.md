# Inbox Deduplication — Failure Cases & Production Chaos

This document details failure cases, edge scenarios, and mitigations for the **Inbox Deduplication Feature**, supporting [ADR-0007](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/inbox-deduplication/adr.md) and [Business Logic Tree v2](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/inbox-deduplication/business-logic.md).

---

## 1. Production Failure Taxonomy Matrix

| Ref ID | Failure Scenario | Severity | Impact | Mitigation Strategy | Invariant |
|---|---|---|---|---|---|
| **IB-FC-01** | **Offset-based identity across cluster migration** | **LOSS** | Offsets reset or shift during DR failover; messages incorrectly classified as duplicates or reprocessed. | Identity anchored strictly to producer message ID (`external_id`), never Kafka topic/partition/offset. | **IB-01** |
| **IB-FC-02** | **Single message ID shared across handlers** | **LOSS** | Table unique key on `(tenant_id, external_id)` without handler name; first handler consumes event, second handler is blocked as duplicate. | Unique constraint includes `handler_name` and `handler_generation`. | **IB-02** |
| **IB-FC-03** | **Crash between inbox insert and business effect** | **LOSS** | Two-phase write: inbox recorded, worker crashes before business state saved; redelivery marked duplicate, business logic lost forever. | Single atomic PostgreSQL transaction enclosing both inbox insert and business state mutations. | **IB-03** |
| **IB-FC-04** | **Reused Event ID with different payload** | **CORRUPT** | Upstream bug reuses `event_101` for a second distinct transaction; naive inbox drops it silently. | Compare canonical SHA-256 hash. If mismatch detected, reject acknowledgment, hold key, and quarantine. | **IB-04** |
| **IB-FC-05** | **Cache written before database commit** | **LOSS** | Fast-path cache updated; DB transaction subsequently rolls back; redelivery treated as duplicate from cache. | Cache is written strictly AFTER DB transaction commits. Cache misses always query the database. | **IB-07** |
| **IB-FC-06** | **Attempt counter rolled back in transaction** | **OUTAGE** | Attempt counter stored on failing row; rollback clears attempt count; message retried infinitely in crash loop. | Attempt counters and poison pill state tracked in isolated context or quarantine table surviving rollbacks. | **IB-08** |
| **IB-FC-07** | **Lock timeout on inbox insert treated as duplicate** | **LOSS** | Concurrent insert causes 5s lock timeout; error handler mistakes conflict for duplicate and acknowledges message. | Specific handling: lock timeouts trigger consumer backoff and retry, never offset commit. | **IB-05** |


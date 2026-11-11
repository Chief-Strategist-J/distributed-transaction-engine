# Crash Recovery — Failure Cases, Attack Vectors & Mitigations

This document details failure cases, edge scenarios, race conditions, and mitigation architectures for the Continuous Leaderless Crash Recovery Engine.

---

## 1. Failure Modes & Resilience Matrix

| Failure Mode ID | Scenario / Trigger | Direct Impact | Invariants Challenged | Architectural Mitigation & Detection |
|---|---|---|---|---|
| **FAIL-CR-001** | **StatefulSet Pod Rebirth Collision**: Pod `worker-0` is evicted and rescheduled by Kubernetes with the exact same hostname `worker-0`. | If `lease_owner == 'worker-0'`, the new pod assumes it owns the active leases and heartbeats them without having loaded in-memory step graphs, permanently starving recovery. | **R3, 0.1** | **Mitigation:** Incarnation UUID pinning. The worker generates `incarnation_id = fmt.Sprintf("%s_%s", podName, uuid.New())` on boot. The lease check enforces `lease_owner = incarnation_id`. The rescheduled pod has a different UUID and ignores the previous pod's leases. |
| **FAIL-CR-002** | **Spurious Compensation on `PENDING` Steps**: Worker crashes while Step 3 is `PENDING`. Recovery sweeper queries downstream service, receives `NOT_FOUND`, and marks Step 3 `FAILED`. | Engine triggers reverse compensation on a transaction that never failed, issuing unnecessary refunds and cancellations. | **R5, Bug 1** | **Mitigation:** Zero downstream probing for `PENDING` steps. Because invariant S10 guarantees that transitions to `DISPATCHED` are committed to PostgreSQL *before* any outbound network dispatch, `PENDING` means the command was never sent. Sweeper simply marks it ready for normal forward dispatch. |
| **FAIL-CR-003** | **Downstream `NOT_FOUND` Ambiguity Trap**: Step 2 (Card Charge) was `DISPATCHED` when worker crashed. Sweeper queries payment gateway status; gateway returns `404 NOT_FOUND` because charge is delayed in an async internal ingress queue. | Sweeper incorrectly assumes charge never executed, marks step `FAILED`, and triggers compensation. The payment charge processes 2 seconds later, resulting in an unrefunded customer charge. | **R6, Bug 2** | **Mitigation:** `NOT_FOUND` is never treated as terminal failure. The sweeper re-sends the request using the exact same deterministic idempotency key (`idem:fwd:tenant:tx:step:attempt`). Downstream gateway deduplicates or executes. |
| **FAIL-CR-004** | **Takeover CAS Race with Heartbeat Renewal**: Sweeper scans expired lease at $T_0$. Owner pod sends heartbeat at $T_1$. Sweeper executes takeover at $T_2$. | If takeover CAS checks only `fencing_token`, sweeper steals lease from a healthy, active worker. | **R2, Bug 4** | **Mitigation:** Atomic lease check inside update: `UPDATE saga_instances SET lease_owner = :new, lease_until = NOW() + INTERVAL '30s' WHERE id = :id AND fencing_token = :token AND lease_until < NOW()`. If the owner renewed the heartbeat, `lease_until < NOW()` evaluates false, returning 0 rows. |
| **FAIL-CR-005** | **Zombie Worker Split-Brain Dispatch**: Worker A loses network connection to PostgreSQL (cannot renew lease) but retains outbound Internet connection to downstream partners. Worker B takes over lease. | Both Worker A and Worker B invoke downstream APIs simultaneously. | **R4, Bug 8** | **Mitigation:** Dual-layer defense: (1) Monotonic local self-fencing: Worker A polls local clock; if `now() > lease_until - 5s`, it aborts outbound dispatch. (2) Downstream deduplication: Both workers use identical idempotency keys, ensuring partner executes mutation only once. |
| **FAIL-CR-006** | **Poison Pill Cluster Death Loop**: A corrupted saga step payload causes worker memory corruption or Go runtime panic. Every worker that takes over the lease crashes upon deserialization. | All worker pods in the cluster enter crash loops, leading to a total platform outage. | **R10, Bug 6** | **Mitigation:** Atomic pre-execution takeover counter increment: `UPDATE saga_instances SET takeover_count = takeover_count + 1 WHERE id = :id`. If `takeover_count >= 5`, status is immediately set to `PAUSED_POISON` and processing is aborted before payload deserialization. |
| **FAIL-CR-007** | **Liveness Probe Database Cascading Restarts**: PostgreSQL undergoes a 45-second primary failover. Kubernetes liveness probes tied directly to `db.Ping()` fail across all 50 worker pods. | Kubernetes restarts all 50 pods simultaneously. Upon DB recovery, 50 pods initiate connection bursts, overwhelming the database with connection pool spikes and causing secondary crashes. | **0.1, Liveness Policy** | **Mitigation:** Liveness probes check only local process health and memory limits. Readiness probes check database availability. When the DB fails, pods transition to unready (shedding ingress traffic) but do NOT restart. |
| **FAIL-CR-008** | **Mass Recovery Thundering Herd**: 50,000 sagas were paused during a 1-hour downstream gateway outage. Upon service restoration, recovery sweepers resume all 50,000 sagas in the same second. | Downstream partner APIs are hit with 50,000 requests/sec, causing immediate 429 rate-limiting and cascading downstream collapse. | **R9, Ramp-Up** | **Mitigation:** Token bucket rate limiter applied to recovery sweeper dispatches (`max_recovery_rate = 200 sagas/sec`). Sweeper batches are clamped to `LIMIT 50` with exponential backoff on retry. |

---

## 2. In-Depth Failure Drill: Poison Pill Interception

```mermaid
sequenceDiagram
    autonumber
    participant W1 as Worker 1
    participant DB as PostgreSQL 18
    participant W2 as Worker 2
    participant W3 as Worker 3
    participant Alert as Prometheus Alertmanager

    Note over W1: Processing 'tx-poison' -> PANIC (Nil pointer in payload parser)
    Note over DB: lease_until expires for 'tx-poison' (takeover_count = 1)
    
    W2->>DB: Atomic Takeover (takeover_count increments to 2)
    Note over W2: Deserializes payload -> PANIC (Crash)
    
    Note over DB: lease_until expires again...
    Note over DB: Subsequent workers increment counter to 3, 4...
    
    W3->>DB: Scan expired lease for 'tx-poison'
    W3->>DB: UPDATE saga_instances SET takeover_count = takeover_count + 1 WHERE id = 'tx-poison'
    DB-->>W3: takeover_count = 5
    
    rect rgb(255, 230, 230)
        Note over W3: R10 Circuit Breaker Tripped!
        W3->>DB: UPDATE saga_instances SET status = 'PAUSED_POISON', reason = 'POISON_PILL_PANIC_THRESHOLD' WHERE id = 'tx-poison'
        W3->>Alert: Fire Critical P1 Alert: SagaQuarantinedPoisonPill
        Note over W3: W3 bypasses execution loop; remains healthy!
    end
```

---

## 3. Operator Recovery Runbook for Quarantined Sagas (`PAUSED_POISON`)

When a saga enters `PAUSED_POISON`:

1. **Locate quarantined sagas:**
   ```sql
   SELECT id, tenant_id, workflow_id, takeover_count, updated_at 
   FROM saga_instances 
   WHERE status = 'PAUSED_POISON';
   ```
2. **Inspect step payloads and crash audit trail:**
   ```sql
   SELECT event, details, created_at 
   FROM saga_recovery_audit_log 
   WHERE saga_id = 'tx-poison-id' 
   ORDER BY created_at DESC LIMIT 5;
   ```
3. **Remediate:**
   - **Patch payload via intervention API:**
     `POST /api/v1/sagas/tx-poison-id/intervene/patch-context`
   - **Reset takeover counter and resume:**
     ```sql
     UPDATE saga_instances 
     SET status = 'RUNNING', takeover_count = 0, lease_until = NOW() + INTERVAL '30s' 
     WHERE id = 'tx-poison-id';
     ```


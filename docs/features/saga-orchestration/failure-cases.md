# Saga Orchestration Runtime — Failure Cases, Production Chaos & Mitigations

---

## 1. Production Failure Taxonomy Matrix (v4)

| Failure Mode | Root Cause | Impact in Naive Engines | Mitigation Strategy in Engine v4 |
|---|---|---|---|
| **Post-Pivot Step Failure** | Downstream service fails after the pivot step (e.g. payment charged, but shipping label generation fails). | Naive engine unwinds pre-pivot steps (releases inventory hold) but leaves the charge intact: customer is charged, stock is lost, parcel never ships. | **Pivot-Aware Failure Routing (Invariant S5)**: If the pivot step reaches `SUCCEEDED`, backward compensation is strictly illegal. The orchestrator transitions to `ROLLING_FORWARD` and retries durable timers. If exhausted, it halts in `STUCK_FORWARD` for human review. |
| **Ambiguous Downstream Timeout (504 / Connection Drop)** | Read timeout, gateway 504, or socket reset after TCP request dispatched. | Naive engine marks step `FAILED` and excludes it from rollback; downstream action actually completed, resulting in orphaned resources (e.g. shipping parcel created and delivered for a refunded order). | **Presumptive UNKNOWN State (Invariant S14)**: Ambiguous failures transition to `UNKNOWN`. If retries or reconciliation fail, `UNKNOWN` steps are presumed executed and explicitly included in the compensation chain. |
| **Business-Key Double-Tap / Gateway Retry** | Client clicks "Pay" twice or API gateway retries a slow POST request. | Naive engine creates two independent sagas, debiting the customer twice. | **Relational Constraint (Invariant S13)**: PostgreSQL enforces `UNIQUE (tenant_id, workflow_definition_id, external_id)`. Identical payloads return the existing saga instance (`200 OK`); mismatched payloads abort with `409 Conflict`. |
| **Idempotency Key Collisions on Reverse Calls** | Orchestrator sends identical `Idempotency-Key` to both `/charge` and `/refund` endpoints. | Payment gateway deduplicates the refund request as a replay of the charge, returning the cached 200 OK without issuing the refund. | **Distinct Suffix Protocol (Invariant S6)**: Forward dispatches use `idem:<saga_id>:<step_id>:fwd`; compensations use `idem:<saga_id>:<step_id>:comp`. |
| **In-Flight Drain Race Condition** | Parallel branch B is still executing when branch A fails. | Naive engine builds compensation plan immediately from currently succeeded steps. Branch B completes 200 ms later and is permanently omitted from rollback. | **Drain Barrier (Invariant S15)**: Forward execution halt enforces a drain barrier. All in-flight `DISPATCHED` steps must settle or timeout before deriving the rollback chain. |
| **Split-Brain Worker Collisions** | Worker A undergoes a 40s GC pause; lease expires; Worker B takes over; Worker A awakens and writes stale state. | Corrupted step journal, double-execution of downstream actions. | **Monotonic Fencing Tokens (Invariant S16)**: Every lease claim increments `fencing_token`. Every database write enforces `AND fencing_token = :my_token`. Stale workers receive 0 rows updated and terminate immediately. |
| **PCI/PII Leakage to Loki** | Orchestrator logs request and response payloads to standard out on state transitions. | Cardholder PANs, billing addresses, and credentials leak into centralized logging sinks (Loki, Elasticsearch). | **Structured Payload Scrubbing (Invariant S12)**: Step payloads are restricted to PostgreSQL JSONB columns. Standard out logs contain only opaque IDs, timestamps, states, and sanitized error codes. |
| **Hot-Row CAS Bottleneck** | Parallel steps complete and attempt concurrent CAS updates on the parent `saga_instances` row. | Database serialization failures, excessive transaction rollbacks, worker starvation. | **Granular Row Journaling (Invariant S7)**: Step completions update only `saga_step_executions` rows. The parent saga row is updated only upon global state transitions. |

---

## 2. Deep-Dive Chaos Scenarios & Formal Mitigations

### 2.1 The Post-Pivot Disaster
* **Scenario**: A travel booking workflow contains `reserve_flight` (step 1), `reserve_hotel` (step 2), `charge_card` (pivot step 3), and `issue_tickets` (step 4). Steps 1, 2, and 3 succeed. Step 4 fails due to a downstream airline ticketing API 500 error.
* **Catastrophic Failure in v3**: The engine rolled back Step 2 and Step 1. The customer credit card remained charged \$2,500, but flight and hotel holds were cancelled.
* **Engine v4 Resolution**:
  1. The failure routing evaluator checks the pivot state in PostgreSQL: `charge_card.state == SUCCEEDED`.
  2. Failure routing returns `FORWARD_ONLY`.
  3. The engine transitions the saga to `ROLLING_FORWARD`.
  4. Durable retries continue with exponential backoff and full jitter.
  5. If the ticketing API remains down past the retry budget, the saga moves to `STUCK_FORWARD` and pages the on-call operator. Pre-pivot steps are NEVER compensated.

### 2.2 The Ambiguous 504 Timeout Dilemma
* **Scenario**: The orchestrator dispatches `POST /v1/shipments/create` with a 15-second timeout. At second 15, the gateway returns `504 Gateway Timeout`. Did the shipping partner create the shipment?
* **Catastrophic Failure in v3**: Step was marked `FAILED`. Since only `SUCCEEDED` steps were rolled back, the shipping label was created by the carrier, but never cancelled by the orchestrator.
* **Engine v4 Resolution**:
  1. Step dispatcher classifies 504 as `AmbiguousStepError`.
  2. Step execution state transitions to `UNKNOWN`.
  3. Orchestrator attempts reconciliation or retries with the identical `:fwd` idempotency key.
  4. If unresolved when rollback is initiated, `UNKNOWN` steps are included in the compensation set.
  5. The compensation endpoint is dispatched with `idem:<saga_id>:<step_id>:comp` to void any potential label.

### 2.3 The Gateway Replay Double-Debit Incident
* **Scenario**: A user clicks "Checkout" on an e-commerce platform. Due to mobile network lag, the client sends two identical HTTP POST requests within 50 milliseconds.
* **Engine v4 Resolution**:
  1. PostgreSQL evaluates `CONSTRAINT saga_instances_tenant_wf_ext_uk UNIQUE (tenant_id, workflow_definition_id, external_id)`.
  2. Request 1 inserts the saga row with state `CREATED` and `input_hash`.
  3. Request 2 detects duplicate key collision:
     - If `input_hash` matches: Returns HTTP 200 OK with the existing saga instance metadata.
     - If `input_hash` differs: Returns HTTP 409 Conflict (`E_EXTERNAL_ID_CONFLICT`).
  4. Only a single saga instance executes; duplicate charges are physically impossible.

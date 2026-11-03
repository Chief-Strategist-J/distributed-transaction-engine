# Saga Orchestration Runtime — Failure Cases, Chaos Scenarios & Mitigations

---

## 1. Failure Taxonomy Matrix

| Failure Mode | Root Cause | Impact | Mitigation Strategy |
|---|---|---|---|
| **Downstream Timeout (504 / Connection Reset)** | Third-party microservice hangs or drops network packet. | Step execution state unknown. | Client transmits `Idempotency-Key`. Worker retries using exponential backoff with full jitter. If retries exhaust, step marks `FAILED` and initiates reverse compensation. |
| **Worker Process Crash Mid-Step** | Node OOM, kernel panic, or Kubernetes pod eviction while step is in flight. | In-flight saga left in `RUNNING` or `COMPENSATING`. | Persistent `saga_step_executions` journal. Background recovery daemon selects uncompleted sagas (`FOR UPDATE SKIP LOCKED`), checks fencing tokens, queries downstream status via idempotency keys, and resumes execution. |
| **Partial Compensation Failure** | A downstream compensation endpoint fails with 5xx or network partition during rollback. | Distributed system enters an inconsistent, partially compensated state. | Compensations must never give up: retries with capped backoff continue. If retries hit fatal limit, event is enqueued to Dead Letter Queue (DLQ) for human operator review. |
| **Split-Brain Concurrency Collision** | Two distributed worker pods attempt to advance the same saga simultaneously. | Duplicate step execution, race condition. | Optimistic Concurrency Control (CAS) with monotonic `version` increment on `saga_instances`. Second worker receives `409 Conflict` and aborts. |
| **Version Drift Mutation** | An operator deploys an updated workflow definition version while an older saga is running. | Schema mismatch panics or incompatible dependency resolution. | Sagas freeze immutable foreign keys to `(workflow_definition_id, workflow_version, spec_hash)`. Running sagas ignore subsequent workflow versions. |

---

## 2. Deep-Dive Chaos Mitigations

### 2.1 The "Lost HTTP Response" Dilemma
When an orchestrator issues `POST /charge` to a payment gateway and the TCP socket drops before the response is received, the orchestrator cannot know whether the charge succeeded or failed.
* **Mitigation**:
  1. The orchestrator embeds `Idempotency-Key: idemp_<saga_id>_<step_id>`.
  2. The retry worker repeats the identical request with the identical key.
  3. The downstream service returns the cached original result without double-charging.

### 2.2 The "Dual Failure" During Compensation
If Step 3 fails, triggering rollback of Step 2 and Step 1, what happens if the rollback endpoint of Step 2 also fails?
* **Mitigation**:
  1. The saga stays in `COMPENSATING` state.
  2. The failed compensation step remains in `COMPENSATING` or `FAILED_COMPENSATION` with an incremented attempt counter.
  3. The orchestrator never marks the entire saga as `FAILED` (terminal) until all executed non-pivot steps have confirmed successful compensation.


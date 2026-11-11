# ADR-0005: Dependency-Directed Dynamic DAG Compensation Engine, Pivot Invariant Boundaries & Explicit Reversal Protocols

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-11-04 |
| Deciders | Lead Systems Architect & Core Architecture Guild |
| Related Docs | [Business Logic & Decision Tree v2](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/compensation-engine/business-logic.md) · [Diagrams & Component Models](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/compensation-engine/diagrams.md) · [Failure Cases & Mitigations](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/docs/features/compensation-engine/failure-cases.md) · [OpenAPI v1](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/src/api/contracts/openapi/v1.yaml) |
| Target Feature | [Compensation Engine Feature](file:///home/btpl-lap-22/live/System-Architect/distributed-transaction-engine/src/features/compensation-engine) |

---

## 1. Context & Problem Statement

In distributed, event-driven architectures coordinating across heterogeneous microservices (e.g., flight bookings, hotel reservations, payment authorizations, inventory holds, logistics manifests), cross-network ACID transactions and Two-Phase Commit (2PC) protocols introduce prohibitive lock contention, synchronous network latency, and cascading single points of failure. The platform uses the Saga pattern to achieve eventual consistency.

When a distributed saga encounters an unrecoverable failure, unexpected cancellation, or deadline expiration, the system must execute backward recovery to reverse forward side effects. A naive implementation—relying on strict sequential reverse unwinding ($N \to N-1 \to 0$) and naive parameter inversion (e.g. multiplying forward amounts by $-1$)—introduces fatal production failure modes:

1. **The Post-Pivot Catastrophe (Direct Financial Loss)**: If an irreversible pivot operation (e.g., capture of customer funds or warehouse box sealing) succeeds, backward compensation is logically and legally invalid. Executing compensation unwinds pre-pivot steps (releasing inventory) while leaving settled funds untouched or attempting unexecutable refunds on unsettled authorizations.
2. **Sequential Lockup & Cascading Hold Expirations**: If an independent hotel reservation reversal hangs due to a 3rd-party gateway outage, linear reverse execution freezes the entire compensation process. Unrelated steps (such as car rental cancellations) sit idle until their cancellation deadlines expire, incurring real-world non-refundable penalty charges.
3. **Ambiguous Forward Outcomes ("Ghost Mutations")**: Network timeouts (HTTP 504, TCP resets, connection drops) do not indicate whether the downstream mutation occurred. Marking a timed-out forward step as `FAILED` and omitting it from the compensation plan creates zombie resources (e.g., booked flights never cancelled).
4. **Parameter Inversion Hallucinations**: Real-world reversals are not symmetric inverses of forward actions. A refund requires the specific `authorization_code` and `capture_reference` produced as output by the forward payment step, plus accurate calculations of fees and FX rates. Inverting forward inputs guarantees payment gateway schema rejection.
5. **Idempotency Key Collisions Between Forward and Reverse Dispatches**: Reusing identical idempotency keys for both forward and reverse calls causes partner payment processors to return cached forward `200 OK` responses, resulting in phantom reversals where no refund actually occurs.
6. **Late-Arriving Forward Confirmations**: A forward step assumed dead during a network partition may finish execution minutes after backward compensation has completed, re-introducing orphaned side effects into an already closed saga.

---

## 2. Decision Drivers & Architecture Invariants (CE-01 – CE-15)

The Compensation Engine is strictly governed by fifteen architectural invariants:

* **CE-01 (Pre-Compensation Drain Barrier)**: Compensation must never dispatch while any relevant forward step remains in an in-flight (`DISPATCHED`) or ambiguous (`UNKNOWN`) state. All in-flight branches must be drained, cancelled, or actively probed to terminal resolution before the compensation plan is finalized.
* **CE-02 (Pivot Inviolability)**: Once a designated pivot step achieves terminal `SUCCEEDED` status, backward compensation is mathematically and operationally prohibited:
  $$\forall s \in \text{Saga}, \quad \text{Status}(\text{Pivot}(s)) = \text{SUCCEEDED} \implies \text{Mode}(s) := \text{ROLL\_FORWARD}$$
  Subsequent failures trigger durable forward retries or human operational escalation; pre-pivot steps are never reversed.
* **CE-03 (Output-Driven Compensation Payloads)**: Compensation payloads are derived strictly from the recorded, immutable **output context** of forward step executions. Inverting forward input payloads is strictly prohibited.
* **CE-04 (Deterministic Independent Keying)**: Forward and reverse dispatches use disjoint, cryptographically isolated idempotency namespaces:
  $$\text{Key}_{\text{fwd}} = \text{idem:fwd:} \langle \text{tenant\_id} \rangle : \langle \text{saga\_id} \rangle : \langle \text{step\_id} \rangle : \langle \text{attempt} \rangle$$
  $$\text{Key}_{\text{comp}} = \text{idem:comp:} \langle \text{tenant\_id} \rangle : \langle \text{saga\_id} \rangle : \langle \text{step\_id} \rangle : \langle \text{attempt} \rangle$$
* **CE-05 (Ambiguity Probing Protocol)**: Steps in an `UNKNOWN` state must be probed via a dedicated active query API or partner reconciliation endpoint. If downstream status is unverifiable, the engine presumes execution occurred and mandates compensation.
* **CE-06 (Strongly Typed Outcome Contracts)**: Downstream step workers must return explicit, typed compensation outcomes: `SUCCEEDED`, `ALREADY_COMPENSATED`, `NOTHING_TO_COMPENSATE`, `NOT_COMPENSABLE`, `FAILED_RETRYABLE`, or `FAILED_TERMINAL`. Protocol status (HTTP 200/500) alone is never trusted.
* **CE-07 (Branch Independence & Dynamic Fan-Out)**: Disjoint branches in the reverse DAG compensate concurrently. A failure in one branch never blocks or halts the rollback of independent branches.
* **CE-08 (Late Forward Arrival Containment)**: If a late forward success arrives for a saga currently in `COMPENSATING` or `COMPENSATED` state, the engine dynamically instantiates an immediate reactive compensation job for that step, preventing state drift.
* **CE-09 (Static Definition Validation)**: Every workflow definition submitted to the engine must define an explicit, schema-validated compensation action for every non-pivot, non-retriable step at registration time.
* **CE-10 (Residual Financial Ledgering)**: If an external system rejects reversal with `NOT_COMPENSABLE` (e.g. customer boarded flight), the engine enters `COMPENSATION_STUCK`, records the unreversed delta in the append-only `compensation_residuals` ledger, and halts automatic actions.
* **CE-11 (Zero In-Memory Volatility)**: Every compensation state transition, retry counter, and outcome payload commits to PostgreSQL before outbound network I/O via the Transactional Outbox.
* **CE-12 (Dual-Token CAS Fencing)**: All compensation row updates check `(version = :expected_version AND epoch = :worker_epoch)` to eliminate zombie worker updates during failovers.
* **CE-13 (Compensations Have Dedicated Time Budgets)**: Forward saga deadlines do not truncate compensation timeouts. The compensation phase receives an independent, configurable deadline budget.
* **CE-14 (Monotonic State Absorption)**: Terminal compensation states (`COMPENSATED`, `COMPENSATED_WITH_RESIDUALS`) are strictly absorbing; no subsequent transition can revert them.
* **CE-15 (Tenant Boundary Enforcement)**: Multi-tenant partitioning via composite primary keys `(tenant_id, ...)` is enforced at the database, outbox partition, and Redis cache layers.

---

## 3. Considered Architectural Options

### Option 1: Static Reverse-Sequential Rollback
* **Architecture**: The orchestrator traverses the executed step history in reverse linear order ($N-1, N-2, \dots, 0$), executing compensations synchronously one by one.
* **Pros**: Simple conceptual model; trivial state tracking via a decrementing pointer.
* **Cons**:
  - Unrelated independent steps (e.g., car rental, event tickets) are serialized, dramatically extending hold times.
  - A transient failure in step $K$ halts rollback of steps $0 \dots K-1$, risking partner hold timeouts.
  - Fails to reflect DAG branch dependencies.
* **Verdict**: **Rejected (Unacceptable Operational Latency & Contention)**.

### Option 2: Choreographed Decentralized Compensation
* **Architecture**: Services publish `StepFailed` events; other services listen to the event bus and trigger local compensations independently without a central coordinator.
* **Pros**: Decentralized; no single orchestrator component.
* **Cons**:
  - Zero centralized visibility into compensation progress or overall saga state.
  - High probability of cyclic dependencies and split-brain states when multiple branches fail simultaneously.
  - Implementing pivot point of no return (CE-02) across 10 distributed services in pure choreography requires complex distributed consensus (Raft/Paxos).
* **Verdict**: **Rejected for Core Financial Transactions**.

### Option 3: Two-Phase Commit / Distributed XA Locking
* **Architecture**: Services lock local rows across the network until the entire distributed transaction is ready to commit.
* **Pros**: Strong immediate consistency; standard ACID guarantees.
* **Cons**:
  - Services must hold open database locks across unreliable WAN networks, causing thread pool exhaustion and catastrophic lock contention.
  - Incompatible with 3rd-party SaaS APIs (Stripe, Twilio, airlines) that do not expose XA interfaces.
* **Verdict**: **Fundamentally Incompatible with Microservices**.

### Option 4: Dependency-Directed Dynamic DAG Compensation with Relational Fencing & Transactional Outbox (Selected)
* **Architecture**: Orchestrator dynamically constructs a reverse execution graph based on completed forward steps, respecting topological dependencies. Independent steps are dispatched concurrently via PostgreSQL transactional outbox tables with monotonic worker epoch fencing.
* **Pros**:
  - Maximum safe parallelism across independent service boundaries.
  - Provable pivot boundary enforcement.
  - Bulletproof crash recovery and zero dual-write bugs.
* **Verdict**: **Adopted**.

---

## 4. Entity-Relationship Model & Relational Schema

```sql
-- Compensation execution status enum
CREATE TYPE compensation_status AS ENUM (
    'PENDING',
    'DRAINING_FORWARD',
    'DISPATCHED',
    'SUCCEEDED',
    'ALREADY_COMPENSATED',
    'NOTHING_TO_COMPENSATE',
    'NOT_COMPENSABLE',
    'FAILED_RETRYABLE',
    'FAILED_TERMINAL'
);

-- Core compensation execution records
CREATE TABLE IF NOT EXISTS saga_compensations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id VARCHAR(64) NOT NULL,
    saga_id VARCHAR(64) NOT NULL,
    step_id VARCHAR(64) NOT NULL,
    forward_execution_id UUID NOT NULL,
    idempotency_key VARCHAR(128) NOT NULL,
    status compensation_status NOT NULL DEFAULT 'PENDING',
    attempt_count INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 5,
    version INT NOT NULL DEFAULT 1,
    epoch BIGINT NOT NULL DEFAULT 1,
    input_payload JSONB NOT NULL,
    output_payload JSONB,
    last_error_code VARCHAR(64),
    last_error_message TEXT,
    dispatched_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    CONSTRAINT uq_comp_idempotency UNIQUE (tenant_id, idempotency_key),
    CONSTRAINT fk_comp_saga FOREIGN KEY (saga_id) REFERENCES saga_instances(id) ON DELETE CASCADE
);

-- Reverse dependency edges for parallel/DAG workflows
CREATE TABLE IF NOT EXISTS saga_compensation_dependencies (
    tenant_id VARCHAR(64) NOT NULL,
    saga_id VARCHAR(64) NOT NULL,
    step_id VARCHAR(64) NOT NULL,
    prerequisite_step_id VARCHAR(64) NOT NULL,
    PRIMARY KEY (tenant_id, saga_id, step_id, prerequisite_step_id),
    CONSTRAINT fk_dep_saga FOREIGN KEY (saga_id) REFERENCES saga_instances(id) ON DELETE CASCADE
);

-- Residual financial liabilities ledger (CE-10)
CREATE TABLE IF NOT EXISTS saga_compensation_residuals (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id VARCHAR(64) NOT NULL,
    saga_id VARCHAR(64) NOT NULL,
    step_id VARCHAR(64) NOT NULL,
    compensation_id UUID NOT NULL REFERENCES saga_compensations(id),
    residual_amount NUMERIC(18, 4),
    currency VARCHAR(3),
    reason TEXT NOT NULL,
    operator_assigned VARCHAR(128),
    resolved BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ
);

-- Indexes for performance and high-throughput polling
CREATE INDEX IF NOT EXISTS idx_comp_lookup 
    ON saga_compensations (tenant_id, saga_id, step_id);

CREATE INDEX IF NOT EXISTS idx_comp_pending_poll 
    ON saga_compensations (tenant_id, status, updated_at) 
    WHERE status IN ('PENDING', 'FAILED_RETRYABLE');
```

---

## 5. State Machine Transition Matrix

| Current State | Trigger Event | Guard Condition | Next State | Outbound Side Effect | Idempotency / Safety |
|---|---|---|---|---|---|
| `RUNNING` | `FAILURE_DETECTED` / `CANCEL_REQUESTED` | `pivot_succeeded == FALSE` | `COMPENSATING` | Barrier check initiated; outbox event emitted | CAS on `saga_instances` version |
| `RUNNING` | `FAILURE_DETECTED` | `pivot_succeeded == TRUE` | `ROLLING_FORWARD` | Alert operator; dispatch forward retries | CE-02 pivot gate |
| `COMPENSATING` | `FORWARD_DRAIN_RESOLVED` | `all_dispatched_resolved == TRUE` | `COMPENSATING` | Build reverse DAG; dispatch ready compensations | Pre-compensation barrier (CE-01) |
| `PENDING` | `DISPATCH_STEP` | `in_degree == 0` | `DISPATCHED` | Insert outbox row; bump epoch | Unique `idempotency_key` |
| `DISPATCHED` | `WORKER_SUCCESS` | None | `SUCCEEDED` | Mark completed; decrement dependent in-degrees | Typed response validation |
| `DISPATCHED` | `ALREADY_COMPENSATED` | None | `ALREADY_COMPENSATED` | Treated as success; decrement dependent in-degrees | Provider idempotency |
| `DISPATCHED` | `NOTHING_TO_COMPENSATE` | Forward step was no-op | `NOTHING_TO_COMPENSATE` | Decrement dependent in-degrees | Skip unnecessary mutations |
| `DISPATCHED` | `NOT_COMPENSABLE` | Provider rejected reversal | `NOT_COMPENSABLE` | Record in `compensation_residuals`; trigger P1 alert | Halts auto-retry on this step |
| `DISPATCHED` | `NETWORK_ERROR` | `attempt_count < max_attempts` | `FAILED_RETRYABLE` | Schedule timer with full exponential jitter | Jitter backoff algorithm |
| `DISPATCHED` | `NETWORK_ERROR` | `attempt_count >= max_attempts` | `FAILED_TERMINAL` | Route to DLQ; transition saga to `COMPENSATION_STUCK` | Operator review required |
| `COMPENSATING` | `ALL_STEPS_TERMINAL` | `count(residuals) == 0` | `COMPENSATED` | Emit `SAGA_COMPENSATED` lifecycle event | Absorbing terminal state |
| `COMPENSATING` | `ALL_STEPS_TERMINAL` | `count(residuals) > 0` | `COMPENSATED_WITH_RESIDUALS` | Emit P2 audit compliance alert | Requires manual ledgering |

---

## 6. Hexagonal Architecture Ports & Go Contracts

```go
package compensation

import (
	"context"
	"time"
	"github.com/google/uuid"
)

type StepCompensationOutcome string

const (
	OutcomeSucceeded           StepCompensationOutcome = "SUCCEEDED"
	OutcomeAlreadyCompensated  StepCompensationOutcome = "ALREADY_COMPENSATED"
	OutcomeNothingToCompensate StepCompensationOutcome = "NOTHING_TO_COMPENSATE"
	OutcomeNotCompensable      StepCompensationOutcome = "NOT_COMPENSABLE"
	OutcomeFailedRetryable     StepCompensationOutcome = "FAILED_RETRYABLE"
	OutcomeFailedTerminal      StepCompensationOutcome = "FAILED_TERMINAL"
)

type CompensationPlanItem struct {
	ID                 uuid.UUID               `json:"id"`
	StepID             string                  `json:"step_id"`
	ForwardExecutionID uuid.UUID               `json:"forward_execution_id"`
	IdempotencyKey     string                  `json:"idempotency_key"`
	Payload            map[string]interface{}  `json:"payload"`
	Dependencies       []string                `json:"dependencies"`
	Status             StepCompensationOutcome `json:"status"`
}

// Engine Port defines domain orchestrator operations
type EnginePort interface {
	InitiateCompensation(ctx context.Context, tenantID, sagaID, reason string) error
	ProcessWorkerOutcome(ctx context.Context, tenantID, sagaID, stepID string, outcome StepCompensationOutcome, payload map[string]interface{}) error
	ResolveLateSuccess(ctx context.Context, tenantID, sagaID, stepID string, forwardOutput map[string]interface{}) error
}

// Repository Port abstracts PostgreSQL storage operations
type RepositoryPort interface {
	GetSagaPivotStatus(ctx context.Context, tenantID, sagaID string) (bool, error)
	StoreCompensationPlan(ctx context.Context, tenantID, sagaID string, items []CompensationPlanItem) error
	GetExecutableSteps(ctx context.Context, tenantID, sagaID string) ([]CompensationPlanItem, error)
	UpdateStepStatusCAS(ctx context.Context, tenantID, sagaID, stepID string, expectedVersion int, expectedEpoch int64, outcome StepCompensationOutcome, output map[string]interface{}) (bool, error)
	RecordResidual(ctx context.Context, tenantID, sagaID, stepID string, compID uuid.UUID, amount float64, currency, reason string) error
}

// Dispatcher Port decouples dispatching via the Transactional Outbox
type DispatcherPort interface {
	DispatchCompensationCommand(ctx context.Context, tenantID, sagaID string, item CompensationPlanItem) error
}
```

---

## 7. Metrics & Observability

The compensation engine exports the following Prometheus metrics:

* `saga_compensation_duration_seconds{tenant_id, saga_type, status}`: Histogram of complete compensation cycle latency.
* `saga_compensation_step_attempts_total{tenant_id, step_id, outcome}`: Counter tracking individual step compensation statuses.
* `saga_compensation_stuck_gauge{tenant_id}`: Gauge tracking sagas stuck waiting for human intervention.
* `saga_compensation_residual_amount_total{tenant_id, currency}`: Counter accumulating unresolved financial losses due to `NOT_COMPENSABLE` rejections.

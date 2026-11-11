package tests

import (
	"context"
	"strings"
	"testing"
	"time"

	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
	sagaRules "distributed-transaction-engine/src/features/saga-orchestration/domain/rules"
	sagaAdapters "distributed-transaction-engine/src/features/saga-orchestration/infrastructure/adapters"
	sagaService "distributed-transaction-engine/src/features/saga-orchestration/service"
	workflowContracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
	workflowAdapters "distributed-transaction-engine/src/features/workflow-definitions/infrastructure/adapters"
	workflowService "distributed-transaction-engine/src/features/workflow-definitions/service"
	"distributed-transaction-engine/src/shared/errors"
)

func TestSagaService_DecoupledIngressReturnsCreatedAndPendingSteps(t *testing.T) {
	ctx := context.Background()
	tenantID := "tenant_ingress_01"

	workflowRepo := workflowAdapters.NewMemoryWorkflowDefinitionsRepositoryAdapter()
	wfService := workflowService.NewWorkflowDefinitionsService(workflowRepo)

	_, err := wfService.RegisterWorkflow(ctx, workflowContracts.CreateWorkflowDefinitionInput{
		TenantID:      tenantID,
		Name:          "async-flow",
		Version:       1,
		Source:        "TEST",
		ExternalID:    "ext_async_wf_01",
		CorrelationID: "c1000000-0000-0000-0000-000000000101",
		Steps: []workflowContracts.StepDefinition{
			{
				StepID:               "step_1",
				StepName:             "Step 1",
				IsPivot:              false,
				ActionType:           "REST",
				ActionEndpoint:       "http://service/1",
				CompensationAction:   "UNDO_1",
				CompensationEndpoint: "http://service/undo-1",
				TimeoutSeconds:       30,
				Dependencies:         []string{},
			},
		},
	})
	if err != nil {
		t.Fatalf("workflow registration failed: %v", err)
	}

	sagaRepo := sagaAdapters.NewMemorySagaRepositoryAdapter()
	dispatcher := &MockStepDispatcher{}
	svc := sagaService.NewSagaOrchestrationService(sagaRepo, workflowRepo, dispatcher)

	instance, err := svc.StartSaga(ctx, sagaContracts.ExecuteSagaInput{
		TenantID:        tenantID,
		WorkflowName:    "async-flow",
		WorkflowVersion: 1,
		Source:          "INGRESS_TEST",
		ExternalID:      "ext_ingress_order_001",
		CorrelationID:   "c2000000-0000-0000-0000-000000000101",
		InitialPayload:  map[string]interface{}{"data": "ok"},
	})
	if err != nil {
		t.Fatalf("StartSaga failed: %v", err)
	}

	if instance.CurrentState != sagaContracts.SagaStateCreated {
		t.Fatalf("expected state CREATED, got %s", instance.CurrentState)
	}
	if instance.FencingToken != 0 {
		t.Fatalf("expected fencingToken 0 upon creation, got %d", instance.FencingToken)
	}
	if len(instance.Steps) != 1 || instance.Steps[0].ExecutionState != sagaContracts.StepStatePending {
		t.Fatalf("expected pending steps, got: %v", instance.Steps)
	}
	if len(dispatcher.executedSteps) != 0 {
		t.Fatalf("expected zero dispatched steps on decoupled ingress, got: %v", dispatcher.executedSteps)
	}
}

func TestSagaPoller_ClaimsLeaseAdvancesFencingAndExecutes(t *testing.T) {
	ctx := context.Background()
	tenantID := "tenant_poller_01"

	workflowRepo := workflowAdapters.NewMemoryWorkflowDefinitionsRepositoryAdapter()
	wfService := workflowService.NewWorkflowDefinitionsService(workflowRepo)

	_, err := wfService.RegisterWorkflow(ctx, workflowContracts.CreateWorkflowDefinitionInput{
		TenantID:      tenantID,
		Name:          "poller-flow",
		Version:       1,
		Source:        "TEST",
		ExternalID:    "ext_poller_wf_01",
		CorrelationID: "c1000000-0000-0000-0000-000000000102",
		Steps: []workflowContracts.StepDefinition{
			{
				StepID:               "poll_step",
				StepName:             "Poll Step",
				IsPivot:              false,
				ActionType:           "REST",
				ActionEndpoint:       "http://service/poll",
				CompensationAction:   "UNDO_POLL",
				CompensationEndpoint: "http://service/undo-poll",
				TimeoutSeconds:       30,
				Dependencies:         []string{},
			},
		},
	})
	if err != nil {
		t.Fatalf("workflow registration failed: %v", err)
	}

	sagaRepo := sagaAdapters.NewMemorySagaRepositoryAdapter()
	dispatcher := &MockStepDispatcher{}
	svc := sagaService.NewSagaOrchestrationService(sagaRepo, workflowRepo, dispatcher)

	created, err := svc.StartSaga(ctx, sagaContracts.ExecuteSagaInput{
		TenantID:        tenantID,
		WorkflowName:    "poller-flow",
		WorkflowVersion: 1,
		Source:          "POLLER_TEST",
		ExternalID:      "ext_poller_order_001",
		CorrelationID:   "c2000000-0000-0000-0000-000000000102",
		InitialPayload:  map[string]interface{}{"val": 42},
	})
	if err != nil {
		t.Fatalf("StartSaga failed: %v", err)
	}

	poller := sagaService.NewSagaPoller(sagaRepo, svc, "worker-node-1", 30*time.Second)

	worked, err := poller.PollAndExecuteNext(ctx)
	if err != nil {
		t.Fatalf("PollAndExecuteNext failed: %v", err)
	}
	if !worked {
		t.Fatalf("expected poller to pick up created saga")
	}

	persisted, err := sagaRepo.GetSagaByID(ctx, tenantID, created.ID)
	if err != nil {
		t.Fatalf("GetSagaByID failed: %v", err)
	}
	if persisted.CurrentState != sagaContracts.SagaStateCompleted {
		t.Fatalf("expected completed saga after poller execution, got: %s", persisted.CurrentState)
	}
	if persisted.FencingToken != 1 {
		t.Fatalf("expected fencingToken to be 1, got %d", persisted.FencingToken)
	}
}

func TestSagaService_FencingConflictRejectsStaleWorker(t *testing.T) {
	ctx := context.Background()
	tenantID := "tenant_fencing_01"

	sagaRepo := sagaAdapters.NewMemorySagaRepositoryAdapter()
	inst := &sagaContracts.SagaInstance{
		ID:           "sga_fencing_id_01",
		PublicID:     "sga_00000000000000000000000001",
		TenantID:     tenantID,
		CurrentState: sagaContracts.SagaStateCreated,
		Version:      1,
		FencingToken: 0,
		DeadlineAt:   time.Now().UTC().Add(30 * time.Minute).Format(time.RFC3339Nano),
	}

	if err := sagaRepo.CreateSaga(ctx, inst); err != nil {
		t.Fatalf("CreateSaga failed: %v", err)
	}

	leasedA, err := sagaRepo.AcquireLeaseForSaga(ctx, tenantID, inst.ID, "worker-A", 10*time.Second)
	if err != nil {
		t.Fatalf("worker-A lease failed: %v", err)
	}
	if leasedA.FencingToken != 1 {
		t.Fatalf("expected token 1 for worker-A, got %d", leasedA.FencingToken)
	}

	leasedB, err := sagaRepo.AcquireLeaseForSaga(ctx, tenantID, inst.ID, "worker-B", 10*time.Second)
	if err != nil {
		t.Fatalf("worker-B lease failed: %v", err)
	}
	if leasedB.FencingToken != 2 {
		t.Fatalf("expected token 2 for worker-B, got %d", leasedB.FencingToken)
	}

	err = sagaRepo.UpdateSagaStateWithFencing(ctx, tenantID, inst.ID, leasedA.Version, leasedA.FencingToken, sagaContracts.SagaStateRunning)
	if err == nil {
		t.Fatalf("expected fencing conflict error for worker-A with stale token")
	}

	appErr, ok := err.(*errors.ApplicationError)
	if !ok || appErr.StatusCode != 409 {
		t.Fatalf("expected HTTP 409 Conflict, got: %v", err)
	}
}

func TestSagaService_CircuitBreakerBlocksCascadingFailures(t *testing.T) {
	cb := sagaRules.NewActionCircuitBreaker(3, 500*time.Millisecond, 2)

	if err := cb.AcquirePermission(); err != nil {
		t.Fatalf("initial acquire failed: %v", err)
	}
	cb.ReleasePermission(errors.NewInternalError("fail 1"))

	if err := cb.AcquirePermission(); err != nil {
		t.Fatalf("second acquire failed: %v", err)
	}
	cb.ReleasePermission(errors.NewInternalError("fail 2"))

	if err := cb.AcquirePermission(); err != nil {
		t.Fatalf("third acquire failed: %v", err)
	}
	cb.ReleasePermission(errors.NewInternalError("fail 3"))

	if cb.GetState() != sagaRules.CircuitStateOpen {
		t.Fatalf("expected circuit breaker state OPEN after 3 failures, got: %s", cb.GetState())
	}

	if err := cb.AcquirePermission(); err == nil {
		t.Fatalf("expected permission to be rejected when circuit breaker is OPEN")
	}

	time.Sleep(600 * time.Millisecond)

	if err := cb.AcquirePermission(); err != nil {
		t.Fatalf("expected permission allowed after cooldown (HALF_OPEN), got: %v", err)
	}
	cb.ReleasePermission(nil)

	if cb.GetState() != sagaRules.CircuitStateClosed {
		t.Fatalf("expected circuit breaker to recover to CLOSED after successful probe, got: %s", cb.GetState())
	}
}

func TestSagaService_DurableRetriesHonorsNextAttemptAt(t *testing.T) {
	futureTime := sagaRules.FormatNextAttemptTimestamp(1 * time.Hour)
	if sagaRules.IsRetryEligible(futureTime) {
		t.Fatalf("future attempt timestamp should NOT be retry eligible")
	}

	pastTime := sagaRules.FormatNextAttemptTimestamp(-1 * time.Minute)
	if !sagaRules.IsRetryEligible(pastTime) {
		t.Fatalf("past attempt timestamp must be retry eligible")
	}

	backoff1 := sagaRules.ComputeExponentialBackoff(1, 1*time.Second, 10*time.Second)
	if backoff1 != 1*time.Second {
		t.Fatalf("expected 1s backoff for attempt 1, got %v", backoff1)
	}

	backoff3 := sagaRules.ComputeExponentialBackoff(3, 1*time.Second, 10*time.Second)
	if backoff3 != 4*time.Second {
		t.Fatalf("expected 4s backoff for attempt 3, got %v", backoff3)
	}

	backoffMax := sagaRules.ComputeExponentialBackoff(10, 1*time.Second, 10*time.Second)
	if backoffMax != 10*time.Second {
		t.Fatalf("expected capped 10s backoff, got %v", backoffMax)
	}
}

type recoveryFixture struct {
	repo       *sagaAdapters.MemorySagaRepositoryAdapter
	svc        *sagaService.SagaOrchestrationService
	dispatcher *MockStepDispatcher
	tenantID   string
	sagaID     string
}

func newRecoveryFixture(t *testing.T, tenantID string) recoveryFixture {
	t.Helper()
	ctx := context.Background()
	workflowRepo := workflowAdapters.NewMemoryWorkflowDefinitionsRepositoryAdapter()
	steps := make([]workflowContracts.StepDefinition, 0, 3)
	for index, id := range []string{"s1", "s2", "s3"} {
		dependencies := []string{}
		if index > 0 {
			dependencies = []string{steps[index-1].StepID}
		}
		steps = append(steps, workflowContracts.StepDefinition{
			StepID: id, StepName: id, ActionType: "REST", ActionEndpoint: "http://service/" + id,
			CompensationAction: "UNDO_" + id, CompensationEndpoint: "http://service/undo-" + id, TimeoutSeconds: 30, Dependencies: dependencies,
		})
	}
	_, err := workflowService.NewWorkflowDefinitionsService(workflowRepo).RegisterWorkflow(ctx, workflowContracts.CreateWorkflowDefinitionInput{
		TenantID: tenantID, Name: "recovery-flow", Version: 1, Source: "TEST", ExternalID: "ext_recovery_wf_" + tenantID,
		CorrelationID: "c1000000-0000-0000-0000-000000000201", Steps: steps,
	})
	if err != nil {
		t.Fatalf("workflow registration failed: %v", err)
	}
	repo := sagaAdapters.NewMemorySagaRepositoryAdapter()
	dispatcher := &MockStepDispatcher{}
	svc := sagaService.NewSagaOrchestrationService(repo, workflowRepo, dispatcher)
	created, err := svc.StartSaga(ctx, sagaContracts.ExecuteSagaInput{
		TenantID: tenantID, WorkflowName: "recovery-flow", WorkflowVersion: 1, Source: "TEST",
		ExternalID: "ext_recovery_order_" + tenantID, CorrelationID: "c2000000-0000-0000-0000-000000000201", InitialPayload: map[string]interface{}{"k": 1},
	})
	if err != nil {
		t.Fatalf("StartSaga failed: %v", err)
	}
	return recoveryFixture{repo: repo, svc: svc, dispatcher: dispatcher, tenantID: tenantID, sagaID: created.ID}
}

func (f recoveryFixture) persistState(t *testing.T, states ...sagaContracts.SagaState) {
	t.Helper()
	ctx := context.Background()
	for _, next := range states {
		current, err := f.repo.GetSagaByID(ctx, f.tenantID, f.sagaID)
		if err != nil {
			t.Fatal(err)
		}
		if err = f.repo.UpdateSagaState(ctx, f.tenantID, f.sagaID, current.Version, next); err != nil {
			t.Fatalf("persist %s: %v", next, err)
		}
	}
}

func (f recoveryFixture) persistStep(t *testing.T, stepID string, state sagaContracts.StepExecutionState, nextAttemptAt string) {
	t.Helper()
	ctx := context.Background()
	current, err := f.repo.GetSagaByID(ctx, f.tenantID, f.sagaID)
	if err != nil {
		t.Fatal(err)
	}
	for index := range current.Steps {
		if current.Steps[index].StepID == stepID {
			current.Steps[index].ExecutionState = state
			current.Steps[index].NextAttemptAt = nextAttemptAt
			current.Steps[index].StepOutput = map[string]interface{}{"charge_id": "ch_" + stepID}
			if err = f.repo.UpdateStepExecution(ctx, f.tenantID, f.sagaID, &current.Steps[index]); err != nil {
				t.Fatal(err)
			}
			return
		}
	}
	t.Fatalf("step %s not found", stepID)
}

func (f recoveryFixture) process(t *testing.T) *sagaContracts.SagaInstance {
	t.Helper()
	ctx := context.Background()
	leased, err := f.repo.AcquireLeaseForSaga(ctx, f.tenantID, f.sagaID, "recovery-worker", 30*time.Second)
	if err != nil || leased == nil {
		t.Fatalf("lease failed: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- f.svc.ProcessSaga(ctx, leased) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("ProcessSaga did not return: it is spinning on a step that is not yet eligible")
	}
	persisted, err := f.repo.GetSagaByID(ctx, f.tenantID, f.sagaID)
	if err != nil {
		t.Fatal(err)
	}
	return persisted
}

func TestRecovery_CrashDuringCompensationResumesAndFinishesCompensation(t *testing.T) {
	f := newRecoveryFixture(t, "tenant_recovery_comp")
	f.persistState(t, sagaContracts.SagaStateRunning, sagaContracts.SagaStateCompensating)
	f.persistStep(t, "s1", sagaContracts.StepStateSucceeded, "")
	f.persistStep(t, "s2", sagaContracts.StepStateCompensated, "")
	f.persistStep(t, "s3", sagaContracts.StepStateNotStarted, "")
	persisted := f.process(t)
	if persisted.CurrentState != sagaContracts.SagaStateFailedCompensated {
		t.Fatalf("a saga interrupted during compensation must finish compensating, got %s", persisted.CurrentState)
	}
	if len(f.dispatcher.executedSteps) != 0 {
		t.Fatalf("recovery must never re-run forward steps, dispatched %v", f.dispatcher.executedSteps)
	}
	if len(f.dispatcher.compensatedSteps) != 1 || f.dispatcher.compensatedSteps[0] != "s1" {
		t.Fatalf("only the not-yet-compensated step must be compensated, got %v", f.dispatcher.compensatedSteps)
	}
}

func TestRecovery_CompensationResumesWithInterruptedStepAndKeepsReverseOrder(t *testing.T) {
	f := newRecoveryFixture(t, "tenant_recovery_order")
	f.persistState(t, sagaContracts.SagaStateRunning, sagaContracts.SagaStateCompensating)
	f.persistStep(t, "s1", sagaContracts.StepStateSucceeded, "")
	f.persistStep(t, "s2", sagaContracts.StepStateCompensating, "")
	f.persistStep(t, "s3", sagaContracts.StepStateSucceeded, "")
	persisted := f.process(t)
	if got := strings.Join(f.dispatcher.compensatedSteps, ","); got != "s3,s2,s1" {
		t.Fatalf("expected reverse order s3,s2,s1 including the interrupted step, got %s", got)
	}
	if persisted.CurrentState != sagaContracts.SagaStateFailedCompensated {
		t.Fatalf("expected FAILED_COMPENSATED, got %s", persisted.CurrentState)
	}
}

func TestRecovery_StepWaitingForRetryDoesNotSpinOrCompleteTheSaga(t *testing.T) {
	f := newRecoveryFixture(t, "tenant_recovery_wait")
	f.persistState(t, sagaContracts.SagaStateRunning)
	f.persistStep(t, "s1", sagaContracts.StepStateRetryWait, sagaRules.FormatNextAttemptTimestamp(time.Hour))
	persisted := f.process(t)
	if persisted.CurrentState != sagaContracts.SagaStateRunning {
		t.Fatalf("a saga with an unfinished step must stay RUNNING, got %s", persisted.CurrentState)
	}
	if len(f.dispatcher.executedSteps) != 0 {
		t.Fatalf("a step inside its backoff window must not be dispatched, got %v", f.dispatcher.executedSteps)
	}
}

func TestRecovery_ForwardProgressResumesAfterCrashWithoutRepeatingFinishedSteps(t *testing.T) {
	f := newRecoveryFixture(t, "tenant_recovery_fwd")
	f.persistState(t, sagaContracts.SagaStateRunning)
	f.persistStep(t, "s1", sagaContracts.StepStateSucceeded, "")
	persisted := f.process(t)
	if got := strings.Join(f.dispatcher.executedSteps, ","); got != "s2,s3" {
		t.Fatalf("only unfinished steps may run after recovery, got %s", got)
	}
	if persisted.CurrentState != sagaContracts.SagaStateCompleted {
		t.Fatalf("expected COMPLETED, got %s", persisted.CurrentState)
	}
}

func TestRecovery_TerminalSagaIsNotReprocessed(t *testing.T) {
	f := newRecoveryFixture(t, "tenant_recovery_done")
	f.persistState(t, sagaContracts.SagaStateRunning, sagaContracts.SagaStateCompleted)
	f.process(t)
	if len(f.dispatcher.executedSteps) != 0 || len(f.dispatcher.compensatedSteps) != 0 {
		t.Fatalf("a finished saga must not run anything: %v %v", f.dispatcher.executedSteps, f.dispatcher.compensatedSteps)
	}
}

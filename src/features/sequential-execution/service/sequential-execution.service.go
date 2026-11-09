package service

import (
	"context"
	"time"

	contracts "distributed-transaction-engine/src/features/sequential-execution/contracts"
	machines "distributed-transaction-engine/src/features/sequential-execution/domain/machines"
	ports "distributed-transaction-engine/src/features/sequential-execution/infrastructure/ports"
	workflowPorts "distributed-transaction-engine/src/features/workflow-definitions/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
)

type SequentialExecutionServiceDeps struct {
	Repo         ports.SequentialExecutionRepositoryPort
	WorkflowRepo workflowPorts.WorkflowDefinitionsRepositoryPort
	Dispatcher   ports.SequentialDispatcherPort
	Machine      *machines.SequentialExecutionMachine
	Tracer       *observability.DefaultTracer
}

type SequentialExecutionService struct {
	repo         ports.SequentialExecutionRepositoryPort
	workflowRepo workflowPorts.WorkflowDefinitionsRepositoryPort
	dispatcher   ports.SequentialDispatcherPort
	machine      *machines.SequentialExecutionMachine
	tracer       *observability.DefaultTracer
}

func NewSequentialExecutionService(deps SequentialExecutionServiceDeps) *SequentialExecutionService {
	return &SequentialExecutionService{
		repo:         deps.Repo,
		workflowRepo: deps.WorkflowRepo,
		dispatcher:   deps.Dispatcher,
		machine:      deps.Machine,
		tracer:       deps.Tracer,
	}
}

func (s *SequentialExecutionService) StartSequentialSaga(ctx context.Context, input *contracts.StartSequentialSagaInput) (*contracts.SequentialSagaState, error) {
	_, span := s.tracer.StartSpan(ctx, "StartSequentialSaga")
	defer span.End()

	if err := contracts.ValidateStartSequentialSagaInput(input); err != nil {
		return nil, err
	}

	wf, err := s.workflowRepo.GetByID(ctx, input.TenantID, input.WorkflowDefinitionID)
	if err != nil {
		return nil, errors.NewNotFoundError("workflow definition not found")
	}

	now := time.Now().UTC()
	saga := &contracts.SequentialSagaState{
		ID:                   input.ID,
		PublicID:             input.PublicID,
		TenantID:             input.TenantID,
		WorkflowDefinitionID: input.WorkflowDefinitionID,
		CurrentState:         "RUNNING",
		CurrentStepIndex:     0,
		TotalSteps:           len(wf.Steps),
		Phase:                contracts.PhaseForward,
		PivotReached:         false,
		Version:              1,
		OwnerEpoch:           1,
		FinalOutput:          make(map[string]interface{}),
		CreatedAt:            now,
		UpdatedAt:            now,
	}

	if err := s.repo.CreateSequentialSaga(ctx, saga); err != nil {
		return nil, err
	}

	if len(wf.Steps) > 0 {
		_ = s.dispatcher.DispatchStep(ctx, saga.TenantID, saga.ID, 0, input.Payload)
	}
	return saga, nil
}

func (s *SequentialExecutionService) HandleStepCompletion(
	ctx context.Context,
	tenantID string,
	sagaID string,
	stepIndex int,
	isSuccess bool,
	output map[string]interface{},
	isRetryable bool,
) error {
	_, span := s.tracer.StartSpan(ctx, "HandleStepCompletion")
	defer span.End()

	saga, err := s.repo.GetSequentialSaga(ctx, tenantID, sagaID)
	if err != nil {
		return err
	}

	wf, err := s.workflowRepo.GetByID(ctx, tenantID, saga.WorkflowDefinitionID)
	if err != nil {
		return errors.NewNotFoundError("workflow definition not found")
	}

	isPivot := false
	if stepIndex >= 0 && stepIndex < len(wf.Steps) {
		isPivot = wf.Steps[stepIndex].IsPivot
	}

	var dec machines.SequentialTransitionDecision
	if isSuccess {
		dec, err = s.machine.EvaluateStepSuccess(saga.Phase, saga.CurrentStepIndex, saga.TotalSteps, isPivot, saga.PivotReached)
	} else {
		dec, err = s.machine.EvaluateStepFailure(saga.Phase, saga.CurrentStepIndex, saga.PivotReached, isRetryable)
	}
	if err != nil {
		return err
	}

	if err := s.repo.UpdateSagaCAS(
		ctx,
		tenantID,
		sagaID,
		saga.Version,
		saga.OwnerEpoch,
		dec.NextSagaState,
		dec.NextStepIndex,
		dec.NextPhase,
		dec.NextPivotReached,
		output,
	); err != nil {
		return err
	}

	for _, cmd := range dec.Commands {
		switch cmd.Type {
		case machines.CommandDispatchStep:
			_ = s.dispatcher.DispatchStep(ctx, tenantID, sagaID, cmd.StepIndex, output)
		case machines.CommandCompensateStep:
			_ = s.dispatcher.CompensateStep(ctx, tenantID, sagaID, cmd.StepIndex)
		case machines.CommandScheduleRetry:
			_ = s.dispatcher.ScheduleTimer(ctx, tenantID, sagaID, cmd.StepIndex, 5)
		}
	}
	return nil
}

func (s *SequentialExecutionService) GetSaga(ctx context.Context, tenantID string, sagaID string) (*contracts.SequentialSagaState, error) {
	_, span := s.tracer.StartSpan(ctx, "GetSaga")
	defer span.End()

	return s.repo.GetSequentialSaga(ctx, tenantID, sagaID)
}

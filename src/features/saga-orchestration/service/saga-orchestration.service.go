package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
	sagaRules "distributed-transaction-engine/src/features/saga-orchestration/domain/rules"
	sagaPorts "distributed-transaction-engine/src/features/saga-orchestration/infrastructure/ports"
	workflowContracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
	workflowRules "distributed-transaction-engine/src/features/workflow-definitions/domain/rules"
	workflowPorts "distributed-transaction-engine/src/features/workflow-definitions/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
)

type SagaOrchestrationService struct {
	repo         sagaPorts.SagaRepositoryPort
	workflowRepo workflowPorts.WorkflowDefinitionsRepositoryPort
	dispatcher   sagaPorts.StepDispatcherPort
	tracer       *observability.DefaultTracer
	logger       *observability.LokiLogger
}

func NewSagaOrchestrationService(
	repo sagaPorts.SagaRepositoryPort,
	workflowRepo workflowPorts.WorkflowDefinitionsRepositoryPort,
	dispatcher sagaPorts.StepDispatcherPort,
) *SagaOrchestrationService {
	return &SagaOrchestrationService{
		repo:         repo,
		workflowRepo: workflowRepo,
		dispatcher:   dispatcher,
		tracer:       observability.NewTracer("saga-orchestration-service"),
		logger:       observability.GetLogger(),
	}
}

func (s *SagaOrchestrationService) ExecuteSaga(ctx context.Context, input sagaContracts.ExecuteSagaInput) (*sagaContracts.SagaInstance, error) {
	ctx, span := s.tracer.StartSpan(ctx, "saga_orchestration.execute")
	defer span.End()

	if err := sagaContracts.ValidateExecuteSagaInput(input); err != nil {
		observability.RecordError(span, err)
		return nil, err
	}

	workflowDef, err := s.workflowRepo.GetByNameAndVersion(ctx, input.TenantID, input.WorkflowName, input.WorkflowVersion)
	if err != nil {
		observability.RecordError(span, err)
		return nil, err
	}
	if workflowDef == nil {
		err := errors.NewNotFoundError("workflow definition not found: " + input.WorkflowName)
		observability.RecordError(span, err)
		return nil, err
	}

	sagaID := uuid.New().String()
	publicID := fmt.Sprintf("sga_%s", generateRandomAlpha(26))
	now := time.Now().UTC().Format(time.RFC3339Nano)

	stepExecutions := make([]sagaContracts.SagaStepExecution, len(workflowDef.Steps))
	for i, stepDef := range workflowDef.Steps {
		stepExecutions[i] = sagaContracts.SagaStepExecution{
			ID:             uuid.New().String(),
			PublicID:       fmt.Sprintf("stx_%s", generateRandomAlpha(26)),
			TenantID:       input.TenantID,
			SagaID:         sagaID,
			StepID:         stepDef.StepID,
			ExecutionState: sagaContracts.StepStatePending,
			AttemptCount:   0,
			StepInput:      input.InitialPayload,
			StepOutput:     make(map[string]interface{}),
			IdempotencyKey: fmt.Sprintf("idemp_%s_%s", sagaID, stepDef.StepID),
			CreatedAt:      now,
			UpdatedAt:      now,
		}
	}

	instance := &sagaContracts.SagaInstance{
		ID:                   sagaID,
		PublicID:             publicID,
		TenantID:             input.TenantID,
		WorkflowDefinitionID: workflowDef.ID,
		WorkflowVersion:      workflowDef.Version,
		SpecHash:             workflowDef.SpecHash,
		Source:               input.Source,
		ExternalID:           input.ExternalID,
		CorrelationID:        input.CorrelationID,
		CurrentState:         sagaContracts.SagaStateCreated,
		Version:              1,
		FencingToken:         1,
		InitialPayload:       input.InitialPayload,
		FinalOutput:          make(map[string]interface{}),
		Steps:                stepExecutions,
		CreatedAt:            now,
		UpdatedAt:            now,
	}

	if err := s.repo.CreateSaga(ctx, instance); err != nil {
		observability.RecordError(span, err)
		return nil, err
	}

	s.logger.Info(ctx, "saga instance created",
		observability.String("saga_id", publicID),
		observability.String("workflow_name", workflowDef.Name),
		observability.String("tenant_id", input.TenantID),
	)

	if err := s.repo.UpdateSagaState(ctx, input.TenantID, sagaID, instance.Version, sagaContracts.SagaStateRunning); err != nil {
		observability.RecordError(span, err)
		return nil, err
	}
	instance.CurrentState = sagaContracts.SagaStateRunning
	instance.Version++

	topoResult := workflowRules.ValidateDAGTopology(workflowDef.Steps)
	if !topoResult.IsValid {
		observability.RecordError(span, topoResult.Error)
		return nil, topoResult.Error
	}

	completedSteps := make(map[string]bool)
	succeededSteps := make(map[string]bool)
	inFlightSteps := make(map[string]bool)
	stepMap := make(map[string]workflowContracts.StepDefinition, len(workflowDef.Steps))
	for _, step := range workflowDef.Steps {
		stepMap[step.StepID] = step
	}

	hasFailed := false
	var stepFailureError error

	for len(completedSteps) < len(workflowDef.Steps) && !hasFailed {
		executable := sagaRules.FindNextExecutableSteps(workflowDef.Steps, completedSteps, inFlightSteps)
		if len(executable) == 0 {
			break
		}

		for _, stepID := range executable {
			inFlightSteps[stepID] = true
			stepDef := stepMap[stepID]

			var stepExec *sagaContracts.SagaStepExecution
			for i := range instance.Steps {
				if instance.Steps[i].StepID == stepID {
					stepExec = &instance.Steps[i]
					break
				}
			}

			stepExec.ExecutionState = sagaContracts.StepStateDispatched
			stepExec.AttemptCount++
			stepExec.StartedAt = time.Now().UTC().Format(time.RFC3339Nano)
			_ = s.repo.UpdateStepExecution(ctx, input.TenantID, sagaID, stepExec)

			output, dispatchErr := s.dispatcher.DispatchStep(ctx, stepDef, input.InitialPayload)
			if dispatchErr != nil {
				stepExec.ExecutionState = sagaContracts.StepStateFailed
				stepExec.ErrorMessage = dispatchErr.Error()
				stepExec.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
				_ = s.repo.UpdateStepExecution(ctx, input.TenantID, sagaID, stepExec)

				hasFailed = true
				stepFailureError = dispatchErr
				break
			}

			stepExec.ExecutionState = sagaContracts.StepStateSucceeded
			stepExec.StepOutput = output
			stepExec.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
			_ = s.repo.UpdateStepExecution(ctx, input.TenantID, sagaID, stepExec)

			completedSteps[stepID] = true
			succeededSteps[stepID] = true
			delete(inFlightSteps, stepID)
		}
	}

	if hasFailed {
		s.logger.Warn(ctx, "saga step failed, beginning reverse compensation",
			observability.String("saga_id", publicID),
			observability.String("error", stepFailureError.Error()),
		)

		_ = s.repo.UpdateSagaState(ctx, input.TenantID, sagaID, instance.Version, sagaContracts.SagaStateCompensating)
		instance.CurrentState = sagaContracts.SagaStateCompensating
		instance.Version++

		compensationChain := sagaRules.ComputeReverseCompensationChain(workflowDef.Steps, topoResult.TopologicalOrder, succeededSteps)
		for _, compStepID := range compensationChain {
			compDef := stepMap[compStepID]
			var compExec *sagaContracts.SagaStepExecution
			for i := range instance.Steps {
				if instance.Steps[i].StepID == compStepID {
					compExec = &instance.Steps[i]
					break
				}
			}

			compExec.ExecutionState = sagaContracts.StepStateCompensating
			_ = s.repo.UpdateStepExecution(ctx, input.TenantID, sagaID, compExec)

			_ = s.dispatcher.CompensateStep(ctx, compDef, input.InitialPayload)

			compExec.ExecutionState = sagaContracts.StepStateCompensated
			compExec.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
			_ = s.repo.UpdateStepExecution(ctx, input.TenantID, sagaID, compExec)
		}

		_ = s.repo.UpdateSagaState(ctx, input.TenantID, sagaID, instance.Version, sagaContracts.SagaStateFailed)
		instance.CurrentState = sagaContracts.SagaStateFailed
		instance.Version++

		return instance, stepFailureError
	}

	_ = s.repo.UpdateSagaState(ctx, input.TenantID, sagaID, instance.Version, sagaContracts.SagaStateCompleted)
	instance.CurrentState = sagaContracts.SagaStateCompleted
	instance.Version++

	s.logger.Info(ctx, "saga successfully completed",
		observability.String("saga_id", publicID),
		observability.String("tenant_id", input.TenantID),
	)

	return instance, nil
}

func (s *SagaOrchestrationService) GetSaga(ctx context.Context, tenantID string, id string) (*sagaContracts.SagaInstance, error) {
	if tenantID == "" || id == "" {
		return nil, errors.NewValidationError("tenantId and id are required")
	}
	if len(id) > 4 && id[:4] == "sga_" {
		return s.repo.GetSagaByPublicID(ctx, tenantID, id)
	}
	return s.repo.GetSagaByID(ctx, tenantID, id)
}

func (s *SagaOrchestrationService) ListSagas(ctx context.Context, tenantID string, limit int, offset int) ([]sagaContracts.SagaInstance, error) {
	if tenantID == "" {
		return nil, errors.NewValidationError("tenantId is required")
	}
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	return s.repo.ListSagas(ctx, tenantID, limit, offset)
}

func (s *SagaOrchestrationService) CreateSaga(ctx context.Context, instance *sagaContracts.SagaInstance) error {
	if instance == nil {
		return errors.NewValidationError("instance cannot be nil")
	}
	return s.repo.CreateSaga(ctx, instance)
}

func generateRandomAlpha(length int) string {
	bytes := make([]byte, length/2+1)
	if _, err := rand.Read(bytes); err != nil {
		return "0123456789abcdef0123456789"
	}
	hexStr := hex.EncodeToString(bytes)
	if len(hexStr) > length {
		return hexStr[:length]
	}
	return hexStr
}

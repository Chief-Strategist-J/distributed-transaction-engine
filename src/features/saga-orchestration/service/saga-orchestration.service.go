package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
	sagaRules "distributed-transaction-engine/src/features/saga-orchestration/domain/rules"
	sagaAdapters "distributed-transaction-engine/src/features/saga-orchestration/infrastructure/adapters"
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
	breakers     *sagaRules.CircuitBreakerRegistry
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
		breakers:     sagaRules.NewCircuitBreakerRegistry(),
		tracer:       observability.NewTracer("saga-orchestration-service"),
		logger:       observability.GetLogger(),
	}
}

func (s *SagaOrchestrationService) transitionSagaState(ctx context.Context, tenantID string, instance *sagaContracts.SagaInstance, nextState sagaContracts.SagaState) error {
	if err := sagaRules.ValidateSagaTransition(instance.CurrentState, nextState); err != nil {
		return err
	}
	if instance.FencingToken > 0 {
		if err := s.repo.UpdateSagaStateWithFencing(ctx, tenantID, instance.ID, instance.Version, instance.FencingToken, nextState); err != nil {
			return err
		}
	} else {
		if err := s.repo.UpdateSagaState(ctx, tenantID, instance.ID, instance.Version, nextState); err != nil {
			return err
		}
	}
	instance.CurrentState = nextState
	instance.Version++
	instance.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return nil
}

func (s *SagaOrchestrationService) findStepExecution(instance *sagaContracts.SagaInstance, stepID string) *sagaContracts.SagaStepExecution {
	for i := range instance.Steps {
		if instance.Steps[i].StepID == stepID {
			return &instance.Steps[i]
		}
	}
	return nil
}

func (s *SagaOrchestrationService) recordStepTransition(
	ctx context.Context,
	tenantID string,
	sagaID string,
	stepExec *sagaContracts.SagaStepExecution,
	state sagaContracts.StepExecutionState,
	output map[string]interface{},
	stepErr error,
) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	stepExec.ExecutionState = state
	stepExec.UpdatedAt = now

	if state == sagaContracts.StepStateDispatched {
		stepExec.AttemptCount++
		stepExec.StartedAt = now
	} else if state == sagaContracts.StepStateSucceeded || state == sagaContracts.StepStateFailed || state == sagaContracts.StepStateUnknown || state == sagaContracts.StepStateCompensated {
		stepExec.CompletedAt = now
	}

	if output != nil {
		stepExec.StepOutput = output
	}
	if stepErr != nil {
		stepExec.ErrorMessage = stepErr.Error()
	}
	observability.NoteSwallowed(s.repo.UpdateStepExecution(ctx, tenantID, sagaID, stepExec))
}

func (s *SagaOrchestrationService) StartSaga(ctx context.Context, input sagaContracts.ExecuteSagaInput) (*sagaContracts.SagaInstance, error) {
	ctx, span := s.tracer.StartSpan(ctx, "saga_orchestration.start")
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

	inputHash := sagaContracts.ComputePayloadHash(input.InitialPayload)

	existing, err := s.repo.GetSagaByExternalID(ctx, input.TenantID, workflowDef.ID, input.ExternalID)
	if err == nil && existing != nil {
		if existing.InputHash == inputHash {
			s.logger.Info(ctx, "idempotent replay of existing saga",
				observability.String("saga_id", existing.PublicID),
				observability.String("external_id", input.ExternalID),
			)
			return existing, nil
		}
		conflictErr := errors.NewConflictError("externalId conflict: payload mismatch with existing saga")
		observability.RecordError(span, conflictErr)
		return nil, conflictErr
	}

	sagaID := uuid.New().String()
	publicID := fmt.Sprintf("sga_%s", generateRandomAlpha(26))
	now := time.Now().UTC().Format(time.RFC3339Nano)
	deadline := time.Now().UTC().Add(30 * time.Minute).Format(time.RFC3339Nano)

	stepExecutions := make([]sagaContracts.SagaStepExecution, len(workflowDef.Steps))
	for i, stepDef := range workflowDef.Steps {
		fwdKey := sagaRules.DeriveForwardIdempotencyKey(sagaID, stepDef.StepID)
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
			IdempotencyKey: fwdKey,
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
		InputHash:            inputHash,
		CorrelationID:        input.CorrelationID,
		CurrentState:         sagaContracts.SagaStateCreated,
		Version:              1,
		FencingToken:         0,
		DeadlineAt:           deadline,
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

	return instance, nil
}

func (s *SagaOrchestrationService) ProcessSaga(ctx context.Context, instance *sagaContracts.SagaInstance) error {
	ctx, span := s.tracer.StartSpan(ctx, "saga_orchestration.process")
	defer span.End()

	workflowDef, err := s.workflowRepo.GetByID(ctx, instance.TenantID, instance.WorkflowDefinitionID)
	if err != nil {
		observability.RecordError(span, err)
		return err
	}
	if workflowDef == nil {
		err := errors.NewNotFoundError("workflow definition not found: " + instance.WorkflowDefinitionID)
		observability.RecordError(span, err)
		return err
	}

	if instance.CurrentState == sagaContracts.SagaStateCreated {
		if err := s.transitionSagaState(ctx, instance.TenantID, instance, sagaContracts.SagaStateRunning); err != nil {
			observability.RecordError(span, err)
			return err
		}
	}

	topoResult := workflowRules.ValidateDAGTopology(workflowDef.Steps)
	if !topoResult.IsValid {
		observability.RecordError(span, topoResult.Error)
		return topoResult.Error
	}

	completedSteps := make(map[string]bool)
	succeededSteps := make(map[string]bool)
	inFlightSteps := make(map[string]bool)
	eligibleForCompensation := make(map[string]bool)
	stepMap := make(map[string]workflowContracts.StepDefinition, len(workflowDef.Steps))
	for _, step := range workflowDef.Steps {
		stepMap[step.StepID] = step
	}

	for _, stepExec := range instance.Steps {
		if stepExec.ExecutionState == sagaContracts.StepStateSucceeded {
			completedSteps[stepExec.StepID] = true
			succeededSteps[stepExec.StepID] = true
			eligibleForCompensation[stepExec.StepID] = true
		} else if stepExec.ExecutionState == sagaContracts.StepStateUnknown {
			eligibleForCompensation[stepExec.StepID] = true
		}
	}

	isPivotSucceeded := false
	hasFailed := false
	var stepFailureError error

	for len(completedSteps) < len(workflowDef.Steps) && !hasFailed {
		executable := sagaRules.FindNextExecutableSteps(workflowDef.Steps, completedSteps, inFlightSteps)
		if len(executable) == 0 {
			break
		}

		for _, stepID := range executable {
			stepDef := stepMap[stepID]
			stepExec := s.findStepExecution(instance, stepID)
			if stepExec == nil {
				now := time.Now().UTC()
				newExec := sagaContracts.SagaStepExecution{
					ID:             uuid.New().String(),
					PublicID:       "stx_" + generateRandomAlpha(26),
					TenantID:       instance.TenantID,
					SagaID:         instance.ID,
					StepID:         stepID,
					ExecutionState: sagaContracts.StepStatePending,
					AttemptCount:   0,
					StepInput:      instance.InitialPayload,
					StepOutput:     make(map[string]interface{}),
					IdempotencyKey: fmt.Sprintf("%s:%s:1", instance.ID, stepID),
					CreatedAt:      now.Format(time.RFC3339),
					UpdatedAt:      now.Format(time.RFC3339),
				}
				instance.Steps = append(instance.Steps, newExec)
				stepExec = &instance.Steps[len(instance.Steps)-1]
			}

			if !sagaRules.IsRetryEligible(stepExec.NextAttemptAt) {
				continue
			}

			cb := s.breakers.GetBreaker(stepDef.ActionEndpoint)
			if cbErr := cb.AcquirePermission(); cbErr != nil {
				backoff := sagaRules.ComputeExponentialBackoff(stepExec.AttemptCount+1, 1*time.Second, 10*time.Second)
				stepExec.NextAttemptAt = sagaRules.FormatNextAttemptTimestamp(backoff)
				s.recordStepTransition(ctx, instance.TenantID, instance.ID, stepExec, sagaContracts.StepStateRetryWait, nil, cbErr)
				continue
			}

			inFlightSteps[stepID] = true
			s.recordStepTransition(ctx, instance.TenantID, instance.ID, stepExec, sagaContracts.StepStateDispatched, nil, nil)

			fwdKey := sagaRules.DeriveForwardIdempotencyKey(instance.ID, stepDef.StepID)
			output, dispatchErr := s.dispatcher.DispatchStep(ctx, stepDef, instance.InitialPayload, fwdKey)
			cb.ReleasePermission(dispatchErr)

			classifiedState := sagaRules.ClassifyStepResult(sagaAdapters.IsAmbiguousError(dispatchErr), dispatchErr)

			if dispatchErr != nil {
				s.recordStepTransition(ctx, instance.TenantID, instance.ID, stepExec, classifiedState, nil, dispatchErr)

				if classifiedState == sagaContracts.StepStateUnknown {
					eligibleForCompensation[stepID] = true
				}

				hasFailed = true
				stepFailureError = dispatchErr
				break
			}

			s.recordStepTransition(ctx, instance.TenantID, instance.ID, stepExec, classifiedState, output, nil)

			completedSteps[stepID] = true
			succeededSteps[stepID] = true
			eligibleForCompensation[stepID] = true
			delete(inFlightSteps, stepID)

			if stepDef.IsPivot {
				isPivotSucceeded = true
			}
		}
	}

	if hasFailed {
		failureRoute := sagaRules.DetermineFailureRouting(isPivotSucceeded)

		if failureRoute == "FORWARD_ONLY" {
			s.logger.Warn(ctx, "post-pivot failure encountered, rolling forward without compensation",
				observability.String("saga_id", instance.PublicID),
				observability.String("error", stepFailureError.Error()),
			)

			targetState := sagaRules.EvaluatePostPivotFailureState()
			observability.NoteSwallowed(s.transitionSagaState(ctx, instance.TenantID, instance, targetState))
			observability.NoteSwallowed(s.repo.ReleaseLease(ctx, instance.TenantID, instance.ID, instance.FencingToken))
			return stepFailureError
		}

		s.logger.Warn(ctx, "pre-pivot failure encountered, draining and beginning reverse compensation",
			observability.String("saga_id", instance.PublicID),
			observability.String("error", stepFailureError.Error()),
		)

		drainedSteps, drainCompensationSet := sagaRules.DeriveDrainPlan(instance.Steps, inFlightSteps)
		instance.Steps = drainedSteps
		for stepID := range drainCompensationSet {
			eligibleForCompensation[stepID] = true
		}
		for i := range instance.Steps {
			observability.NoteSwallowed(s.repo.UpdateStepExecution(ctx, instance.TenantID, instance.ID, &instance.Steps[i]))
		}
		observability.NoteSwallowed(s.transitionSagaState(ctx, instance.TenantID, instance, sagaContracts.SagaStateCompensating))

		compensationChain := sagaRules.ComputeReverseCompensationChain(workflowDef.Steps, topoResult.TopologicalOrder, eligibleForCompensation)
		allCompensated := true

		for _, compStepID := range compensationChain {
			compDef := stepMap[compStepID]
			compExec := s.findStepExecution(instance, compStepID)

			s.recordStepTransition(ctx, instance.TenantID, instance.ID, compExec, sagaContracts.StepStateCompensating, nil, nil)

			compKey := sagaRules.DeriveCompensationIdempotencyKey(instance.ID, compStepID)
			compErr := s.dispatcher.CompensateStep(ctx, compDef, instance.InitialPayload, compExec.StepOutput, compKey)
			if compErr != nil {
				allCompensated = false
				s.recordStepTransition(ctx, instance.TenantID, instance.ID, compExec, sagaContracts.StepStateCompensating, nil, compErr)
				break
			}

			s.recordStepTransition(ctx, instance.TenantID, instance.ID, compExec, sagaContracts.StepStateCompensated, nil, nil)
		}

		terminalCompState := sagaRules.EvaluatePostCompensationState(allCompensated)
		observability.NoteSwallowed(s.transitionSagaState(ctx, instance.TenantID, instance, terminalCompState))
		observability.NoteSwallowed(s.repo.ReleaseLease(ctx, instance.TenantID, instance.ID, instance.FencingToken))

		return stepFailureError
	}
	observability.NoteSwallowed(s.transitionSagaState(ctx, instance.TenantID, instance, sagaContracts.SagaStateCompleted))
	observability.NoteSwallowed(s.repo.ReleaseLease(ctx, instance.TenantID, instance.ID, instance.FencingToken))

	s.logger.Info(ctx, "saga successfully completed",
		observability.String("saga_id", instance.PublicID),
		observability.String("tenant_id", instance.TenantID),
	)

	return nil
}

func (s *SagaOrchestrationService) ExecuteSaga(ctx context.Context, input sagaContracts.ExecuteSagaInput) (spanResult0 *sagaContracts.SagaInstance, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	instance, err := s.StartSaga(ctx, input)
	if err != nil {
		return nil, err
	}
	if instance.CurrentState == sagaContracts.SagaStateCompleted || instance.CurrentState == sagaContracts.SagaStateFailedCompensated {
		return instance, nil
	}

	leased, err := s.repo.AcquireLeaseForSaga(ctx, instance.TenantID, instance.ID, "inline-executor", 30*time.Minute)
	if err != nil {
		return nil, err
	}
	instance = leased

	processErr := s.ProcessSaga(ctx, instance)
	return instance, processErr
}

func (s *SagaOrchestrationService) GetSaga(ctx context.Context, tenantID string, id string) (spanResult0 *sagaContracts.SagaInstance, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if tenantID == "" || id == "" {
		return nil, errors.NewValidationError("tenantId and id are required")
	}
	if len(id) > 4 && id[:4] == "sga_" {
		return s.repo.GetSagaByPublicID(ctx, tenantID, id)
	}
	return s.repo.GetSagaByID(ctx, tenantID, id)
}

func (s *SagaOrchestrationService) CancelSaga(ctx context.Context, tenantID string, id string) (*sagaContracts.SagaInstance, error) {
	ctx, span := s.tracer.StartSpan(ctx, "saga_orchestration.cancel")
	defer span.End()

	instance, err := s.GetSaga(ctx, tenantID, id)
	if err != nil {
		observability.RecordError(span, err)
		return nil, err
	}

	if instance.CurrentState == sagaContracts.SagaStateCompleted ||
		instance.CurrentState == sagaContracts.SagaStateFailedCompensated ||
		instance.CurrentState == sagaContracts.SagaStateCancelled {
		conflictErr := errors.NewConflictError("saga is already in terminal state: " + string(instance.CurrentState))
		observability.RecordError(span, conflictErr)
		return nil, conflictErr
	}

	workflowDef, err := s.workflowRepo.GetByID(ctx, instance.TenantID, instance.WorkflowDefinitionID)
	if err != nil {
		observability.RecordError(span, err)
		return nil, err
	}

	stepMap := make(map[string]workflowContracts.StepDefinition, len(workflowDef.Steps))
	for _, step := range workflowDef.Steps {
		stepMap[step.StepID] = step
		if step.IsPivot {
			for _, stepExec := range instance.Steps {
				if stepExec.StepID == step.StepID {
					if stepExec.ExecutionState == sagaContracts.StepStateSucceeded ||
						stepExec.ExecutionState == sagaContracts.StepStateUnknown ||
						stepExec.ExecutionState == sagaContracts.StepStateDispatched {
						pastPivotErr := errors.NewConflictError("cannot cancel saga: past point of no return (pivot step executed)")
						observability.RecordError(span, pastPivotErr)
						return nil, pastPivotErr
					}
				}
			}
		}
	}

	if instance.CurrentState == sagaContracts.SagaStateCreated {
		observability.NoteSwallowed(s.transitionSagaState(ctx, instance.TenantID, instance, sagaContracts.SagaStateCancelled))
		observability.NoteSwallowed(s.repo.ReleaseLease(ctx, instance.TenantID, instance.ID, instance.FencingToken))
		s.logger.Info(ctx, "saga successfully cancelled before execution started",
			observability.String("saga_id", instance.PublicID),
			observability.String("tenant_id", instance.TenantID),
		)
		return instance, nil
	}
	observability.NoteSwallowed(s.transitionSagaState(ctx, instance.TenantID, instance, sagaContracts.SagaStateCompensating))

	drainedSteps, compSet := sagaRules.DeriveDrainPlan(instance.Steps, map[string]bool{})
	instance.Steps = drainedSteps
	for i := range instance.Steps {
		observability.NoteSwallowed(s.repo.UpdateStepExecution(ctx, instance.TenantID, instance.ID, &instance.Steps[i]))
	}

	topoResult := workflowRules.ValidateDAGTopology(workflowDef.Steps)
	compensationChain := sagaRules.ComputeReverseCompensationChain(workflowDef.Steps, topoResult.TopologicalOrder, compSet)

	for _, compStepID := range compensationChain {
		compDef := stepMap[compStepID]
		compExec := s.findStepExecution(instance, compStepID)

		s.recordStepTransition(ctx, instance.TenantID, instance.ID, compExec, sagaContracts.StepStateCompensating, nil, nil)

		compKey := sagaRules.DeriveCompensationIdempotencyKey(instance.ID, compStepID)
		observability.NoteSwallowed(s.dispatcher.CompensateStep(ctx, compDef, instance.InitialPayload, compExec.StepOutput, compKey))

		s.recordStepTransition(ctx, instance.TenantID, instance.ID, compExec, sagaContracts.StepStateCompensated, nil, nil)
	}
	observability.NoteSwallowed(s.transitionSagaState(ctx, instance.TenantID, instance, sagaContracts.SagaStateCancelled))
	observability.NoteSwallowed(s.repo.ReleaseLease(ctx, instance.TenantID, instance.ID, instance.FencingToken))

	s.logger.Info(ctx, "saga successfully cancelled",
		observability.String("saga_id", instance.PublicID),
		observability.String("tenant_id", instance.TenantID),
	)

	return instance, nil
}

func (s *SagaOrchestrationService) ListSagas(ctx context.Context, tenantID string, limit int, offset int) (spanResult0 []sagaContracts.SagaInstance, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
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

func (s *SagaOrchestrationService) CreateSaga(ctx context.Context, instance *sagaContracts.SagaInstance) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
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

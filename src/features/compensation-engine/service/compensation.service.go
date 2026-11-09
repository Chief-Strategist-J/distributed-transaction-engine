package service

import (
	"context"
	"fmt"
	"time"

	contracts "distributed-transaction-engine/src/features/compensation-engine/contracts"
	machines "distributed-transaction-engine/src/features/compensation-engine/domain/machines"
	rules "distributed-transaction-engine/src/features/compensation-engine/domain/rules"
	ports "distributed-transaction-engine/src/features/compensation-engine/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
)

type CompensationServiceDeps struct {
	Repo       ports.CompensationRepositoryPort
	Dispatcher ports.CompensationDispatcherPort
	Machine    *machines.CompensationStateMachine
	Tracer     *observability.DefaultTracer
}

type CompensationService struct {
	repo       ports.CompensationRepositoryPort
	dispatcher ports.CompensationDispatcherPort
	machine    *machines.CompensationStateMachine
	tracer     *observability.DefaultTracer
}

func NewCompensationService(deps CompensationServiceDeps) *CompensationService {
	return &CompensationService{
		repo:       deps.Repo,
		dispatcher: deps.Dispatcher,
		machine:    deps.Machine,
		tracer:     deps.Tracer,
	}
}

func (s *CompensationService) TriggerCompensation(
	ctx context.Context,
	input *contracts.TriggerCompensationInput,
) (*contracts.SagaCompensationPlanInstance, error) {
	_, span := s.tracer.StartSpan(ctx, "compensation.trigger")
	defer span.End()

	if err := contracts.ValidateTriggerCompensationInput(input); err != nil {
		return nil, err
	}

	planSteps, err := rules.BuildCompensationPlan(input.ForwardSteps)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	plan := &contracts.SagaCompensationPlanInstance{
		ID:           input.ID,
		PublicID:     input.PublicID,
		TenantID:     input.TenantID,
		SagaID:       input.SagaID,
		CurrentState: contracts.PlanStateCompensating,
		TotalSteps:   len(planSteps),
		Residuals:    make([]map[string]interface{}, 0),
		Version:      1,
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	steps := make([]contracts.SagaStepCompensationInstance, len(planSteps))
	for i, st := range planSteps {
		stepUUID := uuid.New().String()
		uStr := uuid.New().String()
		cleaned := ""
		for _, ch := range uStr {
			if ch != '-' {
				cleaned += string(ch)
			}
		}
		if len(cleaned) > 26 {
			cleaned = cleaned[:26]
		}

		steps[i] = contracts.SagaStepCompensationInstance{
			ID:                  stepUUID,
			PublicID:            fmt.Sprintf("cst_%s", cleaned),
			TenantID:            input.TenantID,
			PlanID:              input.ID,
			SagaID:              input.SagaID,
			StepID:              st.StepID,
			StepName:            st.StepName,
			ExecutionOrder:      st.ExecutionOrder,
			Status:              contracts.StepStatusPending,
			ForwardExecutionID:  st.ForwardExecutionID,
			CompensationAction:  st.CompensationAction,
			CompensationPayload: st.ForwardOutput,
			ResidualPayload:     make(map[string]interface{}),
			IdempotencyKey:      fmt.Sprintf("%s:%s:%s:compensate", input.SagaID, st.StepID, st.ForwardExecutionID),
			Version:             1,
			CreatedAt:           now,
			UpdatedAt:           now,
		}
	}

	if err := s.repo.CreateCompensationPlanAtomic(ctx, plan, steps); err != nil {
		return nil, err
	}

	for _, step := range steps {
		_ = s.dispatcher.DispatchCompensation(ctx, input.TenantID, input.SagaID, step)
	}

	plan.Steps = steps
	return plan, nil
}

func (s *CompensationService) CompleteStepCompensation(
	ctx context.Context,
	input *contracts.CompleteStepCompensationInput,
) error {
	_, span := s.tracer.StartSpan(ctx, "compensation.complete_step")
	defer span.End()

	if err := contracts.ValidateCompleteStepCompensationInput(input); err != nil {
		return err
	}

	if err := s.repo.UpdateStepOutcome(
		ctx,
		input.TenantID,
		input.PlanID,
		input.StepID,
		input.OutcomeStatus,
		input.ResidualPayload,
		input.ErrorMessage,
	); err != nil {
		return err
	}

	plan, err := s.repo.GetCompensationPlan(ctx, input.TenantID, input.PlanID)
	if err != nil {
		return err
	}

	newState := s.machine.EvaluatePlan(plan.Steps)
	compensatedCount := 0
	failedCount := 0
	residuals := make([]map[string]interface{}, 0)

	for _, st := range plan.Steps {
		if st.Status == contracts.StepStatusCompensated {
			compensatedCount++
		} else if st.Status == contracts.StepStatusNeedsAttention || st.Status == contracts.StepStatusNotCompensable {
			failedCount++
		}
		if len(st.ResidualPayload) > 0 {
			residuals = append(residuals, map[string]interface{}{
				"stepId":   st.StepID,
				"residual": st.ResidualPayload,
			})
		}
	}

	return s.repo.UpdatePlanCAS(ctx, input.TenantID, plan.ID, plan.Version, newState, compensatedCount, failedCount, residuals)
}

func (s *CompensationService) GetCompensationPlan(
	ctx context.Context,
	tenantID string,
	sagaID string,
) (*contracts.SagaCompensationPlanInstance, error) {
	_, span := s.tracer.StartSpan(ctx, "compensation.get_plan")
	defer span.End()

	if tenantID == "" || sagaID == "" {
		return nil, errors.NewValidationError("tenantId and sagaId cannot be empty")
	}
	return s.repo.GetCompensationPlan(ctx, tenantID, sagaID)
}

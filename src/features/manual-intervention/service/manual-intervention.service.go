package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	contracts "distributed-transaction-engine/src/features/manual-intervention/contracts"
	"distributed-transaction-engine/src/features/manual-intervention/domain/machines"
	"distributed-transaction-engine/src/features/manual-intervention/domain/rules"
	"distributed-transaction-engine/src/features/manual-intervention/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
)

type ServiceDeps struct {
	Repo    ports.ManualInterventionRepositoryPort
	Rules   *rules.ManualInterventionRules
	Machine *machines.ManualInterventionMachine
}

type ManualInterventionService struct {
	repo    ports.ManualInterventionRepositoryPort
	rules   *rules.ManualInterventionRules
	machine *machines.ManualInterventionMachine
	tracer  *observability.DefaultTracer
}

func NewService(deps ServiceDeps) *ManualInterventionService {
	return &ManualInterventionService{
		repo:    deps.Repo,
		rules:   deps.Rules,
		machine: deps.Machine,
		tracer:  observability.NewTracer("manual-intervention-service"),
	}
}

func (s *ManualInterventionService) Intervene(ctx context.Context, input contracts.ExecuteInterventionInput) (spanResult0 contracts.InterventionExecutionResult, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "manual_intervention.intervene")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if err := s.validatePreconditions(input); err != nil {
		return contracts.InterventionExecutionResult{}, err
	}

	nextState, err := s.rules.DeriveNextState(input.ActionType)
	if err != nil {
		return contracts.InterventionExecutionResult{}, err
	}

	record := s.buildAuditRecord(input, nextState)
	if err := s.repo.SaveAuditRecord(ctx, record); err != nil {
		return contracts.InterventionExecutionResult{}, err
	}

	return contracts.InterventionExecutionResult{
		Success:          true,
		ActionType:       input.ActionType,
		OldState:         input.CurrentSagaState,
		NewState:         nextState,
		AuditRecordID:    record.ID,
		AuditPublicID:    record.PublicID,
		ExecutionSummary: fmt.Sprintf("Operator %s successfully applied %s", input.OperatorID, input.ActionType),
	}, nil
}

func (s *ManualInterventionService) validatePreconditions(input contracts.ExecuteInterventionInput) error {
	if err := contracts.ValidateExecuteInterventionInput(input); err != nil {
		return err
	}
	if err := s.rules.ValidateOperatorRole(input.OperatorRole); err != nil {
		return err
	}
	if err := s.rules.ValidateTargetSagaState(input.CurrentSagaState); err != nil {
		return err
	}
	return s.machine.ValidateActionEligibility(input.ActionType, input.CurrentSagaState)
}

func (s *ManualInterventionService) buildAuditRecord(input contracts.ExecuteInterventionInput, nextState string) contracts.ManualInterventionAuditRecord {
	externalID := fmt.Sprintf("ops_ext_%s_%s_%d", input.TenantID, input.SagaID.String(), time.Now().UnixNano())
	rawUUID := strings.ReplaceAll(uuid.New().String(), "-", "")
	publicID := "ops_" + rawUUID[:26]

	return contracts.ManualInterventionAuditRecord{
		ID:               uuid.New(),
		PublicID:         publicID,
		TenantID:         input.TenantID,
		Source:           "MANUAL_OPERATOR_ACTION",
		ExternalID:       externalID,
		CorrelationID:    uuid.New(),
		SagaID:           input.SagaID,
		OperatorID:       input.OperatorID,
		OperatorRole:     input.OperatorRole,
		ActionType:       input.ActionType,
		Reason:           input.Reason,
		ApprovalTicketID: input.ApprovalTicketID,
		SyntheticPayload: input.SyntheticPayload,
		OldState:         input.CurrentSagaState,
		NewState:         nextState,
		Version:          1,
		CreatedAt:        time.Now().UTC(),
	}
}

func (s *ManualInterventionService) ListAuditBySaga(ctx context.Context, tenantID string, sagaID uuid.UUID) (spanResult0 []contracts.ManualInterventionAuditRecord, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "manual_intervention.list_audit_by_saga")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if strings.TrimSpace(tenantID) == "" {
		return nil, appErrors.NewValidationError("tenantId cannot be empty")
	}
	return s.repo.ListAuditRecordsBySaga(ctx, tenantID, sagaID)
}

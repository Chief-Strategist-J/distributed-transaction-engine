package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	contracts "distributed-transaction-engine/src/features/reconciliation-engine/contracts"
	"distributed-transaction-engine/src/features/reconciliation-engine/domain/machines"
	"distributed-transaction-engine/src/features/reconciliation-engine/domain/rules"
	"distributed-transaction-engine/src/features/reconciliation-engine/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
)

type ServiceDeps struct {
	Repo    ports.ReconciliationRepositoryPort
	Rules   *rules.ReconciliationRules
	Machine *machines.ReconciliationMachine
}

type ReconciliationService struct {
	repo    ports.ReconciliationRepositoryPort
	rules   *rules.ReconciliationRules
	machine *machines.ReconciliationMachine
	tracer  *observability.DefaultTracer
}

func NewService(deps ServiceDeps) *ReconciliationService {
	return &ReconciliationService{
		repo:    deps.Repo,
		rules:   deps.Rules,
		machine: deps.Machine,
		tracer:  observability.NewTracer("reconciliation-service"),
	}
}

func (s *ReconciliationService) Evaluate(ctx context.Context, input contracts.EvaluateReconciliationInput) (spanResult0 contracts.ReconciliationEvaluationResult, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "reconciliation.evaluate")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if err := contracts.ValidateEvaluateReconciliationInput(input); err != nil {
		return contracts.ReconciliationEvaluationResult{}, err
	}

	decision := s.rules.ClassifyDrift(input)
	if !decision.IsAnomaly {
		return decision, nil
	}

	record := s.buildAnomalyRecord(input, decision)
	if err := s.repo.SaveAnomaly(ctx, record); err != nil {
		return contracts.ReconciliationEvaluationResult{}, err
	}
	return decision, nil
}

func (s *ReconciliationService) buildAnomalyRecord(input contracts.EvaluateReconciliationInput, decision contracts.ReconciliationEvaluationResult) contracts.ReconciliationAnomalyRecord {
	externalID := fmt.Sprintf("rec_ext_%s_%s", input.TenantID, input.SagaID.String())
	rawUUID := strings.ReplaceAll(uuid.New().String(), "-", "")
	publicID := "rec_" + rawUUID[:26]

	return contracts.ReconciliationAnomalyRecord{
		ID:                  uuid.New(),
		PublicID:            publicID,
		TenantID:            input.TenantID,
		Source:              "ENGINE_RECONCILIATION",
		ExternalID:          externalID,
		CorrelationID:       uuid.New(),
		SagaID:              input.SagaID,
		AnomalyType:         *decision.AnomalyType,
		Status:              contracts.StatusAnomalyDetected,
		InternalStatus:      input.InternalStatus,
		ExternalStatus:      input.ExternalStatus,
		InternalAmountCents: input.InternalAmountCents,
		ExternalAmountCents: input.ExternalAmountCents,
		Currency:            strings.ToUpper(strings.TrimSpace(input.Currency)),
		DiscrepancyReason:   decision.DiscrepancyReason,
		ResolutionAction:    decision.SuggestedAction,
		Version:             1,
		CreatedAt:           time.Now().UTC(),
		UpdatedAt:           time.Now().UTC(),
	}
}

func (s *ReconciliationService) ListAnomalies(ctx context.Context, tenantID string, status *contracts.ReconciliationStatus) (spanResult0 []contracts.ReconciliationAnomalyRecord, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "reconciliation.list_anomalies")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if strings.TrimSpace(tenantID) == "" {
		return nil, appErrors.NewValidationError("tenantId cannot be empty")
	}
	return s.repo.ListAnomalies(ctx, tenantID, status)
}

func (s *ReconciliationService) ResolveAnomaly(ctx context.Context, tenantID string, id uuid.UUID, expectedVersion int64, action string) (spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "reconciliation.resolve_anomaly")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if strings.TrimSpace(tenantID) == "" {
		return appErrors.NewValidationError("tenantId cannot be empty")
	}
	return s.repo.UpdateAnomalyStatus(ctx, tenantID, id, expectedVersion, contracts.StatusAutoResolved, action)
}

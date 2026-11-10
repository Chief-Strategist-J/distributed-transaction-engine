package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"distributed-transaction-engine/src/features/optimistic-concurrency-control/contracts"
	"distributed-transaction-engine/src/features/optimistic-concurrency-control/domain/machines"
	"distributed-transaction-engine/src/features/optimistic-concurrency-control/domain/rules"
	"distributed-transaction-engine/src/features/optimistic-concurrency-control/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"

	"github.com/google/uuid"
)

type ServiceDeps struct {
	Repo    ports.RepositoryPort
	Rules   *rules.OCCRules
	Machine *machines.OCCStrategyMachine
	Tracer  *observability.DefaultTracer
}

type OCCService struct {
	repo    ports.RepositoryPort
	rules   *rules.OCCRules
	machine *machines.OCCStrategyMachine
	tracer  *observability.DefaultTracer
}

func NewService(deps ServiceDeps) *OCCService {
	r := deps.Rules
	if r == nil {
		r = rules.NewOCCRules()
	}
	m := deps.Machine
	if m == nil {
		m = machines.NewOCCStrategyMachine()
	}
	tr := deps.Tracer
	if tr == nil {
		tr = observability.NewTracer("optimistic-concurrency-control-service")
	}

	return &OCCService{
		repo:    deps.Repo,
		rules:   r,
		machine: m,
		tracer:  tr,
	}
}

func (s *OCCService) MutateWithOCC(
	ctx context.Context,
	input contracts.CASMutationInput,
	maxRetries int,
) (spanResult0 *contracts.CASResult, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "occ.service_mutate_cas")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if err := contracts.ValidateCASMutationInput(input); err != nil {
		return nil, err
	}

	if maxRetries <= 0 {
		maxRetries = 5
	}

	applied, err := s.repo.IsInputApplied(ctx, input.TenantID, input.SagaID, input.InputID)
	if err != nil {
		return nil, err
	}
	if applied {
		return &contracts.CASResult{
			Succeeded:    true,
			ErrorMessage: "input already applied, skipping redundant mutation",
		}, nil
	}

	for attempt := 0; attempt <= maxRetries; attempt++ {
		res, err := s.repo.ExecuteCAS(ctx, input)
		if err != nil {
			return nil, err
		}

		if res.Succeeded {
			observability.NoteSwallowed(s.repo.MarkInputApplied(ctx, input.TenantID, input.SagaID, input.InputID))
			return res, nil
		}

		entryUUID := uuid.New()
		entryPubID := fmt.Sprintf("ocj_%s", strings.ReplaceAll(entryUUID.String(), "-", "")[:26])
		observability.NoteSwallowed(s.repo.RecordContention(ctx, contracts.ContentionJournalEntry{
			ID:               entryUUID,
			PublicID:         entryPubID,
			TenantID:         input.TenantID,
			SagaID:           input.SagaID,
			AttemptedVersion: input.ExpectedVersion,
			CurrentVersion:   res.CurrentVersion,
			AttemptedEpoch:   input.ExpectedEpoch,
			CurrentEpoch:     res.CurrentEpoch,
			Diagnosis:        res.Cause,
			WorkerID:         input.WorkerID,
			CreatedAt:        time.Now().UTC(),
		}))

		switch res.Cause {
		case contracts.CauseNotOwner:
			return res, appErrors.NewForbiddenError("ownership lost; worker epoch is obsolete")
		case contracts.CauseDataError:
			return res, appErrors.NewNotFoundError("saga root aggregate not found")
		case contracts.CauseStateConflict:
			return res, appErrors.NewConflictError("saga state conflict: target state invalid for current state " + res.CurrentState)
		case contracts.CauseSecurityViolation:
			return res, appErrors.NewForbiddenError("tenant boundary security violation")
		case contracts.CauseVersionConflict:
			if attempt >= maxRetries {
				if input.InputType != contracts.InputTypeOperatorCommand {
					pendUUID := uuid.New()
					pendPubID := fmt.Sprintf("opn_%s", strings.ReplaceAll(pendUUID.String(), "-", "")[:26])
					observability.NoteSwallowed(s.repo.ParkPendingInput(ctx, contracts.OCCPendingInput{
						ID:                 pendUUID,
						PublicID:           pendPubID,
						TenantID:           input.TenantID,
						Source:             "ENGINE_OCC",
						ExternalID:         input.InputID,
						CorrelationID:      pendUUID,
						SagaID:             input.SagaID,
						InputType:          input.InputType,
						InputPayload:       input.InputPayload,
						AttemptCount:       attempt + 1,
						LastConflictReason: "retry budget exhausted",
						Status:             contracts.PendingStatusPending,
					}))
					return res, appErrors.NewConflictError("concurrency retry budget exhausted; input parked as pending")
				}
				return res, appErrors.NewConflictError("optimistic concurrency conflict on client command")
			}

			backoff := s.rules.ComputeJitteredBackoff(attempt)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil, ctx.Err()
			}

			isAppliedNow, _ := s.repo.IsInputApplied(ctx, input.TenantID, input.SagaID, input.InputID)
			if isAppliedNow {
				return &contracts.CASResult{
					Succeeded:    true,
					ErrorMessage: "input applied by competing worker during backoff",
				}, nil
			}

			input.ExpectedVersion = res.CurrentVersion
		}
	}

	return nil, appErrors.NewConflictError("optimistic concurrency retry loop exhausted")
}

func (s *OCCService) ApplyClientEdit(
	ctx context.Context,
	input contracts.ClientEditInput,
) (spanResult0 *contracts.CASResult, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "occ.service_client_edit")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if err := contracts.ValidateClientEditInput(input); err != nil {
		return nil, err
	}

	state, err := s.repo.DiagnoseState(ctx, input.TenantID, input.SagaID)
	if err != nil {
		return nil, err
	}

	if state.Version != input.ExpectedVersion {
		return &contracts.CASResult{
			Succeeded:      false,
			Cause:          contracts.CauseVersionConflict,
			CurrentVersion: state.Version,
			CurrentEpoch:   state.OwnerEpoch,
			CurrentState:   state.CurrentState,
			ErrorMessage:   "client-supplied expected version does not match current saga version",
		}, appErrors.NewConflictError("optimistic concurrency conflict on client edit")
	}

	casInput := contracts.CASMutationInput{
		TenantID:        input.TenantID,
		SagaID:          input.SagaID,
		ExpectedVersion: state.Version,
		ExpectedEpoch:   state.OwnerEpoch,
		TargetState:     state.CurrentState,
		WorkerID:        input.ActorID,
		InputID:         uuid.New().String(),
		InputType:       contracts.InputTypeOperatorCommand,
		InputPayload:    input.Parameters,
	}

	return s.repo.ExecuteCAS(ctx, casInput)
}

func (s *OCCService) DiagnoseAggregate(
	ctx context.Context,
	tenantID string,
	sagaID uuid.UUID,
) (spanResult0 *contracts.SagaRootState, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "occ.service_diagnose_aggregate")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	return s.repo.DiagnoseState(ctx, tenantID, sagaID)
}

func (s *OCCService) EvaluateStrategy(
	ctx context.Context,
	tenantID string,
	sagaID uuid.UUID,
) (spanResult0 contracts.ConcurrencyStrategy, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "occ.service_evaluate_strategy")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	count, err := s.repo.GetRecentContentionCount(ctx, tenantID, sagaID, 5*time.Minute)
	if err != nil {
		return contracts.StrategyOCC, err
	}

	if s.rules.ShouldSerializeSaga(count, 3) {
		return contracts.StrategySerialized, nil
	}
	return contracts.StrategyOCC, nil
}

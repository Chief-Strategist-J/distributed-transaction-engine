package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"distributed-transaction-engine/src/features/compensation-retry/contracts"
	"distributed-transaction-engine/src/features/compensation-retry/domain/machines"
	"distributed-transaction-engine/src/features/compensation-retry/domain/rules"
	"distributed-transaction-engine/src/features/compensation-retry/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	"github.com/google/uuid"
)

type ServiceDeps struct {
	Repo    ports.RepositoryPort
	Rules   *rules.CompensationRetryRules
	Machine *machines.CompensationRetryMachine
	Tracer  *observability.DefaultTracer
}

type CompensationRetryService struct {
	repo    ports.RepositoryPort
	rules   *rules.CompensationRetryRules
	machine *machines.CompensationRetryMachine
	tracer  *observability.DefaultTracer
}

func NewService(deps ServiceDeps) *CompensationRetryService {
	r := deps.Rules
	if r == nil {
		r = rules.NewCompensationRetryRules()
	}
	m := deps.Machine
	if m == nil {
		m = machines.NewCompensationRetryMachine()
	}
	tr := deps.Tracer
	if tr == nil {
		tr = observability.NewTracer("compensation-retry-service")
	}

	return &CompensationRetryService{
		repo:    deps.Repo,
		rules:   r,
		machine: m,
		tracer:  tr,
	}
}

func (s *CompensationRetryService) Evaluate(ctx context.Context, input contracts.EvaluateCompensationRetryInput) (spanResult0 *contracts.CompensationRetryDecision, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "compensation_retry.service_evaluate")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	sagaUUID, firstTime, deadlineTime, err := contracts.ValidateEvaluateCompensationRetryInput(input)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()

	isPaused, err := s.repo.IsStepTypePaused(ctx, input.TenantID, input.StepType)
	if err == nil && isPaused {
		return &contracts.CompensationRetryDecision{
			Action:          "PAUSE_STEP_TYPE",
			OutcomeClass:    contracts.OutcomeContractDefect,
			DelayMillis:     0,
			EscalationLevel: contracts.EscalationNone,
			Reason:          "compensation step type is globally paused for defect resolution",
		}, nil
	}

	if input.IsBreakerOpen {
		observability.NoteSwallowed(s.repo.UpdateStatus(ctx, input.TenantID, *sagaUUID, input.StepID, string(machines.CompensationStateFrozenBreaker), nil, "dependency circuit breaker open"))
		return &contracts.CompensationRetryDecision{
			Action:          "FREEZE_BREAKER",
			OutcomeClass:    contracts.OutcomeDependencyDown,
			DelayMillis:     0,
			EscalationLevel: contracts.EscalationNone,
			Reason:          "downstream dependency circuit breaker is OPEN; compensation retry frozen without consuming budget",
		}, nil
	}

	outcomeClass := s.rules.ClassifyResult(input.StatusCode, input.ErrorMessage)
	escalation, isExhausted := s.rules.DetermineEscalation(now, *firstTime, *deadlineTime, input.AttemptNumber, 10)

	if outcomeClass == contracts.OutcomeDone {
		observability.NoteSwallowed(s.repo.UpdateStatus(ctx, input.TenantID, *sagaUUID, input.StepID, string(machines.CompensationStateCompensated), nil, ""))
		return &contracts.CompensationRetryDecision{
			Action:          "DONE",
			OutcomeClass:    outcomeClass,
			DelayMillis:     0,
			EscalationLevel: contracts.EscalationNone,
			Reason:          "compensation successfully executed",
		}, nil
	}

	if outcomeClass == contracts.OutcomeAmbiguous {
		observability.NoteSwallowed(s.repo.UpdateStatus(ctx, input.TenantID, *sagaUUID, input.StepID, string(machines.CompensationStateVerifyingAmbiguous), nil, input.ErrorMessage))
		return &contracts.CompensationRetryDecision{
			Action:          "VERIFY_AMBIGUOUS",
			OutcomeClass:    outcomeClass,
			DelayMillis:     0,
			EscalationLevel: escalation,
			Reason:          "timeout or 504 received; status query required before re-sending to prevent double refund",
		}, nil
	}

	if outcomeClass == contracts.OutcomeContractDefect {
		observability.NoteSwallowed(s.repo.PauseStepType(ctx, input.TenantID, input.StepType))
		observability.NoteSwallowed(s.repo.UpdateStatus(ctx, input.TenantID, *sagaUUID, input.StepID, string(machines.CompensationStateStepTypePaused), nil, input.ErrorMessage))
		return &contracts.CompensationRetryDecision{
			Action:          "PAUSE_STEP_TYPE",
			OutcomeClass:    outcomeClass,
			DelayMillis:     0,
			EscalationLevel: contracts.EscalationLevel2Deadline,
			Reason:          "schema or contract defect detected (HTTP 400); step type paused globally for bulk fix",
		}, nil
	}

	if outcomeClass == contracts.OutcomeCredential {
		observability.NoteSwallowed(s.repo.PauseStepType(ctx, input.TenantID, input.StepType))
		observability.NoteSwallowed(s.repo.UpdateStatus(ctx, input.TenantID, *sagaUUID, input.StepID, string(machines.CompensationStateStepTypePaused), nil, input.ErrorMessage))
		return &contracts.CompensationRetryDecision{
			Action:          "PAUSE_STEP_TYPE",
			OutcomeClass:    outcomeClass,
			DelayMillis:     0,
			EscalationLevel: contracts.EscalationLevel2Deadline,
			Reason:          "credential or authorization failure (HTTP 401/403); step type paused for credential rotation",
		}, nil
	}

	if outcomeClass == contracts.OutcomeBusinessAlternative {
		observability.NoteSwallowed(s.repo.UpdateStatus(ctx, input.TenantID, *sagaUUID, input.StepID, string(machines.CompensationStateNeedsAttention), nil, input.ErrorMessage))
		return &contracts.CompensationRetryDecision{
			Action:          "BUSINESS_ALTERNATIVE",
			OutcomeClass:    outcomeClass,
			DelayMillis:     0,
			EscalationLevel: contracts.EscalationLevel2Deadline,
			Reason:          "target reports not compensable (already shipped, window closed, or locked); requires business alternative",
		}, nil
	}

	var retryAfterDuration time.Duration
	if input.RetryAfterHeader != "" {
		if d, ok := contracts.ParseRetryAfter(input.RetryAfterHeader, now); ok {
			retryAfterDuration = d
		}
	}

	backoff := s.rules.CalculateBackoff(input.AttemptNumber, 1000, 30000, retryAfterDuration)
	nextRetry := now.Add(backoff)

	recordID := uuid.New()
	pubID := fmt.Sprintf("cpr_%s", strings.ReplaceAll(recordID.String(), "-", "")[:26])
	extID := fmt.Sprintf("%s:%s", sagaUUID.String(), input.StepID)

	record := contracts.CompensationRetryRecord{
		ID:               recordID,
		PublicID:         pubID,
		TenantID:         input.TenantID,
		Source:           "ENGINE_COMPENSATION",
		ExternalID:       extID,
		CorrelationID:    recordID,
		SagaID:           *sagaUUID,
		StepID:           input.StepID,
		StepType:         input.StepType,
		DependencyID:     input.DependencyID,
		AttemptNumber:    input.AttemptNumber,
		MaxAttempts:      10,
		FirstAttemptAt:   *firstTime,
		BusinessDeadline: *deadlineTime,
		NextRetryAt:      &nextRetry,
		Status:           string(machines.CompensationStateRetryScheduled),
		LastError:        input.ErrorMessage,
	}

	if isExhausted {
		record.Status = string(machines.CompensationStateNeedsAttention)
		record.NextRetryAt = nil
		record.LastError = "retry budget or business deadline exhausted"
		_, swallowedErr2 := s.repo.UpsertAttempt(ctx, record)
		observability.NoteSwallowed(swallowedErr2)
		observability.NoteSwallowed(s.repo.UpdateStatus(ctx, input.TenantID, *sagaUUID, input.StepID, string(machines.CompensationStateNeedsAttention), nil, "retry budget or business deadline exhausted"))
		return &contracts.CompensationRetryDecision{
			Action:          "NEEDS_ATTENTION",
			OutcomeClass:    contracts.OutcomeNeedsAttention,
			DelayMillis:     0,
			EscalationLevel: escalation,
			Reason:          "compensation retry horizon or business deadline exhausted; non-terminal escalation to operator",
		}, nil
	}
	_, swallowedErr1 := s.repo.UpsertAttempt(ctx, record)
	observability.NoteSwallowed(swallowedErr1)

	return &contracts.CompensationRetryDecision{
		Action:          "RETRY",
		OutcomeClass:    outcomeClass,
		DelayMillis:     backoff.Milliseconds(),
		EscalationLevel: escalation,
		Reason:          "transient rollback error; compensation retry scheduled with jittered backoff",
	}, nil
}

func (s *CompensationRetryService) GetRecord(ctx context.Context, tenantID string, sagaID uuid.UUID, stepID string) (spanResult0 *contracts.CompensationRetryRecord, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "compensation_retry.service_get_record")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	return s.repo.GetRecord(ctx, tenantID, sagaID, stepID)
}

func (s *CompensationRetryService) PauseStepType(ctx context.Context, tenantID string, stepType string) (spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "compensation_retry.service_pause_step_type")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	return s.repo.PauseStepType(ctx, tenantID, stepType)
}

func (s *CompensationRetryService) ResumeStepType(ctx context.Context, tenantID string, stepType string) (spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "compensation_retry.service_resume_step_type")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	return s.repo.ResumeStepType(ctx, tenantID, stepType)
}

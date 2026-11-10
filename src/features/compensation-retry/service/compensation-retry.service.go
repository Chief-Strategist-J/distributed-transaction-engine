package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"distributed-transaction-engine/src/features/compensation-retry/contracts"
	"distributed-transaction-engine/src/features/compensation-retry/domain/machines"
	"distributed-transaction-engine/src/features/compensation-retry/domain/rules"
	"distributed-transaction-engine/src/features/compensation-retry/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
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
		if err := s.recordStatus(ctx, input, sagaUUID, firstTime, deadlineTime, string(machines.CompensationStateFrozenBreaker), "dependency circuit breaker open"); err != nil {
			return nil, err
		}
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
		if err := s.recordStatus(ctx, input, sagaUUID, firstTime, deadlineTime, string(machines.CompensationStateCompensated), ""); err != nil {
			return nil, err
		}
		return &contracts.CompensationRetryDecision{
			Action:          "DONE",
			OutcomeClass:    outcomeClass,
			DelayMillis:     0,
			EscalationLevel: contracts.EscalationNone,
			Reason:          "compensation successfully executed",
		}, nil
	}

	if outcomeClass == contracts.OutcomeAmbiguous {
		if err := s.recordStatus(ctx, input, sagaUUID, firstTime, deadlineTime, string(machines.CompensationStateVerifyingAmbiguous), input.ErrorMessage); err != nil {
			return nil, err
		}
		return &contracts.CompensationRetryDecision{
			Action:          "VERIFY_AMBIGUOUS",
			OutcomeClass:    outcomeClass,
			DelayMillis:     0,
			EscalationLevel: escalation,
			Reason:          "timeout or 504 received; status query required before re-sending to prevent double refund",
		}, nil
	}

	if outcomeClass == contracts.OutcomeContractDefect {
		if err := s.repo.PauseStepType(ctx, input.TenantID, input.StepType); err != nil {
			return nil, err
		}
		if err := s.recordStatus(ctx, input, sagaUUID, firstTime, deadlineTime, string(machines.CompensationStateStepTypePaused), input.ErrorMessage); err != nil {
			return nil, err
		}
		return &contracts.CompensationRetryDecision{
			Action:          "PAUSE_STEP_TYPE",
			OutcomeClass:    outcomeClass,
			DelayMillis:     0,
			EscalationLevel: contracts.EscalationLevel2Deadline,
			Reason:          "schema or contract defect detected (HTTP 400); step type paused globally for bulk fix",
		}, nil
	}

	if outcomeClass == contracts.OutcomeCredential {
		if err := s.repo.PauseStepType(ctx, input.TenantID, input.StepType); err != nil {
			return nil, err
		}
		if err := s.recordStatus(ctx, input, sagaUUID, firstTime, deadlineTime, string(machines.CompensationStateStepTypePaused), input.ErrorMessage); err != nil {
			return nil, err
		}
		return &contracts.CompensationRetryDecision{
			Action:          "PAUSE_STEP_TYPE",
			OutcomeClass:    outcomeClass,
			DelayMillis:     0,
			EscalationLevel: contracts.EscalationLevel2Deadline,
			Reason:          "credential or authorization failure (HTTP 401/403); step type paused for credential rotation",
		}, nil
	}

	if outcomeClass == contracts.OutcomeBusinessAlternative {
		if err := s.recordStatus(ctx, input, sagaUUID, firstTime, deadlineTime, string(machines.CompensationStateNeedsAttention), input.ErrorMessage); err != nil {
			return nil, err
		}
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

	record := s.buildRecord(input, sagaUUID, firstTime, deadlineTime, string(machines.CompensationStateRetryScheduled), &nextRetry, input.ErrorMessage)

	if isExhausted {
		record.Status = string(machines.CompensationStateNeedsAttention)
		record.NextRetryAt = nil
		record.LastError = "retry budget or business deadline exhausted"
		if _, err := s.repo.UpsertAttempt(ctx, record); err != nil {
			return nil, err
		}
		if err := s.recordStatus(ctx, input, sagaUUID, firstTime, deadlineTime, string(machines.CompensationStateNeedsAttention), "retry budget or business deadline exhausted"); err != nil {
			return nil, err
		}
		return &contracts.CompensationRetryDecision{
			Action:          "NEEDS_ATTENTION",
			OutcomeClass:    contracts.OutcomeNeedsAttention,
			DelayMillis:     0,
			EscalationLevel: escalation,
			Reason:          "compensation retry horizon or business deadline exhausted; non-terminal escalation to operator",
		}, nil
	}
	if _, err := s.repo.UpsertAttempt(ctx, record); err != nil {
		return nil, err
	}

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

func (s *CompensationRetryService) buildRecord(input contracts.EvaluateCompensationRetryInput, sagaUUID *uuid.UUID, firstTime *time.Time, deadlineTime *time.Time, status string, nextRetry *time.Time, lastError string) contracts.CompensationRetryRecord {
	recordID := uuid.New()
	return contracts.CompensationRetryRecord{
		ID:               recordID,
		PublicID:         fmt.Sprintf("cpr_%s", strings.ReplaceAll(recordID.String(), "-", "")[:26]),
		TenantID:         input.TenantID,
		Source:           "ENGINE_COMPENSATION",
		ExternalID:       fmt.Sprintf("%s:%s", sagaUUID.String(), input.StepID),
		CorrelationID:    recordID,
		SagaID:           *sagaUUID,
		StepID:           input.StepID,
		StepType:         input.StepType,
		DependencyID:     input.DependencyID,
		AttemptNumber:    input.AttemptNumber,
		MaxAttempts:      10,
		FirstAttemptAt:   *firstTime,
		BusinessDeadline: *deadlineTime,
		NextRetryAt:      nextRetry,
		Status:           status,
		LastError:        lastError,
	}
}

func (s *CompensationRetryService) recordStatus(ctx context.Context, input contracts.EvaluateCompensationRetryInput, sagaUUID *uuid.UUID, firstTime *time.Time, deadlineTime *time.Time, status string, lastError string) error {
	err := s.repo.UpdateStatus(ctx, input.TenantID, *sagaUUID, input.StepID, status, nil, lastError)
	var applicationError *appErrors.ApplicationError
	if err == nil || !errors.As(err, &applicationError) || applicationError.StatusCode != http.StatusNotFound {
		return err
	}
	_, upsertErr := s.repo.UpsertAttempt(ctx, s.buildRecord(input, sagaUUID, firstTime, deadlineTime, status, nil, lastError))
	return upsertErr
}

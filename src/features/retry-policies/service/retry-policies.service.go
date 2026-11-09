package service

import (
	"context"
	"time"

	"distributed-transaction-engine/src/features/retry-policies/contracts"
	"distributed-transaction-engine/src/features/retry-policies/domain/machines"
	"distributed-transaction-engine/src/features/retry-policies/domain/rules"
	"distributed-transaction-engine/src/features/retry-policies/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
)

type ServiceDeps struct {
	Repo    ports.RepositoryPort
	Breaker *machines.CircuitBreakerManager
	Tracer  *observability.DefaultTracer
}

type RetryPolicyService struct {
	repo    ports.RepositoryPort
	breaker *machines.CircuitBreakerManager
	tracer  *observability.DefaultTracer
	logger  *observability.LokiLogger
}

func NewService(deps ServiceDeps) *RetryPolicyService {
	tracer := deps.Tracer
	if tracer == nil {
		tracer = observability.NewTracer("retry-policies-service")
	}
	breaker := deps.Breaker
	if breaker == nil {
		breaker = machines.NewCircuitBreakerManager()
	}
	return &RetryPolicyService{
		repo:    deps.Repo,
		breaker: breaker,
		tracer:  tracer,
		logger:  observability.GetLogger(),
	}
}

func (s *RetryPolicyService) Evaluate(
	ctx context.Context,
	input contracts.EvaluateRetryInput,
) (*contracts.RetryDecision, error) {
	ctx, span := s.tracer.StartSpan(ctx, "retry_policies.evaluate")
	defer span.End()

	sagaUUID, firstTime, effectiveDeadline, err := contracts.ValidateEvaluateInput(input)
	if err != nil {
		observability.RecordError(span, err)
		return nil, err
	}

	if input.DependencyID != "" && !s.breaker.AllowRequest(input.DependencyID) {
		decision := contracts.RetryDecision{
			Action:     contracts.ActionPostponeBreaker,
			ErrorClass: contracts.ClassTransientNotExecuted,
			Reason:     "dependency circuit breaker is OPEN; retry postponed with frozen budget",
		}
		return &decision, nil
	}

	now := time.Now().UTC()
	retryAfterDelay, hasRetryAfter := contracts.ParseRetryAfter(input.RetryAfterHeader, now)

	errClass := rules.ClassifyError(
		input.StatusCode,
		input.ErrorMessage,
		input.AttemptNumber,
		input.FingerprintHistory,
	)

	computedDelay := rules.CalculateBackoff(
		input.AttemptNumber,
		input.Policy,
		retryAfterDelay,
		hasRetryAfter,
	)

	decision := rules.EvaluateDecision(
		errClass,
		input.AttemptNumber,
		*firstTime,
		effectiveDeadline,
		input.Capability,
		input.Position,
		input.Policy,
		computedDelay,
		now,
	)

	errFp := rules.ComputeFingerprint(input.StatusCode, input.ErrorMessage)
	var nextAttempt *time.Time
	if decision.Action == contracts.ActionRetry {
		t := now.Add(decision.Delay)
		nextAttempt = &t
	}

	retryAfterSec := 0
	if hasRetryAfter {
		retryAfterSec = int(retryAfterDelay.Seconds())
	}

	_ = s.repo.RecordStepAttempt(
		ctx,
		input.TenantID,
		sagaUUID.String(),
		input.StepID,
		input.AttemptNumber,
		string(errClass),
		input.ErrorMessage,
		errFp,
		retryAfterSec,
		decision.DelayMillis,
		nextAttempt,
	)

	if input.DependencyID != "" {
		if errClass == contracts.ClassTransientNotExecuted || errClass == contracts.ClassAmbiguous {
			s.breaker.RecordFailure(input.DependencyID)
		} else if decision.Action != contracts.ActionRetry {
			s.breaker.RecordSuccess(input.DependencyID)
		}
	}

	return &decision, nil
}

func (s *RetryPolicyService) GetBreakerState(ctx context.Context, dependencyID string) machines.BreakerState {
	return s.breaker.GetState(dependencyID)
}

func (s *RetryPolicyService) RecordBreakerSuccess(ctx context.Context, dependencyID string) {
	s.breaker.RecordSuccess(dependencyID)
}

func (s *RetryPolicyService) RecordBreakerFailure(ctx context.Context, dependencyID string) {
	s.breaker.RecordFailure(dependencyID)
}

func (s *RetryPolicyService) ResetBreaker(ctx context.Context, dependencyID string) {
	s.breaker.Reset(dependencyID)
}


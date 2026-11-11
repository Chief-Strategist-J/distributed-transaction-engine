package machines

import (
	contracts "distributed-transaction-engine/src/features/transactional-outbox/contracts"
	rules "distributed-transaction-engine/src/features/transactional-outbox/domain/rules"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
)

type OutboxStateMachine struct{}

func NewOutboxStateMachine() *OutboxStateMachine {
	return &OutboxStateMachine{}
}

func (m *OutboxStateMachine) EvaluatePublishOutcome(
	currentStatus contracts.OutboxStatus,
	attemptCount int,
	publishErr error,
) (spanResult0 contracts.OutboxStatus, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if publishErr == nil {
		if !rules.CanTransitionOutboxStatus(currentStatus, contracts.OutboxStatusPublished) {
			return currentStatus, errors.NewConflictError("illegal state transition to PUBLISHED")
		}
		return contracts.OutboxStatusPublished, nil
	}

	classification := rules.ClassifyPublishError(publishErr)
	if classification == rules.ErrorNonRetriable || rules.ExceededPublishAttempts(attemptCount) {
		if !rules.CanTransitionOutboxStatus(currentStatus, contracts.OutboxStatusParked) {
			return currentStatus, errors.NewConflictError("illegal state transition to PARKED")
		}
		return contracts.OutboxStatusParked, nil
	}

	if !rules.CanTransitionOutboxStatus(currentStatus, contracts.OutboxStatusPending) {
		return currentStatus, errors.NewConflictError("illegal state transition to PENDING")
	}
	return contracts.OutboxStatusPending, nil
}

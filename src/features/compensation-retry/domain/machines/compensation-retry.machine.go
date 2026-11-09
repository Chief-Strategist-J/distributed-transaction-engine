package machines

import (
	appErrors "distributed-transaction-engine/src/shared/errors"
)

type CompensationState string

const (
	CompensationStatePending            CompensationState = "PENDING"
	CompensationStateRetryScheduled     CompensationState = "RETRY_SCHEDULED"
	CompensationStateVerifyingAmbiguous CompensationState = "VERIFYING_AMBIGUOUS"
	CompensationStateCompensated        CompensationState = "COMPENSATED"
	CompensationStateNeedsAttention     CompensationState = "NEEDS_ATTENTION"
	CompensationStateStepTypePaused     CompensationState = "STEP_TYPE_PAUSED"
	CompensationStateFrozenBreaker      CompensationState = "FROZEN_BREAKER"
)

type CompensationEvent string

const (
	CompensationEventSucceed           CompensationEvent = "SUCCEED"
	CompensationEventScheduleRetry     CompensationEvent = "SCHEDULE_RETRY"
	CompensationEventMarkAmbiguous     CompensationEvent = "MARK_AMBIGUOUS"
	CompensationEventPauseStepType     CompensationEvent = "PAUSE_STEP_TYPE"
	CompensationEventFreezeBreaker     CompensationEvent = "FREEZE_BREAKER"
	CompensationEventUnfreezeBreaker   CompensationEvent = "UNFREEZE_BREAKER"
	CompensationEventEscalateAttention CompensationEvent = "ESCALATE_ATTENTION"
)

type CompensationRetryMachine struct{}

func NewCompensationRetryMachine() *CompensationRetryMachine {
	return &CompensationRetryMachine{}
}

func (m *CompensationRetryMachine) Transition(current CompensationState, event CompensationEvent) (CompensationState, error) {
	switch current {
	case CompensationStatePending, CompensationStateRetryScheduled:
		switch event {
		case CompensationEventSucceed:
			return CompensationStateCompensated, nil
		case CompensationEventScheduleRetry:
			return CompensationStateRetryScheduled, nil
		case CompensationEventMarkAmbiguous:
			return CompensationStateVerifyingAmbiguous, nil
		case CompensationEventPauseStepType:
			return CompensationStateStepTypePaused, nil
		case CompensationEventFreezeBreaker:
			return CompensationStateFrozenBreaker, nil
		case CompensationEventEscalateAttention:
			return CompensationStateNeedsAttention, nil
		}
	case CompensationStateVerifyingAmbiguous:
		switch event {
		case CompensationEventSucceed:
			return CompensationStateCompensated, nil
		case CompensationEventScheduleRetry:
			return CompensationStateRetryScheduled, nil
		case CompensationEventEscalateAttention:
			return CompensationStateNeedsAttention, nil
		}
	case CompensationStateFrozenBreaker:
		switch event {
		case CompensationEventUnfreezeBreaker, CompensationEventScheduleRetry:
			return CompensationStateRetryScheduled, nil
		case CompensationEventEscalateAttention:
			return CompensationStateNeedsAttention, nil
		}
	case CompensationStateStepTypePaused:
		if event == CompensationEventScheduleRetry {
			return CompensationStateRetryScheduled, nil
		}
	}

	return current, appErrors.NewConflictError("illegal state transition from " + string(current) + " with event " + string(event))
}

func (m *CompensationRetryMachine) IsTerminal(state CompensationState) bool {
	return state == CompensationStateCompensated || state == CompensationStateNeedsAttention
}


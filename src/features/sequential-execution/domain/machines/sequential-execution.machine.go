package machines

import (
	contracts "distributed-transaction-engine/src/features/sequential-execution/contracts"
	rules "distributed-transaction-engine/src/features/sequential-execution/domain/rules"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
)

type SequentialCommandType string

const (
	CommandDispatchStep      SequentialCommandType = "DISPATCH_STEP"
	CommandCompensateStep    SequentialCommandType = "COMPENSATE_STEP"
	CommandCompleteSaga      SequentialCommandType = "COMPLETE_SAGA"
	CommandFailCompensated   SequentialCommandType = "FAIL_COMPENSATED"
	CommandScheduleRetry     SequentialCommandType = "SCHEDULE_RETRY"
	CommandEscalateAttention SequentialCommandType = "ESCALATE_ATTENTION"
)

type SequentialCommand struct {
	Type      SequentialCommandType
	StepIndex int
}

type SequentialTransitionDecision struct {
	NextPhase        contracts.ExecutionPhase
	NextStepIndex    int
	NextSagaState    string
	NextPivotReached bool
	Commands         []SequentialCommand
}

type SequentialExecutionMachine struct{}

func NewSequentialExecutionMachine() *SequentialExecutionMachine {
	return &SequentialExecutionMachine{}
}

func (m *SequentialExecutionMachine) EvaluateStepSuccess(
	currentPhase contracts.ExecutionPhase,
	currentIndex int,
	totalSteps int,
	isPivot bool,
	pivotReached bool,
) (spanResult0 SequentialTransitionDecision, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if currentPhase == contracts.PhaseForward {
		updatedPivotReached := pivotReached || isPivot
		nextIndex, hasMore := rules.NextForwardStepIndex(currentIndex, totalSteps)
		if hasMore {
			return SequentialTransitionDecision{
				NextPhase:        contracts.PhaseForward,
				NextStepIndex:    nextIndex,
				NextSagaState:    "RUNNING",
				NextPivotReached: updatedPivotReached,
				Commands:         []SequentialCommand{{Type: CommandDispatchStep, StepIndex: nextIndex}},
			}, nil
		}
		return SequentialTransitionDecision{
			NextPhase:        contracts.PhaseForward,
			NextStepIndex:    currentIndex,
			NextSagaState:    "COMPLETED",
			NextPivotReached: updatedPivotReached,
			Commands:         []SequentialCommand{{Type: CommandCompleteSaga, StepIndex: currentIndex}},
		}, nil
	}

	if currentPhase == contracts.PhaseCompensating {
		prevIndex, hasMore := rules.NextCompensatingStepIndex(currentIndex)
		if hasMore {
			return SequentialTransitionDecision{
				NextPhase:        contracts.PhaseCompensating,
				NextStepIndex:    prevIndex,
				NextSagaState:    "COMPENSATING",
				NextPivotReached: pivotReached,
				Commands:         []SequentialCommand{{Type: CommandCompensateStep, StepIndex: prevIndex}},
			}, nil
		}
		return SequentialTransitionDecision{
			NextPhase:        contracts.PhaseCompensating,
			NextStepIndex:    0,
			NextSagaState:    "FAILED_COMPENSATED",
			NextPivotReached: pivotReached,
			Commands:         []SequentialCommand{{Type: CommandFailCompensated, StepIndex: 0}},
		}, nil
	}

	return SequentialTransitionDecision{}, errors.NewConflictError("invalid phase for success evaluation")
}

func (m *SequentialExecutionMachine) EvaluateStepFailure(
	currentPhase contracts.ExecutionPhase,
	currentIndex int,
	pivotReached bool,
	isRetryable bool,
) (spanResult0 SequentialTransitionDecision, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if isRetryable {
		return SequentialTransitionDecision{
			NextPhase:        currentPhase,
			NextStepIndex:    currentIndex,
			NextSagaState:    "RUNNING",
			NextPivotReached: pivotReached,
			Commands:         []SequentialCommand{{Type: CommandScheduleRetry, StepIndex: currentIndex}},
		}, nil
	}

	if pivotReached {
		return SequentialTransitionDecision{
			NextPhase:        contracts.PhaseRollForward,
			NextStepIndex:    currentIndex,
			NextSagaState:    "ROLLING_FORWARD",
			NextPivotReached: true,
			Commands:         []SequentialCommand{{Type: CommandEscalateAttention, StepIndex: currentIndex}},
		}, nil
	}

	prevIndex, hasMore := rules.NextCompensatingStepIndex(currentIndex)
	if hasMore {
		return SequentialTransitionDecision{
			NextPhase:        contracts.PhaseCompensating,
			NextStepIndex:    prevIndex,
			NextSagaState:    "COMPENSATING",
			NextPivotReached: false,
			Commands:         []SequentialCommand{{Type: CommandCompensateStep, StepIndex: prevIndex}},
		}, nil
	}

	return SequentialTransitionDecision{
		NextPhase:        contracts.PhaseCompensating,
		NextStepIndex:    0,
		NextSagaState:    "FAILED_COMPENSATED",
		NextPivotReached: false,
		Commands:         []SequentialCommand{{Type: CommandFailCompensated, StepIndex: 0}},
	}, nil
}

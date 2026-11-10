package rules

import (
	contracts "distributed-transaction-engine/src/features/sequential-execution/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/errors"
)

func NextForwardStepIndex(currentIndex int, totalSteps int) (int, bool) {
	defer observability.TraceEnter(nil)()
	if currentIndex < 0 || totalSteps <= 0 {
		return currentIndex, false
	}
	nextIndex := currentIndex + 1
	if nextIndex >= totalSteps {
		return currentIndex, false
	}
	return nextIndex, true
}

func NextCompensatingStepIndex(currentIndex int) (int, bool) {
	defer observability.TraceEnter(nil)()
	if currentIndex <= 0 {
		return 0, false
	}
	return currentIndex - 1, true
}

func CanCompensate(phase contracts.ExecutionPhase, pivotReached bool, nature contracts.StepNature) bool {
	defer observability.TraceEnter(nil)()
	if pivotReached {
		return false
	}
	if phase != contracts.PhaseForward && phase != contracts.PhaseCompensating {
		return false
	}
	return nature == contracts.NatureCompensable
}

func DetermineNextPhaseOnFailure(pivotReached bool) contracts.ExecutionPhase {
	defer observability.TraceEnter(nil)()
	if pivotReached {
		return contracts.PhaseRollForward
	}
	return contracts.PhaseCompensating
}

func ValidateStepOrdering(natures []contracts.StepNature) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if len(natures) == 0 {
		return errors.NewValidationError("workflow must contain at least one step")
	}
	seenPivot := false
	seenRetriable := false
	for _, n := range natures {
		if n == contracts.NatureCompensable {
			if seenPivot || seenRetriable {
				return errors.NewValidationError("compensable steps cannot follow pivot or retriable steps")
			}
		} else if n == contracts.NaturePivot {
			if seenPivot {
				return errors.NewValidationError("workflow cannot contain multiple pivot steps")
			}
			if seenRetriable {
				return errors.NewValidationError("pivot step cannot follow retriable steps")
			}
			seenPivot = true
		} else if n == contracts.NatureRetriable {
			seenRetriable = true
		} else {
			return errors.NewValidationError("unknown step nature")
		}
	}
	return nil
}

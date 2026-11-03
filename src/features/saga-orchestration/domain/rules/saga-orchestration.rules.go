package rules

import (
	"fmt"
	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
	workflowContracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
	"distributed-transaction-engine/src/shared/errors"
)

func CanTransition(from sagaContracts.SagaState, to sagaContracts.SagaState) bool {
	switch from {
	case sagaContracts.SagaStatePending, sagaContracts.SagaStateCreated:
		return to == sagaContracts.SagaStateRunning || to == sagaContracts.SagaStateFailed || to == sagaContracts.SagaStateCancelled
	case sagaContracts.SagaStateRunning:
		return to == sagaContracts.SagaStateRollingForward || to == sagaContracts.SagaStateStuckForward || to == sagaContracts.SagaStateCompensating || to == sagaContracts.SagaStateCompleted || to == sagaContracts.SagaStateFailed || to == sagaContracts.SagaStateCancelled
	case sagaContracts.SagaStateRollingForward:
		return to == sagaContracts.SagaStateStuckForward || to == sagaContracts.SagaStateCompleted || to == sagaContracts.SagaStateFailed
	case sagaContracts.SagaStateCompensating:
		return to == sagaContracts.SagaStateCompensationStuck || to == sagaContracts.SagaStateFailedCompensated || to == sagaContracts.SagaStateFailed || to == sagaContracts.SagaStateCancelled
	default:
		return false
	}
}

func ValidateSagaTransition(from sagaContracts.SagaState, to sagaContracts.SagaState) error {
	if !CanTransition(from, to) {
		return errors.NewConflictError(fmt.Sprintf("illegal saga state transition from %s to %s", from, to))
	}
	return nil
}

func DeriveForwardIdempotencyKey(sagaID string, stepID string) string {
	return "idem:" + sagaID + ":" + stepID + ":fwd"
}

func DeriveCompensationIdempotencyKey(sagaID string, stepID string) string {
	return "idem:" + sagaID + ":" + stepID + ":comp"
}

func DetermineFailureRouting(isPivotSucceeded bool) string {
	if isPivotSucceeded {
		return "FORWARD_ONLY"
	}
	return "BACKWARD"
}

func FindInitialExecutableSteps(steps []workflowContracts.StepDefinition) []string {
	var initialSteps []string
	for _, step := range steps {
		if len(step.Dependencies) == 0 {
			initialSteps = append(initialSteps, step.StepID)
		}
	}
	return initialSteps
}

func FindNextExecutableSteps(steps []workflowContracts.StepDefinition, completedSteps map[string]bool, inFlightSteps map[string]bool) []string {
	var nextSteps []string
	for _, step := range steps {
		if completedSteps[step.StepID] || inFlightSteps[step.StepID] {
			continue
		}
		allDepsSatisfied := true
		for _, dep := range step.Dependencies {
			if !completedSteps[dep] {
				allDepsSatisfied = false
				break
			}
		}
		if allDepsSatisfied {
			nextSteps = append(nextSteps, step.StepID)
		}
	}
	return nextSteps
}

func ClassifyStepResult(isAmbiguous bool, err error) sagaContracts.StepExecutionState {
	if err == nil {
		return sagaContracts.StepStateSucceeded
	}
	if isAmbiguous {
		return sagaContracts.StepStateUnknown
	}
	return sagaContracts.StepStateFailed
}

func DeriveDrainPlan(steps []sagaContracts.SagaStepExecution, inFlightSteps map[string]bool) ([]sagaContracts.SagaStepExecution, map[string]bool) {
	updated := make([]sagaContracts.SagaStepExecution, len(steps))
	compensationSet := make(map[string]bool)
	for i, st := range steps {
		updated[i] = st
		if st.ExecutionState == sagaContracts.StepStatePending {
			updated[i].ExecutionState = sagaContracts.StepStateNotStarted
		} else if inFlightSteps[st.StepID] && st.ExecutionState == sagaContracts.StepStateDispatched {
			updated[i].ExecutionState = sagaContracts.StepStateUnknown
			compensationSet[st.StepID] = true
		} else if st.ExecutionState == sagaContracts.StepStateSucceeded || st.ExecutionState == sagaContracts.StepStateUnknown {
			compensationSet[st.StepID] = true
		}
	}
	return updated, compensationSet
}

func EvaluatePostCompensationState(allCompensated bool) sagaContracts.SagaState {
	if allCompensated {
		return sagaContracts.SagaStateFailedCompensated
	}
	return sagaContracts.SagaStateCompensationStuck
}

func EvaluatePostPivotFailureState() sagaContracts.SagaState {
	return sagaContracts.SagaStateStuckForward
}

func ComputeReverseCompensationChain(steps []workflowContracts.StepDefinition, topoOrder []string, eligibleSteps map[string]bool) []string {
	stepMap := make(map[string]workflowContracts.StepDefinition, len(steps))
	for _, s := range steps {
		stepMap[s.StepID] = s
	}

	var compensationChain []string
	for i := len(topoOrder) - 1; i >= 0; i-- {
		stepID := topoOrder[i]
		if !eligibleSteps[stepID] {
			continue
		}
		stepDef, exists := stepMap[stepID]
		if !exists || stepDef.IsPivot {
			continue
		}
		if stepDef.CompensationAction != "" {
			compensationChain = append(compensationChain, stepID)
		}
	}
	return compensationChain
}

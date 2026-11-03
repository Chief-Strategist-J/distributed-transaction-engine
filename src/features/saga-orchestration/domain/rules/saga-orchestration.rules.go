package rules

import (
	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
	workflowContracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
)

func CanTransition(from sagaContracts.SagaState, to sagaContracts.SagaState) bool {
	switch from {
	case sagaContracts.SagaStatePending, sagaContracts.SagaStateCreated:
		return to == sagaContracts.SagaStateRunning || to == sagaContracts.SagaStateFailed
	case sagaContracts.SagaStateRunning:
		return to == sagaContracts.SagaStateCompensating || to == sagaContracts.SagaStateCompleted || to == sagaContracts.SagaStateFailed
	case sagaContracts.SagaStateCompensating:
		return to == sagaContracts.SagaStateFailed
	default:
		return false
	}
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

func ComputeReverseCompensationChain(steps []workflowContracts.StepDefinition, topoOrder []string, succeededSteps map[string]bool) []string {
	stepMap := make(map[string]workflowContracts.StepDefinition, len(steps))
	for _, s := range steps {
		stepMap[s.StepID] = s
	}

	var compensationChain []string
	for i := len(topoOrder) - 1; i >= 0; i-- {
		stepID := topoOrder[i]
		if !succeededSteps[stepID] {
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

package rules

import (
	contracts "distributed-transaction-engine/src/features/parallel-execution/contracts"
	"distributed-transaction-engine/src/infra/observability"
)

type JoinEvaluationResult struct {
	IsDecided bool
	IsSuccess bool
	State     contracts.GroupState
}

func EvaluateJoinPolicy(
	policy contracts.JoinPolicy,
	quorumCount int,
	branches []contracts.ParallelBranchInstance,
) JoinEvaluationResult {
	defer observability.TraceEnter(nil)()
	total := len(branches)
	succeeded := 0
	failed := 0
	pending := 0
	hasRequiredFailure := false

	for _, b := range branches {
		switch b.CurrentState {
		case contracts.BranchStateSucceeded:
			succeeded++
		case contracts.BranchStateRejected, contracts.BranchStateUnknown:
			failed++
			if b.Role == contracts.RoleRequired {
				hasRequiredFailure = true
			}
		case contracts.BranchStatePending, contracts.BranchStateDispatched, contracts.BranchStateFailedRetry:
			pending++
		}
	}

	switch policy {
	case contracts.PolicyFirstSuccess:
		if succeeded >= 1 {
			return JoinEvaluationResult{IsDecided: true, IsSuccess: true, State: contracts.GroupStateJoinSucceeded}
		}
		if pending == 0 {
			return JoinEvaluationResult{IsDecided: true, IsSuccess: false, State: contracts.GroupStateJoinFailed}
		}
		return JoinEvaluationResult{IsDecided: false, IsSuccess: false, State: contracts.GroupStateFanoutActive}

	case contracts.PolicyNOfM:
		if succeeded >= quorumCount {
			return JoinEvaluationResult{IsDecided: true, IsSuccess: true, State: contracts.GroupStateJoinSucceeded}
		}
		if total-failed < quorumCount {
			return JoinEvaluationResult{IsDecided: true, IsSuccess: false, State: contracts.GroupStateJoinFailed}
		}
		return JoinEvaluationResult{IsDecided: false, IsSuccess: false, State: contracts.GroupStateFanoutActive}

	case contracts.PolicyAllRequired:
		if hasRequiredFailure {
			return JoinEvaluationResult{IsDecided: true, IsSuccess: false, State: contracts.GroupStateJoinFailed}
		}
		if pending == 0 {
			return JoinEvaluationResult{IsDecided: true, IsSuccess: true, State: contracts.GroupStateJoinSucceeded}
		}
		return JoinEvaluationResult{IsDecided: false, IsSuccess: false, State: contracts.GroupStateFanoutActive}

	case contracts.PolicyAllSettled:
		if pending == 0 {
			success := !hasRequiredFailure
			if success {
				return JoinEvaluationResult{IsDecided: true, IsSuccess: true, State: contracts.GroupStateJoinSucceeded}
			}
			return JoinEvaluationResult{IsDecided: true, IsSuccess: false, State: contracts.GroupStateJoinFailed}
		}
		return JoinEvaluationResult{IsDecided: false, IsSuccess: false, State: contracts.GroupStateFanoutActive}

	default:
		return JoinEvaluationResult{IsDecided: false, IsSuccess: false, State: contracts.GroupStateFanoutActive}
	}
}

func NeedsCompensation(b contracts.ParallelBranchInstance) bool {
	defer observability.TraceEnter(nil)()
	if b.CurrentState != contracts.BranchStateSucceeded {
		return false
	}
	return b.EffectType == contracts.EffectCompensable || b.EffectType == contracts.EffectHold
}

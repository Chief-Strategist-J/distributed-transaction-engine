package machines

import (
	contracts "distributed-transaction-engine/src/features/parallel-execution/contracts"
	rules "distributed-transaction-engine/src/features/parallel-execution/domain/rules"
)

type ParallelCommandType string

const (
	CommandCancelBranch     ParallelCommandType = "CANCEL_BRANCH"
	CommandCompensateBranch ParallelCommandType = "COMPENSATE_BRANCH"
	CommandCompleteGroup    ParallelCommandType = "COMPLETE_GROUP"
	CommandFailGroup        ParallelCommandType = "FAIL_GROUP"
)

type ParallelCommand struct {
	Type     ParallelCommandType
	BranchID string
}

type ParallelTransitionDecision struct {
	GroupState contracts.GroupState
	Commands   []ParallelCommand
}

type ParallelExecutionMachine struct{}

func NewParallelExecutionMachine() *ParallelExecutionMachine {
	return &ParallelExecutionMachine{}
}

func (m *ParallelExecutionMachine) EvaluateGroupJoin(
	policy contracts.JoinPolicy,
	quorumCount int,
	branches []contracts.ParallelBranchInstance,
) ParallelTransitionDecision {
	res := rules.EvaluateJoinPolicy(policy, quorumCount, branches)

	if !res.IsDecided {
		return ParallelTransitionDecision{
			GroupState: contracts.GroupStateFanoutActive,
			Commands:   nil,
		}
	}

	if res.IsSuccess {
		var cmds []ParallelCommand
		if policy == contracts.PolicyFirstSuccess {
			for _, b := range branches {
				if b.CurrentState == contracts.BranchStateDispatched || b.CurrentState == contracts.BranchStatePending {
					cmds = append(cmds, ParallelCommand{Type: CommandCancelBranch, BranchID: b.BranchID})
				}
			}
		}
		cmds = append(cmds, ParallelCommand{Type: CommandCompleteGroup})
		return ParallelTransitionDecision{
			GroupState: contracts.GroupStateJoinSucceeded,
			Commands:   cmds,
		}
	}

	var cmds []ParallelCommand
	for _, b := range branches {
		if rules.NeedsCompensation(b) {
			cmds = append(cmds, ParallelCommand{Type: CommandCompensateBranch, BranchID: b.BranchID})
		} else if b.CurrentState == contracts.BranchStateDispatched || b.CurrentState == contracts.BranchStatePending {
			cmds = append(cmds, ParallelCommand{Type: CommandCancelBranch, BranchID: b.BranchID})
		}
	}
	cmds = append(cmds, ParallelCommand{Type: CommandFailGroup})

	return ParallelTransitionDecision{
		GroupState: contracts.GroupStateJoinFailed,
		Commands:   cmds,
	}
}

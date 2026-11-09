package tests

import (
	"testing"

	contracts "distributed-transaction-engine/src/features/parallel-execution/contracts"
	machines "distributed-transaction-engine/src/features/parallel-execution/domain/machines"
)

func TestParallelExecutionMachine_JoinSuccess(t *testing.T) {
	m := machines.NewParallelExecutionMachine()

	branches := []contracts.ParallelBranchInstance{
		{BranchID: "b1", Role: contracts.RoleRequired, CurrentState: contracts.BranchStateSucceeded},
		{BranchID: "b2", Role: contracts.RoleRequired, CurrentState: contracts.BranchStateSucceeded},
	}

	dec := m.EvaluateGroupJoin(contracts.PolicyAllRequired, 0, branches)
	if dec.GroupState != contracts.GroupStateJoinSucceeded {
		t.Fatalf("expected JoinSucceeded, got %s", dec.GroupState)
	}
	if len(dec.Commands) != 1 || dec.Commands[0].Type != machines.CommandCompleteGroup {
		t.Fatalf("expected CommandCompleteGroup on success, got %+v", dec.Commands)
	}
}

func TestParallelExecutionMachine_JoinFailureTriggersSelectiveCompensation(t *testing.T) {
	m := machines.NewParallelExecutionMachine()

	branches := []contracts.ParallelBranchInstance{
		{BranchID: "b1", Role: contracts.RoleRequired, EffectType: contracts.EffectCompensable, CurrentState: contracts.BranchStateSucceeded},
		{BranchID: "b2", Role: contracts.RoleRequired, EffectType: contracts.EffectCompensable, CurrentState: contracts.BranchStatePending},
		{BranchID: "b3", Role: contracts.RoleRequired, EffectType: contracts.EffectCompensable, CurrentState: contracts.BranchStateRejected},
	}

	dec := m.EvaluateGroupJoin(contracts.PolicyAllRequired, 0, branches)
	if dec.GroupState != contracts.GroupStateJoinFailed {
		t.Fatalf("expected JoinFailed, got %s", dec.GroupState)
	}

	var hasCancel, hasCompensate bool
	for _, cmd := range dec.Commands {
		if cmd.Type == machines.CommandCancelBranch && cmd.BranchID == "b2" {
			hasCancel = true
		}
		if cmd.Type == machines.CommandCompensateBranch && cmd.BranchID == "b1" {
			hasCompensate = true
		}
	}

	if !hasCancel {
		t.Fatal("expected pending branch b2 to receive Cancel command")
	}
	if !hasCompensate {
		t.Fatal("expected succeeded branch b1 to receive Compensate command")
	}
}

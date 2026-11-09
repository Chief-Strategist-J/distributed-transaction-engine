package tests

import (
	"testing"

	contracts "distributed-transaction-engine/src/features/parallel-execution/contracts"
	rules "distributed-transaction-engine/src/features/parallel-execution/domain/rules"
)

func TestEvaluateJoinPolicy_AllRequired(t *testing.T) {
	branches := []contracts.ParallelBranchInstance{
		{BranchID: "b1", Role: contracts.RoleRequired, CurrentState: contracts.BranchStateSucceeded},
		{BranchID: "b2", Role: contracts.RoleRequired, CurrentState: contracts.BranchStatePending},
	}

	res := rules.EvaluateJoinPolicy(contracts.PolicyAllRequired, 0, branches)
	if res.IsDecided {
		t.Fatalf("expected undecided while branch pending, got decided: %v", res)
	}

	branches[1].CurrentState = contracts.BranchStateSucceeded
	res = rules.EvaluateJoinPolicy(contracts.PolicyAllRequired, 0, branches)
	if !res.IsDecided || !res.IsSuccess || res.State != contracts.GroupStateJoinSucceeded {
		t.Fatalf("expected join succeeded, got %v", res)
	}

	branches[1].CurrentState = contracts.BranchStateRejected
	res = rules.EvaluateJoinPolicy(contracts.PolicyAllRequired, 0, branches)
	if !res.IsDecided || res.IsSuccess || res.State != contracts.GroupStateJoinFailed {
		t.Fatalf("expected join failed on required failure, got %v", res)
	}
}

func TestEvaluateJoinPolicy_FirstSuccess(t *testing.T) {
	branches := []contracts.ParallelBranchInstance{
		{BranchID: "b1", Role: contracts.RoleAlternative, CurrentState: contracts.BranchStateRejected},
		{BranchID: "b2", Role: contracts.RoleAlternative, CurrentState: contracts.BranchStatePending},
	}

	res := rules.EvaluateJoinPolicy(contracts.PolicyFirstSuccess, 0, branches)
	if res.IsDecided {
		t.Fatalf("expected undecided, got %v", res)
	}

	branches[1].CurrentState = contracts.BranchStateSucceeded
	res = rules.EvaluateJoinPolicy(contracts.PolicyFirstSuccess, 0, branches)
	if !res.IsDecided || !res.IsSuccess || res.State != contracts.GroupStateJoinSucceeded {
		t.Fatalf("expected first success to trigger join succeeded, got %v", res)
	}
}

func TestEvaluateJoinPolicy_NOfM(t *testing.T) {
	branches := []contracts.ParallelBranchInstance{
		{BranchID: "b1", Role: contracts.RoleRequired, CurrentState: contracts.BranchStateSucceeded},
		{BranchID: "b2", Role: contracts.RoleRequired, CurrentState: contracts.BranchStatePending},
		{BranchID: "b3", Role: contracts.RoleRequired, CurrentState: contracts.BranchStatePending},
	}

	res := rules.EvaluateJoinPolicy(contracts.PolicyNOfM, 2, branches)
	if res.IsDecided {
		t.Fatalf("expected undecided when 1 of 2 succeeded, got %v", res)
	}

	branches[1].CurrentState = contracts.BranchStateSucceeded
	res = rules.EvaluateJoinPolicy(contracts.PolicyNOfM, 2, branches)
	if !res.IsDecided || !res.IsSuccess || res.State != contracts.GroupStateJoinSucceeded {
		t.Fatalf("expected 2 of 3 quorum to succeed, got %v", res)
	}

	branches[0].CurrentState = contracts.BranchStateRejected
	branches[1].CurrentState = contracts.BranchStateRejected
	res = rules.EvaluateJoinPolicy(contracts.PolicyNOfM, 2, branches)
	if !res.IsDecided || res.IsSuccess || res.State != contracts.GroupStateJoinFailed {
		t.Fatalf("expected quorum impossible to fail join, got %v", res)
	}
}

func TestEvaluateJoinPolicy_AllSettled(t *testing.T) {
	branches := []contracts.ParallelBranchInstance{
		{BranchID: "b1", Role: contracts.RoleRequired, CurrentState: contracts.BranchStateSucceeded},
		{BranchID: "b2", Role: contracts.RoleOptional, CurrentState: contracts.BranchStateRejected},
	}

	res := rules.EvaluateJoinPolicy(contracts.PolicyAllSettled, 0, branches)
	if !res.IsDecided || !res.IsSuccess || res.State != contracts.GroupStateJoinSucceeded {
		t.Fatalf("expected AllSettled with optional failure to succeed, got %v", res)
	}

	branches[0].CurrentState = contracts.BranchStateRejected
	res = rules.EvaluateJoinPolicy(contracts.PolicyAllSettled, 0, branches)
	if !res.IsDecided || res.IsSuccess || res.State != contracts.GroupStateJoinFailed {
		t.Fatalf("expected AllSettled with required failure to fail, got %v", res)
	}
}

func TestNeedsCompensation(t *testing.T) {
	b1 := contracts.ParallelBranchInstance{CurrentState: contracts.BranchStateSucceeded, EffectType: contracts.EffectCompensable}
	if !rules.NeedsCompensation(b1) {
		t.Fatal("expected compensable branch to need compensation")
	}

	b2 := contracts.ParallelBranchInstance{CurrentState: contracts.BranchStateRejected, EffectType: contracts.EffectCompensable}
	if rules.NeedsCompensation(b2) {
		t.Fatal("expected rejected branch not to need compensation")
	}
}

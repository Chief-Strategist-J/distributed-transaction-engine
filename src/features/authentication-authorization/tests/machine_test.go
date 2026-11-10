package tests

import (
	"testing"

	"distributed-transaction-engine/src/features/authentication-authorization/domain/machines"
)

func TestAuthRiskMachine_Transitions(t *testing.T) {
	m := machines.NewAuthRiskMachine()

	st, err := m.Transition(machines.RiskStateStandard, "EVALUATE_PASS")
	if err != nil || st != machines.RiskStateApproved {
		t.Fatalf("expected transition to APPROVED, got %s err: %v", st, err)
	}

	st, err = m.Transition(machines.RiskStateStandard, "REQUIRE_STEP_UP")
	if err != nil || st != machines.RiskStateStepUpRequired {
		t.Fatalf("expected transition to STEP_UP_REQUIRED, got %s err: %v", st, err)
	}

	st, err = m.Transition(st, "STEP_UP_VERIFIED")
	if err != nil || st != machines.RiskStateApproved {
		t.Fatalf("expected step up verification to yield APPROVED, got %s", st)
	}

	st, err = m.Transition(machines.RiskStateStandard, "REQUIRE_DUAL_APPROVAL")
	if err != nil || st != machines.RiskStateDualApprovalPending {
		t.Fatalf("expected transition to DUAL_APPROVAL_PENDING, got %s err: %v", st, err)
	}

	st, err = m.Transition(st, "APPROVER_CONFIRMED")
	if err != nil || st != machines.RiskStateApproved {
		t.Fatalf("expected approver confirmation to yield APPROVED, got %s", st)
	}

	_, err = m.Transition(machines.RiskStateApproved, "EVALUATE_PASS")
	if err == nil {
		t.Fatal("expected error transitioning from terminal APPROVED state")
	}
}


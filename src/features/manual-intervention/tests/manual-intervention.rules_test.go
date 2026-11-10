package tests

import (
	"testing"

	"distributed-transaction-engine/src/features/manual-intervention/contracts"
	"distributed-transaction-engine/src/features/manual-intervention/domain/rules"
)

func TestManualInterventionRulesRoleValidation(t *testing.T) {
	r := rules.NewManualInterventionRules()

	if err := r.ValidateOperatorRole("admin:saga:intervene"); err != nil {
		t.Fatalf("expected valid operator role to pass")
	}

	if err := r.ValidateOperatorRole("viewer"); err == nil {
		t.Fatalf("expected unauthorized role to fail")
	}
}

func TestManualInterventionRulesTargetStateValidation(t *testing.T) {
	r := rules.NewManualInterventionRules()

	for _, st := range []string{"STUCK", "FAILED", "WAITING"} {
		if err := r.ValidateTargetSagaState(st); err != nil {
			t.Fatalf("expected state %s to be intervenable", st)
		}
	}

	for _, st := range []string{"RUNNING", "COMPLETED"} {
		if err := r.ValidateTargetSagaState(st); err == nil {
			t.Fatalf("expected state %s to be rejected", st)
		}
	}
}

func TestManualInterventionRulesDeriveNextState(t *testing.T) {
	r := rules.NewManualInterventionRules()

	st, err := r.DeriveNextState(contracts.ActionForceRetry)
	if err != nil || st != "RUNNING" {
		t.Fatalf("expected RUNNING for FORCE_RETRY")
	}

	st, err = r.DeriveNextState(contracts.ActionForceCompensate)
	if err != nil || st != "COMPENSATING" {
		t.Fatalf("expected COMPENSATING for FORCE_COMPENSATE")
	}

	st, err = r.DeriveNextState(contracts.ActionForceComplete)
	if err != nil || st != "COMPLETED" {
		t.Fatalf("expected COMPLETED for FORCE_COMPLETE")
	}

	st, err = r.DeriveNextState(contracts.ActionForceTerminate)
	if err != nil || st != "FAILED_TERMINATED" {
		t.Fatalf("expected FAILED_TERMINATED for FORCE_TERMINATE")
	}
}

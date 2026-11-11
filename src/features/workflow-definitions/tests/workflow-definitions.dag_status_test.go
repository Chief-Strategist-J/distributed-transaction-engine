package tests

import (
	stderrors "errors"
	"testing"

	contracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
	rules "distributed-transaction-engine/src/features/workflow-definitions/domain/rules"
	apperrors "distributed-transaction-engine/src/shared/errors"
)

func dagStep(id string, deps ...string) contracts.StepDefinition {
	return contracts.StepDefinition{
		StepID:               id,
		StepName:             id,
		ActionType:           "REST",
		ActionEndpoint:       "http://svc/" + id,
		CompensationAction:   "UNDO_" + id,
		CompensationEndpoint: "http://svc/undo/" + id,
		TimeoutSeconds:       10,
		Dependencies:         deps,
	}
}

func TestWorkflowRules_InvalidGraphsReturn400(t *testing.T) {
	cases := map[string][]contracts.StepDefinition{
		"self_loop":    {dagStep("a", "a")},
		"three_cycle":  {dagStep("a", "c"), dagStep("b", "a"), dagStep("c", "b")},
		"cycle_in_dag": {dagStep("root"), dagStep("a", "root", "c"), dagStep("b", "a"), dagStep("c", "b")},
		"disconnected": {dagStep("a"), dagStep("b")},
		"missing_dep":  {dagStep("a", "ghost")},
	}
	for name, steps := range cases {
		t.Run(name, func(t *testing.T) {
			res := rules.ValidateDAGTopology(steps)
			if res.IsValid {
				t.Fatalf("expected invalid graph")
			}
			var appErr *apperrors.ApplicationError
			if !stderrors.As(res.Error, &appErr) || appErr.StatusCode != 400 || appErr.CanonicalCode != "VALIDATION_FAILED" {
				t.Fatalf("expected 400 VALIDATION_FAILED, got %#v", res.Error)
			}
		})
	}
}

func TestWorkflowRules_DiamondIsValid(t *testing.T) {
	steps := []contracts.StepDefinition{dagStep("a"), dagStep("b", "a"), dagStep("c", "a"), dagStep("d", "b", "c")}
	res := rules.ValidateDAGTopology(steps)
	if !res.IsValid || len(res.TopologicalOrder) != 4 {
		t.Fatalf("diamond should be valid: %#v", res)
	}
}

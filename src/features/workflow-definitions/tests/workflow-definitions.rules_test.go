package tests

import (
	stderrors "errors"
	"strings"
	"testing"

	contracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
	rules "distributed-transaction-engine/src/features/workflow-definitions/domain/rules"
	apperrors "distributed-transaction-engine/src/shared/errors"
)

func TestWorkflowRules_ValidLinearDAG(t *testing.T) {
	steps := []contracts.StepDefinition{
		{
			StepID:               "reserve_inventory",
			StepName:             "Reserve Inventory",
			IsPivot:              false,
			ActionType:           "REST",
			ActionEndpoint:       "http://inventory:8080/reserve",
			CompensationAction:   "RELEASE",
			CompensationEndpoint: "http://inventory:8080/release",
			TimeoutSeconds:       30,
			MaxRetries:           3,
			Dependencies:         []string{},
		},
		{
			StepID:               "charge_card",
			StepName:             "Charge Payment Card",
			IsPivot:              true,
			ActionType:           "REST",
			ActionEndpoint:       "http://payments:8080/charge",
			CompensationAction:   "",
			CompensationEndpoint: "",
			TimeoutSeconds:       60,
			MaxRetries:           1,
			Dependencies:         []string{"reserve_inventory"},
		},
	}

	result := rules.ValidateDAGTopology(steps)
	if !result.IsValid {
		t.Fatalf("expected valid DAG, got error: %v", result.Error)
	}
	if len(result.TopologicalOrder) != 2 {
		t.Fatalf("expected 2 ordered steps, got %d", len(result.TopologicalOrder))
	}
	if result.TopologicalOrder[0] != "reserve_inventory" || result.TopologicalOrder[1] != "charge_card" {
		t.Fatalf("unexpected order: %v", result.TopologicalOrder)
	}
}

func TestWorkflowRules_MultipleRootsSingleComponentValid(t *testing.T) {
	steps := []contracts.StepDefinition{
		{
			StepID:               "reserve_flight",
			StepName:             "Reserve Flight",
			IsPivot:              false,
			ActionType:           "REST",
			ActionEndpoint:       "http://flights/reserve",
			CompensationAction:   "RELEASE_FLIGHT",
			CompensationEndpoint: "http://flights/release",
			TimeoutSeconds:       30,
			Dependencies:         []string{},
		},
		{
			StepID:               "reserve_hotel",
			StepName:             "Reserve Hotel",
			IsPivot:              false,
			ActionType:           "REST",
			ActionEndpoint:       "http://hotel/reserve",
			CompensationAction:   "RELEASE_HOTEL",
			CompensationEndpoint: "http://hotel/release",
			TimeoutSeconds:       30,
			Dependencies:         []string{},
		},
		{
			StepID:         "charge_bundle",
			StepName:       "Charge Bundle",
			IsPivot:        true,
			ActionType:     "REST",
			ActionEndpoint: "http://billing/charge",
			TimeoutSeconds: 60,
			Dependencies:   []string{"reserve_flight", "reserve_hotel"},
		},
	}

	result := rules.ValidateDAGTopology(steps)
	if !result.IsValid {
		t.Fatalf("expected valid multi-root single-component DAG, got error: %v", result.Error)
	}
	if len(result.TopologicalOrder) != 3 {
		t.Fatalf("expected 3 ordered steps, got %d", len(result.TopologicalOrder))
	}
}

func TestWorkflowRules_DisconnectedComponentsRejected(t *testing.T) {
	steps := []contracts.StepDefinition{
		{
			StepID:               "order_step_a",
			StepName:             "Step A",
			IsPivot:              false,
			ActionType:           "REST",
			ActionEndpoint:       "http://service/a",
			CompensationAction:   "UNDO_A",
			CompensationEndpoint: "http://service/a/undo",
			TimeoutSeconds:       10,
			Dependencies:         []string{},
		},
		{
			StepID:         "order_step_b",
			StepName:       "Step B",
			IsPivot:        true,
			ActionType:     "REST",
			ActionEndpoint: "http://service/b",
			TimeoutSeconds: 10,
			Dependencies:   []string{"order_step_a"},
		},
		{
			StepID:               "legacy_notify",
			StepName:             "Legacy Notify Unconnected",
			IsPivot:              false,
			ActionType:           "REST",
			ActionEndpoint:       "http://service/notify",
			CompensationAction:   "UNDO_NOTIFY",
			CompensationEndpoint: "http://service/notify/undo",
			TimeoutSeconds:       10,
			Dependencies:         []string{},
		},
	}

	result := rules.ValidateDAGTopology(steps)
	if result.IsValid {
		t.Fatalf("expected disconnected components rejection, got valid")
	}
}

func TestWorkflowRules_CycleDetection(t *testing.T) {
	steps := []contracts.StepDefinition{
		{
			StepID:               "step_a",
			StepName:             "Step A",
			IsPivot:              false,
			ActionType:           "REST",
			ActionEndpoint:       "http://step-a",
			CompensationAction:   "UNDO_A",
			CompensationEndpoint: "http://step-a/undo",
			TimeoutSeconds:       30,
			Dependencies:         []string{"step_b"},
		},
		{
			StepID:               "step_b",
			StepName:             "Step B",
			IsPivot:              false,
			ActionType:           "REST",
			ActionEndpoint:       "http://step-b",
			CompensationAction:   "UNDO_B",
			CompensationEndpoint: "http://step-b/undo",
			TimeoutSeconds:       30,
			Dependencies:         []string{"step_a"},
		},
	}

	result := rules.ValidateDAGTopology(steps)
	if result.IsValid {
		t.Fatalf("expected cycle error, but result was valid")
	}
}

func TestWorkflowRules_OrphanDependencyDetection(t *testing.T) {
	steps := []contracts.StepDefinition{
		{
			StepID:               "step_a",
			StepName:             "Step A",
			IsPivot:              false,
			ActionType:           "REST",
			ActionEndpoint:       "http://step-a",
			CompensationAction:   "UNDO_A",
			CompensationEndpoint: "http://step-a/undo",
			TimeoutSeconds:       30,
			Dependencies:         []string{"non_existent_step"},
		},
	}

	result := rules.ValidateDAGTopology(steps)
	if result.IsValid {
		t.Fatalf("expected orphan dependency error, but got valid")
	}
}

func TestWorkflowRules_MultiplePivotsRejected(t *testing.T) {
	input := contracts.CreateWorkflowDefinitionInput{
		TenantID:      "tenant_test",
		Name:          "multi-pivot-flow",
		Version:       1,
		Source:        "TEST",
		ExternalID:    "ext_1",
		CorrelationID: "c0000000-0000-0000-0000-000000000001",
		Steps: []contracts.StepDefinition{
			{
				StepID:         "pivot_1",
				StepName:       "Pivot 1",
				IsPivot:        true,
				ActionType:     "REST",
				ActionEndpoint: "http://pivot1",
				TimeoutSeconds: 30,
				Dependencies:   []string{},
			},
			{
				StepID:         "pivot_2",
				StepName:       "Pivot 2",
				IsPivot:        true,
				ActionType:     "REST",
				ActionEndpoint: "http://pivot2",
				TimeoutSeconds: 30,
				Dependencies:   []string{},
			},
		},
	}

	err := contracts.ValidateCreateWorkflowDefinitionInput(input)
	if err == nil {
		t.Fatalf("expected multiple pivots rejection, got nil")
	}
}

func TestWorkflowRules_CyclePathDetailedReporting(t *testing.T) {
	steps := []contracts.StepDefinition{
		{
			StepID:               "step_alpha",
			StepName:             "Alpha",
			IsPivot:              false,
			ActionType:           "REST",
			ActionEndpoint:       "http://service/alpha",
			CompensationAction:   "UNDO_ALPHA",
			CompensationEndpoint: "http://service/undo_alpha",
			TimeoutSeconds:       30,
			Dependencies:         []string{"step_beta"},
		},
		{
			StepID:               "step_beta",
			StepName:             "Beta",
			IsPivot:              false,
			ActionType:           "REST",
			ActionEndpoint:       "http://service/beta",
			CompensationAction:   "UNDO_BETA",
			CompensationEndpoint: "http://service/undo_beta",
			TimeoutSeconds:       30,
			Dependencies:         []string{"step_alpha"},
		},
	}

	result := rules.ValidateDAGTopology(steps)
	if result.IsValid {
		t.Fatalf("expected cycle error, but result was valid")
	}

	errMsg := result.Error.Error()
	if !strings.Contains(errMsg, "step_alpha") || !strings.Contains(errMsg, "step_beta") {
		t.Fatalf("expected error message to contain cycle participants, got: %s", errMsg)
	}
}

func TestWorkflowRules_SSRFRejection(t *testing.T) {
	cases := []struct {
		name     string
		endpoint string
	}{
		{name: "localhost", endpoint: "http://localhost:8080/charge"},
		{name: "127.0.0.1 loopback", endpoint: "http://127.0.0.1:9090/charge"},
		{name: "AWS metadata link-local", endpoint: "http://169.254.169.254/latest/meta-data"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := contracts.CreateWorkflowDefinitionInput{
				TenantID:      "tenant_test",
				Name:          "ssrf-flow",
				Version:       1,
				Source:        "TEST",
				ExternalID:    "ext_1",
				CorrelationID: "c0000000-0000-0000-0000-000000000001",
				Steps: []contracts.StepDefinition{
					{
						StepID:               "step_charge",
						StepName:             "Charge",
						IsPivot:              false,
						ActionType:           "REST",
						ActionEndpoint:       tc.endpoint,
						CompensationAction:   "UNDO",
						CompensationEndpoint: "http://service/undo",
						TimeoutSeconds:       30,
						Dependencies:         []string{},
					},
				},
			}
			err := contracts.ValidateCreateWorkflowDefinitionInput(input)
			if err == nil {
				t.Fatalf("expected SSRF error for %s, got nil", tc.endpoint)
			}
		})
	}
}

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

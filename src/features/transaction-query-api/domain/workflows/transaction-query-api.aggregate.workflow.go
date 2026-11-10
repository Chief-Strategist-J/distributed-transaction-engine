package workflows

import "distributed-transaction-engine/src/infra/observability"

const StepValidateAggregates = "VALIDATE_AGGREGATES"

func AggregateSteps() []Step {
	defer observability.TraceEnter(nil)()
	return []Step{
		{ID: StepParse},
		{ID: StepValidate, DependsOn: []string{StepParse}},
		{ID: StepValidateAggregates, DependsOn: []string{StepValidate}},
		{ID: StepEntitle, DependsOn: []string{StepValidateAggregates}},
		{ID: StepInject, DependsOn: []string{StepEntitle}},
		{ID: StepNormalize, DependsOn: []string{StepInject}},
		{ID: StepCost, DependsOn: []string{StepNormalize}},
		{ID: StepRateLimit, DependsOn: []string{StepCost}},
		{ID: StepPlan, DependsOn: []string{StepRateLimit}},
		{ID: StepExecute, DependsOn: []string{StepPlan}},
		{ID: StepShape, DependsOn: []string{StepExecute}},
		{ID: StepAudit, DependsOn: []string{StepShape}},
	}
}

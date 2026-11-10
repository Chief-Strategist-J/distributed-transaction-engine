package workflows

import (
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
)

type Step struct {
	ID        string
	DependsOn []string
}

const (
	StepRateLimit = "RATE_LIMIT"
	StepParse     = "PARSE"
	StepValidate  = "VALIDATE"
	StepEntitle   = "ENTITLE"
	StepInject    = "INJECT"
	StepNormalize = "NORMALIZE"
	StepCost      = "COST"
	StepPlan      = "PLAN"
	StepExecute   = "EXECUTE"
	StepShape     = "SHAPE"
	StepAudit     = "AUDIT"
)

func SearchSteps() []Step {
	defer observability.TraceEnter(nil)()
	return []Step{
		{ID: StepParse},
		{ID: StepValidate, DependsOn: []string{StepParse}},
		{ID: StepEntitle, DependsOn: []string{StepValidate}},
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

func OrderSteps(steps []Step) (spanResult0 []string, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	known := make(map[string]Step, len(steps))
	for _, step := range steps {
		if _, duplicate := known[step.ID]; duplicate {
			return nil, appErrors.NewInternalError("duplicate workflow step " + step.ID)
		}
		known[step.ID] = step
	}
	ordered := make([]string, 0, len(steps))
	done := make(map[string]struct{}, len(steps))
	for len(ordered) < len(steps) {
		progressed := false
		for _, step := range steps {
			if _, finished := done[step.ID]; finished || !dependenciesDone(step, known, done) {
				continue
			}
			done[step.ID] = struct{}{}
			ordered = append(ordered, step.ID)
			progressed = true
		}
		if !progressed {
			return nil, appErrors.NewInternalError("workflow has a cycle or an unknown dependency")
		}
	}
	return ordered, nil
}

func dependenciesDone(step Step, known map[string]Step, done map[string]struct{}) bool {
	for _, dependency := range step.DependsOn {
		if _, exists := known[dependency]; !exists {
			return false
		}
		if _, finished := done[dependency]; !finished {
			return false
		}
	}
	return true
}

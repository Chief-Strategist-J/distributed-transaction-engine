package ports

import (
	"context"

	workflowContracts "distributed-transaction-engine/src/features/workflow-definitions/contracts"
)

type StepDispatcherPort interface {
	DispatchStep(ctx context.Context, stepDef workflowContracts.StepDefinition, input map[string]interface{}, idempotencyKey string) (map[string]interface{}, error)
	CompensateStep(ctx context.Context, stepDef workflowContracts.StepDefinition, input map[string]interface{}, output map[string]interface{}, idempotencyKey string) error
}

package sagaorchestration

import (
	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
	sagaService "distributed-transaction-engine/src/features/saga-orchestration/service"
)

type Service = sagaService.SagaOrchestrationService
type Instance = sagaContracts.SagaInstance
type State = sagaContracts.SagaState

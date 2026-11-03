package contracts

import (
	"time"

	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
)

type OrphanCandidate struct {
	ID                   string
	PublicID             string
	TenantID             string
	CurrentState         sagaContracts.SagaState
	FencingToken         int64
	WorkflowDefinitionID string
	WorkflowVersion      int64
	SpecHash             string
	LeaseOwner           string
	LeaseUntil           *time.Time
	TakeoverCount        int
	InitialPayload       map[string]interface{}
}

type TakeoverResult struct {
	SagaID        string
	TenantID      string
	NewToken      int64
	TakeoverCount int
	IsPoisoned    bool
}

type StepReconcileResult struct {
	StepID         string
	ExecutionState sagaContracts.StepExecutionState
	StepOutput     map[string]interface{}
	NeedsDispatch  bool
}

type CrashRecoveryConfig struct {
	IncarnationID        string
	ScanInterval         time.Duration
	BatchSize            int
	LeaseDuration        time.Duration
	MaxTakeovers         int
	HeartbeatInterval    time.Duration
	SelfFenceMargin      time.Duration
	MaxConcurrentOwned   int
}


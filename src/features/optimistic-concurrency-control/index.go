package optimisticconcurrencycontrol

import (
	"database/sql"

	"distributed-transaction-engine/src/features/optimistic-concurrency-control/contracts"
	"distributed-transaction-engine/src/features/optimistic-concurrency-control/domain/machines"
	"distributed-transaction-engine/src/features/optimistic-concurrency-control/domain/rules"
	"distributed-transaction-engine/src/features/optimistic-concurrency-control/infrastructure/adapters"
	"distributed-transaction-engine/src/features/optimistic-concurrency-control/infrastructure/ports"
	"distributed-transaction-engine/src/features/optimistic-concurrency-control/service"
)

type (
	RepositoryPort         = ports.RepositoryPort
	OCCService             = service.OCCService
	ServiceDeps            = service.ServiceDeps
	SagaRootState          = contracts.SagaRootState
	CASResult              = contracts.CASResult
	CASMutationInput       = contracts.CASMutationInput
	ChildStepMutation      = contracts.ChildStepMutation
	OutboxEventPayload     = contracts.OutboxEventPayload
	OCCPendingInput        = contracts.OCCPendingInput
	ContentionJournalEntry = contracts.ContentionJournalEntry
	ClientEditInput        = contracts.ClientEditInput
	ZeroRowCause           = contracts.ZeroRowCause
	InputType              = contracts.InputType
	PendingInputStatus     = contracts.PendingInputStatus
	ConcurrencyStrategy    = contracts.ConcurrencyStrategy
	OCCRules               = rules.OCCRules
	OCCStrategyMachine     = machines.OCCStrategyMachine
	ConcurrencyState       = machines.ConcurrencyState
	ConcurrencyEvent       = machines.ConcurrencyEvent
)

const (
	CauseVersionConflict   = contracts.CauseVersionConflict
	CauseNotOwner          = contracts.CauseNotOwner
	CauseStateConflict     = contracts.CauseStateConflict
	CauseDataError         = contracts.CauseDataError
	CauseSecurityViolation = contracts.CauseSecurityViolation

	InputTypeReply           = contracts.InputTypeReply
	InputTypeTimer           = contracts.InputTypeTimer
	InputTypeOperatorCommand = contracts.InputTypeOperatorCommand

	PendingStatusPending     = contracts.PendingStatusPending
	PendingStatusReprocessed = contracts.PendingStatusReprocessed
	PendingStatusQuarantined = contracts.PendingStatusQuarantined

	StrategyOCC        = contracts.StrategyOCC
	StrategySerialized = contracts.StrategySerialized
)

func NewService(deps ServiceDeps) *OCCService {
	return service.NewService(deps)
}

func NewMemoryAdapter() *adapters.MemoryOCCRepositoryAdapter {
	return adapters.NewMemoryOCCRepositoryAdapter()
}

func NewPostgresAdapter(db *sql.DB) *adapters.PostgresOCCRepositoryAdapter {
	return adapters.NewPostgresOCCRepositoryAdapter(db)
}

func NewOCCRules() *OCCRules {
	return rules.NewOCCRules()
}

func NewOCCStrategyMachine() *OCCStrategyMachine {
	return machines.NewOCCStrategyMachine()
}


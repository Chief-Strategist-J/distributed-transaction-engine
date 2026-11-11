package deadletterqueue

import (
	"database/sql"

	"distributed-transaction-engine/src/features/dead-letter-queue/contracts"
	"distributed-transaction-engine/src/features/dead-letter-queue/domain/machines"
	"distributed-transaction-engine/src/features/dead-letter-queue/domain/rules"
	"distributed-transaction-engine/src/features/dead-letter-queue/infrastructure/adapters"
	"distributed-transaction-engine/src/features/dead-letter-queue/infrastructure/ports"
	"distributed-transaction-engine/src/features/dead-letter-queue/service"
)

type (
	RepositoryPort         = ports.RepositoryPort
	IdempotencyLedgerPort  = ports.IdempotencyLedgerPort
	DeadLetterService      = service.DeadLetterService
	ServiceDeps            = service.ServiceDeps
	DeadLetterRecord       = contracts.DeadLetterRecord
	SystemicIncident       = contracts.SystemicIncident
	StepTypePause          = contracts.StepTypePause
	AdmitDLQInput          = contracts.AdmitDLQInput
	TriageInput            = contracts.TriageInput
	RetryActionInput       = contracts.RetryActionInput
	ResolveEvidenceInput   = contracts.ResolveEvidenceInput
	CorrectiveActionInput  = contracts.CorrectiveActionInput
	WriteOffInput          = contracts.WriteOffInput
	ResolveSagaActionInput = contracts.ResolveSagaActionInput
	ReplayEvaluation       = contracts.ReplayEvaluation
	OutcomeClass           = contracts.OutcomeClass
	FailureClass           = contracts.FailureClass
	RecordStatus           = contracts.RecordStatus
	DispositionType        = contracts.DispositionType
	DeadLetterRules        = rules.DeadLetterRules
	DeadLetterMachine      = machines.DeadLetterMachine
	RecordEvent            = machines.RecordEvent
)

const (
	OutcomeNotExecuted = contracts.OutcomeNotExecuted
	OutcomeUnknown     = contracts.OutcomeUnknown
	OutcomeFailed      = contracts.OutcomeFailed

	FailurePoison              = contracts.FailurePoison
	FailureDefinitionDefect    = contracts.FailureDefinitionDefect
	FailureRetryExhausted      = contracts.FailureRetryExhausted
	FailureBusinessRejection   = contracts.FailureBusinessRejection
	FailureCompensationFailure = contracts.FailureCompensationFailure
	FailureIntegrityViolation  = contracts.FailureIntegrityViolation

	StatusQuarantined      = contracts.StatusQuarantined
	StatusTriaged          = contracts.StatusTriaged
	StatusActionInProgress = contracts.StatusActionInProgress
	StatusResolved         = contracts.StatusResolved
	StatusEscalated        = contracts.StatusEscalated

	DispositionRetriedSucceeded       = contracts.DispositionRetriedSucceeded
	DispositionResolvedAsExecuted     = contracts.DispositionResolvedAsExecuted
	DispositionResolvedAsNotExecuted = contracts.DispositionResolvedAsNotExecuted
	DispositionCorrected              = contracts.DispositionCorrected
	DispositionSagaCompensated        = contracts.DispositionSagaCompensated
	DispositionSagaRolledForward      = contracts.DispositionSagaRolledForward
	DispositionSagaCompletedManually  = contracts.DispositionSagaCompletedManually
	DispositionWrittenOff             = contracts.DispositionWrittenOff
)

func NewService(deps ServiceDeps) *DeadLetterService {
	return service.NewService(deps)
}

func NewMemoryAdapter() *adapters.MemoryDeadLetterRepositoryAdapter {
	return adapters.NewMemoryDeadLetterRepositoryAdapter()
}

func NewPostgresAdapter(db *sql.DB) *adapters.PostgresDeadLetterRepositoryAdapter {
	return adapters.NewPostgresDeadLetterRepositoryAdapter(db)
}

func NewDeadLetterRules() *DeadLetterRules {
	return rules.NewDeadLetterRules()
}

func NewDeadLetterMachine() *DeadLetterMachine {
	return machines.NewDeadLetterMachine()
}


func NewPostgresIdempotencyLedger(db *sql.DB) IdempotencyLedgerPort {
	return adapters.NewPostgresIdempotencyLedgerAdapter(db)
}

func NewMemoryIdempotencyLedger() *adapters.MemoryIdempotencyLedgerAdapter {
	return adapters.NewMemoryIdempotencyLedgerAdapter()
}

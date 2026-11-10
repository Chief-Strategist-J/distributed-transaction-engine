package scheduledexecution

import (
	"distributed-transaction-engine/src/features/scheduled-execution/contracts"
	"distributed-transaction-engine/src/features/scheduled-execution/domain/machines"
	"distributed-transaction-engine/src/features/scheduled-execution/domain/rules"
	"distributed-transaction-engine/src/features/scheduled-execution/infrastructure/adapters"
	"distributed-transaction-engine/src/features/scheduled-execution/infrastructure/ports"
	"distributed-transaction-engine/src/features/scheduled-execution/service"
)

type (
	RepositoryPort             = ports.ScheduledExecutionRepositoryPort
	ScheduledExecutionService  = service.ScheduledExecutionService
	ServiceDeps                = service.ServiceDeps
	ScheduleRecord             = contracts.ScheduleRecord
	ScheduleOccurrenceRecord   = contracts.ScheduleOccurrenceRecord
	CreateScheduleInput        = contracts.CreateScheduleInput
	FireScheduleResult         = contracts.FireScheduleResult
	ScheduleType               = contracts.ScheduleType
	DSTPolicy                  = contracts.DSTPolicy
	MisfirePolicy              = contracts.MisfirePolicy
	OverlapPolicy              = contracts.OverlapPolicy
	ScheduleStatus             = contracts.ScheduleStatus
	OccurrenceStatus           = contracts.OccurrenceStatus
	ScheduledExecutionRules    = rules.ScheduledExecutionRules
	ScheduledExecutionMachine  = machines.ScheduledExecutionMachine
)

func NewService(deps ServiceDeps) *ScheduledExecutionService {
	return service.NewService(deps)
}

func NewMemoryAdapter() *adapters.MemoryScheduledExecutionRepositoryAdapter {
	return adapters.NewMemoryScheduledExecutionRepositoryAdapter()
}

func NewPostgresAdapter(db adapters.DBExecutor) *adapters.PostgresScheduledExecutionRepositoryAdapter {
	return adapters.NewPostgresScheduledExecutionRepositoryAdapter(db)
}

func NewScheduledExecutionRules() *ScheduledExecutionRules {
	return rules.NewScheduledExecutionRules()
}

func NewScheduledExecutionMachine() *ScheduledExecutionMachine {
	return machines.NewScheduledExecutionMachine()
}

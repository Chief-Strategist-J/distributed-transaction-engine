package crashRecovery

import (
	"database/sql"

	"distributed-transaction-engine/src/features/crash-recovery/contracts"
	"distributed-transaction-engine/src/features/crash-recovery/infrastructure/adapters"
	"distributed-transaction-engine/src/features/crash-recovery/infrastructure/ports"
	"distributed-transaction-engine/src/features/crash-recovery/service"
	sagaService "distributed-transaction-engine/src/features/saga-orchestration/service"
)

type CrashRecoveryService = service.CrashRecoveryService
type CrashRecoveryRepositoryPort = ports.CrashRecoveryRepositoryPort
type CrashRecoveryConfig = contracts.CrashRecoveryConfig

func NewPostgresAdapter(db *sql.DB) ports.CrashRecoveryRepositoryPort {
	return adapters.NewPostgresCrashRecoveryRepositoryAdapter(db)
}

func NewService(
	cfg contracts.CrashRecoveryConfig,
	repo ports.CrashRecoveryRepositoryPort,
	sagaSvc *sagaService.SagaOrchestrationService,
) *service.CrashRecoveryService {
	return service.NewCrashRecoveryService(cfg, repo, sagaSvc)
}

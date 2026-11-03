package service

import (
	"context"
	"time"

	sagaPorts "distributed-transaction-engine/src/features/saga-orchestration/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
)

type SagaPoller struct {
	repo          sagaPorts.SagaRepositoryPort
	sagaService   *SagaOrchestrationService
	workerID      string
	leaseDuration time.Duration
	logger        *observability.LokiLogger
}

func NewSagaPoller(
	repo sagaPorts.SagaRepositoryPort,
	sagaService *SagaOrchestrationService,
	workerID string,
	leaseDuration time.Duration,
) *SagaPoller {
	if workerID == "" {
		workerID = "worker-" + generateRandomAlpha(8)
	}
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}
	return &SagaPoller{
		repo:          repo,
		sagaService:   sagaService,
		workerID:      workerID,
		leaseDuration: leaseDuration,
		logger:        observability.GetLogger(),
	}
}

func (p *SagaPoller) PollAndExecuteNext(ctx context.Context) (bool, error) {
	instance, err := p.repo.AcquireNextLease(ctx, p.workerID, p.leaseDuration)
	if err != nil {
		return false, err
	}
	if instance == nil {
		return false, nil
	}

	p.logger.Info(ctx, "poller claimed saga lease",
		observability.String("saga_id", instance.PublicID),
		observability.String("worker_id", p.workerID),
	)

	execErr := p.sagaService.ProcessSaga(ctx, instance)
	return true, execErr
}

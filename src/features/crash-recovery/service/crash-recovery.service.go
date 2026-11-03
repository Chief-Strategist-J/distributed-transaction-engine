package service

import (
	"context"
	"time"

	"distributed-transaction-engine/src/features/crash-recovery/contracts"
	"distributed-transaction-engine/src/features/crash-recovery/domain/rules"
	"distributed-transaction-engine/src/features/crash-recovery/infrastructure/ports"
	sagaContracts "distributed-transaction-engine/src/features/saga-orchestration/contracts"
	sagaService "distributed-transaction-engine/src/features/saga-orchestration/service"
	"distributed-transaction-engine/src/infra/observability"
)

type CrashRecoveryService struct {
	cfg         contracts.CrashRecoveryConfig
	repo        ports.CrashRecoveryRepositoryPort
	sagaService *sagaService.SagaOrchestrationService
	logger      *observability.LokiLogger
	tracer      *observability.DefaultTracer
}

func NewCrashRecoveryService(
	cfg contracts.CrashRecoveryConfig,
	repo ports.CrashRecoveryRepositoryPort,
	sagaService *sagaService.SagaOrchestrationService,
) *CrashRecoveryService {
	return &CrashRecoveryService{
		cfg:         cfg,
		repo:        repo,
		sagaService: sagaService,
		logger:      observability.GetLogger(),
		tracer:      observability.NewTracer("crash-recovery-service"),
	}
}

func (s *CrashRecoveryService) Start(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.ScanInterval)
	defer ticker.Stop()

	s.logger.Info(ctx, "crash recovery service started",
		observability.String("incarnation_id", s.cfg.IncarnationID),
		observability.Int64("scan_interval_ms", s.cfg.ScanInterval.Milliseconds()),
		observability.Int("max_takeovers", s.cfg.MaxTakeovers),
	)

	for {
		select {
		case <-ctx.Done():
			s.logger.Info(ctx, "crash recovery service stopping")
			return
		case <-ticker.C:
			reclaimed, err := s.SweepAndReclaim(ctx)
			if err != nil {
				s.logger.Error(ctx, "crash recovery sweep encountered error",
					observability.String("error", err.Error()),
				)
			} else if reclaimed > 0 {
				s.logger.Info(ctx, "crash recovery sweep completed",
					observability.Int("reclaimed_sagas", reclaimed),
				)
			}
		}
	}
}

func (s *CrashRecoveryService) SweepAndReclaim(ctx context.Context) (int, error) {
	ctx, span := s.tracer.StartSpan(ctx, "crash_recovery.sweep")
	defer span.End()

	orphans, err := s.repo.FindExpiredLeases(ctx, s.cfg.BatchSize)
	if err != nil {
		observability.RecordError(span, err)
		return 0, err
	}

	if len(orphans) == 0 {
		return 0, nil
	}

	reclaimedCount := 0
	for _, orphan := range orphans {
		if ctx.Err() != nil {
			break
		}

		takeoverRes, err := s.repo.TakeoverLease(
			ctx,
			orphan.TenantID,
			orphan.ID,
			orphan.FencingToken,
			s.cfg.IncarnationID,
			s.cfg.LeaseDuration,
		)
		if err != nil {
			s.logger.Warn(ctx, "takeover lease failed",
				observability.String("saga_id", orphan.ID),
				observability.String("error", err.Error()),
			)
			continue
		}

		if takeoverRes == nil {
			continue
		}

		if rules.IsPoisonPill(takeoverRes.TakeoverCount, s.cfg.MaxTakeovers) {
			s.logger.Error(ctx, "saga exceeded max takeover count, marking poisoned",
				observability.String("saga_id", orphan.ID),
				observability.Int("takeover_count", takeoverRes.TakeoverCount),
			)
			_ = s.repo.MarkPoisoned(ctx, orphan.TenantID, orphan.ID, takeoverRes.NewToken)
			continue
		}

		steps, err := s.repo.LoadStepExecutions(ctx, orphan.TenantID, orphan.ID)
		if err != nil {
			s.logger.Error(ctx, "failed to load step executions for reclaimed saga",
				observability.String("saga_id", orphan.ID),
				observability.String("error", err.Error()),
			)
			_ = s.repo.ReleaseLease(ctx, orphan.TenantID, orphan.ID, takeoverRes.NewToken)
			continue
		}

		for i := range steps {
			action := rules.ClassifyStepReconciliation(steps[i].ExecutionState)
			if action == rules.ActionRedispatchForward && steps[i].ExecutionState == sagaContracts.StepStateDispatched {
				steps[i].ExecutionState = sagaContracts.StepStateUnknown
				_ = s.repo.UpdateStepExecution(ctx, orphan.TenantID, orphan.ID, &steps[i])
			}
		}

		instance := &sagaContracts.SagaInstance{
			ID:                   orphan.ID,
			PublicID:             orphan.PublicID,
			TenantID:             orphan.TenantID,
			WorkflowDefinitionID: orphan.WorkflowDefinitionID,
			WorkflowVersion:      orphan.WorkflowVersion,
			SpecHash:             orphan.SpecHash,
			CurrentState:         orphan.CurrentState,
			FencingToken:         takeoverRes.NewToken,
			LeaseOwner:           s.cfg.IncarnationID,
			InitialPayload:       orphan.InitialPayload,
			Steps:                steps,
		}

		reclaimedCount++
		go func(inst *sagaContracts.SagaInstance) {
			procErr := s.sagaService.ProcessSaga(context.Background(), inst)
			if procErr != nil {
				s.logger.Warn(context.Background(), "reclaimed saga execution ended with error",
					observability.String("saga_id", inst.PublicID),
					observability.String("error", procErr.Error()),
				)
			}
		}(instance)
	}

	return reclaimedCount, nil
}

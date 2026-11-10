package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	contracts "distributed-transaction-engine/src/features/scheduled-execution/contracts"
	"distributed-transaction-engine/src/features/scheduled-execution/domain/machines"
	"distributed-transaction-engine/src/features/scheduled-execution/domain/rules"
	"distributed-transaction-engine/src/features/scheduled-execution/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	"github.com/google/uuid"
)

type ServiceDeps struct {
	Repo    ports.ScheduledExecutionRepositoryPort
	Rules   *rules.ScheduledExecutionRules
	Machine *machines.ScheduledExecutionMachine
}

type ScheduledExecutionService struct {
	repo    ports.ScheduledExecutionRepositoryPort
	rules   *rules.ScheduledExecutionRules
	machine *machines.ScheduledExecutionMachine
	tracer  *observability.DefaultTracer
}

func NewService(deps ServiceDeps) *ScheduledExecutionService {
	return &ScheduledExecutionService{
		repo:    deps.Repo,
		rules:   deps.Rules,
		machine: deps.Machine,
		tracer:  observability.NewTracer("scheduled-execution-service"),
	}
}

func (s *ScheduledExecutionService) CreateSchedule(ctx context.Context, input contracts.CreateScheduleInput) (spanResult0 *contracts.ScheduleRecord, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "scheduled_execution.create_schedule")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	if err := contracts.ValidateCreateScheduleInput(input); err != nil {
		return nil, err
	}

	sch := s.buildNewSchedule(input)
	if err := s.repo.SaveSchedule(ctx, sch); err != nil {
		return nil, err
	}
	return &sch, nil
}

func (s *ScheduledExecutionService) buildNewSchedule(input contracts.CreateScheduleInput) contracts.ScheduleRecord {
	schID := uuid.New()
	pubID := "sch_" + strings.ReplaceAll(schID.String(), "-", "")[:26]
	now := time.Now().UTC()
	extID := fmt.Sprintf("sch_ext_%s_%d", input.TenantID, now.UnixNano())

	return contracts.ScheduleRecord{
		ID: schID, PublicID: pubID, TenantID: input.TenantID, Source: "ENGINE_SCHEDULER",
		ExternalID: extID, CorrelationID: uuid.New(), WorkflowID: input.WorkflowID,
		ScheduleType: input.ScheduleType, CronExpression: input.CronExpression,
		TimeZone: input.TimeZone, DSTPolicy: s.resolveDSTPolicy(input.DSTPolicy),
		MisfirePolicy:      s.resolveMisfirePolicy(input.MisfirePolicy),
		OverlapPolicy:      s.resolveOverlapPolicy(input.OverlapPolicy),
		MaxLatenessSeconds: s.resolveLateness(input.MaxLatenessSeconds),
		Status:             contracts.ScheduleActive, RunAsPrincipal: input.RunAsPrincipal,
		RequiredPermission: "action:workflow:execute", FrozenPayload: input.FrozenPayload,
		NextOccurrenceAt:    s.determineFirstOccurrence(input.FirstOccurrenceAt, now),
		ConsecutiveFailures: 0, Version: 1, CreatedAt: now, UpdatedAt: now,
	}
}

func (s *ScheduledExecutionService) determineFirstOccurrence(first *time.Time, now time.Time) time.Time {
	if first != nil {
		return *first
	}
	return now.Add(time.Minute)
}

func (s *ScheduledExecutionService) FireDueSchedule(ctx context.Context, tenantID string, scheduleID uuid.UUID, now time.Time, isRunning bool) (spanResult0 *contracts.FireScheduleResult, spanErr error) {
	ctx, span := s.tracer.StartSpan(ctx, "scheduled_execution.fire_schedule")
	defer func() {
		observability.RecordError(span, spanErr)
		span.End()
	}()

	sch, err := s.repo.GetScheduleByID(ctx, tenantID, scheduleID)
	if err != nil {
		return nil, err
	}

	if sch.NextOccurrenceAt.After(now) {
		return &contracts.FireScheduleResult{
			Fired:  false,
			Reason: "Schedule is not due yet",
		}, nil
	}

	logicalTime := sch.NextOccurrenceAt
	if s.rules.IsOccurrenceExpired(logicalTime, now, sch.MaxLatenessSeconds) {
		return s.handleExpiredOccurrence(ctx, sch, logicalTime)
	}

	shouldDispatch, status := s.rules.DecideOverlapAction(sch.OverlapPolicy, isRunning)
	if !shouldDispatch {
		return s.recordSkippedOccurrence(ctx, sch, logicalTime, status)
	}

	return s.executeFiring(ctx, sch, logicalTime)
}

func (s *ScheduledExecutionService) handleExpiredOccurrence(ctx context.Context, sch *contracts.ScheduleRecord, logicalTime time.Time) (*contracts.FireScheduleResult, error) {
	occID := uuid.New()
	pubID := "occ_" + strings.ReplaceAll(occID.String(), "-", "")[:26]
	occ := contracts.ScheduleOccurrenceRecord{
		ID:                  occID,
		PublicID:            pubID,
		TenantID:            sch.TenantID,
		Source:              "ENGINE_SCHEDULER",
		ExternalID:          fmt.Sprintf("occ_ext_%s_%d", sch.TenantID, logicalTime.Unix()),
		CorrelationID:       uuid.New(),
		ScheduleID:          sch.ID,
		LogicalOccurrenceAt: logicalTime,
		Status:              contracts.OccurrenceExpired,
		FailureReason:       "Exceeded max lateness seconds",
		Version:             1,
		CreatedAt:           time.Now().UTC(),
	}
	observability.NoteSwallowed(s.repo.SaveOccurrence(ctx, occ))
	nextOcc, _ := s.rules.ComputeNextOccurrence(*sch, logicalTime)
	observability.NoteSwallowed(s.repo.AdvanceSchedule(ctx, sch.TenantID, sch.ID, sch.Version, nextOcc, logicalTime))

	return &contracts.FireScheduleResult{
		Fired:            false,
		OccurrenceID:     occID,
		OccurrenceStatus: contracts.OccurrenceExpired,
		Reason:           "Occurrence expired due to lateness",
	}, nil
}

func (s *ScheduledExecutionService) recordSkippedOccurrence(ctx context.Context, sch *contracts.ScheduleRecord, logicalTime time.Time, status contracts.OccurrenceStatus) (*contracts.FireScheduleResult, error) {
	occID := uuid.New()
	pubID := "occ_" + strings.ReplaceAll(occID.String(), "-", "")[:26]
	occ := contracts.ScheduleOccurrenceRecord{
		ID:                  occID,
		PublicID:            pubID,
		TenantID:            sch.TenantID,
		Source:              "ENGINE_SCHEDULER",
		ExternalID:          fmt.Sprintf("occ_ext_%s_%d", sch.TenantID, logicalTime.Unix()),
		CorrelationID:       uuid.New(),
		ScheduleID:          sch.ID,
		LogicalOccurrenceAt: logicalTime,
		Status:              status,
		FailureReason:       "Skipped by overlap policy",
		Version:             1,
		CreatedAt:           time.Now().UTC(),
	}
	observability.NoteSwallowed(s.repo.SaveOccurrence(ctx, occ))
	nextOcc, _ := s.rules.ComputeNextOccurrence(*sch, logicalTime)
	observability.NoteSwallowed(s.repo.AdvanceSchedule(ctx, sch.TenantID, sch.ID, sch.Version, nextOcc, logicalTime))

	return &contracts.FireScheduleResult{
		Fired:            false,
		OccurrenceID:     occID,
		OccurrenceStatus: status,
		Reason:           "Skipped by overlap policy",
	}, nil
}

func (s *ScheduledExecutionService) executeFiring(ctx context.Context, sch *contracts.ScheduleRecord, logicalTime time.Time) (*contracts.FireScheduleResult, error) {
	occID := uuid.New()
	sagaID := uuid.New()
	occ := s.buildDispatchedOccurrence(sch, logicalTime, occID, sagaID)
	if err := s.repo.SaveOccurrence(ctx, occ); err != nil {
		return nil, err
	}

	nextOcc, _ := s.rules.ComputeNextOccurrence(*sch, logicalTime)
	if err := s.repo.AdvanceSchedule(ctx, sch.TenantID, sch.ID, sch.Version, nextOcc, logicalTime); err != nil {
		return nil, err
	}

	return &contracts.FireScheduleResult{
		Fired:            true,
		OccurrenceID:     occID,
		OccurrenceStatus: contracts.OccurrenceDispatched,
		DispatchedSagaID: &sagaID,
		Reason:           "Dispatched successfully",
	}, nil
}

func (s *ScheduledExecutionService) buildDispatchedOccurrence(sch *contracts.ScheduleRecord, logicalTime time.Time, occID, sagaID uuid.UUID) contracts.ScheduleOccurrenceRecord {
	pubID := "occ_" + strings.ReplaceAll(occID.String(), "-", "")[:26]
	return contracts.ScheduleOccurrenceRecord{
		ID:                  occID,
		PublicID:            pubID,
		TenantID:            sch.TenantID,
		Source:              "ENGINE_SCHEDULER",
		ExternalID:          fmt.Sprintf("occ_ext_%s_%d", sch.TenantID, logicalTime.Unix()),
		CorrelationID:       uuid.New(),
		ScheduleID:          sch.ID,
		LogicalOccurrenceAt: logicalTime,
		Status:              contracts.OccurrenceDispatched,
		DispatchedSagaID:    &sagaID,
		Version:             1,
		CreatedAt:           time.Now().UTC(),
	}
}

func (s *ScheduledExecutionService) resolveDSTPolicy(p contracts.DSTPolicy) contracts.DSTPolicy {
	if p == "" {
		return contracts.DSTRunAtNextValid
	}
	return p
}

func (s *ScheduledExecutionService) resolveMisfirePolicy(p contracts.MisfirePolicy) contracts.MisfirePolicy {
	if p == "" {
		return contracts.MisfireRunAllMissed
	}
	return p
}

func (s *ScheduledExecutionService) resolveOverlapPolicy(p contracts.OverlapPolicy) contracts.OverlapPolicy {
	if p == "" {
		return contracts.OverlapSkipIfRunning
	}
	return p
}

func (s *ScheduledExecutionService) resolveLateness(sec int) int {
	if sec <= 0 {
		return 3600
	}
	return sec
}

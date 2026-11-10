package tests

import (
	"context"
	"testing"
	"time"

	contracts "distributed-transaction-engine/src/features/scheduled-execution/contracts"
	"distributed-transaction-engine/src/features/scheduled-execution/domain/machines"
	"distributed-transaction-engine/src/features/scheduled-execution/domain/rules"
	"distributed-transaction-engine/src/features/scheduled-execution/infrastructure/adapters"
	"distributed-transaction-engine/src/features/scheduled-execution/service"
)

func TestScheduledExecutionServiceIntegration(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryScheduledExecutionRepositoryAdapter()
	r := rules.NewScheduledExecutionRules()
	m := machines.NewScheduledExecutionMachine()

	svc := service.NewService(service.ServiceDeps{
		Repo:    repo,
		Rules:   r,
		Machine: m,
	})

	tenantID := "tenant-gamma"
	firstDue := time.Now().UTC().Add(-10 * time.Minute)

	input := contracts.CreateScheduleInput{
		TenantID:           tenantID,
		WorkflowID:         "workflow-billing-v1",
		ScheduleType:       contracts.TypeRecurringCron,
		CronExpression:     "0 * * * *",
		TimeZone:           "Asia/Kolkata",
		RunAsPrincipal:     "system-cron",
		MaxLatenessSeconds: 3600,
		FirstOccurrenceAt:  &firstDue,
	}

	sch, err := svc.CreateSchedule(ctx, input)
	if err != nil {
		t.Fatalf("unexpected error creating schedule: %v", err)
	}

	fireRes, err := svc.FireDueSchedule(ctx, tenantID, sch.ID, time.Now().UTC(), false)
	if err != nil {
		t.Fatalf("unexpected error firing schedule: %v", err)
	}

	if !fireRes.Fired || fireRes.OccurrenceStatus != contracts.OccurrenceDispatched {
		t.Fatalf("expected schedule to fire and dispatch")
	}

	doubleFireRes, err := svc.FireDueSchedule(ctx, tenantID, sch.ID, time.Now().UTC(), false)
	if err == nil && doubleFireRes != nil && doubleFireRes.Fired {
		t.Fatalf("expected subsequent firing on advanced schedule not to duplicate")
	}
}

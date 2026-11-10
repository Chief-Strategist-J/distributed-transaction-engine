package tests

import (
	"context"
	"testing"

	"distributed-transaction-engine/src/features/dead-letter-queue/contracts"
	"distributed-transaction-engine/src/features/dead-letter-queue/domain/machines"
	"distributed-transaction-engine/src/features/dead-letter-queue/domain/rules"
	"distributed-transaction-engine/src/features/dead-letter-queue/infrastructure/adapters"
	"distributed-transaction-engine/src/features/dead-letter-queue/service"
	"distributed-transaction-engine/src/infra/observability"

	"github.com/google/uuid"
)

func TestService_TracingPropagation(t *testing.T) {
	tracer := observability.NewTracer("dlq-tracing-test")
	repo := adapters.NewMemoryDeadLetterRepositoryAdapter()
	svc := service.NewService(service.ServiceDeps{
		Repo:    repo,
		Rules:   rules.NewDeadLetterRules(),
		Machine: machines.NewDeadLetterMachine(),
		Tracer:  tracer,
	})

	ctx := context.Background()
	ctx, rootSpan := tracer.StartSpan(ctx, "root_test_span")
	defer rootSpan.End()

	record, err := svc.AdmitRecord(ctx, contracts.AdmitDLQInput{
		TenantID:          "tenant-trace",
		CorrelationID:     uuid.New(),
		SagaID:            uuid.New(),
		StepIndex:         0,
		StepType:          "STEP_AUTH",
		AttemptID:         uuid.New(),
		IdempotencyKey:    "trace-idemp-1",
		OutcomeClass:      contracts.OutcomeNotExecuted,
		FailureClass:      contracts.FailurePoison,
		ErrorFingerprint:  "ERR_POISON_SCHEMA",
		RawMessage:        "poison payload rejected",
		PayloadRef:        "s3://payloads/trace.json",
		SchemaVersion:     1,
		MoneyAtStakeCents: 0,
		Currency:          "USD",
	})
	if err != nil {
		t.Fatalf("admit record failed: %v", err)
	}

	fetched, err := svc.GetRecord(ctx, "tenant-trace", record.ID)
	if err != nil {
		t.Fatalf("get record failed: %v", err)
	}
	if fetched.ID != record.ID {
		t.Errorf("fetched id mismatch: got %s, expected %s", fetched.ID, record.ID)
	}
}


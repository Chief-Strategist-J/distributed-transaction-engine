package tests

import (
	"context"
	"testing"
	"time"

	"distributed-transaction-engine/src/features/dead-letter-queue/contracts"
	"distributed-transaction-engine/src/features/dead-letter-queue/domain/machines"
	"distributed-transaction-engine/src/features/dead-letter-queue/domain/rules"
	"distributed-transaction-engine/src/features/dead-letter-queue/infrastructure/adapters"
	"distributed-transaction-engine/src/features/dead-letter-queue/service"

	"github.com/google/uuid"
)

func TestService_EndToEndLifecycle(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryDeadLetterRepositoryAdapter()
	svc := service.NewService(service.ServiceDeps{
		Repo:    repo,
		Rules:   rules.NewDeadLetterRules(),
		Machine: machines.NewDeadLetterMachine(),
	})

	tenantID := "tenant-test-1"
	sagaID := uuid.New()
	attemptID := uuid.New()
	deadline := time.Now().UTC().Add(4 * time.Hour)

	admitInput := contracts.AdmitDLQInput{
		TenantID:          tenantID,
		CorrelationID:     uuid.New(),
		SagaID:            sagaID,
		StepIndex:         1,
		StepType:          "PAYMENT_STEP",
		AttemptID:         attemptID,
		IdempotencyKey:    "idemp-12345",
		OutcomeClass:      contracts.OutcomeNotExecuted,
		FailureClass:      contracts.FailureRetryExhausted,
		ErrorFingerprint:  "FINGERPRINT_PAYMENT_DOWN",
		RawMessage:        "payment failed for user user@example.com with card 4111-1111-1111-1111 and token Bearer xyz123",
		PayloadRef:        "s3://payloads/attempt-12345.json",
		SchemaVersion:     1,
		MoneyAtStakeCents: 5000,
		Currency:          "USD",
		BusinessDeadline:  &deadline,
	}

	record, err := svc.AdmitRecord(ctx, admitInput)
	if err != nil {
		t.Fatalf("failed to admit record: %v", err)
	}

	if record.Status != contracts.StatusQuarantined {
		t.Errorf("expected initial status QUARANTINED, got %s", record.Status)
	}
	if record.SanitizedMessage == admitInput.RawMessage {
		t.Errorf("expected message to be sanitized, got raw message")
	}

	triaged, err := svc.TriageRecord(ctx, contracts.TriageInput{
		TenantID: tenantID,
		RecordID: record.ID,
		ActorID:  "operator-1",
	})
	if err != nil {
		t.Fatalf("failed to triage record: %v", err)
	}
	if triaged.Status != contracts.StatusTriaged {
		t.Errorf("expected status TRIAGED, got %s", triaged.Status)
	}

	eval, err := svc.EvaluateReplay(ctx, tenantID, record.ID, "RUNNING", 1)
	if err != nil {
		t.Fatalf("failed to evaluate replay: %v", err)
	}
	if !eval.Allowed {
		t.Errorf("expected replay allowed, got false: %s", eval.Reason)
	}

	retried, err := svc.RetryRecord(ctx, contracts.RetryActionInput{
		TenantID: tenantID,
		RecordID: record.ID,
		ActorID:  "operator-1",
	}, "RUNNING", 1)
	if err != nil {
		t.Fatalf("failed to start retry: %v", err)
	}
	if retried.Status != contracts.StatusActionInProgress {
		t.Errorf("expected status ACTION_IN_PROGRESS, got %s", retried.Status)
	}

	resolved, err := svc.ResolveEvidence(ctx, contracts.ResolveEvidenceInput{
		TenantID:    tenantID,
		RecordID:    record.ID,
		ActorID:     "operator-1",
		Executed:    true,
		EvidenceRef: "provider-tx-ref-987",
		Reason:      "verified provider settled the payment",
	})
	if err != nil {
		t.Fatalf("failed to resolve record: %v", err)
	}
	if resolved.Status != contracts.StatusResolved {
		t.Errorf("expected status RESOLVED, got %s", resolved.Status)
	}
	if resolved.Disposition == nil || *resolved.Disposition != contracts.DispositionResolvedAsExecuted {
		t.Errorf("unexpected disposition: %v", resolved.Disposition)
	}
}

func TestService_DualApproverEnforcement(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryDeadLetterRepositoryAdapter()
	svc := service.NewService(service.ServiceDeps{
		Repo:    repo,
		Rules:   rules.NewDeadLetterRules(),
		Machine: machines.NewDeadLetterMachine(),
	})

	tenantID := "tenant-test-2"
	rec, err := svc.AdmitRecord(ctx, contracts.AdmitDLQInput{
		TenantID:          tenantID,
		CorrelationID:     uuid.New(),
		SagaID:            uuid.New(),
		StepIndex:         0,
		StepType:          "REFUND_STEP",
		AttemptID:         uuid.New(),
		IdempotencyKey:    "ref-1",
		OutcomeClass:      contracts.OutcomeNotExecuted,
		FailureClass:      contracts.FailureBusinessRejection,
		ErrorFingerprint:  "ERR_REFUND_LIMIT",
		RawMessage:        "refund limit exceeded",
		PayloadRef:        "s3://payloads/ref.json",
		SchemaVersion:     1,
		MoneyAtStakeCents: 10000,
		Currency:          "USD",
	})
	if err != nil {
		t.Fatalf("failed to admit: %v", err)
	}

	_, err = svc.WriteOffRecord(ctx, contracts.WriteOffInput{
		TenantID:         tenantID,
		RecordID:         rec.ID,
		ActorID:          "operator-1",
		SecondApproverID: "",
		Reason:           "approved write off",
	})
	if err == nil {
		t.Fatalf("expected error without second approver for monetary write-off")
	}

	_, err = svc.WriteOffRecord(ctx, contracts.WriteOffInput{
		TenantID:         tenantID,
		RecordID:         rec.ID,
		ActorID:          "operator-1",
		SecondApproverID: "operator-1",
		Reason:           "approved write off self approval",
	})
	if err == nil {
		t.Fatalf("expected error when second approver is identical to actor")
	}

	writtenOff, err := svc.WriteOffRecord(ctx, contracts.WriteOffInput{
		TenantID:         tenantID,
		RecordID:         rec.ID,
		ActorID:          "operator-1",
		SecondApproverID: "manager-2",
		Reason:           "approved write off by supervisor",
	})
	if err != nil {
		t.Fatalf("failed to write off with dual approval: %v", err)
	}
	if writtenOff.Status != contracts.StatusResolved {
		t.Errorf("expected status RESOLVED, got %s", writtenOff.Status)
	}
}

func TestService_SystemicBurstIncident(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryDeadLetterRepositoryAdapter()
	svc := service.NewService(service.ServiceDeps{
		Repo:    repo,
		Rules:   rules.NewDeadLetterRules(),
		Machine: machines.NewDeadLetterMachine(),
	})

	tenantID := "tenant-burst"
	fp := "ERR_DATABASE_DOWN"
	stepType := "DATABASE_STEP"

	for i := 0; i < 20; i++ {
		_, err := svc.AdmitRecord(ctx, contracts.AdmitDLQInput{
			TenantID:          tenantID,
			CorrelationID:     uuid.New(),
			SagaID:            uuid.New(),
			StepIndex:         0,
			StepType:          stepType,
			AttemptID:         uuid.New(),
			IdempotencyKey:    uuid.New().String(),
			OutcomeClass:      contracts.OutcomeFailed,
			FailureClass:      contracts.FailureDefinitionDefect,
			ErrorFingerprint:  fp,
			RawMessage:        "database connection lost",
			PayloadRef:        "s3://payloads/burst.json",
			SchemaVersion:     1,
			MoneyAtStakeCents: 0,
			Currency:          "USD",
		})
		if err != nil {
			t.Fatalf("failed to admit in loop %d: %v", i, err)
		}
	}

	isPaused, err := repo.IsStepTypePaused(ctx, tenantID, stepType)
	if err != nil {
		t.Fatalf("failed to check pause: %v", err)
	}
	if !isPaused {
		t.Errorf("expected step type to be paused after systemic burst threshold reached")
	}
}


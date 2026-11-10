package tests

import (
	"context"
	"testing"

	"distributed-transaction-engine/src/features/manual-intervention/contracts"
	"distributed-transaction-engine/src/features/manual-intervention/domain/machines"
	"distributed-transaction-engine/src/features/manual-intervention/domain/rules"
	"distributed-transaction-engine/src/features/manual-intervention/infrastructure/adapters"
	"distributed-transaction-engine/src/features/manual-intervention/service"
	"github.com/google/uuid"
)

func TestManualInterventionServiceIntegration(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryManualInterventionRepositoryAdapter()
	r := rules.NewManualInterventionRules()
	m := machines.NewManualInterventionMachine()

	svc := service.NewService(service.ServiceDeps{
		Repo:    repo,
		Rules:   r,
		Machine: m,
	})

	sagaID := uuid.New()
	tenantID := "tenant-alpha"

	input := contracts.ExecuteInterventionInput{
		TenantID:         tenantID,
		SagaID:           sagaID,
		OperatorID:       "operator-42",
		OperatorRole:     "admin:saga:intervene",
		ActionType:       contracts.ActionForceComplete,
		CurrentSagaState: "STUCK",
		Reason:           "Bank confirmed out-of-band payment clearance",
		ApprovalTicketID: "TICKET-9921",
		SyntheticPayload: "{\"status\":\"PAID\"}",
	}

	res, err := svc.Intervene(ctx, input)
	if err != nil {
		t.Fatalf("unexpected error during intervene: %v", err)
	}

	if !res.Success || res.NewState != "COMPLETED" {
		t.Fatalf("expected successful intervention advancing to COMPLETED")
	}

	audits, err := svc.ListAuditBySaga(ctx, tenantID, sagaID)
	if err != nil || len(audits) != 1 {
		t.Fatalf("expected 1 audit record, got %d", len(audits))
	}
	if audits[0].ApprovalTicketID != "TICKET-9921" {
		t.Fatalf("expected audit ticket TICKET-9921")
	}
}

func TestManualInterventionUnauthorized(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryManualInterventionRepositoryAdapter()
	svc := service.NewService(service.ServiceDeps{
		Repo:    repo,
		Rules:   rules.NewManualInterventionRules(),
		Machine: machines.NewManualInterventionMachine(),
	})

	input := contracts.ExecuteInterventionInput{
		TenantID:         "tenant-beta",
		SagaID:           uuid.New(),
		OperatorID:       "operator-99",
		OperatorRole:     "readonly-analyst",
		ActionType:       contracts.ActionForceRetry,
		CurrentSagaState: "FAILED",
		Reason:           "Retry requested",
	}

	_, err := svc.Intervene(ctx, input)
	if err == nil {
		t.Fatalf("expected unauthorized operator to fail")
	}
}

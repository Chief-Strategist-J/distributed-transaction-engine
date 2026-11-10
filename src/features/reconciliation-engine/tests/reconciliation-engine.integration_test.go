package tests

import (
	"context"
	"testing"

	"distributed-transaction-engine/src/features/reconciliation-engine/contracts"
	"distributed-transaction-engine/src/features/reconciliation-engine/domain/machines"
	"distributed-transaction-engine/src/features/reconciliation-engine/domain/rules"
	"distributed-transaction-engine/src/features/reconciliation-engine/infrastructure/adapters"
	"distributed-transaction-engine/src/features/reconciliation-engine/service"
	"github.com/google/uuid"
)

func TestReconciliationServiceIntegration(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryReconciliationRepositoryAdapter()
	r := rules.NewReconciliationRules()
	m := machines.NewReconciliationMachine()

	svc := service.NewService(service.ServiceDeps{
		Repo:    repo,
		Rules:   r,
		Machine: m,
	})

	sagaID := uuid.New()
	tenantID := "tenant-alpha"

	input := contracts.EvaluateReconciliationInput{
		TenantID:            tenantID,
		SagaID:              sagaID,
		InternalStatus:      "COMPENSATED",
		ExternalStatus:      "COMMITTED",
		InternalAmountCents: 1000,
		ExternalAmountCents: 1000,
		Currency:            "USD",
	}

	res, err := svc.Evaluate(ctx, input)
	if err != nil {
		t.Fatalf("unexpected error during evaluate: %v", err)
	}

	if !res.IsAnomaly || *res.AnomalyType != contracts.AnomalyGhostTransaction {
		t.Fatalf("expected GHOST_TRANSACTION anomaly")
	}

	anomalies, err := svc.ListAnomalies(ctx, tenantID, nil)
	if err != nil {
		t.Fatalf("unexpected error listing anomalies: %v", err)
	}
	if len(anomalies) != 1 {
		t.Fatalf("expected 1 anomaly record in repository, got %d", len(anomalies))
	}

	err = svc.ResolveAnomaly(ctx, tenantID, anomalies[0].ID, anomalies[0].Version, "REFUND_DISPATCHED")
	if err != nil {
		t.Fatalf("unexpected error resolving anomaly: %v", err)
	}

	status := contracts.StatusAutoResolved
	resolvedList, err := svc.ListAnomalies(ctx, tenantID, &status)
	if err != nil || len(resolvedList) != 1 {
		t.Fatalf("expected 1 resolved anomaly, got %d", len(resolvedList))
	}
}

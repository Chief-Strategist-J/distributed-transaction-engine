package tests

import (
	"testing"

	"distributed-transaction-engine/src/features/reconciliation-engine/contracts"
	"distributed-transaction-engine/src/features/reconciliation-engine/domain/rules"
	"github.com/google/uuid"
)

func TestReconciliationRulesBalanced(t *testing.T) {
	r := rules.NewReconciliationRules()
	res := r.ClassifyDrift(contracts.EvaluateReconciliationInput{
		TenantID:            "tenant-1",
		SagaID:              uuid.New(),
		InternalStatus:      "COMPLETED",
		ExternalStatus:      "COMMITTED",
		InternalAmountCents: 5000,
		ExternalAmountCents: 5000,
		Currency:            "USD",
	})

	if res.IsAnomaly {
		t.Fatalf("expected no anomaly for balanced state, got true")
	}
}

func TestReconciliationRulesGhostTransaction(t *testing.T) {
	r := rules.NewReconciliationRules()
	res := r.ClassifyDrift(contracts.EvaluateReconciliationInput{
		TenantID:            "tenant-1",
		SagaID:              uuid.New(),
		InternalStatus:      "COMPENSATED",
		ExternalStatus:      "COMMITTED",
		InternalAmountCents: 5000,
		ExternalAmountCents: 5000,
		Currency:            "USD",
	})

	if !res.IsAnomaly {
		t.Fatalf("expected anomaly for ghost transaction, got false")
	}
	if *res.AnomalyType != contracts.AnomalyGhostTransaction {
		t.Fatalf("expected GHOST_TRANSACTION anomaly, got %s", *res.AnomalyType)
	}
	if !res.CanAutoRepair {
		t.Fatalf("expected auto-repair true for ghost transaction refund, got false")
	}
}

func TestReconciliationRulesPhantomSettlement(t *testing.T) {
	r := rules.NewReconciliationRules()
	res := r.ClassifyDrift(contracts.EvaluateReconciliationInput{
		TenantID:            "tenant-1",
		SagaID:              uuid.New(),
		InternalStatus:      "COMPLETED",
		ExternalStatus:      "NOT_FOUND",
		InternalAmountCents: 5000,
		ExternalAmountCents: 0,
		Currency:            "USD",
	})

	if !res.IsAnomaly {
		t.Fatalf("expected anomaly for phantom settlement, got false")
	}
	if *res.AnomalyType != contracts.AnomalyPhantomSettlement {
		t.Fatalf("expected PHANTOM_SETTLEMENT anomaly, got %s", *res.AnomalyType)
	}
}

func TestReconciliationRulesNumericalDrift(t *testing.T) {
	r := rules.NewReconciliationRules()
	resSmall := r.ClassifyDrift(contracts.EvaluateReconciliationInput{
		TenantID:            "tenant-1",
		SagaID:              uuid.New(),
		InternalStatus:      "COMPLETED",
		ExternalStatus:      "COMMITTED",
		InternalAmountCents: 5050,
		ExternalAmountCents: 5000,
		Currency:            "USD",
	})

	if !resSmall.IsAnomaly {
		t.Fatalf("expected anomaly for numerical drift, got false")
	}
	if *resSmall.AnomalyType != contracts.AnomalyNumericalDrift {
		t.Fatalf("expected NUMERICAL_DRIFT anomaly, got %s", *resSmall.AnomalyType)
	}
	if !resSmall.CanAutoRepair {
		t.Fatalf("expected small drift to be auto-repairable")
	}

	resLarge := r.ClassifyDrift(contracts.EvaluateReconciliationInput{
		TenantID:            "tenant-1",
		SagaID:              uuid.New(),
		InternalStatus:      "COMPLETED",
		ExternalStatus:      "COMMITTED",
		InternalAmountCents: 100000,
		ExternalAmountCents: 5000,
		Currency:            "USD",
	})
	if resLarge.CanAutoRepair {
		t.Fatalf("expected large drift to require manual review")
	}
}

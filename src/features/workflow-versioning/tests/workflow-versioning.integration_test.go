package tests

import (
	"context"
	"testing"

	"distributed-transaction-engine/src/features/workflow-versioning/contracts"
	"distributed-transaction-engine/src/features/workflow-versioning/infrastructure/adapters"
	"distributed-transaction-engine/src/features/workflow-versioning/service"
	"github.com/google/uuid"
)

func TestService_EndToEndLifecycleAndMakerChecker(t *testing.T) {
	ctx := context.Background()
	repo := adapters.NewMemoryWorkflowVersioningRepositoryAdapter()
	svc := service.NewWorkflowVersioningService(service.ServiceDeps{Repo: repo})

	bundle, err := svc.RegisterBundle(ctx, contracts.RegisterBundleCommand{
		TenantID:             "tenant_corp",
		WorkflowName:         "payment-checkout",
		VersionTag:           "v1.0.0",
		BundlePayload:        map[string]interface{}{"steps": []string{"validate", "charge"}},
		CapabilitiesRequired: []string{"pci-dss"},
	})
	if err != nil {
		t.Fatalf("failed to register bundle: %v", err)
	}
	if bundle.Status != contracts.BundleStatusDraft {
		t.Fatalf("expected initial status DRAFT, got: %s", bundle.Status)
	}

	promo1, err := svc.ProposePromotion(ctx, contracts.ProposePromotionCommand{
		TenantID:   "tenant_corp",
		BundleID:   bundle.ID,
		Action:     contracts.PromotionActionPublish,
		ProposerID: "engineer_1",
		Reason:     "Initial release candidate",
	})
	if err != nil {
		t.Fatalf("failed to propose publish: %v", err)
	}

	decided1, err := svc.DecidePromotion(ctx, contracts.DecidePromotionCommand{
		TenantID:    "tenant_corp",
		PromotionID: promo1.ID,
		ApproverID:  "qa_lead",
		Approved:    true,
		Reason:      "Passed synthetic test suite",
	})
	if err != nil || decided1.Status != contracts.PromotionStatusApproved {
		t.Fatalf("failed to approve publish: %v", err)
	}

	promo2, err := svc.ProposePromotion(ctx, contracts.ProposePromotionCommand{
		TenantID:   "tenant_corp",
		BundleID:   bundle.ID,
		Action:     contracts.PromotionActionPromote,
		ProposerID: "lead_architect",
		Reason:     "Ready for active production traffic",
	})
	if err != nil {
		t.Fatalf("failed to propose promote: %v", err)
	}

	_, err = svc.DecidePromotion(ctx, contracts.DecidePromotionCommand{
		TenantID:    "tenant_corp",
		PromotionID: promo2.ID,
		ApproverID:  "lead_architect",
		Approved:    true,
		Reason:      "Self approval attempt",
	})
	if err == nil {
		t.Fatal("expected maker-checker violation when approver equals proposer")
	}

	decided2, err := svc.DecidePromotion(ctx, contracts.DecidePromotionCommand{
		TenantID:    "tenant_corp",
		PromotionID: promo2.ID,
		ApproverID:  "secops_officer",
		Approved:    true,
		Reason:      "Security signed off",
	})
	if err != nil || decided2.Status != contracts.PromotionStatusApproved {
		t.Fatalf("failed to approve promote: %v", err)
	}

	res1, err := svc.ResolveVersion(ctx, contracts.ResolveVersionQuery{
		TenantID:     "tenant_corp",
		WorkflowName: "payment-checkout",
		EntityID:     "entity_customer_999",
		LiveInputs:   map[string]interface{}{"fee_rate": 0.025},
	})
	if err != nil {
		t.Fatalf("failed to resolve active version: %v", err)
	}
	if res1.Bundle.Status != contracts.BundleStatusActive {
		t.Fatalf("expected ACTIVE bundle, got %s", res1.Bundle.Status)
	}
	if res1.IsStickyCohort {
		t.Fatal("first call should establish cohort, not be existing sticky")
	}

	res2, err := svc.ResolveVersion(ctx, contracts.ResolveVersionQuery{
		TenantID:     "tenant_corp",
		WorkflowName: "payment-checkout",
		EntityID:     "entity_customer_999",
		LiveInputs:   map[string]interface{}{"fee_rate": 0.050},
	})
	if err != nil {
		t.Fatalf("failed to resolve second time: %v", err)
	}
	if !res2.IsStickyCohort {
		t.Fatal("subsequent call for same entity must return sticky cohort")
	}

	integrity, err := svc.VerifyBundleIntegrity(ctx, contracts.VerifyIntegrityQuery{
		TenantID: bundle.TenantID,
		BundleID: bundle.ID,
	})
	if err != nil || !integrity.IntegrityMatch {
		t.Fatalf("integrity check failed: %v", err)
	}

	migration, err := svc.MigrateSaga(ctx, contracts.MigrateSagaCommand{
		TenantID:            "tenant_corp",
		Source:              "payment-checkout",
		SagaID:              uuid.New(),
		TargetVersionTag:    "v1.0.0",
		SafePointStepID:     "validate",
		StateMappingApplied: map[string]interface{}{"remapped_fields": true},
		OperatorID:          "ops_user",
	})
	if err != nil {
		t.Fatalf("failed to execute saga safe point migration: %v", err)
	}
	if migration.Status != contracts.MigrationStatusExecuted {
		t.Fatalf("expected EXECUTED migration status, got: %s", migration.Status)
	}
}


package tests

import (
	"context"
	"testing"

	"distributed-transaction-engine/src/features/workflow-versioning/contracts"
	"distributed-transaction-engine/src/features/workflow-versioning/infrastructure/adapters"
	"distributed-transaction-engine/src/features/workflow-versioning/service"
	"distributed-transaction-engine/src/infra/observability"
)

func TestService_TracingPropagation(t *testing.T) {
	tracer := observability.NewTracer("workflow-versioning-test")
	repo := adapters.NewMemoryWorkflowVersioningRepositoryAdapter()
	svc := service.NewWorkflowVersioningService(service.ServiceDeps{Repo: repo})

	ctx := context.Background()
	ctx, span := tracer.StartSpan(ctx, "root_wv_test")
	defer span.End()

	bundle, err := svc.RegisterBundle(ctx, contracts.RegisterBundleCommand{
		TenantID:      "tenant_trace",
		WorkflowName:  "telemetry-flow",
		VersionTag:    "v1.0.0",
		BundlePayload: map[string]interface{}{"step": "ping"},
	})
	if err != nil {
		t.Fatalf("failed to register bundle: %v", err)
	}

	res, err := svc.VerifyBundleIntegrity(ctx, contracts.VerifyIntegrityQuery{
		TenantID: bundle.TenantID,
		BundleID: bundle.ID,
	})
	if err != nil || !res.IntegrityMatch {
		t.Fatalf("failed tracing verified integrity: %v", err)
	}
}


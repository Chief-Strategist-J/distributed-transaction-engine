package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	contracts "distributed-transaction-engine/src/features/compensation-engine/contracts"
	machines "distributed-transaction-engine/src/features/compensation-engine/domain/machines"
	adapters "distributed-transaction-engine/src/features/compensation-engine/infrastructure/adapters"
	service "distributed-transaction-engine/src/features/compensation-engine/service"
	"distributed-transaction-engine/src/infra/observability"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

func TestCompensationEngine_RealPostgresEndToEnd(t *testing.T) {
	db := getRealPostgresDB(t)
	defer db.Close()

	ctx := context.Background()
	tenantID := fmt.Sprintf("tenant_cmp_e2e_%d", time.Now().UnixNano())
	sagaID := uuid.New().String()
	planID := uuid.New().String()
	planPubID := fmt.Sprintf("cmp_%026d", time.Now().UnixNano()%10000000000)

	_, err := db.ExecContext(ctx, `
		INSERT INTO workflow_definitions (
			id, public_id, tenant_id, name, version, source, external_id,
			correlation_id, spec_hash, raw_spec, status, total_steps, created_at, updated_at
		) VALUES (
			$1, 'wfd_01HXYZ0123456789ABCDEF0123', $2, 'cmp-e2e-wf', 1, 'TEST', 'ext_cmp_e2e',
			'11111111-1111-4111-8111-111111111111', 'hash_real', '{"steps":[]}', 'ACTIVE', 2, NOW(), NOW()
		) ON CONFLICT DO NOTHING;
	`, sagaID, tenantID)
	if err != nil {
		t.Fatalf("failed to seed workflow definition: %v", err)
	}

	_, err = db.ExecContext(ctx, `
		INSERT INTO saga_instances (
			id, public_id, tenant_id, workflow_definition_id, workflow_version,
			spec_hash, source, external_id, input_hash, correlation_id,
			current_state, version, current_step_index, total_steps, execution_phase,
			pivot_reached, owner_epoch, deadline_at, initial_payload, final_output,
			created_at, updated_at
		) VALUES (
			$1, $2, $3, $1, 1,
			'hash_placeholder', 'SYSTEM', 'ext_init_cmp_e2e', 'in_hash', $4,
			'COMPENSATING', 1, 0, 1, 'FORWARD',
			false, 1, NOW() + INTERVAL '1 day', '{}', '{}',
			NOW(), NOW()
		)
	`, sagaID, fmt.Sprintf("seq_%026d", time.Now().Unix()%10000000000), tenantID, uuid.New().String())
	if err != nil {
		t.Fatalf("failed to seed saga instance: %v", err)
	}

	repo := adapters.NewPostgresCompensationRepositoryAdapter(db)
	dispatcher := adapters.NewMemoryCompensationDispatcherAdapter()
	machine := machines.NewCompensationStateMachine()
	tracer := observability.NewTracer("compensation-e2e-test")

	cmpSvc := service.NewCompensationService(service.CompensationServiceDeps{
		Repo:       repo,
		Dispatcher: dispatcher,
		Machine:    machine,
		Tracer:     tracer,
	})

	input := &contracts.TriggerCompensationInput{
		ID:       planID,
		PublicID: planPubID,
		TenantID: tenantID,
		SagaID:   sagaID,
		Reason:   "Downstream payment gateway permanent timeout",
		ForwardSteps: []contracts.ForwardStepExecutionRecord{
			{
				StepID:             "step_inventory_reserve",
				StepName:           "Reserve Inventory",
				IsPivot:            false,
				ForwardExecutionID: "exec_inv_101",
				ForwardOutcome:     "SUCCEEDED",
				ForwardOutput:      map[string]interface{}{"reservationId": "res_8899"},
				CompensationAction: "HTTP_RELEASE_INVENTORY",
				ExecutionOrder:     1,
			},
			{
				StepID:             "step_loyalty_deduct",
				StepName:           "Deduct Loyalty Points",
				IsPivot:            false,
				ForwardExecutionID: "exec_loy_202",
				ForwardOutcome:     "SUCCEEDED",
				ForwardOutput:      map[string]interface{}{"points": 500},
				CompensationAction: "HTTP_REFUND_LOYALTY",
				ExecutionOrder:     2,
			},
		},
	}

	plan, err := cmpSvc.TriggerCompensation(ctx, input)
	if err != nil {
		t.Fatalf("TriggerCompensation failed: %v", err)
	}
	if plan.CurrentState != contracts.PlanStateCompensating || len(plan.Steps) != 2 {
		t.Fatalf("unexpected plan state: %+v", plan)
	}

	if plan.Steps[0].StepID != "step_loyalty_deduct" {
		t.Fatalf("expected step_loyalty_deduct first in reverse plan, got %s", plan.Steps[0].StepID)
	}

	completeInput1 := &contracts.CompleteStepCompensationInput{
		TenantID:      tenantID,
		PlanID:        planID,
		StepID:        "step_loyalty_deduct",
		OutcomeStatus: contracts.StepStatusCompensated,
	}
	if err := cmpSvc.CompleteStepCompensation(ctx, completeInput1); err != nil {
		t.Fatalf("CompleteStepCompensation 1 failed: %v", err)
	}

	completeInput2 := &contracts.CompleteStepCompensationInput{
		TenantID:      tenantID,
		PlanID:        planID,
		StepID:        "step_inventory_reserve",
		OutcomeStatus: contracts.StepStatusCompensated,
	}
	if err := cmpSvc.CompleteStepCompensation(ctx, completeInput2); err != nil {
		t.Fatalf("CompleteStepCompensation 2 failed: %v", err)
	}

	finalPlan, err := cmpSvc.GetCompensationPlan(ctx, tenantID, sagaID)
	if err != nil {
		t.Fatalf("GetCompensationPlan failed: %v", err)
	}
	if finalPlan.CurrentState != contracts.PlanStateFullyCompensated || finalPlan.CompensatedSteps != 2 {
		t.Fatalf("expected FULLY_COMPENSATED with 2 compensated steps, got: %+v", finalPlan)
	}
}

package tests

import (
	"context"
	"fmt"
	"testing"

	transactionStateMachine "distributed-transaction-engine/src/features/transaction-state-machine"
	contracts "distributed-transaction-engine/src/features/transaction-state-machine/contracts"
)

func createTestSvc(t *testing.T) (*transactionStateMachine.Service, string, context.Context) {
	t.Helper()
	repo := transactionStateMachine.NewMemoryAdapter()
	svc := transactionStateMachine.NewService(repo)
	tenantID := "tenant_domain_scenarios"
	ctx := context.Background()
	return svc, tenantID, ctx
}

func initSaga(t *testing.T, svc *transactionStateMachine.Service, ctx context.Context, tenantID, sagaID, source, extID, corrID string) *contracts.SagaStateRecord {
	t.Helper()
	publicID := fmt.Sprintf("sag_000000000000000000%08d", len(sagaID))
	rec, err := svc.InitializeState(ctx, contracts.InitializeStateInput{
		ID:            sagaID,
		PublicID:      publicID,
		TenantID:      tenantID,
		Source:        source,
		ExternalID:    extID,
		CorrelationID: corrID,
	})
	if err != nil {
		t.Fatalf("failed to init saga %s: %v", sagaID, err)
	}
	return rec
}

func TestProblem1EcommerceCheckout(t *testing.T) {
	svc, tenantID, ctx := createTestSvc(t)
	sagaID := "018d9f1c-7201-7000-8000-000000001001"
	rec := initSaga(t, svc, ctx, tenantID, sagaID, "checkout", "order_fail_01", "018d9f1c-7201-7000-9000-000000001001")

	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "worker claimed checkout",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventStepSuccess,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "step 1: inventory reserved",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventStepSuccess,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "step 2: payment authorized",
	})
	rec, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventBusinessFailure,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "step 3: order creation failed in database",
	})
	if err != nil || rec.CurrentState != contracts.StateCompensating {
		t.Fatalf("expected COMPENSATING after order creation failure: %v", err)
	}
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventCompensationStepOK,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "step 2 undo: payment refunded",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventCompensationStepOK,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "step 1 undo: inventory released",
	})
	rec, err = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventCompensationDone,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "all steps compensated cleanly",
	})
	if err != nil || rec.CurrentState != contracts.StateCompensated {
		t.Fatalf("expected COMPENSATED: %v", err)
	}
}

func TestProblem2RideBookingConcurrentSeatLock(t *testing.T) {
	svc, tenantID, ctx := createTestSvc(t)
	sagaID1 := "018d9f1c-7201-7000-8000-000000001002"
	rec1 := initSaga(t, svc, ctx, tenantID, sagaID1, "ride-app", "customer_a_seat_4b", "018d9f1c-7201-7000-9000-000000001002")

	rec1, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID1, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: rec1.Version, FencingToken: 1, Reason: "customer A acquires lock",
	})
	rec1, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID1, Event: contracts.EventStepSuccess,
		ExpectedVersion: rec1.Version, FencingToken: 1, Reason: "seat 4B locked for customer A",
	})
	rec1, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID1, Event: contracts.EventDAGCompleted,
		ExpectedVersion: rec1.Version, FencingToken: 1, Reason: "payment captured & booking confirmed",
	})
	if rec1.CurrentState != contracts.StateCompleted {
		t.Fatalf("expected customer A booking COMPLETED: %v", rec1.CurrentState)
	}

	sagaID2 := "018d9f1c-7201-7000-8000-000000001003"
	rec2 := initSaga(t, svc, ctx, tenantID, sagaID2, "ride-app", "customer_b_seat_4b", "018d9f1c-7201-7000-9000-000000001003")

	rec2, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID2, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: rec2.Version, FencingToken: 1, Reason: "customer B worker claim",
	})
	rec2, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID2, Event: contracts.EventBusinessFailure,
		ExpectedVersion: rec2.Version, FencingToken: 1, Reason: "seat 4B already occupied: optimistic lock conflict",
	})
	if err != nil || rec2.CurrentState != contracts.StateCompensating {
		t.Fatalf("expected customer B transition to COMPENSATING: %v", err)
	}
	rec2, err = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID2, Event: contracts.EventCompensationDone,
		ExpectedVersion: rec2.Version, FencingToken: 1, Reason: "no side-effects executed, fast-abort",
	})
	if err != nil || rec2.CurrentState != contracts.StateCompensated {
		t.Fatalf("expected customer B COMPENSATED: %v", err)
	}
}

func TestProblem3HotelReservationPaymentOKRoomConfirmFails(t *testing.T) {
	svc, tenantID, ctx := createTestSvc(t)
	sagaID := "018d9f1c-7201-7000-8000-000000001004"
	rec := initSaga(t, svc, ctx, tenantID, sagaID, "hotel-broker", "booking_htl_301", "018d9f1c-7201-7000-9000-000000001004")

	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "claimed hold room",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventStepSuccess,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "room held",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventStepSuccess,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "payment charged $450",
	})
	rec, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventBusinessFailure,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "hotel PMS API rejected confirmation",
	})
	if err != nil || rec.CurrentState != contracts.StateCompensating {
		t.Fatalf("expected COMPENSATING: %v", err)
	}
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventCompensationStepOK,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "payment gateway refund issued for $450",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventCompensationStepOK,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "room hold released in PMS",
	})
	rec, err = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventCompensationDone,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "clean rollback verified",
	})
	if err != nil || rec.CurrentState != contracts.StateCompensated {
		t.Fatalf("expected COMPENSATED: %v", err)
	}
}

func TestProblem4MoneyTransferDebitOKCreditRecipientFails(t *testing.T) {
	svc, tenantID, ctx := createTestSvc(t)
	sagaID := "018d9f1c-7201-7000-8000-000000001005"
	rec := initSaga(t, svc, ctx, tenantID, sagaID, "core-banking", "wire_100k_eur", "018d9f1c-7201-7000-9000-000000001005")

	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "transfer initiated",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventStepSuccess,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "sender debited 100,000 EUR",
	})
	rec, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventBusinessFailure,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "recipient bank account closed/frozen",
	})
	if err != nil || rec.CurrentState != contracts.StateCompensating {
		t.Fatalf("expected COMPENSATING: %v", err)
	}
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventCompensationStepOK,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "credit back 100,000 EUR to sender account",
	})
	rec, err = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventCompensationDone,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "reversal complete",
	})
	if err != nil || rec.CurrentState != contracts.StateCompensated {
		t.Fatalf("expected COMPENSATED: %v", err)
	}
}

func TestProblem5SubscriptionActivationPaymentOKProvisioningFails(t *testing.T) {
	svc, tenantID, ctx := createTestSvc(t)
	sagaID := "018d9f1c-7201-7000-8000-000000001006"
	rec := initSaga(t, svc, ctx, tenantID, sagaID, "billing-saas", "sub_annual_pro", "018d9f1c-7201-7000-9000-000000001006")

	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "billing subscription claim",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventStepSuccess,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "credit card charged $1,200",
	})
	rec, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventBusinessFailure,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "tenant quota exceeded; account provisioning failed",
	})
	if err != nil || rec.CurrentState != contracts.StateCompensating {
		t.Fatalf("expected COMPENSATING: %v", err)
	}
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventCompensationStepOK,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "void charge and refund customer $1,200",
	})
	rec, err = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventCompensationDone,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "subscription rollback finalized",
	})
	if err != nil || rec.CurrentState != contracts.StateCompensated {
		t.Fatalf("expected COMPENSATED: %v", err)
	}
}

func TestProblem6FoodDeliveryPartnerAssignFailsAfterPayment(t *testing.T) {
	svc, tenantID, ctx := createTestSvc(t)
	sagaID := "018d9f1c-7201-7000-8000-000000001007"
	rec := initSaga(t, svc, ctx, tenantID, sagaID, "food-delivery", "order_pizza_882", "018d9f1c-7201-7000-9000-000000001007")

	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "claim order",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventStepSuccess,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "restaurant accepted & capacity held",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventStepSuccess,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "customer card charged $35",
	})
	rec, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventBusinessFailure,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "no delivery drivers available within 10km radius",
	})
	if err != nil || rec.CurrentState != contracts.StateCompensating {
		t.Fatalf("expected COMPENSATING: %v", err)
	}
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventCompensationStepOK,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "customer refund $35 executed",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventCompensationStepOK,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "cancel restaurant order and release kitchen capacity",
	})
	rec, err = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventCompensationDone,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "clean delivery rollback",
	})
	if err != nil || rec.CurrentState != contracts.StateCompensated {
		t.Fatalf("expected COMPENSATED: %v", err)
	}
}

func TestProblem7InventoryReplenishmentStockConfirmedUpdateFails(t *testing.T) {
	svc, tenantID, ctx := createTestSvc(t)
	sagaID := "018d9f1c-7201-7000-8000-000000001008"
	rec := initSaga(t, svc, ctx, tenantID, sagaID, "erp-supply-chain", "po_sku_9091", "018d9f1c-7201-7000-9000-000000001008")

	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "po workflow initiated",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventStepSuccess,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "purchase order created",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventStepSuccess,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "supplier confirmed stock allocation",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventStepSuccess,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "wire payment approved to supplier",
	})
	rec, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventBusinessFailure,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "warehouse ERP system down; inventory record update rejected",
	})
	if err != nil || rec.CurrentState != contracts.StateCompensating {
		t.Fatalf("expected COMPENSATING: %v", err)
	}
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventCompensationStepOK,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "recall wire transfer from supplier",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventCompensationStepOK,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "cancel supplier stock allocation",
	})
	rec, err = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventCompensationDone,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "ERP rollback finalized",
	})
	if err != nil || rec.CurrentState != contracts.StateCompensated {
		t.Fatalf("expected COMPENSATED: %v", err)
	}
}

func TestProblem8TravelBookingFlightOKHotelFails(t *testing.T) {
	svc, tenantID, ctx := createTestSvc(t)
	sagaID := "018d9f1c-7201-7000-8000-000000001009"
	rec := initSaga(t, svc, ctx, tenantID, sagaID, "travel-agency", "itinerary_lon_nyc", "018d9f1c-7201-7000-9000-000000001009")

	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "travel booking claim",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventStepSuccess,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "British Airways flight confirmed",
	})
	rec, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventBusinessFailure,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "Manhattan Hotel has no vacancy",
	})
	if err != nil || rec.CurrentState != contracts.StateCompensating {
		t.Fatalf("expected COMPENSATING: %v", err)
	}
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventCompensationStepOK,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "cancel BA flight seat within free-cancellation grace window",
	})
	rec, err = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventCompensationDone,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "travel rollback complete",
	})
	if err != nil || rec.CurrentState != contracts.StateCompensated {
		t.Fatalf("expected COMPENSATED: %v", err)
	}
}

func TestProblem9EmployeeOnboardingPartialSystemFailure(t *testing.T) {
	svc, tenantID, ctx := createTestSvc(t)
	sagaID := "018d9f1c-7201-7000-8000-000000001010"
	rec := initSaga(t, svc, ctx, tenantID, sagaID, "hr-portal", "hire_emp_8820", "018d9f1c-7201-7000-9000-000000001010")

	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "hr onboarding workflow claim",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventStepSuccess,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "Okta identity created",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventStepSuccess,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "ActiveDirectory permissions assigned",
	})
	rec, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventBusinessFailure,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "Workday payroll record rejected: invalid tax ID",
	})
	if err != nil || rec.CurrentState != contracts.StateCompensating {
		t.Fatalf("expected COMPENSATING: %v", err)
	}
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventCompensationStepOK,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "revoke ActiveDirectory permissions",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventCompensationStepOK,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "disable and delete Okta identity",
	})
	rec, err = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventCompensationDone,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "deprovisioning completed cleanly",
	})
	if err != nil || rec.CurrentState != contracts.StateCompensated {
		t.Fatalf("expected COMPENSATED: %v", err)
	}
}

func TestProblem10CloudResourceProvisioningPartialAllocation(t *testing.T) {
	svc, tenantID, ctx := createTestSvc(t)
	sagaID := "018d9f1c-7201-7000-8000-000000001011"
	rec := initSaga(t, svc, ctx, tenantID, sagaID, "cloud-control-plane", "k8s_cluster_alpha", "018d9f1c-7201-7000-9000-000000001011")

	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventOrchestratorClaim,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "cloud control plane claim",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventStepSuccess,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "VPC networking configured",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventStepSuccess,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "Compute VMs allocated",
	})
	rec, err := svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventBusinessFailure,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "SAN storage attachment timed out: SAN zone saturated",
	})
	if err != nil || rec.CurrentState != contracts.StateCompensating {
		t.Fatalf("expected COMPENSATING: %v", err)
	}
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventCompensationStepOK,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "tear down Compute VMs",
	})
	rec, _ = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventCompensationStepOK,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "release VPC subnet and routing table",
	})
	rec, err = svc.TransitionState(ctx, contracts.TransitionStateInput{
		TenantID: tenantID, SagaID: sagaID, Event: contracts.EventCompensationDone,
		ExpectedVersion: rec.Version, FencingToken: 1, Reason: "all cloud infrastructure resources deallocated",
	})
	if err != nil || rec.CurrentState != contracts.StateCompensated {
		t.Fatalf("expected COMPENSATED: %v", err)
	}
}

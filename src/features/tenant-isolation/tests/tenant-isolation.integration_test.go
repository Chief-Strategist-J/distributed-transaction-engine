package tests

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	tenantisolation "distributed-transaction-engine/src/features/tenant-isolation"
	contracts "distributed-transaction-engine/src/features/tenant-isolation/contracts"
	"distributed-transaction-engine/src/features/tenant-isolation/infrastructure/adapters"
	"distributed-transaction-engine/src/infra/observability"
	"github.com/google/uuid"
)

func newMemoryServices() (tenantisolation.Services, *adapters.MemoryTenantIsolationRepositoryAdapter) {
	memory := tenantisolation.NewMemoryAdapter()
	runtime := tenantisolation.NewSystemRuntimeAdapter()
	services := tenantisolation.NewServices(tenantisolation.ServicesDeps{
		Registry: memory, Quota: memory, Quarantine: memory, Security: memory, Clock: runtime, IDs: runtime,
		Tracer: observability.NewTracer("tenant-isolation-test"),
	})
	return services, memory
}

func registerTenant(t *testing.T, services tenantisolation.Services, tenantID string, maxSagas int) contracts.TenantRecord {
	t.Helper()
	record, err := services.Registry.CreateTenant(context.Background(), contracts.CreateTenantInput{
		TenantID: tenantID, MaxConcurrentSagas: maxSagas, CorrelationID: uuid.New(),
	})
	if err != nil {
		t.Fatalf("register %s: %v", tenantID, err)
	}
	return record
}

func TestHeaderTenantCannotOverrideCredentialTenant(t *testing.T) {
	services, memory := newMemoryServices()
	registerTenant(t, services, "tenant-a", 5)
	registerTenant(t, services, "tenant-b", 5)
	requested := "tenant-b"
	_, err := services.Access.EvaluateTenantResolution(context.Background(), contracts.ResolveTenantInput{
		PrincipalTenantID: "tenant-a", RequestedTenantID: &requested, CorrelationID: uuid.New(),
	})
	if statusCodeOf(err) != http.StatusNotFound {
		t.Fatalf("selecting a foreign tenant must answer 404, got %v", err)
	}
	events := memory.ListSecurityEvents()
	if len(events) != 1 || events[0].EventType != contracts.SecurityEventTenantSelectionRejected || events[0].ActorTenantID != "tenant-a" {
		t.Fatalf("expected one TENANT_SELECTION_REJECTED event for tenant-a, got %+v", events)
	}
}

func TestVerifiedMembershipSelectionSucceeds(t *testing.T) {
	services, memory := newMemoryServices()
	requested := "tenant-b"
	resolved, err := services.Access.EvaluateTenantResolution(context.Background(), contracts.ResolveTenantInput{
		PrincipalTenantID: "tenant-a", VerifiedMemberships: []string{"tenant-b"}, RequestedTenantID: &requested, CorrelationID: uuid.New(),
	})
	if err != nil || resolved.TenantID != "tenant-b" || len(memory.ListSecurityEvents()) != 0 {
		t.Fatalf("verified membership must resolve without a security event: %v %v", resolved, err)
	}
}

func TestCrossTenantReferenceAnswersNotFoundAndRecords(t *testing.T) {
	services, memory := newMemoryServices()
	registerTenant(t, services, "tenant-a", 5)
	resourceID := "saga-of-tenant-b"
	err := services.Access.EvaluateCrossTenantReference(context.Background(), contracts.CrossTenantReferenceInput{
		ActorTenantID: "tenant-a", ReferencedTenantID: "tenant-b", ResourceKind: "saga", ResourceID: &resourceID, CorrelationID: uuid.New(),
	})
	if statusCodeOf(err) != http.StatusNotFound || err.Error() != "resource not found" {
		t.Fatalf("cross tenant reference must answer a bare 404, got %v", err)
	}
	if events := memory.ListSecurityEvents(); len(events) != 1 || events[0].EventType != contracts.SecurityEventCrossTenantReference {
		t.Fatalf("expected one CROSS_TENANT_REFERENCE event, got %+v", events)
	}
	sameTenant := services.Access.EvaluateCrossTenantReference(context.Background(), contracts.CrossTenantReferenceInput{
		ActorTenantID: "tenant-a", ReferencedTenantID: "tenant-a", ResourceKind: "saga", CorrelationID: uuid.New(),
	})
	if sameTenant != nil {
		t.Fatalf("same tenant reference must pass, got %v", sameTenant)
	}
}

func TestCrossTenantReferenceSurvivesSecurityEventFailure(t *testing.T) {
	services, _ := newMemoryServices()
	err := services.Access.EvaluateCrossTenantReference(context.Background(), contracts.CrossTenantReferenceInput{
		ActorTenantID: "tenant-unregistered", ReferencedTenantID: "tenant-b", ResourceKind: "saga", CorrelationID: uuid.New(),
	})
	if statusCodeOf(err) != http.StatusNotFound {
		t.Fatalf("a failed security event write must not change the 404 answer, got %v", err)
	}
}

func TestQuotaAdmitsExactlyTheLimitUnderConcurrency(t *testing.T) {
	services, _ := newMemoryServices()
	registerTenant(t, services, "tenant-a", 10)
	var admitted, refused atomic.Int64
	var group sync.WaitGroup
	for worker := 0; worker < 20; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := services.Quota.CreateSagaQuotaReservation(context.Background(), "tenant-a")
			if err == nil {
				admitted.Add(1)
				return
			}
			if statusCodeOf(err) == http.StatusTooManyRequests {
				refused.Add(1)
			}
		}()
	}
	group.Wait()
	if admitted.Load() != 10 || refused.Load() != 10 {
		t.Fatalf("admitted %d refused %d, want 10 and 10", admitted.Load(), refused.Load())
	}
}

func TestQuotaReleaseReopensCapacityAndRejectsUnderflow(t *testing.T) {
	services, _ := newMemoryServices()
	registerTenant(t, services, "tenant-a", 1)
	if _, err := services.Quota.CreateSagaQuotaReservation(context.Background(), "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := services.Quota.CreateSagaQuotaReservation(context.Background(), "tenant-a"); statusCodeOf(err) != http.StatusTooManyRequests {
		t.Fatalf("second reservation must be refused, got %v", err)
	}
	if err := services.Quota.DeleteSagaQuotaReservation(context.Background(), "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if err := services.Quota.DeleteSagaQuotaReservation(context.Background(), "tenant-a"); statusCodeOf(err) != http.StatusConflict {
		t.Fatalf("release without reservation must conflict, got %v", err)
	}
	if _, err := services.Quota.CreateSagaQuotaReservation(context.Background(), "tenant-a"); err != nil {
		t.Fatalf("capacity must reopen after release, got %v", err)
	}
}

func TestQuotaReconciliationRepairsDriftWithVersionGuard(t *testing.T) {
	services, _ := newMemoryServices()
	registerTenant(t, services, "tenant-a", 5)
	for reservation := 0; reservation < 3; reservation++ {
		if _, err := services.Quota.CreateSagaQuotaReservation(context.Background(), "tenant-a"); err != nil {
			t.Fatal(err)
		}
	}
	quota, err := services.Quota.GetSagaQuota(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	repaired, err := services.Quota.UpdateSagaQuotaCounter(context.Background(), contracts.UpdateSagaQuotaCounterInput{
		TenantID: "tenant-a", ExpectedVersion: quota.Version, ObservedActiveSagas: 1,
	})
	if err != nil || repaired.ActiveSagaCount != 1 {
		t.Fatalf("expected counter repaired to 1, got %+v %v", repaired, err)
	}
	_, err = services.Quota.UpdateSagaQuotaCounter(context.Background(), contracts.UpdateSagaQuotaCounterInput{
		TenantID: "tenant-a", ExpectedVersion: quota.Version, ObservedActiveSagas: 0,
	})
	if statusCodeOf(err) != http.StatusPreconditionFailed {
		t.Fatalf("stale version must be refused with 412, got %v", err)
	}
}

func TestSuspendedBillingRejectsNewCommandsButKeepsRefundsFlowing(t *testing.T) {
	services, _ := newMemoryServices()
	record := registerTenant(t, services, "tenant-a", 5)
	billing := contracts.SuspensionReasonBilling
	if _, err := services.Registry.UpdateTenantStatus(context.Background(), contracts.UpdateTenantStatusInput{
		TenantID: "tenant-a", ExpectedVersion: record.Version, TargetStatus: contracts.TenantStatusSuspended, SuspensionReason: &billing,
	}); err != nil {
		t.Fatal(err)
	}
	_, err := services.Access.EvaluateCommandAdmission(context.Background(), contracts.EvaluateAdmissionInput{TenantID: "tenant-a", Kind: contracts.CommandKindNewCommand})
	if statusCodeOf(err) != http.StatusForbidden {
		t.Fatalf("new command on a suspended tenant must be 403, got %v", err)
	}
	for _, kind := range []contracts.CommandKind{contracts.CommandKindInFlightContinue, contracts.CommandKindMoneyOwedToTenant} {
		decision, evalErr := services.Access.EvaluateCommandAdmission(context.Background(), contracts.EvaluateAdmissionInput{TenantID: "tenant-a", Kind: kind})
		if evalErr != nil || decision.Action != contracts.AdmissionActionAdmit {
			t.Fatalf("%s must continue under billing suspension, got %+v %v", kind, decision, evalErr)
		}
	}
	if _, err = services.Quota.CreateSagaQuotaReservation(context.Background(), "tenant-a"); statusCodeOf(err) != http.StatusForbidden {
		t.Fatalf("a suspended tenant must not reserve saga capacity, got %v", err)
	}
}

func TestAdmissionForUnknownTenantIsNotFound(t *testing.T) {
	services, _ := newMemoryServices()
	_, err := services.Access.EvaluateCommandAdmission(context.Background(), contracts.EvaluateAdmissionInput{TenantID: "tenant-ghost", Kind: contracts.CommandKindNewCommand})
	if statusCodeOf(err) != http.StatusNotFound {
		t.Fatalf("unknown tenant must be 404, got %v", err)
	}
}

func TestOffboardedTenantIdIsNeverReissued(t *testing.T) {
	services, _ := newMemoryServices()
	record := registerTenant(t, services, "tenant-a", 5)
	steps := []contracts.TenantStatus{contracts.TenantStatusOffboarding, contracts.TenantStatusOffboarded}
	version := record.Version
	for _, target := range steps {
		updated, err := services.Registry.UpdateTenantStatus(context.Background(), contracts.UpdateTenantStatusInput{TenantID: "tenant-a", ExpectedVersion: version, TargetStatus: target})
		if err != nil {
			t.Fatalf("transition to %s: %v", target, err)
		}
		version = updated.Version
	}
	_, err := services.Registry.CreateTenant(context.Background(), contracts.CreateTenantInput{TenantID: "tenant-a", MaxConcurrentSagas: 5, CorrelationID: uuid.New()})
	if statusCodeOf(err) != http.StatusConflict {
		t.Fatalf("an offboarded tenant id must never be reissued, got %v", err)
	}
}

func TestStatusUpdateRejectsIllegalTransitionAndStaleVersion(t *testing.T) {
	services, _ := newMemoryServices()
	record := registerTenant(t, services, "tenant-a", 5)
	_, err := services.Registry.UpdateTenantStatus(context.Background(), contracts.UpdateTenantStatusInput{TenantID: "tenant-a", ExpectedVersion: record.Version, TargetStatus: contracts.TenantStatusOffboarded})
	if statusCodeOf(err) != http.StatusConflict {
		t.Fatalf("ACTIVE -> OFFBOARDED must conflict, got %v", err)
	}
	billing := contracts.SuspensionReasonBilling
	_, err = services.Registry.UpdateTenantStatus(context.Background(), contracts.UpdateTenantStatusInput{TenantID: "tenant-a", ExpectedVersion: record.Version + 1, TargetStatus: contracts.TenantStatusSuspended, SuspensionReason: &billing})
	if statusCodeOf(err) != http.StatusPreconditionFailed {
		t.Fatalf("stale version must be 412, got %v", err)
	}
}

func TestDuplicateTenantRegistrationConflicts(t *testing.T) {
	services, _ := newMemoryServices()
	registerTenant(t, services, "tenant-a", 5)
	_, err := services.Registry.CreateTenant(context.Background(), contracts.CreateTenantInput{TenantID: "tenant-a", MaxConcurrentSagas: 5, CorrelationID: uuid.New()})
	if statusCodeOf(err) != http.StatusConflict {
		t.Fatalf("duplicate registration must conflict, got %v", err)
	}
}

func envelopeForTenant(tenantID string, offset int64) contracts.MessageTenantEnvelope {
	envelope := messageEnvelope(&tenantID, &tenantID, &tenantID, nil)
	envelope.SourceOffset = offset
	return envelope
}

func TestMismatchedMessageIsQuarantinedWithAlertNeverDropped(t *testing.T) {
	services, memory := newMemoryServices()
	registerTenant(t, services, "tenant-a", 5)
	header, payload := "tenant-a", "tenant-b"
	envelope := messageEnvelope(&header, &header, &payload, nil)
	decision, err := services.Quarantine.EvaluateMessageTenant(context.Background(), envelope)
	if err != nil || decision.Proceed || *decision.QuarantineReason != contracts.QuarantineReasonIntegrityMismatch {
		t.Fatalf("expected integrity quarantine, got %+v %v", decision, err)
	}
	pending, err := services.Quarantine.ListQuarantinedMessages(context.Background(), contracts.QuarantineStatusPending, 10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("expected one pending quarantine record, got %d %v", len(pending), err)
	}
	if events := memory.ListSecurityEvents(); len(events) != 1 || events[0].EventType != contracts.SecurityEventMessageIntegrityMismatch {
		t.Fatalf("expected an integrity security event, got %+v", events)
	}
}

func TestQuarantineIsIdempotentOnRedelivery(t *testing.T) {
	services, _ := newMemoryServices()
	for attempt := 0; attempt < 3; attempt++ {
		if _, err := services.Quarantine.EvaluateMessageTenant(context.Background(), envelopeForTenant("tenant-early", 7)); err != nil {
			t.Fatal(err)
		}
	}
	pending, _ := services.Quarantine.ListQuarantinedMessages(context.Background(), contracts.QuarantineStatusPending, 10)
	if len(pending) != 1 {
		t.Fatalf("redelivery must not create duplicates, got %d", len(pending))
	}
}

func TestUnknownTenantMessageIsReleasedAfterRegistration(t *testing.T) {
	services, _ := newMemoryServices()
	decision, err := services.Quarantine.EvaluateMessageTenant(context.Background(), envelopeForTenant("tenant-early", 1))
	if err != nil || decision.Proceed || *decision.QuarantineReason != contracts.QuarantineReasonUnknownTenant {
		t.Fatalf("expected unknown tenant quarantine, got %+v %v", decision, err)
	}
	_, err = services.Quarantine.UpdateQuarantinedMessagesRelease(context.Background(), contracts.ReleaseQuarantinedMessagesInput{TenantID: "tenant-early"})
	if statusCodeOf(err) != http.StatusConflict {
		t.Fatalf("release before registration must conflict, got %v", err)
	}
	registerTenant(t, services, "tenant-early", 5)
	released, err := services.Quarantine.UpdateQuarantinedMessagesRelease(context.Background(), contracts.ReleaseQuarantinedMessagesInput{TenantID: "tenant-early"})
	if err != nil || len(released) != 1 || released[0].Status != contracts.QuarantineStatusReleased || released[0].ResolvedAt == nil {
		t.Fatalf("expected one released record, got %+v %v", released, err)
	}
	again, _ := services.Quarantine.UpdateQuarantinedMessagesRelease(context.Background(), contracts.ReleaseQuarantinedMessagesInput{TenantID: "tenant-early"})
	if len(again) != 0 {
		t.Fatalf("a released record must not be released twice, got %d", len(again))
	}
	decision, err = services.Quarantine.EvaluateMessageTenant(context.Background(), envelopeForTenant("tenant-early", 2))
	if err != nil || !decision.Proceed {
		t.Fatalf("registered tenant message must proceed, got %+v %v", decision, err)
	}
}

func TestOffboardedTenantMessageIsQuarantined(t *testing.T) {
	services, _ := newMemoryServices()
	record := registerTenant(t, services, "tenant-a", 5)
	version := record.Version
	for _, target := range []contracts.TenantStatus{contracts.TenantStatusOffboarding, contracts.TenantStatusOffboarded} {
		updated, err := services.Registry.UpdateTenantStatus(context.Background(), contracts.UpdateTenantStatusInput{TenantID: "tenant-a", ExpectedVersion: version, TargetStatus: target})
		if err != nil {
			t.Fatal(err)
		}
		version = updated.Version
	}
	decision, err := services.Quarantine.EvaluateMessageTenant(context.Background(), envelopeForTenant("tenant-a", 9))
	if err != nil || decision.Proceed || *decision.QuarantineReason != contracts.QuarantineReasonOffboardedTenant {
		t.Fatalf("expected offboarded quarantine, got %+v %v", decision, err)
	}
	if _, err = services.Quarantine.UpdateQuarantinedMessagesRelease(context.Background(), contracts.ReleaseQuarantinedMessagesInput{TenantID: "tenant-a"}); statusCodeOf(err) != http.StatusConflict {
		t.Fatalf("offboarded tenant must never release parked messages, got %v", err)
	}
}

func TestHostileTenantFieldsAreQuarantinedNotRejected(t *testing.T) {
	services, _ := newMemoryServices()
	hostile := "Not A Tenant \n"
	envelope := messageEnvelope(&hostile, &hostile, &hostile, nil)
	decision, err := services.Quarantine.EvaluateMessageTenant(context.Background(), envelope)
	if err != nil || decision.Proceed {
		t.Fatalf("hostile tenant must be quarantined, got %+v %v", decision, err)
	}
}

func TestWorkerLaneAlternatingTenantsNeverBleeds(t *testing.T) {
	services, _ := newMemoryServices()
	registerTenant(t, services, "tenant-a", 3)
	registerTenant(t, services, "tenant-b", 1)
	for round := 0; round < 3; round++ {
		if _, err := services.Quota.CreateSagaQuotaReservation(context.Background(), "tenant-a"); err != nil {
			t.Fatalf("tenant-a round %d: %v", round, err)
		}
		if round == 0 {
			if _, err := services.Quota.CreateSagaQuotaReservation(context.Background(), "tenant-b"); err != nil {
				t.Fatalf("tenant-b round %d: %v", round, err)
			}
			continue
		}
		if _, err := services.Quota.CreateSagaQuotaReservation(context.Background(), "tenant-b"); statusCodeOf(err) != http.StatusTooManyRequests {
			t.Fatalf("tenant-b must stay at its own limit, got %v", err)
		}
	}
	quotaA, _ := services.Quota.GetSagaQuota(context.Background(), "tenant-a")
	quotaB, _ := services.Quota.GetSagaQuota(context.Background(), "tenant-b")
	if quotaA.ActiveSagaCount != 3 || quotaB.ActiveSagaCount != 1 {
		t.Fatalf("counters bled across tenants: a=%d b=%d", quotaA.ActiveSagaCount, quotaB.ActiveSagaCount)
	}
}

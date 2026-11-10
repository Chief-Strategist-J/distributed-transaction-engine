package tests

import (
	"net/http"
	"strings"
	"testing"
	"time"

	contracts "distributed-transaction-engine/src/features/tenant-isolation/contracts"
	"distributed-transaction-engine/src/features/tenant-isolation/domain/rules"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
)

func stringPointer(value string) *string {
	return &value
}

func reasonPointer(value contracts.SuspensionReason) *contracts.SuspensionReason {
	return &value
}

func statusPointer(value contracts.TenantStatus) *contracts.TenantStatus {
	return &value
}

func statusCodeOf(err error) int {
	applicationError, ok := err.(*appErrors.ApplicationError)
	if !ok {
		return 0
	}
	return applicationError.StatusCode
}

func TestResolveTenantPartition(t *testing.T) {
	cases := []struct {
		name       string
		input      contracts.ResolveTenantInput
		wantTenant string
		wantStatus int
	}{
		{"no selection uses principal tenant", contracts.ResolveTenantInput{PrincipalTenantID: "tenant-a"}, "tenant-a", 0},
		{"selection equal to principal tenant", contracts.ResolveTenantInput{PrincipalTenantID: "tenant-a", RequestedTenantID: stringPointer("tenant-a")}, "tenant-a", 0},
		{"selection among verified memberships", contracts.ResolveTenantInput{PrincipalTenantID: "tenant-a", VerifiedMemberships: []string{"tenant-b"}, RequestedTenantID: stringPointer("tenant-b")}, "tenant-b", 0},
		{"header selects a tenant the principal does not belong to", contracts.ResolveTenantInput{PrincipalTenantID: "tenant-a", RequestedTenantID: stringPointer("tenant-b")}, "", http.StatusNotFound},
		{"near miss prefix is not a membership", contracts.ResolveTenantInput{PrincipalTenantID: "tenant-a", VerifiedMemberships: []string{"tenant-bb"}, RequestedTenantID: stringPointer("tenant-b")}, "", http.StatusNotFound},
		{"principal without tenant fails closed", contracts.ResolveTenantInput{}, "", http.StatusForbidden},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resolved, err := rules.ResolveTenant(tc.input)
			if statusCodeOf(err) != tc.wantStatus || resolved.TenantID != tc.wantTenant {
				t.Fatalf("got tenant %q status %d, want tenant %q status %d", resolved.TenantID, statusCodeOf(err), tc.wantTenant, tc.wantStatus)
			}
		})
	}
}

func TestEvaluateAdmissionTableIsExhaustiveAndExact(t *testing.T) {
	kinds := []contracts.CommandKind{
		contracts.CommandKindNewCommand, contracts.CommandKindInFlightContinue, contracts.CommandKindMoneyOwedToTenant,
		contracts.CommandKindScheduledJob, contracts.CommandKindPartnerInboundEvent,
	}
	scopes := []struct {
		status contracts.TenantStatus
		reason *contracts.SuspensionReason
		want   map[contracts.CommandKind]contracts.AdmissionAction
	}{
		{contracts.TenantStatusActive, nil, map[contracts.CommandKind]contracts.AdmissionAction{
			contracts.CommandKindNewCommand: contracts.AdmissionActionAdmit, contracts.CommandKindInFlightContinue: contracts.AdmissionActionAdmit,
			contracts.CommandKindMoneyOwedToTenant: contracts.AdmissionActionAdmit, contracts.CommandKindScheduledJob: contracts.AdmissionActionAdmit,
			contracts.CommandKindPartnerInboundEvent: contracts.AdmissionActionAdmit}},
		{contracts.TenantStatusSuspended, reasonPointer(contracts.SuspensionReasonBilling), map[contracts.CommandKind]contracts.AdmissionAction{
			contracts.CommandKindNewCommand: contracts.AdmissionActionReject, contracts.CommandKindInFlightContinue: contracts.AdmissionActionAdmit,
			contracts.CommandKindMoneyOwedToTenant: contracts.AdmissionActionAdmit, contracts.CommandKindScheduledJob: contracts.AdmissionActionPause,
			contracts.CommandKindPartnerInboundEvent: contracts.AdmissionActionAdmit}},
		{contracts.TenantStatusSuspended, reasonPointer(contracts.SuspensionReasonAbuse), map[contracts.CommandKind]contracts.AdmissionAction{
			contracts.CommandKindNewCommand: contracts.AdmissionActionReject, contracts.CommandKindInFlightContinue: contracts.AdmissionActionPause,
			contracts.CommandKindMoneyOwedToTenant: contracts.AdmissionActionAdmit, contracts.CommandKindScheduledJob: contracts.AdmissionActionPause,
			contracts.CommandKindPartnerInboundEvent: contracts.AdmissionActionPark}},
		{contracts.TenantStatusOffboarding, nil, map[contracts.CommandKind]contracts.AdmissionAction{
			contracts.CommandKindNewCommand: contracts.AdmissionActionReject, contracts.CommandKindInFlightContinue: contracts.AdmissionActionAdmit,
			contracts.CommandKindMoneyOwedToTenant: contracts.AdmissionActionAdmit, contracts.CommandKindScheduledJob: contracts.AdmissionActionPause,
			contracts.CommandKindPartnerInboundEvent: contracts.AdmissionActionPark}},
		{contracts.TenantStatusOffboarded, nil, map[contracts.CommandKind]contracts.AdmissionAction{
			contracts.CommandKindNewCommand: contracts.AdmissionActionReject, contracts.CommandKindInFlightContinue: contracts.AdmissionActionReject,
			contracts.CommandKindMoneyOwedToTenant: contracts.AdmissionActionReject, contracts.CommandKindScheduledJob: contracts.AdmissionActionReject,
			contracts.CommandKindPartnerInboundEvent: contracts.AdmissionActionQuarantine}},
	}
	for _, scope := range scopes {
		for _, kind := range kinds {
			decision, err := rules.EvaluateAdmission(scope.status, scope.reason, kind)
			if err != nil || decision.Action != scope.want[kind] {
				t.Fatalf("status %s reason %v kind %s: got %v err %v, want %s", scope.status, scope.reason, kind, decision.Action, err, scope.want[kind])
			}
			if decision.Action != contracts.AdmissionActionAdmit && decision.ReasonCode == "" {
				t.Fatalf("non-admit decision for %s/%s must carry a reason code", scope.status, kind)
			}
		}
	}
}

func TestEvaluateAdmissionFailsLoudlyOnUnmappedInput(t *testing.T) {
	if _, err := rules.EvaluateAdmission(contracts.TenantStatusSuspended, nil, contracts.CommandKindNewCommand); statusCodeOf(err) != http.StatusInternalServerError {
		t.Fatalf("suspended without reason must fail loudly, got %v", err)
	}
	if _, err := rules.EvaluateAdmission(contracts.TenantStatusActive, reasonPointer(contracts.SuspensionReasonAbuse), contracts.CommandKindNewCommand); statusCodeOf(err) != http.StatusInternalServerError {
		t.Fatalf("active with a reason must fail loudly, got %v", err)
	}
	if _, err := rules.EvaluateAdmission(contracts.TenantStatusActive, nil, contracts.CommandKind("UNKNOWN")); statusCodeOf(err) != http.StatusInternalServerError {
		t.Fatalf("unknown kind must fail loudly, got %v", err)
	}
}

func TestRejectionErrorOnlyForRejectDecisions(t *testing.T) {
	for _, action := range []contracts.AdmissionAction{contracts.AdmissionActionAdmit, contracts.AdmissionActionPause, contracts.AdmissionActionPark, contracts.AdmissionActionQuarantine} {
		if err := rules.RejectionError(contracts.AdmissionDecision{Action: action}); err != nil {
			t.Fatalf("action %s must not produce an error", action)
		}
	}
	err := rules.RejectionError(contracts.AdmissionDecision{Action: contracts.AdmissionActionReject, ReasonCode: "TENANT_SUSPENDED_BILLING"})
	if statusCodeOf(err) != http.StatusForbidden {
		t.Fatalf("reject must map to 403, got %v", err)
	}
}

func messageEnvelope(envelope, key, payload, aggregate *string) contracts.MessageTenantEnvelope {
	return contracts.MessageTenantEnvelope{
		SourceTopic: "saga.events", EnvelopeTenantID: envelope, KeyTenantID: key, PayloadTenantID: payload, AggregateTenantID: aggregate,
		PayloadSHA256: strings.Repeat("a", 64), CorrelationID: uuid.New(),
	}
}

func TestEvaluateMessageTenantPartition(t *testing.T) {
	active, offboarded := statusPointer(contracts.TenantStatusActive), statusPointer(contracts.TenantStatusOffboarded)
	cases := []struct {
		name       string
		envelope   contracts.MessageTenantEnvelope
		status     *contracts.TenantStatus
		wantReason *contracts.QuarantineReason
	}{
		{"all agree and tenant active", messageEnvelope(stringPointer("tenant-a"), stringPointer("tenant-a"), stringPointer("tenant-a"), stringPointer("tenant-a")), active, nil},
		{"new aggregate has no owner yet", messageEnvelope(stringPointer("tenant-a"), stringPointer("tenant-a"), stringPointer("tenant-a"), nil), active, nil},
		{"header differs from payload", messageEnvelope(stringPointer("tenant-a"), stringPointer("tenant-a"), stringPointer("tenant-b"), nil), active, quarantineReasonPointer(contracts.QuarantineReasonIntegrityMismatch)},
		{"header differs from key", messageEnvelope(stringPointer("tenant-a"), stringPointer("tenant-b"), stringPointer("tenant-a"), nil), active, quarantineReasonPointer(contracts.QuarantineReasonIntegrityMismatch)},
		{"header differs from aggregate owner", messageEnvelope(stringPointer("tenant-a"), stringPointer("tenant-a"), stringPointer("tenant-a"), stringPointer("tenant-b")), active, quarantineReasonPointer(contracts.QuarantineReasonIntegrityMismatch)},
		{"missing envelope tenant", messageEnvelope(nil, stringPointer("tenant-a"), stringPointer("tenant-a"), nil), active, quarantineReasonPointer(contracts.QuarantineReasonIntegrityMismatch)},
		{"missing payload tenant", messageEnvelope(stringPointer("tenant-a"), stringPointer("tenant-a"), nil, nil), active, quarantineReasonPointer(contracts.QuarantineReasonIntegrityMismatch)},
		{"non canonical envelope tenant", messageEnvelope(stringPointer("Tenant-A"), stringPointer("Tenant-A"), stringPointer("Tenant-A"), nil), nil, quarantineReasonPointer(contracts.QuarantineReasonIntegrityMismatch)},
		{"consistent but unknown tenant", messageEnvelope(stringPointer("tenant-a"), stringPointer("tenant-a"), stringPointer("tenant-a"), nil), nil, quarantineReasonPointer(contracts.QuarantineReasonUnknownTenant)},
		{"consistent but offboarded tenant", messageEnvelope(stringPointer("tenant-a"), stringPointer("tenant-a"), stringPointer("tenant-a"), nil), offboarded, quarantineReasonPointer(contracts.QuarantineReasonOffboardedTenant)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			decision := rules.EvaluateMessageTenant(tc.envelope, tc.status)
			if decision.Proceed != (tc.wantReason == nil) {
				t.Fatalf("proceed = %v, want %v", decision.Proceed, tc.wantReason == nil)
			}
			if tc.wantReason != nil && *decision.QuarantineReason != *tc.wantReason {
				t.Fatalf("reason = %s, want %s", *decision.QuarantineReason, *tc.wantReason)
			}
		})
	}
}

func quarantineReasonPointer(value contracts.QuarantineReason) *contracts.QuarantineReason {
	return &value
}

func TestBoundTenantIDForStorage(t *testing.T) {
	cases := []struct {
		name  string
		input *string
		keep  bool
	}{
		{"nil", nil, false},
		{"empty", stringPointer(""), false},
		{"padded", stringPointer(" tenant-a"), false},
		{"exactly 64", stringPointer(strings.Repeat("a", 64)), true},
		{"65 characters", stringPointer(strings.Repeat("a", 65)), false},
		{"canonical", stringPointer("tenant-a"), true},
	}
	for _, tc := range cases {
		if kept := rules.BoundTenantIDForStorage(tc.input) != nil; kept != tc.keep {
			t.Fatalf("%s: kept = %v, want %v", tc.name, kept, tc.keep)
		}
	}
}

func TestEvaluateOperatorTenantAccessBoundaries(t *testing.T) {
	issued := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	grant := contracts.OperatorAccessGrant{OperatorID: "op-1", TenantScope: []string{"tenant-a"}, Reason: "support case", TicketID: "T-1", IssuedAt: issued, ExpiresAt: issued.Add(time.Hour)}
	cases := []struct {
		name   string
		grant  contracts.OperatorAccessGrant
		tenant string
		now    time.Time
		allow  bool
	}{
		{"inside window and scope", grant, "tenant-a", issued.Add(time.Minute), true},
		{"at issue instant is inclusive", grant, "tenant-a", issued, true},
		{"just before issue", grant, "tenant-a", issued.Add(-time.Nanosecond), false},
		{"just before expiry", grant, "tenant-a", issued.Add(time.Hour - time.Nanosecond), true},
		{"at expiry is exclusive", grant, "tenant-a", issued.Add(time.Hour), false},
		{"tenant outside scope", grant, "tenant-b", issued.Add(time.Minute), false},
		{"empty scope denies", contracts.OperatorAccessGrant{OperatorID: "op-1", Reason: "r", TicketID: "t", IssuedAt: issued, ExpiresAt: issued.Add(time.Hour)}, "tenant-a", issued, false},
		{"blank reason", withReason(grant, " "), "tenant-a", issued, false},
		{"blank ticket", withTicket(grant, ""), "tenant-a", issued, false},
		{"blank operator", withOperator(grant, ""), "tenant-a", issued, false},
	}
	for _, tc := range cases {
		if allowed := rules.EvaluateOperatorTenantAccess(tc.grant, tc.tenant, tc.now) == nil; allowed != tc.allow {
			t.Fatalf("%s: allowed = %v, want %v", tc.name, allowed, tc.allow)
		}
	}
}

func withReason(grant contracts.OperatorAccessGrant, reason string) contracts.OperatorAccessGrant {
	grant.Reason = reason
	return grant
}

func withTicket(grant contracts.OperatorAccessGrant, ticket string) contracts.OperatorAccessGrant {
	grant.TicketID = ticket
	return grant
}

func withOperator(grant contracts.OperatorAccessGrant, operator string) contracts.OperatorAccessGrant {
	grant.OperatorID = operator
	return grant
}

func TestValidateWebhookEndpoint(t *testing.T) {
	cases := []struct {
		url   string
		valid bool
	}{
		{"https://hooks.example.com/v1/events", true},
		{"https://hooks.example.com:443/v1", true},
		{"https://203.0.114.10/hook", true},
		{"https://[2606:4700::1111]/hook", true},
		{"http://hooks.example.com/v1", false},
		{"ftp://hooks.example.com/v1", false},
		{"https://hooks.example.com:8443/v1", false},
		{"https://user:pass@hooks.example.com/v1", false},
		{"https://localhost/hook", false},
		{"https://localhost./hook", false},
		{"https://app.localhost/hook", false},
		{"https://intranet/hook", false},
		{"https://db.internal/hook", false},
		{"https://svc.cluster.local/hook", false},
		{"https://127.0.0.1/hook", false},
		{"https://127.1/hook", false},
		{"https://2130706433/hook", false},
		{"https://0x7f000001/hook", false},
		{"https://010.0.0.1/hook", false},
		{"https://10.0.0.5/hook", false},
		{"https://172.16.4.4/hook", false},
		{"https://192.168.1.1/hook", false},
		{"https://169.254.169.254/latest/meta-data", false},
		{"https://100.64.0.1/hook", false},
		{"https://0.0.0.0/hook", false},
		{"https://[::1]/hook", false},
		{"https://[fe80::1]/hook", false},
		{"https://[fc00::1]/hook", false},
		{"https://[::ffff:127.0.0.1]/hook", false},
		{"https://[::ffff:10.0.0.1]/hook", false},
		{"https://[64:ff9b::7f00:1]/hook", false},
		{"", false},
		{"not a url", false},
		{"https://" + strings.Repeat("a", 2050), false},
	}
	for _, tc := range cases {
		if err := rules.ValidateWebhookEndpoint(tc.url); (err == nil) != tc.valid {
			t.Fatalf("%q: valid = %v, want %v (err %v)", tc.url, err == nil, tc.valid, err)
		}
	}
}

func TestIsCrossTenantReference(t *testing.T) {
	if rules.IsCrossTenantReference("tenant-a", "tenant-a") {
		t.Fatal("same tenant must not be a cross tenant reference")
	}
	if !rules.IsCrossTenantReference("tenant-a", "tenant-ab") || !rules.IsCrossTenantReference("tenant-a", "Tenant-A") {
		t.Fatal("equality must be exact, never prefix or case folded")
	}
}

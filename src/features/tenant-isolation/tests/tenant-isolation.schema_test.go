package tests

import (
	"net/http"
	"strings"
	"testing"

	contracts "distributed-transaction-engine/src/features/tenant-isolation/contracts"
	"github.com/google/uuid"
)

func TestValidateTenantIDBoundaries(t *testing.T) {
	cases := []struct {
		value string
		valid bool
	}{
		{"abc", true},
		{"tenant-alpha", true},
		{"a1b", true},
		{strings.Repeat("a", 64), true},
		{strings.Repeat("a", 65), false},
		{"ab", false},
		{"", false},
		{"-abc", false},
		{"abc-", false},
		{"Tenant-A", false},
		{" tenant-a", false},
		{"tenant_a", false},
		{"tenant-a\n", false},
		{"tenant‑a", false},
		{"tenant a", false},
	}
	for _, tc := range cases {
		if err := contracts.ValidateTenantID("tenantId", tc.value); (err == nil) != tc.valid {
			t.Fatalf("%q: valid = %v, want %v", tc.value, err == nil, tc.valid)
		}
	}
}

func TestValidateCreateTenantInputBounds(t *testing.T) {
	valid := contracts.CreateTenantInput{TenantID: "tenant-a", MaxConcurrentSagas: 10, CorrelationID: uuid.New()}
	cases := []struct {
		name   string
		mutate func(contracts.CreateTenantInput) contracts.CreateTenantInput
		valid  bool
	}{
		{"valid", func(in contracts.CreateTenantInput) contracts.CreateTenantInput { return in }, true},
		{"lower bound", func(in contracts.CreateTenantInput) contracts.CreateTenantInput { in.MaxConcurrentSagas = 1; return in }, true},
		{"below lower bound", func(in contracts.CreateTenantInput) contracts.CreateTenantInput { in.MaxConcurrentSagas = 0; return in }, false},
		{"upper bound", func(in contracts.CreateTenantInput) contracts.CreateTenantInput {
			in.MaxConcurrentSagas = 1000000
			return in
		}, true},
		{"above upper bound", func(in contracts.CreateTenantInput) contracts.CreateTenantInput {
			in.MaxConcurrentSagas = 1000001
			return in
		}, false},
		{"nil correlation", func(in contracts.CreateTenantInput) contracts.CreateTenantInput {
			in.CorrelationID = uuid.Nil
			return in
		}, false},
		{"bad tenant", func(in contracts.CreateTenantInput) contracts.CreateTenantInput { in.TenantID = "Bad"; return in }, false},
	}
	for _, tc := range cases {
		if err := contracts.ValidateCreateTenantInput(tc.mutate(valid)); (err == nil) != tc.valid {
			t.Fatalf("%s: valid = %v, want %v", tc.name, err == nil, tc.valid)
		}
	}
}

func TestValidateUpdateTenantStatusInputReasonPairing(t *testing.T) {
	billing, bogus := contracts.SuspensionReasonBilling, contracts.SuspensionReason("OTHER")
	cases := []struct {
		name   string
		target contracts.TenantStatus
		reason *contracts.SuspensionReason
		valid  bool
	}{
		{"suspend with reason", contracts.TenantStatusSuspended, &billing, true},
		{"suspend without reason", contracts.TenantStatusSuspended, nil, false},
		{"suspend with unknown reason", contracts.TenantStatusSuspended, &bogus, false},
		{"activate without reason", contracts.TenantStatusActive, nil, true},
		{"activate with reason", contracts.TenantStatusActive, &billing, false},
		{"offboard with reason", contracts.TenantStatusOffboarding, &billing, false},
		{"unknown target", contracts.TenantStatus("DELETED"), nil, false},
	}
	for _, tc := range cases {
		input := contracts.UpdateTenantStatusInput{TenantID: "tenant-a", ExpectedVersion: 1, TargetStatus: tc.target, SuspensionReason: tc.reason}
		if err := contracts.ValidateUpdateTenantStatusInput(input); (err == nil) != tc.valid {
			t.Fatalf("%s: valid = %v, want %v", tc.name, err == nil, tc.valid)
		}
	}
	if err := contracts.ValidateUpdateTenantStatusInput(contracts.UpdateTenantStatusInput{TenantID: "tenant-a", TargetStatus: contracts.TenantStatusActive}); statusCodeOf(err) != http.StatusBadRequest {
		t.Fatal("expectedVersion below 1 must be rejected")
	}
}

func TestValidateMessageTenantEnvelopeAcceptsUntrustedTenantFields(t *testing.T) {
	hostile := "Not A Tenant \n"
	envelope := messageEnvelope(&hostile, &hostile, &hostile, &hostile)
	if err := contracts.ValidateMessageTenantEnvelope(envelope); err != nil {
		t.Fatalf("tenant fields on a message are untrusted claims and must reach quarantine, got %v", err)
	}
	envelope.PayloadSHA256 = strings.Repeat("A", 64)
	if err := contracts.ValidateMessageTenantEnvelope(envelope); err == nil {
		t.Fatal("uppercase digest must be rejected")
	}
	envelope = messageEnvelope(nil, nil, nil, nil)
	envelope.SourceOffset = -1
	if err := contracts.ValidateMessageTenantEnvelope(envelope); err == nil {
		t.Fatal("negative offset must be rejected")
	}
}

func TestValidateResolveTenantInputBounds(t *testing.T) {
	memberships := make([]string, contracts.MaxVerifiedMemberships+1)
	for index := range memberships {
		memberships[index] = "tenant-a"
	}
	input := contracts.ResolveTenantInput{PrincipalTenantID: "tenant-a", VerifiedMemberships: memberships, CorrelationID: uuid.New()}
	if err := contracts.ValidateResolveTenantInput(input); err == nil {
		t.Fatal("membership list above the bound must be rejected")
	}
	input.VerifiedMemberships = []string{"BAD"}
	if err := contracts.ValidateResolveTenantInput(input); err == nil {
		t.Fatal("non canonical membership must be rejected")
	}
}

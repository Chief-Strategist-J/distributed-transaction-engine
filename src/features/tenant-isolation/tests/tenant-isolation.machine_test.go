package tests

import (
	"net/http"
	"testing"

	contracts "distributed-transaction-engine/src/features/tenant-isolation/contracts"
	"distributed-transaction-engine/src/features/tenant-isolation/domain/machines"
)

func TestTenantStatusMachineEveryPair(t *testing.T) {
	statuses := []contracts.TenantStatus{
		contracts.TenantStatusActive, contracts.TenantStatusSuspended, contracts.TenantStatusOffboarding, contracts.TenantStatusOffboarded,
	}
	allowed := map[[2]contracts.TenantStatus]bool{
		{contracts.TenantStatusActive, contracts.TenantStatusSuspended}:       true,
		{contracts.TenantStatusActive, contracts.TenantStatusOffboarding}:     true,
		{contracts.TenantStatusSuspended, contracts.TenantStatusActive}:       true,
		{contracts.TenantStatusSuspended, contracts.TenantStatusOffboarding}:  true,
		{contracts.TenantStatusOffboarding, contracts.TenantStatusOffboarded}: true,
	}
	for _, from := range statuses {
		for _, to := range statuses {
			next, err := machines.TransitionToTenantStatus(from, to)
			if allowed[[2]contracts.TenantStatus{from, to}] {
				if err != nil || next != to {
					t.Fatalf("%s -> %s must be allowed", from, to)
				}
				continue
			}
			if statusCodeOf(err) != http.StatusConflict || next != from {
				t.Fatalf("%s -> %s must be rejected without changing state", from, to)
			}
		}
	}
}

func TestOffboardedIsTerminal(t *testing.T) {
	for _, transition := range machines.ListTenantStatusTransitions() {
		if transition.From == contracts.TenantStatusOffboarded {
			t.Fatal("OFFBOARDED must have no outgoing transition")
		}
	}
}

func TestListTenantStatusTransitionsIsDeterministic(t *testing.T) {
	first, second := machines.ListTenantStatusTransitions(), machines.ListTenantStatusTransitions()
	if len(first) != 5 || len(second) != 5 {
		t.Fatalf("expected 5 transitions, got %d", len(first))
	}
	for index := range first {
		if first[index] != second[index] {
			t.Fatal("transition listing order must be stable")
		}
	}
}

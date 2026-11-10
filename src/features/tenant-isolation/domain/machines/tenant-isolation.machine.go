package machines

import (
	contracts "distributed-transaction-engine/src/features/tenant-isolation/contracts"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"sort"
)

type TenantStatusTransition struct {
	From contracts.TenantStatus
	To   contracts.TenantStatus
}

var tenantStatusTransitions = map[TenantStatusTransition]struct{}{
	{From: contracts.TenantStatusActive, To: contracts.TenantStatusSuspended}:       {},
	{From: contracts.TenantStatusActive, To: contracts.TenantStatusOffboarding}:     {},
	{From: contracts.TenantStatusSuspended, To: contracts.TenantStatusActive}:       {},
	{From: contracts.TenantStatusSuspended, To: contracts.TenantStatusOffboarding}:  {},
	{From: contracts.TenantStatusOffboarding, To: contracts.TenantStatusOffboarded}: {},
}

func ListTenantStatusTransitions() []TenantStatusTransition {
	defer observability.TraceEnter(nil)()
	transitions := make([]TenantStatusTransition, 0, len(tenantStatusTransitions))
	for transition := range tenantStatusTransitions {
		transitions = append(transitions, transition)
	}
	sort.Slice(transitions, func(i int, j int) bool {
		if transitions[i].From != transitions[j].From {
			return transitions[i].From < transitions[j].From
		}
		return transitions[i].To < transitions[j].To
	})
	return transitions
}

func IsTenantStatusTransitionAllowed(from contracts.TenantStatus, to contracts.TenantStatus) bool {
	defer observability.TraceEnter(nil)()
	_, allowed := tenantStatusTransitions[TenantStatusTransition{From: from, To: to}]
	return allowed
}

func TransitionToTenantStatus(from contracts.TenantStatus, to contracts.TenantStatus) (spanResult0 contracts.TenantStatus, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if IsTenantStatusTransitionAllowed(from, to) {
		return to, nil
	}
	return from, appErrors.NewConflictError("tenant status transition " + string(from) + " -> " + string(to) + " is not allowed")
}

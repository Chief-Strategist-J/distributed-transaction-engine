package machines

import (
	contracts "distributed-transaction-engine/src/features/transaction-query-api/contracts"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"sort"
)

type ExportTransition struct {
	From contracts.ExportStatus
	To   contracts.ExportStatus
}

var exportTransitions = map[ExportTransition]struct{}{
	{From: contracts.ExportQueued, To: contracts.ExportRunning}:    {},
	{From: contracts.ExportQueued, To: contracts.ExportCancelled}:  {},
	{From: contracts.ExportQueued, To: contracts.ExportExpired}:    {},
	{From: contracts.ExportRunning, To: contracts.ExportCompleted}: {},
	{From: contracts.ExportRunning, To: contracts.ExportFailed}:    {},
	{From: contracts.ExportRunning, To: contracts.ExportCancelled}: {},
	{From: contracts.ExportCompleted, To: contracts.ExportExpired}: {},
	{From: contracts.ExportFailed, To: contracts.ExportExpired}:    {},
}

func ListExportTransitions() []ExportTransition {
	defer observability.TraceEnter(nil)()
	transitions := make([]ExportTransition, 0, len(exportTransitions))
	for transition := range exportTransitions {
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

func IsExportTransitionAllowed(from contracts.ExportStatus, to contracts.ExportStatus) bool {
	defer observability.TraceEnter(nil)()
	_, allowed := exportTransitions[ExportTransition{From: from, To: to}]
	return allowed
}

func TransitionToExportStatus(from contracts.ExportStatus, to contracts.ExportStatus) (spanResult0 contracts.ExportStatus, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if IsExportTransitionAllowed(from, to) {
		return to, nil
	}
	return from, appErrors.NewConflictError("export status transition " + string(from) + " -> " + string(to) + " is not allowed")
}

func IsExportTerminal(status contracts.ExportStatus) bool {
	defer observability.TraceEnter(nil)()
	return status == contracts.ExportExpired || status == contracts.ExportCancelled
}

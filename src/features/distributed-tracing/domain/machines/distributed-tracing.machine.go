package machines

import (
	"distributed-transaction-engine/src/features/distributed-tracing/contracts"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"time"
)

type DeepTraceSessionMachine struct{}

func NewDeepTraceSessionMachine() *DeepTraceSessionMachine {
	return &DeepTraceSessionMachine{}
}

func (m *DeepTraceSessionMachine) EvaluateSessionState(current contracts.DeepSessionStatus, expiresAt time.Time, currentSpans int, maxSpans int) (spanResult0 contracts.DeepSessionStatus, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if current == contracts.DeepSessionCancelled {
		return current, appErrors.NewConflictError("deep session has been cancelled")
	}
	if time.Now().UTC().After(expiresAt) {
		return contracts.DeepSessionExpired, nil
	}
	if currentSpans >= maxSpans {
		return contracts.DeepSessionCapped, nil
	}
	return contracts.DeepSessionActive, nil
}

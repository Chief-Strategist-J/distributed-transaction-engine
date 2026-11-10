package machines

import (
	"distributed-transaction-engine/src/features/optimistic-concurrency-control/contracts"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
)

type ConcurrencyState string

const (
	StateOCCStandard         ConcurrencyState = "OCC_STANDARD"
	StateContentionWarning   ConcurrencyState = "CONTENTION_WARNING"
	StateSerializedPartition ConcurrencyState = "SERIALIZED_PARTITION"
)

type ConcurrencyEvent string

const (
	EventConflictRecorded  ConcurrencyEvent = "CONFLICT_RECORDED"
	EventThresholdExceeded ConcurrencyEvent = "THRESHOLD_EXCEEDED"
	EventContentionCleared ConcurrencyEvent = "CONTENTION_CLEARED"
	EventForceSerialize    ConcurrencyEvent = "FORCE_SERIALIZE"
)

type OCCStrategyMachine struct{}

func NewOCCStrategyMachine() *OCCStrategyMachine {
	return &OCCStrategyMachine{}
}

func (m *OCCStrategyMachine) Transition(current ConcurrencyState, event ConcurrencyEvent) (spanResult0 ConcurrencyState, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	switch current {
	case StateOCCStandard:
		switch event {
		case EventConflictRecorded:
			return StateContentionWarning, nil
		case EventThresholdExceeded, EventForceSerialize:
			return StateSerializedPartition, nil
		case EventContentionCleared:
			return StateOCCStandard, nil
		}
	case StateContentionWarning:
		switch event {
		case EventThresholdExceeded, EventForceSerialize:
			return StateSerializedPartition, nil
		case EventContentionCleared:
			return StateOCCStandard, nil
		case EventConflictRecorded:
			return StateContentionWarning, nil
		}
	case StateSerializedPartition:
		switch event {
		case EventContentionCleared:
			return StateOCCStandard, nil
		case EventConflictRecorded, EventThresholdExceeded:
			return StateSerializedPartition, nil
		}
	}

	return "", appErrors.NewConflictError("illegal concurrency machine transition from " + string(current) + " on event " + string(event))
}

func (m *OCCStrategyMachine) MapToStrategy(state ConcurrencyState) contracts.ConcurrencyStrategy {
	defer observability.TraceEnter(nil)()
	if state == StateSerializedPartition {
		return contracts.StrategySerialized
	}
	return contracts.StrategyOCC
}

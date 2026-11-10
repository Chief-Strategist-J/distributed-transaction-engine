package rules

import (
	contracts "distributed-transaction-engine/src/features/transactional-outbox/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"strings"
)

type ErrorClassification string

const (
	ErrorRetriable    ErrorClassification = "RETRIABLE"
	ErrorNonRetriable ErrorClassification = "NON_RETRIABLE"
)

func ClassifyPublishError(err error) ErrorClassification {
	defer observability.TraceEnter(nil)()
	if err == nil {
		return ErrorRetriable
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "record too large") ||
		strings.Contains(msg, "unknown topic") ||
		strings.Contains(msg, "authorization failed") ||
		strings.Contains(msg, "invalid message") ||
		strings.Contains(msg, "invalid record") {
		return ErrorNonRetriable
	}
	return ErrorRetriable
}

func ValidateSequenceProgression(currentSeq int64, nextSeq int64) bool {
	defer observability.TraceEnter(nil)()
	return nextSeq == currentSeq+1
}

func CanTransitionOutboxStatus(from contracts.OutboxStatus, to contracts.OutboxStatus) bool {
	defer observability.TraceEnter(nil)()
	switch from {
	case contracts.OutboxStatusPending:
		return to == contracts.OutboxStatusInFlight
	case contracts.OutboxStatusInFlight:
		return to == contracts.OutboxStatusPublished || to == contracts.OutboxStatusPending || to == contracts.OutboxStatusParked
	case contracts.OutboxStatusPublished:
		return false
	case contracts.OutboxStatusParked:
		return to == contracts.OutboxStatusPending
	default:
		return false
	}
}

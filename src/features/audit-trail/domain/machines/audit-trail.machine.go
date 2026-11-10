package machines

import (
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
)

type SegmentState string

const (
	SegmentStateActive      SegmentState = "ACTIVE"
	SegmentStateSealed      SegmentState = "SEALED"
	SegmentStateCompromised SegmentState = "COMPROMISED"
)

type AuditChainMachine struct{}

func NewAuditChainMachine() *AuditChainMachine {
	return &AuditChainMachine{}
}

func (m *AuditChainMachine) ValidateTransition(current SegmentState, next SegmentState) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	switch current {
	case SegmentStateActive:
		if next == SegmentStateSealed || next == SegmentStateCompromised {
			return nil
		}
	case SegmentStateSealed:
		return appErrors.NewConflictError("sealed segment cannot transition to " + string(next))
	case SegmentStateCompromised:
		return appErrors.NewConflictError("compromised segment cannot transition to " + string(next))
	}
	return appErrors.NewConflictError("invalid segment state transition from " + string(current) + " to " + string(next))
}

func (m *AuditChainMachine) CreateNewSegmentID() uuid.UUID {
	defer observability.TraceEnter(nil)()
	return uuid.New()
}

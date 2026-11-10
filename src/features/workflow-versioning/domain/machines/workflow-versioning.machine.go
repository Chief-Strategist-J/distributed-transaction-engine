package machines

import (
	"distributed-transaction-engine/src/features/workflow-versioning/contracts"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"fmt"
)

type BundleMachine struct{}

func NewBundleMachine() *BundleMachine {
	return &BundleMachine{}
}

func (m *BundleMachine) Transition(current contracts.BundleStatus, action contracts.PromotionAction) (spanResult0 contracts.BundleStatus, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	switch current {
	case contracts.BundleStatusDraft:
		return m.transitionFromDraft(action)
	case contracts.BundleStatusValidated:
		return m.transitionFromValidated(action)
	case contracts.BundleStatusActive:
		return m.transitionFromActive(action)
	case contracts.BundleStatusDeprecated:
		return m.transitionFromDeprecated(action)
	case contracts.BundleStatusBlocked:
		return m.transitionFromBlocked(action)
	case contracts.BundleStatusArchived:
		return "", appErrors.NewConflictError("cannot transition from ARCHIVED terminal status")
	default:
		return "", appErrors.NewValidationError(fmt.Sprintf("unknown bundle status: %s", current))
	}
}

func (m *BundleMachine) transitionFromDraft(action contracts.PromotionAction) (contracts.BundleStatus, error) {
	if action == contracts.PromotionActionPublish {
		return contracts.BundleStatusValidated, nil
	}
	return "", appErrors.NewConflictError(fmt.Sprintf("cannot %s from DRAFT", action))
}

func (m *BundleMachine) transitionFromValidated(action contracts.PromotionAction) (contracts.BundleStatus, error) {
	if action == contracts.PromotionActionPromote {
		return contracts.BundleStatusActive, nil
	}
	if action == contracts.PromotionActionBlock {
		return contracts.BundleStatusBlocked, nil
	}
	return "", appErrors.NewConflictError(fmt.Sprintf("cannot %s from VALIDATED", action))
}

func (m *BundleMachine) transitionFromActive(action contracts.PromotionAction) (contracts.BundleStatus, error) {
	if action == contracts.PromotionActionDeprecate {
		return contracts.BundleStatusDeprecated, nil
	}
	if action == contracts.PromotionActionBlock {
		return contracts.BundleStatusBlocked, nil
	}
	return "", appErrors.NewConflictError(fmt.Sprintf("cannot %s from ACTIVE", action))
}

func (m *BundleMachine) transitionFromDeprecated(action contracts.PromotionAction) (contracts.BundleStatus, error) {
	if action == contracts.PromotionActionArchive {
		return contracts.BundleStatusArchived, nil
	}
	if action == contracts.PromotionActionBlock {
		return contracts.BundleStatusBlocked, nil
	}
	return "", appErrors.NewConflictError(fmt.Sprintf("cannot %s from DEPRECATED", action))
}

func (m *BundleMachine) transitionFromBlocked(action contracts.PromotionAction) (contracts.BundleStatus, error) {
	if action == contracts.PromotionActionArchive {
		return contracts.BundleStatusArchived, nil
	}
	return "", appErrors.NewConflictError(fmt.Sprintf("cannot %s from BLOCKED", action))
}

type PromotionMachine struct{}

func NewPromotionMachine() *PromotionMachine {
	return &PromotionMachine{}
}

func (m *PromotionMachine) Decide(current contracts.PromotionStatus, approved bool) (spanResult0 contracts.PromotionStatus, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if current != contracts.PromotionStatusPending {
		return "", appErrors.NewConflictError(fmt.Sprintf("promotion is already finalized with status: %s", current))
	}
	if approved {
		return contracts.PromotionStatusApproved, nil
	}
	return contracts.PromotionStatusRejected, nil
}

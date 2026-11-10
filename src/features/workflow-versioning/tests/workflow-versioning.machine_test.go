package tests

import (
	"testing"

	"distributed-transaction-engine/src/features/workflow-versioning/contracts"
	"distributed-transaction-engine/src/features/workflow-versioning/domain/machines"
)

func TestBundleMachine_HappyPathLifecycle(t *testing.T) {
	m := machines.NewBundleMachine()

	s1, err := m.Transition(contracts.BundleStatusDraft, contracts.PromotionActionPublish)
	if err != nil || s1 != contracts.BundleStatusValidated {
		t.Fatalf("expected VALIDATED, got: %s (err: %v)", s1, err)
	}

	s2, err := m.Transition(s1, contracts.PromotionActionPromote)
	if err != nil || s2 != contracts.BundleStatusActive {
		t.Fatalf("expected ACTIVE, got: %s (err: %v)", s2, err)
	}

	s3, err := m.Transition(s2, contracts.PromotionActionDeprecate)
	if err != nil || s3 != contracts.BundleStatusDeprecated {
		t.Fatalf("expected DEPRECATED, got: %s (err: %v)", s3, err)
	}

	s4, err := m.Transition(s3, contracts.PromotionActionArchive)
	if err != nil || s4 != contracts.BundleStatusArchived {
		t.Fatalf("expected ARCHIVED, got: %s (err: %v)", s4, err)
	}

	_, err = m.Transition(s4, contracts.PromotionActionPromote)
	if err == nil {
		t.Fatal("expected error transitioning from ARCHIVED")
	}
}

func TestBundleMachine_EmergencyBlock(t *testing.T) {
	m := machines.NewBundleMachine()

	blocked, err := m.Transition(contracts.BundleStatusActive, contracts.PromotionActionBlock)
	if err != nil || blocked != contracts.BundleStatusBlocked {
		t.Fatalf("expected BLOCKED, got: %s (err: %v)", blocked, err)
	}

	archived, err := m.Transition(blocked, contracts.PromotionActionArchive)
	if err != nil || archived != contracts.BundleStatusArchived {
		t.Fatalf("expected ARCHIVED from BLOCKED, got: %s (err: %v)", archived, err)
	}
}

func TestBundleMachine_IllegalTransitions(t *testing.T) {
	m := machines.NewBundleMachine()

	if _, err := m.Transition(contracts.BundleStatusDraft, contracts.PromotionActionPromote); err == nil {
		t.Fatal("expected error directly promoting DRAFT to ACTIVE")
	}
	if _, err := m.Transition(contracts.BundleStatusDraft, contracts.PromotionActionArchive); err == nil {
		t.Fatal("expected error archiving DRAFT")
	}
}

func TestPromotionMachine_Decisions(t *testing.T) {
	m := machines.NewPromotionMachine()

	app, err := m.Decide(contracts.PromotionStatusPending, true)
	if err != nil || app != contracts.PromotionStatusApproved {
		t.Fatalf("expected APPROVED, got %s (err: %v)", app, err)
	}

	rej, err := m.Decide(contracts.PromotionStatusPending, false)
	if err != nil || rej != contracts.PromotionStatusRejected {
		t.Fatalf("expected REJECTED, got %s (err: %v)", rej, err)
	}

	if _, err := m.Decide(contracts.PromotionStatusApproved, true); err == nil {
		t.Fatal("expected error deciding on finalized promotion")
	}
}


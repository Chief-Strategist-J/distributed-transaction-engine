package machines

import (
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
)

type RiskAuthorizationState string

const (
	RiskStateStandard            RiskAuthorizationState = "STANDARD"
	RiskStateStepUpRequired      RiskAuthorizationState = "STEP_UP_REQUIRED"
	RiskStateDualApprovalPending RiskAuthorizationState = "DUAL_APPROVAL_PENDING"
	RiskStateApproved            RiskAuthorizationState = "APPROVED"
	RiskStateDenied              RiskAuthorizationState = "DENIED"
)

type AuthRiskMachine struct{}

func NewAuthRiskMachine() *AuthRiskMachine {
	return &AuthRiskMachine{}
}

func (m *AuthRiskMachine) Transition(current RiskAuthorizationState, event string) (spanResult0 RiskAuthorizationState, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	switch current {
	case RiskStateStandard:
		switch event {
		case "EVALUATE_PASS":
			return RiskStateApproved, nil
		case "REQUIRE_STEP_UP":
			return RiskStateStepUpRequired, nil
		case "REQUIRE_DUAL_APPROVAL":
			return RiskStateDualApprovalPending, nil
		case "EVALUATE_FAIL":
			return RiskStateDenied, nil
		}
	case RiskStateStepUpRequired:
		switch event {
		case "STEP_UP_VERIFIED":
			return RiskStateApproved, nil
		case "STEP_UP_FAILED":
			return RiskStateDenied, nil
		}
	case RiskStateDualApprovalPending:
		switch event {
		case "APPROVER_CONFIRMED":
			return RiskStateApproved, nil
		case "APPROVER_REJECTED":
			return RiskStateDenied, nil
		}
	case RiskStateApproved, RiskStateDenied:
		return current, appErrors.NewConflictError("terminal state reached: " + string(current))
	}
	return current, appErrors.NewConflictError("unhandled transition for state " + string(current) + " on event " + event)
}

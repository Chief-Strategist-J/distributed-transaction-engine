package rules

import (
	"distributed-transaction-engine/src/features/dead-letter-queue/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"time"
)

type DeadLetterRules struct{}

func NewDeadLetterRules() *DeadLetterRules {
	return &DeadLetterRules{}
}

func (r *DeadLetterRules) CanAdmit(failureClass contracts.FailureClass) bool {
	defer observability.TraceEnter(nil)()
	switch failureClass {
	case contracts.FailurePoison,
		contracts.FailureDefinitionDefect,
		contracts.FailureRetryExhausted,
		contracts.FailureBusinessRejection,
		contracts.FailureCompensationFailure,
		contracts.FailureIntegrityViolation:
		return true
	default:
		return false
	}
}

func (r *DeadLetterRules) EvaluateReplaySafety(
	record contracts.DeadLetterRecord,
	sagaCurrentState string,
	sagaCurrentStep int,
	isStepTypePaused bool,
	now time.Time,
) contracts.ReplayEvaluation {
	defer observability.TraceEnter(nil)()
	if isStepTypePaused {
		return contracts.ReplayEvaluation{
			Allowed:                     false,
			Reason:                      "step type is paused due to systemic incident",
			RequiresAmbiguityResolution: false,
		}
	}

	if record.Status == contracts.StatusResolved {
		return contracts.ReplayEvaluation{
			Allowed:                     false,
			Reason:                      "record is already in terminal resolved state",
			RequiresAmbiguityResolution: false,
		}
	}

	if sagaCurrentState == "COMPENSATED" || sagaCurrentState == "COMPENSATING" || sagaCurrentState == "FAILED" {
		return contracts.ReplayEvaluation{
			Allowed:                     false,
			Reason:                      "saga state is already compensated or failed",
			RequiresAmbiguityResolution: false,
		}
	}

	if record.StepIndex != sagaCurrentStep {
		return contracts.ReplayEvaluation{
			Allowed:                     false,
			Reason:                      "saga execution step does not match dead letter step index",
			RequiresAmbiguityResolution: false,
		}
	}

	if record.OutcomeClass == contracts.OutcomeUnknown {
		return contracts.ReplayEvaluation{
			Allowed:                     false,
			Reason:                      "ambiguous outcome requires status verification before replay",
			RequiresAmbiguityResolution: true,
		}
	}

	if record.BusinessDeadline != nil && now.After(*record.BusinessDeadline) {
		return contracts.ReplayEvaluation{
			Allowed:                     false,
			Reason:                      "business deadline has expired",
			RequiresAmbiguityResolution: false,
		}
	}

	return contracts.ReplayEvaluation{
		Allowed:                     true,
		Reason:                      "replay safety verification passed",
		RequiresAmbiguityResolution: false,
	}
}

func (r *DeadLetterRules) RequiresSecondApprover(moneyAtStakeCents int64, disposition contracts.DispositionType) bool {
	defer observability.TraceEnter(nil)()
	if moneyAtStakeCents <= 0 {
		return false
	}
	return disposition == contracts.DispositionWrittenOff || disposition == contracts.DispositionCorrected
}

func (r *DeadLetterRules) IsSystemicBurst(recentCount int, threshold int) bool {
	defer observability.TraceEnter(nil)()
	if threshold <= 0 {
		threshold = 20
	}
	return recentCount >= threshold
}

func (r *DeadLetterRules) ShouldEscalate(record contracts.DeadLetterRecord, now time.Time, moneyThresholdCents int64) bool {
	defer observability.TraceEnter(nil)()
	if record.Status == contracts.StatusResolved {
		return false
	}
	if record.FailureClass == contracts.FailureIntegrityViolation {
		return true
	}
	if moneyThresholdCents > 0 && record.MoneyAtStakeCents >= moneyThresholdCents {
		return true
	}
	if record.BusinessDeadline != nil && record.BusinessDeadline.Sub(now) <= (2*time.Hour) {
		return true
	}
	return false
}

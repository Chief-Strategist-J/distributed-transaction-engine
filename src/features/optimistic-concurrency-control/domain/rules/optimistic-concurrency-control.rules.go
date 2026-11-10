package rules

import (
	"distributed-transaction-engine/src/features/optimistic-concurrency-control/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"math"
	"math/rand"
	"time"
)

type OCCRules struct{}

func NewOCCRules() *OCCRules {
	return &OCCRules{}
}

func (r *OCCRules) DiagnoseZeroRows(
	exists bool,
	loadedTenant string,
	currentTenant string,
	loadedVersion int64,
	currentVersion int64,
	loadedEpoch int64,
	currentEpoch int64,
	currentState string,
) contracts.ZeroRowCause {
	defer observability.TraceEnter(nil)()
	if !exists {
		return contracts.CauseDataError
	}
	if loadedTenant != currentTenant {
		return contracts.CauseSecurityViolation
	}
	if currentEpoch != loadedEpoch {
		return contracts.CauseNotOwner
	}
	if currentVersion != loadedVersion {
		return contracts.CauseVersionConflict
	}
	return contracts.CauseStateConflict
}

func (r *OCCRules) CanRetryConflict(cause contracts.ZeroRowCause, currentAttempt int, maxAttempts int) bool {
	defer observability.TraceEnter(nil)()
	if cause != contracts.CauseVersionConflict {
		return false
	}
	if maxAttempts <= 0 {
		maxAttempts = 5
	}
	return currentAttempt < maxAttempts
}

func (r *OCCRules) ComputeJitteredBackoff(attempt int) time.Duration {
	defer observability.TraceEnter(nil)()
	floorMs := 5
	capMs := 200
	baseMs := 10

	factor := math.Pow(2, float64(attempt))
	rawMs := int(float64(baseMs) * factor)
	if rawMs > capMs {
		rawMs = capMs
	}
	if rawMs < floorMs {
		rawMs = floorMs
	}

	half := rawMs / 2
	jitter := 0
	if half > 0 {
		jitter = rand.Intn(half)
	}

	totalMs := half + jitter
	if totalMs < floorMs {
		totalMs = floorMs
	}
	return time.Duration(totalMs) * time.Millisecond
}

func (r *OCCRules) IsInputAlreadyApplied(inputID string, appliedRegistry map[string]bool) bool {
	defer observability.TraceEnter(nil)()
	return appliedRegistry[inputID]
}

func (r *OCCRules) ShouldSerializeSaga(consecutiveConflicts int, threshold int) bool {
	defer observability.TraceEnter(nil)()
	if threshold <= 0 {
		threshold = 3
	}
	return consecutiveConflicts >= threshold
}

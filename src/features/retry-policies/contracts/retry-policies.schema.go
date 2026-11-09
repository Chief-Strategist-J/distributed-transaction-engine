package contracts

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	appErrors "distributed-transaction-engine/src/shared/errors"

	"github.com/google/uuid"
)

func ValidateEvaluateInput(input EvaluateRetryInput) (*uuid.UUID, *time.Time, *time.Time, error) {
	if strings.TrimSpace(input.TenantID) == "" {
		return nil, nil, nil, appErrors.NewValidationError("tenantId cannot be empty")
	}

	if strings.TrimSpace(input.SagaID) == "" {
		return nil, nil, nil, appErrors.NewValidationError("sagaId cannot be empty")
	}
	sagaUUID, err := uuid.Parse(input.SagaID)
	if err != nil {
		return nil, nil, nil, appErrors.NewValidationError("sagaId must be a valid uuid")
	}

	if strings.TrimSpace(input.StepID) == "" {
		return nil, nil, nil, appErrors.NewValidationError("stepId cannot be empty")
	}

	if input.AttemptNumber < 1 {
		return nil, nil, nil, appErrors.NewValidationError("attemptNumber must be at least 1")
	}

	if strings.TrimSpace(input.FirstAttemptTime) == "" {
		return nil, nil, nil, appErrors.NewValidationError("firstAttemptTime cannot be empty")
	}
	firstTime, err := time.Parse(time.RFC3339, input.FirstAttemptTime)
	if err != nil {
		return nil, nil, nil, appErrors.NewValidationError("firstAttemptTime must be RFC3339 timestamp")
	}

	var effectiveDeadline *time.Time
	if strings.TrimSpace(input.EffectiveDeadline) != "" {
		t, err := time.Parse(time.RFC3339, input.EffectiveDeadline)
		if err != nil {
			return nil, nil, nil, appErrors.NewValidationError("effectiveDeadline must be RFC3339 timestamp")
		}
		effectiveDeadline = &t
	}

	if input.Policy.MaxAttempts < 1 {
		input.Policy.MaxAttempts = 5
	}
	if input.Policy.BaseIntervalMillis < 10 {
		input.Policy.BaseIntervalMillis = 200
	}
	if input.Policy.MaxIntervalMillis < input.Policy.BaseIntervalMillis {
		input.Policy.MaxIntervalMillis = 30000
	}
	if input.Policy.MaxElapsedSeconds < 1 {
		input.Policy.MaxElapsedSeconds = 300
	}
	if input.Policy.Algorithm == "" {
		input.Policy.Algorithm = AlgorithmFullJitter
	}

	return &sagaUUID, &firstTime, effectiveDeadline, nil
}

func ParseRetryAfter(headerValue string, now time.Time) (time.Duration, bool) {
	headerValue = strings.TrimSpace(headerValue)
	if headerValue == "" {
		return 0, false
	}

	if seconds, err := strconv.Atoi(headerValue); err == nil {
		if seconds < 0 {
			return 0, false
		}
		return time.Duration(seconds) * time.Second, true
	}

	if t, err := http.ParseTime(headerValue); err == nil {
		diff := t.Sub(now)
		if diff < 0 {
			return 0, false
		}
		return diff, true
	}

	return 0, false
}


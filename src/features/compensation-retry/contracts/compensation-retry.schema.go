package contracts

import (
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	appErrors "distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
)

var (
	cprPublicIDRegex = regexp.MustCompile(`^cpr_[0-9a-zA-Z]{26}$`)
)

func ValidateEvaluateCompensationRetryInput(input EvaluateCompensationRetryInput) (*uuid.UUID, *time.Time, *time.Time, error) {
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
	if strings.TrimSpace(input.StepType) == "" {
		return nil, nil, nil, appErrors.NewValidationError("stepType cannot be empty")
	}
	if strings.TrimSpace(input.DependencyID) == "" {
		return nil, nil, nil, appErrors.NewValidationError("dependencyId cannot be empty")
	}
	if strings.TrimSpace(input.CompensationKey) == "" {
		return nil, nil, nil, appErrors.NewValidationError("compensationKey cannot be empty")
	}

	if input.AttemptNumber < 1 {
		return nil, nil, nil, appErrors.NewValidationError("attemptNumber must be at least 1")
	}

	if strings.TrimSpace(input.FirstAttemptAt) == "" {
		return nil, nil, nil, appErrors.NewValidationError("firstAttemptAt cannot be empty")
	}
	firstTime, err := time.Parse(time.RFC3339, input.FirstAttemptAt)
	if err != nil {
		return nil, nil, nil, appErrors.NewValidationError("firstAttemptAt must be RFC3339 timestamp")
	}

	if strings.TrimSpace(input.BusinessDeadline) == "" {
		return nil, nil, nil, appErrors.NewValidationError("businessDeadline cannot be empty")
	}
	deadlineTime, err := time.Parse(time.RFC3339, input.BusinessDeadline)
	if err != nil {
		return nil, nil, nil, appErrors.NewValidationError("businessDeadline must be RFC3339 timestamp")
	}

	return &sagaUUID, &firstTime, &deadlineTime, nil
}

func ValidatePublicID(publicID string) bool {
	return cprPublicIDRegex.MatchString(publicID)
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


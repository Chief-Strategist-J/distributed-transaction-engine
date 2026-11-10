package contracts

import (
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"strings"
	"time"
)

func ValidateCreateScheduleInput(input CreateScheduleInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if strings.TrimSpace(input.TenantID) == "" {
		return appErrors.NewValidationError("tenantId cannot be empty")
	}
	if strings.TrimSpace(input.WorkflowID) == "" {
		return appErrors.NewValidationError("workflowId cannot be empty")
	}
	if strings.TrimSpace(input.TimeZone) == "" {
		return appErrors.NewValidationError("timeZone is mandatory")
	}
	if _, err := time.LoadLocation(strings.TrimSpace(input.TimeZone)); err != nil {
		return appErrors.NewValidationError("invalid IANA timeZone: " + err.Error())
	}
	if strings.TrimSpace(input.RunAsPrincipal) == "" {
		return appErrors.NewValidationError("runAsPrincipal cannot be empty")
	}
	if input.ScheduleType == TypeRecurringCron && strings.TrimSpace(input.CronExpression) == "" {
		return appErrors.NewValidationError("cronExpression is mandatory for RECURRING_CRON")
	}
	return nil
}

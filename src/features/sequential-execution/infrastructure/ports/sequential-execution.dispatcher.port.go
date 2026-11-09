package ports

import (
	"context"
)

type SequentialDispatcherPort interface {
	DispatchStep(ctx context.Context, tenantID string, sagaID string, stepIndex int, payload map[string]interface{}) error
	CompensateStep(ctx context.Context, tenantID string, sagaID string, stepIndex int) error
	ScheduleTimer(ctx context.Context, tenantID string, sagaID string, stepIndex int, delaySeconds int) error
}

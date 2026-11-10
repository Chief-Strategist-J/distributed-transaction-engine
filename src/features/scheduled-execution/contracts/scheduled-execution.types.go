package contracts

import (
	"time"

	"github.com/google/uuid"
)

type ScheduleType string

const (
	TypeOneOff        ScheduleType = "ONE_OFF"
	TypeRecurringCron ScheduleType = "RECURRING_CRON"
)

type DSTPolicy string

const (
	DSTRunAtNextValid DSTPolicy = "RUN_AT_NEXT_VALID"
	DSTSkip           DSTPolicy = "SKIP"
	DSTRunFirst       DSTPolicy = "RUN_FIRST"
	DSTRunSecond      DSTPolicy = "RUN_SECOND"
)

type MisfirePolicy string

const (
	MisfireRunAllMissed MisfirePolicy = "RUN_ALL_MISSED"
	MisfireRunOnce      MisfirePolicy = "RUN_ONCE"
	MisfireSkipMissed   MisfirePolicy = "SKIP_MISSED"
)

type OverlapPolicy string

const (
	OverlapAllow          OverlapPolicy = "ALLOW"
	OverlapSkipIfRunning  OverlapPolicy = "SKIP_IF_RUNNING"
	OverlapQueueOne       OverlapPolicy = "QUEUE_ONE"
	OverlapCancelPrevious OverlapPolicy = "CANCEL_PREVIOUS"
)

type ScheduleStatus string

const (
	ScheduleActive    ScheduleStatus = "ACTIVE"
	SchedulePaused    ScheduleStatus = "PAUSED"
	ScheduleExpired   ScheduleStatus = "EXPIRED"
	ScheduleCompleted ScheduleStatus = "COMPLETED"
)

type OccurrenceStatus string

const (
	OccurrenceTriggered      OccurrenceStatus = "TRIGGERED"
	OccurrenceDispatched     OccurrenceStatus = "DISPATCHED"
	OccurrenceSkippedMisfire OccurrenceStatus = "SKIPPED_MISFIRE"
	OccurrenceSkippedOverlap OccurrenceStatus = "SKIPPED_OVERLAP"
	OccurrenceExpired        OccurrenceStatus = "EXPIRED"
	OccurrenceDenied         OccurrenceStatus = "DENIED"
	OccurrenceFailed         OccurrenceStatus = "FAILED"
)

type ScheduleRecord struct {
	ID                  uuid.UUID      `json:"id"`
	PublicID            string         `json:"publicId"`
	TenantID            string         `json:"tenantId"`
	Source              string         `json:"source"`
	ExternalID          string         `json:"externalId"`
	CorrelationID       uuid.UUID      `json:"correlationId"`
	WorkflowID          string         `json:"workflowId"`
	ScheduleType        ScheduleType   `json:"scheduleType"`
	CronExpression      string         `json:"cronExpression"`
	TimeZone            string         `json:"timeZone"`
	DSTPolicy           DSTPolicy      `json:"dstPolicy"`
	MisfirePolicy       MisfirePolicy  `json:"misfirePolicy"`
	OverlapPolicy       OverlapPolicy  `json:"overlapPolicy"`
	MaxLatenessSeconds  int            `json:"maxLatenessSeconds"`
	Status              ScheduleStatus `json:"status"`
	RunAsPrincipal      string         `json:"runAsPrincipal"`
	RequiredPermission  string         `json:"requiredPermission"`
	FrozenPayload       string         `json:"frozenPayload"`
	NextOccurrenceAt    time.Time      `json:"nextOccurrenceAt"`
	LastOccurrenceAt    *time.Time     `json:"lastOccurrenceAt,omitempty"`
	ConsecutiveFailures int            `json:"consecutiveFailures"`
	Version             int64          `json:"version"`
	CreatedAt           time.Time      `json:"createdAt"`
	UpdatedAt           time.Time      `json:"updatedAt"`
}

type ScheduleOccurrenceRecord struct {
	ID                  uuid.UUID        `json:"id"`
	PublicID            string           `json:"publicId"`
	TenantID            string           `json:"tenantId"`
	Source              string           `json:"source"`
	ExternalID          string           `json:"externalId"`
	CorrelationID       uuid.UUID        `json:"correlationId"`
	ScheduleID          uuid.UUID        `json:"scheduleId"`
	LogicalOccurrenceAt time.Time        `json:"logicalOccurrenceAt"`
	Status              OccurrenceStatus `json:"status"`
	DispatchedSagaID    *uuid.UUID       `json:"dispatchedSagaId,omitempty"`
	FailureReason       string           `json:"failureReason"`
	Version             int64            `json:"version"`
	CreatedAt           time.Time        `json:"createdAt"`
}

type CreateScheduleInput struct {
	TenantID           string         `json:"tenantId"`
	WorkflowID         string         `json:"workflowId"`
	ScheduleType       ScheduleType   `json:"scheduleType"`
	CronExpression     string         `json:"cronExpression,omitempty"`
	TimeZone           string         `json:"timeZone"`
	DSTPolicy          DSTPolicy      `json:"dstPolicy,omitempty"`
	MisfirePolicy      MisfirePolicy  `json:"misfirePolicy,omitempty"`
	OverlapPolicy      OverlapPolicy  `json:"overlapPolicy,omitempty"`
	MaxLatenessSeconds int            `json:"maxLatenessSeconds,omitempty"`
	RunAsPrincipal     string         `json:"runAsPrincipal"`
	RequiredPermission string         `json:"requiredPermission,omitempty"`
	FrozenPayload      string         `json:"frozenPayload,omitempty"`
	FirstOccurrenceAt  *time.Time     `json:"firstOccurrenceAt,omitempty"`
}

type FireScheduleResult struct {
	Fired            bool             `json:"fired"`
	OccurrenceID     uuid.UUID        `json:"occurrenceId"`
	OccurrenceStatus OccurrenceStatus `json:"occurrenceStatus"`
	DispatchedSagaID *uuid.UUID       `json:"dispatchedSagaId,omitempty"`
	Reason           string           `json:"reason"`
}

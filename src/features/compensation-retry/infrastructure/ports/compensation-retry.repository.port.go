package ports

import (
	"context"
	"time"

	"distributed-transaction-engine/src/features/compensation-retry/contracts"
	"github.com/google/uuid"
)

type RepositoryPort interface {
	UpsertAttempt(ctx context.Context, record contracts.CompensationRetryRecord) (*contracts.CompensationRetryRecord, error)
	GetRecord(ctx context.Context, tenantID string, sagaID uuid.UUID, stepID string) (*contracts.CompensationRetryRecord, error)
	UpdateStatus(ctx context.Context, tenantID string, sagaID uuid.UUID, stepID string, status string, nextRetryAt *time.Time, lastError string) error
	PauseStepType(ctx context.Context, tenantID string, stepType string) error
	ResumeStepType(ctx context.Context, tenantID string, stepType string) error
	IsStepTypePaused(ctx context.Context, tenantID string, stepType string) (bool, error)
}


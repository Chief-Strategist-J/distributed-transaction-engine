package ports

import (
	"context"
	"time"

	"distributed-transaction-engine/src/features/dead-letter-queue/contracts"

	"github.com/google/uuid"
)

type RepositoryPort interface {
	AdmitAtomic(ctx context.Context, record contracts.DeadLetterRecord, outboxPayload string) (*contracts.DeadLetterRecord, error)
	GetRecordByID(ctx context.Context, tenantID string, id uuid.UUID) (*contracts.DeadLetterRecord, error)
	UpdateRecordStatus(ctx context.Context, tenantID string, id uuid.UUID, expectedVersion int64, newStatus contracts.RecordStatus) (*contracts.DeadLetterRecord, error)
	ResolveRecord(ctx context.Context, tenantID string, id uuid.UUID, expectedVersion int64, disposition contracts.DispositionType, actorID, secondApproverID, reason, evidenceRef string, correctiveAttemptID *uuid.UUID, correctiveIdempotencyKey string) (*contracts.DeadLetterRecord, error)
	CountRecentByFingerprint(ctx context.Context, tenantID, fingerprint string, window time.Duration) (int, error)
	CreateSystemicIncident(ctx context.Context, incident contracts.SystemicIncident, pause contracts.StepTypePause) error
	IsStepTypePaused(ctx context.Context, tenantID, stepType string) (bool, error)
	ListRecords(ctx context.Context, tenantID string, status *contracts.RecordStatus, limit int) ([]contracts.DeadLetterRecord, error)
}


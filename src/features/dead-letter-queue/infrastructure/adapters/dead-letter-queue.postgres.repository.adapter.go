package adapters

import (
	"context"
	"database/sql"
	"distributed-transaction-engine/src/features/dead-letter-queue/contracts"
	"distributed-transaction-engine/src/features/dead-letter-queue/infrastructure/ports"
	"distributed-transaction-engine/src/features/dead-letter-queue/infrastructure/queries"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"fmt"
	"github.com/google/uuid"
	"strings"
	"time"
)

type PostgresDeadLetterRepositoryAdapter struct {
	db *sql.DB
}

var _ ports.RepositoryPort = (*PostgresDeadLetterRepositoryAdapter)(nil)

func NewPostgresDeadLetterRepositoryAdapter(db *sql.DB) *PostgresDeadLetterRepositoryAdapter {
	return &PostgresDeadLetterRepositoryAdapter{db: db}
}

func (a *PostgresDeadLetterRepositoryAdapter) AdmitAtomic(
	ctx context.Context,
	record contracts.DeadLetterRecord,
	outboxPayload string,
) (spanResult0 *contracts.DeadLetterRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, appErrors.NewInternalError("failed to begin transaction: " + err.Error())
	}
	defer tx.Rollback()

	var inserted contracts.DeadLetterRecord
	var disposition, actorID, secondApproverID, dispositionReason, evidenceRef, correctiveIdempotencyKey sql.NullString
	var correctiveAttemptID sql.NullString
	var businessDeadline sql.NullTime

	err = tx.QueryRowContext(
		ctx,
		queries.QueryInsertDeadLetterRecord,
		record.ID, record.PublicID, record.TenantID, record.Source, record.ExternalID, record.CorrelationID,
		record.SagaID, record.StepIndex, record.StepType, record.AttemptID, record.IdempotencyKey,
		string(record.OutcomeClass), string(record.FailureClass), record.ErrorFingerprint, record.SanitizedMessage,
		record.PayloadRef, record.SchemaVersion, record.MoneyAtStakeCents, record.Currency,
		record.BusinessDeadline,
	).Scan(
		&inserted.ID, &inserted.PublicID, &inserted.TenantID, &inserted.Source, &inserted.ExternalID, &inserted.CorrelationID,
		&inserted.SagaID, &inserted.StepIndex, &inserted.StepType, &inserted.AttemptID, &inserted.IdempotencyKey,
		&inserted.OutcomeClass, &inserted.FailureClass, &inserted.ErrorFingerprint, &inserted.SanitizedMessage,
		&inserted.PayloadRef, &inserted.SchemaVersion, &inserted.MoneyAtStakeCents, &inserted.Currency,
		&businessDeadline, &inserted.Status, &disposition, &actorID, &secondApproverID,
		&dispositionReason, &evidenceRef, &correctiveAttemptID, &correctiveIdempotencyKey,
		&inserted.Version, &inserted.CreatedAt, &inserted.UpdatedAt,
	)
	if err != nil {
		return nil, appErrors.NewConflictError("failed to insert dead letter record: " + err.Error())
	}

	_, err = tx.ExecContext(ctx, queries.QueryUpdateSagaNeedsAttention, record.TenantID, record.SagaID)
	if err != nil {
		return nil, appErrors.NewInternalError("failed to transition saga to NEEDS_ATTENTION: " + err.Error())
	}

	if outboxPayload != "" {
		outboxID := uuid.New()
		outboxPubID := fmt.Sprintf("oxe_%s", strings.ReplaceAll(outboxID.String(), "-", "")[:26])
		_, err = tx.ExecContext(
			ctx,
			queries.QueryInsertOutboxNotification,
			outboxID, outboxPubID, record.TenantID, record.SagaID,
			record.ID.String(), time.Now().UnixNano(),
			"saga.dead-letter-queue", record.SagaID.String(),
			outboxPayload,
		)
		if err != nil {
			return nil, appErrors.NewInternalError("failed to emit outbox event: " + err.Error())
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, appErrors.NewInternalError("failed to commit transaction: " + err.Error())
	}

	if businessDeadline.Valid {
		inserted.BusinessDeadline = &businessDeadline.Time
	}
	return &inserted, nil
}

func (a *PostgresDeadLetterRepositoryAdapter) GetRecordByID(
	ctx context.Context,
	tenantID string,
	id uuid.UUID,
) (spanResult0 *contracts.DeadLetterRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var rec contracts.DeadLetterRecord
	var disposition, actorID, secondApproverID, dispositionReason, evidenceRef, correctiveIdempotencyKey sql.NullString
	var correctiveAttemptID sql.NullString
	var businessDeadline sql.NullTime

	err := a.db.QueryRowContext(ctx, queries.QueryGetRecordByID, tenantID, id).Scan(
		&rec.ID, &rec.PublicID, &rec.TenantID, &rec.Source, &rec.ExternalID, &rec.CorrelationID,
		&rec.SagaID, &rec.StepIndex, &rec.StepType, &rec.AttemptID, &rec.IdempotencyKey,
		&rec.OutcomeClass, &rec.FailureClass, &rec.ErrorFingerprint, &rec.SanitizedMessage,
		&rec.PayloadRef, &rec.SchemaVersion, &rec.MoneyAtStakeCents, &rec.Currency,
		&businessDeadline, &rec.Status, &disposition, &actorID, &secondApproverID,
		&dispositionReason, &evidenceRef, &correctiveAttemptID, &correctiveIdempotencyKey,
		&rec.Version, &rec.CreatedAt, &rec.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, appErrors.NewNotFoundError("dead letter record not found")
	}
	if err != nil {
		return nil, appErrors.NewInternalError(err.Error())
	}

	if businessDeadline.Valid {
		rec.BusinessDeadline = &businessDeadline.Time
	}
	if disposition.Valid {
		disp := contracts.DispositionType(disposition.String)
		rec.Disposition = &disp
	}
	if actorID.Valid {
		rec.ActorID = &actorID.String
	}
	if secondApproverID.Valid {
		rec.SecondApproverID = &secondApproverID.String
	}
	if dispositionReason.Valid {
		rec.DispositionReason = &dispositionReason.String
	}
	if evidenceRef.Valid {
		rec.EvidenceRef = &evidenceRef.String
	}
	if correctiveAttemptID.Valid {
		parsed, _ := uuid.Parse(correctiveAttemptID.String)
		rec.CorrectiveAttemptID = &parsed
	}
	if correctiveIdempotencyKey.Valid {
		rec.CorrectiveIdempotencyKey = &correctiveIdempotencyKey.String
	}

	return &rec, nil
}

func (a *PostgresDeadLetterRepositoryAdapter) UpdateRecordStatus(
	ctx context.Context,
	tenantID string,
	id uuid.UUID,
	expectedVersion int64,
	newStatus contracts.RecordStatus,
) (spanResult0 *contracts.DeadLetterRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var rec contracts.DeadLetterRecord
	var disposition, actorID, secondApproverID, dispositionReason, evidenceRef, correctiveIdempotencyKey sql.NullString
	var correctiveAttemptID sql.NullString
	var businessDeadline sql.NullTime

	err := a.db.QueryRowContext(
		ctx,
		queries.QueryUpdateRecordStatus,
		string(newStatus), tenantID, id, expectedVersion,
	).Scan(
		&rec.ID, &rec.PublicID, &rec.TenantID, &rec.Source, &rec.ExternalID, &rec.CorrelationID,
		&rec.SagaID, &rec.StepIndex, &rec.StepType, &rec.AttemptID, &rec.IdempotencyKey,
		&rec.OutcomeClass, &rec.FailureClass, &rec.ErrorFingerprint, &rec.SanitizedMessage,
		&rec.PayloadRef, &rec.SchemaVersion, &rec.MoneyAtStakeCents, &rec.Currency,
		&businessDeadline, &rec.Status, &disposition, &actorID, &secondApproverID,
		&dispositionReason, &evidenceRef, &correctiveAttemptID, &correctiveIdempotencyKey,
		&rec.Version, &rec.CreatedAt, &rec.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, appErrors.NewPreconditionFailedError("optimistic concurrency conflict or record not found")
	}
	if err != nil {
		return nil, appErrors.NewInternalError(err.Error())
	}

	if businessDeadline.Valid {
		rec.BusinessDeadline = &businessDeadline.Time
	}
	return &rec, nil
}

func (a *PostgresDeadLetterRepositoryAdapter) ResolveRecord(
	ctx context.Context,
	tenantID string,
	id uuid.UUID,
	expectedVersion int64,
	disposition contracts.DispositionType,
	actorID string,
	secondApproverID string,
	reason string,
	evidenceRef string,
	correctiveAttemptID *uuid.UUID,
	correctiveIdempotencyKey string,
) (spanResult0 *contracts.DeadLetterRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var rec contracts.DeadLetterRecord
	var retDisp, retActor, retSecondApprover, retReason, retEvidence, retCorrKey sql.NullString
	var retCorrAttempt sql.NullString
	var businessDeadline sql.NullTime

	err := a.db.QueryRowContext(
		ctx,
		queries.QueryResolveRecord,
		string(disposition), actorID, secondApproverID, reason, evidenceRef,
		correctiveAttemptID, correctiveIdempotencyKey,
		tenantID, id, expectedVersion,
	).Scan(
		&rec.ID, &rec.PublicID, &rec.TenantID, &rec.Source, &rec.ExternalID, &rec.CorrelationID,
		&rec.SagaID, &rec.StepIndex, &rec.StepType, &rec.AttemptID, &rec.IdempotencyKey,
		&rec.OutcomeClass, &rec.FailureClass, &rec.ErrorFingerprint, &rec.SanitizedMessage,
		&rec.PayloadRef, &rec.SchemaVersion, &rec.MoneyAtStakeCents, &rec.Currency,
		&businessDeadline, &rec.Status, &retDisp, &retActor, &retSecondApprover,
		&retReason, &retEvidence, &retCorrAttempt, &retCorrKey,
		&rec.Version, &rec.CreatedAt, &rec.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, appErrors.NewPreconditionFailedError("optimistic concurrency conflict or record not found")
	}
	if err != nil {
		return nil, appErrors.NewInternalError(err.Error())
	}

	if businessDeadline.Valid {
		rec.BusinessDeadline = &businessDeadline.Time
	}
	if retDisp.Valid {
		disp := contracts.DispositionType(retDisp.String)
		rec.Disposition = &disp
	}
	return &rec, nil
}

func (a *PostgresDeadLetterRepositoryAdapter) CountRecentByFingerprint(
	ctx context.Context,
	tenantID string,
	fingerprint string,
	window time.Duration,
) (spanResult0 int, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	cutoff := time.Now().UTC().Add(-window)
	var count int
	err := a.db.QueryRowContext(ctx, queries.QueryCountRecentFingerprint, tenantID, fingerprint, cutoff).Scan(&count)
	if err != nil {
		return 0, appErrors.NewInternalError(err.Error())
	}
	return count, nil
}

func (a *PostgresDeadLetterRepositoryAdapter) CreateSystemicIncident(
	ctx context.Context,
	incident contracts.SystemicIncident,
	pause contracts.StepTypePause,
) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return appErrors.NewInternalError(err.Error())
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(
		ctx,
		queries.QueryInsertSystemicIncident,
		incident.ID, incident.PublicID, incident.TenantID, incident.Source, incident.ExternalID, incident.CorrelationID,
		incident.Fingerprint, incident.StepType, incident.AffectedCount, incident.OwnerTeam, incident.ReviewDeadline,
	)
	if err != nil {
		return appErrors.NewInternalError(err.Error())
	}

	_, err = tx.ExecContext(
		ctx,
		queries.QueryInsertStepTypePause,
		pause.TenantID, pause.StepType, pause.IncidentID, pause.ReviewDeadline, pause.OwnerTeam,
	)
	if err != nil {
		return appErrors.NewInternalError(err.Error())
	}

	return tx.Commit()
}

func (a *PostgresDeadLetterRepositoryAdapter) IsStepTypePaused(
	ctx context.Context,
	tenantID string,
	stepType string,
) (spanResult0 bool, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var count int
	err := a.db.QueryRowContext(ctx, queries.QueryCheckStepTypePaused, tenantID, stepType).Scan(&count)
	if err != nil {
		return false, appErrors.NewInternalError(err.Error())
	}
	return count > 0, nil
}

func (a *PostgresDeadLetterRepositoryAdapter) ListRecords(
	ctx context.Context,
	tenantID string,
	status *contracts.RecordStatus,
	limit int,
) (spanResult0 []contracts.DeadLetterRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var rows *sql.Rows
	var err error

	if status != nil {
		rows, err = a.db.QueryContext(ctx, queries.QueryListRecordsByStatus, tenantID, string(*status), limit)
	} else {
		rows, err = a.db.QueryContext(ctx, queries.QueryListRecords, tenantID, limit)
	}
	if err != nil {
		return nil, appErrors.NewInternalError(err.Error())
	}
	defer rows.Close()

	records := make([]contracts.DeadLetterRecord, 0)
	for rows.Next() {
		var rec contracts.DeadLetterRecord
		var disposition, actorID, secondApproverID, dispositionReason, evidenceRef, correctiveIdempotencyKey sql.NullString
		var correctiveAttemptID sql.NullString
		var businessDeadline sql.NullTime

		err := rows.Scan(
			&rec.ID, &rec.PublicID, &rec.TenantID, &rec.Source, &rec.ExternalID, &rec.CorrelationID,
			&rec.SagaID, &rec.StepIndex, &rec.StepType, &rec.AttemptID, &rec.IdempotencyKey,
			&rec.OutcomeClass, &rec.FailureClass, &rec.ErrorFingerprint, &rec.SanitizedMessage,
			&rec.PayloadRef, &rec.SchemaVersion, &rec.MoneyAtStakeCents, &rec.Currency,
			&businessDeadline, &rec.Status, &disposition, &actorID, &secondApproverID,
			&dispositionReason, &evidenceRef, &correctiveAttemptID, &correctiveIdempotencyKey,
			&rec.Version, &rec.CreatedAt, &rec.UpdatedAt,
		)
		if err != nil {
			return nil, appErrors.NewInternalError(err.Error())
		}
		if businessDeadline.Valid {
			rec.BusinessDeadline = &businessDeadline.Time
		}
		if disposition.Valid {
			disp := contracts.DispositionType(disposition.String)
			rec.Disposition = &disp
		}
		records = append(records, rec)
	}

	return records, nil
}

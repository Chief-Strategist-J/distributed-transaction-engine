package adapters

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"distributed-transaction-engine/src/features/optimistic-concurrency-control/contracts"
	"distributed-transaction-engine/src/features/optimistic-concurrency-control/infrastructure/ports"
	"distributed-transaction-engine/src/features/optimistic-concurrency-control/infrastructure/queries"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"strconv"
	"strings"
	"time"
)

const maxInboxExternalIDLength = 128

type PostgresOCCRepositoryAdapter struct {
	db *sql.DB
}

var _ ports.RepositoryPort = (*PostgresOCCRepositoryAdapter)(nil)

func NewPostgresOCCRepositoryAdapter(db *sql.DB) *PostgresOCCRepositoryAdapter {
	return &PostgresOCCRepositoryAdapter{db: db}
}

func (a *PostgresOCCRepositoryAdapter) ExecuteCAS(ctx context.Context, input contracts.CASMutationInput) (spanResult0 *contracts.CASResult, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, appErrors.NewInternalError("failed to start transaction: " + err.Error())
	}
	defer tx.Rollback()

	var id uuid.UUID
	var publicID, tenantID, currentState string
	var newVersion, ownerEpoch int64
	var updatedAt time.Time

	err = tx.QueryRowContext(
		ctx,
		queries.QueryExecuteCAS,
		input.TargetState, input.TenantID, input.SagaID, input.ExpectedVersion, input.ExpectedEpoch,
	).Scan(&id, &publicID, &tenantID, &currentState, &newVersion, &ownerEpoch, &updatedAt)

	if err == nil {
		for _, evt := range input.OutboxEvents {
			outboxID := uuid.New()
			outboxPubID := fmt.Sprintf("oxe_%s", strings.ReplaceAll(outboxID.String(), "-", "")[:26])
			_, err = tx.ExecContext(
				ctx,
				queries.QueryInsertOutbox,
				outboxID, outboxPubID, input.TenantID, input.SagaID,
				input.SagaID.String(), time.Now().UnixNano(),
				evt.Topic, input.SagaID.String(),
				evt.Payload, fencingHeaders(input.SagaID.String(), ownerEpoch),
			)
			if err != nil {
				return nil, appErrors.NewInternalError("failed to insert outbox event: " + err.Error())
			}
		}

		if err := tx.Commit(); err != nil {
			return nil, appErrors.NewInternalError("failed to commit CAS transaction: " + err.Error())
		}

		return &contracts.CASResult{
			Succeeded:      true,
			CurrentVersion: newVersion,
			CurrentEpoch:   ownerEpoch,
			CurrentState:   currentState,
		}, nil
	}

	if err != sql.ErrNoRows {
		return nil, appErrors.NewInternalError("unexpected database error during CAS: " + err.Error())
	}

	var diagID uuid.UUID
	var diagPublicID, diagTenantID, diagState string
	var diagVersion, diagEpoch int64
	var diagUpdatedAt time.Time

	diagErr := a.db.QueryRowContext(ctx, queries.QueryDiagnoseState, input.TenantID, input.SagaID).Scan(
		&diagID, &diagPublicID, &diagTenantID, &diagState, &diagVersion, &diagEpoch, &diagUpdatedAt,
	)

	if diagErr == sql.ErrNoRows {
		return &contracts.CASResult{
			Succeeded:    false,
			Cause:        contracts.CauseDataError,
			ErrorMessage: "saga root aggregate does not exist",
		}, nil
	}
	if diagErr != nil {
		return nil, appErrors.NewInternalError("failed to diagnose state: " + diagErr.Error())
	}

	cause := contracts.CauseStateConflict
	if diagTenantID != input.TenantID {
		cause = contracts.CauseSecurityViolation
	} else if diagEpoch != input.ExpectedEpoch {
		cause = contracts.CauseNotOwner
	} else if diagVersion != input.ExpectedVersion {
		cause = contracts.CauseVersionConflict
	}

	return &contracts.CASResult{
		Succeeded:      false,
		Cause:          cause,
		CurrentVersion: diagVersion,
		CurrentEpoch:   diagEpoch,
		CurrentState:   diagState,
		ErrorMessage:   fmt.Sprintf("CAS failed: diagnosis=%s", cause),
	}, nil
}

func (a *PostgresOCCRepositoryAdapter) DiagnoseState(ctx context.Context, tenantID string, sagaID uuid.UUID) (spanResult0 *contracts.SagaRootState, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var state contracts.SagaRootState
	err := a.db.QueryRowContext(ctx, queries.QueryDiagnoseState, tenantID, sagaID).Scan(
		&state.ID, &state.PublicID, &state.TenantID, &state.CurrentState,
		&state.Version, &state.OwnerEpoch, &state.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, appErrors.NewNotFoundError("saga root state not found")
	}
	if err != nil {
		return nil, appErrors.NewInternalError(err.Error())
	}
	return &state, nil
}

func (a *PostgresOCCRepositoryAdapter) ParkPendingInput(ctx context.Context, pending contracts.OCCPendingInput) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	_, err := a.db.ExecContext(
		ctx,
		queries.QueryParkPendingInput,
		pending.ID, pending.PublicID, pending.TenantID, pending.Source, pending.ExternalID, pending.CorrelationID,
		pending.SagaID, string(pending.InputType), pending.InputPayload, pending.AttemptCount, pending.LastConflictReason,
	)
	if err != nil {
		return appErrors.NewInternalError(err.Error())
	}
	return nil
}

func (a *PostgresOCCRepositoryAdapter) RecordContention(ctx context.Context, entry contracts.ContentionJournalEntry) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	_, err := a.db.ExecContext(
		ctx,
		queries.QueryRecordContention,
		entry.ID, entry.PublicID, entry.TenantID, entry.SagaID,
		entry.AttemptedVersion, entry.CurrentVersion, entry.AttemptedEpoch, entry.CurrentEpoch,
		string(entry.Diagnosis), entry.WorkerID,
	)
	if err != nil {
		return appErrors.NewInternalError(err.Error())
	}
	return nil
}

func inputExternalID(sagaID uuid.UUID, inputID string) string {
	external := sagaID.String() + ":" + inputID
	if len(external) <= maxInboxExternalIDLength {
		return external
	}
	digest := sha256.Sum256([]byte(external))
	return hex.EncodeToString(digest[:])
}

func (a *PostgresOCCRepositoryAdapter) IsInputApplied(ctx context.Context, tenantID string, sagaID uuid.UUID, inputID string) (spanResult0 bool, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var applied bool
	err := a.db.QueryRowContext(ctx, queries.QuerySelectInputApplied, tenantID, inputExternalID(sagaID, inputID)).Scan(&applied)
	if err != nil {
		return false, appErrors.NewInternalError("input dedupe lookup failed")
	}
	return applied, nil
}

func (a *PostgresOCCRepositoryAdapter) MarkInputApplied(ctx context.Context, tenantID string, sagaID uuid.UUID, inputID string) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	rawUUID := uuid.New()
	external := inputExternalID(sagaID, inputID)
	digest := sha256.Sum256([]byte(external))
	pubID := fmt.Sprintf("inb_%s", strings.ReplaceAll(rawUUID.String(), "-", "")[:26])
	if _, err := a.db.ExecContext(ctx, queries.QueryInsertInputApplied, rawUUID, pubID, tenantID, external, rawUUID, hex.EncodeToString(digest[:])); err != nil {
		return appErrors.NewInternalError("input dedupe record failed")
	}
	return nil
}

func (a *PostgresOCCRepositoryAdapter) GetRecentContentionCount(ctx context.Context, tenantID string, sagaID uuid.UUID, window time.Duration) (spanResult0 int, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	cutoff := time.Now().UTC().Add(-window)
	var count int
	err := a.db.QueryRowContext(ctx, queries.QueryCountRecentContention, tenantID, sagaID, cutoff).Scan(&count)
	if err != nil {
		return 0, appErrors.NewInternalError(err.Error())
	}
	return count, nil
}

func fencingHeaders(sagaID string, ownerEpoch int64) string {
	encoded, err := json.Marshal(map[string]string{
		strings.ToLower(constants.HeaderXFencingToken): strconv.FormatInt(ownerEpoch, 10),
		strings.ToLower(constants.HeaderXSagaID):       sagaID,
	})
	if err != nil {
		return "{}"
	}
	return string(encoded)
}

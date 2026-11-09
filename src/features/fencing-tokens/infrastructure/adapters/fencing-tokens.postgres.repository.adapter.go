package adapters

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"distributed-transaction-engine/src/features/fencing-tokens/contracts"
	"distributed-transaction-engine/src/features/fencing-tokens/infrastructure/ports"
	"distributed-transaction-engine/src/features/fencing-tokens/infrastructure/queries"
	appErrors "distributed-transaction-engine/src/shared/errors"

	"github.com/google/uuid"
)

type DBExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type PostgresFencingRepositoryAdapter struct {
	db DBExecutor
}

var _ ports.RepositoryPort = (*PostgresFencingRepositoryAdapter)(nil)

func NewPostgresFencingRepositoryAdapter(db DBExecutor) *PostgresFencingRepositoryAdapter {
	return &PostgresFencingRepositoryAdapter{db: db}
}

func (a *PostgresFencingRepositoryAdapter) AcquireOrTakeover(ctx context.Context, tenantID, resourceID, holderID string, durationMs int) (*contracts.FencingLease, error) {
	rawUUID := uuid.New()
	pubID := fmt.Sprintf("fnl_%s", strings.ReplaceAll(rawUUID.String(), "-", "")[:26])

	var lease contracts.FencingLease
	err := a.db.QueryRowContext(
		ctx,
		queries.QueryAcquireOrTakeoverLease,
		rawUUID, pubID, tenantID, "ENGINE_LEASING", resourceID, rawUUID,
		resourceID, holderID, durationMs,
	).Scan(
		&lease.ID, &lease.PublicID, &lease.TenantID, &lease.Source,
		&lease.ExternalID, &lease.CorrelationID, &lease.ResourceID,
		&lease.HolderID, &lease.Epoch, &lease.LeaseDurationMs,
		&lease.AcquiredAt, &lease.ExpiresAt, &lease.ReleasedAt,
		&lease.CreatedAt, &lease.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, appErrors.NewConflictError("lease currently held by active worker")
	}
	if err != nil {
		return nil, appErrors.NewInternalError(err.Error())
	}

	return &lease, nil
}

func (a *PostgresFencingRepositoryAdapter) Renew(ctx context.Context, tenantID, resourceID, holderID string, epoch int64, durationMs int) (*contracts.FencingLease, error) {
	var lease contracts.FencingLease
	err := a.db.QueryRowContext(
		ctx,
		queries.QueryRenewLease,
		durationMs, tenantID, resourceID, holderID, epoch,
	).Scan(
		&lease.ID, &lease.PublicID, &lease.TenantID, &lease.Source,
		&lease.ExternalID, &lease.CorrelationID, &lease.ResourceID,
		&lease.HolderID, &lease.Epoch, &lease.LeaseDurationMs,
		&lease.AcquiredAt, &lease.ExpiresAt, &lease.ReleasedAt,
		&lease.CreatedAt, &lease.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, appErrors.NewConflictError("renewal failed: lease expired or owned by another epoch")
	}
	if err != nil {
		return nil, appErrors.NewInternalError(err.Error())
	}

	return &lease, nil
}

func (a *PostgresFencingRepositoryAdapter) Release(ctx context.Context, tenantID, resourceID, holderID string, epoch int64) error {
	res, err := a.db.ExecContext(ctx, queries.QueryReleaseLease, tenantID, resourceID, holderID, epoch)
	if err != nil {
		return appErrors.NewInternalError(err.Error())
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return appErrors.NewInternalError(err.Error())
	}
	if rows == 0 {
		return appErrors.NewConflictError("cannot release lease owned by another holder or epoch")
	}
	return nil
}

func (a *PostgresFencingRepositoryAdapter) GetLease(ctx context.Context, tenantID, resourceID string) (*contracts.FencingLease, error) {
	var lease contracts.FencingLease
	err := a.db.QueryRowContext(ctx, queries.QueryGetLease, tenantID, resourceID).Scan(
		&lease.ID, &lease.PublicID, &lease.TenantID, &lease.Source,
		&lease.ExternalID, &lease.CorrelationID, &lease.ResourceID,
		&lease.HolderID, &lease.Epoch, &lease.LeaseDurationMs,
		&lease.AcquiredAt, &lease.ExpiresAt, &lease.ReleasedAt,
		&lease.CreatedAt, &lease.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, appErrors.NewNotFoundError("lease not found")
	}
	if err != nil {
		return nil, appErrors.NewInternalError(err.Error())
	}

	return &lease, nil
}

func (a *PostgresFencingRepositoryAdapter) ExecuteFencedBarrier(ctx context.Context, tenantID, resourceID string, token int64) (*contracts.FencingDecision, error) {
	var updatedToken int64
	err := a.db.QueryRowContext(ctx, queries.QueryFencedBarrier, token, tenantID, resourceID).Scan(&updatedToken)

	if err == nil {
		return &contracts.FencingDecision{
			Allowed:        true,
			StoredToken:    updatedToken,
			AttemptedToken: token,
			Reason:         "fencing barrier check passed",
		}, nil
	}

	if err == sql.ErrNoRows {
		var currentStoredToken int64
		tokenErr := a.db.QueryRowContext(ctx, queries.QueryGetSagaToken, tenantID, resourceID).Scan(&currentStoredToken)
		if tokenErr != nil {
			return nil, appErrors.NewNotFoundError("saga resource not found")
		}

		return &contracts.FencingDecision{
			Allowed:        false,
			StoredToken:    currentStoredToken,
			AttemptedToken: token,
			Reason:         "stale fencing token; zombie write suppressed",
			ViolationType:  "STALE_TOKEN",
		}, nil
	}

	return nil, appErrors.NewInternalError(err.Error())
}

func (a *PostgresFencingRepositoryAdapter) RecordAudit(ctx context.Context, audit contracts.FencingAudit) error {
	_, err := a.db.ExecContext(
		ctx,
		queries.QueryRecordAudit,
		audit.ID, audit.PublicID, audit.TenantID, audit.ResourceID,
		audit.AttemptedToken, audit.CurrentToken, audit.RejectedHolder,
		audit.ViolationType, audit.RecordedAt,
	)
	if err != nil {
		return appErrors.NewInternalError(err.Error())
	}
	return nil
}

func (a *PostgresFencingRepositoryAdapter) GetAudits(ctx context.Context, tenantID, resourceID string, limit int) ([]contracts.FencingAudit, error) {
	rows, err := a.db.QueryContext(ctx, queries.QueryGetAudits, tenantID, resourceID, limit)
	if err != nil {
		return nil, appErrors.NewInternalError(err.Error())
	}
	defer rows.Close()

	audits := make([]contracts.FencingAudit, 0)
	for rows.Next() {
		var audit contracts.FencingAudit
		if err := rows.Scan(
			&audit.ID, &audit.PublicID, &audit.TenantID, &audit.ResourceID,
			&audit.AttemptedToken, &audit.CurrentToken, &audit.RejectedHolder,
			&audit.ViolationType, &audit.RecordedAt,
		); err != nil {
			return nil, appErrors.NewInternalError(err.Error())
		}
		audits = append(audits, audit)
	}

	if err := rows.Err(); err != nil {
		return nil, appErrors.NewInternalError(err.Error())
	}

	return audits, nil
}

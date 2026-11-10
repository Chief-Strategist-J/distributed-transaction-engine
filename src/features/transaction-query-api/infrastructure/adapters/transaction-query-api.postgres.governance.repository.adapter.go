package adapters

import (
	"context"
	"database/sql"
	contracts "distributed-transaction-engine/src/features/transaction-query-api/contracts"
	"distributed-transaction-engine/src/features/transaction-query-api/infrastructure/queries"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"errors"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"math"
)

type PostgresTransactionGovernanceRepositoryAdapter struct {
	view *PostgresTransactionViewRepositoryAdapter
}

func NewPostgresTransactionGovernanceRepositoryAdapter(db DBTransactor) *PostgresTransactionGovernanceRepositoryAdapter {
	return &PostgresTransactionGovernanceRepositoryAdapter{view: NewPostgresTransactionViewRepositoryAdapter(db)}
}

func (a *PostgresTransactionGovernanceRepositoryAdapter) inTenantTransaction(ctx context.Context, tenantID string, work func(DBExecutor) error) error {
	return a.view.runWrite(ctx, func(exec DBExecutor) error {
		if _, err := exec.ExecContext(ctx, queries.FLOW_SET_TENANT_CONTEXT, tenantID); err != nil {
			return mapViewError(err)
		}
		return work(exec)
	})
}

func (a *PostgresTransactionGovernanceRepositoryAdapter) CreateAuditRecord(ctx context.Context, record contracts.AuditRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	return a.inTenantTransaction(ctx, record.TenantID, func(exec DBExecutor) error {
		_, err := exec.ExecContext(ctx, queries.FLOW_INSERT_TQ_AUDIT_RECORD,
			record.ID, record.PublicID, record.TenantID, record.PrincipalID, record.Audience, record.Action,
			string(record.NormalizedAST), record.ResultCount, record.Purpose, pq.Array(nonNilStrings(record.ClassesTouched)),
			record.Impersonated, record.CorrelationID, record.OccurredAt)
		return mapViewError(err)
	})
}

func (a *PostgresTransactionGovernanceRepositoryAdapter) UpdateRateBucketTokens(ctx context.Context, tenantID string, bucketKey string, cost int, capacity int, refillPerSecond int) (spanResult0 contracts.RateDecision, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if cost > capacity {
		cost = capacity
	}
	decision := contracts.RateDecision{Allowed: true}
	err := a.inTenantTransaction(ctx, tenantID, func(exec DBExecutor) error {
		var remaining float64
		scanErr := exec.QueryRowContext(ctx, queries.FLOW_UPDATE_TQ_RATE_BUCKET_CONSUME, tenantID, bucketKey, capacity, cost, refillPerSecond).Scan(&remaining)
		if scanErr == nil {
			return nil
		}
		if !errors.Is(scanErr, sql.ErrNoRows) {
			return mapViewError(scanErr)
		}
		return denyRate(ctx, exec, tenantID, bucketKey, cost, capacity, refillPerSecond, &decision)
	})
	return decision, err
}

func denyRate(ctx context.Context, exec DBExecutor, tenantID string, bucketKey string, cost int, capacity int, refillPerSecond int, decision *contracts.RateDecision) error {
	var tokens, elapsed float64
	if err := exec.QueryRowContext(ctx, queries.FLOW_SELECT_TQ_RATE_BUCKET, tenantID, bucketKey).Scan(&tokens, &elapsed); err != nil {
		return mapViewError(err)
	}
	available := math.Min(float64(capacity), tokens+elapsed*float64(refillPerSecond))
	wait := int(math.Ceil((float64(cost) - available) / float64(refillPerSecond)))
	decision.Allowed, decision.RetryAfterSeconds = false, maxInt(wait, 1)
	return nil
}

func maxInt(first int, second int) int {
	if first > second {
		return first
	}
	return second
}

func (a *PostgresTransactionGovernanceRepositoryAdapter) CreateSavedQuery(ctx context.Context, record contracts.SavedQuery) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	return a.inTenantTransaction(ctx, record.TenantID, func(exec DBExecutor) error {
		_, err := exec.ExecContext(ctx, queries.FLOW_INSERT_TQ_SAVED_QUERY,
			record.ID, record.PublicID, record.TenantID, record.OwnerID, record.Visibility, record.TeamRef, record.Name,
			record.TQLVersion, record.RegistryVersion, string(record.QueryJSON), record.Status, record.BrokenReason,
			record.Version, record.CreatedAt, record.UpdatedAt)
		return mapViewError(err)
	})
}

func scanSavedQuery(scanner interface{ Scan(...any) error }) (contracts.SavedQuery, error) {
	var record contracts.SavedQuery
	err := scanner.Scan(&record.ID, &record.PublicID, &record.TenantID, &record.OwnerID, &record.Visibility, &record.TeamRef,
		&record.Name, &record.TQLVersion, &record.RegistryVersion, &record.QueryJSON, &record.Status, &record.BrokenReason,
		&record.Version, &record.CreatedAt, &record.UpdatedAt)
	return record, err
}

func (a *PostgresTransactionGovernanceRepositoryAdapter) FindSavedQuery(ctx context.Context, tenantID string, publicID string) (spanResult0 *contracts.SavedQuery, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var found *contracts.SavedQuery
	err := a.inTenantTransaction(ctx, tenantID, func(exec DBExecutor) error {
		record, scanErr := scanSavedQuery(exec.QueryRowContext(ctx, queries.FLOW_SELECT_TQ_SAVED_QUERY_BY_PUBLIC_ID, tenantID, publicID))
		if errors.Is(scanErr, sql.ErrNoRows) {
			return nil
		}
		if scanErr != nil {
			return mapViewError(scanErr)
		}
		found = &record
		return nil
	})
	return found, err
}

func (a *PostgresTransactionGovernanceRepositoryAdapter) ListSavedQueries(ctx context.Context, tenantID string, limit int) (spanResult0 []contracts.SavedQuery, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	listed := make([]contracts.SavedQuery, 0)
	err := a.inTenantTransaction(ctx, tenantID, func(exec DBExecutor) error {
		rows, queryErr := exec.QueryContext(ctx, queries.FLOW_SELECT_TQ_SAVED_QUERIES_BY_TENANT, tenantID, limit)
		if queryErr != nil {
			return mapViewError(queryErr)
		}
		defer rows.Close()
		for rows.Next() {
			record, scanErr := scanSavedQuery(rows)
			if scanErr != nil {
				return mapViewError(scanErr)
			}
			listed = append(listed, record)
		}
		return mapViewError(rows.Err())
	})
	return listed, err
}

func (a *PostgresTransactionGovernanceRepositoryAdapter) UpdateSavedQueryStatus(ctx context.Context, record contracts.SavedQuery, expectedVersion int64) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	return a.inTenantTransaction(ctx, record.TenantID, func(exec DBExecutor) error {
		result, err := exec.ExecContext(ctx, queries.FLOW_UPDATE_TQ_SAVED_QUERY_STATUS,
			record.TenantID, record.ID, record.Status, record.BrokenReason, record.RegistryVersion, expectedVersion)
		if err != nil {
			return mapViewError(err)
		}
		if affected, _ := result.RowsAffected(); affected == 0 {
			return appErrors.NewPreconditionFailedError("saved query version does not match the expected version")
		}
		return nil
	})
}

func (a *PostgresTransactionGovernanceRepositoryAdapter) CreateExportJob(ctx context.Context, job contracts.ExportJob) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	return a.inTenantTransaction(ctx, job.TenantID, func(exec DBExecutor) error {
		_, err := exec.ExecContext(ctx, queries.FLOW_INSERT_TQ_EXPORT_JOB,
			job.ID, job.PublicID, job.TenantID, job.PrincipalID, job.Status, string(job.QueryJSON), string(job.PrincipalJSON),
			pq.Array(nonNilStrings(job.Fields)), job.TQLVersion, job.AsOfSequence, job.RowCount, job.FailureCode,
			job.CorrelationID, job.Version, job.CreatedAt, job.CompletedAt, job.ExpiresAt)
		return mapViewError(err)
	})
}

func (a *PostgresTransactionGovernanceRepositoryAdapter) FindExportJob(ctx context.Context, tenantID string, publicID string) (spanResult0 *contracts.ExportJob, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var found *contracts.ExportJob
	err := a.inTenantTransaction(ctx, tenantID, func(exec DBExecutor) error {
		var job contracts.ExportJob
		var fields pq.StringArray
		scanErr := exec.QueryRowContext(ctx, queries.FLOW_SELECT_TQ_EXPORT_JOB_BY_PUBLIC_ID, tenantID, publicID).Scan(
			&job.ID, &job.PublicID, &job.TenantID, &job.PrincipalID, &job.Status, &job.QueryJSON, &job.PrincipalJSON, &fields,
			&job.TQLVersion, &job.AsOfSequence, &job.RowCount, &job.FailureCode, &job.CorrelationID, &job.Version,
			&job.CreatedAt, &job.CompletedAt, &job.ExpiresAt)
		if errors.Is(scanErr, sql.ErrNoRows) {
			return nil
		}
		if scanErr != nil {
			return mapViewError(scanErr)
		}
		job.Fields, found = []string(fields), &job
		return nil
	})
	return found, err
}

func (a *PostgresTransactionGovernanceRepositoryAdapter) UpdateExportJob(ctx context.Context, job contracts.ExportJob, expectedVersion int64) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	return a.inTenantTransaction(ctx, job.TenantID, func(exec DBExecutor) error {
		result, err := exec.ExecContext(ctx, queries.FLOW_UPDATE_TQ_EXPORT_JOB,
			job.TenantID, job.ID, job.Status, job.RowCount, job.FailureCode, job.CompletedAt, expectedVersion)
		if err != nil {
			return mapExportUpdateError(err)
		}
		if affected, _ := result.RowsAffected(); affected == 0 {
			return appErrors.NewPreconditionFailedError("export job version does not match the expected version")
		}
		return nil
	})
}

func mapExportUpdateError(err error) error {
	var pgErr *pq.Error
	if errors.As(err, &pgErr) && pgErr.Constraint == "tq_export_job_status_transition_ck" {
		return appErrors.NewConflictError("export status transition is not allowed")
	}
	return mapViewError(err)
}

func (a *PostgresTransactionGovernanceRepositoryAdapter) CreateExportChunk(ctx context.Context, tenantID string, chunk contracts.ExportChunk) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	return a.inTenantTransaction(ctx, tenantID, func(exec DBExecutor) error {
		_, err := exec.ExecContext(ctx, queries.FLOW_INSERT_TQ_EXPORT_CHUNK, tenantID, chunk.JobID, chunk.Sequence, chunk.Ciphertext, chunk.RowCount)
		return mapViewError(err)
	})
}

func (a *PostgresTransactionGovernanceRepositoryAdapter) ListExportChunks(ctx context.Context, tenantID string, jobID uuid.UUID) (spanResult0 []contracts.ExportChunk, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	chunks := make([]contracts.ExportChunk, 0)
	err := a.inTenantTransaction(ctx, tenantID, func(exec DBExecutor) error {
		rows, queryErr := exec.QueryContext(ctx, queries.FLOW_SELECT_TQ_EXPORT_CHUNKS_BY_JOB, tenantID, jobID)
		if queryErr != nil {
			return mapViewError(queryErr)
		}
		defer rows.Close()
		for rows.Next() {
			var chunk contracts.ExportChunk
			if scanErr := rows.Scan(&chunk.JobID, &chunk.Sequence, &chunk.Ciphertext, &chunk.RowCount); scanErr != nil {
				return mapViewError(scanErr)
			}
			chunks = append(chunks, chunk)
		}
		return mapViewError(rows.Err())
	})
	return chunks, err
}

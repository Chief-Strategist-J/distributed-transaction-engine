package adapters

import (
	"context"
	"database/sql"
	contracts "distributed-transaction-engine/src/features/tenant-isolation/contracts"
	"distributed-transaction-engine/src/features/tenant-isolation/infrastructure/queries"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"errors"
	"github.com/lib/pq"
	"sort"
	"strings"
	"time"
)

const (
	immutableConstraintSuffix  = "_immutable_ck"
	appendOnlyConstraintSuffix = "_append_only_ck"
	uniqueViolationState       = "23505"
	quarantineSourceConstraint = "tenant_message_quarantine_source_uk"
)

type DBExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type DBTransactor interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

var constraintErrorRegistry = map[string]*appErrors.ApplicationError{
	"pk_tenant_message_quarantine":                    appErrors.NewConflictError("record already exists"),
	"pk_tenant_registry":                              appErrors.NewConflictError("record already exists"),
	"pk_tenant_saga_quota":                            appErrors.NewConflictError("record already exists"),
	"pk_tenant_security_event":                        appErrors.NewConflictError("record already exists"),
	"pk_tenant_status":                                appErrors.NewConflictError("record already exists"),
	"pk_tenant_status_transition":                     appErrors.NewConflictError("record already exists"),
	"pk_tenant_tombstone":                             appErrors.NewConflictError("record already exists"),
	"tenant_message_quarantine_envelope_tenant_id_ck": appErrors.NewValidationError("value violates the storage contract"),
	"tenant_message_quarantine_payload_sha256_ck":     appErrors.NewValidationError("value violates the storage contract"),
	"tenant_message_quarantine_public_id_ck":          appErrors.NewValidationError("value violates the storage contract"),
	"tenant_message_quarantine_public_id_uk":          appErrors.NewConflictError("record already exists"),
	"tenant_message_quarantine_reason_ck":             appErrors.NewValidationError("value violates the storage contract"),
	"tenant_message_quarantine_resolved_ck":           appErrors.NewValidationError("value violates the storage contract"),
	"tenant_message_quarantine_source_offset_ck":      appErrors.NewValidationError("value violates the storage contract"),
	"tenant_message_quarantine_source_partition_ck":   appErrors.NewValidationError("value violates the storage contract"),
	"tenant_message_quarantine_source_topic_ck":       appErrors.NewValidationError("value violates the storage contract"),
	"tenant_message_quarantine_source_uk":             appErrors.NewConflictError("record already exists"),
	"tenant_message_quarantine_status_ck":             appErrors.NewValidationError("value violates the storage contract"),
	"tenant_message_quarantine_version_ck":            appErrors.NewValidationError("value violates the storage contract"),
	"tenant_registry_initial_status_ck":               appErrors.NewValidationError("value violates the storage contract"),
	"tenant_registry_max_concurrent_sagas_ck":         appErrors.NewValidationError("value violates the storage contract"),
	"tenant_registry_public_id_ck":                    appErrors.NewValidationError("value violates the storage contract"),
	"tenant_registry_public_id_uk":                    appErrors.NewConflictError("record already exists"),
	"tenant_registry_status_fk":                       appErrors.NewValidationError("referenced tenant or status does not exist"),
	"tenant_registry_status_transition_ck":            appErrors.NewConflictError("tenant status transition is not allowed"),
	"tenant_registry_suspension_reason_ck":            appErrors.NewValidationError("value violates the storage contract"),
	"tenant_registry_suspension_reason_value_ck":      appErrors.NewValidationError("value violates the storage contract"),
	"tenant_registry_tenant_id_format_ck":             appErrors.NewValidationError("value violates the storage contract"),
	"tenant_registry_tenant_id_tombstone_ck":          appErrors.NewConflictError("tenant id was offboarded and can never be reissued"),
	"tenant_registry_tenant_id_uk":                    appErrors.NewConflictError("record already exists"),
	"tenant_registry_version_ck":                      appErrors.NewValidationError("value violates the storage contract"),
	"tenant_saga_quota_active_saga_count_ck":          appErrors.NewValidationError("value violates the storage contract"),
	"tenant_saga_quota_tenant_id_fk":                  appErrors.NewValidationError("referenced tenant or status does not exist"),
	"tenant_saga_quota_version_ck":                    appErrors.NewValidationError("value violates the storage contract"),
	"tenant_security_event_actor_tenant_id_fk":        appErrors.NewValidationError("referenced tenant or status does not exist"),
	"tenant_security_event_id_uk":                     appErrors.NewConflictError("record already exists"),
	"tenant_security_event_public_id_ck":              appErrors.NewValidationError("value violates the storage contract"),
	"tenant_security_event_public_id_uk":              appErrors.NewConflictError("record already exists"),
	"tenant_security_event_resource_id_ck":            appErrors.NewValidationError("value violates the storage contract"),
	"tenant_security_event_resource_kind_ck":          appErrors.NewValidationError("value violates the storage contract"),
	"tenant_security_event_type_ck":                   appErrors.NewValidationError("value violates the storage contract"),
	"tenant_status_code_ck":                           appErrors.NewValidationError("value violates the storage contract"),
	"tenant_status_transition_distinct_ck":            appErrors.NewValidationError("value violates the storage contract"),
	"tenant_status_transition_from_status_fk":         appErrors.NewValidationError("referenced tenant or status does not exist"),
	"tenant_status_transition_to_status_fk":           appErrors.NewValidationError("referenced tenant or status does not exist"),
	"tenant_tombstone_tenant_id_fk":                   appErrors.NewValidationError("referenced tenant or status does not exist"),
}

type PostgresTenantIsolationRepositoryAdapter struct {
	db DBTransactor
}

func NewPostgresTenantIsolationRepositoryAdapter(db DBTransactor) *PostgresTenantIsolationRepositoryAdapter {
	return &PostgresTenantIsolationRepositoryAdapter{db: db}
}

func ListRegisteredConstraintNames() []string {
	defer observability.TraceEnter(nil)()
	names := make([]string, 0, len(constraintErrorRegistry))
	for name := range constraintErrorRegistry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (a *PostgresTenantIsolationRepositoryAdapter) CreateTenant(ctx context.Context, record contracts.TenantRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	return a.runInTransaction(ctx, func(exec DBExecutor) error {
		if _, err := exec.ExecContext(ctx, queries.FLOW_INSERT_TENANT_REGISTRY,
			record.ID, record.PublicID, record.TenantID, record.CorrelationID, record.Status, record.SuspensionReason,
			record.MaxConcurrentSagas, record.StatusChangedAt, record.Version, record.CreatedAt, record.UpdatedAt); err != nil {
			return mapPostgresError(err)
		}
		_, err := exec.ExecContext(ctx, queries.FLOW_INSERT_TENANT_SAGA_QUOTA, record.TenantID, record.CreatedAt)
		return mapPostgresError(err)
	})
}

func (a *PostgresTenantIsolationRepositoryAdapter) FindTenantByTenantID(ctx context.Context, tenantID string) (spanResult0 *contracts.TenantRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var found *contracts.TenantRecord
	err := a.runInTenantTransaction(ctx, tenantID, func(exec DBExecutor) error {
		record, scanErr := scanTenantRecord(exec.QueryRowContext(ctx, queries.FLOW_SELECT_TENANT_REGISTRY_BY_TENANT, tenantID))
		if errors.Is(scanErr, sql.ErrNoRows) {
			return nil
		}
		if scanErr != nil {
			return mapPostgresError(scanErr)
		}
		found = &record
		return nil
	})
	return found, err
}

func (a *PostgresTenantIsolationRepositoryAdapter) UpdateTenantStatus(ctx context.Context, input contracts.UpdateTenantStatusInput) (spanResult0 contracts.TenantRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var updated contracts.TenantRecord
	err := a.runInTransaction(ctx, func(exec DBExecutor) error {
		record, scanErr := scanTenantRecord(exec.QueryRowContext(ctx, queries.FLOW_UPDATE_TENANT_REGISTRY_STATUS,
			input.TenantID, input.ExpectedVersion, input.TargetStatus, input.SuspensionReason))
		if errors.Is(scanErr, sql.ErrNoRows) {
			return appErrors.NewPreconditionFailedError("tenant version does not match the expected version")
		}
		updated = record
		return mapPostgresError(scanErr)
	})
	return updated, err
}

func (a *PostgresTenantIsolationRepositoryAdapter) FindSagaQuota(ctx context.Context, tenantID string) (spanResult0 *contracts.SagaQuotaRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var found *contracts.SagaQuotaRecord
	err := a.runInTenantTransaction(ctx, tenantID, func(exec DBExecutor) error {
		record, scanErr := scanSagaQuotaRecord(exec.QueryRowContext(ctx, queries.FLOW_SELECT_TENANT_SAGA_QUOTA, tenantID))
		if errors.Is(scanErr, sql.ErrNoRows) {
			return nil
		}
		if scanErr != nil {
			return mapPostgresError(scanErr)
		}
		found = &record
		return nil
	})
	return found, err
}

func (a *PostgresTenantIsolationRepositoryAdapter) CreateSagaSlotReservation(ctx context.Context, tenantID string) (spanResult0 *contracts.SagaQuotaReservation, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var reservation *contracts.SagaQuotaReservation
	err := a.runInTenantTransaction(ctx, tenantID, func(exec DBExecutor) error {
		reserved := contracts.SagaQuotaReservation{TenantID: tenantID}
		scanErr := exec.QueryRowContext(ctx, queries.FLOW_RESERVE_TENANT_SAGA_SLOT, tenantID, contracts.TenantStatusActive).
			Scan(&reserved.ActiveSagaCount, &reserved.MaxConcurrentSagas)
		if errors.Is(scanErr, sql.ErrNoRows) {
			return nil
		}
		if scanErr != nil {
			return mapPostgresError(scanErr)
		}
		reservation = &reserved
		return nil
	})
	return reservation, err
}

func (a *PostgresTenantIsolationRepositoryAdapter) DeleteSagaSlotReservation(ctx context.Context, tenantID string) (spanResult0 bool, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	released := false
	err := a.runInTenantTransaction(ctx, tenantID, func(exec DBExecutor) error {
		var remaining int64
		scanErr := exec.QueryRowContext(ctx, queries.FLOW_RELEASE_TENANT_SAGA_SLOT, tenantID).Scan(&remaining)
		if errors.Is(scanErr, sql.ErrNoRows) {
			return nil
		}
		if scanErr != nil {
			return mapPostgresError(scanErr)
		}
		released = true
		return nil
	})
	return released, err
}

func (a *PostgresTenantIsolationRepositoryAdapter) UpdateSagaQuotaCounter(ctx context.Context, input contracts.UpdateSagaQuotaCounterInput) (spanResult0 contracts.SagaQuotaRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var updated contracts.SagaQuotaRecord
	err := a.runInTenantTransaction(ctx, input.TenantID, func(exec DBExecutor) error {
		record, scanErr := scanSagaQuotaRecord(exec.QueryRowContext(ctx, queries.FLOW_UPDATE_TENANT_SAGA_QUOTA_COUNTER,
			input.TenantID, input.ExpectedVersion, input.ObservedActiveSagas))
		if errors.Is(scanErr, sql.ErrNoRows) {
			return appErrors.NewPreconditionFailedError("quota version does not match the expected version")
		}
		updated = record
		return mapPostgresError(scanErr)
	})
	return updated, err
}

func (a *PostgresTenantIsolationRepositoryAdapter) CreateQuarantineRecord(ctx context.Context, record contracts.QuarantineRecord) (spanResult0 bool, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	created := false
	err := a.runInTransaction(ctx, func(exec DBExecutor) error {
		_, execErr := exec.ExecContext(ctx, queries.FLOW_INSERT_TENANT_MESSAGE_QUARANTINE,
			record.ID, record.PublicID, record.CorrelationID, record.SourceTopic, record.SourcePartition, record.SourceOffset,
			record.Reason, record.Status, record.EnvelopeTenantID, record.KeyTenantID, record.PayloadTenantID, record.AggregateTenantID,
			record.PayloadSHA256, record.ResolvedAt, record.Version, record.CreatedAt, record.UpdatedAt)
		if execErr != nil {
			return mapPostgresError(execErr)
		}
		created = true
		return nil
	})
	if isAlreadyQuarantined(err) {
		return false, nil
	}
	return created, err
}

func (a *PostgresTenantIsolationRepositoryAdapter) ListQuarantineRecordsByStatus(ctx context.Context, status contracts.QuarantineStatus, limit int) (spanResult0 []contracts.QuarantineRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var listed []contracts.QuarantineRecord
	err := a.runInTransaction(ctx, func(exec DBExecutor) error {
		rows, queryErr := exec.QueryContext(ctx, queries.FLOW_SELECT_TENANT_MESSAGE_QUARANTINE_BY_STATUS, status, limit)
		if queryErr != nil {
			return mapPostgresError(queryErr)
		}
		var scanErr error
		listed, scanErr = scanQuarantineRows(rows)
		return scanErr
	})
	return listed, err
}

func (a *PostgresTenantIsolationRepositoryAdapter) UpdateQuarantineRelease(ctx context.Context, tenantID string, limit int, resolvedAt time.Time) (spanResult0 []contracts.QuarantineRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var released []contracts.QuarantineRecord
	err := a.runInTransaction(ctx, func(exec DBExecutor) error {
		rows, queryErr := exec.QueryContext(ctx, queries.FLOW_UPDATE_TENANT_MESSAGE_QUARANTINE_RELEASE_BY_TENANT,
			contracts.QuarantineStatusPending, contracts.QuarantineReasonUnknownTenant, tenantID, limit,
			contracts.QuarantineStatusReleased, resolvedAt)
		if queryErr != nil {
			return mapPostgresError(queryErr)
		}
		var scanErr error
		released, scanErr = scanQuarantineRows(rows)
		return scanErr
	})
	return released, err
}

func (a *PostgresTenantIsolationRepositoryAdapter) CreateSecurityEvent(ctx context.Context, record contracts.SecurityEventRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	return a.runInTenantTransaction(ctx, record.ActorTenantID, func(exec DBExecutor) error {
		_, err := exec.ExecContext(ctx, queries.FLOW_INSERT_TENANT_SECURITY_EVENT,
			record.ID, record.PublicID, record.ActorTenantID, record.ReferencedTenantID, record.CorrelationID,
			record.EventType, record.ResourceKind, record.ResourceID, record.OccurredAt, record.CreatedAt)
		return mapPostgresError(err)
	})
}

func (a *PostgresTenantIsolationRepositoryAdapter) runInTenantTransaction(ctx context.Context, tenantID string, work func(DBExecutor) error) error {
	return a.runInTransaction(ctx, func(exec DBExecutor) error {
		if _, err := exec.ExecContext(ctx, queries.FLOW_SET_TENANT_CONTEXT, tenantID); err != nil {
			return mapPostgresError(err)
		}
		return work(exec)
	})
}

func (a *PostgresTenantIsolationRepositoryAdapter) runInTransaction(ctx context.Context, work func(DBExecutor) error) error {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return mapPostgresError(err)
	}
	if workErr := work(tx); workErr != nil {
		_ = tx.Rollback()
		return workErr
	}
	return mapPostgresError(tx.Commit())
}

func mapPostgresError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pq.Error
	if !errors.As(err, &pgErr) {
		return appErrors.NewInternalError("persistence operation failed")
	}
	if mapped, registered := constraintErrorRegistry[pgErr.Constraint]; registered {
		return mapped
	}
	if strings.HasSuffix(pgErr.Constraint, immutableConstraintSuffix) || strings.HasSuffix(pgErr.Constraint, appendOnlyConstraintSuffix) {
		return appErrors.NewConflictError("record is immutable")
	}
	if pgErr.Code == uniqueViolationState {
		return appErrors.NewConflictError("record already exists")
	}
	return appErrors.NewInternalError("persistence operation failed")
}

func isAlreadyQuarantined(err error) bool {
	var applicationError *appErrors.ApplicationError
	return errors.As(err, &applicationError) && applicationError == constraintErrorRegistry[quarantineSourceConstraint]
}

func scanTenantRecord(row *sql.Row) (contracts.TenantRecord, error) {
	var record contracts.TenantRecord
	err := row.Scan(&record.ID, &record.PublicID, &record.TenantID, &record.CorrelationID, &record.Status,
		&record.SuspensionReason, &record.MaxConcurrentSagas, &record.StatusChangedAt, &record.Version,
		&record.CreatedAt, &record.UpdatedAt)
	return record, err
}

func scanSagaQuotaRecord(row *sql.Row) (contracts.SagaQuotaRecord, error) {
	var record contracts.SagaQuotaRecord
	err := row.Scan(&record.TenantID, &record.ActiveSagaCount, &record.Version, &record.CreatedAt, &record.UpdatedAt)
	return record, err
}

func scanQuarantineRows(rows *sql.Rows) ([]contracts.QuarantineRecord, error) {
	defer rows.Close()
	records := make([]contracts.QuarantineRecord, 0)
	for rows.Next() {
		var record contracts.QuarantineRecord
		if err := rows.Scan(&record.ID, &record.PublicID, &record.CorrelationID, &record.SourceTopic, &record.SourcePartition,
			&record.SourceOffset, &record.Reason, &record.Status, &record.EnvelopeTenantID, &record.KeyTenantID,
			&record.PayloadTenantID, &record.AggregateTenantID, &record.PayloadSHA256, &record.ResolvedAt,
			&record.Version, &record.CreatedAt, &record.UpdatedAt); err != nil {
			return nil, mapPostgresError(err)
		}
		records = append(records, record)
	}
	sort.Slice(records, func(i int, j int) bool {
		if !records[i].CreatedAt.Equal(records[j].CreatedAt) {
			return records[i].CreatedAt.Before(records[j].CreatedAt)
		}
		return records[i].ID.String() < records[j].ID.String()
	})
	return records, mapPostgresError(rows.Err())
}

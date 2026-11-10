package adapters

import (
	"context"
	"database/sql"
	"distributed-transaction-engine/src/features/audit-trail/contracts"
	"distributed-transaction-engine/src/features/audit-trail/domain/rules"
	"distributed-transaction-engine/src/features/audit-trail/infrastructure/queries"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"time"
)

type AuditTrailPostgresRepositoryAdapter struct {
	db *sql.DB
}

func NewAuditTrailPostgresRepositoryAdapter(db *sql.DB) *AuditTrailPostgresRepositoryAdapter {
	return &AuditTrailPostgresRepositoryAdapter{db: db}
}

func (a *AuditTrailPostgresRepositoryAdapter) AppendOutbox(ctx context.Context, record *contracts.OutboxEventRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	payloadBytes, err := json.Marshal(record.Payload)
	if err != nil {
		return appErrors.NewInternalError("failed to marshal outbox payload: " + err.Error())
	}
	_, err = a.db.ExecContext(ctx, queries.FLOW_APPEND_OUTBOX,
		record.ID, record.PublicID, record.TenantID, record.Source, record.ExternalID,
		record.CorrelationID, record.AuditID, record.OccurredAt, record.Action,
		record.ActorID, record.ActorIP, record.OnBehalfOf, record.TargetResource,
		payloadBytes, string(record.Status), record.CreatedAt,
	)
	if err != nil {
		return appErrors.NewInternalError("failed to append outbox record: " + err.Error())
	}
	return nil
}

func (a *AuditTrailPostgresRepositoryAdapter) GetPendingOutbox(ctx context.Context, tenantID string, limit int) (spanResult0 []contracts.OutboxEventRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	rows, err := a.db.QueryContext(ctx, queries.FLOW_GET_PENDING_OUTBOX, tenantID, limit)
	if err != nil {
		return nil, appErrors.NewInternalError("failed to get pending outbox: " + err.Error())
	}
	defer rows.Close()

	var results []contracts.OutboxEventRecord
	for rows.Next() {
		var rec contracts.OutboxEventRecord
		var rawPayload []byte
		var statusStr string
		var chainedAt sql.NullTime

		if err := rows.Scan(
			&rec.ID, &rec.PublicID, &rec.TenantID, &rec.Source, &rec.ExternalID,
			&rec.CorrelationID, &rec.AuditID, &rec.OccurredAt, &rec.Action,
			&rec.ActorID, &rec.ActorIP, &rec.OnBehalfOf, &rec.TargetResource,
			&rawPayload, &statusStr, &rec.CreatedAt, &chainedAt,
		); err != nil {
			return nil, appErrors.NewInternalError("failed to scan outbox row: " + err.Error())
		}

		rec.Status = contracts.OutboxStatus(statusStr)
		if chainedAt.Valid {
			rec.ChainedAt = &chainedAt.Time
		}
		if len(rawPayload) > 0 {
			observability.NoteSwallowed(json.Unmarshal(rawPayload, &rec.Payload))
		}
		results = append(results, rec)
	}
	return results, nil
}

func (a *AuditTrailPostgresRepositoryAdapter) MarkOutboxChained(ctx context.Context, tenantID string, ids []uuid.UUID, chainedAt time.Time) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	idStrings := make([]string, len(ids))
	for i, id := range ids {
		idStrings[i] = id.String()
	}
	_, err := a.db.ExecContext(ctx, queries.FLOW_MARK_OUTBOX_CHAINED, chainedAt, tenantID, pq.Array(idStrings))
	if err != nil {
		return appErrors.NewInternalError("failed to mark outbox chained: " + err.Error())
	}
	return nil
}

func (a *AuditTrailPostgresRepositoryAdapter) GetTailEvent(ctx context.Context, tenantID string, segmentID uuid.UUID) (spanResult0 *contracts.AuditEventRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	row := a.db.QueryRowContext(ctx, queries.FLOW_GET_TAIL_EVENT, tenantID, segmentID)
	var rec contracts.AuditEventRecord
	var rawPayload []byte

	err := row.Scan(
		&rec.ID, &rec.PublicID, &rec.TenantID, &rec.Source, &rec.ExternalID,
		&rec.CorrelationID, &rec.AuditID, &rec.SegmentID, &rec.SequenceID,
		&rec.KeyID, &rec.ActorID, &rec.ActorIP, &rec.Action,
		&rec.PrevHash, &rec.CurrHash, &rawPayload, &rec.OccurredAt, &rec.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, appErrors.NewInternalError("failed to get tail event: " + err.Error())
	}
	if len(rawPayload) > 0 {
		observability.NoteSwallowed(json.Unmarshal(rawPayload, &rec.RawPayload))
	}
	return &rec, nil
}

func (a *AuditTrailPostgresRepositoryAdapter) AppendChainedEvent(ctx context.Context, record *contracts.AuditEventRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	payloadBytes, err := json.Marshal(record.RawPayload)
	if err != nil {
		return appErrors.NewInternalError("failed to marshal raw payload: " + err.Error())
	}
	_, err = a.db.ExecContext(ctx, queries.FLOW_APPEND_CHAINED_EVENT,
		record.ID, record.PublicID, record.TenantID, record.Source, record.ExternalID,
		record.CorrelationID, record.AuditID, record.SegmentID, record.SequenceID,
		record.KeyID, record.ActorID, record.ActorIP, record.Action,
		record.PrevHash, record.CurrHash, payloadBytes, record.OccurredAt, record.CreatedAt,
	)
	if err != nil {
		return appErrors.NewInternalError("failed to append chained audit event: " + err.Error())
	}
	return nil
}

func (a *AuditTrailPostgresRepositoryAdapter) GetEventsBySegment(ctx context.Context, tenantID string, segmentID uuid.UUID, limit int) (spanResult0 []contracts.AuditEventRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	rows, err := a.db.QueryContext(ctx, queries.FLOW_GET_EVENTS_BY_SEGMENT, tenantID, segmentID, limit)
	if err != nil {
		return nil, appErrors.NewInternalError("failed to get events by segment: " + err.Error())
	}
	defer rows.Close()

	var results []contracts.AuditEventRecord
	for rows.Next() {
		var rec contracts.AuditEventRecord
		var rawPayload []byte
		if err := rows.Scan(
			&rec.ID, &rec.PublicID, &rec.TenantID, &rec.Source, &rec.ExternalID,
			&rec.CorrelationID, &rec.AuditID, &rec.SegmentID, &rec.SequenceID,
			&rec.KeyID, &rec.ActorID, &rec.ActorIP, &rec.Action,
			&rec.PrevHash, &rec.CurrHash, &rawPayload, &rec.OccurredAt, &rec.CreatedAt,
		); err != nil {
			return nil, appErrors.NewInternalError("failed to scan audit event: " + err.Error())
		}
		if len(rawPayload) > 0 {
			observability.NoteSwallowed(json.Unmarshal(rawPayload, &rec.RawPayload))
		}
		results = append(results, rec)
	}
	return results, nil
}

func (a *AuditTrailPostgresRepositoryAdapter) GetEventsByTenant(ctx context.Context, query contracts.ListAuditEventsQuery) (spanResult0 []contracts.AuditEventRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	rows, err := a.db.QueryContext(ctx, queries.FLOW_GET_EVENTS_BY_TENANT, query.TenantID, query.Limit)
	if err != nil {
		return nil, appErrors.NewInternalError("failed to list audit events: " + err.Error())
	}
	defer rows.Close()

	var results []contracts.AuditEventRecord
	for rows.Next() {
		var rec contracts.AuditEventRecord
		var rawPayload []byte
		if err := rows.Scan(
			&rec.ID, &rec.PublicID, &rec.TenantID, &rec.Source, &rec.ExternalID,
			&rec.CorrelationID, &rec.AuditID, &rec.SegmentID, &rec.SequenceID,
			&rec.KeyID, &rec.ActorID, &rec.ActorIP, &rec.Action,
			&rec.PrevHash, &rec.CurrHash, &rawPayload, &rec.OccurredAt, &rec.CreatedAt,
		); err != nil {
			return nil, appErrors.NewInternalError("failed to scan audit event: " + err.Error())
		}
		if len(rawPayload) > 0 {
			observability.NoteSwallowed(json.Unmarshal(rawPayload, &rec.RawPayload))
		}
		if query.MaskPII {
			rec.RawPayload = rules.MaskSensitivePayload(rec.RawPayload)
		}
		results = append(results, rec)
	}
	return results, nil
}

func (a *AuditTrailPostgresRepositoryAdapter) IsAuditIDChained(ctx context.Context, tenantID string, auditID string) (spanResult0 bool, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var exists bool
	err := a.db.QueryRowContext(ctx, queries.FLOW_CHECK_AUDIT_ID_CHAINED, tenantID, auditID).Scan(&exists)
	if err != nil {
		return false, appErrors.NewInternalError("failed to check audit_id: " + err.Error())
	}
	return exists, nil
}

func (a *AuditTrailPostgresRepositoryAdapter) AppendQuarantine(ctx context.Context, record *contracts.AuditQuarantineRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	rawBytes, err := json.Marshal(record.RawRecord)
	if err != nil {
		return appErrors.NewInternalError("failed to marshal quarantine record: " + err.Error())
	}
	_, err = a.db.ExecContext(ctx, queries.FLOW_APPEND_QUARANTINE,
		record.ID, record.PublicID, record.TenantID, record.Source, record.ExternalID,
		record.CorrelationID, record.AuditID, string(record.QuarantineReason),
		rawBytes, record.ErrorDetails, record.Resolved, record.CreatedAt,
	)
	if err != nil {
		return appErrors.NewInternalError("failed to append quarantine record: " + err.Error())
	}
	return nil
}

func (a *AuditTrailPostgresRepositoryAdapter) GetQuarantinedEvents(ctx context.Context, tenantID string, limit int) (spanResult0 []contracts.AuditQuarantineRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	rows, err := a.db.QueryContext(ctx, queries.FLOW_GET_QUARANTINE_EVENTS, tenantID, limit)
	if err != nil {
		return nil, appErrors.NewInternalError("failed to get quarantine events: " + err.Error())
	}
	defer rows.Close()

	var results []contracts.AuditQuarantineRecord
	for rows.Next() {
		var rec contracts.AuditQuarantineRecord
		var rawBytes []byte
		var reasonStr string
		if err := rows.Scan(
			&rec.ID, &rec.PublicID, &rec.TenantID, &rec.Source, &rec.ExternalID,
			&rec.CorrelationID, &rec.AuditID, &reasonStr, &rawBytes,
			&rec.ErrorDetails, &rec.Resolved, &rec.CreatedAt,
		); err != nil {
			return nil, appErrors.NewInternalError("failed to scan quarantine row: " + err.Error())
		}
		rec.QuarantineReason = contracts.QuarantineReason(reasonStr)
		if len(rawBytes) > 0 {
			observability.NoteSwallowed(json.Unmarshal(rawBytes, &rec.RawRecord))
		}
		results = append(results, rec)
	}
	return results, nil
}

func (a *AuditTrailPostgresRepositoryAdapter) SaveCheckpoint(ctx context.Context, record *contracts.AuditCheckpointRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	_, err := a.db.ExecContext(ctx, queries.FLOW_SAVE_CHECKPOINT,
		record.ID, record.PublicID, record.TenantID, record.Source, record.ExternalID,
		record.CorrelationID, record.SegmentID, record.LastSequenceID, record.HeadHash,
		record.Signature, record.KeyID, record.EventCount, record.CreatedAt,
	)
	if err != nil {
		return appErrors.NewInternalError("failed to save checkpoint: " + err.Error())
	}
	return nil
}

func (a *AuditTrailPostgresRepositoryAdapter) GetLatestCheckpoint(ctx context.Context, tenantID string, segmentID uuid.UUID) (spanResult0 *contracts.AuditCheckpointRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	row := a.db.QueryRowContext(ctx, queries.FLOW_GET_LATEST_CHECKPOINT, tenantID, segmentID)
	var rec contracts.AuditCheckpointRecord
	err := row.Scan(
		&rec.ID, &rec.PublicID, &rec.TenantID, &rec.Source, &rec.ExternalID,
		&rec.CorrelationID, &rec.SegmentID, &rec.LastSequenceID, &rec.HeadHash,
		&rec.Signature, &rec.KeyID, &rec.EventCount, &rec.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, appErrors.NewInternalError("failed to get latest checkpoint: " + err.Error())
	}
	return &rec, nil
}

func (a *AuditTrailPostgresRepositoryAdapter) RecordVerification(ctx context.Context, record *contracts.AuditVerificationRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var failSeq sql.NullInt64
	if record.FailureSequenceID != nil {
		failSeq.Int64 = *record.FailureSequenceID
		failSeq.Valid = true
	}
	_, err := a.db.ExecContext(ctx, queries.FLOW_RECORD_VERIFICATION,
		record.ID, record.PublicID, record.TenantID, record.Source, record.ExternalID,
		record.CorrelationID, record.SegmentID, record.VerifiedUpToSequence, record.TotalEventsChecked,
		string(record.Status), failSeq, record.FailureReason, record.VerifiedBy, record.CreatedAt,
	)
	if err != nil {
		return appErrors.NewInternalError("failed to record verification: " + err.Error())
	}
	return nil
}

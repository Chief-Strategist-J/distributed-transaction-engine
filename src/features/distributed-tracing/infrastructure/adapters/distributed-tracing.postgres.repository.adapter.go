package adapters

import (
	"context"
	"database/sql"
	"distributed-transaction-engine/src/features/distributed-tracing/contracts"
	"distributed-transaction-engine/src/features/distributed-tracing/infrastructure/queries"
	"distributed-transaction-engine/src/infra/observability"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"encoding/json"
	"github.com/google/uuid"
)

type TracingPostgresRepositoryAdapter struct {
	db *sql.DB
}

func NewTracingPostgresRepositoryAdapter(db *sql.DB) *TracingPostgresRepositoryAdapter {
	return &TracingPostgresRepositoryAdapter{db: db}
}

func (a *TracingPostgresRepositoryAdapter) SaveDeepSession(ctx context.Context, record *contracts.DeepSessionRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	_, err := a.db.ExecContext(ctx, queries.FLOW_SAVE_DEEP_SESSION,
		record.ID, record.PublicID, record.TenantID, record.Source, record.ExternalID,
		record.CorrelationID, string(record.TargetType), record.TargetValue,
		record.SamplingRate, record.MaxSpans, record.ExpiresAt, string(record.Status), record.CreatedAt,
	)
	if err != nil {
		return appErrors.NewInternalError("failed to save deep session: " + err.Error())
	}
	return nil
}

func (a *TracingPostgresRepositoryAdapter) GetActiveDeepSession(ctx context.Context, tenantID string, targetType contracts.DeepTargetType, targetValue string) (spanResult0 *contracts.DeepSessionRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	row := a.db.QueryRowContext(ctx, queries.FLOW_GET_ACTIVE_DEEP_SESSION, tenantID, string(targetType), targetValue)
	var rec contracts.DeepSessionRecord
	var tType string
	var statusStr string

	err := row.Scan(
		&rec.ID, &rec.PublicID, &rec.TenantID, &rec.Source, &rec.ExternalID,
		&rec.CorrelationID, &tType, &rec.TargetValue, &rec.SamplingRate,
		&rec.MaxSpans, &rec.ExpiresAt, &statusStr, &rec.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, appErrors.NewInternalError("failed to get active deep session: " + err.Error())
	}
	rec.TargetType = contracts.DeepTargetType(tType)
	rec.Status = contracts.DeepSessionStatus(statusStr)
	return &rec, nil
}

func (a *TracingPostgresRepositoryAdapter) UpdateDeepSessionStatus(ctx context.Context, tenantID string, id uuid.UUID, status contracts.DeepSessionStatus) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	_, err := a.db.ExecContext(ctx, queries.FLOW_UPDATE_DEEP_SESSION_STATUS, string(status), tenantID, id)
	if err != nil {
		return appErrors.NewInternalError("failed to update deep session status: " + err.Error())
	}
	return nil
}

func (a *TracingPostgresRepositoryAdapter) SaveSamplingRule(ctx context.Context, record *contracts.SamplingRuleRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	criteriaBytes, err := json.Marshal(record.MatchCriteria)
	if err != nil {
		return appErrors.NewInternalError("failed to marshal match criteria: " + err.Error())
	}
	_, err = a.db.ExecContext(ctx, queries.FLOW_SAVE_SAMPLING_RULE,
		record.ID, record.PublicID, record.TenantID, record.Source, record.ExternalID,
		record.CorrelationID, record.RuleName, criteriaBytes, record.SampleRate,
		record.IsVIP, record.Enabled, record.CreatedAt,
	)
	if err != nil {
		return appErrors.NewInternalError("failed to save sampling rule: " + err.Error())
	}
	return nil
}

func (a *TracingPostgresRepositoryAdapter) GetSamplingRule(ctx context.Context, tenantID string) (spanResult0 *contracts.SamplingRuleRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	row := a.db.QueryRowContext(ctx, queries.FLOW_GET_SAMPLING_RULE, tenantID)
	var rec contracts.SamplingRuleRecord
	var criteriaBytes []byte

	err := row.Scan(
		&rec.ID, &rec.PublicID, &rec.TenantID, &rec.Source, &rec.ExternalID,
		&rec.CorrelationID, &rec.RuleName, &criteriaBytes, &rec.SampleRate,
		&rec.IsVIP, &rec.Enabled, &rec.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, appErrors.NewInternalError("failed to get sampling rule: " + err.Error())
	}
	if len(criteriaBytes) > 0 {
		observability.NoteSwallowed(json.Unmarshal(criteriaBytes, &rec.MatchCriteria))
	}
	return &rec, nil
}

func (a *TracingPostgresRepositoryAdapter) SaveSpan(ctx context.Context, record *contracts.SpanRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	attrsBytes, err := json.Marshal(record.Attributes)
	if err != nil {
		return appErrors.NewInternalError("failed to marshal span attributes: " + err.Error())
	}
	eventsBytes, err := json.Marshal(record.Events)
	if err != nil {
		return appErrors.NewInternalError("failed to marshal span events: " + err.Error())
	}
	linksBytes, err := json.Marshal(record.Links)
	if err != nil {
		return appErrors.NewInternalError("failed to marshal span links: " + err.Error())
	}

	_, err = a.db.ExecContext(ctx, queries.FLOW_SAVE_SPAN,
		record.ID, record.PublicID, record.TenantID, record.Source, record.ExternalID,
		record.CorrelationID, record.TraceID, record.SpanID, record.ParentSpanID,
		record.SpanName, string(record.SpanKind), record.StartTime, record.EndTime,
		record.DurationNS, string(record.StatusCode), attrsBytes, eventsBytes, linksBytes, record.CreatedAt,
	)
	if err != nil {
		return appErrors.NewInternalError("failed to save span record: " + err.Error())
	}
	return nil
}

func (a *TracingPostgresRepositoryAdapter) GetSpansByTraceID(ctx context.Context, tenantID string, traceID string) (spanResult0 []contracts.SpanRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	rows, err := a.db.QueryContext(ctx, queries.FLOW_GET_SPANS_BY_TRACE, tenantID, traceID)
	if err != nil {
		return nil, appErrors.NewInternalError("failed to get spans by trace: " + err.Error())
	}
	defer rows.Close()

	var results []contracts.SpanRecord
	for rows.Next() {
		var rec contracts.SpanRecord
		var kindStr string
		var statusStr string
		var attrsBytes []byte
		var eventsBytes []byte
		var linksBytes []byte

		if err := rows.Scan(
			&rec.ID, &rec.PublicID, &rec.TenantID, &rec.Source, &rec.ExternalID,
			&rec.CorrelationID, &rec.TraceID, &rec.SpanID, &rec.ParentSpanID,
			&rec.SpanName, &kindStr, &rec.StartTime, &rec.EndTime,
			&rec.DurationNS, &statusStr, &attrsBytes, &eventsBytes, &linksBytes, &rec.CreatedAt,
		); err != nil {
			return nil, appErrors.NewInternalError("failed to scan span row: " + err.Error())
		}

		rec.SpanKind = contracts.SpanKind(kindStr)
		rec.StatusCode = contracts.SpanStatusCode(statusStr)
		if len(attrsBytes) > 0 {
			observability.NoteSwallowed(json.Unmarshal(attrsBytes, &rec.Attributes))
		}
		if len(eventsBytes) > 0 {
			observability.NoteSwallowed(json.Unmarshal(eventsBytes, &rec.Events))
		}
		if len(linksBytes) > 0 {
			observability.NoteSwallowed(json.Unmarshal(linksBytes, &rec.Links))
		}
		results = append(results, rec)
	}
	return results, nil
}

func (a *TracingPostgresRepositoryAdapter) CountSpansByTraceID(ctx context.Context, tenantID string, traceID string) (spanResult0 int, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var count int
	err := a.db.QueryRowContext(ctx, queries.FLOW_COUNT_SPANS_BY_TRACE, tenantID, traceID).Scan(&count)
	if err != nil {
		return 0, appErrors.NewInternalError("failed to count spans: " + err.Error())
	}
	return count, nil
}

func (a *TracingPostgresRepositoryAdapter) SaveAsyncLink(ctx context.Context, record *contracts.AsyncLinkRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	_, err := a.db.ExecContext(ctx, queries.FLOW_SAVE_ASYNC_LINK,
		record.ID, record.PublicID, record.TenantID, record.Source, record.ExternalID,
		record.CorrelationID, record.SagaID, record.StepName, record.Traceparent,
		record.Tracestate, string(record.LinkType), record.CreatedAt,
	)
	if err != nil {
		return appErrors.NewInternalError("failed to save async link: " + err.Error())
	}
	return nil
}

func (a *TracingPostgresRepositoryAdapter) GetAsyncLink(ctx context.Context, tenantID string, sagaID uuid.UUID, stepName string, linkType contracts.AsyncLinkType) (spanResult0 *contracts.AsyncLinkRecord, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	row := a.db.QueryRowContext(ctx, queries.FLOW_GET_ASYNC_LINK, tenantID, sagaID, stepName, string(linkType))
	var rec contracts.AsyncLinkRecord
	var linkTypeStr string

	err := row.Scan(
		&rec.ID, &rec.PublicID, &rec.TenantID, &rec.Source, &rec.ExternalID,
		&rec.CorrelationID, &rec.SagaID, &rec.StepName, &rec.Traceparent,
		&rec.Tracestate, &linkTypeStr, &rec.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, appErrors.NewInternalError("failed to get async link: " + err.Error())
	}
	rec.LinkType = contracts.AsyncLinkType(linkTypeStr)
	return &rec, nil
}

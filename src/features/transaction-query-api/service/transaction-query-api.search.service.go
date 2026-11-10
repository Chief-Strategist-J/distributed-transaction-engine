package service

import (
	"context"

	contracts "distributed-transaction-engine/src/features/transaction-query-api/contracts"
	"distributed-transaction-engine/src/features/transaction-query-api/domain/rules"
	"distributed-transaction-engine/src/features/transaction-query-api/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
)

type SearchServiceDeps struct {
	Pipeline ports.QueryPipelineServicePort
	View     ports.TransactionViewRepositoryPort
	Tracer   *observability.DefaultTracer
}

type SearchService struct {
	pipeline ports.QueryPipelineServicePort
	view     ports.TransactionViewRepositoryPort
	tracer   *observability.DefaultTracer
}

func NewSearchService(deps SearchServiceDeps) *SearchService {
	return &SearchService{pipeline: deps.Pipeline, view: deps.View, tracer: deps.Tracer}
}

func (s *SearchService) SearchTransactions(ctx context.Context, principal contracts.Principal, request contracts.SearchRequest) (spanResult0 contracts.SearchResponse, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	return s.SearchTransactionsWithAction(ctx, principal, request, contracts.AuditSearch)
}

func (s *SearchService) SearchTransactionsWithAction(ctx context.Context, principal contracts.Principal, request contracts.SearchRequest, action contracts.AuditAction) (response contracts.SearchResponse, err error) {
	spanCtx, span := s.tracer.StartSpan(ctx, "transaction_query.search_transactions")
	defer func() { observability.RecordError(span, err); span.End() }()
	if err = contracts.ValidateSearchRequestShape(request); err != nil {
		return contracts.SearchResponse{}, err
	}
	prepared, err := s.pipeline.PrepareQuery(spanCtx, principal, contracts.PrepareInput{
		TQLVersion: request.TQLVersion, Text: request.Text, AST: request.AST, Params: request.Params, Fields: request.Fields,
		Cursor: request.Cursor, Consistency: request.Consistency, ConsistencyToken: request.ConsistencyToken,
		TenantID: request.TenantID, Purpose: request.Purpose,
	})
	if err != nil {
		return contracts.SearchResponse{}, err
	}
	return s.executeSearch(spanCtx, prepared, request, action)
}

func (s *SearchService) executeSearch(ctx context.Context, prepared contracts.PreparedQuery, request contracts.SearchRequest, action contracts.AuditAction) (contracts.SearchResponse, error) {
	if prepared.Normalized.Contradiction {
		return s.finishSearch(ctx, prepared, nil, false, nil, action)
	}
	estimate, err := s.pipeline.EvaluatePreparedEstimate(ctx, prepared)
	if err != nil {
		return contracts.SearchResponse{}, err
	}
	var keyset *contracts.KeysetPosition
	if prepared.CursorClaims != nil {
		keyset = &contracts.KeysetPosition{Keys: prepared.Normalized.OrderBy, Last: prepared.CursorClaims.LastValues}
	}
	rows, err := s.pipeline.ListPage(ctx, prepared, keyset, prepared.Normalized.Limit)
	if err != nil {
		return contracts.SearchResponse{}, err
	}
	hasNext := len(rows) > prepared.Normalized.Limit
	if hasNext {
		rows = rows[:prepared.Normalized.Limit]
	}
	count, err := s.resolveCount(ctx, prepared, request.Count, estimate)
	if err != nil {
		return contracts.SearchResponse{}, err
	}
	return s.finishSearch(ctx, prepared, rows, hasNext, count, action)
}

func (s *SearchService) resolveCount(ctx context.Context, prepared contracts.PreparedQuery, mode contracts.CountMode, estimate int64) (*contracts.CountResult, error) {
	if mode == "" || mode == contracts.CountNone {
		return nil, nil
	}
	if prepared.Store == contracts.StoreSearch {
		return nil, nil
	}
	if mode == contracts.CountEstimate {
		return &contracts.CountResult{Mode: contracts.CountEstimate, Value: estimate}, nil
	}
	if !rules.ExactCountAllowed(estimate) {
		return nil, appErrors.NewTypedError(422, constants.TQErrQueryTooExpensive, "an exact count would scan too many rows, use count ESTIMATE")
	}
	total, err := s.view.CountTransactions(ctx, contracts.QueryPlan{
		Store: prepared.Store, Injected: prepared.Injected, Filter: prepared.Normalized.Filter, AsOfSequence: prepared.AsOfSequence,
	})
	return &contracts.CountResult{Mode: contracts.CountExact, Value: total}, err
}

func (s *SearchService) finishSearch(ctx context.Context, prepared contracts.PreparedQuery, rows []contracts.Row, hasNext bool, count *contracts.CountResult, action contracts.AuditAction) (contracts.SearchResponse, error) {
	meta, err := s.pipeline.GetFreshness(ctx, prepared)
	if err != nil {
		return contracts.SearchResponse{}, err
	}
	items := make([]contracts.Row, 0, len(rows))
	for _, row := range rows {
		items = append(items, rules.ProjectRow(prepared.Registry, prepared.Fields, row))
	}
	meta.Count = count
	if count != nil && count.Mode == contracts.CountEstimate {
		meta.Warnings = append(meta.Warnings, "count is a planner estimate")
	}
	if hasNext {
		cursor, cursorErr := s.pipeline.CreateCursor(prepared, rows[len(rows)-1])
		if cursorErr != nil {
			return contracts.SearchResponse{}, cursorErr
		}
		meta.NextCursor = &cursor
	}
	resultCount := int64(len(items))
	if err = s.pipeline.CreateAuditEntry(ctx, prepared, action, &resultCount); err != nil {
		return contracts.SearchResponse{}, err
	}
	return contracts.SearchResponse{Items: items, Meta: meta}, nil
}

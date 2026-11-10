package adapters

import (
	"context"
	contracts "distributed-transaction-engine/src/features/transaction-query-api/contracts"
	"distributed-transaction-engine/src/features/transaction-query-api/infrastructure/queries"
	"distributed-transaction-engine/src/infra/observability"
	"github.com/lib/pq"
)

type PostgresTransactionSearchRepositoryAdapter struct {
	view *PostgresTransactionViewRepositoryAdapter
}

func NewPostgresTransactionSearchRepositoryAdapter(db DBTransactor) *PostgresTransactionSearchRepositoryAdapter {
	return &PostgresTransactionSearchRepositoryAdapter{view: NewPostgresTransactionViewRepositoryAdapter(db)}
}

func compileSearchList(plan contracts.QueryPlan) (string, []any, []string, error) {
	scope := sqlScope{columns: queries.SearchColumns, alias: "d", search: true}
	fields := mergeSortFields([]string{"id"}, plan.OrderBy)
	names, selectList, err := selectFieldList(scope, queries.SearchColumns, fields)
	if err != nil {
		return "", nil, nil, err
	}
	builder := &sqlBuilder{}
	where, err := compileWhere(builder, scope, plan.Injected, plan.Filter, plan.AsOfSequence, plan.Keyset)
	if err != nil {
		return "", nil, nil, err
	}
	order, err := compileOrderBy(scope, plan.OrderBy)
	if err != nil {
		return "", nil, nil, err
	}
	return "SELECT " + selectList + " FROM " + queries.TableSearch + " d WHERE " + where + order + " LIMIT " + builder.bind(plan.Limit+1), builder.args, names, nil
}

func CompileSearchListQuery(plan contracts.QueryPlan) (spanResult0 string, spanResult1 []any, spanResult2 []string, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	return compileSearchList(plan)
}

func (a *PostgresTransactionSearchRepositoryAdapter) ListSearchMatches(ctx context.Context, plan contracts.QueryPlan) (spanResult0 []contracts.Row, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	statement, args, names, err := compileSearchList(plan)
	if err != nil {
		return nil, err
	}
	var result []contracts.Row
	err = a.view.runReadOnly(ctx, plan.Injected.TenantID, func(exec DBExecutor) error {
		rows, queryErr := exec.QueryContext(ctx, statement, args...)
		if queryErr != nil {
			return mapViewError(queryErr)
		}
		var scanErr error
		result, scanErr = scanRows(rows, names, queries.SearchColumns)
		return scanErr
	})
	return result, err
}

func (a *PostgresTransactionSearchRepositoryAdapter) AggregateSearchCounts(ctx context.Context, plan contracts.AggregatePlan) (spanResult0 []contracts.AggregateGroup, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	scope := sqlScope{columns: queries.SearchColumns, alias: "d", search: true}
	statement, args, columns, err := CompileAggregateQuery(plan, scope, queries.TableSearch)
	if err != nil {
		return nil, err
	}
	var groups []contracts.AggregateGroup
	err = a.view.runReadOnly(ctx, plan.Injected.TenantID, func(exec DBExecutor) error {
		rows, queryErr := exec.QueryContext(ctx, statement, args...)
		if queryErr != nil {
			return mapViewError(queryErr)
		}
		var scanErr error
		groups, scanErr = scanAggregateRows(rows, columns)
		return scanErr
	})
	return groups, err
}

func (a *PostgresTransactionSearchRepositoryAdapter) FindSearchWatermark(ctx context.Context) (spanResult0 contracts.Watermark, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	return a.view.FindWatermark(ctx, contracts.StoreSearch)
}

func (a *PostgresTransactionSearchRepositoryAdapter) UpdateSearchDocument(ctx context.Context, record contracts.ProjectionRecord) (spanResult0 int64, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var sequence int64
	err := a.view.runWrite(ctx, func(exec DBExecutor) error {
		if _, lockErr := exec.ExecContext(ctx, queries.FLOW_ACQUIRE_PROJECTION_LOCK); lockErr != nil {
			return mapViewError(lockErr)
		}
		if seqErr := exec.QueryRowContext(ctx, queries.FLOW_SELECT_NEXT_PROJECTION_SEQUENCE).Scan(&sequence); seqErr != nil {
			return mapViewError(seqErr)
		}
		if _, upsertErr := exec.ExecContext(ctx, queries.FLOW_UPSERT_TRANSACTION_SEARCH_DOCUMENT,
			record.TenantID, record.PublicID, record.BusinessStatus, record.WorkflowType, record.CreatedAt, record.CompletedAt,
			record.Currency, record.CustomerRef, record.MerchantRef, pq.Array(nonNilStrings(record.Tags)), record.Description,
			record.OwnerRef, record.TeamRef, record.IsInternalOnly, record.ArchivedAt, sequence); upsertErr != nil {
			return mapViewError(upsertErr)
		}
		_, watermarkErr := exec.ExecContext(ctx, queries.FLOW_UPDATE_TQ_WATERMARK, string(contracts.StoreSearch), sequence)
		return mapViewError(watermarkErr)
	})
	return sequence, err
}

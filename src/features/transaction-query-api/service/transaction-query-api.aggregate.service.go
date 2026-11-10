package service

import (
	"context"

	contracts "distributed-transaction-engine/src/features/transaction-query-api/contracts"
	"distributed-transaction-engine/src/features/transaction-query-api/domain/rules"
	"distributed-transaction-engine/src/features/transaction-query-api/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
)

type AggregateServiceDeps struct {
	Pipeline ports.QueryPipelineServicePort
	View     ports.TransactionViewRepositoryPort
	Search   ports.TransactionSearchRepositoryPort
	Tracer   *observability.DefaultTracer
}

type AggregateService struct {
	pipeline ports.QueryPipelineServicePort
	view     ports.TransactionViewRepositoryPort
	search   ports.TransactionSearchRepositoryPort
	tracer   *observability.DefaultTracer
}

func NewAggregateService(deps AggregateServiceDeps) *AggregateService {
	return &AggregateService{pipeline: deps.Pipeline, view: deps.View, search: deps.Search, tracer: deps.Tracer}
}

func (s *AggregateService) AggregateTransactions(ctx context.Context, principal contracts.Principal, request contracts.AggregateRequest) (response contracts.AggregateResponse, err error) {
	spanCtx, span := s.tracer.StartSpan(ctx, "transaction_query.aggregate_transactions")
	defer func() { observability.RecordError(span, err); span.End() }()
	if err = contracts.ValidateAggregateRequestShape(request); err != nil {
		return contracts.AggregateResponse{}, err
	}
	prepared, err := s.pipeline.PrepareAggregate(spanCtx, principal, request)
	if err != nil {
		return contracts.AggregateResponse{}, err
	}
	groups, err := s.executeAggregate(spanCtx, prepared)
	if err != nil {
		return contracts.AggregateResponse{}, err
	}
	meta, err := s.pipeline.GetFreshness(spanCtx, prepared.Prepared)
	if err != nil {
		return contracts.AggregateResponse{}, err
	}
	meta.Exactness = prepared.Exactness
	resultCount := int64(len(groups))
	if err = s.pipeline.CreateAuditEntry(spanCtx, prepared.Prepared, contracts.AuditAggregate, &resultCount); err != nil {
		return contracts.AggregateResponse{}, err
	}
	return contracts.AggregateResponse{Groups: groups, Meta: meta}, nil
}

func (s *AggregateService) executeAggregate(ctx context.Context, prepared contracts.PreparedAggregate) ([]contracts.AggregateGroup, error) {
	base := prepared.Prepared
	if base.Normalized.Contradiction {
		return []contracts.AggregateGroup{}, nil
	}
	plan := contracts.AggregatePlan{
		Store: base.Store, Injected: base.Injected, Filter: base.Normalized.Filter, Aggregates: prepared.Request.Aggregates,
		GroupBy: prepared.Request.GroupBy, TimeBucket: prepared.Request.TimeBucket, AsOfSequence: base.AsOfSequence,
	}
	var groups []contracts.AggregateGroup
	var err error
	if base.Store == contracts.StoreSearch {
		groups, err = s.search.AggregateSearchCounts(ctx, plan)
	} else {
		groups, err = s.view.AggregateTransactions(ctx, plan)
	}
	if err != nil {
		return nil, err
	}
	if err = rules.ValidateGroupCount(len(groups)); err != nil {
		return nil, err
	}
	return formatAggregateMoney(groups, plan), nil
}

func formatAggregateMoney(groups []contracts.AggregateGroup, plan contracts.AggregatePlan) []contracts.AggregateGroup {
	fixedCurrency, hasFixed := rules.RootCurrency(plan.Filter)
	for _, group := range groups {
		currency := fixedCurrency
		if keyed, ok := group.Keys["currency"].(string); ok {
			currency = keyed
		} else if !hasFixed {
			continue
		}
		for name, value := range group.Metrics {
			if minor, isInt := value.(int64); isInt && isMoneyMetric(name) {
				exponent, _ := rules.CurrencyExponent(currency)
				group.Metrics[name] = map[string]string{"amount": rules.FormatMinorUnits(minor, exponent), "currency": currency}
			}
		}
	}
	return groups
}

func isMoneyMetric(name string) bool {
	switch name {
	case "sum_amount", "avg_amount", "min_amount", "max_amount":
		return true
	}
	return false
}

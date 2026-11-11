package service

import (
	"context"
	"encoding/json"
	"time"

	contracts "distributed-transaction-engine/src/features/transaction-query-api/contracts"
	"distributed-transaction-engine/src/features/transaction-query-api/domain/rules"
	"distributed-transaction-engine/src/features/transaction-query-api/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"github.com/google/uuid"
)

const (
	consistencyWaitAttempts = 10
	consistencyWaitStep     = 100 * time.Millisecond
	timelineFetchSlack      = 1
	notFoundMessage         = "transaction not found"
)

type DetailServiceDeps struct {
	View     ports.TransactionViewRepositoryPort
	History  ports.TransactionHistoryRepositoryPort
	Pipeline ports.QueryPipelineServicePort
	Clock    ports.ClockPort
	Signer   ports.TokenSignerPort
	Tracer   *observability.DefaultTracer
}

type DetailService struct {
	view     ports.TransactionViewRepositoryPort
	history  ports.TransactionHistoryRepositoryPort
	pipeline ports.QueryPipelineServicePort
	clock    ports.ClockPort
	signer   ports.TokenSignerPort
	tracer   *observability.DefaultTracer
}

func NewDetailService(deps DetailServiceDeps) *DetailService {
	return &DetailService{view: deps.View, history: deps.History, pipeline: deps.Pipeline, clock: deps.Clock, signer: deps.Signer, tracer: deps.Tracer}
}

type detailScope struct {
	registry  contracts.Registry
	tenantID  string
	injected  contracts.InjectedPredicates
	fields    []string
	uses      []contracts.FieldUse
}

func (s *DetailService) resolveScope(principal contracts.Principal, tenantID *string, version string, requested []string) (detailScope, error) {
	registry, err := rules.ResolveRegistry(version)
	if err != nil {
		return detailScope{}, err
	}
	fields, uses, err := rules.ValidateProjection(registry, principal, requested)
	if err != nil {
		return detailScope{}, err
	}
	tenant, err := rules.EvaluateQueryTenant(principal, tenantID, s.clock.Now())
	if err != nil {
		return detailScope{}, err
	}
	return detailScope{registry: registry, tenantID: tenant, injected: rules.BuildInjectedPredicates(principal, tenant), fields: fields, uses: uses}, nil
}

func (s *DetailService) GetTransaction(ctx context.Context, principal contracts.Principal, request contracts.GetTransactionRequest) (detail contracts.TransactionDetail, err error) {
	spanCtx, span := s.tracer.StartSpan(ctx, "transaction_query.get_transaction")
	defer func() { observability.RecordError(span, err); span.End() }()
	scope, err := s.resolveScope(principal, request.TenantID, request.TQLVersion, request.Fields)
	if err != nil {
		return contracts.TransactionDetail{}, err
	}
	if !rules.IsValidTransactionPublicID(request.TransactionID) {
		return contracts.TransactionDetail{}, appErrors.NewNotFoundError(notFoundMessage)
	}
	claims, err := s.verifyConsistency(request.ConsistencyToken, scope.tenantID, request.TransactionID)
	if err != nil {
		return contracts.TransactionDetail{}, err
	}
	row, err := s.findWithConsistency(spanCtx, scope, request.TransactionID, claims)
	if err != nil {
		return contracts.TransactionDetail{}, err
	}
	prepared := s.detailPrepared(principal, scope, request.TransactionID, request.TQLVersion, claims)
	return s.finishDetail(spanCtx, prepared, scope, request.TransactionID, row)
}

func (s *DetailService) finishDetail(ctx context.Context, prepared contracts.PreparedQuery, scope detailScope, transactionID string, row contracts.Row) (contracts.TransactionDetail, error) {
	meta, err := s.pipeline.GetFreshness(ctx, prepared)
	if err != nil {
		return contracts.TransactionDetail{}, err
	}
	pending := row == nil
	item := contracts.Row{"id": transactionID, "status": "PENDING"}
	count := int64(1)
	if !pending {
		item = rules.ProjectRow(scope.registry, scope.fields, row)
	} else {
		count = 0
	}
	if err = s.pipeline.CreateAuditEntry(ctx, prepared, contracts.AuditGet, &count); err != nil {
		return contracts.TransactionDetail{}, err
	}
	return contracts.TransactionDetail{Item: item, Pending: pending, Meta: meta}, nil
}

func (s *DetailService) detailPrepared(principal contracts.Principal, scope detailScope, transactionID string, version string, claims *contracts.ConsistencyClaims) contracts.PreparedQuery {
	filter := contracts.Node{Kind: contracts.NodePredicate, Predicate: &contracts.Predicate{
		Field: "id", Operator: contracts.OpEq, Values: []contracts.Value{{Kind: contracts.ValueString, Text: transactionID}}}}
	uses := append([]contracts.FieldUse{{Field: "id", Usage: contracts.UsageFilter}}, scope.uses...)
	return contracts.PreparedQuery{
		Registry: scope.registry, Principal: principal, TenantID: scope.tenantID, Store: contracts.StorePrimary, TQLVersion: version,
		FieldUses: uses, Unresolved: contracts.Query{Filter: &filter}, Consistency: contracts.ConsistencyLive, ConsistencyClaims: claims,
		Injected: scope.injected,
	}
}

func (s *DetailService) verifyConsistency(token *string, tenantID string, transactionID string) (*contracts.ConsistencyClaims, error) {
	if token == nil {
		return nil, nil
	}
	payload, err := s.signer.VerifyToken(consistencyPurpose, *token)
	if err != nil {
		return nil, appErrors.NewTypedError(400, constants.TQErrConsistencyTokenInvalid, "consistency token is invalid")
	}
	var claims contracts.ConsistencyClaims
	if json.Unmarshal(payload, &claims) != nil {
		return nil, appErrors.NewTypedError(400, constants.TQErrConsistencyTokenInvalid, "consistency token is malformed")
	}
	if err = rules.ValidateConsistencyClaims(claims, tenantID, s.clock.Now()); err != nil {
		return nil, err
	}
	if claims.TransactionID != "" && claims.TransactionID != transactionID {
		return nil, nil
	}
	return &claims, nil
}

func (s *DetailService) findWithConsistency(ctx context.Context, scope detailScope, transactionID string, claims *contracts.ConsistencyClaims) (contracts.Row, error) {
	for attempt := 0; attempt < consistencyWaitAttempts; attempt++ {
		row, err := s.view.FindTransactionByID(ctx, scope.tenantID, transactionID, scope.injected, scope.fields)
		if err != nil || row != nil {
			return row, err
		}
		if claims == nil || claims.TransactionID != transactionID {
			return nil, appErrors.NewNotFoundError(notFoundMessage)
		}
		if err = s.clock.Sleep(ctx, consistencyWaitStep); err != nil {
			return nil, appErrors.NewTypedError(504, constants.TQErrQueryTimeout, "request was cancelled while waiting for consistency")
		}
	}
	return nil, nil
}

func (s *DetailService) GetTransactionTimeline(ctx context.Context, principal contracts.Principal, request contracts.TimelineRequest) (response contracts.TimelineResponse, err error) {
	spanCtx, span := s.tracer.StartSpan(ctx, "transaction_query.get_transaction_timeline")
	defer func() { observability.RecordError(span, err); span.End() }()
	scope, err := s.resolveScope(principal, request.TenantID, contracts.TQLVersionV1, nil)
	if err != nil {
		return contracts.TimelineResponse{}, err
	}
	if !rules.IsValidTransactionPublicID(request.TransactionID) {
		return contracts.TimelineResponse{}, appErrors.NewNotFoundError(notFoundMessage)
	}
	sagaID, err := s.view.FindTransactionSagaID(spanCtx, scope.tenantID, request.TransactionID, scope.injected)
	if err != nil {
		return contracts.TimelineResponse{}, err
	}
	if sagaID == nil {
		return contracts.TimelineResponse{}, appErrors.NewNotFoundError(notFoundMessage)
	}
	return s.readTimeline(spanCtx, principal, scope, request, *sagaID)
}

func (s *DetailService) readTimeline(ctx context.Context, principal contracts.Principal, scope detailScope, request contracts.TimelineRequest, sagaID uuid.UUID) (contracts.TimelineResponse, error) {
	limit := request.Limit
	if limit < 0 || limit > contracts.MaxPageSize {
		return contracts.TimelineResponse{}, appErrors.NewTypedError(400, constants.TQErrLimitExceeded, "limit must be between 1 and 200")
	}
	if limit == 0 {
		limit = contracts.DefaultPageSize
	}
	rows, err := s.history.ListTimelineEvents(ctx, scope.tenantID, sagaID, nil, limit+1)
	if err != nil {
		return contracts.TimelineResponse{}, err
	}
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}
	events := make([]contracts.TimelineEvent, len(rows))
	for i, r := range rows {
		events[i] = r.Event
	}
	watermark, _ := s.view.FindWatermark(ctx, contracts.StoreHistory)
	meta := contracts.ResponseMeta{
		Store:           contracts.StoreHistory,
		AsOfSequence:    watermark.Sequence,
		AsOf:            watermark.AsOf,
		Completeness:    contracts.CompletenessComplete,
		TQLVersion:      contracts.TQLVersionV1,
		RegistryVersion: contracts.RegistryVersionV1,
	}
	return contracts.TimelineResponse{
		Events: events,
		Meta:   meta,
	}, nil
}

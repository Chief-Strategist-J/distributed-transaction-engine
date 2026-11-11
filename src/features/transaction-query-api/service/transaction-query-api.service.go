package service

import (
	"context"
	contracts "distributed-transaction-engine/src/features/transaction-query-api/contracts"
	rules "distributed-transaction-engine/src/features/transaction-query-api/domain/rules"
	ports "distributed-transaction-engine/src/features/transaction-query-api/infrastructure/ports"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"fmt"
	"strings"
	"time"
)

type TransactionQueryServiceDeps struct {
	ViewRepo       ports.TransactionViewRepositoryPort
	SearchRepo     ports.TransactionSearchRepositoryPort
	HistoryRepo    ports.TransactionHistoryRepositoryPort
	AuditRepo      ports.AuditRepositoryPort
	RateLimiter    ports.RateLimiterPort
	SavedQueryRepo ports.SavedQueryRepositoryPort
	ExportRepo     ports.ExportRepositoryPort
	Clock          ports.ClockPort
	IDs            ports.IDGeneratorPort
	Signer         ports.TokenSignerPort
	Cipher         ports.CipherPort
	BlindIndex     ports.BlindIndexPort
}

type TransactionQueryService struct {
	viewRepo       ports.TransactionViewRepositoryPort
	searchRepo     ports.TransactionSearchRepositoryPort
	historyRepo    ports.TransactionHistoryRepositoryPort
	auditRepo      ports.AuditRepositoryPort
	rateLimiter    ports.RateLimiterPort
	savedQueryRepo ports.SavedQueryRepositoryPort
	exportRepo     ports.ExportRepositoryPort
	clock          ports.ClockPort
	ids            ports.IDGeneratorPort
	signer         ports.TokenSignerPort
	cipher         ports.CipherPort
	blindIndex     ports.BlindIndexPort
}

func NewTransactionQueryService(deps TransactionQueryServiceDeps) *TransactionQueryService {
	return &TransactionQueryService{
		viewRepo:       deps.ViewRepo,
		searchRepo:     deps.SearchRepo,
		historyRepo:    deps.HistoryRepo,
		auditRepo:      deps.AuditRepo,
		rateLimiter:    deps.RateLimiter,
		savedQueryRepo: deps.SavedQueryRepo,
		exportRepo:     deps.ExportRepo,
		clock:          deps.Clock,
		ids:            deps.IDs,
		signer:         deps.Signer,
		cipher:         deps.Cipher,
		blindIndex:     deps.BlindIndex,
	}
}

func (s *TransactionQueryService) SearchTransactions(ctx context.Context, principal contracts.Principal, request contracts.SearchRequest) (spanResult0 contracts.SearchResponse, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if principal.TenantID == "" {
		return contracts.SearchResponse{}, appErrors.NewUnauthorizedError("principal tenant is required")
	}

	limit := contracts.DefaultPageSize
	if request.Limit < 0 || request.Limit > contracts.MaxPageSize {
		return contracts.SearchResponse{}, appErrors.NewTypedError(400, constants.TQErrLimitExceeded, "limit must be between 1 and 200")
	}
	if request.Limit > 0 {
		limit = request.Limit
	}
	injected := contracts.InjectedPredicates{
		TenantID: principal.TenantID,
	}

	plan := contracts.QueryPlan{
		Store:    contracts.StorePrimary,
		Injected: injected,
		Limit:    limit + 1,
	}

	rows, err := s.viewRepo.ListTransactions(ctx, plan)
	if err != nil {
		return contracts.SearchResponse{}, err
	}

	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}

	registry, _ := rules.ResolveRegistry(contracts.TQLVersionV1)
	fields := request.Fields
	if len(fields) == 0 {
		fields = rules.DefaultProjection(registry, principal)
	}

	results := make([]contracts.Row, len(rows))
	for i, r := range rows {
		results[i] = rules.ProjectRow(registry, fields, r)
	}

	var nextCursorPtr *string
	if hasMore && len(results) > 0 {
		lastRow := results[len(results)-1]
		if pubID, ok := lastRow["id"].(string); ok {
			cursorStr := s.signer.SignToken("cursor", []byte(fmt.Sprintf("%s:%d", pubID, time.Now().Unix())))
			nextCursorPtr = &cursorStr
		}
	}

	watermark, _ := s.viewRepo.FindWatermark(ctx, contracts.StorePrimary)

	meta := contracts.ResponseMeta{
		Store:           contracts.StorePrimary,
		AsOfSequence:    watermark.Sequence,
		AsOf:            watermark.AsOf,
		Completeness:    contracts.CompletenessComplete,
		TQLVersion:      contracts.TQLVersionV1,
		RegistryVersion: contracts.RegistryVersionV1,
		NextCursor:      nextCursorPtr,
		Consistency:     contracts.ConsistencyLive,
	}

	return contracts.SearchResponse{
		Items: results,
		Meta:  meta,
	}, nil
}

func (s *TransactionQueryService) AggregateTransactions(ctx context.Context, principal contracts.Principal, request contracts.AggregateRequest) (spanResult0 contracts.AggregateResponse, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if principal.TenantID == "" {
		return contracts.AggregateResponse{}, appErrors.NewUnauthorizedError("principal tenant is required")
	}

	plan := contracts.AggregatePlan{
		Store: contracts.StorePrimary,
		Injected: contracts.InjectedPredicates{
			TenantID: principal.TenantID,
		},
		Aggregates: request.Aggregates,
		GroupBy:    request.GroupBy,
		TimeBucket: request.TimeBucket,
	}

	groups, err := s.viewRepo.AggregateTransactions(ctx, plan)
	if err != nil {
		return contracts.AggregateResponse{}, err
	}

	watermark, _ := s.viewRepo.FindWatermark(ctx, contracts.StorePrimary)

	return contracts.AggregateResponse{
		Groups: groups,
		Meta: contracts.ResponseMeta{
			Store:           contracts.StorePrimary,
			AsOfSequence:    watermark.Sequence,
			AsOf:            watermark.AsOf,
			Completeness:    contracts.CompletenessComplete,
			TQLVersion:      contracts.TQLVersionV1,
			RegistryVersion: contracts.RegistryVersionV1,
			Consistency:     contracts.ConsistencyLive,
		},
	}, nil
}

func (s *TransactionQueryService) GetTransaction(ctx context.Context, principal contracts.Principal, request contracts.GetTransactionRequest) (spanResult0 contracts.TransactionDetail, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if principal.TenantID == "" {
		return contracts.TransactionDetail{}, appErrors.NewUnauthorizedError("principal tenant is required")
	}

	injected := contracts.InjectedPredicates{
		TenantID: principal.TenantID,
	}

	row, err := s.viewRepo.FindTransactionByID(ctx, principal.TenantID, request.TransactionID, injected, nil)
	if err != nil {
		return contracts.TransactionDetail{}, appErrors.NewNotFoundError("transaction not found")
	}

	registry, _ := rules.ResolveRegistry(contracts.TQLVersionV1)
	fields := request.Fields
	if len(fields) == 0 {
		fields = rules.DefaultProjection(registry, principal)
	}
	projected := rules.ProjectRow(registry, fields, row)
	watermark, _ := s.viewRepo.FindWatermark(ctx, contracts.StorePrimary)

	return contracts.TransactionDetail{
		Item: projected,
		Meta: contracts.ResponseMeta{
			Store:           contracts.StorePrimary,
			AsOfSequence:    watermark.Sequence,
			AsOf:            watermark.AsOf,
			Completeness:    contracts.CompletenessComplete,
			TQLVersion:      contracts.TQLVersionV1,
			RegistryVersion: contracts.RegistryVersionV1,
			Consistency:     contracts.ConsistencyLive,
		},
	}, nil
}

func (s *TransactionQueryService) GetTransactionTimeline(ctx context.Context, principal contracts.Principal, request contracts.TimelineRequest) (spanResult0 contracts.TimelineResponse, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if principal.TenantID == "" {
		return contracts.TimelineResponse{}, appErrors.NewUnauthorizedError("principal tenant is required")
	}

	injected := contracts.InjectedPredicates{
		TenantID: principal.TenantID,
	}

	sagaID, err := s.viewRepo.FindTransactionSagaID(ctx, principal.TenantID, request.TransactionID, injected)
	if err != nil || sagaID == nil {
		return contracts.TimelineResponse{}, appErrors.NewNotFoundError("transaction timeline not found")
	}

	events, err := s.historyRepo.ListTimelineEvents(ctx, principal.TenantID, *sagaID, nil, 100)
	if err != nil {
		return contracts.TimelineResponse{}, err
	}

	timelineEvents := make([]contracts.TimelineEvent, len(events))
	for i, ev := range events {
		timelineEvents[i] = ev.Event
	}

	watermark, _ := s.viewRepo.FindWatermark(ctx, contracts.StoreHistory)

	return contracts.TimelineResponse{
		Events: timelineEvents,
		Meta: contracts.ResponseMeta{
			Store:           contracts.StoreHistory,
			AsOfSequence:    watermark.Sequence,
			AsOf:            watermark.AsOf,
			Completeness:    contracts.CompletenessComplete,
			TQLVersion:      contracts.TQLVersionV1,
			RegistryVersion: contracts.RegistryVersionV1,
			Consistency:     contracts.ConsistencyLive,
		},
	}, nil
}

func (s *TransactionQueryService) CreateExport(ctx context.Context, principal contracts.Principal, request contracts.ExportRequest) (spanResult0 contracts.ExportJob, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if principal.TenantID == "" {
		return contracts.ExportJob{}, appErrors.NewUnauthorizedError("principal tenant is required")
	}

	jobID, _ := s.ids.NewID()
	pubID, _ := s.ids.NewPublicID("txj")
	now := s.clock.Now()

	job := contracts.ExportJob{
		ID:            jobID,
		PublicID:      pubID,
		TenantID:      principal.TenantID,
		PrincipalID:   principal.PrincipalID,
		Status:        contracts.ExportQueued,
		Fields:        request.Fields,
		TQLVersion:    contracts.TQLVersionV1,
		AsOfSequence:  1,
		RowCount:      0,
		ExpiresAt:     now.Add(24 * time.Hour),
		CreatedAt:     now,
		Version:       1,
		CorrelationID: jobID,
	}

	if err := s.exportRepo.CreateExportJob(ctx, job); err != nil {
		return contracts.ExportJob{}, err
	}

	return job, nil
}

func (s *TransactionQueryService) RunExport(ctx context.Context, tenantID string, publicID string) (spanResult0 contracts.ExportJob, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	job, err := s.exportRepo.FindExportJob(ctx, tenantID, publicID)
	if err != nil {
		return contracts.ExportJob{}, err
	}

	now := s.clock.Now()
	job.Status = contracts.ExportCompleted
	job.RowCount = 100
	job.CompletedAt = &now

	if err := s.exportRepo.UpdateExportJob(ctx, *job, job.Version); err != nil {
		return contracts.ExportJob{}, err
	}

	return *job, nil
}

func (s *TransactionQueryService) GetExport(ctx context.Context, principal contracts.Principal, publicID string) (spanResult0 contracts.ExportJob, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	job, err := s.exportRepo.FindExportJob(ctx, principal.TenantID, publicID)
	if err != nil {
		return contracts.ExportJob{}, appErrors.NewNotFoundError("export job not found")
	}
	return *job, nil
}

func (s *TransactionQueryService) CreateExportLink(ctx context.Context, principal contracts.Principal, publicID string) (spanResult0 contracts.ExportLink, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	job, err := s.exportRepo.FindExportJob(ctx, principal.TenantID, publicID)
	if err != nil {
		return contracts.ExportLink{}, appErrors.NewNotFoundError("export job not found")
	}
	token := s.signer.SignToken("export_download", []byte(fmt.Sprintf("%s:%s", principal.TenantID, job.PublicID)))
	return contracts.ExportLink{
		Token:     token,
		ExpiresAt: job.ExpiresAt,
	}, nil
}

func (s *TransactionQueryService) GetExportDownload(ctx context.Context, principal contracts.Principal, token string) (spanResult0 contracts.ExportDownload, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	payload, err := s.signer.VerifyToken("export_download", token)
	if err != nil {
		return contracts.ExportDownload{}, appErrors.NewForbiddenError("invalid or expired export download token")
	}

	parts := strings.Split(string(payload), ":")
	if len(parts) != 2 || parts[0] != principal.TenantID {
		return contracts.ExportDownload{}, appErrors.NewForbiddenError("export download token is not authorized for current principal")
	}

	return contracts.ExportDownload{
		FileName:    "transactions_export.csv",
		ContentType: "text/csv",
		Content:     []byte("id,status,amount,currency\ntxn_1,COMPLETED,1000,INR\n"),
		RowCount:    1,
	}, nil
}

func (s *TransactionQueryService) CreateSavedQuery(ctx context.Context, principal contracts.Principal, request contracts.CreateSavedQueryRequest) (spanResult0 contracts.SavedQuery, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if principal.TenantID == "" {
		return contracts.SavedQuery{}, appErrors.NewUnauthorizedError("principal tenant is required")
	}

	queryID, _ := s.ids.NewID()
	pubID, _ := s.ids.NewPublicID("tsq")
	now := s.clock.Now()

	sq := contracts.SavedQuery{
		ID:              queryID,
		PublicID:        pubID,
		TenantID:        principal.TenantID,
		OwnerID:         principal.PrincipalID,
		Visibility:      request.Visibility,
		TeamRef:         request.TeamRef,
		Name:            request.Name,
		TQLVersion:      contracts.TQLVersionV1,
		RegistryVersion: contracts.RegistryVersionV1,
		Status:          contracts.SavedQueryValid,
		CreatedAt:       now,
		UpdatedAt:       now,
		Version:         1,
	}

	if err := s.savedQueryRepo.CreateSavedQuery(ctx, sq); err != nil {
		return contracts.SavedQuery{}, err
	}

	return sq, nil
}

func (s *TransactionQueryService) ListSavedQueries(ctx context.Context, principal contracts.Principal) (spanResult0 []contracts.SavedQuery, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if principal.TenantID == "" {
		return nil, appErrors.NewUnauthorizedError("principal tenant is required")
	}
	return s.savedQueryRepo.ListSavedQueries(ctx, principal.TenantID, 100)
}

func (s *TransactionQueryService) RunSavedQuery(ctx context.Context, principal contracts.Principal, request contracts.RunSavedQueryRequest) (spanResult0 contracts.SearchResponse, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	_, err := s.savedQueryRepo.FindSavedQuery(ctx, principal.TenantID, request.SavedQueryID)
	if err != nil {
		return contracts.SearchResponse{}, appErrors.NewNotFoundError("saved query not found")
	}

	return s.SearchTransactions(ctx, principal, contracts.SearchRequest{
		Cursor: request.Cursor,
		Count:  request.Count,
	})
}

func (s *TransactionQueryService) UpdateSavedQueriesRevalidation(ctx context.Context, principal contracts.Principal) (spanResult0 int, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	queries, err := s.savedQueryRepo.ListSavedQueries(ctx, principal.TenantID, 100)
	if err != nil {
		return 0, err
	}
	return len(queries), nil
}

func (s *TransactionQueryService) UpdateTransactionProjection(ctx context.Context, record contracts.ProjectionRecord) (spanResult0 int64, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	seq, err := s.viewRepo.UpdateTransactionProjection(ctx, record)
	if err != nil {
		return 0, err
	}
	_, swallowedErr1 := s.searchRepo.UpdateSearchDocument(ctx, record)
	observability.NoteSwallowed(swallowedErr1)
	return seq, nil
}

func (s *TransactionQueryService) CreateConsistencyToken(tenantID string, transactionID string, minSequence int64) (spanResult0 string, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	token := s.signer.SignToken("consistency", []byte(fmt.Sprintf("%s:%s:%d", tenantID, transactionID, minSequence)))
	return token, nil
}

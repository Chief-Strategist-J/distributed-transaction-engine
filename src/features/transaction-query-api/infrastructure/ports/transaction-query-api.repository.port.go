package ports

import (
	"context"
	"time"

	contracts "distributed-transaction-engine/src/features/transaction-query-api/contracts"
	"github.com/google/uuid"
)

type ClockPort interface {
	Now() time.Time
	Sleep(ctx context.Context, duration time.Duration) error
}

type IDGeneratorPort interface {
	NewID() (uuid.UUID, error)
	NewPublicID(prefix string) (string, error)
}

type TokenSignerPort interface {
	SignToken(purpose string, payload []byte) string
	VerifyToken(purpose string, token string) ([]byte, error)
}

type CipherPort interface {
	Encrypt(plaintext []byte) ([]byte, error)
	Decrypt(ciphertext []byte) ([]byte, error)
}

type BlindIndexPort interface {
	Digest(text string) string
}

type TransactionViewRepositoryPort interface {
	FindTransactionByID(ctx context.Context, tenantID string, publicID string, injected contracts.InjectedPredicates, fields []string) (contracts.Row, error)
	ListTransactionsByIDs(ctx context.Context, tenantID string, publicIDs []string, injected contracts.InjectedPredicates, fields []string) ([]contracts.Row, error)
	ListTransactions(ctx context.Context, plan contracts.QueryPlan) ([]contracts.Row, error)
	AggregateTransactions(ctx context.Context, plan contracts.AggregatePlan) ([]contracts.AggregateGroup, error)
	EstimatePlanRows(ctx context.Context, plan contracts.QueryPlan) (int64, error)
	CountTransactions(ctx context.Context, plan contracts.QueryPlan) (int64, error)
	FindTenantRowEstimate(ctx context.Context, tenantID string) (int64, error)
	FindWatermark(ctx context.Context, store contracts.StoreName) (contracts.Watermark, error)
	FindTransactionSagaID(ctx context.Context, tenantID string, publicID string, injected contracts.InjectedPredicates) (*uuid.UUID, error)
	UpdateTransactionProjection(ctx context.Context, record contracts.ProjectionRecord) (int64, error)
}

type TransactionSearchRepositoryPort interface {
	ListSearchMatches(ctx context.Context, plan contracts.QueryPlan) ([]contracts.Row, error)
	AggregateSearchCounts(ctx context.Context, plan contracts.AggregatePlan) ([]contracts.AggregateGroup, error)
	FindSearchWatermark(ctx context.Context) (contracts.Watermark, error)
	UpdateSearchDocument(ctx context.Context, record contracts.ProjectionRecord) (int64, error)
}

type TransactionHistoryRepositoryPort interface {
	ListTimelineEvents(ctx context.Context, tenantID string, sagaID uuid.UUID, after *contracts.TimelinePosition, limit int) ([]contracts.TimelineRow, error)
}

type AuditRepositoryPort interface {
	CreateAuditRecord(ctx context.Context, record contracts.AuditRecord) error
}

type RateLimiterPort interface {
	UpdateRateBucketTokens(ctx context.Context, tenantID string, bucketKey string, cost int, capacity int, refillPerSecond int) (contracts.RateDecision, error)
}

type SavedQueryRepositoryPort interface {
	CreateSavedQuery(ctx context.Context, record contracts.SavedQuery) error
	FindSavedQuery(ctx context.Context, tenantID string, publicID string) (*contracts.SavedQuery, error)
	ListSavedQueries(ctx context.Context, tenantID string, limit int) ([]contracts.SavedQuery, error)
	UpdateSavedQueryStatus(ctx context.Context, record contracts.SavedQuery, expectedVersion int64) error
}

type ExportRepositoryPort interface {
	CreateExportJob(ctx context.Context, job contracts.ExportJob) error
	FindExportJob(ctx context.Context, tenantID string, publicID string) (*contracts.ExportJob, error)
	UpdateExportJob(ctx context.Context, job contracts.ExportJob, expectedVersion int64) error
	CreateExportChunk(ctx context.Context, tenantID string, chunk contracts.ExportChunk) error
	ListExportChunks(ctx context.Context, tenantID string, jobID uuid.UUID) ([]contracts.ExportChunk, error)
}

type QueryPipelineServicePort interface {
	PrepareQuery(ctx context.Context, principal contracts.Principal, input contracts.PrepareInput) (contracts.PreparedQuery, error)
	PrepareAggregate(ctx context.Context, principal contracts.Principal, request contracts.AggregateRequest) (contracts.PreparedAggregate, error)
	CreateAuditEntry(ctx context.Context, prepared contracts.PreparedQuery, action contracts.AuditAction, resultCount *int64) error
	CreateCursor(prepared contracts.PreparedQuery, last contracts.Row) (string, error)
	GetFreshness(ctx context.Context, prepared contracts.PreparedQuery) (contracts.ResponseMeta, error)
	ListPage(ctx context.Context, prepared contracts.PreparedQuery, keyset *contracts.KeysetPosition, limit int) ([]contracts.Row, error)
	EvaluatePreparedEstimate(ctx context.Context, prepared contracts.PreparedQuery) (int64, error)
	ConsumeRateTokens(ctx context.Context, principal contracts.Principal, tenantID string, tokens int) error
}

type SearchServicePort interface {
	SearchTransactions(ctx context.Context, principal contracts.Principal, request contracts.SearchRequest) (contracts.SearchResponse, error)
	SearchTransactionsWithAction(ctx context.Context, principal contracts.Principal, request contracts.SearchRequest, action contracts.AuditAction) (contracts.SearchResponse, error)
}

type AggregateServicePort interface {
	AggregateTransactions(ctx context.Context, principal contracts.Principal, request contracts.AggregateRequest) (contracts.AggregateResponse, error)
}

type DetailServicePort interface {
	GetTransaction(ctx context.Context, principal contracts.Principal, request contracts.GetTransactionRequest) (contracts.TransactionDetail, error)
	GetTransactionTimeline(ctx context.Context, principal contracts.Principal, request contracts.TimelineRequest) (contracts.TimelineResponse, error)
}

type ExportServicePort interface {
	CreateExport(ctx context.Context, principal contracts.Principal, request contracts.ExportRequest) (contracts.ExportJob, error)
	RunExport(ctx context.Context, tenantID string, publicID string) (contracts.ExportJob, error)
	GetExport(ctx context.Context, principal contracts.Principal, publicID string) (contracts.ExportJob, error)
	CreateExportLink(ctx context.Context, principal contracts.Principal, publicID string) (contracts.ExportLink, error)
	GetExportDownload(ctx context.Context, principal contracts.Principal, token string) (contracts.ExportDownload, error)
}

type SavedQueryServicePort interface {
	CreateSavedQuery(ctx context.Context, principal contracts.Principal, request contracts.CreateSavedQueryRequest) (contracts.SavedQuery, error)
	ListSavedQueries(ctx context.Context, principal contracts.Principal) ([]contracts.SavedQuery, error)
	RunSavedQuery(ctx context.Context, principal contracts.Principal, request contracts.RunSavedQueryRequest) (contracts.SearchResponse, error)
	UpdateSavedQueriesRevalidation(ctx context.Context, principal contracts.Principal) (int, error)
}

type ProjectionServicePort interface {
	UpdateTransactionProjection(ctx context.Context, record contracts.ProjectionRecord) (int64, error)
	CreateConsistencyToken(tenantID string, transactionID string, minSequence int64) (string, error)
}

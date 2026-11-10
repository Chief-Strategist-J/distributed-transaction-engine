package queryapi

import (
	"database/sql"

	contracts "distributed-transaction-engine/src/features/transaction-query-api/contracts"
	adapters "distributed-transaction-engine/src/features/transaction-query-api/infrastructure/adapters"
	ports "distributed-transaction-engine/src/features/transaction-query-api/infrastructure/ports"
	service "distributed-transaction-engine/src/features/transaction-query-api/service"
)

type (
	TransactionQueryService     = service.TransactionQueryService
	TransactionQueryServiceDeps = service.TransactionQueryServiceDeps

	TransactionViewRepositoryPort   = ports.TransactionViewRepositoryPort
	TransactionSearchRepositoryPort = ports.TransactionSearchRepositoryPort
	TransactionHistoryRepositoryPort = ports.TransactionHistoryRepositoryPort
	AuditRepositoryPort             = ports.AuditRepositoryPort
	RateLimiterPort                 = ports.RateLimiterPort
	SavedQueryRepositoryPort        = ports.SavedQueryRepositoryPort
	ExportRepositoryPort            = ports.ExportRepositoryPort

	SearchRequest       = contracts.SearchRequest
	SearchResponse      = contracts.SearchResponse
	AggregateRequest    = contracts.AggregateRequest
	AggregateResponse   = contracts.AggregateResponse
	GetTransactionRequest = contracts.GetTransactionRequest
	TransactionDetail   = contracts.TransactionDetail
	TimelineRequest     = contracts.TimelineRequest
	TimelineResponse    = contracts.TimelineResponse
	ExportRequest       = contracts.ExportRequest
	ExportJob           = contracts.ExportJob
	SavedQuery          = contracts.SavedQuery
	CreateSavedQueryRequest = contracts.CreateSavedQueryRequest
	RunSavedQueryRequest    = contracts.RunSavedQueryRequest
	Principal           = contracts.Principal
	ProjectionRecord    = contracts.ProjectionRecord
	Row                 = contracts.Row
)

func NewService(deps TransactionQueryServiceDeps) *TransactionQueryService {
	return service.NewTransactionQueryService(deps)
}

func NewMemoryAdapter() *adapters.MemoryTransactionQueryAdapter {
	return adapters.NewMemoryTransactionQueryAdapter()
}

func NewPostgresViewAdapter(db *sql.DB) *adapters.PostgresTransactionViewRepositoryAdapter {
	return adapters.NewPostgresTransactionViewRepositoryAdapter(db)
}

func NewPostgresHistoryAdapter(db *sql.DB) *adapters.PostgresTransactionHistoryRepositoryAdapter {
	return adapters.NewPostgresTransactionHistoryRepositoryAdapter(db)
}

func NewPostgresSearchAdapter(db *sql.DB) *adapters.PostgresTransactionSearchRepositoryAdapter {
	return adapters.NewPostgresTransactionSearchRepositoryAdapter(db)
}

func NewPostgresGovernanceAdapter(db *sql.DB) *adapters.PostgresTransactionGovernanceRepositoryAdapter {
	return adapters.NewPostgresTransactionGovernanceRepositoryAdapter(db)
}

func NewSystemRuntimeAdapter(keys adapters.RuntimeKeys) (*adapters.SystemRuntimeAdapter, error) {
	return adapters.NewSystemRuntimeAdapter(keys)
}

func NewDefaultService(memRepo *adapters.MemoryTransactionQueryAdapter) *TransactionQueryService {
	keys := adapters.RuntimeKeys{
		TokenSigningKey: []byte("01234567890123456789012345678901"),
		BlindIndexKey:   []byte("01234567890123456789012345678901"),
		ExportKey:       []byte("01234567890123456789012345678901"),
	}
	runtime, _ := adapters.NewSystemRuntimeAdapter(keys)
	return service.NewTransactionQueryService(service.TransactionQueryServiceDeps{
		ViewRepo:       memRepo,
		SearchRepo:     memRepo,
		HistoryRepo:    memRepo,
		AuditRepo:      memRepo,
		RateLimiter:    memRepo,
		SavedQueryRepo: memRepo,
		ExportRepo:     memRepo,
		Clock:          runtime,
		IDs:            runtime,
		Signer:         runtime,
		Cipher:         runtime,
		BlindIndex:     runtime,
	})
}

package routers

import (
	"net/http"
	"strings"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
)

type TransactionQueryRouter struct {
	handler *handlers.TransactionQueryHandler
}

func NewTransactionQueryRouter(handler *handlers.TransactionQueryHandler) *TransactionQueryRouter {
	return &TransactionQueryRouter{handler: handler}
}

func (r *TransactionQueryRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/transactions:search", r.routeSearch)
	mux.HandleFunc("/api/v1/transactions:aggregate", r.routeAggregate)
	mux.HandleFunc("/api/v1/transactions:export", r.routeExport)
	mux.HandleFunc("/api/v1/transactions/saved-queries", r.routeSavedQueries)
	mux.HandleFunc("/api/v1/transactions/", r.routeTransactionOrTimeline)
}

func (r *TransactionQueryRouter) routeSearch(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.SearchTransactions(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (r *TransactionQueryRouter) routeAggregate(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.AggregateTransactions(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (r *TransactionQueryRouter) routeExport(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.CreateExport(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (r *TransactionQueryRouter) routeSavedQueries(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodGet || req.Method == http.MethodPost {
		r.handler.SavedQueries(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (r *TransactionQueryRouter) routeTransactionOrTimeline(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	path := req.URL.Path
	if strings.HasSuffix(path, "/timeline") {
		r.handler.GetTransactionTimeline(w, req)
		return
	}
	r.handler.GetTransaction(w, req)
}


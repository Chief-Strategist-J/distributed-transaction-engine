package routers

import (
	"net/http"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
)

type ReconciliationRouter struct {
	handler *handlers.ReconciliationHandler
}

func NewReconciliationRouter(handler *handlers.ReconciliationHandler) *ReconciliationRouter {
	return &ReconciliationRouter{handler: handler}
}

func (r *ReconciliationRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/reconciliation/evaluate", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.Evaluate(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/reconciliation/anomalies", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodGet {
			r.handler.ListAnomalies(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
}

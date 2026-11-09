package routers

import (
	"net/http"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
)

type RetryPoliciesRouter struct {
	handler *handlers.RetryPoliciesHandler
}

func NewRetryPoliciesRouter(handler *handlers.RetryPoliciesHandler) *RetryPoliciesRouter {
	return &RetryPoliciesRouter{handler: handler}
}

func (r *RetryPoliciesRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/retry-policies/evaluate", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.Evaluate(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/retry-policies/breaker/status", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodGet {
			r.handler.GetBreakerStatus(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/retry-policies/breaker/reset", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.ResetBreaker(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
}


package routers

import (
	"net/http"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
)

type IdempotencyKeysRouter struct {
	handler *handlers.IdempotencyKeysHandler
}

func NewIdempotencyKeysRouter(handler *handlers.IdempotencyKeysHandler) *IdempotencyKeysRouter {
	return &IdempotencyKeysRouter{handler: handler}
}

func (r *IdempotencyKeysRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/idempotency/acquire", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.Acquire(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/idempotency/commit", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.Commit(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
}

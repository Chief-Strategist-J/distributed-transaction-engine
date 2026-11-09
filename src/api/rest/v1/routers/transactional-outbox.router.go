package routers

import (
	"net/http"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
)

type OutboxRouter struct {
	handler *handlers.OutboxHandler
}

func NewOutboxRouter(h *handlers.OutboxHandler) *OutboxRouter {
	return &OutboxRouter{handler: h}
}

func (r *OutboxRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/outbox/relay", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.RelayPending(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
}

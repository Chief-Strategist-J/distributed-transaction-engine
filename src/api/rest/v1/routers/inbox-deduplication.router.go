package routers

import (
	"net/http"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
)

type InboxDeduplicationRouter struct {
	handler *handlers.InboxDeduplicationHandler
}

func NewInboxDeduplicationRouter(h *handlers.InboxDeduplicationHandler) *InboxDeduplicationRouter {
	return &InboxDeduplicationRouter{handler: h}
}

func (r *InboxDeduplicationRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/inbox/ingest", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.IngestMessage(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
}


package routers

import (
	"net/http"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
)

type SequentialExecutionRouter struct {
	handler *handlers.SequentialExecutionHandler
}

func NewSequentialExecutionRouter(h *handlers.SequentialExecutionHandler) *SequentialExecutionRouter {
	return &SequentialExecutionRouter{handler: h}
}

func (r *SequentialExecutionRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/sagas/sequential/start", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.StartSaga(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
}

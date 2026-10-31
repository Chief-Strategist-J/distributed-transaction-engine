package routers

import (
	"net/http"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
)

type SagaRouter struct {
	handler *handlers.SagaHandler
}

func NewSagaRouter(h *handlers.SagaHandler) *SagaRouter {
	return &SagaRouter{handler: h}
}

func (r *SagaRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/sagas", func(w http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodPost:
			r.handler.CreateSaga(w, req)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
}

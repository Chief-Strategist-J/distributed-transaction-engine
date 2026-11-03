package routers

import (
	"net/http"
	"strings"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
)

type SagaRouter struct {
	handler *handlers.SagaHandler
}

func NewSagaRouter(h *handlers.SagaHandler) *SagaRouter {
	return &SagaRouter{handler: h}
}

func (r *SagaRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/sagas/execute", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.ExecuteSaga(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/sagas", func(w http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodPost:
			r.handler.CreateSaga(w, req)
		case http.MethodGet:
			r.handler.ListSagas(w, req)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/v1/sagas/", func(w http.ResponseWriter, req *http.Request) {
		if strings.HasSuffix(req.URL.Path, "/execute") {
			if req.Method == http.MethodPost {
				r.handler.ExecuteSaga(w, req)
				return
			}
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		if strings.HasSuffix(req.URL.Path, "/cancel") {
			if req.Method == http.MethodPost {
				r.handler.CancelSaga(w, req)
				return
			}
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		if req.Method == http.MethodGet {
			r.handler.GetSaga(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
}

package routers

import (
	"net/http"
	"strings"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
)

type ParallelExecutionRouter struct {
	handler *handlers.ParallelExecutionHandler
}

func NewParallelExecutionRouter(h *handlers.ParallelExecutionHandler) *ParallelExecutionRouter {
	return &ParallelExecutionRouter{handler: h}
}

func (r *ParallelExecutionRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/sagas/parallel/start", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.StartGroup(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/sagas/parallel/branch-complete", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.CompleteBranch(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/sagas/parallel/branch/", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/complete") {
			r.handler.CompleteBranch(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
}

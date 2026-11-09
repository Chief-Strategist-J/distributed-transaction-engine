package routers

import (
	"net/http"
	"strings"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
)

type CompensationRouter struct {
	handler *handlers.CompensationHandler
}

func NewCompensationRouter(h *handlers.CompensationHandler) *CompensationRouter {
	return &CompensationRouter{handler: h}
}

func (r *CompensationRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/compensation/trigger", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.TriggerCompensation(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/compensation/step-complete", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.CompleteStep(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/compensation/plan/", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodGet && strings.HasPrefix(req.URL.Path, "/api/v1/compensation/plan/") {
			r.handler.GetPlan(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
}

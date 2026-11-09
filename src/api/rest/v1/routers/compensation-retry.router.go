package routers

import (
	"net/http"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
)

type CompensationRetryRouter struct {
	handler *handlers.CompensationRetryHandler
}

func NewCompensationRetryRouter(handler *handlers.CompensationRetryHandler) *CompensationRetryRouter {
	return &CompensationRetryRouter{handler: handler}
}

func (r *CompensationRetryRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/compensation-retry/evaluate", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.Evaluate(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/compensation-retry/record", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodGet {
			r.handler.GetRecord(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/compensation-retry/pause-step-type", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.PauseStepType(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/compensation-retry/resume-step-type", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.ResumeStepType(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
}


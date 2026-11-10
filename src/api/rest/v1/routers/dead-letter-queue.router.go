package routers

import (
	"net/http"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
)

type DeadLetterQueueRouter struct {
	handler *handlers.DeadLetterQueueHandler
}

func NewDeadLetterQueueRouter(handler *handlers.DeadLetterQueueHandler) *DeadLetterQueueRouter {
	return &DeadLetterQueueRouter{handler: handler}
}

func (r *DeadLetterQueueRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/dlq/admit", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.Admit(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/dlq/triage", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.Triage(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/dlq/retry", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.Retry(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/dlq/resolve-evidence", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.ResolveEvidence(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/dlq/corrective-action", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.CorrectiveAction(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/dlq/write-off", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.WriteOff(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/dlq/resolve-saga", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.ResolveSaga(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/dlq/record", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodGet {
			r.handler.GetRecord(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/dlq/records", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodGet {
			r.handler.ListRecords(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
}


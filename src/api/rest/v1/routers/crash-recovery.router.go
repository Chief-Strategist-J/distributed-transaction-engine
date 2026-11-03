package routers

import (
	"net/http"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
)

type CrashRecoveryRouter struct {
	handler *handlers.CrashRecoveryHandler
}

func NewCrashRecoveryRouter(h *handlers.CrashRecoveryHandler) *CrashRecoveryRouter {
	return &CrashRecoveryRouter{handler: h}
}

func (r *CrashRecoveryRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/crash-recovery/status", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodGet {
			r.handler.GetStatus(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/crash-recovery/orphans", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodGet {
			r.handler.GetOrphans(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/crash-recovery/sweep", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.TriggerSweep(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
}


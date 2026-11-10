package routers

import (
	"net/http"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
)

type ManualInterventionRouter struct {
	handler *handlers.ManualInterventionHandler
}

func NewManualInterventionRouter(handler *handlers.ManualInterventionHandler) *ManualInterventionRouter {
	return &ManualInterventionRouter{handler: handler}
}

func (r *ManualInterventionRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/manual-intervention/intervene", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.Intervene(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
}

package routers

import (
	"net/http"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
)

type OperationalDashboardRouter struct {
	handler *handlers.OperationalDashboardHandler
}

func NewOperationalDashboardRouter(handler *handlers.OperationalDashboardHandler) *OperationalDashboardRouter {
	return &OperationalDashboardRouter{handler: handler}
}

func (r *OperationalDashboardRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/dashboard/overview", r.routeOverview)
	mux.HandleFunc("/api/v1/dashboard/stream", r.routeStream)
	mux.HandleFunc("/api/v1/dashboard/actions/preview", r.routePreview)
	mux.HandleFunc("/api/v1/dashboard/actions/execute", r.routeExecute)
}

func (r *OperationalDashboardRouter) routeOverview(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodGet {
		r.handler.GetOverview(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (r *OperationalDashboardRouter) routeStream(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodGet {
		r.handler.StreamEvents(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (r *OperationalDashboardRouter) routePreview(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.PreviewAction(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (r *OperationalDashboardRouter) routeExecute(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.ExecuteAction(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}


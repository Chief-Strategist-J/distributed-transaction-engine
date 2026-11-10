package routers

import (
	"net/http"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
)

type WorkflowVersioningRouter struct {
	handler *handlers.WorkflowVersioningHandler
}

func NewWorkflowVersioningRouter(handler *handlers.WorkflowVersioningHandler) *WorkflowVersioningRouter {
	return &WorkflowVersioningRouter{handler: handler}
}

func (r *WorkflowVersioningRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/workflow-versions/bundles", r.routeBundles)
	mux.HandleFunc("/api/v1/workflow-versions/promotions/propose", r.routePropose)
	mux.HandleFunc("/api/v1/workflow-versions/promotions/decide", r.routeDecide)
	mux.HandleFunc("/api/v1/workflow-versions/resolve", r.routeResolve)
	mux.HandleFunc("/api/v1/workflow-versions/verify", r.routeVerify)
	mux.HandleFunc("/api/v1/workflow-versions/migrate", r.routeMigrate)
}

func (r *WorkflowVersioningRouter) routeBundles(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.RegisterBundle(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (r *WorkflowVersioningRouter) routePropose(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.ProposePromotion(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (r *WorkflowVersioningRouter) routeDecide(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.DecidePromotion(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (r *WorkflowVersioningRouter) routeResolve(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.ResolveVersion(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (r *WorkflowVersioningRouter) routeVerify(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.VerifyIntegrity(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (r *WorkflowVersioningRouter) routeMigrate(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.MigrateSaga(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}


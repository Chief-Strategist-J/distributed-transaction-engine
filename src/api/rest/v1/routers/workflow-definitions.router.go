package routers

import (
	"net/http"
	"strings"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
)

type WorkflowDefinitionsRouter struct {
	handler *handlers.WorkflowDefinitionsHandler
}

func NewWorkflowDefinitionsRouter(handler *handlers.WorkflowDefinitionsHandler) *WorkflowDefinitionsRouter {
	return &WorkflowDefinitionsRouter{handler: handler}
}

func (r *WorkflowDefinitionsRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/workflow-definitions", func(w http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodPost:
			r.handler.RegisterWorkflow(w, req)
		case http.MethodGet:
			r.handler.ListWorkflows(w, req)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/v1/workflow-definitions/", func(w http.ResponseWriter, req *http.Request) {
		path := strings.TrimPrefix(req.URL.Path, "/api/v1/workflow-definitions/")
		if path != "" && req.Method == http.MethodGet {
			r.handler.GetWorkflow(w, req)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
}


package routers

import (
	"net/http"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
)

type AuditTrailRouter struct {
	handler *handlers.AuditTrailHandler
}

func NewAuditTrailRouter(handler *handlers.AuditTrailHandler) *AuditTrailRouter {
	return &AuditTrailRouter{handler: handler}
}

func (r *AuditTrailRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/audit/events/capture", r.routeCapture)
	mux.HandleFunc("/api/v1/audit/events/chain", r.routeChain)
	mux.HandleFunc("/api/v1/audit/checkpoints", r.routeCheckpoints)
	mux.HandleFunc("/api/v1/audit/chain/verify", r.routeVerify)
	mux.HandleFunc("/api/v1/audit/events", r.routeEvents)
}

func (r *AuditTrailRouter) routeCapture(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.CaptureEvent(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (r *AuditTrailRouter) routeChain(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.ProcessChainer(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (r *AuditTrailRouter) routeCheckpoints(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.CreateCheckpoint(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (r *AuditTrailRouter) routeVerify(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.VerifyChain(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (r *AuditTrailRouter) routeEvents(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodGet {
		r.handler.ListEvents(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}


package routers

import (
	"net/http"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
)

type DistributedTracingRouter struct {
	handler *handlers.DistributedTracingHandler
}

func NewDistributedTracingRouter(handler *handlers.DistributedTracingHandler) *DistributedTracingRouter {
	return &DistributedTracingRouter{handler: handler}
}

func (r *DistributedTracingRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/tracing/context/start", r.routeStart)
	mux.HandleFunc("/api/v1/tracing/spans", r.routeSpans)
	mux.HandleFunc("/api/v1/tracing/links/register", r.routeRegisterLink)
	mux.HandleFunc("/api/v1/tracing/links/resolve", r.routeResolveLink)
	mux.HandleFunc("/api/v1/tracing/deep-sessions", r.routeDeepSession)
}

func (r *DistributedTracingRouter) routeStart(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.StartTrace(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (r *DistributedTracingRouter) routeSpans(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.RecordSpan(w, req)
		return
	}
	if req.Method == http.MethodGet {
		r.handler.GetSpans(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (r *DistributedTracingRouter) routeRegisterLink(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.RegisterAsyncLink(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (r *DistributedTracingRouter) routeResolveLink(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.ResolveAsyncLink(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (r *DistributedTracingRouter) routeDeepSession(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.ActivateDeepSession(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}


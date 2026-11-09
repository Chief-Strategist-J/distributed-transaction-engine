package routers

import (
	"net/http"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
)

type FencingTokensRouter struct {
	handler *handlers.FencingTokensHandler
}

func NewFencingTokensRouter(handler *handlers.FencingTokensHandler) *FencingTokensRouter {
	return &FencingTokensRouter{handler: handler}
}

func (r *FencingTokensRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/fencing/acquire", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.Acquire(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/fencing/renew", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.Renew(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/fencing/release", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.Release(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/fencing/barrier", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.Barrier(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/fencing/audits", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodGet {
			r.handler.GetAudits(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
}


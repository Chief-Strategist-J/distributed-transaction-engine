package routers

import (
	"net/http"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
)

type OCCRouter struct {
	handler *handlers.OCCHandler
}

func NewOCCRouter(handler *handlers.OCCHandler) *OCCRouter {
	return &OCCRouter{handler: handler}
}

func (r *OCCRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/occ/mutate", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.Mutate(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/occ/client-edit", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.ClientEdit(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/occ/diagnose", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodGet {
			r.handler.Diagnose(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/occ/strategy", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodGet {
			r.handler.Strategy(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
}


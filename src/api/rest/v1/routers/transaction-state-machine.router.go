package routers

import (
	"net/http"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
)

type TransactionStateMachineRouter struct {
	handler *handlers.TransactionStateMachineHandler
}

func NewTransactionStateMachineRouter(h *handlers.TransactionStateMachineHandler) *TransactionStateMachineRouter {
	return &TransactionStateMachineRouter{handler: h}
}

func (r *TransactionStateMachineRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/sagas/state", func(w http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodPost:
			r.handler.InitializeState(w, req)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/v1/sagas/transitions", func(w http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodPost:
			r.handler.TransitionState(w, req)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
}

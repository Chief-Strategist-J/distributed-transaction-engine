package routers

import (
	"net/http"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
)

type ScheduledExecutionRouter struct {
	handler *handlers.ScheduledExecutionHandler
}

func NewScheduledExecutionRouter(handler *handlers.ScheduledExecutionHandler) *ScheduledExecutionRouter {
	return &ScheduledExecutionRouter{handler: handler}
}

func (r *ScheduledExecutionRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/scheduled-execution/schedules", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.handler.CreateSchedule(w, req)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
}

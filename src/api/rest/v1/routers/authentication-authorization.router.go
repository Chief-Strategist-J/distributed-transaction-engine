package routers

import (
	"net/http"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
)

type AuthenticationAuthorizationRouter struct {
	handler *handlers.AuthenticationAuthorizationHandler
}

func NewAuthenticationAuthorizationRouter(handler *handlers.AuthenticationAuthorizationHandler) *AuthenticationAuthorizationRouter {
	return &AuthenticationAuthorizationRouter{handler: handler}
}

func (r *AuthenticationAuthorizationRouter) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/auth/tokens/authenticate", r.routeToken)
	mux.HandleFunc("/api/v1/auth/api-keys/authenticate", r.routeAPIKey)
	mux.HandleFunc("/api/v1/auth/services/authenticate", r.routeService)
	mux.HandleFunc("/api/v1/auth/webhooks/authenticate", r.routeWebhook)
	mux.HandleFunc("/api/v1/auth/authorize", r.routeAuthorize)
	mux.HandleFunc("/api/v1/auth/api-keys", r.routeRegisterAPIKey)
	mux.HandleFunc("/api/v1/auth/services", r.routeRegisterService)
	mux.HandleFunc("/api/v1/auth/revocations", r.routeRevocation)
}

func (r *AuthenticationAuthorizationRouter) routeToken(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.AuthenticateToken(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (r *AuthenticationAuthorizationRouter) routeAPIKey(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.AuthenticateAPIKey(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (r *AuthenticationAuthorizationRouter) routeService(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.AuthenticateService(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (r *AuthenticationAuthorizationRouter) routeWebhook(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.AuthenticateWebhook(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (r *AuthenticationAuthorizationRouter) routeAuthorize(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.Authorize(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (r *AuthenticationAuthorizationRouter) routeRegisterAPIKey(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.RegisterAPIKey(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (r *AuthenticationAuthorizationRouter) routeRegisterService(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.RegisterServiceIdentity(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (r *AuthenticationAuthorizationRouter) routeRevocation(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		r.handler.RevokeCredential(w, req)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}


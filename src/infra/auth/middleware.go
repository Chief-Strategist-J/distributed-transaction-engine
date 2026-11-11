package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	"distributed-transaction-engine/src/shared/utils"
)

var tenantFieldNames = []string{"tenantId", "tenant_id", "tenant"}

type rejection struct {
	status  int
	code    string
	message string
}

func Middleware(verifier *Verifier, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, denied := authenticate(verifier, r)
		if denied != nil {
			reject(w, r, denied)
			return
		}
		if denied = enforceTenantScope(r, principal); denied != nil {
			reject(w, r, denied)
			return
		}
		next.ServeHTTP(w, bindPrincipal(r, principal))
	})
}

func authenticate(verifier *Verifier, r *http.Request) (Principal, *rejection) {
	token, ok := bearerToken(r)
	if !ok {
		return Principal{}, &rejection{http.StatusUnauthorized, constants.ErrCodeUnauthorized, "bearer token required"}
	}
	principal, err := verifier.Verify(token)
	if err != nil {
		observability.GetLogger().Warn(r.Context(), "bearer token rejected", observability.String("reason", err.Error()))
		return Principal{}, &rejection{http.StatusUnauthorized, constants.ErrCodeUnauthorized, "invalid or expired token"}
	}
	return principal, nil
}

func bearerToken(r *http.Request) (string, bool) {
	scheme, token, found := strings.Cut(strings.TrimSpace(r.Header.Get(constants.HeaderAuthorization)), " ")
	if !found || !strings.EqualFold(scheme, constants.AuthSchemeBearer) {
		return "", false
	}
	token = strings.TrimSpace(token)
	return token, token != "" && !strings.ContainsAny(token, " \t")
}

func enforceTenantScope(r *http.Request, principal Principal) *rejection {
	mismatch := &rejection{http.StatusForbidden, constants.ErrCodeTenantMismatch, "tenant does not match the authenticated principal"}
	if claimed := r.Header.Get(constants.HeaderXTenantID); claimed != "" && claimed != principal.TenantID {
		return mismatch
	}
	if queryTenantMismatch(r, principal.TenantID) || pathTenantMismatch(r.URL.Path, principal.TenantID) {
		return mismatch
	}
	if bodyTenantMismatch(r, principal.TenantID) {
		return mismatch
	}
	return nil
}

func queryTenantMismatch(r *http.Request, tenantID string) bool {
	values := r.URL.Query()
	for _, name := range tenantFieldNames {
		for _, value := range values[name] {
			if value != tenantID {
				return true
			}
		}
	}
	return false
}

func pathTenantMismatch(path string, tenantID string) bool {
	segments := strings.Split(strings.Trim(path, "/"), "/")
	for index := 0; index+1 < len(segments); index++ {
		if segments[index] == "tenants" && segments[index+1] != tenantID {
			return true
		}
	}
	return false
}

func bodyTenantMismatch(r *http.Request, tenantID string) bool {
	if r.Body == nil || r.Body == http.NoBody || !strings.Contains(r.Header.Get("Content-Type"), "json") {
		return false
	}
	payload, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(payload))
	if err != nil {
		return true
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(payload, &fields) != nil {
		return false
	}
	for _, name := range tenantFieldNames {
		if raw, present := fields[name]; present && !jsonStringEquals(raw, tenantID) {
			return true
		}
	}
	return false
}

func scopeContext(ctx context.Context, tenantID string, operator bool) context.Context {
	ctx = utils.WithTenant(ctx, tenantID)
	if operator {
		ctx = utils.WithPlatform(ctx)
	}
	return ctx
}

func jsonStringEquals(raw json.RawMessage, want string) bool {
	var value string
	return json.Unmarshal(raw, &value) == nil && value == want
}

func bindPrincipal(r *http.Request, principal Principal) *http.Request {
	clone := r.Clone(scopeContext(WithPrincipal(r.Context(), principal), principal.TenantID, principal.HasRole(constants.OperatorRoleSREAdmin)))
	clone.Header.Set(constants.HeaderXTenantID, principal.TenantID)
	clone.Header.Set(constants.HeaderXPrincipalID, principal.Subject)
	clone.Header.Del(constants.HeaderXOperatorRole)
	if principal.HasRole(constants.OperatorRoleSREAdmin) {
		clone.Header.Set(constants.HeaderXOperatorRole, constants.OperatorRoleSREAdmin)
	}
	return clone
}

func reject(w http.ResponseWriter, r *http.Request, denied *rejection) {
	if denied.status == http.StatusUnauthorized {
		w.Header().Set(constants.HeaderWWWAuthenticate, constants.AuthSchemeBearer)
	} else {
		observability.GetLogger().Warn(r.Context(), "request denied", observability.String("code", denied.code))
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(denied.status)
	observability.NoteSwallowed(json.NewEncoder(w).Encode(map[string]any{
		"success": false, "statusCode": denied.status, "data": nil,
		"error": map[string]any{"code": denied.code, "message": denied.message, "retryable": false},
	}))
}

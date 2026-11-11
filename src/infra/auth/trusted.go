package auth

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"distributed-transaction-engine/src/shared/constants"
)

func TrustedHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenantID := claimedTenant(r)
		operator := r.Header.Get(constants.HeaderXOperatorRole) == constants.OperatorRoleSREAdmin
		next.ServeHTTP(w, r.WithContext(scopeContext(r.Context(), tenantID, operator)))
	})
}

func claimedTenant(r *http.Request) string {
	if header := strings.TrimSpace(r.Header.Get(constants.HeaderXTenantID)); header != "" {
		return header
	}
	values := r.URL.Query()
	for _, name := range tenantFieldNames {
		if value := strings.TrimSpace(values.Get(name)); value != "" {
			return value
		}
	}
	if fromPath := pathTenant(r.URL.Path); fromPath != "" {
		return fromPath
	}
	return bodyTenant(r)
}

func pathTenant(path string) string {
	segments := strings.Split(strings.Trim(path, "/"), "/")
	for index := 0; index+1 < len(segments); index++ {
		if segments[index] == "tenants" {
			return segments[index+1]
		}
	}
	return ""
}

func bodyTenant(r *http.Request) string {
	if r.Body == nil || r.Body == http.NoBody || !strings.Contains(r.Header.Get("Content-Type"), "json") {
		return ""
	}
	payload, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(payload))
	var fields map[string]json.RawMessage
	if err != nil || json.Unmarshal(payload, &fields) != nil {
		return ""
	}
	for _, name := range tenantFieldNames {
		var value string
		if json.Unmarshal(fields[name], &value) == nil && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

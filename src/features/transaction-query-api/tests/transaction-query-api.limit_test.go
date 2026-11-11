package tests

import (
	"encoding/json"
	stderrors "errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
	queryapi "distributed-transaction-engine/src/features/transaction-query-api"
	"distributed-transaction-engine/src/features/transaction-query-api/contracts"
	rules "distributed-transaction-engine/src/features/transaction-query-api/domain/rules"
	"distributed-transaction-engine/src/shared/constants"
	apperrors "distributed-transaction-engine/src/shared/errors"
)

func newLimitHandler() *handlers.TransactionQueryHandler {
	return handlers.NewTransactionQueryHandler(queryapi.NewDefaultService(queryapi.NewMemoryAdapter()))
}

func limitRequest(method, target, body string) *http.Request {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.Header.Set(constants.HeaderXTenantID, "tenant-limit-test")
	req.Header.Set("Content-Type", "application/json")
	return req
}

func errorCode(t *testing.T, body string) string {
	t.Helper()
	var envelope struct {
		Success bool `json:"success"`
		Error   struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &envelope); err != nil {
		t.Fatalf("response is not an envelope: %v: %s", err, body)
	}
	return envelope.Error.Code
}

func TestTransactionQueryAPI_SearchLimitEnforced(t *testing.T) {
	cases := []struct {
		name   string
		limit  string
		status int
	}{
		{"one", "1", http.StatusOK},
		{"default_when_zero", "0", http.StatusOK},
		{"default_when_absent", "", http.StatusOK},
		{"max_allowed", "200", http.StatusOK},
		{"one_over_max", "201", http.StatusBadRequest},
		{"typical_abuse", "1000000", http.StatusBadRequest},
		{"int32_overflow", "2147483648", http.StatusBadRequest},
		{"int64_overflow", "99999999999999999999", http.StatusBadRequest},
		{"negative", "-1", http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := `{}`
			if tc.limit != "" {
				body = `{"limit": ` + tc.limit + `}`
			}
			rec := httptest.NewRecorder()
			newLimitHandler().SearchTransactions(rec, limitRequest(http.MethodPost, "/api/v1/transactions:search", body))
			if rec.Code != tc.status {
				t.Fatalf("limit=%q expected %d got %d: %s", tc.limit, tc.status, rec.Code, rec.Body.String())
			}
			if tc.status == http.StatusBadRequest && errorCode(t, rec.Body.String()) == "" {
				t.Fatalf("expected canonical error code in envelope: %s", rec.Body.String())
			}
		})
	}
}

func TestTransactionQueryAPI_SearchHonorsRequestedLimit(t *testing.T) {
	adapter := queryapi.NewMemoryAdapter()
	handler := handlers.NewTransactionQueryHandler(queryapi.NewDefaultService(adapter))
	for _, tc := range []struct {
		limit string
		want  int
	}{{"1", 1}, {"3", 3}, {"", 3}} {
		body := `{}`
		if tc.limit != "" {
			body = `{"limit": ` + tc.limit + `}`
		}
		rec := httptest.NewRecorder()
		handler.SearchTransactions(rec, limitRequest(http.MethodPost, "/api/v1/transactions:search", body))
		if rec.Code != http.StatusOK {
			t.Fatalf("limit=%q expected 200 got %d: %s", tc.limit, rec.Code, rec.Body.String())
		}
		var envelope struct {
			Data struct {
				Items []json.RawMessage `json:"items"`
			} `json:"data"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
			t.Fatalf("bad envelope: %v", err)
		}
		if len(envelope.Data.Items) > tc.want && tc.limit != "" {
			t.Fatalf("limit=%q returned %d items", tc.limit, len(envelope.Data.Items))
		}
	}
}

func TestTQLParser_LimitClauseBounds(t *testing.T) {
	cases := []struct {
		query string
		ok    bool
	}{
		{"status = \"COMPLETED\" LIMIT 1", true},
		{"status = \"COMPLETED\" LIMIT 200", true},
		{"status = \"COMPLETED\" LIMIT 201", false},
		{"status = \"COMPLETED\" LIMIT 1000000", false},
		{"status = \"COMPLETED\" LIMIT 0", false},
		{"status = \"COMPLETED\" LIMIT 99999999999999999999", false},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			parsed, err := contracts.ParseTQLText(tc.query)
			if tc.ok {
				if err != nil {
					t.Fatalf("expected success: %v", err)
				}
				if parsed.Limit < 1 || parsed.Limit > contracts.MaxPageSize {
					t.Fatalf("limit out of range: %d", parsed.Limit)
				}
				return
			}
			var appErr *apperrors.ApplicationError
			if !stderrors.As(err, &appErr) || appErr.StatusCode != 400 || appErr.CanonicalCode != constants.TQErrLimitExceeded {
				t.Fatalf("expected 400 %s, got %#v", constants.TQErrLimitExceeded, err)
			}
		})
	}
}

func TestValidateQuery_LimitBounds(t *testing.T) {
	registry, err := rules.ResolveRegistry(contracts.TQLVersionV1)
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	principal := contracts.Principal{TenantID: "tenant-limit-test", PrincipalID: "u1", Audience: contracts.AudienceTenantUser}
	for _, tc := range []struct {
		limit int
		ok    bool
	}{{0, true}, {1, true}, {contracts.MaxPageSize, true}, {contracts.MaxPageSize + 1, false}, {1000000, false}, {-1, false}} {
		query, parseErr := contracts.ParseTQLText("status = \"COMPLETED\"")
		if parseErr != nil {
			t.Fatalf("parse: %v", parseErr)
		}
		query.Limit = tc.limit
		normalized, validateErr := rules.ValidateQuery(registry, principal, query, nil)
		if tc.ok {
			want := tc.limit
			if want == 0 {
				want = contracts.DefaultPageSize
			}
			if validateErr != nil || normalized.Limit != want {
				t.Fatalf("limit=%d expected %d, got %d err=%v", tc.limit, want, normalized.Limit, validateErr)
			}
			continue
		}
		var appErr *apperrors.ApplicationError
		if !stderrors.As(validateErr, &appErr) || appErr.StatusCode != 400 {
			t.Fatalf("limit=%d expected 400, got %#v", tc.limit, validateErr)
		}
	}
}

func TestTransactionQueryAPI_TimelineLimitParam(t *testing.T) {
	cases := []struct {
		name   string
		limit  string
		reject bool
	}{
		{"valid", "?limit=50", false},
		{"max_allowed", "?limit=200", false},
		{"absent", "", false},
		{"one_over_max", "?limit=201", true},
		{"typical_abuse", "?limit=1000000", true},
		{"zero", "?limit=0", true},
		{"negative", "?limit=-5", true},
		{"non_numeric", "?limit=abc", true},
		{"overflow", "?limit=99999999999999999999", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := limitRequest(http.MethodGet, "/api/v1/transactions/txn_missing/timeline"+tc.limit, "")
			newLimitHandler().GetTransactionTimeline(rec, req)
			if tc.reject {
				if rec.Code != http.StatusBadRequest {
					t.Fatalf("expected 400 got %d: %s", rec.Code, rec.Body.String())
				}
				if code := errorCode(t, rec.Body.String()); code != constants.TQErrLimitExceeded {
					t.Fatalf("expected %s got %s", constants.TQErrLimitExceeded, code)
				}
				return
			}
			if rec.Code == http.StatusBadRequest {
				t.Fatalf("valid limit must not be rejected: %s", rec.Body.String())
			}
		})
	}
}

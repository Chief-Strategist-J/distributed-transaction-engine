package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"distributed-transaction-engine/src/api/rest/v1/handlers"
	queryapi "distributed-transaction-engine/src/features/transaction-query-api"
	"distributed-transaction-engine/src/features/transaction-query-api/contracts"
	"distributed-transaction-engine/src/shared/constants"
	"github.com/google/uuid"
)

type limitCase struct {
	name   string
	limit  string
	status int
}

func newLimitHandler() *handlers.TransactionQueryHandler {
	return handlers.NewTransactionQueryHandler(queryapi.NewDefaultService(queryapi.NewMemoryAdapter()))
}

func limitRequest(method string, target string, body string) *http.Request {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.Header.Set(constants.HeaderXTenantID, "tenant-limit-test")
	req.Header.Set("Content-Type", "application/json")
	return req
}

func envelopeErrorCode(t *testing.T, body string) string {
	t.Helper()
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &envelope); err != nil {
		t.Fatalf("response is not an envelope: %v: %s", err, body)
	}
	return envelope.Error.Code
}

func searchBody(limit string) string {
	if limit == "" {
		return `{}`
	}
	return `{"limit": ` + limit + `}`
}

func assertSearchLimit(t *testing.T, tc limitCase) {
	t.Helper()
	rec := httptest.NewRecorder()
	newLimitHandler().SearchTransactions(rec, limitRequest(http.MethodPost, "/api/v1/transactions:search", searchBody(tc.limit)))
	if rec.Code != tc.status {
		t.Fatalf("limit=%q expected %d got %d: %s", tc.limit, tc.status, rec.Code, rec.Body.String())
	}
	if tc.status == http.StatusBadRequest && envelopeErrorCode(t, rec.Body.String()) == "" {
		t.Fatalf("expected canonical error code: %s", rec.Body.String())
	}
}

func TestEdgeCases_SearchLimitBoundaries(t *testing.T) {
	cases := []limitCase{
		{"one", "1", http.StatusOK},
		{"default_when_zero", "0", http.StatusOK},
		{"default_when_absent", "", http.StatusOK},
		{"max_allowed", "200", http.StatusOK},
		{"one_over_max", "201", http.StatusBadRequest},
		{"typical_abuse", "1000000", http.StatusBadRequest},
		{"int32_overflow", "2147483648", http.StatusBadRequest},
		{"int64_overflow", "99999999999999999999", http.StatusBadRequest},
		{"negative", "-1", http.StatusBadRequest},
		{"fractional", "1.5", http.StatusBadRequest},
		{"string_value", `"50"`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { assertSearchLimit(t, tc) })
	}
}

func assertTimelineLimit(t *testing.T, tc limitCase) {
	t.Helper()
	rec := httptest.NewRecorder()
	target := "/api/v1/transactions/txn_missing/timeline"
	if tc.limit != "" {
		target += "?" + constants.QueryParamLimit + "=" + tc.limit
	}
	newLimitHandler().GetTransactionTimeline(rec, limitRequest(http.MethodGet, target, ""))
	rejected := rec.Code == http.StatusBadRequest
	if rejected != (tc.status == http.StatusBadRequest) {
		t.Fatalf("limit=%q expected status class %d got %d: %s", tc.limit, tc.status, rec.Code, rec.Body.String())
	}
	if rejected && envelopeErrorCode(t, rec.Body.String()) != constants.TQErrLimitExceeded {
		t.Fatalf("expected %s: %s", constants.TQErrLimitExceeded, rec.Body.String())
	}
}

func TestEdgeCases_TimelineLimitBoundaries(t *testing.T) {
	cases := []limitCase{
		{"valid", "50", http.StatusNotFound},
		{"max_allowed", "200", http.StatusNotFound},
		{"absent", "", http.StatusNotFound},
		{"one_over_max", "201", http.StatusBadRequest},
		{"typical_abuse", "1000000", http.StatusBadRequest},
		{"zero", "0", http.StatusBadRequest},
		{"negative", "-5", http.StatusBadRequest},
		{"non_numeric", "abc", http.StatusBadRequest},
		{"empty_value", "", http.StatusNotFound},
		{"overflow", "99999999999999999999", http.StatusBadRequest},
		{"hex_notation", "0x10", http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { assertTimelineLimit(t, tc) })
	}
}

func seedTransactions(t *testing.T, svc *queryapi.TransactionQueryService, tenant string, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		record := contracts.ProjectionRecord{
			TenantID: tenant, PublicID: fmt.Sprintf("txn_%026d", i), SagaID: uuid.New(),
			InternalStatus: "COMPLETED", BusinessStatus: "COMPLETED", WorkflowType: "order", WorkflowVersion: "1",
			Currency: "INR", CustomerRef: "cust", CorrelationID: uuid.New(),
		}
		if _, err := svc.UpdateTransactionProjection(context.Background(), record); err != nil {
			t.Fatalf("seed projection: %v", err)
		}
	}
}

func TestEdgeCases_SearchTruncatesToRequestedLimit(t *testing.T) {
	svc := queryapi.NewDefaultService(queryapi.NewMemoryAdapter())
	tenant := "tenant-" + uuid.NewString()
	seedTransactions(t, svc, tenant, 5)
	principal := contracts.Principal{TenantID: tenant, PrincipalID: "u1", Audience: contracts.AudienceTenantUser}
	for _, tc := range []struct{ limit, want int }{{1, 1}, {2, 2}, {5, 5}, {0, 5}} {
		resp, err := svc.SearchTransactions(context.Background(), principal, contracts.SearchRequest{Limit: tc.limit})
		if err != nil || len(resp.Items) != tc.want {
			t.Fatalf("limit=%d expected %d items, got %d err=%v", tc.limit, tc.want, len(resp.Items), err)
		}
	}
}

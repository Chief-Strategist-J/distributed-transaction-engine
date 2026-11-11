package tests

import (
	stderrors "errors"
	"strconv"
	"testing"

	"distributed-transaction-engine/src/features/transaction-query-api/contracts"
	"distributed-transaction-engine/src/shared/constants"
	apperrors "distributed-transaction-engine/src/shared/errors"
)

func TestSchema_TQLLimitClauseBounds(t *testing.T) {
	cases := []struct {
		name  string
		query string
		limit int
		valid bool
	}{
		{"min", `status = "COMPLETED" LIMIT 1`, 1, true},
		{"max", `status = "COMPLETED" LIMIT 200`, 200, true},
		{"one_over_max", `status = "COMPLETED" LIMIT 201`, 0, false},
		{"abuse", `status = "COMPLETED" LIMIT 1000000`, 0, false},
		{"zero", `status = "COMPLETED" LIMIT 0`, 0, false},
		{"int64_overflow", `status = "COMPLETED" LIMIT 99999999999999999999`, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := contracts.ParseTQLText(tc.query)
			if tc.valid {
				if err != nil || parsed.Limit != tc.limit {
					t.Fatalf("expected limit %d, got %d err=%v", tc.limit, parsed.Limit, err)
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

func TestSchema_LimitRangeErrorShape(t *testing.T) {
	err := contracts.NewLimitRangeError()
	if err.StatusCode != 400 || err.CanonicalCode != constants.TQErrLimitExceeded || err.Retryable {
		t.Fatalf("unexpected limit error: %#v", err)
	}
	if want := "limit must be an integer between 1 and " + strconv.Itoa(contracts.MaxPageSize); err.Message != want {
		t.Fatalf("expected %q got %q", want, err.Message)
	}
}

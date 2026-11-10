package rules

import (
	"crypto/sha256"
	contracts "distributed-transaction-engine/src/features/transaction-query-api/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"time"
)

const (
	tiebreakerField     = "id"
	CursorLifetime      = 30 * time.Minute
	ConsistencyLifetime = 15 * time.Minute
)

func ValidateOrderBy(state *validationContext, requested []contracts.SortKey, filter contracts.Node) (spanResult0 []contracts.SortKey, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if len(requested) > contracts.MaxSortKeys {
		return nil, queryError(400, constants.TQErrLimitExceeded, "ORDER BY exceeds 3 sort keys")
	}
	keys := make([]contracts.SortKey, 0, len(requested)+1)
	seen := make(map[string]struct{}, len(requested))
	for _, key := range requested {
		completed, err := validateSortKey(state, key, filter)
		if err != nil {
			return nil, err
		}
		if _, duplicate := seen[key.Field]; duplicate {
			return nil, queryError(400, constants.TQErrInvalidSyntax, "duplicate sort key "+key.Field)
		}
		seen[key.Field] = struct{}{}
		keys = append(keys, completed)
	}
	if _, present := seen[tiebreakerField]; !present {
		keys = append(keys, contracts.SortKey{Field: tiebreakerField, Nulls: contracts.NullsLast})
	}
	return keys, nil
}

func validateSortKey(state *validationContext, key contracts.SortKey, filter contracts.Node) (contracts.SortKey, error) {
	definition, err := resolveField(state, key.Field)
	if err != nil {
		return contracts.SortKey{}, err
	}
	if !definition.Sortable || definition.Collection != "" {
		return contracts.SortKey{}, queryError(400, constants.TQErrOperatorNotAllowed, key.Field+" is not sortable")
	}
	if definition.Type == contracts.FieldTypeMoney {
		if _, single := RootCurrency(filter); !single {
			return contracts.SortKey{}, queryError(400, constants.TQErrMoneyCurrencyRequired, "sorting by amount requires currency = X in the filter")
		}
	}
	if key.Nulls == contracts.NullsDefault {
		key.Nulls = contracts.NullsLast
		if key.Descending {
			key.Nulls = contracts.NullsFirst
		}
	}
	return key, nil
}

func HashQuery(tqlVersion string, registryVersion string, query contracts.NormalizedQuery, injected contracts.InjectedPredicates) string {
	defer observability.TraceEnter(nil)()
	payload, _ := json.Marshal(struct {
		Version  string                       `json:"v"`
		Registry string                       `json:"r"`
		Filter   contracts.Node               `json:"f"`
		OrderBy  []contracts.SortKey          `json:"o"`
		Injected contracts.InjectedPredicates `json:"i"`
	}{tqlVersion, registryVersion, query.Filter, query.OrderBy, injected})
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

func ValidateCursorClaims(claims contracts.CursorClaims, expected contracts.CursorClaims, now time.Time) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if claims.TQLVersion != expected.TQLVersion || claims.RegistryVersion != expected.RegistryVersion {
		return queryError(400, constants.TQErrCursorInvalid, "cursor was issued for a different language or registry version, restart the query")
	}
	if now.Unix() >= claims.ExpiresAt {
		return queryError(410, constants.TQErrCursorExpired, "cursor has expired, restart the query")
	}
	if claims.PrincipalID != expected.PrincipalID || claims.TenantID != expected.TenantID || claims.QueryHash != expected.QueryHash {
		return queryError(400, constants.TQErrCursorQueryMismatch, "cursor does not belong to this query and caller")
	}
	if len(claims.SortFields) != len(claims.LastValues) || len(claims.SortFields) != len(expected.SortFields) {
		return queryError(400, constants.TQErrCursorInvalid, "cursor sort position is malformed")
	}
	for index, field := range claims.SortFields {
		if field != expected.SortFields[index] {
			return queryError(400, constants.TQErrCursorQueryMismatch, "cursor sort order differs from the query")
		}
	}
	return nil
}

func SortFieldNames(keys []contracts.SortKey) []string {
	defer observability.TraceEnter(nil)()
	names := make([]string, 0, len(keys))
	for _, key := range keys {
		names = append(names, key.Field)
	}
	return names
}

func BuildCursorClaims(base contracts.CursorClaims, keys []contracts.SortKey, last contracts.Row, now time.Time) contracts.CursorClaims {
	defer observability.TraceEnter(nil)()
	values := make([]contracts.CursorValue, 0, len(keys))
	for _, key := range keys {
		values = append(values, EncodeCursorValue(last[key.Field]))
	}
	base.SortFields, base.LastValues = SortFieldNames(keys), values
	base.ExpiresAt = now.Add(CursorLifetime).Unix()
	return base
}

func EncodeCursorValue(raw any) contracts.CursorValue {
	defer observability.TraceEnter(nil)()
	switch value := raw.(type) {
	case nil:
		return contracts.CursorValue{Null: true}
	case time.Time:
		return contracts.CursorValue{Text: value.UTC().Format(time.RFC3339Nano)}
	case *time.Time:
		if value == nil {
			return contracts.CursorValue{Null: true}
		}
		return contracts.CursorValue{Text: value.UTC().Format(time.RFC3339Nano)}
	case int64:
		return contracts.CursorValue{Text: strconv.FormatInt(value, 10)}
	case string:
		return contracts.CursorValue{Text: value}
	}
	return contracts.CursorValue{Null: true}
}

func ValidateConsistencyClaims(claims contracts.ConsistencyClaims, tenantID string, now time.Time) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if claims.TenantID != tenantID {
		return queryError(400, constants.TQErrConsistencyTokenInvalid, "consistency token belongs to another tenant")
	}
	if now.Unix() >= claims.ExpiresAt {
		return queryError(400, constants.TQErrConsistencyTokenInvalid, "consistency token has expired")
	}
	return nil
}

func HashTimelineQuery(tenantID string, transactionID string) string {
	defer observability.TraceEnter(nil)()
	digest := sha256.Sum256([]byte("timeline|" + tenantID + "|" + transactionID))
	return hex.EncodeToString(digest[:])
}

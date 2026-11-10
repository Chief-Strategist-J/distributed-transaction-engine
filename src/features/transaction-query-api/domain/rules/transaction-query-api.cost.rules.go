package rules

import (
	contracts "distributed-transaction-engine/src/features/transaction-query-api/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"strconv"
	"time"
)

const (
	LargeTenantRowThreshold = 100000
	SmallTenantScanLimit    = 20000
	MaxTimeWindow           = 92 * 24 * time.Hour
	MaxEstimatedRowsScanned = 500000
	ExactCountMaxRows       = 10000
	PrincipalBucketCapacity = 200
	TenantBucketCapacity    = 2000
	PrincipalRefillPerSec   = 20
	TenantRefillPerSec      = 200
)

var selectiveFields = map[string]struct{}{
	"id": {}, "correlation_id": {}, "provider_reference": {}, "customer_ref": {}, "customer_email": {}, "merchant_ref": {},
}

var timeFields = map[string]struct{}{"created_at": {}, "effective_at": {}, "completed_at": {}}

func RouteStore(query contracts.NormalizedQuery) contracts.StoreName {
	defer observability.TraceEnter(nil)()
	if query.HasMatch {
		return contracts.StoreSearch
	}
	return contracts.StorePrimary
}

func CheckStoreCoverage(registry contracts.Registry, store contracts.StoreName, uses []contracts.FieldUse) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	for _, use := range uses {
		definition := registry.Fields[use.Field]
		if use.Usage == contracts.UsageProject {
			continue
		}
		if !IsStoreIndexed(definition, store) {
			return tooExpensive("field "+use.Field+" is not indexed in the "+string(store)+" store", "remove the field from the filter or sort, or drop MATCH")
		}
	}
	return nil
}

func tooExpensive(message string, hint string) error {
	return queryError(422, constants.TQErrQueryTooExpensive, message, appErrors.ErrorDetail{Field: "query", Issue: hint, Rule: "cost"})
}

func EvaluateQueryCost(input contracts.CostInput) (spanResult0 contracts.CostDecision, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	selective := isSelective(input.Query.Filter)
	timeBound := IsTimeBounded(input.Query.Filter, input.Now)
	if input.TenantRowEstimate >= LargeTenantRowThreshold && !timeBound {
		return contracts.CostDecision{}, queryError(422, constants.TQErrTimeRangeRequired,
			"queries on large tenants need a created_at range of at most 92 days",
			appErrors.ErrorDetail{Field: "created_at", Issue: "add created_at >= now()-30d or a BETWEEN range", Rule: "time_bound"})
	}
	if !selective && !timeBound && input.TenantRowEstimate > SmallTenantScanLimit {
		return contracts.CostDecision{}, tooExpensive("query has no selective indexed predicate", "add an id, reference or time range predicate")
	}
	return contracts.CostDecision{Selective: selective, TimeBound: timeBound, Tokens: costTokens(input, selective)}, nil
}

func costTokens(input contracts.CostInput, selective bool) int {
	tokens := 1 + len(input.Query.Uses)/4 + len(input.Query.OrderBy)
	if input.Store == contracts.StoreSearch {
		tokens += 2
	}
	if !selective {
		tokens += 3
	}
	return tokens
}

func EvaluatePlanEstimate(estimatedRows int64) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if estimatedRows > MaxEstimatedRowsScanned {
		return tooExpensive("estimated "+strconv.FormatInt(estimatedRows, 10)+" rows to scan", "narrow the time range or add a selective predicate")
	}
	return nil
}

func ExactCountAllowed(estimatedRows int64) bool {
	defer observability.TraceEnter(nil)()
	return estimatedRows <= ExactCountMaxRows
}

func isSelective(node contracts.Node) bool {
	switch node.Kind {
	case contracts.NodePredicate:
		return isSelectivePredicate(*node.Predicate)
	case contracts.NodeAnd:
		for _, child := range node.Children {
			if isSelective(child) {
				return true
			}
		}
		return false
	case contracts.NodeOr:
		for _, child := range node.Children {
			if !isSelective(child) {
				return false
			}
		}
		return true
	}
	return false
}

func isSelectivePredicate(predicate contracts.Predicate) bool {
	if _, selective := selectiveFields[predicate.Field]; !selective {
		return false
	}
	return predicate.Operator == contracts.OpEq || predicate.Operator == contracts.OpIn || predicate.Operator == contracts.OpPrefix
}

func IsTimeBounded(node contracts.Node, now time.Time) bool {
	defer observability.TraceEnter(nil)()
	if node.Kind == contracts.NodeOr {
		for _, child := range node.Children {
			if !IsTimeBounded(child, now) {
				return false
			}
		}
		return true
	}
	window, bounded := TimeWindow(node, now)
	return bounded && window <= MaxTimeWindow
}

func TimeWindow(filter contracts.Node, now time.Time) (time.Duration, bool) {
	defer observability.TraceEnter(nil)()
	conjuncts := []contracts.Node{filter}
	if filter.Kind == contracts.NodeAnd {
		conjuncts = filter.Children
	}
	for field := range timeFields {
		if window, found := windowForField(conjuncts, field, now); found {
			return window, true
		}
	}
	return 0, false
}

func windowForField(conjuncts []contracts.Node, field string, now time.Time) (time.Duration, bool) {
	var lower, upper *time.Time
	for _, node := range conjuncts {
		if node.Kind != contracts.NodePredicate || node.Predicate.Field != field {
			continue
		}
		low, high := boundsOf(*node.Predicate)
		lower, upper = tighterLower(lower, low), tighterUpper(upper, high)
	}
	if lower == nil {
		return 0, false
	}
	end := now
	if upper != nil {
		end = *upper
	}
	if end.Before(*lower) {
		return 0, true
	}
	return end.Sub(*lower), true
}

func boundsOf(predicate contracts.Predicate) (*time.Time, *time.Time) {
	instants := make([]time.Time, 0, len(predicate.Values))
	for _, value := range predicate.Values {
		parsed, err := time.Parse(time.RFC3339Nano, value.Text)
		if err != nil {
			return nil, nil
		}
		instants = append(instants, parsed)
	}
	switch predicate.Operator {
	case contracts.OpGe, contracts.OpGt:
		return &instants[0], nil
	case contracts.OpLt, contracts.OpLe:
		return nil, &instants[0]
	case contracts.OpBetween:
		return &instants[0], &instants[1]
	}
	return nil, nil
}

func tighterLower(current *time.Time, candidate *time.Time) *time.Time {
	if candidate != nil && (current == nil || candidate.After(*current)) {
		return candidate
	}
	return current
}

func tighterUpper(current *time.Time, candidate *time.Time) *time.Time {
	if candidate != nil && (current == nil || candidate.Before(*current)) {
		return candidate
	}
	return current
}

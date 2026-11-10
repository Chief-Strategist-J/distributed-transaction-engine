package rules

import (
	contracts "distributed-transaction-engine/src/features/transaction-query-api/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	"time"
)

var bucketSizes = map[string]time.Duration{"hour": time.Hour, "day": 24 * time.Hour, "week": 7 * 24 * time.Hour, "month": 31 * 24 * time.Hour}

func ValidateAggregates(registry contracts.Registry, principal contracts.Principal, request contracts.AggregateRequest, filter contracts.Node, now time.Time) (spanResult0 []contracts.FieldUse, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	state := &validationContext{registry: registry, principal: principal}
	for _, expression := range request.Aggregates {
		if err := validateAggregateExpression(state, request, expression, filter); err != nil {
			return nil, err
		}
	}
	for _, field := range request.GroupBy {
		if err := validateGroupField(state, field); err != nil {
			return nil, err
		}
	}
	if request.TimeBucket != nil {
		if err := validateTimeBucket(state, *request.TimeBucket, filter, now); err != nil {
			return nil, err
		}
	}
	if len(request.GroupBy) == 0 && request.TimeBucket == nil && len(request.Aggregates) == 0 {
		return nil, queryError(400, constants.TQErrInvalidSyntax, "aggregate request has nothing to compute")
	}
	return dedupeUses(state.uses), nil
}

func validateAggregateExpression(state *validationContext, request contracts.AggregateRequest, expression contracts.AggregateExpression, filter contracts.Node) error {
	switch expression.Function {
	case contracts.AggCount:
		return requireNoField(expression)
	case contracts.AggSum, contracts.AggAvg, contracts.AggMin, contracts.AggMax:
		return validateMoneyAggregate(state, request, expression, filter)
	case contracts.AggCountDistinct:
		return validateDistinctAggregate(state, expression)
	}
	return queryError(400, constants.TQErrOperatorNotAllowed, "unsupported aggregate function")
}

func requireNoField(expression contracts.AggregateExpression) error {
	if expression.Field != "" {
		return queryError(400, constants.TQErrInvalidSyntax, "COUNT takes no field")
	}
	return nil
}

func validateMoneyAggregate(state *validationContext, request contracts.AggregateRequest, expression contracts.AggregateExpression, filter contracts.Node) error {
	definition, err := resolveField(state, expression.Field)
	if err != nil {
		return err
	}
	if definition.Type != contracts.FieldTypeMoney {
		return queryError(400, constants.TQErrOperatorNotAllowed, string(expression.Function)+" is only supported on money fields")
	}
	_, single := RootCurrency(filter)
	if !single && !containsString(request.GroupBy, currencyField) {
		return queryError(400, constants.TQErrMoneyCurrencyRequired, "money aggregates require GROUP BY currency or a single-currency filter")
	}
	state.uses = append(state.uses, contracts.FieldUse{Field: definition.Name, Usage: contracts.UsageAggregate})
	return nil
}

func validateDistinctAggregate(state *validationContext, expression contracts.AggregateExpression) error {
	definition, err := resolveField(state, expression.Field)
	if err != nil {
		return err
	}
	if definition.Collection != "" || definition.Type == contracts.FieldTypeMoney || definition.Type == contracts.FieldTypeSet || definition.Type == contracts.FieldTypeText {
		return queryError(400, constants.TQErrOperatorNotAllowed, "COUNT_DISTINCT is not supported on "+expression.Field)
	}
	state.uses = append(state.uses, contracts.FieldUse{Field: definition.Name, Usage: contracts.UsageAggregate})
	return nil
}

func validateGroupField(state *validationContext, field string) error {
	definition, err := resolveField(state, field)
	if err != nil {
		return err
	}
	if !definition.Groupable || definition.Collection != "" {
		return queryError(400, constants.TQErrOperatorNotAllowed, field+" is not groupable")
	}
	state.uses = append(state.uses, contracts.FieldUse{Field: definition.Name, Usage: contracts.UsageGroup})
	return nil
}

func validateTimeBucket(state *validationContext, bucket contracts.TimeBucket, filter contracts.Node, now time.Time) error {
	definition, err := resolveField(state, bucket.Field)
	if err != nil {
		return err
	}
	size, validSize := bucketSizes[bucket.Size]
	if definition.Type != contracts.FieldTypeTimestamp || !definition.Groupable || !validSize {
		return queryError(400, constants.TQErrOperatorNotAllowed, "time_bucket requires a groupable timestamp field and size hour, day, week or month")
	}
	if _, loadErr := time.LoadLocation(bucket.Zone); loadErr != nil || bucket.Zone == "" {
		return queryError(400, constants.TQErrTimezoneRequired, "time_bucket requires an explicit valid time zone")
	}
	window, bounded := TimeWindow(filter, now)
	if !bounded || int(window/size) > contracts.MaxTimeBuckets {
		return tooExpensive("time_bucket would produce more than 366 buckets or has no time bound", "add a created_at range or use a larger bucket size")
	}
	state.uses = append(state.uses, contracts.FieldUse{Field: definition.Name, Usage: contracts.UsageGroup})
	return nil
}

func ValidateGroupCount(groupCount int) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if groupCount > contracts.MaxAggregateGroups {
		return tooExpensive("aggregate produced more than 1000 groups", "group by fewer fields or narrow the filter")
	}
	return nil
}

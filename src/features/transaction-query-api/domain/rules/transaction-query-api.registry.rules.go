package rules

import (
	contracts "distributed-transaction-engine/src/features/transaction-query-api/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"sort"
	"strings"
)

var businessStatuses = []string{"PENDING", "PROCESSING", "COMPLETED", "FAILED", "COMPENSATING", "COMPENSATED", "CANCELLED"}

var internalStatuses = []string{
	"CREATED", "RUNNING", "ROLLING_FORWARD", "STUCK_FORWARD", "COMPENSATING", "COMPENSATION_STUCK",
	"COMPLETED", "FAILED", "FAILED_COMPENSATED", "CANCELLED",
}

var stepStatuses = []string{
	"NOT_STARTED", "PENDING", "DISPATCHED", "RETRY_WAIT", "SUCCEEDED", "FAILED", "UNKNOWN", "COMPENSATING", "COMPENSATED", "SKIPPED",
}

var compensationStatuses = []string{"NONE", "IN_PROGRESS", "COMPLETED", "FAILED"}

var currencyExponents = map[string]int{
	"USD": 2, "EUR": 2, "GBP": 2, "INR": 2, "AUD": 2, "CAD": 2, "SGD": 2, "AED": 2, "CHF": 2, "JPY": 0, "KWD": 3,
}

var internalToBusinessStatus = map[string]string{
	"CREATED": "PENDING", "RUNNING": "PROCESSING", "ROLLING_FORWARD": "PROCESSING", "STUCK_FORWARD": "PROCESSING",
	"COMPENSATING": "COMPENSATING", "COMPENSATION_STUCK": "COMPENSATING", "COMPLETED": "COMPLETED",
	"FAILED": "FAILED", "FAILED_COMPENSATED": "COMPENSATED", "CANCELLED": "CANCELLED",
}

var both = []contracts.StoreName{contracts.StorePrimary, contracts.StoreSearch}
var primaryOnly = []contracts.StoreName{contracts.StorePrimary}
var searchOnly = []contracts.StoreName{contracts.StoreSearch}

var (
	opsEquality  = []contracts.Operator{contracts.OpEq, contracts.OpIn}
	opsEnum      = []contracts.Operator{contracts.OpEq, contracts.OpNe, contracts.OpIn, contracts.OpNotIn}
	opsTime      = []contracts.Operator{contracts.OpLt, contracts.OpLe, contracts.OpGt, contracts.OpGe, contracts.OpBetween}
	opsTimeNull  = []contracts.Operator{contracts.OpLt, contracts.OpLe, contracts.OpGt, contracts.OpGe, contracts.OpBetween, contracts.OpIsNull, contracts.OpIsNotNull}
	opsMoney     = []contracts.Operator{contracts.OpEq, contracts.OpLt, contracts.OpLe, contracts.OpGt, contracts.OpGe, contracts.OpBetween}
	opsInteger   = []contracts.Operator{contracts.OpEq, contracts.OpNe, contracts.OpLt, contracts.OpLe, contracts.OpGt, contracts.OpGe, contracts.OpBetween, contracts.OpIn}
	opsReference = []contracts.Operator{contracts.OpEq, contracts.OpIn, contracts.OpPrefix}
	opsSet       = []contracts.Operator{contracts.OpHas, contracts.OpHasAny, contracts.OpHasAll}
)

func baseField(name string, fieldType contracts.FieldType, operators []contracts.Operator, stores []contracts.StoreName) contracts.FieldDefinition {
	return contracts.FieldDefinition{
		Name: name, Type: fieldType, Operators: operators, Filterable: true, Projectable: true, IndexedIn: stores,
		Entitlement: contracts.EntitlementPublic, Mask: contracts.MaskNone, SinceVersion: contracts.TQLVersionV1, MaxLength: contracts.MaxStringLiteral,
	}
}

func withEntitlement(definition contracts.FieldDefinition, class contracts.EntitlementClass) contracts.FieldDefinition {
	definition.Entitlement = class
	return definition
}

func sortableGroupable(definition contracts.FieldDefinition, sortable bool, groupable bool) contracts.FieldDefinition {
	definition.Sortable, definition.Groupable = sortable, groupable
	return definition
}

func buildRegistryV1() contracts.Registry {
	fields := make(map[string]contracts.FieldDefinition)
	add := func(definition contracts.FieldDefinition) { fields[definition.Name] = definition }

	add(sortableGroupable(baseField("id", contracts.FieldTypeOpaqueID, opsEquality, both), true, false))
	status := sortableGroupable(baseField("status", contracts.FieldTypeEnum, opsEnum, both), false, true)
	status.EnumValues = businessStatuses
	status.OperatorEnum = internalStatuses
	add(status)
	internal := withEntitlement(sortableGroupable(baseField("internal_status", contracts.FieldTypeEnum, opsEnum, primaryOnly), false, true), contracts.EntitlementRestricted)
	internal.EnumValues, internal.OperatorOnly = internalStatuses, true
	add(internal)
	add(sortableGroupable(baseField("workflow_type", contracts.FieldTypeReference, opsEquality, both), false, true))
	add(withEntitlement(baseField("workflow_version", contracts.FieldTypeReference, opsEquality, primaryOnly), contracts.EntitlementInternal))
	add(sortableGroupable(baseField("created_at", contracts.FieldTypeTimestamp, opsTime, both), true, true))
	completed := sortableGroupable(baseField("completed_at", contracts.FieldTypeTimestamp, opsTimeNull, both), true, true)
	completed.Nullable = true
	add(completed)
	add(sortableGroupable(baseField("effective_at", contracts.FieldTypeTimestamp, opsTime, primaryOnly), true, true))
	add(withEntitlement(sortableGroupable(baseField("amount", contracts.FieldTypeMoney, opsMoney, primaryOnly), true, false), contracts.EntitlementFinancial))
	currency := sortableGroupable(baseField("currency", contracts.FieldTypeEnum, opsEquality, both), false, true)
	currency.EnumValues = sortedKeys(currencyExponents)
	add(currency)
	add(baseField("customer_ref", contracts.FieldTypeOpaqueID, opsEquality, both))
	email := withEntitlement(baseField("customer_email", contracts.FieldTypeSensitive, []contracts.Operator{contracts.OpEq}, primaryOnly), contracts.EntitlementPII)
	email.Mask = contracts.MaskEmailPartial
	add(email)
	merchant := baseField("merchant_ref", contracts.FieldTypeReference, opsReference, both)
	merchant.Nullable = true
	add(merchant)
	provider := withEntitlement(baseField("provider_reference", contracts.FieldTypeReference, []contracts.Operator{contracts.OpEq}, primaryOnly), contracts.EntitlementRestricted)
	provider.Mask, provider.Nullable = contracts.MaskLastFour, true
	add(provider)
	add(withEntitlement(baseField("correlation_id", contracts.FieldTypeOpaqueID, []contracts.Operator{contracts.OpEq}, primaryOnly), contracts.EntitlementInternal))
	add(baseField("tags", contracts.FieldTypeSet, opsSet, both))
	description := baseField("description", contracts.FieldTypeText, []contracts.Operator{contracts.OpMatch}, searchOnly)
	description.Searchable = true
	description.Nullable = true
	add(description)
	compensation := baseField("compensation_status", contracts.FieldTypeEnum, opsEquality, primaryOnly)
	compensation.EnumValues = compensationStatuses
	add(sortableGroupable(compensation, false, true))
	attention := withEntitlement(baseField("needs_attention", contracts.FieldTypeBoolean, []contracts.Operator{contracts.OpEq}, primaryOnly), contracts.EntitlementRestricted)
	attention.OperatorOnly = true
	add(attention)
	addStepFields(add)
	return contracts.Registry{
		TQLVersion: contracts.TQLVersionV1, RegistryVersion: contracts.RegistryVersionV1, Fields: fields,
		Collections: map[string]contracts.CollectionDefinition{
			"steps": {Name: "steps", Fields: []string{"steps.name", "steps.status", "steps.attempts", "steps.last_error_class", "steps.completed_at"}},
		},
	}
}

func addStepFields(add func(contracts.FieldDefinition)) {
	stepField := func(name string, fieldType contracts.FieldType, operators []contracts.Operator) contracts.FieldDefinition {
		definition := baseField("steps."+name, fieldType, operators, primaryOnly)
		definition.Collection, definition.Projectable = "steps", false
		return definition
	}
	add(stepField("name", contracts.FieldTypeReference, opsEquality))
	status := stepField("status", contracts.FieldTypeEnum, opsEnum)
	status.EnumValues = stepStatuses
	add(status)
	add(stepField("attempts", contracts.FieldTypeInteger, opsInteger))
	lastError := withEntitlement(stepField("last_error_class", contracts.FieldTypeReference, opsEquality), contracts.EntitlementInternal)
	lastError.Nullable = true
	add(lastError)
	completed := stepField("completed_at", contracts.FieldTypeTimestamp, opsTimeNull)
	completed.Nullable = true
	add(completed)
}

var registryV1 = buildRegistryV1()

var registries = map[string]contracts.Registry{contracts.TQLVersionV1: registryV1}

func sortedKeys(source map[string]int) []string {
	keys := make([]string, 0, len(source))
	for key := range source {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func ResolveRegistry(version string) (spanResult0 contracts.Registry, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	registry, supported := registries[version]
	if !supported {
		return contracts.Registry{}, appErrors.NewTypedError(400, constants.TQErrUnsupportedTQLVersion, "tql_version "+version+" is not supported")
	}
	return registry, nil
}

func CurrencyExponent(currency string) (int, bool) {
	defer observability.TraceEnter(nil)()
	exponent, known := currencyExponents[currency]
	return exponent, known
}

func MapInternalToBusinessStatus(internal string) (string, bool) {
	defer observability.TraceEnter(nil)()
	business, known := internalToBusinessStatus[internal]
	return business, known
}

func AllowedEnumValues(definition contracts.FieldDefinition, audience contracts.Audience) []string {
	defer observability.TraceEnter(nil)()
	if len(definition.OperatorEnum) > 0 && (audience == contracts.AudienceOperator || audience == contracts.AudienceInternalService) {
		return definition.OperatorEnum
	}
	return definition.EnumValues
}

func SuggestEnumValues(allowed []string, given string) []string {
	defer observability.TraceEnter(nil)()
	suggestions := make([]string, 0, 3)
	for _, candidate := range allowed {
		if editDistance(strings.ToUpper(given), candidate) <= 2 {
			suggestions = append(suggestions, candidate)
		}
	}
	sort.Strings(suggestions)
	if len(suggestions) > 3 {
		return suggestions[:3]
	}
	return suggestions
}

func editDistance(left string, right string) int {
	previous := make([]int, len(right)+1)
	for column := range previous {
		previous[column] = column
	}
	for row := 1; row <= len(left); row++ {
		current := make([]int, len(right)+1)
		current[0] = row
		for column := 1; column <= len(right); column++ {
			cost := 1
			if left[row-1] == right[column-1] {
				cost = 0
			}
			current[column] = minimum(previous[column]+1, current[column-1]+1, previous[column-1]+cost)
		}
		previous = current
	}
	return previous[len(right)]
}

func minimum(first int, rest ...int) int {
	smallest := first
	for _, candidate := range rest {
		if candidate < smallest {
			smallest = candidate
		}
	}
	return smallest
}

func IsStoreIndexed(definition contracts.FieldDefinition, store contracts.StoreName) bool {
	defer observability.TraceEnter(nil)()
	for _, indexed := range definition.IndexedIn {
		if indexed == store {
			return true
		}
	}
	return false
}

func SupportsOperator(definition contracts.FieldDefinition, operator contracts.Operator) bool {
	defer observability.TraceEnter(nil)()
	for _, supported := range definition.Operators {
		if supported == operator {
			return true
		}
	}
	return false
}

func sortedStrings(values []string) []string {
	sorted := append([]string(nil), values...)
	sort.Strings(sorted)
	return sorted
}

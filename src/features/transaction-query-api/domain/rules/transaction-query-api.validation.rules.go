package rules

import (
	contracts "distributed-transaction-engine/src/features/transaction-query-api/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"encoding/json"
	"github.com/google/uuid"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	integerLiteralPattern = regexp.MustCompile(`^\d{1,18}$`)
	offsetSuffixPattern   = regexp.MustCompile(`(Z|[+-]\d{2}:\d{2})$`)
	dateOnlyPattern       = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
)

type validationContext struct {
	registry   contracts.Registry
	principal  contracts.Principal
	params     map[string]string
	collection string
	uses       []contracts.FieldUse
}

func queryError(status int, code string, message string, details ...appErrors.ErrorDetail) error {
	return appErrors.NewTypedError(status, code, message, details...)
}

func resolveField(state *validationContext, name string) (contracts.FieldDefinition, error) {
	definition, known := state.registry.Fields[name]
	if !known {
		if IsEnumerationSensitive(state.principal) {
			return contracts.FieldDefinition{}, queryError(403, constants.TQErrFieldNotPermitted, "field is not permitted")
		}
		return contracts.FieldDefinition{}, queryError(400, constants.TQErrUnknownField, "unknown field "+name)
	}
	if !IsFieldEntitled(definition, state.principal) {
		return contracts.FieldDefinition{}, queryError(403, constants.TQErrFieldNotPermitted, "field is not permitted")
	}
	return definition, nil
}

func ValidateQuery(registry contracts.Registry, principal contracts.Principal, query contracts.Query, params map[string]string) (spanResult0 contracts.NormalizedQuery, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if err := contracts.ValidateQueryStructure(query); err != nil {
		return contracts.NormalizedQuery{}, err
	}
	state := &validationContext{registry: registry, principal: principal, params: params}
	typed, err := validateNode(state, *query.Filter)
	if err != nil {
		return contracts.NormalizedQuery{}, err
	}
	if err = checkMoneyCurrencyRule(typed, nil); err != nil {
		return contracts.NormalizedQuery{}, err
	}
	normalized := NormalizeNode(typed)
	if err = checkOrBranchLimit(normalized); err != nil {
		return contracts.NormalizedQuery{}, err
	}
	return finishQuery(state, query, normalized)
}

func finishQuery(state *validationContext, query contracts.Query, filter contracts.Node) (contracts.NormalizedQuery, error) {
	limit, err := validateLimit(query.Limit)
	if err != nil {
		return contracts.NormalizedQuery{}, err
	}
	orderBy, err := ValidateOrderBy(state, query.OrderBy, filter)
	if err != nil {
		return contracts.NormalizedQuery{}, err
	}
	for _, key := range orderBy {
		state.uses = append(state.uses, contracts.FieldUse{Field: key.Field, Usage: contracts.UsageSort})
	}
	return contracts.NormalizedQuery{
		Filter: filter, OrderBy: orderBy, Limit: limit, Uses: dedupeUses(state.uses),
		Contradiction: detectContradiction(filter), HasMatch: containsMatch(filter), Warnings: collectDeprecations(state),
	}, nil
}

func validateLimit(limit int) (int, error) {
	if limit == 0 {
		return contracts.DefaultPageSize, nil
	}
	if limit < 0 || limit > contracts.MaxPageSize {
		return 0, queryError(400, constants.TQErrLimitExceeded, "LIMIT must be between 1 and 200")
	}
	return limit, nil
}

func validateNode(state *validationContext, node contracts.Node) (contracts.Node, error) {
	switch node.Kind {
	case contracts.NodePredicate:
		return validatePredicateNode(state, node)
	case contracts.NodeQuantified:
		return validateQuantifiedNode(state, node)
	}
	children := make([]contracts.Node, 0, len(node.Children))
	for _, child := range node.Children {
		typed, err := validateNode(state, child)
		if err != nil {
			return contracts.Node{}, err
		}
		children = append(children, typed)
	}
	return contracts.Node{Kind: node.Kind, Children: children}, nil
}

func validateQuantifiedNode(state *validationContext, node contracts.Node) (contracts.Node, error) {
	collection := node.Quantified.Collection
	if _, known := state.registry.Collections[collection]; !known {
		if IsEnumerationSensitive(state.principal) {
			return contracts.Node{}, queryError(403, constants.TQErrFieldNotPermitted, "field is not permitted")
		}
		return contracts.Node{}, queryError(400, constants.TQErrUnknownField, "unknown collection "+collection)
	}
	if state.collection != "" {
		return contracts.Node{}, queryError(400, constants.TQErrLimitExceeded, "nested collections are not supported")
	}
	state.collection = collection
	body, err := validateNode(state, *node.Quantified.Body)
	state.collection = ""
	if err != nil {
		return contracts.Node{}, err
	}
	return contracts.Node{Kind: contracts.NodeQuantified, Quantified: &contracts.QuantifiedNode{Collection: collection, Quantifier: node.Quantified.Quantifier, Body: &body}}, nil
}

func validatePredicateNode(state *validationContext, node contracts.Node) (contracts.Node, error) {
	predicate := node.Predicate
	definition, err := resolveField(state, predicate.Field)
	if err != nil {
		return contracts.Node{}, err
	}
	if err = checkFieldContext(state, definition); err != nil {
		return contracts.Node{}, err
	}
	if err = checkOperatorAndArity(definition, *predicate); err != nil {
		return contracts.Node{}, err
	}
	values, err := typeValues(state, definition, *predicate)
	if err != nil {
		return contracts.Node{}, err
	}
	usage := contracts.UsageFilter
	if predicate.Operator == contracts.OpMatch {
		usage = contracts.UsageMatch
	}
	state.uses = append(state.uses, contracts.FieldUse{Field: definition.Name, Usage: usage})
	return contracts.Node{Kind: contracts.NodePredicate, Predicate: &contracts.Predicate{Field: definition.Name, Operator: predicate.Operator, Values: values}}, nil
}

func checkFieldContext(state *validationContext, definition contracts.FieldDefinition) error {
	if definition.Collection != "" && state.collection != definition.Collection {
		return queryError(400, constants.TQErrNestedFieldOutsideQuant, definition.Name+" can only be used inside "+definition.Collection+" ANY, ALL or NONE")
	}
	if definition.Collection == "" && state.collection != "" {
		return queryError(400, constants.TQErrUnknownField, definition.Name+" is not an element field of "+state.collection)
	}
	if !definition.Filterable {
		return queryError(400, constants.TQErrOperatorNotAllowed, definition.Name+" is not filterable")
	}
	return nil
}

func checkOperatorAndArity(definition contracts.FieldDefinition, predicate contracts.Predicate) error {
	if !SupportsOperator(definition, predicate.Operator) {
		return queryError(400, constants.TQErrOperatorNotAllowed, string(predicate.Operator)+" is not allowed on "+definition.Name)
	}
	count := len(predicate.Values)
	switch predicate.Operator {
	case contracts.OpIsNull, contracts.OpIsNotNull:
		return requireArity(count == 0, predicate.Operator)
	case contracts.OpBetween:
		return requireArity(count == 2, predicate.Operator)
	case contracts.OpIn, contracts.OpNotIn, contracts.OpHasAny, contracts.OpHasAll:
		return requireArity(count >= 1, predicate.Operator)
	}
	return requireArity(count == 1, predicate.Operator)
}

func requireArity(valid bool, operator contracts.Operator) error {
	if valid {
		return nil
	}
	return queryError(400, constants.TQErrInvalidSyntax, "wrong number of values for "+string(operator))
}

func typeValues(state *validationContext, definition contracts.FieldDefinition, predicate contracts.Predicate) ([]contracts.Value, error) {
	typed := make([]contracts.Value, 0, len(predicate.Values))
	for _, value := range predicate.Values {
		bound, err := bindParam(state, definition, value)
		if err != nil {
			return nil, err
		}
		converted, err := typeValue(state, definition, predicate.Operator, bound)
		if err != nil {
			return nil, err
		}
		typed = append(typed, converted)
	}
	return typed, nil
}

func bindParam(state *validationContext, definition contracts.FieldDefinition, value contracts.Value) (contracts.Value, error) {
	if value.Kind != contracts.ValueParam {
		return value, nil
	}
	raw, bound := state.params[value.Text]
	if !bound {
		return contracts.Value{}, queryError(400, constants.TQErrTypeMismatch, "parameter :"+value.Text+" is not bound")
	}
	return contracts.ParseParamValue(definition.Type, raw)
}

func typeValue(state *validationContext, definition contracts.FieldDefinition, operator contracts.Operator, value contracts.Value) (contracts.Value, error) {
	switch definition.Type {
	case contracts.FieldTypeEnum:
		return typeEnum(state, definition, value)
	case contracts.FieldTypeInteger:
		return typeInteger(value)
	case contracts.FieldTypeBoolean:
		return requireKind(value, contracts.ValueBoolean, "a boolean literal")
	case contracts.FieldTypeTimestamp:
		return typeTimestamp(value)
	case contracts.FieldTypeMoney:
		return typeMoney(value)
	}
	return typeString(definition, operator, value)
}

func requireKind(value contracts.Value, kind contracts.ValueKind, label string) (contracts.Value, error) {
	if value.Kind != kind {
		return contracts.Value{}, queryError(400, constants.TQErrTypeMismatch, "expected "+label)
	}
	return value, nil
}

func typeString(definition contracts.FieldDefinition, operator contracts.Operator, value contracts.Value) (contracts.Value, error) {
	if _, err := requireKind(value, contracts.ValueString, "a string literal"); err != nil {
		return contracts.Value{}, err
	}
	text, err := contracts.NormalizeLiteralText(value.Text)
	if err != nil {
		return contracts.Value{}, err
	}
	if text == "" || utf8.RuneCountInString(text) > definition.MaxLength {
		return contracts.Value{}, queryError(400, constants.TQErrLimitExceeded, definition.Name+" literal must be 1.."+strconv.Itoa(definition.MaxLength)+" characters")
	}
	if operator == contracts.OpPrefix && utf8.RuneCountInString(text) < contracts.MinPrefixLength {
		return contracts.Value{}, queryError(400, constants.TQErrPrefixTooShort, "PREFIX requires at least 3 characters")
	}
	return contracts.Value{Kind: contracts.ValueString, Text: text}, nil
}

func typeEnum(state *validationContext, definition contracts.FieldDefinition, value contracts.Value) (contracts.Value, error) {
	if _, err := requireKind(value, contracts.ValueString, "a string literal"); err != nil {
		return contracts.Value{}, err
	}
	allowed := AllowedEnumValues(definition, state.principal.Audience)
	for _, candidate := range allowed {
		if candidate == value.Text {
			return value, nil
		}
	}
	return contracts.Value{}, queryError(400, constants.TQErrUnknownEnumValue, "unknown value for "+definition.Name,
		appErrors.ErrorDetail{Field: definition.Name, Issue: "did you mean: " + strings.Join(SuggestEnumValues(allowed, value.Text), ", "), Rule: "enum"})
}

func typeInteger(value contracts.Value) (contracts.Value, error) {
	if value.Kind != contracts.ValueNumber || !integerLiteralPattern.MatchString(value.Text) {
		return contracts.Value{}, queryError(400, constants.TQErrTypeMismatch, "expected a non-negative integer literal")
	}
	return value, nil
}

func typeMoney(value contracts.Value) (contracts.Value, error) {
	if value.Kind != contracts.ValueMoney {
		return contracts.Value{}, queryError(400, constants.TQErrTypeMismatch, "a money literal requires a currency, for example 125.50 INR")
	}
	exponent, known := CurrencyExponent(value.Currency)
	if !known {
		return contracts.Value{}, queryError(400, constants.TQErrUnknownEnumValue, "unsupported currency "+value.Currency)
	}
	minor, err := ToMinorUnits(value.Text, exponent)
	if err != nil {
		return contracts.Value{}, err
	}
	return contracts.Value{Kind: contracts.ValueMoney, Text: minor, Currency: value.Currency}, nil
}

func ToMinorUnits(decimal string, exponent int) (spanResult0 string, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	whole, fraction, _ := strings.Cut(decimal, ".")
	if len(fraction) > exponent || whole == "" || len(whole)+exponent > 18 {
		return "", queryError(400, constants.TQErrTypeMismatch, "amount precision exceeds the currency exponent or is out of range")
	}
	for len(fraction) < exponent {
		fraction += "0"
	}
	minor := strings.TrimLeft(whole+fraction, "0")
	if minor == "" {
		return "0", nil
	}
	return minor, nil
}

func typeTimestamp(value contracts.Value) (contracts.Value, error) {
	if value.Kind == contracts.ValueRelative {
		return validateRelative(value)
	}
	if value.Kind == contracts.ValueString {
		value = contracts.Value{Kind: contracts.ValueTime, Text: value.Text}
	}
	if value.Kind != contracts.ValueTime {
		return contracts.Value{}, queryError(400, constants.TQErrTypeMismatch, "expected a timestamp literal")
	}
	return validateTimeLiteral(value)
}

func validateTimeLiteral(value contracts.Value) (contracts.Value, error) {
	if value.Zone != "" {
		if _, err := time.LoadLocation(value.Zone); err != nil || !dateOnlyPattern.MatchString(value.Text) {
			return contracts.Value{}, queryError(400, constants.TQErrTypeMismatch, "unknown time zone or malformed date@zone literal")
		}
		return value, nil
	}
	if !strings.Contains(value.Text, "T") || !offsetSuffixPattern.MatchString(value.Text) {
		return contracts.Value{}, queryError(400, constants.TQErrTimezoneRequired, "timestamps must carry an offset or use date@zone")
	}
	if _, err := time.Parse(time.RFC3339Nano, value.Text); err != nil {
		return contracts.Value{}, queryError(400, constants.TQErrTypeMismatch, "malformed timestamp literal")
	}
	return value, nil
}

func validateRelative(value contracts.Value) (contracts.Value, error) {
	relative := value.Relative
	if relative == nil || (relative.Base != contracts.RelativeNow && relative.Base != contracts.RelativeStartOf) {
		return contracts.Value{}, queryError(400, constants.TQErrTypeMismatch, "malformed relative time")
	}
	if relative.Base == contracts.RelativeStartOf {
		if _, err := time.LoadLocation(relative.Zone); err != nil || !containsString([]string{"day", "week", "month"}, relative.StartOf) {
			return contracts.Value{}, queryError(400, constants.TQErrTimezoneRequired, "start_of requires day, week or month and a valid time zone")
		}
	}
	if relative.Base == contracts.RelativeNow && relative.Sign != 0 && (relative.Amount < 1 || !containsString([]string{"m", "h", "d", "w"}, relative.Unit)) {
		return contracts.Value{}, queryError(400, constants.TQErrTypeMismatch, "malformed duration")
	}
	return value, nil
}

func marshalCanonical(node contracts.Node) string {
	encoded, _ := json.Marshal(node)
	return string(encoded)
}

func dedupeUses(uses []contracts.FieldUse) []contracts.FieldUse {
	seen := make(map[contracts.FieldUse]struct{}, len(uses))
	unique := make([]contracts.FieldUse, 0, len(uses))
	for _, use := range uses {
		if _, duplicate := seen[use]; !duplicate {
			seen[use] = struct{}{}
			unique = append(unique, use)
		}
	}
	return unique
}

func collectDeprecations(state *validationContext) []string {
	warnings := make([]string, 0)
	for _, use := range dedupeUses(state.uses) {
		definition := state.registry.Fields[use.Field]
		if definition.Deprecated {
			warnings = append(warnings, "field "+definition.Name+" is deprecated and will be removed in version "+definition.RemovalVersion)
		}
	}
	return sortedStrings(warnings)
}

func containsMatch(node contracts.Node) bool {
	if node.Kind == contracts.NodePredicate {
		return node.Predicate.Operator == contracts.OpMatch
	}
	if node.Kind == contracts.NodeQuantified {
		return containsMatch(*node.Quantified.Body)
	}
	for _, child := range node.Children {
		if containsMatch(child) {
			return true
		}
	}
	return false
}

var (
	publicIDPattern  = regexp.MustCompile(`^txn_[0-9a-zA-Z]{26}$`)
	hex64Pattern     = regexp.MustCompile(`^[0-9a-f]{64}$`)
	terminalBusiness = map[string]struct{}{"COMPLETED": {}, "FAILED": {}, "COMPENSATED": {}, "CANCELLED": {}}
)

func IsValidTransactionPublicID(publicID string) bool {
	defer observability.TraceEnter(nil)()
	return publicIDPattern.MatchString(publicID)
}

func ValidateProjectionRecord(record contracts.ProjectionRecord) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	business, mapped := MapInternalToBusinessStatus(record.InternalStatus)
	switch {
	case record.TenantID == "" || record.OwnerRef == "" || record.WorkflowType == "" || record.WorkflowVersion == "" || record.CustomerRef == "":
		return appErrors.NewValidationError("projection record is missing required identity fields")
	case !IsValidTransactionPublicID(record.PublicID) || record.SagaID == uuid.Nil || record.CorrelationID == uuid.Nil:
		return appErrors.NewValidationError("projection record identifiers are malformed")
	case !mapped || business != record.BusinessStatus:
		return appErrors.NewValidationError("business status must be the mapping of the internal status")
	case record.AmountMinor < 0 || !knownCurrency(record.Currency):
		return appErrors.NewValidationError("amount must be non-negative and the currency supported")
	case len(record.Tags) > 50 || len(record.Steps) > 200:
		return appErrors.NewValidationError("projection record exceeds collection bounds")
	case record.CustomerEmailBlind != nil && !hex64Pattern.MatchString(*record.CustomerEmailBlind):
		return appErrors.NewValidationError("customer email blind index must be 64 lowercase hex characters")
	case !containsString(compensationStatuses, record.CompensationStatus):
		return appErrors.NewValidationError("compensation status is not supported")
	}
	_, terminal := terminalBusiness[record.BusinessStatus]
	if terminal != (record.CompletedAt != nil) {
		return appErrors.NewValidationError("completed_at must be present exactly for terminal statuses")
	}
	return nil
}

func knownCurrency(currency string) bool {
	_, known := CurrencyExponent(currency)
	return known
}

package rules

import (
	contracts "distributed-transaction-engine/src/features/transaction-query-api/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const (
	MaxTimelineMessage = 160
	maskedPlaceholder  = "***"
)

var (
	emailInTextPattern = regexp.MustCompile(`[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+`)
	digitRunPattern    = regexp.MustCompile(`\d{6,}`)
	csvFormulaPrefixes = "=+-@\t\r"
)

var businessStepStates = map[string]string{
	"NOT_STARTED": "Not started", "PENDING": "In progress", "DISPATCHED": "In progress", "RETRY_WAIT": "Waiting to retry",
	"SUCCEEDED": "Completed", "FAILED": "Failed", "UNKNOWN": "Outcome being verified", "COMPENSATING": "Reversing",
	"COMPENSATED": "Reversed", "SKIPPED": "Skipped",
}

func MapStepStateToBusiness(state string) string {
	defer observability.TraceEnter(nil)()
	if business, known := businessStepStates[state]; known {
		return business
	}
	return "In progress"
}

func IsCompensationState(state string) bool {
	defer observability.TraceEnter(nil)()
	return state == "COMPENSATING" || state == "COMPENSATED"
}

func SanitizeErrorMessage(raw string) string {
	defer observability.TraceEnter(nil)()
	cleaned := strings.Map(func(character rune) rune {
		if unicode.IsPrint(character) {
			return character
		}
		return -1
	}, raw)
	cleaned = emailInTextPattern.ReplaceAllString(cleaned, maskedPlaceholder)
	cleaned = digitRunPattern.ReplaceAllString(cleaned, maskedPlaceholder)
	runes := []rune(strings.TrimSpace(cleaned))
	if len(runes) > MaxTimelineMessage {
		return string(runes[:MaxTimelineMessage])
	}
	return string(runes)
}

func MaskValue(rule contracts.MaskRule, text string) string {
	defer observability.TraceEnter(nil)()
	switch rule {
	case contracts.MaskLastFour:
		runes := []rune(text)
		if len(runes) <= 4 {
			return maskedPlaceholder
		}
		return maskedPlaceholder + string(runes[len(runes)-4:])
	case contracts.MaskEmailPartial:
		local, domain, found := strings.Cut(text, "@")
		if !found || local == "" {
			return maskedPlaceholder
		}
		return string([]rune(local)[:1]) + maskedPlaceholder + "@" + domain
	}
	return text
}

func FormatMinorUnits(minor int64, exponent int) string {
	defer observability.TraceEnter(nil)()
	negative := minor < 0
	digits := strconv.FormatInt(minor, 10)
	if negative {
		digits = digits[1:]
	}
	for len(digits) <= exponent {
		digits = "0" + digits
	}
	formatted := digits
	if exponent > 0 {
		formatted = digits[:len(digits)-exponent] + "." + digits[len(digits)-exponent:]
	}
	if negative {
		return "-" + formatted
	}
	return formatted
}

func DefaultProjection(registry contracts.Registry, principal contracts.Principal) []string {
	defer observability.TraceEnter(nil)()
	defaults := []string{"id", "status", "workflow_type", "created_at", "completed_at", "amount", "currency", "merchant_ref"}
	if principal.Audience == contracts.AudienceOperator {
		defaults = append(defaults, "internal_status", "needs_attention", "correlation_id")
	}
	entitled := make([]string, 0, len(defaults))
	for _, name := range defaults {
		if definition := registry.Fields[name]; IsFieldEntitled(definition, principal) {
			entitled = append(entitled, name)
		}
	}
	return entitled
}

func ValidateProjection(registry contracts.Registry, principal contracts.Principal, requested []string) (spanResult0 []string, spanResult1 []contracts.FieldUse, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if len(requested) == 0 {
		requested = DefaultProjection(registry, principal)
	}
	state := &validationContext{registry: registry, principal: principal}
	fields := make([]string, 0, len(requested))
	seen := make(map[string]struct{}, len(requested))
	for _, name := range requested {
		definition, err := resolveField(state, name)
		if err != nil {
			return nil, nil, err
		}
		if !definition.Projectable {
			return nil, nil, queryError(400, constants.TQErrOperatorNotAllowed, name+" cannot be projected")
		}
		if _, duplicate := seen[name]; !duplicate {
			seen[name] = struct{}{}
			fields = append(fields, name)
			state.uses = append(state.uses, contracts.FieldUse{Field: name, Usage: contracts.UsageProject})
		}
	}
	return fields, state.uses, nil
}

func ProjectRow(registry contracts.Registry, fields []string, raw contracts.Row) contracts.Row {
	defer observability.TraceEnter(nil)()
	projected := make(contracts.Row, len(fields))
	for _, name := range fields {
		definition := registry.Fields[name]
		value, present := raw[name]
		if !present {
			continue
		}
		if definition.Type == contracts.FieldTypeMoney {
			projected[name] = projectMoney(raw)
			continue
		}
		projected[name] = maskIfNeeded(definition, value)
	}
	return projected
}

func projectMoney(raw contracts.Row) any {
	minor, hasMinor := raw[amountField].(int64)
	currency, hasCurrency := raw[currencyField].(string)
	if !hasMinor || !hasCurrency {
		return nil
	}
	exponent, _ := CurrencyExponent(currency)
	return map[string]string{"amount": FormatMinorUnits(minor, exponent), "currency": currency}
}

func maskIfNeeded(definition contracts.FieldDefinition, value any) any {
	text, isText := value.(string)
	if definition.Mask == contracts.MaskNone || !isText {
		return value
	}
	return MaskValue(definition.Mask, text)
}

func RedactNodeForAudit(registry contracts.Registry, node contracts.Node, hasher func(string) string) contracts.Node {
	defer observability.TraceEnter(nil)()
	switch node.Kind {
	case contracts.NodePredicate:
		return redactPredicate(registry, node, hasher)
	case contracts.NodeQuantified:
		body := RedactNodeForAudit(registry, *node.Quantified.Body, hasher)
		return contracts.Node{Kind: contracts.NodeQuantified, Quantified: &contracts.QuantifiedNode{
			Collection: node.Quantified.Collection, Quantifier: node.Quantified.Quantifier, Body: &body}}
	}
	children := make([]contracts.Node, 0, len(node.Children))
	for _, child := range node.Children {
		children = append(children, RedactNodeForAudit(registry, child, hasher))
	}
	return contracts.Node{Kind: node.Kind, Children: children}
}

func redactPredicate(registry contracts.Registry, node contracts.Node, hasher func(string) string) contracts.Node {
	definition := registry.Fields[node.Predicate.Field]
	sensitive := definition.Entitlement == contracts.EntitlementPII || definition.Entitlement == contracts.EntitlementRestricted || definition.Type == contracts.FieldTypeSensitive
	values := make([]contracts.Value, 0, len(node.Predicate.Values))
	for _, value := range node.Predicate.Values {
		if sensitive {
			value = contracts.Value{Kind: contracts.ValueString, Text: "h:" + hasher(value.Text)}
		}
		values = append(values, value)
	}
	return contracts.Node{Kind: contracts.NodePredicate, Predicate: &contracts.Predicate{Field: node.Predicate.Field, Operator: node.Predicate.Operator, Values: values}}
}

func FormatExportCell(raw any) string {
	defer observability.TraceEnter(nil)()
	var text string
	switch value := raw.(type) {
	case nil:
		text = ""
	case string:
		text = value
	case time.Time:
		text = value.UTC().Format(time.RFC3339Nano)
	case *time.Time:
		if value != nil {
			text = value.UTC().Format(time.RFC3339Nano)
		}
	case bool:
		text = strconv.FormatBool(value)
	case map[string]string:
		text = value["amount"] + " " + value["currency"]
	case []string:
		text = strings.Join(value, ";")
	case int64:
		text = strconv.FormatInt(value, 10)
	}
	if text != "" && strings.ContainsRune(csvFormulaPrefixes, rune(text[0])) {
		return "'" + text
	}
	return text
}

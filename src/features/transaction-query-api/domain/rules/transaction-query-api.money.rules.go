package rules

import (
	contracts "distributed-transaction-engine/src/features/transaction-query-api/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
)

const (
	amountField   = "amount"
	currencyField = "currency"
)

func checkMoneyCurrencyRule(node contracts.Node, andGroups [][]contracts.Node) error {
	switch node.Kind {
	case contracts.NodePredicate:
		if node.Predicate.Field == amountField {
			return requireGroupCurrency(*node.Predicate, andGroups)
		}
		return nil
	case contracts.NodeAnd:
		andGroups = append(append([][]contracts.Node(nil), andGroups...), node.Children)
	case contracts.NodeQuantified:
		return nil
	}
	for _, child := range node.Children {
		if err := checkMoneyCurrencyRule(child, andGroups); err != nil {
			return err
		}
	}
	return nil
}

func requireGroupCurrency(predicate contracts.Predicate, andGroups [][]contracts.Node) error {
	currency, uniform := uniformCurrency(predicate.Values)
	if !uniform {
		return queryError(400, constants.TQErrMoneyCurrencyRequired, "amount literals must share one currency")
	}
	for index := len(andGroups) - 1; index >= 0; index-- {
		if groupFixesCurrency(andGroups[index], currency) {
			return nil
		}
	}
	return queryError(400, constants.TQErrMoneyCurrencyRequired, "amount predicates require currency = \""+currency+"\" in the same AND group")
}

func uniformCurrency(values []contracts.Value) (string, bool) {
	if len(values) == 0 {
		return "", false
	}
	currency := values[0].Currency
	for _, value := range values {
		if value.Currency != currency {
			return "", false
		}
	}
	return currency, true
}

func groupFixesCurrency(group []contracts.Node, currency string) bool {
	for _, child := range group {
		if fixed, ok := fixedCurrency(child); ok && fixed == currency {
			return true
		}
	}
	return false
}

func fixedCurrency(node contracts.Node) (string, bool) {
	if node.Kind != contracts.NodePredicate || node.Predicate.Field != currencyField || len(node.Predicate.Values) != 1 {
		return "", false
	}
	if node.Predicate.Operator != contracts.OpEq && node.Predicate.Operator != contracts.OpIn {
		return "", false
	}
	return node.Predicate.Values[0].Text, true
}

func RootCurrency(filter contracts.Node) (string, bool) {
	defer observability.TraceEnter(nil)()
	if filter.Kind == contracts.NodeAnd {
		for _, child := range filter.Children {
			if currency, ok := fixedCurrency(child); ok {
				return currency, true
			}
		}
		return "", false
	}
	return fixedCurrency(filter)
}

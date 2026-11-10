package rules

import (
	contracts "distributed-transaction-engine/src/features/transaction-query-api/contracts"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	"strconv"
	"time"
)

func NormalizeNode(node contracts.Node) contracts.Node {
	defer observability.TraceEnter(nil)()
	switch node.Kind {
	case contracts.NodePredicate:
		return node
	case contracts.NodeQuantified:
		body := NormalizeNode(*node.Quantified.Body)
		return contracts.Node{Kind: contracts.NodeQuantified, Quantified: &contracts.QuantifiedNode{
			Collection: node.Quantified.Collection, Quantifier: node.Quantified.Quantifier, Body: &body}}
	case contracts.NodeNot:
		return normalizeNot(node)
	}
	return normalizeGroup(node)
}

func normalizeNot(node contracts.Node) contracts.Node {
	child := NormalizeNode(node.Children[0])
	if child.Kind == contracts.NodeNot {
		return child.Children[0]
	}
	return contracts.Node{Kind: contracts.NodeNot, Children: []contracts.Node{child}}
}

func normalizeGroup(node contracts.Node) contracts.Node {
	flattened := make([]contracts.Node, 0, len(node.Children))
	seen := make(map[string]struct{}, len(node.Children))
	for _, child := range node.Children {
		normalized := NormalizeNode(child)
		for _, member := range expandSameKind(normalized, node.Kind) {
			canonical := marshalCanonical(member)
			if _, duplicate := seen[canonical]; !duplicate {
				seen[canonical] = struct{}{}
				flattened = append(flattened, member)
			}
		}
	}
	if len(flattened) == 1 {
		return flattened[0]
	}
	return contracts.Node{Kind: node.Kind, Children: flattened}
}

func expandSameKind(node contracts.Node, kind contracts.NodeKind) []contracts.Node {
	if node.Kind == kind {
		return node.Children
	}
	return []contracts.Node{node}
}

func checkOrBranchLimit(node contracts.Node) error {
	if node.Kind == contracts.NodeOr && len(node.Children) > contracts.MaxOrBranches {
		return queryError(400, constants.TQErrLimitExceeded, "OR exceeds 20 branches")
	}
	for _, child := range node.Children {
		if err := checkOrBranchLimit(child); err != nil {
			return err
		}
	}
	if node.Kind == contracts.NodeQuantified {
		return checkOrBranchLimit(*node.Quantified.Body)
	}
	return nil
}

func detectContradiction(root contracts.Node) bool {
	if root.Kind != contracts.NodeAnd {
		return false
	}
	equalities := make(map[string]string)
	for _, child := range root.Children {
		if child.Kind != contracts.NodePredicate || child.Predicate.Operator != contracts.OpEq || len(child.Predicate.Values) != 1 {
			continue
		}
		value := child.Predicate.Values[0]
		identity := string(value.Kind) + "|" + value.Text + "|" + value.Currency
		if previous, exists := equalities[child.Predicate.Field]; exists && previous != identity {
			return true
		}
		equalities[child.Predicate.Field] = identity
	}
	return false
}

func ResolveQueryTimes(query contracts.NormalizedQuery, now time.Time) (spanResult0 contracts.NormalizedQuery, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	resolved := make([]contracts.ResolvedTime, 0)
	filter, err := resolveNodeTimes(query.Filter, now.UTC(), &resolved)
	if err != nil {
		return contracts.NormalizedQuery{}, err
	}
	query.Filter, query.ResolvedTimes = filter, resolved
	return query, nil
}

func resolveNodeTimes(node contracts.Node, now time.Time, resolved *[]contracts.ResolvedTime) (contracts.Node, error) {
	switch node.Kind {
	case contracts.NodePredicate:
		return resolvePredicateTimes(node, now, resolved)
	case contracts.NodeQuantified:
		body, err := resolveNodeTimes(*node.Quantified.Body, now, resolved)
		return contracts.Node{Kind: contracts.NodeQuantified, Quantified: &contracts.QuantifiedNode{
			Collection: node.Quantified.Collection, Quantifier: node.Quantified.Quantifier, Body: &body}}, err
	}
	children := make([]contracts.Node, 0, len(node.Children))
	for _, child := range node.Children {
		converted, err := resolveNodeTimes(child, now, resolved)
		if err != nil {
			return contracts.Node{}, err
		}
		children = append(children, converted)
	}
	return contracts.Node{Kind: node.Kind, Children: children}, nil
}

func resolvePredicateTimes(node contracts.Node, now time.Time, resolved *[]contracts.ResolvedTime) (contracts.Node, error) {
	values := make([]contracts.Value, 0, len(node.Predicate.Values))
	for _, value := range node.Predicate.Values {
		converted, err := resolveValueTime(value, now, resolved)
		if err != nil {
			return contracts.Node{}, err
		}
		values = append(values, converted)
	}
	return contracts.Node{Kind: contracts.NodePredicate, Predicate: &contracts.Predicate{Field: node.Predicate.Field, Operator: node.Predicate.Operator, Values: values}}, nil
}

func resolveValueTime(value contracts.Value, now time.Time, resolved *[]contracts.ResolvedTime) (contracts.Value, error) {
	switch value.Kind {
	case contracts.ValueRelative:
		instant, err := resolveRelative(*value.Relative, now)
		if err != nil {
			return contracts.Value{}, err
		}
		*resolved = append(*resolved, contracts.ResolvedTime{Expression: RelativeExpression(*value.Relative), Instant: instant})
		return contracts.Value{Kind: contracts.ValueTime, Text: instant.Format(time.RFC3339Nano)}, nil
	case contracts.ValueTime:
		instant, err := resolveTimeLiteral(value)
		return contracts.Value{Kind: contracts.ValueTime, Text: instant.Format(time.RFC3339Nano)}, err
	}
	return value, nil
}

func resolveTimeLiteral(value contracts.Value) (time.Time, error) {
	if value.Zone != "" {
		location, err := time.LoadLocation(value.Zone)
		if err != nil {
			return time.Time{}, queryError(400, constants.TQErrTypeMismatch, "unknown time zone")
		}
		instant, parseErr := time.ParseInLocation("2006-01-02", value.Text, location)
		return instant.UTC(), parseErr
	}
	instant, err := time.Parse(time.RFC3339Nano, value.Text)
	return instant.UTC(), err
}

func resolveRelative(relative contracts.RelativeTime, now time.Time) (time.Time, error) {
	if relative.Base == contracts.RelativeNow {
		return now.Add(time.Duration(relative.Sign*relative.Amount) * unitDuration(relative.Unit)), nil
	}
	location, err := time.LoadLocation(relative.Zone)
	if err != nil {
		return time.Time{}, queryError(400, constants.TQErrTimezoneRequired, "unknown time zone")
	}
	local := now.In(location)
	switch relative.StartOf {
	case "week":
		offset := (int(local.Weekday()) + 6) % 7
		return time.Date(local.Year(), local.Month(), local.Day()-offset, 0, 0, 0, 0, location).UTC(), nil
	case "month":
		return time.Date(local.Year(), local.Month(), 1, 0, 0, 0, 0, location).UTC(), nil
	}
	return time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, location).UTC(), nil
}

func unitDuration(unit string) time.Duration {
	switch unit {
	case "m":
		return time.Minute
	case "h":
		return time.Hour
	case "w":
		return 7 * 24 * time.Hour
	}
	return 24 * time.Hour
}

func RelativeExpression(relative contracts.RelativeTime) string {
	defer observability.TraceEnter(nil)()
	if relative.Base == contracts.RelativeStartOf {
		return "start_of(" + relative.StartOf + "," + relative.Zone + ")"
	}
	if relative.Sign == 0 {
		return "now()"
	}
	sign := "+"
	if relative.Sign < 0 {
		sign = "-"
	}
	return "now()" + sign + strconv.Itoa(relative.Amount) + relative.Unit
}

package contracts

import (
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"golang.org/x/text/unicode/norm"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

type tokenKind int

const (
	tkEOF tokenKind = iota
	tkIdent
	tkString
	tkNumber
	tkTime
	tkOp
	tkLParen
	tkRParen
	tkComma
	tkColon
	tkSign
)

type token struct {
	kind tokenKind
	text string
	line int
	col  int
}

var (
	timeLiteralPattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:\d{2})?|@[A-Za-z_][A-Za-z0-9_/+\-]*)?`)
	numberPattern      = regexp.MustCompile(`^\d+(\.\d+)?`)
	identPattern       = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_./]*`)
	currencyPattern    = regexp.MustCompile(`^[A-Z]{3}$`)
	moneyParamPattern  = regexp.MustCompile(`^(\d+(\.\d+)?) ([A-Z]{3})$`)
	decimalPattern     = regexp.MustCompile(`^\d+(\.\d+)?$`)
	reservedWords      = map[string]struct{}{
		"AND": {}, "OR": {}, "NOT": {}, "IN": {}, "BETWEEN": {}, "IS": {}, "NULL": {}, "MATCH": {}, "HAS": {},
		"HAS_ANY": {}, "HAS_ALL": {}, "ANY": {}, "ALL": {}, "NONE": {}, "ORDER": {}, "BY": {}, "ASC": {}, "DESC": {},
		"NULLS": {}, "FIRST": {}, "LAST": {}, "LIMIT": {}, "PREFIX": {}, "TRUE": {}, "FALSE": {},
	}
)

func syntaxError(line int, col int, message string, expected string) *appErrors.ApplicationError {
	return appErrors.NewTypedError(400, constants.TQErrInvalidSyntax, message, appErrors.ErrorDetail{
		Field: "query", Issue: message, Rule: "line " + strconv.Itoa(line) + ", column " + strconv.Itoa(col) + "; expected " + expected,
	})
}

func NormalizeLiteralText(raw string) (spanResult0 string, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if !utf8.ValidString(raw) {
		return "", appErrors.NewTypedError(400, constants.TQErrTypeMismatch, "literal is not valid UTF-8")
	}
	for _, character := range raw {
		if unicode.IsControl(character) {
			return "", appErrors.NewTypedError(400, constants.TQErrTypeMismatch, "literal contains a control character")
		}
	}
	return norm.NFC.String(raw), nil
}

func tokenize(text string) ([]token, error) {
	tokens := make([]token, 0, 32)
	line, col, index := 1, 1, 0
	for index < len(text) {
		rest := text[index:]
		character, width := utf8.DecodeRuneInString(rest)
		if character == '\n' {
			line, col, index = line+1, 1, index+width
			continue
		}
		if unicode.IsSpace(character) {
			col, index = col+1, index+width
			continue
		}
		next, consumed, err := scanToken(rest, line, col)
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, next)
		col, index = col+utf8.RuneCountInString(rest[:consumed]), index+consumed
	}
	return append(tokens, token{kind: tkEOF, line: line, col: col}), nil
}

func scanToken(rest string, line int, col int) (token, int, error) {
	switch character := rest[0]; {
	case character == '"':
		return scanString(rest, line, col)
	case character >= '0' && character <= '9':
		return scanNumberOrTime(rest, line, col), len(firstMatch(rest)), nil
	case character == '_' || (character >= 'A' && character <= 'Z') || (character >= 'a' && character <= 'z'):
		matched := identPattern.FindString(rest)
		return token{kind: tkIdent, text: matched, line: line, col: col}, len(matched), nil
	}
	return scanPunctuation(rest, line, col)
}

func firstMatch(rest string) string {
	if matched := timeLiteralPattern.FindString(rest); matched != "" {
		return matched
	}
	return numberPattern.FindString(rest)
}

func scanNumberOrTime(rest string, line int, col int) token {
	if matched := timeLiteralPattern.FindString(rest); matched != "" {
		return token{kind: tkTime, text: matched, line: line, col: col}
	}
	return token{kind: tkNumber, text: numberPattern.FindString(rest), line: line, col: col}
}

func scanString(rest string, line int, col int) (token, int, error) {
	var builder strings.Builder
	for index := 1; index < len(rest); index++ {
		switch rest[index] {
		case '"':
			return token{kind: tkString, text: builder.String(), line: line, col: col}, index + 1, nil
		case '\\':
			index++
			if index >= len(rest) || (rest[index] != '"' && rest[index] != '\\') {
				return token{}, 0, syntaxError(line, col, "invalid escape sequence in string literal", `\" or \\`)
			}
			builder.WriteByte(rest[index])
		default:
			builder.WriteByte(rest[index])
		}
	}
	return token{}, 0, syntaxError(line, col, "unterminated string literal", `closing "`)
}

func scanPunctuation(rest string, line int, col int) (token, int, error) {
	for _, operator := range []string{"!=", "<=", ">=", "=", "<", ">"} {
		if strings.HasPrefix(rest, operator) {
			return token{kind: tkOp, text: operator, line: line, col: col}, len(operator), nil
		}
	}
	single := map[byte]tokenKind{'(': tkLParen, ')': tkRParen, ',': tkComma, ':': tkColon, '-': tkSign, '+': tkSign}
	if kind, ok := single[rest[0]]; ok {
		return token{kind: kind, text: rest[:1], line: line, col: col}, 1, nil
	}
	return token{}, 0, syntaxError(line, col, "unexpected character "+strconv.Quote(rest[:1]), "a field, operator, literal or parenthesis")
}

type parser struct {
	tokens []token
	index  int
	nodes  int
}

func ParseTQLText(text string) (spanResult0 Query, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if len(text) > MaxQueryTextBytes {
		return Query{}, appErrors.NewTypedError(400, constants.TQErrLimitExceeded, "query text exceeds 4096 bytes")
	}
	if !utf8.ValidString(text) {
		return Query{}, syntaxError(1, 1, "query text is not valid UTF-8", "valid UTF-8")
	}
	tokens, err := tokenize(norm.NFC.String(text))
	if err != nil {
		return Query{}, err
	}
	state := &parser{tokens: tokens}
	return state.parseQuery()
}

func (p *parser) peek() token {
	return p.tokens[p.index]
}

func (p *parser) advance() token {
	current := p.tokens[p.index]
	if current.kind != tkEOF {
		p.index++
	}
	return current
}

func (p *parser) isKeyword(word string) bool {
	current := p.peek()
	return current.kind == tkIdent && strings.EqualFold(current.text, word)
}

func (p *parser) acceptKeyword(word string) bool {
	if !p.isKeyword(word) {
		return false
	}
	p.advance()
	return true
}

func (p *parser) expectKeyword(word string) error {
	if p.acceptKeyword(word) {
		return nil
	}
	return p.unexpected(word)
}

func (p *parser) expectKind(kind tokenKind, label string) (token, error) {
	if p.peek().kind != kind {
		return token{}, p.unexpected(label)
	}
	return p.advance(), nil
}

func (p *parser) unexpected(expected string) error {
	current := p.peek()
	found := "end of query"
	if current.kind != tkEOF {
		found = strconv.Quote(current.text)
	}
	return syntaxError(current.line, current.col, "unexpected "+found, expected)
}

func (p *parser) countNode() error {
	p.nodes++
	if p.nodes > MaxASTNodes {
		return appErrors.NewTypedError(400, constants.TQErrLimitExceeded, "query exceeds 100 AST nodes")
	}
	return nil
}

func (p *parser) parseQuery() (Query, error) {
	filter, err := p.parseOr(1)
	if err != nil {
		return Query{}, err
	}
	query := Query{Filter: &filter}
	if p.acceptKeyword("ORDER") {
		if query.OrderBy, err = p.parseOrderBy(); err != nil {
			return Query{}, err
		}
	}
	if p.acceptKeyword("LIMIT") {
		if query.Limit, err = p.parseLimit(); err != nil {
			return Query{}, err
		}
	}
	if p.peek().kind != tkEOF {
		return Query{}, p.unexpected("end of query")
	}
	return query, nil
}

func (p *parser) parseLimit() (int, error) {
	number, err := p.expectKind(tkNumber, "integer")
	if err != nil {
		return 0, err
	}
	limit, convErr := strconv.Atoi(number.text)
	if convErr != nil || limit < 1 || limit > MaxPageSize {
		return 0, appErrors.NewTypedError(400, constants.TQErrLimitExceeded, "LIMIT must be an integer between 1 and 200")
	}
	return limit, nil
}

func (p *parser) parseOrderBy() ([]SortKey, error) {
	if err := p.expectKeyword("BY"); err != nil {
		return nil, err
	}
	keys := make([]SortKey, 0, MaxSortKeys)
	for {
		key, err := p.parseSortKey()
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
		if p.peek().kind != tkComma {
			return keys, nil
		}
		p.advance()
	}
}

func (p *parser) parseSortKey() (SortKey, error) {
	field, err := p.expectKind(tkIdent, "field name")
	if err != nil {
		return SortKey{}, err
	}
	key := SortKey{Field: field.text}
	if p.acceptKeyword("DESC") {
		key.Descending = true
	} else {
		p.acceptKeyword("ASC")
	}
	if p.acceptKeyword("NULLS") {
		return p.parseNullsOrder(key)
	}
	return key, nil
}

func (p *parser) parseNullsOrder(key SortKey) (SortKey, error) {
	switch {
	case p.acceptKeyword("FIRST"):
		key.Nulls = NullsFirst
	case p.acceptKeyword("LAST"):
		key.Nulls = NullsLast
	default:
		return SortKey{}, p.unexpected("FIRST or LAST")
	}
	return key, nil
}

func (p *parser) parseOr(depth int) (Node, error) {
	if depth > MaxBooleanDepth {
		return Node{}, appErrors.NewTypedError(400, constants.TQErrLimitExceeded, "boolean nesting exceeds depth 8")
	}
	first, err := p.parseAnd(depth)
	if err != nil || !p.isKeyword("OR") {
		return first, err
	}
	children := []Node{first}
	for p.acceptKeyword("OR") {
		next, nextErr := p.parseAnd(depth)
		if nextErr != nil {
			return Node{}, nextErr
		}
		children = append(children, next)
	}
	return Node{Kind: NodeOr, Children: children}, p.countNode()
}

func (p *parser) parseAnd(depth int) (Node, error) {
	first, err := p.parseNot(depth)
	if err != nil || !p.isKeyword("AND") {
		return first, err
	}
	children := []Node{first}
	for p.isKeyword("AND") {
		p.advance()
		next, nextErr := p.parseNot(depth)
		if nextErr != nil {
			return Node{}, nextErr
		}
		children = append(children, next)
	}
	return Node{Kind: NodeAnd, Children: children}, p.countNode()
}

func (p *parser) parseNot(depth int) (Node, error) {
	if !p.acceptKeyword("NOT") {
		return p.parsePrimary(depth)
	}
	child, err := p.parseNot(depth)
	if err != nil {
		return Node{}, err
	}
	return Node{Kind: NodeNot, Children: []Node{child}}, p.countNode()
}

func (p *parser) parsePrimary(depth int) (Node, error) {
	if p.peek().kind != tkLParen {
		return p.parsePredicate(depth)
	}
	p.advance()
	inner, err := p.parseOr(depth + 1)
	if err != nil {
		return Node{}, err
	}
	if _, err = p.expectKind(tkRParen, ")"); err != nil {
		return Node{}, err
	}
	return inner, nil
}

func (p *parser) parsePredicate(depth int) (Node, error) {
	field, err := p.expectKind(tkIdent, "field name or (")
	if err != nil {
		return Node{}, err
	}
	if quantifier, ok := p.acceptQuantifier(); ok {
		return p.parseQuantified(field.text, quantifier, depth)
	}
	predicate, err := p.parsePredicateBody(field.text)
	if err != nil {
		return Node{}, err
	}
	return Node{Kind: NodePredicate, Predicate: &predicate}, p.countNode()
}

func (p *parser) acceptQuantifier() (Quantifier, bool) {
	for _, quantifier := range []Quantifier{QuantifierAny, QuantifierAll, QuantifierNone} {
		if p.acceptKeyword(string(quantifier)) {
			return quantifier, true
		}
	}
	return "", false
}

func (p *parser) parseQuantified(collection string, quantifier Quantifier, depth int) (Node, error) {
	if _, err := p.expectKind(tkLParen, "("); err != nil {
		return Node{}, err
	}
	body, err := p.parseOr(depth + 1)
	if err != nil {
		return Node{}, err
	}
	if _, err = p.expectKind(tkRParen, ")"); err != nil {
		return Node{}, err
	}
	return Node{Kind: NodeQuantified, Quantified: &QuantifiedNode{Collection: collection, Quantifier: quantifier, Body: &body}}, p.countNode()
}

func (p *parser) parsePredicateBody(field string) (Predicate, error) {
	switch {
	case p.peek().kind == tkOp:
		return p.parseComparison(field, Operator(p.advance().text))
	case p.acceptKeyword("PREFIX"):
		return p.parseComparison(field, OpPrefix)
	case p.isKeyword("NOT") || p.isKeyword("IN"):
		return p.parseInList(field)
	case p.acceptKeyword("BETWEEN"):
		return p.parseBetween(field)
	case p.acceptKeyword("IS"):
		return p.parseIsNull(field)
	case p.acceptKeyword("MATCH"):
		return p.parseMatch(field)
	}
	return p.parseSetPredicate(field)
}

func (p *parser) parseComparison(field string, operator Operator) (Predicate, error) {
	value, err := p.parseValue()
	if err != nil {
		return Predicate{}, err
	}
	return Predicate{Field: field, Operator: operator, Values: []Value{value}}, nil
}

func (p *parser) parseInList(field string) (Predicate, error) {
	operator := OpIn
	if p.acceptKeyword("NOT") {
		operator = OpNotIn
	}
	if err := p.expectKeyword("IN"); err != nil {
		return Predicate{}, err
	}
	values, err := p.parseValueList(MaxInListSize)
	return Predicate{Field: field, Operator: operator, Values: values}, err
}

func (p *parser) parseValueList(limit int) ([]Value, error) {
	if _, err := p.expectKind(tkLParen, "("); err != nil {
		return nil, err
	}
	values := make([]Value, 0, 8)
	for {
		value, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		if values = append(values, value); len(values) > limit {
			return nil, appErrors.NewTypedError(400, constants.TQErrLimitExceeded, "list exceeds "+strconv.Itoa(limit)+" values")
		}
		if p.peek().kind != tkComma {
			break
		}
		p.advance()
	}
	_, err := p.expectKind(tkRParen, ")")
	return values, err
}

func (p *parser) parseBetween(field string) (Predicate, error) {
	lower, err := p.parseValue()
	if err != nil {
		return Predicate{}, err
	}
	if err = p.expectKeyword("AND"); err != nil {
		return Predicate{}, err
	}
	upper, err := p.parseValue()
	return Predicate{Field: field, Operator: OpBetween, Values: []Value{lower, upper}}, err
}

func (p *parser) parseIsNull(field string) (Predicate, error) {
	operator := OpIsNull
	if p.acceptKeyword("NOT") {
		operator = OpIsNotNull
	}
	return Predicate{Field: field, Operator: operator}, p.expectKeyword("NULL")
}

func (p *parser) parseMatch(field string) (Predicate, error) {
	text, err := p.expectKind(tkString, "string")
	if err != nil {
		return Predicate{}, err
	}
	return Predicate{Field: field, Operator: OpMatch, Values: []Value{{Kind: ValueString, Text: text.text}}}, nil
}

func (p *parser) parseSetPredicate(field string) (Predicate, error) {
	for _, operator := range []Operator{OpHasAny, OpHasAll, OpHas} {
		if !p.acceptKeyword(string(operator)) {
			continue
		}
		if p.peek().kind == tkLParen {
			values, err := p.parseValueList(MaxInListSize)
			return Predicate{Field: field, Operator: operator, Values: values}, err
		}
		return p.parseComparison(field, operator)
	}
	return Predicate{}, p.unexpected("an operator")
}

func (p *parser) parseValue() (Value, error) {
	current := p.advance()
	switch current.kind {
	case tkString:
		return Value{Kind: ValueString, Text: current.text}, nil
	case tkTime:
		return timeValue(current.text), nil
	case tkNumber:
		return p.parseNumberOrMoney(current), nil
	case tkColon:
		name, err := p.expectKind(tkIdent, "parameter name")
		return Value{Kind: ValueParam, Text: name.text}, err
	case tkIdent:
		return p.parseIdentValue(current)
	}
	p.index--
	return Value{}, p.unexpected("a value")
}

func timeValue(text string) Value {
	if at := strings.Index(text, "@"); at >= 0 {
		return Value{Kind: ValueTime, Text: text[:at], Zone: text[at+1:]}
	}
	return Value{Kind: ValueTime, Text: text}
}

func (p *parser) parseNumberOrMoney(number token) Value {
	next := p.peek()
	if next.kind == tkIdent && currencyPattern.MatchString(next.text) {
		if _, reserved := reservedWords[next.text]; !reserved {
			p.advance()
			return Value{Kind: ValueMoney, Text: number.text, Currency: next.text}
		}
	}
	return Value{Kind: ValueNumber, Text: number.text}
}

func (p *parser) parseIdentValue(current token) (Value, error) {
	switch strings.ToLower(current.text) {
	case "true", "false":
		return Value{Kind: ValueBoolean, Text: strings.ToLower(current.text)}, nil
	case "now":
		return p.parseNow()
	case "start_of":
		return p.parseStartOf()
	}
	p.index--
	return Value{}, p.unexpected("a value")
}

func (p *parser) parseNow() (Value, error) {
	if _, err := p.expectKind(tkLParen, "("); err != nil {
		return Value{}, err
	}
	if _, err := p.expectKind(tkRParen, ")"); err != nil {
		return Value{}, err
	}
	relative := RelativeTime{Base: RelativeNow}
	if p.peek().kind != tkSign {
		return Value{Kind: ValueRelative, Relative: &relative}, nil
	}
	relative.Sign = 1
	if p.advance().text == "-" {
		relative.Sign = -1
	}
	return p.parseDuration(relative)
}

func (p *parser) parseDuration(relative RelativeTime) (Value, error) {
	number, err := p.expectKind(tkNumber, "duration amount")
	if err != nil {
		return Value{}, err
	}
	unit, err := p.expectKind(tkIdent, "duration unit m, h, d or w")
	if err != nil {
		return Value{}, err
	}
	amount, convErr := strconv.Atoi(number.text)
	if convErr != nil || amount < 1 || len(unit.text) != 1 || !strings.Contains("mhdw", unit.text) {
		return Value{}, syntaxError(unit.line, unit.col, "invalid duration", "a positive integer followed by m, h, d or w")
	}
	relative.Amount, relative.Unit = amount, unit.text
	return Value{Kind: ValueRelative, Relative: &relative}, nil
}

func (p *parser) parseStartOf() (Value, error) {
	if _, err := p.expectKind(tkLParen, "("); err != nil {
		return Value{}, err
	}
	unit, err := p.expectKind(tkIdent, "unit day, week or month")
	if err != nil {
		return Value{}, err
	}
	if _, err = p.expectKind(tkComma, ","); err != nil {
		return Value{}, err
	}
	zone := p.advance()
	if zone.kind != tkIdent && zone.kind != tkString {
		p.index--
		return Value{}, p.unexpected("a time zone name")
	}
	_, err = p.expectKind(tkRParen, ")")
	return Value{Kind: ValueRelative, Relative: &RelativeTime{Base: RelativeStartOf, StartOf: strings.ToLower(unit.text), Zone: zone.text}}, err
}

func ValidateQueryStructure(query Query) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if query.Filter == nil {
		return appErrors.NewTypedError(400, constants.TQErrInvalidSyntax, "query requires a filter")
	}
	counter := 0
	return validateNodeStructure(*query.Filter, 1, &counter)
}

func validateNodeStructure(node Node, depth int, counter *int) error {
	if *counter++; *counter > MaxASTNodes {
		return appErrors.NewTypedError(400, constants.TQErrLimitExceeded, "query exceeds 100 AST nodes")
	}
	if depth > MaxBooleanDepth {
		return appErrors.NewTypedError(400, constants.TQErrLimitExceeded, "boolean nesting exceeds depth 8")
	}
	switch node.Kind {
	case NodeAnd, NodeOr:
		return validateChildrenStructure(node, 2, depth, counter)
	case NodeNot:
		return validateChildrenStructure(node, 1, depth, counter)
	case NodePredicate:
		return validatePredicateStructure(node)
	case NodeQuantified:
		return validateQuantifiedStructure(node, depth, counter)
	}
	return appErrors.NewTypedError(400, constants.TQErrInvalidSyntax, "unknown AST node kind")
}

func validateChildrenStructure(node Node, minimum int, depth int, counter *int) error {
	exact := node.Kind == NodeNot
	if len(node.Children) < minimum || (exact && len(node.Children) != 1) || node.Predicate != nil || node.Quantified != nil {
		return appErrors.NewTypedError(400, constants.TQErrInvalidSyntax, "malformed "+string(node.Kind)+" node")
	}
	if node.Kind == NodeOr && len(node.Children) > MaxOrBranches {
		return appErrors.NewTypedError(400, constants.TQErrLimitExceeded, "OR exceeds 20 branches")
	}
	for _, child := range node.Children {
		if err := validateNodeStructure(child, depth+1, counter); err != nil {
			return err
		}
	}
	return nil
}

func validatePredicateStructure(node Node) error {
	if node.Predicate == nil || len(node.Children) != 0 || node.Quantified != nil || node.Predicate.Field == "" {
		return appErrors.NewTypedError(400, constants.TQErrInvalidSyntax, "malformed PREDICATE node")
	}
	if len(node.Predicate.Values) > MaxInListSize {
		return appErrors.NewTypedError(400, constants.TQErrLimitExceeded, "list exceeds 500 values")
	}
	return nil
}

func validateQuantifiedStructure(node Node, depth int, counter *int) error {
	if node.Quantified == nil || node.Quantified.Body == nil || len(node.Children) != 0 || node.Predicate != nil {
		return appErrors.NewTypedError(400, constants.TQErrInvalidSyntax, "malformed QUANTIFIED node")
	}
	return validateNodeStructure(*node.Quantified.Body, depth+1, counter)
}

func ParseParamValue(fieldType FieldType, raw string) (spanResult0 Value, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	text, err := NormalizeLiteralText(raw)
	if err != nil {
		return Value{}, err
	}
	switch fieldType {
	case FieldTypeInteger:
		return numberParam(text, `^\d+$`, "integer")
	case FieldTypeBoolean:
		return booleanParam(text)
	case FieldTypeMoney:
		return moneyParam(text)
	case FieldTypeTimestamp:
		return timeValue(text), nil
	}
	return Value{Kind: ValueString, Text: text}, nil
}

func numberParam(text string, pattern string, label string) (Value, error) {
	if !regexp.MustCompile(pattern).MatchString(text) {
		return Value{}, appErrors.NewTypedError(400, constants.TQErrTypeMismatch, "parameter must be a "+label)
	}
	return Value{Kind: ValueNumber, Text: text}, nil
}

func booleanParam(text string) (Value, error) {
	if text != "true" && text != "false" {
		return Value{}, appErrors.NewTypedError(400, constants.TQErrTypeMismatch, "parameter must be true or false")
	}
	return Value{Kind: ValueBoolean, Text: text}, nil
}

func moneyParam(text string) (Value, error) {
	matched := moneyParamPattern.FindStringSubmatch(text)
	if matched == nil {
		return Value{}, appErrors.NewTypedError(400, constants.TQErrTypeMismatch, "money parameter must look like 125.50 INR")
	}
	return Value{Kind: ValueMoney, Text: matched[1], Currency: matched[3]}, nil
}

func ValidateQuerySource(version string, text *string, ast *Query, params map[string]string) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if version == "" {
		return appErrors.NewTypedError(400, constants.TQErrUnsupportedTQLVersion, "tql_version is required")
	}
	if (text == nil) == (ast == nil) {
		return appErrors.NewValidationError("exactly one of text or ast must be provided")
	}
	if len(params) > MaxParams {
		return appErrors.NewTypedError(400, constants.TQErrLimitExceeded, "params exceed 50 entries")
	}
	return nil
}

func ValidateSearchRequestShape(request SearchRequest) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if err := ValidateQuerySource(request.TQLVersion, request.Text, request.AST, request.Params); err != nil {
		return err
	}
	if len(request.Fields) > MaxProjectedFields {
		return appErrors.NewTypedError(400, constants.TQErrLimitExceeded, "fields exceed 40 entries")
	}
	if request.Count != "" && request.Count != CountNone && request.Count != CountEstimate && request.Count != CountExact {
		return appErrors.NewValidationError("count must be NONE, ESTIMATE or EXACT")
	}
	if request.Consistency != "" && request.Consistency != ConsistencyLive && request.Consistency != ConsistencySnapshot {
		return appErrors.NewValidationError("consistency must be LIVE or SNAPSHOT")
	}
	return validatePurpose(request.Purpose)
}

func ValidateAggregateRequestShape(request AggregateRequest) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if err := ValidateQuerySource(request.TQLVersion, request.Text, request.AST, request.Params); err != nil {
		return err
	}
	if len(request.Aggregates) == 0 || len(request.Aggregates) > 10 {
		return appErrors.NewValidationError("aggregates must contain 1..10 expressions")
	}
	if len(request.GroupBy) > MaxGroupBy {
		return appErrors.NewTypedError(400, constants.TQErrLimitExceeded, "group_by exceeds 3 fields")
	}
	if request.Exactness != "" && request.Exactness != ExactnessExact && request.Exactness != ExactnessApproximate {
		return appErrors.NewValidationError("exactness must be EXACT or APPROXIMATE")
	}
	return validatePurpose(request.Purpose)
}

func ValidateExportRequestShape(request ExportRequest) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if err := ValidateQuerySource(request.TQLVersion, request.Text, request.AST, request.Params); err != nil {
		return err
	}
	if len(request.Fields) > MaxProjectedFields {
		return appErrors.NewTypedError(400, constants.TQErrLimitExceeded, "fields exceed 40 entries")
	}
	return validatePurpose(request.Purpose)
}

func ValidateSavedQueryRequestShape(request CreateSavedQueryRequest) (spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	if err := ValidateQuerySource(request.TQLVersion, request.Text, request.AST, request.Params); err != nil {
		return err
	}
	if name := strings.TrimSpace(request.Name); name == "" || name != request.Name || len(name) > MaxSavedQueryNameLen {
		return appErrors.NewValidationError("name must be trimmed and 1..128 characters")
	}
	if request.Visibility != VisibilityPrivate && request.Visibility != VisibilityTeam {
		return appErrors.NewValidationError("visibility must be PRIVATE or TEAM")
	}
	return nil
}

func validatePurpose(purpose *string) error {
	if purpose != nil && (strings.TrimSpace(*purpose) == "" || len(*purpose) > MaxPurposeLength) {
		return appErrors.NewValidationError("purpose must be 1..256 characters when present")
	}
	return nil
}

func NewLimitRangeError() *appErrors.ApplicationError {
	defer observability.TraceEnter(nil)()
	return appErrors.NewTypedError(400, constants.TQErrLimitExceeded, "limit must be an integer between 1 and "+strconv.Itoa(MaxPageSize))
}

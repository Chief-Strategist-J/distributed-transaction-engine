package adapters

import (
	"context"
	"database/sql"
	contracts "distributed-transaction-engine/src/features/transaction-query-api/contracts"
	"distributed-transaction-engine/src/features/transaction-query-api/infrastructure/queries"
	"distributed-transaction-engine/src/infra/observability"
	"distributed-transaction-engine/src/shared/constants"
	appErrors "distributed-transaction-engine/src/shared/errors"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	statementTimeoutMillis = "5000"
	queryCanceledState     = "57014"
	rowLimitProbe          = 1
)

type DBExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type DBTransactor interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

type PostgresTransactionViewRepositoryAdapter struct {
	db DBTransactor
}

func NewPostgresTransactionViewRepositoryAdapter(db DBTransactor) *PostgresTransactionViewRepositoryAdapter {
	return &PostgresTransactionViewRepositoryAdapter{db: db}
}

type sqlScope struct {
	columns map[string]queries.ColumnSpec
	alias   string
	nested  bool
	search  bool
}

type sqlBuilder struct {
	args []any
}

func (b *sqlBuilder) bind(value any) string {
	b.args = append(b.args, value)
	return "$" + strconv.Itoa(len(b.args))
}

func columnFor(scope sqlScope, field string) (queries.ColumnSpec, string, error) {
	spec, known := scope.columns[field]
	if !known {
		return queries.ColumnSpec{}, "", appErrors.NewTypedError(422, constants.TQErrQueryTooExpensive, "field "+field+" is not available in this store")
	}
	return spec, scope.alias + "." + spec.Expression, nil
}

func operandValue(kind queries.ColumnKind, value contracts.Value) (any, error) {
	switch kind {
	case queries.KindTime, queries.KindNullTime:
		parsed, err := time.Parse(time.RFC3339Nano, value.Text)
		return parsed, err
	case queries.KindInt64:
		return strconv.ParseInt(value.Text, 10, 64)
	case queries.KindBool:
		return value.Text == "true", nil
	}
	return value.Text, nil
}

func operandList(kind queries.ColumnKind, values []contracts.Value) (any, error) {
	switch kind {
	case queries.KindTime, queries.KindNullTime:
		list := make([]time.Time, 0, len(values))
		for _, value := range values {
			parsed, err := time.Parse(time.RFC3339Nano, value.Text)
			if err != nil {
				return nil, err
			}
			list = append(list, parsed)
		}
		return pq.Array(list), nil
	case queries.KindInt64:
		list := make([]int64, 0, len(values))
		for _, value := range values {
			parsed, err := strconv.ParseInt(value.Text, 10, 64)
			if err != nil {
				return nil, err
			}
			list = append(list, parsed)
		}
		return pq.Array(list), nil
	}
	list := make([]string, 0, len(values))
	for _, value := range values {
		list = append(list, value.Text)
	}
	return pq.Array(list), nil
}

func escapeLikePrefix(text string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return replacer.Replace(text) + "%"
}

func compileNode(builder *sqlBuilder, scope sqlScope, node contracts.Node) (string, error) {
	switch node.Kind {
	case contracts.NodePredicate:
		return compilePredicate(builder, scope, *node.Predicate)
	case contracts.NodeQuantified:
		return compileQuantified(builder, scope, *node.Quantified)
	case contracts.NodeNot:
		inner, err := compileNode(builder, scope, node.Children[0])
		return "NOT (" + inner + ")", err
	}
	joiner := " AND "
	if node.Kind == contracts.NodeOr {
		joiner = " OR "
	}
	parts := make([]string, 0, len(node.Children))
	for _, child := range node.Children {
		part, err := compileNode(builder, scope, child)
		if err != nil {
			return "", err
		}
		parts = append(parts, "("+part+")")
	}
	return strings.Join(parts, joiner), nil
}

func compileQuantified(builder *sqlBuilder, scope sqlScope, quantified contracts.QuantifiedNode) (string, error) {
	if scope.search || scope.nested {
		return "", appErrors.NewTypedError(422, constants.TQErrQueryTooExpensive, "nested predicates are not available in this store")
	}
	inner, err := compileNode(builder, sqlScope{columns: queries.StepColumns, alias: "s", nested: true}, *quantified.Body)
	if err != nil {
		return "", err
	}
	exists := "EXISTS (SELECT 1 FROM " + queries.TableStep + " s WHERE s.tenant_id = " + scope.alias + ".tenant_id AND s.transaction_id = " + scope.alias + ".id AND "
	switch quantified.Quantifier {
	case contracts.QuantifierNone:
		return "NOT " + exists + "(" + inner + "))", nil
	case contracts.QuantifierAll:
		return "NOT " + exists + "NOT COALESCE((" + inner + "), false))", nil
	}
	return exists + "(" + inner + "))", nil
}

func compilePredicate(builder *sqlBuilder, scope sqlScope, predicate contracts.Predicate) (string, error) {
	spec, column, err := columnFor(scope, predicate.Field)
	if err != nil {
		return "", err
	}
	switch predicate.Operator {
	case contracts.OpIsNull:
		return column + " IS NULL", nil
	case contracts.OpIsNotNull:
		return column + " IS NOT NULL", nil
	case contracts.OpIn, contracts.OpNotIn:
		return compileList(builder, column, spec, predicate)
	case contracts.OpBetween:
		return compileBetween(builder, column, spec, predicate)
	case contracts.OpPrefix:
		return column + " LIKE " + builder.bind(escapeLikePrefix(predicate.Values[0].Text)) + ` ESCAPE '\'`, nil
	case contracts.OpMatch:
		return compileMatch(builder, column, predicate.Values[0].Text), nil
	case contracts.OpHas, contracts.OpHasAny, contracts.OpHasAll:
		return compileSet(builder, column, predicate), nil
	}
	operand, err := operandValue(spec.Operand, predicate.Values[0])
	if err != nil {
		return "", appErrors.NewTypedError(400, constants.TQErrTypeMismatch, "literal does not fit field "+predicate.Field)
	}
	return column + " " + string(predicate.Operator) + " " + builder.bind(operand), nil
}

func compileList(builder *sqlBuilder, column string, spec queries.ColumnSpec, predicate contracts.Predicate) (string, error) {
	list, err := operandList(spec.Operand, predicate.Values)
	if err != nil {
		return "", appErrors.NewTypedError(400, constants.TQErrTypeMismatch, "literal does not fit field "+predicate.Field)
	}
	if predicate.Operator == contracts.OpNotIn {
		return column + " <> ALL(" + builder.bind(list) + ")", nil
	}
	return column + " = ANY(" + builder.bind(list) + ")", nil
}

func compileBetween(builder *sqlBuilder, column string, spec queries.ColumnSpec, predicate contracts.Predicate) (string, error) {
	lower, err := operandValue(spec.Operand, predicate.Values[0])
	if err != nil {
		return "", appErrors.NewTypedError(400, constants.TQErrTypeMismatch, "literal does not fit field "+predicate.Field)
	}
	upper, err := operandValue(spec.Operand, predicate.Values[1])
	if err != nil {
		return "", appErrors.NewTypedError(400, constants.TQErrTypeMismatch, "literal does not fit field "+predicate.Field)
	}
	return column + " >= " + builder.bind(lower) + " AND " + column + " < " + builder.bind(upper), nil
}

func compileMatch(builder *sqlBuilder, column string, text string) string {
	function := "plainto_tsquery"
	if len(text) >= 2 && strings.HasPrefix(text, `"`) && strings.HasSuffix(text, `"`) {
		function, text = "phraseto_tsquery", text[1:len(text)-1]
	}
	return column + " @@ " + function + "('simple', " + builder.bind(text) + ")"
}

func compileSet(builder *sqlBuilder, column string, predicate contracts.Predicate) string {
	values := make([]string, 0, len(predicate.Values))
	for _, value := range predicate.Values {
		values = append(values, value.Text)
	}
	switch predicate.Operator {
	case contracts.OpHasAny:
		return column + " && " + builder.bind(pq.Array(values)) + "::text[]"
	}
	return column + " @> " + builder.bind(pq.Array(values)) + "::text[]"
}

func compileInjected(builder *sqlBuilder, scope sqlScope, injected contracts.InjectedPredicates) string {
	clauses := []string{scope.alias + ".tenant_id = " + builder.bind(injected.TenantID)}
	if injected.RestrictToOwner {
		clauses = append(clauses, "("+scope.alias+".owner_ref = "+builder.bind(injected.OwnerRef)+" OR "+scope.alias+".team_ref = ANY("+builder.bind(pq.Array(nonNilStrings(injected.TeamRefs)))+"))")
	}
	if injected.ExcludeInternalOnly {
		clauses = append(clauses, scope.alias+".is_internal_only = false")
	}
	if injected.ExcludeArchived {
		clauses = append(clauses, scope.alias+".archived_at IS NULL")
	}
	return strings.Join(clauses, " AND ")
}

func nonNilStrings(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}

func compileWhere(builder *sqlBuilder, scope sqlScope, injected contracts.InjectedPredicates, filter contracts.Node, snapshot *int64, keyset *contracts.KeysetPosition) (string, error) {
	user, err := compileNode(builder, scope, filter)
	if err != nil {
		return "", err
	}
	where := compileInjected(builder, scope, injected) + " AND (" + user + ")"
	if snapshot != nil {
		sequenceColumn := scope.alias + ".projection_sequence"
		if scope.search {
			sequenceColumn = scope.alias + ".index_sequence"
		}
		where += " AND " + sequenceColumn + " <= " + builder.bind(*snapshot)
	}
	if keyset == nil {
		return where, nil
	}
	after, err := compileKeyset(builder, scope, *keyset)
	return where + " AND (" + after + ")", err
}

func cursorOperand(kind queries.ColumnKind, text string) (any, error) {
	return operandValue(kind, contracts.Value{Text: text})
}

func compileKeyset(builder *sqlBuilder, scope sqlScope, keyset contracts.KeysetPosition) (string, error) {
	alternatives := make([]string, 0, len(keyset.Keys))
	for index := range keyset.Keys {
		term, include, err := compileKeysetTerm(builder, scope, keyset, index)
		if err != nil {
			return "", err
		}
		if include {
			alternatives = append(alternatives, "("+term+")")
		}
	}
	if len(alternatives) == 0 {
		return "FALSE", nil
	}
	return strings.Join(alternatives, " OR "), nil
}

func compileKeysetTerm(builder *sqlBuilder, scope sqlScope, keyset contracts.KeysetPosition, index int) (string, bool, error) {
	parts := make([]string, 0, index+1)
	for earlier := 0; earlier < index; earlier++ {
		equality, err := compileKeysetEquality(builder, scope, keyset.Keys[earlier], keyset.Last[earlier])
		if err != nil {
			return "", false, err
		}
		parts = append(parts, equality)
	}
	after, include, err := compileKeysetAfter(builder, scope, keyset.Keys[index], keyset.Last[index])
	if err != nil || !include {
		return "", false, err
	}
	return strings.Join(append(parts, after), " AND "), true, nil
}

func compileKeysetEquality(builder *sqlBuilder, scope sqlScope, key contracts.SortKey, last contracts.CursorValue) (string, error) {
	spec, column, err := columnFor(scope, key.Field)
	if err != nil {
		return "", err
	}
	if last.Null {
		return column + " IS NULL", nil
	}
	operand, err := cursorOperand(spec.Operand, last.Text)
	if err != nil {
		return "", appErrors.NewTypedError(400, constants.TQErrCursorInvalid, "cursor position does not fit the sort field")
	}
	return column + " = " + builder.bind(operand), nil
}

func compileKeysetAfter(builder *sqlBuilder, scope sqlScope, key contracts.SortKey, last contracts.CursorValue) (string, bool, error) {
	spec, column, err := columnFor(scope, key.Field)
	if err != nil {
		return "", false, err
	}
	nullsFirst := key.Nulls == contracts.NullsFirst
	if last.Null {
		return column + " IS NOT NULL", nullsFirst, nil
	}
	operand, err := cursorOperand(spec.Operand, last.Text)
	if err != nil {
		return "", false, appErrors.NewTypedError(400, constants.TQErrCursorInvalid, "cursor position does not fit the sort field")
	}
	comparison := column + " > " + builder.bind(operand)
	if key.Descending {
		comparison = column + " < " + builder.bind(operand)
	}
	if !nullsFirst {
		comparison = "(" + comparison + " OR " + column + " IS NULL)"
	}
	return comparison, true, nil
}

func compileOrderBy(scope sqlScope, keys []contracts.SortKey) (string, error) {
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		_, column, err := columnFor(scope, key.Field)
		if err != nil {
			return "", err
		}
		direction, nulls := "ASC", "NULLS LAST"
		if key.Descending {
			direction = "DESC"
		}
		if key.Nulls == contracts.NullsFirst {
			nulls = "NULLS FIRST"
		}
		parts = append(parts, column+" "+direction+" "+nulls)
	}
	return " ORDER BY " + strings.Join(parts, ", "), nil
}

func selectFieldList(scope sqlScope, projection map[string]queries.ColumnSpec, fields []string) ([]string, string, error) {
	ordered := make([]string, 0, len(fields)+2)
	seen := make(map[string]struct{}, len(fields)+2)
	add := func(name string) {
		if _, present := seen[name]; !present {
			seen[name] = struct{}{}
			ordered = append(ordered, name)
		}
	}
	add("id")
	for _, name := range fields {
		add(name)
		if name == "amount" {
			add("currency")
		}
	}
	expressions := make([]string, 0, len(ordered))
	for _, name := range ordered {
		spec, known := projection[name]
		if !known {
			return nil, "", appErrors.NewTypedError(422, constants.TQErrQueryTooExpensive, "field "+name+" is not available in this store")
		}
		expressions = append(expressions, scope.alias+"."+spec.Expression+` AS "`+name+`"`)
	}
	return ordered, strings.Join(expressions, ", "), nil
}

func CompileListQuery(plan contracts.QueryPlan) (spanResult0 string, spanResult1 []any, spanResult2 []string, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	scope := sqlScope{columns: queries.ViewColumns, alias: "v"}
	fields := mergeSortFields(plan.Fields, plan.OrderBy)
	ordered, selectList, err := selectFieldList(scope, queries.ProjectionSelectColumns, fields)
	if err != nil {
		return "", nil, nil, err
	}
	builder := &sqlBuilder{}
	where, err := compileWhere(builder, scope, plan.Injected, plan.Filter, plan.AsOfSequence, plan.Keyset)
	if err != nil {
		return "", nil, nil, err
	}
	order, err := compileOrderBy(scope, plan.OrderBy)
	if err != nil {
		return "", nil, nil, err
	}
	statement := "SELECT " + selectList + " FROM " + queries.TableView + " v WHERE " + where + order + " LIMIT " + builder.bind(plan.Limit+1)
	return statement, builder.args, ordered, nil
}

func mergeSortFields(fields []string, keys []contracts.SortKey) []string {
	merged := append([]string(nil), fields...)
	for _, key := range keys {
		merged = append(merged, key.Field)
	}
	return merged
}

func CompileCountQuery(plan contracts.QueryPlan, selectExpression string) (spanResult0 string, spanResult1 []any, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	scope := sqlScope{columns: queries.ViewColumns, alias: "v"}
	builder := &sqlBuilder{}
	where, err := compileWhere(builder, scope, plan.Injected, plan.Filter, plan.AsOfSequence, nil)
	if err != nil {
		return "", nil, err
	}
	return "SELECT " + selectExpression + " FROM " + queries.TableView + " v WHERE " + where, builder.args, nil
}

type scanTarget struct {
	name string
	kind queries.ColumnKind
	dest any
}

func newScanTarget(name string, kind queries.ColumnKind) scanTarget {
	switch kind {
	case queries.KindNullText:
		return scanTarget{name: name, kind: kind, dest: new(sql.NullString)}
	case queries.KindTime:
		return scanTarget{name: name, kind: kind, dest: new(time.Time)}
	case queries.KindNullTime:
		return scanTarget{name: name, kind: kind, dest: new(sql.NullTime)}
	case queries.KindInt64:
		return scanTarget{name: name, kind: kind, dest: new(int64)}
	case queries.KindBool:
		return scanTarget{name: name, kind: kind, dest: new(bool)}
	case queries.KindTextArray:
		return scanTarget{name: name, kind: kind, dest: new(pq.StringArray)}
	case queries.KindNullInt64:
		return scanTarget{name: name, kind: kind, dest: new(sql.NullInt64)}
	}
	return scanTarget{name: name, kind: kind, dest: new(string)}
}

func (t scanTarget) value() any {
	switch dest := t.dest.(type) {
	case *sql.NullString:
		if !dest.Valid {
			return nil
		}
		return dest.String
	case *sql.NullTime:
		if !dest.Valid {
			return nil
		}
		return dest.Time.UTC()
	case *sql.NullInt64:
		if !dest.Valid {
			return nil
		}
		return dest.Int64
	case *time.Time:
		return dest.UTC()
	case *int64:
		return *dest
	case *bool:
		return *dest
	case *pq.StringArray:
		return []string(*dest)
	case *string:
		return *dest
	}
	return nil
}

func scanRows(rows *sql.Rows, names []string, projection map[string]queries.ColumnSpec) ([]contracts.Row, error) {
	defer rows.Close()
	result := make([]contracts.Row, 0)
	for rows.Next() {
		targets := make([]scanTarget, 0, len(names))
		destinations := make([]any, 0, len(names))
		for _, name := range names {
			target := newScanTarget(name, projection[name].Kind)
			targets = append(targets, target)
			destinations = append(destinations, target.dest)
		}
		if err := rows.Scan(destinations...); err != nil {
			return nil, mapViewError(err)
		}
		row := make(contracts.Row, len(targets))
		for _, target := range targets {
			row[target.name] = target.value()
		}
		result = append(result, row)
	}
	return result, mapViewError(rows.Err())
}

func mapViewError(err error) error {
	if err == nil {
		return nil
	}
	var applicationError *appErrors.ApplicationError
	if errors.As(err, &applicationError) {
		return applicationError
	}
	var pgErr *pq.Error
	if (errors.As(err, &pgErr) && string(pgErr.Code) == queryCanceledState) || errors.Is(err, context.DeadlineExceeded) {
		return appErrors.NewTypedError(504, constants.TQErrQueryTimeout, "query exceeded its time budget")
	}
	return appErrors.NewTypedError(503, constants.TQErrStoreUnavailable, "the data store is unavailable")
}

func (a *PostgresTransactionViewRepositoryAdapter) runReadOnly(ctx context.Context, tenantID string, work func(DBExecutor) error) error {
	tx, err := a.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return mapViewError(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.ExecContext(ctx, queries.FLOW_SET_TENANT_CONTEXT, tenantID); err != nil {
		return mapViewError(err)
	}
	if _, err = tx.ExecContext(ctx, queries.FLOW_SET_STATEMENT_TIMEOUT, statementTimeoutMillis); err != nil {
		return mapViewError(err)
	}
	if err = work(tx); err != nil {
		return err
	}
	return mapViewError(tx.Commit())
}

func (a *PostgresTransactionViewRepositoryAdapter) runWrite(ctx context.Context, work func(DBExecutor) error) error {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return mapViewError(err)
	}
	if err = work(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return mapViewError(tx.Commit())
}

func (a *PostgresTransactionViewRepositoryAdapter) ListTransactions(ctx context.Context, plan contracts.QueryPlan) (spanResult0 []contracts.Row, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	statement, args, names, err := CompileListQuery(plan)
	if err != nil {
		return nil, err
	}
	var result []contracts.Row
	err = a.runReadOnly(ctx, plan.Injected.TenantID, func(exec DBExecutor) error {
		rows, queryErr := exec.QueryContext(ctx, statement, args...)
		if queryErr != nil {
			return mapViewError(queryErr)
		}
		var scanErr error
		result, scanErr = scanRows(rows, names, queries.ProjectionSelectColumns)
		return scanErr
	})
	return result, err
}

func (a *PostgresTransactionViewRepositoryAdapter) CountTransactions(ctx context.Context, plan contracts.QueryPlan) (spanResult0 int64, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	statement, args, err := CompileCountQuery(plan, "count(*)")
	if err != nil {
		return 0, err
	}
	var total int64
	err = a.runReadOnly(ctx, plan.Injected.TenantID, func(exec DBExecutor) error {
		return mapViewError(exec.QueryRowContext(ctx, statement, args...).Scan(&total))
	})
	return total, err
}

func (a *PostgresTransactionViewRepositoryAdapter) EstimatePlanRows(ctx context.Context, plan contracts.QueryPlan) (spanResult0 int64, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	statement, args, err := CompileCountQuery(plan, "1")
	if err != nil {
		return 0, err
	}
	var estimate int64
	err = a.runReadOnly(ctx, plan.Injected.TenantID, func(exec DBExecutor) error {
		var raw []byte
		if scanErr := exec.QueryRowContext(ctx, "EXPLAIN (FORMAT JSON) "+statement, args...).Scan(&raw); scanErr != nil {
			return mapViewError(scanErr)
		}
		var parsed []struct {
			Plan struct {
				PlanRows float64 `json:"Plan Rows"`
			} `json:"Plan"`
		}
		if jsonErr := json.Unmarshal(raw, &parsed); jsonErr != nil || len(parsed) == 0 {
			return appErrors.NewTypedError(503, constants.TQErrStoreUnavailable, "the data store is unavailable")
		}
		estimate = int64(parsed[0].Plan.PlanRows)
		return nil
	})
	return estimate, err
}

func (a *PostgresTransactionViewRepositoryAdapter) FindTransactionByID(ctx context.Context, tenantID string, publicID string, injected contracts.InjectedPredicates, fields []string) (spanResult0 contracts.Row, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	rows, err := a.ListTransactionsByIDs(ctx, tenantID, []string{publicID}, injected, fields)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return rows[0], nil
}

func (a *PostgresTransactionViewRepositoryAdapter) ListTransactionsByIDs(ctx context.Context, tenantID string, publicIDs []string, injected contracts.InjectedPredicates, fields []string) (spanResult0 []contracts.Row, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	scope := sqlScope{columns: queries.ViewColumns, alias: "v"}
	names, selectList, err := selectFieldList(scope, queries.ProjectionSelectColumns, fields)
	if err != nil {
		return nil, err
	}
	builder := &sqlBuilder{}
	clause := compileInjected(builder, scope, injected) + " AND v.public_id = ANY(" + builder.bind(pq.Array(publicIDs)) + ")"
	statement := "SELECT " + selectList + " FROM " + queries.TableView + " v WHERE " + clause
	var result []contracts.Row
	err = a.runReadOnly(ctx, tenantID, func(exec DBExecutor) error {
		rows, queryErr := exec.QueryContext(ctx, statement, builder.args...)
		if queryErr != nil {
			return mapViewError(queryErr)
		}
		var scanErr error
		result, scanErr = scanRows(rows, names, queries.ProjectionSelectColumns)
		return scanErr
	})
	return result, err
}

func (a *PostgresTransactionViewRepositoryAdapter) FindTransactionSagaID(ctx context.Context, tenantID string, publicID string, injected contracts.InjectedPredicates) (spanResult0 *uuid.UUID, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	scope := sqlScope{columns: queries.ViewColumns, alias: "v"}
	builder := &sqlBuilder{}
	clause := compileInjected(builder, scope, injected) + " AND v.public_id = " + builder.bind(publicID)
	var found *uuid.UUID
	err := a.runReadOnly(ctx, tenantID, func(exec DBExecutor) error {
		var sagaID uuid.UUID
		scanErr := exec.QueryRowContext(ctx, "SELECT v.saga_id FROM "+queries.TableView+" v WHERE "+clause, builder.args...).Scan(&sagaID)
		if errors.Is(scanErr, sql.ErrNoRows) {
			return nil
		}
		if scanErr != nil {
			return mapViewError(scanErr)
		}
		found = &sagaID
		return nil
	})
	return found, err
}

func (a *PostgresTransactionViewRepositoryAdapter) FindTenantRowEstimate(ctx context.Context, tenantID string) (spanResult0 int64, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var total int64
	err := a.runReadOnly(ctx, tenantID, func(exec DBExecutor) error {
		return mapViewError(exec.QueryRowContext(ctx, queries.FLOW_SELECT_TENANT_ROW_ESTIMATE_SOURCE, tenantID).Scan(&total))
	})
	return total, err
}

func (a *PostgresTransactionViewRepositoryAdapter) FindWatermark(ctx context.Context, store contracts.StoreName) (spanResult0 contracts.Watermark, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	watermark := contracts.Watermark{Store: store}
	err := a.runWrite(ctx, func(exec DBExecutor) error {
		var name string
		return mapViewError(exec.QueryRowContext(ctx, queries.FLOW_SELECT_TQ_WATERMARK, string(store)).Scan(&name, &watermark.Sequence, &watermark.AsOf, &watermark.Rebuilding))
	})
	return watermark, err
}

func (a *PostgresTransactionViewRepositoryAdapter) UpdateTransactionProjection(ctx context.Context, record contracts.ProjectionRecord) (spanResult0 int64, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	var sequence int64
	err := a.runWrite(ctx, func(exec DBExecutor) error {
		var workErr error
		sequence, workErr = writeProjection(ctx, exec, record)
		return workErr
	})
	return sequence, err
}

func writeProjection(ctx context.Context, exec DBExecutor, record contracts.ProjectionRecord) (int64, error) {
	if _, err := exec.ExecContext(ctx, queries.FLOW_ACQUIRE_PROJECTION_LOCK); err != nil {
		return 0, mapViewError(err)
	}
	var sequence int64
	if err := exec.QueryRowContext(ctx, queries.FLOW_SELECT_NEXT_PROJECTION_SEQUENCE).Scan(&sequence); err != nil {
		return 0, mapViewError(err)
	}
	id := projectionRowID(record)
	if _, err := exec.ExecContext(ctx, queries.FLOW_UPSERT_TRANSACTION_VIEW,
		record.TenantID, id, record.PublicID, record.SagaID, record.InternalStatus, record.BusinessStatus, record.WorkflowType,
		record.WorkflowVersion, record.CreatedAt, record.EffectiveAt, record.CompletedAt, record.AmountMinor, record.Currency,
		record.CustomerRef, record.CustomerEmailBlind, record.CustomerEmailMask, record.MerchantRef, record.ProviderReference,
		record.CorrelationID.String(), pq.Array(nonNilStrings(record.Tags)), record.Description, record.CompensationStatus,
		record.NeedsAttention, record.OwnerRef, record.TeamRef, record.IsInternalOnly, record.ArchivedAt, sequence); err != nil {
		return 0, mapViewError(err)
	}
	if err := replaceSteps(ctx, exec, record, id); err != nil {
		return 0, err
	}
	_, err := exec.ExecContext(ctx, queries.FLOW_UPDATE_TQ_WATERMARK, string(contracts.StorePrimary), sequence)
	return sequence, mapViewError(err)
}

func projectionRowID(record contracts.ProjectionRecord) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(record.TenantID+"|"+record.PublicID))
}

func replaceSteps(ctx context.Context, exec DBExecutor, record contracts.ProjectionRecord, id uuid.UUID) error {
	if _, err := exec.ExecContext(ctx, queries.FLOW_DELETE_TRANSACTION_VIEW_STEPS, record.TenantID, id); err != nil {
		return mapViewError(err)
	}
	for position, step := range record.Steps {
		if _, err := exec.ExecContext(ctx, queries.FLOW_INSERT_TRANSACTION_VIEW_STEP,
			record.TenantID, id, position+1, step.Name, step.Status, step.Attempts, step.LastErrorClass, step.CompletedAt); err != nil {
			return mapViewError(err)
		}
	}
	return nil
}

type aggregateColumn struct {
	name  string
	kind  queries.ColumnKind
	isKey bool
}

func CompileAggregateQuery(plan contracts.AggregatePlan, scope sqlScope, table string) (spanResult0 string, spanResult1 []any, spanResult2 []aggregateColumn, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	builder := &sqlBuilder{}
	columns, selectParts, groupParts := make([]aggregateColumn, 0), make([]string, 0), make([]string, 0)
	for _, field := range plan.GroupBy {
		spec, column, err := columnFor(scope, field)
		if err != nil {
			return "", nil, nil, err
		}
		columns = append(columns, aggregateColumn{name: field, kind: spec.Kind, isKey: true})
		selectParts, groupParts = append(selectParts, column), append(groupParts, column)
	}
	if plan.TimeBucket != nil {
		_, column, err := columnFor(scope, plan.TimeBucket.Field)
		if err != nil {
			return "", nil, nil, err
		}
		expression := "(date_trunc(" + builder.bind(plan.TimeBucket.Size) + ", " + column + " AT TIME ZONE " + builder.bind(plan.TimeBucket.Zone) + ") AT TIME ZONE " + builder.bind(plan.TimeBucket.Zone) + ")"
		columns = append(columns, aggregateColumn{name: "bucket", kind: queries.KindTime, isKey: true})
		selectParts, groupParts = append(selectParts, expression), append(groupParts, expression)
	}
	for _, expression := range plan.Aggregates {
		part, column, err := compileAggregateExpression(scope, expression)
		if err != nil {
			return "", nil, nil, err
		}
		columns, selectParts = append(columns, column), append(selectParts, part)
	}
	where, err := compileWhere(builder, scope, plan.Injected, plan.Filter, plan.AsOfSequence, nil)
	if err != nil {
		return "", nil, nil, err
	}
	statement := "SELECT " + strings.Join(selectParts, ", ") + " FROM " + table + " " + scope.alias + " WHERE " + where
	if len(groupParts) > 0 {
		statement += " GROUP BY " + strings.Join(groupParts, ", ") + " ORDER BY " + strings.Join(groupParts, ", ")
	}
	return statement + " LIMIT " + strconv.Itoa(contracts.MaxAggregateGroups+rowLimitProbe), builder.args, columns, nil
}

func compileAggregateExpression(scope sqlScope, expression contracts.AggregateExpression) (string, aggregateColumn, error) {
	if expression.Function == contracts.AggCount {
		return "count(*)", aggregateColumn{name: "count", kind: queries.KindInt64}, nil
	}
	_, column, err := columnFor(scope, expression.Field)
	if err != nil {
		return "", aggregateColumn{}, err
	}
	name := strings.ToLower(string(expression.Function)) + "_" + expression.Field
	switch expression.Function {
	case contracts.AggCountDistinct:
		return "count(DISTINCT " + column + ")", aggregateColumn{name: name, kind: queries.KindInt64}, nil
	case contracts.AggAvg:
		return "round(avg(" + column + "))::bigint", aggregateColumn{name: name, kind: queries.KindNullInt64}, nil
	case contracts.AggSum:
		return "sum(" + column + ")::bigint", aggregateColumn{name: name, kind: queries.KindNullInt64}, nil
	}
	return strings.ToLower(string(expression.Function)) + "(" + column + ")", aggregateColumn{name: name, kind: queries.KindNullInt64}, nil
}

func scanAggregateRows(rows *sql.Rows, columns []aggregateColumn) ([]contracts.AggregateGroup, error) {
	defer rows.Close()
	groups := make([]contracts.AggregateGroup, 0)
	for rows.Next() {
		targets := make([]scanTarget, 0, len(columns))
		destinations := make([]any, 0, len(columns))
		for _, column := range columns {
			target := newScanTarget(column.name, column.kind)
			targets = append(targets, target)
			destinations = append(destinations, target.dest)
		}
		if err := rows.Scan(destinations...); err != nil {
			return nil, mapViewError(err)
		}
		groups = append(groups, buildAggregateGroup(columns, targets))
	}
	return groups, mapViewError(rows.Err())
}

func buildAggregateGroup(columns []aggregateColumn, targets []scanTarget) contracts.AggregateGroup {
	group := contracts.AggregateGroup{Keys: map[string]any{}, Metrics: map[string]any{}}
	for index, column := range columns {
		if column.isKey {
			group.Keys[column.name] = targets[index].value()
		} else {
			group.Metrics[column.name] = targets[index].value()
		}
	}
	return group
}

func (a *PostgresTransactionViewRepositoryAdapter) AggregateTransactions(ctx context.Context, plan contracts.AggregatePlan) (spanResult0 []contracts.AggregateGroup, spanErr error) {
	defer observability.TraceEnter(&spanErr)()
	statement, args, columns, err := CompileAggregateQuery(plan, sqlScope{columns: queries.ViewColumns, alias: "v"}, queries.TableView)
	if err != nil {
		return nil, err
	}
	var groups []contracts.AggregateGroup
	err = a.runReadOnly(ctx, plan.Injected.TenantID, func(exec DBExecutor) error {
		rows, queryErr := exec.QueryContext(ctx, statement, args...)
		if queryErr != nil {
			return mapViewError(queryErr)
		}
		var scanErr error
		groups, scanErr = scanAggregateRows(rows, columns)
		return scanErr
	})
	return groups, err
}

func SortedFieldNames(columns map[string]queries.ColumnSpec) []string {
	defer observability.TraceEnter(nil)()
	names := make([]string, 0, len(columns))
	for name := range columns {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

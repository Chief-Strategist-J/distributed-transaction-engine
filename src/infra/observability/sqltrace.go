package observability

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"regexp"
	"runtime"
	"strings"
	"sync"

	"distributed-transaction-engine/src/shared/utils"
	"github.com/lib/pq"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	TracedPostgresDriverName = "postgres-traced"
	maxStatementAttribute    = 400
	adapterPathMarker        = "/infrastructure/adapters/"
	maxCallerFrames          = 24
)

var (
	registerOnce sync.Once
	tablePattern = regexp.MustCompile(`(?is)\b(?:from|into|update|join)\s+([a-z_][a-z0-9_.]*)`)
	spacePattern = regexp.MustCompile(`\s+`)
)

const (
	RoleApp        = "dtx_app"
	RolePlatform   = "dtx_platform"
	scopeStatement = "SELECT set_config('app.tenant_id', $1, false), set_config('role', $2, false)"
)

func OpenTracedPostgres(dsn string) (*sql.DB, error) {
	registerOnce.Do(func() {
		sql.Register(TracedPostgresDriverName, &tracedDriver{base: &pq.Driver{}})
	})
	return sql.Open(TracedPostgresDriverName, dsn)
}

func OpenTracedPostgresScoped(dsn string) (*sql.DB, error) {
	base, err := pq.NewConnector(dsn)
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(&scopedConnector{base: base}), nil
}

type tracedDriver struct {
	base driver.Driver
}

func (d *tracedDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.base.Open(name)
	if err != nil {
		return nil, err
	}
	return &tracedConn{Conn: conn}, nil
}

type scopedConnector struct {
	base driver.Connector
}

func (s *scopedConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := s.base.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &tracedConn{Conn: conn, scoped: true}, nil
}

func (s *scopedConnector) Driver() driver.Driver {
	return &tracedDriver{base: &pq.Driver{}}
}

type tracedConn struct {
	driver.Conn
	scoped  bool
	inTx    bool
	applied bool
	current utils.TenantScope
}

func dbTracer() trace.Tracer {
	return otel.Tracer("distributed-transaction-engine/db")
}

type sqlCall struct {
	operation string
	table     string
	statement string
	adapter   CodeLocation
}

func describeStatement(query string) sqlCall {
	statement := strings.TrimSpace(spacePattern.ReplaceAllString(query, " "))
	call := sqlCall{operation: "SQL", statement: statement, adapter: locateAdapterFrame()}
	if fields := strings.Fields(statement); len(fields) > 0 {
		call.operation = strings.ToUpper(strings.Trim(fields[0], "("))
	}
	if matched := tablePattern.FindStringSubmatch(statement); matched != nil {
		call.table = matched[1]
	}
	if len(call.statement) > maxStatementAttribute {
		call.statement = call.statement[:maxStatementAttribute]
	}
	return call
}

func locateAdapterFrame() CodeLocation {
	pcs := make([]uintptr, maxCallerFrames)
	count := runtime.Callers(3, pcs)
	frames := runtime.CallersFrames(pcs[:count])
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.File, adapterPathMarker) {
			location := locatePath(frame.File)
			location.Function = shortFunctionName(frame.Function)
			return location
		}
		if !more {
			return CodeLocation{Layer: LayerAdapter}
		}
	}
}

func (c sqlCall) spanName() string {
	name := c.operation
	if c.table != "" {
		name += " " + c.table
	}
	if c.adapter.Function != "" {
		return c.adapter.Function + ": " + name
	}
	return "db " + name
}

func startSQLSpan(ctx context.Context, query string) (context.Context, trace.Span, sqlCall) {
	ctx = adoptCurrent(ctx)
	call := describeStatement(query)
	attributes := []attribute.KeyValue{
		attribute.String("db.system", "postgresql"),
		attribute.String("db.operation", call.operation),
		attribute.String("db.statement", call.statement),
		attribute.String(AttrLayer, LayerAdapter),
	}
	if call.table != "" {
		attributes = append(attributes, attribute.String("db.sql.table", call.table))
	}
	if call.adapter.Feature != "" {
		attributes = append(attributes, attribute.String(AttrFeature, call.adapter.Feature))
	}
	if call.adapter.Function != "" {
		attributes = append(attributes, attribute.String(AttrFunction, call.adapter.Function))
	}
	spanCtx, span := dbTracer().Start(ctx, call.spanName(), trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attributes...))
	return spanCtx, span, call
}

func finishSQLSpan(span trace.Span, err error) {
	if err == nil {
		span.SetStatus(codes.Ok, "")
		span.SetAttributes(attribute.String(AttrOutcome, OutcomeOK))
		span.End()
		return
	}
	safe := newSanitizedError(err)
	span.RecordError(safe)
	span.SetStatus(codes.Error, safe.Error())
	span.SetAttributes(attribute.String(AttrOutcome, OutcomeError))
	if coded, ok := err.(interface{ SQLState() string }); ok {
		span.SetAttributes(attribute.String("db.error.sqlstate", coded.SQLState()))
	}
	span.End()
}

func (c *tracedConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	execer, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	if scopeErr := c.applyScope(ctx); scopeErr != nil {
		return nil, scopeErr
	}
	spanCtx, span, _ := startSQLSpan(ctx, query)
	result, err := execer.ExecContext(spanCtx, query, args)
	if err == nil && result != nil {
		if affected, affectedErr := result.RowsAffected(); affectedErr == nil {
			span.SetAttributes(attribute.Int64("db.rows_affected", affected))
		}
	}
	finishSQLSpan(span, err)
	return result, err
}

func (c *tracedConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	queryer, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	if scopeErr := c.applyScope(ctx); scopeErr != nil {
		return nil, scopeErr
	}
	spanCtx, span, _ := startSQLSpan(ctx, query)
	rows, err := queryer.QueryContext(spanCtx, query, args)
	finishSQLSpan(span, err)
	return rows, err
}

func (c *tracedConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	var statement driver.Stmt
	var err error
	if preparer, ok := c.Conn.(driver.ConnPrepareContext); ok {
		statement, err = preparer.PrepareContext(ctx, query)
	} else {
		statement, err = c.Conn.Prepare(query)
	}
	if err != nil {
		return nil, err
	}
	return &tracedStmt{Stmt: statement, query: query, conn: c}, nil
}

func (c *tracedConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *tracedConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *tracedConn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	ctx = adoptCurrent(ctx)
	if scopeErr := c.applyScope(ctx); scopeErr != nil {
		return nil, scopeErr
	}
	spanCtx, span := dbTracer().Start(ctx, "db BEGIN", trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("db.system", "postgresql"), attribute.String("db.operation", "BEGIN"), attribute.String(AttrLayer, LayerAdapter)))
	var tx driver.Tx
	var err error
	if beginner, ok := c.Conn.(driver.ConnBeginTx); ok {
		tx, err = beginner.BeginTx(spanCtx, options)
	} else {
		tx, err = c.Conn.Begin()
	}
	finishSQLSpan(span, err)
	if err != nil {
		return nil, err
	}
	c.inTx = true
	return &tracedTx{Tx: tx, ctx: ctx, conn: c}, nil
}

func (c *tracedConn) Ping(ctx context.Context) error {
	if pinger, ok := c.Conn.(driver.Pinger); ok {
		return pinger.Ping(ctx)
	}
	return nil
}

func (c *tracedConn) CheckNamedValue(value *driver.NamedValue) error {
	if checker, ok := c.Conn.(driver.NamedValueChecker); ok {
		return checker.CheckNamedValue(value)
	}
	return driver.ErrSkip
}

func (c *tracedConn) ResetSession(ctx context.Context) error {
	if resetter, ok := c.Conn.(driver.SessionResetter); ok {
		return resetter.ResetSession(ctx)
	}
	return nil
}

func (c *tracedConn) IsValid() bool {
	if validator, ok := c.Conn.(driver.Validator); ok {
		return validator.IsValid()
	}
	return true
}

type tracedTx struct {
	driver.Tx
	ctx  context.Context
	conn *tracedConn
}

func (t *tracedTx) Commit() error {
	return t.finish("COMMIT", t.Tx.Commit)
}

func (t *tracedTx) Rollback() error {
	return t.finish("ROLLBACK", t.Tx.Rollback)
}

func (t *tracedTx) finish(operation string, action func() error) error {
	_, span := dbTracer().Start(t.ctx, "db "+operation, trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("db.system", "postgresql"), attribute.String("db.operation", operation), attribute.String(AttrLayer, LayerAdapter)))
	err := action()
	t.conn.inTx = false
	finishSQLSpan(span, err)
	return err
}

type tracedStmt struct {
	driver.Stmt
	query string
	conn  *tracedConn
}

func (s *tracedStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if scopeErr := s.conn.applyScope(ctx); scopeErr != nil {
		return nil, scopeErr
	}
	spanCtx, span, _ := startSQLSpan(ctx, s.query)
	var result driver.Result
	var err error
	if execer, ok := s.Stmt.(driver.StmtExecContext); ok {
		result, err = execer.ExecContext(spanCtx, args)
	} else {
		result, err = s.Stmt.Exec(namedToValues(args))
	}
	finishSQLSpan(span, err)
	return result, err
}

func (s *tracedStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if scopeErr := s.conn.applyScope(ctx); scopeErr != nil {
		return nil, scopeErr
	}
	spanCtx, span, _ := startSQLSpan(ctx, s.query)
	var rows driver.Rows
	var err error
	if queryer, ok := s.Stmt.(driver.StmtQueryContext); ok {
		rows, err = queryer.QueryContext(spanCtx, args)
	} else {
		rows, err = s.Stmt.Query(namedToValues(args))
	}
	finishSQLSpan(span, err)
	return rows, err
}

func namedToValues(args []driver.NamedValue) []driver.Value {
	values := make([]driver.Value, 0, len(args))
	for _, arg := range args {
		values = append(values, arg.Value)
	}
	return values
}

func (c *tracedConn) applyScope(ctx context.Context) error {
	if !c.scoped || c.inTx {
		return nil
	}
	want := utils.TenantScopeFrom(ctx)
	if c.applied && c.current == want {
		return nil
	}
	role := RoleApp
	if want.Platform {
		role = RolePlatform
	}
	queryer, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return errors.New("scoped connection requires a queryer driver")
	}
	rows, err := queryer.QueryContext(ctx, scopeStatement, []driver.NamedValue{{Ordinal: 1, Value: want.TenantID}, {Ordinal: 2, Value: role}})
	if err != nil {
		c.applied = false
		return err
	}
	c.applied, c.current = true, want
	return rows.Close()
}

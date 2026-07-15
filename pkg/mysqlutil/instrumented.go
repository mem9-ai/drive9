package mysqlutil

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"go.uber.org/zap"
)

const (
	RoleMeta       = "meta"
	RoleUser       = "user"
	RoleUserSchema = "user_schema"
)

const (
	maxSQLTraceLen      = 4096
	maxSQLTraceInputLen = maxSQLTraceLen * 2
)

func OpenInstrumented(ctx context.Context, dsn, role string) (*sql.DB, error) {
	return OpenInstrumentedForTenant(ctx, dsn, role, "")
}

func OpenInstrumentedForTenant(ctx context.Context, dsn, role, tenantID string) (*sql.DB, error) {
	connector, err := (&mysql.MySQLDriver{}).OpenConnector(dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql connector: %w", err)
	}
	db := sql.OpenDB(instrumentedConnector{base: connector, role: role})
	ApplyPoolDefaults(db, role)
	metrics.RegisterTenantDB(role, tenantID, db)
	if err := db.PingContext(ctx); err != nil {
		metrics.UnregisterDB(db)
		_ = db.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	return db, nil
}

func CloseInstrumented(db *sql.DB) error {
	if db == nil {
		return nil
	}
	metrics.UnregisterDB(db)
	return db.Close()
}

type instrumentedConnector struct {
	base driver.Connector
	role string
}

func (c instrumentedConnector) Connect(ctx context.Context) (driver.Conn, error) {
	start := time.Now()
	conn, err := c.base.Connect(ctx)
	if !errors.Is(err, driver.ErrSkip) {
		observeDBOperation(ctx, c.role, "connect", "", start, err)
	}
	if err != nil {
		return nil, err
	}
	return instrumentedConn{base: conn, role: c.role}, nil
}

func (c instrumentedConnector) Driver() driver.Driver {
	return c.base.Driver()
}

type instrumentedConn struct {
	base driver.Conn
	role string
}

func (c instrumentedConn) Prepare(query string) (driver.Stmt, error) {
	start := time.Now()
	stmt, err := c.base.Prepare(query)
	if !errors.Is(err, driver.ErrSkip) {
		observeDBOperation(context.Background(), c.role, "prepare", query, start, err)
	}
	if err != nil {
		return nil, err
	}
	return instrumentedStmt{base: stmt, role: c.role, query: query}, nil
}

func (c instrumentedConn) Close() error {
	return c.base.Close()
}

func (c instrumentedConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c instrumentedConn) Ping(ctx context.Context) error {
	pinger, ok := c.base.(driver.Pinger)
	if !ok {
		return nil
	}
	start := time.Now()
	err := pinger.Ping(ctx)
	if !errors.Is(err, driver.ErrSkip) {
		observeDBOperation(ctx, c.role, "ping", "", start, err)
	}
	return err
}

func (c instrumentedConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	prep, ok := c.base.(driver.ConnPrepareContext)
	if !ok {
		return c.Prepare(query)
	}
	start := time.Now()
	stmt, err := prep.PrepareContext(ctx, query)
	if !errors.Is(err, driver.ErrSkip) {
		observeDBOperation(ctx, c.role, "prepare", query, start, err)
	}
	if err != nil {
		return nil, err
	}
	return instrumentedStmt{base: stmt, role: c.role, query: query}, nil
}

func (c instrumentedConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if beginner, ok := c.base.(driver.ConnBeginTx); ok {
		start := time.Now()
		tx, err := beginner.BeginTx(ctx, opts)
		if !errors.Is(err, driver.ErrSkip) {
			observeDBOperation(ctx, c.role, "begin", "", start, err)
		}
		if err != nil {
			return nil, err
		}
		return instrumentedTx{base: tx, role: c.role}, nil
	}
	if opts.Isolation != driver.IsolationLevel(0) || opts.ReadOnly {
		return nil, fmt.Errorf("driver does not support non-default transaction options")
	}
	return nil, driver.ErrSkip
}

func (c instrumentedConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	execer, ok := c.base.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	start := time.Now()
	res, err := execer.ExecContext(ctx, query, args)
	if !errors.Is(err, driver.ErrSkip) {
		observeDBOperation(ctx, c.role, "exec", query, start, err, res)
	}
	return res, err
}

func (c instrumentedConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	queryer, ok := c.base.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	start := time.Now()
	rows, err := queryer.QueryContext(ctx, query, args)
	if !errors.Is(err, driver.ErrSkip) {
		observeDBOperation(ctx, c.role, "query", query, start, err)
	}
	return rows, err
}

func (c instrumentedConn) ResetSession(ctx context.Context) error {
	resetter, ok := c.base.(driver.SessionResetter)
	if !ok {
		return nil
	}
	return resetter.ResetSession(ctx)
}

func (c instrumentedConn) IsValid() bool {
	validator, ok := c.base.(driver.Validator)
	if !ok {
		return true
	}
	return validator.IsValid()
}

func (c instrumentedConn) CheckNamedValue(nv *driver.NamedValue) error {
	checker, ok := c.base.(driver.NamedValueChecker)
	if !ok {
		return driver.ErrSkip
	}
	return checker.CheckNamedValue(nv)
}

type instrumentedStmt struct {
	base  driver.Stmt
	role  string
	query string
}

func (s instrumentedStmt) Close() error {
	return s.base.Close()
}

func (s instrumentedStmt) NumInput() int {
	return s.base.NumInput()
}

func (s instrumentedStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), toNamedValues(args))
}

func (s instrumentedStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), toNamedValues(args))
}

func (s instrumentedStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	execer, ok := s.base.(driver.StmtExecContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	start := time.Now()
	res, err := execer.ExecContext(ctx, args)
	if !errors.Is(err, driver.ErrSkip) {
		observeDBOperation(ctx, s.role, "exec", s.query, start, err, res)
	}
	return res, err
}

func (s instrumentedStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	queryer, ok := s.base.(driver.StmtQueryContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	start := time.Now()
	rows, err := queryer.QueryContext(ctx, args)
	if !errors.Is(err, driver.ErrSkip) {
		observeDBOperation(ctx, s.role, "query", s.query, start, err)
	}
	return rows, err
}

func (s instrumentedStmt) CheckNamedValue(nv *driver.NamedValue) error {
	checker, ok := s.base.(driver.NamedValueChecker)
	if !ok {
		return driver.ErrSkip
	}
	return checker.CheckNamedValue(nv)
}

type instrumentedTx struct {
	base driver.Tx
	role string
}

func (tx instrumentedTx) Commit() error {
	start := time.Now()
	err := tx.base.Commit()
	if !errors.Is(err, driver.ErrSkip) {
		observeDBOperation(context.Background(), tx.role, "commit", "", start, err)
	}
	return err
}

func (tx instrumentedTx) Rollback() error {
	start := time.Now()
	err := tx.base.Rollback()
	if !errors.Is(err, driver.ErrSkip) {
		observeDBOperation(context.Background(), tx.role, "rollback", "", start, err)
	}
	return err
}

func observeDBOperation(ctx context.Context, role, operation, query string, start time.Time, err error, results ...driver.Result) {
	elapsed := time.Since(start)
	result := dbResult(err)
	metrics.RecordDBOperation(role, operation, result, elapsed)

	if !logger.BenchTimingLogEnabled() {
		return
	}

	fields := []zap.Field{
		zap.String("role", role),
		zap.String("operation", operation),
		zap.String("result", result),
		zap.Float64("duration_ms", float64(elapsed.Microseconds())/1000.0),
	}
	if sqlText, truncated, sourceLen := sqlTraceText(query); sqlText != "" {
		fields = append(fields, zap.String("sql", sqlText))
		fields = append(fields, zap.Int("sql_len", sourceLen))
		if truncated {
			fields = append(fields, zap.Bool("sql_truncated", true))
		}
	}
	if len(results) > 0 && results[0] != nil {
		if affected, rowsErr := results[0].RowsAffected(); rowsErr == nil {
			fields = append(fields, zap.Int64("rows_affected", affected))
		}
	}
	if err != nil {
		fields = append(fields, safeDBErrorFields(err)...)
	}
	logger.InfoBenchTiming(ctx, "db_operation_timing", fields...)
}

func dbResult(err error) string {
	// Delegate to metrics.ResultForError so DB operation labels match worker
	// labels, especially for transient bad connections.
	return metrics.ResultForError(err)
}

func toNamedValues(args []driver.Value) []driver.NamedValue {
	if len(args) == 0 {
		return nil
	}
	named := make([]driver.NamedValue, 0, len(args))
	for idx, arg := range args {
		named = append(named, driver.NamedValue{
			Ordinal: idx + 1,
			Value:   arg,
		})
	}
	return named
}

var sqlWhitespace = regexp.MustCompile(`\s+`)

func sqlTraceText(query string) (string, bool, int) {
	sourceLen := len(query)
	if sourceLen == 0 {
		return "", false, 0
	}
	truncated := false
	if len(query) > maxSQLTraceInputLen {
		query = query[:maxSQLTraceInputLen]
		truncated = true
	}
	out := normalizeSQLForTrace(query)
	if len(out) > maxSQLTraceLen {
		out = out[:maxSQLTraceLen]
		truncated = true
	}
	return out, truncated, sourceLen
}

func normalizeSQLForTrace(query string) string {
	return sqlWhitespace.ReplaceAllString(strings.TrimSpace(redactSQLLiterals(query)), " ")
}

func redactSQLLiterals(query string) string {
	var b strings.Builder
	b.Grow(len(query))
	for i := 0; i < len(query); {
		switch query[i] {
		case '\'', '"':
			b.WriteByte('?')
			i = skipQuotedSQLLiteral(query, i, query[i])
		case '/':
			if i+1 < len(query) && query[i+1] == '*' {
				b.WriteByte(' ')
				i = skipBlockSQLComment(query, i)
				continue
			}
			b.WriteByte(query[i])
			i++
		case '-':
			if i+1 < len(query) && query[i+1] == '-' {
				b.WriteByte(' ')
				i = skipLineSQLComment(query, i)
				continue
			}
			b.WriteByte(query[i])
			i++
		case '#':
			b.WriteByte(' ')
			i = skipLineSQLComment(query, i)
		default:
			if isNumericSQLLiteralStart(query, i) {
				b.WriteByte('?')
				i = skipNumericSQLLiteral(query, i)
				continue
			}
			b.WriteByte(query[i])
			i++
		}
	}
	return b.String()
}

func safeDBErrorFields(err error) []zap.Field {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		fields := []zap.Field{
			zap.String("db_error_type", "mysql"),
			zap.Uint16("db_error_number", mysqlErr.Number),
		}
		if mysqlErr.SQLState != [5]byte{} {
			fields = append(fields, zap.String("db_error_sql_state", string(mysqlErr.SQLState[:])))
		}
		return fields
	}
	switch {
	case errors.Is(err, context.Canceled):
		return []zap.Field{zap.String("db_error_type", "context_canceled")}
	case errors.Is(err, context.DeadlineExceeded):
		return []zap.Field{zap.String("db_error_type", "context_deadline_exceeded")}
	case errors.Is(err, driver.ErrBadConn):
		return []zap.Field{zap.String("db_error_type", "bad_conn")}
	default:
		return []zap.Field{zap.String("db_error_type", fmt.Sprintf("%T", err))}
	}
}

func skipQuotedSQLLiteral(query string, start int, quote byte) int {
	for i := start + 1; i < len(query); i++ {
		switch query[i] {
		case '\\':
			if i+1 < len(query) {
				i++
			}
		case quote:
			if i+1 < len(query) && query[i+1] == quote {
				i++
				continue
			}
			return i + 1
		}
	}
	return len(query)
}

func skipBlockSQLComment(query string, start int) int {
	for i := start + 2; i < len(query)-1; i++ {
		if query[i] == '*' && query[i+1] == '/' {
			return i + 2
		}
	}
	return len(query)
}

func skipLineSQLComment(query string, start int) int {
	for i := start + 1; i < len(query); i++ {
		if query[i] == '\n' || query[i] == '\r' {
			return i
		}
	}
	return len(query)
}

func isNumericSQLLiteralStart(query string, i int) bool {
	if query[i] < '0' || query[i] > '9' {
		return false
	}
	return i == 0 || !isSQLIdentByte(query[i-1])
}

func skipNumericSQLLiteral(query string, start int) int {
	i := start
	for i < len(query) {
		c := query[i]
		if (c >= '0' && c <= '9') ||
			(c >= 'a' && c <= 'f') ||
			(c >= 'A' && c <= 'F') ||
			c == 'x' || c == 'X' || c == '.' || c == '+' || c == '-' {
			i++
			continue
		}
		break
	}
	return i
}

func isSQLIdentByte(c byte) bool {
	return (c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') ||
		c == '_' ||
		c == '$'
}

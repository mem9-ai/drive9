package mysqlutil

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/mem9-ai/dat9/pkg/metrics"
)

const (
	RoleMeta = "meta"
	RoleUser = "user"
)

func OpenInstrumented(ctx context.Context, dsn, role string) (*sql.DB, error) {
	connector, err := (&mysql.MySQLDriver{}).OpenConnector(dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql connector: %w", err)
	}
	db := sql.OpenDB(instrumentedConnector{base: connector, role: role})
	ApplyPoolDefaults(db)
	metrics.RegisterDB(role, db)
	if err := db.PingContext(ctx); err != nil {
		metrics.UnregisterDB(db)
		_ = db.Close()
		return nil, err
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
		observeDBOperation(c.role, "connect", start, err)
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
		observeDBOperation(c.role, "prepare", start, err)
	}
	if err != nil {
		return nil, err
	}
	return instrumentedStmt{base: stmt, role: c.role}, nil
}

func (c instrumentedConn) Close() error {
	return c.base.Close()
}

func (c instrumentedConn) Begin() (driver.Tx, error) {
	start := time.Now()
	tx, err := c.base.Begin()
	if !errors.Is(err, driver.ErrSkip) {
		observeDBOperation(c.role, "begin", start, err)
	}
	if err != nil {
		return nil, err
	}
	return instrumentedTx{base: tx, role: c.role}, nil
}

func (c instrumentedConn) Ping(ctx context.Context) error {
	pinger, ok := c.base.(driver.Pinger)
	if !ok {
		return nil
	}
	start := time.Now()
	err := pinger.Ping(ctx)
	if !errors.Is(err, driver.ErrSkip) {
		observeDBOperation(c.role, "ping", start, err)
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
		observeDBOperation(c.role, "prepare", start, err)
	}
	if err != nil {
		return nil, err
	}
	return instrumentedStmt{base: stmt, role: c.role}, nil
}

func (c instrumentedConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if beginner, ok := c.base.(driver.ConnBeginTx); ok {
		start := time.Now()
		tx, err := beginner.BeginTx(ctx, opts)
		if !errors.Is(err, driver.ErrSkip) {
			observeDBOperation(c.role, "begin", start, err)
		}
		if err != nil {
			return nil, err
		}
		return instrumentedTx{base: tx, role: c.role}, nil
	}
	if opts.Isolation != driver.IsolationLevel(0) || opts.ReadOnly {
		return nil, fmt.Errorf("driver does not support non-default transaction options")
	}
	return c.Begin()
}

func (c instrumentedConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	execer, ok := c.base.(driver.Execer)
	if !ok {
		return nil, driver.ErrSkip
	}
	start := time.Now()
	res, err := execer.Exec(query, args)
	if !errors.Is(err, driver.ErrSkip) {
		observeDBOperation(c.role, "exec", start, err)
	}
	return res, err
}

func (c instrumentedConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	queryer, ok := c.base.(driver.Queryer)
	if !ok {
		return nil, driver.ErrSkip
	}
	start := time.Now()
	rows, err := queryer.Query(query, args)
	if !errors.Is(err, driver.ErrSkip) {
		observeDBOperation(c.role, "query", start, err)
	}
	return rows, err
}

func (c instrumentedConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	execer, ok := c.base.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	start := time.Now()
	res, err := execer.ExecContext(ctx, query, args)
	if !errors.Is(err, driver.ErrSkip) {
		observeDBOperation(c.role, "exec", start, err)
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
		observeDBOperation(c.role, "query", start, err)
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
	base driver.Stmt
	role string
}

func (s instrumentedStmt) Close() error {
	return s.base.Close()
}

func (s instrumentedStmt) NumInput() int {
	return s.base.NumInput()
}

func (s instrumentedStmt) Exec(args []driver.Value) (driver.Result, error) {
	start := time.Now()
	res, err := s.base.Exec(args)
	if !errors.Is(err, driver.ErrSkip) {
		observeDBOperation(s.role, "exec", start, err)
	}
	return res, err
}

func (s instrumentedStmt) Query(args []driver.Value) (driver.Rows, error) {
	start := time.Now()
	rows, err := s.base.Query(args)
	if !errors.Is(err, driver.ErrSkip) {
		observeDBOperation(s.role, "query", start, err)
	}
	return rows, err
}

func (s instrumentedStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	execer, ok := s.base.(driver.StmtExecContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	start := time.Now()
	res, err := execer.ExecContext(ctx, args)
	if !errors.Is(err, driver.ErrSkip) {
		observeDBOperation(s.role, "exec", start, err)
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
		observeDBOperation(s.role, "query", start, err)
	}
	return rows, err
}

func (s instrumentedStmt) ColumnConverter(idx int) driver.ValueConverter {
	converter, ok := s.base.(driver.ColumnConverter)
	if !ok {
		return driver.DefaultParameterConverter
	}
	return converter.ColumnConverter(idx)
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
		observeDBOperation(tx.role, "commit", start, err)
	}
	return err
}

func (tx instrumentedTx) Rollback() error {
	start := time.Now()
	err := tx.base.Rollback()
	if !errors.Is(err, driver.ErrSkip) {
		observeDBOperation(tx.role, "rollback", start, err)
	}
	return err
}

func observeDBOperation(role, operation string, start time.Time, err error) {
	metrics.RecordDBOperation(role, operation, dbResult(err), time.Since(start))
}

func dbResult(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, driver.ErrBadConn):
		return "bad_conn"
	default:
		return "error"
	}
}

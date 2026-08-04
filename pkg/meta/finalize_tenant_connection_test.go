package meta

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
)

var finalizeRowsAffectedDriverSequence atomic.Int64

type finalizeRowsAffectedErrorDriver struct {
	err error
}

func (d finalizeRowsAffectedErrorDriver) Open(string) (driver.Conn, error) {
	return finalizeRowsAffectedErrorConn(d), nil
}

type finalizeRowsAffectedErrorConn struct {
	err error
}

func (c finalizeRowsAffectedErrorConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not supported")
}

func (c finalizeRowsAffectedErrorConn) Close() error { return nil }

func (c finalizeRowsAffectedErrorConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not supported")
}

func (c finalizeRowsAffectedErrorConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return finalizeRowsAffectedErrorResult(c), nil
}

type finalizeRowsAffectedErrorResult struct {
	err error
}

func (r finalizeRowsAffectedErrorResult) LastInsertId() (int64, error) { return 0, nil }

func (r finalizeRowsAffectedErrorResult) RowsAffected() (int64, error) { return 0, r.err }

func TestFinalizeTenantConnectionReturnsRowsAffectedError(t *testing.T) {
	wantErr := errors.New("rows affected unavailable")
	driverName := fmt.Sprintf("finalize-rows-affected-error-%d", finalizeRowsAffectedDriverSequence.Add(1))
	sql.Register(driverName, finalizeRowsAffectedErrorDriver{err: wantErr})
	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	s := &Store{db: db}
	updated, err := s.FinalizeTenantConnection(context.Background(), "tenant-1", TenantPending, TenantProvisioning, &Tenant{})
	if updated || !errors.Is(err, wantErr) {
		t.Fatalf("FinalizeTenantConnection updated=%v err=%v, want false and rows-affected error", updated, err)
	}
}

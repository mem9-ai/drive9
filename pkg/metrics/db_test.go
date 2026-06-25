package metrics

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeConnector is a minimal database/sql driver whose reachability can be
// toggled at runtime so the health probe can be exercised without a real MySQL.
type fakeConnector struct {
	healthy *atomic.Bool
}

func (c fakeConnector) Connect(context.Context) (driver.Conn, error) {
	if !c.healthy.Load() {
		return nil, errors.New("fake db unreachable")
	}
	return fakeConn{healthy: c.healthy}, nil
}

func (c fakeConnector) Driver() driver.Driver { return fakeDriver{} }

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) { return nil, errors.New("not supported") }

type fakeConn struct {
	healthy *atomic.Bool
}

func (fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (fakeConn) Close() error                        { return nil }
func (fakeConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }

func (c fakeConn) Ping(context.Context) error {
	if !c.healthy.Load() {
		return driver.ErrBadConn
	}
	return nil
}

func renderDB(t *testing.T) string {
	t.Helper()
	rec := httptest.NewRecorder()
	writeDBPrometheus(rec)
	return rec.Body.String()
}

func TestDBHealthProbeFlipsDriveDBUp(t *testing.T) {
	const role = "probe_test_role"

	healthy := &atomic.Bool{}
	healthy.Store(true)
	db := sql.OpenDB(fakeConnector{healthy: healthy})
	// Force a fresh connection per ping so toggling health takes effect immediately.
	db.SetMaxIdleConns(0)
	t.Cleanup(func() { UnregisterDB(db); _ = db.Close() })

	RegisterDB(role, db)

	var mu sync.Mutex
	type change struct {
		up  bool
		err error
	}
	var changes []change
	onChange := func(r string, up bool, err error) {
		if r != role {
			return
		}
		mu.Lock()
		changes = append(changes, change{up: up, err: err})
		mu.Unlock()
	}

	// 1) First probe while healthy: up=1, no transition logged.
	globalDB.probeOnce(context.Background(), time.Second, onChange)
	if out := renderDB(t); !strings.Contains(out, `drive9_db_up{role="`+role+`"} 1`) {
		t.Fatalf("expected drive9_db_up=1 after healthy probe, got:\n%s", out)
	}
	mu.Lock()
	if len(changes) != 0 {
		mu.Unlock()
		t.Fatalf("expected no transition on first healthy probe, got %+v", changes)
	}
	mu.Unlock()

	// 2) Database goes down: up=0, one down transition.
	healthy.Store(false)
	globalDB.probeOnce(context.Background(), time.Second, onChange)
	out := renderDB(t)
	if !strings.Contains(out, `drive9_db_up{role="`+role+`"} 0`) {
		t.Fatalf("expected drive9_db_up=0 after failed probe, got:\n%s", out)
	}
	if !strings.Contains(out, `drive9_db_unreachable_pools{role="`+role+`"} 1`) {
		t.Fatalf("expected drive9_db_unreachable_pools=1, got:\n%s", out)
	}

	// 3) Database recovers: up=1, one up transition.
	healthy.Store(true)
	globalDB.probeOnce(context.Background(), time.Second, onChange)
	if out := renderDB(t); !strings.Contains(out, `drive9_db_up{role="`+role+`"} 1`) {
		t.Fatalf("expected drive9_db_up=1 after recovery, got:\n%s", out)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(changes) != 2 {
		t.Fatalf("expected exactly 2 transitions (down, up), got %+v", changes)
	}
	if changes[0].up || changes[0].err == nil {
		t.Fatalf("expected first transition to be down with an error, got %+v", changes[0])
	}
	if !changes[1].up {
		t.Fatalf("expected second transition to be up, got %+v", changes[1])
	}
}

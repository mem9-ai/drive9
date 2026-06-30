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
	return fakeConn(c), nil
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
	onChange := func(info DBPoolInfo, up bool, err error) {
		if info.Role != role {
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

func TestDBMetricsIncludeTenantIDForTenantPools(t *testing.T) {
	const (
		role     = "user"
		tenantID = "tenant-db-metrics-test"
	)

	healthy := &atomic.Bool{}
	healthy.Store(true)
	db := sql.OpenDB(fakeConnector{healthy: healthy})
	db.SetMaxIdleConns(0)
	t.Cleanup(func() { UnregisterDB(db); _ = db.Close() })

	RegisterTenantDB(role, tenantID, db)
	globalDB.probeOnce(context.Background(), time.Second, nil)
	out := renderDB(t)

	if !strings.Contains(out, `drive9_db_up{role="user",tenant_id="`+tenantID+`"} 1`) {
		t.Fatalf("expected tenant db_up series, got:\n%s", out)
	}
	if !strings.Contains(out, `drive9_db_pool_registered{role="user",tenant_id="`+tenantID+`"} 1`) {
		t.Fatalf("expected tenant pool_registered series, got:\n%s", out)
	}
	if !strings.Contains(out, `drive9_db_pool_wait_count_total{role="user",tenant_id="`+tenantID+`"} 0`) {
		t.Fatalf("expected tenant pool wait series, got:\n%s", out)
	}
	rec := httptest.NewRecorder()
	WritePrometheus(rec)
	fullText := rec.Body.String()
	if !strings.Contains(fullText, `drive9_db_probe_duration_seconds_bucket{result="ok",role="user",tenant_id="`+tenantID+`"`) {
		t.Fatalf("expected tenant probe duration series, got:\n%s", fullText)
	}
}

func TestDBHealthProbeDropsUnregisteredTenantState(t *testing.T) {
	const tenantID = "tenant-db-unregister-test"

	healthy := &atomic.Bool{}
	healthy.Store(false)
	db := sql.OpenDB(fakeConnector{healthy: healthy})
	db.SetMaxIdleConns(0)
	t.Cleanup(func() { _ = db.Close() })

	RegisterTenantDB("user", tenantID, db)
	globalDB.probeOnce(context.Background(), time.Second, nil)
	if _, ok := globalDB.probe[dbMetricKey{role: "user", tenantID: tenantID}]; !ok {
		t.Fatalf("expected tenant probe state to be recorded")
	}

	UnregisterDB(db)
	globalDB.probeOnce(context.Background(), time.Second, nil)
	if _, ok := globalDB.probe[dbMetricKey{role: "user", tenantID: tenantID}]; ok {
		t.Fatalf("expected tenant probe state to be removed after unregister")
	}
}

func TestDBHealthProbeDoesNotMarkSaturatedTenantPoolDown(t *testing.T) {
	const tenantID = "tenant-db-saturated-test"

	healthy := &atomic.Bool{}
	healthy.Store(true)
	db := sql.OpenDB(fakeConnector{healthy: healthy})
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)
	t.Cleanup(func() { UnregisterDB(db); _ = db.Close() })

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open held conn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	RegisterTenantDB("user", tenantID, db)
	globalDB.probeOnce(context.Background(), 10*time.Millisecond, func(info DBPoolInfo, up bool, err error) {
		if info.TenantID == tenantID {
			t.Fatalf("expected no down transition for saturated tenant pool, got up=%v err=%v", up, err)
		}
	})

	out := renderDB(t)
	if !strings.Contains(out, `drive9_db_up{role="user",tenant_id="`+tenantID+`"} 1`) {
		t.Fatalf("expected saturated tenant pool to remain up, got:\n%s", out)
	}
	if !strings.Contains(out, `drive9_db_unreachable_pools{role="user",tenant_id="`+tenantID+`"} 0`) {
		t.Fatalf("expected saturated tenant pool not to count as unreachable, got:\n%s", out)
	}

	rec := httptest.NewRecorder()
	WritePrometheus(rec)
	fullText := rec.Body.String()
	if !strings.Contains(fullText, `drive9_db_probe_duration_seconds_bucket{result="pool_saturated",role="user",tenant_id="`+tenantID+`"`) {
		t.Fatalf("expected saturated probe duration series, got:\n%s", fullText)
	}
}

func TestDBHealthProbeDoesNotMarkSaturatedMetaPoolDown(t *testing.T) {
	healthy := &atomic.Bool{}
	healthy.Store(true)
	db := sql.OpenDB(fakeConnector{healthy: healthy})
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)
	t.Cleanup(func() {
		UnregisterDB(db)
		globalDB.probeOnce(context.Background(), time.Second, nil)
		_ = db.Close()
	})

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("open held conn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	RegisterDB("meta", db)
	globalDB.probeOnce(context.Background(), 10*time.Millisecond, func(info DBPoolInfo, up bool, err error) {
		if info.Role == "meta" {
			t.Fatalf("expected no down transition for saturated meta pool, got up=%v err=%v", up, err)
		}
	})

	out := renderDB(t)
	if !strings.Contains(out, `drive9_db_up{role="meta"} 1`) {
		t.Fatalf("expected saturated meta pool to remain up, got:\n%s", out)
	}
	if !strings.Contains(out, `drive9_db_unreachable_pools{role="meta"} 0`) {
		t.Fatalf("expected saturated meta pool not to count as unreachable, got:\n%s", out)
	}

	rec := httptest.NewRecorder()
	WritePrometheus(rec)
	fullText := rec.Body.String()
	if !strings.Contains(fullText, `drive9_db_probe_duration_seconds_bucket{result="pool_saturated",role="meta"`) {
		t.Fatalf("expected saturated meta probe duration series, got:\n%s", fullText)
	}
	if strings.Contains(fullText, `role="meta",tenant_id=`) {
		t.Fatalf("expected meta probe duration series to omit tenant_id, got:\n%s", fullText)
	}
}

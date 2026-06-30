package metrics

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"
)

// dbProbeState is the cached result of the most recent health probe for a role.
type dbProbeState struct {
	up               bool
	unreachablePools int
	totalPools       int
	lastProbe        time.Time
	known            bool
}

type registeredDB struct {
	role     string
	tenantID string
}

type dbMetricKey struct {
	role     string
	tenantID string
}

type dbMetrics struct {
	mu      sync.RWMutex
	dbs     map[*sql.DB]registeredDB
	probe   map[string]dbProbeState
	started bool
}

type dbPoolTotals struct {
	registered         int64
	openConnections    int64
	inUseConnections   int64
	idleConnections    int64
	maxOpenConnections int64
	waitCount          int64
	waitDuration       float64
	maxIdleClosed      int64
	maxIdleTimeClosed  int64
	maxLifetimeClosed  int64
}

var dbOperationDurationBounds = []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}
var dbProbeDurationBounds = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

var globalDB = &dbMetrics{
	dbs:   map[*sql.DB]registeredDB{},
	probe: map[string]dbProbeState{},
}

var dbMeter = globalMeterProvider.Meter("db")
var dbOperationsTotal = dbMeter.Int64Counter("drive9_db_operations_total", "Database operations by role/operation/result")
var dbOperationDuration = dbMeter.Float64Histogram("drive9_db_operation_duration_seconds", "Database operation duration histogram", dbOperationDurationBounds)
var dbProbeDuration = dbMeter.Float64Histogram("drive9_db_probe_duration_seconds", "Database health probe ping duration histogram by role/result", dbProbeDurationBounds)

// DefaultDBHealthProbeInterval and DefaultDBHealthProbeTimeout are the fallbacks
// used by StartDBHealthProbe when callers pass non-positive values.
const (
	DefaultDBHealthProbeInterval = 15 * time.Second
	DefaultDBHealthProbeTimeout  = 3 * time.Second
)

// DBHealthProbeOptions controls which registered DB pools are actively pinged.
// drive9-server only probes the long-lived control-plane DB; tenant user DB
// pools are short-lived workload pools and are observed through normal traffic.
type DBHealthProbeOptions struct {
	ProbeMeta bool
}

func RecordDBOperation(role, operation, result string, d time.Duration) {
	RegisterModule("db")
	attrs := []Attribute{
		Attr("role", cleanMetricValue(role, "unknown")),
		Attr("operation", cleanMetricValue(operation, "unknown")),
		Attr("result", cleanMetricValue(result, "unknown")),
	}
	dbOperationsTotal.Add(1, attrs...)
	dbOperationDuration.Observe(d.Seconds(), attrs...)
}

func RegisterDB(role string, db *sql.DB) {
	RegisterTenantDB(role, "", db)
}

func RegisterTenantDB(role, tenantID string, db *sql.DB) {
	if db == nil {
		return
	}
	RegisterModule("db")
	globalDB.mu.Lock()
	globalDB.dbs[db] = registeredDB{role: role, tenantID: tenantID}
	globalDB.mu.Unlock()
}

func UnregisterDB(db *sql.DB) {
	if db == nil {
		return
	}
	globalDB.mu.Lock()
	pool, ok := globalDB.dbs[db]
	delete(globalDB.dbs, db)
	if ok {
		role := pool.metricRole()
		if !globalDB.hasPoolLocked(role) {
			delete(globalDB.probe, role)
		}
	}
	globalDB.mu.Unlock()
}

// StartDBHealthProbe launches a background goroutine that periodically pings every
// registered long-lived dependency *sql.DB and publishes availability as the
// drive9_db_up gauge. In drive9-server this means the metadata store ("meta").
// Tenant user/user_schema pools are short-lived workload pools, so probing them
// would only keep connections warm and compete with business traffic.
//
// The probe never blocks the /metrics scrape path — scrapes read the cached probe
// state. onChange (optional) is invoked outside the lock on a role's first
// observed failure and on every subsequent up<->down transition, so callers can
// emit a log without coupling this package to a logger. The goroutine stops when
// ctx is done; repeat calls after the first are no-ops.
//
// Example alert (critical — control-plane DB unreachable):
//
//	max(drive9_db_up{role="meta"}) == 0
func StartDBHealthProbe(ctx context.Context, interval, timeout time.Duration, onChange func(role string, up bool, err error)) {
	StartDBHealthProbeWithOptions(ctx, interval, timeout, DBHealthProbeOptions{
		ProbeMeta: true,
	}, onChange)
}

// StartDBHealthProbeWithOptions is StartDBHealthProbe with role filtering.
func StartDBHealthProbeWithOptions(ctx context.Context, interval, timeout time.Duration, opts DBHealthProbeOptions, onChange func(role string, up bool, err error)) {
	if interval <= 0 {
		interval = DefaultDBHealthProbeInterval
	}
	if timeout <= 0 {
		timeout = DefaultDBHealthProbeTimeout
	}

	globalDB.mu.Lock()
	if globalDB.started {
		globalDB.mu.Unlock()
		return
	}
	globalDB.started = true
	globalDB.mu.Unlock()

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		globalDB.probeOnceWithOptions(ctx, timeout, opts, onChange)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				globalDB.probeOnceWithOptions(ctx, timeout, opts, onChange)
			}
		}
	}()
}

// probeOnce runs a single probe cycle across all registered pools and updates the
// cached per-role state. Exported indirectly for tests via probeOnce.
func (m *dbMetrics) probeOnce(ctx context.Context, timeout time.Duration, onChange func(role string, up bool, err error)) {
	m.probeOnceWithOptions(ctx, timeout, DBHealthProbeOptions{
		ProbeMeta: true,
	}, onChange)
}

func (m *dbMetrics) probeOnceWithOptions(ctx context.Context, timeout time.Duration, opts DBHealthProbeOptions, onChange func(role string, up bool, err error)) {
	m.mu.RLock()
	dbByPool := make(map[*sql.DB]registeredDB, len(m.dbs))
	for db, pool := range m.dbs {
		if !m.shouldProbePool(pool, opts) {
			continue
		}
		dbByPool[db] = pool
	}
	m.mu.RUnlock()

	type roleAgg struct {
		total       int
		unreachable int
		firstErr    error
	}
	results := map[string]*roleAgg{}
	for _, pool := range dbByPool {
		role := pool.metricRole()
		if results[role] == nil {
			results[role] = &roleAgg{}
		}
		results[role].total++
	}

	// Ping pools concurrently with a fixed bound so one slow/unreachable pool
	// cannot serialize the whole cycle if multiple long-lived DB dependencies
	// are registered in tests or future server modes.
	const probeConcurrency = 8
	sem := make(chan struct{}, probeConcurrency)
	var wg sync.WaitGroup
	var aggMu sync.Mutex
	for db, pool := range dbByPool {
		db, pool := db, pool
		role := pool.metricRole()
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			// PingContext borrows a connection from the pool (reusing an idle one,
			// else opening a new one when under MaxOpenConns) and returns it
			// immediately, so each cycle adds a transient +1 connection per pinged
			// pool — at most probeConcurrency in flight cluster-wide. Any opened
			// conn is later reaped by ConnMaxIdleTime.
			//
			start := time.Now()
			result := "ok"
			// A saturated bounded pool cannot lend a connection to PingContext,
			// so probing through it would report a false database outage. Keep
			// drive9_db_up unchanged and let pool wait/saturation metrics cover
			// this state, even if the database is also unreachable underneath.
			if isDBPoolSaturated(db) {
				result = "pool_saturated"
				observeDBProbeDuration(start, role, result)
				return
			}

			pingCtx, cancel := context.WithTimeout(ctx, timeout)
			err := db.PingContext(pingCtx)
			cancel()
			if err != nil {
				result = "error"
				aggMu.Lock()
				agg := results[role]
				agg.unreachable++
				if agg.firstErr == nil {
					agg.firstErr = err
				}
				aggMu.Unlock()
			}
			observeDBProbeDuration(start, role, result)
		}()
	}
	wg.Wait()

	type transition struct {
		role string
		up   bool
		err  error
	}
	var transitions []transition
	now := time.Now()

	m.mu.Lock()
	prevProbe := m.probe
	nextProbe := make(map[string]dbProbeState, len(results))
	for role, agg := range results {
		up := agg.unreachable == 0
		prev, existed := prevProbe[role]
		nextProbe[role] = dbProbeState{
			up:               up,
			unreachablePools: agg.unreachable,
			totalPools:       agg.total,
			lastProbe:        now,
			known:            true,
		}
		// Fire on the first observed failure, and on every up<->down change
		// thereafter. A first observation that is healthy is not a transition.
		if onChange != nil && ((!existed && !up) || (existed && prev.up != up)) {
			transitions = append(transitions, transition{role: role, up: up, err: agg.firstErr})
		}
	}
	m.probe = nextProbe
	m.mu.Unlock()

	for _, t := range transitions {
		onChange(t.role, t.up, t.err)
	}
}

func writeDBPrometheus(w http.ResponseWriter) {
	globalDB.mu.RLock()
	dbByPool := make(map[*sql.DB]registeredDB, len(globalDB.dbs))
	for db, pool := range globalDB.dbs {
		dbByPool[db] = pool
	}
	globalDB.mu.RUnlock()

	poolTotals := make(map[dbMetricKey]dbPoolTotals)
	for db, pool := range dbByPool {
		key := pool.metricKey()
		stats := db.Stats()
		totals := poolTotals[key]
		totals.registered++
		totals.openConnections += int64(stats.OpenConnections)
		totals.inUseConnections += int64(stats.InUse)
		totals.idleConnections += int64(stats.Idle)
		totals.maxOpenConnections += int64(stats.MaxOpenConnections)
		totals.waitCount += stats.WaitCount
		totals.waitDuration += stats.WaitDuration.Seconds()
		totals.maxIdleClosed += stats.MaxIdleClosed
		totals.maxIdleTimeClosed += stats.MaxIdleTimeClosed
		totals.maxLifetimeClosed += stats.MaxLifetimeClosed
		poolTotals[key] = totals
	}
	keys := sortedDBMetricKeys(poolTotals)

	globalDB.mu.RLock()
	probeByRole := make(map[string]dbProbeState, len(globalDB.probe))
	for role, state := range globalDB.probe {
		probeByRole[role] = state
	}
	globalDB.mu.RUnlock()

	probeRoles := SortedKeys(probeByRole)

	// drive9_db_up is the active availability signal: 1 when every probed pool
	// for the role answered its last health ping, 0 when any pool is unreachable.
	// Unprobed roles/pools do not emit a cold availability series.
	_, _ = fmt.Fprintln(w, "# HELP drive9_db_up Database availability by role (1 = all probed pools reachable on last probe)")
	_, _ = fmt.Fprintln(w, "# TYPE drive9_db_up gauge")
	for _, role := range probeRoles {
		up := 1.0
		if state, ok := probeByRole[role]; ok && state.known && !state.up {
			up = 0.0
		}
		_, _ = fmt.Fprintf(w, "drive9_db_up{role=\"%s\"} %s\n", EscapePromLabel(role), formatFloat(up))
	}

	_, _ = fmt.Fprintln(w, "# HELP drive9_db_unreachable_pools Registered pools that failed their last health probe by role")
	_, _ = fmt.Fprintln(w, "# TYPE drive9_db_unreachable_pools gauge")
	for _, role := range probeRoles {
		unreachable := 0
		if state, ok := probeByRole[role]; ok && state.known {
			unreachable = state.unreachablePools
		}
		_, _ = fmt.Fprintf(w, "drive9_db_unreachable_pools{role=\"%s\"} %d\n", EscapePromLabel(role), unreachable)
	}

	_, _ = fmt.Fprintln(w, "# HELP drive9_db_pool_registered Database pools currently registered for metrics by role")
	_, _ = fmt.Fprintln(w, "# TYPE drive9_db_pool_registered gauge")
	for _, key := range keys {
		_, _ = fmt.Fprintf(w, "drive9_db_pool_registered{%s} %d\n", dbLabels(key), poolTotals[key].registered)
	}

	_, _ = fmt.Fprintln(w, "# HELP drive9_db_pool_connections Aggregated database pool connections by role/state")
	_, _ = fmt.Fprintln(w, "# TYPE drive9_db_pool_connections gauge")
	for _, key := range keys {
		totals := poolTotals[key]
		labels := dbLabels(key)
		_, _ = fmt.Fprintf(w, "drive9_db_pool_connections{%s,state=\"open\"} %d\n", labels, totals.openConnections)
		_, _ = fmt.Fprintf(w, "drive9_db_pool_connections{%s,state=\"in_use\"} %d\n", labels, totals.inUseConnections)
		_, _ = fmt.Fprintf(w, "drive9_db_pool_connections{%s,state=\"idle\"} %d\n", labels, totals.idleConnections)
		_, _ = fmt.Fprintf(w, "drive9_db_pool_connections{%s,state=\"max_open\"} %d\n", labels, totals.maxOpenConnections)
	}

	_, _ = fmt.Fprintln(w, "# HELP drive9_db_pool_wait_count_total Aggregated database pool wait count by role")
	_, _ = fmt.Fprintln(w, "# TYPE drive9_db_pool_wait_count_total counter")
	for _, key := range keys {
		_, _ = fmt.Fprintf(w, "drive9_db_pool_wait_count_total{%s} %d\n", dbLabels(key), poolTotals[key].waitCount)
	}

	_, _ = fmt.Fprintln(w, "# HELP drive9_db_pool_wait_duration_seconds_total Aggregated database pool wait duration by role")
	_, _ = fmt.Fprintln(w, "# TYPE drive9_db_pool_wait_duration_seconds_total counter")
	for _, key := range keys {
		_, _ = fmt.Fprintf(w, "drive9_db_pool_wait_duration_seconds_total{%s} %.6f\n", dbLabels(key), poolTotals[key].waitDuration)
	}

	_, _ = fmt.Fprintln(w, "# HELP drive9_db_pool_closes_total Aggregated database pool closes by role/reason")
	_, _ = fmt.Fprintln(w, "# TYPE drive9_db_pool_closes_total counter")
	for _, key := range keys {
		totals := poolTotals[key]
		labels := dbLabels(key)
		_, _ = fmt.Fprintf(w, "drive9_db_pool_closes_total{%s,reason=\"max_idle\"} %d\n", labels, totals.maxIdleClosed)
		_, _ = fmt.Fprintf(w, "drive9_db_pool_closes_total{%s,reason=\"max_idle_time\"} %d\n", labels, totals.maxIdleTimeClosed)
		_, _ = fmt.Fprintf(w, "drive9_db_pool_closes_total{%s,reason=\"max_lifetime\"} %d\n", labels, totals.maxLifetimeClosed)
	}
}

func (p registeredDB) metricKey() dbMetricKey {
	return dbMetricKey{
		role:     cleanMetricValue(p.role, "unknown"),
		tenantID: cleanMetricValue(p.tenantID, "unknown"),
	}
}

func (p registeredDB) metricRole() string {
	return cleanMetricValue(p.role, "unknown")
}

func (m *dbMetrics) hasPoolLocked(role string) bool {
	for _, pool := range m.dbs {
		if pool.metricRole() == role {
			return true
		}
	}
	return false
}

func (m *dbMetrics) shouldProbePool(pool registeredDB, opts DBHealthProbeOptions) bool {
	return pool.metricRole() == "meta" && opts.ProbeMeta
}

func isDBPoolSaturated(db *sql.DB) bool {
	stats := db.Stats()
	return stats.MaxOpenConnections > 0 && stats.Idle == 0 && stats.InUse >= stats.MaxOpenConnections
}

func observeDBProbeDuration(start time.Time, role, result string) {
	attrs := []Attribute{
		Attr("role", role),
		Attr("result", result),
	}
	dbProbeDuration.Observe(time.Since(start).Seconds(), attrs...)
}

func dbLabels(key dbMetricKey) string {
	labels := fmt.Sprintf("role=\"%s\"", EscapePromLabel(key.role))
	if key.tenantID != "" && key.tenantID != "unknown" {
		labels += fmt.Sprintf(",tenant_id=\"%s\"", EscapePromLabel(key.tenantID))
	}
	return labels
}

func sortedDBMetricKeys(totals map[dbMetricKey]dbPoolTotals) []dbMetricKey {
	keys := make([]dbMetricKey, 0, len(totals))
	for key := range totals {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].role != keys[j].role {
			return keys[i].role < keys[j].role
		}
		return keys[i].tenantID < keys[j].tenantID
	})
	return keys
}

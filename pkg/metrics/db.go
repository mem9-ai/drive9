package metrics

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
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

type dbMetrics struct {
	mu      sync.RWMutex
	dbs     map[*sql.DB]string
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
	dbs:   map[*sql.DB]string{},
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
	if db == nil {
		return
	}
	RegisterModule("db")
	globalDB.mu.Lock()
	globalDB.dbs[db] = role
	globalDB.mu.Unlock()
}

func UnregisterDB(db *sql.DB) {
	if db == nil {
		return
	}
	globalDB.mu.Lock()
	delete(globalDB.dbs, db)
	globalDB.mu.Unlock()
}

// StartDBHealthProbe launches a background goroutine that periodically pings every
// registered *sql.DB and publishes a per-role availability signal as the
// drive9_db_up gauge. It is the active dependency-health probe for the metadata
// store ("meta") and tenant data stores ("user"): pool/operation metrics only move
// while there is live traffic, so an idle-but-unreachable database is otherwise
// invisible.
//
// The probe never blocks the /metrics scrape path — scrapes read the cached probe
// state. onChange (optional) is invoked outside the lock on a role's first observed
// failure and on every subsequent up<->down transition, so callers can emit a log
// without coupling this package to a logger. The goroutine stops when ctx is done;
// repeat calls after the first are no-ops.
//
// Example alert (critical — control-plane DB unreachable):
//
//	max(drive9_db_up{role="meta"}) == 0
func StartDBHealthProbe(ctx context.Context, interval, timeout time.Duration, onChange func(role string, up bool, err error)) {
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
		globalDB.probeOnce(ctx, timeout, onChange)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				globalDB.probeOnce(ctx, timeout, onChange)
			}
		}
	}()
}

// probeOnce runs a single probe cycle across all registered pools and updates the
// cached per-role state. Exported indirectly for tests via probeOnce.
func (m *dbMetrics) probeOnce(ctx context.Context, timeout time.Duration, onChange func(role string, up bool, err error)) {
	m.mu.RLock()
	dbByRole := make(map[*sql.DB]string, len(m.dbs))
	for db, role := range m.dbs {
		dbByRole[db] = role
	}
	m.mu.RUnlock()

	type roleAgg struct {
		total       int
		unreachable int
		firstErr    error
	}
	results := map[string]*roleAgg{}
	for _, role := range dbByRole {
		if results[role] == nil {
			results[role] = &roleAgg{}
		}
		results[role].total++
	}

	// Ping pools concurrently with a fixed bound so one slow/unreachable pool
	// cannot serialize the whole cycle: with many unreachable tenant ("user")
	// pools a serial loop would burn timeout*N and leave drive9_db_up stale.
	const probeConcurrency = 8
	sem := make(chan struct{}, probeConcurrency)
	var wg sync.WaitGroup
	var aggMu sync.Mutex
	for db, role := range dbByRole {
		db, role := db, role
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			pingCtx, cancel := context.WithTimeout(ctx, timeout)
			start := time.Now()
			err := db.PingContext(pingCtx)
			cancel()

			result := "ok"
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
			dbProbeDuration.Observe(time.Since(start).Seconds(), Attr("role", cleanMetricValue(role, "unknown")), Attr("result", result))
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
	if m.probe == nil {
		m.probe = map[string]dbProbeState{}
	}
	for role, agg := range results {
		up := agg.unreachable == 0
		prev, existed := m.probe[role]
		m.probe[role] = dbProbeState{
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
	m.mu.Unlock()

	for _, t := range transitions {
		onChange(t.role, t.up, t.err)
	}
}

func writeDBPrometheus(w http.ResponseWriter) {
	globalDB.mu.RLock()
	dbByRole := make(map[*sql.DB]string, len(globalDB.dbs))
	for db, role := range globalDB.dbs {
		dbByRole[db] = role
	}
	globalDB.mu.RUnlock()

	poolTotals := make(map[string]dbPoolTotals)
	for db, role := range dbByRole {
		stats := db.Stats()
		totals := poolTotals[role]
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
		poolTotals[role] = totals
	}
	roles := SortedKeys(poolTotals)

	globalDB.mu.RLock()
	probeByRole := make(map[string]dbProbeState, len(globalDB.probe))
	for role, state := range globalDB.probe {
		probeByRole[role] = state
	}
	globalDB.mu.RUnlock()

	// drive9_db_up is the active availability signal: 1 when every registered pool
	// for the role answered its last health ping, 0 when any pool is unreachable.
	// Roles not yet probed default to 1 (the startup ping already succeeded), so the
	// `== 0` alert never fires on a cold series.
	_, _ = fmt.Fprintln(w, "# HELP drive9_db_up Database availability by role (1 = all registered pools reachable on last probe)")
	_, _ = fmt.Fprintln(w, "# TYPE drive9_db_up gauge")
	for _, role := range roles {
		up := 1.0
		if state, ok := probeByRole[role]; ok && state.known && !state.up {
			up = 0.0
		}
		_, _ = fmt.Fprintf(w, "drive9_db_up{role=\"%s\"} %s\n", EscapePromLabel(role), formatFloat(up))
	}

	_, _ = fmt.Fprintln(w, "# HELP drive9_db_unreachable_pools Registered pools that failed their last health probe by role")
	_, _ = fmt.Fprintln(w, "# TYPE drive9_db_unreachable_pools gauge")
	for _, role := range roles {
		unreachable := 0
		if state, ok := probeByRole[role]; ok && state.known {
			unreachable = state.unreachablePools
		}
		_, _ = fmt.Fprintf(w, "drive9_db_unreachable_pools{role=\"%s\"} %d\n", EscapePromLabel(role), unreachable)
	}

	_, _ = fmt.Fprintln(w, "# HELP drive9_db_pool_registered Database pools currently registered for metrics by role")
	_, _ = fmt.Fprintln(w, "# TYPE drive9_db_pool_registered gauge")
	for _, role := range roles {
		_, _ = fmt.Fprintf(w, "drive9_db_pool_registered{role=\"%s\"} %d\n", EscapePromLabel(role), poolTotals[role].registered)
	}

	_, _ = fmt.Fprintln(w, "# HELP drive9_db_pool_connections Aggregated database pool connections by role/state")
	_, _ = fmt.Fprintln(w, "# TYPE drive9_db_pool_connections gauge")
	for _, role := range roles {
		totals := poolTotals[role]
		escapedRole := EscapePromLabel(role)
		_, _ = fmt.Fprintf(w, "drive9_db_pool_connections{role=\"%s\",state=\"open\"} %d\n", escapedRole, totals.openConnections)
		_, _ = fmt.Fprintf(w, "drive9_db_pool_connections{role=\"%s\",state=\"in_use\"} %d\n", escapedRole, totals.inUseConnections)
		_, _ = fmt.Fprintf(w, "drive9_db_pool_connections{role=\"%s\",state=\"idle\"} %d\n", escapedRole, totals.idleConnections)
		_, _ = fmt.Fprintf(w, "drive9_db_pool_connections{role=\"%s\",state=\"max_open\"} %d\n", escapedRole, totals.maxOpenConnections)
	}

	_, _ = fmt.Fprintln(w, "# HELP drive9_db_pool_wait_count_total Aggregated database pool wait count by role")
	_, _ = fmt.Fprintln(w, "# TYPE drive9_db_pool_wait_count_total counter")
	for _, role := range roles {
		_, _ = fmt.Fprintf(w, "drive9_db_pool_wait_count_total{role=\"%s\"} %d\n", EscapePromLabel(role), poolTotals[role].waitCount)
	}

	_, _ = fmt.Fprintln(w, "# HELP drive9_db_pool_wait_duration_seconds_total Aggregated database pool wait duration by role")
	_, _ = fmt.Fprintln(w, "# TYPE drive9_db_pool_wait_duration_seconds_total counter")
	for _, role := range roles {
		_, _ = fmt.Fprintf(w, "drive9_db_pool_wait_duration_seconds_total{role=\"%s\"} %.6f\n", EscapePromLabel(role), poolTotals[role].waitDuration)
	}

	_, _ = fmt.Fprintln(w, "# HELP drive9_db_pool_closes_total Aggregated database pool closes by role/reason")
	_, _ = fmt.Fprintln(w, "# TYPE drive9_db_pool_closes_total counter")
	for _, role := range roles {
		totals := poolTotals[role]
		escapedRole := EscapePromLabel(role)
		_, _ = fmt.Fprintf(w, "drive9_db_pool_closes_total{role=\"%s\",reason=\"max_idle\"} %d\n", escapedRole, totals.maxIdleClosed)
		_, _ = fmt.Fprintf(w, "drive9_db_pool_closes_total{role=\"%s\",reason=\"max_idle_time\"} %d\n", escapedRole, totals.maxIdleTimeClosed)
		_, _ = fmt.Fprintf(w, "drive9_db_pool_closes_total{role=\"%s\",reason=\"max_lifetime\"} %d\n", escapedRole, totals.maxLifetimeClosed)
	}
}

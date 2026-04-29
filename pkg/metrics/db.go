package metrics

import (
	"database/sql"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type dbMetrics struct {
	mu             sync.RWMutex
	counts         map[string]int64
	durationCount  map[string]int64
	durationSum    map[string]float64
	durationBucket map[string][]int64
	dbs            map[*sql.DB]string
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

var globalDB = &dbMetrics{
	counts:         map[string]int64{},
	durationCount:  map[string]int64{},
	durationSum:    map[string]float64{},
	durationBucket: map[string][]int64{},
	dbs:            map[*sql.DB]string{},
}

func RecordDBOperation(role, operation, result, tenantID string, d time.Duration) {
	key := dbLabels(role, operation, result, tenantID)
	globalDB.mu.Lock()
	globalDB.counts[key]++
	globalDB.durationCount[key]++
	globalDB.durationSum[key] += d.Seconds()
	buckets := globalDB.durationBucket[key]
	if buckets == nil {
		buckets = make([]int64, len(dbOperationDurationBounds))
		globalDB.durationBucket[key] = buckets
	}
	seconds := d.Seconds()
	for i, bound := range dbOperationDurationBounds {
		if seconds <= bound {
			buckets[i]++
		}
	}
	globalDB.mu.Unlock()
}

func RegisterDB(role string, db *sql.DB) {
	if db == nil {
		return
	}
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

func writeDBPrometheus(w http.ResponseWriter) {
	globalDB.mu.RLock()
	countKeys := SortedKeys(globalDB.counts)
	counts := CloneIntMap(globalDB.counts)
	durationCount := CloneIntMap(globalDB.durationCount)
	durationSum := CloneFloatMap(globalDB.durationSum)
	durationBucket := CloneBucketMap(globalDB.durationBucket)
	dbByRole := make(map[*sql.DB]string, len(globalDB.dbs))
	for db, role := range globalDB.dbs {
		dbByRole[db] = role
	}
	globalDB.mu.RUnlock()

	_, _ = fmt.Fprintln(w, "# HELP dat9_db_operations_total Database operations by role/operation/result/tenant_id")
	_, _ = fmt.Fprintln(w, "# TYPE dat9_db_operations_total counter")
	for _, k := range countKeys {
		_, _ = fmt.Fprintf(w, "dat9_db_operations_total{%s} %d\n", k, counts[k])
	}

	_, _ = fmt.Fprintln(w, "# HELP dat9_db_operation_duration_seconds Database operation duration histogram")
	_, _ = fmt.Fprintln(w, "# TYPE dat9_db_operation_duration_seconds histogram")
	for _, k := range countKeys {
		buckets := durationBucket[k]
		for i, bound := range dbOperationDurationBounds {
			_, _ = fmt.Fprintf(w, "dat9_db_operation_duration_seconds_bucket{%s,le=\"%s\"} %d\n", k, FormatPromBound(bound), buckets[i])
		}
		_, _ = fmt.Fprintf(w, "dat9_db_operation_duration_seconds_bucket{%s,le=\"+Inf\"} %d\n", k, durationCount[k])
	}

	_, _ = fmt.Fprintln(w, "# HELP dat9_db_operation_duration_seconds_count Database operation duration count")
	_, _ = fmt.Fprintln(w, "# TYPE dat9_db_operation_duration_seconds_count counter")
	for _, k := range countKeys {
		_, _ = fmt.Fprintf(w, "dat9_db_operation_duration_seconds_count{%s} %d\n", k, durationCount[k])
	}

	_, _ = fmt.Fprintln(w, "# HELP dat9_db_operation_duration_seconds_sum Database operation duration sum in seconds")
	_, _ = fmt.Fprintln(w, "# TYPE dat9_db_operation_duration_seconds_sum counter")
	for _, k := range countKeys {
		_, _ = fmt.Fprintf(w, "dat9_db_operation_duration_seconds_sum{%s} %.6f\n", k, durationSum[k])
	}

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

	_, _ = fmt.Fprintln(w, "# HELP dat9_db_pool_registered Database pools currently registered for metrics by role")
	_, _ = fmt.Fprintln(w, "# TYPE dat9_db_pool_registered gauge")
	for _, role := range roles {
		_, _ = fmt.Fprintf(w, "dat9_db_pool_registered{role=\"%s\"} %d\n", EscapePromLabel(role), poolTotals[role].registered)
	}

	_, _ = fmt.Fprintln(w, "# HELP dat9_db_pool_connections Aggregated database pool connections by role/state")
	_, _ = fmt.Fprintln(w, "# TYPE dat9_db_pool_connections gauge")
	for _, role := range roles {
		totals := poolTotals[role]
		escapedRole := EscapePromLabel(role)
		_, _ = fmt.Fprintf(w, "dat9_db_pool_connections{role=\"%s\",state=\"open\"} %d\n", escapedRole, totals.openConnections)
		_, _ = fmt.Fprintf(w, "dat9_db_pool_connections{role=\"%s\",state=\"in_use\"} %d\n", escapedRole, totals.inUseConnections)
		_, _ = fmt.Fprintf(w, "dat9_db_pool_connections{role=\"%s\",state=\"idle\"} %d\n", escapedRole, totals.idleConnections)
		_, _ = fmt.Fprintf(w, "dat9_db_pool_connections{role=\"%s\",state=\"max_open\"} %d\n", escapedRole, totals.maxOpenConnections)
	}

	_, _ = fmt.Fprintln(w, "# HELP dat9_db_pool_wait_count_total Aggregated database pool wait count by role")
	_, _ = fmt.Fprintln(w, "# TYPE dat9_db_pool_wait_count_total counter")
	for _, role := range roles {
		_, _ = fmt.Fprintf(w, "dat9_db_pool_wait_count_total{role=\"%s\"} %d\n", EscapePromLabel(role), poolTotals[role].waitCount)
	}

	_, _ = fmt.Fprintln(w, "# HELP dat9_db_pool_wait_duration_seconds_total Aggregated database pool wait duration by role")
	_, _ = fmt.Fprintln(w, "# TYPE dat9_db_pool_wait_duration_seconds_total counter")
	for _, role := range roles {
		_, _ = fmt.Fprintf(w, "dat9_db_pool_wait_duration_seconds_total{role=\"%s\"} %.6f\n", EscapePromLabel(role), poolTotals[role].waitDuration)
	}

	_, _ = fmt.Fprintln(w, "# HELP dat9_db_pool_closes_total Aggregated database pool closes by role/reason")
	_, _ = fmt.Fprintln(w, "# TYPE dat9_db_pool_closes_total counter")
	for _, role := range roles {
		totals := poolTotals[role]
		escapedRole := EscapePromLabel(role)
		_, _ = fmt.Fprintf(w, "dat9_db_pool_closes_total{role=\"%s\",reason=\"max_idle\"} %d\n", escapedRole, totals.maxIdleClosed)
		_, _ = fmt.Fprintf(w, "dat9_db_pool_closes_total{role=\"%s\",reason=\"max_idle_time\"} %d\n", escapedRole, totals.maxIdleTimeClosed)
		_, _ = fmt.Fprintf(w, "dat9_db_pool_closes_total{role=\"%s\",reason=\"max_lifetime\"} %d\n", escapedRole, totals.maxLifetimeClosed)
	}
}

func dbLabels(role, operation, result, tenantID string) string {
	return "role=\"" + EscapePromLabel(role) + "\",operation=\"" + EscapePromLabel(operation) + "\",result=\"" + EscapePromLabel(result) + "\",tenant_id=\"" + EscapePromLabel(tenantID) + "\""
}

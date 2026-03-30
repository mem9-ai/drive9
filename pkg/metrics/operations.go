// Package metrics provides process-wide service operation metrics.
package metrics

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

type opMetrics struct {
	mu             sync.RWMutex
	counts         map[string]int64
	durationCount  map[string]int64
	durationSum    map[string]float64
	durationBucket map[string][]int64
	gauges         map[string]float64
}

var operationDurationBounds = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}

var globalOps = &opMetrics{
	counts:         map[string]int64{},
	durationCount:  map[string]int64{},
	durationSum:    map[string]float64{},
	durationBucket: map[string][]int64{},
	gauges:         map[string]float64{},
}

func RecordOperation(component, operation, result string, d time.Duration) {
	key := labels(component, operation, result)
	globalOps.mu.Lock()
	globalOps.counts[key]++
	globalOps.durationCount[key]++
	globalOps.durationSum[key] += d.Seconds()
	buckets := globalOps.durationBucket[key]
	if buckets == nil {
		buckets = make([]int64, len(operationDurationBounds))
		globalOps.durationBucket[key] = buckets
	}
	seconds := d.Seconds()
	for i, bound := range operationDurationBounds {
		if seconds <= bound {
			buckets[i]++
		}
	}
	globalOps.mu.Unlock()
}

func RecordGauge(component, name string, value float64) {
	key := gaugeLabels(component, name)
	globalOps.mu.Lock()
	globalOps.gauges[key] = value
	globalOps.mu.Unlock()
}

func WritePrometheus(w http.ResponseWriter) {
	globalOps.mu.RLock()
	countKeys := SortedKeys(globalOps.counts)
	counts := CloneIntMap(globalOps.counts)
	durationCount := CloneIntMap(globalOps.durationCount)
	durationSum := CloneFloatMap(globalOps.durationSum)
	durationBucket := CloneBucketMap(globalOps.durationBucket)
	gaugeKeys := SortedKeys(globalOps.gauges)
	gauges := CloneFloatMap(globalOps.gauges)
	globalOps.mu.RUnlock()

	_, _ = fmt.Fprintln(w, "# HELP dat9_service_operations_total Service operations by component/operation/result")
	_, _ = fmt.Fprintln(w, "# TYPE dat9_service_operations_total counter")
	for _, k := range countKeys {
		_, _ = fmt.Fprintf(w, "dat9_service_operations_total{%s} %d\n", k, counts[k])
	}

	_, _ = fmt.Fprintln(w, "# HELP dat9_service_operation_duration_seconds Service operation duration histogram")
	_, _ = fmt.Fprintln(w, "# TYPE dat9_service_operation_duration_seconds histogram")
	for _, k := range countKeys {
		buckets := durationBucket[k]
		for i, bound := range operationDurationBounds {
			_, _ = fmt.Fprintf(w, "dat9_service_operation_duration_seconds_bucket{%s,le=\"%s\"} %d\n", k, FormatPromBound(bound), buckets[i])
		}
		_, _ = fmt.Fprintf(w, "dat9_service_operation_duration_seconds_bucket{%s,le=\"+Inf\"} %d\n", k, durationCount[k])
	}

	_, _ = fmt.Fprintln(w, "# HELP dat9_service_operation_duration_seconds_count Service operation duration count")
	_, _ = fmt.Fprintln(w, "# TYPE dat9_service_operation_duration_seconds_count counter")
	for _, k := range countKeys {
		_, _ = fmt.Fprintf(w, "dat9_service_operation_duration_seconds_count{%s} %d\n", k, durationCount[k])
	}

	_, _ = fmt.Fprintln(w, "# HELP dat9_service_operation_duration_seconds_sum Service operation duration sum in seconds")
	_, _ = fmt.Fprintln(w, "# TYPE dat9_service_operation_duration_seconds_sum counter")
	for _, k := range countKeys {
		_, _ = fmt.Fprintf(w, "dat9_service_operation_duration_seconds_sum{%s} %.6f\n", k, durationSum[k])
	}

	_, _ = fmt.Fprintln(w, "# HELP dat9_service_gauge Service gauges by component/name")
	_, _ = fmt.Fprintln(w, "# TYPE dat9_service_gauge gauge")
	for _, k := range gaugeKeys {
		_, _ = fmt.Fprintf(w, "dat9_service_gauge{%s} %.6f\n", k, gauges[k])
	}
}

func labels(component, operation, result string) string {
	return "component=\"" + EscapePromLabel(component) + "\",operation=\"" + EscapePromLabel(operation) + "\",result=\"" + EscapePromLabel(result) + "\""
}

func gaugeLabels(component, name string) string {
	return "component=\"" + EscapePromLabel(component) + "\",name=\"" + EscapePromLabel(name) + "\""
}

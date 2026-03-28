// Package metrics provides process-wide service operation metrics.
package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

type opMetrics struct {
	mu            sync.RWMutex
	counts        map[string]int64
	durationCount map[string]int64
	durationSum   map[string]float64
}

var globalOps = &opMetrics{
	counts:        map[string]int64{},
	durationCount: map[string]int64{},
	durationSum:   map[string]float64{},
}

func RecordOperation(component, operation, result string, d time.Duration) {
	key := labels(component, operation, result)
	globalOps.mu.Lock()
	globalOps.counts[key]++
	globalOps.durationCount[key]++
	globalOps.durationSum[key] += d.Seconds()
	globalOps.mu.Unlock()
}

func WritePrometheus(w http.ResponseWriter) {
	globalOps.mu.RLock()
	countKeys := sorted(globalOps.counts)
	counts := cloneInt(globalOps.counts)
	durationCount := cloneInt(globalOps.durationCount)
	durationSum := cloneFloat(globalOps.durationSum)
	globalOps.mu.RUnlock()

	_, _ = fmt.Fprintln(w, "# HELP dat9_service_operations_total Service operations by component/operation/result")
	_, _ = fmt.Fprintln(w, "# TYPE dat9_service_operations_total counter")
	for _, k := range countKeys {
		_, _ = fmt.Fprintf(w, "dat9_service_operations_total{%s} %d\n", k, counts[k])
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
}

func labels(component, operation, result string) string {
	return "component=\"" + esc(component) + "\",operation=\"" + esc(operation) + "\",result=\"" + esc(result) + "\""
}

func esc(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, `"`, `\\"`)
	s = strings.ReplaceAll(s, "\n", "\\n")
	return s
}

func sorted[T any](m map[string]T) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func cloneInt(m map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneFloat(m map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

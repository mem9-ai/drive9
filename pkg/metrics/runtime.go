package metrics

import (
	"fmt"
	"io"
	"runtime"
	"runtime/pprof"
)

// writeRuntimeMetrics exports Go runtime saturation/health signals using the
// conventional go_* metric names, so the standard Go runtime dashboards
// (Grafana 6671 / 10826) and the goroutine-leak / GC-pressure alerts work out of
// the box. The custom drive9 registry does not pull in client_golang's default
// collectors, so without this the process exports no go_goroutines, heap, thread,
// or GC metrics at all — the "Saturation" golden signal had no runtime coverage.
//
// Read on the scrape path (like client_golang). ReadMemStats briefly stops the
// world; at a 15-30s scrape interval the cost is negligible.
func writeRuntimeMetrics(w io.Writer) {
	if w == nil {
		return
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	writeRuntimeGauge(w, "go_goroutines", "Number of goroutines that currently exist", float64(runtime.NumGoroutine()))
	writeRuntimeGauge(w, "go_threads", "Number of OS threads created", float64(pprof.Lookup("threadcreate").Count()))
	writeRuntimeGauge(w, "go_memstats_alloc_bytes", "Number of bytes allocated and still in use", float64(ms.Alloc))
	writeRuntimeGauge(w, "go_memstats_heap_inuse_bytes", "Number of heap bytes that are in use", float64(ms.HeapInuse))
	writeRuntimeGauge(w, "go_memstats_heap_sys_bytes", "Number of heap bytes obtained from system", float64(ms.HeapSys))
	writeRuntimeGauge(w, "go_memstats_sys_bytes", "Number of bytes obtained from system", float64(ms.Sys))
	writeRuntimeGauge(w, "go_memstats_next_gc_bytes", "Number of heap bytes when next GC will take place", float64(ms.NextGC))

	writeRuntimeCounter(w, "go_gc_runs_total", "Total number of completed GC cycles", float64(ms.NumGC))
	writeRuntimeCounter(w, "go_gc_pause_seconds_total", "Cumulative stop-the-world GC pause time in seconds", float64(ms.PauseTotalNs)/1e9)
}

func writeRuntimeGauge(w io.Writer, name, help string, value float64) {
	_, _ = fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	_, _ = fmt.Fprintf(w, "# TYPE %s gauge\n", name)
	_, _ = fmt.Fprintf(w, "%s %s\n", name, formatFloat(value))
}

func writeRuntimeCounter(w io.Writer, name, help string, value float64) {
	_, _ = fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	_, _ = fmt.Fprintf(w, "# TYPE %s counter\n", name)
	_, _ = fmt.Fprintf(w, "%s %s\n", name, formatFloat(value))
}

package metrics

import (
	"io"
	"os"
	"strconv"
	"strings"
)

// userHZ is the Linux clock-tick frequency assumed for converting /proc CPU
// counters to seconds. It is 100 on effectively all Linux kernels drive9 runs on
// (the value sysconf(_SC_CLK_TCK) returns); reading it precisely needs cgo, which
// this pure-Go collector avoids.
const userHZ = 100.0

// writeProcessMetrics exports OS process performance/saturation signals using the
// conventional process_* metric names (CPU, resident/virtual memory, and file
// descriptors), matching client_golang's process collector so standard dashboards
// and the FD-exhaustion / CPU-saturation alerts work. Without this the process
// exported no CPU or file-descriptor metrics at all.
//
// Linux-only: it reads /proc/self. On other platforms (local dev on macOS) /proc
// is absent and the function emits nothing rather than failing.
func writeProcessMetrics(w io.Writer) {
	if w == nil {
		return
	}
	stat, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return // not Linux / no procfs: skip process metrics
	}
	ps, ok := parseProcStat(string(stat))
	if !ok {
		return
	}

	writeRuntimeCounter(w, "process_cpu_seconds_total", "Total user and system CPU time spent in seconds", ps.cpuSeconds)
	writeRuntimeGauge(w, "process_resident_memory_bytes", "Resident memory size in bytes", ps.residentBytes)
	writeRuntimeGauge(w, "process_virtual_memory_bytes", "Virtual memory size in bytes", ps.virtualBytes)

	if openFDs, ok := openFileDescriptors(); ok {
		writeRuntimeGauge(w, "process_open_fds", "Number of open file descriptors", float64(openFDs))
	}
	if maxFDs, ok := maxFileDescriptors(); ok {
		writeRuntimeGauge(w, "process_max_fds", "Maximum number of open file descriptors", float64(maxFDs))
	}
}

type procStat struct {
	cpuSeconds    float64
	residentBytes float64
	virtualBytes  float64
}

// parseProcStat extracts CPU and memory fields from a /proc/<pid>/stat line.
// The 2nd field (comm) may contain spaces and parentheses, so fields are taken
// from after the final ')'. Field numbers are 1-based per proc(5):
// utime=14, stime=15, vsize=23, rss=24 (in pages).
func parseProcStat(data string) (procStat, bool) {
	rparen := strings.LastIndex(data, ")")
	if rparen < 0 || rparen+2 >= len(data) {
		return procStat{}, false
	}
	fields := strings.Fields(data[rparen+2:])
	// After comm, index 0 == field 3 (state); field N -> index N-3.
	const (
		idxUtime = 14 - 3
		idxStime = 15 - 3
		idxVsize = 23 - 3
		idxRss   = 24 - 3
	)
	if len(fields) <= idxRss {
		return procStat{}, false
	}
	utime, err1 := strconv.ParseFloat(fields[idxUtime], 64)
	stime, err2 := strconv.ParseFloat(fields[idxStime], 64)
	vsize, err3 := strconv.ParseFloat(fields[idxVsize], 64)
	rssPages, err4 := strconv.ParseFloat(fields[idxRss], 64)
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
		return procStat{}, false
	}
	return procStat{
		cpuSeconds:    (utime + stime) / userHZ,
		residentBytes: rssPages * float64(os.Getpagesize()),
		virtualBytes:  vsize,
	}, true
}

func openFileDescriptors() (int, bool) {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0, false
	}
	// Subtract 1 for the fd opened by ReadDir itself (best-effort, matches
	// client_golang's behaviour closely enough for saturation alerting).
	n := len(entries)
	if n > 0 {
		n--
	}
	return n, true
}

// maxFileDescriptors is implemented per-platform (process_unix.go / process_other.go)
// because the RLIMIT_NOFILE syscall constants do not exist on Windows.

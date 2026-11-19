package membudget

import (
	"os"
	"strconv"
	"strings"
)

// autoLimitFraction is the share of the container memory limit a mount may
// budget for its own caches and buffers. The remainder is headroom for the Go
// runtime, the kernel FUSE queues and the rest of the process.
const autoLimitFraction = 0.6

// cgroupLimitFiles are the container memory-limit files, most specific first:
// cgroup v2 then v1. A container with several limits uses the tightest one it
// can find; the v2 hierarchy is authoritative when both exist.
var cgroupLimitFiles = []string{
	"/sys/fs/cgroup/memory.max",                   // cgroup v2
	"/sys/fs/cgroup/memory/memory.limit_in_bytes", // cgroup v1
}

// readLimitFile is a test seam.
var readLimitFile = os.ReadFile

// AutoLimit derives the memory budget a mount should use when the operator did
// not configure one: autoLimitFraction of the container's memory limit. It
// returns 0 (budget disabled) when the process is not cgroup-limited, which is
// the case on a bare host — there the operator has to state a limit explicitly
// rather than inherit an arbitrary default.
//
// The returned source names the file the limit came from, for the mount log.
func AutoLimit() (limit int64, source string) {
	for _, path := range cgroupLimitFiles {
		raw, err := readLimitFile(path)
		if err != nil {
			continue
		}
		value, ok := parseCgroupLimit(string(raw))
		if !ok {
			continue
		}
		budget := int64(float64(value) * autoLimitFraction)
		if budget <= 0 {
			continue
		}
		return budget, path
	}
	return 0, ""
}

// parseCgroupLimit reads one cgroup limit value. ok is false for the "max"
// marker (v2), the v1 "unlimited" sentinels (PAGE_COUNTER_MAX and the
// 0x7FFFFFFFFFFFF000-style ceiling), and anything unparseable.
func parseCgroupLimit(raw string) (int64, bool) {
	text := strings.TrimSpace(raw)
	if text == "" || strings.EqualFold(text, "max") {
		return 0, false
	}
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil || value <= 0 {
		return 0, false
	}
	// cgroup v1 reports "no limit" as a huge page-aligned number (on 64-bit:
	// 9223372036854771712). Anything near the int64 ceiling is unlimited.
	const unlimitedFloor = int64(1) << 60
	if value >= unlimitedFloor {
		return 0, false
	}
	return value, true
}

//go:build !windows

package fuse

import (
	"runtime"
	"syscall"
	"time"
)

func readContinuousPerfProcessStats() continuousPerfProcessStats {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return continuousPerfProcessStats{}
	}
	return continuousPerfProcessStats{
		UserCPUNS:   timevalNS(ru.Utime),
		SystemCPUNS: timevalNS(ru.Stime),
		MaxRSSBytes: normalizeMaxRSSBytes(ru.Maxrss),
	}
}

func timevalNS(tv syscall.Timeval) uint64 {
	return uint64(tv.Sec)*uint64(time.Second) + uint64(tv.Usec)*uint64(time.Microsecond)
}

func normalizeMaxRSSBytes(maxRSS int64) int64 {
	if runtime.GOOS == "linux" {
		return maxRSS * 1024
	}
	return maxRSS
}

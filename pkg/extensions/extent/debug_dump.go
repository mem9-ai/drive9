package extent

// Debug-only instrumentation for client-side wedge diagnosis. When
// DRIVE9_DEBUG_STACK_DUMP is set to a file path, a goroutine appends the
// runtime's goroutine stacks plus a summary of the bridge's commit state
// (pinned budget, per-inode pending writes, sticky commit errors) to that
// file every DRIVE9_DEBUG_STACK_INTERVAL (default 30s). The dump needs no
// fork and no logger, so it keeps producing evidence on a box whose
// userspace is too starved to even run `ps` or serve a pprof HTTP request.

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	debugBridge atomic.Pointer[Bridge]
	debugOnce   sync.Once
)

func debugRegisterBridge(b *Bridge) {
	debugBridge.Store(b)
	debugOnce.Do(startDebugStackDump)
}

func startDebugStackDump() {
	path := os.Getenv("DRIVE9_DEBUG_STACK_DUMP")
	if path == "" {
		return
	}
	interval := 30 * time.Second
	if v := os.Getenv("DRIVE9_DEBUG_STACK_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			interval = d
		}
	}
	go func() {
		buf := make([]byte, 4<<20)
		for {
			f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
			if err != nil {
				time.Sleep(interval)
				continue
			}
			n := runtime.Stack(buf, true)
			fmt.Fprintf(f, "==== stacks %s ====\n", time.Now().UTC().Format(time.RFC3339))
			fmt.Fprintf(f, "---- proc ----\n%s%s", debugProcSnapshot(), debugSelfStatus())
			if b := debugBridge.Load(); b != nil {
				fmt.Fprintf(f, "---- bridge state ----\n%s", b.debugState())
			}
			_, _ = f.Write(buf[:n])
			_ = f.Close()
			time.Sleep(interval)
		}
	}()
}

// debugProcSnapshot captures the memory lines a wedge diagnosis needs; on
// a starved box nothing else may ever get to read /proc/meminfo.
func debugProcSnapshot() string {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return fmt.Sprintf("meminfo: %v\n", err)
	}
	var out string
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "MemTotal") || strings.HasPrefix(line, "MemFree") ||
			strings.HasPrefix(line, "MemAvailable") || strings.HasPrefix(line, "Dirty") ||
			strings.HasPrefix(line, "Writeback") || strings.HasPrefix(line, "AnonPages") ||
			strings.HasPrefix(line, "SUnreclaim") || strings.HasPrefix(line, "Slab") {
			out += line + "\n"
		}
	}
	return out
}

func debugSelfStatus() string {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return fmt.Sprintf("status: %v\n", err)
	}
	var out string
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmRSS") || strings.HasPrefix(line, "VmSwap") ||
			strings.HasPrefix(line, "Threads") || strings.HasPrefix(line, "State") {
			out += line + "\n"
		}
	}
	return out
}

// debugState summarizes the commit machinery under b.mu. The pinned
// budget is read outside b.mu (the registry lock is a leaf lock).
func (b *Bridge) debugState() string {
	pinned := b.blocks.pinnedBudget()
	b.mu.Lock()
	defer b.mu.Unlock()
	out := fmt.Sprintf("pinned_bytes=%d staged_slices=%d pending_inos=%d commitErr_inos=%d\n",
		pinned, len(b.stagedSlices), len(b.pending), len(b.commitErr))
	for ino, p := range b.pending {
		var bytes int64
		for _, w := range p {
			bytes += w.size
		}
		out += fmt.Sprintf("  ino %d: pending_writes=%d pending_bytes=%d\n", ino, len(p), bytes)
	}
	for ino, err := range b.commitErr {
		if err != nil {
			out += fmt.Sprintf("  ino %d: commitErr=%v\n", ino, err)
		}
	}
	return out
}

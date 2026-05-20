package fuse

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sync"
	"syscall"
	"time"
)

// ContinuousPerfRecorder writes low-overhead mount performance samples as JSONL.
type ContinuousPerfRecorder struct {
	opts ProfilingOptions
	fs   *Dat9FS

	mu      sync.Mutex
	file    *os.File
	encoder *json.Encoder
	stopCh  chan struct{}
	doneCh  chan struct{}
	once    sync.Once
}

type continuousPerfSample struct {
	Timestamp string                         `json:"timestamp"`
	Reason    string                         `json:"reason"`
	UptimeMS  int64                          `json:"uptime_ms"`
	Runtime   continuousPerfRuntimeStats     `json:"runtime"`
	Process   continuousPerfProcessStats     `json:"process"`
	FuseOps   map[string]continuousPerfStats `json:"fuse_ops,omitempty"`
	RemoteOps map[string]continuousPerfStats `json:"remote_ops,omitempty"`
	Counters  map[string]uint64              `json:"counters,omitempty"`
	Queues    continuousPerfQueueStats       `json:"queues"`
}

type continuousPerfStats struct {
	Count   uint64 `json:"count"`
	Errors  uint64 `json:"errors"`
	Bytes   uint64 `json:"bytes"`
	TotalNS uint64 `json:"total_ns"`
	AvgNS   uint64 `json:"avg_ns"`
}

type continuousPerfRuntimeStats struct {
	Goroutines      int    `json:"goroutines"`
	HeapAllocBytes  uint64 `json:"heap_alloc_bytes"`
	HeapInuseBytes  uint64 `json:"heap_inuse_bytes"`
	HeapObjects     uint64 `json:"heap_objects"`
	StackInuseBytes uint64 `json:"stack_inuse_bytes"`
	SysBytes        uint64 `json:"sys_bytes"`
	NextGCBytes     uint64 `json:"next_gc_bytes"`
	NumGC           uint32 `json:"num_gc"`
	PauseTotalNS    uint64 `json:"pause_total_ns"`
}

type continuousPerfProcessStats struct {
	UserCPUNS   uint64 `json:"user_cpu_ns"`
	SystemCPUNS uint64 `json:"system_cpu_ns"`
	MaxRSSBytes int64  `json:"max_rss_bytes"`
}

type continuousPerfQueueStats struct {
	CommitPending        int   `json:"commit_pending"`
	CommitPendingBytes   int64 `json:"commit_pending_bytes"`
	UploaderQueued       int   `json:"uploader_queued"`
	UploaderInFlight     int   `json:"uploader_in_flight"`
	DirtyInodes          int   `json:"dirty_inodes"`
	OpenFileHandles      int   `json:"open_file_handles"`
	OpenDirectoryHandles int   `json:"open_directory_handles"`
}

// StartContinuousPerf starts periodic JSONL sampling when configured.
func StartContinuousPerf(opts ProfilingOptions, fs *Dat9FS) (*ContinuousPerfRecorder, error) {
	r := &ContinuousPerfRecorder{
		opts:   opts,
		fs:     fs,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
	if opts.PerfSamplesPath == "" {
		close(r.doneCh)
		return r, nil
	}
	if opts.PerfSampleInterval <= 0 {
		opts.PerfSampleInterval = 10 * time.Second
		r.opts.PerfSampleInterval = opts.PerfSampleInterval
	}
	if err := ensureParentDir(opts.PerfSamplesPath); err != nil {
		close(r.doneCh)
		return nil, err
	}
	f, err := os.OpenFile(opts.PerfSamplesPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		close(r.doneCh)
		return nil, fmt.Errorf("open continuous perf samples %s: %w", opts.PerfSamplesPath, err)
	}
	r.file = f
	r.encoder = json.NewEncoder(f)
	if err := r.writeSample("start"); err != nil {
		_ = f.Close()
		close(r.doneCh)
		return nil, err
	}
	go r.loop()
	return r, nil
}

// Stop stops periodic sampling, writes a final sample, and closes the file.
func (r *ContinuousPerfRecorder) Stop() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		if r.file == nil {
			return
		}
		close(r.stopCh)
		<-r.doneCh
		if err := r.writeSample("stop"); err != nil {
			fmt.Fprintf(os.Stderr, "drive9: write continuous perf sample: %v\n", err)
		}
		if err := r.file.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "drive9: close continuous perf samples %s: %v\n", r.opts.PerfSamplesPath, err)
		}
	})
}

func (r *ContinuousPerfRecorder) loop() {
	defer close(r.doneCh)
	ticker := time.NewTicker(r.opts.PerfSampleInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			if err := r.writeSample("interval"); err != nil {
				fmt.Fprintf(os.Stderr, "drive9: write continuous perf sample: %v\n", err)
			}
		}
	}
}

func (r *ContinuousPerfRecorder) writeSample(reason string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.encoder == nil {
		return nil
	}
	sample := r.sample(reason)
	if err := r.encoder.Encode(sample); err != nil {
		return fmt.Errorf("encode continuous perf sample: %w", err)
	}
	return nil
}

func (r *ContinuousPerfRecorder) sample(reason string) continuousPerfSample {
	now := time.Now()
	sample := continuousPerfSample{
		Timestamp: now.UTC().Format(time.RFC3339Nano),
		Reason:    reason,
		Runtime:   readContinuousPerfRuntimeStats(),
		Process:   readContinuousPerfProcessStats(),
	}
	if r.fs != nil && r.fs.perf != nil && r.fs.perf.isEnabled() {
		snap := r.fs.perf.snapshot()
		sample.UptimeMS = snap.Uptime.Milliseconds()
		sample.FuseOps = continuousPerfStatsMap(snap.FuseOps)
		sample.RemoteOps = continuousPerfStatsMap(snap.RemoteOps)
		sample.Counters = snap.Counters
	}
	sample.Queues = r.queueStats()
	return sample
}

func (r *ContinuousPerfRecorder) queueStats() continuousPerfQueueStats {
	if r.fs == nil {
		return continuousPerfQueueStats{}
	}
	var stats continuousPerfQueueStats
	if r.fs.commitQueue != nil {
		stats.CommitPending, stats.CommitPendingBytes = r.fs.commitQueue.PendingStats()
	}
	if r.fs.uploader != nil {
		stats.UploaderQueued, stats.UploaderInFlight = r.fs.uploader.PendingStats()
	}
	r.fs.dirtyMu.Lock()
	stats.DirtyInodes = len(r.fs.dirtyInodes)
	r.fs.dirtyMu.Unlock()
	stats.OpenFileHandles = r.fs.fileHandles.Len()
	stats.OpenDirectoryHandles = r.fs.dirHandles.Len()
	return stats
}

func continuousPerfStatsMap(src map[string]perfOpStats) map[string]continuousPerfStats {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]continuousPerfStats, len(src))
	for name, st := range src {
		avg := uint64(0)
		if st.count > 0 {
			avg = st.totalNS / st.count
		}
		dst[name] = continuousPerfStats{
			Count:   st.count,
			Errors:  st.errors,
			Bytes:   st.bytes,
			TotalNS: st.totalNS,
			AvgNS:   avg,
		}
	}
	return dst
}

func readContinuousPerfRuntimeStats() continuousPerfRuntimeStats {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return continuousPerfRuntimeStats{
		Goroutines:      runtime.NumGoroutine(),
		HeapAllocBytes:  ms.HeapAlloc,
		HeapInuseBytes:  ms.HeapInuse,
		HeapObjects:     ms.HeapObjects,
		StackInuseBytes: ms.StackInuse,
		SysBytes:        ms.Sys,
		NextGCBytes:     ms.NextGC,
		NumGC:           ms.NumGC,
		PauseTotalNS:    ms.PauseTotalNs,
	}
}

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

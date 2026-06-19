package fuse

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/buildinfo"
)

const (
	defaultPerfMaxSamples      = 7200
	defaultPerfMaxSampleFiles  = 2
	defaultPerfMaxProfileFiles = 48
)

// ContinuousPerfRecorder writes low-overhead mount performance samples as JSONL.
type ContinuousPerfRecorder struct {
	opts ProfilingOptions
	fs   *Dat9FS

	mu          sync.Mutex
	file        *os.File
	encoder     *json.Encoder
	sampleCount int
	stopCh      chan struct{}
	doneCh      chan struct{}
	once        sync.Once
}

type continuousPerfSample struct {
	Timestamp string                         `json:"timestamp"`
	Reason    string                         `json:"reason"`
	UptimeMS  int64                          `json:"uptime_ms"`
	Context   continuousPerfContext          `json:"context"`
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
	P50NS   uint64 `json:"p50_ns"`
	P95NS   uint64 `json:"p95_ns"`
	P99NS   uint64 `json:"p99_ns"`
	MaxNS   uint64 `json:"max_ns"`
}

type continuousPerfContext struct {
	Component           string `json:"component"`
	Version             string `json:"version"`
	GitHash             string `json:"git_hash"`
	GitBranch           string `json:"git_branch"`
	BuildTime           string `json:"build_time"`
	GoVersion           string `json:"go_version"`
	GOOS                string `json:"goos"`
	GOARCH              string `json:"goarch"`
	PID                 int    `json:"pid"`
	MountPointHash      string `json:"mount_point_hash,omitempty"`
	RemoteRootHash      string `json:"remote_root_hash,omitempty"`
	ServerHash          string `json:"server_hash,omitempty"`
	SyncMode            string `json:"sync_mode,omitempty"`
	WritePolicy         string `json:"write_policy,omitempty"`
	Profile             string `json:"profile,omitempty"`
	PprofAddr           string `json:"pprof_addr,omitempty"`
	PerfIntervalMS      int64  `json:"perf_interval_ms,omitempty"`
	PerfMaxSamples      int    `json:"perf_max_samples,omitempty"`
	PerfMaxSampleFiles  int    `json:"perf_max_sample_files,omitempty"`
	PerfMaxProfileFiles int    `json:"perf_max_profile_files,omitempty"`
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
	// Re-apply defaults defensively for direct package callers that construct
	// ProfilingOptions without going through MountOptions.setDefaults.
	if opts.PerfSampleInterval <= 0 {
		opts.PerfSampleInterval = 10 * time.Second
		r.opts.PerfSampleInterval = opts.PerfSampleInterval
	}
	if opts.PerfMaxSamples <= 0 {
		opts.PerfMaxSamples = defaultPerfMaxSamples
		r.opts.PerfMaxSamples = opts.PerfMaxSamples
	}
	if opts.PerfMaxSampleFiles <= 0 {
		opts.PerfMaxSampleFiles = defaultPerfMaxSampleFiles
		r.opts.PerfMaxSampleFiles = opts.PerfMaxSampleFiles
	}
	if err := ensureParentDir(opts.PerfSamplesPath); err != nil {
		close(r.doneCh)
		return nil, err
	}
	if err := r.openSamplesFile(); err != nil {
		close(r.doneCh)
		return nil, err
	}
	if err := r.writeSample("start"); err != nil {
		_ = r.file.Close()
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
		// Route the final sample through writeSample so it follows the same
		// segment-size and rotation rules as interval samples.
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
	if r.opts.PerfMaxSamples > 0 && r.sampleCount >= r.opts.PerfMaxSamples {
		if err := r.rotateLocked(); err != nil {
			return err
		}
	}
	sample := r.sample(reason)
	if err := r.encoder.Encode(sample); err != nil {
		return fmt.Errorf("encode continuous perf sample: %w", err)
	}
	r.sampleCount++
	return nil
}

func (r *ContinuousPerfRecorder) sample(reason string) continuousPerfSample {
	now := time.Now()
	sample := continuousPerfSample{
		Timestamp: now.UTC().Format(time.RFC3339Nano),
		Reason:    reason,
		Context:   r.context(),
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

func (r *ContinuousPerfRecorder) openSamplesFile() error {
	f, err := os.OpenFile(r.opts.PerfSamplesPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open continuous perf samples %s: %w", r.opts.PerfSamplesPath, err)
	}
	r.file = f
	r.encoder = json.NewEncoder(f)
	return nil
}

func (r *ContinuousPerfRecorder) rotateLocked() error {
	if r.file != nil {
		if err := r.file.Close(); err != nil {
			return fmt.Errorf("close continuous perf samples before rotate %s: %w", r.opts.PerfSamplesPath, err)
		}
	}
	if err := r.rotateSampleFilesLocked(); err != nil {
		return err
	}
	r.file = nil
	r.encoder = nil
	r.sampleCount = 0
	return r.openSamplesFile()
}

func (r *ContinuousPerfRecorder) rotateSampleFilesLocked() error {
	maxFiles := r.opts.PerfMaxSampleFiles
	if maxFiles <= 0 {
		maxFiles = defaultPerfMaxSampleFiles
	}
	if maxFiles <= 1 {
		if err := os.Remove(r.opts.PerfSamplesPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove continuous perf samples %s: %w", r.opts.PerfSamplesPath, err)
		}
		return nil
	}

	historyCount := maxFiles - 1
	oldest := r.sampleFilePath(historyCount)
	if err := os.Remove(oldest); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove old continuous perf samples %s: %w", oldest, err)
	}
	for i := historyCount - 1; i >= 1; i-- {
		src := r.sampleFilePath(i)
		dst := r.sampleFilePath(i + 1)
		if err := os.Rename(src, dst); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("rotate continuous perf samples %s to %s: %w", src, dst, err)
		}
	}
	previous := r.sampleFilePath(1)
	if err := os.Rename(r.opts.PerfSamplesPath, previous); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("rotate continuous perf samples %s to %s: %w", r.opts.PerfSamplesPath, previous, err)
	}
	return nil
}

func (r *ContinuousPerfRecorder) sampleFilePath(index int) string {
	if index <= 0 {
		return r.opts.PerfSamplesPath
	}
	return r.opts.PerfSamplesPath + "." + strconv.Itoa(index)
}

func (r *ContinuousPerfRecorder) context() continuousPerfContext {
	info := buildinfo.Get("drive9-fuse")
	ctx := continuousPerfContext{
		Component:           info.Component,
		Version:             info.Version,
		GitHash:             info.GitHash,
		GitBranch:           info.GitBranch,
		BuildTime:           info.BuildTime,
		GoVersion:           info.GoVersion,
		GOOS:                runtime.GOOS,
		GOARCH:              runtime.GOARCH,
		PID:                 os.Getpid(),
		PprofAddr:           r.opts.PprofAddr,
		PerfIntervalMS:      r.opts.PerfSampleInterval.Milliseconds(),
		PerfMaxSamples:      r.opts.PerfMaxSamples,
		PerfMaxSampleFiles:  r.opts.PerfMaxSampleFiles,
		PerfMaxProfileFiles: r.opts.PerfMaxProfileFiles,
	}
	if r.fs == nil || r.fs.opts == nil {
		return ctx
	}
	opts := r.fs.opts
	ctx.MountPointHash = shortHash(opts.MountPoint)
	ctx.RemoteRootHash = shortHash(opts.RemoteRoot)
	ctx.ServerHash = shortHash(opts.Server)
	ctx.SyncMode = r.fs.syncMode.String()
	ctx.WritePolicy = opts.WritePolicy.String()
	ctx.Profile = opts.Profile
	return ctx
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
			P50NS:   st.p50NS,
			P95NS:   st.p95NS,
			P99NS:   st.p99NS,
			MaxNS:   st.maxNS,
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

func shortHash(s string) string {
	if s == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(s))
	return fmt.Sprintf("%x", sum[:6])
}

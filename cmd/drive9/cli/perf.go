package cli

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/buildinfo"
	"github.com/mem9-ai/dat9/pkg/mountstate"
)

var perfHTTPClient = &http.Client{Timeout: 10 * time.Second}

// Perf handles local performance collection and summarization commands.
func Perf(args []string) error {
	if len(args) == 0 {
		perfUsage()
		return fmt.Errorf("drive9 perf: command required")
	}
	switch args[0] {
	case "collect":
		return perfCollectCmd(args[1:])
	case "summarize":
		return perfSummarizeCmd(args[1:])
	case "sync":
		return perfSyncCmd(args[1:])
	case "-h", "-help", "--help", "help":
		perfUsage()
		return nil
	default:
		return fmt.Errorf("drive9 perf: unknown command %q", args[0])
	}
}

func perfUsage() {
	fmt.Fprint(os.Stderr, `usage: drive9 perf <command> [arguments]

commands:
  collect      collect a local support bundle for a running mount
  summarize    summarize a perf JSONL file into summary.json
  sync         ask a running profiled FUSE mount to drain remote writes
`)
}

func perfSummarizeCmd(args []string) error {
	fs := flag.NewFlagSet("perf summarize", flag.ExitOnError)
	input := fs.String("input", "", "perf JSONL input file")
	out := fs.String("out", "", "summary JSON output file (default stdout)")
	pretty := fs.Bool("pretty", true, "pretty-print JSON output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *input == "" {
		return fmt.Errorf("drive9 perf summarize: --input is required")
	}
	summary, err := summarizePerfJSONL(*input)
	if err != nil {
		return err
	}
	data, err := marshalSummary(summary, *pretty)
	if err != nil {
		return err
	}
	if *out == "" {
		_, err = os.Stdout.Write(data)
		return err
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil && filepath.Dir(*out) != "." {
		return fmt.Errorf("create summary output dir: %w", err)
	}
	if err := os.WriteFile(*out, data, 0o644); err != nil {
		return fmt.Errorf("write summary %s: %w", *out, err)
	}
	return nil
}

func perfCollectCmd(args []string) error {
	fs := flag.NewFlagSet("perf collect", flag.ExitOnError)
	mountPoint := fs.String("mountpoint", "", "drive9 mountpoint to inspect")
	duration := fs.Duration("duration", 30*time.Second, "CPU profile collection window; set 0 to skip waiting")
	out := fs.String("out", "", "output .tar.gz path")
	perfJSONL := fs.String("perf-jsonl", "", "perf JSONL path, overrides mount state")
	profileDir := fs.String("profile-dir", "", "profile directory, overrides mount state")
	pprofAddr := fs.String("pprof-addr", "", "mount pprof address, overrides mount state")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *duration < 0 {
		return fmt.Errorf("drive9 perf collect: --duration must be >= 0")
	}
	var state mountstate.ProcessState
	var statePath string
	var warnings []string
	if *mountPoint != "" {
		var err error
		state, statePath, err = mountstate.ReadProcessState(*mountPoint)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("read mount state: %v", err))
		} else {
			if *perfJSONL == "" {
				*perfJSONL = state.PerfJSONL
			}
			if *profileDir == "" {
				*profileDir = state.ProfileDir
			}
			if *pprofAddr == "" {
				*pprofAddr = state.PprofAddr
			}
		}
	}
	if *perfJSONL == "" && *profileDir == "" && *pprofAddr == "" {
		return fmt.Errorf("drive9 perf collect: provide --mountpoint for a profiled mount, or at least one of --perf-jsonl, --profile-dir, --pprof-addr")
	}
	if *out == "" {
		*out = fmt.Sprintf("drive9-perf-%s.tar.gz", time.Now().UTC().Format("20060102-150405"))
	}

	tmp, err := os.MkdirTemp("", "drive9-perf-collect-*")
	if err != nil {
		return fmt.Errorf("create temp bundle dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmp) }()

	if *pprofAddr != "" && *duration > 0 {
		cpuPath := filepath.Join(tmp, "cpu.pprof")
		if err := pprofCPUWindow(*pprofAddr, cpuPath, *duration); err != nil {
			warnings = append(warnings, err.Error())
		}
	} else if *duration > 0 {
		time.Sleep(*duration)
	}
	if *pprofAddr != "" {
		if err := fetchPprof(*pprofAddr, "/debug/pprof/heap", filepath.Join(tmp, "heap.pprof")); err != nil {
			warnings = append(warnings, err.Error())
		}
		if err := fetchPprof(*pprofAddr, "/debug/pprof/goroutine?debug=2", filepath.Join(tmp, "goroutine.txt")); err != nil {
			warnings = append(warnings, err.Error())
		}
	}

	copiedPerf, copyWarnings := copyPerfInputs(tmp, *perfJSONL)
	warnings = append(warnings, copyWarnings...)
	if *profileDir != "" {
		warnings = append(warnings, copyProfileDir(tmp, *profileDir)...)
	}
	if len(copiedPerf) > 0 {
		summary, err := summarizePerfJSONL(copiedPerf[len(copiedPerf)-1])
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("summarize perf jsonl: %v", err))
		} else if data, err := marshalSummary(summary, true); err != nil {
			warnings = append(warnings, fmt.Sprintf("marshal summary: %v", err))
		} else if err := os.WriteFile(filepath.Join(tmp, "summary.json"), data, 0o644); err != nil {
			warnings = append(warnings, fmt.Sprintf("write summary.json: %v", err))
		}
	}
	manifest := perfCollectManifest{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Duration:    duration.String(),
		Output:      filepath.Base(*out),
		MountPoint:  redactedHashValue(*mountPoint),
		StatePath:   statePath,
		State:       redactProcessState(state),
		Build:       buildinfo.Get("drive9"),
		GOOS:        runtime.GOOS,
		GOARCH:      runtime.GOARCH,
		Warnings:    warnings,
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(filepath.Join(tmp, "manifest.json"), data, 0o644); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	if err := writeTarGz(*out, tmp); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "drive9: perf bundle written to %s\n", *out)
	return nil
}

func perfSyncCmd(args []string) error {
	fs := flag.NewFlagSet("perf sync", flag.ExitOnError)
	mountPoint := fs.String("mountpoint", "", "drive9 mountpoint to inspect")
	pprofAddr := fs.String("pprof-addr", "", "mount pprof address, overrides mount state")
	timeout := fs.Duration("timeout", 5*time.Minute, "maximum time to wait for remote write drain")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *timeout <= 0 {
		return fmt.Errorf("drive9 perf sync: --timeout must be > 0")
	}
	if *mountPoint != "" && *pprofAddr == "" {
		state, _, err := mountstate.ReadProcessState(*mountPoint)
		if err != nil {
			return fmt.Errorf("drive9 perf sync: read mount state: %w", err)
		}
		*pprofAddr = state.PprofAddr
	}
	if *pprofAddr == "" {
		return fmt.Errorf("drive9 perf sync: provide --mountpoint for a profiled mount or --pprof-addr")
	}
	if err := perfMountSync(*pprofAddr, *timeout); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "drive9: mount sync completed")
	return nil
}

type perfCollectManifest struct {
	GeneratedAt string                  `json:"generated_at"`
	Duration    string                  `json:"duration"`
	Output      string                  `json:"output"`
	MountPoint  string                  `json:"mountpoint,omitempty"`
	StatePath   string                  `json:"state_path,omitempty"`
	State       mountstate.ProcessState `json:"state,omitempty"`
	Build       buildinfo.Info          `json:"build"`
	GOOS        string                  `json:"goos"`
	GOARCH      string                  `json:"goarch"`
	Warnings    []string                `json:"warnings,omitempty"`
}

type perfJSONSample struct {
	Timestamp string                     `json:"timestamp"`
	Reason    string                     `json:"reason"`
	UptimeMS  int64                      `json:"uptime_ms"`
	Context   map[string]any             `json:"context"`
	Runtime   perfJSONRuntimeStats       `json:"runtime"`
	Process   perfJSONProcessStats       `json:"process"`
	FuseOps   map[string]perfJSONOpStats `json:"fuse_ops"`
	RemoteOps map[string]perfJSONOpStats `json:"remote_ops"`
	Counters  map[string]uint64          `json:"counters"`
	Queues    perfJSONQueueStats         `json:"queues"`
}

type perfJSONOpStats struct {
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

type perfJSONRuntimeStats struct {
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

type perfJSONProcessStats struct {
	UserCPUNS   uint64 `json:"user_cpu_ns"`
	SystemCPUNS uint64 `json:"system_cpu_ns"`
	MaxRSSBytes int64  `json:"max_rss_bytes"`
}

type perfJSONQueueStats struct {
	CommitPending        int   `json:"commit_pending"`
	CommitPendingBytes   int64 `json:"commit_pending_bytes"`
	UploaderQueued       int   `json:"uploader_queued"`
	UploaderInFlight     int   `json:"uploader_in_flight"`
	DirtyInodes          int   `json:"dirty_inodes"`
	OpenFileHandles      int   `json:"open_file_handles"`
	OpenDirectoryHandles int   `json:"open_directory_handles"`
}

type perfSummary struct {
	GeneratedAt string                     `json:"generated_at"`
	Input       string                     `json:"input"`
	Samples     int                        `json:"samples"`
	First       string                     `json:"first,omitempty"`
	Last        string                     `json:"last,omitempty"`
	DurationMS  int64                      `json:"duration_ms,omitempty"`
	Context     map[string]any             `json:"context,omitempty"`
	RuntimeMax  perfSummaryRuntimeMax      `json:"runtime_max"`
	Process     perfSummaryProcess         `json:"process"`
	FuseOps     map[string]perfJSONOpStats `json:"fuse_ops,omitempty"`
	RemoteOps   map[string]perfJSONOpStats `json:"remote_ops,omitempty"`
	Counters    perfSummaryCounters        `json:"counters"`
	QueuesMax   perfJSONQueueStats         `json:"queues_max"`
}

type perfSummaryRuntimeMax struct {
	Goroutines      int    `json:"goroutines"`
	HeapAllocBytes  uint64 `json:"heap_alloc_bytes"`
	HeapInuseBytes  uint64 `json:"heap_inuse_bytes"`
	HeapObjects     uint64 `json:"heap_objects"`
	StackInuseBytes uint64 `json:"stack_inuse_bytes"`
	SysBytes        uint64 `json:"sys_bytes"`
}

type perfSummaryProcess struct {
	UserCPUSeconds   float64 `json:"user_cpu_seconds"`
	SystemCPUSeconds float64 `json:"system_cpu_seconds"`
	CPUPercent       float64 `json:"cpu_percent"`
	MaxRSSBytes      int64   `json:"max_rss_bytes"`
}

type perfSummaryCounters struct {
	First map[string]uint64 `json:"first,omitempty"`
	Last  map[string]uint64 `json:"last,omitempty"`
	Delta map[string]uint64 `json:"delta,omitempty"`
}

func summarizePerfJSONL(path string) (perfSummary, error) {
	f, err := os.Open(path)
	if err != nil {
		return perfSummary{}, fmt.Errorf("open perf jsonl %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), 32*1024*1024)
	var first, last perfJSONSample
	var firstTime, lastTime time.Time
	var samples int
	var queuesMax perfJSONQueueStats
	var runtimeMax perfSummaryRuntimeMax
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var sample perfJSONSample
		if err := json.Unmarshal([]byte(line), &sample); err != nil {
			return perfSummary{}, fmt.Errorf("parse perf jsonl %s line %d: %w", path, samples+1, err)
		}
		if samples == 0 {
			first = sample
			firstTime, _ = time.Parse(time.RFC3339Nano, sample.Timestamp)
		}
		last = sample
		lastTime, _ = time.Parse(time.RFC3339Nano, sample.Timestamp)
		samples++
		updateRuntimeMax(&runtimeMax, sample.Runtime)
		updateQueueMax(&queuesMax, sample.Queues)
	}
	if err := scanner.Err(); err != nil {
		return perfSummary{}, fmt.Errorf("read perf jsonl %s: %w", path, err)
	}
	if samples == 0 {
		return perfSummary{}, fmt.Errorf("perf jsonl %s has no samples", path)
	}
	durationMS := int64(0)
	if !firstTime.IsZero() && !lastTime.IsZero() && lastTime.After(firstTime) {
		durationMS = lastTime.Sub(firstTime).Milliseconds()
	}
	process := summarizeProcess(first, last, durationMS)
	return perfSummary{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Input:       path,
		Samples:     samples,
		First:       first.Timestamp,
		Last:        last.Timestamp,
		DurationMS:  durationMS,
		Context:     last.Context,
		RuntimeMax:  runtimeMax,
		Process:     process,
		FuseOps:     cleanOpMap(last.FuseOps),
		RemoteOps:   cleanOpMap(last.RemoteOps),
		Counters: perfSummaryCounters{
			First: first.Counters,
			Last:  last.Counters,
			Delta: counterDelta(first.Counters, last.Counters),
		},
		QueuesMax: queuesMax,
	}, nil
}

func updateRuntimeMax(dst *perfSummaryRuntimeMax, src perfJSONRuntimeStats) {
	if src.Goroutines > dst.Goroutines {
		dst.Goroutines = src.Goroutines
	}
	if src.HeapAllocBytes > dst.HeapAllocBytes {
		dst.HeapAllocBytes = src.HeapAllocBytes
	}
	if src.HeapInuseBytes > dst.HeapInuseBytes {
		dst.HeapInuseBytes = src.HeapInuseBytes
	}
	if src.HeapObjects > dst.HeapObjects {
		dst.HeapObjects = src.HeapObjects
	}
	if src.StackInuseBytes > dst.StackInuseBytes {
		dst.StackInuseBytes = src.StackInuseBytes
	}
	if src.SysBytes > dst.SysBytes {
		dst.SysBytes = src.SysBytes
	}
}

func updateQueueMax(dst *perfJSONQueueStats, src perfJSONQueueStats) {
	if src.CommitPending > dst.CommitPending {
		dst.CommitPending = src.CommitPending
	}
	if src.CommitPendingBytes > dst.CommitPendingBytes {
		dst.CommitPendingBytes = src.CommitPendingBytes
	}
	if src.UploaderQueued > dst.UploaderQueued {
		dst.UploaderQueued = src.UploaderQueued
	}
	if src.UploaderInFlight > dst.UploaderInFlight {
		dst.UploaderInFlight = src.UploaderInFlight
	}
	if src.DirtyInodes > dst.DirtyInodes {
		dst.DirtyInodes = src.DirtyInodes
	}
	if src.OpenFileHandles > dst.OpenFileHandles {
		dst.OpenFileHandles = src.OpenFileHandles
	}
	if src.OpenDirectoryHandles > dst.OpenDirectoryHandles {
		dst.OpenDirectoryHandles = src.OpenDirectoryHandles
	}
}

func summarizeProcess(first perfJSONSample, last perfJSONSample, durationMS int64) perfSummaryProcess {
	userNS := saturatingSub(last.Process.UserCPUNS, first.Process.UserCPUNS)
	sysNS := saturatingSub(last.Process.SystemCPUNS, first.Process.SystemCPUNS)
	cpuPercent := float64(0)
	if durationMS > 0 {
		cpuPercent = (float64(userNS+sysNS) / float64(time.Duration(durationMS)*time.Millisecond)) * 100
	}
	return perfSummaryProcess{
		UserCPUSeconds:   float64(userNS) / float64(time.Second),
		SystemCPUSeconds: float64(sysNS) / float64(time.Second),
		CPUPercent:       cpuPercent,
		MaxRSSBytes:      maxInt64(first.Process.MaxRSSBytes, last.Process.MaxRSSBytes),
	}
}

func cleanOpMap(src map[string]perfJSONOpStats) map[string]perfJSONOpStats {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]perfJSONOpStats, len(src))
	for name, st := range src {
		if st.Count == 0 {
			continue
		}
		out[name] = st
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func counterDelta(first, last map[string]uint64) map[string]uint64 {
	if len(last) == 0 {
		return nil
	}
	out := make(map[string]uint64, len(last))
	for k, v := range last {
		out[k] = saturatingSub(v, first[k])
	}
	return out
}

func saturatingSub(a, b uint64) uint64 {
	if a < b {
		return 0
	}
	return a - b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func marshalSummary(summary perfSummary, pretty bool) ([]byte, error) {
	var (
		data []byte
		err  error
	)
	if pretty {
		data, err = json.MarshalIndent(summary, "", "  ")
	} else {
		data, err = json.Marshal(summary)
	}
	if err != nil {
		return nil, fmt.Errorf("marshal summary: %w", err)
	}
	return append(data, '\n'), nil
}

func pprofCPUWindow(addr, path string, duration time.Duration) error {
	base := normalizePprofBase(addr)
	startURL := base + "/debug/drive9/profile/cpu/start?path=" + url.QueryEscape(path)
	if err := httpGetOK(startURL); err != nil {
		return fmt.Errorf("start CPU profile: %w", err)
	}
	time.Sleep(duration)
	stopURL := base + "/debug/drive9/profile/cpu/stop"
	if err := httpGetOK(stopURL); err != nil {
		return fmt.Errorf("stop CPU profile: %w", err)
	}
	return nil
}

func fetchPprof(addr, endpoint, out string) error {
	base := normalizePprofBase(addr)
	resp, err := perfHTTPClient.Get(base + endpoint)
	if err != nil {
		return fmt.Errorf("fetch pprof %s: %w", endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch pprof %s: status %d", endpoint, resp.StatusCode)
	}
	f, err := os.Create(out)
	if err != nil {
		return fmt.Errorf("create pprof output %s: %w", out, err)
	}
	defer func() { _ = f.Close() }()
	if _, err := io.Copy(f, resp.Body); err != nil {
		return fmt.Errorf("write pprof output %s: %w", out, err)
	}
	return nil
}

func perfMountSync(addr string, timeout time.Duration) error {
	base := normalizePprofBase(addr)
	syncURL := base + "/debug/drive9/mount/sync?timeout=" + url.QueryEscape(timeout.String())
	client := &http.Client{Timeout: timeout + 5*time.Second}
	resp, err := client.Get(syncURL)
	if err != nil {
		return fmt.Errorf("mount sync: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("mount sync returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func httpGetOK(rawURL string) error {
	resp, err := perfHTTPClient.Get(rawURL)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%s returned status %d: %s", rawURL, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func normalizePprofBase(addr string) string {
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return strings.TrimRight(addr, "/")
	}
	return "http://" + strings.TrimRight(addr, "/")
}

func copyPerfInputs(bundleDir, perfJSONL string) ([]string, []string) {
	if perfJSONL == "" {
		return nil, nil
	}
	var copied []string
	var warnings []string
	for _, src := range []string{perfJSONL + ".1", perfJSONL} {
		if _, err := os.Stat(src); err != nil {
			if !os.IsNotExist(err) {
				warnings = append(warnings, fmt.Sprintf("stat perf jsonl %s: %v", src, err))
			}
			continue
		}
		dst := filepath.Join(bundleDir, filepath.Base(src))
		if err := copyFile(src, dst); err != nil {
			warnings = append(warnings, err.Error())
			continue
		}
		copied = append(copied, dst)
	}
	return copied, warnings
}

func copyProfileDir(bundleDir, profileDir string) []string {
	var warnings []string
	entries, err := os.ReadDir(profileDir)
	if err != nil {
		return []string{fmt.Sprintf("read profile dir %s: %v", profileDir, err)}
	}
	dstDir := filepath.Join(bundleDir, "profile-dir")
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return []string{fmt.Sprintf("create profile bundle dir: %v", err)}
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !isPerfArtifactName(name) {
			continue
		}
		if err := copyFile(filepath.Join(profileDir, name), filepath.Join(dstDir, name)); err != nil {
			warnings = append(warnings, err.Error())
		}
	}
	return warnings
}

func isPerfArtifactName(name string) bool {
	for _, suffix := range []string{".pprof", ".json", ".jsonl", ".txt", ".log", ".svg"} {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return false
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open %s: %w", src, err)
	}
	defer func() { _ = in.Close() }()
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("create dir for %s: %w", dst, err)
	}
	out, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("create %s: %w", dst, err)
	}
	defer func() { _ = out.Close() }()
	if _, err := io.Copy(out, in); err != nil {
		return fmt.Errorf("copy %s to %s: %w", src, dst, err)
	}
	return nil
}

func writeTarGz(outPath, srcDir string) error {
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil && filepath.Dir(outPath) != "." {
		return fmt.Errorf("create output dir: %w", err)
	}
	out, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("create bundle %s: %w", outPath, err)
	}
	defer func() { _ = out.Close() }()
	gz := gzip.NewWriter(out)
	defer func() { _ = gz.Close() }()
	tw := tar.NewWriter(gz)
	defer func() { _ = tw.Close() }()

	var files []string
	if err := filepath.WalkDir(srcDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		files = append(files, path)
		return nil
	}); err != nil {
		return fmt.Errorf("walk bundle dir: %w", err)
	}
	sort.Strings(files)
	for _, path := range files {
		if err := addTarFile(tw, srcDir, path); err != nil {
			return err
		}
	}
	return nil
}

func addTarFile(tw *tar.Writer, root, path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat bundle file %s: %w", path, err)
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return fmt.Errorf("bundle relative path %s: %w", path, err)
	}
	hdr, err := tar.FileInfoHeader(info, "")
	if err != nil {
		return fmt.Errorf("create tar header for %s: %w", path, err)
	}
	hdr.Name = filepath.ToSlash(rel)
	if err := tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("write tar header for %s: %w", path, err)
	}
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open bundle file %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	if _, err := io.Copy(tw, f); err != nil {
		return fmt.Errorf("write tar file %s: %w", path, err)
	}
	return nil
}

func redactProcessState(state mountstate.ProcessState) mountstate.ProcessState {
	if state.Server != "" {
		state.Server = redactedHashValue(state.Server)
	}
	if state.MountPoint != "" {
		state.MountPoint = redactedHashValue(state.MountPoint)
	}
	if state.RemoteRoot != "" {
		state.RemoteRoot = redactedHashValue(state.RemoteRoot)
	}
	return state
}

func redactedHashValue(s string) string {
	if s == "" {
		return ""
	}
	return "sha256:" + shortCLIHash(s)
}

func shortCLIHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return fmt.Sprintf("%x", sum[:6])
}

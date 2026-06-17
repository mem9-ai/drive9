package fuse

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestContinuousPerfRecorderWritesJSONLSamples(t *testing.T) {
	path := filepath.Join(t.TempDir(), "perf", "samples.jsonl")
	opts := &MountOptions{
		CacheSize:             1 << 20,
		ReadCacheMaxFileBytes: 1 << 20,
		Profiling: ProfilingOptions{
			PerfSamplesPath:    path,
			PerfSampleInterval: 10 * time.Millisecond,
		},
	}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
	fs.fileHandles.Allocate(&FileHandle{Path: "/open.txt"})
	fs.dirtyMu.Lock()
	fs.dirtyInodes[42] = dirtyInodeState{size: 128, seq: 1}
	fs.dirtyMu.Unlock()
	fs.perf.recordFuseOp(perfFuseWrite, gofuse.OK, 2*time.Millisecond, 64)
	fs.perf.recordRemoteOp(perfRemoteWrite, nil, 3*time.Millisecond, 64)
	fs.perf.readCacheHit.add(2)

	recorder, err := StartContinuousPerf(opts.Profiling, fs)
	if err != nil {
		t.Fatalf("StartContinuousPerf: %v", err)
	}
	waitForPerfLines(t, path, 2)
	recorder.Stop()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read samples: %v", err)
	}
	lines := strings.Fields(strings.TrimSpace(string(data)))
	if len(lines) < 3 {
		t.Fatalf("sample lines = %d, want at least 3; data=%s", len(lines), data)
	}

	var first, last continuousPerfSample
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("unmarshal first sample: %v", err)
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &last); err != nil {
		t.Fatalf("unmarshal last sample: %v", err)
	}
	if first.Reason != "start" {
		t.Fatalf("first reason = %q, want start", first.Reason)
	}
	if last.Reason != "stop" {
		t.Fatalf("last reason = %q, want stop", last.Reason)
	}
	if last.Runtime.Goroutines <= 0 || last.Runtime.SysBytes == 0 {
		t.Fatalf("runtime stats not populated: %+v", last.Runtime)
	}
	if last.Context.Component != "drive9-fuse" || last.Context.PID <= 0 {
		t.Fatalf("context not populated: %+v", last.Context)
	}
	if last.FuseOps["write"].Count != 1 || last.FuseOps["write"].Bytes != 64 {
		t.Fatalf("write fuse stats = %+v, want count=1 bytes=64", last.FuseOps["write"])
	}
	if last.FuseOps["write"].P95NS == 0 || last.FuseOps["write"].MaxNS == 0 {
		t.Fatalf("write latency stats not populated: %+v", last.FuseOps["write"])
	}
	if last.RemoteOps["write"].Count != 1 || last.RemoteOps["write"].Bytes != 64 {
		t.Fatalf("write remote stats = %+v, want count=1 bytes=64", last.RemoteOps["write"])
	}
	if last.Counters["read_cache_hit"] != 2 {
		t.Fatalf("read_cache_hit = %d, want 2", last.Counters["read_cache_hit"])
	}
	if last.Queues.OpenFileHandles != 1 || last.Queues.DirtyInodes != 1 {
		t.Fatalf("queue stats = %+v, want one open file handle and one dirty inode", last.Queues)
	}
}

func TestContinuousPerfRecorderRotatesSamples(t *testing.T) {
	path := filepath.Join(t.TempDir(), "samples.jsonl")
	opts := &MountOptions{
		CacheSize:             1 << 20,
		ReadCacheMaxFileBytes: 1 << 20,
		Profiling: ProfilingOptions{
			PerfSamplesPath:    path,
			PerfSampleInterval: time.Hour,
			PerfMaxSamples:     2,
		},
	}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
	recorder, err := StartContinuousPerf(opts.Profiling, fs)
	if err != nil {
		t.Fatalf("StartContinuousPerf: %v", err)
	}
	if err := recorder.writeSample("manual-1"); err != nil {
		t.Fatalf("write manual sample 1: %v", err)
	}
	if err := recorder.writeSample("manual-2"); err != nil {
		t.Fatalf("write manual sample 2: %v", err)
	}
	recorder.Stop()

	if _, err := os.Stat(path + ".1"); err != nil {
		t.Fatalf("stat rotated segment: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read active segment: %v", err)
	}
	if !strings.Contains(string(data), `"reason":"stop"`) {
		t.Fatalf("active segment missing stop sample: %s", data)
	}
}

func TestContinuousPerfRecorderRetainsConfiguredSampleFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "samples.jsonl")
	opts := &MountOptions{
		CacheSize:             1 << 20,
		ReadCacheMaxFileBytes: 1 << 20,
		Profiling: ProfilingOptions{
			PerfSamplesPath:    path,
			PerfSampleInterval: time.Hour,
			PerfMaxSamples:     1,
			PerfMaxSampleFiles: 3,
		},
	}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
	recorder, err := StartContinuousPerf(opts.Profiling, fs)
	if err != nil {
		t.Fatalf("StartContinuousPerf: %v", err)
	}
	for i := 0; i < 4; i++ {
		if err := recorder.writeSample("manual"); err != nil {
			t.Fatalf("write manual sample %d: %v", i, err)
		}
	}
	recorder.Stop()

	matches, err := filepath.Glob(path + "*")
	if err != nil {
		t.Fatalf("glob sample files: %v", err)
	}
	if len(matches) != 3 {
		t.Fatalf("sample files = %v, want exactly 3", matches)
	}
	for _, name := range []string{path, path + ".1", path + ".2"} {
		if _, err := os.Stat(name); err != nil {
			t.Fatalf("stat retained sample file %s: %v", name, err)
		}
	}
	if _, err := os.Stat(path + ".3"); !os.IsNotExist(err) {
		t.Fatalf("stat sample file %s error = %v, want not exist", path+".3", err)
	}
}

func waitForPerfLines(t *testing.T, path string, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil && strings.Count(string(data), "\n") >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	data, _ := os.ReadFile(path)
	t.Fatalf("timed out waiting for %d perf sample lines; data=%s", want, data)
}

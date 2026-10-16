package perf

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// TestContinuousPerfRecorderWritesJSONLSamples ports v1's
// TestContinuousPerfRecorderWritesJSONLSamples onto the probe API: the mount
// context, counters, and queue depths are attached through SetEnv/SetPerf/
// SetQueueStats instead of reading them off the fs.
func TestContinuousPerfRecorderWritesJSONLSamples(t *testing.T) {
	path := filepath.Join(t.TempDir(), "perf", "samples.jsonl")
	counters := NewCounters(true)
	counters.RecordFuseOpByName("write", gofuse.OK, 2*time.Millisecond, 64)
	counters.RecordRemoteOpByName("write", nil, 3*time.Millisecond, 64)
	counters.RecordRemoteOpByName("append_log", nil, 4*time.Millisecond, 8)
	counters.appendLogOutcomeSuccess.add(1)
	counters.appendLogGenerationResetBytes.add(32)
	counters.appendLogGenerationResetShadowReady.add(1)
	counters.appendLogGenerationResetShadowDegraded.add(1)
	counters.readCacheHit.add(2)

	recorder, err := StartContinuousPerf(ProfilingOptions{
		// The probes below attach after StartContinuousPerf has spawned the
		// sampling loop, so the probe fields are unsynchronized with that
		// goroutine. A long interval keeps every probe-reading sample on the
		// caller's goroutine (the manual sample and Stop's final sample),
		// which is the ordering the facade's Start→Set wiring guarantees in
		// practice before its first tick.
		PerfSamplesPath:    path,
		PerfSampleInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("StartContinuousPerf: %v", err)
	}
	recorder.SetEnv(&ContinuousPerfEnv{
		MountPoint:  "/mnt/drive9",
		RemoteRoot:  "/",
		Server:      "http://127.0.0.1:8080",
		SyncMode:    "strict",
		WritePolicy: "writeback",
		Profile:     "balanced",
	})
	recorder.SetPerf(counters)
	recorder.SetQueueStats(func() *QueueStats {
		return &QueueStats{OpenFileHandles: 1, DirtyInodes: 1}
	})
	if err := recorder.writeSample("interval"); err != nil {
		t.Fatalf("write probe-driven sample: %v", err)
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
	if last.Process.UserCPUNS == 0 && last.Process.SystemCPUNS == 0 {
		t.Fatalf("process stats not populated: %+v", last.Process)
	}
	if last.Context.Component != "drive9-fuse" || last.Context.PID <= 0 {
		t.Fatalf("context not populated: %+v", last.Context)
	}
	if last.Context.SyncMode != "strict" {
		t.Fatalf("context sync_mode = %q, want strict", last.Context.SyncMode)
	}
	if last.Context.WritePolicy != "writeback" {
		t.Fatalf("context write_policy = %q, want writeback", last.Context.WritePolicy)
	}
	if last.Context.Profile != "balanced" {
		t.Fatalf("context profile = %q, want balanced", last.Context.Profile)
	}
	if last.Context.MountPointHash == "" || last.Context.ServerHash == "" {
		t.Fatalf("context hashes not populated: %+v", last.Context)
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
	if last.RemoteOps["append_log"].Count != 1 || last.RemoteOps["append_log"].Bytes != 8 || last.Counters["append_log_outcome_success"] != 1 {
		t.Fatalf("append-log sample = remote=%+v counters=%v", last.RemoteOps["append_log"], last.Counters)
	}
	if last.Counters["append_log_generation_reset_bytes"] != 32 {
		t.Fatalf("generation-reset counters = %v", last.Counters)
	}
	if last.Counters["append_log_generation_reset_shadow_ready"] != 1 || last.Counters["append_log_generation_reset_shadow_degraded"] != 1 {
		t.Fatalf("generation-reset shadow counters = %v", last.Counters)
	}
	if last.Counters["read_cache_hit"] != 2 {
		t.Fatalf("read_cache_hit = %d, want 2", last.Counters["read_cache_hit"])
	}
	if last.Queues.OpenFileHandles != 1 || last.Queues.DirtyInodes != 1 {
		t.Fatalf("queue stats = %+v, want one open file handle and one dirty inode", last.Queues)
	}
}

// TestContinuousPerfRecorderWithoutCounters verifies the recorder samples
// fine with no counters attached (SetPerf(nil)) and a queue probe that
// returns nil: samples carry zero queues and omit the op maps.
func TestContinuousPerfRecorderWithoutCounters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "samples.jsonl")
	recorder, err := StartContinuousPerf(ProfilingOptions{
		PerfSamplesPath:    path,
		PerfSampleInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("StartContinuousPerf: %v", err)
	}
	recorder.SetPerf(nil)
	recorder.SetQueueStats(func() *QueueStats { return nil })
	recorder.Stop()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read samples: %v", err)
	}
	lines := strings.Fields(strings.TrimSpace(string(data)))
	if len(lines) != 2 {
		t.Fatalf("sample lines = %d, want start+stop; data=%s", len(lines), data)
	}
	var last continuousPerfSample
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &last); err != nil {
		t.Fatalf("unmarshal stop sample: %v", err)
	}
	if last.Reason != "stop" {
		t.Fatalf("last reason = %q, want stop", last.Reason)
	}
	if last.FuseOps != nil || last.RemoteOps != nil || last.Counters != nil {
		t.Fatalf("stop sample without counters = %+v, want nil op maps", last)
	}
	if last.Queues != (QueueStats{}) {
		t.Fatalf("stop sample queues = %+v, want zero value", last.Queues)
	}
	if last.Context.Component != "drive9-fuse" {
		t.Fatalf("stop sample component = %q, want drive9-fuse", last.Context.Component)
	}
}

func TestContinuousPerfRecorderRotatesSamples(t *testing.T) {
	path := filepath.Join(t.TempDir(), "samples.jsonl")
	recorder, err := StartContinuousPerf(ProfilingOptions{
		PerfSamplesPath:    path,
		PerfSampleInterval: time.Hour,
		PerfMaxSamples:     2,
	})
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
	recorder, err := StartContinuousPerf(ProfilingOptions{
		PerfSamplesPath:    path,
		PerfSampleInterval: time.Hour,
		PerfMaxSamples:     1,
		PerfMaxSampleFiles: 3,
	})
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

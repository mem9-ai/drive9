package fuse

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStartProfilerWritesFinalHeapProfile(t *testing.T) {
	heapPath := filepath.Join(t.TempDir(), "heap.pprof")
	profiler, err := StartProfiler(ProfilingOptions{HeapProfilePath: heapPath})
	if err != nil {
		t.Fatalf("StartProfiler: %v", err)
	}
	profiler.Stop()
	info, err := os.Stat(heapPath)
	if err != nil {
		t.Fatalf("stat heap profile: %v", err)
	}
	if info.Size() == 0 {
		t.Fatalf("heap profile is empty: %s", heapPath)
	}
}

func TestStartProfilerRejectsIntervalWithoutDir(t *testing.T) {
	_, err := StartProfiler(ProfilingOptions{HeapProfileInterval: time.Second})
	if err == nil {
		t.Fatal("StartProfiler error = nil, want interval without dir error")
	}
}

func TestStartProfilerWritesBoundedCPUProfile(t *testing.T) {
	dir := t.TempDir()
	profiler, err := StartProfiler(ProfilingOptions{
		ProfileDir:         dir,
		CPUProfileDuration: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("StartProfiler: %v", err)
	}
	busyWait(80 * time.Millisecond)
	profiler.Stop()

	matches, err := filepath.Glob(filepath.Join(dir, "cpu-*.pprof"))
	if err != nil {
		t.Fatalf("glob cpu profiles: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("bounded CPU profiles = %v, want one timestamped profile", matches)
	}
	info, err := os.Stat(matches[0])
	if err != nil {
		t.Fatalf("stat cpu profile: %v", err)
	}
	if info.Size() == 0 {
		t.Fatalf("cpu profile is empty: %s", matches[0])
	}
}

func TestStartProfilerRejectsCPUIntervalWithoutDuration(t *testing.T) {
	_, err := StartProfiler(ProfilingOptions{
		ProfileDir:         t.TempDir(),
		CPUProfileInterval: time.Second,
	})
	if err == nil {
		t.Fatal("StartProfiler error = nil, want CPU interval without duration error")
	}
}

func TestStartProfilerWritesPeriodicCPUProfiles(t *testing.T) {
	dir := t.TempDir()
	profiler, err := StartProfiler(ProfilingOptions{
		ProfileDir:         dir,
		CPUProfileDuration: 10 * time.Millisecond,
		CPUProfileInterval: 30 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("StartProfiler: %v", err)
	}
	busyWait(90 * time.Millisecond)
	profiler.Stop()

	matches, err := filepath.Glob(filepath.Join(dir, "cpu-*.pprof"))
	if err != nil {
		t.Fatalf("glob cpu profiles: %v", err)
	}
	if len(matches) == 0 {
		t.Fatalf("periodic CPU profiles = %v, want at least one", matches)
	}
}

func TestMountOptionsProfileDirDefaultsCPUWindow(t *testing.T) {
	opts := &MountOptions{
		Profiling: ProfilingOptions{ProfileDir: t.TempDir()},
	}
	opts.setDefaults()
	if opts.Profiling.CPUProfileDuration != defaultCPUProfileDuration {
		t.Fatalf("CPUProfileDuration = %v, want %v", opts.Profiling.CPUProfileDuration, defaultCPUProfileDuration)
	}
	if opts.Profiling.CPUProfileInterval != defaultCPUProfileInterval {
		t.Fatalf("CPUProfileInterval = %v, want %v", opts.Profiling.CPUProfileInterval, defaultCPUProfileInterval)
	}
}

func TestPprofMuxServesIndex(t *testing.T) {
	profiler := &Profiler{}
	ts := httptest.NewServer(profiler.newPprofMux())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/debug/pprof/")
	if err != nil {
		t.Fatalf("GET pprof index: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("pprof index status = %d, want 200", resp.StatusCode)
	}
}

func TestStartProfilerRecordsActualPprofAddr(t *testing.T) {
	profiler, err := StartProfiler(ProfilingOptions{PprofAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("StartProfiler: %v", err)
	}
	defer profiler.Stop()

	if got := profiler.PprofAddr(); got == "" || got == "127.0.0.1:0" {
		t.Fatalf("PprofAddr = %q, want actual listener address", got)
	}
}

func TestPprofMuxControlsCPUProfile(t *testing.T) {
	dir := t.TempDir()
	profiler := &Profiler{opts: ProfilingOptions{ProfileDir: dir}}
	ts := httptest.NewServer(profiler.newPprofMux())
	defer ts.Close()
	t.Cleanup(profiler.Stop)

	resp, err := http.Get(ts.URL + "/debug/drive9/profile/cpu/start")
	if err != nil {
		t.Fatalf("start cpu profile: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("start cpu profile status = %d, want 200", resp.StatusCode)
	}

	resp, err = http.Get(ts.URL + "/debug/drive9/profile/cpu/stop")
	if err != nil {
		t.Fatalf("stop cpu profile: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stop cpu profile status = %d, want 200", resp.StatusCode)
	}

	matches, err := filepath.Glob(filepath.Join(dir, "cpu-*.pprof"))
	if err != nil {
		t.Fatalf("glob cpu profile: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("cpu profile matches = %v, want one timestamped profile", matches)
	}
	info, err := os.Stat(matches[0])
	if err != nil {
		t.Fatalf("stat cpu profile: %v", err)
	}
	if info.Size() == 0 {
		t.Fatalf("cpu profile is empty: %s", matches[0])
	}
}

func busyWait(duration time.Duration) {
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
	}
}

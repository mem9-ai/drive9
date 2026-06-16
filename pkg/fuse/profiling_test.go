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

func TestMountOptionsProfileDirDefaultsProfileWindows(t *testing.T) {
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
	if opts.Profiling.HeapProfileInterval != defaultHeapProfileInterval {
		t.Fatalf("HeapProfileInterval = %v, want %v", opts.Profiling.HeapProfileInterval, defaultHeapProfileInterval)
	}
	if opts.Profiling.PerfMaxProfileFiles != defaultPerfMaxProfileFiles {
		t.Fatalf("PerfMaxProfileFiles = %d, want %d", opts.Profiling.PerfMaxProfileFiles, defaultPerfMaxProfileFiles)
	}
}

func TestPruneProfileFilesKeepsNewestPerPattern(t *testing.T) {
	dir := t.TempDir()
	writeProfile := func(name string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte("profile"), 0o644); err != nil {
			t.Fatalf("write profile %s: %v", name, err)
		}
	}
	writeProfile("cpu-20260101-000001.000000000.pprof")
	writeProfile("cpu-20260101-000002.000000000.pprof")
	writeProfile("cpu-20260101-000003.000000000.pprof")
	writeProfile("heap-20260101-000001.pprof")
	writeProfile("heap-20260101-000002.pprof")
	writeProfile("heap-final.pprof")

	if err := pruneProfileFiles(dir, "cpu-*.pprof", 2); err != nil {
		t.Fatalf("prune CPU profiles: %v", err)
	}
	assertMissingFile(t, filepath.Join(dir, "cpu-20260101-000001.000000000.pprof"))
	assertExistingFile(t, filepath.Join(dir, "cpu-20260101-000002.000000000.pprof"))
	assertExistingFile(t, filepath.Join(dir, "cpu-20260101-000003.000000000.pprof"))
	assertExistingFile(t, filepath.Join(dir, "heap-20260101-000001.pprof"))
	assertExistingFile(t, filepath.Join(dir, "heap-20260101-000002.pprof"))

	if err := pruneProfileFiles(dir, "heap-[0-9]*.pprof", 1); err != nil {
		t.Fatalf("prune heap profiles: %v", err)
	}
	assertMissingFile(t, filepath.Join(dir, "heap-20260101-000001.pprof"))
	assertExistingFile(t, filepath.Join(dir, "heap-20260101-000002.pprof"))
	assertExistingFile(t, filepath.Join(dir, "heap-final.pprof"))
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

func assertExistingFile(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stat existing file %s: %v", path, err)
	}
}

func assertMissingFile(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("stat missing file %s error = %v, want not exist", path, err)
	}
}

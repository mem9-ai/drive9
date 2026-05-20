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

	info, err := os.Stat(filepath.Join(dir, "cpu.pprof"))
	if err != nil {
		t.Fatalf("stat cpu profile: %v", err)
	}
	if info.Size() == 0 {
		t.Fatalf("cpu profile is empty")
	}
}

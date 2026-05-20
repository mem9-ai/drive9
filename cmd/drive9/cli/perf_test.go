package cli

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPerfSummarizeJSONL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "perf.jsonl")
	writePerfFixture(t, path)

	summary, err := summarizePerfJSONL(path)
	if err != nil {
		t.Fatalf("summarizePerfJSONL: %v", err)
	}
	if summary.Samples != 2 {
		t.Fatalf("Samples = %d, want 2", summary.Samples)
	}
	if summary.RuntimeMax.HeapAllocBytes != 300 {
		t.Fatalf("HeapAllocBytes max = %d, want 300", summary.RuntimeMax.HeapAllocBytes)
	}
	if summary.FuseOps["write"].Count != 3 || summary.FuseOps["write"].P95NS != 1000 {
		t.Fatalf("write summary = %+v, want count=3 p95=1000", summary.FuseOps["write"])
	}
	if summary.Counters.Delta["read_cache_hit"] != 2 {
		t.Fatalf("read_cache_hit delta = %d, want 2", summary.Counters.Delta["read_cache_hit"])
	}
	if summary.QueuesMax.CommitPending != 2 {
		t.Fatalf("CommitPending max = %d, want 2", summary.QueuesMax.CommitPending)
	}
}

func TestPerfCollectCreatesBundle(t *testing.T) {
	dir := t.TempDir()
	perfPath := filepath.Join(dir, "perf.jsonl")
	writePerfFixture(t, perfPath)
	out := filepath.Join(dir, "bundle.tar.gz")

	if err := Perf([]string{
		"collect",
		"--duration", "0",
		"--perf-jsonl", perfPath,
		"--out", out,
	}); err != nil {
		t.Fatalf("Perf collect: %v", err)
	}

	names := readTarGzNames(t, out)
	for _, want := range []string{"manifest.json", "perf.jsonl", "summary.json"} {
		if !containsString(names, want) {
			t.Fatalf("bundle names = %v, missing %s", names, want)
		}
	}
}

func TestPerfMountSyncCallsEndpoint(t *testing.T) {
	var gotPath string
	var gotTimeout string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotTimeout = r.URL.Query().Get("timeout")
		_, _ = w.Write([]byte("synced mount\n"))
	}))
	defer ts.Close()

	if err := perfMountSync(ts.URL, 2*time.Second); err != nil {
		t.Fatalf("perfMountSync: %v", err)
	}
	if gotPath != "/debug/drive9/mount/sync" {
		t.Fatalf("path = %q, want /debug/drive9/mount/sync", gotPath)
	}
	if gotTimeout != "2s" {
		t.Fatalf("timeout = %q, want 2s", gotTimeout)
	}
}

func writePerfFixture(t *testing.T, path string) {
	t.Helper()
	data := strings.Join([]string{
		`{"timestamp":"2026-05-20T00:00:00Z","reason":"start","context":{"component":"drive9-fuse"},"runtime":{"goroutines":1,"heap_alloc_bytes":100,"heap_inuse_bytes":200,"heap_objects":3,"stack_inuse_bytes":4,"sys_bytes":500},"process":{"user_cpu_ns":100000000,"system_cpu_ns":50000000,"max_rss_bytes":1000},"fuse_ops":{"write":{"count":1,"bytes":10,"total_ns":1000,"avg_ns":1000,"p50_ns":1000,"p95_ns":1000,"p99_ns":1000,"max_ns":1000}},"remote_ops":{},"counters":{"read_cache_hit":1},"queues":{"commit_pending":1,"commit_pending_bytes":10}}`,
		`{"timestamp":"2026-05-20T00:00:01Z","reason":"stop","context":{"component":"drive9-fuse"},"runtime":{"goroutines":2,"heap_alloc_bytes":300,"heap_inuse_bytes":400,"heap_objects":5,"stack_inuse_bytes":6,"sys_bytes":700},"process":{"user_cpu_ns":300000000,"system_cpu_ns":100000000,"max_rss_bytes":2000},"fuse_ops":{"write":{"count":3,"bytes":30,"total_ns":3000,"avg_ns":1000,"p50_ns":1000,"p95_ns":1000,"p99_ns":1000,"max_ns":1000}},"remote_ops":{},"counters":{"read_cache_hit":3},"queues":{"commit_pending":2,"commit_pending_bytes":20}}`,
		"",
	}, "\n")
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatalf("write perf fixture: %v", err)
	}
}

func readTarGzNames(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open bundle: %v", err)
	}
	defer func() { _ = f.Close() }()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("open gzip: %v", err)
	}
	defer func() { _ = gz.Close() }()
	tr := tar.NewReader(gz)
	var names []string
	for {
		hdr, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("read tar: %v", err)
		}
		names = append(names, hdr.Name)
	}
	return names
}

func containsString(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

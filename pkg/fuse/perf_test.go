package fuse

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/dat9/pkg/client"
)

func TestFusePerfCountersSummary(t *testing.T) {
	perf := newFusePerfCounters(true)
	perf.recordFuseOp(perfFuseLookup, gofuse.OK, 10*time.Millisecond, 0)
	perf.recordFuseOp(perfFuseRead, gofuse.EIO, 20*time.Millisecond, 128)
	perf.recordRemoteOp(perfRemoteRead, nil, 5*time.Millisecond, 128)
	perf.readCacheHit.add(2)
	perf.readCacheMiss.add(1)
	perf.lookupRetryTotal.add(1)
	perf.lookupRetrySuccess.add(1)
	perf.commitEnqueue.add(1)
	perf.commitSuccess.add(1)
	perf.uploaderSubmit.add(1)
	perf.sseChange.add(1)
	perf.notifyEntry.add(1)
	perf.recordLocalPolicy(policyMatchLocalOnly)
	perf.recordLocalPolicy(policyMatchRemoteOverride)
	perf.recordLocalPolicy(policyMatchRemoteDefault)
	perf.gitCleanReadCount.add(3)
	perf.gitCleanTreeHit.add(1)
	perf.gitCleanBlobCacheHit.add(1)
	perf.gitCleanCacheMiss.add(1)
	perf.gitCatFileCount.add(1)
	perf.gitCatFileSlowCount.add(1)
	perf.gitCatFileTotalNS.add(uint64(75 * time.Millisecond))
	perf.gitHydrateStart.add(1)
	perf.gitHydrateSuccess.add(1)
	perf.gitHydrateBytes.add(1024)
	perf.gitHydrateTotalNS.add(uint64(2 * time.Second))
	perf.gitHydrateObjects.add(4)
	perf.gitHydrateObjectBytes.add(512)
	perf.gitHydrateObjectSkipped.add(1)
	perf.gitHydrateObjectMismatch.add(2)
	perf.gitHydrateObjectFallbacks.add(1)
	perf.gitOverlayEnqueue.add(2)
	perf.gitOverlaySync.add(1)
	perf.gitOverlaySuccess.add(3)
	perf.gitOverlayFailure.add(1)
	perf.gitOverlayDrainCount.add(1)
	perf.gitOverlayDrainTotalNS.add(uint64(150 * time.Millisecond))

	snap := perf.snapshot()
	if got := snap.FuseOps["lookup"].count; got != 1 {
		t.Fatalf("lookup count = %d, want 1", got)
	}
	if got := snap.FuseOps["read"].errors; got != 1 {
		t.Fatalf("read errors = %d, want 1", got)
	}
	if got := snap.RemoteOps["read"].bytes; got != 128 {
		t.Fatalf("remote read bytes = %d, want 128", got)
	}
	if got := snap.FuseOps["read"].maxNS; got != uint64(20*time.Millisecond) {
		t.Fatalf("read maxNS = %d, want %d", got, uint64(20*time.Millisecond))
	}
	if got := snap.FuseOps["lookup"].p95NS; got == 0 {
		t.Fatal("lookup p95NS should be populated")
	}
	if got := snap.Counters["read_cache_hit"]; got != 2 {
		t.Fatalf("read cache hits = %d, want 2", got)
	}

	var out bytes.Buffer
	perf.printSummary(&out)
	text := out.String()
	for _, want := range []string{
		"drive9: FUSE perf summary",
		"perf fuse lookup count=1",
		"perf fuse read count=1 errors=1 bytes=128",
		"perf remote read count=1",
		"perf cache read_hit=2 read_miss=1",
		"perf retries lookup_total=1 lookup_success=1",
		"perf commit enqueue=1",
		"perf uploader submit=1",
		"perf sse change=1",
		"perf local_policy local_only=1 remote_override=1 remote_default=1",
		"perf git clean_read=3 tree_hit=1 blob_cache_hit=1 cache_miss=1 cat_file=1 cat_file_slow=1",
		"hydrate_start=1 hydrate_success=1 hydrate_failure=0 hydrate_bytes=1024 hydrate_total=2s",
		"hydrate_objects=4 hydrate_object_bytes=512 hydrate_object_skipped=1 hydrate_object_mismatch=2 hydrate_object_fallbacks=1",
		"perf git_overlay enqueue=2 sync=1 success=3 failure=1 drain_count=1 drain_total=150ms",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("summary missing %q:\n%s", want, text)
		}
	}
}

func TestFusePerfDisabledByDefault(t *testing.T) {
	opts := &MountOptions{
		CacheSize: 1 << 20,
		DirTTL:    time.Second,
	}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
	if fs.perf != nil {
		t.Fatal("perf counters should be nil when disabled")
	}
	fs.notifyEntry(1, "x")
	fs.notifyInode(1)
}

func TestFusePerfCountsNotifyAndSSE(t *testing.T) {
	opts := &MountOptions{
		CacheSize:    1 << 20,
		DirTTL:       time.Second,
		PerfCounters: true,
	}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)
	fs.inodes.Lookup("/file.txt", false, 4, time.Now())

	watcher := &SSEWatcher{fs: fs, actor: "actor-a"}
	watcher.handleEvent(&client.ChangeEvent{Path: "/file.txt", Actor: "actor-a"}, nil)
	watcher.handleEvent(&client.ChangeEvent{Path: "/file.txt", Actor: "actor-b"}, nil)
	watcher.handleEvent(nil, &client.ResetEvent{Reason: "structural_change", Actor: "actor-a"})
	watcher.handleEvent(nil, &client.ResetEvent{Reason: "structural_change", Actor: "actor-b"})

	snap := fs.perf.snapshot()
	if got := snap.Counters["sse_self_filtered"]; got != 2 {
		t.Fatalf("sse self-filtered = %d, want 2", got)
	}
	if got := snap.Counters["sse_change"]; got != 1 {
		t.Fatalf("sse change = %d, want 1", got)
	}
	if got := snap.Counters["sse_reset"]; got != 1 {
		t.Fatalf("sse reset = %d, want 1", got)
	}
	if got := snap.Counters["notify_inode"]; got == 0 {
		t.Fatal("notify inode counter should be incremented by change/reset")
	}
}

func TestCommitQueuePerfCounters(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","revision":9}`))
	}))
	defer ts.Close()

	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := shadow.WriteFull("/ok.txt", []byte("data"), 8); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.PutWithBaseRev("/ok.txt", 4, PendingOverwrite, 8); err != nil {
		t.Fatal(err)
	}

	perf := newFusePerfCounters(true)
	cq := NewCommitQueue(newTestClient(ts.URL), shadow, pending, nil, 1, 8)
	cq.SetPerfCounters(perf)
	if err := cq.Enqueue(&CommitEntry{
		Path:    "/ok.txt",
		BaseRev: 8,
		Size:    4,
		Kind:    PendingOverwrite,
	}); err != nil {
		t.Fatal(err)
	}
	cq.DrainAll()

	snap := perf.snapshot()
	if got := snap.Counters["commit_enqueue"]; got != 1 {
		t.Fatalf("commit enqueue = %d, want 1", got)
	}
	if got := snap.Counters["commit_success"]; got != 1 {
		t.Fatalf("commit success = %d, want 1", got)
	}
	if got := snap.RemoteOps["write"].count; got != 1 {
		t.Fatalf("remote write count = %d, want 1", got)
	}
	if got := snap.Counters["commit_drain_count"]; got != 1 {
		t.Fatalf("commit drain count = %d, want 1", got)
	}
}

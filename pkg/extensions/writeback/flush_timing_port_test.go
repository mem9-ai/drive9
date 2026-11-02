package writeback

// Flush-timing ports from v1 pkg/fuse/writeback_test.go: the write-back
// flush boundary snapshots locally and returns without a PUT (the .dat/.meta
// track owns the landing, submitted exactly once at Release); close-sync
// pays the remote acknowledgement inside Flush itself; the flush debouncer
// coalesces rapid flushes of one path into a single landing and Release
// cancels the pending callback; and the O_SYNC/O_DSYNC open promotion
// variants across the remaining track shapes. The staged-ledger lifecycle
// rows (multiple files, write-between-flush-and-release, large images)
// already live in writeback_v1_port_test.go; this file pins the same
// boundaries over the small-file write-cache track and the debouncer.

import (
	"context"
	"fmt"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// newWriteCacheStack wires the small-file write-back track (.dat/.meta
// cache + background uploader) without a staging ledger, matching the v1
// SetWriteBack mount shape those tests exercised.
func newWriteCacheStack(t *testing.T, policy Policy, b *testbackend.Backend) (*fs.Chain, *Writeback, *staging.WriteBackCache) {
	t.Helper()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{Policy: policy, Client: cl, WriteCache: cache})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	return ch, wb, cache
}

// v1 TestFlush_WriteBack_SmallFile: Flush() writes the image to the local
// cache and returns without any HTTP upload; Release submits the snapshot
// exactly once, and the successful upload cleans the cache entry.
func TestPortFlushWriteBackSnapshotStaysLocal(t *testing.T) {
	b := testbackend.NewBackend()
	ch, wb, cache := newWriteCacheStack(t, PolicyWriteBack, b)
	ctx := context.Background()

	h := open(t, ch, "/wb_test.txt")
	if err := h.Write(ctx, 0, []byte("write-back test")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got := b.Requests()["PUT /v1/fs/wb_test.txt"]; got != 0 {
		t.Fatalf("Flush made %d HTTP PUT calls — should use the write-back cache", got)
	}
	data, ok := cache.Get("/wb_test.txt")
	if !ok || string(data) != "write-back test" {
		t.Fatalf("cached data = %q ok=%v, want the flushed image", data, ok)
	}

	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.uploader.WaitIdle(ctx); err != nil {
		t.Fatal(err)
	}
	if got := b.Requests()["PUT /v1/fs/wb_test.txt"]; got != 1 {
		t.Fatalf("PUT calls after Release + drain = %d, want 1", got)
	}
	if n, ok := b.Node("/wb_test.txt"); !ok || string(n.Data) != "write-back test" {
		t.Fatalf("remote after drain = %q ok=%v", n.Data, ok)
	}
	if _, ok := cache.Get("/wb_test.txt"); ok {
		t.Fatal("cache entry should be removed after the successful upload")
	}
}

// v1 TestFlush_WriteBack_Lifecycle: the Create → Write → Flush → Release
// lifecycle completes fast (Flush is local disk only); the uploader drains
// the acknowledged image afterwards.
func TestPortFlushWriteBackLifecycleFastClose(t *testing.T) {
	b := testbackend.NewBackend()
	ch, wb, _ := newWriteCacheStack(t, PolicyWriteBack, b)
	ctx := context.Background()

	done := make(chan error, 1)
	go func() {
		h := open(t, ch, "/lifecycle.txt")
		if err := h.Write(ctx, 0, []byte("lifecycle data\n")); err != nil {
			done <- err
			return
		}
		if err := h.Flush(ctx); err != nil {
			done <- err
			return
		}
		done <- h.Release(ctx)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Create→Write→Flush→Release timed out (Flush must be local-disk only)")
	}
	if err := wb.uploader.WaitIdle(ctx); err != nil {
		t.Fatal(err)
	}
	if n, ok := b.Node("/lifecycle.txt"); !ok || string(n.Data) != "lifecycle data\n" {
		t.Fatalf("uploaded = %q ok=%v, want lifecycle data", n.Data, ok)
	}
}

// v1 TestFlush_WriteBack_MultipleFiles over the write-cache track: several
// concurrent files each land their own flushed snapshot after Release.
func TestPortFlushWriteBackMultipleFilesTrack(t *testing.T) {
	b := testbackend.NewBackend()
	ch, wb, _ := newWriteCacheStack(t, PolicyWriteBack, b)
	ctx := context.Background()

	const N = 10
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := fmt.Sprintf("/multi_%d.txt", idx)
			h := open(t, ch, name)
			if err := h.Write(ctx, 0, []byte(fmt.Sprintf("data-%d", idx))); err != nil {
				t.Errorf("write %s: %v", name, err)
				return
			}
			if err := h.Flush(ctx); err != nil {
				t.Errorf("flush %s: %v", name, err)
				return
			}
			if err := h.Release(ctx); err != nil {
				t.Errorf("release %s: %v", name, err)
			}
		}(i)
	}
	wg.Wait()
	if err := wb.uploader.WaitIdle(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < N; i++ {
		name := fmt.Sprintf("/multi_%d.txt", i)
		if n, ok := b.Node(name); !ok || string(n.Data) != fmt.Sprintf("data-%d", i) {
			t.Fatalf("%s = %q ok=%v", name, n.Data, ok)
		}
	}
}

// v1 TestFlush_WriteBack_WriteBetweenFlushAndRelease (the data-loss
// regression): a Write landing between Flush and Release must not let the
// stale cache snapshot win — the remote's final image is the latest buffer.
func TestPortFlushWriteBackWriteBetweenFlushAndReleaseLatestWins(t *testing.T) {
	b := testbackend.NewBackend()
	ch, wb, _ := newWriteCacheStack(t, PolicyWriteBack, b)
	ctx := context.Background()

	h := open(t, ch, "/race.txt")
	if err := h.Write(ctx, 0, []byte("version1")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(ctx, 0, []byte("version2-longer")); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.uploader.WaitIdle(ctx); err != nil {
		t.Fatal(err)
	}
	n, ok := b.Node("/race.txt")
	if !ok || string(n.Data) != "version2-longer" {
		t.Fatalf("uploaded = %q ok=%v, want version2-longer (stale cache snapshot was used)", n.Data, ok)
	}
	// The remote image must not regress afterwards either: the Flush
	// snapshot still sits in the cache and a late drain must not resurrect
	// it (its stale CAS base is rejected server-side).
	if err := wb.uploader.WaitIdle(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/race.txt"); string(n.Data) != "version2-longer" {
		t.Fatalf("remote regressed to %q after the second drain", n.Data)
	}
}

// v1 TestFlush_CloseSyncUploadsBeforeReturning: close-sync's Flush uploads
// before returning — exactly one PUT with the payload, no residual cache
// entry, and Release adds no second upload.
func TestPortFlushCloseSyncUploadsExactlyOnce(t *testing.T) {
	b := testbackend.NewBackend()
	ch, wb, cache := newWriteCacheStack(t, PolicyCloseSync, b)
	ctx := context.Background()

	h := open(t, ch, "/close_sync.txt")
	if err := h.Write(ctx, 0, []byte("close-sync data")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got := b.Requests()["PUT /v1/fs/close_sync.txt"]; got != 1 {
		t.Fatalf("PUT calls after Flush = %d, want 1", got)
	}
	if n, _ := b.Node("/close_sync.txt"); string(n.Data) != "close-sync data" {
		t.Fatalf("uploaded = %q", n.Data)
	}
	if _, ok := cache.Get("/close_sync.txt"); ok {
		t.Fatal("close-sync Flush should not leave a write-back cache entry")
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.uploader.WaitIdle(ctx); err != nil {
		t.Fatal(err)
	}
	if got := b.Requests()["PUT /v1/fs/close_sync.txt"]; got != 1 {
		t.Fatalf("PUT calls after Release = %d, want still 1", got)
	}
}

// v1 flushHandleDebounced coalescing (防抖合并): rapid flushes of one path
// under the writeback policy merge into a single landing — flush stages
// synchronously (locally durable), and the debounced callback pays one
// remote commit for the merged window.
func TestPortFlushDebounceCoalescesRapidFlushes(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	ledger, err := staging.NewStaging(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledger.Close)
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, Staging: ledger, FlushDebounce: 150 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	h := open(t, ch, "/debounce.txt")
	for i := 0; i < 5; i++ {
		if err := h.Write(ctx, int64(i*7), []byte(fmt.Sprintf("chunk-%d", i))); err != nil {
			t.Fatal(err)
		}
		if err := h.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond) // inside the debounce window
	}
	// Nothing landed yet: the debounced callback has not fired.
	if _, ok := b.Node("/debounce.txt"); ok {
		t.Fatal("debounced commit landed inside the merge window")
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	n, ok := b.Node("/debounce.txt")
	if !ok {
		t.Fatal("merged commit never landed")
	}
	want := "chunk-0chunk-1chunk-2chunk-3chunk-4"
	if string(n.Data) != want {
		t.Fatalf("merged image = %q, want %q", n.Data, want)
	}
	if got := b.Requests()["PUT /v1/fs/debounce.txt"]; got != 1 {
		t.Fatalf("PUT count = %d, want 1 (rapid flushes must coalesce into one upload)", got)
	}
}

// v1 Release cancels the pending debounce (防抖取消) and commits
// synchronously: close(2) semantics hold without waiting out the debounce
// window, and the cancelled callback never issues a second landing.
func TestPortReleaseCancelsPendingDebounce(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	ledger, err := staging.NewStaging(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledger.Close)
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, Staging: ledger, FlushDebounce: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	h := open(t, ch, "/cancel.txt")
	if err := h.Write(ctx, 0, []byte("cancelled")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err) // schedules the debounced commit
	}
	// Release before the 1s window elapses: the debounce is cancelled and
	// Release owns the commit boundary.
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if n, ok := b.Node("/cancel.txt"); !ok || string(n.Data) != "cancelled" {
		t.Fatalf("remote after release = %q ok=%v", n.Data, ok)
	}
	// The cancelled callback must not fire a second commit later.
	time.Sleep(300 * time.Millisecond)
	if got := b.Requests()["PUT /v1/fs/cancel.txt"]; got != 1 {
		t.Fatalf("PUT count after debounce window = %d, want 1", got)
	}
	if n, _ := b.Node("/cancel.txt"); string(n.Data) != "cancelled" {
		t.Fatalf("remote changed after the cancelled debounce: %q", n.Data)
	}
}

// v1 TestFlushHandle_Path2_DebounceAlreadyFiredSkipsUpload (public-path
// shape): once a forced boundary committed the buffer and cleared dirty,
// the debounced callback that fires afterwards is a no-op — the clean
// handle never re-lands the same image.
func TestPortDebounceFiredAfterForcedCommitSkips(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	ledger, err := staging.NewStaging(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledger.Close)
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, Staging: ledger, FlushDebounce: 40 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	h := open(t, ch, "/fired.txt")
	if err := h.Write(ctx, 0, []byte("forced")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err) // schedules the debounced commit
	}
	// A forced fsync boundary commits (and clears dirty) while the debounce
	// timer is still pending.
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if n, ok := b.Node("/fired.txt"); !ok || string(n.Data) != "forced" {
		t.Fatalf("remote after forced fsync = %q ok=%v", n.Data, ok)
	}
	// Let the debounce timer fire well past its window: the callback sees a
	// clean handle and must not re-upload.
	time.Sleep(250 * time.Millisecond)
	if got := b.Requests()["PUT /v1/fs/fired.txt"]; got > 1 {
		t.Fatalf("PUT count = %d, want <= 1 (already-fired debounce must skip the clean handle)", got)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/fired.txt"); string(n.Data) != "forced" {
		t.Fatalf("final remote = %q", n.Data)
	}
}

// v1 TestOSyncOpenPromotesWriteBackToWriteSync over the write-cache track
// (the one policy/track combination batch 1's flag matrix left open): with
// the .dat/.meta track armed, an O_SYNC open still commits inline per Write
// — the write-back Flush branch must not intercept a forceSync handle, and
// a clean Flush adds no second PUT.
func TestPortSyncOpenFlagsWithWriteCacheTrackStillInline(t *testing.T) {
	b := testbackend.NewBackend()
	ch, wb, _ := newWriteCacheStack(t, PolicyWriteBack, b)
	ctx := context.Background()

	res, oerr := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/osync.txt",
		Flags: uint32(syscall.O_CREAT | syscall.O_RDWR | syscall.O_SYNC), Mode: 0o644})
	if oerr != nil || res.Handle == nil {
		t.Fatalf("open: %v handle=%v", oerr, res.Handle)
	}
	h := res.Handle
	if bh, ok := h.(*bufferingHandle); !ok || !bh.forceSync {
		t.Fatal("O_SYNC open did not arm forceSync on the buffering handle")
	}
	if err := h.Write(ctx, 0, []byte("osync data")); err != nil {
		t.Fatal(err)
	}
	if got := b.Requests()["PUT /v1/fs/osync.txt"]; got != 1 {
		t.Fatalf("PUT calls after O_SYNC Write = %d, want 1 (per-write inline commit)", got)
	}
	if n, _ := b.Node("/osync.txt"); string(n.Data) != "osync data" {
		t.Fatalf("remote after O_SYNC Write = %q", n.Data)
	}
	// v1 TestWriteSyncUploadsBeforeWriteReturns's flush row: a Flush of the
	// clean handle must not re-land the committed image.
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got := b.Requests()["PUT /v1/fs/osync.txt"]; got != 1 {
		t.Fatalf("PUT calls after clean Flush = %d, want still 1", got)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.uploader.WaitIdle(ctx); err != nil {
		t.Fatal(err)
	}
	if got := b.Requests()["PUT /v1/fs/osync.txt"]; got != 1 {
		t.Fatalf("PUT calls after Release = %d, want still 1", got)
	}
}

// v1 TestWriteSyncUploadsBeforeWriteReturns (write-sync row): every Write
// commits before returning and a clean Flush is a no-op.
func TestPortWriteSyncCleanFlushIsNoop(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newStack(t, PolicyWriteSync, b)
	ctx := context.Background()

	h := open(t, ch, "/write_sync.txt")
	if err := h.Write(ctx, 0, []byte("write-sync data")); err != nil {
		t.Fatal(err)
	}
	if got := b.Requests()["PUT /v1/fs/write_sync.txt"]; got != 1 {
		t.Fatalf("PUT calls after Write = %d, want 1", got)
	}
	if n, _ := b.Node("/write_sync.txt"); string(n.Data) != "write-sync data" {
		t.Fatalf("uploaded = %q", n.Data)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got := b.Requests()["PUT /v1/fs/write_sync.txt"]; got != 1 {
		t.Fatalf("PUT calls after clean Flush = %d, want still 1", got)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if got := b.Requests()["PUT /v1/fs/write_sync.txt"]; got != 1 {
		t.Fatalf("PUT calls after Release = %d, want still 1", got)
	}
}

// v1 FlushAll (mount drain of an OPEN handle): no Release boundary is
// coming, so Drain publishes the buffered image through the durable track
// instead of merely snapshotting it.
func TestPortDrainFlushesOpenHandle(t *testing.T) {
	b := testbackend.NewBackend()
	ch, wb := newStagedStack(t, PolicyWriteBack, b)
	ctx := context.Background()

	h := open(t, ch, "/drainme.txt")
	if err := h.Write(ctx, 0, []byte("open-handle image")); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if n, ok := b.Node("/drainme.txt"); !ok || string(n.Data) != "open-handle image" {
		t.Fatalf("remote after drain = %q ok=%v, want the OPEN handle's image", n.Data, ok)
	}
	// The handle stays usable after the drain published its image.
	if err := h.Write(ctx, 17, []byte("!")); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/drainme.txt"); string(n.Data) != "open-handle image!" {
		t.Fatalf("remote after post-drain write = %q", n.Data)
	}
	_ = syscall.O_SYNC
}

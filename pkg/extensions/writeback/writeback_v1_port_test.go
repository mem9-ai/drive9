package writeback

// Behavior rows ported from v1 pkg/fuse/writeback_test.go (the
// FUSE-observable subset; the WriteBackCache/.dat/.meta uploader machinery
// those tests exercised was not ported and its rows are intentionally
// absent): close-sync truncation read-your-writes, writeback lifecycle
// (multi-file, write-between-flush-and-release, large images), pending
// visibility on re-open, fsync idempotence, and write-sync failure retry.

import (
	"context"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

func newStagedStack(t *testing.T, policy Policy, b *testbackend.Backend) (*fs.Chain, *Writeback) {
	t.Helper()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	ledger, err := staging.NewStaging(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledger.Close)
	wb, err := New(Options{Policy: policy, Client: cl, Staging: ledger})
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
	return ch, wb
}

func dispatchOpen(t *testing.T, ch *fs.Chain, path string, flags uint32) fs.Handle {
	t.Helper()
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: path, Flags: flags, Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if res.Handle == nil {
		t.Fatal("no handle")
	}
	return res.Handle
}

// v1 TestCloseSyncOpenTruncateOverwriteReadSeesLatestBytes: after
// open(O_TRUNC)+overwrite on close-sync, reads (fresh handle) serve the
// latest bytes, not the pre-truncation tail.
func TestPortCloseSyncOpenTruncateReadSeesLatestBytes(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/big", []byte("0123456789ABCDEFGHIJ"))
	ch, _ := newStagedStack(t, PolicyCloseSync, b)
	ctx := context.Background()

	h := dispatchOpen(t, ch, "/big", uint32(syscall.O_RDWR))
	// kernel delivers open(O_TRUNC) as SETATTR(size=0) on the live handle.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/big", Size: 0}); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(ctx, 0, []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	reader := dispatchOpen(t, ch, "/big", uint32(syscall.O_RDONLY))
	buf := make([]byte, 16)
	n, err := reader.Read(ctx, 0, buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "new" {
		t.Fatalf("read after truncate-overwrite = %q, want new", buf[:n])
	}
}

// v1 TestFlush_WriteBack_MultipleFiles + TestFlush_WriteBack_WriteBetween-
// FlushAndRelease: flushing several files lands each staged image, and a
// write between Flush and Release re-dirties so Release commits the latest
// image.
func TestPortWriteBackLifecycleMultipleFiles(t *testing.T) {
	b := testbackend.NewBackend()
	ch, wb := newStagedStack(t, PolicyWriteBack, b)
	ctx := context.Background()

	for _, name := range []string{"/a", "/b", "/c"} {
		h := dispatchOpen(t, ch, name, uint32(syscall.O_CREAT|syscall.O_RDWR))
		if err := h.Write(ctx, 0, []byte("content-"+name[1:])); err != nil {
			t.Fatal(err)
		}
		if err := h.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"/a", "/b", "/c"} {
		if n, ok := b.Node(name); !ok || string(n.Data) != "content-"+name[1:] {
			t.Fatalf("%s = %q ok=%v", name, n.Data, ok)
		}
	}
}

func TestPortWriteBackWriteBetweenFlushAndRelease(t *testing.T) {
	b := testbackend.NewBackend()
	ch, wb := newStagedStack(t, PolicyWriteBack, b)
	ctx := context.Background()

	h := dispatchOpen(t, ch, "/w", uint32(syscall.O_CREAT|syscall.O_RDWR))
	if err := h.Write(ctx, 0, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(ctx, 5, []byte("-second")); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if n, ok := b.Node("/w"); !ok || string(n.Data) != "first-second" {
		t.Fatalf("/w = %q ok=%v, want first-second", n.Data, ok)
	}
}

// v1 TestReopenAfterClose_ReadsPendingCache: a path flushed into the async
// queue is visible (its own queued image) to a re-open before it lands.
func TestPortReopenSeesQueuedImage(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newStagedStack(t, PolicyWriteBack, b)
	ctx := context.Background()

	h := dispatchOpen(t, ch, "/p", uint32(syscall.O_CREAT|syscall.O_RDWR))
	if err := h.Write(ctx, 0, []byte("pending-image")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	reader := dispatchOpen(t, ch, "/p", uint32(syscall.O_RDONLY))
	buf := make([]byte, 32)
	n, err := reader.Read(ctx, 0, buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "pending-image" {
		t.Fatalf("re-open read = %q, want pending-image", buf[:n])
	}
	if err := reader.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

// v1 TestFsync_NoDuplicateUpload: repeated fsyncs of an unchanged buffer
// do not re-upload (write count for the path stays at one).
func TestPortFsyncNoDuplicateUpload(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newStagedStack(t, PolicyCloseSync, b)
	ctx := context.Background()

	h := dispatchOpen(t, ch, "/d", uint32(syscall.O_CREAT|syscall.O_RDWR))
	if err := h.Write(ctx, 0, []byte("once")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := h.Sync(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if got := b.Requests()["PUT /v1/fs/d"]; got > 1 {
		t.Fatalf("PUT count = %d, want <= 1", got)
	}
}

// v1 TestWriteSyncFailureRollsBackDirtyState / TestFlushCloseSyncRemote-
// FailureKeepsDirtyForRetry: a failed commit keeps the buffer dirty so the
// next boundary retries with the same image.
func TestPortCommitFailureRetriesOnNextBoundary(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/r", []byte("base"))
	b.ForceConflict("/r", 10) // exhaust the inline retry budget
	ch, _ := newStagedStack(t, PolicyWriteSync, b)
	ctx := context.Background()

	h := dispatchOpen(t, ch, "/r", uint32(syscall.O_RDWR))
	if err := h.Write(ctx, 4, []byte("NEW")); err == nil {
		t.Fatal("write-sync write with forced conflicts unexpectedly succeeded")
	}
	// The buffered image survives the failed boundary (v1 keeps dirty for
	// retry); once conflicts clear, a flush lands it.
	b.ForceConflict("/r", 0)
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/r"); string(n.Data) != "baseNEW" {
		t.Fatalf("/r = %q, want baseNEW", n.Data)
	}
}

// v1 TestFlush_WriteBack_NoWriteBack_LargeFile: images beyond the preload
// limit still commit whole at the release boundary.
func TestPortWriteBackLargeImageCommitsWhole(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	defer ts.Close()
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, PreloadLimit: 1 << 20})
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

	h := dispatchOpen(t, ch, "/l", uint32(syscall.O_CREAT|syscall.O_RDWR))
	payload := make([]byte, 3<<20)
	for i := range payload {
		payload[i] = byte(i)
	}
	if err := h.Write(ctx, 0, payload); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ := b.Node("/l")
	if len(n.Data) != len(payload) {
		t.Fatalf("committed %d bytes, want %d", len(n.Data), len(payload))
	}
	for i := 0; i < len(payload); i += 65537 {
		if n.Data[i] != payload[i] {
			t.Fatalf("byte %d = %d, want %d", i, n.Data[i], payload[i])
		}
	}
	_ = time.Second
}

// v1 interrupt-safe mutations row: a commit whose caller context is already
// cancelled still lands remotely — the commit context detaches from the FUSE
// cancel channel (default; --legacy-interruptible-mutations rebinds it).
func TestPortCommitDetachedFromCancelledCaller(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newStagedStack(t, PolicyCloseSync, b)

	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: "/c", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Write(context.Background(), 0, []byte("durable")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := res.Handle.Flush(ctx); err != nil {
		t.Fatalf("flush with cancelled caller: %v", err)
	}
	if n, ok := b.Node("/c"); !ok || string(n.Data) != "durable" {
		t.Fatalf("remote = %q ok=%v, want durable", n.Data, ok)
	}
}

// v1 streaming-upload row: a large sequential overwrite beyond the preload
// cap streams parts (multipart) while the write stream continues; the
// commit boundary finishes the upload with the true total size.
func TestPortStreamingUploadLargeSequentialWrite(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/stream.bin", make([]byte, 4<<20))
	ch, _ := newStagedStack(t, PolicyCloseSync, b)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/stream.bin", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if bh, ok := h.(*bufferingHandle); ok {
		bh.w.preload = 1 << 20 // force the streaming arm
	}
	payload := make([]byte, 6<<20)
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	if err := h.Write(ctx, 0, payload); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ := b.Node("/stream.bin")
	if int64(len(n.Data)) != int64(len(payload)) {
		t.Fatalf("streamed size = %d, want %d", len(n.Data), len(payload))
	}
	for i := 0; i < len(payload); i += 65531 {
		if n.Data[i] != payload[i] {
			t.Fatalf("streamed byte %d = %d, want %d", i, n.Data[i], payload[i])
		}
	}
}

// v1 TestUnlink_PendingNew_SkipsRemoteDelete, adapted: unlinking a path whose
// only existence is a staged (not yet landed) write-cache image deletes the
// staged state without touching the remote (no DELETE for a missing path).
func TestPortUnlinkPendingNewSkipsRemoteDelete(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := newTestClient(ts.URL)
	cache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCache: cache})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	if err := cache.Put("/ghost.txt", []byte("local"), 5, staging.PendingNew); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/ghost.txt"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := cache.GetMeta("/ghost.txt"); ok {
		t.Fatal("staged pending-new survived its own unlink")
	}
	if _, ok := b.Node("/ghost.txt"); ok {
		t.Fatal("remote delete issued for a never-landed path")
	}
}

// v1 TestRmdir_CleansPendingDescendants, adapted: removing a directory drops
// every staged descendant under its prefix.
func TestPortRmdirCleansPendingDescendants(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/dir", 0o755)
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := newTestClient(ts.URL)
	cache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCache: cache})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	for _, p := range []string{"/dir/a.txt", "/dir/sub/b.txt"} {
		if err := cache.Put(p, []byte("x"), 1, staging.PendingNew); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRmdir, Path: "/dir"}); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"/dir/a.txt", "/dir/sub/b.txt"} {
		if _, ok := cache.GetMeta(p); ok {
			t.Fatalf("staged descendant %s survived rmdir", p)
		}
	}
}

// v1 TestRename_Directory_MigratesPendingDescendants, adapted: renaming a
// directory moves the staged descendants onto the new prefix.
func TestPortRenameDirectoryMigratesPendingDescendants(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/old", 0o755)
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := newTestClient(ts.URL)
	cache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCache: cache})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	if err := cache.Put("/old/nested/file.txt", []byte("x"), 1, staging.PendingNew); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/old", Path2: "/new"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := cache.GetMeta("/new/nested/file.txt"); !ok {
		t.Fatal("staged descendant did not migrate to the new prefix")
	}
	if _, ok := cache.GetMeta("/old/nested/file.txt"); ok {
		t.Fatal("staged descendant still keyed under the old prefix")
	}
}

// v1 TestFlush_SkipsRedundantCacheWrite, adapted: a flush with no dirty
// buffer does not restage the image (write-cache Put count stays flat).
func TestPortFlushSkipsRedundantCacheWrite(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/clean.txt", []byte("server"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := newTestClient(ts.URL)
	cache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCache: cache})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	h := dispatchOpen(t, ch, "/clean.txt", uint32(syscall.O_RDWR))
	before := len(b.Requests())
	for i := 0; i < 3; i++ {
		if err := h.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	after := len(b.Requests())
	if after != before {
		t.Fatalf("flush of a clean handle issued remote work: %d -> %d", before, after)
	}
}

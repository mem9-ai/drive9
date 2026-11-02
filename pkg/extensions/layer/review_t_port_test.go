package layer

// qiffang seventeenth round: FATTR_FH resolves through UnwrapHandle
// decorators; a directory rename moves open child files; O_CREAT claims
// a whiteouted name fresh; an unlink that won the mutation lock
// discards the waiting commit; large commits stream in bounded memory.

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// deco wraps a handle the way the read cache (and localize's accounting
// wrapper) does — the FATTR_FH resolution must walk through it.
type deco struct{ inner fs.Handle }

func (d deco) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	return d.inner.Read(ctx, off, dest)
}
func (d deco) Write(ctx context.Context, off int64, data []byte) error {
	return d.inner.Write(ctx, off, data)
}
func (d deco) Sync(ctx context.Context) error  { return d.inner.Sync(ctx) }
func (d deco) Flush(ctx context.Context) error { return d.inner.Flush(ctx) }
func (d deco) Release(ctx context.Context) error {
	return d.inner.Release(ctx)
}
func (d deco) UnwrapHandle() fs.Handle { return d.inner }

// FATTR_FH through a decorated handle reaches the NAMED descriptor, not
// the per-path newest claim.
func TestPortFDTruncateResolvesDecoratedHandle(t *testing.T) {
	_, ch, _ := pStack(t)
	ctx := context.Background()

	open := func() fs.Handle {
		res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
		if err != nil {
			t.Fatal(err)
		}
		return res.Handle
	}
	h1, h2 := open(), open()
	if err := h1.Write(ctx, 0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/f", Size: 2, Handle: deco{h1}}); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 5)
	if n, err := h1.Read(ctx, 0, got); err != nil || string(got[:n]) != "he" {
		t.Fatalf("named (decorated) descriptor read = %q (%d) err=%v, want he", got[:n], n, err)
	}
	_ = h2
}

// A directory rename moves every open CHILD file: its commit lands under
// the new parent, and the old path stays gone.
func TestPortDirectoryRenameMovesChildHandles(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/d/f", []byte("hello"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	_, ch := mStack(t, ts)
	ctx := context.Background()

	// Own the directory as a LAYER dir (a base-only prefix answers 404
	// to the rename's directory probe).
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpMkdir, Path: "/d", Mode: 0o755}); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/d/f", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if err := h.Write(ctx, 0, []byte("A")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/d", Path2: "/new"}); err != nil {
		t.Fatalf("rename directory: %v", err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatalf("sync through the child descriptor: %v", err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	rd, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/new/f", Offset: 0, Limit: 8})
	if err != nil || string(rd.Data) != "Aello" {
		t.Fatalf("/new/f = %q err=%v, want Aello", rd.Data, err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/d/f"}); err == nil {
		t.Fatal("old parent path resurrected by the child's sync")
	}
}

// A create over a whiteout owns the name fresh: the deleted base object
// is neither served nor inherited.
func TestPortCreateOverWhiteoutClaimsFreshIdentity(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("old"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	_, ch := mStack(t, ts)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/f"}); err != nil {
		t.Fatal(err)
	}
	// Recreate the deleted name.
	res2, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatalf("create over whiteout: %v", err)
	}
	if err := res2.Handle.Write(ctx, 0, []byte("NEW")); err != nil {
		t.Fatal(err)
	}
	if err := res2.Handle.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := res2.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	rd, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/f", Offset: 0, Limit: 8})
	if err != nil || string(rd.Data) != "NEW" {
		t.Fatalf("recreated /f = %q err=%v, want NEW (fresh identity, no base inheritance)", rd.Data, err)
	}
}

// An unlink that holds the mutation lock anonymizes the fd and lands the
// whiteout BEFORE the waiting sync acquires the lock: the commit
// re-verifies and discards its snapshot instead of resurrecting the name.
func TestPortSyncAfterUnlinkedLockWindowDiscardsSnapshot(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("hello"))
	b.DelayLayerEntriesForTest(150 * time.Millisecond)
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	_, ch := mStack(t, ts)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if err := h.Write(ctx, 0, []byte("A")); err != nil {
		t.Fatal(err)
	}
	unlinkErr := make(chan error, 1)
	go func() {
		_, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/f"})
		unlinkErr <- err
	}()
	// Let the unlink take the lock and park in its delayed whiteout POST;
	// the sync then captures its dirty snapshot and waits on the lock.
	time.Sleep(50 * time.Millisecond)
	syncErr := make(chan error, 1)
	go func() { syncErr <- h.Sync(ctx) }()
	if err := <-unlinkErr; err != nil {
		t.Fatalf("unlink: %v", err)
	}
	<-syncErr
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/f"}); err == nil {
		t.Fatal("unlinked name resurrected by the sync that waited out the lock")
	}
}

// Large images commit through the bounded streaming path (chunked reads
// over the buffer; no whole-image materialization) and read back intact
// through the lazy object-backed overlay.
func TestPortLargeCommitStreamsAndReadsBack(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	l, ch := mStack(t, ts)
	// Force the streamed path with the test knob.
	l.mu.Lock()
	l.inlineMaxOverride = 8
	l.mu.Unlock()
	ctx := context.Background()

	image := make([]byte, 64*1024)
	for i := range image {
		image[i] = byte(i % 251)
	}
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/big", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Write(ctx, 0, image); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	rd, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/big", Offset: 0, Limit: int64(len(image))})
	if err != nil {
		t.Fatal(err)
	}
	if len(rd.Data) != len(image) {
		t.Fatalf("streamed commit readback = %d bytes, want %d", len(rd.Data), len(image))
	}
	for i := range image {
		if rd.Data[i] != image[i] {
			t.Fatalf("streamed commit byte %d = %d, want %d", i, rd.Data[i], image[i])
		}
	}
}

// A partial write to a LARGE existing file must not zero the unloaded
// tail: the streamed spill loads retained base parts instead of treating
// them as sparse holes (16MiB of B with one modified first byte
// persisted 8MiB of zeros before).
func TestPortStreamedPartialWriteKeepsUnloadedTail(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	l, ch := mStack(t, ts)
	l.mu.Lock()
	l.inlineMaxOverride = 8
	l.mu.Unlock()
	ctx := context.Background()

	full := bytes.Repeat([]byte("B"), 16<<20)
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/big", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle.(*layerHandle)
	if err := h.Write(ctx, 0, full); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	// Reopen (fresh buffer, everything unloaded) and modify one byte.
	res2, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/big", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h2 := res2.Handle.(*layerHandle)
	if err := h2.Write(ctx, 0, []byte("A")); err != nil {
		t.Fatal(err)
	}
	if err := h2.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	// The persisted image keeps the B tail.
	rd, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/big", Offset: 0, Limit: int64(len(full))})
	if err != nil {
		t.Fatal(err)
	}
	if len(rd.Data) != len(full) {
		t.Fatalf("readback = %d bytes, want %d", len(rd.Data), len(full))
	}
	if rd.Data[0] != 'A' {
		t.Fatalf("first byte = %q, want A", rd.Data[0])
	}
	for i := 1; i < len(full); i++ {
		if rd.Data[i] != 'B' {
			t.Fatalf("tail byte %d = %q, want B (unloaded tail was zeroed)", i, rd.Data[i])
		}
	}
}

// A rename that aborts the commit (moved fd) clears its landing marker:
// the fd's NEW path must not stay permanently deferred in the event
// sweep — a later foreign upsert under that path fences the fd's sync.
func TestPortAbortedLandingClearsDeferral(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/old", []byte("hello"))
	b.DelayLayerEntriesForTest(150 * time.Millisecond)
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	l, ch := mStack(t, ts)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/old", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle.(*layerHandle)
	if err := h.Write(ctx, 0, []byte("A")); err != nil {
		t.Fatal(err)
	}
	renameErr := make(chan error, 1)
	go func() {
		_, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/old", Path2: "/new"})
		renameErr <- err
	}()
	time.Sleep(50 * time.Millisecond)
	syncErr := make(chan error, 1)
	go func() { syncErr <- h.Sync(ctx) }()
	if err := <-renameErr; err != nil {
		t.Fatal(err)
	}
	<-syncErr // EAGAIN (moved) — the aborted landing must still be done
	h.mu.Lock()
	landing := h.landing
	h.mu.Unlock()
	if landing {
		t.Fatal("aborted (moved) commit left the landing marker set — the path stays deferred forever")
	}
	// Land our image at the new name, then a FOREIGN upsert under it:
	// with the marker cleared, the sweep fences the dirty fd.
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(ctx, 0, []byte("Z")); err != nil {
		t.Fatal(err)
	}
	postLayerEntry(t, ts, "L1", "/new", "upsert", "FOREIGN")
	l.refreshEventsForTest(ctx)
	if err := h.Write(ctx, 1, []byte("!")); err == nil {
		t.Fatal("write over a foreign tip succeeded after an aborted-landing cycle — deferral leaked")
	}
}

// qiffang nineteenth round: hardlinks publish INSIDE the layer (never
// the base plane), object-backed sources hydrate before linking, rmdir
// judges the merged view, and a locally-unlinked fd keeps reading.
func TestPortLinkBaseSourceStaysInLayer(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/src", []byte("base-data"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	l, ch := mStack(t, ts)
	ctx := context.Background()

	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpLink, Path: "/src", Path2: "/dest"}); err != nil {
		t.Fatalf("link: %v", err)
	}
	// The target is a layer entry, NOT a base mutation.
	kind, _, _, content, ok := l.lookup("/dest")
	if !ok || kind != "file" || string(content) != "base-data" {
		t.Fatalf("layer /dest = kind=%s content=%q ok=%v, want the base image", kind, content, ok)
	}
	if _, baseHas := b.Node("/dest"); baseHas {
		t.Fatal("base plane gained /dest from a layer link (uncommitted layer state leaked)")
	}
}

func TestPortLinkObjectBackedSourceHydrates(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	l, ch := mStack(t, ts)
	l.mu.Lock()
	l.inlineMaxOverride = 8
	l.mu.Unlock()
	ctx := context.Background()

	image := bytes.Repeat([]byte("Z"), 256*1024)
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/big", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Write(ctx, 0, image); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	// The entry is object-backed and unmaterialized; linking must hydrate.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpLink, Path: "/big", Path2: "/copy"}); err != nil {
		t.Fatalf("link of object-backed source: %v", err)
	}
	rd, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/copy", Offset: 0, Limit: int64(len(image))})
	if err != nil || !bytes.Equal(rd.Data, image) {
		t.Fatalf("/copy = %d bytes err=%v, want the full %d-byte image", len(rd.Data), err, len(image))
	}
}

func TestPortRmdirJudgesMergedView(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/d/f", []byte("base child"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	_, ch := mStack(t, ts)
	ctx := context.Background()

	// (a) base children count: the layer dir holds only base content.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpMkdir, Path: "/d", Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRmdir, Path: "/d"}); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("rmdir over base children = %v, want ENOTEMPTY", err)
	}

	// (b) whiteouted children are NOT presence: create, delete, then the
	// emptied directory removes.
	b2 := testbackend.NewBackend()
	ts2 := httptest.NewServer(b2)
	t.Cleanup(ts2.Close)
	_, ch2 := mStack(t, ts2)
	if _, err := ch2.Dispatch(ctx, &fs.Op{Kind: fs.OpMkdir, Path: "/e", Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	res, err := ch2.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/e/f", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch2.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/e/f"}); err != nil {
		t.Fatal(err)
	}
	if _, err := ch2.Dispatch(ctx, &fs.Op{Kind: fs.OpRmdir, Path: "/e"}); err != nil {
		t.Fatalf("rmdir of emptied dir = %v, want OK (whiteout counted as presence)", err)
	}
}

func TestPortLocallyUnlinkedFDKeepsReading(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("hello"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	_, ch := mStack(t, ts)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if err := h.Write(ctx, 0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/f"}); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 5)
	if n, rerr := h.Read(ctx, 0, got); rerr != nil || string(got[:n]) != "hello" {
		t.Fatalf("locally-unlinked fd read = %q (%d) err=%v, want its buffered image", got[:n], n, rerr)
	}
}

// Drain×Write over a streamed commit stays race-free: the spill holds
// the handle mutex, so a concurrent write cannot splice the image.
func TestPortStreamedCommitConcurrentWriteStaysClean(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	l, ch := mStack(t, ts)
	l.mu.Lock()
	l.inlineMaxOverride = 8
	l.mu.Unlock()
	ctx := context.Background()

	image := bytes.Repeat([]byte("Q"), 512*1024)
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/big", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle.(*layerHandle)
	if err := h.Write(ctx, 0, image); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	syncErr := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		syncErr <- h.Sync(ctx)
	}()
	for i := 0; i < 200; i++ {
		if err := h.Write(ctx, int64(len(image)), []byte("x")); err != nil {
			t.Fatalf("concurrent write: %v", err)
		}
	}
	wg.Wait()
	if err := <-syncErr; err != nil {
		t.Fatalf("streamed sync: %v", err)
	}
	// The raced write keeps the fd dirty; a final sync lands the whole image.
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	want := append(append([]byte(nil), image...), []byte("xxxxxxxx")...)
	rd, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/big", Offset: 0, Limit: int64(len(want))})
	if err != nil || !bytes.Equal(rd.Data, want[:len(rd.Data)]) {
		t.Fatalf("final image = %d bytes err=%v, want image+writes", len(rd.Data), err)
	}
}

// qiffang twenty-first round: base-dir chmod keeps the directory; the
// restore replay moves content BEFORE the source whiteout.
func TestPortBaseDirChmodKeepsDirectory(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/d/f", []byte("x"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	l, ch := mStack(t, ts)
	ctx := context.Background()

	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpMkdir, Path: "/d", Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/d", HasMode: true, Mode: 0o700, Size: -1}); err != nil {
		t.Fatal(err)
	}
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/d"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Attr == nil || res.Attr.Kind != fs.KindDir {
		t.Fatalf("chmod'd base dir = %+v, want a directory", res.Attr)
	}
	kind, _, _, _, ok := l.lookup("/d")
	if !ok || kind != "dir" {
		t.Fatalf("overlay /d kind = %q ok=%v, want dir", kind, ok)
	}
}

// The rename-after-upsert replay order: the replayed image moves to the
// destination instead of re-reading the stale base.
func TestPortReplayRenameAfterUpsertUsesMovedContent(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/a", []byte("old"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	l, ch := mStack(t, ts)
	ctx := context.Background()

	// upsert /a = new, then rename /a -> /b through the live plane.
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/a", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Write(ctx, 0, []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/a", Path2: "/b"}); err != nil {
		t.Fatal(err)
	}
	// A fresh restore replays upsert-then-rename from the log.
	if err := l.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	kind, _, _, content, ok := l.lookup("/b")
	if !ok || kind != "file" || string(content) != "new" {
		t.Fatalf("restored /b = kind=%s content=%q ok=%v, want the replayed new image", kind, content, ok)
	}
}

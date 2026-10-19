package layer

// qiffang seventeenth round: FATTR_FH resolves through UnwrapHandle
// decorators; a directory rename moves open child files; O_CREAT claims
// a whiteouted name fresh; an unlink that won the mutation lock
// discards the waiting commit; large commits stream in bounded memory.

import (
	"context"
	"net/http/httptest"
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

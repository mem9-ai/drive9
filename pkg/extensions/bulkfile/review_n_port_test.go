package bulkfile

// qiffang twenty-second round: a surrendered descriptor stays in the
// namespace-tracking registry, so a rename follows it instead of its later
// session recreating the old name.

import (
	"context"
	"sync"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func TestPortSurrenderedDescriptorFollowsRename(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newWritebackStack(t, b)
	ctx := context.Background()

	first := openForWrite(t, ch, "/a")
	if err := first.Write(ctx, 0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	// A second writable open hands the path to the lower plane and turns
	// the first descriptor into a passthrough.
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/a", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatalf("second open = %v", err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/a", Path2: "/b"}); err != nil {
		t.Fatalf("rename = %v", err)
	}
	if err := first.Write(ctx, 0, []byte("X")); err != nil {
		t.Fatal(err)
	}
	if err := first.Sync(ctx); err != nil {
		t.Fatalf("sync = %v", err)
	}
	if _, ok := b.Node("/a"); ok {
		t.Fatal("surrendered descriptor recreated the old name")
	}
	n, ok := b.Node("/b")
	if !ok {
		t.Fatal("/b missing after the write")
	}
	if string(n.Data) != "Xello" {
		t.Fatalf("/b = %q, want the write composed on the renamed image", n.Data)
	}
	if err := first.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

// An unlink retires EVERY descriptor's name binding: a surrendered fd keeps
// its anonymous backing (bound before the delete) and must not re-open the
// deleted path with its O_CREAT flags.
func TestPortSurrenderedDescriptorSurvivesUnlink(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newWritebackStack(t, b)
	ctx := context.Background()

	first := openForWrite(t, ch, "/a")
	if err := first.Write(ctx, 0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/a", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatalf("second open = %v", err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/a"}); err != nil {
		t.Fatalf("unlink = %v", err)
	}
	if err := first.Write(ctx, 0, []byte("X")); err != nil {
		t.Fatalf("write after unlink = %v", err)
	}
	if err := first.Sync(ctx); err != nil {
		t.Fatalf("sync after unlink = %v", err)
	}
	if _, ok := b.Node("/a"); ok {
		t.Fatal("surrendered descriptor resurrected the unlinked name")
	}
	if err := first.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

// The over-limit handoff keeps the descriptor tracked: after a rename, a
// path-level setattr through the original fd must hit the renamed file.
func TestPortHandoffDescriptorFollowsRename(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newWritebackStack(t, b)
	ctx := context.Background()

	first := openForWrite(t, ch, "/a")
	if err := first.Write(ctx, 0, make([]byte, bulkfileMaxBufferedBytes+1)); err != nil {
		t.Fatalf("over-limit write = %v", err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/a", Path2: "/b"}); err != nil {
		t.Fatalf("rename = %v", err)
	}
	// Recreate the old name with different content.
	other := openForWrite(t, ch, "/a")
	if err := other.Write(ctx, 0, []byte("OTHER")); err != nil {
		t.Fatal(err)
	}
	if err := other.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := other.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/b", Size: 1, Handle: first}); err != nil {
		t.Fatalf("ftruncate via original fd = %v", err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/b"}); err != nil {
		t.Fatalf("getattr /b = %v", err)
	}
	if node, ok := b.Node("/a"); !ok || len(node.Data) != 5 {
		t.Fatalf("/a = %q ok=%v, want the recreated OTHER untouched", node.Data, ok)
	}
	if err := first.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

// chmod(000) on a live claim stays 000 in the reply and the overlay.
func TestPortChmodZeroKeepsMode(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newWritebackStack(t, b)
	ctx := context.Background()

	h := openForWrite(t, ch, "/z")
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/z", HasMode: true, Mode: 0, Size: -1, Handle: h})
	if err != nil {
		t.Fatalf("chmod = %v", err)
	}
	if res.Attr == nil || res.Attr.Mode&0o777 != 0 {
		t.Fatalf("chmod reply mode = %+v, want 000", res.Attr)
	}
	lres, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/z"})
	if err != nil || lres.Attr == nil || lres.Attr.Mode&0o777 != 0 {
		t.Fatalf("getattr mode = %+v err=%v, want 000", lres.Attr, err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

type anonProbeHandle struct{ fs.Handle }

func (anonProbeHandle) Anonymous() bool { return true }

// The bulk claim wrapper forwards the inner handle's anonymous identity so
// outer path-keyed caches bypass for it.
func TestPortBulkfileForwardsAnonymous(t *testing.T) {
	h := &streamHandle{inner: anonProbeHandle{}}
	if !h.Anonymous() {
		t.Fatal("streamHandle dropped the Anonymous identity")
	}
}

// An unlinked claim's large/sparse write spills instead of allocating the
// hole, and the image stays readable.
func TestPortUnlinkedClaimSpillsSparseWrite(t *testing.T) {
	h := &streamHandle{unlinked: true}
	ctx := context.Background()
	if err := h.Write(ctx, 0, []byte("HELLO")); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(ctx, 128<<20, []byte("Z")); err != nil {
		t.Fatalf("sparse write = %v", err)
	}
	h.mu.Lock()
	spilled := h.spill != nil && h.image == nil
	size := h.spillSize
	h.mu.Unlock()
	if !spilled {
		t.Fatal("sparse unlinked write kept the whole image in memory")
	}
	if size != 128<<20+1 {
		t.Fatalf("image size = %d, want %d", size, 128<<20+1)
	}
	buf := make([]byte, 5)
	if n, err := h.Read(ctx, 0, buf); err != nil || string(buf[:n]) != "HELLO" {
		t.Fatalf("head read = %q err=%v", buf[:n], err)
	}
	one := make([]byte, 1)
	if n, err := h.Read(ctx, 128<<20, one); err != nil || n != 1 || one[0] != 'Z' {
		t.Fatalf("tail read = %q err=%v", one[:n], err)
	}
	h.mu.Lock()
	if h.spill != nil {
		_ = h.spill.Close()
	}
	h.mu.Unlock()
}

// A read-only claim re-open forwards the anonymous identity.
func TestPortClaimReadHandleForwardsAnonymous(t *testing.T) {
	claim := &streamHandle{unlinked: true}
	rh := &claimReadHandle{claim: claim}
	if !rh.Anonymous() {
		t.Fatal("claimReadHandle dropped the Anonymous identity")
	}
}

type wrapProbe struct {
	inner fs.Handle
}

func (w *wrapProbe) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	return w.inner.Read(ctx, off, dest)
}
func (w *wrapProbe) Write(ctx context.Context, off int64, data []byte) error {
	return w.inner.Write(ctx, off, data)
}
func (w *wrapProbe) Sync(context.Context) error    { return nil }
func (w *wrapProbe) Flush(context.Context) error   { return nil }
func (w *wrapProbe) Release(context.Context) error { return nil }
func (w *wrapProbe) Unwrap() fs.Handle             { return w.inner }

// A FD-directed setattr must resolve OUR handle through decorator wrappers,
// not fall back to the path (which truncated a same-name replacement).
func TestPortResolvesHandleThroughDecorators(t *testing.T) {
	b := &Bulkfile{}
	h := &streamHandle{unlinked: true, refs: 1}
	if got := b.resolveStreamHandle(&wrapProbe{inner: &wrapProbe{inner: h}}); got != h {
		t.Fatalf("resolveStreamHandle = %p, want the claim %p", got, h)
	}
	if got := b.resolveStreamHandle(nil); got != nil {
		t.Fatal("nil handle resolved to a claim")
	}
}

// Once the spill exists, a later LOW-offset write must land in it (reads
// serve the spill, so the in-memory branch silently lost the write).
func TestPortSpillRoutesSubsequentWrites(t *testing.T) {
	h := &streamHandle{unlinked: true, refs: 1}
	ctx := context.Background()
	if err := h.Write(ctx, 0, []byte("OTHER")); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(ctx, 128<<20, []byte("Z")); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(ctx, 0, []byte("AFTER")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if n, err := h.Read(ctx, 0, buf); err != nil || string(buf[:n]) != "AFTER" {
		t.Fatalf("read after spill write = %q err=%v", buf[:n], err)
	}
	h.mu.Lock()
	_ = h.spill.Close()
	h.mu.Unlock()
}

// ftruncate must truncate the disk image and update its logical size.
func TestPortSpillFtruncate(t *testing.T) {
	h := &streamHandle{unlinked: true, refs: 1}
	ctx := context.Background()
	if err := h.Write(ctx, 0, []byte("OTHER")); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(ctx, 128<<20, []byte("Z")); err != nil {
		t.Fatal(err)
	}
	attr, err := h.applyAttr(ctx, &fs.Op{Size: 2})
	if err != nil {
		t.Fatal(err)
	}
	if attr.Size != 2 {
		t.Fatalf("attr size = %d, want 2", attr.Size)
	}
	buf := make([]byte, 4)
	n, rerr := h.Read(ctx, 2, buf)
	if rerr != nil || n != 0 {
		t.Fatalf("tail read = %d err=%v, want EOF", n, rerr)
	}
	h.mu.Lock()
	size := h.spillSize
	_ = h.spill.Close()
	h.mu.Unlock()
	if size != 2 {
		t.Fatalf("spillSize = %d, want 2", size)
	}
}

// The write descriptor's Release is not the last reference while a
// claimReadHandle is open: the read-only fd keeps reading the old image.
func TestPortSpillOutlivesWriteDescriptor(t *testing.T) {
	h := &streamHandle{unlinked: true, refs: 1}
	ctx := context.Background()
	if err := h.Write(ctx, 0, []byte("OTHER")); err != nil {
		t.Fatal(err)
	}
	rh := &claimReadHandle{claim: h}
	h.addRef()
	if err := h.Write(ctx, 128<<20, []byte("Z")); err != nil {
		t.Fatal(err)
	}
	h.released = true
	h.dropRef() // the write descriptor's close
	buf := make([]byte, 5)
	if n, err := rh.Read(ctx, 0, buf); err != nil || string(buf[:n]) != "OTHER" {
		t.Fatalf("read-only fd after writer close = %q err=%v, want OTHER", buf[:n], err)
	}
	if err := rh.Release(ctx); err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	closed := h.spill == nil
	h.mu.Unlock()
	if !closed {
		t.Fatal("spill leaked after the last reference closed")
	}
}

type fakeInnerHandle struct {
	fs.Handle
	data      []byte
	released  bool
	releaseN  int
	releaseMu sync.Mutex
}

func (f *fakeInnerHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	if off >= int64(len(f.data)) {
		return 0, nil
	}
	return copy(dest, f.data[off:]), nil
}
func (f *fakeInnerHandle) Write(context.Context, int64, []byte) error { return nil }
func (f *fakeInnerHandle) Sync(context.Context) error                 { return nil }
func (f *fakeInnerHandle) Flush(context.Context) error                { return nil }
func (f *fakeInnerHandle) Release(context.Context) error {
	f.releaseMu.Lock()
	f.released = true
	f.releaseN++
	f.releaseMu.Unlock()
	return nil
}

// A passthrough truncate must reach the BOUND handle, not the path.
func TestPortPassthroughTruncateUsesBoundHandle(t *testing.T) {
	inner := &fakeInnerHandle{data: []byte("OTHER")}
	var got *fs.Op
	below := fs.HandlerFilesystem(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		got = op
		return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Size: op.Size}}, nil
	})
	h := &streamHandle{below: below, inner: inner, passthrough: true, refs: 1}
	attr, err := h.applyAttr(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Size: 2})
	if err != nil {
		t.Fatal(err)
	}
	if attr.Size != 2 {
		t.Fatalf("attr size = %d, want 2", attr.Size)
	}
	if got == nil || got.Handle != inner {
		t.Fatalf("truncate forwarded with handle %v, want the bound inner handle", got)
	}
	if got.Path != "" && got.Size != 2 {
		t.Fatalf("forwarded op = %+v", got)
	}
}

// The passthrough backing must outlive the write descriptor while a
// claimReadHandle is open (its Read falls through the claim).
func TestPortPassthroughBackingOutlivesWriter(t *testing.T) {
	inner := &fakeInnerHandle{data: []byte("OTHER")}
	h := &streamHandle{inner: inner, passthrough: true, refs: 1}
	rh := &claimReadHandle{claim: h}
	h.addRef()
	ctx := context.Background()
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	inner.releaseMu.Lock()
	premature := inner.released
	inner.releaseMu.Unlock()
	if premature {
		t.Fatal("backing released while a claim reader is still open")
	}
	buf := make([]byte, 5)
	if n, err := rh.Read(ctx, 0, buf); err != nil || string(buf[:n]) != "OTHER" {
		t.Fatalf("reader after writer close = %q err=%v", buf[:n], err)
	}
	if err := rh.Release(ctx); err != nil {
		t.Fatal(err)
	}
	inner.releaseMu.Lock()
	released := inner.released
	inner.releaseMu.Unlock()
	if !released {
		t.Fatal("backing leaked after the last reference")
	}
}

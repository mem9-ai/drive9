package bulkfile

// qiffang twenty-second round: a surrendered descriptor stays in the
// namespace-tracking registry, so a rename follows it instead of its later
// session recreating the old name.

import (
	"context"
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

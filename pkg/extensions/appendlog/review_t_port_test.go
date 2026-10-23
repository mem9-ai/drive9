package appendlog

// Review round T ports: a sibling composing over an uncommitted truncation
// inherits the truncation's whole geometry (shrink to the floor, zero-grow
// to the final size) — a shrink-then-extend leaves zero fill, not remote
// bytes, even when the final size equals the original; and a NEW open of a
// claimed object renamed out of the pattern joins the existing append data
// plane instead of passing through to the lower plane.

import (
	"bytes"
	"context"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// h1 does ftruncate(0) then ftruncate(6): h2 reads six zero bytes, and its
// write of X at 0 composes over the zero fill (X + five zeros), never over
// the remote's abcdef that the shrink discarded.
func TestPortShrinkThenExtendTruncationInheritsZeroFill(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app.log", []byte("abcdef"))
	ch := newStack(t, b, "**/*.log")
	ctx := context.Background()

	h1 := openLog(t, ch, "/app.log")
	h2 := openLog(t, ch, "/app.log")
	for _, size := range []int64{0, 6} {
		if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/app.log", Handle: h1, Size: size, HasSize: true}); err != nil {
			t.Fatalf("h1 ftruncate(%d) = %v", size, err)
		}
	}
	zeros := make([]byte, 6)
	if got := mustRead(t, h2, ctx, 0, 6); got != string(zeros) {
		t.Fatalf("h2 read after shrink+extend = %q, want six zero bytes", got)
	}
	if err := h2.Write(ctx, 0, []byte("X")); err != nil {
		t.Fatal(err)
	}
	want := append([]byte("X"), make([]byte, 5)...)
	if got := mustRead(t, h2, ctx, 0, 6); got != string(want) {
		t.Fatalf("h2 read after its write = %q, want X + five zeros (the shrink boundary was lost)", got)
	}
	if err := h2.Sync(ctx); err != nil {
		t.Fatalf("h2 fsync = %v", err)
	}
	if node, _ := b.Node("/app.log"); !bytes.Equal(node.Data, want) {
		t.Fatalf("/app.log = %q, want X + five zeros", node.Data)
	}
	if err := h1.Release(ctx); err != nil {
		t.Fatalf("truncator release = %v", err)
	}
	if node, _ := b.Node("/app.log"); !bytes.Equal(node.Data, want) {
		t.Fatalf("/app.log after truncator close = %q, want X + five zeros", node.Data)
	}
}

// A new open of a claimed object renamed out of the pattern joins the
// existing data plane: it reads the sibling's pending truncation (ab), its
// write composes over it, and both fsyncs land aY.
func TestPortRenamedClaimedObjectNewOpenJoinsDataPlane(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app.log", []byte("abcdef"))
	ch := newStack(t, b, "**/*.log")
	ctx := context.Background()

	h1 := openLog(t, ch, "/app.log")
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/app.log", Path2: "/app.txt"}); err != nil {
		t.Fatalf("rename = %v", err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/app.txt", Handle: h1, Size: 2, HasSize: true}); err != nil {
		t.Fatalf("h1 ftruncate(2) = %v", err)
	}
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/app.txt", Flags: uint32(syscall.O_RDWR), Mode: 0o644})
	if err != nil || res.Handle == nil {
		t.Fatalf("open of the renamed name = %v", err)
	}
	h2, ok := res.Handle.(*Handle)
	if !ok {
		t.Fatalf("new open of the renamed claimed object was not claimed by appendlog: %T", res.Handle)
	}
	if got := mustRead(t, h2, ctx, 0, 6); got != "ab" {
		t.Fatalf("new fd read = %q, want the sibling's pending ab", got)
	}
	if err := h2.Write(ctx, 1, []byte("Y")); err != nil {
		t.Fatal(err)
	}
	if err := h2.Sync(ctx); err != nil {
		t.Fatalf("h2 fsync = %v", err)
	}
	if node, _ := b.Node("/app.txt"); string(node.Data) != "aY" {
		t.Fatalf("/app.txt after h2 fsync = %q, want aY", node.Data)
	}
	if err := h1.Sync(ctx); err != nil {
		t.Fatalf("h1 fsync = %v", err)
	}
	if node, _ := b.Node("/app.txt"); string(node.Data) != "aY" {
		t.Fatalf("/app.txt after h1 fsync = %q, want aY", node.Data)
	}
	if _, ok := b.Node("/app.log"); ok {
		t.Fatal("the old name must stay gone")
	}
}

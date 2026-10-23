package appendlog

// Review round R ports: a sibling's confirmed-but-uncommitted truncation is
// the base every later write of the object composes over (a plain write
// and an O_APPEND write both land relative to the truncated image, never
// resurrecting the truncated-away tail from the remote); and open(O_TRUNC)
// truncates the object for every fd — a sibling's uncommitted bytes are
// discarded and stats report the truncation, not the stale larger image.

import (
	"context"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func statSize(t *testing.T, ch *fs.Chain, path string) int64 {
	t.Helper()
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpGetAttr, Path: path})
	if err != nil || res.Attr == nil {
		t.Fatalf("getattr %s = %+v err=%v", path, res.Attr, err)
	}
	return res.Attr.Size
}

// After h2 truncates abcdef to 2, a plain write on h1 composes over the
// truncated image: write Y at 1 gives aY (not aYcdef), and the flush lands
// exactly that.
func TestPortWriteAfterSiblingTruncateComposesOverTruncation(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app.log", []byte("abcdef"))
	ch := newStack(t, b, "**/*.log")
	ctx := context.Background()

	h1 := openLog(t, ch, "/app.log")
	h2 := openLog(t, ch, "/app.log")
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/app.log", Handle: h2, Size: 2, HasSize: true}); err != nil {
		t.Fatalf("h2 ftruncate(2) = %v", err)
	}
	if got := mustRead(t, h1, ctx, 0, 6); got != "ab" {
		t.Fatalf("h1 read after sibling truncate = %q, want ab", got)
	}
	if err := h1.Write(ctx, 1, []byte("Y")); err != nil {
		t.Fatal(err)
	}
	if got := mustRead(t, h1, ctx, 0, 6); got != "aY" {
		t.Fatalf("h1 read after its write = %q, want aY (the truncated tail was resurrected)", got)
	}
	if got := statSize(t, ch, "/app.log"); got != 2 {
		t.Fatalf("getattr size = %d, want 2", got)
	}
	if err := h1.Sync(ctx); err != nil {
		t.Fatalf("h1 fsync = %v", err)
	}
	if node, _ := b.Node("/app.log"); string(node.Data) != "aY" {
		t.Fatalf("/app.log = %q, want aY", node.Data)
	}
	// The pure truncator was superseded by the landing that embodied its
	// truncation: its own close publishes nothing and does not conflict.
	if err := h2.Release(ctx); err != nil {
		t.Fatalf("truncator release = %v", err)
	}
	if node, _ := b.Node("/app.log"); string(node.Data) != "aY" {
		t.Fatalf("/app.log after truncator close = %q, want aY", node.Data)
	}
}

// The O_APPEND twin: after h2 truncates to 2, h1's append lands at the
// truncated tail (abY), not at the stale remote size (abcdefY).
func TestPortAppendAfterSiblingTruncateLandsAtTruncatedTail(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app.log", []byte("abcdef"))
	ch := newStack(t, b, "**/*.log")
	ctx := context.Background()

	h1 := openFlags(t, ch, "/app.log", syscall.O_RDWR|syscall.O_APPEND)
	h2 := openLog(t, ch, "/app.log")
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/app.log", Handle: h2, Size: 2, HasSize: true}); err != nil {
		t.Fatalf("h2 ftruncate(2) = %v", err)
	}
	if err := h1.Write(ctx, 0, []byte("Y")); err != nil {
		t.Fatal(err)
	}
	if got := mustRead(t, h1, ctx, 0, 8); got != "abY" {
		t.Fatalf("O_APPEND fd read after sibling truncate = %q, want abY", got)
	}
	if got := statSize(t, ch, "/app.log"); got != 3 {
		t.Fatalf("getattr size = %d, want 3", got)
	}
	if err := h1.Sync(ctx); err != nil {
		t.Fatalf("h1 fsync = %v", err)
	}
	if node, _ := b.Node("/app.log"); string(node.Data) != "abY" {
		t.Fatalf("/app.log = %q, want abY", node.Data)
	}
}

// open(O_TRUNC) while a sibling holds uncommitted bytes: the truncation is
// the object's image for every fd — stats report 0 (not the sibling's
// stale 10), the sibling reads empty, and its next write composes over the
// truncation.
func TestPortOpenTruncDiscardsDirtySiblingImage(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app.log", []byte("abcdef"))
	ch := newStack(t, b, "**/*.log")
	ctx := context.Background()

	h1 := openLog(t, ch, "/app.log")
	if err := h1.Write(ctx, 6, []byte("TAIL")); err != nil {
		t.Fatal(err)
	}
	if got := statSize(t, ch, "/app.log"); got != 10 {
		t.Fatalf("getattr before O_TRUNC = %d, want the pending 10", got)
	}
	h2 := openFlags(t, ch, "/app.log", syscall.O_RDWR|syscall.O_TRUNC)
	if got := statSize(t, ch, "/app.log"); got != 0 {
		t.Fatalf("getattr after O_TRUNC = %d, want 0 (the dirty sibling's stale image was reported)", got)
	}
	if got := mustRead(t, h1, ctx, 0, 16); got != "" {
		t.Fatalf("dirty sibling read after O_TRUNC = %q, want empty", got)
	}
	// The sibling's next write composes over the truncation, and its flush
	// lands exactly that image.
	if err := h1.Write(ctx, 0, []byte("X")); err != nil {
		t.Fatal(err)
	}
	if got := mustRead(t, h1, ctx, 0, 16); got != "X" {
		t.Fatalf("sibling read after its write = %q, want X", got)
	}
	if got := statSize(t, ch, "/app.log"); got != 1 {
		t.Fatalf("getattr after the composed write = %d, want 1", got)
	}
	if err := h1.Sync(ctx); err != nil {
		t.Fatalf("h1 fsync = %v", err)
	}
	if node, _ := b.Node("/app.log"); string(node.Data) != "X" {
		t.Fatalf("/app.log = %q, want X", node.Data)
	}
	if err := h2.Release(ctx); err != nil {
		t.Fatalf("truncator release = %v", err)
	}
	if node, _ := b.Node("/app.log"); string(node.Data) != "X" {
		t.Fatalf("/app.log after truncator close = %q, want X", node.Data)
	}
}

// The write-less variant: the O_TRUNC fd closes first and publishes the
// empty object; the discarded sibling then re-bases onto the landed image.
func TestPortOpenTruncDiscardedSiblingRebasesAfterLanding(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app.log", []byte("abcdef"))
	ch := newStack(t, b, "**/*.log")
	ctx := context.Background()

	h1 := openLog(t, ch, "/app.log")
	if err := h1.Write(ctx, 6, []byte("TAIL")); err != nil {
		t.Fatal(err)
	}
	h2 := openFlags(t, ch, "/app.log", syscall.O_WRONLY|syscall.O_TRUNC)
	if err := h2.Release(ctx); err != nil {
		t.Fatalf("truncator release = %v", err)
	}
	if node, _ := b.Node("/app.log"); len(node.Data) != 0 {
		t.Fatalf("/app.log after O_TRUNC close = %q, want empty", node.Data)
	}
	if got := mustRead(t, h1, ctx, 0, 16); got != "" {
		t.Fatalf("sibling read after the landed truncation = %q, want empty", got)
	}
	if err := h1.Write(ctx, 0, []byte("X")); err != nil {
		t.Fatal(err)
	}
	if err := h1.Sync(ctx); err != nil {
		t.Fatalf("h1 fsync = %v", err)
	}
	if node, _ := b.Node("/app.log"); string(node.Data) != "X" {
		t.Fatalf("/app.log = %q, want X (the discarded sibling re-seeded from its pre-truncation size)", node.Data)
	}
}

// A sibling whose OWN uncommitted truncate is discarded by open(O_TRUNC)
// must not keep that truncate's rewrite base: after the truncating fd
// lands, the sibling re-bases onto the landed image and its next flush
// commits against the live revision, not the vanished pre-truncate one.
func TestPortOpenTruncDiscardsSiblingTruncateFence(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app.log", []byte("abcdef"))
	ch := newStack(t, b, "**/*.log")
	ctx := context.Background()

	h1 := openLog(t, ch, "/app.log")
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/app.log", Handle: h1, Size: 3, HasSize: true}); err != nil {
		t.Fatalf("h1 ftruncate(3) = %v", err)
	}
	h2 := openFlags(t, ch, "/app.log", syscall.O_WRONLY|syscall.O_TRUNC)
	if err := h2.Release(ctx); err != nil {
		t.Fatalf("truncator release = %v", err)
	}
	if node, _ := b.Node("/app.log"); len(node.Data) != 0 {
		t.Fatalf("/app.log after O_TRUNC close = %q, want empty", node.Data)
	}
	if err := h1.Write(ctx, 0, []byte("X")); err != nil {
		t.Fatal(err)
	}
	if err := h1.Sync(ctx); err != nil {
		t.Fatalf("h1 fsync after its truncate was discarded = %v (stale rewrite base)", err)
	}
	if node, _ := b.Node("/app.log"); string(node.Data) != "X" {
		t.Fatalf("/app.log = %q, want X", node.Data)
	}
}

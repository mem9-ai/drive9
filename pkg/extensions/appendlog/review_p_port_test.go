package appendlog

// Review round P ports: open(O_TRUNC) starts the append image empty and
// retires the lower plane's pending truncation once the append plane
// publishes; cross-fd sharing (read-your-writes, append baselines) is
// scoped to one object generation so anonymous fds of a deleted object and
// live fds of its same-name successor never see each other's bytes; and an
// fd-truncate is the object's image for every fd — clean siblings read
// through it, a dirty sibling makes it EAGAIN.

import (
	"context"
	"errors"
	"net/http/httptest"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// newWritebackStack builds appendlog → writeback(close-sync) → core, the
// chain the reviewer drives through real FUSE callbacks.
func newWritebackStack(t *testing.T, b *testbackend.Backend, patterns ...string) *fs.Chain {
	t.Helper()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	ext, err := New(Options{Patterns: patterns, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	wb, err := writeback.New(writeback.Options{Policy: writeback.PolicyCloseSync, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(ext, wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	return ch
}

func openFlags(t *testing.T, ch *fs.Chain, path string, flags int) fs.Handle {
	t.Helper()
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: path, Flags: uint32(flags), Mode: 0o644})
	if err != nil || res.Handle == nil {
		t.Fatalf("open %s flags=%#o: %v", path, flags, err)
	}
	return res.Handle
}

// open(O_RDWR|O_TRUNC) on an existing append-log file: the first write
// composes over an EMPTY image (not the pre-open bytes), the append plane
// publishes it, and the lower plane's pending truncation is retired instead
// of CAS-ing the pre-upload revision.
func TestPortOpenTruncDiscardsOldImage(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app.log", []byte("abcdef"))
	ch := newWritebackStack(t, b, "**/*.log")
	ctx := context.Background()

	h := openFlags(t, ch, "/app.log", syscall.O_RDWR|syscall.O_TRUNC)
	if err := h.Write(ctx, 0, []byte("X")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatalf("fsync after O_TRUNC write = %v", err)
	}
	if node, ok := b.Node("/app.log"); !ok || string(node.Data) != "X" {
		t.Fatalf("/app.log = %q ok=%v, want X (the old image leaked into the composition)", node.Data, ok)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatalf("release = %v", err)
	}
	if node, _ := b.Node("/app.log"); string(node.Data) != "X" {
		t.Fatalf("/app.log after close = %q, want X", node.Data)
	}
}

// A write-less open(O_TRUNC) + close still publishes size 0, once, and the
// pending size is visible to stats before the close.
func TestPortOpenTruncWritelessClosePublishesEmpty(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app.log", []byte("abcdef"))
	ch := newWritebackStack(t, b, "**/*.log")
	ctx := context.Background()

	h := openFlags(t, ch, "/app.log", syscall.O_WRONLY|syscall.O_TRUNC)
	gres, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/app.log"})
	if err != nil || gres.Attr == nil || gres.Attr.Size != 0 {
		t.Fatalf("getattr after O_TRUNC open = %+v err=%v, want size 0", gres.Attr, err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatalf("release = %v", err)
	}
	if node, ok := b.Node("/app.log"); !ok || len(node.Data) != 0 {
		t.Fatalf("/app.log after write-less O_TRUNC close = %q ok=%v, want empty", node.Data, ok)
	}
}

// Cross-fd sharing is scoped to the object generation: an anonymous fd of
// a deleted object never reads (or appends over) the dirty buffer of the
// same-name successor, in either direction, and two anonymous generations
// never mix either.
func TestPortAnonymousFdSharesOnlyItsOwnObject(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newStack(t, b, "**/*.log")
	ctx := context.Background()

	// Generation 1: an empty new file whose fd goes anonymous.
	gen1 := openLog(t, ch, "/app.log")
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/app.log"}); err != nil {
		t.Fatalf("unlink gen1 = %v", err)
	}
	// Generation 2: the successor holds unflushed FRESH bytes.
	gen2 := openLog(t, ch, "/app.log")
	if err := gen2.Write(ctx, 0, []byte("FRESH")); err != nil {
		t.Fatal(err)
	}
	if got := mustRead(t, gen1, ctx, 0, 16); got != "" {
		t.Fatalf("anonymous gen1 read = %q, want empty (it read the successor's dirty buffer)", got)
	}
	// Generation 3 after a second unlink: neither anonymous generation sees
	// the other or the live successor.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/app.log"}); err != nil {
		t.Fatalf("unlink gen2 = %v", err)
	}
	gen3 := openLog(t, ch, "/app.log")
	if err := gen3.Write(ctx, 0, []byte("THIRD-GENERATION")); err != nil {
		t.Fatal(err)
	}
	if got := mustRead(t, gen1, ctx, 0, 16); got != "" {
		t.Fatalf("anonymous gen1 read after gen3 = %q, want empty", got)
	}
	if got := mustRead(t, gen2, ctx, 0, 16); got != "FRESH" {
		t.Fatalf("anonymous gen2 read = %q, want its own FRESH", got)
	}
	if got := mustRead(t, gen3, ctx, 0, 16); got != "THIRD-GENERATION" {
		t.Fatalf("live gen3 read = %q, want THIRD-GENERATION", got)
	}
}

// An anonymous O_APPEND fd appends over ITS object's tail, never over the
// same-name successor's unflushed bytes.
func TestPortAnonymousAppendIgnoresSuccessorTail(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app.log", []byte("abcdef"))
	ch := newStack(t, b, "**/*.log")
	ctx := context.Background()

	old := openFlags(t, ch, "/app.log", syscall.O_RDWR|syscall.O_APPEND)
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/app.log"}); err != nil {
		t.Fatalf("unlink = %v", err)
	}
	successor := openLog(t, ch, "/app.log")
	if err := successor.Write(ctx, 0, []byte("NEW-TAIL")); err != nil {
		t.Fatal(err)
	}
	if err := old.Write(ctx, 0, []byte("!")); err != nil {
		t.Fatal(err)
	}
	if got := mustRead(t, old, ctx, 0, 32); got != "abcdef!" {
		t.Fatalf("anonymous O_APPEND fd read = %q, want abcdef! (the successor's tail leaked into the baseline)", got)
	}
	if got := mustRead(t, successor, ctx, 0, 32); got != "NEW-TAIL" {
		t.Fatalf("successor read = %q, want NEW-TAIL", got)
	}
}

// A successful fd-truncate is the object's image for EVERY fd: the clean
// sibling reads the truncated image (and stats report it) before the
// truncation lands.
func TestPortFtruncateVisibleToCleanSibling(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app.log", []byte("abcdef"))
	ch := newStack(t, b, "**/*.log")
	ctx := context.Background()

	h1 := openLog(t, ch, "/app.log")
	h2 := openLog(t, ch, "/app.log")
	if err := h1.Write(ctx, 0, []byte("X")); err != nil {
		t.Fatal(err)
	}
	if err := h1.Sync(ctx); err != nil {
		t.Fatalf("h1 fsync = %v", err)
	}
	if got := mustRead(t, h1, ctx, 0, 6); got != "Xbcdef" {
		t.Fatalf("h1 read before truncate = %q", got)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/app.log", Handle: h2, Size: 2, HasSize: true}); err != nil {
		t.Fatalf("h2 ftruncate(2) = %v", err)
	}
	if got := mustRead(t, h1, ctx, 0, 6); got != "Xb" {
		t.Fatalf("h1 read after sibling truncate = %q, want Xb", got)
	}
	gres, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/app.log"})
	if err != nil || gres.Attr == nil || gres.Attr.Size != 2 {
		t.Fatalf("getattr after sibling truncate = %+v err=%v, want size 2", gres.Attr, err)
	}
	// The truncation lands and the sibling keeps reading the landed image.
	if err := h2.Sync(ctx); err != nil {
		t.Fatalf("h2 fsync = %v", err)
	}
	if node, _ := b.Node("/app.log"); string(node.Data) != "Xb" {
		t.Fatalf("/app.log = %q, want Xb", node.Data)
	}
	if got := mustRead(t, h1, ctx, 0, 6); got != "Xb" {
		t.Fatalf("h1 read after landing = %q, want Xb", got)
	}
}

// An fd-truncate while a sibling still holds unflushed bytes is a conflict
// the two fds cannot reconcile locally: EAGAIN, and the sibling's pending
// image stays the size of record.
func TestPortFtruncateConflictsWithDirtySibling(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app.log", []byte("abcdef"))
	ch := newStack(t, b, "**/*.log")
	ctx := context.Background()

	h1 := openLog(t, ch, "/app.log")
	h2 := openLog(t, ch, "/app.log")
	if err := h1.Write(ctx, 6, []byte("0123")); err != nil {
		t.Fatal(err)
	}
	_, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/app.log", Handle: h2, Size: 2, HasSize: true})
	if !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("ftruncate against a dirty sibling = %v, want EAGAIN", err)
	}
	gres, gerr := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/app.log"})
	if gerr != nil || gres.Attr == nil || gres.Attr.Size != 10 {
		t.Fatalf("getattr after rejected truncate = %+v err=%v, want the dirty sibling's 10", gres.Attr, gerr)
	}
	if got := mustRead(t, h1, ctx, 0, 10); got != "abcdef0123" {
		t.Fatalf("dirty sibling read = %q, want its untouched 10 bytes", got)
	}
}

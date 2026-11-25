package appendlog

// Review round O ports: the WAL generation reset must refresh clean
// siblings like the tail/rewrite landings; an unlink anonymizes EVERY open
// fd of the object (not just the newest claim); a same-name recreate lands
// the deferred delete and stops hiding the new name behind the old
// tombstone; and a truncated append buffer reports its pending size to
// SETATTR/GETATTR instead of the lower plane's committed size.

import (
	"bytes"
	"context"
	"net/http/httptest"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// A WAL rotation through the generation-reset transport re-publishes the
// object at a fresh revision: a clean sibling fd must adopt the new
// generation, or its next frame append CAS-es against the pre-rotation
// baseline and fails with EAGAIN.
func TestPortGenerationResetRefreshesCleanSiblingBaseline(t *testing.T) {
	b := testbackend.NewBackend()
	seedWAL(t, b, "/wal.db", 1, 2, 64) // 96-byte WAL, rev 1
	h1 := rotatedHeader(t)
	ch := newStack(t, b, "**/wal.db")
	ctx := context.Background()

	rotator := openLog(t, ch, "/wal.db").(*Handle)
	sibling := openLog(t, ch, "/wal.db").(*Handle)
	if err := rotator.Write(ctx, 0, h1); err != nil {
		t.Fatal(err)
	}
	if err := rotator.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if node, _ := b.Node("/wal.db"); len(node.Data) != walHeaderSize {
		t.Fatalf("reset landing = %d bytes, want the 32-byte generation", len(node.Data))
	}

	// The clean sibling appends the first frame of the new generation.
	if err := sibling.Write(ctx, walHeaderSize, []byte("frame")); err != nil {
		t.Fatal(err)
	}
	if err := sibling.Sync(ctx); err != nil {
		t.Fatalf("clean sibling fsync after rotation = %v (stale CAS baseline was not refreshed)", err)
	}
	node, _ := b.Node("/wal.db")
	want := append(append([]byte{}, h1...), []byte("frame")...)
	if !bytes.Equal(node.Data, want) {
		t.Fatalf("backend after sibling frame = %q, want header+frame", node.Data)
	}
}

// An unlink anonymizes EVERY open fd of the object: after a same-name
// recreate, an earlier fd that stayed live on the path adopted the new
// object's revision and its commit published the old image over the new
// file.
func TestPortUnlinkAnonymizesEveryOpenHandle(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app.log", []byte("OLDDATA"))
	ch := newStack(t, b, "**/*.log")
	ctx := context.Background()

	h1 := openLog(t, ch, "/app.log")
	h2 := openLog(t, ch, "/app.log")
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/app.log"}); err != nil {
		t.Fatalf("unlink = %v", err)
	}

	// Same-name recreate lands the deferred delete and publishes fresh
	// content under a new revision.
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/app.log",
		Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil || res.Handle == nil {
		t.Fatalf("recreate = %v", err)
	}
	fresh := res.Handle
	if err := fresh.Write(ctx, 0, []byte("FRESH")); err != nil {
		t.Fatal(err)
	}
	if err := fresh.Sync(ctx); err != nil {
		t.Fatalf("fresh fsync = %v", err)
	}

	// The earlier fd is anonymous: it still reads the deleted object's
	// image, and its write buffers locally.
	if got := mustRead(t, h1, ctx, 0, 7); got != "OLDDATA" {
		t.Fatalf("anonymous fd read = %q, want OLDDATA", got)
	}
	if err := h1.Write(ctx, 0, []byte("X")); err != nil {
		t.Fatal(err)
	}
	// Its fsync must NOT publish over the new object.
	if err := h1.Sync(ctx); err != nil {
		t.Fatalf("anonymous fd fsync = %v", err)
	}
	if node, ok := b.Node("/app.log"); !ok || string(node.Data) != "FRESH" {
		t.Fatalf("/app.log = %q ok=%v, want FRESH (the old fd overwrote the new object)", node.Data, ok)
	}
	// And the new fd reads its own content.
	if got := mustRead(t, fresh, ctx, 0, 5); got != "FRESH" {
		t.Fatalf("new fd read = %q, want FRESH", got)
	}
	_ = h2
}

// A recreated name is visible immediately: the old tombstone must not hide
// the new file until the last old fd closes. Both the direct-defer flow
// and the facade handshake (MarkPathUnlinked + landing unlink) must agree.
func TestPortRecreateAfterUnlinkVisibleImmediately(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app.log", []byte("OTHER"))
	ch := newStack(t, b, "**/*.log")
	ctx := context.Background()

	h := openLog(t, ch, "/app.log")
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/app.log"}); err != nil {
		t.Fatalf("unlink = %v", err)
	}
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/app.log",
		Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil || res.Handle == nil {
		t.Fatalf("recreate = %v", err)
	}
	if err := res.Handle.Write(ctx, 0, []byte("FRESH")); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	lres, lerr := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpLookup, Path: "/app.log"})
	if lerr != nil {
		t.Fatalf("lookup of the recreated name = %v, want the new file", lerr)
	}
	if lres.Attr == nil || lres.Attr.Size != 5 {
		t.Fatalf("recreated lookup attr = %+v, want size 5", lres.Attr)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

// The facade-handshake variant of the recreate-visibility row: the facade
// defers its delete (MarkPathUnlinked), then lands it right before the
// same-name create — the tombstone the landing stores must not outlive the
// create.
func TestPortRecreateAfterFacadeUnlinkVisibleImmediately(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app.log", []byte("OTHER"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	ext, err := New(Options{Patterns: []string{"**/*.log"}, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(ext, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	h := openLog(t, ch, "/app.log")
	// The facade hides the name, defers its own delete, and later lands it
	// through a second unlink dispatch before the recreate.
	ext.MarkPathUnlinked("/app.log")
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/app.log"}); err != nil {
		t.Fatalf("landing unlink = %v", err)
	}
	if _, ok := b.Node("/app.log"); ok {
		t.Fatal("the landing unlink did not remove the old remote entry")
	}
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/app.log",
		Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil || res.Handle == nil {
		t.Fatalf("recreate = %v", err)
	}
	if _, lerr := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpLookup, Path: "/app.log"}); lerr != nil {
		t.Fatalf("lookup of the recreated name = %v, want the new file", lerr)
	}
	// The old fd is anonymous and never republishes the name.
	if err := h.Write(ctx, 0, []byte("X")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if node, ok := b.Node("/app.log"); !ok || len(node.Data) != 0 {
		t.Fatalf("/app.log = %q ok=%v, want the untouched empty new file", node.Data, ok)
	}
}

// A truncated append buffer is the size of record: SETATTR replies with the
// truncated Attr and later GETATTRs override the lower plane's committed
// size until the buffer lands.
func TestPortTruncateReportsPendingSizeOnSetAttrAndGetAttr(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app.log", []byte("abcdef")) // 6 bytes
	ch := newStack(t, b, "**/*.log")
	ctx := context.Background()

	h := openLog(t, ch, "/app.log")
	sres, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/app.log", Handle: h, Size: 2, HasSize: true})
	if err != nil {
		t.Fatalf("ftruncate(2) = %v", err)
	}
	if sres.Attr == nil || sres.Attr.Size != 2 {
		t.Fatalf("setattr reply attr = %+v, want size 2", sres.Attr)
	}
	gres, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/app.log"})
	if err != nil {
		t.Fatalf("getattr = %v", err)
	}
	if gres.Attr == nil || gres.Attr.Size != 2 {
		t.Fatalf("getattr after truncate = %+v, want size 2", gres.Attr)
	}

	// Extending back up reports the grown pending size too.
	sres, err = ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/app.log", Handle: h, Size: 12, HasSize: true})
	if err != nil {
		t.Fatalf("ftruncate(12) = %v", err)
	}
	if sres.Attr == nil || sres.Attr.Size != 12 {
		t.Fatalf("setattr reply attr = %+v, want size 12", sres.Attr)
	}
	gres, err = ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/app.log"})
	if err != nil {
		t.Fatalf("getattr = %v", err)
	}
	if gres.Attr == nil || gres.Attr.Size != 12 {
		t.Fatalf("getattr after extend = %+v, want size 12", gres.Attr)
	}

	// The pending size is real: the flush lands the truncated image.
	if err := h.Sync(ctx); err != nil {
		t.Fatalf("fsync after truncate = %v", err)
	}
	node, _ := b.Node("/app.log")
	if len(node.Data) != 12 || string(node.Data[:2]) != "ab" {
		t.Fatalf("backend after truncate flush = %q, want ab + zero pad (12 bytes)", node.Data)
	}
}

// A live-name fd's O_APPEND baseline never adopts an anonymous fd's
// buffered bytes: after a same-name recreate they belong to the deleted
// object. The anonymous fd with the dirty tail is NOT the newest claim, so
// a head-only anonymization would leave it live on the shared path.
func TestPortRecreateAppendIgnoresAnonymousBuffer(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app.log", []byte("OLDDATA"))
	ch := newStack(t, b, "**/*.log")
	ctx := context.Background()

	old1 := openLog(t, ch, "/app.log")
	old2 := openLog(t, ch, "/app.log") // newest claim (head)
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/app.log"}); err != nil {
		t.Fatalf("unlink = %v", err)
	}
	// The earlier fd holds unflushed tail bytes of the deleted object.
	if err := old1.Write(ctx, 7, []byte("TAIL")); err != nil {
		t.Fatal(err)
	}
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/app.log",
		Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil || res.Handle == nil {
		t.Fatalf("recreate = %v", err)
	}
	fresh := res.Handle
	if err := fresh.Write(ctx, 0, []byte("NEW")); err != nil {
		t.Fatal(err)
	}
	if err := fresh.Sync(ctx); err != nil {
		t.Fatalf("fresh fsync = %v", err)
	}
	// A later O_APPEND writer of the recreated name must compose over the
	// NEW object only.
	ares, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/app.log",
		Flags: uint32(syscall.O_CREAT | syscall.O_APPEND | syscall.O_RDWR), Mode: 0o644})
	if err != nil || ares.Handle == nil {
		t.Fatalf("append open = %v", err)
	}
	if err := ares.Handle.Write(ctx, 0, []byte("!")); err != nil {
		t.Fatal(err)
	}
	if err := ares.Handle.Sync(ctx); err != nil {
		t.Fatalf("append fsync = %v (the anonymous tail leaked into the baseline)", err)
	}
	node, _ := b.Node("/app.log")
	if string(node.Data) != "NEW!" {
		t.Fatalf("/app.log = %q, want NEW! (the anonymous tail leaked into the new object)", node.Data)
	}
	_ = old2
}

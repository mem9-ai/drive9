package layer

// qiffang fourteenth round: a handle's own landed entry never fences its
// newer dirty buffer; an empty overlay image is authoritative across a
// rename; every open descriptor follows a rename; every mode mutation
// advances the generation.

import (
	"context"
	"net/http/httptest"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// write → fsync → write before the next event poll: the previous commit
// appears in `changed`, but it is THIS handle's own landing — the newer
// dirty buffer must stay unfenced (Write/Sync/Release all succeed and the
// final overlay carries the latest bytes).
func TestPortOwnLandingDoesNotFenceNewerDirtyBuffer(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("seed"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	l, ch := mStack(t, ts)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if err := h.Write(ctx, 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(ctx, 0, []byte("BBBB")); err != nil {
		t.Fatal(err)
	}
	// The event poll that observes the path changed (by our own Sync).
	l.refreshEventsForTest(ctx)
	// The newer buffer is not conflicted: further writes and the sync
	// land it whole.
	if err := h.Write(ctx, 4, []byte("CC")); err != nil {
		t.Fatalf("write after self-landing poll: %v", err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatalf("sync after self-landing poll: %v", err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	rd, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/f", Offset: 0, Limit: 8})
	if err != nil {
		t.Fatal(err)
	}
	if string(rd.Data) != "BBBBCC" {
		t.Fatalf("final overlay = %q, want BBBBCC", rd.Data)
	}
}

// A foreign update still fences: the same sequence with an EXTERNAL
// upsert between our sync and the poll must conflict, never splice.
func TestPortForeignLandingStillFences(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("seed"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	l, ch := mStack(t, ts)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if err := h.Write(ctx, 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(ctx, 0, []byte("BBBB")); err != nil {
		t.Fatal(err)
	}
	postLayerEntry(t, ts, "L1", "/f", "upsert", "FOREIGN")
	l.refreshEventsForTest(ctx)
	if err := h.Write(ctx, 4, []byte("CC")); err == nil {
		t.Fatal("write over foreign tip succeeded, want EAGAIN fence")
	}
}

// An empty overlay entry is authoritative: a created-then-closed
// layer-only file renames as an empty image (no 404), and a base-backed
// file truncated to zero renames as empty instead of resurrecting the
// base bytes at the destination.
func TestPortRenameTreatsEmptyOverlayAsAuthoritative(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/base", []byte("original"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	_, ch := mStack(t, ts)
	ctx := context.Background()

	// Layer-only empty file: create, close, rename.
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/empty", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/empty", Path2: "/moved-empty"}); err != nil {
		t.Fatalf("rename of empty layer-only file: %v", err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/moved-empty"}); err != nil {
		t.Fatalf("renamed empty file missing: %v", err)
	}

	// Base-backed file truncated to zero, then renamed: the destination
	// is empty, not the original base contents.
	res2, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/base", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/base", Size: 0, Handle: res2.Handle}); err != nil {
		t.Fatal(err)
	}
	if err := res2.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/base", Path2: "/moved-zero"}); err != nil {
		t.Fatalf("rename of zeroed file: %v", err)
	}
	rd, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/moved-zero", Offset: 0, Limit: 8})
	if err != nil {
		t.Fatal(err)
	}
	if len(rd.Data) != 0 {
		t.Fatalf("moved zeroed file = %q, want empty", rd.Data)
	}
}

// Every open descriptor follows a rename: with two writable descriptors
// on one path, writing through the first, renaming, then syncing it must
// land the edit at the NEW name — the old name stays gone.
func TestPortRenameRetargetsEveryDescriptor(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/old", []byte("hello"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	_, ch := mStack(t, ts)
	ctx := context.Background()

	open := func() fs.Handle {
		res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/old", Flags: uint32(syscall.O_RDWR)})
		if err != nil {
			t.Fatal(err)
		}
		return res.Handle
	}
	first, second := open(), open()
	if err := first.Write(ctx, 0, []byte("A")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/old", Path2: "/new"}); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if err := first.Sync(ctx); err != nil {
		t.Fatalf("sync through the older descriptor: %v", err)
	}
	if err := second.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := first.Release(ctx); err != nil {
		t.Fatal(err)
	}
	rd, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/new", Offset: 0, Limit: 8})
	if err != nil {
		t.Fatal(err)
	}
	if string(rd.Data) != "Aello" {
		t.Fatalf("/new = %q, want Aello", rd.Data)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/old"}); err == nil {
		t.Fatal("old name resurrected by the older descriptor's sync")
	}
}

// A chmod acknowledged while an older snapshot uploads advances the
// mutation generation: the stale completion must not retire it. The
// white-box row pins the generation advance over an already-dirty
// handle; the behavioral row pins the persisted mode after the racing
// completion.
func TestPortSetModeAdvancesGenerationOverDirtyHandle(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("seed"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	_, ch := mStack(t, ts)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle.(*layerHandle)
	if err := h.Write(ctx, 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	before := h.wgen
	h.mu.Unlock()
	h.SetMode(0o600)
	h.mu.Lock()
	after := h.wgen
	h.mu.Unlock()
	if after == before {
		t.Fatal("SetMode over a dirty handle did not advance the mutation generation")
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	// The persisted entry carries the chmod, not the pre-chmod snapshot.
	kind, mode, _, _, ok := h.l.lookup("/f")
	if !ok || kind != "file" {
		t.Fatalf("entry missing after chmod sync: ok=%v kind=%v", ok, kind)
	}
	if mode&0o777 != 0o600 {
		t.Fatalf("persisted mode = %o, want 600", mode&0o777)
	}
}

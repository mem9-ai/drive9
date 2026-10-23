package writeback

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/cache"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func newStack(t *testing.T, policy Policy, b *testbackend.Backend) (*fs.Chain, *Writeback) {
	t.Helper()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: policy, Client: cl})
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

func open(t *testing.T, s *fs.Chain, path string) fs.Handle {
	t.Helper()
	res, err := s.Dispatch(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: path, Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if res.Handle == nil {
		t.Fatal("no handle")
	}
	return res.Handle
}

func TestWriteSyncCommitsPerWrite(t *testing.T) {
	b := testbackend.NewBackend()
	s, _ := newStack(t, PolicyWriteSync, b)
	h := open(t, s, "/f")
	ctx := context.Background()

	if err := h.Write(ctx, 0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	// write-sync: durable the moment Write returns, no flush needed
	if n, ok := b.Node("/f"); !ok || string(n.Data) != "hello" {
		t.Fatalf("backend after write = %+v", n)
	}
	if err := h.Write(ctx, 5, []byte(" world")); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/f"); string(n.Data) != "hello world" {
		t.Fatalf("backend after second write = %q", n.Data)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/f"); string(n.Data) != "hello world" {
		t.Fatalf("backend after release = %q", n.Data)
	}
}

func TestCloseSyncBuffersUntilClose(t *testing.T) {
	b := testbackend.NewBackend()
	s, _ := newStack(t, PolicyCloseSync, b)
	h := open(t, s, "/f")
	ctx := context.Background()

	if err := h.Write(ctx, 0, []byte("buffered")); err != nil {
		t.Fatal(err)
	}
	if _, ok := b.Node("/f"); ok {
		t.Fatal("close-sync must not commit before close")
	}
	// read-your-writes through the buffer
	buf := make([]byte, 8)
	if n, err := h.Read(ctx, 0, buf); err != nil || string(buf[:n]) != "buffered" {
		t.Fatalf("read-your-writes = %d %q %v", n, buf[:n], err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if n, ok := b.Node("/f"); !ok || string(n.Data) != "buffered" {
		t.Fatalf("backend after release = %+v", n)
	}
}

func TestWriteBackPolicyCommitsByClose(t *testing.T) {
	// writeback degrades to close-sync until the async queue ports; the
	// durability boundary (visible after Release) must hold either way.
	b := testbackend.NewBackend()
	s, _ := newStack(t, PolicyWriteBack, b)
	h := open(t, s, "/f")
	ctx := context.Background()

	if err := h.Write(ctx, 0, []byte("wb")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if n, ok := b.Node("/f"); !ok || string(n.Data) != "wb" {
		t.Fatalf("backend after flush = %+v", n)
	}
}

func TestCommitRetriesOnConflict(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("base"))
	b.ForceConflict("/f", 2) // two forced 409s, then success
	s, _ := newStack(t, PolicyWriteSync, b)
	h := open(t, s, "/f")

	if err := h.Write(context.Background(), 0, []byte("rewritten")); err != nil {
		t.Fatalf("conflict retry failed: %v", err)
	}
	if n, _ := b.Node("/f"); string(n.Data) != "rewritten" {
		t.Fatalf("backend = %q", n.Data)
	}
}

func TestReadCacheInvalidatedByCommit(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("old"))
	ts := httptest.NewServer(b)
	defer ts.Close()
	cl := client.New(ts.URL, "")

	bus := fs.NewEventBus()
	cacheExt, err := cache.New(cache.Options{Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{Policy: PolicyWriteSync, Client: cl, Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(cacheExt, wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// warm the read cache
	if res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/f"}); err != nil || string(res.Data) != "old" {
		t.Fatalf("warm read = %q %v", res.Data, err)
	}
	gets := b.Requests()["GET /v1/fs/f"]

	// a write through writeback must invalidate the cached copy
	h := open(t, ch, "/f")
	if err := h.Write(ctx, 0, []byte("new")); err != nil {
		t.Fatal(err)
	}
	if res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/f"}); err != nil || string(res.Data) != "new" {
		t.Fatalf("post-write read = %q %v", res.Data, err)
	}
	if got := b.Requests()["GET /v1/fs/f"]; got <= gets {
		t.Fatalf("stale cache served: GETs %d -> %d", gets, got)
	}
}

func TestUnlinkKeepsAnonymousHandleDiscardsAtRelease(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newStack(t, PolicyCloseSync, b)
	ctx := context.Background()

	h := open(t, ch, "/f")
	if err := h.Write(ctx, 0, []byte("anon")); err != nil {
		t.Fatal(err)
	}
	// unlink while open: the entry goes, the fd keeps serving
	if res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/f"}); err != nil {
		t.Fatalf("unlink: %v %v", res, err)
	}
	buf := make([]byte, 4)
	if n, err := h.Read(ctx, 0, buf); err != nil || string(buf[:n]) != "anon" {
		t.Fatalf("anonymous read = %d %q %v", n, buf[:n], err)
	}
	if err := h.Write(ctx, 4, []byte("!")); err != nil {
		t.Fatal(err)
	}
	// Release must NOT recreate the deleted remote pathname
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if n, ok := b.Node("/f"); ok && n.Data != nil {
		t.Fatalf("release resurrected unlinked path: %q", n.Data)
	}
}

// TestUnlinkThenRecreateStaysCommittable locks the per-handle unlink flag
// (v1 fh.Unlinked): unlinking a path must not doom a fresh writer on a
// re-created path, and the stale handle's Release must not clobber it.
func TestUnlinkThenRecreateStaysCommittable(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newStack(t, PolicyCloseSync, b)
	ctx := context.Background()

	stale := open(t, ch, "/re")
	if err := stale.Write(ctx, 0, []byte("stale")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/re"}); err != nil {
		t.Fatal(err)
	}
	fresh := open(t, ch, "/re")
	if err := fresh.Write(ctx, 0, []byte("fresh")); err != nil {
		t.Fatal(err)
	}
	if err := fresh.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/re"); string(n.Data) != "fresh" {
		t.Fatalf("fresh writer discarded: %q", n.Data)
	}
	_ = stale.Release(ctx)
	if n, _ := b.Node("/re"); string(n.Data) != "fresh" {
		t.Fatalf("stale handle clobbered re-created path: %q", n.Data)
	}
}

// TestRenameRetargetsOpenWriter pins v1 rename-of-open-writer: buffered
// bytes commit at the new path and the old path stays gone.
func TestRenameRetargetsOpenWriter(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newStack(t, PolicyCloseSync, b)
	ctx := context.Background()

	h := open(t, ch, "/src")
	if err := h.Write(ctx, 0, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/src", Path2: "/dst"}); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if n, ok := b.Node("/src"); ok && n.Data != nil {
		t.Fatalf("old path resurrected: %q", n.Data)
	}
	if n, _ := b.Node("/dst"); string(n.Data) != "payload" {
		t.Fatalf("new path = %q", n.Data)
	}
}

func TestAppendOnExistingKeepsBase(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newStack(t, PolicyCloseSync, b)
	ctx := context.Background()

	seed := open(t, ch, "/log2")
	if err := seed.Write(ctx, 0, []byte("head")); err != nil {
		t.Fatal(err)
	}
	if err := seed.Release(ctx); err != nil {
		t.Fatal(err)
	}
	// bash `>>` passes O_CREAT|O_APPEND: the existing base must be
	// preserved and the append land at its tail (v1 IsNew is existence).
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/log2", Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY | syscall.O_APPEND), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if err := h.Write(ctx, 0, []byte("-tail")); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/log2"); string(n.Data) != "head-tail" {
		t.Fatalf("appended = %q", n.Data)
	}
}

// TestLazyCreateVisibleToLookupBeforeCommit pins the v1 pendingIndex
// overlay: a lazily created file must resolve (git chmods .git/config.lock
// by path after creating it) before its commit boundary.
func TestLazyCreateVisibleToLookupBeforeCommit(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newStack(t, PolicyCloseSync, b)
	ctx := context.Background()

	h := open(t, ch, "/lazy.lock")
	if err := h.Write(ctx, 0, []byte("config")); err != nil {
		t.Fatal(err)
	}
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpLookup, Path: "/lazy.lock"})
	if err != nil {
		t.Fatalf("lookup before commit: %v", err)
	}
	if res.Attr == nil || res.Attr.Size != 6 {
		t.Fatalf("overlay attr = %+v", res.Attr)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/lazy.lock"); string(n.Data) != "config" {
		t.Fatalf("backend = %q", n.Data)
	}
}

// TestCreateNoWriteMaterializesEmptyFile pins git's filesystem probe
// (create `.git/ä`, never write, close, unlink): an O_CREAT claim with no
// writes must still land as an empty remote file at Release.
func TestCreateNoWriteMaterializesEmptyFile(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newStack(t, PolicyCloseSync, b)
	ctx := context.Background()

	h := open(t, ch, "/probe")
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	n, ok := b.Node("/probe")
	if !ok || len(n.Data) != 0 {
		t.Fatalf("empty create vanished: ok=%v data=%q", ok, n.Data)
	}
	// and the follow-up unlink must succeed against the materialized file
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/probe"}); err != nil {
		t.Fatalf("unlink after empty create: %v", err)
	}
	if _, ok := b.Node("/probe"); ok {
		t.Fatal("probe resurrected after unlink")
	}
}

// TestWriteSyncReleaseKeepsCommittedContent locks the landed-guard: under
// write-sync every Write commits and clears dirty, so Release must NOT
// re-materialize an empty image over the committed file (the fuse-smoke
// regression where `printf > f` left an empty remote file).
func TestWriteSyncReleaseKeepsCommittedContent(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newStack(t, PolicyWriteSync, b)
	ctx := context.Background()

	h := open(t, ch, "/sync.txt")
	if err := h.Write(ctx, 0, []byte("committed")); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/sync.txt"); string(n.Data) != "committed" {
		t.Fatalf("per-write commit = %q", n.Data)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/sync.txt"); string(n.Data) != "committed" {
		t.Fatalf("release clobbered: %q", n.Data)
	}
}

// TestTruncateAtOpenRebasesBuffer pins the tier-transition regression: the
// kernel delivers open(O_TRUNC) on an existing file as OPEN (flags without
// O_CREAT/O_TRUNC) + SETATTR(size=0). The live handle must rebase locally —
// a remote truncate leaves the buffer preloading the old base and the close
// republishes the stale size (8MiB instead of 10KiB in the smoke).
func TestTruncateAtOpenRebasesBuffer(t *testing.T) {
	b := testbackend.NewBackend()
	ch, _ := newStack(t, PolicyCloseSync, b)
	ctx := context.Background()

	seed := open(t, ch, "/tier.bin")
	big := make([]byte, 256*1024)
	if err := seed.Write(ctx, 0, big); err != nil {
		t.Fatal(err)
	}
	if err := seed.Release(ctx); err != nil {
		t.Fatal(err)
	}

	// kernel open(O_TRUNC) on existing: OpCreate flags carry neither
	// O_CREAT nor O_TRUNC, followed by SETATTR(size=0) on the live handle.
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/tier.bin", Flags: uint32(syscall.O_WRONLY)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/tier.bin", Size: 0}); err != nil {
		t.Fatal(err)
	}
	if err := h.Write(ctx, 0, []byte("small")); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ := b.Node("/tier.bin")
	if len(n.Data) != 5 || string(n.Data) != "small" {
		t.Fatalf("tier rewrite = %d bytes %q", len(n.Data), n.Data)
	}
}

func TestAppendWritesTrackBufferTail(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	defer ts.Close()
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyCloseSync, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/log", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR | syscall.O_APPEND), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	for _, chunk := range []string{"one", "two", "three"} {
		if err := h.Write(ctx, 0, []byte(chunk)); err != nil { // kernel may pass off=0
			t.Fatal(err)
		}
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/log"); string(n.Data) != "onetwothree" {
		t.Fatalf("appended = %q", n.Data)
	}
}

func TestPendingModeAppliesAtCommitBoundary(t *testing.T) {
	b := testbackend.NewBackend()
	ch, wb := newStack(t, PolicyCloseSync, b)
	ctx := context.Background()

	h := open(t, ch, "/f")
	if err := h.Write(ctx, 0, []byte("data")); err != nil {
		t.Fatal(err)
	}
	// chmod while dirty: deferred, not applied remotely yet
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/f", Mode: 0o600}); err != nil {
		t.Fatal(err)
	}
	// close-sync: nothing is remote yet, so there is no mode to leak
	if n, ok := b.Node("/f"); ok && n.Data != nil {
		t.Fatalf("dirty image leaked before close: %q", n.Data)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/f"); n.Mode != 0o600 {
		t.Fatalf("pending mode not applied at commit: %o", n.Mode)
	}
	_ = wb
}

// The streaming clean-path truncate: a size change beyond the in-memory
// threshold commits through a lazily-read multipart upload — the kept
// prefix is fetched in ranges, never the whole image, and the content
// lands as prefix + zero tail.
func TestCleanPathTruncateStreamsLargeSize(t *testing.T) {
	old := maxPathTruncateInMemoryBytes
	maxPathTruncateInMemoryBytes = 4096
	t.Cleanup(func() { maxPathTruncateInMemoryBytes = old })

	b := testbackend.NewBackend()
	seed := bytes.Repeat([]byte("x"), 64*1024)
	b.Seed("/big", seed)
	ch, _ := newStack(t, PolicyCloseSync, b)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/big", Size: 16 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	if res.Attr == nil || res.Attr.Size != 16*1024 {
		t.Fatalf("reply attr = %+v", res.Attr)
	}
	n, ok := b.Node("/big")
	if !ok || int64(len(n.Data)) != 16*1024 {
		t.Fatalf("node = %d bytes", len(n.Data))
	}
	// The kept prefix is min(old, new) = 16KB, all sourced remotely.
	if !bytes.Equal(n.Data, seed[:16*1024]) {
		t.Fatal("truncated image is not the kept prefix")
	}
	// The prefix streamed in ranged GETs (multipart parts), not one
	// whole-file download.
	if gets := b.Requests()["GET /v1/fs/big"]; gets < 1 {
		t.Fatal("prefix was never fetched (streamed upload read nothing)")
	}
}

// A large grow zero-extends without reading beyond the old size.
func TestCleanPathTruncateGrowZeroExtends(t *testing.T) {
	old := maxPathTruncateInMemoryBytes
	maxPathTruncateInMemoryBytes = 4096
	t.Cleanup(func() { maxPathTruncateInMemoryBytes = old })

	b := testbackend.NewBackend()
	b.Seed("/g", bytes.Repeat([]byte("y"), 8*1024))
	ch, _ := newStack(t, PolicyCloseSync, b)

	if _, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Path: "/g", Size: 32 * 1024}); err != nil {
		t.Fatal(err)
	}
	n, _ := b.Node("/g")
	if int64(len(n.Data)) != 32*1024 || !bytes.Equal(n.Data[:8*1024], bytes.Repeat([]byte("y"), 8*1024)) {
		t.Fatalf("grown node = %d bytes", len(n.Data))
	}
	if !bytes.Equal(n.Data[8*1024:], make([]byte, 24*1024)) {
		t.Fatal("tail not zero-filled")
	}
}

// A size change on a path with no remote entry falls through to the
// terminal (ENOENT), not a streaming upload of nothing.
func TestCleanPathTruncateMissingPathFallsThrough(t *testing.T) {
	old := maxPathTruncateInMemoryBytes
	maxPathTruncateInMemoryBytes = 4096
	t.Cleanup(func() { maxPathTruncateInMemoryBytes = old })

	b := testbackend.NewBackend()
	ch, _ := newStack(t, PolicyCloseSync, b)

	_, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Path: "/none", Size: 64 * 1024})
	if !errors.Is(err, fs.ErrNotFound) {
		t.Fatalf("missing-path truncate = %v, want ErrNotFound", err)
	}
}

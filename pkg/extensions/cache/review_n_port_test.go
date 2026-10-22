package cache

// qiffang twenty-second round: a checkout/reset must invalidate the OPEN
// handles' block prefetchers too — Read serves prefetched blocks ahead of
// the memory/disk tiers, so a live descriptor kept reading the pre-reset
// image.

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func TestPortCheckoutDropsOpenHandlePrefetch(t *testing.T) {
	const size = 5 << 20
	base := bytes.Repeat([]byte("A"), size)
	b := testbackend.NewBackend()
	b.Seed("/big.bin", base)
	url := newTestServer(t, b)
	cl := client.New(url, "")
	bus := fs.NewEventBus()
	c, err := New(Options{
		MaxSize:            64 << 20,
		MaxFileSize:        1 << 20, // smaller than the file: the prefetcher arms
		SequentialPrefetch: true,
		PrefetchClient:     cl,
		Events:             bus,
	})
	if err != nil {
		t.Fatal(err)
	}
	ch := newChain(t, url, c)

	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: "/big.bin", Flags: 0})
	if err != nil || res.Handle == nil {
		t.Fatalf("open = %v handle=%v", err, res.Handle)
	}
	h := res.Handle
	ctx := context.Background()
	buf := make([]byte, 64<<10)
	if _, err := h.Read(ctx, 0, buf); err != nil {
		t.Fatalf("first read = %v", err)
	}
	// The reset: the remote image becomes B while the fd stays open.
	if err := cl.WriteCtx(ctx, "/big.bin", bytes.Repeat([]byte("B"), size)); err != nil {
		t.Fatal(err)
	}
	bus.Emit(fs.Event{Kind: fs.EventCheckout, Path: "/"})

	next := make([]byte, 64<<10)
	if _, err := h.Read(ctx, 64<<10, next); err != nil {
		t.Fatalf("second read = %v", err)
	}
	if next[0] != 'B' {
		t.Fatalf("read after checkout = %q..., want the reset image", next[:4])
	}
}

// A read dispatched before a checkout must not admit its pre-reset bytes
// when it completes afterwards.
func TestPortCheckoutFencesInFlightReadAdmission(t *testing.T) {
	gate := make(chan struct{})
	release := make(chan struct{})
	inner := terminalFunc(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		if op.Kind == fs.OpRead {
			close(gate)
			<-release
			return fs.Result{Data: []byte("STALE")}, nil
		}
		return fs.Result{}, nil
	})
	bus := fs.NewEventBus()
	c, err := New(Options{MaxSize: 1 << 20, MaxFileSize: 1 << 20, Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(c, inner)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, rerr := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpRead, Path: "/f", Offset: 0, Limit: 0})
		done <- rerr
	}()
	<-gate
	bus.Emit(fs.Event{Kind: fs.EventCheckout, Path: "/"})
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("read = %v", err)
	}
	if _, ok := c.ReadCached("/f", 0); ok {
		t.Fatal("pre-reset bytes admitted into the read cache")
	}
}

// terminalFunc adapts a plain function to fs.Handler for tests.
type terminalFunc func(context.Context, *fs.Op) (fs.Result, error)

func (f terminalFunc) Do(ctx context.Context, op *fs.Op, _ fs.Next) (fs.Result, error) {
	return f(ctx, op)
}

// gatedHandle blocks its Read until released (a fetch racing a reset).
type gatedHandle struct {
	gate    chan struct{}
	release chan struct{}
	data    []byte
	closed  bool
}

func (g *gatedHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	if !g.closed {
		g.closed = true
		close(g.gate)
	}
	<-g.release
	n := copy(dest, g.data[off:])
	if int64(off)+int64(n) < int64(len(g.data)) {
		return n, nil
	}
	return n, nil
}
func (g *gatedHandle) Write(ctx context.Context, off int64, data []byte) error { return nil }
func (g *gatedHandle) Sync(ctx context.Context) error                          { return nil }
func (g *gatedHandle) Flush(ctx context.Context) error                         { return nil }
func (g *gatedHandle) Release(ctx context.Context) error                       { return nil }

// The handle-level whole-file admission carries the same reset fence as the
// path-addressed branch: a fetch that raced a checkout is not cached.
func TestPortCheckoutFencesHandleReadAdmission(t *testing.T) {
	c, err := New(Options{MaxSize: 1 << 20, MaxFileSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	gh := &gatedHandle{gate: make(chan struct{}), release: make(chan struct{}), data: []byte("OLDOLD00")}
	h := &cachingHandle{cache: c, inner: gh, path: "/f", rev: 7, size: 8}
	done := make(chan struct{})
	go func() {
		_, _ = h.Read(context.Background(), 0, make([]byte, 8))
		close(done)
	}()
	<-gh.gate
	c.invalidateAllTiers()
	close(gh.release)
	<-done
	if _, ok := c.ReadCached("/f", 7); ok {
		t.Fatal("pre-reset bytes admitted into the read cache from a handle read")
	}
}

// A per-path invalidated event drops the open handles' prefetched blocks
// too (Read serves them ahead of the invalidated tiers).
func TestPortInvalidatedDropsOpenHandlePrefetch(t *testing.T) {
	const size = 5 << 20
	b := testbackend.NewBackend()
	b.Seed("/big.bin", bytes.Repeat([]byte("A"), size))
	url := newTestServer(t, b)
	cl := client.New(url, "")
	bus := fs.NewEventBus()
	c, err := New(Options{MaxSize: 64 << 20, MaxFileSize: 1 << 20, SequentialPrefetch: true, PrefetchClient: cl, Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	ch := newChain(t, url, c)
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: "/big.bin", Flags: 0})
	if err != nil || res.Handle == nil {
		t.Fatalf("open = %v", err)
	}
	h := res.Handle
	ctx := context.Background()
	if _, err := h.Read(ctx, 0, make([]byte, 64<<10)); err != nil {
		t.Fatal(err)
	}
	if err := cl.WriteCtx(ctx, "/big.bin", bytes.Repeat([]byte("B"), size)); err != nil {
		t.Fatal(err)
	}
	bus.Emit(fs.Event{Kind: fs.EventInvalidated, Path: "/big.bin"})
	next := make([]byte, 64<<10)
	if _, err := h.Read(ctx, 64<<10, next); err != nil {
		t.Fatal(err)
	}
	if next[0] != 'B' {
		t.Fatalf("read after invalidate = %q..., want the new image", next[:4])
	}
}

// recNamespace is a bare DirListingCache recording publications.
type recNamespace struct {
	mu      sync.Mutex
	puts    map[string][]fs.DirEntryInfo
	upserts []fs.DirEntryInfo
}

func (n *recNamespace) Get(string) ([]fs.DirEntryInfo, bool) { return nil, false }
func (n *recNamespace) Put(dir string, entries []fs.DirEntryInfo) {
	n.mu.Lock()
	if n.puts == nil {
		n.puts = map[string][]fs.DirEntryInfo{}
	}
	n.puts[dir] = append([]fs.DirEntryInfo(nil), entries...)
	n.mu.Unlock()
}
func (n *recNamespace) Upsert(_ string, entry fs.DirEntryInfo) {
	n.mu.Lock()
	n.upserts = append(n.upserts, entry)
	n.mu.Unlock()
}
func (n *recNamespace) Invalidate(string)       {}
func (n *recNamespace) InvalidatePrefix(string) {}
func (n *recNamespace) InvalidateAll()          {}

// The sibling listing publication preserves the extent identity/ownership
// and is fenced by the reset epoch.
func TestPortSiblingPrefetchKeepsIdentityAndFencesReset(t *testing.T) {
	var epoch atomic.Uint64
	ns := &recNamespace{}
	p := &dirPrefetcher{namespace: ns, genFn: func() uint64 { return epoch.Load() }}
	entries := []fs.Entry{{
		Name: "f", Attr: fs.Attr{Kind: fs.KindFile, Mode: 0o644, Size: 3, Rev: 7,
			ResourceID: "res-1", ExtentIno: 77, Uid: 2000, Gid: 2000, HasOwner: true},
	}}
	load := func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		return fs.Result{Entries: entries}, nil
	}
	p.loadSiblingDirectory(load, "/d")
	got := ns.puts["/d"]
	if len(got) != 1 || got[0].ExtentIno != 77 || !got[0].HasUID || got[0].Uid != 2000 || got[0].ResourceID != "res-1" {
		t.Fatalf("sibling row = %+v, want the extent identity/owner preserved", got)
	}

	// A reset while the LIST is in flight: the publish must be dropped.
	epoch.Add(1)
	racing := func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		epoch.Add(1)
		return fs.Result{Entries: entries}, nil
	}
	p.loadSiblingDirectory(racing, "/reset")
	if _, ok := ns.puts["/reset"]; ok {
		t.Fatal("pre-reset sibling listing repopulated the cleared namespace cache")
	}
}

// The disk-hit promotion runs through the same reset protocol as every
// other publication: an epoch captured before a reset can no longer
// repopulate the memory tier.
func TestPortDiskHitPromotionFencesReset(t *testing.T) {
	c, err := New(Options{MaxSize: 1 << 20, MaxFileSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	epoch := c.epoch.Load()
	c.invalidateAllTiers()
	called := false
	if c.publishIfCurrent(epoch, func() { called = true }) {
		t.Fatal("stale publication reported current")
	}
	if called {
		t.Fatal("disk-hit promotion ran after the reset")
	}
	if !c.publishIfCurrent(c.epoch.Load(), func() { called = true }) || !called {
		t.Fatal("current publication was refused")
	}
}

// An anonymous fd bypasses the cache's path-keyed tiers: after a rename-over
// the old target fd must read its OLD object, not the replacement's pending
// image.
func TestPortAnonymousFdBypassesPendingCache(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/a", []byte("HELLO"))
	b.Seed("/b", []byte("OTHER"))
	url := newTestServer(t, b)
	cl := client.New(url, "")
	c, err := New(Options{MaxSize: 1 << 20, MaxFileSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	wb, err := writeback.New(writeback.Options{Policy: writeback.PolicyWriteBack, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(c, wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	// The old target's read-only fd.
	target, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/b", Flags: 0})
	if err != nil || target.Handle == nil {
		t.Fatalf("open target = %v", err)
	}
	// The source writer, kept open across the rename.
	src, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/a", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR)})
	if err != nil || src.Handle == nil {
		t.Fatalf("open source = %v", err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/a", Path2: "/b"}); err != nil {
		t.Fatalf("rename = %v", err)
	}
	if err := src.Handle.Write(ctx, 0, []byte("FRESH")); err != nil {
		t.Fatal(err)
	}
	if err := src.Handle.Flush(ctx); err != nil { // staged under the new name
		t.Fatal(err)
	}
	buf := make([]byte, 16)
	n, rerr := target.Handle.Read(ctx, 0, buf)
	if rerr != nil {
		t.Fatal(rerr)
	}
	if got := string(buf[:n]); got != "OTHER" {
		t.Fatalf("old target fd read %q, want the OLD object OTHER", got)
	}
	_ = src.Handle.Release(ctx)
	_ = target.Handle.Release(ctx)
}

// A dirty sibling's write must not be shadowed by a stable tier hit on the
// same path.
func TestPortSiblingWriteBypassesStableTiers(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("OOOOO"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	c, err := New(Options{MaxSize: 1 << 20, MaxFileSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(c, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	reader, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: 0})
	if err != nil || reader.Handle == nil {
		t.Fatalf("open reader = %v", err)
	}
	// Warm the stable tiers through the reader.
	buf := make([]byte, 5)
	if n, rerr := reader.Handle.Read(ctx, 0, buf); rerr != nil || n != 5 {
		t.Fatalf("warm read = %d err=%v", n, rerr)
	}
	writer, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
	if err != nil || writer.Handle == nil {
		t.Fatalf("open writer = %v", err)
	}
	if err := writer.Handle.Write(ctx, 0, []byte("AFTER")); err != nil {
		t.Fatal(err)
	}
	after := make([]byte, 5)
	if n, rerr := reader.Handle.Read(ctx, 0, after); rerr != nil || string(after[:n]) != "AFTER" {
		t.Fatalf("reader after sibling write = %q err=%v, want AFTER", after[:n], rerr)
	}
	_ = writer.Handle.Release(ctx)
	_ = reader.Handle.Release(ctx)
}

// A single-path invalidation revokes the reader's open-time identity.
func TestPortPathInvalidationRevokesReaderIdentity(t *testing.T) {
	c, err := New(Options{MaxSize: 1 << 20, MaxFileSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	h := &cachingHandle{cache: c, path: "/f", size: 5, rev: 7, grown: true}
	c.open.Store(h, struct{}{})
	c.invalidatePathTiers("/f")
	h.mu.Lock()
	size, rev, grown := h.size, h.rev, h.grown
	h.mu.Unlock()
	if size != 0 || rev != 0 || grown {
		t.Fatalf("identity = size %d rev %d grown %v, want a clean slate", size, rev, grown)
	}
}

// Dirty ownership: an unrelated reader's boundary must not un-dirty the
// writer's path.
func TestPortDirtyOwnershipIsPerHandle(t *testing.T) {
	c, err := New(Options{MaxSize: 1 << 20, MaxFileSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	writer := &cachingHandle{cache: c, path: "/f"}
	reader := &cachingHandle{cache: c, path: "/f"}
	c.noteDirty("/f", writer)
	c.noteDirty("/f", writer) // repeated writes keep ONE mark
	if !c.dirtyPath("/f") {
		t.Fatal("write did not mark the path dirty")
	}
	c.clearDirty("/f", reader)
	if !c.dirtyPath("/f") {
		t.Fatal("a reader's boundary cleared the writer's dirty mark")
	}
	c.clearDirty("/f", writer)
	if c.dirtyPath("/f") {
		t.Fatal("the writer's own commit boundary did not clear the mark")
	}
}

type failingSyncHandle struct{ fs.Handle }

func (failingSyncHandle) Sync(context.Context) error  { return errors.New("commit rejected") }
func (failingSyncHandle) Flush(context.Context) error { return errors.New("commit rejected") }

// A FAILED commit boundary keeps the writer's dirty ownership.
func TestPortFailedSyncKeepsDirtyOwnership(t *testing.T) {
	c, err := New(Options{MaxSize: 1 << 20, MaxFileSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	h := &cachingHandle{cache: c, path: "/f", inner: failingSyncHandle{}}
	c.noteDirty("/f", h)
	if err := h.Sync(context.Background()); err == nil {
		t.Fatal("sync unexpectedly succeeded")
	}
	if !c.dirtyPath("/f") {
		t.Fatal("a FAILED sync released the dirty ownership")
	}
	if err := h.Flush(context.Background()); err == nil {
		t.Fatal("flush unexpectedly succeeded")
	}
	if !c.dirtyPath("/f") {
		t.Fatal("a FAILED flush released the dirty ownership")
	}
}

// A landed commit revokes same-path readers' open-time identity.
func TestPortLandedRevokesReaderIdentity(t *testing.T) {
	bus := fs.NewEventBus()
	c, err := New(Options{MaxSize: 1 << 20, MaxFileSize: 1 << 20, Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	h := &cachingHandle{cache: c, path: "/f", size: 5, rev: 3, grown: true}
	c.open.Store(h, struct{}{})
	bus.Emit(fs.Event{Kind: fs.EventLanded, Path: "/f", Rev: 4})
	h.mu.Lock()
	size, rev, grown := h.size, h.rev, h.grown
	h.mu.Unlock()
	if size != 0 || rev != 0 || grown {
		t.Fatalf("identity = size %d rev %d grown %v, want a clean slate", size, rev, grown)
	}
}

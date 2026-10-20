package cache

// qiffang twenty-second round: a checkout/reset must invalidate the OPEN
// handles' block prefetchers too — Read serves prefetched blocks ahead of
// the memory/disk tiers, so a live descriptor kept reading the pre-reset
// image.

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
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

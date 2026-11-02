package cache

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// requests counts the backend requests of one method whose path ends in
// suffix (HEAD = stat, GET = data).
func requests(b *testbackend.Backend, method, suffix string) int {
	n := 0
	for k, v := range b.Requests() {
		if strings.HasPrefix(k, method+" ") && strings.HasSuffix(k, suffix) {
			n += v
		}
	}
	return n
}

// Loss of event-stream continuity (an SSE disconnect, and the catch-up that
// ends it) used to sweep the disk tier and zero every open descriptor's
// revision, leaving the descriptor off the revision-keyed tiers for the rest
// of its life (#1064, mechanism 3). Now the tier stays and the descriptor
// re-pins its revision against the remote: one stat per event, one per read
// while the stream is down, none once it caught up — and a remote change
// with a new revision is read fresh.
func TestUnverifiedRevalidatesHandleAndKeepsDiskTier(t *testing.T) {
	b := testbackend.NewBackend()
	old := bytes.Repeat([]byte("0123456789abcdef"), 1<<19) // 8 MiB
	b.Seed("/big", old)
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	bus := fs.NewEventBus()
	// No sequential prefetch: the disk range tier alone answers the repeat
	// reads, so a served read is exactly one GET or none.
	c, err := New(Options{MaxSize: 1 << 20, MaxFileSize: 1 << 20, PrefetchClient: cl, Events: bus,
		Disk: DiskReadCacheOptions{Dir: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	wb, err := writeback.New(writeback.Options{Policy: writeback.PolicyWriteBack, Client: cl, Events: bus})
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
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/big", Flags: 0})
	if err != nil || res.Handle == nil {
		t.Fatalf("open = %v", err)
	}
	h := res.Handle.(*cachingHandle)
	t.Cleanup(func() { _ = h.Release(ctx) })

	const off = 2 << 20
	buf := make([]byte, 1<<20)
	read := func(want []byte, step string) {
		t.Helper()
		n, err := h.Read(ctx, off, buf)
		if err != nil {
			t.Fatalf("%s: read = %v", step, err)
		}
		if !bytes.Equal(buf[:n], want[off:off+int64(len(buf))]) {
			t.Fatalf("%s: wrong bytes (n=%d)", step, n)
		}
	}
	expect := func(step string, gets, heads int) {
		t.Helper()
		if g, hd := requests(b, "GET", "/big"), requests(b, "HEAD", "/big"); g != gets || hd != heads {
			t.Fatalf("%s: GET=%d HEAD=%d, want GET=%d HEAD=%d", step, g, hd, gets, heads)
		}
	}

	read(old, "cold")
	gets, heads := requests(b, "GET", "/big"), requests(b, "HEAD", "/big")
	read(old, "warm")
	expect("warm read served from the disk tier", gets, heads)

	bus.Emit(fs.Event{Kind: fs.EventUnverified, Path: "/"})
	read(old, "after disconnect")
	expect("first read after the disconnect: one stat, no refetch", gets, heads+1)
	read(old, "during the gap")
	expect("every read during the gap re-pins: one more stat, still no refetch", gets, heads+2)

	bus.Emit(fs.Event{Kind: fs.EventVerified, Path: "/"})
	read(old, "after catch-up")
	expect("the stream caught up: no stat, no refetch", gets, heads+2)

	// A remote write in a later gap lands with a new revision: the stat
	// moves the descriptor onto it and the old tier entries are not served.
	fresh := bytes.Repeat([]byte("fedcba9876543210"), 1<<19)
	if err := cl.WriteCtx(ctx, "/big", fresh); err != nil {
		t.Fatal(err)
	}
	puts := requests(b, "GET", "/big") // the write itself does not read
	bus.Emit(fs.Event{Kind: fs.EventUnverified, Path: "/"})
	read(fresh, "after a remote change in a gap")
	expect("changed revision: one stat, one refetch", puts+1, heads+3)
}

// A block prefetched before a sibling's write describes the pre-write
// remote image. The reader drops its blocks itself only if it reads during
// the dirty window; a reader that next reads after the write has landed —
// here with no event bus, so no EventLanded reaches it — must still not be
// answered from that block.
func TestSiblingWriteInvalidatesReaderPrefetch(t *testing.T) {
	b := testbackend.NewBackend()
	const size = 2 << 20
	b.Seed("/f", bytes.Repeat([]byte("A"), size))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	c, err := New(Options{MaxSize: 1 << 20, MaxFileSize: 1 << 20, SequentialPrefetch: true, PrefetchClient: cl})
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
	reader, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: 0})
	if err != nil || reader.Handle == nil {
		t.Fatalf("open reader = %v", err)
	}
	rh := reader.Handle.(*cachingHandle)
	t.Cleanup(func() { _ = rh.Release(ctx) })
	if rh.prefetch == nil {
		t.Fatal("reader did not arm the prefetcher")
	}
	const block = 128 << 10
	// The block the reader prefetched before the write: the old image.
	ready := make(chan struct{})
	close(ready)
	rh.prefetch.mu.Lock()
	rh.prefetch.cache[block] = &prefetchBlock{offset: block, data: bytes.Repeat([]byte("A"), block), ready: ready}
	rh.prefetch.mu.Unlock()

	writer, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
	if err != nil || writer.Handle == nil {
		t.Fatalf("open writer = %v", err)
	}
	t.Cleanup(func() { _ = writer.Handle.Release(ctx) })
	if err := writer.Handle.Write(ctx, block, bytes.Repeat([]byte("N"), block)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Handle.Flush(ctx); err != nil { // lands inline: no staging configured
		t.Fatal(err)
	}
	if node, ok := b.Node("/f"); !ok || node.Data[block] != 'N' {
		t.Fatal("the write did not land")
	}
	got := make([]byte, block)
	n, err := rh.Read(ctx, block, got)
	if err != nil {
		t.Fatal(err)
	}
	if n != block || bytes.IndexByte(got, 'A') >= 0 {
		t.Fatalf("reader answered from the pre-write prefetched block (n=%d, first=%q)", n, got[:1])
	}
}

// The prefetcher clips its last block to its own file size. Left at the
// open-time length after a sibling grew the file, a reader crossing the
// old (non-aligned) EOF got a short block that Get took for EOF — a short
// read of a range the committed file fully contains. The landed size now
// moves the prefetcher too.
func TestPrefetchTracksLandedSizeAcrossOldEOF(t *testing.T) {
	b := testbackend.NewBackend()
	const base = 2<<20 + 13
	const grow = 256 << 10
	b.Seed("/grow", bytes.Repeat([]byte("A"), base))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	bus := fs.NewEventBus()
	c, err := New(Options{MaxSize: 1 << 20, MaxFileSize: 1 << 20, SequentialPrefetch: true, PrefetchClient: cl, Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	wb, err := writeback.New(writeback.Options{Policy: writeback.PolicyWriteBack, Client: cl, Events: bus})
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
	reader, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/grow", Flags: 0})
	if err != nil || reader.Handle == nil {
		t.Fatalf("open reader = %v", err)
	}
	rh := reader.Handle.(*cachingHandle)
	t.Cleanup(func() { _ = rh.Release(ctx) })
	if rh.prefetch == nil {
		t.Fatal("reader did not arm the prefetcher")
	}

	writer, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/grow", Flags: uint32(syscall.O_RDWR)})
	if err != nil || writer.Handle == nil {
		t.Fatalf("open writer = %v", err)
	}
	t.Cleanup(func() { _ = writer.Handle.Release(ctx) })
	if err := writer.Handle.Write(ctx, base, bytes.Repeat([]byte("B"), grow)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Handle.Sync(ctx); err != nil { // commits inline: EventLanded carries the new size
		t.Fatal(err)
	}
	rh.mu.Lock()
	size := rh.size
	rh.mu.Unlock()
	if size != base+grow {
		t.Fatalf("reader size after the landed growth = %d, want %d", size, base+grow)
	}

	const block = 128 << 10
	buf := make([]byte, block)
	for off := int64(0); off < base+grow; off += block {
		n, err := rh.Read(ctx, off, buf)
		if err != nil {
			t.Fatalf("read at %d = %v", off, err)
		}
		want := block
		if rest := int64(base+grow) - off; rest < block {
			want = int(rest)
		}
		if n != want {
			t.Fatalf("read at %d returned %d bytes, want %d (old EOF at %d)", off, n, want, base)
		}
		for i := 0; i < n; i++ {
			exp := byte('A')
			if off+int64(i) >= base {
				exp = 'B'
			}
			if buf[i] != exp {
				t.Fatalf("byte %d = %q, want %q", off+int64(i), buf[i], exp)
			}
		}
	}
}

// revalidateRevision stats the remote without the handle lock. When the
// handle's identity moves while that stat is in flight (here a per-path
// remote-change invalidation), the stat describes the older world: applying
// it would re-pin the stale revision, clear the flag, and let the read admit
// current bytes under that revision. The result is discarded, the handle
// stays flagged, and the read is served from the plane below.
func TestRevalidationDiscardsStatThatRacedAnIdentityChange(t *testing.T) {
	b := testbackend.NewBackend()
	data := bytes.Repeat([]byte("0123456789abcdef"), 1<<17) // 2 MiB
	b.Seed("/f", data)
	var gate atomic.Bool
	blocked := make(chan struct{}, 1)
	release := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && strings.HasSuffix(r.URL.Path, "/f") && gate.Load() {
			select {
			case blocked <- struct{}{}:
			default:
			}
			<-release
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	bus := fs.NewEventBus()
	c, err := New(Options{MaxSize: 1 << 20, MaxFileSize: 1 << 20, PrefetchClient: cl, Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	ch := newChain(t, ts.URL, c)
	ctx := context.Background()
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: 0})
	if err != nil || res.Handle == nil {
		t.Fatalf("open = %v", err)
	}
	h := res.Handle.(*cachingHandle)
	t.Cleanup(func() { _ = h.Release(ctx) })

	bus.Emit(fs.Event{Kind: fs.EventUnverified, Path: "/"}) // flags the handle, keeps rev=1
	gate.Store(true)
	buf := make([]byte, 64<<10)
	done := make(chan error, 1)
	go func() {
		n, err := h.Read(ctx, 0, buf)
		if err == nil && !bytes.Equal(buf[:n], data[:n]) {
			err = fmt.Errorf("read served wrong bytes (n=%d)", n)
		}
		done <- err
	}()
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("the revalidation stat never reached the server")
	}
	// A remote change lands while the stat is parked: the handle's identity
	// moves (revision zeroed, flagged, generation bumped).
	c.invalidatePathTiers("/f")
	gate.Store(false)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	rev, flagged := h.rev, h.revalidate
	h.mu.Unlock()
	if rev != 0 || !flagged {
		t.Fatalf("stale stat applied over the identity change: rev=%d revalidate=%v, want rev=0 and still flagged", rev, flagged)
	}
}

// A flagged handle whose inner plane cannot re-pin its revision (a decorator
// that does not state IsRemoteNamespaceReadable, e.g. localize's accounting
// wrapper) must not fall back to the revision-keyed tiers with the retained
// revision after the tiers stopped being swept: without this, a handle that
// warmed the disk tier before a disconnect kept answering old bytes across
// remote changes in the gap.
func TestFlaggedHandleWithoutRemoteClaimBypassesTiers(t *testing.T) {
	c, err := New(Options{MaxSize: 1 << 20, MaxFileSize: 1 << 20,
		PrefetchClient: client.New("http://127.0.0.1:1", ""), // never contacted: no claim to re-pin through
		Disk:           DiskReadCacheOptions{Dir: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	inner := &stubInner{body: func(int32) []byte { return []byte("BELOW--") }}
	h := &cachingHandle{cache: c, inner: inner, path: "/f", rev: 3, size: 8 << 20}
	c.open.Store(h, struct{}{})
	// Off 0 with a small buffer takes the whole-image fast path straight to
	// the plane below; an interior offset is what the disk range tier serves.
	const off = 4096
	c.dsk.PutOwned(DiskReadCacheKey{FileID: pathDiskReadCacheFileID("/f"), Path: "/f", Revision: 3, Offset: off, Length: 7}, []byte("STALE!!"))
	c.markUnverified()
	buf := make([]byte, 7)
	n, err := h.Read(context.Background(), off, buf)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(buf[:n]); got != "BELOW--" {
		t.Fatalf("read %q from the retained disk tier, want the plane below's bytes", got)
	}
}

// aliasTable is a minimal inode table where /a and /b are hardlink aliases
// of one object.
type aliasTable struct{}

func (aliasTable) Ino(p string) (uint64, bool) {
	if p == "/a" || p == "/b" {
		return 7, true
	}
	return 0, false
}
func (aliasTable) Path(ino uint64) (string, bool) {
	if ino == 7 {
		return "/a", true
	}
	return "", false
}
func (aliasTable) Epoch(string) uint64     { return 0 }
func (aliasTable) NextEpoch(string) uint64 { return 0 }
func (aliasTable) Aliases(ino uint64) []string {
	if ino == 7 {
		return []string{"/a", "/b"}
	}
	return nil
}

// A prefetched block must not survive a write through a hardlink alias: the
// object is the same, so the pre-write block is stale for the other name's
// readers as soon as the write lands, dirty window or no dirty window.
func TestSiblingWriteThroughAliasInvalidatesReaderPrefetch(t *testing.T) {
	b := testbackend.NewBackend()
	const size = 2 << 20
	b.Seed("/a", bytes.Repeat([]byte("A"), size))
	b.SeedHardlink("/a", "/b")
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	c, err := New(Options{MaxSize: 1 << 20, MaxFileSize: 1 << 20, SequentialPrefetch: true, PrefetchClient: cl})
	if err != nil {
		t.Fatal(err)
	}
	c.SetInodeTable(aliasTable{})
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
	reader, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/a", Flags: 0})
	if err != nil || reader.Handle == nil {
		t.Fatalf("open reader = %v", err)
	}
	rh := reader.Handle.(*cachingHandle)
	t.Cleanup(func() { _ = rh.Release(ctx) })
	const block = 128 << 10
	ready := make(chan struct{})
	close(ready)
	rh.prefetch.mu.Lock()
	rh.prefetch.cache[block] = &prefetchBlock{offset: block, data: bytes.Repeat([]byte("A"), block), ready: ready}
	rh.prefetch.mu.Unlock()

	writer, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/b", Flags: uint32(syscall.O_RDWR)})
	if err != nil || writer.Handle == nil {
		t.Fatalf("open writer = %v", err)
	}
	t.Cleanup(func() { _ = writer.Handle.Release(ctx) })
	if err := writer.Handle.Write(ctx, block, bytes.Repeat([]byte("N"), block)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Handle.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, block)
	n, err := rh.Read(ctx, block, got)
	if err != nil {
		t.Fatal(err)
	}
	if n != block || bytes.IndexByte(got, 'A') >= 0 {
		t.Fatalf("reader answered the pre-write block through the alias (n=%d, first=%q)", n, got[:1])
	}
}

// A stat that fails after the identity moved (a rename retargets the
// descriptor mid-stat; the old name is 404 by the time the HEAD lands) must
// not fail the read: the fd is valid and the plane below can still answer.
func TestRevalidationStatFailureAfterIdentityChangeFallsThrough(t *testing.T) {
	b := testbackend.NewBackend()
	data := bytes.Repeat([]byte("0123456789abcdef"), 1<<17) // 2 MiB
	b.Seed("/f", data)
	var gate atomic.Bool
	blocked := make(chan struct{}, 1)
	release := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && strings.HasSuffix(r.URL.Path, "/f") && gate.Load() {
			select {
			case blocked <- struct{}{}:
			default:
			}
			<-release
			w.WriteHeader(http.StatusNotFound) // the old name after the rename
			return
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	bus := fs.NewEventBus()
	c, err := New(Options{MaxSize: 1 << 20, MaxFileSize: 1 << 20, PrefetchClient: cl, Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	ch := newChain(t, ts.URL, c)
	ctx := context.Background()
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: 0})
	if err != nil || res.Handle == nil {
		t.Fatalf("open = %v", err)
	}
	h := res.Handle.(*cachingHandle)
	t.Cleanup(func() { _ = h.Release(ctx) })
	bus.Emit(fs.Event{Kind: fs.EventUnverified, Path: "/"})
	gate.Store(true)
	buf := make([]byte, 64<<10)
	done := make(chan error, 1)
	go func() {
		n, err := h.Read(ctx, 0, buf)
		if err == nil && !bytes.Equal(buf[:n], data[:n]) {
			err = fmt.Errorf("read served wrong bytes (n=%d)", n)
		}
		done <- err
	}()
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("the revalidation stat never reached the server")
	}
	h.SetPath("/g") // a rename retargets the descriptor while the stat is parked
	gate.Store(false)
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("read failed on a valid descriptor: %v", err)
	}
}

// A reset (retention gap) sweeps every tier and re-synchronizes the view —
// and SSE's reset path clears its own unverified flag without a catch-up, so
// no EventVerified follows it. Gap mode must end with the sweep: left set,
// every warm read of a handle with a pinned revision pays a remote HEAD for
// the rest of the session.
func TestCheckoutEndsGapMode(t *testing.T) {
	b := testbackend.NewBackend()
	data := bytes.Repeat([]byte("0123456789abcdef"), 1<<19) // 8 MiB
	b.Seed("/big", data)
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	bus := fs.NewEventBus()
	c, err := New(Options{MaxSize: 1 << 20, MaxFileSize: 1 << 20, PrefetchClient: cl, Events: bus,
		Disk: DiskReadCacheOptions{Dir: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(c, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	const off = 2 << 20
	read := func(step string) {
		t.Helper()
		res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/big", Flags: 0})
		if err != nil || res.Handle == nil {
			t.Fatalf("open (%s) = %v", step, err)
		}
		t.Cleanup(func() { _ = res.Handle.Release(ctx) })
		buf := make([]byte, 1<<20)
		n, err := res.Handle.Read(ctx, off, buf)
		if err != nil || !bytes.Equal(buf[:n], data[off:off+int64(len(buf))]) {
			t.Fatalf("%s: read n=%d err=%v", step, n, err)
		}
	}
	gets := func() int { return requests(b, "GET", "/big") }
	heads := func() int { return requests(b, "HEAD", "/big") }

	bus.Emit(fs.Event{Kind: fs.EventUnverified, Path: "/"})
	read("in the gap")
	g0, h0 := gets(), heads()
	if h0 == 0 {
		t.Fatal("gap-mode read did not revalidate")
	}
	bus.Emit(fs.Event{Kind: fs.EventCheckout, Path: "/"}) // reset: sweep, no EventVerified
	read("after the reset")
	read("warm after the reset")
	// Each read() opens fresh, and an open stats once by itself: exactly one
	// HEAD per read means gap mode is over; one more per read means it is not.
	if g, hd := gets(), heads(); hd != h0+2 {
		t.Fatalf("reads after the reset kept paying revalidation HEADs: %d -> %d, want %d (GET %d -> %d)", h0, hd, h0+2, g0, g)
	}
}

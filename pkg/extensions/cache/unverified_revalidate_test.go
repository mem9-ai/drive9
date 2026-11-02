package cache

import (
	"bytes"
	"context"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"

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

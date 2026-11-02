package cache

import (
	"bytes"
	"context"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// The default stack opens every existing file through writeback's
// bufferingHandle. #1026 gated the sequential prefetcher on a capability
// only core's terminal handle stated, so behind writeback it never armed:
// a large sequential read became one synchronous remote fetch per FUSE
// request and the --parallel-read-* knobs were inert. A clean read-only
// descriptor now forwards the terminal's claim and arms; a writable open
// (v1 parity) does not; with a live dirty sibling the claim is withdrawn
// and an armed reader serves the sibling's unflushed bytes rather than a
// prefetched remote block.
func TestPrefetchArmsBehindCleanWritebackHandle(t *testing.T) {
	b := testbackend.NewBackend()
	big := bytes.Repeat([]byte("0123456789abcdef"), 1<<17) // 2 MiB: above the whole-file tier
	b.Seed("/big", big)
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

	reader, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/big", Flags: 0})
	if err != nil || reader.Handle == nil {
		t.Fatalf("open read-only = %v", err)
	}
	rh, ok := reader.Handle.(*cachingHandle)
	if !ok {
		t.Fatalf("outer handle = %T, want *cachingHandle", reader.Handle)
	}
	t.Cleanup(func() { _ = rh.Release(ctx) })
	if !rh.prefetchArmed || rh.prefetch == nil {
		t.Fatal("clean read-only open behind writeback did not arm the prefetcher")
	}
	if !remoteNamespaceReadable(rh.inner) {
		t.Fatalf("writeback handle %T withheld the remote-namespace claim for a clean read-only open", rh.inner)
	}
	buf := make([]byte, 128<<10)
	n, err := rh.Read(ctx, 0, buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf[:n], big[:n]) {
		t.Fatalf("armed reader served wrong bytes (n=%d)", n)
	}

	// v1 parity: a writable descriptor never arms (its first write would
	// only invalidate the blocks).
	writer, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/big", Flags: uint32(syscall.O_RDWR)})
	if err != nil || writer.Handle == nil {
		t.Fatalf("open rdwr = %v", err)
	}
	wh := writer.Handle.(*cachingHandle)
	t.Cleanup(func() { _ = wh.Release(ctx) })
	if wh.prefetchArmed {
		t.Fatal("writable open armed the prefetcher")
	}

	// A live dirty sibling is a local image: the path's bytes are no longer
	// the remote namespace, for the armed reader and for any new open.
	if err := wh.Write(ctx, 0, []byte("DIRTY")); err != nil {
		t.Fatal(err)
	}
	if remoteNamespaceReadable(rh.inner) {
		t.Fatal("writeback reported the remote namespace readable with a dirty sibling buffer")
	}
	n, err = rh.Read(ctx, 0, buf[:5])
	if err != nil {
		t.Fatal(err)
	}
	if got := string(buf[:n]); got != "DIRTY" {
		t.Fatalf("armed reader read %q behind a dirty sibling, want DIRTY", got)
	}
	// A read-only open while the sibling is dirty flushes the writer first
	// (v1 flushPendingWriteBack), so the new descriptor reads the committed
	// bytes from the remote namespace — and arms.
	second, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/big", Flags: 0})
	if err != nil || second.Handle == nil {
		t.Fatalf("second open read-only = %v", err)
	}
	sh := second.Handle.(*cachingHandle)
	t.Cleanup(func() { _ = sh.Release(ctx) })
	if node, ok := b.Node("/big"); !ok || string(node.Data[:5]) != "DIRTY" {
		t.Fatalf("read-only open did not flush the live writer to the remote (ok=%v)", ok)
	}
	if !sh.prefetchArmed {
		t.Fatal("read-only open after the writer flushed did not arm the prefetcher")
	}
	n, err = sh.Read(ctx, 0, buf[:5])
	if err != nil {
		t.Fatal(err)
	}
	if got := string(buf[:n]); got != "DIRTY" {
		t.Fatalf("second reader read %q, want the flushed DIRTY", got)
	}
}

// remoteAwareInner is a terminal whose remote-namespace claim can be
// withdrawn mid-handle, standing in for writeback's descriptor once its
// path acquires a local image the cache's own dirty mark does not track (a
// released image still landing).
type remoteAwareInner struct {
	stubInner
	readable atomic.Bool
}

func (s *remoteAwareInner) IsRemoteNamespaceReadable() bool { return s.readable.Load() }

// A prefetcher armed at open answers only while the plane below still
// reads the remote namespace: once it does not, its blocks describe the
// remote base, so the read goes below and the blocks are dropped instead
// of answering stale.
func TestCachingHandleReadDropsPrefetchWhenPlaneLeavesRemoteNamespace(t *testing.T) {
	c := newHandleCache(t)
	inner := &remoteAwareInner{stubInner: stubInner{body: func(int32) []byte { return []byte("BELOW") }}}
	inner.readable.Store(true)
	const size = 8 << 20
	p := newTestPrefetcher("/f", size)
	ready := make(chan struct{})
	close(ready)
	inject := func() {
		p.mu.Lock()
		p.cache[0] = &prefetchBlock{offset: 0, data: []byte("STALE"), ready: ready}
		p.mu.Unlock()
	}
	h := &cachingHandle{cache: c, inner: inner, path: "/f", rev: 3, size: size, prefetch: p, prefetchArmed: true}
	ctx := context.Background()
	buf := make([]byte, 5)

	inject()
	n, err := h.Read(ctx, 0, buf)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(buf[:n]); got != "STALE" {
		t.Fatalf("remote-readable plane: prefetched block not served, got %q", got)
	}

	inject()
	inner.readable.Store(false)
	n, err = h.Read(ctx, 0, buf)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(buf[:n]); got != "BELOW" {
		t.Fatalf("plane left the remote namespace but the stale prefetched block answered: %q", got)
	}
	p.mu.Lock()
	left := len(p.cache)
	p.mu.Unlock()
	if left != 0 {
		t.Fatalf("%d prefetched block(s) kept after the plane left the remote namespace", left)
	}
}

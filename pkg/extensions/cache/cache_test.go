package cache

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func newChain(t *testing.T, url string, exts ...fs.Handler) *fs.Chain {
	t.Helper()
	terminal, err := core.New(client.New(url, ""), core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	handlers := append(append([]fs.Handler(nil), exts...), terminal)
	ch, err := fs.NewChain(handlers...)
	if err != nil {
		t.Fatal(err)
	}
	return ch
}

func TestReadCacheLRUAndRevision(t *testing.T) {
	rc := NewReadCacheWithMaxFileSize(6, -1, 4)
	rc.Put("/a", []byte("aa"), 1)
	rc.Put("/b", []byte("bbbb"), 1)
	rc.Put("/c", []byte("cc"), 1) // evicts /a (LRU, over budget)

	if _, ok := rc.Get("/a", 1); ok {
		t.Fatal("/a should have been evicted")
	}
	if _, ok := rc.Get("/b", 1); !ok {
		t.Fatal("/b missing")
	}
	if _, ok := rc.Get("/b", 0); !ok {
		t.Fatal("revision 0 must skip the check")
	}
	if _, ok := rc.Get("/b", 2); ok {
		t.Fatal("revision mismatch should miss")
	}
	if _, ok := rc.Get("/b", 1); ok {
		t.Fatal("stale entry should have been evicted by the mismatch")
	}
	rc.Put("/big", []byte("0123456789"), 1) // exceeds per-file limit
	if _, ok := rc.Get("/big", 1); ok {
		t.Fatal("oversized entry admitted")
	}
}

func TestReadCacheTTLExpiry(t *testing.T) {
	rc := NewReadCacheWithMaxFileSize(0, time.Millisecond, 0)
	rc.Put("/a", []byte("x"), 1)
	if _, ok := rc.Get("/a", 1); !ok {
		t.Fatal("fresh entry missing")
	}
	time.Sleep(3 * time.Millisecond)
	if _, ok := rc.Get("/a", 1); ok {
		t.Fatal("expired entry served")
	}
}

func TestCacheHandleDecoratorServesHitsWithoutBackend(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("hello"))
	ts := httptest.NewServer(b)
	defer ts.Close()

	c, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch := newChain(t, ts.URL, c)

	open := &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: 0}
	res, err := ch.Dispatch(context.Background(), open)
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if h == nil {
		t.Fatal("no handle")
	}

	buf := make([]byte, 5)
	if n, err := h.Read(context.Background(), 0, buf); err != nil || n != 5 {
		t.Fatalf("first read = %d %v", n, err)
	}
	gets := b.Requests()["GET /v1/fs/f"]
	if gets != 1 {
		t.Fatalf("backend GETs after first read = %d, want 1", gets)
	}

	// second read must be served from cache
	buf2 := make([]byte, 5)
	if n, err := h.Read(context.Background(), 0, buf2); err != nil || string(buf2) != "hello" {
		t.Fatalf("cached read = %d %q %v", n, buf2, err)
	}
	if got := b.Requests()["GET /v1/fs/f"]; got != gets {
		t.Fatalf("backend GETs after cached read = %d, want %d", got, gets)
	}

	// a write invalidates: next read refetches
	if err := h.Write(context.Background(), 0, []byte("HELLO")); err != nil {
		t.Fatal(err)
	}
	buf3 := make([]byte, 5)
	if n, err := h.Read(context.Background(), 0, buf3); err != nil || string(buf3) != "HELLO" {
		t.Fatalf("post-write read = %d %q %v", n, buf3, err)
	}
	if got := b.Requests()["GET /v1/fs/f"]; got != gets+1 {
		t.Fatalf("backend GETs after invalidation = %d, want %d", got, gets+1)
	}
}

func TestCachePathReadAndEventInvalidation(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("data"))
	ts := httptest.NewServer(b)
	defer ts.Close()

	bus := fs.NewEventBus()
	c, err := New(Options{Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	ch := newChain(t, ts.URL, c)
	ctx := context.Background()

	if res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/f"}); err != nil || string(res.Data) != "data" {
		t.Fatalf("path read = %q %v", res.Data, err)
	}
	if res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/f"}); err != nil || string(res.Data) != "data" {
		t.Fatalf("cached path read = %q %v", res.Data, err)
	}
	if got := b.Requests()["GET /v1/fs/f"]; got != 1 {
		t.Fatalf("backend GETs = %d, want 1", got)
	}

	// a write event must drop the entry
	bus.Emit(fs.Event{Kind: fs.EventWrite, Path: "/f"})
	if res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/f"}); err != nil || string(res.Data) != "data" {
		t.Fatalf("post-event read = %q %v", res.Data, err)
	}
	if got := b.Requests()["GET /v1/fs/f"]; got != 2 {
		t.Fatalf("backend GETs after event = %d, want 2", got)
	}

	// delete events sweep subtrees
	bus.Emit(fs.Event{Kind: fs.EventDelete, Path: "/d"})
	bus.Emit(fs.Event{Kind: fs.EventCheckout, Path: "/"})
}

// TestLandedEventDropsOnlyCommittedPath pins the v1 local-commit settle
// (onCommitQueueSuccess / onWriteBackUploadSuccess): the read cache drops the
// committed path's pre-commit image and nothing else — wiping the whole cache
// per commit would discard every other file's warm entries.
func TestLandedEventDropsOnlyCommittedPath(t *testing.T) {
	bus := fs.NewEventBus()
	c, err := New(Options{Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	c.StoreOwned("/a", []byte("aa"), 1)
	c.StoreOwned("/b", []byte("bb"), 1)

	bus.Emit(fs.Event{Kind: fs.EventLanded, Path: "/a"})
	if _, ok := c.ReadCached("/a", 1); ok {
		t.Fatal("committed path should have been dropped")
	}
	if _, ok := c.ReadCached("/b", 1); !ok {
		t.Fatal("unrelated entry lost to a local commit of another path")
	}

	// remote-origin invalidation still sweeps everything
	bus.Emit(fs.Event{Kind: fs.EventInvalidated, Path: "/"})
	if _, ok := c.ReadCached("/b", 1); ok {
		t.Fatal("/b should be gone after remote-origin invalidation")
	}
}

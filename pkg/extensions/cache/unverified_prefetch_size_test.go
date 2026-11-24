package cache

import (
	"bytes"
	"context"
	"net/http/httptest"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// TestUnverifiedKeepsPrefetcherSize pins the blocking regression a review found:
// marking the cache unverified must not leave every open prefetcher at "size
// unknown" for good. The reset is always followed by a revalidation, and that
// revalidation republishes the size only when the identity moved — so dropping
// the open-time size here silently disabled readahead on unchanged files after
// every stream interruption.
func TestUnverifiedKeepsPrefetcherSize(t *testing.T) {
	const size = 8 << 20
	b := testbackend.NewBackend()
	payload := bytes.Repeat([]byte("0123456789abcdef"), size/16)
	b.Seed("/big", payload)
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	bus := fs.NewEventBus()
	c, err := New(Options{MaxSize: 1 << 20, MaxFileSize: 1 << 20, SequentialPrefetch: true,
		PrefetchClient: cl, Events: bus})
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
	if h.prefetch == nil {
		t.Fatal("sequential prefetch did not arm for a large file")
	}

	buf := make([]byte, 1<<20)
	if n, err := h.Read(ctx, 0, buf); err != nil || n != len(buf) {
		t.Fatalf("cold read = %d, %v", n, err)
	}

	bus.Emit(fs.Event{Kind: fs.EventUnverified, Path: "/"})
	if n, err := h.Read(ctx, 0, buf); err != nil || n != len(buf) {
		t.Fatalf("read after the disconnect = %d, %v", n, err)
	}

	h.prefetch.mu.Lock()
	fileSize := h.prefetch.fileSize
	h.prefetch.mu.Unlock()
	if fileSize < 0 {
		t.Fatal("prefetcher size is unknown after the revalidating read: readahead is disabled for the rest of the descriptor's life")
	}
	if fileSize != size {
		t.Fatalf("prefetcher size = %d, want %d", fileSize, int64(size))
	}
}

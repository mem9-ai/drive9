package cache

// qiffang twenty-second round: a checkout/reset must invalidate the OPEN
// handles' block prefetchers too — Read serves prefetched blocks ahead of
// the memory/disk tiers, so a live descriptor kept reading the pre-reset
// image.

import (
	"bytes"
	"context"
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

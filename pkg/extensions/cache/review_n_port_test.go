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

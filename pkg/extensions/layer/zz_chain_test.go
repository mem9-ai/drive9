package layer

import (
	"context"
	"net/http/httptest"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// TestChainTruncatingOpenOverWriteback reproduces the e2e shape: an
// O_TRUNC open (no O_CREAT) of a classic-backed name through the fuller
// chain [layer → writeback → core] over the fake server.
func TestChainTruncatingOpenOverWriteback(t *testing.T) {
	b := testbackend.NewBackend()
	b.SetLayerState("L1", "active")
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	b.Seed("/base.txt", []byte("live moved on"))

	bus := fs.NewEventBus()
	ledger, err := staging.NewStagingWithQuota(t.TempDir(), 0.10, 64)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ledger.Close() })
	if err := ledger.Recover(); err != nil {
		t.Fatal(err)
	}
	wb, err := writeback.New(writeback.Options{
		Policy:        writeback.PolicyWriteBack,
		Client:        cl,
		RemoteRoot:    "/",
		Events:        bus,
		Staging:       ledger,
		FlushDebounce: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wb.Stop(context.Background()) })
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	l, err := New(Options{LayerRef: "L1", IO: NewClientDeps(cl), Registry: NewClientDeps(cl), Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Stop(context.Background()) })
	ch, err := fs.NewChain(l, wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// read first (kernel would have looked the name up)
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/base.txt"}); err != nil {
		t.Fatalf("getattr: %v", err)
	}
	// shell `>` on the resolved name: O_TRUNC, no O_CREAT.
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/base.txt", Flags: uint32(syscall.O_WRONLY | syscall.O_TRUNC)})
	if err != nil {
		t.Fatalf("truncating open: %v", err)
	}
	if h := res.Handle; h != nil {
		if err := h.Write(ctx, 0, []byte("replaced in layer")); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := h.Flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
		if err := h.Release(ctx); err != nil {
			t.Fatalf("release: %v", err)
		}
	}
}

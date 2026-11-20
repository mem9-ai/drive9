package layer

import (
	"bytes"
	"context"
	"net/http/httptest"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/membudget"
)

// TestStreamedCommitLoadsCleanBasePartsUnderEnforcedBudget: an edit that the
// write path admitted must stay publishable when the streamed commit has to
// load untouched base parts the budget cannot hold. Refusing those loads left
// the file unpublishable for good, because every retry failed the same way.
func TestStreamedCommitLoadsCleanBasePartsUnderEnforcedBudget(t *testing.T) {
	b := testbackend.NewBackend()
	// 9MiB = one full 8MiB part plus a 1MiB tail part.
	base := bytes.Repeat([]byte("A"), 9<<20)
	b.Seed("/big.bin", base)
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	l, err := New(Options{LayerRef: "L1", Client: cl, InlineMax: 16})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Stop(context.Background()) })
	// The dirty tail part (1MiB) fits; one clean 8MiB base part does not.
	l.SetMemoryBudget(membudget.New(4<<20, membudget.ModeEnforce, nil))
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(l, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/big.bin", Flags: uint32(syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	h, ok := res.Handle.(*layerHandle)
	if !ok {
		t.Fatalf("handle = %T", res.Handle)
	}
	if err := h.Write(ctx, int64(len(base))-1, []byte("B")); err != nil {
		t.Fatalf("tail write: %v", err)
	}
	// The streamed commit spills the whole image, loading the untouched base
	// part the budget cannot hold.
	if err := h.Flush(ctx); err != nil {
		t.Fatalf("flush (streamed commit) = %v, want success", err)
	}
	rres, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/big.bin"})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(rres.Data) != len(base) {
		t.Fatalf("image size = %d, want %d", len(rres.Data), len(base))
	}
	if !bytes.Equal(rres.Data[:4], []byte("AAAA")) {
		t.Fatalf("published prefix = %q, want the untouched base bytes", rres.Data[:4])
	}
	if got := rres.Data[len(rres.Data)-1]; got != 'B' {
		t.Fatalf("published tail = %q, want the edit", got)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
}

// TestLayerReleaseReturnsBufferCharge: closing a claimed handle drops its
// buffer, so the buffer's memory-budget charge must go back — otherwise an
// open/write/close loop exhausts the budget for bytes nobody retains.
func TestLayerReleaseReturnsBufferCharge(t *testing.T) {
	b := testbackend.NewBackend()
	ch, l := newStack(t, b, "")
	budget := membudget.New(8<<20, membudget.ModeEnforce, nil)
	l.SetMemoryBudget(budget)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/charged.bin", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	h, ok := res.Handle.(*layerHandle)
	if !ok {
		t.Fatalf("handle = %T", res.Handle)
	}
	if err := h.Write(ctx, 0, make([]byte, 4096)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := budget.UsedBy(membudget.ClassWriteBuffer); got == 0 {
		t.Fatal("write did not charge the budget")
	}
	if err := h.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	if got := budget.UsedBy(membudget.ClassWriteBuffer); got != 0 {
		t.Fatalf("charge after Release = %d, want 0", got)
	}
}

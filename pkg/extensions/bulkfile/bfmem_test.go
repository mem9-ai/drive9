package bulkfile

import (
	"context"
	"net/http/httptest"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// newBelowStack builds a core terminal over the in-memory backend for the
// forwarding paths (push + SetAttr need a real plane below).
func newBelowStack(t *testing.T) *fs.Chain {
	t.Helper()
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(terminal)
	if err != nil {
		t.Fatal(err)
	}
	return ch
}

func chainDispatch(t *testing.T) fs.HandlerFilesystem {
	t.Helper()
	ch := newBelowStack(t)
	return fs.HandlerFilesystem(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		return ch.Dispatch(ctx, op)
	})
}

func newClaimHandle(t *testing.T) *streamHandle {
	t.Helper()
	b := &Bulkfile{live: make(map[*streamHandle]struct{})}
	h := &streamHandle{b: b, path: "/repo/sparse.bin", below: chainDispatch(t),
		openFlags: uint32(syscall.O_CREAT | syscall.O_RDWR)}
	b.track(h)
	t.Cleanup(func() { b.untrack(h) })
	return h
}

// A sparse write past the memory bound forwards to the plane below instead
// of allocating the whole logical image (qiffang: one byte at 64 MiB must
// not allocate 67 MB).
func TestSparseWriteForwardsPastBound(t *testing.T) {
	h := newClaimHandle(t)
	if err := h.Write(context.Background(), 64<<20, []byte("x")); err != nil {
		t.Fatalf("sparse write: %v", err)
	}
	h.mu.Lock()
	buf := len(h.image)
	pt := h.passthrough
	h.mu.Unlock()
	if !pt {
		t.Fatalf("write past the bound did not forward (passthrough=false)")
	}
	if buf != 0 {
		t.Fatalf("image retained %d bytes after forwarding", buf)
	}
}

// A large ftruncate expresses the size below instead of allocating zeros.
func TestLargeTruncateForwardsPastBound(t *testing.T) {
	h := newClaimHandle(t)
	if err := h.Write(context.Background(), 0, []byte("head")); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	size := int64(64 << 20)
	if _, err := h.applyAttr(context.Background(), &fs.Op{Kind: fs.OpSetAttr, Size: size}); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	h.mu.Lock()
	buf := len(h.image)
	h.mu.Unlock()
	if buf > 1<<20 {
		t.Fatalf("image retained %d bytes after large truncate", buf)
	}
}

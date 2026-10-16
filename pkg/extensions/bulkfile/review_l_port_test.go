package bulkfile

// qiffang L1/L2: a surrendered claim keeps its live descriptors reading the
// shared lower state, and a failed handoff flush aborts with the claim
// intact.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func lStack(t *testing.T, ts *httptest.Server) (*Bulkfile, *fs.Chain, *writeback.Writeback) {
	t.Helper()
	cl := client.New(ts.URL, "")
	wb, _ := writeback.New(writeback.Options{Policy: writeback.PolicyCloseSync, Client: cl})
	bf, _ := New(Options{Client: cl})
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(bf, wb, terminal)
	return bf, ch, wb
}

func openExistingRW(t *testing.T, ch *fs.Chain, path string) fs.Handle {
	t.Helper()
	res, err := ch.Dispatch(context.Background(), &fs.Op{
		Kind: fs.OpCreate, Path: path, Flags: uint32(syscall.O_RDWR),
	})
	if err != nil {
		t.Fatal(err)
	}
	return res.Handle
}

func openRW(t *testing.T, ch *fs.Chain, path string) fs.Handle {
	t.Helper()
	res, err := ch.Dispatch(context.Background(), &fs.Op{
		Kind: fs.OpCreate, Path: path,
		Flags: uint32(syscall.O_CREAT | syscall.O_RDWR | syscall.O_TRUNC), Mode: 0o644,
	})
	if err != nil {
		t.Fatal(err)
	}
	return res.Handle
}

// The original descriptor keeps reading (and appending) the shared lower
// state after a second writer takes over.
func TestPortSurrenderKeepsLiveDescriptors(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	_, ch, wb := lStack(t, ts)
	ctx := context.Background()

	h1 := openRW(t, ch, "/f")
	if err := h1.Write(ctx, 0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	h2 := openExistingRW(t, ch, "/f")
	if err := h2.Write(ctx, 0, []byte("Y")); err != nil {
		t.Fatal(err)
	}
	if err := h2.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	// The original descriptor reads the shared state, not EOF.
	got := make([]byte, 5)
	if _, err := h1.Read(ctx, 0, got); err != nil {
		t.Fatalf("original descriptor read: %v", err)
	}
	if string(got) != "Yello" {
		t.Fatalf("original descriptor read = %q, want Yello", got)
	}
	// An append through the original descriptor persists real bytes.
	if err := h1.Write(ctx, 5, []byte("!")); err != nil {
		t.Fatal(err)
	}
	if err := h1.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ := b.Node("/f")
	if string(n.Data) != "Yello!" {
		t.Fatalf("final image = %q, want Yello! (no NUL fill)", string(n.Data))
	}
}

// A failed handoff flush aborts the second open with the error; the claim
// keeps its acknowledged bytes for retry.
func TestPortSurrenderAbortsOnFlushFailure(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("old"))
	var failPuts atomic.Bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failPuts.Load() && r.Method == http.MethodPut {
			http.Error(w, "quota", http.StatusForbidden)
			return
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	_, ch, _ := lStack(t, ts)
	ctx := context.Background()

	h1 := openRW(t, ch, "/f")
	if err := h1.Write(ctx, 0, []byte("new")); err != nil {
		t.Fatal(err)
	}
	failPuts.Store(true)
	_, openErr := ch.Dispatch(ctx, &fs.Op{
		Kind: fs.OpCreate, Path: "/f",
		Flags: uint32(syscall.O_CREAT | syscall.O_RDWR | syscall.O_TRUNC), Mode: 0o644,
	})
	failPuts.Store(false)
	if openErr == nil {
		t.Fatal("second writable open succeeded while the claim flush failed")
	}
	// The original handle's fsync still recovers the buffered image.
	if err := h1.Sync(ctx); err != nil {
		t.Fatalf("original sync after failed handoff: %v", err)
	}
	if err := h1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ := b.Node("/f")
	if string(n.Data) != "new" {
		t.Fatalf("final image = %q, want new (the acknowledged bytes were lost)", string(n.Data))
	}
}

// qiffang M1: a buffered claim's small growth truncation no longer
// deadlocks (the recursive lock is gone) — create, write one byte, then
// set size 2 completes.
func TestPortBulkSmallGrowTruncateCompletes(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	_, ch, _ := lStack(t, ts)
	ctx := context.Background()

	h := openRW(t, ch, "/f")
	if err := h.Write(ctx, 0, []byte("x")); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/f", Size: 2, HasSize: true, Handle: h})
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("small-grow truncate: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("small-grow truncate deadlocked")
	}
}

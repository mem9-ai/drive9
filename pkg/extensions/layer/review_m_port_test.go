package layer

// qiffang M-round: retired layer handles never re-register; metadata-only
// chmod commits; a failed base-prefix read aborts the truncation.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"time"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func mStack(t *testing.T, ts *httptest.Server) (*Layer, *fs.Chain) {
	t.Helper()
	cl := client.New(ts.URL, "")
	l, err := New(Options{LayerRef: "L1", RemoteRoot: "/", Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Stop(context.Background()) })
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(l, terminal)
	return l, ch
}

// Releasing both descriptors retires both; a later path truncate reaches a
// LIVE handle (or the base), never a retired one.
func TestPortRetiredLayerHandleNotReselected(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/file", []byte("hello"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	_, ch := mStack(t, ts)
	ctx := context.Background()

	open := func() fs.Handle {
		res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/file", Flags: uint32(syscall.O_RDWR)})
		if err != nil {
			t.Fatal(err)
		}
		return res.Handle
	}
	h1, h2 := open(), open()
	if err := h1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/file", Size: 2, HasSize: true}); err != nil {
		t.Fatalf("post-retire truncate: %v", err)
	}
	rd, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/file", Offset: 0, Limit: 8})
	if err != nil || string(rd.Data) != "he" {
		t.Fatalf("post-truncate read = %q err=%v, want he (the truncation landed)", string(rd.Data), err)
	}
}

// A metadata-only chmod on an open clean handle persists at sync/release.
func TestPortLayerChmodCommitsWithoutContentWrite(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	_, ch := mStack(t, ts)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{
		Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644, HasMode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Write(ctx, 0, []byte("data")); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/f", Mode: 0o600, HasMode: true, Handle: res.Handle}); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	// A fresh stack replays the log: the mode-only change must be there.
	_, ch2 := mStack(t, ts)
	deadline := attempts(t, func() bool {
		st, err := ch2.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/f"})
		return err == nil && st.Attr != nil && st.Attr.Mode&0o777 == 0o600
	})
	if !deadline {
		st, _ := ch2.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/f"})
		t.Fatalf("replayed mode = %+v, want 0600 (chmod never committed)", st.Attr)
	}
}

func attempts(t *testing.T, try func() bool) bool {
	t.Helper()
	for i := 0; i < 50; i++ {
		if try() {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// A failed retained-prefix read aborts the truncation instead of
// publishing zero-filled bytes.
func TestPortLayerTruncateAbortsOnBaseReadFailure(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("hello"))
	var failReads atomic.Bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failReads.Load() && r.Method == http.MethodGet {
			http.Error(w, "injected", http.StatusForbidden)
			return
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	_, ch := mStack(t, ts)
	ctx := context.Background()

	failReads.Store(true)
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/f", Size: 3, HasSize: true}); err == nil {
		t.Fatal("truncation succeeded while the retained-prefix read failed")
	}
	failReads.Store(false)
	rd, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/f", Offset: 0, Limit: 5})
	if err != nil || string(rd.Data) != "hello" {
		t.Fatalf("post-abort read = %q err=%v, want untouched hello", string(rd.Data), err)
	}
}

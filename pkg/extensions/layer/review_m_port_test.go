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

// qiffang N1: EVERY descriptor of a changed path follows the checkout —
// an older sibling fd must not keep serving its stale buffer after the
// newest one refreshed.
func TestPortSiblingHandlesFollowCheckout(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("Aello"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	l, ch := mStack(t, ts)
	ctx := context.Background()

	open := func() fs.Handle {
		res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
		if err != nil {
			t.Fatal(err)
		}
		return res.Handle
	}
	h1, h2 := open(), open()
	// An external upsert replaces the entry; the layer's periodic event
	// refresh picks it up and the checkout sweeps every open descriptor.
	postLayerEntry(t, ts, "L1", "/f", "upsert", "world")
	l.refreshEventsForTest(ctx)
	deadline := attempts(t, func() bool {
		got := make([]byte, 5)
		if _, err := h2.Read(ctx, 0, got); err != nil || string(got) != "world" {
			return false
		}
		if _, err := h1.Read(ctx, 0, got); err != nil || string(got) != "world" {
			return false
		}
		return true
	})
	if !deadline {
		got := make([]byte, 5)
		_, e1 := h1.Read(ctx, 0, got)
		t.Fatalf("sibling checkout: h1 = %q err=%v (older fd kept its stale buffer)", string(got), e1)
	}
}

// qiffang N2: a rename MOVES the registry slot without retiring the open
// descriptor — a sibling open/close between two renames must not strand
// the first fd.
func TestPortRenameAcrossSiblingOpenCloseKeepsHandle(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/file", []byte("hello"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	_, ch := mStack(t, ts)
	ctx := context.Background()

	h1 := func() fs.Handle {
		res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/file", Flags: uint32(syscall.O_RDWR)})
		if err != nil {
			t.Fatal(err)
		}
		return res.Handle
	}()
	if err := h1.Write(ctx, 0, []byte("X")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/file", Path2: "/moved"}); err != nil {
		t.Fatal(err)
	}
	// A sibling opens and releases the renamed path.
	res2, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/moved", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	if err := res2.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	// Rename again: h1 must still be selected and retargeted.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/moved", Path2: "/again"}); err != nil {
		t.Fatal(err)
	}
	if err := h1.Write(ctx, 4, []byte("!")); err != nil {
		t.Fatalf("write through the renamed fd: %v", err)
	}
	if err := h1.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	st, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/again"})
	if err != nil || st.Attr == nil || st.Attr.Size != 5 {
		t.Fatalf("/again attr = %+v err=%v, want size 5 (the fd was stranded by the sibling release)", st.Attr, err)
	}
	rd, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/again", Offset: 0, Limit: 5})
	if err != nil || string(rd.Data) != "Xell!" && string(rd.Data) != "Xello" {
		t.Fatalf("/again read = %q err=%v", string(rd.Data), err)
	}
}

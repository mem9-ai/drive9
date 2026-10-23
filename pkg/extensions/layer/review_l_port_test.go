package layer

// qiffang L4/L5: a surviving layer handle stays registered when a sibling
// closes (rename retargeting), and an explicit zero create mode survives.

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

func lLayerStack(t *testing.T, ts *httptest.Server) (*Layer, *fs.Chain) {
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

// Releasing the FIRST of two writable descriptors must not unregister the
// surviving writer: a rename retargets it and its pending edit follows.
func TestPortSurvivingLayerHandleStaysRegistered(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/file", []byte("hello"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	_, ch := lLayerStack(t, ts)
	ctx := context.Background()

	open := func() fs.Handle {
		res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/file", Flags: uint32(syscall.O_RDWR)})
		if err != nil {
			t.Fatal(err)
		}
		return res.Handle
	}
	h1 := open()
	h2 := open()
	if err := h2.Write(ctx, 0, []byte("X")); err != nil {
		t.Fatal(err)
	}
	// Close the FIRST descriptor: the surviving writer must stay retargetable.
	if err := h1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/file", Path2: "/moved"}); err != nil {
		t.Fatal(err)
	}
	if err := h2.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	// Layer entries publish through the layer plane: verify through the
	// chain (a fresh stack replays the log).
	st, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/moved"})
	if err != nil || st.Attr == nil || st.Attr.Size != 5 {
		t.Fatalf("/moved attr = %+v err=%v, want size 5 (the pending edit followed the rename)", st.Attr, err)
	}
	rd, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/moved", Offset: 0, Limit: 5})
	if err != nil || string(rd.Data) != "Xello" {
		t.Fatalf("/moved read = %q err=%v, want Xello", string(rd.Data), err)
	}
}

// An explicitly requested zero create mode is a valid mask: create(0000)
// must not widen to 0644.
func TestPortLayerExplicitZeroModeSurvives(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	_, ch := lLayerStack(t, ts)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{
		Kind: fs.OpCreate, Path: "/locked", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0, HasMode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Attr == nil || res.Attr.Mode&0o777 != 0 {
		t.Fatalf("create reply mode = %+v, want explicit 0000", res.Attr)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	st, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/locked"})
	if err != nil {
		t.Fatal(err)
	}
	if st.Attr == nil || st.Attr.Mode&0o777 != 0 {
		t.Fatalf("post-release mode = %+v, want 0000 (explicit zero widened to 0644)", st.Attr)
	}
}

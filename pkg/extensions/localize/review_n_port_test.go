package localize

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/cache"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// The anonymous identity must survive the localize decorator: with
// cache→localize→writeback→core, an overwritten target fd reads its OLD
// object, not the replacement's pending image.
func TestPortAnonymousIdentityThroughLocalize(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/a", []byte("HELLO"))
	b.Seed("/b", []byte("OTHER"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cch, err := cache.New(cache.Options{MaxSize: 1 << 20, MaxFileSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	lz, err := New(Options{LocalRoot: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	wb, err := writeback.New(writeback.Options{Policy: writeback.PolicyWriteBack, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(cch, lz, wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	target, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/b", Flags: 0})
	if err != nil || target.Handle == nil {
		t.Fatalf("open target = %v", err)
	}
	src, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/a", Flags: 0})
	if err != nil || src.Handle == nil {
		t.Fatalf("open source = %v", err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/a", Path2: "/b"}); err != nil {
		t.Fatalf("rename = %v", err)
	}
	if err := src.Handle.Write(ctx, 0, []byte("FRESH")); err != nil {
		t.Fatal(err)
	}
	if err := src.Handle.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 16)
	n, rerr := target.Handle.Read(ctx, 0, buf)
	if rerr != nil {
		t.Fatal(rerr)
	}
	if got := string(buf[:n]); got != "OTHER" {
		t.Fatalf("old target fd read %q through localize, want OTHER", got)
	}
	_ = src.Handle.Release(ctx)
	_ = target.Handle.Release(ctx)
}

type anonProbeHandle struct{ fs.Handle }

func (anonProbeHandle) Anonymous() bool { return true }

// The decorator forwards the inner handle's anonymous identity (the chain
// test above is a smoke test; this pins the propagation itself).
func TestPortLocalizeForwardsAnonymous(t *testing.T) {
	h := &dbAccountingHandle{inner: anonProbeHandle{}}
	if !h.Anonymous() {
		t.Fatal("dbAccountingHandle dropped the Anonymous identity")
	}
	plain := &dbAccountingHandle{inner: nil}
	if plain.Anonymous() {
		t.Fatal("a nil inner must not be anonymous")
	}
}

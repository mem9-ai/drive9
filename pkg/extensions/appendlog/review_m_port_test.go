package appendlog

// qiffang twenty-second round: an fd truncate routes through the NAMED
// handle, a rename retargets every live descriptor, and a no-op flush does
// not retire the lower plane's empty-create claim.

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
)

// Two open descriptors: a truncate addressed to the OLDER handle must hit
// that handle's buffer, not the newest per-path claim.
func TestPortFtruncateUsesNamedHandle(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedWithLayout("/f.log", []byte("abcdef"), "append_log")
	ch := newStack(t, b, "**/f.log")
	ctx := context.Background()

	h1 := openLog(t, ch, "/f.log").(*Handle)
	h2 := openLog(t, ch, "/f.log").(*Handle)
	if err := h1.Write(ctx, 0, []byte("X")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/f.log", Size: 2, Handle: h1}); err != nil {
		t.Fatalf("truncate = %v", err)
	}
	if err := h1.Sync(ctx); err != nil {
		t.Fatalf("sync = %v", err)
	}
	if node, _ := b.Node("/f.log"); string(node.Data) != "Xb" {
		t.Fatalf("backend = %q, want Xb (the named handle's truncated image)", node.Data)
	}
	if err := h2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h1.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

// Every open descriptor follows a rename: an older dirty fd's sync must
// land on the new name instead of ENOENT against the old one.
func TestPortRenameRetargetsEveryHandle(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedWithLayout("/a.log", []byte("abcdef"), "append_log")
	ch := newStack(t, b, "**/*.log")
	ctx := context.Background()

	h1 := openLog(t, ch, "/a.log").(*Handle)
	if _, err := openLog(t, ch, "/a.log"), error(nil); err != nil {
		t.Fatal(err)
	}
	if err := h1.Write(ctx, 0, []byte("X")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/a.log", Path2: "/b.log"}); err != nil {
		t.Fatalf("rename = %v", err)
	}
	if err := h1.Sync(ctx); err != nil {
		t.Fatalf("sync after rename = %v", err)
	}
	if node, ok := b.Node("/b.log"); !ok || string(node.Data) != "Xbcdef" {
		t.Fatalf("/b.log = %q ok=%v, want Xbcdef", node.Data, ok)
	}
	if _, ok := b.Node("/a.log"); ok {
		t.Fatal("old name resurrected by the older descriptor's sync")
	}
}

// appendLogOverWriteback builds [appendlog → writeback(close-sync) → core].
func appendLogOverWriteback(t *testing.T, b *testbackend.Backend, policy writeback.Policy) *fs.Chain {
	t.Helper()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	wb, err := writeback.New(writeback.Options{Policy: policy, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	al, err := New(Options{Patterns: []string{"**/*.log"}, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(al, wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	return ch
}

// An empty file created through the append plane publishes NOTHING (no
// dirty buffer): it must not retire the lower plane's creation claim, or
// the empty file never lands.
func TestPortEmptyAppendLogCreateLands(t *testing.T) {
	b := testbackend.NewBackend()
	ch := appendLogOverWriteback(t, b, writeback.PolicyCloseSync)
	ctx := context.Background()

	h := openLog(t, ch, "/empty.log").(*Handle)
	if err := h.Sync(ctx); err != nil {
		t.Fatalf("sync = %v", err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatalf("release = %v", err)
	}
	if _, ok := b.Node("/empty.log"); !ok {
		t.Fatal("empty append-log create never landed")
	}
}

var _ = syscall.O_RDWR

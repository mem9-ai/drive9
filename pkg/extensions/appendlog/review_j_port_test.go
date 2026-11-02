package appendlog

// mashenjun's rows: the append plane must not let the lower writeback
// handle's empty image overwrite its committed upload (J1), synchronous
// durability completes the protocol commit before acknowledging (J4), and
// renames re-key the live index by the OLD path before changing it (J8).

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

func appendStack(t *testing.T, ts *httptest.Server, syncPolicy string) (*AppendLog, *fs.Chain) {
	t.Helper()
	cl := client.New(ts.URL, "")
	a, err := New(Options{Patterns: []string{"*.log"}, Client: cl, SyncPolicy: syncPolicy})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(a, terminal)
	return a, ch
}

// J1: after the append plane uploads, Sync/Release must not materialize
// the lower handle's empty image over the committed payload.
func TestPortAppendUploadNotOverwrittenByLowerHandle(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	wb, _ := writeback.New(writeback.Options{Policy: writeback.PolicyCloseSync, Client: client.New(ts.URL, "")})
	cl := client.New(ts.URL, "")
	a, err := New(Options{Patterns: []string{"*.log"}, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(a, wb, terminal)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/app.log", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Write(ctx, 0, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	n, ok := b.Node("/app.log")
	if !ok || string(n.Data) != "payload" {
		t.Fatalf("backend image = %q ok=%v, want payload (the lower empty handle overwrote the append upload)", string(n.Data), ok)
	}
}

// J4: a write-sync mount's append-log Write acknowledges only after the
// protocol commit — the remote file exists when Write returns.
func TestPortAppendWriteSyncCommitsBeforeAck(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	_, ch := appendStack(t, ts, "write-sync")
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/app.log", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Write(ctx, 0, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	n, ok := b.Node("/app.log")
	if !ok || string(n.Data) != "payload" {
		t.Fatalf("backend image after write-sync ack = %q ok=%v, want payload", string(n.Data), ok)
	}
}

// J8: a rename re-keys the live index by the old path first — the stale
// released handle must not defer a later unlink forever.
func TestPortAppendRenameRekeysLiveIndex(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	_, ch := appendStack(t, ts, "")
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/a.log", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Write(ctx, 0, []byte("data")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/a.log", Path2: "/b.log"}); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	// A replacement at the old name must be deletable.
	rep, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/a.log", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if err := rep.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/a.log"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := b.Node("/a.log"); ok {
		t.Fatal("unlink left the replacement on the backend (stale released handle deferred the delete)")
	}
}

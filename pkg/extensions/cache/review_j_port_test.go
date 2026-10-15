package cache

// mashenjun's row: after a file grows through its open descriptor, the
// handle's open-time size/rev are stale — whole-file cache admission
// under that identity must be bypassed, or a 4096-byte read returns only
// the open-time prefix forever.

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

func TestPortGrownHandleReadsCurrentImage(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("hello"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wb, _ := writeback.New(writeback.Options{Policy: writeback.PolicyCloseSync, Client: cl})
	rc, _ := New(Options{PrefetchClient: cl, SequentialPrefetch: true})
	wb.SetCommitSeeder(rc.SeedCommit)
	wb.SetPendingSeeder(rc.SeedPending)
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(rc, wb, terminal)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Write(ctx, 5, []byte("-tail")); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 4096)
	n, err := res.Handle.Read(ctx, 0, got)
	if err != nil {
		t.Fatal(err)
	}
	if string(got[:n]) != "hello-tail" {
		t.Fatalf("grown-handle read = %q, want hello-tail (stale open-time image admitted)", string(got[:n]))
	}
}

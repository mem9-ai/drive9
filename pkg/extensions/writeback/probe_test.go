package writeback

import (
	"context"
	"net/http/httptest"
	"sort"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

func dumpRequests(b *testbackend.Backend) string {
	var keys []string
	for k, v := range b.Requests() {
		for i := 0; i < v; i++ {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	out := ""
	for _, k := range keys {
		out += k + "; "
	}
	return out
}

func TestProbePendingNewRenameWriteBackCache(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCache: cache})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/app/file.txt.tmp", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if err := h.Write(ctx, 0, []byte("probe-data")); err != nil {
		t.Fatal(err)
	}
	t.Logf("requests after write: %s", dumpRequests(b))
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/app/file.txt.tmp", Path2: "/app/file.txt"}); err != nil {
		t.Fatalf("rename: %v", err)
	}
	t.Logf("requests after rename: %s", dumpRequests(b))
	if err := h.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatalf("drain: %v", err)
	}
	t.Logf("requests after drain: %s", dumpRequests(b))
	if n, ok := b.Node("/app/file.txt"); ok {
		t.Logf("final node: %q", n.Data)
	} else {
		t.Logf("final node MISSING")
	}
	if n, ok := b.Node("/app/file.txt.tmp"); ok {
		t.Logf("temp node SURVIVED: %q", n.Data)
	}
}

func TestProbePendingNewRenameCloseSyncStaging(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	ledger, err := staging.NewStaging(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledger.Close)
	wb, err := New(Options{Policy: PolicyCloseSync, Client: cl, Staging: ledger})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/app/file.txt.tmp", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if err := h.Write(ctx, 0, []byte("probe-data")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/app/file.txt.tmp", Path2: "/app/file.txt"}); err != nil {
		t.Fatalf("rename: %v", err)
	}
	t.Logf("requests after rename: %s", dumpRequests(b))
	if err := h.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatalf("drain: %v", err)
	}
	t.Logf("requests after drain: %s", dumpRequests(b))
	if n, ok := b.Node("/app/file.txt"); ok {
		t.Logf("final node: %q", n.Data)
	} else {
		t.Logf("final node MISSING")
	}
	if n, ok := b.Node("/app/file.txt.tmp"); ok {
		t.Logf("temp node SURVIVED: %q", n.Data)
	}
}

func TestProbeDirtyExistingRenameWriteBackCache(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app/file.txt", []byte("old-remote"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cache, _ := staging.NewWriteBackCache(t.TempDir())
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCache: cache})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/app/file.txt", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if err := h.Write(ctx, 0, []byte("NEW!")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/app/file.txt", Path2: "/app/moved.txt"}); err != nil {
		t.Fatalf("rename: %v", err)
	}
	t.Logf("requests after rename: %s", dumpRequests(b))
	if err := h.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatalf("drain: %v", err)
	}
	t.Logf("requests after drain: %s", dumpRequests(b))
	if n, ok := b.Node("/app/moved.txt"); ok {
		t.Logf("moved node: %q", n.Data)
	} else {
		t.Logf("moved node MISSING")
	}
}

func TestProbePendingNewRenameTargetExists(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app/file.txt", []byte("preexisting-target"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cache, _ := staging.NewWriteBackCache(t.TempDir())
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCache: cache})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/app/file.txt.tmp", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if err := h.Write(ctx, 0, []byte("fresh-data")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/app/file.txt.tmp", Path2: "/app/file.txt"}); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if err := h.Release(ctx); err != nil {
		t.Logf("release err: %v", err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Logf("drain err: %v", err)
	}
	t.Logf("requests after drain: %s", dumpRequests(b))
	if n, ok := b.Node("/app/file.txt"); ok {
		t.Logf("final node: %q", n.Data)
	}
	if reason, n := wb.DrainPending(); n > 0 {
		t.Logf("drain pending: %s %d", reason, n)
	}
}

func TestProbePendingNewRenameCloseSyncFailure(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	ledger, _ := staging.NewStaging(t.TempDir())
	t.Cleanup(ledger.Close)
	wb, err := New(Options{Policy: PolicyCloseSync, Client: cl, Staging: ledger})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/app/f.txt.tmp", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if err := h.Write(ctx, 0, []byte("will-fail")); err != nil {
		t.Fatal(err)
	}
	// Make every conditional PUT to the temp path fail with a forced conflict
	// beyond the retry budget.
	b.ForceConflict("/app/f.txt.tmp", 99)
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/app/f.txt.tmp", Path2: "/app/f.txt"}); err != nil {
		t.Logf("rename err: %v", err)
	} else {
		t.Logf("rename returned OK despite failing upload")
	}
	if err := h.Release(ctx); err != nil {
		t.Logf("release err: %v", err)
	}
	if reason, n := wb.DrainPending(); n > 0 {
		t.Logf("drain pending: %s %d", reason, n)
	}
	t.Logf("requests: %s", dumpRequests(b))
	if n, ok := b.Node("/app/f.txt"); ok {
		t.Logf("final node: %q", n.Data)
	} else {
		t.Logf("final node MISSING")
	}
}

func TestProbePendingNewRenameAllPutsFail(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	ledger, _ := staging.NewStaging(t.TempDir())
	t.Cleanup(ledger.Close)
	wb, err := New(Options{Policy: PolicyCloseSync, Client: cl, Staging: ledger})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/app/g.txt.tmp", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if err := h.Write(ctx, 0, []byte("doomed")); err != nil {
		t.Fatal(err)
	}
	b.ForceConflict("/app/g.txt.tmp", 99)
	b.ForceConflict("/app/g.txt", 99)
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/app/g.txt.tmp", Path2: "/app/g.txt"}); err != nil {
		t.Logf("rename err: %v", err)
	} else {
		t.Logf("rename returned OK")
	}
	if err := h.Release(ctx); err != nil {
		t.Logf("release err: %v", err)
	} else {
		t.Logf("release returned OK")
	}
	if reason, n := wb.DrainPending(); n > 0 {
		t.Logf("drain pending: %s %d", reason, n)
	}
	t.Logf("requests: %s", dumpRequests(b))
}

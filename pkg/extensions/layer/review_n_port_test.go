package layer

// qiffang twenty-second round: a directory rename migrates the committed
// overlay subtree, and the restore replay reads the overlay maps under the
// lock (the background refresh races live mutations).

import (
	"context"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// A committed child of a layer directory follows a directory rename: the
// namespace entry moved alone, so /e/f used to read ENOENT.
func TestPortDirRenameMigratesCommittedSubtree(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/d", 0o755)
	b.Seed("/d/other", []byte("base"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	_, ch := mStack(t, ts)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/d/f", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Write(ctx, 0, []byte("EDIT")); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/d", Path2: "/e"}); err != nil {
		t.Fatalf("rename = %v", err)
	}
	rd, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/e/f", Offset: 0, Limit: 16})
	if err != nil || string(rd.Data) != "EDIT" {
		t.Fatalf("/e/f = %q err=%v, want the committed EDIT", rd.Data, err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/d/f"}); err == nil {
		t.Fatal("old path still resolves after the directory rename")
	}
}

// The restore replay must read the overlay maps under the lock: it runs on
// the background refresh goroutine while live requests mutate them.
func TestPortRestoreReplayReadsOverlayUnderLock(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	l, ch := mStack(t, ts)
	ctx := context.Background()

	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpMkdir, Path: "/dd", Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/dd", HasMode: true, Mode: 0o700, Size: -1}); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			l.markLayerDir("/dd", 0o750)
			l.markLayerFileMode("/dd", 0o640)
		}
	}()
	for i := 0; i < 50; i++ {
		if err := l.Restore(ctx); err != nil {
			t.Fatalf("restore = %v", err)
		}
	}
	wg.Wait()
}

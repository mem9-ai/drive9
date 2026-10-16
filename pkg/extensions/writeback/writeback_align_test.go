package writeback

// v1 behavioral alignment rows for the buffering handle: open(O_TRUNC)
// truncates locally (remote keeps the pre-truncation image until the commit
// boundary), a failed remote DELETE rolls the anonymous marking back so
// buffered writes are not dropped, rename lands pending data first, and
// O_SYNC opens force per-Write commits.

import (
	"context"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/debounce"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

func TestOpenTruncateKeepsRemoteUntilCommit(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("0123456789"))
	ch, _ := newStack(t, PolicyCloseSync, b)
	ctx := context.Background()

	// open(O_TRUNC|O_WRONLY) without O_CREAT: the handle rebases to empty.
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_WRONLY | syscall.O_TRUNC)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	// v1: the remote image survives the open; only the commit publishes
	// the truncation.
	if n, _ := b.Node("/f"); string(n.Data) != "0123456789" {
		t.Fatalf("remote after open(O_TRUNC) = %q, want pre-truncation image", n.Data)
	}
	if err := h.Write(ctx, 0, []byte("ab")); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/f"); string(n.Data) != "ab" {
		t.Fatalf("remote after close = %q, want ab", n.Data)
	}
}

func TestOpenTruncateNoWritePublishesEmptyAtFlush(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("0123456789"))
	ch, _ := newStack(t, PolicyCloseSync, b)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_WRONLY | syscall.O_TRUNC)})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/f"); len(n.Data) != 0 {
		t.Fatalf("remote after flush = %d bytes, want 0", len(n.Data))
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

// v1 createEmptyHandleRemote: an O_CREAT claim that never wrote still
// materializes at the Flush (close(2)) boundary under the sync policies —
// close returns with the name existing remotely. Deferring to Release lets
// the next syscall (chmod/link/unlink/rmdir preflight) answer from a remote
// that has not seen the file (pjdfstest rename/00, link/00, rmdir/01 rows).
func TestCreateNoWritePublishesEmptyAtFlush(t *testing.T) {
	for _, policy := range []Policy{PolicyCloseSync, PolicyWriteSync} {
		for _, track := range []string{"inline", "uploader"} {
			t.Run(policy.String()+"-"+track, func(t *testing.T) {
				b := testbackend.NewBackend()
				ts := httptest.NewServer(b)
				t.Cleanup(ts.Close)
				cl := client.New(ts.URL, "")
				opts := Options{Policy: policy, Client: cl}
				if track == "uploader" {
					cache, err := staging.NewWriteBackCache(t.TempDir())
					if err != nil {
						t.Fatal(err)
					}
					opts.WriteCache = cache
				}
				wb, err := New(opts)
				if err != nil {
					t.Fatal(err)
				}
				terminal, _ := core.New(cl, core.Options{})
				ch, _ := fs.NewChain(wb, terminal)
				ctx := context.Background()

				res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/new.txt", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
				if err != nil {
					t.Fatal(err)
				}
				if err := res.Handle.Flush(ctx); err != nil {
					t.Fatal(err)
				}
				n, ok := b.Node("/new.txt")
				if !ok {
					t.Fatal("remote after flush: /new.txt missing")
				}
				if len(n.Data) != 0 {
					t.Fatalf("remote after flush = %d bytes, want 0", len(n.Data))
				}
				if err := res.Handle.Release(ctx); err != nil {
					t.Fatal(err)
				}
				if n, ok := b.Node("/new.txt"); !ok || len(n.Data) != 0 {
					t.Fatalf("remote after release = %v, want one empty file", n)
				}
			})
		}
	}
}

// rmdir/01 regression (close-sync, uploader track — the mount config): an
// empty create+close, then unlink, then rmdir of the parent must see a
// consistent remote. When the empty create landed only at Release, the
// unlink's DELETE raced the late upload: the name resurrected inside the
// parent and rmdir returned ENOTEMPTY.
func TestRmdirSeesEmptyDirAfterEmptyCreateUnlink(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{Policy: PolicyCloseSync, Client: cl, WriteCache: cache})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpMkdir, Path: "/d", Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/d/f", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/d/f"}); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRmdir, Path: "/d"}); err != nil {
		t.Fatalf("rmdir after empty create+unlink: %v", err)
	}
	if _, ok := b.Node("/d"); ok {
		t.Fatal("remote directory survived rmdir")
	}
}

func TestUnlinkFailureRollsBackAnonymousMarking(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("base"))
	// Every DELETE fails with a transient 503.
	b.ForceDeleteFailure("/f", 1000)
	ch, _ := newStack(t, PolicyCloseSync, b)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Write(ctx, 4, []byte("NEW")); err != nil {
		t.Fatal(err)
	}
	// unlink while the handle is open: the DELETE fails, so the handle must
	// stay live — its buffered write survives and the next flush commits it
	// (v1: a transient DELETE error must not silently drop buffered data).
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/f"}); err == nil {
		t.Fatal("unlink with failing DELETE unexpectedly succeeded")
	}
	if err := res.Handle.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/f"); string(n.Data) != "baseNEW" {
		t.Fatalf("remote after post-unlink-failure flush = %q, want baseNEW", n.Data)
	}
}

func TestRenameLandsPendingDataBeforeMoving(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/old", []byte("one"))
	ch, _ := newStack(t, PolicyCloseSync, b)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/old", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Write(ctx, 0, []byte("two!")); err != nil {
		t.Fatal(err)
	}
	// rename before any flush: the pending write must land on /old first,
	// then the rename moves it — the new path reads "two!" and nothing
	// resurrects the old path afterwards (v1 flushPendingWriteBack).
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/old", Path2: "/new"}); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/new"); string(n.Data) != "two!" {
		t.Fatalf("/new = %q, want two!", n.Data)
	}
	if _, ok := b.Node("/old"); ok {
		t.Fatal("/old still present after rename")
	}
	if err := res.Handle.Write(ctx, 0, []byte("X")); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/new"); string(n.Data) != "Xwo!" {
		t.Fatalf("retargeted writer committed to %q, want Xwo!", n.Data)
	}
}

func TestSyncOpenFlagForcesPerWriteCommit(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	defer ts.Close()
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/s", Flags: uint32(syscall.O_WRONLY | syscall.O_CREAT | syscall.O_SYNC)})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Write(ctx, 0, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	// v1 writePolicyForOpen: O_SYNC commits inline per Write even under
	// the writeback policy.
	if n, ok := b.Node("/s"); !ok || string(n.Data) != "abc" {
		t.Fatalf("remote after single write = %q ok=%v, want abc", n.Data, ok)
	}
}

func TestFlushDebounceNegativeMeansDefault(t *testing.T) {
	wb, err := New(Options{Policy: PolicyWriteBack, Client: client.New("http://x", ""), FlushDebounce: -1})
	if err != nil {
		t.Fatal(err)
	}
	if wb.debounce == nil {
		t.Fatal("FlushDebounce=-1 must install the default debouncer (v1 default 2s)")
	}
	if wb.debounce.Delay() != debounce.DefaultFlushDebounce {
		t.Fatalf("debounce delay = %v, want %v", wb.debounce.Delay(), debounce.DefaultFlushDebounce)
	}
	wb2, err := New(Options{Policy: PolicyWriteBack, Client: client.New("http://x", ""), FlushDebounce: 0})
	if err != nil {
		t.Fatal(err)
	}
	if wb2.debounce != nil {
		t.Fatal("FlushDebounce=0 must disable the debouncer")
	}
	_ = time.Second
}

func TestUnlinkSurvivorRebindsHardlinkedWriter(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/orig", []byte("hello"))
	b.SeedHardlink("/orig", "/alias")
	ch, wb := newStack(t, PolicyWriteBack, b)
	// writeback-without-staging: flush commits inline; survivor applies.
	wb.SetSurvivorLookup(func(path string) (string, bool) {
		if path == "/orig" {
			return "/alias", true
		}
		return "", false
	})
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/orig", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Write(ctx, 5, []byte("!")); err != nil {
		t.Fatal(err)
	}
	// unlink the name the writer holds: the writer must rebind to /alias —
	// its write lands there and /orig disappears (v1 unlink-survivor).
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/orig"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := b.Node("/orig"); ok {
		t.Fatal("/orig still present after unlink")
	}
	if n, ok := b.Node("/alias"); !ok || string(n.Data) != "hello!" {
		t.Fatalf("/alias = %q ok=%v, want hello!", n.Data, ok)
	}
}

func TestInteractiveSyncStagesWithoutQueueWait(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	defer ts.Close()
	cl := client.New(ts.URL, "")
	dir := t.TempDir()
	ledger, err := staging.NewStaging(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledger.Close)
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, Staging: ledger, SyncMode: "interactive"})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/i", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Write(ctx, 0, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	// v1 interactive: fsync returned on local durability; the remote image
	// lands asynchronously. Strict mode would have it already.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if n, ok := b.Node("/i"); ok && string(n.Data) == "payload" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("interactive staged image never landed remotely")
}

func TestStrictSyncWaitsForRemote(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	defer ts.Close()
	cl := client.New(ts.URL, "")
	dir := t.TempDir()
	ledger, err := staging.NewStaging(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledger.Close)
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, Staging: ledger, SyncMode: "strict"})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/s", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Write(ctx, 0, []byte("strict")); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if n, ok := b.Node("/s"); !ok || string(n.Data) != "strict" {
		t.Fatalf("strict fsync did not land: %q ok=%v", n.Data, ok)
	}
}

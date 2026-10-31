package extent

import (
	"context"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// Round-8 P1: a directory rename arrives as ONE event; open descendants
// must follow it in the handler AND the bridge, so a later create at the
// old child pathname (full mode) gets its own inode instead of the
// retained descriptor's — and the retained descriptor keeps committing to
// its moved name.
func TestDirectoryRenameKeepsOpenDescendantIdentity(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	f.seedExtent(t, "/d/a", "AAAA")
	h := newTestHandler(t, f, 0) // full mode: fresh creates ride the engine
	b := h.opts.Engine.Bridge
	ehA := claimOpen(t, h, "/d/a")

	// mv /d /e server-side, reported as one directory rename event.
	f.rename(t, "/d/a", "/e/a")
	f.setDir("/d", false)
	f.setDir("/e", true)
	renameEvent(h, "/d", "/e")
	if ehA.path != "/e/a" {
		t.Fatalf("open descendant did not follow the directory rename: %q", ehA.path)
	}
	b.mu.Lock()
	pathA := b.paths[ehA.ino]
	b.mu.Unlock()
	if pathA != "/e/a" {
		t.Fatalf("bridge mapping for A = %q, want /e/a", pathA)
	}

	// mkdir /d; create B at /d/a while A stays open.
	f.setDir("/d", true)
	res, err, passed := dispatchCreate(context.Background(), h, "/d/a", syscall.O_RDWR|syscall.O_CREAT)
	if err != nil || passed {
		t.Fatalf("full-mode create: err=%v passed=%v", err, passed)
	}
	ehB := res.Handle.(*engineHandle)
	if ehB.ino == ehA.ino {
		t.Fatalf("B reused the retained descendant's inode %d", ehA.ino)
	}
	b.mu.Lock()
	pathA, pathB := b.paths[ehA.ino], b.paths[ehB.ino]
	b.mu.Unlock()
	if pathA != "/e/a" || pathB != "/d/a" {
		t.Fatalf("mappings A=%q B=%q, want /e/a and /d/a", pathA, pathB)
	}
	// A's write still lands on /e/a, never on B.
	if err := ehA.Write(context.Background(), 0, []byte("ZZZZ")); err != nil {
		t.Fatal(err)
	}
	if err := ehA.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	patched := append([]string(nil), f.patchPaths...)
	f.mu.Unlock()
	if len(patched) == 0 {
		t.Fatal("sync through A committed nothing")
	}
	for _, p := range patched {
		if p != "/e/a" {
			t.Fatalf("A's commit targeted %q, want /e/a", p)
		}
	}
	if got := f.contentOf("/e/a"); got != "ZZZZ" {
		t.Fatalf("/e/a = %q after A's write, want ZZZZ", got)
	}
}

// Round-8 P2: reopening a migrated replacement at its NEW name must return
// the same inode its open descriptor uses — JuiceFS invalidates readers per
// inode, so a second key would leave one handle's cache stale after a
// write through the other.
func TestReopenOfMigratedReplacementSharesInode(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	h := newTestHandler(t, f, 1<<20)
	claimOpen(t, h, "/a") // A stays open throughout
	f.rename(t, "/a", "/b")
	renameEvent(h, "/a", "/b")
	f.seedExtent(t, "/a", "BBBB")
	ehB := claimOpen(t, h, "/a") // the replacement incarnation
	f.rename(t, "/a", "/c")
	renameEvent(h, "/a", "/c")

	ehC := claimOpen(t, h, "/c")
	if ehC.ino != ehB.ino {
		t.Fatalf("reopen of /c resolved inode %d, want the migrated replacement's %d", ehC.ino, ehB.ino)
	}
	// Read-cache coherence: populate C's reader, write through B, read C.
	if got := readFront(t, ehC, 16); got != "BBBB" {
		t.Fatalf("C reads %q before the write, want BBBB", got)
	}
	if err := ehB.Write(context.Background(), 0, []byte("ZZZZ")); err != nil {
		t.Fatal(err)
	}
	if err := ehB.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf("/c"); got != "ZZZZ" {
		t.Fatalf("server content after B's sync = %q, want ZZZZ", got)
	}
	if got := readFront(t, ehC, 16); got != "ZZZZ" {
		t.Fatalf("C reads %q after B's write+sync through the shared inode, want ZZZZ", got)
	}
}

// Round-8 P2: a replacement's local state is retired when its last handle
// closes / its name is deleted, so the next incarnation at the same name
// cannot inherit its length — while the original descriptor at the
// renamed-away name stays protected, and a second open of the STILL-LIVE
// replacement keeps reusing its inode.
func TestClosedDeletedReplacementStateRetired(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAAAAAA")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	ehA := claimOpen(t, h, "/a")
	f.rename(t, "/a", "/b")
	renameEvent(h, "/a", "/b")

	f.seedExtent(t, "/a", "BBBBBBBB")
	ehB := claimOpen(t, h, "/a")
	ehB2 := claimOpen(t, h, "/a")
	if ehB2.ino != ehB.ino {
		t.Fatalf("second open of the live replacement got %d, want %d", ehB2.ino, ehB.ino)
	}
	if err := ehB2.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	_, stillOwned := b.replacements["/a"]
	b.mu.Unlock()
	if !stillOwned {
		t.Fatal("closing one of two handles retired the live replacement")
	}
	if err := ehB.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	_, owned := b.replacements["/a"]
	_, sized := b.sizes[ehB.ino]
	pathA, sizeA := b.paths[ehA.ino], b.sizes[ehA.ino]
	b.mu.Unlock()
	if owned || sized {
		t.Fatalf("closed replacement state survived: owned=%v sized=%v", owned, sized)
	}
	if pathA != "/b" || sizeA != 8 {
		t.Fatalf("original descriptor disturbed: path=%q size=%d", pathA, sizeA)
	}

	// rm /a, then another client creates a 4-byte C there.
	f.remove("/a")
	h.opts.Events.Emit(fs.Event{Kind: fs.EventDelete, Path: "/a"})
	f.seedExtent(t, "/a", "CCCC")
	ehC := claimOpen(t, h, "/a")
	if ehC.ino == ehB.ino {
		t.Fatalf("new incarnation inherited the retired replacement inode %d", ehB.ino)
	}
	if size, ok := b.LocalSizeOf("/a"); !ok || size != 4 {
		t.Fatalf("LocalSizeOf(/a) = (%d,%v), want 4", size, ok)
	}
	res, err := h.Do(context.Background(), &fs.Op{Kind: fs.OpGetAttr, Path: "/a", Caller: fs.Caller{Pid: 1}}, fs.Next(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		return fs.Result{Attr: &fs.Attr{Size: 4}}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if res.Attr == nil || res.Attr.Size != 4 {
		t.Fatalf("GetAttr(/a) published %+v, want 4", res.Attr)
	}
}

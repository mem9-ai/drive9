package extent

import (
	"context"
	"sync"
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// claimOpen claims an existing extent file through the plane and returns
// its engine handle.
func claimOpen(t *testing.T, h *Handler, path string) *engineHandle {
	t.Helper()
	res, err, passed := dispatchCreate(context.Background(), h, path, syscall.O_RDWR)
	if err != nil || passed {
		t.Fatalf("claim %s: err=%v passed=%v", path, err, passed)
	}
	eh, ok := res.Handle.(*engineHandle)
	if !ok {
		t.Fatalf("claim %s: handle type = %T", path, res.Handle)
	}
	return eh
}

// renameEvent replays what pkg/fuse emits for a rename: EventWrite on the
// OLD name with RenamedTo, then a plain EventDelete for the old name.
func renameEvent(h *Handler, from, to string) {
	h.opts.Events.Emit(fs.Event{Kind: fs.EventWrite, Path: from, RenamedTo: to})
	h.opts.Events.Emit(fs.Event{Kind: fs.EventDelete, Path: from})
}

// unlinkOpen drives OpUnlink through the plane for a path with live
// handles; the classic chain must never see it.
func unlinkOpen(t *testing.T, h *Handler, path string) {
	t.Helper()
	passed := false
	_, err := h.Do(context.Background(), &fs.Op{Kind: fs.OpUnlink, Path: path, Caller: fs.Caller{Pid: 1}}, fs.Next(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		passed = true
		return fs.Result{}, nil
	}))
	if err != nil {
		t.Fatalf("unlink %s: %v", path, err)
	}
	if passed {
		t.Fatalf("unlink %s with live handles reached the classic plane", path)
	}
}

func readFront(t *testing.T, eh *engineHandle, n int) string {
	t.Helper()
	buf := make([]byte, n)
	got, err := eh.Read(context.Background(), 0, buf)
	if err != nil {
		t.Fatalf("read %s: %v", eh.path, err)
	}
	return string(buf[:got])
}

// Round-7 P1: renaming a REPLACEMENT again must retarget the replacement,
// not the unrelated original inode whose pathname hash it reused. Open A
// at /a, rename it to /b, open the new incarnation B at /a (synthetic key
// H), then rename B to /c while A stays open: A keeps committing to /b.
func TestSecondRenameKeepsOriginalIncarnation(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	ehA := claimOpen(t, h, "/a")

	f.rename(t, "/a", "/b")
	renameEvent(h, "/a", "/b")

	f.seedExtent(t, "/a", "BBBB") // another client's new incarnation at /a
	ehB := claimOpen(t, h, "/a")
	if ehB.ino == ehA.ino {
		t.Fatalf("replacement reused the original inode %d", ehA.ino)
	}

	f.rename(t, "/a", "/c")
	renameEvent(h, "/a", "/c")

	b.mu.Lock()
	pathA, pathB := b.paths[ehA.ino], b.paths[ehB.ino]
	repC, hasC := b.replacements["/c"]
	_, staleA := b.replacements["/a"]
	b.mu.Unlock()
	if pathA != "/b" {
		t.Fatalf("original inode retargeted by the replacement's rename: paths[A]=%q, want /b", pathA)
	}
	if pathB != "/c" || !hasC || repC != ehB.ino || staleA {
		t.Fatalf("replacement did not follow its rename: paths[B]=%q replacements[/c]=(%d,%v) stale[/a]=%v", pathB, repC, hasC, staleA)
	}
	if ehA.path != "/b" || ehB.path != "/c" {
		t.Fatalf("handle paths A=%q B=%q, want /b and /c", ehA.path, ehB.path)
	}
	// The behavioral proof: a write through A's descriptor commits to /b,
	// never to B's /c.
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
		if p != "/b" {
			t.Fatalf("A's commit targeted %q (B's object), want /b", p)
		}
	}
}

// Round-7 P1: two simultaneously open, unlinked generations of one
// pathname must retain DISTINCT sentinel objects — the server's rename
// replaces an existing target, so a shared name would overwrite the first
// retained file while it is still open.
func TestDeletedOpenGenerationsKeepDistinctSentinels(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	ehA := claimOpen(t, h, "/a")
	unlinkOpen(t, h, "/a")
	sentinelA := ehA.path
	if sentinelA == "/a" {
		t.Fatal("unlink with a live handle did not silly-rename")
	}

	f.seedExtent(t, "/a", "BBBB") // recreate + commit the next generation
	ehB := claimOpen(t, h, "/a")
	unlinkOpen(t, h, "/a")
	sentinelB := ehB.path
	if sentinelB == "/a" || sentinelB == sentinelA {
		t.Fatalf("second generation sentinel %q collides with the first %q", sentinelB, sentinelA)
	}
	if got := f.contentOf(b.fullPath(sentinelA)); got != "AAAA" {
		t.Fatalf("first retained object overwritten by the second retention: %q", got)
	}
	if got := f.contentOf(b.fullPath(sentinelB)); got != "BBBB" {
		t.Fatalf("second retained object = %q, want BBBB", got)
	}
	if got := readFront(t, ehA, 16); got != "AAAA" {
		t.Fatalf("A reads %q after B's retention, want AAAA", got)
	}
	if got := readFront(t, ehB, 16); got != "BBBB" {
		t.Fatalf("B reads %q, want BBBB", got)
	}
	// B's last close drops ONLY B's sentinel; A stays readable until its
	// own last close.
	if err := ehB.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf(b.fullPath(sentinelB)); got != "" {
		t.Fatalf("B's sentinel survived its last close: %q", got)
	}
	if got := f.contentOf(b.fullPath(sentinelA)); got != "AAAA" {
		t.Fatalf("A's retained object lost with B's close: %q", got)
	}
	if got := readFront(t, ehA, 16); got != "AAAA" {
		t.Fatalf("A reads %q after B closed, want AAAA", got)
	}
	if err := ehA.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf(b.fullPath(sentinelA)); got != "" {
		t.Fatalf("A's sentinel survived its last close: %q", got)
	}
	b.mu.Lock()
	left := len(b.sentinels)
	b.mu.Unlock()
	if left != 0 {
		t.Fatalf("%d sentinel registrations leaked", left)
	}
}

// Round-7 P2: the size overlay must read the LIVE incarnation. After the
// 8-byte original is renamed away and a 4-byte replacement is opened at
// the old name, GetAttr on that name must not inherit the original's
// length.
func TestGetAttrOverlayReadsReplacementIncarnation(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAAAAAA")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	ehA := claimOpen(t, h, "/a")
	f.rename(t, "/a", "/b")
	renameEvent(h, "/a", "/b")

	f.seedExtent(t, "/a", "BBBB")
	ehB := claimOpen(t, h, "/a")
	if ehB.ino == ehA.ino {
		t.Fatalf("replacement reused the original inode %d", ehA.ino)
	}
	if size, ok := b.LocalSizeOf("/a"); !ok || size != 4 {
		t.Fatalf("LocalSizeOf(/a) = (%d,%v), want the replacement's 4", size, ok)
	}
	if size, ok := b.LocalSizeOf("/b"); !ok || size != 8 {
		t.Fatalf("LocalSizeOf(/b) = (%d,%v), want the original's 8", size, ok)
	}
	res, err := h.Do(context.Background(), &fs.Op{Kind: fs.OpGetAttr, Path: "/a", Caller: fs.Caller{Pid: 1}}, fs.Next(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		return fs.Result{Attr: &fs.Attr{Size: 4}}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if res.Attr == nil || res.Attr.Size != 4 {
		t.Fatalf("GetAttr(/a) published %+v, want the replacement's size 4", res.Attr)
	}
}

// Round-7 P2: concurrent first resolutions of one replacement incarnation
// must converge on a single synthetic inode — JuiceFS keys reader/writer
// state and invalidation by inode, so two keys for one file would let one
// handle keep stale bytes after a write through the other.
func TestConcurrentResolveLocalConvergesOnOneInode(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	b := newTestHandler(t, f, 1<<20).opts.Engine.Bridge
	orig, errno := b.ResolveLocal(context.Background(), "/a")
	if errno != 0 {
		t.Fatal(errno)
	}
	f.rename(t, "/a", "/b")
	b.RemapLocal("/a", "/b")      // the original moved away: /a's hash is held
	f.seedExtent(t, "/a", "BBBB") // another client created a new /a

	const n = 32
	var (
		start = make(chan struct{})
		wg    sync.WaitGroup
		mu    sync.Mutex
		seen  = map[meta.Ino]int{}
	)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ino, errno := b.ResolveLocal(context.Background(), "/a")
			mu.Lock()
			defer mu.Unlock()
			if errno != 0 {
				seen[0]++
				return
			}
			seen[ino]++
		}()
	}
	close(start)
	wg.Wait()
	if len(seen) != 1 {
		t.Fatalf("concurrent resolutions published %d distinct inodes: %v", len(seen), seen)
	}
	for ino := range seen {
		if ino == 0 || ino == orig {
			t.Fatalf("replacement resolved to %d (original %d)", ino, orig)
		}
		b.mu.Lock()
		rep := b.replacements["/a"]
		b.mu.Unlock()
		if rep != ino {
			t.Fatalf("published inode %d but the replacement map says %d", ino, rep)
		}
	}
}

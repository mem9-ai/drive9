package extent

import (
	"context"
	"testing"
)

// Round-9 P1: moving another file into a VACATED pathname must not take
// over the hash key still used by the moved-open file that vacated it.
// Open B at /b, rename it to /c (B stays open), open A at /a and rename it
// into the now-empty /b: B keeps committing to /c and keeps its own length;
// A owns /b.
func TestRenameIntoVacatedNameKeepsMovedOpenInode(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	f.seedExtent(t, "/b", "BBBBBBBB")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge

	ehB := claimOpen(t, h, "/b")
	f.rename(t, "/b", "/c")
	renameEvent(h, "/b", "/c")

	ehA := claimOpen(t, h, "/a")
	f.rename(t, "/a", "/b")
	renameEvent(h, "/a", "/b")

	b.mu.Lock()
	pathB, sizeB := b.paths[ehB.ino], b.sizes[ehB.ino]
	pathA := b.paths[ehA.ino]
	owner, owned := b.replacements["/b"]
	b.mu.Unlock()
	if pathB != "/c" || sizeB != 8 {
		t.Fatalf("moved-open B hijacked by the rename into its vacated name: path=%q size=%d, want /c and 8", pathB, sizeB)
	}
	if pathA != "/b" || !owned || owner != ehA.ino {
		t.Fatalf("A does not own /b: path=%q owner=(%d,%v)", pathA, owner, owned)
	}
	if ehA.path != "/b" || ehB.path != "/c" {
		t.Fatalf("handle paths A=%q B=%q, want /b and /c", ehA.path, ehB.path)
	}
	if size, ok := b.LocalSizeOf("/b"); !ok || size != 4 {
		t.Fatalf("LocalSizeOf(/b) = (%d,%v), want A's 4", size, ok)
	}
	// Where the old descriptor's write actually lands: B's commit goes to
	// /c; A's file at /b is untouched.
	if err := ehB.Write(context.Background(), 0, []byte("ZZZZ")); err != nil {
		t.Fatal(err)
	}
	if err := ehB.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	patched := append([]string(nil), f.patchPaths...)
	f.mu.Unlock()
	if len(patched) == 0 {
		t.Fatal("sync through B committed nothing")
	}
	for _, p := range patched {
		if p != "/c" {
			t.Fatalf("B's commit targeted %q (A's file), want /c", p)
		}
	}
	if got := f.contentOf("/c"); got != "ZZZZBBBB" {
		t.Fatalf("/c = %q after B's write, want ZZZZBBBB", got)
	}
	if got := f.contentOf("/b"); got != "AAAA" {
		t.Fatalf("/b = %q, A's file was modified through B's descriptor", got)
	}
	if got := readFront(t, ehA, 16); got != "AAAA" {
		t.Fatalf("A reads %q, want AAAA", got)
	}
}

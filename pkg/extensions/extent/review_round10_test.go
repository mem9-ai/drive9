package extent

import (
	"context"
	"errors"
	"testing"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// renameThrough drives OpRename through the plane; next models the chain
// performing the server rename (replacing an existing destination like the
// real server) unless told to fail.
func renameThrough(t *testing.T, h *Handler, f *fakeV2, from, to string, fail error) (passed bool, err error) {
	t.Helper()
	_, err = h.Do(context.Background(), &fs.Op{Kind: fs.OpRename, Path: from, Path2: to, Caller: fs.Caller{Pid: 1}}, fs.Next(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		passed = true
		if fail != nil {
			return fs.Result{}, fail
		}
		f.rename(t, op.Path, op.Path2)
		return fs.Result{}, nil
	}))
	return passed, err
}

// Round-10 P1: renaming onto a still-open engine file must preserve the
// displaced descriptor's original object. A at /a and B at /b both open;
// mv /a /b: B moves to its own sentinel BEFORE the server replaces /b, so a
// write through B lands on B's retained object and never on A.
func TestRenameOverOpenTargetPreservesDisplacedDescriptor(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	f.seedExtent(t, "/b", "BBBBBBBB")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	ehA := claimOpen(t, h, "/a")
	ehB := claimOpen(t, h, "/b")

	passed, err := renameThrough(t, h, f, "/a", "/b", nil)
	if err != nil || !passed {
		t.Fatalf("rename: err=%v passed=%v", err, passed)
	}
	renameEvent(h, "/a", "/b") // the facade's event after the chain's rename

	sentinel := ehB.path
	if sentinel == "/b" {
		t.Fatal("displaced open target was not moved to a sentinel")
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "BBBBBBBB" {
		t.Fatalf("displaced object = %q, want BBBBBBBB", got)
	}
	if got := f.contentOf("/b"); got != "AAAA" {
		t.Fatalf("/b = %q after the rename, want A's AAAA", got)
	}
	h.mu.Lock()
	atB := append([]*engineHandle(nil), h.handles["/b"]...)
	h.mu.Unlock()
	if ehA.path != "/b" || len(atB) != 1 || atB[0] != ehA {
		t.Fatalf("handles at /b = %d (A at %q), want only A", len(atB), ehA.path)
	}
	// A write through the displaced descriptor leaves the new /b unchanged.
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
		if p != b.fullPath(sentinel) {
			t.Fatalf("B's commit targeted %q, want its sentinel %q", p, b.fullPath(sentinel))
		}
	}
	if got := f.contentOf("/b"); got != "AAAA" {
		t.Fatalf("/b = %q, the renamed file was modified through the displaced descriptor", got)
	}
	if got := readFront(t, ehA, 16); got != "AAAA" {
		t.Fatalf("A reads %q, want AAAA", got)
	}
	if got := readFront(t, ehB, 16); got != "ZZZZBBBB" {
		t.Fatalf("B reads %q, want ZZZZBBBB", got)
	}
	// B's last close drops its sentinel; /b (A) stays.
	if err := ehB.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "" {
		t.Fatalf("sentinel survived B's last close: %q", got)
	}
	if got := f.contentOf("/b"); got != "AAAA" {
		t.Fatalf("/b = %q after B's close, want AAAA", got)
	}
}

// Round-10: a rename that fails after the displaced target was set aside
// puts it back under its name, with no sentinel registration left behind.
func TestRenameOverOpenTargetRestoresOnFailure(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	f.seedExtent(t, "/b", "BBBBBBBB")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	claimOpen(t, h, "/a")
	ehB := claimOpen(t, h, "/b")

	boom := errors.New("rename refused")
	passed, err := renameThrough(t, h, f, "/a", "/b", boom)
	if !passed || !errors.Is(err, boom) {
		t.Fatalf("rename: err=%v passed=%v, want the chain's error", err, passed)
	}
	if ehB.path != "/b" {
		t.Fatalf("displaced target not restored: handle at %q", ehB.path)
	}
	if got := f.contentOf("/b"); got != "BBBBBBBB" {
		t.Fatalf("/b = %q after the failed rename, want BBBBBBBB", got)
	}
	if got := f.contentOf("/a"); got != "AAAA" {
		t.Fatalf("/a = %q, want AAAA", got)
	}
	b.mu.Lock()
	left := len(b.sentinels)
	pathB := b.paths[ehB.ino]
	b.mu.Unlock()
	if left != 0 || pathB != "/b" {
		t.Fatalf("sentinels=%d paths[B]=%q, want 0 and /b", left, pathB)
	}
	if err := ehB.Write(context.Background(), 0, []byte("ZZZZ")); err != nil {
		t.Fatal(err)
	}
	if err := ehB.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf("/b"); got != "ZZZZBBBB" {
		t.Fatalf("/b = %q after B's write, want ZZZZBBBB", got)
	}
}

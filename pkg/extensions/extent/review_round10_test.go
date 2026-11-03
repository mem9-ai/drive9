package extent

import (
	"context"
	"errors"
	"strings"
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

// hiddenNames lists the sentinel-looking names the fake currently holds.
func hiddenNames(f *fakeV2) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for p := range f.extent {
		if strings.Contains(p, ".d9open-") {
			out = append(out, p)
		}
	}
	return out
}

// Round-10 P1: renaming onto a still-open engine file must preserve the
// displaced descriptor's original object. A at /a and B at /b both open;
// mv /a /b: B gains a hardlink alias BEFORE the server replaces /b, so a
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
		t.Fatal("displaced open target was not given a retained alias")
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
			t.Fatalf("B's commit targeted %q, want its alias %q", p, b.fullPath(sentinel))
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
	// B's last close drops its alias — the last reference; /b (A) stays.
	if err := ehB.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "" {
		t.Fatalf("alias survived B's last close: %q", got)
	}
	if got := f.contentOf("/b"); got != "AAAA" {
		t.Fatalf("/b = %q after B's close, want AAAA", got)
	}
}

// Round-10 (round-13 semantics): a rename the chain refuses (nothing
// applied) leaves the displaced file attached to its alias — the file
// never left its name, so the alias shares the object and a write through
// the descriptor is visible at /b — and the alias drops at its own last
// close, leaving /b exactly as it was.
func TestRenameOverOpenTargetFailedRenameKeepsAlias(t *testing.T) {
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
	sentinel := ehB.path
	if sentinel == "/b" {
		t.Fatal("displaced descriptor was pulled back onto the replaceable name")
	}
	if got := f.contentOf("/b"); got != "BBBBBBBB" {
		t.Fatalf("/b = %q after the failed rename, want BBBBBBBB", got)
	}
	if got := f.contentOf("/a"); got != "AAAA" {
		t.Fatalf("/a = %q, want AAAA", got)
	}
	// The alias shares the object: a write through the displaced descriptor
	// lands on the alias and is the same bytes /b shows.
	if err := ehB.Write(context.Background(), 0, []byte("ZZZZ")); err != nil {
		t.Fatal(err)
	}
	if err := ehB.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "ZZZZBBBB" {
		t.Fatalf("alias = %q after B's write, want ZZZZBBBB", got)
	}
	if got := f.contentOf("/b"); got != "ZZZZBBBB" {
		t.Fatalf("/b = %q after B's write (the object is shared), want ZZZZBBBB", got)
	}
	// A later descriptor opens the same file through the name; closing the
	// ORIGINAL descriptor then drops the alias but must not discard B
	// while the later descriptor is still open (B still holds /b).
	ehB2 := claimOpen(t, h, "/b")
	if ehB2.ino == ehB.ino {
		t.Fatal("the post-failure open reused the retained inode")
	}
	if err := ehB.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if left := hiddenNames(f); len(left) != 0 {
		t.Fatalf("alias left behind after B's close: %v", left)
	}
	if got := f.contentOf("/b"); got != "ZZZZBBBB" {
		t.Fatalf("/b = %q after B's close, want ZZZZBBBB", got)
	}
	if got := readFront(t, ehB2, 16); got != "ZZZZBBBB" {
		t.Fatalf("the later descriptor reads %q, want ZZZZBBBB", got)
	}
	if err := ehB2.Write(context.Background(), 0, []byte("YYYY")); err != nil {
		t.Fatal(err)
	}
	if err := ehB2.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf("/b"); got != "YYYYBBBB" {
		t.Fatalf("/b = %q after the later descriptor's write, want YYYYBBBB", got)
	}
	if reg := registeredSentinels(b); reg != 0 {
		t.Fatalf("%d registrations left behind", reg)
	}
}

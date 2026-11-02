package extent

import (
	"context"
	"errors"
	"testing"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// Round-12 P1: a file appearing at the destination right after a vacant
// probe must never be destroyed. The rename fails without applying while
// someone removes /b; the probe answers 404 and, before the plane's next
// request, another client creates C there. Nothing is restored over C: the
// displaced B stays alive at its alias.
func TestRenameOverOpenTargetNeverTouchesALateArrival(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	f.seedExtent(t, "/b", "BBBBBBBB")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	claimOpen(t, h, "/a")
	ehB := claimOpen(t, h, "/b")

	boom := errors.New("server failure")
	arrived := false
	err := renameWith(h, "/a", "/b", func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		f.remove("/b") // the name vanished underneath the failing rename
		f.setAfterHead(func(path string) {
			// Right after the probe's 404: C lands at /b (f.mu is held by
			// the HEAD handler; mutate the maps directly).
			if path == "/b" && !arrived {
				arrived = true
				f.extent["/b"] = "CCCC"
				f.nextID++
				f.ids["/b"] = "late-arrival"
			}
		})
		return fs.Result{}, boom
	})
	f.setAfterHead(nil)
	if !errors.Is(err, boom) {
		t.Fatalf("rename error = %v, want the chain's", err)
	}
	if !arrived {
		t.Fatal("the probe never ran")
	}
	sentinel := ehB.path
	if got := f.contentOf("/b"); got != "CCCC" {
		t.Fatalf("/b = %q: the late arrival was destroyed (want CCCC)", got)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "BBBBBBBB" {
		t.Fatalf("displaced object = %q, want BBBBBBBB", got)
	}
	if got := readFront(t, ehB, 16); got != "BBBBBBBB" {
		t.Fatalf("B reads %q, want BBBBBBBB", got)
	}
	if err := ehB.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf("/b"); got != "CCCC" {
		t.Fatalf("/b = %q after B's close, want CCCC", got)
	}
}

// Round-12 P1: retention names are unique per operation, so a later
// retention of the same pathname can never land on an earlier retained
// object — whether that object is still live or a leftover.
func TestRetentionNamesNeverCollide(t *testing.T) {
	if a, b := deletedOpenSentinel("/b", 7), deletedOpenSentinel("/b", 7); a == b {
		t.Fatalf("two retentions of the same path/inode produced one name %q", a)
	}
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	f.seedExtent(t, "/b", "BBBBBBBB")
	f.seedExtent(t, "/x", "XXXX")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	ehB := claimOpen(t, h, "/b")

	// mv /a /b with B open: B retained at S1.
	if passed, err := renameThrough(t, h, f, "/a", "/b", nil); err != nil || !passed {
		t.Fatalf("first rename: err=%v passed=%v", err, passed)
	}
	renameEvent(h, "/a", "/b")
	s1 := ehB.path
	// Open the file now at /b (A) and mv /x /b over it: A retained at S2.
	ehA := claimOpen(t, h, "/b")
	if passed, err := renameThrough(t, h, f, "/x", "/b", nil); err != nil || !passed {
		t.Fatalf("second rename: err=%v passed=%v", err, passed)
	}
	renameEvent(h, "/x", "/b")
	s2 := ehA.path
	if s1 == s2 || s1 == "/b" || s2 == "/b" {
		t.Fatalf("retention names collide or missing: s1=%q s2=%q", s1, s2)
	}
	if got := f.contentOf(b.fullPath(s1)); got != "BBBBBBBB" {
		t.Fatalf("first retained object = %q after the second retention, want BBBBBBBB", got)
	}
	if got := f.contentOf(b.fullPath(s2)); got != "AAAA" {
		t.Fatalf("second retained object = %q, want AAAA", got)
	}
	if got := f.contentOf("/b"); got != "XXXX" {
		t.Fatalf("/b = %q, want XXXX", got)
	}
	if got := readFront(t, ehB, 16); got != "BBBBBBBB" {
		t.Fatalf("B reads %q, want BBBBBBBB", got)
	}
	if got := readFront(t, ehA, 16); got != "AAAA" {
		t.Fatalf("A reads %q, want AAAA", got)
	}
}

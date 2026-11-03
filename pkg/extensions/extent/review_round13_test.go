package extent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// Round-13 P1: an identity probe is not a reservation on the name. The
// probe returns B's identity at /b and another client replaces /b with C
// immediately afterwards; the displaced descriptor must stay attached to
// B, its alias must survive (it may be B's last reference), and C must be
// untouched.
func TestRenameOverOpenTargetDisplacedProbeIsNotAReservation(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	f.seedExtent(t, "/b", "BBBBBBBB")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	claimOpen(t, h, "/a")
	ehB := claimOpen(t, h, "/b")
	dstID := func() string {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.ids["/b"]
	}()

	boom := errors.New("server failure")
	err := renameWith(h, "/a", "/b", func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		// The probe will see B's identity... and C replaces /b right
		// after that HEAD was answered (f.mu is held by the handler).
		f.setAfterHead(func(path string) {
			if path == "/b" && f.ids["/b"] == dstID {
				f.extent["/b"] = "CCCC"
				f.nextID++
				f.ids["/b"] = "foreign-c"
			}
		})
		return fs.Result{}, boom // the rename itself never applied
	})
	f.setAfterHead(nil)
	if !errors.Is(err, boom) {
		t.Fatalf("rename error = %v, want the chain's", err)
	}
	sentinel := ehB.path
	if sentinel == "/b" {
		t.Fatal("the displaced descriptor was redirected onto the replaceable name")
	}
	if got := f.contentOf("/b"); got != "CCCC" {
		t.Fatalf("/b = %q: the concurrent replacement was damaged (want CCCC)", got)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "BBBBBBBB" {
		t.Fatalf("B's retained alias = %q, want BBBBBBBB", got)
	}
	// A write through the displaced descriptor targets B's alias, never C.
	if err := ehB.Write(context.Background(), 0, []byte("ZZZZ")); err != nil {
		t.Fatal(err)
	}
	if err := ehB.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf("/b"); got != "CCCC" {
		t.Fatalf("/b = %q after B's write, want CCCC untouched", got)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "ZZZZBBBB" {
		t.Fatalf("B's alias = %q after its write, want ZZZZBBBB", got)
	}
	if got := readFront(t, ehB, 16); got != "ZZZZBBBB" {
		t.Fatalf("B reads %q, want ZZZZBBBB", got)
	}
	// Both original objects survive; B's close drops only its alias.
	if err := ehB.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf("/b"); got != "CCCC" {
		t.Fatalf("/b = %q after B's close, want CCCC", got)
	}
}

// Round-13 P1: reopening the destination while the rename is pending must
// not drag the retained inode's mapping back onto the replaceable name.
// The later open resolves the name as its own incarnation; after the
// rename succeeds, a write/fsync through B's original descriptor still
// targets B's retained alias, never the source now at /b.
func TestOpenDuringPendingRenameKeepsRetainedMapping(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	f.seedExtent(t, "/b", "BBBBBBBB")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	ehA := claimOpen(t, h, "/a")
	ehB := claimOpen(t, h, "/b")

	blocked, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- renameWith(h, "/a", "/b", func(ctx context.Context, op *fs.Op) (fs.Result, error) {
			close(blocked)
			<-release
			f.rename(t, op.Path, op.Path2)
			return fs.Result{}, nil
		})
	}()
	select {
	case <-blocked:
	case <-time.After(10 * time.Second):
		t.Fatal("rename never reached the chain")
	}
	sentinel := ehB.path
	// Another Open of /b while the rename is pending: it must get its own
	// incarnation, not B's retained inode.
	ehC := claimOpen(t, h, "/b")
	if ehC.ino == ehB.ino {
		t.Fatal("an open of the pending destination reused the retained inode")
	}
	if ehB.path != sentinel {
		t.Fatalf("B's mapping moved to %q during the pending rename, want its alias", ehB.path)
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("rename: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("rename did not finish")
	}
	renameEvent(h, "/a", "/b")

	b.mu.Lock()
	pathB := b.paths[ehB.ino]
	b.mu.Unlock()
	if pathB != b.fullPath(sentinel) {
		t.Fatalf("B's mapping = %q after the rename, want its alias %q", pathB, b.fullPath(sentinel))
	}
	if err := ehB.Write(context.Background(), 0, []byte("ZZZZ")); err != nil {
		t.Fatal(err)
	}
	if err := ehB.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf("/b"); got != "AAAA" {
		t.Fatalf("/b = %q after B's write, want the source's AAAA untouched", got)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "ZZZZBBBB" {
		t.Fatalf("B's alias = %q after its write, want ZZZZBBBB", got)
	}
	if got := readFront(t, ehA, 16); got != "AAAA" {
		t.Fatalf("A reads %q, want AAAA", got)
	}
}

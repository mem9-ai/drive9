package extent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// renameWith drives OpRename through the plane with an arbitrary chain
// continuation.
func renameWith(h *Handler, from, to string, next fs.Next) error {
	_, err := h.Do(context.Background(), &fs.Op{Kind: fs.OpRename, Path: from, Path2: to, Caller: fs.Caller{Pid: 1}}, next)
	return err
}

func sentinelsLeft(b *Bridge) (registered, deferred int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.sentinels), len(b.dropDeferred)
}

// Round-11 P1 (retention): the displaced destination must survive its own
// last close while the source rename is still pending. B's last descriptor
// closes while the chain is blocked; the rename then fails without moving
// the source; B must be back at /b with its contents — nothing deleted.
func TestRenameOverOpenTargetSurvivesLastCloseDuringRename(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	f.seedExtent(t, "/b", "BBBBBBBB")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	claimOpen(t, h, "/a")
	ehB := claimOpen(t, h, "/b")

	blocked, release := make(chan struct{}), make(chan struct{})
	boom := errors.New("server failure")
	done := make(chan error, 1)
	go func() {
		done <- renameWith(h, "/a", "/b", func(ctx context.Context, op *fs.Op) (fs.Result, error) {
			close(blocked)
			<-release
			return fs.Result{}, boom // nothing moved
		})
	}()
	select {
	case <-blocked:
	case <-time.After(10 * time.Second):
		t.Fatal("rename never reached the chain")
	}
	sentinel := ehB.path
	if sentinel == "/b" {
		t.Fatal("displaced target was not set aside before the chain ran")
	}
	// B's last close while the rename is pending: the drop must be
	// deferred, not executed.
	if err := ehB.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "BBBBBBBB" {
		t.Fatalf("retained object deleted by last close during the pending rename: %q", got)
	}
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, boom) {
			t.Fatalf("rename error = %v, want the chain's", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("rename did not finish")
	}
	if got := f.contentOf("/b"); got != "BBBBBBBB" {
		t.Fatalf("/b = %q after the failed rename, want the original B", got)
	}
	if got := f.contentOf("/a"); got != "AAAA" {
		t.Fatalf("/a = %q, want AAAA", got)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "" {
		t.Fatalf("sentinel still holds %q after the restore", got)
	}
	f.mu.Lock()
	deletes := append([]string(nil), f.deletes...)
	f.mu.Unlock()
	for _, d := range deletes {
		if d == b.fullPath(sentinel) {
			t.Fatal("the retained object was DELETEd while the rename was unresolved")
		}
	}
	if reg, def := sentinelsLeft(b); reg != 0 || def != 0 {
		t.Fatalf("registrations=%d deferred=%d left behind", reg, def)
	}
}

// Round-11 P1 (rollback): an error from the chain is not proof the rename
// did not commit. Here the server applied mv /a /b and the response was
// lost; rollback must recognize the source at /b by identity and leave it
// — never move B back over it — while following the source's descriptors.
func TestRenameOverOpenTargetAppliedThenErrorKeepsSource(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	f.seedExtent(t, "/b", "BBBBBBBB")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	ehA := claimOpen(t, h, "/a")
	ehB := claimOpen(t, h, "/b")

	lost := errors.New("response lost")
	err := renameWith(h, "/a", "/b", func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		f.rename(t, op.Path, op.Path2) // committed server-side...
		return fs.Result{}, lost       // ...but the client never heard
	})
	if !errors.Is(err, lost) {
		t.Fatalf("rename error = %v, want the chain's", err)
	}
	sentinel := ehB.path
	if got := f.contentOf("/b"); got != "AAAA" {
		t.Fatalf("/b = %q: rollback overwrote the moved source (want AAAA)", got)
	}
	if got := f.contentOf("/a"); got != "" {
		t.Fatalf("/a = %q, the source should have moved", got)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "BBBBBBBB" {
		t.Fatalf("displaced object = %q, want BBBBBBBB", got)
	}
	// The source's descriptors follow where its bytes are.
	b.mu.Lock()
	pathA := b.paths[ehA.ino]
	b.mu.Unlock()
	if ehA.path != "/b" || pathA != "/b" {
		t.Fatalf("A not followed to /b: handle %q mapping %q", ehA.path, pathA)
	}
	if err := ehA.Write(context.Background(), 0, []byte("ZZZZ")); err != nil {
		t.Fatal(err)
	}
	if err := ehA.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf("/b"); got != "ZZZZ" {
		t.Fatalf("/b = %q after A's write, want ZZZZ", got)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "BBBBBBBB" {
		t.Fatalf("displaced object = %q after A's write, want BBBBBBBB", got)
	}
	// Ordinary rename-over lifecycle: B's last close drops its sentinel.
	if err := ehB.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "" {
		t.Fatalf("sentinel survived B's last close: %q", got)
	}
}

// Round-11 P1 (rollback): a different file at the destination — created
// by another client while the rename was failing — is never overwritten;
// the displaced object is stranded as a hidden name that no close deletes.
func TestRenameOverOpenTargetForeignDestinationNeverOverwritten(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	f.seedExtent(t, "/b", "BBBBBBBB")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	claimOpen(t, h, "/a")
	ehB := claimOpen(t, h, "/b")

	boom := errors.New("server failure")
	err := renameWith(h, "/a", "/b", func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		f.seedExtent(t, "/b", "CCCC") // another client took the vacated name
		return fs.Result{}, boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("rename error = %v, want the chain's", err)
	}
	sentinel := ehB.path
	if got := f.contentOf("/b"); got != "CCCC" {
		t.Fatalf("/b = %q: rollback overwrote another client's file (want CCCC)", got)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "BBBBBBBB" {
		t.Fatalf("displaced object = %q, want BBBBBBBB", got)
	}
	if reg, _ := sentinelsLeft(b); reg != 0 {
		t.Fatalf("stranded sentinel still registered (%d): its last close would delete it", reg)
	}
	if err := ehB.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "BBBBBBBB" {
		t.Fatalf("stranded object = %q after B's close, want it preserved", got)
	}
}

// Round-11 P1 (rollback): when the destination cannot be probed at all the
// outcome is unknown — nothing is overwritten and the displaced object is
// stranded, recoverable.
func TestRenameOverOpenTargetUnknownOutcomeStrands(t *testing.T) {
	f := newFakeV2(t)
	f.seedExtent(t, "/a", "AAAA")
	f.seedExtent(t, "/b", "BBBBBBBB")
	h := newTestHandler(t, f, 1<<20)
	b := h.opts.Engine.Bridge
	claimOpen(t, h, "/a")
	ehB := claimOpen(t, h, "/b")

	lost := errors.New("response lost")
	err := renameWith(h, "/a", "/b", func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		f.rename(t, op.Path, op.Path2)
		f.setStatFail("/b", true) // the probes cannot reach the destination
		return fs.Result{}, lost
	})
	if !errors.Is(err, lost) {
		t.Fatalf("rename error = %v, want the chain's", err)
	}
	f.setStatFail("/b", false)
	sentinel := ehB.path
	if got := f.contentOf("/b"); got != "AAAA" {
		t.Fatalf("/b = %q: rollback touched an unestablished destination (want AAAA)", got)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "BBBBBBBB" {
		t.Fatalf("displaced object = %q, want BBBBBBBB", got)
	}
	if reg, _ := sentinelsLeft(b); reg != 0 {
		t.Fatalf("stranded sentinel still registered (%d)", reg)
	}
	if err := ehB.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.contentOf(b.fullPath(sentinel)); got != "BBBBBBBB" {
		t.Fatalf("stranded object = %q after B's close, want it preserved", got)
	}
}

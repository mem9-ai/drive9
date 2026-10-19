package layer

// qiffang sixteenth round: in-flight landings defer their events instead
// of self-conflicting; opens join the registry protocol (post-CAS
// promotions cannot overwrite a newer claim); FD truncate reaches the
// named descriptor; a late open during the whiteout window is
// anonymized by the re-sweep.

import (
	"context"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// The server publishes the entry/event BEFORE the commit's HTTP reply
// returns: a poll in that window must defer the path (no false
// conflicted, cursor not consumed) — the next Write succeeds after the
// reply lands.
func TestPortInFlightLandingDefersItsEvent(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("seed"))
	b.HoldLayerEntryResponses(300 * time.Millisecond)
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	l, ch := mStack(t, ts)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if err := h.Write(ctx, 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	syncErr := make(chan error, 1)
	go func() { syncErr <- h.Sync(ctx) }()
	// The upsert is applied and its event visible, the reply still held.
	time.Sleep(120 * time.Millisecond)
	l.refreshEventsForTest(ctx)
	select {
	case err := <-syncErr:
		t.Fatalf("sync returned before the held response: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if err := <-syncErr; err != nil {
		t.Fatalf("sync: %v", err)
	}
	// The deferred event re-examines after the identity landed: no
	// conflict, the newer write succeeds.
	if err := h.Write(ctx, 0, []byte("BBBB")); err != nil {
		t.Fatalf("write after in-flight self-event: %v", err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	rd, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/f", Offset: 0, Limit: 8})
	if err != nil || string(rd.Data) != "BBBB" {
		t.Fatalf("final overlay = %q err=%v, want BBBB", rd.Data, err)
	}
}

// An open arriving between a release's successful CAS and its survivor
// Store is the newer owner: the promotion (and the open itself) run
// under regMu, so the open's registration is never overwritten — after
// the dust settles the NEWEST claim owns the slot.
func TestPortPostCASOpenIsNotOverwritten(t *testing.T) {
	l, ch, _ := pStack(t)
	ctx := context.Background()

	open := func() *layerHandle {
		res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
		if err != nil {
			t.Fatal(err)
		}
		return res.Handle.(*layerHandle)
	}
	h1, h2 := open(), open()
	// Park h2's release inside the survivor scan (it blocks on h1.mu):
	// an open arriving NOW must WAIT for the registry protocol instead of
	// registering underneath the parked promotion.
	h1.mu.Lock()
	done := make(chan struct{})
	go func() { defer close(done); l.untrackHandle(h2) }()
	time.Sleep(30 * time.Millisecond)
	openDone := make(chan *layerHandle, 1)
	go func() { openDone <- open() }()
	select {
	case <-openDone:
		h1.mu.Unlock()
		t.Fatal("open completed while a release promotion was parked — opens do not serialize with the registry protocol")
	case <-time.After(80 * time.Millisecond):
	}
	h1.mu.Unlock()
	<-done
	h3 := <-openDone
	if cur, ok := l.live.Load("/f"); !ok || cur != h3 {
		t.Fatalf("live[/f] = %v ok=%v, want the post-park open (newest claim)", cur, ok)
	}
}

// FD truncate (FATTR_FH) reaches the NAMED descriptor, not the per-path
// newest claim.
func TestPortFDTruncateReachesNamedDescriptor(t *testing.T) {
	_, ch, _ := pStack(t)
	ctx := context.Background()

	open := func() fs.Handle {
		res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
		if err != nil {
			t.Fatal(err)
		}
		return res.Handle
	}
	h1, h2 := open(), open()
	if err := h1.Write(ctx, 0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/f", Size: 2, Handle: h1}); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 5)
	if n, err := h1.Read(ctx, 0, got); err != nil || string(got[:n]) != "he" {
		t.Fatalf("named descriptor read = %q (%d) err=%v, want he", got[:n], n, err)
	}
	_ = h2
}

// A writable open landing between the unlink sweep and the whiteout is
// anonymized by the post-whiteout re-sweep: its sync cannot resurrect
// the deleted name.
func TestPortUnlinkCatchesLateOpen(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("hello"))
	b.DelayLayerEntriesForTest(200 * time.Millisecond)
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	_, ch := mStack(t, ts)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h1 := res.Handle
	unlinkErr := make(chan error, 1)
	go func() {
		_, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/f"})
		unlinkErr <- err
	}()
	time.Sleep(60 * time.Millisecond)
	// A second open sneaks inside the whiteout window.
	late, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatalf("late open inside the whiteout window: %v", err)
	}
	if err := <-unlinkErr; err != nil {
		t.Fatalf("unlink: %v", err)
	}
	if err := late.Handle.Sync(ctx); err != nil {
		t.Fatalf("late handle sync: %v", err)
	}
	if err := late.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/f"}); err == nil {
		t.Fatal("deleted name resurrected by the late open's sync")
	}
}

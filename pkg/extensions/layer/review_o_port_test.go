package layer

// qiffang twelfth round (post-#984): retarget detaches the OLD registry
// slot by name; the checkout sweep snapshots and applies under one lock
// hold so a mid-sweep rename cannot write the old name's state into a
// moved fd.

import (
	"context"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// After a rename retarget, the OLD path's registry slot must not keep
// pointing at the moved fd: a SetAttr/Unlink on the old name must not
// reach it (the old slot is detached by name — h.path already reads the
// new name when the detach runs).
func TestPortRenameRetargetLeavesNoOldPathSlot(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/old", []byte("seed"))
	b.Seed("/new", []byte("other"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	l, ch := mStack(t, ts)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/old", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle.(*layerHandle)
	h.retarget("/new")

	// The old slot no longer resolves to the moved fd: live[/old] is
	// empty (no other descriptor holds the name).
	if cur, ok := l.live.Load("/old"); ok && cur == h {
		t.Fatal("live[/old] still maps the retargeted fd after rename")
	}
	if cur, ok := l.live.Load("/new"); !ok || cur != h {
		t.Fatal("live[/new] does not map the moved fd")
	}
	// A second open on the old name becomes its own slot owner — a
	// later release of THAT descriptor must not disturb the moved fd's
	// registration either.
	res2, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/old", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h2 := res2.Handle.(*layerHandle)
	if cur, ok := l.live.Load("/old"); !ok || cur != h2 {
		t.Fatal("second open did not take over live[/old]")
	}
	if cur, ok := l.live.Load("/new"); !ok || cur != h {
		t.Fatal("moved fd lost its new-name slot")
	}
}

// A rename racing the checkout sweep must never write the OLD name's
// whiteout/content into the moved fd: the sweep's snapshot and apply are
// one lock hold, so a fd renamed before the sweep locks it is filtered by
// its current path, and a fd the sweep already processed then renames
// carries only its own verdict. Run under -race the same interleavings
// also pin the unlocked-field-read class (path/released under h.mu).
func TestPortCheckoutSweepSurvivesConcurrentRenames(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/old", []byte("seed"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	l, ch := mStack(t, ts)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/old", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle.(*layerHandle)
	if err := h.Write(ctx, 0, []byte("dirty")); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		flip := true
		for {
			select {
			case <-stop:
				return
			default:
			}
			if flip {
				h.retarget("/new")
			} else {
				h.retarget("/old")
			}
			flip = !flip
		}
	}()
	// Sweep with real layer events for /old (upserts and whiteouts)
	// landing repeatedly while the rename flips the fd's path.
	for i := 0; i < 25; i++ {
		if i%2 == 0 {
			postLayerEntry(t, ts, "L1", "/old", "upsert", "world")
		} else {
			postLayerEntry(t, ts, "L1", "/old", "delete", "")
		}
		l.refreshEventsForTest(ctx)
	}
	close(stop)
	wg.Wait()

	// After quiescence the fd is on SOME path; whatever state it carries
	// was decided against the path it held at decision time (the sweep
	// never consults a stale snapshot). Assert structural invariants: the
	// handle is still registered exactly under its current path.
	h.mu.Lock()
	path, unlinked := h.path, h.unlinked
	h.mu.Unlock()
	if unlinked {
		// A whiteout verdict requires the sweep to have seen the fd on
		// /old; that is legal — but then the fd must not be registered
		// as /new's live slot with unlinked content pending.
		t.Logf("fd ended unlinked (sweep verdict while named /old), path=%s", path)
	}
	if cur, ok := l.live.Load(path); !ok || cur != h {
		t.Fatalf("fd registered under its current path %s: ok=%v", path, ok)
	}
}

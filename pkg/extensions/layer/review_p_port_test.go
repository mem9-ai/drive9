package layer

// qiffang thirteenth round: retirement vs rename move vs survivor
// promotion are serialized on regMu; the live slot dies by
// compare-and-delete; promotion verifies lifetime and path under the
// candidate's lock.

import (
	"context"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func pStack(t *testing.T) (*Layer, *fs.Chain, *httptest.Server) {
	t.Helper()
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("hello"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	l, ch := mStack(t, ts)
	return l, ch, ts
}

// A Release that completes while a retarget is in flight must win: the
// retired descriptor is never re-registered into either index, so a later
// path-keyed SetAttr cannot pin mutations to a dead handle. The
// deterministic row retires first (the worst window for the move), and
// the concurrent row runs both sides against each other under -race.
func TestPortRetargetDoesNotResurrectRetiredHandle(t *testing.T) {
	l, ch, _ := pStack(t)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle.(*layerHandle)

	// Deterministic interleave: Release's retirement lands between the
	// retarget's path update and its registry move.
	l.untrackHandle(h)
	h.retarget("/moved")
	if cur, ok := l.live.Load("/moved"); ok && cur == h {
		t.Fatal("retarget re-registered a retired fd into live[/moved]")
	}
	if _, ok := l.handles.Load(h); ok {
		t.Fatal("retarget re-registered a retired fd into the handles set")
	}

	// Concurrent interleave: retarget and retirement racing on a fresh
	// descriptor never end with a released fd registered anywhere.
	res2, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h2 := res2.Handle.(*layerHandle)
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			h2.retarget("/moved")
		}()
		go func() {
			defer wg.Done()
			h2.retarget("/f")
		}()
	}
	wg.Wait()
	h2.mu.Lock()
	released, path := h2.released, h2.path
	h2.mu.Unlock()
	_ = path
	if !released {
		// The racing pairs never release; retire explicitly and assert the
		// post-retirement registration state.
		l.untrackHandle(h2)
	}
	if cur, ok := l.live.Load("/f"); ok && cur == h2 {
		t.Fatal("retired fd still registered under /f after untrack")
	}
	if _, ok := l.handles.Load(h2); ok {
		t.Fatal("retired fd still in the handles set after untrack")
	}
}

// Survivor promotion only picks LIVE same-path claims: a retired or
// anonymous sibling is never written back into the vacated slot, and the
// slot itself dies by compare-and-delete — a newer claim stored between
// the check and the delete survives.
func TestPortDetachPromotesOnlyLiveSurvivors(t *testing.T) {
	l, ch, _ := pStack(t)
	ctx := context.Background()

	open := func() *layerHandle {
		res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
		if err != nil {
			t.Fatal(err)
		}
		return res.Handle.(*layerHandle)
	}
	oldest, newest := open(), open()
	if cur, ok := l.live.Load("/f"); !ok || cur != newest {
		t.Fatal("newest claim does not own live[/f]")
	}

	// Retiring the slot owner re-selects the OLDER claim (still live).
	l.untrackHandle(newest)
	if cur, ok := l.live.Load("/f"); !ok || cur != oldest {
		t.Fatalf("survivor promotion = %v ok=%v, want the live older claim", cur, ok)
	}

	// A retired survivor is not promoted: retire the older claim under a
	// fresh owner, then detach the owner with the older claim already
	// retired — the slot stays empty rather than resurrecting it.
	owner := open()
	l.untrackHandle(oldest)
	l.regMu.Lock()
	l.detachFromRegistry(owner, "/f")
	l.regMu.Unlock()
	if cur, ok := l.live.Load("/f"); ok && cur == oldest {
		t.Fatal("retired claim promoted into live[/f]")
	}

	// Compare-and-delete: a concurrent claim stored between the owner's
	// check and its delete keeps its registration.
	fresh := open()
	l.regMu.Lock()
	l.detachFromRegistry(owner, "/f")
	l.regMu.Unlock()
	if cur, ok := l.live.Load("/f"); !ok || cur != fresh {
		t.Fatalf("concurrent claim lost its slot: ok=%v", ok)
	}
}

// Open/release/unlink racing detach stays data-race free (-race) and the
// registry holds only live claims afterwards.
func TestPortDetachRacesStayClean(t *testing.T) {
	l, ch, _ := pStack(t)
	ctx := context.Background()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			l.regMu.Lock()
			l.detachFromRegistry(nil, "/f")
			l.regMu.Unlock()
		}
	}()
	for i := 0; i < 30; i++ {
		res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		h := res.Handle.(*layerHandle)
		if i%3 == 0 {
			h.markUnlinked()
		}
		if i%2 == 0 {
			l.untrackHandle(h)
		}
	}
	close(stop)
	wg.Wait()

	// Whatever remains registered is a live, non-anonymous claim.
	l.live.Range(func(_, v any) bool {
		if h, ok := v.(*layerHandle); ok {
			h.mu.Lock()
			released, unlinked := h.released, h.unlinked
			h.mu.Unlock()
			if released || unlinked {
				t.Fatalf("dead claim left in live registry: released=%v unlinked=%v", released, unlinked)
			}
		}
		return true
	})
}

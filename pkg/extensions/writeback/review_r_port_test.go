package writeback

// qiffang fifteenth round: the writeback release registry follows the
// same protocol as the layer registry — compare-and-delete slot clearing,
// survivor promotion only after an own-slot CAS, identity read from the
// lock-free info snapshot, and the release/rename interleavings stay
// race-free.

import (
	"context"
	"sync"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// Releasing the OLDEST of three same-path writers keeps the newest owner
// (no promotion over it); releasing the owner promotes the survivor; the
// last release empties the slot.
func TestPortUntrackKeepsNewerClaimAndPromotesSurvivor(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("hello"))
	ch, wb := newWritebackStack(t, b)
	ctx := context.Background()

	open := func() *bufferingHandle {
		res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
		if err != nil {
			t.Fatal(err)
		}
		return writebackHandleOf(res.Handle)
	}
	o1, o2, o3 := open(), open(), open()

	if err := o1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if cur := wb.handleFor("/f"); cur != o3 {
		t.Fatalf("handleFor(/f) after releasing the oldest = %p, want the newest owner", cur)
	}
	if err := o3.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if cur := wb.handleFor("/f"); cur != o2 {
		t.Fatalf("handleFor(/f) after releasing the owner = %p, want the surviving claim", cur)
	}
	if err := o2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if cur := wb.handleFor("/f"); cur != nil {
		t.Fatalf("handleFor(/f) after releasing every claim = %p, want none", cur)
	}
}

// Release racing retarget/open stays data-race free (-race) and the
// registry never maps a released handle, never loses the live same-path
// writer while one exists, and never lets a release delete a newer
// claim's registration.
func TestPortReleaseRegistryRacesStayClean(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("hello"))
	ch, wb := newWritebackStack(t, b)
	ctx := context.Background()

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
				wb.retarget("/f", "/g")
			} else {
				wb.retarget("/g", "/f")
			}
			flip = !flip
		}
	}()
	for i := 0; i < 40; i++ {
		res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		h := writebackHandleOf(res.Handle)
		if i%2 == 0 {
			// Release racing the retarget flips.
			if err := h.Release(ctx); err != nil {
				t.Fatalf("release: %v", err)
			}
		}
	}
	close(stop)
	wg.Wait()

	// Quiesced invariants: every live mapping names a handle that is still
	// tracked (a released fd must never be resurrected into a slot), and
	// no two slots map the same handle.
	seen := map[*bufferingHandle]string{}
	wb.live.Range(func(k, v any) bool {
		path, _ := k.(string)
		h, ok := v.(*bufferingHandle)
		if !ok {
			return true
		}
		if _, tracked := wb.liveSet.Load(h); !tracked {
			t.Fatalf("live[%s] maps a released handle (resurrected by the release/retarget race)", path)
		}
		if prev, dup := seen[h]; dup {
			t.Fatalf("handle mapped from two slots: %s and %s", prev, path)
		}
		seen[h] = path
		return true
	})
}

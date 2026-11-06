package extent

import (
	"context"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// A write through the extent plane must announce itself on the bus.
//
// The read cache is an OUTER plane: it drops a path's cached image and moves
// open readers onto the new revision only when it hears EventLanded/EventWrite.
// The extent plane emitted neither, so a file written through the engine kept
// serving its pre-write image to the very descriptor that wrote it — SQLite
// re-read page 1 with a stale schema cookie, reported SQLITE_SCHEMA, and lost
// the table its previous statement had created.
func TestExtentWriteEmitsLandedEvent(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: engineCacheDir(t)})
	bus := fs.NewEventBus()
	h := New(Options{Engine: eng, Threshold: 0, Events: bus})

	var mu sync.Mutex
	var landed []fs.Event
	bus.On(fs.EventLanded, func(e fs.Event) {
		mu.Lock()
		landed = append(landed, e)
		mu.Unlock()
	})

	res, err, _ := dispatchCreate(context.Background(), h, "/d/a.db", syscall.O_RDWR|syscall.O_CREAT)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(context.Background(), 0, make([]byte, 8192)); err != nil {
		t.Fatalf("write: %v", err)
	}

	mu.Lock()
	got := append([]fs.Event(nil), landed...)
	mu.Unlock()
	if len(got) == 0 {
		t.Fatal("a write through the extent plane emitted no EventLanded: every outer cache keeps the pre-write image")
	}
	if got[0].Path != "/d/a.db" {
		t.Fatalf("landed event path = %q, want /d/a.db", got[0].Path)
	}
	if !got[0].HasSize || got[0].Size < 8192 {
		t.Fatalf("landed event size = %d (has=%v), want at least the 8192 bytes just written",
			got[0].Size, got[0].HasSize)
	}
}

// Every write announces, not only the first: a second write to the same handle
// is exactly the case a stale cache survives.
func TestEveryExtentWriteAnnounces(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	eng := NewEngine(EngineConfig{Client: client.New(f.URL, ""), RemoteRoot: "/", CacheDir: engineCacheDir(t)})
	bus := fs.NewEventBus()
	h := New(Options{Engine: eng, Threshold: 0, Events: bus})

	var count atomic.Int32
	bus.On(fs.EventLanded, func(fs.Event) { count.Add(1) })

	res, err, _ := dispatchCreate(context.Background(), h, "/d/b.db", syscall.O_RDWR|syscall.O_CREAT)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	eh := res.Handle.(*engineHandle)
	for i := 0; i < 3; i++ {
		if err := eh.Write(context.Background(), int64(i)*4096, make([]byte, 4096)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if got := count.Load(); got != 3 {
		t.Fatalf("landed events = %d, want 3 (one per write)", got)
	}
}

package sse

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/cache"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// Reads made during a gap can refill caches. Both replay and a retention-gap
// reset must expose remote writes after the stream catches up.
func TestReconnectRevalidatesGapReads(t *testing.T) {
	for _, recovery := range []string{"replay", "reset"} {
		t.Run(recovery, func(t *testing.T) {
			b := testbackend.NewBackend()
			b.Seed("/f", []byte("OLD"))
			disconnect, catchUp := make(chan struct{}), make(chan struct{})
			connected := make(chan struct{})
			var connections atomic.Int32
			var ready sync.Once
			ts := httptest.NewServer(http.HandlerFunc(func(hw http.ResponseWriter, hr *http.Request) {
				if hr.URL.Path != "/v1/events" {
					b.ServeHTTP(hw, hr)
					return
				}
				hw.Header().Set("Content-Type", "text/event-stream")
				flush := hw.(http.Flusher).Flush
				if connections.Add(1) == 1 {
					_, _ = fmt.Fprint(hw, "event: heartbeat\ndata: {\"seq\":9}\n\n")
					flush()
					ready.Do(func() { close(connected) })
					select {
					case <-disconnect:
					case <-hr.Context().Done():
					}
					return
				}
				if got := hr.URL.Query().Get("since"); got != "9" {
					t.Errorf("reconnect since=%q want 9", got)
				}
				select {
				case <-catchUp:
				case <-hr.Context().Done():
					return
				}
				if recovery == "replay" {
					_, _ = fmt.Fprint(hw, "event: file_changed\ndata: {\"seq\":10,\"path\":\"/f\",\"op\":\"write\",\"actor\":\"other\"}\n\n")
				} else {
					_, _ = fmt.Fprint(hw, "event: reset\ndata: {\"seq\":10,\"reason\":\"retention_gap\"}\n\n")
				}
				_, _ = fmt.Fprint(hw, "event: heartbeat\ndata: {\"seq\":10}\n\n")
				flush()
				<-hr.Context().Done()
			}))
			t.Cleanup(ts.Close)
			cl := client.New(ts.URL, "")
			bus := fs.NewEventBus()
			readCache, err := cache.New(cache.Options{Events: bus, Disk: cache.DiskReadCacheOptions{Dir: t.TempDir()}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = readCache.Stop(context.Background()) })
			wb, err := writeback.New(writeback.Options{Client: cl, Policy: writeback.PolicyWriteBack, Events: bus})
			if err != nil {
				t.Fatal(err)
			}
			terminal, err := core.New(cl, core.Options{})
			if err != nil {
				t.Fatal(err)
			}
			terminal.AttachEvents(bus)
			chain, err := fs.NewChain(readCache, wb, terminal)
			if err != nil {
				t.Fatal(err)
			}
			notifications := make(chan fs.EventKind, 4)
			for _, kind := range []fs.EventKind{fs.EventUnverified, fs.EventCheckout} {
				bus.On(kind, func(e fs.Event) { notifications <- e.Kind })
			}
			watcher, err := New(Options{Client: cl, Events: bus})
			if err != nil {
				t.Fatal(err)
			}
			if err := watcher.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = watcher.Stop(context.Background()) })
			select {
			case <-connected:
			case <-time.After(5 * time.Second):
				t.Fatal("SSE not connected")
			}
			res, err := chain.Dispatch(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: "/f"})
			if err != nil || res.Handle == nil {
				t.Fatalf("open: %v", err)
			}
			initialHandle := res.Handle
			t.Cleanup(func() { _ = initialHandle.Release(context.Background()) })
			read := func(want string) {
				t.Helper()
				buf := make([]byte, 3)
				n, err := res.Handle.Read(context.Background(), 0, buf)
				if err != nil || string(buf[:n]) != want {
					t.Fatalf("read=%q err=%v want %q", buf[:n], err, want)
				}
			}
			wait := func(want fs.EventKind) {
				t.Helper()
				select {
				case got := <-notifications:
					if got != want {
						t.Fatalf("event=%v want %v", got, want)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("missing invalidation")
				}
			}
			read("OLD")
			close(disconnect)
			wait(fs.EventUnverified)
			// A new open pins the still-current old revision and can refill
			// stable cache tiers even though the event stream is disconnected.
			res, err = chain.Dispatch(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: "/f"})
			if err != nil || res.Handle == nil {
				t.Fatalf("open during gap: %v", err)
			}
			gapHandle := res.Handle
			defer func() { _ = gapHandle.Release(context.Background()) }()
			read("OLD")
			if data, ok := readCache.ReadCached("/f", 0); !ok || string(data) != "OLD" {
				t.Fatalf("gap did not refill cache: %q cached=%v", data, ok)
			}
			b.Seed("/f", []byte("NEW"))
			close(catchUp)
			if recovery == "reset" {
				wait(fs.EventCheckout)
			} else {
				wait(fs.EventUnverified)
			}
			read("NEW")
		})
	}
}

func TestCurrentOnlyRevalidatesAfterDisconnect(t *testing.T) {
	w, bus, _ := newSSE(t, "", "/")
	var invalidations int
	bus.On(fs.EventUnverified, func(fs.Event) { invalidations++ })
	w.handleCurrent()
	if invalidations != 0 {
		t.Fatal("initial heartbeat invalidated caches")
	}
	w.handleDisconnect()
	w.handleCurrent()
	w.handleCurrent()
	if invalidations != 2 {
		t.Fatalf("invalidations=%d want disconnect and catch-up only", invalidations)
	}
}

package fuse

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/sse"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// Ordinary event-stream EOF must not fail an unchanged file read.
// A real reset must still reject replies from the superseded view.
func TestSSEDisconnectReadOverlap(t *testing.T) {
	for _, trigger := range []string{"connected", "clean_eof", "transport_error", "real_reset"} {
		for _, mode := range []string{"handle_data", "handle_eof", "dispatch_data", "dispatch_eof"} {
			t.Run(trigger+"/"+mode, func(t *testing.T) {
				gate, release := make(chan struct{}), make(chan struct{})
				var releaseOnce sync.Once
				t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
				core := newMemCore()
				core.paths["/f"] = &memEntry{data: []byte("OLD"), mode: 0o644}
				bus := fs.NewEventBus()
				ext := &blockReadExt{gate: gate, release: release}
				f, err := New(Options{Extensions: []fs.Handler{ext, core}, Uid: 1000, Gid: 1000, Events: bus})
				if err != nil {
					t.Fatal(err)
				}
				entry := lookupForTest(t, f, "/f")
				in := &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: entry.NodeId}, Size: 4}
				switch mode {
				case "handle_data":
					in.Fh = 7
					f.handles.put(7, &fileHandle{ino: entry.NodeId, path: "/f", h: &gatedReadHandle{gate: gate, release: release}})
				case "handle_eof":
					in.Fh, in.Offset = 7, 3
					f.handles.put(7, &fileHandle{ino: entry.NodeId, path: "/f", h: &gatedEOFHandle{gate: gate, release: release}})
				case "dispatch_eof":
					in.Offset = 3
				}
				t.Cleanup(func() { f.handles.drop(7) })
				checkout := make(chan struct{}, 1)
				onInvalidation := func(fs.Event) {
					select {
					case checkout <- struct{}{}:
					default:
					}
				}
				bus.On(fs.EventCheckout, onInvalidation)
				bus.On(fs.EventUnverified, onInvalidation)
				ready, act := make(chan struct{}), make(chan struct{})
				var readyOnce sync.Once
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = fmt.Fprint(w, "event: heartbeat\ndata: {\"seq\":9}\n\n")
					w.(http.Flusher).Flush()
					readyOnce.Do(func() { close(ready) })
					select {
					case <-act:
					case <-r.Context().Done():
						return
					}
					if trigger == "clean_eof" {
						return
					}
					if trigger == "transport_error" {
						conn, _, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
							return
						}
						_ = conn.Close() // incomplete chunked response
						return
					}
					_, _ = fmt.Fprint(w, "event: reset\ndata: {\"seq\":10,\"reason\":\"structural_change\",\"actor\":\"other\"}\n\n")
					w.(http.Flusher).Flush()
					<-r.Context().Done()
				}))
				t.Cleanup(ts.Close)
				watcher, err := sse.New(sse.Options{Client: client.New(ts.URL, ""), Events: bus})
				if err != nil {
					t.Fatal(err)
				}
				if err := watcher.Start(context.Background()); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = watcher.Stop(context.Background()) })
				wait := func(ch <-chan struct{}) {
					t.Helper()
					select {
					case <-ch:
					case <-time.After(5 * time.Second):
						t.Fatal("coordination timeout")
					}
				}
				wait(ready)
				before := f.viewGen.Load()
				type reply struct {
					result gofuse.ReadResult
					status gofuse.Status
				}
				done := make(chan reply, 1)
				go func() { result, status := f.Read(nil, in, make([]byte, 4)); done <- reply{result, status} }()
				wait(gate)
				if trigger != "connected" {
					close(act)
					wait(checkout)
				}
				releaseOnce.Do(func() { close(release) })
				var got reply
				select {
				case got = <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("read timeout")
				}
				want := gofuse.OK
				if trigger == "real_reset" {
					want = gofuse.EAGAIN
				}
				if got.status != want {
					t.Fatalf("status=%v want %v", got.status, want)
				}
				if trigger != "real_reset" {
					if got.result == nil {
						t.Fatal("successful read has nil result")
					}
					data, status := got.result.Bytes(make([]byte, 4))
					got.result.Done()
					wantData := "OLD"
					if mode == "handle_eof" || mode == "dispatch_eof" {
						wantData = ""
					}
					if status != gofuse.OK || string(data) != wantData {
						t.Fatalf("data=%q status=%v", data, status)
					}
				}
				t.Logf("view=%d->%d status=%v unchanged file, successful underlying data/EOF", before, f.viewGen.Load(), got.status)
			})
		}
	}
}

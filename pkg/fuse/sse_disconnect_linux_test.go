package fuse

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/cache"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/sse"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// Read through the kernel while a real SSE response ends, with the normal
// cache/writeback/core data path. Applications must receive all unchanged bytes.
func TestSSEDisconnectReadKernel(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		if os.Getenv("DRIVE9_REQUIRE_KERNEL_FUSE") == "1" {
			t.Fatal(err)
		}
		t.Skipf("Linux FUSE unavailable: %v", err)
	}
	payload := bytes.Repeat([]byte("unchanged\x00"), 6554)
	backend := testbackend.NewBackend()
	backend.Seed("/f", payload)
	readStarted, releaseRead := make(chan struct{}), make(chan struct{})
	disconnect, connected := make(chan struct{}), make(chan struct{})
	var readOnce, releaseOnce, connectedOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseRead) }) }
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/events" {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprint(w, "event: heartbeat\ndata: {\"seq\":1}\n\n")
			w.(http.Flusher).Flush()
			connectedOnce.Do(func() { close(connected) })
			select {
			case <-disconnect:
			case <-r.Context().Done():
			}
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/v1/fs/f" {
			readOnce.Do(func() {
				close(readStarted)
				select {
				case <-releaseRead:
				case <-r.Context().Done():
				}
			})
		}
		backend.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	bus := fs.NewEventBus()
	readCache, err := cache.New(cache.Options{Events: bus})
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
	f, err := New(Options{Extensions: []fs.Handler{readCache, wb, terminal}, Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	if dc, ok := f.Ctx.DirCache.(core.NamespaceCache); ok {
		terminal.Attach(dc)
	}
	invalidated := make(chan struct{}, 1)
	bus.On(fs.EventUnverified, func(fs.Event) {
		select {
		case invalidated <- struct{}{}:
		default:
		}
	})
	mp := t.TempDir()
	server, err := gofuse.NewServer(f, mp, &gofuse.MountOptions{FsName: "drive9", Name: "drive9", MaxWrite: 1 << 20, DirectMountStrict: os.Geteuid() == 0})
	if err != nil {
		if os.Getenv("DRIVE9_REQUIRE_KERNEL_FUSE") == "1" {
			t.Fatal(err)
		}
		t.Skipf("cannot mount Linux FUSE: %v", err)
	}
	f.Init2(context.Background())
	go server.Serve()
	t.Cleanup(func() {
		if err := server.Unmount(); err != nil {
			t.Error(err)
		}
	})
	if err := server.WaitMount(); err != nil {
		t.Fatal(err)
	}
	watcher, err := sse.New(sse.Options{Client: cl, Events: bus})
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
		case <-time.After(10 * time.Second):
			t.Fatal("coordination timeout")
		}
	}
	wait(connected)
	fd, err := os.Open(filepath.Join(mp, "f"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fd.Close() })
	// Release a parked request before cleanup closes the fd or unmounts.
	t.Cleanup(release)
	type reply struct {
		data []byte
		err  error
	}
	done := make(chan reply, 1)
	go func() { data, err := io.ReadAll(fd); done <- reply{data, err} }()
	wait(readStarted)
	close(disconnect)
	wait(invalidated)
	release()
	select {
	case got := <-done:
		if got.err != nil || !bytes.Equal(got.data, payload) {
			t.Fatalf("read bytes=%d err=%v want %d unchanged bytes", len(got.data), got.err, len(payload))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("kernel read did not complete")
	}
}

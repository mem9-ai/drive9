package sse

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func TestStopDoesNotClaimSuccessWhileEventCallbackIsRunning(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "canceled", true: "deadline"}[deadline], func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = fmt.Fprint(w, "event: file_changed\ndata: {\"seq\":1,\"path\":\"/a\",\"op\":\"write\",\"actor\":\"other\"}\n\n")
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			}))
			t.Cleanup(server.Close)
			bus := fs.NewEventBus()
			bus.On(fs.EventInvalidated, func(fs.Event) { close(entered); <-release })
			watcher, err := New(Options{Client: client.New(server.URL, ""), Events: bus})
			if err != nil {
				t.Fatal(err)
			}
			if err := watcher.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { unblock(); _ = watcher.Stop(context.Background()) })
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("event callback did not start")
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			want := context.Canceled
			if deadline {
				ctx, cancel = context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
				defer cancel()
				want = context.DeadlineExceeded
			}
			if err := watcher.Stop(ctx); !errors.Is(err, want) {
				t.Fatalf("unfinished Stop: %v", err)
			}
			select {
			case <-watcher.doneCh:
				t.Fatal("callback unexpectedly finished")
			default:
			}
			unblock()
			if err := watcher.Stop(t.Context()); err != nil {
				t.Fatal(err)
			}
			// A known completed stop stays successful even if its retry is canceled.
			if err := watcher.Stop(ctx); err != nil {
				t.Fatalf("completed Stop retry: %v", err)
			}
		})
	}
}

package extent

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/client"
)

func TestHandlerLifecycleDrainAndStop(t *testing.T) {
	var failFlush atomic.Bool
	failFlush.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failFlush.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"revision":2}`))
	}))
	defer srv.Close()

	cl := client.New(srv.URL, "token")
	eng := NewEngine(EngineConfig{
		Client:     cl,
		RemoteRoot: "/",
		CacheSize:  0,
	})
	h := New(Options{Engine: eng, Threshold: 1})

	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if reason, n := h.ReexecRefusal(); reason != "" || n != 0 {
		t.Fatalf("idle ReexecRefusal = (%q, %d), want empty", reason, n)
	}
	if err := h.Drain(context.Background()); err != nil {
		t.Fatalf("idle Drain: %v", err)
	}

	ino := meta.Ino(42)
	cause := errors.New("transient commit error")
	eng.Bridge.mu.Lock()
	eng.Bridge.paths[ino] = "/extent.bin"
	eng.Bridge.commitErr[ino] = cause
	eng.Bridge.mu.Unlock()
	eng.Bridge.kickPendingFlush()

	if reason, n := h.ReexecRefusal(); reason != "extent_pending" || n != 1 {
		t.Fatalf("pending ReexecRefusal = (%q, %d), want (extent_pending, 1)", reason, n)
	}
	if err := h.Drain(context.Background()); err == nil {
		t.Fatal("Drain succeeded with unresolved commitErr")
	}

	if err := h.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	eng.Bridge.mu.Lock()
	running := eng.Bridge.retryRunning
	eng.Bridge.mu.Unlock()
	if running {
		t.Fatal("background retry loop still running after Stop")
	}
}

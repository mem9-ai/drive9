package git

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
)

func TestGitStopCancelsDiscoveryAndRestore(t *testing.T) {
	for _, restore := range []bool{false, true} {
		name := "discovery"
		if restore {
			name = "restore"
		}
		t.Run(name, func(t *testing.T) {
			b := testbackend.NewBackend()
			b.SeedGitWorkspace("ws-1", "/repo")
			entered, canceled := make(chan struct{}), make(chan struct{})
			var once sync.Once
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				blocked := r.URL.Path == "/v1/git-workspaces"
				if restore {
					blocked = strings.HasSuffix(r.URL.Path, "/git-state")
				}
				if blocked {
					once.Do(func() {
						close(entered)
						<-r.Context().Done()
						close(canceled)
					})
					return
				}
				b.ServeHTTP(w, r)
			}))
			defer ts.Close()
			g, err := New(Options{Client: client.New(ts.URL, ""), LocalRoot: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = g.Stop(context.Background()) }()
			g.armed = true
			if err := g.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("background request did not start")
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if err := g.Stop(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case <-canceled:
			case <-ctx.Done():
				t.Fatal("old request was not canceled")
			}
			g.backgroundMu.Lock()
			n := len(g.background)
			g.backgroundMu.Unlock()
			if n != 0 {
				t.Fatalf("Stop left %d directory owners", n)
			}
			if err := g.Start(t.Context()); err == nil {
				t.Fatal("stopped runtime restarted")
			}
		})
	}
}

func TestGitStopTimeoutRetainsCleanupAndRejectsNewWork(t *testing.T) {
	g, err := New(Options{Client: client.New("http://example.invalid", ""), LocalRoot: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	canceled, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer func() { releaseOnce.Do(func() { close(release) }); _ = g.Stop(context.Background()) }()
	g.runBackground(func(ctx context.Context) {
		<-ctx.Done()
		close(canceled)
		<-release // Simulate a local filesystem call that has not returned yet.
	})
	var checkpoints atomic.Int32
	g.checkpoints.Schedule("repo", func() { checkpoints.Add(1) })
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if err := g.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop before task exit = %v", err)
	}
	<-canceled
	if checkpoints.Load() != 0 {
		t.Fatal("checkpoint raced an old directory writer")
	}
	if g.runBackground(func(context.Context) { t.Error("new work ran during Stop") }) {
		t.Fatal("Stop admitted new work")
	}
	releaseOnce.Do(func() { close(release) })
	ctx2, cancel2 := context.WithTimeout(t.Context(), time.Second)
	defer cancel2()
	for i := 0; i < 2; i++ {
		if err := g.Stop(ctx2); err != nil {
			t.Fatal(err)
		}
		if checkpoints.Load() != 1 {
			t.Fatalf("checkpoint count = %d", checkpoints.Load())
		}
	}
}

func TestGitStopBeforeStartFlushesPendingCheckpoint(t *testing.T) {
	g, err := New(Options{Client: client.New("http://example.invalid", ""), LocalRoot: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	g.checkpoints.Schedule("repo", func() { calls.Add(1) })
	if err := g.Stop(t.Context()); err != nil || calls.Load() != 1 {
		t.Fatalf("Stop = %v, checkpoints = %d", err, calls.Load())
	}
}

func TestGitViewSwitchKeepsOpenHandleRestriction(t *testing.T) {
	g := &Git{}
	if reason, n := g.ViewSwitchRefusal(); reason != "" || n != 0 {
		t.Fatalf("idle runtime refused: %s %d", reason, n)
	}
	h := &gitHandle{}
	g.openHandles.Store(h, "/repo/f")
	if reason, n := g.ViewSwitchRefusal(); reason == "" || n != 1 {
		t.Fatalf("open handle accepted: %s %d", reason, n)
	}
	g.openHandles.Delete(h)
	if reason, n := g.ViewSwitchRefusal(); reason != "" || n != 0 {
		t.Fatalf("released handle refused: %s %d", reason, n)
	}
	if _, n := g.ReexecRefusal(); n == 0 {
		t.Fatal("process re-exec restriction was relaxed")
	}
}

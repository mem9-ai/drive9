package git

import (
	"context"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// TestOverlayMirrorSurvivesDiscoveryFetch: a first-discovery overlay
// fetch is in flight while the workspace is already published and serving;
// a write landing in the mirror during the fetch must survive the
// fetch's replacement (the fetched log predates the write).
func TestOverlayMirrorSurvivesDiscoveryFetch(t *testing.T) {
	for iter := 0; iter < 200; iter++ {
		b := testbackend.NewBackend()
		b.Seed("/repo/keep.txt", []byte("keep"))
		b.Seed("/repo/gone.txt", []byte("gone"))
		b.SeedGitWorkspace("ws-1", "/repo")
		ts := httptest.NewServer(b)
		cl := client.New(ts.URL, "")
		bus := fs.NewEventBus()
		g, err := New(Options{Client: cl, Events: bus})
		if err != nil {
			t.Fatal(err)
		}
		g.armed = true
		if err := g.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		terminal, _ := core.New(cl, core.Options{})
		ch, _ := fs.NewChain(g, terminal)
		ctx := context.Background()
		if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/repo/gone.txt"}); err != nil {
			t.Fatalf("iter %d unlink: %v", iter, err)
		}
		failed := false
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpList, Path: "/repo"})
			if err != nil {
				t.Errorf("iter %d list: %v", iter, err)
				return
			}
			for _, e := range res.Entries {
				if e.Name == "gone.txt" {
					failed = true
				}
			}
		}()
		wg.Wait()
		if failed {
			g.mu.RLock()
			ws := g.workspaces
			g.mu.RUnlock()
			for _, w := range ws {
				w.mu.RLock()
				_, hasWhiteout := w.overlay["gone.txt"]
				n := len(w.overlay)
				w.mu.RUnlock()
				t.Errorf("iter %d: gone.txt visible; ws=%s overlayN=%d hasWhiteout=%v", iter, w.id, n, hasWhiteout)
			}
		}
		ts.Close()
	}
}

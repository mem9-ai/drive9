package git

// #1029 review round, FS-native port: the overlay refresh fence must
// protect per-entry mutations — an overwrite of an existing mirror entry
// (or its removal) that lands while the discovery walk is in flight wins
// over the walked snapshot, which predates it. Deterministic: the walk's
// directory listing is captured on request receipt and delivered only
// after the local overwrite has landed.

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// fencingStore wraps the tail chain and stalls the overlay directory
// listing until released, so the walk snapshot is provably stale by the
// time discovery applies it.
type fencingStore struct {
	fs.Filesystem
	overlayDir string
	release    chan struct{}
	blocked    chan struct{}
	blockOnce  sync.Once
	armed      sync.Map // dir -> visit count
}

func (s *fencingStore) List(ctx context.Context, dir string) ([]fs.Entry, error) {
	// Match the overlay plane exactly (dir or a subdir), not the
	// sibling overlay-blackouts plane.
	if dir == s.overlayDir || strings.HasPrefix(dir, s.overlayDir+"/") {
		if v, loaded := s.armed.LoadOrStore(dir, 1); loaded {
			if n, _ := v.(int); n >= 1 {
				s.armed.Store(dir, n+1)
				s.blockOnce.Do(func() { close(s.blocked) })
				<-s.release
			}
		}
	}
	return s.Filesystem.List(ctx, dir)
}

func TestOverlayRefreshPreservesInFlightOverwrite(t *testing.T) {
	b := testbackend.NewBackend()
	seedGitWorkspaceFS(t, b, "ws-1", "/repo", map[string]string{
		"f":        "base",
		"gone":     "gone",
		"keep.txt": "keep",
	})
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	storeChain, err := fs.NewChain(terminal)
	if err != nil {
		t.Fatal(err)
	}
	fence := &fencingStore{
		Filesystem: fs.HandlerFilesystem(storeChain.Dispatch),
		overlayDir: "/.drive9/git-workspaces/ws-1/overlay",
		release:    make(chan struct{}),
		blocked:    make(chan struct{}),
	}
	g, err := New(Options{Store: fence})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(g, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// First load (unfenced).
	g.Reload(ctx)

	// Fence the next walk (the overlay listing above armed the
	// first-visit pass-through), then start a reload in the background.
	done := make(chan struct{})
	go func() {
		defer close(done)
		g.Reload(ctx)
	}()

	// Wait until the walk is stalled on the overlay listing (the fence's
	// second visit), then land a local overwrite of /repo/f through the
	// extension while the snapshot is still in flight. The landing goes
	// through the store (the write itself is not what is under test —
	// the fence merge is), and its mtime postdates the walk start.
	select {
	case <-fence.blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("walk never stalled on the overlay listing")
	}
	w, rel, ok := g.workspaceFor("/repo/f")
	if !ok || rel != "f" {
		t.Fatalf("workspace lookup rel=%q ok=%v", rel, ok)
	}
	if err := g.landOverlayFile(ctx, w, "f", []byte("local"), 0o644); err != nil {
		t.Fatal(err)
	}
	close(fence.release)
	<-done

	// The in-flight overwrite wins over the stale snapshot.
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/repo/f"})
	if err != nil || string(res.Data) != "local" {
		t.Fatalf("fenced read = %q %v, want the in-flight local overwrite", res.Data, err)
	}
}

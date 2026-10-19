// The fast-clone view: the workspace's HEAD tree serves clean files from
// git objects, and the remote overlay log is fetched at load so other
// clients' modifications and whiteouts are visible (v1 gitEntry/tree
// machinery).
package git

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// gitWorkspaceRescanInterval bounds the on-demand discovery loop (v1
// gitWorkspaceIndexRescanInterval): workspaces registered after the mount
// arm without a remount.
const gitWorkspaceRescanInterval = 60 * time.Second

// gitBlobCacheSize bounds the materialized-blob cache.
const gitBlobCacheSize = 64

// loadRemoteOverlay fetches the server-side overlay log into the workspace
// mirror (v1 rt.overlay): other clients' upserts and deletes become
// visible. Entries whose payload lives server-side (storage-backed) keep
// their metadata; their reads fall through to the base plane.
func (g *Git) loadRemoteOverlay(ctx context.Context, w *workspace) {
	// Snapshot the mirror's VALUES before the fetch: a mutation landing
	// while the list is in flight (the workspace is already published and
	// serving) must survive the replacement — the fetched log predates it.
	// Key membership alone fences only additions; an OVERWRITE of an
	// existing key would let the older response replace the acknowledged
	// newer bytes, and a local deletion would resurrect the deleted row.
	w.mu.RLock()
	before := make(map[string]overlayEntry, len(w.overlay))
	for rel, e := range w.overlay {
		before[rel] = e
	}
	w.mu.RUnlock()
	entries, err := g.cl.ListGitOverlayEntries(ctx, w.id)
	if err != nil {
		// 404-style absences are fine (a workspace with no overlay log).
		return
	}
	overlay := make(map[string]overlayEntry, len(entries))
	for _, ge := range entries {
		e := overlayEntry{Op: ge.Op, Kind: ge.Kind, Mode: ge.Mode, Content: ge.Content, mtime: ge.UpdatedAt}
		if e.Kind == "" {
			e.Kind = "file"
		}
		if e.Op == "" {
			e.Op = "upsert"
		}
		if len(e.Content) == 0 && ge.SizeBytes > 0 {
			// Storage-backed payload: the local mirror cannot serve the
			// bytes; record a marker so attrs list the size while reads
			// fall to the base view.
			e.remote = true
			e.size = ge.SizeBytes
		}
		// The fetched inline bytes ARE the authoritative image — including
		// a zero-length one (a synced ftruncate(0)). Without the marker
		// ensureBufferLocked skipped the overlay and reloaded the stale
		// HEAD/base under the next write.
		if !e.remote && (e.Kind == "file" || e.Kind == "symlink") {
			e.Materialized = true
		}
		overlay[strings.TrimPrefix(ge.Path, "/")] = e
	}
	w.mu.Lock()
	// Local mirrors of our own writes already match the log; the fetched
	// log is the server truth (our writes round-tripped through
	// PutGitOverlayEntry).
	for rel, e := range w.overlay {
		old, existed := before[rel]
		if !existed || !sameOverlayEntry(old, e) {
			// New, or locally mutated while the fetch was in flight: the
			// response's row predates the acknowledged change.
			overlay[rel] = e
		}
	}
	for rel := range before {
		if _, still := w.overlay[rel]; !still {
			// Deleted locally during the fetch: the response's row is
			// pre-deletion truth and must not resurrect the name.
			delete(overlay, rel)
		}
	}
	w.overlay = overlay
	w.mu.Unlock()
}

// sameOverlayEntry reports whether two mirror entries carry identical
// state — the fetch fence replays a local entry only when it differs from
// the fetch-start snapshot.
func sameOverlayEntry(a, b overlayEntry) bool {
	return a.Op == b.Op && a.Kind == b.Kind && a.Mode == b.Mode &&
		a.remote == b.remote && a.size == b.size &&
		a.mtime.Equal(b.mtime) && bytes.Equal(a.Content, b.Content)
}

// loadTree fetches the workspace's HEAD tree (v1 ListGitTree → rt.nodes):
// clean files resolve here before the base view.
func (g *Git) loadTree(ctx context.Context, w *workspace) bool {
	head := w.head()
	if head == "" {
		return false
	}
	nodes, err := g.cl.ListGitTree(ctx, w.id, head)
	if err != nil {
		return false
	}
	tree := make(map[string]client.GitTreeNode, len(nodes))
	for _, n := range nodes {
		if n.Path == "" {
			continue
		}
		tree[strings.TrimPrefix(n.Path, "/")] = n
	}
	w.mu.Lock()
	w.tree = tree
	w.mu.Unlock()
	return true
}

// treeLookup resolves rel against the HEAD tree.
func (w *workspace) treeLookup(rel string) (client.GitTreeNode, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.tree == nil || rel == "" {
		return client.GitTreeNode{}, false
	}
	n, ok := w.tree[rel]
	return n, ok
}

// treeAttr projects a tree node onto the framework attr shape. mtime is
// the workspace's stable clean-tree time (cleanTreeMtime).
func treeAttr(n client.GitTreeNode, mtime time.Time) fs.Attr {
	kind := fs.KindFile
	switch n.Kind {
	case "dir":
		kind = fs.KindDir
	case "symlink":
		kind = fs.KindSymlink
	}
	mode := parseMode(n.Mode)
	if mode == 0 {
		mode = 0o644
		switch kind {
		case fs.KindDir:
			mode = 0o755
		case fs.KindSymlink:
			mode = 0o777
		}
	}
	size := n.SizeBytes
	if size < 0 {
		// Blobless trees report unknown sizes; resolve lazily on read.
		size = 0
	}
	return fs.Attr{Kind: kind, Mode: mode, Size: size, Mtime: mtime}
}

// blobOnce deduplicates concurrent blob materializations (v1
// materializeGitBlob's singleflight).
var blobOnce singleFlight

type singleFlight struct {
	mu sync.Mutex
	m  map[string]*blobCall
}

type blobCall struct {
	done chan struct{}
	data []byte
	err  error
}

func (s *singleFlight) do(key string, fn func() ([]byte, error)) ([]byte, error) {
	s.mu.Lock()
	if s.m == nil {
		s.m = map[string]*blobCall{}
	}
	if c, ok := s.m[key]; ok {
		s.mu.Unlock()
		<-c.done
		return c.data, c.err
	}
	c := &blobCall{done: make(chan struct{})}
	s.m[key] = c
	s.mu.Unlock()
	c.data, c.err = fn()
	close(c.done)
	s.mu.Lock()
	delete(s.m, key)
	s.mu.Unlock()
	return c.data, c.err
}

// blobCache memoizes materialized blobs (bounded).
var (
	blobCacheMu sync.Mutex
	blobCache   = map[string][]byte{}
)

func cacheBlob(key string, data []byte) {
	blobCacheMu.Lock()
	defer blobCacheMu.Unlock()
	if len(blobCache) >= gitBlobCacheSize {
		for k := range blobCache {
			delete(blobCache, k)
			break
		}
	}
	blobCache[key] = data
}

func cachedBlob(key string) ([]byte, bool) {
	blobCacheMu.Lock()
	defer blobCacheMu.Unlock()
	d, ok := blobCache[key]
	return d, ok
}

// materializeBlob reads one blob out of the workspace's restored local
// .git (v1 gitCatFileBlob): `git cat-file blob <sha>`.
func (g *Git) materializeBlob(w *workspace, sha string) ([]byte, error) {
	if sha == "" {
		return nil, fmt.Errorf("git: tree node without object sha")
	}
	if d, ok := cachedBlob(w.id + ":" + sha); ok {
		return d, nil
	}
	gitDir := g.gitDirFor(w.rootPath())
	data, err := blobOnce.do(w.id+":"+sha, func() ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "git", "--git-dir", gitDir, "cat-file", "blob", sha)
		var stdout bytes.Buffer
		cmd.Stdout = &stdout
		if err := cmd.Run(); err != nil {
			return nil, fmt.Errorf("git cat-file %s: %w", sha, err)
		}
		return stdout.Bytes(), nil
	})
	if err == nil {
		cacheBlob(w.id+":"+sha, data)
	}
	return data, err
}

// blobSize resolves an unknown tree-node size (blobless -1) from the
// local object store.
func (g *Git) blobSize(w *workspace, sha string) int64 {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", "--git-dir", g.gitDirFor(w.rootPath()), "cat-file", "-s", sha)
	out, err := cmd.Output()
	if err != nil {
		return 0
	}
	var size int64
	_, _ = fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &size)
	return size
}

// Package git is the git-workspace extension: workspaces are discovered
// through the client's GitWorkspace API (ListGitWorkspaces) and their
// writes go to the server-side overlay log (PutGitOverlayEntry) — never
// through fs-path rewriting. Reads merge the base view with each
// workspace's overlay (upsert entries add/replace, delete entries hide).
package git

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/debounce"
)

// gitCheckpointTimeout bounds one state checkpoint or restore.
const gitCheckpointTimeout = 2 * time.Minute

// gitCheckpointDebounce coalesces .git mutations into one checkpoint.
const gitCheckpointDebounce = 2 * time.Second

// gitWorkspaceHydrateTimeout bounds a workspace's state restore (remote
// clone + packs + archive) and the blobless HEAD-tree hydration — large
// clones need far more than a checkpoint's budget.
const gitWorkspaceHydrateTimeout = 30 * time.Minute

type overlayEntry struct {
	Op      string // "upsert" | "delete"
	Kind    string // "file" | "dir" | "symlink"
	Mode    string
	Content []byte
}

// workspace is one loaded git workspace: its root and overlay log.
type workspace struct {
	id   string
	root string // canonical view root, "/"-prefixed, no trailing slash
	// headCommit/repoURL/fastBlobless feed the checkpoint machine.
	headCommit   string
	repoURL      string
	fastBlobless bool

	mu      sync.RWMutex
	overlay map[string]overlayEntry // rel path (no leading slash) → entry
}

func (w *workspace) lookup(rel string) (overlayEntry, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	e, ok := w.overlay[rel]
	return e, ok
}

func (w *workspace) set(rel string, e overlayEntry) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.overlay == nil {
		w.overlay = map[string]overlayEntry{}
	}
	w.overlay[rel] = e
}

// Options configures the extension.
type Options struct {
	Client *client.Client
	Events *fs.EventBus
	// LocalRoot is the mount's local overlay root. Workspaces under it
	// carry a real .git directory whose state is checkpointed to the
	// server and restored on mount.
	LocalRoot string
}

// Git is the fs.Handler overlaying git workspaces on the base view.
type Git struct {
	cl  *client.Client
	bus *fs.EventBus
	// localRoot is the mount's overlay root; non-empty arms the local
	// .git checkpoint machinery (state.go).
	localRoot   string
	checkpoints *debounce.FlushDebouncer
	// ckptMu serializes checkpoint uploads per extension.
	ckptMu sync.Mutex

	mu         sync.RWMutex
	workspaces []*workspace
	loaded     bool

	// live indexes claimed workspace handles by path (SetAttr
	// interception, unlink/rename retargeting).
	live sync.Map
}

// trackWorkspace/untrackWorkspace maintain the claimed-handle index.
func (g *Git) trackWorkspace(path string, h *gitHandle) { g.live.Store(path, h) }
func (g *Git) untrackWorkspace(path string)             { g.live.Delete(path) }

func (g *Git) handleFor(path string) *gitHandle {
	if v, ok := g.live.Load(path); ok {
		return v.(*gitHandle)
	}
	return nil
}

func (g *Git) retargetWorkspace(oldPath, newPath string, h *gitHandle) {
	g.live.Delete(oldPath)
	g.live.Store(newPath, h)
}

func New(opts Options) (*Git, error) {
	if opts.Client == nil {
		return nil, fmt.Errorf("git: client is required")
	}
	g := &Git{cl: opts.Client, bus: opts.Events, localRoot: opts.LocalRoot}
	if g.localRoot != "" {
		g.checkpoints = debounce.NewFlushDebouncer(gitCheckpointDebounce)
	}
	if g.bus != nil {
		g.bus.On(fs.EventInvalidated, func(e fs.Event) {
			if e.Path != "/" {
				return
			}
			// coarse reset signal: force a workspace reload
			g.mu.Lock()
			g.loaded = false
			g.mu.Unlock()
		})
		g.bus.On(fs.EventWrite, func(e fs.Event) {
			// Writes under a workspace's .git (namespace mutations —
			// git's own file traffic) debounce a state checkpoint.
			if g.localRoot != "" {
				g.scheduleGitStateCheckpoint(e.Path)
			}
		})
	}
	return g, nil
}

// Reload forces a fresh workspace list.
func (g *Git) Reload(ctx context.Context) {
	g.mu.Lock()
	g.loaded = false
	g.mu.Unlock()
	g.ensure(ctx)
}

// ensure loads the workspace list once; a failed load retries on the next
// op.
func (g *Git) ensure(ctx context.Context) {
	g.mu.RLock()
	loaded := g.loaded
	g.mu.RUnlock()
	if loaded {
		return
	}
	list, err := g.cl.ListGitWorkspaces(ctx)
	if err != nil {
		return
	}
	var ws []*workspace
	for _, gw := range list {
		if gw.WorkspaceID == "" || strings.TrimSpace(gw.RootPath) == "" {
			continue
		}
		root := strings.TrimSuffix(strings.TrimSpace(gw.RootPath), "/")
		if root == "" {
			root = "/"
		}
		if !strings.HasPrefix(root, "/") {
			root = "/" + root
		}
		if gw.Status == "deleted" {
			continue
		}
		ws = append(ws, &workspace{
			id:           gw.WorkspaceID,
			root:         root,
			headCommit:   gw.HeadCommit,
			repoURL:      gw.RepoURL,
			fastBlobless: gw.Mode == "fast-blobless",
		})
	}
	g.mu.Lock()
	g.workspaces = ws
	g.loaded = true
	g.mu.Unlock()
}

// workspaceFor returns the workspace whose root is the longest prefix of
// path, plus the path relative to that root.
func (g *Git) workspaceFor(path string) (*workspace, string, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	var best *workspace
	for _, w := range g.workspaces {
		if w.root == "/" || path == w.root || strings.HasPrefix(path, w.root+"/") {
			if best == nil || len(w.root) > len(best.root) {
				best = w
			}
		}
	}
	if best == nil {
		return nil, "", false
	}
	rel := strings.TrimPrefix(strings.TrimPrefix(path, best.root), "/")
	if best.root == "/" {
		rel = strings.TrimPrefix(path, "/")
	}
	return best, rel, true
}

// Start reloads workspaces at mount start. The reload is async and retries
// lazily on the next op — a slow or absent /v1/git-workspaces endpoint must
// never block Init (go-fuse calls Init inside the serve loop). With a local
// root, each workspace's checkpointed .git state comes back before the
// mount serves (objects from the remote clone, then stored packs, then the
// state archive — swapped atomically).
func (g *Git) Start(ctx context.Context) error {
	go func() {
		reloadCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		g.Reload(reloadCtx)
		cancel()
		if g.localRoot != "" {
			g.mu.RLock()
			workspaces := make([]*workspace, len(g.workspaces))
			copy(workspaces, g.workspaces)
			g.mu.RUnlock()
			restoreCtx, restoreCancel := context.WithTimeout(context.Background(), gitWorkspaceHydrateTimeout)
			defer restoreCancel()
			for _, w := range workspaces {
				if err := g.restoreWorkspaceState(restoreCtx, w); err != nil {
					fmt.Fprintf(os.Stderr, "drive9: git state restore failed for workspace %s: %v\n", w.id, err)
					continue
				}
				if w.fastBlobless {
					// A blobless restore prefetches the HEAD tree's blobs
					// before serving.
					if err := g.MaterializeHeadTree(restoreCtx, g.gitDirFor(w.root)); err != nil {
						fmt.Fprintf(os.Stderr, "drive9: git head tree materialize failed for workspace %s: %v\n", w.id, err)
					}
				}
			}
		}
	}()
	return nil
}

// Drain flushes pending debounced checkpoints.
func (g *Git) Drain(ctx context.Context) error {
	if g.checkpoints != nil {
		g.checkpoints.FlushAll()
	}
	return nil
}

// Stop checkpoints every workspace's state at shutdown.
func (g *Git) Stop(ctx context.Context) error {
	g.checkpointAllGitWorkspaces()
	return nil
}

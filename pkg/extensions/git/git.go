// Package git is the git-workspace extension, re-implemented FS-native:
// workspaces live under /.drive9/git-workspaces/ as plain drive9 files
// (pkg/gitws layout) and the extension is a pure view transformation over
// the rest of the chain. Reads of a workspace path resolve through three
// layers — the dirty mirror (overlay/), the clean tree (tree/<commit>/),
// then the classic namespace — by rewriting the op's path and dispatching
// into next, so every store access rides the same cached, commit-pipelined
// filesystem the mount already provides. The extension holds no client
// and no backend-specific interface: its store view is the chain below
// itself (Options.Store / AttachStore), which is why the default wiring
// places git outside the cache extension.
package git

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/debounce"
	"github.com/mem9-ai/drive9/pkg/gitcache"
	"github.com/mem9-ai/drive9/pkg/gitws"
	"github.com/mem9-ai/drive9/pkg/gitwsindex"
)

// gitCheckpointTimeout bounds one state checkpoint or restore.
const gitCheckpointTimeout = 2 * time.Minute

// gitCheckpointDebounce coalesces .git mutations into one checkpoint.
const gitCheckpointDebounce = 2 * time.Second

// gitWorkspaceHydrateTimeout bounds a workspace's state restore (remote
// clone + packs + archive) and the blobless HEAD-tree hydration — large
// clones need far more than a checkpoint's budget.
const gitWorkspaceHydrateTimeout = 30 * time.Minute

// overlayNode is one dirty-mirror routing record. The fs is the index:
// nodes derive from walking overlay/ and overlay-blackouts/ (and from
// this mount's own landings); no sidecar manifest exists.
type overlayNode struct {
	blackout bool    // overlay-blackouts/<rel> marker present
	kind     fs.Kind // valid when !blackout
	mode     uint32
	size     int64
	target   string    // symlink target (overlay layer only)
	mtime    time.Time // landing/observed time, for stable stat replies
}

// workspace is one loaded git workspace: its view root, its tracked-path
// oracle, and its dirty-mirror index.
type workspace struct {
	id string

	mu           sync.RWMutex
	root         string // canonical view root, "/"-prefixed, no trailing slash
	headCommit   string
	repoURL      string
	fastBlobless bool
	kind         string // "main" | "linked"
	commonID     string
	worktreeName string
	// createdAt anchors the clean-tree mtime (v1 gitCleanTreeMtime): a
	// stable timestamp per commit lets git's stat comparison trust clean
	// tree files instead of content-checking every one on every status.
	createdAt time.Time

	tracked map[string]gitws.TreeEntry // rel → clean-tree entry
	overlay map[string]overlayNode     // rel → mirror/blackout state
}

// cleanTreeMtime returns the stable stat time for clean tree files: the
// workspace's creation time with a sub-second component derived from the
// head commit, so every stat of an unchanged tree file answers the same
// timestamp and git's index trust survives status passes.
func (w *workspace) cleanTreeMtime() time.Time {
	w.mu.RLock()
	base, commit := w.createdAt, w.headCommit
	w.mu.RUnlock()
	if base.IsZero() {
		base = time.Unix(1, 0).UTC()
	}
	commit = strings.TrimSpace(commit)
	if commit == "" {
		return base
	}
	sum := sha256.Sum256([]byte(strings.ToLower(commit)))
	raw := uint32(sum[0])<<24 | uint32(sum[1])<<16 | uint32(sum[2])<<8 | uint32(sum[3])
	return time.Unix(base.Unix(), int64(raw%uint32(time.Second))).UTC()
}

// noteTreeSize records a blob size learned by serving the file (v1
// updateGitKnownSize): blobless manifests carry unknown sizes (Size < 0),
// and until the first read resolves one, getattr answers a placeholder
// that git status flags as phantom-modified. Recording the size lets the
// next stat pass the index comparison.
func (w *workspace) noteTreeSize(rel string, size int64) {
	if size < 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if e, ok := w.tracked[rel]; ok && e.Size < 0 {
		e.Size = size
		w.tracked[rel] = e
	}
}

func (w *workspace) lookup(rel string) (overlayNode, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.overlay == nil {
		return overlayNode{}, false
	}
	e, ok := w.overlay[rel]
	return e, ok
}

func (w *workspace) set(rel string, e overlayNode) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.overlay == nil {
		w.overlay = map[string]overlayNode{}
	}
	w.overlay[rel] = e
}

func (w *workspace) drop(rel string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.overlay, rel)
}

// snapshotOverlay copies the routing index for listing merges.
func (w *workspace) snapshotOverlay() map[string]overlayNode {
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make(map[string]overlayNode, len(w.overlay))
	for k, v := range w.overlay {
		out[k] = v
	}
	return out
}

func (w *workspace) isTracked(rel string) (gitws.TreeEntry, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	e, ok := w.tracked[rel]
	return e, ok
}

// root/head/repo/blobless are refreshed when a reload re-reads the
// manifests; readers outside g.mu must snapshot through these accessors
// instead of touching the fields directly.
func (w *workspace) rootPath() string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.root
}

func (w *workspace) head() string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.headCommit
}

func (w *workspace) repo() string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.repoURL
}

func (w *workspace) blobless() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.fastBlobless
}

func (w *workspace) linked() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.kind == "linked"
}

func (w *workspace) commonWorkspaceID() string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.commonID
}

func (w *workspace) worktree() string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.worktreeName
}

// setMeta refreshes the record fields a reload re-derived from the store.
func (w *workspace) setMeta(root, head, repo string, blobless bool, kind, commonID, worktree string, createdAt time.Time, tracked map[string]gitws.TreeEntry, overlay map[string]overlayNode) {
	w.mu.Lock()
	w.root, w.headCommit, w.repoURL, w.fastBlobless = root, head, repo, blobless
	w.kind, w.commonID, w.worktreeName = kind, commonID, worktree
	if !createdAt.IsZero() {
		w.createdAt = createdAt
	}
	w.tracked, w.overlay = tracked, overlay
	w.mu.Unlock()
}

// headTreeMtime snapshots the clean-tree stat time for attr assembly.
func (w *workspace) treeMtime() time.Time {
	return w.cleanTreeMtime()
}

// Options configures the extension.
type Options struct {
	// Store is the extension's view of the filesystem below itself (the
	// chain tail with the cache included). Assembly attaches it after
	// construction when nil (AttachStore); a nil store keeps the
	// extension dormant and fully pass-through.
	Store  fs.Filesystem
	Events *fs.EventBus
	// LocalRoot is the mount's local overlay root. Workspaces under it
	// carry a real .git directory whose state is checkpointed to the
	// store and restored on mount.
	LocalRoot string
	// RemoteRoot is the mount's remote subtree root (":"-mount). Store
	// records carry tenant-absolute roots; they are matched and re-rooted
	// onto the mount-local vocabulary (v1 fs.localPath) at discovery.
	RemoteRoot string
	// OnWorkspaceRefresh reports one store discovery pass; forced marks a
	// reload an explicit event or Reload asked for, as opposed to the
	// lazy first-op load (v1's forced_refresh counter).
	OnWorkspaceRefresh func(forced bool)
}

// Git is the fs.Handler overlaying git workspaces on the base view.
type Git struct {
	store     fs.Filesystem
	bus       *fs.EventBus
	localRoot string
	// remoteRoot re-roots tenant-absolute workspace roots onto this
	// mount's path vocabulary.
	remoteRoot string
	onRefresh  func(forced bool)
	// checkpoints debounces .git state uploads per git dir.
	checkpoints *debounce.FlushDebouncer
	// ckptMu serializes checkpoint uploads per extension.
	ckptMu sync.Mutex
	// reloadMu single-flights discovery reloads.
	reloadMu sync.Mutex

	mu         sync.RWMutex
	workspaces []*workspace
	loaded     bool

	// inodes/notify translate store events back onto the kernel's view
	// of the workspace paths (attached by assembly with the facade's).
	inodes fs.InodeTable
	notify fs.Notifier

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

// AttachStore wires the chain-tail filesystem view (called by assembly
// once the handler list exists; before the first dispatch).
func (g *Git) AttachStore(store fs.Filesystem) {
	g.mu.Lock()
	g.store = store
	g.mu.Unlock()
}

// Attach wires the facade-owned identity and notifier services so remote
// store changes invalidate the kernel's view of the mapped workspace
// paths.
func (g *Git) Attach(inodes fs.InodeTable, notify fs.Notifier) {
	g.mu.Lock()
	g.inodes, g.notify = inodes, notify
	g.mu.Unlock()
}

func (g *Git) storeView() fs.Filesystem {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.store
}

// ReexecRefusal keeps the v1 git gate: the git workspace layer refuses
// re-exec outright — its in-memory workspace runtimes and mirror indexes
// do not survive the handoff.
func (g *Git) ReexecRefusal() (string, int) { return "git_workspace", 1 }

// localPathOf maps a tenant-absolute workspace root onto this mount's
// path vocabulary; a root outside the subtree (ok=false) is not this
// mount's workspace (v1 fs.localPath).
func (g *Git) localPathOf(remote string) (string, bool) {
	p := strings.TrimSuffix(strings.TrimSpace(remote), "/")
	if p == "" {
		p = "/"
	}
	root := g.remoteRoot
	if root == "" {
		return p, true
	}
	if p == root {
		return "/", true
	}
	if strings.HasPrefix(p, root+"/") {
		return p[len(root):], true
	}
	return "", false
}

// RoutesLocal reports view paths the git plane declares local (v1
// gitDirShouldBeLocalOverlay): a discovered workspace's .git directory
// routes to the local overlay, so the restored .git state backs git's own
// file traffic instead of the remote. A pending marker widens the claim
// to a workspace still being created — `drive9 git clone --fast` writes
// the marker before git runs and only registers the record after the
// clone; without it a live mount classifies .git as remote during the
// clone and uploads the object database.
func (g *Git) RoutesLocal(viewPath string) bool {
	if g == nil {
		return false
	}
	// Cheap necessary condition: only a path with a `.git` segment can
	// match (segment-exact — `.gitignore` must not).
	if !pathHasGitDirSegment(viewPath) {
		return false
	}
	root, ok := mountRootForGitDirPath(viewPath)
	if !ok {
		return false
	}
	if g.localRoot != "" && gitcache.WorkspacePending(context.Background(), g.localRoot, root) {
		return true
	}
	// In-memory workspace list only — this must not trigger discovery.
	w, rel, hit := g.workspaceFor(viewPath)
	if !hit || w == nil {
		return false
	}
	return rel == ".git" || strings.HasPrefix(rel, ".git/")
}

// pathHasGitDirSegment reports whether viewPath contains a path segment
// named exactly ".git".
func pathHasGitDirSegment(viewPath string) bool {
	for _, part := range strings.Split(viewPath, "/") {
		if part == ".git" {
			return true
		}
	}
	return false
}

// mountRootForGitDirPath returns the mount-local root that owns the
// nearest `.git` segment of viewPath (for example "/repo" for
// "/repo/.git/config").
func mountRootForGitDirPath(viewPath string) (string, bool) {
	parts := strings.Split(viewPath, "/")
	for i, part := range parts {
		if part != ".git" {
			continue
		}
		root := strings.Join(parts[:i], "/")
		if root == "" {
			return "/", true
		}
		return root, true
	}
	return "", false
}

func New(opts Options) (*Git, error) {
	root := strings.TrimSuffix(strings.TrimSpace(opts.RemoteRoot), "/")
	if root != "" && !strings.HasPrefix(root, "/") {
		root = "/" + root
	}
	if root == "/" {
		root = ""
	}
	g := &Git{store: opts.Store, bus: opts.Events, localRoot: opts.LocalRoot, remoteRoot: root, onRefresh: opts.OnWorkspaceRefresh}
	if g.localRoot != "" {
		g.checkpoints = debounce.NewFlushDebouncer(gitCheckpointDebounce)
	}
	if g.bus != nil {
		for _, kind := range []fs.EventKind{fs.EventInvalidated, fs.EventDelete} {
			g.bus.On(kind, func(e fs.Event) { g.onStoreEvent(e.Path) })
		}
		g.bus.On(fs.EventCheckout, func(e fs.Event) { g.onStoreEvent(e.Path) })
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

// onStoreEvent reacts to coherence events: the store documents reload
// discovery, overlay mutations revalidate one routing entry and knock
// the kernel's cached user path, and coarse resets drop the loaded flag.
func (g *Git) onStoreEvent(path string) {
	if path == "" {
		return
	}
	if path == "/" {
		g.mu.Lock()
		g.loaded = false
		g.mu.Unlock()
		return
	}
	if !strings.HasPrefix(path, gitws.Root+"/") {
		return
	}
	if isStoreDocPath(path) {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			g.Reload(ctx)
		}()
		// A checkpoint landing for a workspace whose local .git is still
		// missing (this mount discovered the record before the CLI
		// uploaded its state) gets its restore retried.
		if strings.HasSuffix(path, "/git-state.json") {
			g.scheduleMissingRestore(path)
		}
		return
	}
	if id, rel, ok := splitOverlayStorePath(path); ok {
		g.revalidateOverlayRel(id, rel)
	}
}

// isStoreDocPath reports a registry/record document whose change reloads
// discovery (the index, and each workspace's manifest, state, tree index,
// and binary-sidecar metadata all reshape the workspace set or inputs).
func isStoreDocPath(p string) bool {
	rest := strings.TrimPrefix(p, gitws.Root+"/")
	id, tail, ok := strings.Cut(rest, "/")
	if !ok || id == "" {
		return true // the index document or the root itself
	}
	switch tail {
	case "manifest.json", "state", "tree-index.json", "git-state.json", "git-state.tar.gz":
		return true
	}
	return strings.HasPrefix(tail, "packs/")
}

// splitOverlayStorePath maps a store event path onto (workspace, rel)
// for overlay and blackout mutations.
func splitOverlayStorePath(p string) (string, string, bool) {
	rest := strings.TrimPrefix(p, gitws.Root+"/")
	id, tail, ok := strings.Cut(rest, "/")
	if !ok || id == "" {
		return "", "", false
	}
	for _, plane := range []string{"overlay/", "overlay-blackouts/"} {
		if rel, ok := strings.CutPrefix(tail, plane); ok && rel != "" {
			return id, rel, true
		}
	}
	return "", "", false
}

// revalidateOverlayRel re-derives one routing entry from the store and
// invalidates the kernel's view of the mapped user path.
func (g *Git) revalidateOverlayRel(id, rel string) {
	store := g.storeView()
	if store == nil {
		return
	}
	w := g.workspaceByID(id)
	if w == nil {
		return
	}
	rel, err := gitws.CleanRel(rel)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var next overlayNode
	if attr, err := store.GetAttr(ctx, gitws.OverlayPath(id, rel)); err == nil {
		next = overlayNodeFromAttr(attr)
	} else if _, err := store.GetAttr(ctx, gitws.BlackoutPath(id, rel)); err == nil {
		next = overlayNode{blackout: true}
	}
	w.set(rel, next)
	g.notifyUserPath(w.rootPath(), rel)
}

// notifyUserPath knocks the kernel's caches for <root>/<rel> after a
// remote store change (the SSE layer already invalidated the store path;
// only this extension knows the mapping back onto the view).
func (g *Git) notifyUserPath(root, rel string) {
	if g.notify == nil || g.inodes == nil {
		return
	}
	user := strings.TrimSuffix(root, "/") + "/" + rel
	if root == "/" {
		user = "/" + rel
	}
	g.mu.RLock()
	inodes, notify := g.inodes, g.notify
	g.mu.RUnlock()
	if ino, ok := inodes.Ino(user); ok {
		notify.InvalidateContent(ino, 0)
	}
	if dir, base := splitDirBase(user); dir != "" {
		if parentIno, ok := inodes.Ino(dir); ok {
			notify.InvalidateEntry(parentIno, base)
		}
	}
}

func splitDirBase(p string) (string, string) {
	i := strings.LastIndexByte(p, '/')
	if i < 0 {
		return "", p
	}
	if i == 0 {
		return "/", p[1:]
	}
	return p[:i], p[i+1:]
}

// Reload forces a fresh workspace discovery from the store.
func (g *Git) Reload(ctx context.Context) {
	g.mu.Lock()
	g.loaded = false
	g.mu.Unlock()
	g.reloadMu.Lock()
	defer g.reloadMu.Unlock()
	g.discover(ctx)
	if g.onRefresh != nil {
		g.onRefresh(true)
	}
}

// discover loads the workspace set once; a failed load retries on the
// next op. Loaded workspace objects are reused by id so in-flight claimed
// handles keep a stable identity across reloads.
func (g *Git) discover(ctx context.Context) {
	g.mu.RLock()
	loaded := g.loaded
	g.mu.RUnlock()
	if loaded {
		return
	}
	store := g.storeView()
	if store == nil {
		return
	}
	idx, err := readStoreIndex(ctx, store)
	if err != nil {
		return
	}
	g.mu.Lock()
	prev := make(map[string]*workspace, len(g.workspaces))
	for _, w := range g.workspaces {
		prev[w.id] = w
	}
	g.mu.Unlock()

	var next []*workspace
	for _, entry := range idx.Workspaces {
		if err := gitws.ValidateID(entry.WorkspaceID); err != nil {
			continue
		}
		if strings.TrimSpace(entry.RootPath) == "" {
			continue
		}
		w, ok := g.loadWorkspace(ctx, store, entry, prev[entry.WorkspaceID])
		if !ok {
			continue
		}
		delete(prev, entry.WorkspaceID)
		next = append(next, w)
	}
	g.mu.Lock()
	g.workspaces = next
	g.loaded = true
	g.mu.Unlock()
	if g.onRefresh != nil {
		g.onRefresh(false)
	}
}

// loadWorkspace reads one workspace record (manifest, state, tree index)
// and rebuilds its dirty-mirror routing index from the store, reusing w
// when the record already exists.
func (g *Git) loadWorkspace(ctx context.Context, store fs.Filesystem, entry gitwsindex.Entry, prev *workspace) (*workspace, bool) {
	manifestBody, err := readStoreDoc(ctx, store, gitws.ManifestPath(entry.WorkspaceID))
	if err != nil {
		return nil, false // not registered in the store (or unreadable): skip
	}
	m, err := gitws.ParseManifest(manifestBody)
	if err != nil {
		fmt.Fprintf(os.Stderr, "drive9: git workspace %s manifest unreadable: %v\n", entry.WorkspaceID, err)
		return nil, false
	}
	if body, err := readStoreDoc(ctx, store, gitws.StatePath(entry.WorkspaceID)); err == nil {
		if gitws.ParseState(body) == gitws.StateDeleted {
			return nil, false
		}
	} else if !fs.IsNotFound(err) {
		return nil, false
	}

	root, ok := g.localPathOf(m.RootPath)
	if !ok {
		return nil, false // registered outside this mount's subtree
	}
	var createdAt time.Time
	if m.CreatedAt != "" {
		if t, terr := time.Parse(time.RFC3339, m.CreatedAt); terr == nil {
			createdAt = t
		}
	}
	tracked := map[string]gitws.TreeEntry{}
	if body, err := readStoreDoc(ctx, store, gitws.TreeIndexPath(entry.WorkspaceID)); err == nil {
		if idx, perr := gitws.ParseTreeIndex(body); perr == nil {
			for _, e := range idx.Entries {
				if rel, cerr := gitws.CleanRel(e.Path); cerr == nil {
					tracked[rel] = e
				}
			}
		}
	}
	overlay := walkOverlayIndex(ctx, store, entry.WorkspaceID)

	w := prev
	if w == nil {
		w = &workspace{id: entry.WorkspaceID}
	}
	w.setMeta(root, m.HeadCommit, m.RepoURL, m.Mode == "fast-blobless",
		m.WorkspaceKind, m.CommonWorkspaceID, m.WorktreeName, createdAt, tracked, overlay)
	return w, true
}

func (g *Git) workspaceByID(id string) *workspace {
	g.mu.RLock()
	defer g.mu.RUnlock()
	for _, w := range g.workspaces {
		if w.id == id {
			return w
		}
	}
	return nil
}

// workspaceFor returns the workspace whose root is the longest prefix of
// path, plus the path relative to that root.
func (g *Git) workspaceFor(path string) (*workspace, string, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	var best *workspace
	bestRoot := ""
	for _, w := range g.workspaces {
		root := w.rootPath()
		if root == "/" || path == root || strings.HasPrefix(path, root+"/") {
			if best == nil || len(root) > len(bestRoot) {
				best, bestRoot = w, root
			}
		}
	}
	if best == nil {
		return nil, "", false
	}
	rel := strings.TrimPrefix(strings.TrimPrefix(path, bestRoot), "/")
	if bestRoot == "/" {
		rel = strings.TrimPrefix(path, "/")
	}
	return best, rel, true
}

// Start reloads workspaces at mount start. The reload is async and
// retries lazily on the next op — a slow or absent store must never
// block Init (go-fuse calls Init inside the serve loop). With a local
// root, each workspace's checkpointed .git state comes back before the
// mount serves (objects from the remote clone, then stored packs, then
// the state archive — swapped atomically), and the record's state file
// flips from dormant to armed.
func (g *Git) Start(ctx context.Context) error {
	go func() {
		reloadCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		g.Reload(reloadCtx)
		cancel()
		if g.localRoot == "" {
			return
		}
		g.mu.RLock()
		workspaces := make([]*workspace, len(g.workspaces))
		copy(workspaces, g.workspaces)
		g.mu.RUnlock()
		restoreCtx, restoreCancel := context.WithTimeout(context.Background(), gitWorkspaceHydrateTimeout)
		defer restoreCancel()
		// Main workspaces restore first so linked worktrees can anchor
		// their gitdir against the common .git.
		for pass := 0; pass < 2; pass++ {
			for _, w := range workspaces {
				if (pass == 0) == w.linked() {
					continue
				}
				g.armWorkspace(restoreCtx, w)
				if err := g.restoreWorkspaceState(restoreCtx, w); err != nil {
					fmt.Fprintf(os.Stderr, "drive9: git state restore failed for workspace %s: %v\n", w.id, err)
					continue
				}
				if w.blobless() && !w.linked() {
					// A blobless restore prefetches the HEAD tree's
					// blobs before serving.
					if err := g.MaterializeHeadTree(restoreCtx, g.gitDirForWorkspace(w)); err != nil {
						fmt.Fprintf(os.Stderr, "drive9: git head tree materialize failed for workspace %s: %v\n", w.id, err)
					}
				}
			}
		}
	}()
	return nil
}

// armWorkspace flips the record's state file from dormant to armed
// (advisory; best-effort).
func (g *Git) armWorkspace(ctx context.Context, w *workspace) {
	store := g.storeView()
	if store == nil {
		return
	}
	body, err := readStoreDoc(ctx, store, gitws.StatePath(w.id))
	if err != nil {
		return
	}
	if gitws.ParseState(body) != gitws.StateDormant {
		return
	}
	_ = store.Write(ctx, gitws.StatePath(w.id), []byte(gitws.StateArmed+"\n"), fs.WriteOptions{Mode: 0o644})
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

// walkOverlayIndex rebuilds the dirty-mirror routing index by walking
// overlay/ and overlay-blackouts/ (the fs is the index). A torn writer
// that left both an upsert and a blackout for one path resolves with the
// upsert opinion for routing (the marker is dropped at the next landing).
func walkOverlayIndex(ctx context.Context, store fs.Filesystem, id string) map[string]overlayNode {
	out := map[string]overlayNode{}
	walkStore(ctx, store, gitws.OverlayDir(id), "", func(rel string, attr fs.Attr) {
		out[rel] = overlayNodeFromAttr(attr)
	})
	walkStore(ctx, store, gitws.BlackoutDir(id), "", func(rel string, attr fs.Attr) {
		if _, upserted := out[rel]; !upserted {
			out[rel] = overlayNode{blackout: true}
		}
	})
	return out
}

func overlayNodeFromAttr(attr fs.Attr) overlayNode {
	node := overlayNode{kind: attr.Kind, mode: attr.Mode, size: attr.Size, mtime: attr.Mtime}
	if node.mode == 0 {
		switch node.kind {
		case fs.KindDir:
			node.mode = 0o755
		case fs.KindSymlink:
			node.mode = 0o777
		default:
			node.mode = 0o644
		}
	}
	return node
}

// walkStore recursively lists a store subtree, reporting file and symlink
// leaves (directories are implied by their children).
func walkStore(ctx context.Context, store fs.Filesystem, dir, prefix string, fn func(rel string, attr fs.Attr)) {
	entries, err := store.List(ctx, dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		rel := e.Name
		if prefix != "" {
			rel = prefix + "/" + e.Name
		}
		if e.Attr.IsDir() {
			walkStore(ctx, store, dir+"/"+e.Name, rel, fn)
			continue
		}
		fn(rel, e.Attr)
	}
}

// readStoreDoc reads one whole store document.
func readStoreDoc(ctx context.Context, store fs.Filesystem, p string) ([]byte, error) {
	return store.Read(ctx, p, 0, 0)
}

// readStoreIndex loads and parses the registry document (empty when the
// store has no workspaces yet).
func readStoreIndex(ctx context.Context, store fs.Filesystem) (*gitwsindex.Index, error) {
	body, err := readStoreDoc(ctx, store, gitwsindex.Path)
	if err != nil {
		if fs.IsNotFound(err) {
			return &gitwsindex.Index{}, nil
		}
		return nil, err
	}
	return gitwsindex.Parse(body)
}

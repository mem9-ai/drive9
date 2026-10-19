// Package git is the git-workspace extension: workspaces are discovered
// through the client's GitWorkspace API (ListGitWorkspaces) and their
// writes go to the server-side overlay log (PutGitOverlayEntry) — never
// through fs-path rewriting. Reads merge the base view with each
// workspace's overlay (upsert entries add/replace, delete entries hide).
package git

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/debounce"
	"github.com/mem9-ai/drive9/pkg/gitcache"
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
	Op   string // "upsert" | "whiteout"
	Kind string // "file" | "dir" | "symlink"
	Mode string
	// remote marks a storage-backed overlay payload this mirror cannot
	// serve: attrs carry size, reads fall through to the base view.
	remote bool
	size   int64
	// Content carries inline payloads.
	Content []byte
	// mtime is the entry's landed time (server UpdatedAt, or now for our
	// own writes): a rewrite must advance the served stat past whatever
	// the index last recorded, or git treats the file as racily clean.
	mtime time.Time
}

// workspace is one loaded git workspace: its root, overlay log, and HEAD
// tree (the fast-clone view).
type workspace struct {
	id   string
	root string // canonical view root, "/"-prefixed, no trailing slash
	// headCommit/repoURL/fastBlobless feed the checkpoint machine.
	headCommit   string
	repoURL      string
	fastBlobless bool
	// createdAt anchors the clean-tree mtime (v1 gitCleanTreeMtime): a
	// stable timestamp per commit lets git's stat comparison trust clean
	// tree files instead of content-checking every one on every status.
	createdAt time.Time

	mu sync.RWMutex
	// uploadMu serializes overlay publication: concurrent commits (a
	// Drain flush racing a Fsync) could land their PUTs out of order and
	// leave the server holding the OLDER image while the newer upload
	// already cleared its dirty state — no later boundary republished it.
	uploadMu   sync.Mutex
	overlay    map[string]overlayEntry // rel path (no leading slash) → entry
	tree       map[string]client.GitTreeNode
	treeLoaded string // head commit the tree was loaded for
	// stateRestoreMu serializes the lazy local .git restore. An attempt
	// that fails (or finds the checkpoint not uploaded yet) leaves the
	// gate open — the next entry point retries — while restoreLatched
	// makes every later call a no-op once a restore has succeeded. A
	// sync.Once here consumed the attempt on failure and never retried
	// (review).
	stateRestoreMu sync.Mutex
	restoreLatched bool
}

// noteTreeSize records a blob size learned by materializing it (v1
// updateGitKnownSize): blobless manifests carry unknown sizes, and until
// the first read resolves one, getattr answers a placeholder that git
// status flags as phantom-modified. Recording the size lets the next
// stat pass the index comparison and the status converge.
func (w *workspace) noteTreeSize(rel string, size int64) {
	if size < 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if n, ok := w.tree[rel]; ok && n.SizeBytes < 0 {
		// map values are copies: write the updated node back or every
		// lookup re-resolves via cat-file and a later failure re-exposes
		// the placeholder (review).
		n.SizeBytes = size
		w.tree[rel] = n
	}
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

// root/head/repo/blobless are refreshed when a reload re-lists
// workspaces; readers outside g.mu must snapshot through these
// accessors instead of touching the fields directly.
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

func (w *workspace) setMeta(root, head, repo string, blobless bool, createdAt time.Time) {
	w.mu.Lock()
	w.root, w.headCommit, w.repoURL, w.fastBlobless = root, head, repo, blobless
	if !createdAt.IsZero() {
		w.createdAt = createdAt
	}
	w.mu.Unlock()
}

// cleanTreeMtime returns the stable stat time for clean tree files (v1
// gitCleanTreeMtime): the workspace's creation time with a sub-second
// component derived from the head commit, so every stat of an unchanged
// tree file answers the same timestamp and git's index trust survives
// status/checkout passes without re-hashing or rewriting each file.
var gitCleanTreeDefaultMtime = time.Unix(1, 0).UTC()

func (w *workspace) cleanTreeMtime() time.Time {
	w.mu.RLock()
	base, commit := w.createdAt, w.headCommit
	w.mu.RUnlock()
	if base.IsZero() {
		base = gitCleanTreeDefaultMtime
	}
	commit = strings.TrimSpace(commit)
	if commit == "" {
		return base
	}
	sum := sha256.Sum256([]byte(strings.ToLower(commit)))
	raw := uint32(sum[0])<<24 | uint32(sum[1])<<16 | uint32(sum[2])<<8 | uint32(sum[3])
	nsec := int64(raw % uint32(time.Second))
	return time.Unix(base.Unix(), nsec).UTC()
}

// Options configures the extension.
// localPathOf maps a server-absolute workspace root onto this mount's
// path vocabulary; a root outside the subtree (ok=false) is not this
// mount's workspace (v1 fs.localPath).
func (g *Git) localPathOf(remote string) (string, bool) {
	p := strings.TrimSuffix(strings.TrimSpace(remote), "/")
	if p == "" {
		p = "/"
	}
	root := g.remoteRoot
	if root == "" || root == "/" {
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

type Options struct {
	Client *client.Client
	Events *fs.EventBus
	// OnWorkspaceRefresh reports one workspace list; forced marks a
	// reload an explicit invalidation or arm signal asked for (v1's
	// forced_refresh counter) as opposed to a lazy miss.
	OnWorkspaceRefresh func(forced bool)

	// LocalRoot is the mount's local overlay root. Workspaces under it
	// carry a real .git directory whose state is checkpointed to the
	// server and restored on mount.
	LocalRoot string
	// SyncPolicy mirrors the mount's write policy: write-sync/strict
	// commits Git workspace writes before acknowledging them (v1).
	SyncPolicy string
	// RemoteRoot is the mount's remote subtree root (":"-mount). Server
	// paths under it are matched and re-rooted onto the mount-local
	// vocabulary (v1 fs.localPath): a workspace registered server-side
	// with the absolute path must resolve for the mount's local lookups.
	RemoteRoot string
}

// Git is the fs.Handler overlaying git workspaces on the base view.
type Git struct {
	onRefresh func(forced bool)
	// forceNext marks the next list as explicitly forced (v1 pendingForce).
	forceNext bool
	// ensureMu serializes workspace lists (v1 ensureInflight/ensureWait):
	// a mount-start Reload racing the first op's lazy load issues two
	// concurrent lists, and the stale completion's loadRemoteOverlay
	// replaces the mirror after a write already landed in it — dropping
	// the write from the local view until the next reload.
	ensureMu sync.Mutex
	// On-demand discovery state (v1 dormant → armed): a mount with no
	// workspace under its root stays dormant — zero ListGitWorkspaces —
	// until a local arm signal (the CLI's markers under LocalRoot) or a
	// non-empty remote index arms it.
	armed            bool
	localArmGen      string
	localArmScanAt   time.Time
	dormantConfirmed bool // index confirmed empty for this root; skip probes until local arm
	indexRevision    int64
	indexMtime       time.Time
	indexScanAt      time.Time
	cl               *client.Client
	bus              *fs.EventBus
	// localRoot is the mount's overlay root; non-empty arms the local
	// .git checkpoint machinery (state.go).
	localRoot string
	// syncPolicy mirrors the mount's write policy: write-sync/strict
	// commits workspace writes before acknowledging them (v1).
	syncPolicy string
	// remoteRoot is the mount's subtree root; server workspace paths are
	// re-rooted through localPathOf.
	remoteRoot  string
	checkpoints *debounce.FlushDebouncer
	// ckptMu serializes checkpoint uploads per extension.
	ckptMu sync.Mutex

	mu         sync.RWMutex
	workspaces []*workspace
	loaded     bool
	// lastListAt throttles miss-driven re-lists: a workspace registered
	// server-side after this mount started (git clone --fast) is
	// discovered on the next overlay miss, not only at mount start.
	lastListAt time.Time
	// rescanCancel stops the periodic workspace rescan (Start arms it).
	rescanCancel context.CancelFunc

	// live indexes claimed workspace handles by path (SetAttr
	// interception, unlink/rename retargeting).
	live sync.Map
	// openHandles tracks every claimed descriptor (the live slot keeps
	// only the newest per path).
	openHandles sync.Map
}

// trackWorkspace/untrackWorkspace maintain the claimed-handle index.
func (g *Git) trackWorkspace(path string, h *gitHandle) {
	g.live.Store(path, h)
	// The full set keeps every open descriptor discoverable: the per-path
	// slot holds only the NEWEST claim, and unlink's anonymization must
	// reach older fds too — their later sync would otherwise republish
	// the deleted name over a recreated object.
	g.openHandles.Store(h, path)
}
func (g *Git) untrackWorkspace(path string) { g.live.Delete(path) }

// untrackHandle removes a descriptor from both indexes (its path slot
// may already belong to a newer claim).
func (g *Git) untrackHandle(h *gitHandle) {
	g.openHandles.Delete(h)
	h.mu.Lock()
	path := h.path
	h.mu.Unlock()
	g.live.Delete(path)
}

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

// ReexecRefusal ports the v1 git gate: the git workspace layer refuses
// re-exec outright — its in-memory workspace runtimes and overlay mirrors
// do not survive the handoff.
func (g *Git) ReexecRefusal() (string, int) { return "git_workspace", 1 }

// RoutesLocal reports view paths the git plane declares local (v1
// gitDirShouldBeLocalOverlay): a discovered workspace's .git directory
// routes to the local overlay, so the restored .git state backs git's own
// file traffic instead of the remote. A pending marker widens the claim to
// a workspace still being created — `drive9 git clone --fast` writes the
// marker before git runs and only registers the workspace row after the
// clone; without the marker a live mount classifies .git as remote during
// the clone (uploading the object database) and flips to an EMPTY overlay
// on discovery, shadowing the .git the checkpoint is about to read.
func (g *Git) RoutesLocal(viewPath string) bool {
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
	// In-memory workspace list only — this must not trigger workspace
	// routing (a throttled refresh from every `.git` access during a
	// clone).
	w, rel, ok := g.workspaceFor(viewPath)
	if !ok || w == nil {
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
	if opts.Client == nil {
		return nil, fmt.Errorf("git: client is required")
	}
	g := &Git{cl: opts.Client, bus: opts.Events, localRoot: opts.LocalRoot, syncPolicy: opts.SyncPolicy,
		remoteRoot: strings.TrimSuffix(strings.TrimSpace(opts.RemoteRoot), "/"),
		onRefresh:  opts.OnWorkspaceRefresh}
	if g.localRoot != "" {
		g.checkpoints = debounce.NewFlushDebouncer(gitCheckpointDebounce)
	}
	if g.bus != nil {
		g.bus.On(fs.EventInvalidated, func(e fs.Event) {
			if e.Path != "/" {
				return
			}
			// coarse reset signal: force a workspace reload (the forced
			// counter distinguishes it from lazy miss-driven lists)
			g.mu.Lock()
			g.loaded = false
			g.forceNext = true
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

// ensureWorkspaceState lazily restores a workspace's local .git once its
// server-side state exists: the discovery-time restore races the CLI's
// checkpoint upload (git clone --fast registers the workspace BEFORE the
// state lands). Sync per workspace, bounded by the hydrate budget.
func (g *Git) ensureWorkspaceState(ctx context.Context, w *workspace) error {
	if g.localRoot == "" {
		return nil
	}
	w.stateRestoreMu.Lock()
	defer w.stateRestoreMu.Unlock()
	if w.restoreLatched {
		return nil
	}
	rctx, cancel := context.WithTimeout(context.Background(), gitWorkspaceHydrateTimeout)
	defer cancel()
	if err := g.restoreWorkspaceState(rctx, w); err != nil {
		// Not latched: a transient failure (or a checkpoint the CLI
		// has not uploaded yet — discovery races the upload) retries
		// at the next entry point. A not-yet-uploaded checkpoint is
		// quiet; real failures surface for diagnosis.
		if !errors.Is(err, errGitStateNotReady) {
			fmt.Fprintf(os.Stderr, "drive9: git state restore failed for workspace %s: %v\n", w.id, err)
		}
		return nil
	}
	if w.blobless() {
		// A blobless restore prefetches the HEAD tree's blobs
		// before serving.
		if err := g.MaterializeHeadTree(rctx, g.gitDirFor(w.rootPath())); err != nil {
			fmt.Fprintf(os.Stderr, "drive9: git head tree materialize failed for workspace %s: %v\n", w.id, err)
		}
	}
	w.restoreLatched = true
	return nil
}

// gitWorkspaceDiscoveryWindow bounds how often a miss re-lists workspaces.
const gitWorkspaceDiscoveryWindow = 2 * time.Second

// reloadIfStale re-lists workspaces when a path fell outside every known
// workspace and the last list is older than the discovery window — the
// miss-driven half of v1's index rescan (git clone --fast registers the
// workspace server-side mid-mount; lookups must then discover it).
func (g *Git) reloadIfStale(ctx context.Context) {
	g.mu.RLock()
	stale := time.Since(g.lastListAt) >= gitWorkspaceDiscoveryWindow
	g.mu.RUnlock()
	if !stale {
		return
	}
	g.Reload(ctx)
}

// Reload forces a fresh workspace list.
func (g *Git) Reload(ctx context.Context) {
	g.mu.Lock()
	g.loaded = false
	g.mu.Unlock()
	g.ensure(ctx)
}

// ensure loads the workspace list once; a failed load retries on the next
// op. Reloading merges by workspace ID and reuses the loaded objects: the
// mount-start Reload races the first ops' lazy load, and a wholesale swap
// would drop overlay state already mirrored locally (writes the server
// accepted but nothing re-fetches).
// tryArmFromLocalSignals scans the CLI's local arm markers, throttled on
// the hot path (v1 tryArmFromLocalSignals): a new marker generation arms
// the dormant state and forces one list. Generation fingerprinting (not
// mtime) makes a second same-second --fast still count.
func (g *Git) tryArmFromLocalSignals() {
	if g.localRoot == "" {
		return
	}
	now := time.Now()
	g.mu.RLock()
	lastScan := g.localArmScanAt
	g.mu.RUnlock()
	if !lastScan.IsZero() && now.Sub(lastScan) < gitWorkspaceDiscoveryWindow {
		return
	}
	signal, gen := gitcache.LocalArmSignal(context.Background(), g.localRoot)
	g.mu.Lock()
	g.localArmScanAt = now
	if signal {
		genAdvanced := gen != "" && gen != g.localArmGen
		if gen != "" {
			g.localArmGen = gen
		}
		if !g.armed || genAdvanced {
			g.armed = true
			g.dormantConfirmed = false
			g.forceNext = true
			g.loaded = false
		}
	}
	g.mu.Unlock()
}

// maybeArmFromIndex probes the remote index once while dormant: an entry
// rooted under this mount arms and forces one list; an absent or
// root-empty index confirms dormancy (v1 dormantConfirmed) so ordinary
// traffic never lists again until a local arm signal appears.
func (g *Git) maybeArmFromIndex(ctx context.Context) {
	g.mu.RLock()
	confirmed := g.dormantConfirmed
	armed := g.armed
	g.mu.RUnlock()
	if confirmed || armed {
		return
	}
	idx, _, err := g.cl.ReadGitWorkspaceIndex(ctx)
	if err != nil {
		return
	}
	relevant := false
	if idx != nil {
		root := strings.TrimSuffix(g.remoteRoot, "/")
		for _, e := range idx.Workspaces {
			p := strings.TrimSuffix(strings.TrimSpace(e.RootPath), "/")
			if root == "" || root == "/" || p == root || strings.HasPrefix(p, root+"/") {
				relevant = true
				break
			}
		}
	}
	g.mu.Lock()
	if relevant {
		g.armed = true
		g.forceNext = true
		g.loaded = false
	} else {
		g.dormantConfirmed = true
	}
	g.mu.Unlock()
}

// maybeRescanRemoteIndex probes the remote workspace index (v1
// maybeRescanRemoteIndex): a revision advance on an armed mount forces one
// list; a dormant mount arms when a probe first finds the index (a fresh
// LocalRoot remount discovers via the index, G2).
func (g *Git) maybeRescanRemoteIndex(ctx context.Context) {
	g.mu.RLock()
	armed := g.armed
	lastScan := g.indexScanAt
	prevRev := g.indexRevision
	prevMT := g.indexMtime
	g.mu.RUnlock()
	// The index never arms a dormant mount (v1 AC5): ordinary traffic on
	// a workspace-free root lists zero times. An armed mount re-lists
	// when the index revision advances (cross-host discovery).
	if !armed {
		return
	}
	if !lastScan.IsZero() && time.Since(lastScan) < gitWorkspaceRescanInterval {
		return
	}
	exists, rev, mtime, err := g.cl.StatGitWorkspaceIndex(ctx)
	g.mu.Lock()
	g.indexScanAt = time.Now()
	if err != nil {
		g.mu.Unlock()
		return
	}
	if !exists {
		g.mu.Unlock()
		return
	}
	advanced := rev != prevRev || (!mtime.IsZero() && mtime.After(prevMT))
	g.indexRevision = rev
	g.indexMtime = mtime
	if advanced {
		g.forceNext = true
		g.loaded = false
	}
	g.mu.Unlock()
}

func (g *Git) ensure(ctx context.Context) {
	g.tryArmFromLocalSignals()
	g.mu.RLock()
	loaded := g.loaded
	g.mu.RUnlock()
	if loaded {
		return
	}
	// Single-flight (v1 ensureInflight): wait for any in-flight list and
	// re-check — the leader's completion satisfies this caller too, and a
	// stale second loadRemoteOverlay must not race the leader's mirror
	// publication.
	g.ensureMu.Lock()
	defer g.ensureMu.Unlock()
	g.mu.RLock()
	loaded = g.loaded
	forced := g.forceNext
	armed := g.armed
	g.mu.RUnlock()
	if loaded || !armed {
		if !armed {
			// Dormant: no ListGitWorkspaces (v1 G0/AC5). One index probe
			// decides: entries under this mount's root arm the plane (a
			// fresh-LocalRoot remount still discovers, G2); an empty or
			// absent index confirms dormant until a local arm signal.
			g.maybeArmFromIndex(ctx)
		}
		return
	}
	if g.onRefresh != nil {
		g.onRefresh(forced)
	}
	g.mu.Lock()
	g.forceNext = false
	g.mu.Unlock()
	list, err := g.cl.ListGitWorkspaces(ctx)
	if err != nil {
		return
	}
	g.mu.Lock()
	prev := make(map[string]*workspace, len(g.workspaces))
	existingIDs := make(map[string]bool, len(g.workspaces))
	for _, w := range g.workspaces {
		prev[w.id] = w
		existingIDs[w.id] = true
	}
	var ws []*workspace
	for _, gw := range list {
		if gw.WorkspaceID == "" || strings.TrimSpace(gw.RootPath) == "" {
			continue
		}
		root, local := g.localPathOf(gw.RootPath)
		if !local {
			// outside this mount's subtree
			continue
		}
		if gw.Status == "deleted" {
			continue
		}
		var w *workspace
		if existing, ok := prev[gw.WorkspaceID]; ok {
			w = existing
			w.setMeta(root, gw.HeadCommit, gw.RepoURL, gw.Mode == "fast-blobless", gw.CreatedAt)
			delete(prev, gw.WorkspaceID)
		} else {
			w = &workspace{
				id:           gw.WorkspaceID,
				root:         root,
				headCommit:   gw.HeadCommit,
				repoURL:      gw.RepoURL,
				fastBlobless: gw.Mode == "fast-blobless",
				createdAt:    gw.CreatedAt,
			}
		}
		ws = append(ws, w)
	}
	g.workspaces = ws
	g.loaded = true
	g.lastListAt = time.Now()
	g.mu.Unlock()
	// Fetch each workspace's remote overlay and HEAD tree OFF the list
	// lock (v1 rt.overlay + rt.nodes): other clients' modifications and
	// the fast-clone clean-file view become visible. The overlay mirror
	// follows the server log on every list (our writes round-trip through
	// PutGitOverlayEntry); entries added to the mirror while the fetch was
	// in flight are re-applied after the replacement so a local write never
	// disappears under a stale snapshot.
	for _, w := range ws {
		g.loadRemoteOverlay(ctx, w)
		g.refreshTree(ctx, w)
		// Every workspace gets a restore attempt on every load: one
		// discovered after mount start (git clone --fast registers it
		// server-side) needs its .git before clean-file reads can
		// materialize blobs, and one whose earlier attempt failed (the
		// checkpoint raced the discovery) retries on the next reload —
		// the success latch makes repeat attempts a no-op.
		if g.localRoot != "" {
			go func(w *workspace) {
				_ = g.ensureWorkspaceState(ctx, w)
			}(w)
		}
	}
}

// refreshTree loads the HEAD tree when its commit changed since the last
// load (v1 armed-runtime tree load; the periodic rescan re-checks cheaply).
func (g *Git) refreshTree(ctx context.Context, w *workspace) {
	head := w.head()
	if head == "" {
		return
	}
	w.mu.RLock()
	loadedFor := w.treeLoaded
	w.mu.RUnlock()
	if loadedFor == head {
		return
	}
	g.loadTree(ctx, w)
	w.mu.Lock()
	w.treeLoaded = head
	w.mu.Unlock()
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

// Start reloads workspaces at mount start. The reload is async and retries
// lazily on the next op — a slow or absent /v1/git-workspaces endpoint must
// never block Init (go-fuse calls Init inside the serve loop). With a local
// root, each workspace's checkpointed .git state comes back before the
// mount serves (objects from the remote clone, then stored packs, then the
// state archive — swapped atomically). A periodic rescan (v1
// gitWorkspaceIndexRescanInterval) arms workspaces registered after the
// mount without a remount.
func (g *Git) Start(ctx context.Context) error {
	go func() {
		// v1 starts DORMANT: no initial ListGitWorkspaces. Local arm
		// signals or a non-empty remote index arm the first list; an
		// armed mount's checkpointed workspaces then restore.
		g.tryArmFromLocalSignals()
		g.mu.RLock()
		armed := g.armed
		g.mu.RUnlock()
		if !armed {
			probeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			g.maybeArmFromIndex(probeCtx)
			cancel()
			g.mu.RLock()
			armed = g.armed
			g.mu.RUnlock()
		}
		if armed {
			reloadCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			g.Reload(reloadCtx)
			cancel()
		}
		if g.localRoot != "" {
			g.mu.RLock()
			workspaces := make([]*workspace, len(g.workspaces))
			copy(workspaces, g.workspaces)
			g.mu.RUnlock()
			restoreCtx, restoreCancel := context.WithTimeout(context.Background(), gitWorkspaceHydrateTimeout)
			defer restoreCancel()
			for _, w := range workspaces {
				// All restore entry points (mount start, discovery, lazy
				// blob reads) serialize on the workspace's restore gate:
				// two concurrent restores race their RemoveAll+Rename
				// swap and can leave a half-deleted .git behind.
				_ = g.ensureWorkspaceState(restoreCtx, w)
			}
		}
	}()
	rescanCtx, rescanCancel := context.WithCancel(context.Background())
	g.rescanCancel = rescanCancel
	go func() {
		ticker := time.NewTicker(gitWorkspaceRescanInterval)
		defer ticker.Stop()
		for {
			select {
			case <-rescanCtx.Done():
				return
			case <-ticker.C:
				rescanCtx2, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				g.tryArmFromLocalSignals()
				g.maybeRescanRemoteIndex(rescanCtx2)
				g.mu.RLock()
				armed := g.armed
				g.mu.RUnlock()
				if armed {
					g.Reload(rescanCtx2)
				}
				cancel()
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
	if g.rescanCancel != nil {
		g.rescanCancel()
	}
	g.checkpointAllGitWorkspaces()
	return nil
}

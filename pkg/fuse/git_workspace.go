package fuse

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/gitcache"
)

const gitCheckpointTimeout = 2 * time.Minute
const gitCheckpointDebounce = 2 * time.Second
const gitWorkspaceHydrateTimeout = 30 * time.Minute
const gitCheckIgnoreTimeout = 2 * time.Second
const gitWorkspaceRefreshInterval = time.Second // armed list coalescing only (not empty poll)
const gitWorkspaceIndexRescanInterval = 60 * time.Second
const gitWorkspaceLocalArmScanInterval = 250 * time.Millisecond
const gitWorkspaceListBackoffMin = time.Second
const gitWorkspaceListBackoffMax = 30 * time.Second
const gitStateStorageTarGzNoObjects = "tar.gz-no-objects"
const gitWorkspaceModeFastBlobless = "fast-blobless"
const gitLocalObjectMaxBlobBytes int64 = 5 << 20
const gitLocalObjectMaxPackBytes int64 = 256 << 20

var gitCleanTreeDefaultMtime = time.Unix(1, 0).UTC()

type gitWorkspaceLayer struct {
	mu         sync.Mutex
	checkpoint sync.Mutex
	loaded     bool
	loadedAt   time.Time
	workspaces []*gitWorkspaceRuntime

	// On-demand discovery (design: dormant → armed).
	armed            bool
	dormantConfirmed bool // index 404/empty confirmed; skip remote index until local arm
	localArmGen      string // last LocalArmSignal generation token (marker set fingerprint)
	localArmScanAt   time.Time
	indexRevision    int64
	indexMtime       time.Time
	indexScanAt      time.Time
	indexProbeDone   bool
	listBackoffUntil time.Time
	listBackoff      time.Duration
	listFailures     int
	pendingForce     bool // force requested while in backoff/in-flight; sticky until consumed
	pendingForceGen  uint64
	ensureInflight   bool
	ensureWait       chan struct{}

	// stopCtx cancels background probe/hydrate loops on mount teardown.
	stopCtx    context.Context
	stopCancel context.CancelFunc

	materialize     map[string]*gitMaterializeCall
	hydrateStarted  map[string]struct{}
	ignoreCache     map[string]bool
	gitStateRefresh map[string]time.Time
}

type gitWorkspaceRuntime struct {
	mu        sync.RWMutex
	restoreMu sync.Mutex
	workspace client.GitWorkspace
	localRoot string
	nodes     map[string]client.GitTreeNode
	children  map[string][]client.GitTreeNode
	overlay   map[string]client.GitOverlayEntry
	loadedAt  time.Time
	restored  bool

	localTreeCommit   string
	localNodes        map[string]client.GitTreeNode
	localChildren     map[string][]client.GitTreeNode
	localTreeLoadedAt time.Time
	locallyDeleted    bool
}

func (rt *gitWorkspaceRuntime) isLinked() bool {
	return rt != nil && rt.workspace.WorkspaceKind == "linked"
}

func (rt *gitWorkspaceRuntime) markLocallyDeleted() {
	if rt == nil {
		return
	}
	rt.mu.Lock()
	rt.locallyDeleted = true
	rt.mu.Unlock()
}

func (rt *gitWorkspaceRuntime) isLocallyDeleted() bool {
	if rt == nil {
		return false
	}
	rt.mu.RLock()
	deleted := rt.locallyDeleted
	rt.mu.RUnlock()
	return deleted
}

func gitWorkspaceIsFastBlobless(rt *gitWorkspaceRuntime) bool {
	return rt != nil && rt.workspace.Mode == gitWorkspaceModeFastBlobless
}

type gitMaterializeCall struct {
	done chan struct{}
	data []byte
	err  error
}

type pendingGitOverlayEntry struct {
	seq   uint64
	entry client.GitOverlayEntry
}

func newGitWorkspaceLayer() *gitWorkspaceLayer {
	ctx, cancel := context.WithCancel(context.Background())
	return &gitWorkspaceLayer{
		stopCtx:         ctx,
		stopCancel:      cancel,
		materialize:     make(map[string]*gitMaterializeCall),
		hydrateStarted:  make(map[string]struct{}),
		ignoreCache:     make(map[string]bool),
		gitStateRefresh: make(map[string]time.Time),
	}
}

// stopGitWorkspaceBackground cancels long-lived git-workspace goroutines
// (index probe loop, SSE probe helpers). Safe to call multiple times.
func (fs *Dat9FS) stopGitWorkspaceBackground() {
	if fs == nil || fs.git == nil || fs.git.stopCancel == nil {
		return
	}
	fs.git.stopCancel()
}

func (fs *Dat9FS) ensureGitWorkspaces(ctx context.Context) error {
	// Explicit ensure arms and lists subject to the short freshness window.
	// Hot-path FUSE ops must not use this for empty polling — they use
	// gitWorkspaceForPath's event-driven arming + force flags instead.
	fs.markGitWorkspacesArmed()
	return fs.ensureGitWorkspacesWithRefresh(ctx, false)
}

func (fs *Dat9FS) forceRefreshGitWorkspaces(ctx context.Context) error {
	fs.markGitWorkspacesArmed()
	return fs.ensureGitWorkspacesWithRefresh(ctx, true)
}

func (fs *Dat9FS) markGitWorkspacesArmed() {
	if fs == nil || fs.git == nil {
		return
	}
	fs.git.mu.Lock()
	fs.git.armed = true
	fs.git.dormantConfirmed = false
	fs.git.mu.Unlock()
}

func (fs *Dat9FS) gitWorkspacesArmed() bool {
	if fs == nil || fs.git == nil {
		return false
	}
	fs.git.mu.Lock()
	defer fs.git.mu.Unlock()
	return fs.git.armed
}

func (fs *Dat9FS) disarmGitWorkspacesLocked() {
	fs.git.armed = false
	fs.git.loaded = false // allow needInitialLoad if re-armed on this mount
	fs.git.loadedAt = time.Time{}
	fs.git.workspaces = nil
	fs.git.dormantConfirmed = true
	fs.git.pendingForce = false
	fs.git.localArmGen = ""
	fs.git.localArmScanAt = time.Time{}
}

// probeGitWorkspaceIndexAsync starts a background probe with retries on network
// errors (exponential backoff capped at gitWorkspaceListBackoffMax).
func (fs *Dat9FS) probeGitWorkspaceIndexAsync() {
	if fs == nil || fs.git == nil || fs.client == nil || fs.opts == nil || !fs.opts.EnableGitWorkspaces {
		return
	}
	fs.git.mu.Lock()
	if fs.git.indexProbeDone || fs.git.armed {
		fs.git.mu.Unlock()
		return
	}
	fs.git.indexProbeDone = true // in-flight; cleared only on hard success/404 confirm
	fs.git.mu.Unlock()

	go fs.runGitWorkspaceIndexProbeLoop()
}

func (fs *Dat9FS) runGitWorkspaceIndexProbeLoop() {
	backoff := gitWorkspaceListBackoffMin
	for {
		if fs == nil || fs.git == nil {
			return
		}
		stopCtx := fs.git.stopCtx
		fs.git.mu.Lock()
		if fs.git.armed || fs.git.dormantConfirmed {
			fs.git.mu.Unlock()
			return
		}
		fs.git.mu.Unlock()
		if stopCtx != nil {
			select {
			case <-stopCtx.Done():
				return
			default:
			}
		}

		base := context.Background()
		if stopCtx != nil {
			base = stopCtx
		}
		ctx, cancel := context.WithTimeout(base, fuseTimeout)
		err := fs.probeGitWorkspaceIndex(ctx)
		cancel()
		if err == nil {
			return
		}
		if stopCtx != nil && stopCtx.Err() != nil {
			return
		}
		// Permanent auth/permission failures: stop probing (e.g. fs_scoped cannot
		// read tenant-root /.drive9/, or the mount's static credential is invalid).
		// The client holds a static bearer with no refresh path; server 401 is a
		// persistent credential failure (not retryable without remount). 403 is
		// permanent scope denial. Transient backend failures are 5xx, not 401.
		if client.IsForbidden(err) || client.IsUnauthorized(err) {
			fs.git.mu.Lock()
			if !fs.git.armed {
				fs.git.dormantConfirmed = true
			}
			fs.git.indexProbeDone = true
			fs.git.mu.Unlock()
			safeLogPrintf("git workspace index probe permanent error, staying dormant: %v", err)
			return
		}
		safeLogPrintf("git workspace index probe: %v (retry in %s)", err, backoff)
		timer := time.NewTimer(backoff)
		if stopCtx != nil {
			select {
			case <-stopCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		} else {
			<-timer.C
		}
		if backoff < gitWorkspaceListBackoffMax {
			backoff *= 2
			if backoff > gitWorkspaceListBackoffMax {
				backoff = gitWorkspaceListBackoffMax
			}
		}
	}
}

func (fs *Dat9FS) probeGitWorkspaceIndex(ctx context.Context) error {
	if fs == nil || fs.git == nil || fs.client == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	exists, rev, mtime, err := fs.client.StatGitWorkspaceIndex(ctx)
	if err != nil {
		// Network error: do not dormant-confirm; caller retries.
		return err
	}
	if !exists {
		fs.git.mu.Lock()
		if !fs.git.armed {
			fs.git.dormantConfirmed = true
		}
		fs.git.indexRevision = 0
		fs.git.indexMtime = time.Time{}
		fs.git.indexScanAt = time.Now()
		fs.git.indexProbeDone = true
		fs.git.mu.Unlock()
		return nil
	}
	// Index exists: read and arm only if entries remain after RemoteRoot filter.
	idx, gotRev, readErr := fs.client.ReadGitWorkspaceIndex(ctx)
	if readErr != nil {
		return readErr
	}
	if gotRev != 0 {
		rev = gotRev
	}
	if !client.GitWorkspaceIndexHasEntries(idx) {
		fs.git.mu.Lock()
		if !fs.git.armed {
			fs.git.dormantConfirmed = true
		}
		fs.git.indexRevision = rev
		fs.git.indexMtime = mtime
		fs.git.indexScanAt = time.Now()
		fs.git.indexProbeDone = true
		fs.git.mu.Unlock()
		return nil
	}
	if !fs.gitWorkspaceIndexRelevant(idx) {
		// Tenant has workspaces elsewhere; this mount stays dormant.
		fs.git.mu.Lock()
		if !fs.git.armed {
			fs.git.dormantConfirmed = true
		}
		fs.git.indexRevision = rev
		fs.git.indexMtime = mtime
		fs.git.indexScanAt = time.Now()
		fs.git.indexProbeDone = true
		fs.git.mu.Unlock()
		return nil
	}
	fs.git.mu.Lock()
	fs.git.armed = true
	fs.git.dormantConfirmed = false
	fs.git.indexRevision = rev
	fs.git.indexMtime = mtime
	fs.git.indexScanAt = time.Now()
	fs.git.indexProbeDone = true
	fs.git.mu.Unlock()
	return fs.ensureGitWorkspacesWithRefresh(ctx, true)
}

func (fs *Dat9FS) gitWorkspaceIndexRelevant(idx *client.GitWorkspaceIndex) bool {
	if idx == nil || fs == nil {
		return false
	}
	for _, e := range idx.Workspaces {
		if _, ok := fs.localPath(strings.TrimSuffix(e.RootPath, "/")); ok {
			return true
		}
	}
	return false
}

// tryArmFromLocalSignals returns (isArmed, forceReload).
// forceReload is true when local markers advanced after we were already armed
// (e.g. same-mount second --fast registering a new workspace id).
func (fs *Dat9FS) tryArmFromLocalSignals() (armed bool, forceReload bool) {
	if fs == nil || fs.git == nil || fs.opts == nil {
		return false, false
	}
	localRoot := strings.TrimSpace(fs.opts.LocalRoot)
	if localRoot == "" {
		return false, false
	}
	now := time.Now()
	fs.git.mu.Lock()
	// Throttle local marker scans on the hot path in BOTH dormant and armed
	// states. Unthrottled dormant rescans turn every lookup/getattr/readdir into
	// ReadFile+ReadDir of arm markers — a busy never-`--fast` mount would pay
	// thousands of local syscalls/sec. Cost: same-LocalRoot live `--fast` arms
	// on the first op after the window (≤gitWorkspaceLocalArmScanInterval);
	// cross-host discovery relies on SSE + the remote index, not per-op rescans.
	if !fs.git.localArmScanAt.IsZero() && now.Sub(fs.git.localArmScanAt) < gitWorkspaceLocalArmScanInterval {
		armed := fs.git.armed
		fs.git.mu.Unlock()
		return armed, false
	}
	lastGen := fs.git.localArmGen
	wasArmed := fs.git.armed
	fs.git.localArmScanAt = now
	fs.git.mu.Unlock()

	// Generation fingerprints marker names+bodies, not only FS mtime, so a
	// second --fast in the same coarse-mtime second still force-lists.
	signal, gen := gitcache.LocalArmSignal(context.Background(), localRoot)
	fs.git.mu.Lock()
	defer fs.git.mu.Unlock()
	genAdvanced := gen != "" && gen != lastGen
	if gen != "" {
		fs.git.localArmGen = gen
	}
	if signal {
		fs.git.armed = true
		fs.git.dormantConfirmed = false
		// First arm after dormant must force-list: needInitialLoad alone is not
		// enough if a prior empty list left loaded=true with zero workspaces
		// (e.g. index SSE raced registration). Same-LocalRoot second --fast
		// changes the marker generation and also needs a force re-list.
		if !wasArmed || genAdvanced {
			return true, true
		}
		return true, false
	}
	return fs.git.armed, false
}

func (fs *Dat9FS) maybeRescanRemoteIndex(ctx context.Context) bool {
	if fs == nil || fs.git == nil || fs.client == nil {
		return false
	}
	fs.git.mu.Lock()
	armed := fs.git.armed
	lastScan := fs.git.indexScanAt
	prevRev := fs.git.indexRevision
	prevMT := fs.git.indexMtime
	fs.git.mu.Unlock()
	if !armed {
		return false
	}
	if !lastScan.IsZero() && time.Since(lastScan) < gitWorkspaceIndexRescanInterval {
		return false
	}
	exists, rev, mtime, err := fs.client.StatGitWorkspaceIndex(ctx)
	fs.git.mu.Lock()
	fs.git.indexScanAt = time.Now()
	fs.git.mu.Unlock()
	if err != nil {
		return false
	}
	if !exists {
		// Only force a list when we previously observed an index that disappeared.
		if prevRev == 0 && prevMT.IsZero() {
			return false
		}
		// Clear observed index state so subsequent rescans do not re-force list
		// every gitWorkspaceIndexRescanInterval (would reintroduce idle polling).
		fs.git.mu.Lock()
		fs.git.indexRevision = 0
		fs.git.indexMtime = time.Time{}
		fs.git.mu.Unlock()
		return true
	}
	if rev != prevRev || (!mtime.IsZero() && mtime.After(prevMT)) {
		fs.git.mu.Lock()
		fs.git.indexRevision = rev
		fs.git.indexMtime = mtime
		fs.git.mu.Unlock()
		return true
	}
	return false
}

// notifyGitWorkspaceIndexChanged is called from SSE when the remote index path changes.
func (fs *Dat9FS) notifyGitWorkspaceIndexChanged() {
	if fs == nil || fs.git == nil {
		return
	}
	fs.git.mu.Lock()
	fs.git.dormantConfirmed = false
	fs.git.indexScanAt = time.Time{} // force rescan
	// If we were dormant-confirmed empty, re-probe and arm if needed.
	wasArmed := fs.git.armed
	if !wasArmed {
		// Allow the async probe loop to run again (including retries on
		// transient network errors). A single-shot probe can drop a cross-host
		// --fast after a timeout/5xx.
		fs.git.indexProbeDone = false
	}
	fs.git.mu.Unlock()
	if !wasArmed {
		fs.probeGitWorkspaceIndexAsync()
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), fuseTimeout)
		defer cancel()
		_ = fs.forceRefreshGitWorkspaces(ctx)
	}()
}

func (fs *Dat9FS) ensureGitWorkspacesWithRefresh(ctx context.Context, force bool) error {
	if fs == nil || fs.git == nil || fs.client == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// Singleflight: only one list in flight.
	fs.git.mu.Lock()
	if !fs.git.armed {
		fs.git.mu.Unlock()
		return nil
	}
	// force=true means an *external* force request (local arm gen, index change,
	// forceRefreshGitWorkspaces). Ordinary lookups must call force=false so they
	// join an existing pending force without bumping pendingForceGen (otherwise
	// concurrent lookups self-amplify into a list storm).
	if force {
		if !fs.git.pendingForce {
			fs.git.pendingForce = true
			fs.git.pendingForceGen++
		} else if fs.git.ensureInflight {
			// Mid-flight external force: bump gen so the leader cannot clear sticky force.
			fs.git.pendingForceGen++
		}
	}
	// Backoff applies to soft and forced lists (G4: no op-level storm).
	if !fs.git.listBackoffUntil.IsZero() && time.Now().Before(fs.git.listBackoffUntil) {
		fs.git.mu.Unlock()
		return nil
	}
	// Promote soft joins when a pending force already exists (no gen bump).
	if fs.git.pendingForce {
		force = true
	}
	// Soft freshness window (skip only when we already have a successful snapshot).
	if !force && fs.git.loaded && len(fs.git.workspaces) > 0 && time.Since(fs.git.loadedAt) < gitWorkspaceRefreshInterval {
		fs.git.mu.Unlock()
		return nil
	}
	// After failed load (loaded but empty + failures), allow retry once backoff elapsed.
	if !force && fs.git.loaded && len(fs.git.workspaces) == 0 && fs.git.listFailures == 0 && time.Since(fs.git.loadedAt) < gitWorkspaceRefreshInterval {
		fs.git.mu.Unlock()
		return nil
	}
	if fs.git.ensureInflight {
		wait := fs.git.ensureWait
		fs.git.mu.Unlock()
		if wait != nil {
			select {
			case <-wait:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		// After waiting: if force arrived mid-flight (pendingForce still set) or
		// we still lack a successful snapshot, re-enter as leader.
		// Empty workspaces with loaded=true is a valid snapshot (no workspaces
		// under this mount) — do not re-list per waiter.
		// Re-enter with force=false so we join/consume pending without a gen bump.
		fs.git.mu.Lock()
		need := fs.git.pendingForce || !fs.git.loaded
		fs.git.mu.Unlock()
		if need {
			return fs.ensureGitWorkspacesWithRefresh(ctx, false)
		}
		return nil
	}
	// Capture gen at leader start: only clear pendingForce on success if no
	// newer force arrived while we were listing.
	startForceGen := fs.git.pendingForceGen
	fs.git.ensureInflight = true
	fs.git.ensureWait = make(chan struct{})
	waitCh := fs.git.ensureWait
	fs.git.mu.Unlock()
	defer func() {
		fs.git.mu.Lock()
		fs.git.ensureInflight = false
		close(waitCh)
		fs.git.ensureWait = nil
		fs.git.mu.Unlock()
	}()

	if fs.perfEnabled() {
		fs.perf.gitWorkspaceRefresh.add(1)
		if force {
			fs.perf.gitWorkspaceForcedRefresh.add(1)
		}
	}
	listStart := fs.perfStart()
	workspaces, err := fs.client.ListGitWorkspaces(ctx)
	if client.IsNotFound(err) {
		// Servers without git workspaces — same as empty list.
		fs.perfRecordRemote(perfRemoteList, listStart, nil, 0)
		err = nil
		workspaces = nil
	} else {
		fs.perfRecordRemote(perfRemoteList, listStart, err, 0)
	}
	if err != nil {
		fs.git.mu.Lock()
		fs.git.listFailures++
		if fs.git.listBackoff == 0 {
			fs.git.listBackoff = gitWorkspaceListBackoffMin
		} else {
			fs.git.listBackoff *= 2
			if fs.git.listBackoff > gitWorkspaceListBackoffMax {
				fs.git.listBackoff = gitWorkspaceListBackoffMax
			}
		}
		fs.git.listBackoffUntil = time.Now().Add(fs.git.listBackoff)
		// Keep previous non-empty snapshot. If we never loaded successfully,
		// leave loaded=false so needInitialLoad retries after backoff (G1/G2).
		if len(fs.git.workspaces) == 0 {
			fs.git.loaded = false
		}
		fs.git.mu.Unlock()
		return err
	}

	loaded := make([]*gitWorkspaceRuntime, 0, len(workspaces))
	var loadErrs []error
	for i := range workspaces {
		ws := workspaces[i]
		if fs.gitWorkspaceDeletedLocally(ctx, ws.WorkspaceID) {
			continue
		}
		localRoot, ok := fs.localPath(strings.TrimSuffix(ws.RootPath, "/"))
		if !ok {
			continue
		}
		if localRoot != "/" {
			localRoot = strings.TrimSuffix(localRoot, "/")
		}
		treeStart := fs.perfStart()
		nodes, err := fs.client.ListGitTree(ctx, ws.WorkspaceID, ws.HeadCommit)
		fs.perfRecordRemote(perfRemoteList, treeStart, err, 0)
		if err != nil {
			loadErrs = append(loadErrs, fmt.Errorf("load git tree workspace=%s root=%s: %w", ws.WorkspaceID, ws.RootPath, err))
			continue
		}
		overlayStart := fs.perfStart()
		overlays, err := fs.client.ListGitOverlayEntries(ctx, ws.WorkspaceID)
		if client.IsNotFound(err) {
			err = nil
		}
		fs.perfRecordRemote(perfRemoteList, overlayStart, err, 0)
		if err != nil {
			loadErrs = append(loadErrs, fmt.Errorf("load git overlay workspace=%s root=%s: %w", ws.WorkspaceID, ws.RootPath, err))
			continue
		}
		rt := &gitWorkspaceRuntime{
			workspace: ws,
			localRoot: localRoot,
			nodes:     make(map[string]client.GitTreeNode, len(nodes)),
			children:  make(map[string][]client.GitTreeNode),
			overlay:   make(map[string]client.GitOverlayEntry, len(overlays)),
		}
		for _, n := range nodes {
			rt.nodes[n.Path] = n
			rt.children[n.ParentPath] = append(rt.children[n.ParentPath], n)
		}
		for _, e := range overlays {
			rt.overlay[e.Path] = e
		}
		// Preserve in-memory size fills across force-refresh (blobless trees often
		// arrive with SizeBytes=-1; losing fills causes phantom git status dirty).
		fs.carryGitKnownSizesIntoRuntime(rt)
		fs.mergePendingGitOverlayEntries(ws.WorkspaceID, rt.overlay)
		loaded = append(loaded, rt)
	}
	var incomplete error
	if len(loadErrs) > 0 {
		incomplete = errors.Join(loadErrs...)
		safeLogPrintf("git workspace refresh incomplete: %v", incomplete)
		// On total failure this round: keep previous non-empty snapshot if any;
		// otherwise leave loaded=false so post-backoff needInitialLoad retries.
		if len(loaded) == 0 {
			fs.git.mu.Lock()
			fs.git.listFailures++
			if fs.git.listBackoff == 0 {
				fs.git.listBackoff = gitWorkspaceListBackoffMin
			} else {
				fs.git.listBackoff *= 2
				if fs.git.listBackoff > gitWorkspaceListBackoffMax {
					fs.git.listBackoff = gitWorkspaceListBackoffMax
				}
			}
			fs.git.listBackoffUntil = time.Now().Add(fs.git.listBackoff)
			if len(fs.git.workspaces) == 0 {
				fs.git.loaded = false
			} else {
				fs.git.loaded = true
				fs.git.loadedAt = time.Now()
			}
			fs.git.mu.Unlock()
			return incomplete
		}
		// Partial success: install what we have, still report error.
	}
	sort.Slice(loaded, func(i, j int) bool {
		return len(loaded[i].localRoot) > len(loaded[j].localRoot)
	})
	loadedAt := time.Now()
	for _, rt := range loaded {
		if rt.loadedAt.IsZero() {
			rt.loadedAt = loadedAt
		}
	}

	fs.git.mu.Lock()
	// On partial load failure, keep previous runtimes for workspaces that failed
	// this round so they are not dropped until a successful reload.
	if incomplete != nil && len(fs.git.workspaces) > 0 {
		have := make(map[string]struct{}, len(loaded))
		for _, rt := range loaded {
			have[rt.workspace.WorkspaceID] = struct{}{}
		}
		for _, prev := range fs.git.workspaces {
			if prev == nil {
				continue
			}
			id := strings.TrimSpace(prev.workspace.WorkspaceID)
			if id == "" {
				continue
			}
			if _, ok := have[id]; ok {
				continue
			}
			if fs.gitWorkspaceDeletedLocally(ctx, id) {
				continue
			}
			loaded = append(loaded, prev)
			have[id] = struct{}{}
		}
		sort.Slice(loaded, func(i, j int) bool {
			return len(loaded[i].localRoot) > len(loaded[j].localRoot)
		})
	}
	fs.git.workspaces = loaded
	fs.git.loaded = true
	fs.git.loadedAt = loadedAt
	if incomplete != nil {
		// Keep backoff so event-driven path still retries failed workspaces.
		fs.git.listFailures++
		if fs.git.listBackoff == 0 {
			fs.git.listBackoff = gitWorkspaceListBackoffMin
		} else {
			fs.git.listBackoff *= 2
			if fs.git.listBackoff > gitWorkspaceListBackoffMax {
				fs.git.listBackoff = gitWorkspaceListBackoffMax
			}
		}
		fs.git.listBackoffUntil = time.Now().Add(fs.git.listBackoff)
		// Leave pendingForce sticky so a later lookup after backoff re-lists.
	} else {
		fs.git.listFailures = 0
		fs.git.listBackoff = 0
		fs.git.listBackoffUntil = time.Time{}
		// Consume sticky force only if no newer force arrived mid-flight.
		if fs.git.pendingForceGen == startForceGen {
			fs.git.pendingForce = false
		}
	}
	if len(loaded) == 0 {
		// Empty list: disarm if no local arm signal remains.
		localRoot := ""
		if fs.opts != nil {
			localRoot = strings.TrimSpace(fs.opts.LocalRoot)
		}
		fs.git.mu.Unlock()
		signal, _ := gitcache.LocalArmSignal(ctx, localRoot)
		fs.git.mu.Lock()
		// Re-check emptiness under lock so we do not wipe a concurrent install.
		if !signal && len(fs.git.workspaces) == 0 {
			fs.disarmGitWorkspacesLocked()
		}
	}
	fs.git.mu.Unlock()

	for _, rt := range loaded {
		fs.maybeStartGitWorkspaceHydrate(rt)
	}
	return incomplete
}

func (fs *Dat9FS) gitWorkspaceForPath(ctx context.Context, localPath string) (*gitWorkspaceRuntime, string, bool) {
	if fs == nil || fs.git == nil || fs.opts == nil || !fs.opts.EnableGitWorkspaces {
		return nil, "", false
	}
	if ctx == nil {
		ctx = context.TODO()
	}
	baseCtx := ctx

	// DORMANT: only arm from local signals (remote index is mount-time / SSE).
	armed, localForce := fs.tryArmFromLocalSignals()
	if !armed {
		return nil, "", false
	}

	// Armed: event-driven refresh only (no empty poll).
	// force=true only for real external events (local gen / cache invalid / index).
	// Observing pendingForce must NOT pass force=true (would self-amplify gen).
	force := localForce
	if fs.gitWorkspaceCacheInvalidatedLocally(baseCtx) {
		force = true
	}
	if fs.maybeRescanRemoteIndex(baseCtx) {
		force = true
	}
	fs.git.mu.Lock()
	needInitialLoad := fs.git.armed && !fs.git.loaded
	joinPending := fs.git.pendingForce
	fs.git.mu.Unlock()
	if force || needInitialLoad || joinPending {
		refreshCtx, refreshCancel := context.WithTimeout(baseCtx, fuseTimeout)
		// External force bumps gen; soft join/needInitialLoad does not.
		if err := fs.ensureGitWorkspacesWithRefresh(refreshCtx, force); err != nil {
			safeLogPrintf("git workspace refresh failed for %s: %v", localPath, err)
		}
		refreshCancel()
	}

	if rt, rel, ok := fs.loadedGitWorkspaceForPath(localPath); ok {
		return rt, rel, true
	}
	// .git force only when we already have loaded workspaces (not empty dormant arm).
	fs.git.mu.Lock()
	hasWS := len(fs.git.workspaces) > 0
	fs.git.mu.Unlock()
	if hasWS && fs.shouldForceRefreshGitWorkspacesForGitStatePath(localPath) {
		refreshCtx, refreshCancel := context.WithTimeout(baseCtx, fuseTimeout)
		if err := fs.forceRefreshGitWorkspaces(refreshCtx); err != nil {
			safeLogPrintf("git workspace forced refresh failed for git state path %s: %v", localPath, err)
		}
		refreshCancel()
		if rt, rel, ok := fs.loadedGitWorkspaceForPath(localPath); ok {
			return rt, rel, true
		}
	}
	return nil, "", false
}

func (fs *Dat9FS) shouldForceRefreshGitWorkspacesForGitStatePath(localPath string) bool {
	if fs == nil || fs.git == nil {
		return false
	}
	root, ok := gitStatePathRefreshRoot(localPath)
	if !ok {
		return false
	}
	now := time.Now()
	fs.git.mu.Lock()
	defer fs.git.mu.Unlock()
	if fs.git.gitStateRefresh == nil {
		fs.git.gitStateRefresh = make(map[string]time.Time)
	}
	if last := fs.git.gitStateRefresh[root]; !last.IsZero() && now.Sub(last) < gitWorkspaceRefreshInterval {
		return false
	}
	fs.git.gitStateRefresh[root] = now
	return true
}

func gitStatePathRefreshRoot(localPath string) (string, bool) {
	clean := path.Clean(localPath)
	if clean == "." {
		return "", false
	}
	parts := strings.Split(strings.Trim(clean, "/"), "/")
	for i, part := range parts {
		if part == ".git" {
			if clean == "/" || len(parts) == 0 {
				return "/.git", true
			}
			return "/" + strings.Join(parts[:i+1], "/"), true
		}
	}
	return "", false
}

func (fs *Dat9FS) loadedGitWorkspaceForPath(localPath string) (*gitWorkspaceRuntime, string, bool) {
	if fs == nil || fs.git == nil || fs.opts == nil || !fs.opts.EnableGitWorkspaces {
		return nil, "", false
	}
	clean := path.Clean(localPath)
	fs.git.mu.Lock()
	workspaces := make([]*gitWorkspaceRuntime, len(fs.git.workspaces))
	copy(workspaces, fs.git.workspaces)
	fs.git.mu.Unlock()
	for _, rt := range workspaces {
		if fs.gitWorkspaceRuntimeHiddenLocally(context.Background(), rt) {
			continue
		}
		root := rt.localRoot
		if root == "/" {
			return rt, strings.TrimPrefix(clean, "/"), true
		}
		if clean == root {
			return rt, "", true
		}
		prefix := root + "/"
		if strings.HasPrefix(clean, prefix) {
			return rt, strings.TrimPrefix(clean, prefix), true
		}
	}
	return nil, "", false
}

func (fs *Dat9FS) gitWorkspaceRuntimeHiddenLocally(ctx context.Context, rt *gitWorkspaceRuntime) bool {
	if rt == nil {
		return false
	}
	if rt.isLocallyDeleted() {
		return true
	}
	if fs.gitWorkspaceDeletedLocally(ctx, rt.workspace.WorkspaceID) {
		rt.markLocallyDeleted()
		return true
	}
	return fs.gitWorkspaceRuntimeRefreshInvalidated(ctx, rt)
}

func (fs *Dat9FS) gitWorkspaceDeletedLocally(ctx context.Context, workspaceID string) bool {
	if fs == nil || fs.opts == nil {
		return false
	}
	return gitcache.WorkspaceDeleted(ctx, fs.opts.LocalRoot, workspaceID)
}

func (fs *Dat9FS) gitWorkspaceRuntimeRefreshInvalidated(ctx context.Context, rt *gitWorkspaceRuntime) bool {
	if fs == nil || fs.opts == nil || rt == nil {
		return false
	}
	markerTime, ok := gitcache.WorkspaceRefreshMarkerTime(ctx, fs.opts.LocalRoot, rt.workspace.WorkspaceID)
	if !ok {
		return false
	}
	return markerTime.After(rt.loadedAt)
}

func (fs *Dat9FS) gitWorkspaceCacheInvalidatedLocally(ctx context.Context) bool {
	if fs == nil || fs.git == nil || fs.opts == nil {
		return false
	}
	fs.git.mu.Lock()
	workspaces := make([]*gitWorkspaceRuntime, len(fs.git.workspaces))
	copy(workspaces, fs.git.workspaces)
	fs.git.mu.Unlock()
	for _, rt := range workspaces {
		if fs.gitWorkspaceRuntimeRefreshInvalidated(ctx, rt) {
			return true
		}
	}
	return false
}

func (fs *Dat9FS) loadedGitWorkspaceForGitStatePath(localPath string) (*gitWorkspaceRuntime, string, bool) {
	rt, rel, ok := fs.loadedGitWorkspaceForPath(localPath)
	if !ok {
		return nil, "", false
	}
	if linked, linkedRel, linkedOK := fs.linkedWorkspaceForCommonGitStatePath(rt, rel); linkedOK {
		return linked, linkedRel, true
	}
	return rt, rel, true
}

func (fs *Dat9FS) linkedWorkspaceForCommonGitStatePath(commonRT *gitWorkspaceRuntime, rel string) (*gitWorkspaceRuntime, string, bool) {
	if fs == nil || fs.git == nil || commonRT == nil || commonRT.isLinked() {
		return nil, "", false
	}
	rel = strings.TrimPrefix(path.Clean("/"+rel), "/")
	const prefix = ".git/worktrees/"
	if !strings.HasPrefix(rel, prefix) {
		return nil, "", false
	}
	tail := strings.TrimPrefix(rel, prefix)
	name, rest, _ := strings.Cut(tail, "/")
	if name == "" {
		return nil, "", false
	}
	fs.git.mu.Lock()
	workspaces := make([]*gitWorkspaceRuntime, len(fs.git.workspaces))
	copy(workspaces, fs.git.workspaces)
	fs.git.mu.Unlock()
	for _, candidate := range workspaces {
		if fs.gitWorkspaceRuntimeHiddenLocally(context.Background(), candidate) {
			continue
		}
		if candidate.workspace.CommonWorkspaceID == commonRT.workspace.WorkspaceID && candidate.workspace.WorktreeName == name {
			if rest == "" {
				return candidate, ".git", true
			}
			return candidate, ".git/" + rest, true
		}
	}
	return nil, "", false
}

func (fs *Dat9FS) gitWorkspaceRuntimeByID(workspaceID string) (*gitWorkspaceRuntime, bool) {
	if fs == nil || fs.git == nil || strings.TrimSpace(workspaceID) == "" {
		return nil, false
	}
	fs.git.mu.Lock()
	workspaces := make([]*gitWorkspaceRuntime, len(fs.git.workspaces))
	copy(workspaces, fs.git.workspaces)
	fs.git.mu.Unlock()
	for _, rt := range workspaces {
		if fs.gitWorkspaceRuntimeHiddenLocally(context.Background(), rt) {
			continue
		}
		if rt.workspace.WorkspaceID == workspaceID {
			return rt, true
		}
	}
	return nil, false
}

func (fs *Dat9FS) commonRuntimeForLinked(rt *gitWorkspaceRuntime) (*gitWorkspaceRuntime, error) {
	if rt == nil {
		return nil, fmt.Errorf("git workspace runtime is nil")
	}
	commonID := strings.TrimSpace(rt.workspace.CommonWorkspaceID)
	if commonID == "" {
		return nil, fmt.Errorf("linked git workspace %s has no common workspace id", rt.workspace.WorkspaceID)
	}
	commonRT, ok := fs.gitWorkspaceRuntimeByID(commonID)
	if !ok {
		return nil, fmt.Errorf("linked git workspace %s common workspace %s is not loaded", rt.workspace.WorkspaceID, commonID)
	}
	return commonRT, nil
}

func (fs *Dat9FS) gitDirForRuntime(rt *gitWorkspaceRuntime) (string, error) {
	if fs == nil || fs.localOverlay == nil || rt == nil {
		return "", syscall.EIO
	}
	if !rt.isLinked() {
		return fs.localOverlay.abs(path.Join(rt.localRoot, ".git"))
	}
	commonRT, err := fs.commonRuntimeForLinked(rt)
	if err != nil {
		return "", err
	}
	return fs.linkedGitDir(rt, commonRT)
}

func (fs *Dat9FS) linkedGitDir(rt, commonRT *gitWorkspaceRuntime) (string, error) {
	if fs == nil || fs.localOverlay == nil || rt == nil || commonRT == nil {
		return "", syscall.EIO
	}
	commonGitDir, err := fs.localOverlay.abs(path.Join(commonRT.localRoot, ".git"))
	if err != nil {
		return "", err
	}
	rel, err := linkedGitDirRel(rt)
	if err != nil {
		return "", err
	}
	return filepath.Join(commonGitDir, filepath.FromSlash(rel)), nil
}

func linkedGitDirRel(rt *gitWorkspaceRuntime) (string, error) {
	if rt == nil {
		return "", fmt.Errorf("git workspace runtime is nil")
	}
	rel := strings.Trim(strings.TrimSpace(rt.workspace.GitDirRel), "/")
	if rel == "" {
		name := strings.TrimSpace(rt.workspace.WorktreeName)
		if name == "" {
			return "", fmt.Errorf("linked git workspace %s has no worktree name", rt.workspace.WorkspaceID)
		}
		rel = path.Join("worktrees", name)
	}
	if strings.ContainsRune(rel, '\x00') || strings.ContainsRune(rel, '\\') {
		return "", fmt.Errorf("linked git workspace %s has unsafe gitdir_rel %q", rt.workspace.WorkspaceID, rt.workspace.GitDirRel)
	}
	for _, part := range strings.Split(rel, "/") {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("linked git workspace %s has unsafe gitdir_rel %q", rt.workspace.WorkspaceID, rt.workspace.GitDirRel)
		}
	}
	return path.Clean(rel), nil
}

func (fs *Dat9FS) writeLinkedGitFile(rt, commonRT *gitWorkspaceRuntime, gitFile string) error {
	if rt == nil || commonRT == nil {
		return fmt.Errorf("linked git runtime is incomplete")
	}
	if err := os.MkdirAll(filepath.Dir(gitFile), 0o755); err != nil {
		return err
	}
	relGitDir, err := linkedGitDirRel(rt)
	if err != nil {
		return err
	}
	target := path.Join(commonRT.localRoot, ".git", relGitDir)
	rel, err := filepath.Rel(filepath.FromSlash(rt.localRoot), filepath.FromSlash(target))
	if err != nil {
		return err
	}
	return os.WriteFile(gitFile, []byte("gitdir: "+filepath.ToSlash(rel)+"\n"), 0o644)
}

func (fs *Dat9FS) mountedGitFileForRuntime(rt *gitWorkspaceRuntime, fallback string) string {
	if rt == nil {
		return fallback
	}
	localGitFile := path.Join(rt.localRoot, ".git")
	if fs != nil && fs.opts != nil {
		if mountPoint := strings.TrimSpace(fs.opts.MountPoint); mountPoint != "" {
			return filepath.Join(mountPoint, filepath.FromSlash(strings.TrimPrefix(localGitFile, "/")))
		}
	}
	return filepath.FromSlash(localGitFile)
}

func (fs *Dat9FS) gitWorkspaceOwnsPath(ctx context.Context, localPath string) bool {
	rt, rel, ok := fs.gitWorkspaceForPath(ctx, localPath)
	if !ok || rel == "" {
		return false
	}
	if _, ok := rt.overlayEntry(rel); ok {
		return true
	}
	if localTree, _ := fs.ensureGitLocalHeadTree(ctx, rt); localTree {
		if _, ok := rt.localHeadNode(rel); ok {
			return true
		}
		return rt.hasLocalHeadImpliedDir(rel)
	}
	if _, ok := rt.cleanNode(rel); ok {
		return true
	}
	return rt.hasImpliedDir(rel)
}

func (fs *Dat9FS) gitIgnoredPathLocalOnly(ctx context.Context, localPath string, dirHint bool) bool {
	if fs == nil || fs.opts == nil || fs.opts.Profile != MountProfileCodingAgent || fs.git == nil || fs.localOverlay == nil {
		return false
	}
	if !dirHint {
		if info, err := fs.localOverlay.Lstat(localPath); err == nil && info.IsDir() {
			dirHint = true
		}
	}
	rt, rel, ok := fs.gitWorkspaceForPath(ctx, localPath)
	if !ok || rt == nil || rel == "" || rel == "." {
		return false
	}
	if rel == ".git" || strings.HasPrefix(rel, ".git/") {
		return false
	}
	if _, ok := rt.overlayEntry(rel); ok {
		return false
	}
	if localTree, _ := fs.ensureGitLocalHeadTree(ctx, rt); localTree {
		if _, ok := rt.localHeadNode(rel); ok {
			return false
		}
		if rt.hasLocalHeadImpliedDir(rel) {
			return false
		}
	}
	if _, ok := rt.cleanNode(rel); ok {
		return false
	}
	if rt.hasImpliedDir(rel) {
		return false
	}

	key := gitIgnoreCacheKey(rt, rel, dirHint)
	fs.git.mu.Lock()
	if ignored, ok := fs.git.ignoreCache[key]; ok {
		fs.git.mu.Unlock()
		return ignored
	}
	fs.git.mu.Unlock()

	ignored, cacheable := fs.gitCheckIgnoredPath(ctx, rt, rel, dirHint)
	if cacheable {
		fs.git.mu.Lock()
		if fs.git.ignoreCache == nil {
			fs.git.ignoreCache = make(map[string]bool)
		}
		fs.git.ignoreCache[key] = ignored
		fs.git.mu.Unlock()
	}
	return ignored
}

func gitIgnoreCacheKey(rt *gitWorkspaceRuntime, rel string, dirHint bool) string {
	if dirHint {
		rel += "/"
	}
	return rt.workspace.WorkspaceID + ":" + rt.workspace.HeadCommit + ":" + rel
}

func (fs *Dat9FS) gitCheckIgnoredPath(ctx context.Context, rt *gitWorkspaceRuntime, rel string, dirHint bool) (bool, bool) {
	if strings.TrimSpace(fs.opts.LocalRoot) == "" {
		return false, false
	}
	treeRoot := gitcache.TreeRoot(fs.opts.LocalRoot, rt.workspace.WorkspaceID, rt.workspace.HeadCommit)
	if info, err := os.Stat(treeRoot); err != nil || !info.IsDir() {
		return false, false
	}
	if ctx == nil {
		ctx = context.TODO()
	}
	ctx, cancel := context.WithTimeout(ctx, gitCheckIgnoreTimeout)
	defer cancel()
	if err := fs.ensureGitStateRestored(ctx, rt); err != nil {
		return false, false
	}
	gitDir, err := fs.gitDirForRuntime(rt)
	if err != nil {
		return false, false
	}
	ignored, cacheable := runGitCheckIgnore(ctx, gitDir, treeRoot, rel)
	if ignored || !cacheable || !dirHint {
		return ignored, cacheable
	}
	if !strings.HasSuffix(rel, "/") {
		return runGitCheckIgnore(ctx, gitDir, treeRoot, rel+"/")
	}
	return false, true
}

func runGitCheckIgnore(ctx context.Context, gitDir, workTree, rel string) (bool, bool) {
	cmd := exec.CommandContext(ctx, "git", "--git-dir", gitDir, "--work-tree", workTree, "check-ignore", "-q", "--", rel)
	cmd.Env = append(os.Environ(), "GIT_OPTIONAL_LOCKS=0")
	err := cmd.Run()
	if err == nil {
		return true, true
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		return false, true
	}
	return false, false
}

func (rt *gitWorkspaceRuntime) overlayEntry(rel string) (client.GitOverlayEntry, bool) {
	if rt == nil {
		return client.GitOverlayEntry{}, false
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	e, ok := rt.overlay[rel]
	return e, ok
}

func (rt *gitWorkspaceRuntime) cleanNode(rel string) (client.GitTreeNode, bool) {
	if rt == nil {
		return client.GitTreeNode{}, false
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	n, ok := rt.nodes[rel]
	return n, ok
}

func (rt *gitWorkspaceRuntime) localHeadNode(rel string) (client.GitTreeNode, bool) {
	if rt == nil {
		return client.GitTreeNode{}, false
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	n, ok := rt.localNodes[rel]
	return n, ok
}

type gitOverlaySnapshot struct {
	path  string
	entry client.GitOverlayEntry
}

func (rt *gitWorkspaceRuntime) childrenFor(rel string) []client.GitTreeNode {
	if rt == nil {
		return nil
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	children := rt.children[rel]
	out := make([]client.GitTreeNode, len(children))
	copy(out, children)
	return out
}

func (rt *gitWorkspaceRuntime) localHeadChildrenFor(rel string) []client.GitTreeNode {
	if rt == nil {
		return nil
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	children := rt.localChildren[rel]
	out := make([]client.GitTreeNode, len(children))
	copy(out, children)
	return out
}

func (rt *gitWorkspaceRuntime) overlaySnapshot() []gitOverlaySnapshot {
	if rt == nil {
		return nil
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	out := make([]gitOverlaySnapshot, 0, len(rt.overlay))
	for p, e := range rt.overlay {
		out = append(out, gitOverlaySnapshot{path: p, entry: e})
	}
	return out
}

func (fs *Dat9FS) gitEntry(ctx context.Context, localPath string, incrementLookup bool) (*InodeEntry, bool) {
	rt, rel, ok := fs.gitWorkspaceForPath(ctx, localPath)
	if !ok {
		return nil, false
	}
	if rel == "" {
		return fs.gitInode(localPath, true, 0, 0o755, true, incrementLookup), true
	}
	if e, ok := rt.overlayEntry(rel); ok {
		if e.Op == "whiteout" {
			return nil, true
		}
		return fs.gitOverlayInode(ctx, rt, rel, e, incrementLookup), true
	}
	if localTree, _ := fs.ensureGitLocalHeadTree(ctx, rt); localTree {
		if n, ok := rt.localHeadNode(rel); ok {
			return fs.gitTreeInode(ctx, rt, localPath, rel, n, incrementLookup), true
		}
		if rt.hasLocalHeadImpliedDir(rel) {
			return fs.gitInode(localPath, true, 0, 0o755, true, incrementLookup), true
		}
		return nil, true
	}
	if n, ok := rt.cleanNode(rel); ok {
		return fs.gitTreeInode(ctx, rt, localPath, rel, n, incrementLookup), true
	}
	if rt.hasImpliedDir(rel) {
		return fs.gitInode(localPath, true, 0, 0o755, true, incrementLookup), true
	}
	return nil, true
}

func (fs *Dat9FS) ensureGitLocalHeadTree(ctx context.Context, rt *gitWorkspaceRuntime) (bool, error) {
	if fs == nil || rt == nil {
		return false, nil
	}
	rt.mu.RLock()
	if rt.localTreeCommit != "" {
		active := rt.localNodes != nil
		rt.mu.RUnlock()
		return active, nil
	}
	rt.mu.RUnlock()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := fs.ensureGitStateRestored(ctx, rt); err != nil {
		return false, err
	}
	gitDir, err := fs.gitDirForRuntime(rt)
	if err != nil {
		return false, err
	}
	headOut, err := gitCommandOutputNoLazy(ctx, "--git-dir", gitDir, "rev-parse", "--verify", "HEAD")
	if err != nil {
		return false, err
	}
	head := strings.ToLower(strings.TrimSpace(string(headOut)))
	if !isGitObjectID(head) || strings.EqualFold(head, rt.workspace.HeadCommit) {
		rt.mu.Lock()
		rt.localTreeCommit = head
		rt.localNodes = nil
		rt.localChildren = nil
		rt.localTreeLoadedAt = time.Time{}
		rt.mu.Unlock()
		return false, nil
	}

	treeOut, err := gitCommandOutputNoLazy(ctx, "--git-dir", gitDir, "ls-tree", "-r", "-t", "-z", head)
	if err != nil {
		return false, err
	}
	nodes, children := parseLocalGitTree(rt.workspace.WorkspaceID, head, treeOut)
	if err := fs.fillLocalGitTreeSizes(ctx, rt, gitDir, nodes, children); err != nil {
		safeLogPrintf("git workspace local HEAD size fill failed workspace=%s head=%s: %v", rt.workspace.WorkspaceID, head, err)
	}
	rt.mu.Lock()
	rt.localTreeCommit = head
	rt.localNodes = nodes
	rt.localChildren = children
	rt.localTreeLoadedAt = time.Now()
	rt.mu.Unlock()
	return true, nil
}

func (rt *gitWorkspaceRuntime) clearLocalHeadTree() {
	if rt == nil {
		return
	}
	rt.mu.Lock()
	rt.localTreeCommit = ""
	rt.localNodes = nil
	rt.localChildren = nil
	rt.localTreeLoadedAt = time.Time{}
	rt.mu.Unlock()
}

func parseLocalGitTree(workspaceID, commit string, raw []byte) (map[string]client.GitTreeNode, map[string][]client.GitTreeNode) {
	nodes := make(map[string]client.GitTreeNode)
	children := make(map[string][]client.GitTreeNode)
	for _, rec := range bytes.Split(raw, []byte{0}) {
		if len(rec) == 0 {
			continue
		}
		tab := bytes.IndexByte(rec, '\t')
		if tab < 0 {
			continue
		}
		meta := strings.Fields(string(rec[:tab]))
		if len(meta) < 3 {
			continue
		}
		mode, typ, objectSHA := meta[0], meta[1], meta[2]
		rel := strings.TrimPrefix(path.Clean("/"+string(rec[tab+1:])), "/")
		if rel == "" || rel == "." {
			continue
		}
		kind := "file"
		switch typ {
		case "tree":
			kind = "dir"
		case "commit":
			kind = "submodule"
		case "blob":
			if mode == "120000" {
				kind = "symlink"
			}
		}
		parent := path.Dir(rel)
		if parent == "." {
			parent = ""
		}
		node := client.GitTreeNode{
			WorkspaceID: workspaceID,
			CommitSHA:   commit,
			Path:        rel,
			ParentPath:  parent,
			Name:        path.Base(rel),
			Kind:        kind,
			Mode:        mode,
			ObjectSHA:   objectSHA,
			SizeBytes:   -1,
		}
		nodes[rel] = node
		children[parent] = append(children[parent], node)
	}
	for parent := range children {
		sort.Slice(children[parent], func(i, j int) bool { return children[parent][i].Name < children[parent][j].Name })
	}
	return nodes, children
}

func (fs *Dat9FS) fillLocalGitTreeSizes(ctx context.Context, rt *gitWorkspaceRuntime, gitDir string, nodes map[string]client.GitTreeNode, children map[string][]client.GitTreeNode) error {
	if fs == nil || rt == nil || len(nodes) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	sizeByObject, baseCommit, workspaceID := rt.knownBaseGitObjectSizes()
	localRoot := ""
	if fs.opts != nil {
		localRoot = strings.TrimSpace(fs.opts.LocalRoot)
	}
	missing := make(map[string]struct{})
	for rel, n := range nodes {
		if !gitNodeHasBlobSize(n) {
			continue
		}
		oid := strings.ToLower(strings.TrimSpace(n.ObjectSHA))
		if oid == "" {
			continue
		}
		if size, ok := sizeByObject[oid]; ok {
			setGitTreeNodeSize(nodes, children, rel, size)
			continue
		}
		if localRoot != "" && workspaceID != "" && baseCommit != "" {
			if size, hit, err := gitcache.StatBlob(ctx, localRoot, workspaceID, baseCommit, oid); err != nil {
				safeLogPrintf("git workspace base blob cache stat failed workspace=%s commit=%s oid=%s: %v", workspaceID, baseCommit, oid, err)
			} else if hit {
				sizeByObject[oid] = size
				setGitTreeNodeSize(nodes, children, rel, size)
				continue
			}
		}
		missing[oid] = struct{}{}
	}
	if len(missing) == 0 || strings.TrimSpace(gitDir) == "" {
		return nil
	}
	ids := mapKeys(missing)
	sort.Strings(ids)
	infos, err := gitObjectInfoBatch(ctx, gitDir, ids)
	if err != nil {
		return fmt.Errorf("batch check local git objects gitDir=%s ids=%v: %w", gitDir, ids, err)
	}
	for rel, n := range nodes {
		if !gitNodeHasBlobSize(n) || n.SizeBytes >= 0 {
			continue
		}
		oid := strings.ToLower(strings.TrimSpace(n.ObjectSHA))
		info := infos[oid]
		if info.typ != "blob" || info.size < 0 {
			continue
		}
		setGitTreeNodeSize(nodes, children, rel, info.size)
	}
	return nil
}

func (rt *gitWorkspaceRuntime) knownBaseGitObjectSizes() (map[string]int64, string, string) {
	sizeByObject := make(map[string]int64)
	if rt == nil {
		return sizeByObject, "", ""
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	for _, n := range rt.nodes {
		if !gitNodeHasBlobSize(n) || n.SizeBytes < 0 {
			continue
		}
		oid := strings.ToLower(strings.TrimSpace(n.ObjectSHA))
		if oid == "" {
			continue
		}
		sizeByObject[oid] = n.SizeBytes
	}
	return sizeByObject, rt.workspace.HeadCommit, rt.workspace.WorkspaceID
}

func gitNodeHasBlobSize(n client.GitTreeNode) bool {
	return n.Kind == "file" || n.Kind == "symlink"
}

func gitObjectCacheKey(objectSHA string) string {
	return strings.ToLower(strings.TrimSpace(objectSHA))
}

func setGitTreeNodeSize(nodes map[string]client.GitTreeNode, children map[string][]client.GitTreeNode, rel string, size int64) {
	if size < 0 {
		return
	}
	n, ok := nodes[rel]
	if !ok {
		return
	}
	n.SizeBytes = size
	nodes[rel] = n
	childNodes := children[n.ParentPath]
	for i := range childNodes {
		if childNodes[i].Path == rel {
			childNodes[i].SizeBytes = size
			children[n.ParentPath] = childNodes
			return
		}
	}
}

func (rt *gitWorkspaceRuntime) hasImpliedDir(rel string) bool {
	if rel == "" {
		return true
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	if _, ok := rt.children[rel]; ok {
		return true
	}
	prefix := rel + "/"
	for p, e := range rt.overlay {
		if e.Op != "whiteout" && strings.HasPrefix(p, prefix) {
			return true
		}
	}
	return false
}

func (rt *gitWorkspaceRuntime) hasLocalHeadImpliedDir(rel string) bool {
	if rel == "" {
		return true
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	if _, ok := rt.localChildren[rel]; ok {
		return true
	}
	return false
}

func (fs *Dat9FS) gitTreeInode(ctx context.Context, rt *gitWorkspaceRuntime, localPath, rel string, n client.GitTreeNode, incrementLookup bool) *InodeEntry {
	mode, hasMode, isDir := gitNodeMode(n)
	size := n.SizeBytes
	if isDir {
		size = 0
	} else if size < 0 {
		size = fs.resolveGitCleanNodeSize(ctx, rt, rel, n)
	}
	return fs.gitInodeWithMtime(localPath, isDir, size, mode, hasMode, gitCleanTreeMtime(rt), incrementLookup)
}

func (fs *Dat9FS) gitOverlayInode(ctx context.Context, rt *gitWorkspaceRuntime, rel string, e client.GitOverlayEntry, incrementLookup bool) *InodeEntry {
	localPath := path.Join(rt.localRoot, rel)
	if rt.localRoot == "/" {
		localPath = "/" + rel
	}
	if e.Op == "chmod" {
		n, ok := rt.cleanNode(rel)
		if localTree, _ := fs.ensureGitLocalHeadTree(ctx, rt); localTree {
			n, ok = rt.localHeadNode(rel)
		}
		if ok {
			base := fs.gitTreeInode(ctx, rt, localPath, rel, n, incrementLookup)
			if parsed, ok := parseGitMode(e.Mode); ok && base != nil {
				fs.inodes.SetModeState(base.Ino, parsed, true)
				base.Mode = parsed
				base.HasMode = true
			}
			return base
		}
	}
	isDir := e.Kind == "dir"
	mode := uint32(0o644)
	hasMode := true
	if e.Kind == "symlink" || e.Op == "symlink" {
		mode = symlinkMode()
	} else if isDir {
		mode = 0o755
	}
	if parsed, ok := parseGitMode(e.Mode); ok {
		mode = parsed
	}
	if isDir && mode&0o777 == 0 {
		mode = 0o755
	}
	size := e.SizeBytes
	if len(e.Content) > 0 {
		size = int64(len(e.Content))
	}
	if info, ok := fs.gitWorkspaceDirtyMirrorStat(rt, rel); ok {
		size = info.Size()
	}
	if isDir {
		size = 0
	}
	return fs.gitInode(localPath, isDir, size, mode, hasMode, incrementLookup)
}

func (fs *Dat9FS) gitInode(localPath string, isDir bool, size int64, mode uint32, hasMode bool, incrementLookup bool) *InodeEntry {
	return fs.gitInodeWithMtime(localPath, isDir, size, mode, hasMode, time.Now(), incrementLookup)
}

func (fs *Dat9FS) gitInodeWithMtime(localPath string, isDir bool, size int64, mode uint32, hasMode bool, mtime time.Time, incrementLookup bool) *InodeEntry {
	if mtime.IsZero() {
		mtime = time.Now()
	}
	var ino uint64
	if incrementLookup {
		ino = fs.inodes.Lookup(localPath, isDir, size, mtime)
	} else {
		ino = fs.inodes.EnsureInode(localPath, isDir, size, mtime)
	}
	fs.inodes.SetModeState(ino, mode, hasMode)
	entry, _ := fs.inodes.GetEntry(ino)
	return entry
}

func gitCleanTreeMtime(rt *gitWorkspaceRuntime) time.Time {
	base := gitCleanTreeDefaultMtime
	commit := ""
	if rt != nil {
		if !rt.workspace.CreatedAt.IsZero() {
			base = rt.workspace.CreatedAt
		}
		commit = strings.TrimSpace(rt.workspace.HeadCommit)
	}
	if commit == "" {
		return base
	}
	sum := sha256.Sum256([]byte(strings.ToLower(commit)))
	raw := uint32(sum[0])<<24 | uint32(sum[1])<<16 | uint32(sum[2])<<8 | uint32(sum[3])
	nsec := int64(raw % uint32(time.Second))
	return time.Unix(base.Unix(), nsec).UTC()
}

func gitNodeMode(n client.GitTreeNode) (mode uint32, hasMode bool, isDir bool) {
	switch n.Kind {
	case "dir":
		return 0o755, true, true
	case "symlink":
		return symlinkMode(), true, false
	}
	if parsed, ok := parseGitMode(n.Mode); ok {
		return parsed, true, false
	}
	return 0o644, true, false
}

func parseGitMode(raw string) (uint32, bool) {
	if raw == "" {
		return 0, false
	}
	v, err := strconv.ParseUint(raw, 8, 32)
	if err != nil {
		return 0, false
	}
	mode := uint32(v)
	if mode&uint32(syscall.S_IFMT) != 0 {
		return mode, true
	}
	return mode & 0o777, true
}

func (fs *Dat9FS) listGitDir(ctx context.Context, dirPath string) ([]DirEntry, bool, error) {
	rt, rel, ok := fs.gitWorkspaceForPath(ctx, dirPath)
	if !ok {
		return nil, false, nil
	}
	localTree, _ := fs.ensureGitLocalHeadTree(ctx, rt)
	if rel != "" {
		if e, ok := rt.overlayEntry(rel); ok {
			if e.Op == "whiteout" {
				return nil, true, os.ErrNotExist
			}
			if e.Kind != "dir" {
				return nil, true, syscall.ENOTDIR
			}
		} else if localTree {
			if n, ok := rt.localHeadNode(rel); ok {
				if n.Kind != "dir" {
					return nil, true, syscall.ENOTDIR
				}
			} else if !rt.hasLocalHeadImpliedDir(rel) {
				return nil, true, os.ErrNotExist
			}
		} else if n, ok := rt.cleanNode(rel); ok {
			if n.Kind != "dir" {
				return nil, true, syscall.ENOTDIR
			}
		} else if !rt.hasImpliedDir(rel) {
			return nil, true, os.ErrNotExist
		}
	}
	entriesByName := make(map[string]DirEntry)
	whiteouts := make(map[string]struct{})
	overlays := rt.overlaySnapshot()
	children := rt.childrenFor(rel)
	if localTree {
		children = rt.localHeadChildrenFor(rel)
	}
	for _, n := range children {
		localPath := path.Join(rt.localRoot, n.Path)
		if rt.localRoot == "/" {
			localPath = "/" + n.Path
		}
		entry := fs.gitTreeInode(ctx, rt, localPath, n.Path, n, false)
		if entry == nil {
			continue
		}
		entriesByName[n.Name] = DirEntry{
			Name:        n.Name,
			Ino:         entry.Ino,
			Mode:        dirEntryMode(entry.IsDir, entry.HasMode, entry.Mode),
			Size:        entry.Size,
			Mtime:       entry.Mtime,
			AttrMode:    entry.Mode,
			HasMode:     entry.HasMode,
			IsDir:       entry.IsDir,
			HasMetadata: true,
		}
	}
	prefix := ""
	if rel != "" {
		prefix = rel + "/"
	}
	for _, overlay := range overlays {
		p, e := overlay.path, overlay.entry
		if prefix != "" && !strings.HasPrefix(p, prefix) {
			continue
		}
		childRel := strings.TrimPrefix(p, prefix)
		if childRel == "" || strings.Contains(childRel, "/") {
			continue
		}
		if e.Op == "whiteout" {
			whiteouts[childRel] = struct{}{}
			delete(entriesByName, childRel)
			continue
		}
		entry := fs.gitOverlayInode(ctx, rt, p, e, false)
		entriesByName[childRel] = DirEntry{
			Name:        childRel,
			Ino:         entry.Ino,
			Mode:        dirEntryMode(entry.IsDir, entry.HasMode, entry.Mode),
			Size:        entry.Size,
			Mtime:       entry.Mtime,
			AttrMode:    entry.Mode,
			HasMode:     entry.HasMode,
			IsDir:       entry.IsDir,
			HasMetadata: true,
		}
	}
	for _, overlay := range overlays {
		p, e := overlay.path, overlay.entry
		if e.Op == "whiteout" {
			continue
		}
		if prefix != "" && !strings.HasPrefix(p, prefix) {
			continue
		}
		childRel := strings.TrimPrefix(p, prefix)
		if idx := strings.Index(childRel, "/"); idx > 0 {
			name := childRel[:idx]
			if _, whiteout := whiteouts[name]; whiteout {
				continue
			}
			if _, exists := entriesByName[name]; exists {
				continue
			}
			localPath := dirEntryChildPath(dirPath, name)
			entry := fs.gitInode(localPath, true, 0, 0o755, true, false)
			entriesByName[name] = DirEntry{Name: name, Ino: entry.Ino, Mode: dirEntryMode(true, true, 0o755), IsDir: true, HasMode: true, AttrMode: 0o755, HasMetadata: true}
		}
	}
	entries := make([]DirEntry, 0, len(entriesByName))
	for _, e := range entriesByName {
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return entries, true, nil
}

func (fs *Dat9FS) readGitFile(ctx context.Context, localPath string, offset, size int64) ([]byte, error) {
	rt, rel, ok := fs.gitWorkspaceForPath(ctx, localPath)
	if !ok || rel == "" {
		return nil, os.ErrNotExist
	}
	if e, ok := rt.overlayEntry(rel); ok {
		if e.Op == "whiteout" {
			return nil, os.ErrNotExist
		}
		if e.Kind == "dir" {
			return nil, syscall.EISDIR
		}
		if e.Op != "chmod" || len(e.Content) > 0 {
			return fs.readGitOverlayFile(ctx, rt, localPath, rel, e, offset, size)
		}
	}
	n, ok := rt.cleanNode(rel)
	if localTree, _ := fs.ensureGitLocalHeadTree(ctx, rt); localTree {
		n, ok = rt.localHeadNode(rel)
	}
	if !ok {
		return nil, os.ErrNotExist
	}
	if n.Kind == "dir" || n.Kind == "submodule" {
		return nil, syscall.EISDIR
	}
	return fs.readGitCleanFile(ctx, rt, localPath, rel, n, offset, size)
}

func gitWorkspaceOpenFlags(rt *gitWorkspaceRuntime, rel string, flags uint32) uint32 {
	accMode := flags & syscall.O_ACCMODE
	if accMode != syscall.O_RDONLY {
		return 0
	}
	// macFUSE mmap readers can SIGBUS on read-only DIRECT_IO handles. Git uses
	// mmap in porcelain paths such as diff/status, so keep read-only workspace
	// handles cacheable and rely on write/overlay updates to invalidate inodes.
	return gofuse.FOPEN_KEEP_CACHE
}

func (fs *Dat9FS) gitWorkspaceCurrentSize(ctx context.Context, rt *gitWorkspaceRuntime, rel string) (int64, bool) {
	if rt == nil || rel == "" {
		return 0, false
	}
	if info, ok := fs.gitWorkspaceDirtyMirrorStat(rt, rel); ok {
		return info.Size(), true
	}
	if e, ok := rt.overlayEntry(rel); ok {
		if e.Op == "whiteout" || e.Kind == "dir" || e.Kind == "submodule" {
			return 0, false
		}
		if e.Op != "chmod" || len(e.Content) > 0 {
			if len(e.Content) > 0 {
				return int64(len(e.Content)), true
			}
			return e.SizeBytes, true
		}
	}
	if localTree, _ := fs.ensureGitLocalHeadTree(ctx, rt); localTree {
		if n, ok := rt.localHeadNode(rel); ok {
			if n.Kind == "dir" || n.Kind == "submodule" {
				return 0, false
			}
			if n.SizeBytes < 0 {
				return fs.resolveGitCleanNodeSize(ctx, rt, rel, n), true
			}
			return n.SizeBytes, true
		}
	} else if n, ok := rt.cleanNode(rel); ok {
		if n.Kind == "dir" || n.Kind == "submodule" {
			return 0, false
		}
		if n.SizeBytes < 0 {
			return fs.resolveGitCleanNodeSize(ctx, rt, rel, n), true
		}
		return n.SizeBytes, true
	}
	return 0, false
}

func (fs *Dat9FS) gitWorkspaceDirtyMirrorPath(rt *gitWorkspaceRuntime, rel string) (string, bool) {
	if fs == nil || fs.opts == nil || rt == nil || rt.workspace.WorkspaceID == "" || rt.workspace.HeadCommit == "" {
		return "", false
	}
	localRoot := strings.TrimSpace(fs.opts.LocalRoot)
	if localRoot == "" {
		return "", false
	}
	cleanRel := strings.TrimPrefix(path.Clean("/"+rel), "/")
	if cleanRel == "" || cleanRel == "." || strings.HasPrefix(cleanRel, "../") {
		return "", false
	}
	return filepath.Join(localRoot, "git-workspaces", rt.workspace.WorkspaceID, rt.workspace.HeadCommit, "dirty", filepath.FromSlash(cleanRel)), true
}

func (fs *Dat9FS) gitWorkspaceDirtyMirrorStat(rt *gitWorkspaceRuntime, rel string) (os.FileInfo, bool) {
	mirrorPath, ok := fs.gitWorkspaceDirtyMirrorPath(rt, rel)
	if !ok {
		return nil, false
	}
	info, err := os.Stat(mirrorPath)
	if err != nil || info.IsDir() {
		return nil, false
	}
	return info, true
}

func (fs *Dat9FS) removeGitDirtyMirror(rt *gitWorkspaceRuntime, rel string) {
	mirrorPath, ok := fs.gitWorkspaceDirtyMirrorPath(rt, rel)
	if !ok {
		return
	}
	_ = os.Remove(mirrorPath)
}

func (fs *Dat9FS) replaceGitDirtyMirror(rt *gitWorkspaceRuntime, rel string, data []byte) {
	mirrorPath, ok := fs.gitWorkspaceDirtyMirrorPath(rt, rel)
	if !ok {
		return
	}
	if err := os.MkdirAll(filepath.Dir(mirrorPath), 0o755); err != nil {
		return
	}
	_ = os.WriteFile(mirrorPath, data, 0o644)
}

func (fs *Dat9FS) readGitDirtyMirror(rt *gitWorkspaceRuntime, rel string, offset, size int64) ([]byte, bool, error) {
	mirrorPath, ok := fs.gitWorkspaceDirtyMirrorPath(rt, rel)
	if !ok {
		return nil, false, nil
	}
	data, err := os.ReadFile(mirrorPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		return nil, true, err
	}
	return sliceRead(data, offset, size), true, nil
}

func (fs *Dat9FS) readGitOverlayFile(ctx context.Context, rt *gitWorkspaceRuntime, localPath, rel string, e client.GitOverlayEntry, offset, size int64) ([]byte, error) {
	if e.Kind == "dir" || e.Kind == "submodule" {
		return nil, syscall.EISDIR
	}
	if data, ok, err := fs.readGitDirtyMirror(rt, rel, 0, -1); err != nil {
		return nil, err
	} else if ok {
		e.Content = data
		e.SizeBytes = int64(len(data))
		fs.applyGitOverlayEntry(rt.workspace.WorkspaceID, e)
		if ino, ok := fs.inodes.GetInode(localPath); ok {
			fs.inodes.UpdateSize(ino, int64(len(data)))
		}
		return sliceRead(data, offset, size), nil
	}
	if len(e.Content) == 0 && e.SizeBytes > 0 {
		if fs == nil || fs.client == nil {
			return nil, syscall.EIO
		}
		loaded, err := fs.client.GetGitOverlayEntry(ctx, rt.workspace.WorkspaceID, rel)
		if err != nil {
			return nil, err
		}
		e = *loaded
		// The response may be a pre-whiteout snapshot (the GET raced an
		// unlink's whiteout commit). Never cache it back over an authoritative
		// whiteout — that would resurrect the deleted name locally and
		// mislead the open registration check. The fetched bytes are still
		// served to this reader: an open that raced the unlink keeps its
		// anonymous-fd reads.
		_, _ = fs.applyGitOverlayEntryUnlessWhiteout(rt.workspace.WorkspaceID, e)
	}
	if e.Op == "whiteout" {
		return nil, os.ErrNotExist
	}
	if e.Kind == "dir" || e.Kind == "submodule" {
		return nil, syscall.EISDIR
	}
	if len(e.Content) == 0 && e.SizeBytes > 0 {
		return nil, fmt.Errorf("git overlay %s has size %d but no local content", rel, e.SizeBytes)
	}
	if e.SizeBytes > int64(len(e.Content)) {
		return nil, fmt.Errorf("git overlay %s content shorter than metadata: got %d want %d", rel, len(e.Content), e.SizeBytes)
	}
	if e.SizeBytes != int64(len(e.Content)) {
		e.SizeBytes = int64(len(e.Content))
		_, _ = fs.applyGitOverlayEntryUnlessWhiteout(rt.workspace.WorkspaceID, e)
	}
	if ino, ok := fs.inodes.GetInode(localPath); ok {
		fs.inodes.UpdateSize(ino, int64(len(e.Content)))
	}
	return sliceRead(e.Content, offset, size), nil
}

func (fs *Dat9FS) readGitCleanFile(ctx context.Context, rt *gitWorkspaceRuntime, localPath, rel string, n client.GitTreeNode, offset, size int64) ([]byte, error) {
	if fs.perfEnabled() {
		fs.perf.gitCleanReadCount.add(1)
	}
	nodeCommit := gitNodeCommit(rt, n)
	objectSHA := gitObjectCacheKey(n.ObjectSHA)
	if localRoot := strings.TrimSpace(fs.opts.LocalRoot); localRoot != "" {
		data, hit, err := gitcache.ReadTreeFile(ctx, localRoot, rt.workspace.WorkspaceID, nodeCommit, rel, offset, size)
		if err != nil {
			return nil, err
		}
		if hit {
			if fs.perfEnabled() {
				fs.perf.gitCleanTreeHit.add(1)
			}
			if offset == 0 && size < 0 {
				fs.updateGitKnownSize(rt, localPath, rel, n, int64(len(data)))
			}
			return data, nil
		}
		data, hit, err = gitcache.ReadBlob(ctx, localRoot, rt.workspace.WorkspaceID, nodeCommit, objectSHA, offset, size)
		if err != nil {
			return nil, err
		}
		if hit {
			if fs.perfEnabled() {
				fs.perf.gitCleanBlobCacheHit.add(1)
			}
			if offset == 0 && size < 0 {
				fs.updateGitKnownSize(rt, localPath, rel, n, int64(len(data)))
			}
			return data, nil
		}
		baseCommit := strings.TrimSpace(rt.workspace.HeadCommit)
		if baseCommit != "" && !strings.EqualFold(baseCommit, nodeCommit) {
			data, hit, err = gitcache.ReadBlob(ctx, localRoot, rt.workspace.WorkspaceID, baseCommit, objectSHA, offset, size)
			if err != nil {
				return nil, err
			}
			if hit {
				if fs.perfEnabled() {
					fs.perf.gitCleanBlobCacheHit.add(1)
				}
				if offset == 0 && size < 0 {
					fs.updateGitKnownSize(rt, localPath, rel, n, int64(len(data)))
				}
				return data, nil
			}
		}
	}
	if fs.perfEnabled() {
		fs.perf.gitCleanCacheMiss.add(1)
	}
	data, err := fs.materializeGitBlob(ctx, rt, nodeCommit, objectSHA)
	if err != nil {
		return nil, err
	}
	fs.updateGitKnownSize(rt, localPath, rel, n, int64(len(data)))
	return sliceRead(data, offset, size), nil
}

func (fs *Dat9FS) updateGitKnownSize(rt *gitWorkspaceRuntime, localPath, rel string, n client.GitTreeNode, size int64) {
	if fs == nil || rt == nil || n.SizeBytes >= 0 || size < 0 {
		return
	}
	if ino, ok := fs.inodes.GetInode(localPath); ok {
		fs.inodes.UpdateSize(ino, size)
	}
	rt.updateCleanNodeSize(rel, gitNodeCommit(rt, n), size)
}

// carryGitKnownSizesIntoRuntime copies SizeBytes fills from the previous runtime
// snapshot for the same workspace id (matched by path + object SHA). Force
// refresh rebuilds runtimes from ListGitTree which re-introduces SizeBytes=-1
// for blobless trees; without this, getattr oscillates and git status reports
// phantom modifications.
func (fs *Dat9FS) carryGitKnownSizesIntoRuntime(rt *gitWorkspaceRuntime) {
	if fs == nil || fs.git == nil || rt == nil {
		return
	}
	wsID := strings.TrimSpace(rt.workspace.WorkspaceID)
	if wsID == "" {
		return
	}
	fs.git.mu.Lock()
	var prev *gitWorkspaceRuntime
	for _, candidate := range fs.git.workspaces {
		if candidate != nil && strings.TrimSpace(candidate.workspace.WorkspaceID) == wsID {
			prev = candidate
			break
		}
	}
	fs.git.mu.Unlock()
	if prev == nil {
		return
	}
	prev.mu.RLock()
	defer prev.mu.RUnlock()
	for rel, n := range rt.nodes {
		if n.SizeBytes >= 0 || !gitNodeHasBlobSize(n) {
			continue
		}
		old, ok := prev.nodes[rel]
		if !ok || old.SizeBytes < 0 {
			continue
		}
		if gitObjectCacheKey(old.ObjectSHA) != gitObjectCacheKey(n.ObjectSHA) {
			continue
		}
		rt.updateCleanNodeSize(rel, gitNodeCommit(rt, n), old.SizeBytes)
	}
}

func (fs *Dat9FS) resolveGitCleanNodeSize(ctx context.Context, rt *gitWorkspaceRuntime, rel string, n client.GitTreeNode) int64 {
	if n.SizeBytes >= 0 || n.Kind == "dir" || n.Kind == "submodule" {
		if n.Kind == "dir" {
			return 0
		}
		return n.SizeBytes
	}
	if ctx == nil {
		ctx = context.Background()
	}
	nodeCommit := gitNodeCommit(rt, n)
	objectSHA := gitObjectCacheKey(n.ObjectSHA)
	if localRoot := strings.TrimSpace(fs.opts.LocalRoot); localRoot != "" {
		if size, hit, err := gitcache.StatTreeFile(ctx, localRoot, rt.workspace.WorkspaceID, nodeCommit, rel); err == nil && hit {
			rt.updateCleanNodeSize(rel, nodeCommit, size)
			return size
		}
		if size, hit, err := gitcache.StatBlob(ctx, localRoot, rt.workspace.WorkspaceID, nodeCommit, objectSHA); err == nil && hit {
			rt.updateCleanNodeSize(rel, nodeCommit, size)
			return size
		}
		baseCommit := strings.TrimSpace(rt.workspace.HeadCommit)
		if baseCommit != "" && !strings.EqualFold(baseCommit, nodeCommit) {
			if size, hit, err := gitcache.StatBlob(ctx, localRoot, rt.workspace.WorkspaceID, baseCommit, objectSHA); err == nil && hit {
				rt.updateCleanNodeSize(rel, nodeCommit, size)
				return size
			}
		}
	}
	if gitWorkspaceIsFastBlobless(rt) {
		return n.SizeBytes
	}
	if objectSHA != "" {
		if size, err := fs.gitCatFileBlobSize(ctx, rt, objectSHA); err == nil {
			rt.updateCleanNodeSize(rel, nodeCommit, size)
			return size
		}
	}
	return n.SizeBytes
}

func gitNodeCommit(rt *gitWorkspaceRuntime, n client.GitTreeNode) string {
	if commit := strings.TrimSpace(n.CommitSHA); commit != "" {
		return commit
	}
	if rt == nil {
		return ""
	}
	return rt.workspace.HeadCommit
}

func (rt *gitWorkspaceRuntime) updateCleanNodeSize(rel, commit string, size int64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	commit = strings.TrimSpace(commit)
	shouldUpdate := func(n client.GitTreeNode, expectedCommit string) bool {
		if n.SizeBytes >= 0 {
			return false
		}
		if commit == "" {
			return true
		}
		if nodeCommit := strings.TrimSpace(n.CommitSHA); nodeCommit != "" {
			return strings.EqualFold(nodeCommit, commit)
		}
		expectedCommit = strings.TrimSpace(expectedCommit)
		return expectedCommit == "" || strings.EqualFold(expectedCommit, commit)
	}
	n, ok := rt.nodes[rel]
	if ok && shouldUpdate(n, rt.workspace.HeadCommit) {
		n.SizeBytes = size
		rt.nodes[rel] = n
		children := rt.children[n.ParentPath]
		for i := range children {
			if children[i].Path == rel {
				children[i].SizeBytes = size
				rt.children[n.ParentPath] = children
				break
			}
		}
	}
	n, ok = rt.localNodes[rel]
	if ok && shouldUpdate(n, rt.localTreeCommit) {
		n.SizeBytes = size
		rt.localNodes[rel] = n
		children := rt.localChildren[n.ParentPath]
		for i := range children {
			if children[i].Path == rel {
				children[i].SizeBytes = size
				rt.localChildren[n.ParentPath] = children
				break
			}
		}
	}
}

func (fs *Dat9FS) materializeGitBlob(ctx context.Context, rt *gitWorkspaceRuntime, commit, objectSHA string) ([]byte, error) {
	if fs == nil || fs.git == nil || rt == nil {
		return nil, os.ErrNotExist
	}
	commit = strings.TrimSpace(commit)
	if commit == "" {
		commit = rt.workspace.HeadCommit
	}
	key := rt.workspace.WorkspaceID + ":" + commit + ":" + objectSHA
	fs.git.mu.Lock()
	if fs.git.materialize == nil {
		fs.git.materialize = make(map[string]*gitMaterializeCall)
	}
	if call, ok := fs.git.materialize[key]; ok {
		fs.git.mu.Unlock()
		select {
		case <-call.done:
			return call.data, call.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	call := &gitMaterializeCall{done: make(chan struct{})}
	fs.git.materialize[key] = call
	fs.git.mu.Unlock()

	call.data, call.err = fs.materializeGitBlobOnce(ctx, rt, commit, objectSHA)
	close(call.done)

	fs.git.mu.Lock()
	delete(fs.git.materialize, key)
	fs.git.mu.Unlock()
	return call.data, call.err
}

func (fs *Dat9FS) materializeGitBlobOnce(ctx context.Context, rt *gitWorkspaceRuntime, commit, objectSHA string) ([]byte, error) {
	if localRoot := strings.TrimSpace(fs.opts.LocalRoot); localRoot != "" {
		data, hit, err := gitcache.ReadBlob(ctx, localRoot, rt.workspace.WorkspaceID, commit, objectSHA, 0, -1)
		if err != nil {
			return nil, err
		}
		if hit {
			if fs.perfEnabled() {
				fs.perf.gitCleanBlobCacheHit.add(1)
			}
			return data, nil
		}
	}
	data, err := fs.gitCatFileBlob(ctx, rt, objectSHA)
	if err != nil {
		return nil, err
	}
	if localRoot := strings.TrimSpace(fs.opts.LocalRoot); localRoot != "" {
		if err := gitcache.WriteBlob(ctx, localRoot, rt.workspace.WorkspaceID, commit, objectSHA, data); err != nil {
			return nil, err
		}
	}
	return data, nil
}

func (fs *Dat9FS) gitCatFileBlobSize(ctx context.Context, rt *gitWorkspaceRuntime, objectSHA string) (int64, error) {
	if err := fs.ensureGitStateRestored(ctx, rt); err != nil {
		return 0, err
	}
	gitDir, err := fs.gitDirForRuntime(rt)
	if err != nil {
		return 0, err
	}
	start := time.Now()
	if fs.perfEnabled() {
		fs.perf.gitCatFileCount.add(1)
	}
	cmd := exec.CommandContext(ctx, "git", "--git-dir", gitDir, "cat-file", "-s", objectSHA)
	setGitNoLazyEnv(cmd)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	dur := time.Since(start)
	if fs.perfEnabled() {
		fs.perf.gitCatFileTotalNS.add(uint64(dur))
		if dur >= 50*time.Millisecond {
			fs.perf.gitCatFileSlowCount.add(1)
		}
	}
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return 0, fmt.Errorf("git cat-file -s %s: %w: %s", objectSHA, err, msg)
		}
		return 0, err
	}
	size, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse git cat-file -s %s output %q: %w", objectSHA, strings.TrimSpace(string(out)), err)
	}
	return size, nil
}

func (fs *Dat9FS) gitCatFileBlob(ctx context.Context, rt *gitWorkspaceRuntime, objectSHA string) ([]byte, error) {
	if err := fs.ensureGitStateRestored(ctx, rt); err != nil {
		return nil, err
	}
	gitDir, err := fs.gitDirForRuntime(rt)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	if fs.perfEnabled() {
		fs.perf.gitCatFileCount.add(1)
	}
	cmd := exec.CommandContext(ctx, "git", "--git-dir", gitDir, "cat-file", "blob", objectSHA)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	data, err := cmd.Output()
	dur := time.Since(start)
	if fs.perfEnabled() {
		fs.perf.gitCatFileTotalNS.add(uint64(dur))
		if dur >= 50*time.Millisecond {
			fs.perf.gitCatFileSlowCount.add(1)
		}
	}
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return nil, fmt.Errorf("git cat-file blob %s: %w: %s", objectSHA, err, msg)
		}
		return nil, err
	}
	return data, nil
}

func sliceRead(data []byte, offset, size int64) []byte {
	if offset < 0 {
		return nil
	}
	if offset >= int64(len(data)) {
		return nil
	}
	end := int64(len(data))
	if size >= 0 {
		end = offset + size
	}
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	if end < offset {
		return nil
	}
	out := make([]byte, end-offset)
	copy(out, data[offset:end])
	return out
}

func (fs *Dat9FS) ensureGitStateRestored(ctx context.Context, rt *gitWorkspaceRuntime) error {
	if rt == nil || fs.localOverlay == nil {
		return nil
	}
	if rt.isLinked() {
		return fs.ensureLinkedGitStateRestored(ctx, rt)
	}
	rt.mu.RLock()
	if rt.restored {
		rt.mu.RUnlock()
		return nil
	}
	rt.mu.RUnlock()

	rt.restoreMu.Lock()
	defer rt.restoreMu.Unlock()
	rt.mu.RLock()
	if rt.restored {
		rt.mu.RUnlock()
		return nil
	}
	rt.mu.RUnlock()

	gitDir, err := fs.gitDirForRuntime(rt)
	if err != nil {
		return err
	}
	if gitDirLooksUsable(ctx, gitDir) {
		if err := fs.applyRestoredGitHeadOverlay(ctx, rt, gitDir); err != nil {
			return err
		}
		rt.mu.Lock()
		rt.restored = true
		rt.mu.Unlock()
		return nil
	}
	state, err := fs.client.GetGitState(ctx, rt.workspace.WorkspaceID)
	if err != nil {
		return err
	}
	if len(state.Content) == 0 {
		return fmt.Errorf("git workspace %s has no .git checkpoint", rt.workspace.WorkspaceID)
	}
	if err := fs.restoreGitStateAtomically(ctx, rt, gitDir, state); err != nil {
		return err
	}
	if !gitDirLooksUsable(ctx, gitDir) {
		return fmt.Errorf("git workspace %s restored unusable .git state", rt.workspace.WorkspaceID)
	}
	if err := fs.applyRestoredGitHeadOverlay(ctx, rt, gitDir); err != nil {
		return err
	}
	rt.mu.Lock()
	rt.restored = true
	rt.mu.Unlock()
	return nil
}

func (fs *Dat9FS) ensureLinkedGitStateRestored(ctx context.Context, rt *gitWorkspaceRuntime) error {
	rt.mu.RLock()
	if rt.restored {
		rt.mu.RUnlock()
		return nil
	}
	rt.mu.RUnlock()

	rt.restoreMu.Lock()
	defer rt.restoreMu.Unlock()
	rt.mu.RLock()
	if rt.restored {
		rt.mu.RUnlock()
		return nil
	}
	rt.mu.RUnlock()

	commonRT, err := fs.commonRuntimeForLinked(rt)
	if err != nil {
		return err
	}
	if err := fs.ensureGitStateRestored(ctx, commonRT); err != nil {
		return err
	}
	linkedGitDir, err := fs.linkedGitDir(rt, commonRT)
	if err != nil {
		return err
	}
	linkedGitFile, err := fs.localOverlay.abs(path.Join(rt.localRoot, ".git"))
	if err != nil {
		return err
	}
	if _, err := os.Lstat(linkedGitFile); err == nil && gitDirLooksUsable(ctx, linkedGitDir) {
		if err := fs.writeLinkedGitFile(rt, commonRT, linkedGitFile); err != nil {
			return err
		}
		if err := fs.applyRestoredGitHeadOverlay(ctx, rt, linkedGitDir); err != nil {
			return err
		}
		rt.mu.Lock()
		rt.restored = true
		rt.mu.Unlock()
		return nil
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	state, err := fs.client.GetGitState(ctx, rt.workspace.WorkspaceID)
	if err != nil {
		return err
	}
	if len(state.Content) == 0 {
		return fmt.Errorf("git workspace %s has no .git checkpoint", rt.workspace.WorkspaceID)
	}
	commonGitDir, err := fs.gitDirForRuntime(commonRT)
	if err != nil {
		return err
	}
	if err := fs.restoreGitObjectPacks(ctx, rt, commonGitDir); err != nil {
		return err
	}
	if err := fs.restoreLinkedGitStateAtomically(ctx, rt, commonRT, linkedGitDir, linkedGitFile, state); err != nil {
		return err
	}
	if !gitDirLooksUsable(ctx, linkedGitDir) {
		return fmt.Errorf("git workspace %s restored unusable linked .git state", rt.workspace.WorkspaceID)
	}
	if err := fs.applyRestoredGitHeadOverlay(ctx, rt, linkedGitDir); err != nil {
		return err
	}
	rt.mu.Lock()
	rt.restored = true
	rt.mu.Unlock()
	return nil
}

func gitDirLooksUsable(ctx context.Context, gitDir string) bool {
	info, err := os.Stat(gitDir)
	if err != nil || !info.IsDir() {
		return false
	}
	head, err := os.ReadFile(filepath.Join(gitDir, "HEAD"))
	if err != nil || strings.TrimSpace(string(head)) == "" {
		return false
	}
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(checkCtx, "git", "--git-dir", gitDir, "cat-file", "-e", "HEAD^{commit}")
	cmd.Env = append(os.Environ(), "GIT_NO_LAZY_FETCH=1")
	return cmd.Run() == nil
}

func (fs *Dat9FS) restoreGitStateAtomically(ctx context.Context, rt *gitWorkspaceRuntime, gitDir string, state *client.GitState) error {
	parent := filepath.Dir(gitDir)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return err
	}
	tmpRoot, err := os.MkdirTemp(parent, ".drive9-git-state-restore-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(tmpRoot) }()
	tmpGitDir := filepath.Join(tmpRoot, ".git")
	if state.StorageType == gitStateStorageTarGzNoObjects {
		if err := fs.restoreGitObjectsFromRemote(ctx, rt, tmpGitDir); err != nil {
			return err
		}
		if err := fs.restoreGitObjectPacks(ctx, rt, tmpGitDir); err != nil {
			return err
		}
	}
	if err := extractGitArchive(state.Content, tmpGitDir); err != nil {
		return err
	}
	if !gitDirLooksUsable(ctx, tmpGitDir) {
		return fmt.Errorf("restored git state is missing a usable HEAD")
	}
	if err := os.RemoveAll(gitDir); err != nil {
		return err
	}
	if err := os.Rename(tmpGitDir, gitDir); err != nil {
		return err
	}
	return nil
}

func (fs *Dat9FS) restoreLinkedGitStateAtomically(ctx context.Context, rt, commonRT *gitWorkspaceRuntime, linkedGitDir, linkedGitFile string, state *client.GitState) error {
	parent := filepath.Dir(linkedGitDir)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return err
	}
	tmpRoot, err := os.MkdirTemp(parent, ".drive9-linked-git-state-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(tmpRoot) }()
	tmpGitDir := filepath.Join(tmpRoot, rt.workspace.WorktreeName)
	if err := extractGitArchive(state.Content, tmpGitDir); err != nil {
		return err
	}
	commonGitDir, err := fs.gitDirForRuntime(commonRT)
	if err != nil {
		return err
	}
	commonDirRel, err := filepath.Rel(linkedGitDir, commonGitDir)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(tmpGitDir, "commondir"), []byte(filepath.ToSlash(commonDirRel)+"\n"), 0o644); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(linkedGitFile), 0o755); err != nil {
		return err
	}
	mountedGitFile := fs.mountedGitFileForRuntime(rt, linkedGitFile)
	if err := os.WriteFile(filepath.Join(tmpGitDir, "gitdir"), []byte(mountedGitFile+"\n"), 0o644); err != nil {
		return err
	}
	if err := os.RemoveAll(linkedGitDir); err != nil {
		return err
	}
	if err := os.Rename(tmpGitDir, linkedGitDir); err != nil {
		return err
	}
	return fs.writeLinkedGitFile(rt, commonRT, linkedGitFile)
}

func (fs *Dat9FS) restoreGitObjectsFromRemote(ctx context.Context, rt *gitWorkspaceRuntime, gitDir string) error {
	repoURL := strings.TrimSpace(rt.workspace.RepoURL)
	if repoURL == "" {
		return fmt.Errorf("git workspace %s has no repo URL for object restore", rt.workspace.WorkspaceID)
	}
	worktreeRoot := filepath.Dir(gitDir)
	parent := filepath.Dir(worktreeRoot)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return err
	}
	tmpWorktree, err := os.MkdirTemp(parent, ".drive9-git-restore-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(tmpWorktree) }()
	if err := os.RemoveAll(tmpWorktree); err != nil {
		return err
	}
	args := []string{"clone"}
	if rt.workspace.Mode == gitWorkspaceModeFastBlobless {
		args = append(args, "--filter=blob:none")
	}
	args = append(args, "--no-checkout", repoURL, tmpWorktree)
	cmd := exec.CommandContext(ctx, "git", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return fmt.Errorf("restore git objects from remote: %w: %s", err, msg)
		}
		return fmt.Errorf("restore git objects from remote: %w", err)
	}
	tmpGitDir := filepath.Join(tmpWorktree, ".git")
	if err := os.MkdirAll(worktreeRoot, 0o755); err != nil {
		return err
	}
	if err := os.RemoveAll(gitDir); err != nil {
		return err
	}
	if err := os.Rename(tmpGitDir, gitDir); err != nil {
		return err
	}
	return nil
}

func (fs *Dat9FS) restoreGitObjectPacks(ctx context.Context, rt *gitWorkspaceRuntime, gitDir string) error {
	packs, err := fs.client.ListGitObjectPacks(ctx, rt.workspace.WorkspaceID)
	if err != nil {
		if client.IsNotFound(err) {
			return nil
		}
		return err
	}
	for _, meta := range packs {
		pack, err := fs.client.GetGitObjectPack(ctx, rt.workspace.WorkspaceID, meta.PackID)
		if err != nil {
			return err
		}
		if len(pack.Content) == 0 {
			continue
		}
		if pack.SizeBytes != 0 && pack.SizeBytes != int64(len(pack.Content)) {
			return fmt.Errorf("git object pack %s size mismatch", pack.PackID)
		}
		sum := sha256.Sum256(pack.Content)
		checksum := hex.EncodeToString(sum[:])
		if pack.ChecksumSHA256 != "" && !strings.EqualFold(pack.ChecksumSHA256, checksum) {
			return fmt.Errorf("git object pack %s checksum mismatch", pack.PackID)
		}
		cmd := exec.CommandContext(ctx, "git", "--git-dir", gitDir, "unpack-objects", "-q")
		cmd.Stdin = bytes.NewReader(pack.Content)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			msg := strings.TrimSpace(stderr.String())
			if msg != "" {
				return fmt.Errorf("unpack git object pack %s: %w: %s", pack.PackID, err, msg)
			}
			return fmt.Errorf("unpack git object pack %s: %w", pack.PackID, err)
		}
	}
	return nil
}

type gitHeadTreeEntry struct {
	path string
	kind string
	mode string
	oid  string
	size int64
}

func (fs *Dat9FS) applyRestoredGitHeadOverlay(ctx context.Context, rt *gitWorkspaceRuntime, gitDir string) error {
	if rt == nil {
		return nil
	}
	overlay, err := buildRestoredGitHeadOverlay(ctx, gitDir, rt)
	if err != nil || len(overlay) == 0 {
		return err
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.overlay == nil {
		rt.overlay = make(map[string]client.GitOverlayEntry)
	}
	for rel, entry := range overlay {
		if _, exists := rt.overlay[rel]; exists {
			continue
		}
		rt.overlay[rel] = entry
	}
	return nil
}

func buildRestoredGitHeadOverlay(ctx context.Context, gitDir string, rt *gitWorkspaceRuntime) (map[string]client.GitOverlayEntry, error) {
	if rt == nil {
		return nil, nil
	}
	head, err := gitCommandOutputNoLazy(ctx, "--git-dir", gitDir, "rev-parse", "--verify", "HEAD")
	if err != nil {
		return nil, err
	}
	headCommit := strings.ToLower(strings.TrimSpace(string(head)))
	if headCommit == "" || strings.EqualFold(headCommit, strings.TrimSpace(rt.workspace.HeadCommit)) {
		return nil, nil
	}

	baseNodes, existingOverlay := rt.gitHeadOverlayInputs()
	headTree, err := gitHeadTreeEntries(ctx, gitDir)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	out := make(map[string]client.GitOverlayEntry)
	for rel, headEntry := range headTree {
		if _, exists := existingOverlay[rel]; exists {
			continue
		}
		base, hasBase := baseNodes[rel]
		if hasBase && gitHeadEntryMatchesBase(headEntry, base) {
			continue
		}
		entry := client.GitOverlayEntry{
			WorkspaceID:   rt.workspace.WorkspaceID,
			Path:          rel,
			Op:            "upsert",
			Kind:          headEntry.kind,
			Mode:          headEntry.mode,
			SizeBytes:     headEntry.size,
			BaseObjectSHA: base.ObjectSHA,
			CreatedAt:     now,
			UpdatedAt:     now,
		}
		if headEntry.kind == "file" || headEntry.kind == "symlink" {
			object, err := gitRestoredHeadBlobInfo(ctx, gitDir, headEntry.oid, rel)
			if err != nil {
				return nil, err
			}
			if object.size > gitLocalObjectMaxBlobBytes {
				return nil, fmt.Errorf("git HEAD tree object %s for %s is %d bytes, exceeds local restore limit %d", headEntry.oid, rel, object.size, gitLocalObjectMaxBlobBytes)
			}
			content, err := gitCatBlobNoLazy(ctx, gitDir, headEntry.oid)
			if err != nil {
				return nil, err
			}
			sum := sha256.Sum256(content)
			entry.Content = content
			entry.SizeBytes = int64(len(content))
			entry.ChecksumSHA256 = hex.EncodeToString(sum[:])
		}
		out[rel] = entry
	}
	for rel, base := range baseNodes {
		if _, exists := existingOverlay[rel]; exists {
			continue
		}
		if _, exists := headTree[rel]; exists {
			continue
		}
		out[rel] = client.GitOverlayEntry{
			WorkspaceID:   rt.workspace.WorkspaceID,
			Path:          rel,
			Op:            "whiteout",
			Kind:          base.Kind,
			Mode:          base.Mode,
			BaseObjectSHA: base.ObjectSHA,
			CreatedAt:     now,
			UpdatedAt:     now,
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func gitRestoredHeadBlobInfo(ctx context.Context, gitDir, oid, rel string) (gitObjectInfo, error) {
	info, err := gitObjectInfoBatch(ctx, gitDir, []string{oid})
	if err != nil {
		return gitObjectInfo{}, err
	}
	object := info[oid]
	if object.typ == "" || object.typ == "missing" {
		return gitObjectInfo{}, fmt.Errorf("git HEAD tree object %s for %s is missing", oid, rel)
	}
	if object.typ != "blob" {
		return gitObjectInfo{}, fmt.Errorf("git HEAD tree object %s for %s is %s, want blob", oid, rel, object.typ)
	}
	return object, nil
}

func (rt *gitWorkspaceRuntime) gitHeadOverlayInputs() (map[string]client.GitTreeNode, map[string]struct{}) {
	baseNodes := make(map[string]client.GitTreeNode)
	existingOverlay := make(map[string]struct{})
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	for rel, node := range rt.nodes {
		if node.Kind == "dir" {
			continue
		}
		baseNodes[rel] = node
	}
	for rel := range rt.overlay {
		existingOverlay[rel] = struct{}{}
	}
	return baseNodes, existingOverlay
}

func gitHeadTreeEntries(ctx context.Context, gitDir string) (map[string]gitHeadTreeEntry, error) {
	out, err := gitCommandOutputNoLazy(ctx, "--git-dir", gitDir, "ls-tree", "-r", "-z", "--full-tree", "HEAD")
	if err != nil {
		return nil, err
	}
	entries := make(map[string]gitHeadTreeEntry)
	for _, rec := range bytes.Split(out, []byte{0}) {
		if len(rec) == 0 {
			continue
		}
		tab := bytes.IndexByte(rec, '\t')
		if tab < 0 {
			continue
		}
		meta := strings.Fields(string(rec[:tab]))
		if len(meta) < 3 {
			continue
		}
		rel := strings.Trim(filepath.ToSlash(string(rec[tab+1:])), "/")
		if rel == "" || rel == ".git" || strings.HasPrefix(rel, ".git/") {
			continue
		}
		mode, typ, oid := meta[0], meta[1], strings.ToLower(meta[2])
		kind := gitHeadTreeKind(mode, typ)
		if kind == "" {
			continue
		}
		entries[rel] = gitHeadTreeEntry{path: rel, kind: kind, mode: mode, oid: oid, size: -1}
	}
	return entries, nil
}

func gitHeadTreeKind(mode, typ string) string {
	switch {
	case typ == "blob" && mode == "120000":
		return "symlink"
	case typ == "blob":
		return "file"
	case typ == "commit":
		return "submodule"
	default:
		return ""
	}
}

func gitHeadEntryMatchesBase(head gitHeadTreeEntry, base client.GitTreeNode) bool {
	return strings.EqualFold(head.oid, base.ObjectSHA) &&
		head.kind == base.Kind &&
		head.mode == base.Mode
}

func gitCatBlobNoLazy(ctx context.Context, gitDir, objectSHA string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", "--git-dir", gitDir, "cat-file", "blob", objectSHA)
	setGitNoLazyEnv(cmd)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	data, err := cmd.Output()
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return nil, fmt.Errorf("git cat-file blob %s: %w: %s", objectSHA, err, msg)
		}
		return nil, fmt.Errorf("git cat-file blob %s: %w", objectSHA, err)
	}
	return data, nil
}

func (fs *Dat9FS) ensureGitStateForLocalPath(ctx context.Context, localPath string) error {
	if fs == nil || fs.git == nil || fs.opts == nil || !fs.opts.EnableGitWorkspaces {
		return nil
	}
	if ctx == nil {
		ctx = context.TODO()
	}
	// Reuse on-demand arm path; do not list while dormant.
	rt, rel, ok := fs.gitWorkspaceForPath(ctx, localPath)
	if !ok || rt == nil {
		return nil
	}
	// Prefer linked-aware git state path resolution when available.
	if linked, linkedRel, linkedOK := fs.loadedGitWorkspaceForGitStatePath(localPath); linkedOK {
		rt, rel = linked, linkedRel
	}
	if rel == ".git" || strings.HasPrefix(rel, ".git/") {
		return fs.ensureGitStateRestored(ctx, rt)
	}
	return nil
}

func (fs *Dat9FS) maybeStartGitWorkspaceHydrate(rt *gitWorkspaceRuntime) {
	if fs == nil || fs.git == nil || rt == nil || rt.workspace.Mode != gitWorkspaceModeFastBlobless {
		return
	}
	if strings.TrimSpace(fs.opts.LocalRoot) == "" {
		return
	}
	key := gitWorkspaceCacheKey(rt)
	fs.git.mu.Lock()
	if fs.git.hydrateStarted == nil {
		fs.git.hydrateStarted = make(map[string]struct{})
	}
	if _, ok := fs.git.hydrateStarted[key]; ok {
		fs.git.mu.Unlock()
		return
	}
	fs.git.hydrateStarted[key] = struct{}{}
	fs.git.mu.Unlock()

	go fs.runGitWorkspaceHydrate(rt)
}

func gitWorkspaceCacheKey(rt *gitWorkspaceRuntime) string {
	if rt == nil {
		return ""
	}
	return rt.workspace.WorkspaceID + ":" + rt.workspace.HeadCommit
}

func (fs *Dat9FS) runGitWorkspaceHydrate(rt *gitWorkspaceRuntime) {
	ctx, cancel := context.WithTimeout(context.Background(), gitWorkspaceHydrateTimeout)
	defer cancel()
	if fs.perfEnabled() {
		fs.perf.gitHydrateStart.add(1)
	}

	if err := fs.ensureGitStateRestored(ctx, rt); err != nil {
		if fs.perfEnabled() {
			fs.perf.gitHydrateFailure.add(1)
		}
		safeLogPrintf("git workspace hydrate restore failed for %s: %v", rt.workspace.RootPath, err)
		return
	}
	gitDir := ""
	if p, err := fs.gitDirForRuntime(rt); err == nil {
		gitDir = p
	}
	result, err := gitcache.Hydrate(ctx, gitcache.HydrateOptions{
		LocalRoot:   fs.opts.LocalRoot,
		WorkspaceID: rt.workspace.WorkspaceID,
		Commit:      rt.workspace.HeadCommit,
		RepoURL:     rt.workspace.RepoURL,
		GitDir:      gitDir,
		Token:       gitWorkspaceHydrateToken(rt.workspace.RepoURL),
		TreeEntries: gitHydrateEntriesFromRuntime(rt),
	})
	if fs.perfEnabled() {
		fs.perf.gitHydrateBytes.add(uint64(result.Bytes))
		fs.perf.gitHydrateTotalNS.add(uint64(result.Duration))
		fs.perf.gitHydrateObjects.add(uint64(result.Objects))
		fs.perf.gitHydrateObjectBytes.add(uint64(result.ObjectBytes))
		fs.perf.gitHydrateObjectSkipped.add(uint64(result.ObjectSkipped))
		fs.perf.gitHydrateObjectMismatch.add(uint64(result.ObjectMismatch))
		fs.perf.gitHydrateObjectFallbacks.add(uint64(result.ObjectFallbacks))
		if err != nil {
			fs.perf.gitHydrateFailure.add(1)
		} else {
			fs.perf.gitHydrateSuccess.add(1)
		}
	}
	if err != nil {
		safeLogPrintf("git workspace hydrate failed for %s: %v", rt.workspace.RootPath, err)
	}
}

func gitHydrateEntriesFromRuntime(rt *gitWorkspaceRuntime) []gitcache.HydrateTreeEntry {
	if rt == nil {
		return nil
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	entries := make([]gitcache.HydrateTreeEntry, 0, len(rt.nodes))
	for _, n := range rt.nodes {
		entries = append(entries, gitcache.HydrateTreeEntry{
			Path:      n.Path,
			Kind:      n.Kind,
			Mode:      n.Mode,
			ObjectSHA: n.ObjectSHA,
		})
	}
	return entries
}

func gitWorkspaceHydrateToken(repoURL string) string {
	for _, key := range []string{"GITHUB_TOKEN", "GH_TOKEN"} {
		if token := strings.TrimSpace(os.Getenv(key)); token != "" {
			return token
		}
	}
	return ""
}

func extractGitArchive(content []byte, dst string) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	gz, err := gzip.NewReader(bytes.NewReader(content))
	if err != nil {
		return err
	}
	defer func() { _ = gz.Close() }()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		target, err := safeGitArchiveTarget(dst, hdr.Name, hdr.Typeflag != tar.TypeSymlink)
		if err != nil {
			return err
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(hdr.Mode)&0o777); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode)&0o777)
			if err != nil {
				return err
			}
			_, copyErr := io.Copy(f, tr)
			closeErr := f.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
		case tar.TypeSymlink:
			if err := validateGitArchiveLinkname(hdr.Linkname); err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			_ = os.Remove(target)
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				return err
			}
		}
	}
}

func safeGitArchiveTarget(dst, name string, includeTarget bool) (string, error) {
	if name == "" || strings.ContainsRune(name, '\x00') {
		return "", fmt.Errorf("unsafe git archive path %q", name)
	}
	for _, part := range strings.Split(filepath.ToSlash(name), "/") {
		if part == ".." {
			return "", fmt.Errorf("unsafe git archive path %q", name)
		}
	}
	cleanName := filepath.Clean(filepath.FromSlash(name))
	if cleanName == "." || filepath.IsAbs(cleanName) || cleanName == ".." || strings.HasPrefix(cleanName, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("unsafe git archive path %q", name)
	}
	root := filepath.Clean(dst)
	target := filepath.Join(root, cleanName)
	if target != root && !strings.HasPrefix(target, root+string(filepath.Separator)) {
		return "", fmt.Errorf("unsafe git archive path %q", name)
	}
	if err := ensureNoGitArchiveSymlinkTraversal(root, target, includeTarget); err != nil {
		return "", err
	}
	return target, nil
}

func ensureNoGitArchiveSymlinkTraversal(root, target string, includeTarget bool) error {
	rel, err := filepath.Rel(root, target)
	if err != nil {
		return err
	}
	if rel == "." {
		return nil
	}
	parts := strings.Split(rel, string(filepath.Separator))
	limit := len(parts)
	if !includeTarget {
		limit--
	}
	current := root
	for i := 0; i < limit; i++ {
		current = filepath.Join(current, parts[i])
		info, err := os.Lstat(current)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("unsafe git archive path %q traverses symlink", rel)
		}
	}
	return nil
}

func validateGitArchiveLinkname(linkname string) error {
	if linkname == "" || strings.ContainsRune(linkname, '\x00') {
		return fmt.Errorf("unsafe git archive symlink target %q", linkname)
	}
	for _, part := range strings.Split(filepath.ToSlash(linkname), "/") {
		if part == ".." {
			return fmt.Errorf("unsafe git archive symlink target %q", linkname)
		}
	}
	cleanName := filepath.Clean(filepath.FromSlash(linkname))
	if filepath.IsAbs(cleanName) || cleanName == ".." || strings.HasPrefix(cleanName, ".."+string(filepath.Separator)) {
		return fmt.Errorf("unsafe git archive symlink target %q", linkname)
	}
	return nil
}

func (fs *Dat9FS) scheduleGitStateCheckpoint(localPath string) {
	if fs == nil || fs.gitCheckpoints == nil || !localPathShouldCheckpointGitState(localPath) {
		return
	}
	rt, rel, ok := fs.loadedGitWorkspaceForGitStatePath(localPath)
	if !ok || (rel != ".git" && !strings.HasPrefix(rel, ".git/")) {
		return
	}
	rt.clearLocalHeadTree()
	key := gitWorkspaceCacheKey(rt)
	fs.gitCheckpoints.Schedule(key, func() {
		ctx, cancel := context.WithTimeout(context.Background(), gitCheckpointTimeout)
		defer cancel()
		if err := fs.checkpointGitStateForRuntime(ctx, rt); err != nil {
			safeLogPrintf("git state checkpoint failed for workspace %s root %s: %v", rt.workspace.WorkspaceID, rt.workspace.RootPath, err)
		}
	})
}

func (fs *Dat9FS) checkpointGitStateForPath(ctx context.Context, localPath string) error {
	if fs == nil || !localPathShouldCheckpointGitState(localPath) {
		return nil
	}
	rt, rel, ok := fs.loadedGitWorkspaceForGitStatePath(localPath)
	if !ok || (rel != ".git" && !strings.HasPrefix(rel, ".git/")) {
		return nil
	}
	rt.clearLocalHeadTree()
	if fs.gitCheckpoints != nil {
		fs.gitCheckpoints.Cancel(gitWorkspaceCacheKey(rt))
	}
	return fs.checkpointGitStateForRuntime(ctx, rt)
}

func (fs *Dat9FS) checkpointGitStateAfterLocalWrite(ctx context.Context, localPath string, durable bool) error {
	if durable {
		return fs.checkpointGitStateForPath(ctx, localPath)
	}
	fs.scheduleGitStateCheckpoint(localPath)
	return nil
}

func (fs *Dat9FS) drainGitStateCheckpoints() {
	if fs == nil || fs.gitCheckpoints == nil {
		return
	}
	fs.gitCheckpoints.FlushAll()
}

func (fs *Dat9FS) checkpointAllGitWorkspaces() {
	if fs == nil || fs.git == nil {
		return
	}
	fs.git.mu.Lock()
	workspaces := make([]*gitWorkspaceRuntime, len(fs.git.workspaces))
	copy(workspaces, fs.git.workspaces)
	fs.git.mu.Unlock()
	if len(workspaces) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), gitCheckpointTimeout)
	defer cancel()
	for _, rt := range workspaces {
		if err := fs.checkpointGitStateForRuntime(ctx, rt); err != nil {
			safeLogPrintf("git state final checkpoint failed for workspace %s root %s: %v", rt.workspace.WorkspaceID, rt.workspace.RootPath, err)
		}
	}
}

func (fs *Dat9FS) checkpointGitStateForRuntime(ctx context.Context, rt *gitWorkspaceRuntime) error {
	if fs == nil || fs.git == nil || fs.client == nil || fs.localOverlay == nil || rt == nil {
		return nil
	}
	gitDir, err := fs.gitDirForRuntime(rt)
	if err != nil {
		return err
	}
	if _, err := os.Stat(gitDir); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	fs.git.checkpoint.Lock()
	defer fs.git.checkpoint.Unlock()

	pack, sanitize, err := buildLocalGitObjectPack(ctx, gitDir, rt)
	if err != nil {
		return err
	}
	if len(pack) > 0 {
		if _, err := fs.client.PutGitObjectPack(ctx, rt.workspace.WorkspaceID, client.GitObjectPackRequest{Content: pack}); err != nil {
			return err
		}
	}
	content, err := archiveLocalGitStateForCheckpoint(ctx, gitDir, rt, sanitize)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(content)
	_, err = fs.client.UpsertGitState(ctx, rt.workspace.WorkspaceID, client.GitStateRequest{
		CheckpointCommit: rt.workspace.HeadCommit,
		StorageType:      gitStateStorageTarGzNoObjects,
		ChecksumSHA256:   hex.EncodeToString(sum[:]),
		SizeBytes:        int64(len(content)),
		Content:          content,
	})
	return err
}

type gitCheckpointSanitization struct {
	indexRestores []gitIndexRestore
	dropLocalRefs bool
}

type gitIndexRestore struct {
	path      string
	mode      string
	objectSHA string
}

type gitObjectInfo struct {
	typ  string
	size int64
}

func buildLocalGitObjectPack(ctx context.Context, gitDir string, rt *gitWorkspaceRuntime) ([]byte, gitCheckpointSanitization, error) {
	var sanitize gitCheckpointSanitization

	refObjects, refOversize, err := collectLocalRefObjects(ctx, gitDir, rt)
	if err != nil {
		return nil, sanitize, err
	}
	if refOversize {
		refObjects = nil
		sanitize.dropLocalRefs = true
	}

	stagedObjects, stagedAllRestores, stagedRestores, err := collectStagedLocalObjects(ctx, gitDir, rt)
	if err != nil {
		return nil, sanitize, err
	}
	sanitize.indexRestores = append(sanitize.indexRestores, stagedRestores...)

	pack, err := packGitObjects(ctx, gitDir, mergeObjectSets(refObjects, stagedObjects))
	if err != nil {
		return nil, sanitize, err
	}
	if int64(len(pack)) > gitLocalObjectMaxPackBytes {
		pack, err = packGitObjects(ctx, gitDir, refObjects)
		if err != nil {
			return nil, sanitize, err
		}
		sanitize.indexRestores = mergeIndexRestores(sanitize.indexRestores, stagedAllRestores)
	}
	if int64(len(pack)) > gitLocalObjectMaxPackBytes {
		sanitize.dropLocalRefs = true
		pack, err = packGitObjects(ctx, gitDir, stagedObjects)
		if err != nil {
			return nil, sanitize, err
		}
		if int64(len(pack)) > gitLocalObjectMaxPackBytes {
			pack = nil
			sanitize.indexRestores = mergeIndexRestores(sanitize.indexRestores, stagedAllRestores)
		} else {
			sanitize.indexRestores = stagedRestores
		}
	}
	return pack, sanitize, nil
}

func collectLocalRefObjects(ctx context.Context, gitDir string, rt *gitWorkspaceRuntime) (map[string]struct{}, bool, error) {
	tips, err := collectLocalRefTips(ctx, gitDir)
	if err != nil {
		return nil, false, err
	}
	if len(tips) == 0 {
		return nil, false, nil
	}
	objects := make(map[string]struct{})
	seenCommits := make(map[string]struct{})
	seenTrees := make(map[string]struct{})
	baseCommit := ""
	if rt != nil {
		baseCommit = strings.ToLower(strings.TrimSpace(rt.workspace.HeadCommit))
	}
	for _, tip := range tips {
		oversize, err := collectLocalCommitObjects(ctx, gitDir, rt, tip, baseCommit, objects, seenCommits, seenTrees)
		if err != nil {
			return nil, false, err
		}
		if oversize {
			return nil, true, nil
		}
	}
	if len(objects) == 0 {
		return nil, false, nil
	}
	return objects, false, nil
}

func collectStagedLocalObjects(ctx context.Context, gitDir string, rt *gitWorkspaceRuntime) (map[string]struct{}, []gitIndexRestore, []gitIndexRestore, error) {
	out, err := gitCommandOutput(ctx, "--git-dir", gitDir, "ls-files", "-s", "-z")
	if err != nil {
		return nil, nil, nil, err
	}
	type stagedBlob struct {
		oid  string
		path string
	}
	var staged []stagedBlob
	for _, rec := range bytes.Split(out, []byte{0}) {
		if len(rec) == 0 {
			continue
		}
		tab := bytes.IndexByte(rec, '\t')
		if tab < 0 {
			continue
		}
		meta := strings.Fields(string(rec[:tab]))
		if len(meta) < 3 {
			continue
		}
		stage := meta[2]
		if stage != "0" {
			continue
		}
		oid := meta[1]
		rel := string(rec[tab+1:])
		if rt != nil {
			if n, ok := rt.cleanNode(rel); ok && strings.EqualFold(n.ObjectSHA, oid) {
				continue
			}
		}
		staged = append(staged, stagedBlob{oid: oid, path: rel})
	}
	if len(staged) == 0 {
		return nil, nil, nil, nil
	}
	ids := make([]string, 0, len(staged))
	for _, blob := range staged {
		ids = append(ids, blob.oid)
	}
	info, err := gitObjectInfoBatch(ctx, gitDir, ids)
	if err != nil {
		return nil, nil, nil, err
	}
	objects := make(map[string]struct{})
	allRestores := make([]gitIndexRestore, 0, len(staged))
	var restores []gitIndexRestore
	for _, blob := range staged {
		restore := gitIndexRestoreForPath(rt, blob.path)
		allRestores = append(allRestores, restore)
		object := info[blob.oid]
		if object.typ == "" || object.typ == "missing" {
			restores = append(restores, restore)
			continue
		}
		if object.typ != "blob" {
			restores = append(restores, restore)
			continue
		}
		if object.size > gitLocalObjectMaxBlobBytes {
			restores = append(restores, restore)
			continue
		}
		objects[blob.oid] = struct{}{}
	}
	if len(objects) == 0 {
		objects = nil
	}
	return objects, allRestores, restores, nil
}

func collectLocalRefTips(ctx context.Context, gitDir string) ([]string, error) {
	seen := make(map[string]struct{})
	add := func(raw string) {
		fields := strings.Fields(raw)
		if len(fields) == 0 {
			return
		}
		oid := strings.ToLower(strings.TrimSpace(fields[0]))
		if !isGitObjectID(oid) {
			return
		}
		seen[oid] = struct{}{}
	}
	if head, err := gitCommandOutputNoLazy(ctx, "--git-dir", gitDir, "rev-parse", "--verify", "HEAD"); err == nil {
		add(string(head))
	} else if !isMissingGitObjectError(err) {
		return nil, err
	}
	out, err := gitCommandOutputNoLazy(ctx, "--git-dir", gitDir, "for-each-ref", "--format=%(objectname)", "refs/heads", "refs/stash")
	if err != nil {
		if !isMissingGitObjectError(err) {
			return nil, err
		}
	} else {
		for _, line := range strings.Split(string(out), "\n") {
			add(line)
		}
	}
	tips := mapKeys(seen)
	sort.Strings(tips)
	return tips, nil
}

func collectLocalCommitObjects(ctx context.Context, gitDir string, rt *gitWorkspaceRuntime, tip, baseCommit string, objects map[string]struct{}, seenCommits, seenTrees map[string]struct{}) (bool, error) {
	tip = strings.ToLower(strings.TrimSpace(tip))
	if !isGitObjectID(tip) || tip == baseCommit {
		return false, nil
	}
	stack := []string{tip}
	for len(stack) > 0 {
		oid := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if oid == "" || oid == baseCommit {
			continue
		}
		if _, ok := seenCommits[oid]; ok {
			continue
		}
		typ, ok, err := gitObjectTypeNoLazy(ctx, gitDir, oid)
		if err != nil {
			return false, err
		}
		if !ok {
			continue
		}
		switch typ {
		case "commit":
			seenCommits[oid] = struct{}{}
			objects[oid] = struct{}{}
			tree, parents, err := gitCommitTreeAndParentsNoLazy(ctx, gitDir, oid)
			if err != nil {
				return false, err
			}
			if tree != "" {
				oversize, err := collectTreeObjectsNoLazy(ctx, gitDir, rt, tree, "", objects, seenTrees)
				if err != nil || oversize {
					return oversize, err
				}
			}
			for _, parent := range parents {
				parent = strings.ToLower(strings.TrimSpace(parent))
				if parent != "" && parent != baseCommit {
					stack = append(stack, parent)
				}
			}
		case "tag":
			target, err := gitTagTargetNoLazy(ctx, gitDir, oid)
			if err != nil {
				return false, err
			}
			if target != "" && target != baseCommit {
				stack = append(stack, target)
			}
		}
	}
	return false, nil
}

func collectTreeObjectsNoLazy(ctx context.Context, gitDir string, rt *gitWorkspaceRuntime, treeOID, relDir string, objects map[string]struct{}, seenTrees map[string]struct{}) (bool, error) {
	treeOID = strings.ToLower(strings.TrimSpace(treeOID))
	if !isGitObjectID(treeOID) {
		return false, nil
	}
	relDir = strings.Trim(filepath.ToSlash(relDir), "/")
	if relDir != "" {
		if rt != nil {
			if n, ok := rt.cleanNode(relDir); ok && n.Kind == "dir" && strings.EqualFold(n.ObjectSHA, treeOID) {
				return false, nil
			}
		}
	}
	if _, ok := seenTrees[treeOID]; ok {
		return false, nil
	}
	seenTrees[treeOID] = struct{}{}
	objects[treeOID] = struct{}{}

	out, err := gitCommandOutputNoLazy(ctx, "--git-dir", gitDir, "ls-tree", "-z", treeOID)
	if err != nil {
		return false, err
	}
	for _, rec := range bytes.Split(out, []byte{0}) {
		if len(rec) == 0 {
			continue
		}
		tab := bytes.IndexByte(rec, '\t')
		if tab < 0 {
			continue
		}
		meta := strings.Fields(string(rec[:tab]))
		if len(meta) < 3 {
			continue
		}
		kind := meta[1]
		oid := strings.ToLower(meta[2])
		name := string(rec[tab+1:])
		entryRel := name
		if relDir != "" {
			entryRel = path.Join(relDir, name)
		}
		switch kind {
		case "tree":
			if rt != nil {
				if n, ok := rt.cleanNode(entryRel); ok && n.Kind == "dir" && strings.EqualFold(n.ObjectSHA, oid) {
					continue
				}
			}
			oversize, err := collectTreeObjectsNoLazy(ctx, gitDir, rt, oid, entryRel, objects, seenTrees)
			if err != nil || oversize {
				return oversize, err
			}
		case "blob":
			if rt != nil {
				if n, ok := rt.cleanNode(entryRel); ok && strings.EqualFold(n.ObjectSHA, oid) {
					continue
				}
			}
			info, err := gitObjectInfoBatch(ctx, gitDir, []string{oid})
			if err != nil {
				return false, err
			}
			object := info[oid]
			if object.typ == "" || object.typ == "missing" {
				continue
			}
			if object.typ != "blob" {
				continue
			}
			if object.size > gitLocalObjectMaxBlobBytes {
				return true, nil
			}
			objects[oid] = struct{}{}
		}
	}
	return false, nil
}

func gitObjectTypeNoLazy(ctx context.Context, gitDir, oid string) (string, bool, error) {
	out, err := gitCommandOutputNoLazy(ctx, "--git-dir", gitDir, "cat-file", "-t", oid)
	if err != nil {
		if isMissingGitObjectError(err) {
			return "", false, nil
		}
		return "", false, err
	}
	return strings.TrimSpace(string(out)), true, nil
}

func gitCommitTreeAndParentsNoLazy(ctx context.Context, gitDir, oid string) (string, []string, error) {
	out, err := gitCommandOutputNoLazy(ctx, "--git-dir", gitDir, "cat-file", "-p", oid)
	if err != nil {
		return "", nil, err
	}
	var tree string
	var parents []string
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "tree ") {
			tree = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(line, "tree ")))
		} else if strings.HasPrefix(line, "parent ") {
			parent := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(line, "parent ")))
			if isGitObjectID(parent) {
				parents = append(parents, parent)
			}
		} else if line == "" {
			break
		}
	}
	return tree, parents, nil
}

func gitTagTargetNoLazy(ctx context.Context, gitDir, oid string) (string, error) {
	out, err := gitCommandOutputNoLazy(ctx, "--git-dir", gitDir, "cat-file", "-p", oid)
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "object ") {
			target := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(line, "object ")))
			if isGitObjectID(target) {
				return target, nil
			}
			return "", nil
		}
		if line == "" {
			break
		}
	}
	return "", nil
}

func gitIndexRestoreForPath(rt *gitWorkspaceRuntime, rel string) gitIndexRestore {
	restore := gitIndexRestore{path: rel}
	if node, ok := rt.cleanNode(rel); ok {
		restore.mode = node.Mode
		restore.objectSHA = node.ObjectSHA
	}
	return restore
}

func mergeIndexRestores(restores ...[]gitIndexRestore) []gitIndexRestore {
	byPath := make(map[string]gitIndexRestore)
	for _, list := range restores {
		for _, restore := range list {
			byPath[restore.path] = restore
		}
	}
	out := make([]gitIndexRestore, 0, len(byPath))
	for _, restore := range byPath {
		out = append(out, restore)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
	return out
}

func gitObjectInfoBatch(ctx context.Context, gitDir string, ids []string) (map[string]gitObjectInfo, error) {
	out := make(map[string]gitObjectInfo, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	cmd := exec.CommandContext(ctx, "git", "--git-dir", gitDir, "cat-file", "--batch-check=%(objectname) %(objecttype) %(objectsize)")
	setGitNoLazyEnv(cmd)
	cmd.Stdin = strings.NewReader(strings.Join(ids, "\n") + "\n")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return nil, fmt.Errorf("git cat-file --batch-check: %w: %s", err, msg)
		}
		return nil, fmt.Errorf("git cat-file --batch-check: %w", err)
	}
	for _, line := range strings.Split(stdout.String(), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			if len(fields) >= 2 && fields[1] == "missing" {
				out[fields[0]] = gitObjectInfo{typ: "missing"}
			}
			continue
		}
		size, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse git object size for %s: %w", fields[0], err)
		}
		out[fields[0]] = gitObjectInfo{typ: fields[1], size: size}
	}
	return out, nil
}

func packGitObjects(ctx context.Context, gitDir string, objects map[string]struct{}) ([]byte, error) {
	if len(objects) == 0 {
		return nil, nil
	}
	ids := mapKeys(objects)
	sort.Strings(ids)
	cmd := exec.CommandContext(ctx, "git", "--git-dir", gitDir, "pack-objects", "--stdout")
	setGitNoLazyEnv(cmd)
	cmd.Stdin = strings.NewReader(strings.Join(ids, "\n") + "\n")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return nil, fmt.Errorf("git pack-objects: %w: %s", err, msg)
		}
		return nil, fmt.Errorf("git pack-objects: %w", err)
	}
	return stdout.Bytes(), nil
}

func mergeObjectSets(sets ...map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{})
	for _, set := range sets {
		for oid := range set {
			out[oid] = struct{}{}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func mapKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

func gitCommandOutput(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return nil, fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, msg)
		}
		return nil, fmt.Errorf("git %s: %w", strings.Join(args, " "), err)
	}
	return out, nil
}

func gitCommandOutputNoLazy(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	setGitNoLazyEnv(cmd)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return nil, fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, msg)
		}
		return nil, fmt.Errorf("git %s: %w", strings.Join(args, " "), err)
	}
	return out, nil
}

func setGitNoLazyEnv(cmd *exec.Cmd) {
	cmd.Env = append(os.Environ(), "GIT_NO_LAZY_FETCH=1", "GIT_TERMINAL_PROMPT=0")
}

func isMissingGitObjectError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "missing") ||
		strings.Contains(msg, "Not a valid object name") ||
		strings.Contains(msg, "could not get object info") ||
		strings.Contains(msg, "bad object")
}

func isGitObjectID(s string) bool {
	if len(s) != 40 && len(s) != 64 {
		return false
	}
	for _, r := range s {
		if !isGitHexDigit(r) {
			return false
		}
	}
	return true
}

func isGitHexDigit(r rune) bool {
	return r >= '0' && r <= '9' ||
		r >= 'a' && r <= 'f' ||
		r >= 'A' && r <= 'F'
}

func archiveLocalGitStateForCheckpoint(ctx context.Context, gitDir string, rt *gitWorkspaceRuntime, sanitize gitCheckpointSanitization) ([]byte, error) {
	if len(sanitize.indexRestores) == 0 && !sanitize.dropLocalRefs {
		return archiveLocalGitStateDir(gitDir)
	}
	content, err := archiveLocalGitStateDir(gitDir)
	if err != nil {
		return nil, err
	}
	tmp, err := os.MkdirTemp(filepath.Dir(gitDir), ".drive9-git-state-*")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(tmp) }()
	if err := extractGitArchive(content, tmp); err != nil {
		return nil, err
	}
	if len(sanitize.indexRestores) > 0 {
		if err := applyGitIndexRestores(ctx, gitDir, tmp, sanitize.indexRestores); err != nil {
			return nil, err
		}
	}
	if sanitize.dropLocalRefs {
		if err := resetLocalGitRefs(tmp, rt.workspace.HeadCommit); err != nil {
			return nil, err
		}
	}
	return archiveLocalGitDir(tmp)
}

func applyGitIndexRestores(ctx context.Context, sourceGitDir, stateDir string, restores []gitIndexRestore) error {
	indexPath := filepath.Join(stateDir, "index")
	for _, restore := range restores {
		if strings.TrimSpace(restore.path) == "" {
			continue
		}
		var args []string
		if restore.objectSHA != "" {
			mode := restore.mode
			if mode == "" {
				mode = "100644"
			}
			args = []string{"--git-dir", sourceGitDir, "update-index", "--cacheinfo", mode, restore.objectSHA, restore.path}
		} else {
			args = []string{"--git-dir", sourceGitDir, "update-index", "--force-remove", "--", restore.path}
		}
		cmd := exec.CommandContext(ctx, "git", args...)
		cmd.Env = append(os.Environ(), "GIT_INDEX_FILE="+indexPath)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			msg := strings.TrimSpace(stderr.String())
			if msg != "" {
				return fmt.Errorf("git update-index restore %s: %w: %s", restore.path, err, msg)
			}
			return fmt.Errorf("git update-index restore %s: %w", restore.path, err)
		}
	}
	return nil
}

func resetLocalGitRefs(stateDir, commit string) error {
	commit = strings.TrimSpace(commit)
	if commit == "" {
		return nil
	}
	headPath := filepath.Join(stateDir, "HEAD")
	head, err := os.ReadFile(headPath)
	if err != nil {
		return err
	}
	headText := strings.TrimSpace(string(head))
	if strings.HasPrefix(headText, "ref: ") {
		refName := strings.TrimSpace(strings.TrimPrefix(headText, "ref: "))
		if strings.HasPrefix(refName, "refs/heads/") {
			if err := os.RemoveAll(filepath.Join(stateDir, "refs", "heads")); err != nil {
				return err
			}
			refPath := filepath.Join(stateDir, filepath.FromSlash(refName))
			if err := os.MkdirAll(filepath.Dir(refPath), 0o755); err != nil {
				return err
			}
			if err := os.WriteFile(refPath, []byte(commit+"\n"), 0o644); err != nil {
				return err
			}
		}
	} else if err := os.WriteFile(headPath, []byte(commit+"\n"), 0o644); err != nil {
		return err
	}
	_ = os.RemoveAll(filepath.Join(stateDir, "logs"))
	_ = os.Remove(filepath.Join(stateDir, "refs", "stash"))
	return filterPackedRefs(stateDir)
}

func filterPackedRefs(stateDir string) error {
	packedPath := filepath.Join(stateDir, "packed-refs")
	content, err := os.ReadFile(packedPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var out []string
	for _, line := range strings.Split(string(content), "\n") {
		if strings.Contains(line, " refs/heads/") || strings.Contains(line, " refs/stash") {
			continue
		}
		out = append(out, line)
	}
	return os.WriteFile(packedPath, []byte(strings.Join(out, "\n")), 0o644)
}

func archiveLocalGitDir(gitDir string) ([]byte, error) {
	return archiveLocalGitDirFiltered(gitDir, nil)
}

func archiveLocalGitStateDir(gitDir string) ([]byte, error) {
	return archiveLocalGitDirFiltered(gitDir, shouldSkipGitObjectStatePath)
}

func archiveLocalGitDirFiltered(gitDir string, skip func(string, fs.DirEntry) bool) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	err := filepath.WalkDir(gitDir, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if p == gitDir {
			return nil
		}
		rel, err := filepath.Rel(gitDir, p)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)
		if skip != nil && skip(name, d) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		link := ""
		if info.Mode()&os.ModeSymlink != 0 {
			link, err = os.Readlink(p)
			if err != nil {
				return err
			}
		}
		hdr, err := tar.FileInfoHeader(info, link)
		if err != nil {
			return err
		}
		hdr.Name = name
		var data []byte
		if info.Mode().IsRegular() && isGitConfigStatePath(name) {
			data, err = os.ReadFile(p)
			if err != nil {
				return err
			}
			data = gitcache.SanitizeGitConfigCredentials(data)
			hdr.Size = int64(len(data))
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		if data != nil {
			_, err := tw.Write(data)
			return err
		}
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(tw, f)
		closeErr := f.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
	if closeErr := tw.Close(); err == nil {
		err = closeErr
	}
	if closeErr := gz.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func shouldSkipGitObjectStatePath(rel string, _ fs.DirEntry) bool {
	for _, part := range strings.Split(filepath.ToSlash(rel), "/") {
		if part == "objects" {
			return true
		}
	}
	return false
}

func isGitConfigStatePath(rel string) bool {
	rel = filepath.ToSlash(rel)
	return rel == "config" || strings.HasSuffix(rel, "/config")
}

func normalizeGitOverlayRequest(req client.GitOverlayEntryRequest) client.GitOverlayEntryRequest {
	if req.Op == "" {
		req.Op = "upsert"
	}
	if req.Kind == "" {
		req.Kind = "file"
	}
	if req.SizeBytes == 0 && len(req.Content) > 0 {
		req.SizeBytes = int64(len(req.Content))
	}
	if len(req.Content) > 0 {
		req.Content = append([]byte(nil), req.Content...)
	}
	return req
}

func gitOverlayEntryFromRequest(workspaceID string, req client.GitOverlayEntryRequest) *client.GitOverlayEntry {
	now := time.Now()
	return &client.GitOverlayEntry{
		WorkspaceID:    workspaceID,
		Path:           req.Path,
		Op:             req.Op,
		Kind:           req.Kind,
		Mode:           req.Mode,
		StorageType:    req.StorageType,
		StorageRef:     req.StorageRef,
		StorageRefHash: req.StorageRefHash,
		ChecksumSHA256: req.ChecksumSHA256,
		SizeBytes:      req.SizeBytes,
		BaseObjectSHA:  req.BaseObjectSHA,
		Content:        append([]byte(nil), req.Content...),
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

func (fs *Dat9FS) applyGitOverlayEntry(workspaceID string, entry client.GitOverlayEntry) {
	if fs == nil || fs.git == nil {
		return
	}
	fs.git.mu.Lock()
	defer fs.git.mu.Unlock()
	for _, rt := range fs.git.workspaces {
		if rt.workspace.WorkspaceID == workspaceID {
			rt.mu.Lock()
			rt.overlay[entry.Path] = entry
			rt.mu.Unlock()
			break
		}
	}
}

// applyGitOverlayEntryUnlessWhiteout mirrors entry into the live overlay only
// when no whiteout has become authoritative for the path in the meantime; the
// check and the update share one rt.mu critical section so a racing
// putGitWhiteout either lands first (and wins) or lands after (and
// overwrites). It is for unsynchronized local overlay mutations — read-through
// caching and open-time dirty-mirror upserts — that race an unlink's
// whiteout. Server-ordered writes must keep using applyGitOverlayEntry via
// putGitOverlayWithPolicy: overwriting the whiteout there is exactly how a
// name is recreated after unlink.
//
// blockedByWhiteout reports that a whiteout is currently authoritative and the
// entry was dropped; applied reports the entry was stored.
func (fs *Dat9FS) applyGitOverlayEntryUnlessWhiteout(workspaceID string, entry client.GitOverlayEntry) (applied, blockedByWhiteout bool) {
	if fs == nil || fs.git == nil {
		return false, false
	}
	fs.git.mu.Lock()
	defer fs.git.mu.Unlock()
	for _, rt := range fs.git.workspaces {
		if rt == nil || rt.workspace.WorkspaceID != workspaceID {
			continue
		}
		rt.mu.Lock()
		if current, ok := rt.overlay[entry.Path]; ok && current.Op == "whiteout" {
			rt.mu.Unlock()
			return false, true
		}
		rt.overlay[entry.Path] = entry
		rt.mu.Unlock()
		return true, false
	}
	return false, false
}

func (fs *Dat9FS) applyGitOverlayMirrorEntry(fh *FileHandle, size int64) {
	if fs == nil || fh == nil || fh.Layer != PathLayerGitWorkspace || fh.GitWorkspaceID == "" || fh.GitRelPath == "" {
		return
	}
	if fh.Unlinked {
		// Open-unlink: the whiteout written by the gitEntry unlink path must
		// stay authoritative in the live overlay and the pending map. An
		// upsert mirror here would resurrect the deleted name in lookups,
		// readdir and reads on the active mount even though the remote PUT
		// is already suppressed by the git flush guards.
		return
	}
	entry := client.GitOverlayEntry{
		WorkspaceID:    fh.GitWorkspaceID,
		Path:           fh.GitRelPath,
		Op:             "upsert",
		Kind:           fh.GitKind,
		Mode:           gitModeForHandle(fh),
		SizeBytes:      size,
		BaseObjectSHA:  fh.GitBaseObjectSHA,
		ChecksumSHA256: "",
		UpdatedAt:      time.Now(),
	}
	fs.rememberPendingGitOverlayEntry(fh.GitWorkspaceID, entry)
	fs.applyGitOverlayEntry(fh.GitWorkspaceID, entry)
}

// applyGitOverlayMirrorEntryUnlessWhiteout is the open-time variant of
// applyGitOverlayMirrorEntry. prepareGitOpenHandle runs before the handle is
// registered, so neither the fh.Unlinked guard above nor the unlink's marking
// passes can protect it from a whiteout racing the open. Here the whiteout
// check and the overlay update are atomic with putGitWhiteout's
// applyGitOverlayEntry; when the whiteout wins, the handle adopts the
// unlinked state instead of resurrecting the name — the same outcome the
// registration-time check in allocateGitWorkspaceFileHandle produces.
// gitCreateHandle keeps applyGitOverlayMirrorEntry because Create must be
// able to overwrite a whiteout (that is how a name is recreated).
func (fs *Dat9FS) applyGitOverlayMirrorEntryUnlessWhiteout(fh *FileHandle, size int64) {
	if fs == nil || fh == nil || fh.Layer != PathLayerGitWorkspace || fh.GitWorkspaceID == "" || fh.GitRelPath == "" {
		return
	}
	if fh.Unlinked {
		return
	}
	entry := client.GitOverlayEntry{
		WorkspaceID:    fh.GitWorkspaceID,
		Path:           fh.GitRelPath,
		Op:             "upsert",
		Kind:           fh.GitKind,
		Mode:           gitModeForHandle(fh),
		SizeBytes:      size,
		BaseObjectSHA:  fh.GitBaseObjectSHA,
		ChecksumSHA256: "",
		UpdatedAt:      time.Now(),
	}
	if _, blocked := fs.applyGitOverlayEntryUnlessWhiteout(fh.GitWorkspaceID, entry); blocked {
		fh.Unlinked = true
		return
	}
	// Capture the pending seq so registration-time whiteout adoption can drop
	// only THIS handle's stale pending entry — a legitimate same-path
	// recreate re-remembers the path with a newer seq and must survive.
	fh.GitPendingMirrorSeq = fs.rememberPendingGitOverlayEntry(fh.GitWorkspaceID, entry)
}

func (fs *Dat9FS) rememberPendingGitOverlayEntry(workspaceID string, entry client.GitOverlayEntry) uint64 {
	if fs == nil {
		return 0
	}
	seq := fs.gitOverlaySeq.Add(1)
	fs.gitOverlayMu.Lock()
	if fs.gitOverlayPending == nil {
		fs.gitOverlayPending = make(map[string]map[string]pendingGitOverlayEntry)
	}
	byPath := fs.gitOverlayPending[workspaceID]
	if byPath == nil {
		byPath = make(map[string]pendingGitOverlayEntry)
		fs.gitOverlayPending[workspaceID] = byPath
	}
	byPath[entry.Path] = pendingGitOverlayEntry{seq: seq, entry: entry}
	fs.gitOverlayMu.Unlock()
	return seq
}

func (fs *Dat9FS) forgetPendingGitOverlayEntry(workspaceID, relPath string, seq uint64) {
	if fs == nil || relPath == "" {
		return
	}
	fs.gitOverlayMu.Lock()
	defer fs.gitOverlayMu.Unlock()
	byPath := fs.gitOverlayPending[workspaceID]
	if byPath == nil {
		return
	}
	pending, ok := byPath[relPath]
	if !ok || (seq != 0 && pending.seq != seq) {
		return
	}
	delete(byPath, relPath)
	if len(byPath) == 0 {
		delete(fs.gitOverlayPending, workspaceID)
	}
}

func (fs *Dat9FS) mergePendingGitOverlayEntries(workspaceID string, overlay map[string]client.GitOverlayEntry) {
	if fs == nil || len(overlay) == 0 && fs.gitOverlayPending == nil {
		return
	}
	fs.gitOverlayMu.Lock()
	defer fs.gitOverlayMu.Unlock()
	for rel, pending := range fs.gitOverlayPending[workspaceID] {
		overlay[rel] = pending.entry
	}
}

func (fs *Dat9FS) gitOverlayWriteBackEnabled(policy WritePolicy, forceSync bool) bool {
	if fs == nil || forceSync {
		return false
	}
	if policy == "" {
		policy = WritePolicyWriteBack
	}
	if policy != WritePolicyWriteBack {
		return false
	}
	return fs.syncMode == SyncInteractive
}

func (fs *Dat9FS) reserveGitOverlayCommitSlot() (<-chan struct{}, chan struct{}) {
	done := make(chan struct{})
	if fs == nil {
		return nil, done
	}
	fs.gitOverlayMu.Lock()
	prev := fs.gitOverlayTail
	fs.gitOverlayTail = done
	fs.gitOverlayMu.Unlock()
	return prev, done
}

func (fs *Dat9FS) putGitOverlayRemote(ctx context.Context, workspaceID string, req client.GitOverlayEntryRequest) (*client.GitOverlayEntry, error) {
	if fs == nil || fs.client == nil {
		return nil, syscall.EIO
	}
	start := fs.perfStart()
	entry, err := fs.client.PutGitOverlayEntry(ctx, workspaceID, req)
	fs.perfRecordRemote(perfRemoteMutation, start, err, uint64(len(req.Content)))
	if fs.perfEnabled() {
		if err != nil {
			fs.perf.gitOverlayFailure.add(1)
		} else {
			fs.perf.gitOverlaySuccess.add(1)
		}
	}
	if err != nil {
		return nil, err
	}
	return entry, nil
}

func (fs *Dat9FS) putGitOverlay(ctx context.Context, workspaceID string, req client.GitOverlayEntryRequest) (*client.GitOverlayEntry, error) {
	policy := WritePolicyWriteBack
	if fs != nil && fs.opts != nil && fs.opts.WritePolicy != "" {
		policy = fs.opts.WritePolicy
	}
	return fs.putGitOverlayWithPolicy(ctx, workspaceID, req, policy, false)
}

func (fs *Dat9FS) putGitOverlayWithPolicy(ctx context.Context, workspaceID string, req client.GitOverlayEntryRequest, policy WritePolicy, forceSync bool) (*client.GitOverlayEntry, error) {
	req = normalizeGitOverlayRequest(req)
	localEntry := gitOverlayEntryFromRequest(workspaceID, req)
	if fs.gitOverlayWriteBackEnabled(policy, forceSync) {
		pendingSeq := fs.rememberPendingGitOverlayEntry(workspaceID, *localEntry)
		fs.applyGitOverlayEntry(workspaceID, *localEntry)
		prev, done := fs.reserveGitOverlayCommitSlot()
		if fs.perfEnabled() {
			fs.perf.gitOverlayEnqueue.add(1)
		}
		fs.gitOverlayWG.Add(1)
		go func() {
			defer fs.gitOverlayWG.Done()
			if prev != nil {
				<-prev
			}
			defer close(done)
			timeout := releaseTimeout(req.SizeBytes)
			commitCtx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			if _, err := fs.putGitOverlayRemote(commitCtx, workspaceID, req); err != nil {
				safeLogPrintf("git workspace overlay async commit failed workspace=%s path=%s op=%s: %v", workspaceID, req.Path, req.Op, err)
			} else {
				fs.forgetPendingGitOverlayEntry(workspaceID, req.Path, pendingSeq)
			}
		}()
		return localEntry, nil
	}
	if fs.perfEnabled() {
		fs.perf.gitOverlaySync.add(1)
	}
	prev, done := fs.reserveGitOverlayCommitSlot()
	if prev != nil {
		<-prev
	}
	defer close(done)
	entry, err := fs.putGitOverlayRemote(ctx, workspaceID, req)
	if err != nil {
		return nil, err
	}
	fs.forgetPendingGitOverlayEntry(workspaceID, entry.Path, 0)
	fs.applyGitOverlayEntry(workspaceID, *entry)
	return entry, nil
}

func (fs *Dat9FS) drainGitOverlayWrites() {
	if fs == nil {
		return
	}
	start := time.Now()
	fs.gitOverlayWG.Wait()
	if fs.perfEnabled() {
		fs.perf.gitOverlayDrainCount.add(1)
		fs.perf.gitOverlayDrainTotalNS.add(uint64(time.Since(start)))
	}
}

func (fs *Dat9FS) syncGitDirtyMirrors() {
	if fs == nil || fs.git == nil || fs.opts == nil || strings.TrimSpace(fs.opts.LocalRoot) == "" {
		return
	}
	localRoot := strings.TrimSpace(fs.opts.LocalRoot)
	workspaceRoot := filepath.Join(localRoot, "git-workspaces")
	fs.git.mu.Lock()
	runtimes := make(map[string]*gitWorkspaceRuntime, len(fs.git.workspaces))
	for _, rt := range fs.git.workspaces {
		if rt != nil && rt.workspace.WorkspaceID != "" {
			runtimes[rt.workspace.WorkspaceID] = rt
		}
	}
	fs.git.mu.Unlock()

	workspaceDirs, err := os.ReadDir(workspaceRoot)
	if err != nil {
		return
	}
	for _, workspaceDir := range workspaceDirs {
		if !workspaceDir.IsDir() {
			continue
		}
		workspaceID := workspaceDir.Name()
		if strings.TrimSpace(workspaceID) == "" {
			continue
		}
		commitDirs, err := os.ReadDir(filepath.Join(workspaceRoot, workspaceID))
		if err != nil {
			continue
		}
		rt := runtimes[workspaceID]
		for _, commitDir := range commitDirs {
			if !commitDir.IsDir() {
				continue
			}
			dirtyRoot := filepath.Join(workspaceRoot, workspaceID, commitDir.Name(), "dirty")
			fs.syncGitDirtyMirrorRoot(workspaceID, rt, dirtyRoot)
		}
	}
}

func (fs *Dat9FS) syncGitDirtyMirrorRoot(workspaceID string, rt *gitWorkspaceRuntime, dirtyRoot string) {
	if fs == nil || workspaceID == "" || dirtyRoot == "" {
		return
	}
	info, err := os.Stat(dirtyRoot)
	if err != nil || !info.IsDir() {
		return
	}
	_ = filepath.WalkDir(dirtyRoot, func(p string, d os.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dirtyRoot, p)
		if err != nil {
			return nil
		}
		rel = filepath.ToSlash(rel)
		if rel == "." || rel == "" {
			return nil
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return nil
		}
		mode := "100644"
		kind := "file"
		base := ""
		if e, ok := rt.overlayEntry(rel); ok {
			if e.Op == "whiteout" {
				return nil
			}
			if e.Mode != "" {
				mode = e.Mode
			}
			if e.Kind != "" {
				kind = e.Kind
			}
			base = e.BaseObjectSHA
		}
		if stat, err := d.Info(); err == nil && mode == "100644" && stat.Mode()&0o111 != 0 {
			mode = "100755"
		}
		ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout(int64(len(data))))
		_, err = fs.putGitOverlayWithPolicy(ctx, workspaceID, client.GitOverlayEntryRequest{
			Path:          rel,
			Op:            "upsert",
			Kind:          kind,
			Mode:          mode,
			BaseObjectSHA: base,
			Content:       data,
			SizeBytes:     int64(len(data)),
		}, WritePolicyWriteSync, true)
		cancel()
		if err != nil {
			safeLogPrintf("git workspace dirty mirror sync failed workspace=%s root=%s path=%s: %v", workspaceID, dirtyRoot, rel, err)
		}
		return nil
	})
}

func (fs *Dat9FS) flushGitHandleLocked(ctx context.Context, fh *FileHandle) gofuse.Status {
	return fs.flushGitHandleLockedWithPolicy(ctx, fh, false)
}

func (fs *Dat9FS) flushGitLocalFileHandleLockedWithPolicy(ctx context.Context, fh *FileHandle, forceSync bool) gofuse.Status {
	if fh == nil || fh.Layer != PathLayerGitWorkspace || fh.LocalFile == nil {
		return gofuse.OK
	}
	if fh.Unlinked {
		// Open-unlink: the gitEntry unlink path marked this handle when it
		// wrote the whiteout. Suppress the overlay upsert so the whiteout is
		// not shadowed by a recreate from the anonymous fd. The fd's data
		// stays in LocalFile untouched for reads (POSIX write/read
		// after-unlink) — unlike Flush/Release on remote-DELETE handles,
		// nothing is discarded here because a write-sync Write also routes
		// through this function and its bytes must remain readable.
		return gofuse.OK
	}
	if fh.DirtySeq == 0 && !fh.HasPendingMode {
		return gofuse.OK
	}
	data, err := os.ReadFile(fh.LocalFile.Name())
	if err != nil {
		return localErrToFuseStatus(err)
	}
	req := client.GitOverlayEntryRequest{
		Path:          fh.GitRelPath,
		Op:            "upsert",
		Kind:          fh.GitKind,
		Mode:          gitModeForHandle(fh),
		BaseObjectSHA: fh.GitBaseObjectSHA,
		Content:       data,
		SizeBytes:     int64(len(data)),
	}
	fh.Unlock()
	_, err = fs.putGitOverlayWithPolicy(ctx, fh.GitWorkspaceID, req, fh.WritePolicy, forceSync)
	fh.Lock()
	if err != nil {
		return httpToFuseStatus(err)
	}
	fs.clearDirtySize(fh.Ino, fh.DirtySeq)
	fh.DirtySeq = 0
	fh.IsNew = false
	if fh.HasPendingMode {
		fs.inodes.UpdateMode(fh.Ino, fh.PendingMode&0o777)
		fh.GitMode = req.Mode
		clearPendingModeLocked(fh)
	}
	fs.inodes.UpdateSize(fh.Ino, int64(len(data)))
	fs.inodes.UpdateMtime(fh.Ino, time.Now())
	fs.invalidateReadCacheAndTargetsExcept(fh.Path, fh)
	fs.notifyInode(fh.Ino)
	return gofuse.OK
}

func (fs *Dat9FS) flushGitHandleLockedWithPolicy(ctx context.Context, fh *FileHandle, forceSync bool) gofuse.Status {
	if fh == nil || fh.Layer != PathLayerGitWorkspace {
		return gofuse.OK
	}
	if fh.Unlinked {
		// Open-unlink: suppress the overlay upsert — the whiteout written by
		// the gitEntry unlink path must not be shadowed by a recreate from
		// the anonymous fd. Its data stays in the Dirty buffer / LocalFile
		// for reads (POSIX write/read-after-unlink); nothing is discarded
		// here because write-sync Writes also route through this function
		// and their bytes must remain readable.
		return gofuse.OK
	}
	if fh.LocalFile != nil {
		return fs.flushGitLocalFileHandleLockedWithPolicy(ctx, fh, forceSync)
	}
	if fh.Dirty == nil || !fh.Dirty.HasDirtyParts() {
		if !fh.HasPendingMode {
			return gofuse.OK
		}
		req := client.GitOverlayEntryRequest{
			Path:          fh.GitRelPath,
			Op:            "chmod",
			Kind:          fh.GitKind,
			Mode:          gitModeForHandle(fh),
			BaseObjectSHA: fh.GitBaseObjectSHA,
			SizeBytes:     fh.OrigSize,
		}
		fh.Unlock()
		_, err := fs.putGitOverlayWithPolicy(ctx, fh.GitWorkspaceID, req, fh.WritePolicy, forceSync)
		fh.Lock()
		if err != nil {
			return httpToFuseStatus(err)
		}
		fs.inodes.UpdateMode(fh.Ino, fh.PendingMode&0o777)
		fh.GitMode = req.Mode
		clearPendingModeLocked(fh)
		return gofuse.OK
	}
	dataView := fh.Dirty.bytesView()
	data := make([]byte, len(dataView))
	copy(data, dataView)
	req := client.GitOverlayEntryRequest{
		Path:          fh.GitRelPath,
		Op:            "upsert",
		Kind:          fh.GitKind,
		Mode:          gitModeForHandle(fh),
		BaseObjectSHA: fh.GitBaseObjectSHA,
		Content:       data,
		SizeBytes:     int64(len(data)),
	}
	fh.Unlock()
	_, err := fs.putGitOverlayWithPolicy(ctx, fh.GitWorkspaceID, req, fh.WritePolicy, forceSync)
	fh.Lock()
	if err != nil {
		return httpToFuseStatus(err)
	}
	fh.Dirty.ClearDirty()
	fs.clearDirtySize(fh.Ino, fh.DirtySeq)
	fh.DirtySeq = 0
	fh.IsNew = false
	if fh.HasPendingMode {
		fs.inodes.UpdateMode(fh.Ino, fh.PendingMode&0o777)
		fh.GitMode = req.Mode
		clearPendingModeLocked(fh)
	}
	fs.inodes.UpdateSize(fh.Ino, int64(len(data)))
	fs.inodes.UpdateMtime(fh.Ino, time.Now())
	fs.invalidateReadCacheAndTargetsExcept(fh.Path, fh)
	fs.notifyInode(fh.Ino)
	return gofuse.OK
}

func gitModeForHandle(fh *FileHandle) string {
	if fh == nil {
		return "100644"
	}
	if fh.GitKind == "symlink" {
		return "120000"
	}
	if fh.HasPendingMode {
		entryMode := fh.PendingMode & 0o777
		if entryMode&0o111 != 0 {
			return "100755"
		}
		return "100644"
	}
	if fh.GitMode != "" {
		return fh.GitMode
	}
	return "100644"
}

func (fs *Dat9FS) putGitWhiteout(ctx context.Context, localPath string) gofuse.Status {
	rt, rel, ok := fs.gitWorkspaceForPath(ctx, localPath)
	if !ok || rel == "" {
		return gofuse.ENOENT
	}
	if _, err := fs.putGitOverlay(ctx, rt.workspace.WorkspaceID, client.GitOverlayEntryRequest{
		Path: rel,
		Op:   "whiteout",
		Kind: "file",
	}); err != nil {
		return httpToFuseStatus(err)
	}
	fs.removeGitDirtyMirror(rt, rel)
	fs.inodes.Remove(localPath)
	fs.invalidateReadCacheAndTargets(localPath)
	fs.dirCache.Invalidate(parentDir(localPath))
	fs.cacheNegativePath(localPath)
	return gofuse.OK
}

func (fs *Dat9FS) removeGitWorkspaceRoot(ctx context.Context, rt *gitWorkspaceRuntime, localPath string) gofuse.Status {
	if rt == nil || fs == nil || fs.client == nil {
		return gofuse.EIO
	}
	wsID := rt.workspace.WorkspaceID
	// Version skew:
	// - New server: DELETE soft-deletes + best-effort index cleanup (failure
	//   logged, still 200). Client Remove below is the immediate dual-write
	//   convergence path and covers old servers that never touch the index.
	// - Old server: DELETE only soft-deletes the row; client Remove required.
	if err := fs.client.DeleteGitWorkspace(ctx, wsID); err != nil {
		return httpToFuseStatus(err)
	}
	// Always attempt client-side index remove: required on old servers; covers
	// new-server cleanup failure; idempotent when the entry is already gone.
	if err := fs.client.RemoveGitWorkspaceIndexEntry(ctx, wsID); err != nil {
		safeLogPrintf("git workspace index remove after delete workspace=%s: %v", wsID, err)
	}
	if fs.opts != nil && strings.TrimSpace(fs.opts.LocalRoot) != "" {
		if err := gitcache.MarkWorkspaceDeleted(ctx, fs.opts.LocalRoot, wsID); err != nil {
			safeLogPrintf("git workspace local deleted marker workspace=%s: %v", wsID, err)
		}
	}
	fs.git.mu.Lock()
	for i, candidate := range fs.git.workspaces {
		if candidate == rt || candidate.workspace.WorkspaceID == wsID {
			fs.git.workspaces = append(fs.git.workspaces[:i], fs.git.workspaces[i+1:]...)
			break
		}
	}
	last := len(fs.git.workspaces) == 0
	if !last {
		fs.git.loadedAt = time.Now()
	}
	localRoot := ""
	if fs.opts != nil {
		localRoot = strings.TrimSpace(fs.opts.LocalRoot)
	}
	fs.git.mu.Unlock()
	// Clear armed/refresh markers first, then re-disarm under lock so a concurrent
	// path op cannot re-arm from stale markers between unlock and clear.
	if last && localRoot != "" {
		if err := gitcache.ClearLocalArmSignals(ctx, localRoot); err != nil {
			safeLogPrintf("git workspace clear local arm signals after last delete: %v", err)
		}
		fs.git.mu.Lock()
		if len(fs.git.workspaces) == 0 {
			fs.disarmGitWorkspacesLocked()
		}
		fs.git.mu.Unlock()
	}
	fs.inodes.Remove(localPath)
	fs.invalidateReadCacheAndTargets(localPath)
	fs.dirCache.Invalidate(parentDir(localPath))
	fs.dirCache.InvalidatePrefix(localPath)
	fs.cacheNegativePath(localPath)
	return gofuse.OK
}

func (fs *Dat9FS) putGitDirectory(ctx context.Context, localPath string, mode uint32) (*InodeEntry, gofuse.Status) {
	rt, rel, ok := fs.gitWorkspaceForPath(ctx, localPath)
	if !ok || rel == "" {
		return nil, gofuse.ENOENT
	}
	if _, err := fs.putGitOverlay(ctx, rt.workspace.WorkspaceID, client.GitOverlayEntryRequest{
		Path:      rel,
		Op:        "upsert",
		Kind:      "dir",
		Mode:      gitFileModeString(mode, true),
		SizeBytes: 0,
	}); err != nil {
		return nil, httpToFuseStatus(err)
	}
	entry := fs.gitInode(localPath, true, 0, mode&0o777, true, true)
	fs.dirCache.Invalidate(parentDir(localPath))
	return entry, gofuse.OK
}

func (fs *Dat9FS) putGitSymlink(ctx context.Context, localPath, target string) (*InodeEntry, gofuse.Status) {
	rt, rel, ok := fs.gitWorkspaceForPath(ctx, localPath)
	if !ok || rel == "" {
		return nil, gofuse.ENOENT
	}
	if _, err := fs.putGitOverlay(ctx, rt.workspace.WorkspaceID, client.GitOverlayEntryRequest{
		Path:      rel,
		Op:        "symlink",
		Kind:      "symlink",
		Mode:      "120000",
		Content:   []byte(target),
		SizeBytes: int64(len(target)),
	}); err != nil {
		return nil, httpToFuseStatus(err)
	}
	entry := fs.gitInode(localPath, false, int64(len(target)), symlinkMode(), true, true)
	fs.dirCache.Invalidate(parentDir(localPath))
	return entry, gofuse.OK
}

func (fs *Dat9FS) gitCreateHandle(ctx context.Context, localPath string, flags uint32, pid uint32, mode uint32, hasMode bool) (*FileHandle, *InodeEntry, gofuse.Status) {
	rt, rel, ok := fs.gitWorkspaceForPath(ctx, localPath)
	if !ok || rel == "" {
		return nil, nil, gofuse.ENOENT
	}
	if existing, handled := fs.gitEntry(ctx, localPath, false); handled && existing != nil && flags&uint32(syscall.O_EXCL) != 0 {
		return nil, nil, gofuse.Status(syscall.EEXIST)
	}
	ino := fs.inodes.Lookup(localPath, false, 0, time.Now())
	if hasMode {
		fs.inodes.UpdateMode(ino, mode)
	}
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		return nil, nil, gofuse.EIO
	}
	wb := fs.newWriteBuffer(localPath, maxPreloadSize, 0)
	wb.touched = true
	wb.sequential = true
	wb.uploadedParts = make(map[int]bool)
	fh := &FileHandle{
		Ino:            ino,
		Path:           localPath,
		Layer:          PathLayerGitWorkspace,
		Flags:          flags,
		OpenPID:        pid,
		Dirty:          wb,
		IsNew:          true,
		WritePolicy:    fs.writePolicyForOpen(flags),
		GitWorkspaceID: rt.workspace.WorkspaceID,
		GitRelPath:     rel,
		GitKind:        "file",
		GitMode:        gitFileModeString(mode, false),
	}
	if hasMode {
		fs.setPendingModeLocked(fh, mode, 0)
	}
	// Apply the create's mirror entry BEFORE creating the dirty mirror file:
	// a straddled open adopting the unlinked state removes the stale mirror
	// pathname while the whiteout is still authoritative
	// (removeGitDirtyMirrorIfWhiteout, under rt.mu). Applying first orders
	// this create against that cleanup — the cleanup either sees this upsert
	// (and skips the removal) or runs before this mirror exists.
	fs.applyGitOverlayMirrorEntry(fh, 0)
	if file, ok := fs.openGitDirtyMirrorFile(rt, rel, flags|uint32(syscall.O_TRUNC), nil); ok {
		fh.LocalFile = file
	}
	fh.DirtySeq = fs.markDirtySize(ino, 0)
	return fh, entry, gofuse.OK
}

func (fs *Dat9FS) prepareGitOpenHandle(ctx context.Context, fh *FileHandle, flags uint32) gofuse.Status {
	rt, rel, ok := fs.gitWorkspaceForPath(ctx, fh.Path)
	if !ok || rel == "" {
		return gofuse.ENOENT
	}
	fh.Layer = PathLayerGitWorkspace
	fh.GitWorkspaceID = rt.workspace.WorkspaceID
	fh.GitRelPath = rel
	fh.GitKind = "file"
	var overlayObserved client.GitOverlayEntry
	overlaySeen := false
	if e, ok := rt.overlayEntry(rel); ok {
		fh.GitKind = e.Kind
		fh.GitMode = e.Mode
		fh.GitBaseObjectSHA = e.BaseObjectSHA
		overlayObserved = e
		overlaySeen = true
	} else if localTree, _ := fs.ensureGitLocalHeadTree(ctx, rt); localTree {
		if n, ok := rt.localHeadNode(rel); ok {
			fh.GitKind = n.Kind
			fh.GitMode = n.Mode
			fh.GitBaseObjectSHA = n.ObjectSHA
		}
	} else if n, ok := rt.cleanNode(rel); ok {
		fh.GitKind = n.Kind
		fh.GitMode = n.Mode
		fh.GitBaseObjectSHA = n.ObjectSHA
	}
	if fh.GitKind == "dir" || fh.GitKind == "submodule" {
		return gofuse.Status(syscall.EISDIR)
	}
	accMode := flags & syscall.O_ACCMODE
	if accMode != syscall.O_WRONLY && accMode != syscall.O_RDWR {
		// Read-only opens stay preload-free for clean-tree files, but an
		// overlay-backed fd must never be stranded without a read backing: if
		// a whiteout races the rest of this open, registration adopts the
		// unlinked state and Read must still serve the pre-unlink bytes — or
		// the open must fail rather than succeed and read EOF.
		switch {
		case overlaySeen && overlayObserved.Op == "whiteout":
			// The name is already gone; a stale kernel dentry can still
			// reach here. Fail fast instead of registering an fd with no
			// backing.
			return gofuse.ENOENT
		case overlaySeen && len(overlayObserved.Content) > 0:
			fh.GitOpenSnapshot = overlayObserved.Content
		case overlaySeen && overlayObserved.SizeBytes > 0:
			// Metadata-only entry (list responses may omit content). Fetch
			// the bytes now: read-through never caches the response back
			// over a whiteout, and if the whiteout won first the read fails
			// and the open fails fast instead of succeeding with no backing.
			data, err := fs.readGitOverlayFile(ctx, rt, fh.Path, rel, overlayObserved, 0, -1)
			if err != nil {
				return gitReadErrToFuseStatus(err)
			}
			fh.GitOpenSnapshot = data
		}
		return gofuse.OK
	}

	if flags&syscall.O_TRUNC != 0 {
		fh.Dirty = fs.newWriteBuffer(fh.Path, maxPreloadSize, 0)
		_ = fh.Dirty.Truncate(0)
		fh.ZeroBase = true
		fh.DirtySeq = fs.markDirtySize(fh.Ino, 0)
		fs.inodes.UpdateSize(fh.Ino, 0)
		if file, ok := fs.openGitDirtyMirrorFile(rt, rel, flags, nil); ok {
			fh.LocalFile = file
			fs.applyGitOverlayMirrorEntryUnlessWhiteout(fh, 0)
			return gofuse.OK
		}
		return gofuse.OK
	}

	size := fh.OrigSize
	if currentSize, ok := fs.gitWorkspaceCurrentSize(ctx, rt, rel); ok {
		size = currentSize
		fh.OrigSize = currentSize
		fs.inodes.UpdateSize(fh.Ino, currentSize)
	}
	data, err := fs.readGitFile(ctx, fh.Path, 0, size)
	if err != nil {
		return gitReadErrToFuseStatus(err)
	}
	if size < 0 || int64(len(data)) < size {
		size = int64(len(data))
		fh.OrigSize = size
		fs.inodes.UpdateSize(fh.Ino, size)
	}
	if file, ok := fs.openGitDirtyMirrorFile(rt, rel, flags, data); ok {
		fh.LocalFile = file
		return gofuse.OK
	}
	bufMax := size * 2
	if bufMax < maxPreloadSize {
		bufMax = maxPreloadSize
	}
	fh.Dirty = fs.newWriteBuffer(fh.Path, bufMax, 0)
	if len(data) > 0 {
		if _, err := fh.Dirty.Write(0, data); err != nil {
			return gofuse.Status(syscall.EFBIG)
		}
	} else {
		_ = fh.Dirty.Truncate(size)
	}
	fh.Dirty.ClearDirty()
	return gofuse.OK
}

// allocateGitWorkspaceFileHandle registers fh and, atomically with the
// registration, adopts the unlinked state when a whiteout raced the open.
//
// prepareGitOpenHandle reads the overlay before the handle is visible to
// anyone, so an unlink's whiteout can land after that read but before the
// handle reaches openHandles — escaping both the unlink's pre-whiteout mark
// and its post-commit pass. Re-checking the overlay and adding the handle
// under one openHandles critical section closes the window: either the Add
// precedes the post-commit pass's snapshot (and that pass marks the handle),
// or the re-check runs after the whiteout was applied and marks it here. The
// handle keeps POSIX anonymous-fd semantics: reads/writes stay local, and the
// fh.Unlinked guards suppress any mirror/flush that would upsert over the
// whiteout. cleanupGitWhiteoutAdoption then finishes the adoption (see its
// contract).
func (fs *Dat9FS) allocateGitWorkspaceFileHandle(ctx context.Context, fh *FileHandle, rt *gitWorkspaceRuntime, rel string) uint64 {
	fhID := fs.fileHandles.Allocate(fh)
	if fs.registerGitWorkspaceHandle(fh, rt, rel) {
		fs.cleanupGitWhiteoutAdoption(ctx, fh, rt, rel)
	}
	return fhID
}

// registerGitWorkspaceHandle adds fh to openHandles, atomically adopting the
// unlinked state when a whiteout raced the open. Reports whether the unlinked
// state was adopted and cleanupGitWhiteoutAdoption must run.
func (fs *Dat9FS) registerGitWorkspaceHandle(fh *FileHandle, rt *gitWorkspaceRuntime, rel string) bool {
	fs.openHandles.mu.Lock()
	defer fs.openHandles.mu.Unlock()
	marked := false
	if e, ok := rt.overlayEntry(rel); ok && e.Op == "whiteout" {
		fh.Unlinked = true
		marked = true
	}
	fs.openHandles.addLocked(fh)
	return marked
}

// cleanupGitWhiteoutAdoption finishes adopting the unlinked state for a
// handle registered after a whiteout. Every step is ownership-scoped so a
// legitimate same-path recreate that landed after the registration re-check
// cannot lose its own state to the straddled handle's cleanup:
//
//  1. Only THIS handle's pending mirror upsert is dropped (seq-scoped): a
//     whiteout landing between the mirror's whiteout-checked apply and its
//     rememberPendingGitOverlayEntry leaves a stale pending upsert that
//     mergePendingGitOverlayEntries would replay over the server whiteout on
//     the next refresh. A recreate re-remembers the path with a newer seq,
//     so its entry survives.
//  2. The dirty-mirror pathname is removed only while the whiteout is still
//     authoritative (atomically under rt.mu): a recreate applies its overlay
//     entry before creating its mirror, so it either flips the check to its
//     upsert (removal skipped) or creates its mirror after the removal.
//     Without this a later same-path recreate/refresh would prefer the stale
//     anonymous mirror bytes over the replacement content. The open fd keeps
//     working as the handle's anonymous backing.
//  3. A read-only handle gets an anonymous-fd read backing
//     (preserveGitUnlinkedReadBacking) so Read keeps serving pre-unlink
//     contents instead of EOF.
func (fs *Dat9FS) cleanupGitWhiteoutAdoption(ctx context.Context, fh *FileHandle, rt *gitWorkspaceRuntime, rel string) {
	if fh.GitPendingMirrorSeq != 0 {
		fs.forgetPendingGitOverlayEntry(fh.GitWorkspaceID, rel, fh.GitPendingMirrorSeq)
	}
	fs.removeGitDirtyMirrorIfWhiteout(rt, rel)
	fs.preserveGitUnlinkedReadBacking(ctx, fh, rt, rel)
}

// removeGitDirtyMirrorIfWhiteout unlinks the dirty-mirror pathname for rel
// only while the overlay whiteout is still authoritative, holding rt.mu
// across the check and the unlink. A legitimate same-path recreate applies
// its overlay entry (under rt.mu via applyGitOverlayEntry/its callers) before
// creating its mirror, so it either flips the check to its upsert — and the
// mirror is left alone — or creates its mirror after this removal.
func (fs *Dat9FS) removeGitDirtyMirrorIfWhiteout(rt *gitWorkspaceRuntime, rel string) {
	if rt == nil || rel == "" {
		return
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if e, ok := rt.overlay[rel]; !ok || e.Op != "whiteout" {
		return
	}
	if mirrorPath, ok := fs.gitWorkspaceDirtyMirrorPath(rt, rel); ok {
		_ = os.Remove(mirrorPath)
	}
}

// preserveGitUnlinkedReadBacking gives a read-only handle that adopted the
// unlinked state at registration an anonymous-fd read backing, so Read keeps
// serving the pre-unlink contents instead of EOF. Writable handles already
// have one (the Dirty buffer or the dirty-mirror fd) and are skipped. The
// snapshot comes from the overlay content the open observed in
// prepareGitOpenHandle, falling back to the clean tree — object packs are
// content-addressed and unaffected by the whiteout. Content that was never
// fetched locally (metadata-only overlay entry) is unreachable once the
// whiteout is authoritative; the handle keeps the pre-existing EOF behavior
// for that residual case.
func (fs *Dat9FS) preserveGitUnlinkedReadBacking(ctx context.Context, fh *FileHandle, rt *gitWorkspaceRuntime, rel string) {
	if fh == nil || rt == nil || rel == "" || fh.Dirty != nil || fh.LocalFile != nil {
		return
	}
	snapshot := fh.GitOpenSnapshot
	if snapshot == nil {
		var n client.GitTreeNode
		found := false
		if localTree, _ := fs.ensureGitLocalHeadTree(ctx, rt); localTree {
			n, found = rt.localHeadNode(rel)
		} else {
			n, found = rt.cleanNode(rel)
		}
		if found && n.Kind != "dir" && n.Kind != "submodule" {
			if data, err := fs.readGitCleanFile(ctx, rt, fh.Path, rel, n, 0, -1); err == nil &&
				int64(len(data)) <= maxPathTruncateInMemoryBytes {
				snapshot = data
			}
		}
	}
	if len(snapshot) == 0 {
		return
	}
	fh.Lock()
	fh.UnlinkedData = snapshot
	fh.UnlinkedSize = int64(len(snapshot))
	fh.UnlinkedSnapshot = true
	fh.Unlock()
}

func (fs *Dat9FS) openGitDirtyMirrorFile(rt *gitWorkspaceRuntime, rel string, flags uint32, initial []byte) (*os.File, bool) {
	mirrorPath, ok := fs.gitWorkspaceDirtyMirrorPath(rt, rel)
	if !ok {
		return nil, false
	}
	if err := os.MkdirAll(filepath.Dir(mirrorPath), 0o755); err != nil {
		return nil, false
	}
	if initial != nil {
		if err := os.WriteFile(mirrorPath, initial, 0o644); err != nil {
			return nil, false
		}
	}
	openFlags := int(flags)
	openFlags &^= syscall.O_ACCMODE
	openFlags |= syscall.O_RDWR | syscall.O_CREAT
	file, err := os.OpenFile(mirrorPath, openFlags, 0o644)
	if err != nil {
		return nil, false
	}
	if flags&uint32(syscall.O_TRUNC) != 0 {
		if err := file.Truncate(0); err != nil {
			_ = file.Close()
			return nil, false
		}
		if _, err := file.Seek(0, 0); err != nil {
			_ = file.Close()
			return nil, false
		}
	}
	return file, true
}

func gitFileModeString(mode uint32, isDir bool) string {
	if isDir {
		perm := mode & 0o777
		if perm == 0 {
			perm = 0o755
		}
		return fmt.Sprintf("04%04o", perm)
	}
	if mode&uint32(syscall.S_IFMT) == uint32(syscall.S_IFLNK) {
		return "120000"
	}
	if mode&0o111 != 0 {
		return "100755"
	}
	return "100644"
}

func gitReadErrToFuseStatus(err error) gofuse.Status {
	if err == nil {
		return gofuse.OK
	}
	if errors.Is(err, os.ErrNotExist) {
		return gofuse.ENOENT
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return gofuse.Status(errno)
	}
	return httpToFuseStatus(err)
}

func (fs *Dat9FS) setGitAttr(ctx context.Context, input *gofuse.SetAttrIn, entry *InodeEntry, out *gofuse.AttrOut) gofuse.Status {
	if entry == nil {
		return gofuse.ENOENT
	}
	rt, rel, ok := fs.gitWorkspaceForPath(ctx, entry.Path)
	if !ok || rel == "" {
		return gofuse.Status(syscall.ENOTSUP)
	}
	if input.Valid&gofuse.FATTR_MODE != 0 {
		mode := input.Mode & 0o777
		entryMode := mode
		if entryIsSymlink(entry) {
			entryMode |= uint32(syscall.S_IFLNK)
		}
		hasDirtyHandle := false
		modeGen := fs.nextPendingModeGen()
		for _, h := range fs.fileHandlesForInode(input.NodeId) {
			h.Lock()
			if h.Layer == PathLayerGitWorkspace && h.Dirty != nil {
				hasDirtyHandle = true
				fs.setPendingModeLocked(h, mode, modeGen)
				if !h.HasPreviousMode {
					if entry.HasMode {
						h.PreviousMode = entry.Mode
					}
					h.HasPreviousMode = true
					h.PreviousModeKnown = entry.HasMode
				}
			}
			h.Unlock()
		}
		if !hasDirtyHandle {
			req := client.GitOverlayEntryRequest{
				Path:          rel,
				Op:            "chmod",
				Kind:          gitOverlayKindForEntry(entry),
				Mode:          gitFileModeString(entryMode, entry.IsDir),
				BaseObjectSHA: fs.gitBaseObjectSHA(ctx, rt, rel),
			}
			if _, err := fs.putGitOverlay(ctx, rt.workspace.WorkspaceID, req); err != nil {
				return httpToFuseStatus(err)
			}
		}
		entry.Mode = entryMode
		entry.HasMode = true
		fs.inodes.UpdateMode(input.NodeId, entryMode)
	}
	if mtime, ok := input.GetMTime(); ok {
		entry.Mtime = mtime
		fs.inodes.UpdateMtime(input.NodeId, mtime)
	}
	if input.Valid&gofuse.FATTR_SIZE != 0 {
		newSize := int64(input.Size)
		if entry.IsDir {
			return gofuse.Status(syscall.EISDIR)
		}
		if input.Valid&gofuse.FATTR_FH != 0 {
			if fh, ok := fs.fileHandles.Get(input.Fh); ok && fh.Layer == PathLayerGitWorkspace && fh.Dirty != nil {
				var abortStreamer func()
				fh.Lock()
				var err error
				abortStreamer, err = fs.truncateWritableHandleLocked(fh, newSize)
				if err != nil {
					fh.Unlock()
					return gofuse.Status(syscall.EFBIG)
				}
				fh.Unlock()
				if abortStreamer != nil {
					abortStreamer()
				}
			}
		} else if newSize != entry.Size {
			readSize := entry.Size
			data, err := fs.readGitFile(ctx, entry.Path, 0, readSize)
			if err != nil {
				return gitReadErrToFuseStatus(err)
			}
			if readSize < 0 {
				entry.Size = int64(len(data))
				fs.inodes.UpdateSize(input.NodeId, entry.Size)
			}
			switch {
			case newSize < int64(len(data)):
				data = data[:newSize]
			case newSize > int64(len(data)):
				grown := make([]byte, newSize)
				copy(grown, data)
				data = grown
			}
			req := client.GitOverlayEntryRequest{
				Path:          rel,
				Op:            "upsert",
				Kind:          "file",
				Mode:          gitFileModeString(entry.Mode, false),
				BaseObjectSHA: fs.gitBaseObjectSHA(ctx, rt, rel),
				Content:       data,
				SizeBytes:     int64(len(data)),
			}
			if _, err := fs.putGitOverlay(ctx, rt.workspace.WorkspaceID, req); err != nil {
				return httpToFuseStatus(err)
			}
			fs.replaceGitDirtyMirror(rt, rel, data)
		}
		entry.Size = newSize
		fs.inodes.UpdateSize(input.NodeId, newSize)
	}
	if refreshed, handled := fs.gitEntry(ctx, entry.Path, false); handled && refreshed != nil {
		entry = refreshed
	}
	fs.fillAttr(entry, &out.Attr)
	out.SetTimeout(fs.opts.AttrTTL)
	return gofuse.OK
}

func (fs *Dat9FS) gitBaseObjectSHA(ctx context.Context, rt *gitWorkspaceRuntime, rel string) string {
	if rt == nil {
		return ""
	}
	if e, ok := rt.overlayEntry(rel); ok && e.BaseObjectSHA != "" {
		return e.BaseObjectSHA
	}
	if localTree, _ := fs.ensureGitLocalHeadTree(ctx, rt); localTree {
		if n, ok := rt.localHeadNode(rel); ok {
			return n.ObjectSHA
		}
	}
	if n, ok := rt.cleanNode(rel); ok {
		return n.ObjectSHA
	}
	return ""
}

func gitOverlayKindForEntry(entry *InodeEntry) string {
	if entry == nil {
		return "file"
	}
	if entry.IsDir {
		return "dir"
	}
	if entryIsSymlink(entry) {
		return "symlink"
	}
	return "file"
}

func (fs *Dat9FS) renameGitPath(ctx context.Context, input *gofuse.RenameIn, oldP, newP string) (bool, gofuse.Status) {
	oldRT, oldRel, oldOK := fs.gitWorkspaceForPath(ctx, oldP)
	newRT, newRel, newOK := fs.gitWorkspaceForPath(ctx, newP)
	if !oldOK && !newOK {
		return false, gofuse.OK
	}
	if !oldOK || !newOK || oldRT.workspace.WorkspaceID != newRT.workspace.WorkspaceID || oldRel == "" || newRel == "" {
		return true, gofuse.Status(syscall.EXDEV)
	}
	if oldRel == newRel {
		return true, gofuse.OK
	}
	oldEntry, handled := fs.gitEntry(ctx, oldP, false)
	if !handled || oldEntry == nil {
		return true, gofuse.ENOENT
	}
	if st := fs.validateGitRenameTarget(ctx, oldRel, newRel, oldEntry, newP); st != gofuse.OK {
		return true, st
	}
	if oldEntry.IsDir {
		if st := fs.renameGitDir(ctx, oldRT, oldRel, newRel); st != gofuse.OK {
			return true, st
		}
	} else {
		if st := fs.copyGitFileOverlay(ctx, oldRT, oldRel, newRel); st != gofuse.OK {
			return true, st
		}
	}
	if _, err := fs.putGitOverlay(ctx, oldRT.workspace.WorkspaceID, client.GitOverlayEntryRequest{
		Path: oldRel,
		Op:   "whiteout",
		Kind: "file",
	}); err != nil {
		return true, httpToFuseStatus(err)
	}
	fs.finishLocalRename(input, oldP, newP)
	fs.dirCache.InvalidatePrefix(oldP)
	fs.dirCache.InvalidatePrefix(newP)
	return true, gofuse.OK
}

func (fs *Dat9FS) validateGitRenameTarget(ctx context.Context, oldRel, newRel string, oldEntry *InodeEntry, newP string) gofuse.Status {
	if oldEntry == nil {
		return gofuse.ENOENT
	}
	if oldEntry.IsDir && strings.HasPrefix(newRel, oldRel+"/") {
		return gofuse.Status(syscall.EINVAL)
	}
	newEntry, handled := fs.gitEntry(ctx, newP, false)
	if !handled || newEntry == nil {
		return gofuse.OK
	}
	if oldEntry.IsDir != newEntry.IsDir {
		if oldEntry.IsDir {
			return gofuse.Status(syscall.ENOTDIR)
		}
		return gofuse.Status(syscall.EISDIR)
	}
	if oldEntry.IsDir {
		entries, _, err := fs.listGitDir(ctx, newP)
		if err != nil {
			return listDirErrToFuseStatus(err)
		}
		if len(entries) > 0 {
			return gofuse.Status(syscall.ENOTEMPTY)
		}
	}
	return gofuse.OK
}

func (fs *Dat9FS) renameGitDir(ctx context.Context, rt *gitWorkspaceRuntime, oldRel, newRel string) gofuse.Status {
	safeLogPrintf("git workspace directory rename requires cross-device fallback workspace=%s old=%s new=%s",
		rt.workspace.WorkspaceID, oldRel, newRel)
	return gofuse.Status(syscall.EXDEV)
}

func (fs *Dat9FS) copyGitFileOverlay(ctx context.Context, rt *gitWorkspaceRuntime, oldRel, newRel string) gofuse.Status {
	oldLocal := path.Join(rt.localRoot, oldRel)
	if rt.localRoot == "/" {
		oldLocal = "/" + oldRel
	}
	size := int64(0)
	mode := "100644"
	kind := "file"
	base := fs.gitBaseObjectSHA(ctx, rt, oldRel)
	if e, ok := rt.overlayEntry(oldRel); ok {
		size = e.SizeBytes
		mode = e.Mode
		kind = e.Kind
		if e.BaseObjectSHA != "" {
			base = e.BaseObjectSHA
		}
	} else {
		var (
			n  client.GitTreeNode
			ok bool
		)
		if localTree, _ := fs.ensureGitLocalHeadTree(ctx, rt); localTree {
			n, ok = rt.localHeadNode(oldRel)
		} else {
			n, ok = rt.cleanNode(oldRel)
		}
		if ok {
			size = n.SizeBytes
			if size < 0 {
				size = fs.resolveGitCleanNodeSize(ctx, rt, oldRel, n)
			}
			mode = n.Mode
			kind = n.Kind
		}
	}
	data, err := fs.readGitFile(ctx, oldLocal, 0, size)
	if err != nil {
		return gitReadErrToFuseStatus(err)
	}
	if kind == "" {
		kind = "file"
	}
	if _, err := fs.putGitOverlay(ctx, rt.workspace.WorkspaceID, client.GitOverlayEntryRequest{
		Path:          newRel,
		Op:            "upsert",
		Kind:          kind,
		Mode:          mode,
		BaseObjectSHA: base,
		Content:       data,
		SizeBytes:     int64(len(data)),
	}); err != nil {
		return httpToFuseStatus(err)
	}
	return gofuse.OK
}

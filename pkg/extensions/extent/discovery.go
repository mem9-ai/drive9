// Path routing state of the extension: the extent-inode cache with its
// bounded negative cache, the open-handle registry that feeds unlink and
// rename their still-open hints, and the live staging claims that overlay
// lookups while a fresh writer is buffered. One mutex covers the four maps;
// none of the accessors ever leaves the package.
package extent

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// missTTLDefault bounds how long "not an extent file" is trusted: a 2s
// hint, so throwing it away wholesale once the cap is crossed costs at
// most one re-probe per path; pruning only expired entries would leave the
// map unbounded while a workload walks a tree of glob-matching classic
// files, and every insert would rescan the whole map under the mutex the
// extent Open/Unlink/Rename/SetAttr paths share.
const (
	missTTLDefault = 2 * time.Second
	missCap        = 4096
)

// pathRouter owns every path-keyed table of the extension.
type pathRouter struct {
	mu        sync.Mutex
	inoByPath map[string]uint64
	missTTL   time.Duration
	misses    map[string]time.Time
	// mutEpoch fences stale discovery responses: every namespace mutation
	// of a path bumps its epoch, and a stat reply may only install an
	// inode mapping when the epoch it started under still holds. Without
	// the fence, a stat that raced an unlink/rename could resurrect the
	// removed mapping after the mutation's forgetIno already ran.
	mutEpoch map[string]uint64

	// live tracks open extent handles by path: the SQLite -shm index of an
	// open database reports its mapped size even at 0, and unlink/rename
	// report still-open holders to the server.
	live map[string]int

	// liveHandles indexes the OPEN descriptors so a rename can update
	// their own path field: Release untracks by h.path, and migrating only
	// the count left the handle releasing the OLD key (the new path stayed
	// "live" forever — a zero-length -shm kept reporting its mapped size).
	// Only extentHandle descriptors live here (buffered claims key the
	// staging map instead), so nesting extentHandle.mu under the router
	// lock cannot meet the staging lock order (h.mu → router.mu) head-on.
	liveHandles map[*extentHandle]struct{}

	// staging tracks live buffered claims by path (several handles may
	// share one path).
	staging map[string][]*stagingHandle
}

// rememberIno records a discovered extent inode and drops any stale miss.
func (r *pathRouter) rememberIno(path string, ino uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.inoByPath == nil {
		r.inoByPath = map[string]uint64{}
	}
	r.inoByPath[path] = ino
	delete(r.misses, path)
}

// forgetIno drops a path's inode mapping and negative-cache entry: both
// names around a mutation stop carrying evidence about the path. The
// mutation epoch advances so an in-flight stat reply for the OLD object
// cannot reinstall the mapping after this point.
func (r *pathRouter) forgetIno(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.inoByPath, path)
	delete(r.misses, path)
	r.bumpEpochLocked(path)
}

func (r *pathRouter) bumpEpochLocked(path string) {
	if r.mutEpoch == nil {
		r.mutEpoch = map[string]uint64{}
	}
	r.mutEpoch[path]++
}

// epochOf reads the path's current mutation epoch (callers compare it
// against rememberInoIfFresh's view to detect races).
func (r *pathRouter) epochOf(path string) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mutEpoch[path]
}

// rememberInoIfFresh installs an inode mapping only when no namespace
// mutation of the path happened since `epoch` was read: a stale stat
// reply (started before an unlink/rename completed) is dropped instead of
// resurrecting the removed mapping.
func (r *pathRouter) rememberInoIfFresh(path string, ino uint64, epoch uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mutEpoch[path] != epoch {
		return
	}
	if r.inoByPath == nil {
		r.inoByPath = map[string]uint64{}
	}
	r.inoByPath[path] = ino
	delete(r.misses, path)
}

func (r *pathRouter) cachedIno(path string) (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ino, ok := r.inoByPath[path]
	return ino, ok
}

func (r *pathRouter) missCached(path string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.misses[path]
	if !ok {
		return false
	}
	if time.Since(t) > r.missTTL {
		delete(r.misses, path)
		return false
	}
	return true
}

func (r *pathRouter) missRemember(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.misses == nil {
		r.misses = map[string]time.Time{}
	}
	if len(r.misses) >= missCap {
		now := time.Now()
		for k, at := range r.misses {
			if now.Sub(at) > r.missTTL {
				delete(r.misses, k)
			}
		}
		if len(r.misses) >= missCap {
			// Still over: drop the hint rather than grow without limit.
			r.misses = make(map[string]time.Time, missCap/2)
		}
	}
	r.misses[path] = time.Now()
}

// trackLive / untrackLive / hasLive maintain the open-handle registry.
func (r *pathRouter) trackLive(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.live == nil {
		r.live = map[string]int{}
	}
	r.live[path]++
}

func (r *pathRouter) untrackLive(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n := r.live[path]; n <= 1 {
		delete(r.live, path)
	} else {
		r.live[path] = n - 1
	}
}

func (r *pathRouter) hasLive(path string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.live[path] > 0
}

// trackLiveHandle registers an open descriptor and its path in one step.
func (r *pathRouter) trackLiveHandle(h *extentHandle) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.live == nil {
		r.live = map[string]int{}
	}
	if r.liveHandles == nil {
		r.liveHandles = map[*extentHandle]struct{}{}
	}
	r.live[h.path]++
	r.liveHandles[h] = struct{}{}
}

// untrackLiveHandle retires an open descriptor: the caller's CURRENT path
// (a rename may have moved it) keys the count. The path is read under the
// handle's own mutex, and the router mutex is held across it, matching
// retargetLivePrefix's order (router.mu → h.mu): a rename and the last
// Release can no longer split the count from the path.
func (r *pathRouter) untrackLiveHandle(h *extentHandle) {
	r.mu.Lock()
	h.mu.Lock()
	p := h.path
	h.mu.Unlock()
	if n := r.live[p]; n <= 1 {
		delete(r.live, p)
	} else {
		r.live[p] = n - 1
	}
	delete(r.liveHandles, h)
	r.mu.Unlock()
}

// migrateInoPrefix moves the path→inode cache entries of a renamed path
// (and of a renamed directory's descendants) to the new prefix, and drops
// the stale negative probes under the old one.
func (r *pathRouter) migrateInoPrefix(oldPath, newPath string) {
	oldPrefix := strings.TrimSuffix(oldPath, "/") + "/"
	newPrefix := strings.TrimSuffix(newPath, "/") + "/"
	r.mu.Lock()
	moves := make(map[string]uint64)
	for p, ino := range r.inoByPath {
		switch {
		case p == oldPath:
			moves[newPath] = ino
			delete(r.inoByPath, p)
		case strings.HasPrefix(p, oldPrefix):
			moves[newPrefix+strings.TrimPrefix(p, oldPrefix)] = ino
			delete(r.inoByPath, p)
		}
	}
	for p, ino := range moves {
		r.inoByPath[p] = ino
	}
	for p := range r.misses {
		if p == oldPath || strings.HasPrefix(p, oldPrefix) {
			delete(r.misses, p)
		}
	}
	// The moved keys and both exact names changed identity: bump their
	// epochs so in-flight stats for the old objects cannot resurrect them
	// at the new or old names.
	for p := range moves {
		r.bumpEpochLocked(p)
	}
	r.bumpEpochLocked(oldPath)
	r.bumpEpochLocked(newPath)
	r.mu.Unlock()
}

// retargetLivePrefix moves the live-handle registry of a renamed path
// (directory descendants included). The count move and the descriptor's own
// path update happen in ONE critical section (router.mu, then each h.mu) so
// a concurrent last Release either untracks the old key before the migration
// or the new key after it — never the old key in between.
func (r *pathRouter) retargetLivePrefix(oldPath, newPath string) {
	oldPrefix := strings.TrimSuffix(oldPath, "/") + "/"
	newPrefix := strings.TrimSuffix(newPath, "/") + "/"
	r.mu.Lock()
	if r.live == nil {
		r.live = map[string]int{}
	}
	for h := range r.liveHandles {
		h.mu.Lock()
		p := h.path
		moved := ""
		switch {
		case p == oldPath:
			moved = newPath
		case strings.HasPrefix(p, oldPrefix):
			moved = newPrefix + strings.TrimPrefix(p, oldPrefix)
		}
		if moved != "" && moved != p {
			if n := r.live[p]; n <= 1 {
				delete(r.live, p)
			} else {
				r.live[p] = n - 1
			}
			r.live[moved]++
			h.path = moved
		}
		h.mu.Unlock()
	}
	// Path-only counts (handles registered without a descriptor) follow.
	moves := make(map[string]int)
	for p, n := range r.live {
		switch {
		case p == oldPath:
			moves[newPath] += n
			delete(r.live, p)
		case strings.HasPrefix(p, oldPrefix):
			moves[newPrefix+strings.TrimPrefix(p, oldPrefix)] += n
			delete(r.live, p)
		}
	}
	for p, n := range moves {
		r.live[p] += n
	}
	r.mu.Unlock()
}

// stagingCount reports how many claims are currently buffered (the
// mount-level aggregate-memory bound's input).
func (r *pathRouter) stagingCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, hs := range r.staging {
		n += len(hs)
	}
	return n
}

// trackStaging / untrackStaging maintain the buffered-claim registry. The
// map key is the router-owned routerPath (assigned here, under r.mu, and
// moved only by retargetStaging under the same lock): reading h.path under
// r.mu raced the rename path's h.mu-guarded update and could untrack the
// wrong key, leaking the claim and its image.
func (r *pathRouter) trackStaging(h *stagingHandle) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.staging == nil {
		r.staging = map[string][]*stagingHandle{}
	}
	h.routerPath = h.path
	r.staging[h.routerPath] = append(r.staging[h.routerPath], h)
}

func (r *pathRouter) untrackStaging(h *stagingHandle) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := h.routerPath
	hs := r.staging[key]
	for i, cur := range hs {
		if cur == h {
			hs = append(hs[:i], hs[i+1:]...)
			break
		}
	}
	if len(hs) == 0 {
		delete(r.staging, key)
	} else {
		r.staging[key] = hs
	}
}

// stagingAt returns a live (unreleased, un-anonymized, un-promoted) claim
// that owns p exactly, if any. The claim-live probe runs after the router
// lock is released: promotion holds the handle lock across its move and
// takes the router lock inside, so the router must never nest the handle
// lock (see stagingHandle).
func (r *pathRouter) stagingAt(p string) *stagingHandle {
	r.mu.Lock()
	candidates := append([]*stagingHandle(nil), r.staging[p]...)
	r.mu.Unlock()
	for _, h := range candidates {
		if h.claimLive() {
			return h
		}
	}
	return nil
}

// hasLiveStagingAt reports whether a live claim owns p exactly, or — for a
// removed directory — under p.
func (r *pathRouter) hasLiveStagingAt(p string, under bool) bool {
	prefix := p
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	r.mu.Lock()
	var candidates []*stagingHandle
	for path, hs := range r.staging {
		if path == p || (under && strings.HasPrefix(path, prefix)) {
			candidates = append(candidates, hs...)
		}
	}
	r.mu.Unlock()
	for _, h := range candidates {
		if h.claimLive() {
			return true
		}
	}
	return false
}

// stagingOverlay answers a lookup from a live claim's buffered image; it
// reports false once the claim went anonymous, promoted (the VFS answers
// by inode) or finished.
func (r *pathRouter) stagingOverlay(p string) (fs.Attr, bool) {
	h := r.stagingAt(p)
	if h == nil {
		return fs.Attr{}, false
	}
	return h.overlayAttr()
}

// markStagingUnlinked flags every live claim at p (or under p, for a
// removed directory) anonymous: the fd keeps serving reads from the image,
// but the boundary push must not resurrect the path. Per-path, so a
// re-created p stays committable.
func (r *pathRouter) markStagingUnlinked(p string, under bool) {
	prefix := p
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	r.mu.Lock()
	var targets []*stagingHandle
	targetPaths := map[string]struct{}{}
	for path, hs := range r.staging {
		if path == p || (under && strings.HasPrefix(path, prefix)) {
			targets = append(targets, hs...)
			targetPaths[path] = struct{}{}
		}
	}
	_ = targetPaths
	for p := range targetPaths {
		r.bumpEpochLocked(p)
	}
	r.mu.Unlock()
	for _, h := range targets {
		h.markUnlinked()
	}
}

// retargetStaging moves live claims onto the renamed path, including
// claims nested under a renamed directory. Each claim detaches its own
// below session: the plane below has re-keyed its state for the renamed
// path (the rename op was forwarded first), and the next boundary opens a
// fresh session there.
func (r *pathRouter) retargetStaging(ctx context.Context, oldPath, newPath string) {
	prefix := oldPath
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	r.mu.Lock()
	moves := make(map[*stagingHandle]string)
	for path, hs := range r.staging {
		var kept []*stagingHandle
		for _, h := range hs {
			switch {
			case path == oldPath:
				moves[h] = newPath
			case strings.HasPrefix(path, prefix):
				moves[h] = newPath + strings.TrimPrefix(path, oldPath)
			default:
				kept = append(kept, h)
			}
		}
		if len(kept) == 0 {
			delete(r.staging, path)
		} else {
			r.staging[path] = kept
		}
	}
	for h, np := range moves {
		r.staging[np] = append(r.staging[np], h)
		// The router key moves WITH the map entry, still under r.mu: a
		// concurrent untrackStaging then removes the right key however the
		// rename interleaves with the release.
		h.routerPath = np
	}
	r.mu.Unlock()
	for h, np := range moves {
		h.retarget(ctx, np)
	}
}

// statIno is layout-based discovery through the server stat
// (ContentLayout=extent + ExtentIno).
func (e *Extent) statIno(ctx context.Context, path string) (uint64, bool) {
	if e.opts.Client == nil {
		return 0, false
	}
	stat, err := e.opts.Client.StatCtx(ctx, e.remotePath(path))
	if err != nil || stat == nil {
		return 0, false
	}
	if stat.ContentLayout == client.ContentLayoutExtent && stat.ExtentIno != 0 {
		return stat.ExtentIno, true
	}
	return 0, false
}

// resolveIno answers the extent inode of a path: cache first, then one
// bounded stat probe.
func (e *Extent) resolveIno(ctx context.Context, path string) (uint64, bool) {
	if ino, ok := e.paths.cachedIno(path); ok {
		return ino, true
	}
	if !e.discoveryEnabled() || e.paths.missCached(path) {
		return 0, false
	}
	probeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	epoch := e.paths.epochOf(path)
	ino, ok := e.statIno(probeCtx, path)
	if ok {
		e.paths.rememberInoIfFresh(path, ino, epoch)
	} else if e.paths.epochOf(path) == epoch {
		e.paths.missRemember(path)
	}
	return ino, ok
}

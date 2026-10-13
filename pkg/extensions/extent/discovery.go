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

	// live tracks open extent handles by path: the SQLite -shm index of an
	// open database reports its mapped size even at 0, and unlink/rename
	// report still-open holders to the server.
	live map[string]int

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
// names around a mutation stop carrying evidence about the path.
func (r *pathRouter) forgetIno(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.inoByPath, path)
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

// retargetLive moves the open-handle registry with a renamed path.
func (r *pathRouter) retargetLive(oldPath, newPath string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n, ok := r.live[oldPath]; ok {
		delete(r.live, oldPath)
		if r.live == nil {
			r.live = map[string]int{}
		}
		r.live[newPath] += n
	}
}

// trackStaging / untrackStaging maintain the buffered-claim registry.
func (r *pathRouter) trackStaging(h *stagingHandle) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.staging == nil {
		r.staging = map[string][]*stagingHandle{}
	}
	r.staging[h.path] = append(r.staging[h.path], h)
}

func (r *pathRouter) untrackStaging(h *stagingHandle) {
	r.mu.Lock()
	defer r.mu.Unlock()
	hs := r.staging[h.path]
	for i, cur := range hs {
		if cur == h {
			hs = append(hs[:i], hs[i+1:]...)
			break
		}
	}
	if len(hs) == 0 {
		delete(r.staging, h.path)
	} else {
		r.staging[h.path] = hs
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
	for path, hs := range r.staging {
		if path == p || (under && strings.HasPrefix(path, prefix)) {
			targets = append(targets, hs...)
		}
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
	ino, ok := e.statIno(probeCtx, path)
	if ok {
		e.paths.rememberIno(path, ino)
	} else {
		e.paths.missRemember(path)
	}
	return ino, ok
}

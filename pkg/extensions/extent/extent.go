// Package extent is the JuiceFS-backed data-plane extension. Files whose
// server stat reports content_layout=extent (or that match the mount's
// extent declarations at creation) are claimed here: their ops resolve to
// JuiceFS VFS inodes and their data flows through the extent runtime's
// session instead of the classic whole-file plane. Discovery is
// layout-based, never name-based.
package extent

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// Options configures the extension.
type Options struct {
	// ExtentPaths are creation declarations (glob semantics in policy.go);
	// new matching files are created on the extent plane.
	ExtentPaths []string
	Client      *client.Client
	// RemoteRoot prefixes engine paths for stat discovery.
	RemoteRoot string
	// CacheDir is the JuiceFS block-cache root (mount cache subtree).
	CacheDir string
}

// Extent is the fs.Handler middleware claiming extent-plane paths. It
// implements fs.Lifecycle: the chain starts/drains/stops the JuiceFS
// runtime with the mount.
type Extent struct {
	opts Options
	root string

	extentMu       sync.Mutex
	extentBuild    extentBuildState
	extentTornDown bool

	// rt holds the *extentRuntime (typed pointer, swapped on build/teardown).
	rt        atomicPointer[extentRuntime]
	inoMu     sync.Mutex
	inoByPath map[string]uint64
	// missTTL bounds how long "not an extent file" is trusted.
	missTTL time.Duration
	misses  map[string]time.Time

	// livePaths tracks open extent handles by path: the SQLite -shm
	// index of an open database reports its mapped size even at 0.
	live sync.Map
}

// extentMissCap bounds the negative cache. It is a 2s hint, so throwing it
// away wholesale once the cap is crossed costs at most one re-probe per
// path; the alternative (pruning only expired entries) leaves the map
// unbounded while a workload walks a tree of glob-matching classic files,
// and every insert then rescans the whole map under a mutex the extent
// Open/Unlink/Rename/SetAttr paths share.
const extentMissCap = 4096

// atomicPointer is a minimal generic atomic pointer.
type atomicPointer[T any] struct {
	mu sync.Mutex
	p  *T
}

func (a *atomicPointer[T]) Load() *T {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.p
}

func (a *atomicPointer[T]) Store(p *T) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.p = p
}

func New(opts Options) (*Extent, error) {
	root := strings.TrimSpace(opts.RemoteRoot)
	if root == "" || root == "/" {
		root = "/"
	} else {
		root = "/" + strings.Trim(root, "/")
	}
	return &Extent{
		opts:      opts,
		root:      root,
		inoByPath: map[string]uint64{},
		missTTL:   2 * time.Second,
		misses:    map[string]time.Time{},
	}, nil
}

func (e *Extent) remotePath(p string) string {
	if e.root == "/" {
		return p
	}
	if p == "/" {
		return e.root
	}
	return e.root + p
}

// enabled reports whether this mount opted into the extent plane at all.
func (e *Extent) enabled() bool {
	return len(e.opts.ExtentPaths) > 0 || e.rt.Load() != nil
}

// discoveryEnabled mirrors enabled: only opted-in mounts pay for probes; a
// stat reply carrying ContentLayout/ExtentIno still routes extent files on
// standard mounts because it costs nothing.
func (e *Extent) discoveryEnabled() bool {
	return e.enabled() || e.vfs() != nil
}

func (e *Extent) rememberExtentIno(path string, ino uint64) {
	e.inoMu.Lock()
	e.inoByPath[path] = ino
	delete(e.misses, path)
	e.inoMu.Unlock()
}

// trackLive/untrackLive maintain the open-handle registry.
func (e *Extent) trackLive(path string)   { e.live.Store(path, struct{}{}) }
func (e *Extent) untrackLive(path string) { e.live.Delete(path) }

// forgetIno drops a path's inode mapping and negative-cache entry: both
// names around a mutation stop carrying evidence about the path.
func (e *Extent) forgetIno(path string) {
	e.inoMu.Lock()
	delete(e.inoByPath, path)
	e.inoMu.Unlock()
	e.missForget(path)
}

// retargetLive moves the open-handle registry with a renamed path.
func (e *Extent) retargetLive(oldPath, newPath string) {
	if _, ok := e.live.Load(oldPath); ok {
		e.live.Delete(oldPath)
		e.live.Store(newPath, struct{}{})
	}
}

func (e *Extent) hasLive(path string) bool {
	_, ok := e.live.Load(path)
	return ok
}

func (e *Extent) cachedIno(path string) (uint64, bool) {
	e.inoMu.Lock()
	defer e.inoMu.Unlock()
	ino, ok := e.inoByPath[path]
	return ino, ok
}

// missCached / missRemember maintain the negative-probe cache.
func (e *Extent) missCached(path string) bool {
	e.inoMu.Lock()
	defer e.inoMu.Unlock()
	t, ok := e.misses[path]
	if !ok {
		return false
	}
	if time.Since(t) > e.missTTL {
		delete(e.misses, path)
		return false
	}
	return true
}

func (e *Extent) missRemember(path string) {
	e.inoMu.Lock()
	if e.misses == nil {
		e.misses = map[string]time.Time{}
	}
	if len(e.misses) >= extentMissCap {
		now := time.Now()
		for k, at := range e.misses {
			if now.Sub(at) > e.missTTL {
				delete(e.misses, k)
			}
		}
		if len(e.misses) >= extentMissCap {
			// Still over: drop the hint rather than grow without limit.
			e.misses = make(map[string]time.Time, extentMissCap/2)
		}
	}
	e.misses[path] = time.Now()
	e.inoMu.Unlock()
}

// missForget drops the cached miss for a path this mount just changed.
func (e *Extent) missForget(path string) {
	if path == "" {
		return
	}
	e.inoMu.Lock()
	delete(e.misses, path)
	e.inoMu.Unlock()
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
	if ino, ok := e.cachedIno(path); ok {
		return ino, true
	}
	if !e.discoveryEnabled() || e.missCached(path) {
		return 0, false
	}
	probeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ino, ok := e.statIno(probeCtx, path)
	if ok {
		e.rememberExtentIno(path, ino)
	} else {
		e.missRemember(path)
	}
	return ino, ok
}

func (e *Extent) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	if e.opts.Client == nil {
		return next(ctx, op)
	}
	switch op.Kind {
	case fs.OpGetAttr, fs.OpLookup:
		ino, ok := e.resolveIno(ctx, op.Path)
		if !ok {
			return next(ctx, op)
		}
		if e.rt.Load() == nil {
			// Server says extent but the runtime is not built yet; build it
			// lazily on first use.
			if err := e.ensureRuntime(); err != nil {
				return next(ctx, op)
			}
		}
		res, err := e.statViaVFS(ctx, op.Path, ino)
		if err != nil {
			return next(ctx, op)
		}
		return res, nil

	case fs.OpCreate:
		claimNew := ShouldUseExtentPath(e.opts.ExtentPaths, op.Path)
		var ino uint64
		var ok bool
		if claimNew {
			if err := e.ensureRuntime(); err != nil {
				return fs.Result{}, err
			}
			ino, ok = e.createInVFS(ctx, op)
		} else {
			ino, ok = e.resolveIno(ctx, op.Path)
		}
		if !ok {
			return next(ctx, op)
		}
		handle, openFlags, attr, err := e.openExtent(ctx, op.Path, ino, op.Flags, op.Caller)
		if err != nil {
			return fs.Result{}, err
		}
		return fs.Result{Handle: handle, OpenFlags: openFlags, Attr: attr}, nil

	case fs.OpSetAttr:
		// An extent inode truncates through the VFS; classic files fall
		// through to the classic plane.
		if op.Size >= 0 {
			if ino, ok := e.resolveIno(ctx, op.Path); ok {
				return fs.Result{}, e.truncateIno(ctx, op.Path, ino, op.Size)
			}
		}

	case fs.OpUnlink, fs.OpRmdir:
		if ino, ok := e.resolveIno(ctx, op.Path); ok {
			if op.Kind == fs.OpRmdir {
				return fs.Result{}, e.removeDirIno(ctx, op.Path, ino)
			}
			return fs.Result{}, e.unlinkIno(ctx, op.Path, ino)
		}

	case fs.OpRename:
		if ino, ok := e.resolveIno(ctx, op.Path); ok {
			if err := e.renameIno(ctx, op, ino); err != nil {
				return fs.Result{}, err
			}
			return fs.Result{}, nil
		}

	case fs.OpRead:
		if op.Handle != nil || op.Fh != 0 {
			break
		}
		if ino, ok := e.resolveIno(ctx, op.Path); ok {
			if h, _, _, err := e.openExtent(ctx, op.Path, ino, 0, op.Caller); err == nil {
				n, rerr := h.Read(ctx, op.Offset, make([]byte, op.Limit))
				if rerr != nil {
					return fs.Result{}, rerr
				}
				_ = h.Release(ctx)
				return fs.Result{Written: uint32(n)}, nil
			}
		}
	}
	return next(ctx, op)
}

// Lifecycle: the facade chain starts/drains/stops the JuiceFS runtime.
func (e *Extent) Start(ctx context.Context) error { return nil }

func (e *Extent) Drain(ctx context.Context) error { return nil }

func (e *Extent) Stop(ctx context.Context) error {
	e.closeRuntime()
	return nil
}

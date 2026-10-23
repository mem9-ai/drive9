// Package extent is the JuiceFS-backed data-plane extension. Files whose
// server stat reports content_layout=extent (or that match the mount's
// extent declarations at creation) are claimed here: their ops resolve to
// JuiceFS VFS inodes and their data flows through the extent runtime's
// session instead of the classic whole-file plane. Discovery is
// layout-based, never name-based.
//
// Takeover of new files: every creating writer (except under write-sync)
// is claimed before the classic buffered-write plane and buffered in
// memory. An image that grows past the server's inline cutoff — the size
// at which the classic plane would fall back to the presign/multipart
// direct-upload link — promotes onto the extent plane instead, so NEW
// files never take the multipart path. Claims that stay below the cutoff
// keep the small-file path (whole-object PUT, db9 inline); claims whose
// promotion cannot build a runtime fall back to the classic plane at
// their durability boundaries. Existing files are never converted: a
// plain re-open or an append keeps serving through the classic plane
// (legacy multipart/S3 files keep updating through presign), and only a
// truncation rewrite is buffered-and-landed classically by a claim.
package extent

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// policyWriteSync names the mount policy whose writes commit inline at
// every Write below; the buffered claim defers to that track there.
const policyWriteSync = "write-sync"

// Options configures the extension.
type Options struct {
	// ExtentPaths are creation declarations (glob semantics in policy.go);
	// new matching files are created on the extent plane from byte zero.
	ExtentPaths []string
	Client      *client.Client
	// RemoteRoot prefixes engine paths for stat discovery.
	RemoteRoot string
	// CacheDir is the JuiceFS block-cache root (mount cache subtree).
	CacheDir string

	// InlineThreshold is the server's DB-inline vs S3 cutoff (bytes): a
	// claimed image that grows past it would land through the multipart
	// direct-upload link, so it promotes onto the extent plane instead.
	// Zero falls back to defaultInlineThreshold.
	InlineThreshold int64
	// LargeFileExempt names paths the default takeover must leave
	// to the planes inside this one (the append-log declarations); nil
	// exempts nothing. Explicit ExtentPaths declarations still win.
	LargeFileExempt func(path string) bool
	// Policy is the mount write policy ("writeback", "close-sync",
	// "write-sync"); blank behaves as writeback. Under write-sync the
	// buffered claim stays out — durability belongs to every Write, not to
	// a boundary.
	Policy string
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
	// promoHoldUntil parks the large-file promotion after a failed runtime
	// build, so a server without the extent plane costs one probe per
	// window instead of one per claimed writer.
	promoHoldUntil time.Time

	// rt holds the *extentRuntime (typed pointer, swapped on build/teardown).
	rt    atomicPointer[extentRuntime]
	paths pathRouter
}

// New builds the extension.
func New(opts Options) (*Extent, error) {
	root := strings.TrimSpace(opts.RemoteRoot)
	if root == "" || root == "/" {
		root = "/"
	} else {
		root = "/" + strings.Trim(root, "/")
	}
	return &Extent{
		opts: opts,
		root: root,
		paths: pathRouter{
			inoByPath: map[string]uint64{},
			missTTL:   missTTLDefault,
			misses:    map[string]time.Time{},
			staging:   map[string][]*stagingHandle{},
		},
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

// EntryTTLHint implements the facade's TTL clamp (v1 extentAttrTimeoutFor
// + clampExtentTTL): extent-plane paths revalidate at the JuiceFS ~1s
// cadence, bounded by the mount's configured TTL.
func (e *Extent) EntryTTLHint(path string) (time.Duration, bool) {
	if !e.enabled() {
		return 0, false
	}
	if _, ok := e.paths.cachedIno(path); ok {
		return juiceFSAttrTimeout, true
	}
	if ShouldUseExtentPath(e.opts.ExtentPaths, path) {
		return juiceFSAttrTimeout, true
	}
	return 0, false
}

// DirKeepCacheOff implements the facade's opendir rule (v1: directories
// with an extent-attached handle must re-list on every open — the jfs side
// can gain children the kernel's cached listing does not know).
func (e *Extent) DirKeepCacheOff(path string) bool {
	if !e.enabled() {
		return false
	}
	_, ok := e.paths.cachedIno(path)
	return ok
}

// enabled reports whether this mount opted into extent-plane discovery:
// declarations, the large-file takeover (a promoted file must be found
// again after a remount even with no declarations and no runtime yet —
// the probe cost is bounded by the negative cache), or an already-built
// runtime.
func (e *Extent) enabled() bool {
	return len(e.opts.ExtentPaths) > 0 || e.largeFileEnabled() || e.rt.Load() != nil
}

// discoveryEnabled mirrors enabled: only opted-in mounts pay for probes; a
// stat reply carrying ContentLayout/ExtentIno still routes extent files on
// standard mounts because it costs nothing.
func (e *Extent) discoveryEnabled() bool {
	return e.enabled() || e.vfs() != nil
}

// largeFileEnabled reports whether the default takeover is on. It is
// unconditional by design (new files never take the multipart link);
// write-sync mounts are the one exclusion — their durability belongs to
// every Write, not to a boundary.
func (e *Extent) largeFileEnabled() bool {
	return e.opts.Policy != policyWriteSync
}

func (e *Extent) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	if e.opts.Client == nil {
		return next(ctx, op)
	}
	switch op.Kind {
	case fs.OpGetAttr, fs.OpLookup:
		// A live staging claim answers from its image until it promotes or
		// releases; the extent plane below that answers by inode.
		if attr, ok := e.paths.stagingOverlay(op.Path); ok {
			return fs.Result{Attr: &attr}, nil
		}
		if ino, ok := e.resolveIno(ctx, op.Path); ok {
			res, err := e.statViaVFS(ctx, op.Path, ino)
			if err != nil {
				return next(ctx, op)
			}
			return res, nil
		}
		res, err := next(ctx, op)
		// K12: a stat reply that already carries the extent identity routes
		// the file even on mounts without ExtentPaths — the published inode
		// costs no extra RPC, and dropping it left known extent files
		// falling to the classic plane's plain GET (HTTP 422).
		if err == nil && res.Attr != nil && res.Attr.ExtentIno != 0 {
			e.paths.rememberIno(op.Path, res.Attr.ExtentIno)
			return res, nil
		}
		return res, err

	case fs.OpCreate:
		return e.createOp(ctx, op, next)

	case fs.OpSetAttr:
		// A staging claim folds the change into its pending image (or its
		// promoted VFS inode); an extent inode truncates or applies its
		// attributes through the VFS; classic files fall through.
		if h, ok := op.Handle.(*stagingHandle); ok {
			attr, err := h.applyAttr(ctx, op)
			if err != nil {
				return fs.Result{}, err
			}
			return fs.Result{Attr: &attr}, nil
		}
		if h := e.paths.stagingAt(op.Path); h != nil {
			attr, err := h.applyAttr(ctx, op)
			if err != nil {
				return fs.Result{}, err
			}
			return fs.Result{Attr: &attr}, nil
		}
		// FATTR_FH names a descriptor: use the handle's BOUND inode — the
		// path may have been renamed over (its cache now holds the
		// replacement's inode), and truncating through the path corrupted
		// the new object instead of the caller's old fd.
		ino, vfsFh := uint64(0), uint64(0)
		if op.Handle != nil {
			if eh := extentHandleOf(op.Handle); eh != nil {
				// BOTH the bound inode and the handle's fh ride into the
				// VFS call: fh=0 makes JuiceFS treat an FD truncate as a
				// PATH truncate (permission re-check against the CURRENT
				// mode — a legal ftruncate after a revoking chmod
				// answered EACCES).
				ino = uint64(eh.ino)
				vfsFh = eh.fh
			}
		}
		if ino == 0 {
			var ok bool
			ino, ok = e.resolveIno(ctx, op.Path)
			if !ok {
				return next(ctx, op)
			}
		}
		// Size and the metadata fields apply TOGETHER: a truncate that
		// early-returned dropped a same-request chmod/chown/mtime (the
		// kernel merges kill-priv mode changes into truncates).
		return e.setAttrViaVFS(ctx, op, ino, vfsFh)

	case fs.OpMkdir:
		// v1 extentMkdir: a directory created inside an already-mirrored
		// subtree lands in the JuiceFS namespace too (with its projection
		// row), then the classic plane creates the drive9 side.
		if e.enabled() {
			if err := e.mirrorMkdir(ctx, op); err != nil {
				return fs.Result{}, err
			}
		}
		return next(ctx, op)

	case fs.OpMknod:
		// v1: metadata-only specials (FIFO/CHR/BLK/SOCK) become persistent
		// JuiceFS nodes on extent mounts — cross-mount visible, surviving
		// restarts — instead of session-local records.
		if e.enabled() && metadataOnlySpecialMode(op.Mode) {
			return e.mknodSpecialInVFS(ctx, op)
		}
		return next(ctx, op)

	case fs.OpSymlink:
		if e.enabled() {
			if err := e.mirrorSymlink(ctx, op); err != nil {
				return fs.Result{}, err
			}
		}
		return next(ctx, op)

	case fs.OpReadlink:
		if ino, ok := e.resolveIno(ctx, op.Path); ok {
			if target, err := e.readlinkIno(ctx, op.Path, ino, op.Caller); err == nil {
				return fs.Result{Data: []byte(target)}, nil
			}
		}

	case fs.OpUnlink, fs.OpRmdir:
		if ino, ok := e.resolveIno(ctx, op.Path); ok {
			if op.Kind == fs.OpRmdir {
				return fs.Result{}, e.removeDirIno(ctx, op.Path, ino, op.Caller)
			}
			return fs.Result{}, e.unlinkIno(ctx, op.Path, ino, op.Caller)
		}
		return e.mutateStaging(ctx, op, next)

	case fs.OpRename:
		if ino, ok := e.resolveIno(ctx, op.Path); ok {
			if err := e.renameIno(ctx, op, ino); err != nil {
				return fs.Result{}, err
			}
			e.paths.migrateInoPrefix(op.Path, op.Path2)
			e.paths.retargetLivePrefix(op.Path, op.Path2)
			return fs.Result{}, nil
		}
		res, err := e.mutateStaging(ctx, op, next)
		if err != nil {
			return res, err
		}
		// A directory (or an uncached file) moved on the plane below: the
		// path→inode cache must follow the namespace, or a cached /d/f.db
		// still resolved through a RECREATED empty /d and openExtent used
		// the stale inode of an object that now lives at /moved/f.db.
		e.paths.migrateInoPrefix(op.Path, op.Path2)
		e.paths.retargetLivePrefix(op.Path, op.Path2)
		return res, nil

	case fs.OpRead:
		if op.Handle != nil || op.Fh != 0 {
			break
		}
		if h := e.paths.stagingAt(op.Path); h != nil {
			data, err := h.readAll(ctx, op.Offset, op.Limit)
			if err != nil {
				return fs.Result{}, err
			}
			return fs.Result{Data: data}, nil
		}
		if ino, ok := e.resolveIno(ctx, op.Path); ok {
			if data, err := e.readViaVFS(ctx, op.Path, ino, op.Offset, op.Limit); err == nil {
				return fs.Result{Data: data}, nil
			}
			// A transient VFS failure serves the read through the classic
			// plane below instead of failing it.
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

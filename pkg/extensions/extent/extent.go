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
	"sync/atomic"
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
	// DropClassicPathState retires the planes-below CACHED state for a
	// path (writeback landed records, queued lazy creates) without a
	// server call. The extent plane uses it after a VFS rename replaces a
	// classic destination: the planes below never heard about the rename,
	// but the path now names the moved extent file, so the classic unlink
	// they would understand would delete it. Wired by the mount to the
	// writeback plane; nil is a no-op (stale classic state then serves the
	// overwritten name until its own expiry).
	DropClassicPathState func(path string)
	// SubmitClassicUpload submits the staged close image of the live
	// below-plane handle at path WITHOUT releasing the handle. The extent
	// layer retains a writer's below session past its Release while claim
	// readers are still open, but the close upload must not wait for the
	// last reader: the write plane below submits at handle release, so the
	// retained-close calls this at the writer's Release instead. Wired by
	// the mount to the writeback plane; nil is a no-op (the upload then
	// rides the eventual drain).
	SubmitClassicUpload func(ctx context.Context, path string)
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

	// readerBytes counts bytes reserved by live claim-reader snapshot
	// fallbacks (see claimReadHandle.trySnapshot); atomic so readers never
	// serialize on the claims.
	readerBytes atomic.Int64
	// frozenBytes counts the claim-wide frozen-image reservations (see
	// stagingHandle.publishFinalImageLocked): unlike reader snapshots,
	// frozen images of RETIRED claims (writer released, claim untracked)
	// can outlive the staging admission slot, so they carry their own
	// hard cap — publication is refused past it (a live backing exists at
	// every publication site; reads are backing-first).
	frozenBytes atomic.Int64
}

// readerBytesCap bounds the aggregate memory of reader snapshots — the
// same order as the whole claim-buffer ceiling (stagingClaimCap claims ×
// stagingMaxBufferedBytes each): snapshots are the shadow copy of the
// same bytes.
const readerBytesCap = int64(stagingClaimCap) * stagingMaxBufferedBytes

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

// declared reports whether the mount carries explicit extent declarations
// (--extent globs / the [extent] profile section): the namespace mirroring
// that puts specials on the VFS is opted in by declaration only, never by
// the default takeover.
func (e *Extent) declared() bool {
	return len(e.opts.ExtentPaths) > 0
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
			res, err := e.statViaVFS(ctx, op.Path, ino, op.Caller)
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
		// JuiceFS nodes on DECLARED extent mounts — cross-mount visible,
		// surviving restarts — instead of session-local records. The
		// takeover alone does not opt specials in: a VFS special beside
		// classic files mixes the two planes in one directory, and the
		// server's jfs engine has no hardlink op (link(2) on such a FIFO
		// answered EPERM where the classic plane links it fine).
		if e.declared() && metadataOnlySpecialMode(op.Mode) {
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

	case fs.OpLink:
		// A hardlink to an extent inode (a VFS special, a promoted file)
		// adds a second edge in the JuiceFS namespace; the classic link RPC
		// below has no object to link (the projection row is not the
		// data) and answered EIO.
		if ino, ok := e.resolveIno(ctx, op.Path); ok {
			return e.linkIno(ctx, op, ino, next)
		}

	case fs.OpUnlink, fs.OpRmdir:
		if ino, ok := e.resolveIno(ctx, op.Path); ok {
			if op.Kind == fs.OpRmdir {
				return fs.Result{}, e.removeDirIno(ctx, op.Path, ino, op.Caller)
			}
			return fs.Result{}, e.unlinkIno(ctx, op.Path, ino, op.Caller)
		}
		// Registered claims whose readers reach the object by path lose
		// the name in a moment: this is their last chance to observe the
		// object's CURRENT bytes (a second writer may have flushed past
		// the claim's frozen prefix), so the frozen image is refreshed
		// from the still-live name BEFORE the removal runs.
		e.paths.refreshRegisteredForRemoval(ctx, op.Path, op.Kind == fs.OpRmdir)
		return e.mutateStaging(ctx, op, next)

	case fs.OpRename:
		if ino, ok := e.resolveIno(ctx, op.Path); ok {
			// Rename FIRST: an upfront retirement of a classic destination
			// would lose it whenever renameIno later fails (runtime build,
			// parent resolution, the RPC itself). After success the
			// destination names the MOVED source, so a classic unlink of
			// the path would delete it — the old destination's client-side
			// state (writeback landed records, queued lazy creates, live
			// staging claims, the cached extent identity of whatever it
			// used to be) is dropped through invalidation instead, without
			// touching the server object the rename just replaced.
			if err := e.renameIno(ctx, op, ino); err != nil {
				return fs.Result{}, err
			}
			e.paths.markStagingUnlinked(op.Path2, false)
			e.paths.forgetIno(op.Path2)
			if e.opts.DropClassicPathState != nil {
				e.opts.DropClassicPathState(op.Path2)
			}
			e.paths.migrateInoPrefix(op.Path, op.Path2)
			e.paths.retargetLivePrefix(op.Path, op.Path2)
			return fs.Result{}, nil
		}
		// The mirror image: a CLASSIC source renamed over an EXTENT
		// destination (a VFS special, a promoted file). The classic rename
		// runs FIRST: retiring the destination's jfs inode before a
		// fallible rename would destroy it whenever the rename then
		// failed, leaving neither name. After the rename succeeds the
		// destination's server projection names the classic source; the
		// old destination's client-side state (cached extent identity,
		// writeback landed records, live staging claims) is dropped
		// WITHOUT any server call after the rename — a name-keyed jfs
		// unlink there would delete the moved-in file (the projection row
		// rides the name), which the EC2 e2e caught as the destination
		// vanishing server-side.
		res, err := e.mutateStaging(ctx, op, next)
		if err != nil {
			return res, err
		}
		// Whatever extent identity the destination path still carried is
		// gone with the replacement; a cached inode would answer lookups
		// with the overwritten object. (The destination's live staging
		// claims were already anonymized inside mutateStaging, BEFORE the
		// source claims moved onto the path.)
		e.paths.forgetIno(op.Path2)
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
			if data, err := e.readViaVFS(ctx, op.Path, ino, op.Offset, op.Limit, op.Caller); err == nil {
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

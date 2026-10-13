// Package fuse provides the gofuse.RawFilesystem facade kernel for the
// pkg/fs composition framework. It owns kernel request translation, the
// inode and handle tables, and fs.Context assembly: namespace operations
// dispatch through the fs.Chain, data operations dispatch through fs.Handle
// decorator chains captured at open time. The engine (remote transport) and
// every policy arrive as fs.Handler plugins; nothing drive9-specific lives
// here.
package fuse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	pathpkg "path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/dircache"
	"github.com/mem9-ai/drive9/pkg/fusecomp/perf"
	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/pathutil"
)

// Options configures a facade.
type Options struct {
	// Extensions wrap the core in registration order: Extensions[0] is
	// the outermost handler.
	Extensions []fs.Handler

	// Staging is the engine-provided staging ledger exposed through the
	// Context. Pass a nil interface to leave the slot empty.
	Staging fs.StagingLedger
	// Events overrides the facade-owned event bus (shared with plugins
	// constructed before the facade). Nil creates a fresh bus.
	Events *fs.EventBus

	// Uid/Gid fill attribute ownership (fs.Attr carries no owner). Zero
	// defaults to the current process owner.
	Uid uint32
	Gid uint32

	// Kernel cache TTLs (v1 defaults: attr/entry 60s — 30s attr on the
	// coding-agent profile —, negative entries 1s, listings 10s). Zero
	// fields fall back to those defaults.
	AttrTTL            time.Duration
	EntryTTL           time.Duration
	NegEntryTTL        time.Duration
	DirTTL             time.Duration
	DirCacheMaxEntries int

	// Perf enables the fuse perf counters (v1: PerfCounters || a perf
	// samples path); the continuous recorder and Prometheus metrics read
	// them.
	Perf bool
	// PerfCounters injects a pre-built counter set (the chain's
	// extensions reference it before the FS exists); nil builds fresh.
	PerfCounters *perf.Counters

	// Debug arms the application-level trace (v1 --debug): mutation
	// boundaries and slow operations log to stderr.
	Debug bool

	// GVisorCompat refreshes listing snapshots on offset-0 restarts: gVisor
	// directfs rewinds and reuses one host directory FD, and the kernel
	// snapshot reuse would hide concurrent changes from it (v1).
	GVisorCompat bool

	// DefaultPermissions reports the kernel default_permissions mount
	// option (the daemon's open gate is redundant when set).
	DefaultPermissions bool
	// LocalDirAttrs answers GetAttr for the root and directories from the
	// inode table without a remote stat (v1: dirs never revalidate). View
	// extensions that own directories (localize, layer) need their overlay
	// consulted instead, so mount clears it for those stacks.
	LocalDirAttrs bool
}

// FS is the gofuse facade kernel: RawFilesystem translation, identity
// tables, fs.Context, and dispatch into the op and handle chains.
type FS struct {
	gofuse.RawFileSystem // ENOSYS defaults for opcodes outside the fs vocabulary

	chain *fs.Chain
	// Ctx is the framework Context shared with every plugin. Its Inodes,
	// Locks, and Notify members are implemented by this facade; Staging
	// comes from Options; Events is owned here for anyone to emit into.
	Ctx *fs.Context

	inodes  *InodeToPath
	bypass  *kernelCacheBypassTable
	openIdx *OpenHandleIndex
	// deferredUnlinks holds ino→path for unlink-of-open-file deferrals.
	deferredUnlinks map[uint64]string
	perf            *perf.Counters
	mountPt         string
	rootTag         string
	srvTag          string
	locks           *fuseLockTable
	handles         *handleTable
	dirs            *dirTable
	pathLocks       *lockTable
	events          *fs.EventBus

	nextFh atomic.Uint64

	mu       sync.Mutex
	server   *gofuse.Server
	startErr error
	// unmounted records that go-fuse completed a clean unmount handshake
	// (OnUnmount): a serve-loop exit without it is an abnormal disconnect.
	unmounted atomic.Bool
	started   bool

	uid uint32
	gid uint32

	attrTTL     time.Duration
	entryTTL    time.Duration
	negEntryTTL time.Duration
	epoch       *epochTable
	// ttlHinters clamp the kernel attr TTL for paths claimed by data planes
	// with their own revalidation cadence (v1 extentAttrTimeoutFor).
	ttlHinters []interface {
		EntryTTLHint(path string) (time.Duration, bool)
	}
	// dirKeepCacheOff marks directories whose listings must not be cached
	// across opens (v1: extent-attached dirs can gain jfs-side children the
	// kernel's cached listing does not know).
	dirKeepCacheOff []interface{ DirKeepCacheOff(path string) bool }

	localDirAttrs bool
	// defaultPermissions mirrors the kernel mount option: when the kernel
	// enforces permission checks, the daemon's open gate is redundant.
	defaultPermissions bool
	gvisorCompat       bool
	debug              bool

	// nativeSyncFSSeen records that the kernel actually dispatched a
	// SyncFs request (v1: protocol support alone must not advertise it).
	nativeSyncFSSeen atomic.Bool
	// notifyWg tracks in-flight kernel notifications so drain can wait
	// them out (v1 notifications phase).
	notifyWg sync.WaitGroup
	// drainMu serializes concurrent drains (v1): syncfs(2) and the mount
	// control socket must not interleave quiesce phases.
	drainMu sync.Mutex
}

// New assembles the facade: chain, Context, and identity tables. Register
// the returned value as the RawFilesystem of a gofuse.Server. The chain has
// no privileged core — core below is simply the last handler, wrapped by
// Extensions.
// New assembles the facade over opts.Extensions, ordered outermost first
// with the terminal (core) last — the caller guarantees the terminal is
// present; an empty list is rejected.
func New(opts Options) (*FS, error) {
	handlers := opts.Extensions
	if len(handlers) == 0 {
		return nil, fmt.Errorf("fuse: no handlers in extension chain (need at least the core terminal)")
	}
	ch, err := fs.NewChain(handlers...)
	if err != nil {
		return nil, fmt.Errorf("fuse: assemble chain: %w", err)
	}
	uid, gid := opts.Uid, opts.Gid
	if uid == 0 && gid == 0 {
		uid, gid = uint32(os.Getuid()), uint32(os.Getgid())
	}
	f := &FS{
		RawFileSystem:   gofuse.NewDefaultRawFileSystem(),
		chain:           ch,
		inodes:          NewInodeToPath(),
		locks:           newFuseLockTable(),
		handles:         &handleTable{m: map[uint64]*fileHandle{}},
		dirs:            &dirTable{m: map[uint64]*dirHandle{}},
		pathLocks:       &lockTable{},
		deferredUnlinks: make(map[uint64]string),
		uid:             uid,
		gid:             gid,
	}
	// Options.Events is the mount-wide bus plugins were constructed with;
	// subscribing on a fresh one here would silently detach the facade from
	// every plugin emission (writeback commits, SSE resets).
	f.events = opts.Events
	if f.events == nil {
		f.events = fs.NewEventBus()
	}
	f.bypass = newKernelCacheBypassTable()
	f.openIdx = NewOpenHandleIndex()
	f.perf = perf.NewCounters(false)
	f.attrTTL = opts.AttrTTL
	if f.attrTTL <= 0 {
		f.attrTTL = defaultAttrTTL
	}
	f.entryTTL = opts.EntryTTL
	if f.entryTTL <= 0 {
		f.entryTTL = defaultEntryTTL
	}
	f.negEntryTTL = opts.NegEntryTTL
	if f.negEntryTTL <= 0 {
		f.negEntryTTL = defaultNegEntryTTL
	}
	dirTTL := opts.DirTTL
	if dirTTL <= 0 {
		dirTTL = defaultDirTTL
	}
	f.localDirAttrs = opts.LocalDirAttrs
	f.defaultPermissions = opts.DefaultPermissions
	f.gvisorCompat = opts.GVisorCompat
	f.debug = opts.Debug
	f.perf = opts.PerfCounters
	if f.perf == nil {
		f.perf = perf.NewCounters(opts.Perf)
	}
	f.epoch = &epochTable{m: f.inodes, epoch: map[string]uint64{}}
	f.Ctx = &fs.Context{
		Inodes:  f.epoch,
		Locks:   f.pathLocks,
		Staging: opts.Staging,
		Notify:  f,
		Events:  f.events,
		// v1 always ran one namespace-listing cache (per-name freshness,
		// negative entries, bounded LRU); owners may swap it through the
		// Context slot.
		DirCache: dircache.NewNamespaceCache(dirTTL, f.negEntryTTL, opts.DirCacheMaxEntries),
		Bypass:   f,
	}
	// Extensions carrying truncate/commit boundaries arm the kernel-cache
	// bypass window through the Context slot (v1 arming points: setattr
	// size, open-truncate, truncate-write, sync-commit).
	for _, h := range handlers {
		if ba, ok := h.(interface{ SetBypassArmer(fs.CacheBypassArmer) }); ok {
			ba.SetBypassArmer(f)
		}
		if nc, ok := h.(interface{ SetNamespaceCache(fs.DirListingCache) }); ok {
			// The cache extension's directory-neighborhood prefetch reads
			// and seeds the namespace listing cache.
			nc.SetNamespaceCache(f.Ctx.DirCache)
		}
		if sl, ok := h.(interface {
			SetSurvivorLookup(func(path string) (string, bool))
		}); ok {
			sl.SetSurvivorLookup(f.SurvivorAlias)
		}
		if it, ok := h.(interface{ SetInodeTable(fs.InodeTable) }); ok {
			// Alias-truncate sync walks the inode's hardlink paths.
			it.SetInodeTable(f.Ctx.Inodes)
		}
		if th, ok := h.(interface {
			EntryTTLHint(path string) (time.Duration, bool)
		}); ok {
			// Data planes with their own revalidation cadence (extent files
			// revalidate at the JuiceFS ~1s rate) clamp the kernel TTLs.
			f.ttlHinters = append(f.ttlHinters, th)
		}
		if dc, ok := h.(interface{ DirKeepCacheOff(path string) bool }); ok {
			f.dirKeepCacheOff = append(f.dirKeepCacheOff, dc)
		}
		if rp, ok := h.(interface {
			SetRemoteOpRecorder(func(name string, err error, dur time.Duration, bytes uint64))
		}); ok {
			// Remote-op perf feed (v1 perfRecordRemote): the terminal and
			// the write plane report their HTTP families.
			rp.SetRemoteOpRecorder(f.recordRemoteOp)
		}
		if ch, ok := h.(interface {
			SetCacheHitRecorder(func(kind string, hit bool))
		}); ok {
			// Read-tier hit/miss feed (v1 readCacheHit/DirCache counters).
			ch.SetCacheHitRecorder(f.recordCacheHit)
		}
	}
	// v1 refreshes cached views after every mutation: directory listings
	// for local namespace ops and data-plane commits, kernel entry drops
	// only for removals and remote-origin changes. The facade subscribes
	// once; every emitter (facade.mutated, writeback commits, bulkfile
	// commits, the async queue, SSE) flows through here.
	f.events.On(fs.EventWrite, f.onLocalMutation)
	f.events.On(fs.EventLanded, f.onLocalMutation)
	f.events.On(fs.EventDelete, f.onRemovedPath)
	f.events.On(fs.EventInvalidated, f.onRemovedPath)
	// A checkout/reset (SSE stream reset or disconnect) drops the local
	// namespace view too (v1 resetMountViewWithInitialSync /
	// markStatCacheUnverified cleared the dir cache): stale listings must
	// not answer lookups after the view restarted.
	f.events.On(fs.EventCheckout, func(fs.Event) {
		if f.Ctx.DirCache != nil {
			f.Ctx.DirCache.InvalidateAll()
		}
	})
	return f, nil
}

// onLocalMutation keeps the inode's size and the cached listing entry
// coherent after a local commit (v1 UpdateSize + cacheFileForPath). It must
// not invalidate the parent listing — the server's listings lag commits, so
// an invalidate-and-refetch would hide the fresh entry (v1 never invalidates
// listings on data commits) — nor drop the kernel entry (v1 updates attrs
// in place).
func (f *FS) onLocalMutation(e fs.Event) {
	if e.Path == "" {
		return
	}
	// Commit settles (EventLanded) publish the remote size and v1 settled
	// inode + listing unconditionally — zero included (empty creates and
	// truncations commit size 0; v1 cacheFileForPath has no zero guard).
	// Size-less EventWrite emissions are the facade's namespace mutations;
	// their cache effects happen at their own call sites (dirCacheUpsert /
	// onRemovedPath), so they stop here.
	if e.Kind != fs.EventLanded && e.Size <= 0 {
		return
	}
	ino, ok := f.inodes.GetInode(e.Path)
	if !ok {
		return
	}
	f.inodes.UpdateSize(ino, e.Size)
	if f.Ctx.DirCache != nil {
		item := fs.DirEntryInfo{Name: pathutil.BaseName(e.Path), Size: e.Size, Rev: e.Rev, Mtime: time.Now()}
		if entry, ok := f.inodes.GetEntry(ino); ok {
			if entry.HasMode {
				item.Mode = entry.Mode
				item.HasMode = true
			}
			// Ownership is mount-local (v1: the backend has no chown verb):
			// a settled row without it makes class checks (renameguard's
			// sticky ownership test) read the remote default owner and
			// EACCES a rename the local view allows (rename/09).
			if entry.HasUID {
				item.Uid = entry.Uid
				item.HasUID = true
			}
			if entry.HasGID {
				item.Gid = entry.Gid
				item.HasGID = true
			}
			if entry.Nlink > item.Nlink {
				// The settle row keeps the tracked link count — a
				// listing rebuilt from it must not flatten hardlinks
				// to nlink 1.
				item.Nlink = entry.Nlink
			}
			if entry.ResourceID != "" {
				item.ResourceID = entry.ResourceID
			}
		}
		f.Ctx.DirCache.Upsert(parentDirOf(e.Path), item)
	}
}

// onRemovedPath drops the cached listings of a path that no longer
// resolves (unlink/rename targets, remote-origin changes). The kernel
// maintains dentries for the mount's OWN namespace ops itself (v1 never
// notifies entries for local mutations — only SSE does for remote ones),
// so this stays listing-level.
func (f *FS) onRemovedPath(e fs.Event) {
	if e.Path == "" {
		return
	}
	if f.Ctx.DirCache != nil {
		f.Ctx.DirCache.Invalidate(parentDirOf(e.Path))
		f.Ctx.DirCache.InvalidatePrefix(e.Path)
	}
}

// markNameNegative records a genuinely-deleted name as a negative cache
// entry (v1 cacheNegativePath): a creator of the same name then resolves
// ENOENT locally instead of racing backend replica lag with a remote stat.
// Only real deletions — a rename's EventDelete must NOT poison the old name.
func (f *FS) markNameNegative(path string) {
	if dc, ok := f.Ctx.DirCache.(*dircache.DirCache); ok {
		parent, name := parentDirOf(path), pathpkg.Base(path)
		// Order matters: DirCache.Remove clears BOTH the positive row and a
		// negative marker for the name (and drops an entry left with no
		// state), so marking before removing would erase the marker in the
		// same breath — every later lookup would miss the cache entirely and
		// race backend replica lag on the just-deleted remote entry.
		dc.Remove(parent, name)
		// v1 dirCache.Remove: this mount deleted the name — remove it from
		// the cached listing too (permanent local knowledge; a fresh
		// listing re-adds it), so creators probing through the chain do
		// not race backend replica lag on the stale remote entry.
		dc.MarkNegative(parent, name)
	}
}

// dirCacheUpsert installs a freshly created entry into its parent's cached
// listing (v1 Create/Mkdir Upsert): the server's directory listings lag
// creation, so a plain invalidate-and-refetch would hide the new entry.
func (f *FS) dirCacheUpsert(child string, attr *fs.Attr) {
	parent := parentDirOf(child)
	name := pathpkg.Base(child)
	if name == "" || name == "/" || name == "." {
		return
	}
	if attr == nil {
		return
	}
	if f.Ctx.DirCache != nil {
		mode := attr.Mode & 0o7777
		if attr.Mode&syscall.S_IFMT != 0 && attr.Mode&syscall.S_IFMT != syscall.S_IFREG {
			// Typed modes (specials, symlinks) keep their type bits: a
			// permission-only row retypes the entry on the next cache
			// read and every later stat replies a regular file.
			mode = attr.Mode
		} else if attr.Kind == fs.KindSymlink {
			mode = uint32(syscall.S_IFLNK) | mode&0o777
		}
		f.Ctx.DirCache.Upsert(parent, fs.DirEntryInfo{
			Name:       name,
			Size:       attr.Size,
			IsDir:      attr.IsDir(),
			Mtime:      attr.Mtime,
			Rev:        attr.Rev,
			Mode:       mode,
			HasMode:    true,
			Nlink:      attr.Nlink,
			ResourceID: attr.ResourceID,
		})
	}
}

// stampOwner records the creating caller's identity on a freshly created
// inode (v1 mount-local ownership: the backend protocol carries no owner,
// so lstat serves the creator — chmod/chown permission checks and the
// pjdfstest -u 65534 rows depend on it).
func (f *FS) stampOwner(ino uint64, caller fs.Caller) {
	f.inodes.UpdateOwner(ino, caller.Uid, caller.Gid, true, true)
	if entry, ok := f.inodes.GetEntry(ino); ok && f.Ctx.DirCache != nil {
		parent, name := parentDirOf(entry.Path), pathpkg.Base(entry.Path)
		// Merge into whatever the cache already knows: overwriting the
		// entry wholesale would drop a symlink's type bits (and retype the
		// link as a regular file for every later stat).
		mode, hasMode, isDir, size := entry.Mode, entry.HasMode, entry.IsDir, entry.Size
		if ns, ok := f.Ctx.DirCache.(core.NamespaceCache); ok {
			if item, kind := ns.LookupNamespace(parent, name); kind == fs.LookupPositive {
				if item.HasMode {
					mode, hasMode = item.Mode, true
				}
				isDir, size = item.IsDir, item.Size
			}
		}
		f.Ctx.DirCache.Upsert(parent, fs.DirEntryInfo{
			Name: name, IsDir: isDir, Size: size,
			Mtime: time.Now(), Mode: mode, HasMode: hasMode,
			Uid: caller.Uid, Gid: caller.Gid, HasUID: true, HasGID: true,
		})
	}
}

// touchParentMtime advances the parent directory's mtime after a namespace
// mutation inside it (v1 touchDirectoryChangeTime): rmdir/mkdir rows compare
// parent mtimes around child changes.
// isDeferredUnlink reports a name whose remote deletion waits for the last
// close of its open handles.
func (f *FS) isDeferredUnlink(path string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, p := range f.deferredUnlinks {
		if p == path {
			return true
		}
	}
	return false
}

// maybeRunDeferredUnlink fires the postponed remote deletion once the last
// handle of the unlinked inode closes.
func (f *FS) maybeRunDeferredUnlink(ino uint64) {
	f.mu.Lock()
	path, ok := f.deferredUnlinks[ino]
	if !ok {
		f.mu.Unlock()
		return
	}
	if f.openIdx.HasOpenInode(ino) {
		f.mu.Unlock()
		return
	}
	delete(f.deferredUnlinks, ino)
	f.mu.Unlock()
	// Synchronous with a detached budget (v1: release completes the
	// survivor deletion before replying, so the storage is gone at close).
	ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), 30*time.Second)
	defer cancel()
	if _, err := f.chain.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: path}); err != nil {
		// The unlink syscall already returned OK; nothing can surface this
		// to its caller — but a silently-lost deletion must at least be
		// visible to the operator (v1 surfaced it inline).
		fmt.Fprintf(os.Stderr, "drive9: deferred unlink of %s failed: %v\n", path, err)
	}
	f.markNameNegative(path)
}

// flushDeferredUnlink lands a postponed deletion now: a fresh creator of
// the same name (create/mkdir/mknod/symlink/link/rename target) must not
// collide with the old entry's not-yet-deleted remote inode.
func (f *FS) flushDeferredUnlink(path string) {
	f.mu.Lock()
	found := false
	for i, p := range f.deferredUnlinks {
		if p == path {
			found = true
			delete(f.deferredUnlinks, i)
			break
		}
	}
	f.mu.Unlock()
	if !found {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), 30*time.Second)
	defer cancel()
	_, _ = f.chain.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: path})
	f.markNameNegative(path)
}

// flushDeferredUnlinksUnder lands every postponed deletion under dir/ —
// removing the directory itself must not race the last-close deferral
// (rmdir would otherwise hit ENOTEMPTY against a name that is already gone
// from the namespace).
func (f *FS) flushDeferredUnlinksUnder(dir string) {
	prefix := strings.TrimSuffix(dir, "/") + "/"
	f.mu.Lock()
	var pending []string
	for ino, p := range f.deferredUnlinks {
		if strings.HasPrefix(p, prefix) {
			pending = append(pending, p)
			delete(f.deferredUnlinks, ino)
		}
	}
	f.mu.Unlock()
	for _, p := range pending {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), 30*time.Second)
		_, _ = f.chain.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: p})
		cancel()
		f.markNameNegative(p)
	}
}

func (f *FS) touchParentMtime(parentIno uint64) {
	if parentIno == 0 || parentIno == 1 {
		return
	}
	// Locally generated mtimes are second-granular (v1): the wire's stat
	// mtime is Unix seconds, and a sub-second local clock that later
	// falls back to the server's row flips the served stat — git flags
	// the file modified once per landing.
	f.inodes.UpdateMtime(parentIno, time.Now().Truncate(time.Second))
	// The kernel caches directory attrs for the attr TTL; rmdir/mkdir rows
	// compare parent mtimes immediately after the child change, so the
	// cached view must be dropped (v1 notifyInode on the parent).
	f.InvalidateContent(parentIno, 0)
}

func (f *FS) touchParentMtimeByName(childPath string) {
	if p, ok := f.inodes.GetInode(parentDirOf(childPath)); ok {
		f.touchParentMtime(p)
	}
}

// readProcessStatusFile is stubbed in tests.
var readProcessStatusFile = os.ReadFile

// processSupplementaryGroups ports v1: FUSE requests carry only the primary
// gid, so gid-change permission checks read the caller's groups from
// /proc/<pid>/status.
func processSupplementaryGroups(pid uint32) []uint32 {
	if pid == 0 {
		return nil
	}
	status, err := readProcessStatusFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return nil
	}
	return procStatusGroups(status)
}

func procStatusGroups(status []byte) []uint32 {
	for _, line := range strings.Split(string(status), "\n") {
		if !strings.HasPrefix(line, "Groups:") {
			continue
		}
		fields := strings.Fields(strings.TrimPrefix(line, "Groups:"))
		out := make([]uint32, 0, len(fields))
		for _, field := range fields {
			v, err := strconv.ParseUint(field, 10, 32)
			if err != nil {
				continue
			}
			out = append(out, uint32(v))
		}
		return out
	}
	return nil
}

func parentDirOf(p string) string {
	dir := pathpkg.Dir(p)
	if dir == "." || dir == "" {
		return "/"
	}
	return dir
}

// publishWriteLength refreshes the inode's size and the cached listing
// entry when a buffered write grew the file (v1 UpdateLength /
// cacheFileForPath at write time): stats racing the commit boundary must
// observe the writer's length, not the pre-write remote image.
func (f *FS) publishWriteLength(ino uint64, eof int64) {
	entry, ok := f.inodes.GetEntry(ino)
	if !ok || eof <= entry.Size {
		return
	}
	f.inodes.UpdateSize(ino, eof)
	if f.Ctx.DirCache != nil {
		f.Ctx.DirCache.Upsert(parentDirOf(entry.Path), fs.DirEntryInfo{
			Name:    pathpkg.Base(entry.Path),
			Size:    eof,
			IsDir:   false,
			Mtime:   time.Now(),
			Mode:    entry.Mode,
			HasMode: entry.HasMode,
		})
	}
}

// StartErr reports the error from chain Start, once Init has run.
func (f *FS) StartErr() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.startErr
}

// classifyDrainErr sorts a phase failure into v1's drain error kinds —
// timeout, remote_timeout_or_retryable, canceled — which the syncfs errno
// mapping (ETIMEDOUT/EAGAIN/EINTR) reads.
func classifyDrainErr(err error) string {
	if err == nil {
		return ""
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "remote_timeout_or_retryable"
	}
	return "drain"
}

// Drain quiesces the mount for `drive9 mount drain` (v1 phase shape): open
// handles flush first (an fd held open across drain must have its buffered
// image committed), the plugin chain drains its queues (debounced uploads +
// commit queue inside writeback), and kernel notifications are waited out.
// The pending snapshot and syncfs support fields mirror v1's response.
func (f *FS) Drain(ctx context.Context) mountcontrol.DrainResponse {
	// v1 drainMu: a syncfs(2) and a `drive9 mount drain` racing the same
	// quiesce must serialize, not interleave phases.
	f.drainMu.Lock()
	defer f.drainMu.Unlock()
	startedAt := time.Now().UTC()
	resp := mountcontrol.NewDrainResponse(f.mountPt, startedAt)
	f.populateNativeSyncFSSupport(&resp)
	runPhase := func(name string, fn func() error) bool {
		phaseStart := time.Now()
		err := fn()
		phase := mountcontrol.DrainPhase{Name: name, DurationMS: time.Since(phaseStart).Milliseconds()}
		if err != nil {
			phase.Error = err.Error()
		}
		resp.Phases = append(resp.Phases, phase)
		if err == nil {
			return true
		}
		resp.Fail(classifyDrainErr(err), f.mountPt, err)
		return false
	}
	if !runPhase("flush_open_handles", func() error { return f.flushOpenHandles(ctx) }) {
		resp.Finish(time.Now().UTC())
		return resp
	}
	if !runPhase("writeback_uploader", func() error { return f.chain.Drain(ctx) }) {
		resp.Finish(time.Now().UTC())
		return resp
	}
	// v1's drain JSON carried the commit-queue wait as its own phase; the
	// chain drain above already covered the work, and WaitIdle is
	// idempotent — the restated phase keeps the contract stable.
	queueIdle := func(context.Context) error { return nil }
	for _, h := range f.chain.Handlers() {
		if q, is := h.(interface{ WaitCommitQueueIdle(context.Context) error }); is {
			queueIdle = q.WaitCommitQueueIdle
			break
		}
	}
	if !runPhase("commit_queue", func() error { return queueIdle(ctx) }) {
		resp.Finish(time.Now().UTC())
		return resp
	}
	if !runPhase("notifications", func() error {
		done := make(chan struct{})
		go func() {
			f.notifyWg.Wait()
			close(done)
		}()
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}) {
		resp.Finish(time.Now().UTC())
		return resp
	}
	// v1 drainPendingWorkError: a drain that leaves write-side work behind
	// (conflicted commits, uploader-cached snapshots, dirty handles) fails
	// — syncfs(2) and `drive9 mount drain` must not report success over
	// abandoned data.
	for _, h := range f.chain.Handlers() {
		if p, is := h.(interface{ DrainPending() (string, int) }); is {
			if reason, n := p.DrainPending(); n > 0 {
				resp.Fail("pending_work_remaining", f.mountPt, fmt.Errorf("%s: %d entries remain", reason, n))
			}
			break
		}
	}
	f.snapshotDrainPending(&resp)
	resp.Finish(time.Now().UTC())
	return resp
}

// populateNativeSyncFSSupport fills the FUSE protocol version and the
// native-syncfs support flag (v1: only after a real SyncFs was observed).
func (f *FS) populateNativeSyncFSSupport(resp *mountcontrol.DrainResponse) {
	if resp == nil {
		return
	}
	if s := f.currentServer(); s != nil {
		if settings := s.KernelSettings(); settings != nil {
			resp.FUSEProtocolMajor = settings.Major
			resp.FUSEProtocolMinor = settings.Minor
		}
	}
	resp.NativeSyncFSSupported = f.nativeSyncFSSeen.Load()
}

// snapshotDrainPending reports undrained work (v1 snapshotDrainPending).
func (f *FS) snapshotDrainPending(resp *mountcontrol.DrainResponse) {
	resp.Pending.OpenHandles = f.handles.len()
	if pending, bytes, ok := f.chainPendingStats(); ok {
		resp.Pending.CommitQueuePending = pending
		resp.Pending.CommitQueueBytes = bytes
	}
}

// chainPendingStats asks the write plane for queue depths.
func (f *FS) chainPendingStats() (pending int, bytes int64, ok bool) {
	for _, h := range f.chain.Handlers() {
		if q, is := h.(interface {
			Queue() interface {
				Pending() int
				PendingStats() (int, int64)
			}
		}); is {
			queue := q.Queue()
			if queue == nil {
				return 0, 0, true
			}
			pending, bytes = queue.PendingStats()
			return pending, bytes, true
		}
	}
	return 0, 0, false
}

// SyncFs serves syncfs(2) with a full drain (v1): records the observation
// so the drain response may advertise native support afterwards.
func (f *FS) SyncFs(cancel <-chan struct{}, header *gofuse.InHeader) gofuse.Status {
	start := time.Now()
	f.nativeSyncFSSeen.Store(true)
	ctx, cf := fuseCtx(cancel)
	defer cf()
	resp := f.Drain(ctx)
	st := gofuse.OK
	if !resp.OK {
		st = drainResponseStatus(resp)
	}
	f.recordFuseOp("syncfs", st, time.Since(start), 0)
	return st
}

// drainResponseStatus maps drain failures onto kernel errnos (v1).
func drainResponseStatus(resp mountcontrol.DrainResponse) gofuse.Status {
	switch resp.ErrorKind {
	case "timeout":
		return gofuse.Status(syscall.ETIMEDOUT)
	case "remote_timeout_or_retryable":
		return gofuse.Status(syscall.EAGAIN)
	case "canceled":
		return gofuse.Status(syscall.EINTR)
	default:
		return gofuse.EIO
	}
}

// flushOpenHandles commits every live kernel handle's buffered image with a
// per-handle deadline, mirroring v1's FlushAll walk over fileHandles.
func (f *FS) flushOpenHandles(ctx context.Context) error {
	f.handles.mu.Lock()
	snapshot := make([]*fileHandle, 0, len(f.handles.m))
	for _, h := range f.handles.m {
		snapshot = append(snapshot, h)
	}
	f.handles.mu.Unlock()
	var err error
	for _, h := range snapshot {
		if h.h == nil {
			continue
		}
		hctx, cf := context.WithTimeout(ctx, 30*time.Second)
		if ferr := h.h.Flush(hctx); ferr != nil {
			err = errors.Join(err, fmt.Errorf("flush open handle %s: %w", h.path, ferr))
		}
		cf()
	}
	return err
}

func (f *FS) String() string { return "fuse" }

func (f *FS) SetDebug(debug bool) {}

// debugf logs the application-level trace (v1 --debug): mounted behind
// Options.Debug; zero cost when off.
func (f *FS) debugf(format string, args ...any) {
	if !f.debug {
		return
	}
	fmt.Fprintf(os.Stderr, "dat9 debug: "+format+"\n", args...)
}

func (f *FS) Init(s *gofuse.Server) {
	f.mu.Lock()
	f.server = s
	f.mu.Unlock()
	// Plugins start off the serve loop: go-fuse invokes Init synchronously
	// in the reader goroutine, and a slow plugin Start (HTTP reloads) would
	// stall every subsequent kernel request behind it. Mount already ran
	// Init2 before Serve; the once-guard keeps a second FUSE_INIT (or a
	// serve-loop replay) from starting every plugin twice — SSE layers a
	// second stream and leaks the first goroutine otherwise.
	f.startPluginsOnce()
}

func (f *FS) startPluginsOnce() {
	f.mu.Lock()
	if f.started {
		f.mu.Unlock()
		return
	}
	f.started = true
	f.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	err := f.chain.Start(ctx)
	f.mu.Lock()
	f.startErr = err
	f.mu.Unlock()
	cancel()
}

func (f *FS) pluginsStarted() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.started
}

// Init2 lets the mount entry start plugins with its own bounded context
// before serve begins (tests use it deterministically).
func (f *FS) Init2(ctx context.Context) {
	f.mu.Lock()
	if f.started {
		f.mu.Unlock()
		return
	}
	f.started = true
	f.mu.Unlock()
	err := f.chain.Start(ctx)
	f.mu.Lock()
	f.startErr = err
	f.mu.Unlock()
}

func (f *FS) OnUnmount() {
	f.unmounted.Store(true)
	// v1 printed the perf summary at unmount (printSummary on stderr):
	// "drive9: FUSE perf summary uptime=..." plus the counter blocks the
	// git-ops budget guard greps (workspace refresh accounting).
	if summary := f.perf.SummaryString(); summary != "" {
		fmt.Fprintf(os.Stderr, "drive9: FUSE perf summary uptime=%s\n%s",
			f.perf.UptimeString(), summary)
	}
	if line := f.perf.GitWorkspaceLine(); line != "" {
		fmt.Fprintln(os.Stderr, line)
	}
	for _, line := range f.perf.PerfLines() {
		fmt.Fprintln(os.Stderr, line)
	}
	if line := f.perf.CommitLine(); line != "" {
		fmt.Fprintln(os.Stderr, line)
	}
	_ = f.chain.Stop(context.Background())
}

func (f *FS) Forget(nodeid, nlookup uint64) {
	// v1 shouldPreserveForgottenInode: a forgotten inode with open handles
	// or pending write-plane state keeps its path mapping, so fds of
	// unlinked files keep resolving and the inode number stays stable until
	// the state drains. Metadata-only specials are kept inside the table.
	if entry, ok := f.inodes.GetEntry(nodeid); ok && !entry.IsDir && f.shouldPreserveForgotten(entry) {
		f.inodes.ForgetKeepMapping(nodeid, nlookup)
		return
	}
	f.inodes.Forget(nodeid, nlookup)
}

func (f *FS) shouldPreserveForgotten(entry *InodeEntry) bool {
	if len(f.openIdx.SnapshotInode(entry.Ino)) > 0 {
		return true
	}
	for _, h := range f.chain.Handlers() {
		if k, ok := h.(fs.ForgetKeeper); ok && k.KeepForgotten(entry.Path) {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Identity tables
// ---------------------------------------------------------------------------

type fileHandle struct {
	ino   uint64
	path  string // view path as known to the kernel
	flags uint32
	h     fs.Handle // decorator chain head
}

type handleTable struct {
	mu sync.Mutex
	m  map[uint64]*fileHandle
}

func (t *handleTable) put(fh uint64, h *fileHandle) {
	t.mu.Lock()
	t.m[fh] = h
	t.mu.Unlock()
}

func (t *handleTable) get(fh uint64) (*fileHandle, bool) {
	t.mu.Lock()
	h, ok := t.m[fh]
	t.mu.Unlock()
	return h, ok
}

func (t *handleTable) len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.m)
}

func (t *handleTable) drop(fh uint64) (*fileHandle, bool) {
	t.mu.Lock()
	h, ok := t.m[fh]
	delete(t.m, fh)
	t.mu.Unlock()
	return h, ok
}

type dirHandle struct {
	readOnce  sync.Once
	readToken chan struct{}
	path      string
	ino       uint64
	attr      fs.Attr
	entries   []fs.Entry // stable snapshot for one enumeration sequence
}

// lockRead serializes READDIR/READDIRPLUS on one handle and honors kernel
// interrupts (v1 DirHandle.lockRead -> EINTR).
func (dh *dirHandle) lockRead(ctx context.Context) bool {
	dh.readOnce.Do(func() {
		dh.readToken = make(chan struct{}, 1)
		dh.readToken <- struct{}{}
	})
	if ctx.Err() != nil {
		return false
	}
	select {
	case <-ctx.Done():
		return false
	case <-dh.readToken:
		if ctx.Err() != nil {
			dh.readToken <- struct{}{}
			return false
		}
		return true
	}
}

func (dh *dirHandle) unlockRead() {
	dh.readToken <- struct{}{}
}

type dirTable struct {
	mu sync.Mutex
	m  map[uint64]*dirHandle
}

func (t *dirTable) put(fh uint64, d *dirHandle) {
	t.mu.Lock()
	t.m[fh] = d
	t.mu.Unlock()
}

func (t *dirTable) get(fh uint64) (*dirHandle, bool) {
	t.mu.Lock()
	d, ok := t.m[fh]
	t.mu.Unlock()
	return d, ok
}

func (t *dirTable) len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.m)
}

func (t *dirTable) drop(fh uint64) (*dirHandle, bool) {
	t.mu.Lock()
	d, ok := t.m[fh]
	delete(t.m, fh)
	t.mu.Unlock()
	return d, ok
}

// lockTable implements fs.LockTable. HandleLock is taken by this facade
// around mutating data operations; every handle decorator below the facade
// assumes it held (the framework's concurrency contract).
type lockTable struct {
	paths   stripe[string]
	handles stripe[uint64]
}

func (l *lockTable) PathLock(path string) func() { return l.paths.lock(path) }

func (l *lockTable) HandleLock(fh uint64) func() { return l.handles.lock(fh) }

// stripe is a lock-per-key table without eviction; a mount observes a
// bounded set of paths and handles.
type stripe[K comparable] struct {
	mu sync.Mutex
	m  map[K]*sync.Mutex
}

func (s *stripe[K]) lock(k K) func() {
	s.mu.Lock()
	if s.m == nil {
		s.m = map[K]*sync.Mutex{}
	}
	l := s.m[k]
	if l == nil {
		l = &sync.Mutex{}
		s.m[k] = l
	}
	s.mu.Unlock()
	l.Lock()
	return l.Unlock
}

func parentDir(p string) string {
	if i := strings.LastIndexByte(p, '/'); i > 0 {
		return p[:i]
	}
	return "/"
}

// fillAttr ports v1's fillAttr: ownership falls back to the mount owner
// unless the entry carries one (chown is mount-local); zero times fall back
// to now; explicit SetAttr time overrides win over server stats; nlink
// prefers the inode's live alias count (hardlinks) with the v1 floors; rdev
// rides specials.
func (f *FS) fillAttr(a fs.Attr, ino uint64, out *gofuse.Attr) {
	var entry *InodeEntry
	if ino != 0 {
		entry, _ = f.inodes.GetEntry(ino)
	}
	size := a.Size
	if size < 0 {
		size = 0
	}
	out.Ino = ino
	out.Size = uint64(size)
	if out.Size > 0 {
		out.Blocks = (out.Size + 511) / 512
	}
	out.Mode = modeBitsOf(a)
	if entry != nil && entry.HasMode && out.Mode&syscall.S_IFMT == syscall.S_IFREG {
		// The chain below can degrade a symlink/special to a plain
		// regular attr (namespace rows rebuilt from bookkeeping carry no
		// kind); the entry's recorded type bits are the inode's identity —
		// a reply that retypes the inode makes the kernel mark it bad.
		switch entry.Mode & syscall.S_IFMT {
		case syscall.S_IFLNK, syscall.S_IFIFO, syscall.S_IFCHR,
			syscall.S_IFBLK, syscall.S_IFSOCK, syscall.S_IFDIR:
			out.Mode = entry.Mode&syscall.S_IFMT | out.Mode&0o777
		}
	}
	out.Uid, out.Gid = f.uid, f.gid
	if a.HasOwner {
		out.Uid, out.Gid = a.Uid, a.Gid
	}
	if entry != nil {
		if entry.HasUID {
			out.Uid = entry.Uid
		}
		if entry.HasGID {
			out.Gid = entry.Gid
		}
	}
	mtime := a.Mtime
	if entry != nil && entry.MtimeOverride != nil {
		mtime = *entry.MtimeOverride
	} else if entry != nil && entry.Mtime.After(mtime) {
		// Locally advanced mtimes (parent touch on child changes, setattr
		// settles) outrank the chain/listing value they race (v1 serves
		// dir mtimes from the inode table).
		mtime = entry.Mtime
	}
	if mtime.IsZero() {
		mtime = time.Now().Truncate(time.Second)
	}
	atime := a.Atime
	if entry != nil && entry.AtimeOverride != nil {
		atime = *entry.AtimeOverride
	}
	if atime.IsZero() {
		atime = mtime
	}
	ctime := mtime
	if entry != nil && entry.Ctime.After(ctime) {
		// v1 serves ctime from the entry: link-count changes (link/unlink)
		// and metadata settles advance it without touching mtime.
		ctime = entry.Ctime
	}
	out.SetTimes(&atime, &mtime, &ctime)
	out.Rdev = a.Rdev
	if entry != nil {
		out.Rdev = entry.Rdev
	}
	nlink := a.Nlink
	if entry != nil {
		// The tracked path count and the recorded count both describe the
		// same server object; the larger one is the fresher view (a
		// listing row pre-dating an in-mount unlink says 2 while Paths
		// already dropped to 1 — and a freshly listed alias row says 2
		// before this mount has walked the sibling directory).
		if entry.Nlink > nlink {
			nlink = entry.Nlink
		}
		if n := uint32(len(entry.Paths)); n > nlink && !entry.IsDir {
			nlink = n
		}
	}
	if a.IsDir() {
		if nlink == 0 {
			nlink = 2
		}
	} else if nlink == 0 && (entry == nil || !entry.Unlinked) {
		nlink = 1
	}
	out.Nlink = nlink
}

// attrTTLFor clamps the mount TTL for paths a data plane revalidates on
// its own cadence (v1 extentAttrTimeoutFor + clampExtentTTL: a JuiceFS
// ~1s hint must not lengthen what the user configured, and the mount TTL
// stays the upper bound).
func (f *FS) attrTTLFor(path string) time.Duration {
	for _, h := range f.ttlHinters {
		if ttl, ok := h.EntryTTLHint(path); ok {
			if ttl > 0 && ttl < f.attrTTL {
				return ttl
			}
			return f.attrTTL
		}
	}
	return f.attrTTL
}

func (f *FS) fillEntryOut(path string, a *fs.Attr, isDir bool, out *gofuse.EntryOut) {
	var size int64
	if a != nil {
		size = a.Size
	}
	// v1 passes `now` when the chain carries no timestamp; a chain mtime
	// (git clean-tree files serve a stable per-commit time) seeds the
	// entry so repeated stats answer the same instant — git's index trust
	// depends on it. The table never rewinds a locally advanced clock.
	seed := time.Now().Truncate(time.Second)
	if a != nil && !a.Mtime.IsZero() {
		seed = a.Mtime
	}
	var ino uint64
	if a != nil && a.ResourceID != "" {
		// v1 LookupWithIdentity: hardlink paths sharing one server object
		// share one FUSE inode (stable st_ino, shared page cache).
		ino = f.inodes.LookupWithIdentity(path, a.ResourceID, a.Nlink, isDir, size, seed)
	} else {
		ino = f.inodes.Lookup(path, isDir, size, seed)
	}
	out.NodeId = ino
	if a != nil {
		f.fillAttr(*a, ino, &out.Attr)
		mode := a.Mode
		if mode&syscall.S_IFMT == 0 || mode&syscall.S_IFMT == syscall.S_IFREG {
			if a.Kind == fs.KindSymlink {
				mode = syscall.S_IFLNK | mode&0o777
			}
		}
		if mode&syscall.S_IFMT != 0 && mode&syscall.S_IFMT != syscall.S_IFREG {
			// keep the type bits on the identity so Forget keeps the
			// entry alive (metadata-only specials) and later bookkeeping
			// (chown listing refreshes) cannot degrade the entry to a
			// plain regular mode — a retyped reply makes the kernel mark
			// the live inode bad (the lchown EIO class).
			f.inodes.UpdateMode(ino, mode)
		}
	} else {
		out.Ino = ino
		out.Nlink = 1
		if isDir {
			out.Mode = syscall.S_IFDIR | 0o755
		} else {
			out.Mode = syscall.S_IFREG | 0o644
		}
	}
	entryTTL := f.entryTTL
	if isLockFilePath(path) {
		// v1: lock files carry no entry cache — O_CREAT|O_EXCL probes must
		// revalidate immediately instead of trusting a cached ENOENT.
		entryTTL = 0
	}
	attrTTL := f.attrTTLFor(path)
	if attrTTL < entryTTL {
		entryTTL = attrTTL
	}
	out.SetAttrTimeout(attrTTL)
	out.SetEntryTimeout(entryTTL)
}

// isLockFilePath matches v1: any basename ending in .lock.
func isLockFilePath(p string) bool {
	return strings.HasSuffix(pathpkg.Base(p), ".lock")
}

// entryTTLFor is fillEntryOut's rule for per-name listings.
func (f *FS) entryTTLFor(name string) time.Duration {
	if strings.HasSuffix(name, ".lock") {
		return 0
	}
	return f.entryTTL
}

// mutated records a kernel-initiated namespace mutation: advance the path
// epoch (cross-plugin handoffs must re-check freshness) and emit the event
// for observers such as caches and overlay layers. Deleted paths only get
// the event — bumping their epoch would re-materialize inode state for a
// path that no longer exists. (v1 equivalents: dirCache.Remove/Upsert,
// cacheNegativePath, invalidateReadCacheAndTargets — all event-driven here.)
func (f *FS) mutated(kind fs.EventKind, paths ...string) {
	for _, p := range paths {
		if p == "" {
			continue
		}
		if kind != fs.EventDelete && kind != fs.EventInvalidated {
			f.epoch.NextEpoch(p)
		}
		f.events.Emit(fs.Event{Kind: kind, Path: p})
	}
}

// ---------------------------------------------------------------------------
// Namespace operations
// ---------------------------------------------------------------------------

func (f *FS) Lookup(cancel <-chan struct{}, header *gofuse.InHeader, name string, out *gofuse.EntryOut) gofuse.Status {
	child, st := f.childOf(header.NodeId, name)
	if st != gofuse.OK {
		return st
	}
	if f.isDeferredUnlink(child) {
		out.SetEntryTimeout(f.negEntryTTL)
		return gofuse.ENOENT
	}
	ctx, cf := fuseCtx(cancel)
	defer cf()
	res, err := f.dispatchErr(ctx, &fs.Op{Kind: fs.OpLookup, Path: child, Ino: header.NodeId, Kernel: header, Caller: fs.CallerFrom(*header)})
	if err != nil {
		// v1 cached the negative entry only for genuine misses; other
		// failures must not poison the name for the negative TTL.
		if fs.IsNotFound(err) {
			out.SetEntryTimeout(f.negEntryTTL)
		}
		return fs.StatusOf(err)
	}
	isDir := res.Attr != nil && res.Attr.IsDir()
	f.fillEntryOut(child, res.Attr, isDir, out)
	return gofuse.OK
}

func (f *FS) GetAttr(cancel <-chan struct{}, input *gofuse.GetAttrIn, out *gofuse.AttrOut) gofuse.Status {
	path, ok := f.inodes.GetPath(input.NodeId)
	if !ok {
		return gofuse.ENOENT
	}
	// An unlinked-while-open inode serves attrs from the entry (v1): the
	// name is gone (negative row), but the open fd's fstat must keep
	// answering with nlink=0 and the remembered size/mode.
	if entry, ok := f.inodes.GetEntry(input.NodeId); ok && entry.Unlinked {
		f.fillAttr(entryToAttr(entry), input.NodeId, &out.Attr)
		out.SetTimeout(f.attrTTL)
		return gofuse.OK
	}
	// v1: the root and directories answer from the inode entry — directory
	// attrs come from mkdir time and never revalidate (some deployments
	// reject HEAD on directories outright). Only files refresh remotely.
	if f.localDirAttrs {
		if entry, ok := f.inodes.GetEntry(input.NodeId); ok && (input.NodeId == 1 || entry.IsDir) {
			f.fillAttr(entryToAttr(entry), input.NodeId, &out.Attr)
			out.SetTimeout(f.attrTTL)
			return gofuse.OK
		}
	}
	ctx, cf := fuseCtx(cancel)
	defer cf()
	res, st := f.dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: path, Ino: input.NodeId, Fh: input.Fh(), Kernel: input, Caller: fs.CallerFrom(input.InHeader)})
	if st != gofuse.OK {
		return st
	}
	if res.Attr == nil {
		return gofuse.EIO
	}
	// v1 getAttrOnce wrote the fresh stat back into the inode entry: size,
	// revision, mode, and identity update so later listing-driven views
	// (readdir's live-size rule, CAS bases) do not serve stale entry state.
	f.inodes.UpdateSize(input.NodeId, res.Attr.Size)
	if res.Attr.Rev > 0 {
		f.inodes.UpdateRevision(input.NodeId, res.Attr.Rev)
	}
	if res.Attr.Mode != 0 && res.Attr.Mode&syscall.S_IFMT != 0 {
		f.inodes.UpdateMode(input.NodeId, res.Attr.Mode)
	}
	if res.Attr.ResourceID != "" || res.Attr.Nlink > 0 {
		f.inodes.SetIdentityWithKind(input.NodeId, res.Attr.ResourceID, res.Attr.Nlink, res.Attr.IsDir())
	}
	f.fillAttr(*res.Attr, input.NodeId, &out.Attr)
	out.SetTimeout(f.attrTTLFor(path))
	return gofuse.OK
}

// entryToAttr projects an inode-table entry onto the framework attr shape.
func entryToAttr(e *InodeEntry) fs.Attr {
	kind := fs.KindFile
	if e.IsDir {
		kind = fs.KindDir
	} else if e.HasMode && e.Mode&syscall.S_IFMT == syscall.S_IFLNK {
		kind = fs.KindSymlink
	}
	mode := e.Mode
	if !e.HasMode {
		mode = 0o644
		if e.IsDir {
			mode = 0o755
		}
	}
	return fs.Attr{Kind: kind, Mode: mode & 0o7777, Size: e.Size, Rev: e.Revision, Mtime: e.Mtime,
		Atime: e.Atime, Rdev: e.Rdev, Nlink: e.Nlink,
		Uid: e.Uid, Gid: e.Gid, HasOwner: e.HasUID || e.HasGID}
}

// posixNameMax is the kernel NAME_MAX the facade enforces (v1).
const posixNameMax = 255

// chownPreserveID is the kernel's uint32(-1) sentinel for "do not change
// this field" in chown calls (v1 resolveSetAttrOwner).
const chownPreserveID = ^uint32(0)

// resolveSetAttrOwner ports v1 resolveSetAttrOwner: normalize the preserve
// sentinel away.
func resolveSetAttrOwner(input *gofuse.SetAttrIn) (uid uint32, hasUID bool, gid uint32, hasGID bool) {
	uid, hasUID = input.GetUID()
	gid, hasGID = input.GetGID()
	if hasUID && uid == chownPreserveID {
		hasUID = false
	}
	if hasGID && gid == chownPreserveID {
		hasGID = false
	}
	return uid, hasUID, gid, hasGID
}

// checkSetAttrOwnerForCaller ports v1's EPERM rules (supplementary-group
// lookup aside — the caller's primary gid must match for group changes).
func (f *FS) checkSetAttrOwnerForCaller(caller fs.Caller, entry *InodeEntry, uid uint32, hasUID bool, gid uint32, hasGID bool) error {
	if !hasUID && !hasGID {
		return nil
	}
	curUid, curGid := f.uid, f.gid
	if entry != nil {
		if entry.HasUID {
			curUid = entry.Uid
		}
		if entry.HasGID {
			curGid = entry.Gid
		}
	}
	if hasUID && uid != curUid && caller.Uid != 0 {
		return syscall.EPERM
	}
	if hasGID && gid != curGid && caller.Uid != 0 {
		if caller.Uid != curUid {
			return syscall.EPERM
		}
		if caller.Gid != gid && !containsUint32(processSupplementaryGroups(caller.Pid), gid) {
			return syscall.EPERM
		}
	}
	return nil
}

// checkSetAttrModeForCaller ports v1 setAttrModeForCaller: a non-root,
// non-owner caller may only change the mode in the kernel's kill-priv form
// (SUID/SGID cleared alongside a write); anything else is EPERM. A non-root
// owner requesting SGID on a regular file keeps it only when their primary
// or supplementary groups include the file's group.
func (f *FS) checkSetAttrModeForCaller(caller fs.Caller, entry *InodeEntry, input *gofuse.SetAttrIn) (uint32, error) {
	mode := input.Mode & 0o7777
	ownerUid, ownerGid := f.uid, f.gid
	isRegular := true
	var oldMode uint32
	if entry != nil {
		if entry.HasUID {
			ownerUid = entry.Uid
		}
		if entry.HasGID {
			ownerGid = entry.Gid
		}
		if entry.HasMode {
			oldMode = entry.Mode & 0o7777
		}
		isRegular = !entry.IsDir && (entry.Mode&syscall.S_IFMT == syscall.S_IFREG || entry.Mode&syscall.S_IFMT == 0)
	}
	if caller.Uid != 0 && caller.Uid != ownerUid {
		cleared := mode &^ 0o6000
		oldCleared := oldMode &^ 0o6000
		if cleared == oldCleared && mode&0o6000 == 0 {
			return mode, nil
		}
		return 0, syscall.EPERM
	}
	if caller.Uid != 0 && isRegular && mode&0o2000 != 0 &&
		caller.Gid != ownerGid && !containsUint32(processSupplementaryGroups(caller.Pid), ownerGid) {
		mode &^= 0o2000
	}
	return mode, nil
}

func (f *FS) SetAttr(cancel <-chan struct{}, input *gofuse.SetAttrIn, out *gofuse.AttrOut) gofuse.Status {
	path, ok := f.inodes.GetPath(input.NodeId)
	if !ok {
		return gofuse.ENOENT
	}
	entry, _ := f.inodes.GetEntry(input.NodeId)
	caller := fs.CallerFrom(input.InHeader)
	// v1: times and ownership are mount-local (the backend derives mtimes
	// from writes and has no chown verb). Apply them before anything else
	// so utimensat/chown never wait behind remote work, arming the local
	// overrides that later stats and commit refreshes must not clobber.
	if mtime, ok := input.GetMTime(); ok {
		f.inodes.SetLocalMtime(input.NodeId, mtime)
	}
	if atime, ok := input.GetATime(); ok {
		f.inodes.SetLocalAtime(input.NodeId, atime)
	}
	if uid, hasUID, gid, hasGID := resolveSetAttrOwner(input); hasUID || hasGID {
		if err := f.checkSetAttrOwnerForCaller(caller, entry, uid, hasUID, gid, hasGID); err != nil {
			return gofuse.Status(err.(syscall.Errno))
		}
		f.inodes.UpdateOwner(input.NodeId, uid, gid, hasUID, hasGID)
		// Ownership is mount-local (v1): chown must also refresh the cached
		// listing entry so chain-level stats (renameguard class checks)
		// resolve the entry's class by its real owner, not the mount's.
		if e, ok := f.inodes.GetEntry(input.NodeId); ok {
			f.Ctx.DirCache.Upsert(parentDirOf(e.Path), fs.DirEntryInfo{
				Name: pathpkg.Base(e.Path), IsDir: e.IsDir, Size: e.Size,
				Mtime: time.Now(), Mode: e.Mode, HasMode: e.HasMode,
				Uid: uid, Gid: gid, HasUID: hasUID, HasGID: hasGID,
			})
		}
	}
	ctx, cf := fuseCtx(cancel)
	defer cf()
	op := &fs.Op{Kind: fs.OpSetAttr, Path: path, Ino: input.NodeId, Fh: input.Fh, Kernel: input, Caller: fs.CallerFrom(input.InHeader), Size: -1}
	if owner := f.openHandle(input.Fh); owner != nil {
		// The owning plane's guards key on op.Handle; leaving it unset
		// classified the local plane's own open files as foreign and
		// forwarded ftruncate to the remote chain (ENOENT on
		// local-only files).
		op.Handle = owner
	}

	if uid, hasUID, gid, hasGID := resolveSetAttrOwner(input); hasUID || hasGID {
		op.SetUID, op.SetGID, op.HasSetUID, op.HasSetGID = uid, gid, hasUID, hasGID
	}
	if input.Valid&gofuse.FATTR_MODE != 0 {
		// v1 setAttrModeForCaller: enforce the owner/EPERM rules first (a
		// non-owner may only clear SUID/SGID; a non-root owner outside the
		// file's group loses a requested SGID bit), then the kernel
		// file_remove_privs note: when a chown arrives with a mode that only
		// differs from the current one by cleared SUID/SGID, the kernel
		// merged its kill-priv into the SETATTR — that is not a chmod by the
		// caller. Apply the ownership change but keep the file's privilege
		// bits (a privileged chown preserves them).
		mode, merr := f.checkSetAttrModeForCaller(caller, entry, input)
		if merr != nil {
			return gofuse.Status(merr.(syscall.Errno))
		}
		if input.Valid&(gofuse.FATTR_UID|gofuse.FATTR_GID) != 0 && entry != nil && entry.HasMode {
			cur := entry.Mode & 0o7777
			switch {
			case mode == cur&^0o6000 && cur&0o6000 != 0 && caller.Uid == 0:
				// Privileged chown: the kernel's kill-priv merge is not the
				// caller's chmod — keep the privilege bits.
				op.HasMode = false
			default:
				op.Mode, op.HasMode = mode, true
			}
		} else {
			op.Mode, op.HasMode = mode, true
		}
	}
	if input.Valid&gofuse.FATTR_SIZE != 0 {
		// v1 guard: sizes beyond int64 (e.g. ftruncate(-1) arriving as
		// 0xFFFFFFFFFFFFFFFF) are EFBIG before any int64 conversion or
		// buffer allocation can overflow.
		if input.Size > uint64(math.MaxInt64) {
			return gofuse.Status(syscall.EFBIG)
		}
		op.Size = int64(input.Size)
	}
	if input.Valid&(gofuse.FATTR_MTIME|gofuse.FATTR_MTIME_NOW) != 0 {
		if input.Valid&gofuse.FATTR_MTIME_NOW != 0 {
			op.Mtime = time.Now()
		} else {
			op.Mtime = time.Unix(int64(input.Mtime), int64(input.Mtimensec))
		}
	}
	res, st := f.dispatch(ctx, op)
	if st != gofuse.OK {
		return st
	}
	f.mutated(fs.EventWrite, path)
	if res.Attr == nil {
		if res, st = f.dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: path, Ino: input.NodeId}); st != gofuse.OK {
			return st
		}
	}
	if res.Attr == nil {
		return gofuse.EIO
	}
	// A chmod — or the mode the kernel merges into a chown for its
	// kill-priv — arrives as permission bits, and the chain's probe reply
	// may carry them without the file-type bits (a namespace row rebuilt
	// from bookkeeping, a permission-only remote stat). The inode's type is
	// immutable under setattr: restore it from the entry before settling
	// the mode anywhere (entry, listing row, reply), or a stripped type
	// retypes the live kernel inode and every later op on it dies with EIO.
	if e, eok := f.inodes.GetEntry(input.NodeId); eok && e.HasMode && e.Mode&syscall.S_IFMT != 0 && res.Attr.Mode&syscall.S_IFMT == 0 {
		res.Attr.Mode = e.Mode&syscall.S_IFMT | res.Attr.Mode&0o7777
	}
	// v1 settles a setattr in place (UpdateMode/UpdateSize +
	// cacheEntryForPath): the inode entry and the cached listing entry must
	// carry the new mode/size immediately, or the next GetAttr inside the
	// attr TTL serves the pre-setattr values from the namespace cache.
	f.inodes.UpdateMode(input.NodeId, res.Attr.Mode)
	if input.Valid&gofuse.FATTR_MODE != 0 {
		// A successful chmod updates ctime (v1): fillAttr derives ctime
		// from mtime, so advance the entry clock with the applied mode.
		f.inodes.UpdateMtime(input.NodeId, time.Now().Truncate(time.Second))
	}
	if input.Valid&gofuse.FATTR_SIZE != 0 {
		f.inodes.UpdateSize(input.NodeId, res.Attr.Size)
		// A truncation is a metadata change: bump the entry mtime so the
		// ctime/mtime comparisons around truncate(2)/ftruncate(2) see it
		// (v1 updates ctime on truncate).
		f.inodes.UpdateMtime(input.NodeId, time.Now().Truncate(time.Second))
	}
	f.dirCacheUpsert(path, res.Attr)
	f.fillAttr(*res.Attr, input.NodeId, &out.Attr)
	out.SetTimeout(f.attrTTL)
	return gofuse.OK
}

func (f *FS) Mknod(cancel <-chan struct{}, input *gofuse.MknodIn, name string, out *gofuse.EntryOut) gofuse.Status {
	child, st := f.childOf(input.NodeId, name)
	if st != gofuse.OK {
		return st
	}
	f.flushDeferredUnlink(child)
	ctx, cf := fuseCtx(cancel)
	defer cf()
	res, err := f.dispatchErr(ctx, &fs.Op{Kind: fs.OpMknod, Path: child, Mode: input.Mode, Rdev: input.Rdev, Ino: input.NodeId, Kernel: input, Caller: fs.CallerFrom(input.InHeader)})
	if err != nil {
		return fs.StatusOf(err)
	}
	f.mutated(fs.EventWrite, child)
	f.fillEntryOut(child, res.Attr, false, out)
	if res.Attr != nil && res.Attr.Mode&syscall.S_IFMT != 0 && res.Attr.Mode&syscall.S_IFMT != syscall.S_IFREG {
		out.Mode = res.Attr.Mode // specials carry their own S_IFMT
		// The device number rides the record (mknod/11 major,minor rows):
		// stamp both the kernel reply and the inode entry.
		out.Rdev = res.Attr.Rdev
		f.inodes.UpdateRdev(out.NodeId, res.Attr.Rdev)
	} else {
		// v1 mknodRegular upserted the fresh entry into the parent listing
		// (server listings lag creation).
		f.dirCacheUpsert(child, res.Attr)
	}
	caller := fs.CallerFrom(input.InHeader)
	f.stampOwner(out.NodeId, caller)
	out.Uid, out.Gid = caller.Uid, caller.Gid
	f.touchParentMtime(input.NodeId)
	return gofuse.OK
}

func (f *FS) Mkdir(cancel <-chan struct{}, input *gofuse.MkdirIn, name string, out *gofuse.EntryOut) gofuse.Status {
	child, st := f.childOf(input.NodeId, name)
	if st != gofuse.OK {
		return st
	}
	f.flushDeferredUnlink(child)
	ctx, cf := fuseCtx(cancel)
	defer cf()
	mode := fs.Attr{Kind: fs.KindDir, Mode: input.Mode & 0o7777}
	if _, st := f.dispatch(ctx, &fs.Op{Kind: fs.OpMkdir, Path: child, Mode: input.Mode & 0o7777, Ino: input.NodeId, Kernel: input, Caller: fs.CallerFrom(input.InHeader)}); st != gofuse.OK {
		return st
	}
	f.mutated(fs.EventWrite, child)
	f.fillEntryOut(child, &mode, true, out)
	f.dirCacheUpsert(child, &mode)
	caller := fs.CallerFrom(input.InHeader)
	f.stampOwner(out.NodeId, caller)
	out.Uid, out.Gid = caller.Uid, caller.Gid
	f.inodes.AdjustLinkCount(input.NodeId, 1) // a new subdirectory bumps the parent dir nlink (v1)
	f.touchParentMtime(input.NodeId)
	return gofuse.OK
}

func (f *FS) Unlink(cancel <-chan struct{}, header *gofuse.InHeader, name string) gofuse.Status {
	child, st := f.childOf(header.NodeId, name)
	if st != gofuse.OK {
		return st
	}
	f.debugf("unlink start path=%s parent_ino=%d name=%s", child, header.NodeId, name)
	ctx, cf := fuseCtx(cancel)
	defer cf()
	if f.openIdx.HasOpenPath(child) {
		// POSIX unlink-of-open-file (v1 survivor): the name disappears now,
		// the open fd keeps reading its data, and the remote entry dies with
		// the last close — deferring the delete keeps fd reads working in a
		// sync core with no local data plane.
		f.openIdx.UnlinkPath(child)
		for _, h := range f.chain.Handlers() {
			if mu, ok := h.(interface{ MarkPathUnlinked(string) }); ok {
				mu.MarkPathUnlinked(child)
			}
		}
		// Capture the ino before the preserve drops the byPath mapping.
		ino, iok := f.inodes.GetInode(child)
		f.inodes.RemoveLinkPreserve(child)
		if iok {
			f.mu.Lock()
			f.deferredUnlinks[ino] = child
			f.mu.Unlock()
		}
		f.touchParentMtime(header.NodeId)
		f.mutated(fs.EventDelete, child)
		f.markNameNegative(child)
		return gofuse.OK
	}
	if _, st := f.dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: child, Ino: header.NodeId, Kernel: header, Caller: fs.CallerFrom(*header)}); st != gofuse.OK {
		return st
	}
	f.openIdx.UnlinkPath(child)
	// Capture the surviving aliases BEFORE the removal drops the path
	// mapping (their cached rows hold the pre-unlink link count).
	siblings := f.inodes.SurvivingAliases(child)
	f.inodes.RemoveLink(child)
	f.dropSiblingRows(child, siblings)
	f.touchParentMtime(header.NodeId)
	f.mutated(fs.EventDelete, child)
	f.markNameNegative(child)
	return gofuse.OK
}

// dropSiblingRows invalidates the cached listing rows of a hardlink's
// surviving aliases after one alias is removed: their recorded link count
// is stale until the next listing (the stat answered the pre-unlink count).
func (f *FS) dropSiblingRows(removed string, siblings []string) {
	if f.Ctx.DirCache == nil {
		return
	}
	for _, p := range siblings {
		if p == removed {
			continue
		}
		if dc, ok := f.Ctx.DirCache.(*dircache.DirCache); ok {
			dc.Remove(parentDirOf(p), pathpkg.Base(p))
		}
	}
}

func (f *FS) Rmdir(cancel <-chan struct{}, header *gofuse.InHeader, name string) gofuse.Status {
	child, st := f.childOf(header.NodeId, name)
	if st != gofuse.OK {
		return st
	}
	f.flushDeferredUnlinksUnder(child)
	ctx, cf := fuseCtx(cancel)
	defer cf()
	if _, st := f.dispatch(ctx, &fs.Op{Kind: fs.OpRmdir, Path: child, Ino: header.NodeId, Kernel: header, Caller: fs.CallerFrom(*header)}); st != gofuse.OK {
		return st
	}
	f.inodes.AdjustLinkCount(header.NodeId, -1)
	f.touchParentMtime(header.NodeId)
	f.mutated(fs.EventDelete, child)
	f.markNameNegative(child)
	return gofuse.OK
}

func (f *FS) Rename(cancel <-chan struct{}, input *gofuse.RenameIn, oldName string, newName string) gofuse.Status {
	// renameat2 flags are not implemented (v1): drive9 rename always
	// replaces the target, so doing a plain rename for RENAME_NOREPLACE
	// would silently clobber the destination and RENAME_EXCHANGE would
	// destroy one side instead of swapping the two. The kernel forwards
	// the flags on any mount that negotiated RENAME2, so refuse them here;
	// callers that probe with RENAME_NOREPLACE (GNU mv, some rename
	// fallbacks) expect EINVAL and retry without flags.
	if input.Flags != 0 {
		return gofuse.Status(syscall.EINVAL)
	}
	oldPath, st := f.childOf(input.NodeId, oldName)
	if st != gofuse.OK {
		return st
	}
	newPath, st := f.childOf(input.Newdir, newName)
	if st != gofuse.OK {
		return st
	}
	// v1: rename onto the same path is an immediate success — no remote
	// round trip, no cache churn.
	if oldPath == newPath {
		return gofuse.OK
	}
	f.debugf("rename start old=%s new=%s", oldPath, newPath)
	f.flushDeferredUnlink(newPath)
	ctx, cf := fuseCtx(cancel)
	defer cf()
	if _, err := f.dispatchErr(ctx, &fs.Op{Kind: fs.OpRename, Path: oldPath, Path2: newPath, Ino: input.NodeId, Ino2: input.Newdir, Kernel: input, Caller: fs.CallerFrom(input.InHeader)}); err != nil {
		return fs.StatusOf(err)
	}
	f.inodes.Rename(oldPath, newPath)
	f.openIdx.RenamePathPrefix(oldPath, newPath)

	// A renamed directory moves its parent link: -1 old parent, +1 new
	// (v1 adjustDirectoryLinkCount both sides).
	if ino, ok := f.inodes.GetInode(newPath); ok {
		if e, eok := f.inodes.GetEntry(ino); eok && e.IsDir {
			f.inodes.AdjustLinkCount(input.NodeId, -1)
			f.inodes.AdjustLinkCount(input.Newdir, 1)
		}
	}
	f.touchParentMtime(input.NodeId)
	f.touchParentMtimeByName(newPath)
	f.mutated(fs.EventDelete, oldPath)
	f.mutated(fs.EventWrite, newPath)
	// v1 notifyRenameTarget: invalidate ONLY the rename TARGET — the kernel
	// moves the old name's dentry natively, and dropping it from userspace
	// disturbs the bookkeeping for the next create of the same name in
	// rename-churn loops. The target notify forces the next stat of the
	// replaced destination to re-LOOKUP instead of serving a type-mismatched
	// cached dentry (the EIO class).
	if pino2, ok := f.inodes.GetInode(parentDirOf(newPath)); ok {
		f.InvalidateEntry(pino2, pathpkg.Base(newPath))
	}
	// v1 surgical rename bookkeeping, applied after the events so their
	// invalidations don't wipe it: remove the old name and upsert the new
	// one in the cached listings — the next stat then answers locally
	// instead of racing backend replica lag (create-after-rename EEXIST).
	if dc, ok := f.Ctx.DirCache.(*dircache.DirCache); ok {
		// v1 finishLocalRename: the old name is removed AND marked negative —
		// a plain remove leaves the next stat to race backend replica lag,
		// and a lagging replica still showing the moved entry resurrects the
		// old name (rename/09-10: re-create EEXIST, 258 rows in close-sync).
		oldParent, oldName := parentDirOf(oldPath), pathpkg.Base(oldPath)
		dc.Remove(oldParent, oldName)
		dc.MarkNegative(oldParent, oldName)
		if ino, iok := f.inodes.GetInode(newPath); iok {
			if e, eok := f.inodes.GetEntry(ino); eok {
				dc.Upsert(parentDirOf(newPath), fs.DirEntryInfo{
					Name: pathpkg.Base(newPath), IsDir: e.IsDir, Size: e.Size,
					Mtime: time.Now(), Mode: e.Mode, HasMode: e.HasMode,
					Uid: e.Uid, Gid: e.Gid, HasUID: e.HasUID, HasGID: e.HasGID,
					Nlink: e.Nlink,
				})
				if e.IsDir {
					dc.InvalidatePrefix(oldPath)
				}
			}
		}
	}
	return gofuse.OK
}

func (f *FS) Link(cancel <-chan struct{}, input *gofuse.LinkIn, filename string, out *gofuse.EntryOut) gofuse.Status {
	oldPath, ok := f.inodes.GetPath(input.Oldnodeid)
	if !ok {
		return gofuse.ENOENT
	}
	newPath, st := f.childOf(input.NodeId, filename)
	if st != gofuse.OK {
		return st
	}
	ctx, cf := fuseCtx(cancel)
	defer cf()
	res, st := f.dispatch(ctx, &fs.Op{Kind: fs.OpLink, Path: oldPath, Path2: newPath, Ino: input.NodeId, Ino2: input.Oldnodeid, Kernel: input, Caller: fs.CallerFrom(input.InHeader)})
	if st != gofuse.OK {
		return st
	}
	f.touchParentMtime(input.NodeId)
	f.mutated(fs.EventWrite, newPath)
	// v1 Link: the new name is an alias of the SOURCE inode — shared nodeid
	// and one Paths set — so lookups of either name resolve to the same
	// kernel inode (shared page cache) and unlink-survivor can rebind open
	// writers onto a surviving hardlink later. The alias must be mapped
	// BEFORE fillEntryOut: the fill allocates a private inode for an
	// unmapped path, and AddAliasIfAbsent would then no-op on the busy path,
	// leaving the alias on its own inode (stale reads through the source).
	if entry, ok := f.inodes.GetEntry(input.Oldnodeid); ok {
		f.inodes.AddAliasIfAbsent(input.Oldnodeid, newPath, entry.ResourceID, entry.Nlink, entry.IsDir, entry.Size, entry.Mtime)
	} else {
	}
	// v1 Link advances the source inode's ctime: unlink/00's hard-link rows
	// compare the survivor's ctime across link+unlink cycles.
	f.inodes.UpdateCtime(input.Oldnodeid, time.Now())
	f.fillEntryOut(newPath, res.Attr, false, out)
	if res.Attr != nil {
		f.dirCacheUpsert(newPath, res.Attr)
	}
	return gofuse.OK
}

// SurvivorAlias reports a surviving hardlink alias of path (v1
// appendReadPaths minus the unlinked name) — the unlink-survivor target for
// open writeback writers.
func (f *FS) SurvivorAlias(path string) (string, bool) {
	ino, ok := f.inodes.GetInode(path)
	if !ok {
		return "", false
	}
	entry, ok := f.inodes.GetEntry(ino)
	if !ok || len(entry.Paths) <= 1 {
		return "", false
	}
	for p := range entry.Paths {
		if p != path {
			return p, true
		}
	}
	return "", false
}

func (f *FS) Symlink(cancel <-chan struct{}, header *gofuse.InHeader, pointedTo string, linkName string, out *gofuse.EntryOut) gofuse.Status {
	child, st := f.childOf(header.NodeId, linkName)
	if st != gofuse.OK {
		return st
	}
	f.flushDeferredUnlink(child)
	ctx, cf := fuseCtx(cancel)
	defer cf()
	if _, st := f.dispatch(ctx, &fs.Op{Kind: fs.OpSymlink, Path: child, Target: pointedTo, Ino: header.NodeId, Kernel: header, Caller: fs.CallerFrom(*header)}); st != gofuse.OK {
		return st
	}
	f.mutated(fs.EventWrite, child)
	// The link's own identity: S_IFLNK mode, size = target length. The
	// entry and the cached listing entry both need the type bits, or a
	// later stat retypes the link as a regular file (kernel EIO on the
	// attr/dentry type mismatch).
	attr := &fs.Attr{Kind: fs.KindSymlink, Mode: uint32(syscall.S_IFLNK) | 0o777, Size: int64(len(pointedTo))}
	f.fillEntryOut(child, attr, false, out)
	f.inodes.UpdateMode(out.NodeId, attr.Mode)
	f.dirCacheUpsert(child, attr)
	caller := fs.CallerFrom(*header)
	f.stampOwner(out.NodeId, caller)
	out.Uid, out.Gid = caller.Uid, caller.Gid
	f.touchParentMtime(header.NodeId)
	return gofuse.OK
}

func (f *FS) Readlink(cancel <-chan struct{}, header *gofuse.InHeader) ([]byte, gofuse.Status) {
	path, ok := f.inodes.GetPath(header.NodeId)
	if !ok {
		return nil, gofuse.ENOENT
	}
	ctx, cf := fuseCtx(cancel)
	defer cf()
	res, st := f.dispatch(ctx, &fs.Op{Kind: fs.OpReadlink, Path: path, Ino: header.NodeId, Kernel: header, Caller: fs.CallerFrom(*header)})
	if st != gofuse.OK {
		return nil, st
	}
	return res.Data, gofuse.OK
}

// hasPOSIXAccess ports v1 hasPOSIXAccess: permission-class check
// (owner/group/other) of the requested mask bits against the mode. Group
// membership reduces to the caller's primary gid (v1 additionally consulted
// supplementary groups via /proc).
func hasPOSIXAccess(caller fs.Caller, ownerUid, ownerGid uint32, mode uint32, mask uint32) bool {
	if caller.Uid == 0 {
		// v1: root passes read/write regardless; X needs some execute bit.
		if mask&0b011 == 0 {
			return true
		}
		return mode&0b001 != 0 || mode&0b010 != 0 || mode&0b100 != 0
	}
	shift := uint32(0)
	switch {
	case caller.Uid == ownerUid:
		shift = 6
	case caller.Gid == ownerGid, containsUint32(processSupplementaryGroups(caller.Pid), ownerGid):
		// The file's group may arrive only as a supplementary group of
		// the caller (v1 consulted /proc/<pid>/status too); testing the
		// other bits there returned EACCES for granted access.
		shift = 3
	}
	return mode>>shift&mask == mask
}

func (f *FS) Access(cancel <-chan struct{}, input *gofuse.AccessIn) gofuse.Status {
	path, ok := f.inodes.GetPath(input.NodeId)
	if !ok {
		return gofuse.ENOENT
	}
	ctx, cf := fuseCtx(cancel)
	defer cf()
	res, st := f.dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: path, Mask: input.Mask, Ino: input.NodeId, Kernel: input, Caller: fs.CallerFrom(input.InHeader)})
	if st != gofuse.OK {
		return st
	}
	if res.Attr == nil {
		return gofuse.EIO
	}
	ownerUid, ownerGid := f.uid, f.gid
	if res.Attr.HasOwner {
		ownerUid, ownerGid = res.Attr.Uid, res.Attr.Gid
	}
	if entry, ok := f.inodes.GetEntry(input.NodeId); ok {
		if entry.HasUID {
			ownerUid = entry.Uid
		}
		if entry.HasGID {
			ownerGid = entry.Gid
		}
	}
	caller := fs.CallerFrom(input.InHeader)
	if !hasPOSIXAccess(caller, ownerUid, ownerGid, res.Attr.Mode&0o777, input.Mask) {
		return gofuse.EACCES
	}
	return gofuse.OK
}

// ---------------------------------------------------------------------------
// Extended attributes
// ---------------------------------------------------------------------------

func (f *FS) GetXAttr(cancel <-chan struct{}, header *gofuse.InHeader, attr string, dest []byte) (uint32, gofuse.Status) {
	path, ok := f.inodes.GetPath(header.NodeId)
	if !ok {
		return 0, gofuse.ENOENT
	}
	ctx, cf := fuseCtx(cancel)
	defer cf()
	res, err := f.dispatchErr(ctx, &fs.Op{Kind: fs.OpGetXattr, Path: path, Name: attr, Ino: header.NodeId, Kernel: header, Caller: fs.CallerFrom(*header)})
	if err != nil {
		return 0, fs.StatusOf(err)
	}
	// A zero-length destination is the standard size probe: it must
	// succeed with the byte count so callers can allocate the follow-up
	// buffer (returning ERANGE there left every probe failing).
	if len(dest) == 0 {
		return uint32(len(res.Data)), gofuse.OK
	}
	if len(dest) < len(res.Data) {
		return uint32(len(res.Data)), gofuse.Status(syscall.ERANGE)
	}
	copy(dest, res.Data)
	return uint32(len(res.Data)), gofuse.OK
}

func (f *FS) ListXAttr(cancel <-chan struct{}, header *gofuse.InHeader, dest []byte) (uint32, gofuse.Status) {
	path, ok := f.inodes.GetPath(header.NodeId)
	if !ok {
		return 0, gofuse.ENOENT
	}
	ctx, cf := fuseCtx(cancel)
	defer cf()
	res, err := f.dispatchErr(ctx, &fs.Op{Kind: fs.OpListXattr, Path: path, Ino: header.NodeId, Kernel: header, Caller: fs.CallerFrom(*header)})
	if err != nil {
		return 0, fs.StatusOf(err)
	}
	// Size probe: same as GetXAttr — success with the required size.
	if len(dest) == 0 {
		return uint32(len(res.Data)), gofuse.OK
	}
	if len(dest) < len(res.Data) {
		return uint32(len(res.Data)), gofuse.Status(syscall.ERANGE)
	}
	copy(dest, res.Data)
	return uint32(len(res.Data)), gofuse.OK
}

func (f *FS) SetXAttr(cancel <-chan struct{}, input *gofuse.SetXAttrIn, attr string, data []byte) gofuse.Status {
	path, ok := f.inodes.GetPath(input.NodeId)
	if !ok {
		return gofuse.ENOENT
	}
	ctx, cf := fuseCtx(cancel)
	defer cf()
	if _, st := f.dispatch(ctx, &fs.Op{Kind: fs.OpSetXattr, Path: path, Name: attr, Data: data, Flags: input.Flags, Ino: input.NodeId, Kernel: input, Caller: fs.CallerFrom(input.InHeader)}); st != gofuse.OK {
		return st
	}
	f.mutated(fs.EventWrite, path)
	return gofuse.OK
}

func (f *FS) RemoveXAttr(cancel <-chan struct{}, header *gofuse.InHeader, attr string) gofuse.Status {
	path, ok := f.inodes.GetPath(header.NodeId)
	if !ok {
		return gofuse.ENOENT
	}
	ctx, cf := fuseCtx(cancel)
	defer cf()
	if _, st := f.dispatch(ctx, &fs.Op{Kind: fs.OpRemoveXattr, Path: path, Name: attr, Ino: header.NodeId, Kernel: header, Caller: fs.CallerFrom(*header)}); st != gofuse.OK {
		return st
	}
	f.mutated(fs.EventWrite, path)
	return gofuse.OK
}

// ---------------------------------------------------------------------------
// Open / create and the handle chain
// ---------------------------------------------------------------------------

func (f *FS) Create(cancel <-chan struct{}, input *gofuse.CreateIn, name string, out *gofuse.CreateOut) gofuse.Status {
	child, st := f.childOf(input.NodeId, name)
	if st != gofuse.OK {
		return st
	}
	f.flushDeferredUnlink(child)
	ctx, cf := fuseCtx(cancel)
	defer cf()
	res, err := f.dispatchErr(ctx, &fs.Op{Kind: fs.OpCreate, Path: child, Flags: input.Flags, Mode: input.Mode & 0o7777, Ino: input.NodeId, Kernel: input, Caller: fs.CallerFrom(input.InHeader)})
	if err != nil {
		return fs.StatusOf(err)
	}
	f.mutated(fs.EventWrite, child)
	isDir := res.Attr != nil && res.Attr.IsDir()
	f.fillEntryOut(child, res.Attr, isDir, &out.EntryOut)
	f.dirCacheUpsert(child, res.Attr)
	// v1 Create records the create mode on the inode entry
	// (inodes.UpdateMode): the entry's mode is what later Landed settles and
	// namespace-cache rows preserve — without it, a create-mode file (chmod/12:
	// 04777) answers every later stat as the 0644 default (HasMode=false).
	if res.Attr != nil && res.Attr.Mode != 0 {
		kind := uint32(syscall.S_IFREG)
		if isDir {
			kind = uint32(syscall.S_IFDIR)
		}
		f.inodes.UpdateMode(out.NodeId, kind|res.Attr.Mode&0o7777)
	}
	if res.Handle != nil {
		fh := f.nextFh.Add(1)
		hnd := &fileHandle{ino: out.NodeId, path: child, flags: input.Flags, h: res.Handle}
		f.handles.put(fh, hnd)
		f.openIdx.Add(hnd)
		out.Fh = fh
	}
	out.OpenFlags = res.OpenFlags
	// v1 mount-local ownership: the fresh entry belongs to its creator —
	// including the reply attrs, which the kernel caches for the attr TTL.
	caller := fs.CallerFrom(input.InHeader)
	f.stampOwner(out.NodeId, caller)
	out.Uid, out.Gid = caller.Uid, caller.Gid
	f.touchParentMtime(input.NodeId)
	return gofuse.OK
}

func (f *FS) Open(cancel <-chan struct{}, input *gofuse.OpenIn, out *gofuse.OpenOut) gofuse.Status {
	path, ok := f.inodes.GetPath(input.NodeId)
	if !ok {
		return gofuse.ENOENT
	}
	ctx, cf := fuseCtx(cancel)
	defer cf()
	caller := fs.CallerFrom(input.InHeader)
	// Userspace permission gate for mounts without default_permissions:
	// the chain's OpCreate carries no mode context of its own, so a chmod
	// 000 file or a write open of a read-only mode would sail through.
	if !f.defaultPermissions {
		if entry, eok := f.inodes.GetEntry(input.NodeId); eok && entry.HasMode {
			mode := entry.Mode & 0o777
			ownerUid, ownerGid := f.uid, f.gid
			if entry.HasUID {
				ownerUid = entry.Uid
			}
			if entry.HasGID {
				ownerGid = entry.Gid
			}
			if acc := input.Flags & uint32(syscall.O_ACCMODE); acc != uint32(syscall.O_RDONLY) {
				if !hasPOSIXAccess(caller, ownerUid, ownerGid, mode, 0b010) {
					return gofuse.EACCES
				}
			}
		}
	}
	res, err := f.dispatchErr(ctx, &fs.Op{Kind: fs.OpCreate, Path: path, Flags: input.Flags, Ino: input.NodeId, Kernel: input, Caller: caller})
	if err != nil {
		return fs.StatusOf(err)
	}
	if res.Handle != nil {
		fh := f.nextFh.Add(1)
		hnd := &fileHandle{ino: input.NodeId, path: path, flags: input.Flags, h: res.Handle}
		f.handles.put(fh, hnd)
		f.openIdx.Add(hnd)
		out.Fh = fh
	}
	// v1 openFlagsForHandle: KEEP_CACHE unless the data plane asked for
	// DIRECT_IO or a bypass window is armed — the two are mutually
	// exclusive replies, never ORed together.
	out.OpenFlags = res.OpenFlags
	if !f.bypass.active(input.NodeId) && out.OpenFlags&gofuse.FOPEN_DIRECT_IO == 0 {
		out.OpenFlags |= gofuse.FOPEN_KEEP_CACHE
	}
	if f.bypass.active(input.NodeId) {
		out.OpenFlags = (out.OpenFlags &^ gofuse.FOPEN_KEEP_CACHE) | gofuse.FOPEN_DIRECT_IO
	}
	return gofuse.OK
}

// openHandle returns the handle chain head for fh, or nil to fall back to
// path-addressed dispatch.
func (f *FS) openHandle(fh uint64) fs.Handle {
	if fh == 0 {
		return nil
	}
	if h, ok := f.handles.get(fh); ok {
		return h.h
	}
	return nil
}

// ---------------------------------------------------------------------------
// Data operations
// ---------------------------------------------------------------------------

func (f *FS) Read(cancel <-chan struct{}, input *gofuse.ReadIn, buf []byte) (gofuse.ReadResult, gofuse.Status) {
	ctx, cf := fuseCtx(cancel)
	defer cf()
	if h := f.openHandle(input.Fh); h != nil {
		n, err := h.Read(ctx, int64(input.Offset), buf)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, fs.StatusOf(err)
		}
		if n > 0 {
			return gofuse.ReadResultData(buf[:n]), gofuse.OK
		}
		if err != nil {
			return nil, fs.StatusOf(err)
		}
		return gofuse.ReadResultData(buf[:n]), gofuse.OK
	}
	path, ok := f.inodes.GetPath(input.NodeId)
	if !ok {
		return nil, gofuse.ENOENT
	}
	res, st := f.dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: path, Ino: input.NodeId, Fh: input.Fh, Offset: int64(input.Offset), Limit: int64(input.Size), Kernel: input, Caller: fs.CallerFrom(input.InHeader)})
	if st != gofuse.OK {
		return nil, st
	}
	return gofuse.ReadResultData(res.Data), gofuse.OK
}

func (f *FS) Write(cancel <-chan struct{}, input *gofuse.WriteIn, data []byte) (uint32, gofuse.Status) {
	ctx, cf := fuseCtx(cancel)
	defer cf()
	if h := f.openHandle(input.Fh); h != nil {
		unlock := f.pathLocks.HandleLock(input.Fh)
		err := h.Write(ctx, int64(input.Offset), data)
		unlock()
		if err != nil {
			return 0, fs.StatusOf(err)
		}
		// v1 writer-vs-attr (UpdateLength): publish a grown size at WRITE
		// time so a GETATTR racing the close-time commit cannot pin the
		// pre-write size for the attr TTL. Shrinks publish at their own
		// truncate/commit boundaries; a same-size overwrite changes nothing.
		f.publishWriteLength(input.NodeId, int64(input.Offset)+int64(len(data)))
		// v1's Write never stamps a local mtime: the clock advances only
		// through the server's post-landing stat (ConfirmedAt). A local
		// now() outruns the row and every later cache-expiry stat flips
		// the served mtime back — git flags the file modified per landing.
		// POSIX SUID/SGID clearing on non-owner writes (chmod/12): the
		// privilege bits die with the first write by a non-owner.
		if e, ok := f.inodes.GetEntry(input.NodeId); ok && e.Mode&0o6000 != 0 {
			if c := fs.CallerFrom(input.InHeader); c.Uid != 0 && (!e.HasUID || c.Uid != e.Uid) {
				cleared := e.Mode &^ 0o6000
				f.inodes.UpdateMode(input.NodeId, cleared)
				if p, pok := f.inodes.GetPath(input.NodeId); pok {
					f.dirCacheUpsert(p, &fs.Attr{Kind: fs.KindFile, Mode: cleared, Size: e.Size})
				}
			}
		}
		return uint32(len(data)), gofuse.OK
	}
	path, ok := f.inodes.GetPath(input.NodeId)
	if !ok {
		return 0, gofuse.ENOENT
	}
	res, st := f.dispatch(ctx, &fs.Op{Kind: fs.OpWrite, Path: path, Ino: input.NodeId, Fh: input.Fh, Offset: int64(input.Offset), Data: data, Kernel: input, Caller: fs.CallerFrom(input.InHeader)})
	if st != gofuse.OK {
		return 0, st
	}
	if res.Written != 0 {
		return res.Written, gofuse.OK
	}
	return uint32(len(data)), gofuse.OK
}

func (f *FS) Flush(cancel <-chan struct{}, input *gofuse.FlushIn) gofuse.Status {
	ctx, cf := fuseCtx(cancel)
	defer cf()
	if lockOwner := fuseLockOwner(input.LockOwner, input.Pid, input.Fh); lockOwner != 0 {
		f.locks.release(input.NodeId, lockOwner)
	}
	if h := f.openHandle(input.Fh); h != nil {
		unlock := f.pathLocks.HandleLock(input.Fh)
		err := h.Flush(ctx)
		unlock()
		return fs.StatusOf(err)
	}
	path, _ := f.inodes.GetPath(input.NodeId)
	_, st := f.dispatch(ctx, &fs.Op{Kind: fs.OpFlush, Path: path, Ino: input.NodeId, Fh: input.Fh, Kernel: input, Caller: fs.CallerFrom(input.InHeader)})
	return st
}

func (f *FS) Fsync(cancel <-chan struct{}, input *gofuse.FsyncIn) gofuse.Status {
	ctx, cf := fuseCtx(cancel)
	defer cf()
	if h := f.openHandle(input.Fh); h != nil {
		unlock := f.pathLocks.HandleLock(input.Fh)
		err := h.Sync(ctx)
		unlock()
		return fs.StatusOf(err)
	}
	path, _ := f.inodes.GetPath(input.NodeId)
	_, st := f.dispatch(ctx, &fs.Op{Kind: fs.OpFsync, Path: path, Ino: input.NodeId, Fh: input.Fh, Kernel: input, Caller: fs.CallerFrom(input.InHeader)})
	return st
}

func (f *FS) Release(cancel <-chan struct{}, input *gofuse.ReleaseIn) {
	ctx, cf := fuseCtx(cancel)
	defer cf()
	if lockOwner := fuseLockOwner(input.LockOwner, input.Pid, input.Fh); lockOwner != 0 {
		f.locks.release(input.NodeId, lockOwner)
	}
	if h, ok := f.handles.drop(input.Fh); ok {
		f.openIdx.Remove(h)
		if h.h != nil {
			unlock := f.pathLocks.HandleLock(input.Fh)
			_ = h.h.Release(ctx)
			unlock()
		}
		f.maybeRunDeferredUnlink(h.ino)
		return
	}
	path, _ := f.inodes.GetPath(input.NodeId)
	_, _ = f.dispatch(ctx, &fs.Op{Kind: fs.OpRelease, Path: path, Ino: input.NodeId, Fh: input.Fh, Kernel: input, Caller: fs.CallerFrom(input.InHeader)})
}

func (f *FS) Lseek(cancel <-chan struct{}, in *gofuse.LseekIn, out *gofuse.LseekOut) gofuse.Status {
	path, ok := f.inodes.GetPath(in.NodeId)
	if !ok {
		return gofuse.ENOENT
	}
	ctx, cf := fuseCtx(cancel)
	defer cf()
	res, st := f.dispatch(ctx, &fs.Op{Kind: fs.OpLseek, Path: path, Ino: in.NodeId, Fh: in.Fh, Offset: int64(in.Offset), Flags: in.Whence, Kernel: in, Caller: fs.CallerFrom(in.InHeader)})
	if st != gofuse.OK {
		return st
	}
	out.Offset = uint64(res.Offset)
	return gofuse.OK
}

func lockPayloadOf(input *gofuse.LkIn) fs.FileLock {
	return fs.FileLock{
		Start: int64(input.Lk.Start),
		End:   int64(input.Lk.End),
		Type:  input.Lk.Typ,
		Pid:   input.Lk.Pid,
		Owner: input.Owner,
		Flock: input.LkFlags&gofuse.FUSE_LK_FLOCK != 0,
	}
}

// handleLockOps returns the open handle's foreign lock namespace, if any
// (v1 routed extent handles to the JuiceFS VFS before the local table).
func (f *FS) handleLockOps(fh uint64) fs.HandleLockOps {
	if h, ok := f.handles.get(fh); ok && h.h != nil {
		// Wrapper planes (the read cache) expose their terminal; probing
		// the wrapper itself would find no capability and silently drop
		// the terminal plane's distributed locks.
		cur := h.h
		for i := 0; i < 8; i++ {
			if lo, is := cur.(fs.HandleLockOps); is {
				return lo
			}
			u, is := cur.(interface{ UnwrapHandle() fs.Handle })
			if !is || u == nil {
				return nil
			}
			cur = u.UnwrapHandle()
			if cur == nil {
				return nil
			}
		}
	}
	return nil
}

func (f *FS) GetLk(cancel <-chan struct{}, input *gofuse.LkIn, out *gofuse.LkOut) gofuse.Status {
	if lo := f.handleLockOps(input.Fh); lo != nil {
		ctx, cf := fuseCtx(cancel)
		defer cf()
		lk, err := lo.GetLk(ctx, lockPayloadOf(input))
		if err != nil {
			return fs.StatusOf(err)
		}
		out.Lk = gofuse.FileLock{Start: uint64(lk.Start), End: uint64(lk.End), Typ: lk.Type, Pid: lk.Pid}
		return gofuse.OK
	}
	owner := fuseLockOwner(input.Owner, input.Pid, input.Fh)
	if lock, ok := f.locks.conflict(input.NodeId, owner, input.Lk); ok {
		out.Lk = gofuse.FileLock{Start: lock.start, End: lock.end, Typ: lock.typ, Pid: lock.pid}
		return gofuse.OK
	}
	out.Lk = input.Lk
	out.Lk.Typ = uint32(syscall.F_UNLCK)
	return gofuse.OK
}

func (f *FS) SetLk(cancel <-chan struct{}, input *gofuse.LkIn) gofuse.Status {
	if lo := f.handleLockOps(input.Fh); lo != nil {
		ctx, cf := fuseCtx(cancel)
		defer cf()
		return fs.StatusOf(lo.SetLk(ctx, lockPayloadOf(input), false))
	}
	owner := fuseLockOwner(input.Owner, input.Pid, input.Fh)
	return f.locks.set(cancel, input.NodeId, owner, input.Pid, input.Lk, false)
}

func (f *FS) SetLkw(cancel <-chan struct{}, input *gofuse.LkIn) gofuse.Status {
	if lo := f.handleLockOps(input.Fh); lo != nil {
		ctx, cf := fuseCtx(cancel)
		defer cf()
		return fs.StatusOf(lo.SetLk(ctx, lockPayloadOf(input), true))
	}
	owner := fuseLockOwner(input.Owner, input.Pid, input.Fh)
	return f.locks.set(cancel, input.NodeId, owner, input.Pid, input.Lk, true)
}

func (f *FS) Fallocate(cancel <-chan struct{}, input *gofuse.FallocateIn) gofuse.Status {
	path, ok := f.inodes.GetPath(input.NodeId)
	if !ok {
		return gofuse.ENOENT
	}
	ctx, cf := fuseCtx(cancel)
	defer cf()
	_, st := f.dispatch(ctx, &fs.Op{Kind: fs.OpFallocate, Path: path, Ino: input.NodeId, Fh: input.Fh, Offset: int64(input.Offset), Size: int64(input.Length), Flags: input.Mode, Kernel: input, Caller: fs.CallerFrom(input.InHeader)})
	if st != gofuse.OK {
		return st
	}
	f.mutated(fs.EventWrite, path)
	return gofuse.OK
}

func (f *FS) CopyFileRange(cancel <-chan struct{}, input *gofuse.CopyFileRangeIn) (uint32, gofuse.Status) {
	srcPath, ok := f.inodes.GetPath(input.NodeId)
	if !ok {
		return 0, gofuse.ENOENT
	}
	dstPath, ok := f.inodes.GetPath(input.NodeIdOut)
	if !ok {
		return 0, gofuse.ENOENT
	}
	ctx, cf := fuseCtx(cancel)
	defer cf()
	res, st := f.dispatch(ctx, &fs.Op{
		Kind:    fs.OpCopyFileRange,
		Path:    srcPath,
		Path2:   dstPath,
		Ino:     input.NodeId,
		Ino2:    input.NodeIdOut,
		Fh:      input.FhIn,
		Fh2:     input.FhOut,
		Offset:  int64(input.OffIn),
		Offset2: int64(input.OffOut),
		Limit:   int64(input.Len),
		Flags:   uint32(input.Flags),
		Kernel:  input,
		Caller:  fs.CallerFrom(input.InHeader),
	})
	if st != gofuse.OK {
		return 0, st
	}
	if res.Written != 0 {
		return res.Written, gofuse.OK
	}
	return uint32(input.Len), gofuse.OK
}

// ---------------------------------------------------------------------------
// Directory listing
// ---------------------------------------------------------------------------

func (f *FS) OpenDir(cancel <-chan struct{}, input *gofuse.OpenIn, out *gofuse.OpenOut) gofuse.Status {
	path, ok := f.inodes.GetPath(input.NodeId)
	if !ok {
		return gofuse.ENOENT
	}
	// v1 OpenDir is purely local: no getattr, and the kernel keeps its
	// cached listing across opens (FOPEN_KEEP_CACHE) — except directories a
	// foreign data plane mirrors (v1 extent dir handles): those can gain
	// children the kernel's cached listing does not know, so they re-list.
	keepCache := true
	for _, dc := range f.dirKeepCacheOff {
		if dc.DirKeepCacheOff(path) {
			keepCache = false
			break
		}
	}
	dh := &dirHandle{path: path, ino: input.NodeId, attr: fs.Attr{Kind: fs.KindDir, Mode: 0o755}}
	fh := f.nextFh.Add(1)
	f.dirs.put(fh, dh)
	out.Fh = fh
	if keepCache {
		out.OpenFlags = gofuse.FOPEN_KEEP_CACHE
	}
	f.recordFuseOp("opendir", gofuse.OK, 0, 0)
	return gofuse.OK
}

// loadDirEntries populates the handle's listing snapshot on first use. The
// snapshot then serves the whole enumeration sequence (v1 DirHandle.Entries):
// an offset-0 restart — gVisor rewinds one host fd this way — replays the
// same snapshot instead of appending a second copy of every entry.
func (f *FS) loadDirEntries(cancel <-chan struct{}, input *gofuse.ReadIn, dh *dirHandle) gofuse.Status {
	// The snapshot holds REAL entries only (v1 DirHandle.Entries): "." and
	// ".." are synthesized at emit time with their fixed offsets, so index
	// math and the v1 offset scheme (dots at 1/2, entry i at i+3) stay
	// aligned.
	if f.Ctx.DirCache != nil {
		if cached, ok := f.Ctx.DirCache.Get(dh.path); ok {
			entries := make([]fs.Entry, 0, len(cached))
			for _, ci := range cached {
				kind := fs.KindFile
				if ci.IsDir {
					kind = fs.KindDir
				}
				entries = append(entries, fs.Entry{Name: ci.Name, Attr: fs.Attr{Kind: kind, Mode: ci.Mode, Size: ci.Size, Mtime: ci.Mtime, Nlink: ci.Nlink}})
			}
			dh.entries = entries
			return gofuse.OK
		}
	}
	ctx, cf := fuseCtx(cancel)
	defer cf()
	res, st := f.dispatch(ctx, &fs.Op{Kind: fs.OpList, Path: dh.path, Ino: input.NodeId, Fh: input.Fh, Kernel: input, Caller: fs.CallerFrom(input.InHeader)})
	if st != gofuse.OK {
		return st
	}
	if f.Ctx.DirCache != nil {
		f.Ctx.DirCache.Put(dh.path, dirEntriesFrom(res.Entries))
	}
	dh.entries = append([]fs.Entry(nil), res.Entries...)
	return gofuse.OK
}

// childPathOf resolves the kernel path of a directory entry name relative
// to the listed directory.
func childPathOf(dir, name string) string {
	switch name {
	case ".":
		return dir
	case "..":
		return pathutil.DirOf(dir)
	}
	return pathutil.JoinPath(dir, name)
}

func (f *FS) ReadDir(cancel <-chan struct{}, input *gofuse.ReadIn, out *gofuse.DirEntryList) gofuse.Status {
	dh, ok := f.dirs.get(input.Fh)
	if !ok {
		return gofuse.ENOENT
	}
	ctx, cf := fuseCtx(cancel)
	defer cf()
	if !dh.lockRead(ctx) {
		return gofuse.EINTR
	}
	defer dh.unlockRead()
	if dh.entries == nil || (f.gvisorCompat && input.Offset == 0) {
		// gVisor rewinds one host FD: treat offset-zero as a new
		// enumeration sequence and refetch (v1 loadDirHandleEntriesForRead
		// with refresh) — drop the cached snapshot too, or the TTL window
		// would answer the "refetch" with the stale listing.
		if f.gvisorCompat && input.Offset == 0 && f.Ctx.DirCache != nil {
			f.Ctx.DirCache.Invalidate(dh.path)
		}
		dh.entries = nil
		if st := f.loadDirEntries(cancel, input, dh); st != gofuse.OK {
			return st
		}
	}
	// POSIX readdir yields "." and ".." first: they occupy offsets 1 and 2,
	// real entry i occupies i+3, so the kernel can resume past the synthetic
	// entries without skipping children (v1 offset scheme).
	dots := f.dotDotEntries(dh)
	for d := int(input.Offset); d < len(dots); d++ {
		if !out.AddDirEntry(dots[d]) {
			return gofuse.OK
		}
	}
	realStart := int(input.Offset) - len(dots)
	if realStart < 0 {
		realStart = 0
	}
	for i := realStart; i < len(dh.entries); i++ {
		e := &dh.entries[i]
		if !validDirEntryName(e.Name) {
			continue
		}
		// Plain READDIR does not pin kernel lookups: observe-only inodes.
		// First touch resolves and pins into the snapshot: later pages of
		// the same enumeration replay the first page's identity instead of
		// re-resolving against a table a mid-stream replacement may have
		// already rewired (#918's per-listing fence).
		if e.Ino == 0 {
			e.Ino = f.inodes.EnsureInode(childPathOf(dh.path, e.Name), e.Attr.IsDir(), e.Attr.Size, e.Attr.Mtime)
			f.syncExtentRouting(e.Ino, e)
		}
		if !out.AddDirEntry(gofuse.DirEntry{Name: e.Name, Ino: e.Ino, Mode: modeBitsOf(e.Attr), Off: uint64(i + len(dots) + 1)}) {
			break
		}
	}
	return gofuse.OK
}

// syncExtentRouting ports v1 cachedToDirEntries' extent rules: the listing
// row is the only layout evidence readdir gets — a nonzero extent_ino
// stamps the FUSE inode (shared across hardlink aliases), a zero one clears
// stale routing (an id-less row cannot prove identity), and a live inode's
// size wins over a listing that lags the open writer.
func (f *FS) syncExtentRouting(ino uint64, e *fs.Entry) {
	if ino == 0 {
		return
	}
	if e.Attr.ExtentIno != 0 {
		f.inodes.SetExtentIno(ino, e.Attr.ExtentIno)
	} else if entry, ok := f.inodes.GetEntry(ino); ok && entry.ExtentIno != 0 {
		f.inodes.ClearExtentIno(ino)
	}
	if entry, ok := f.inodes.GetEntry(ino); ok && e.Attr.Size == 0 && entry.Size > 0 {
		// v1 cachedToDirEntries: a listing that lags the open writer must
		// not publish i_size=0 (kernel would EOF without a READ).
		e.Attr.Size = entry.Size
	}
}

func (f *FS) ReadDirPlus(cancel <-chan struct{}, input *gofuse.ReadIn, out *gofuse.DirEntryList) gofuse.Status {
	dh, ok := f.dirs.get(input.Fh)
	if !ok {
		return gofuse.ENOENT
	}
	ctx, cf := fuseCtx(cancel)
	defer cf()
	if !dh.lockRead(ctx) {
		return gofuse.EINTR
	}
	defer dh.unlockRead()
	if dh.entries == nil || (f.gvisorCompat && input.Offset == 0) {
		if f.gvisorCompat && input.Offset == 0 && f.Ctx.DirCache != nil {
			f.Ctx.DirCache.Invalidate(dh.path)
		}
		dh.entries = nil
		if st := f.loadDirEntries(cancel, input, dh); st != gofuse.OK {
			return st
		}
	}
	// READDIRPLUS treats a known inode as a lookup reference the kernel will
	// FORGET later. Emit "." / ".." with FUSE_UNKNOWN_INO and no attributes
	// (go-fuse nodefs behavior, v1): the kernel builds no dentry for them and
	// the directory/parent Nlookup accounting stays correct.
	dots := f.dotDotEntries(dh)
	for d := int(input.Offset); d < len(dots); d++ {
		e := dots[d]
		e.Ino = 0 // AddDirLookupEntry maps 0 -> FUSE_UNKNOWN_INO; no lookup ref.
		entryOut := out.AddDirLookupEntry(e)
		if entryOut == nil {
			return gofuse.OK
		}
		entryOut.NodeId = gofuse.FUSE_UNKNOWN_INO
	}
	realStart := int(input.Offset) - len(dots)
	if realStart < 0 {
		realStart = 0
	}
	for i := realStart; i < len(dh.entries); i++ {
		e := &dh.entries[i]
		if !validDirEntryName(e.Name) {
			continue
		}
		// READDIRPLUS responses pin kernel lookups, like LOOKUP. The
		// identity-aware lookup hardlink aliases across directories onto
		// one FUSE inode (v1): a plain Lookup would pre-map the path and
		// the identity merge in fillEntryOut could never fire. First touch
		// resolves once and pins into the snapshot; resumed pages replay
		// the pinned identity, so a replacement that lands between two
		// pages of one enumeration cannot mix the old and new objects in
		// a single kernel reply set (#918's per-listing fence).
		childP := childPathOf(dh.path, e.Name)
		if e.Ino == 0 {
			if e.Attr.ResourceID != "" {
				e.Ino = f.inodes.LookupWithIdentity(childP, e.Attr.ResourceID, e.Attr.Nlink, e.Attr.IsDir(), e.Attr.Size, e.Attr.Mtime)
			} else {
				e.Ino = f.inodes.Lookup(childP, e.Attr.IsDir(), e.Attr.Size, e.Attr.Mtime)
			}
			f.syncExtentRouting(e.Ino, e)
		}
		ino := e.Ino
		eo := out.AddDirLookupEntry(gofuse.DirEntry{Name: e.Name, Ino: ino, Mode: modeBitsOf(e.Attr), Off: uint64(i + len(dots) + 1)})
		if eo == nil {
			break
		}
		eo.NodeId = ino
		f.fillAttr(e.Attr, ino, &eo.Attr)
		eo.SetAttrTimeout(f.attrTTLFor(childPathOf(dh.path, e.Name)))
		eo.SetEntryTimeout(f.entryTTLFor(e.Name))
	}
	return gofuse.OK
}

// dotDotEntries returns the synthetic "." and ".." entries for a directory
// handle. Inodes resolve best-effort: unknown parents serialize as
// FUSE_UNKNOWN_INO, which the kernel accepts without a lookup refcount.
func (f *FS) dotDotEntries(dh *dirHandle) []gofuse.DirEntry {
	dirMode := uint32(syscall.S_IFDIR) | 0o755
	parentIno := uint64(0)
	if parent := parentDirOf(dh.path); parent != dh.path {
		if ino, ok := f.inodes.GetInode(parent); ok {
			parentIno = ino
		}
	} else if ino, ok := f.inodes.GetInode(dh.path); ok {
		parentIno = ino // root is its own parent
	}
	return []gofuse.DirEntry{
		{Name: ".", Ino: dh.ino, Mode: dirMode, Off: 1},
		{Name: "..", Ino: parentIno, Mode: dirMode, Off: 2},
	}
}

// validDirEntryName filters server listing rows the kernel cannot represent
// (v1): empty, dot names, separators, or embedded NULs.
func validDirEntryName(name string) bool {
	return name != "" &&
		name != "." &&
		name != ".." &&
		!strings.ContainsRune(name, '/') &&
		!strings.ContainsRune(name, 0)
}

func (f *FS) ReleaseDir(input *gofuse.ReleaseIn) {
	f.dirs.drop(input.Fh)
}

func (f *FS) FsyncDir(cancel <-chan struct{}, input *gofuse.FsyncIn) gofuse.Status {
	path, _ := f.inodes.GetPath(input.NodeId)
	ctx, cf := fuseCtx(cancel)
	defer cf()
	if _, err := f.dispatchErr(ctx, &fs.Op{Kind: fs.OpFsync, Path: path, Ino: input.NodeId, Fh: input.Fh, Kernel: input, Caller: fs.CallerFrom(input.InHeader)}); err != nil {
		if fs.StatusOf(err) == gofuse.Status(syscall.ENOSYS) {
			return gofuse.OK // fsync on directories is advisory
		}
		return fs.StatusOf(err)
	}
	return gofuse.OK
}

// ---------------------------------------------------------------------------
// Statfs
// ---------------------------------------------------------------------------

func (f *FS) StatFs(cancel <-chan struct{}, input *gofuse.InHeader, out *gofuse.StatfsOut) gofuse.Status {
	// v1 answers statfs from constants: a synthetic 1 TiB volume. Never
	// dispatch — an unhandled op here wedges getfsstat consumers (macOS
	// `mount`) on the kernel side.
	const blockSize = 4096
	const totalBlocks = (1 << 40) / blockSize // 1 TiB
	out.Blocks = totalBlocks
	out.Bfree = totalBlocks - 1
	out.Bavail = totalBlocks - 1
	out.Bsize = blockSize
	out.NameLen = 255
	out.Frsize = blockSize
	return gofuse.OK
}

// Kernel/listing cache TTL defaults, ported from v1 (pkg/fuse/read.go
// defaults and mount.go setDefaults; coding-agent attr handled in mount).
const (
	defaultAttrTTL     = 60 * time.Second
	defaultEntryTTL    = 60 * time.Second
	defaultNegEntryTTL = 1 * time.Second
	defaultDirTTL      = 10 * time.Second
)

func dirEntriesFrom(entries []fs.Entry) []fs.DirEntryInfo {
	out := make([]fs.DirEntryInfo, 0, len(entries))
	for _, e := range entries {
		mode := e.Attr.Mode
		if e.Attr.Kind == fs.KindSymlink {
			mode = uint32(syscall.S_IFLNK) | mode&0o777
		}
		item := fs.DirEntryInfo{Name: e.Name, Size: e.Attr.Size, IsDir: e.Attr.IsDir(), Mode: mode, HasMode: true, Nlink: e.Attr.Nlink}
		if e.Attr.HasOwner {
			item.Uid, item.Gid, item.HasUID, item.HasGID = e.Attr.Uid, e.Attr.Gid, true, true
		}
		out = append(out, item)
	}
	return out
}

func modeBitsOf(a fs.Attr) uint32 {
	if a.Mode&syscall.S_IFMT != 0 && a.Mode&syscall.S_IFMT != syscall.S_IFREG {
		return a.Mode // specials carry their own type bits (FIFO/CHR/BLK/SOCK)
	}
	switch a.Kind {
	case fs.KindDir:
		return syscall.S_IFDIR | (a.Mode & 0o7777)
	case fs.KindSymlink:
		return syscall.S_IFLNK | 0o777
	default:
		return syscall.S_IFREG | (a.Mode & 0o7777)
	}
}

func (f *FS) childOf(parentIno uint64, name string) (string, gofuse.Status) {
	// v1 posixNameMax: names longer than 255 bytes are ENAMETOOLONG before
	// any remote round trip (the backend's own long-name behavior is not a
	// contract).
	if len(name) > posixNameMax {
		return "", gofuse.Status(syscall.ENAMETOOLONG)
	}
	parent, ok := f.inodes.GetPath(parentIno)
	if !ok {
		return "", gofuse.ENOENT
	}
	if parent == "/" {
		return "/" + name, gofuse.OK
	}
	return parent + "/" + name, gofuse.OK
}

// InvalidateAll flushes kernel caches for every inode the facade knows.
// v1 resetMountViewWithInitialSync: every inode gets an attr refresh, but
// ENTRY invalidation is reserved for files — dropping a directory's dentry
// kills the cwd of any process inside it (getcwd ENOENT).
func (f *FS) InvalidateAll() {
	s := f.currentServer()
	if s == nil {
		return
	}
	entries := f.inodes.Snapshot()
	type target struct {
		ino       uint64
		parentIno uint64
		name      string
	}
	targets := make([]target, 0, len(entries))
	for _, entry := range entries {
		if entry.Path == "/" {
			continue
		}
		t := target{ino: entry.Ino}
		if !entry.IsDir {
			if parentIno, ok := f.inodes.GetInode(parentDir(entry.Path)); ok {
				t.parentIno, t.name = parentIno, pathutil.BaseName(entry.Path)
			}
		}
		targets = append(targets, t)
	}
	go func() {
		for _, t := range targets {
			_ = s.InodeNotify(t.ino, -1, 0)
			_ = s.InodeNotify(t.ino, 0, 0)
			if t.name != "" {
				_ = s.EntryNotify(t.parentIno, t.name)
			}
		}
	}()
}

func fuseCtx(cancel <-chan struct{}) (context.Context, context.CancelFunc) {
	ctx, cf := context.WithCancel(context.Background())
	if cancel == nil {
		return ctx, cf
	}
	go func() {
		select {
		case <-cancel:
			cf()
		case <-ctx.Done():
		}
	}()
	return ctx, cf
}

func (f *FS) dispatch(ctx context.Context, op *fs.Op) (fs.Result, gofuse.Status) {
	start := time.Now()
	res, err := f.chain.Dispatch(ctx, op)
	st := gofuse.OK
	if err != nil {
		st = fs.StatusOf(err)
	}
	f.recordFuseOp(op.Kind.String(), st, time.Since(start), opBytes(op, res))
	return res, st
}

// recordFuseOp feeds the perf counters (v1 perfRecordFuse); facade-only ops
// (directory walks) call it directly with their v1 counter names.
func (f *FS) recordFuseOp(name string, st gofuse.Status, dur time.Duration, bytes uint64) {
	if f.perf != nil {
		f.perf.RecordFuseOpByName(name, st, dur, bytes)
	}
}

// recordRemoteOp feeds the remote-op perf counters (v1 perfRecordRemote):
// extensions report their HTTP families through the recorder hook.
func (f *FS) recordRemoteOp(name string, err error, dur time.Duration, bytes uint64) {
	if f.perf != nil {
		f.perf.RecordRemoteOpByName(name, err, dur, bytes)
	}
}

// recordCacheHit feeds the read-tier hit/miss counters (v1
// readCacheHit/DirCache).
func (f *FS) recordCacheHit(kind string, hit bool) {
	if f.perf != nil {
		f.perf.RecordCacheHit(kind, hit)
	}
}

func opBytes(op *fs.Op, res fs.Result) uint64 {
	switch op.Kind {
	case fs.OpWrite:
		return uint64(len(op.Data))
	case fs.OpRead:
		return uint64(len(res.Data))
	}
	return 0
}

func (f *FS) dispatchErr(ctx context.Context, op *fs.Op) (fs.Result, error) {
	return f.chain.Dispatch(ctx, op)
}

// ArmBypass flags an inode for a short DIRECT_IO window on its next open
// (v1 armKernelCacheBypass).
func (f *FS) ArmBypass(path string, rev, size int64, reason string) {
	if ino, ok := f.inodes.GetInode(path); ok {
		f.bypass.arm(ino, path, rev, size, reason)
	}
}

// OwnerOf resolves a path's ownership from the local inode table (v1's
// chown authority): the sticky-bit rename comparison and class checks test
// against it, not a chain stat that may race listing invalidation or
// backend replica lag.
func (f *FS) OwnerOf(path string) (uid, gid uint32, hasUID, hasGID bool) {
	ino, ok := f.inodes.GetInode(path)
	if !ok {
		return 0, 0, false, false
	}
	e, ok := f.inodes.GetEntry(ino)
	if !ok {
		return 0, 0, false, false
	}
	return e.Uid, e.Gid, e.HasUID, e.HasGID
}

// InvalidateEntry drops the kernel's cached dentry for parent/name.
func (f *FS) InvalidateEntry(parentIno uint64, name string) {
	if s := f.currentServer(); s != nil {
		f.notifyWg.Add(1)
		go func() {
			defer f.notifyWg.Done()
			_ = s.EntryNotify(parentIno, name)
		}()
	}
}

// InvalidateContent drops the kernel's cached attributes and data for ino.
func (f *FS) InvalidateContent(ino uint64, size uint64) {
	if s := f.currentServer(); s != nil {
		f.notifyWg.Add(1)
		go func() {
			defer f.notifyWg.Done()
			_ = s.InodeNotify(ino, -1, 0) // attributes only (empty range)
			_ = s.InodeNotify(ino, 0, int64(size))
		}()
	}
}

func (f *FS) currentServer() *gofuse.Server {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.server
}

func (f *FS) mountPoint() string    { return f.mountPt }
func (f *FS) remoteRootTag() string { return f.rootTag }
func (f *FS) serverTag() string     { return f.srvTag }

// PerfSnapshotJSON exposes the perf snapshot for the continuous recorder.
func (f *FS) PerfSnapshotJSON() ([]byte, bool) {
	if f.perf == nil {
		return nil, false
	}
	return f.perf.SnapshotJSON()
}

// HandleCount reports live file handles (queue stats probe).
func (f *FS) HandleCount() int { return f.handles.len() }

// InodeOf returns the FUSE inode currently owning path (test hook for the
// extent-routing rows).
func (f *FS) InodeOf(path string) (uint64, bool) { return f.inodes.GetInode(path) }

// ExtentRoutingOf returns the JuiceFS inode bound to path's FUSE inode.
func (f *FS) ExtentRoutingOf(path string) (uint64, bool) {
	ino, ok := f.inodes.GetInode(path)
	if !ok {
		return 0, false
	}
	entry, ok := f.inodes.GetEntry(ino)
	if !ok || entry.ExtentIno == 0 {
		return 0, false
	}
	return entry.ExtentIno, true
}

// InodeByExtentRouting resolves the FUSE inode owning a JuiceFS inode.
func (f *FS) InodeByExtentRouting(extentIno uint64) (uint64, bool) {
	return f.inodes.FindByExtentIno(extentIno)
}

// EnsureInodeForTest registers a live inode with a size (writer view).
func (f *FS) EnsureInodeForTest(path string, isDir bool, size int64, mtime time.Time) uint64 {
	return f.inodes.EnsureInode(path, isDir, size, mtime)
}

// SetExtentRoutingForTest binds a JuiceFS inode onto a FUSE inode.
func (f *FS) SetExtentRoutingForTest(ino, extentIno uint64) { f.inodes.SetExtentIno(ino, extentIno) }

// LiveInodeSizeForTest reports the live inode-table size of a path.
func (f *FS) LiveInodeSizeForTest(path string) int64 {
	ino, ok := f.inodes.GetInode(path)
	if !ok {
		return 0
	}
	entry, ok := f.inodes.GetEntry(ino)
	if !ok {
		return 0
	}
	return entry.Size
}

// DirCount reports live directory handles (queue stats probe).
func (f *FS) DirCount() int { return f.dirs.len() }

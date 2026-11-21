// Package cache is the read-plane cache extension. It wraps handles
// opened through the chain with a caching decorator (in-memory LRU+TTL
// keyed by path/revision, optional on-disk block cache), serves
// path-addressed reads from the cache, and invalidates on bus events.
// The directory-neighborhood prefetcher (sibling metadata, sibling
// directories, readdir content) rides the same events.
package cache

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/membudget"
	"github.com/mem9-ai/drive9/pkg/fusecomp/writebuffer"
)

// MaxPreloadSize re-exports the write-buffer preload cap for callers that
// still reach for it through this package.
const MaxPreloadSize = writebuffer.MaxPreloadSize

// Options configures the read caches. Zero values take the ported
// defaults (128MB / 30s / 4MB per file).
type Options struct {
	MaxSize     int64
	TTL         time.Duration
	MaxFileSize int64
	Disk        DiskReadCacheOptions // Dir empty disables the disk cache

	// MemoryBudget is the process-wide memory budget the in-memory cache
	// charges its retained payloads to. Nil disables the charge (the local
	// MaxSize LRU still applies).
	MemoryBudget *membudget.Budget

	// Events, when set, subscribes this cache to mutation events: every
	// mutation invalidates the cached image, directory-level events sweep
	// the subtree prefix, and staged/written paths stay unprefetched until
	// their commit lands.
	Events *fs.EventBus
	// SequentialPrefetch arms the ported Prefetcher on cached handles:
	// sequential reads trigger parallel read-ahead of upcoming blocks.
	// v1 armed the prefetcher for every handle whose size exceeds the
	// whole-file cache admission limit — there was no separate knob.
	SequentialPrefetch bool
	// PrefetchClient backs the Prefetcher's parallel fetches (the mount's
	// client); nil disables arming.
	PrefetchClient *client.Client
	// ParallelReadConcurrency/ParallelReadBlockSize bound the prefetcher's
	// parallel block fetches (v1 --parallel-read-concurrency/block-size;
	// defaults 4 x 1MiB).
	ParallelReadConcurrency int
	ParallelReadBlockSize   int64

	// RemoteRoot prefixes namespace paths before they reach the client
	// (the prefetch trio's remote paths).
	RemoteRoot string

	// DirContentPrefetch arms the readdir content prefetch (
	// ReadDirPrefetch; sibling metadata and sibling directory prefetch are
	// always-on machinery like).
	DirContentPrefetch bool
	// PrefetchMaxFiles/PrefetchMaxFileBytes/PrefetchMaxBytes/PrefetchTimeout
	// bound the content prefetch (defaults 32 / per-file cap / 1MiB / 1s).
	PrefetchMaxFiles     int
	PrefetchMaxFileBytes int64
	PrefetchMaxBytes     int64
	PrefetchTimeout      time.Duration
}

// Cache is the fs.Handler middleware composing the read caches into the
// chain.
type Cache struct {
	mem  *ReadCache
	dsk  *DiskReadCache
	opts Options
	// dirs is the directory-neighborhood prefetcher (trio).
	dirs *dirPrefetcher
	// hitRecorder feeds the read-tier hit/miss counters (facade-attached).
	hitRecorder func(kind string, hit bool)
	// open tracks live caching handles by pointer so a rename can
	// retarget them (and their prefetchers) onto the new path — the
	// handle keeps serving, but its block requests must follow the name.
	open sync.Map // *cachingHandle → struct{}
	// epoch advances on every whole-view reset: a read dispatched before a
	// checkout must not admit its pre-reset bytes when it completes after.
	epoch atomic.Uint64
	// resetMu orders the epoch check with the tier sweep: a publication
	// that passed the check must not land after a reset cleared the cache
	// (the plain Load/Put pair had exactly that window).
	resetMu sync.RWMutex
	// sidecars tracks open -wal/-journal handles (v1 sqlite routing).
	sidecars sidecarTracker
	// dirty counts HANDLES with unflushed writes per path: stable tiers
	// must not answer for them, and only the OWNING descriptor clears its
	// own mark at its commit boundary (an unrelated reader's Flush/Release
	// must not un-dirty a writer's path).
	dirtyMu sync.Mutex
	dirty   map[string]int
	// unverified is set while the event stream has lost continuity
	// (EventUnverified) and cleared when it catches up (EventVerified):
	// in between, a read re-pins its revision against the remote before
	// the revision-keyed tiers may answer (v1 statCacheUnverified +
	// revalidateReadCacheEntryIfUntrusted), because a remote change in
	// the gap arrives with no event.
	unverified atomic.Bool
	// inodes is the facade's table, attached before the mount serves:
	// hardlink aliases share the object, so invalidation and sibling
	// prefetch hygiene must reach every name bound to the inode.
	inodes fs.InodeTable
}

// SetCacheHitRecorder wires the facade-owned hit/miss perf feed.
func (c *Cache) SetCacheHitRecorder(fn func(kind string, hit bool)) { c.hitRecorder = fn }

// SetInodeTable is called by the facade before the mount serves.
func (c *Cache) SetInodeTable(t fs.InodeTable) { c.inodes = t }

// aliasSet returns every known name bound to the same inode as path: the
// path itself plus its hardlink aliases. A hardlink alias names the same
// remote object, so a write or landing through one name is a change to them
// all. Without the table it is the path alone.
func (c *Cache) aliasSet(path string) map[string]struct{} {
	set := map[string]struct{}{path: {}}
	if c.inodes == nil {
		return set
	}
	ino, ok := c.inodes.Ino(path)
	if !ok || ino == 0 {
		return set
	}
	if aliaser, ok := c.inodes.(interface{ Aliases(ino uint64) []string }); ok {
		for _, a := range aliaser.Aliases(ino) {
			set[a] = struct{}{}
		}
	}
	return set
}

func (c *Cache) noteHit(kind string, hit bool) {
	if c.hitRecorder != nil {
		c.hitRecorder(kind, hit)
	}
}

// SetMemoryBudget wires the process-wide memory budget this extension's
// in-memory read cache charges its retained payloads to.
func (c *Cache) SetMemoryBudget(b *membudget.Budget) {
	if c == nil || c.mem == nil {
		return
	}
	c.mem.SetMemoryBudget(b)
}

// New constructs the read-plane cache.
func New(opts Options) (*Cache, error) {
	c := &Cache{opts: opts,
		dirty: map[string]int{},
		mem:   NewReadCacheWithMaxFileSize(opts.MaxSize, opts.TTL, opts.MaxFileSize),
	}
	c.mem.SetMemoryBudget(opts.MemoryBudget)
	if opts.Disk.Dir != "" {
		d, err := NewDiskReadCache(opts.Disk)
		if err != nil {
			// v1: a failed disk-cache init warns and continues without the
			// tier — the mount itself must not fail on it.
			fmt.Fprintf(os.Stderr, "drive9: disk read cache init failed: %v (continuing without)\n", err)
		} else {
			c.dsk = d
		}
	}
	c.dirs = newDirPrefetcher(opts.PrefetchClient, opts.RemoteRoot)
	// Background prefetch publications share the reset epoch: a fetch that
	// raced a checkout must not repopulate the cleared caches.
	c.dirs.genFn = c.publicationEpoch
	c.dirs.publishFn = c.publishIfCurrent
	c.dirs.contentEnabled = opts.DirContentPrefetch
	c.dirs.contentMaxFiles = opts.PrefetchMaxFiles
	if c.dirs.contentMaxFiles <= 0 {
		c.dirs.contentMaxFiles = contentPrefetchMaxFilesDefault
	}
	c.dirs.contentMaxFile = opts.PrefetchMaxFileBytes
	if c.dirs.contentMaxFile <= 0 || c.dirs.contentMaxFile > writebuffer.SmallFileThreshold {
		// v1: the per-file prefetch cap defaults to (and never exceeds) the
		// small-file threshold — 50KB, not a 512KB guess.
		c.dirs.contentMaxFile = writebuffer.SmallFileThreshold
	}
	c.dirs.contentMaxBytes = opts.PrefetchMaxBytes
	if c.dirs.contentMaxBytes <= 0 {
		c.dirs.contentMaxBytes = contentPrefetchMaxBytesDefault
	}
	c.dirs.contentTimeout = opts.PrefetchTimeout
	if c.dirs.contentTimeout <= 0 {
		c.dirs.contentTimeout = contentPrefetchTimeoutDefault
	}
	if opts.Events != nil {
		// Local-write-state approximation for the prefetch trio (
		// hasLocalWriteState): staged/written paths stay unprefetched until
		// their commit lands.
		opts.Events.On(fs.EventStaged, func(e fs.Event) {
			if c.dirs != nil {
				c.dirs.noteWrite(e.Path)
			}
		})
		opts.Events.On(fs.EventLanded, func(e fs.Event) {
			if c.dirs != nil {
				c.dirs.noteLanded(e.Path)
			}
		})
		opts.Events.On(fs.EventWrite, func(e fs.Event) {
			if c.dirs != nil {
				c.dirs.noteWrite(e.Path)
			}
			c.fencedSweep(func() []detachedDiskReadCacheEntry {
				c.mem.Invalidate(e.Path)
				return c.invalidateDiskPath(e.Path)
			})
		})
		opts.Events.On(fs.EventDelete, func(e fs.Event) {
			c.invalidatePathTiers(e.Path)
		})
		// A remote-origin change invalidates that path's cached images (v1
		// invalidateReadCacheAndTargets was per-path); only a checkout/reset
		// sweeps the whole cache.
		opts.Events.On(fs.EventInvalidated, func(e fs.Event) {
			if e.Path == "/" {
				c.invalidateAllTiers()
				return
			}
			c.invalidatePathTiers(e.Path)
		})
		opts.Events.On(fs.EventCheckout, func(fs.Event) { c.invalidateAllTiers() })
		// Loss of stream continuity is not a view change: the revision-keyed
		// tiers stay and open descriptors re-pin their revision before
		// answering from them (v1 kept the read cache and revalidated).
		opts.Events.On(fs.EventUnverified, func(fs.Event) { c.markUnverified() })
		opts.Events.On(fs.EventVerified, func(fs.Event) { c.unverified.Store(false) })
		opts.Events.On(fs.EventLanded, func(e fs.Event) {
			// A local commit settle (onCommitQueueSuccess /
			// onWriteBackUploadSuccess) replaces the file's image. Same
			// path readers still hold their OPEN-TIME revision, so the old
			// disk blocks look current to them (a sibling fsync made a
			// reader fall back from the writer's bytes to the old image).
			// Drop the stale blocks and move the readers onto the LANDED
			// revision — but KEEP the just-seeded committed image: it
			// covers backend replica lag for this mount's own writes, and
			// sweeping it made a fresh commit read back as the stale
			// remote row (git's refs after clone).
			if e.Path == "/" || e.Path == "" {
				c.invalidateAllTiers()
				return
			}
			// A hardlink alias names the same object: the landing replaces
			// the image its readers hold under their open-time revision too.
			aliases := c.aliasSet(e.Path)
			c.fencedSweep(func() []detachedDiskReadCacheEntry {
				var detached []detachedDiskReadCacheEntry
				for alias := range aliases {
					if e.Rev <= 0 {
						// No trustworthy revision to keep: drop the alias's
						// image (v1 settle) — a rev-keyed seed below
						// supersedes it.
						c.mem.Invalidate(alias)
					}
					detached = append(detached, c.invalidateDiskPath(alias)...)
				}
				return detached
			})
			prefix := strings.TrimSuffix(e.Path, "/") + "/"
			c.open.Range(func(key, _ any) bool {
				h, ok := key.(*cachingHandle)
				if !ok {
					return true
				}
				p := h.identity()
				if _, aliased := aliases[p]; !aliased && p != e.Path && !strings.HasPrefix(p, prefix) {
					return true
				}
				h.mu.Lock()
				if h.prefetch != nil {
					// The prefetcher clips its last block to ITS file size:
					// left at the open-time length, a reader crossing the
					// old EOF after the file grew got a short block that
					// Get took for EOF. Move it onto the landed size too.
					if e.HasSize {
						h.prefetch.invalidateWithSize(e.Size)
					} else {
						h.prefetch.Invalidate()
					}
				}
				if e.Rev > 0 {
					h.rev = e.Rev
				}
				if e.HasSize {
					// Adopt the LANDED size: a reader that kept its
					// open-time size answered EOF past the old end after
					// the file grew (a disk range key clips against it).
					h.size = e.Size
				}
				h.grown = false
				h.identityGen++
				h.mu.Unlock()
				return true
			})
		})
	}
	return c, nil
}

// invalidatePathTiers drops one path's (and its subtree's) images from the
// memory/disk tiers AND invalidates the matching open handles' block
// prefetchers — Read serves prefetched blocks ahead of the invalidated
// tiers, so a per-file SSE change that skipped them kept answering the old
// bytes until a full checkout.
func (c *Cache) invalidatePathTiers(path string) {
	c.fencedSweep(func() []detachedDiskReadCacheEntry {
		var detached []detachedDiskReadCacheEntry
		// Hardlink aliases name the same object: the change is theirs too.
		aliases := c.aliasSet(path)
		for alias := range aliases {
			c.mem.Invalidate(alias)
			detached = append(detached, c.invalidateDiskPath(alias)...)
		}
		c.mem.InvalidatePrefix(path + "/")
		detached = append(detached, c.invalidateDiskPath(path)...)
		prefix := strings.TrimSuffix(path, "/") + "/"
		c.open.Range(func(key, _ any) bool {
			h, ok := key.(*cachingHandle)
			if !ok {
				return true
			}
			p := h.identity()
			if _, aliased := aliases[p]; aliased || p == path || strings.HasPrefix(p, prefix) {
				h.mu.Lock()
				if h.prefetch != nil {
					h.prefetch.Invalidate()
				}
				// The SSE change replaced this file's remote image: the
				// handle's open-time size/rev describe the OLD one, and a
				// whole-file read under it re-cached the stale prefix as the
				// complete image (the grown tail answered EOF). The next read
				// re-pins them against the remote instead of leaving the
				// descriptor off the tiers for the rest of its life.
				h.size, h.rev = 0, 0
				h.grown = false
				h.revalidate = true
				h.identityGen++
				h.mu.Unlock()
			}
			return true
		})
		return detached
	})
}

// invalidateDiskPath detaches the on-disk blocks of one name and its subtree,
// returning the backing files for the caller to unlink after the fence is
// released. The disk tier is a separate, mount-surviving store, so every
// namespace mutation that recycles a name must sweep it explicitly.
func (c *Cache) invalidateDiskPath(path string) []detachedDiskReadCacheEntry {
	if c == nil || c.dsk == nil || path == "" {
		return nil
	}
	detached := c.dsk.detachFile(pathDiskReadCacheFileID(path))
	return append(detached, c.dsk.detachPathPrefix(strings.TrimSuffix(path, "/")+"/")...)
}

// invalidateDiskPaths detaches the on-disk blocks of several names.
func (c *Cache) invalidateDiskPaths(paths ...string) []detachedDiskReadCacheEntry {
	var detached []detachedDiskReadCacheEntry
	for _, p := range paths {
		detached = append(detached, c.invalidateDiskPath(p)...)
	}
	return detached
}

// renameTiers moves the cached images of a rename and drops the replaced
// destination's, under the publication fence. Both the local rename handler
// and the facade's deferred-unlink survivor retarget call it: a rename that
// recycles a name must not leave a pre-rename fetch able to republish.
func (c *Cache) renameTiers(oldPath, newPath string) {
	c.fencedSweep(func() []detachedDiskReadCacheEntry {
		c.mem.Rename(oldPath, newPath)
		return c.invalidateDiskPaths(oldPath, newPath)
	})
}

// markUnverified handles a loss of event-stream continuity (an SSE
// disconnect, and the catch-up that ends it): a remote change in the gap
// arrives with no event, so nothing pinned before it can be trusted blindly
// — but nothing is known to have changed either. v1 kept the read cache and
// revalidated reads while the stat cache was unverified; a full sweep here
// (#1026) dropped up to a GiB of disk tier per blip and zeroed every open
// descriptor's revision with no path back onto the tiers (#1064).
//
//   - The revision-keyed disk tier stays: it is read only through handles,
//     and every handle re-pins its revision before answering from it.
//   - The whole-file memory tier is still swept: writeback's base preload
//     reads it revision-blind (ReadCached with revision 0), and a stale
//     base under a partial write would commit the old bytes.
//   - Open handles keep rev/size as CANDIDATES and are flagged to
//     revalidate; their prefetched blocks (offset-keyed, fetched under the
//     candidate revision) are dropped. The epoch bump fences fetches still
//     in flight from being admitted under a revision that may have moved.
//
// Without a client to stat with, the sweep is the only safe answer.
func (c *Cache) markUnverified() {
	if c.opts.PrefetchClient == nil {
		c.invalidateAllTiers()
		return
	}
	c.fencedSweep(func() []detachedDiskReadCacheEntry {
		c.unverified.Store(true)
		c.mem.InvalidateAll()
		c.open.Range(func(key, _ any) bool {
			if h, ok := key.(*cachingHandle); ok {
				h.mu.Lock()
				if h.prefetch != nil {
					h.prefetch.Invalidate()
				}
				h.revalidate = true
				h.identityGen++
				h.mu.Unlock()
			}
			return true
		})
		return nil
	})
}

// invalidateAllTiers drops the whole-file memory/disk caches AND every open
// handle's block prefetcher: Read serves prefetched blocks ahead of the
// invalidated tiers, so a checkout/reset that skipped them kept feeding the
// pre-reset image to live descriptors (v1 resetMountViewWithInitialSync
// invalidated each handle's Prefetch, cancelling in-flight fetches).
func (c *Cache) invalidateAllTiers() {
	c.fencedSweep(func() []detachedDiskReadCacheEntry {
		// A checkout/reset re-synchronizes the whole view: every tier is
		// swept and every handle re-pins through a fresh open, so the stream
		// is continuous again from here. SSE's reset path clears its own
		// unverified flag without a catch-up, so no EventVerified follows it
		// — left set, gap mode would stat every warm read for the rest of
		// the session.
		c.unverified.Store(false)
		c.mem.InvalidateAll()
		// The disk index is dropped inside the fence; fencedSweep unlinks the
		// backing files after it releases the fence, guarded against a
		// re-fetch that re-installed a digest meanwhile.
		var detached []detachedDiskReadCacheEntry
		if c.dsk != nil {
			detached = c.dsk.detachAll()
		}
		c.open.Range(func(key, _ any) bool {
			if h, ok := key.(*cachingHandle); ok {
				h.mu.Lock()
				if h.prefetch != nil {
					h.prefetch.Invalidate()
				}
				// The open-time size/revision describe the OLD view: keeping
				// them made a whole-file read re-cache the stale prefix as the
				// complete image (a grown remote file read as its old length,
				// with the tail answering EOF). No revalidation either: the
				// view changed, and the facade fences these descriptors' reads.
				h.size, h.rev = 0, 0
				h.grown = false
				h.revalidate = false
				h.identityGen++
				h.mu.Unlock()
			}
			return true
		})
		return detached
	})
}

// remotePathOf maps a view path to the path the client addresses (the
// prefetcher must read the SAME file the terminal does; a view path with a
// RemoteRoot mixed another file's blocks into the stream).
func (c *Cache) remotePathOf(p string) string {
	root := strings.TrimSpace(c.opts.RemoteRoot)
	if root == "" || root == "/" {
		return p
	}
	root = "/" + strings.Trim(root, "/")
	if p == "/" {
		return root
	}
	return root + p
}

func (c *Cache) dirtyPath(path string) bool {
	c.dirtyMu.Lock()
	_, ok := c.dirty[path]
	c.dirtyMu.Unlock()
	return ok
}

// fencedSweep runs a cache-invalidating sweep with the publication fence
// closed: a read that captured an earlier epoch is rejected by
// publishIfCurrent, so a fetch that began before the sweep cannot restore the
// image the sweep just dropped. EVERY tier sweep runs through it — a sweep
// outside the fence leaves exactly the window this closes for renames: a read
// that fetched the pre-sweep bytes and then publishes them after the sweep
// (a same-revision replacement, which lands as an event rather than a local
// OpRename, would look current).
//
// The sweep returns the on-disk blocks it detached; they are unlinked AFTER
// the fence is released. The fence only needs the index drop to be atomic
// with the epoch bump (an unindexed file is already invisible to readers),
// and an unlink batch is O(blocks) I/O that must not serialize every reader
// publication behind it.
func (c *Cache) fencedSweep(sweep func() []detachedDiskReadCacheEntry) {
	// The lock is released by a defer INSIDE the closure: a panic in any
	// sweep leaves the fence unlocked, so a recovered panic cannot wedge every
	// read in the mount on a permanently write-locked fence.
	detached := func() []detachedDiskReadCacheEntry {
		c.resetMu.Lock()
		defer c.resetMu.Unlock()
		c.epoch.Add(1)
		return sweep()
	}()
	// Unlink only AFTER the fence is released, and through the guarded
	// remover: a block a reader re-fetched while the batch ran has the same
	// digest/path and must survive.
	c.dsk.removeDetached(detached)
}

// publicationEpoch captures the epoch a read's publications are fenced
// against. It reads under resetMu's read side so an in-progress sweep finishes
// FIRST: a plain load can observe the new epoch while the tiers and open
// handles still describe the old image, and publishIfCurrent would then accept
// that read after the sweep. A sweep that starts after this capture still
// changes the epoch and rejects the publication.
func (c *Cache) publicationEpoch() uint64 {
	c.resetMu.RLock()
	defer c.resetMu.RUnlock()
	return c.epoch.Load()
}

// publishIfCurrent runs fn iff no reset has landed since gen, atomically
// with the reset's sweep (the read side of resetMu).
func (c *Cache) publishIfCurrent(gen uint64, fn func()) bool {
	c.resetMu.RLock()
	defer c.resetMu.RUnlock()
	if c.epoch.Load() != gen {
		return false
	}
	fn()
	return true
}

// ReadCached returns whole-file cached bytes for path at revision, if
// admitted.
func (c *Cache) ReadCached(path string, revision int64) ([]byte, bool) {
	return c.mem.Get(path, revision)
}

// MaxFileSize reports the read cache's per-file admission bound (the commit
// seeder checks it before materializing an image).
func (c *Cache) MaxFileSize() int64 {
	return c.mem.MaxFileSize()
}

// SeedCommit caches the just-committed image (readCache.Put at commit
// settle) so reads racing backend replica lag see this mount's own writes.
func (c *Cache) SeedCommit(path string, data []byte, rev int64) {
	if rev <= 0 || !c.admits(data) || sqliteJournalPath(path) {
		return
	}
	c.mem.PutOwned(path, data, rev)
}

// SeedPending plants the path's pending seed: the flush-time staged image
// served to this mount's readers until a newer staging replaces it or a
// write/remote-change event drops it.
func (c *Cache) SeedPending(path string, data []byte) {
	c.mem.PutPendingOwned(path, data)
}

// StoreOwned caches whole-file bytes for path at revision without cloning.
func (c *Cache) StoreOwned(path string, data []byte, revision int64) {
	c.mem.PutOwned(path, data, revision)
}

// Invalidate drops one path's cached image.
func (c *Cache) Invalidate(path string) {
	c.fencedSweep(func() []detachedDiskReadCacheEntry {
		c.mem.Invalidate(path)
		return nil
	})
}

// InvalidatePrefix drops every cached image under a prefix.
func (c *Cache) InvalidatePrefix(prefix string) {
	c.fencedSweep(func() []detachedDiskReadCacheEntry {
		c.mem.InvalidatePrefix(prefix)
		return c.invalidateDiskPath(prefix)
	})
}

// InvalidateAll drops the whole in-memory tier. The on-disk tier is only
// cleared by a view reset (invalidateAllTiers), which the checkout/reset
// paths use: its blocks are object-keyed, and keeping them across a plain
// memory flush is what makes warm re-reads survive.
func (c *Cache) InvalidateAll() {
	c.fencedSweep(func() []detachedDiskReadCacheEntry {
		c.mem.InvalidateAll()
		return nil
	})
}

// Start implements fs.Lifecycle (the cache runs no background workers).
func (c *Cache) Start(ctx context.Context) error { return nil }

// Drain implements fs.Lifecycle.
func (c *Cache) Drain(ctx context.Context) error { return nil }

// Stop implements fs.Lifecycle: the on-disk block cache flushes its pending
// writes and closes its index (v1 detachAll at unmount/reload).
func (c *Cache) Stop(ctx context.Context) error {
	if c.dsk != nil {
		c.dsk.Close()
	}
	return nil
}

// Do implements fs.Handler.
func (c *Cache) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	switch op.Kind {
	case fs.OpLookup, fs.OpGetAttr:
		res, err := next(ctx, op)
		if err != nil {
			// attribute-prefetch trigger: a lookup miss counts toward
			// the directory's miss window; two misses within a second
			// batch-stat the siblings into the namespace cache.
			if fs.IsNotFound(err) {
				c.dirs.noteMiss(ctx, op.Path)
			}
			return res, err
		}
		return res, nil
	case fs.OpList:
		res, err := next(ctx, op)
		if err != nil {
			return res, err
		}
		// trio triggers after a listing: sibling directory listings load
		// in the background, and (gated) small file contents batch-read
		// into the read cache.
		if c.dirs.namespace != nil {
			go func() {
				c.dirs.maybePrefetchSiblingDirectories(next, op.Path)
			}()
			c.prefetchDirContents(ctx, op.Path, res.Entries)
		}
		return res, nil
	case fs.OpCreate:
		res, err := next(ctx, op)
		if err != nil || res.Handle == nil {
			return res, err
		}
		rev := int64(0)
		size := int64(0)
		objectID := ""
		if res.Attr != nil {
			rev = res.Attr.Rev
			size = res.Attr.Size
			objectID = res.Attr.ResourceID
		}
		h := &cachingHandle{cache: c, inner: res.Handle, path: op.Path, rev: rev, size: size, objectID: objectID}
		c.open.Store(h, struct{}{})
		if sqliteJournalPath(op.Path) {
			c.trackSidecarOpen(op.Path)
			h.isSidecar = true
		}
		// The prefetcher reads the ORDINARY remote namespace: it may only
		// arm behind a claim whose bytes EQUAL that namespace — an
		// explicit capability on the terminal (core's SyncHandle), not a
		// decorator-shape inference (localize's token has no UnwrapHandle
		// and passed the old check, mixing local and remote images). A
		// decorator that serves the remote bytes unchanged (writeback's
		// clean descriptor of a path without local images) forwards the
		// terminal's claim; one that serves its own bytes withholds it.
		if remoteNamespaceReadable(res.Handle) && op.Flags&(uint32(syscall.O_WRONLY)|uint32(syscall.O_RDWR)) == 0 {
			if c.opts.SequentialPrefetch && c.opts.PrefetchClient != nil && size > c.mem.MaxFileSize() {
				// v1 armed the prefetcher only for READ-ONLY opens of files
				// whose size exceeds the whole-file cache admission limit:
				// smaller files are served by the mem cache itself and need
				// no block read-ahead, and a writable descriptor's first
				// write would only invalidate it.
				p := NewPrefetcher(c.opts.PrefetchClient, c.remotePathOf(op.Path), size)
				p.SetParallelRead(c.opts.ParallelReadConcurrency, c.opts.ParallelReadBlockSize)
				h.prefetch = p
				h.prefetchArmed = true
			}
		}
		res.Handle = h
		return res, nil

	case fs.OpRename:
		res, err := next(ctx, op)
		if err != nil {
			return res, err
		}
		// Retarget open handles and their prefetchers: the inner writeback
		// plane already moved its writers, and a caching handle still
		// keyed on the old path would fetch blocks for whatever replaces
		// the old name after a re-create. Cached whole-file images move
		// with the name (same content, same revision).
		c.open.Range(func(k, _ any) bool {
			if h, ok := k.(*cachingHandle); ok {
				p := h.identity()
				if p == op.Path {
					h.SetPath(op.Path2)
				} else if strings.HasPrefix(p, strings.TrimSuffix(op.Path, "/")+"/") {
					h.SetPath(op.Path2 + strings.TrimPrefix(p, op.Path))
				}
			}
			return true
		})
		// The rename REPLACED whatever the destination named. The memory
		// tier drops that image itself (Rename moves the source's images
		// and evicts the destination's, subtree included); the disk tier is
		// a separate store and must be swept here, or a reader would fall
		// through the memory tier onto blocks describing the destroyed
		// object — the revision a replacement receives can equal the one the
		// old object had, because the per-file counter restarts at 1.
		//
		// The sweep is FENCED: a read that was already in flight may hold
		// pre-rename bytes and pause before publication. publishIfCurrent
		// compares the epoch it captured under resetMu, so bumping it and
		// sweeping in the same critical section makes a pre-rename read
		// unable to restore the replaced image into the tier after the
		// rename has completed.
		c.renameTiers(op.Path, op.Path2)
		return res, nil

	case fs.OpRead:
		if op.Handle != nil || op.Fh != 0 {
			break // handle reads go through the decorator
		}
		if data, ok := c.mem.Get(op.Path, 0); ok {
			if op.Offset > 0 || op.Limit > 0 {
				off := op.Offset
				if off < 0 {
					off = 0
				}
				if off >= int64(len(data)) {
					return fs.Result{}, nil
				}
				end := int64(len(data))
				if op.Limit > 0 && off+op.Limit < end {
					end = off + op.Limit
				}
				data = data[off:end]
			}
			return fs.Result{Data: data}, nil
		}
		epoch := c.publicationEpoch()
		res, err := next(ctx, op)
		if err == nil && res.Attr == nil && len(res.Data) > 0 {
			// Path-addressed reads carry no revision; event invalidation
			// keeps this coherent. Only an UNLIMITED offset-0 request
			// returns the whole image — caching a 16KiB range read under
			// the whole-file key made every later offset answer EOF
			// (the layer staged-append read-back).
			// The epoch fences a read that raced a checkout: its bytes
			// describe the replaced view.
			if op.Offset == 0 && op.Limit <= 0 && c.admits(res.Data) {
				c.publishIfCurrent(epoch, func() { c.mem.Put(op.Path, res.Data, 0) })
			}
		}
		return res, err
	}
	return next(ctx, op)
}

// RetargetPath rebinds open read-plane handles keyed at oldPath onto
// newPath — the facade's deferred-unlink survivor hook (#1029). The write
// plane already moved its writers; a caching handle still keyed at the
// doomed name would seed pending reads and prefetch blocks for whatever
// replaces the name after the re-create. Cached whole-file images move
// with the name (same content, same revision), so the recycled name
// starts without a cached image of the old object.
func (c *Cache) RetargetPath(oldPath, newPath string) {
	prefix := strings.TrimSuffix(oldPath, "/") + "/"
	c.open.Range(func(k, _ any) bool {
		if h, ok := k.(*cachingHandle); ok {
			p := h.identity()
			if p == oldPath {
				h.SetPath(newPath)
			} else if strings.HasPrefix(p, prefix) {
				h.SetPath(newPath + strings.TrimPrefix(p, oldPath))
			}
		}
		return true
	})
	c.renameTiers(oldPath, newPath)
}

func (c *Cache) admits(data []byte) bool {
	return int64(len(data)) <= c.mem.MaxFileSize()
}

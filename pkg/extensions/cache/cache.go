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
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
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
	// sidecars tracks open -wal/-journal handles (v1 sqlite routing).
	sidecars sidecarTracker
}

// SetCacheHitRecorder wires the facade-owned hit/miss perf feed.
func (c *Cache) SetCacheHitRecorder(fn func(kind string, hit bool)) { c.hitRecorder = fn }

func (c *Cache) noteHit(kind string, hit bool) {
	if c.hitRecorder != nil {
		c.hitRecorder(kind, hit)
	}
}

// cachingHandle decorates an inner handle with the whole-file read cache
// and the on-disk block tier.
type cachingHandle struct {
	cache         *Cache
	inner         fs.Handle
	path          string
	rev           int64
	size          int64 // open-time size; 0 = unknown
	prefetch      *Prefetcher
	prefetchArmed bool
	// isSidecar marks an open -wal/-journal handle (tracker cleanup).
	isSidecar bool
}

// New constructs the read-plane cache.
func New(opts Options) (*Cache, error) {
	c := &Cache{opts: opts,
		mem: NewReadCacheWithMaxFileSize(opts.MaxSize, opts.TTL, opts.MaxFileSize),
	}
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
			c.mem.Invalidate(e.Path)
			if c.dsk != nil {
				c.dsk.InvalidateFile(pathDiskReadCacheFileID(e.Path))
			}
		})
		opts.Events.On(fs.EventDelete, func(e fs.Event) {
			c.mem.Invalidate(e.Path)
			c.mem.InvalidatePrefix(e.Path + "/")
			if c.dsk != nil {
				c.dsk.InvalidateFile(pathDiskReadCacheFileID(e.Path))
				c.dsk.InvalidatePathPrefix(e.Path + "/")
			}
		})
		// A remote-origin change invalidates that path's cached images (v1
		// invalidateReadCacheAndTargets was per-path); only a checkout/reset
		// sweeps the whole cache.
		opts.Events.On(fs.EventInvalidated, func(e fs.Event) {
			if e.Path == "/" {
				c.mem.InvalidateAll()
				if c.dsk != nil {
					c.dsk.InvalidateAll()
				}
				return
			}
			c.mem.Invalidate(e.Path)
			c.mem.InvalidatePrefix(e.Path + "/")
			if c.dsk != nil {
				c.dsk.InvalidateFile(pathDiskReadCacheFileID(e.Path))
				c.dsk.InvalidatePathPrefix(e.Path + "/")
			}
		})
		opts.Events.On(fs.EventCheckout, func(e fs.Event) {
			c.mem.InvalidateAll()
			if c.dsk != nil {
				c.dsk.InvalidateAll()
			}
		})
		opts.Events.On(fs.EventLanded, func(e fs.Event) {
			// A local commit settle (onCommitQueueSuccess /
			// onWriteBackUploadSuccess) updates the committed entry in
			// place — seeded the read cache with the committed image
			// and never invalidated on local commits. A rev-keyed seed
			// supersedes the old image on its own (a mismatched-rev Get
			// evicts), so only unknown-revision commits conservatively
			// drop the path.
			if e.Rev <= 0 {
				c.mem.Invalidate(e.Path)
			}
		})
	}
	return c, nil
}

// ReadCached returns whole-file cached bytes for path at revision, if
// admitted.
func (c *Cache) ReadCached(path string, revision int64) ([]byte, bool) {
	return c.mem.Get(path, revision)
}

// SeedCommit caches the just-committed image (readCache.Put at commit
// settle) so reads racing backend replica lag see this mount's own writes.
func (c *Cache) SeedCommit(path string, data []byte, rev int64) {
	if rev <= 0 || !c.admits(data) || sqliteJournalPath(path) {
		return
	}
	c.mem.PutOwned(path, data, rev)
}

// UnwrapHandle exposes the terminal handle (the facade's optional
// capability probes — distributed locks, lseek — must reach the terminal
// plane, not stop at this wrapper).
func (h *cachingHandle) UnwrapHandle() fs.Handle { return h.inner }

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
func (c *Cache) Invalidate(path string) { c.mem.Invalidate(path) }

// InvalidatePrefix drops every cached image under a prefix.
func (c *Cache) InvalidatePrefix(prefix string) { c.mem.InvalidatePrefix(prefix) }

// InvalidateAll drops every cached image.
func (c *Cache) InvalidateAll() { c.mem.InvalidateAll() }

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
		if res.Attr != nil {
			rev = res.Attr.Rev
			size = res.Attr.Size
		}
		h := &cachingHandle{cache: c, inner: res.Handle, path: op.Path, rev: rev, size: size}
		c.open.Store(h, struct{}{})
		if sqliteJournalPath(op.Path) {
			c.trackSidecarOpen(op.Path)
			h.isSidecar = true
		}
		if c.opts.SequentialPrefetch && c.opts.PrefetchClient != nil && size > c.mem.MaxFileSize() {
			// v1 armed the prefetcher only for handles whose size exceeds
			// the whole-file cache admission limit: smaller files are served
			// by the mem cache itself and need no block read-ahead.
			p := NewPrefetcher(c.opts.PrefetchClient, op.Path, size)
			p.SetParallelRead(c.opts.ParallelReadConcurrency, c.opts.ParallelReadBlockSize)
			h.prefetch = p
			h.prefetchArmed = true
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
				if h.path == op.Path {
					h.SetPath(op.Path2)
				} else if strings.HasPrefix(h.path, strings.TrimSuffix(op.Path, "/")+"/") {
					h.SetPath(op.Path2 + strings.TrimPrefix(h.path, op.Path))
				}
			}
			return true
		})
		c.mem.Rename(op.Path, op.Path2)
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
		res, err := next(ctx, op)
		if err == nil && res.Attr == nil && len(res.Data) > 0 {
			// Path-addressed reads carry no revision; event invalidation
			// keeps this coherent. Only an UNLIMITED offset-0 request
			// returns the whole image — caching a 16KiB range read under
			// the whole-file key made every later offset answer EOF
			// (the layer staged-append read-back).
			if op.Offset == 0 && op.Limit <= 0 && c.admits(res.Data) {
				c.mem.Put(op.Path, res.Data, 0)
			}
		}
		return res, err
	}
	return next(ctx, op)
}

// Read implements fs.Handle.
func (h *cachingHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	if data, ok := h.cache.mem.GetPending(h.path); ok {
		if off < 0 || off >= int64(len(data)) {
			return 0, nil
		}
		return copy(dest, data[off:]), nil
	}
	if h.prefetch != nil {
		h.prefetch.OnRead(off, len(dest))
		if data, ok := h.prefetch.Get(off, len(dest)); ok {
			return copy(dest, data), nil
		}
	}
	// h.rev <= 0 means the open could not pin a trustworthy revision (the
	// path is mid-landing locally): skip the revision-keyed tiers and let
	// the inner plane serve its own staged bytes. The same applies while
	// the path has local write state — its authoritative image is staging,
	// not the remote revision this handle pinned.
	if h.rev > 0 && h.size > 0 && h.size <= h.cache.mem.MaxFileSize() && !h.cache.dirs.hasLocalWriteState(h.path) && !h.cache.sqliteBypassStableCaches(h.path) {
		// v1 small-file tier: any offset slices one whole-file image; a
		// miss fetches the WHOLE file so a short final read (the kernel's
		// last block smaller than the request buffer) cannot lock the file
		// out of the caches. The disk tier below the mem tier survives the
		// mem TTL/eviction, serving warm re-reads across mounts.
		if data, ok := h.cache.mem.Get(h.path, h.rev); ok {
			h.cache.noteHit("read", true)
			return copyReadRange(dest, data, off), nil
		}
		h.cache.noteHit("read", false)
		key := DiskReadCacheKey{FileID: pathDiskReadCacheFileID(h.path), Path: h.path, Revision: h.rev, Offset: 0, Length: h.size}
		if h.cache.dsk != nil {
			if cached, ok := h.cache.dsk.Get(key); ok && int64(len(cached)) == h.size {
				h.cache.noteHit("read", true)
				h.cache.mem.PutOwned(h.path, cached, h.rev)
				return copyReadRange(dest, cached, off), nil
			}
		}
		whole := make([]byte, h.size)
		if n, err := h.inner.Read(ctx, 0, whole); err == nil && int64(n) == h.size {
			h.cache.mem.PutOwned(h.path, whole, h.rev)
			if h.cache.dsk != nil {
				h.cache.dsk.PutAsync(key, whole)
			}
			return copyReadRange(dest, whole, off), nil
		}
		// Fetch error or short image: fall through to the direct read.
		return h.inner.Read(ctx, off, dest)
	}
	if off == 0 && h.rev > 0 && h.cache.admits(dest) && !h.cache.sqliteBypassStableCaches(h.path) {
		if data, ok := h.cache.mem.Get(h.path, h.rev); ok && int64(len(data)) <= int64(len(dest)) {
			n := copy(dest, data)
			return n, nil
		}
		n, err := h.inner.Read(ctx, off, dest)
		// Admission proves the COMPLETE image was read (n == the file
		// size), not merely that the request buffer filled: caching a
		// 4096-byte prefix of an 8MiB file under the whole-file key made
		// the next larger read serve a premature EOF.
		if err == nil && int64(n) == int64(len(dest)) && h.size > 0 && int64(n) == h.size &&
			h.cache.admits(dest[:n]) && h.rev > 0 {
			h.cache.mem.PutOwned(h.path, append([]byte(nil), dest[:n]...), h.rev)
		}
		return n, err
	}
	// v1 range disk tier for large files: the request's own (offset,
	// size-clipped) range is fetched once and served from local disk on
	// repeat — including across mounts (the key carries the revision).
	if h.cache.dsk != nil && h.rev > 0 && !h.cache.dirs.hasLocalWriteState(h.path) && !h.cache.sqliteBypassStableCaches(h.path) {
		if n, ok := h.cache.readDiskCachedRange(ctx, h, off, dest); ok {
			return n, nil
		}
	}
	return h.inner.Read(ctx, off, dest)
}

// readDiskCachedRange serves one read through the on-disk block tier (v1
// readDiskCachedRange/diskReadCacheReadSize): the range is clipped to the
// open-time size, keyed by its exact bounds, fetched on miss, and cached
// only when the fetch came back whole. ok=false defers to the caller's
// direct read.
func (c *Cache) readDiskCachedRange(ctx context.Context, h *cachingHandle, off int64, dest []byte) (int, bool) {
	block := c.opts.ParallelReadBlockSize
	if block <= 0 {
		block = defaultParallelReadBlockSize
	}
	length := int64(len(dest))
	if length <= 0 || length > block {
		return 0, false // never split (kernel reads stay under MaxWrite)
	}
	if off < 0 || h.size > 0 && off >= h.size {
		return 0, true // EOF
	}
	if h.size > 0 {
		if end := off + length; end > h.size {
			length = h.size - off
		}
	}
	if length <= 0 {
		return 0, true
	}
	key := DiskReadCacheKey{FileID: pathDiskReadCacheFileID(h.path), Path: h.path, Revision: h.rev, Offset: off, Length: length}
	if data, ok := c.dsk.Get(key); ok && int64(len(data)) == length {
		c.noteHit("read", true)
		return copyReadRange(dest, data, 0), true
	}
	c.noteHit("read", false)
	buf := make([]byte, length)
	n, err := h.inner.Read(ctx, off, buf)
	if err != nil {
		return 0, false
	}
	if int64(n) == length {
		c.dsk.PutOwned(key, buf)
		return copyReadRange(dest, buf, 0), true
	}
	// Short fetch (EOF tail): serve directly; a truncated entry must not
	// poison the tier.
	copy(dest, buf[:n])
	return n, true
}

// copyReadRange copies data[off:] into dest, clipped at both ends.
func copyReadRange(dest, data []byte, off int64) int {
	if off < 0 || off >= int64(len(data)) {
		return 0
	}
	end := off + int64(len(dest))
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return copy(dest, data[off:end])
}

// Write implements fs.Handle.
func (h *cachingHandle) Write(ctx context.Context, off int64, data []byte) error {
	err := h.inner.Write(ctx, off, data)
	if err == nil {
		h.cache.mem.Invalidate(h.path)
		if h.prefetch != nil {
			h.prefetch.Invalidate()
		}
	}
	return err
}

// Sync implements fs.Handle.
func (h *cachingHandle) Sync(ctx context.Context) error { return h.inner.Sync(ctx) }

// Flush implements fs.Handle.
func (h *cachingHandle) Flush(ctx context.Context) error { return h.inner.Flush(ctx) }

// Release implements fs.Handle.
func (h *cachingHandle) Release(ctx context.Context) error {
	h.cache.open.Delete(h)
	if h.isSidecar {
		h.cache.sidecars.remove(h.path)
	}
	return h.inner.Release(ctx)
}

// SetPath retargets the handle (and its prefetcher) onto the renamed path.
func (h *cachingHandle) SetPath(p string) {
	h.path = p
	if h.prefetch != nil {
		h.prefetch.SetPath(p)
	}
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
			if h.path == oldPath {
				h.SetPath(newPath)
			} else if strings.HasPrefix(h.path, prefix) {
				h.SetPath(newPath + strings.TrimPrefix(h.path, oldPath))
			}
		}
		return true
	})
	c.mem.Rename(oldPath, newPath)
}

func (c *Cache) admits(data []byte) bool {
	return int64(len(data)) <= c.mem.MaxFileSize()
}

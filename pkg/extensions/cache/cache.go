// Package cache is the read-plane cache extension. It wraps handles
// opened through the chain with a caching decorator (in-memory LRU+TTL
// keyed by path/revision, optional on-disk block cache), serves
// path-addressed reads from the cache, and invalidates on bus events.
// The directory-neighborhood prefetcher (sibling metadata, sibling
// directories, readdir content) rides the same events.
package cache

import (
	"context"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fusecomp/writebuffer"
	"time"

	"github.com/mem9-ai/drive9/pkg/fs"
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
	SequentialPrefetch bool
	// PrefetchClient backs the Prefetcher's parallel fetches (the mount's
	// client); nil disables arming.
	PrefetchClient *client.Client

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
}

// cachingHandle decorates an inner handle with the whole-file read cache.
// Only complete reads are admitted (kernel small-file reads arrive as one
// full request); partial reads pass through untouched.
type cachingHandle struct {
	cache         *Cache
	inner         fs.Handle
	path          string
	rev           int64
	prefetch      *Prefetcher
	prefetchArmed bool
}

// New constructs the read-plane cache.
func New(opts Options) (*Cache, error) {
	c := &Cache{opts: opts,
		mem: NewReadCacheWithMaxFileSize(opts.MaxSize, opts.TTL, opts.MaxFileSize),
	}
	if opts.Disk.Dir != "" {
		d, err := NewDiskReadCache(opts.Disk)
		if err != nil {
			return nil, err
		}
		c.dsk = d
	}
	c.dirs = newDirPrefetcher(opts.PrefetchClient, opts.RemoteRoot)
	c.dirs.contentEnabled = opts.DirContentPrefetch
	c.dirs.contentMaxFiles = opts.PrefetchMaxFiles
	if c.dirs.contentMaxFiles <= 0 {
		c.dirs.contentMaxFiles = contentPrefetchMaxFilesDefault
	}
	c.dirs.contentMaxFile = opts.PrefetchMaxFileBytes
	if c.dirs.contentMaxFile <= 0 {
		c.dirs.contentMaxFile = 512 * 1024
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
		})
		opts.Events.On(fs.EventDelete, func(e fs.Event) {
			c.mem.Invalidate(e.Path)
			c.mem.InvalidatePrefix(e.Path + "/")
		})
		opts.Events.On(fs.EventCheckout, func(e fs.Event) { c.mem.InvalidateAll() })
		opts.Events.On(fs.EventInvalidated, func(e fs.Event) { c.mem.InvalidateAll() })
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
	if rev <= 0 || !c.admits(data) {
		return
	}
	c.mem.PutOwned(path, data, rev)
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
		h := &cachingHandle{cache: c, inner: res.Handle, path: op.Path, rev: rev}
		if c.opts.SequentialPrefetch && c.opts.PrefetchClient != nil {
			// read handles get a Prefetcher; sequential reads
			// trigger parallel block read-ahead through it.
			p := NewPrefetcher(c.opts.PrefetchClient, op.Path, size)
			h.prefetch = p
			h.prefetchArmed = true
		}
		res.Handle = h
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
			// path-addressed reads carry no revision; event invalidation
			// keeps this coherent
			if c.admits(res.Data) {
				c.mem.Put(op.Path, res.Data, 0)
			}
		}
		return res, err
	}
	return next(ctx, op)
}

// Read implements fs.Handle.
func (h *cachingHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	if h.prefetch != nil {
		h.prefetch.OnRead(off, len(dest))
		if data, ok := h.prefetch.Get(off, len(dest)); ok {
			return copy(dest, data), nil
		}
	}
	if off == 0 && h.cache.admits(dest) {
		if data, ok := h.cache.mem.Get(h.path, h.rev); ok && int64(len(data)) <= int64(len(dest)) {
			n := copy(dest, data)
			return n, nil
		}
		n, err := h.inner.Read(ctx, off, dest)
		if err == nil && int64(n) == int64(len(dest)) && h.cache.admits(dest[:n]) && h.rev > 0 {
			h.cache.mem.PutOwned(h.path, append([]byte(nil), dest[:n]...), h.rev)
		}
		return n, err
	}
	return h.inner.Read(ctx, off, dest)
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
func (h *cachingHandle) Release(ctx context.Context) error { return h.inner.Release(ctx) }

func (c *Cache) admits(data []byte) bool {
	return int64(len(data)) <= c.mem.MaxFileSize()
}

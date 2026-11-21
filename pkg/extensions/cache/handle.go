package cache

import (
	"context"
	"sync"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// cachingHandle decorates an inner handle with the whole-file read cache
// and the on-disk block tier.
type cachingHandle struct {
	cache *Cache
	inner fs.Handle
	path  string
	rev   int64
	size  int64 // open-time size; 0 = unknown
	// objectID is the server's object identity for the path
	// (X-Dat9-Resource-ID, empty when unreported). It names the OBJECT the
	// open pinned: revisions restart at 1 per file, so (path, revision)
	// alone cannot tell a replacement from the object a mount-surviving
	// disk entry was written for.
	objectID      string
	prefetch      *Prefetcher
	prefetchArmed bool
	// isSidecar marks an open -wal/-journal handle (tracker cleanup).
	isSidecar bool
	// grown marks a handle whose file changed through this descriptor:
	// its open-time size/rev are stale and whole-file cache admission
	// under that identity would serve the old image.
	grown bool
	// dirtyOwned marks THIS handle as a dirty-path owner; only it clears
	// its mark.
	dirtyOwned bool
	// revalidate asks the next read to re-pin rev/size against the remote
	// before the revision-keyed tiers answer: set when the stream lost
	// continuity (the pinned revision may be stale) or a remote change
	// replaced the image (the revision is known stale and zeroed). A
	// checkout does not set it: its descriptors stay off the tiers.
	revalidate bool
	// identityGen counts changes to the handle's identity (rev/size/path/
	// grown, including the invalidations that zero or flag them). A
	// revalidation stats the remote without the lock; a result that arrives
	// after the identity moved describes an older world and is discarded.
	identityGen uint64
	mu          sync.Mutex
}

// noteDirty/clearDirty track a HANDLE's unflushed writes. The caller passes
// the handle so the pair is ownership-based: N writes by one descriptor
// leave exactly one mark, and another descriptor's boundary is a no-op.
func (c *Cache) noteDirty(path string, h *cachingHandle) {
	h.mu.Lock()
	already := h.dirtyOwned
	h.dirtyOwned = true
	h.mu.Unlock()
	if already {
		return
	}
	c.dirtyMu.Lock()
	if c.dirty == nil {
		c.dirty = map[string]int{}
	}
	c.dirty[path]++
	c.dirtyMu.Unlock()
}

// moveDirty migrates the mark a handle owns from oldPath to newPath (rename
// retarget). A no-op when the handle owns nothing.
func (c *Cache) moveDirty(oldPath, newPath string, h *cachingHandle) {
	h.mu.Lock()
	owned := h.dirtyOwned
	h.mu.Unlock()
	if !owned || oldPath == newPath {
		return
	}
	c.dirtyMu.Lock()
	if n := c.dirty[oldPath]; n > 1 {
		c.dirty[oldPath] = n - 1
	} else {
		delete(c.dirty, oldPath)
	}
	if c.dirty == nil {
		c.dirty = map[string]int{}
	}
	c.dirty[newPath]++
	c.dirtyMu.Unlock()
}

func (c *Cache) clearDirty(path string, h *cachingHandle) {
	h.mu.Lock()
	owned := h.dirtyOwned
	h.dirtyOwned = false
	h.mu.Unlock()
	if !owned {
		return
	}
	c.dirtyMu.Lock()
	if n := c.dirty[path]; n > 1 {
		c.dirty[path] = n - 1
	} else {
		delete(c.dirty, path)
	}
	c.dirtyMu.Unlock()
}

// UnwrapHandle exposes the terminal handle (the facade's optional
// capability probes — distributed locks, lseek — must reach the terminal
// plane, not stop at this wrapper).
func (h *cachingHandle) UnwrapHandle() fs.Handle { return h.inner }

// Read implements fs.Handle.
// Unwrap exposes the decorated handle so inner extensions can resolve their
// own handle type for FD-directed operations.
func (h *cachingHandle) Unwrap() fs.Handle { return h.inner }

func (h *cachingHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	// A dirty sibling's bytes are the CURRENT image: the mem/disk tiers
	// hold a pre-write copy, so this read must go to the plane below
	// (which serves read-your-writers) instead of a stable cache hit.
	if path := h.identity(); h.cache.dirtyPath(path) {
		h.cache.mem.Invalidate(path)
		return h.inner.Read(ctx, off, dest)
	}
	// An ANONYMOUS fd (its name was unlinked or overwritten) reads a private
	// snapshot of the OLD object: every tier below is path-keyed and would
	// serve the replacement's pending/remote bytes, so bypass them all.
	if a, ok := h.inner.(interface{ Anonymous() bool }); ok && a.Anonymous() {
		return h.inner.Read(ctx, off, dest)
	}
	// The publication epoch is captured BEFORE the identity snapshot: a
	// sweep that lands after this capture (a rename, an event) must reject
	// whatever this read publishes, and a sweep that started earlier has
	// already completed under the capture (publicationEpoch takes the read
	// side of the fence). The reverse order would let a retargeted read
	// republish the OLD object's bytes under its pre-rename path key.
	epoch := h.cache.publicationEpoch()
	// One stable identity for the whole read (cache keys stay coherent
	// across a concurrent retarget).
	path := h.identity()
	if data, ok := h.cache.mem.GetPending(path); ok {
		if off < 0 || off >= int64(len(data)) {
			return 0, nil
		}
		return copy(dest, data[off:]), nil
	}
	// The plane's claim that this descriptor reads the remote namespace is
	// evaluated once per read, and only when a tier below depends on it
	// (it walks the inode's live handles).
	remote, remoteKnown := false, false
	remoteReadable := func() bool {
		if !remoteKnown {
			remote, remoteKnown = remoteNamespaceReadable(h.inner), true
		}
		return remote
	}
	// Re-pin rev/size against the remote before any revision-keyed tier
	// answers: once per loss of continuity or remote change, and on every
	// read while the stream is down (v1 revalidateReadCacheEntryIfUntrusted).
	if h.revalidationDue() {
		if !remoteReadable() {
			// The pinned revision is stale but this plane cannot re-pin it
			// (an inner decorator that does not read the remote namespace):
			// trust nothing revision-keyed, prefetched blocks included, and
			// let the plane below answer.
			if h.prefetch != nil {
				h.prefetch.Invalidate()
			}
			return h.inner.Read(ctx, off, dest)
		}
		pinned, err := h.revalidateRevision(ctx, path)
		if err != nil {
			return 0, err
		}
		if !pinned {
			// The identity moved under the stat: nothing revision-keyed
			// (prefetched blocks included) can answer this read.
			return h.inner.Read(ctx, off, dest)
		}
		// Intended: a revalidation that had to drop the tiers bumped the
		// epoch itself, so THIS read's publication is rejected and the next
		// read fetches again — one deliberate double fetch in exchange for
		// never seeding the pre-move image.
	}
	if h.prefetch != nil {
		if remoteReadable() {
			h.prefetch.OnRead(off, len(dest))
			if data, ok := h.prefetch.Get(off, len(dest)); ok {
				return copy(dest, data), nil
			}
		} else {
			// The path acquired a local image after the open armed the
			// prefetcher (a sibling's released image still landing, a
			// staged write): its blocks describe the remote base, not the
			// bytes the plane below serves now. Drop them instead of
			// answering stale; a later clean read re-arms from the remote.
			h.prefetch.Invalidate()
		}
	}
	// h.rev <= 0 means the open could not pin a trustworthy revision (the
	// path is mid-landing locally): skip the revision-keyed tiers and let
	// the inner plane serve its own staged bytes. The same applies while
	// the path has local write state — its authoritative image is staging,
	// not the remote revision this handle pinned.
	// ONE identity snapshot for the whole read: an invalidation can clear
	// or replace size/rev concurrently, and mixing values from before and
	// after (length, revision key, whole-image test) split one read across
	// two images.
	h.mu.Lock()
	size, rev, grown, objectID := h.size, h.rev, h.grown, h.objectID
	h.mu.Unlock()
	if rev > 0 && size > 0 && size <= h.cache.mem.MaxFileSize() && !grown && !h.cache.dirs.hasLocalWriteState(path) && !h.cache.sqliteBypassStableCaches(path) {
		// v1 small-file tier: any offset slices one whole-file image; a
		// miss fetches the WHOLE file so a short final read (the kernel's
		// last block smaller than the request buffer) cannot lock the file
		// out of the caches. The disk tier below the mem tier survives the
		// mem TTL/eviction, serving warm re-reads across mounts.
		if data, ok := h.cache.mem.Get(path, rev); ok {
			h.cache.noteHit("read", true)
			return copyReadRange(dest, data, off), nil
		}
		h.cache.noteHit("read", false)
		key := DiskReadCacheKey{FileID: pathDiskReadCacheFileID(path), ObjectID: objectID, Path: path, Revision: rev, Offset: 0, Length: size}
		// The capture PRECEDES the disk read: a blocking disk fetch that
		// completes after a reset must not promote its pre-reset bytes.
		if h.cache.dsk != nil {
			if cached, ok := h.cache.dsk.Get(key); ok && int64(len(cached)) == size {
				h.cache.noteHit("read", true)
				h.cache.publishIfCurrent(epoch, func() { h.cache.mem.PutOwned(path, cached, rev) })
				return copyReadRange(dest, cached, off), nil
			}
		}
		whole := make([]byte, size)
		if n, err := h.inner.Read(ctx, 0, whole); err == nil && int64(n) == size {
			// A checkout may have replaced the view while this fetch was
			// in flight: its bytes describe the OLD view and must not be
			// admitted (the OpRead path already carries this fence).
			h.cache.publishIfCurrent(epoch, func() {
				h.cache.mem.PutOwned(path, whole, rev)
				if h.cache.dsk != nil {
					h.cache.dsk.PutAsync(key, whole)
				}
			})
			return copyReadRange(dest, whole, off), nil
		}
		// Fetch error or short image: fall through to the direct read.
		return h.inner.Read(ctx, off, dest)
	}
	if off == 0 && rev > 0 && size > 0 && !grown && h.cache.admits(dest) && !h.cache.sqliteBypassStableCaches(path) {
		if data, ok := h.cache.mem.Get(path, rev); ok && int64(len(data)) <= int64(len(dest)) {
			n := copy(dest, data)
			return n, nil
		}
		n, err := h.inner.Read(ctx, off, dest)
		// Admission proves the COMPLETE image was read (n == the file
		// size), not merely that the request buffer filled: caching a
		// 4096-byte prefix of an 8MiB file under the whole-file key made
		// the next larger read serve a premature EOF.
		if err == nil && int64(n) == int64(len(dest)) && size > 0 && int64(n) == size &&
			h.cache.admits(dest[:n]) && rev > 0 {
			h.cache.publishIfCurrent(epoch, func() {
				h.cache.mem.PutOwned(path, append([]byte(nil), dest[:n]...), rev)
			})
		}
		return n, err
	}
	// v1 range disk tier for large files: the request's own (offset,
	// size-clipped) range is fetched once and served from local disk on
	// repeat — including across mounts (the key carries the revision).
	if h.cache.dsk != nil && rev > 0 && size > 0 && !grown && !h.cache.dirs.hasLocalWriteState(path) && !h.cache.sqliteBypassStableCaches(path) {
		// grown handles share the whole-file tiers' bypass: the disk
		// range key and clip encode the stale open-time size/revision.
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
	// The epoch is captured BEFORE the identity snapshot (path included): a
	// sweep that lands between the snapshot and the publication must reject
	// the pre-sweep bytes this read is about to fetch.
	epoch := c.publicationEpoch()
	path := h.identity()
	// One identity snapshot: an invalidation may reset size/rev mid-read.
	h.mu.Lock()
	size, rev, objectID := h.size, h.rev, h.objectID
	h.mu.Unlock()
	block := c.opts.ParallelReadBlockSize
	if block <= 0 {
		block = defaultParallelReadBlockSize
	}
	length := int64(len(dest))
	if length <= 0 || length > block {
		return 0, false // never split (kernel reads stay under MaxWrite)
	}
	if off < 0 || size > 0 && off >= size {
		return 0, true // EOF
	}
	if size > 0 {
		if end := off + length; end > size {
			length = size - off
		}
	}
	if length <= 0 {
		return 0, true
	}
	key := DiskReadCacheKey{FileID: pathDiskReadCacheFileID(path), ObjectID: objectID, Path: path, Revision: rev, Offset: off, Length: length}
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
		c.publishIfCurrent(epoch, func() { c.dsk.PutOwned(key, buf) })
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

// remoteNamespaceReadable reports the plane's claim that reads through h
// serve the ordinary remote namespace: core's SyncHandle states it, and a
// decorator forwards it only while its own bytes equal the terminal's
// (writeback: a clean descriptor of a path with no local image). It is
// consulted when the prefetcher is armed and again on every read it could
// answer, since the path can acquire a local image while the handle is
// open.
func remoteNamespaceReadable(h fs.Handle) bool {
	r, ok := h.(interface{ IsRemoteNamespaceReadable() bool })
	return ok && r.IsRemoteNamespaceReadable()
}

// revalidationDue reports whether the next read must re-pin the handle's
// revision before a revision-keyed tier may answer: once for a flagged
// handle (the stream lost continuity, or a remote change replaced the
// image), and on every read of a handle with a pinned revision while the
// stream is down. Without a client to stat with there is nothing to
// revalidate against — markUnverified sweeps instead, and never flags.
func (h *cachingHandle) revalidationDue() bool {
	if h.cache.opts.PrefetchClient == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.revalidate || (h.cache.unverified.Load() && h.rev > 0)
}

// revalidateRevision re-pins rev/size against the remote namespace (v1
// revalidateReadCacheEntryIfUntrusted): a matching revision keeps the
// revision-keyed tiers serving; a different one moves the handle onto it,
// drops the path's stale image and prefetched blocks, and clips the
// prefetcher to the new length. The stat runs without the handle lock: when
// the identity moved meanwhile (an invalidation, a landed commit, a
// retarget), the result describes an older world and is discarded —
// pinned=false, the handle stays flagged, and the caller serves this read
// from the plane below. A failed stat takes the same exit: a read that could
// not verify its revision answers from nothing revision-keyed, and the plane
// below reports whether the object is still there (a rename that retargeted
// the descriptor mid-stat makes the old name 404 while the fd stays valid).
func (h *cachingHandle) revalidateRevision(ctx context.Context, path string) (pinned bool, err error) {
	c := h.cache
	h.mu.Lock()
	gen := h.identityGen
	h.mu.Unlock()
	st, err := c.opts.PrefetchClient.StatCtx(ctx, c.remotePathOf(path))
	h.mu.Lock()
	if err != nil || h.identityGen != gen {
		// Unconditional, not only when the mover itself flags: an identity
		// change from EventLanded, a retarget or a concurrent write does not
		// set the flag, and a gap-mode revalidation runs with it unset —
		// one extra stat after a raced revalidation is cheaper than
		// trusting an identity that moved under an in-flight stat.
		h.revalidate = true
		h.mu.Unlock()
		return false, nil
	}
	// The pinning pair is (revision, object): a replacement that inherited
	// the old revision (a per-file counter restarting at 1) still describes
	// a different object, and every tier keyed below must see the change.
	moved := st.Revision <= 0 || st.Revision != h.rev ||
		(st.ResourceID != "" && h.objectID != "" && st.ResourceID != h.objectID)
	if moved && h.prefetch != nil {
		h.prefetch.invalidateWithSize(st.Size)
	}
	if st.Revision > 0 {
		h.rev, h.size, h.objectID = st.Revision, st.Size, st.ResourceID
	} else {
		// An old server reports no revision: nothing keys the tiers safely.
		h.rev, h.size, h.objectID = 0, 0, ""
	}
	h.revalidate = false
	h.mu.Unlock()
	if moved {
		c.fencedSweep(func() []detachedDiskReadCacheEntry {
			c.mem.Invalidate(path)
			return c.invalidateDiskPath(path)
		})
	}
	return true, nil
}

// invalidateSiblingPrefetch drops the prefetched blocks of every OTHER open
// descriptor of path once a write through writer succeeded: a block fetched
// before the write describes the pre-write remote image, and a reader that
// does not read again until the write has landed never sees the dirty
// window that would make it drop the block itself (nor, without an event
// bus, an EventLanded). The cancellation also fails a Get parked on an
// in-flight fetch, so a block completing after the write is not answered.
// Hardlink aliases count as siblings: a write through /b invalidates the
// blocks a reader of /a prefetched for the same object.
func (c *Cache) invalidateSiblingPrefetch(path string, writer *cachingHandle) {
	aliases := c.aliasSet(path)
	c.open.Range(func(key, _ any) bool {
		h, ok := key.(*cachingHandle)
		if !ok || h == writer {
			return true
		}
		if _, sibling := aliases[h.identity()]; !sibling {
			return true
		}
		h.mu.Lock()
		if h.prefetch != nil {
			h.prefetch.Invalidate()
		}
		h.mu.Unlock()
		return true
	})
}

// Write implements fs.Handle.
func (h *cachingHandle) Write(ctx context.Context, off int64, data []byte) error {
	err := h.inner.Write(ctx, off, data)
	if err == nil {
		path := h.identity()
		h.cache.noteDirty(path, h)
		h.cache.mem.Invalidate(path)
		if h.prefetch != nil {
			h.prefetch.Invalidate()
		}
		h.cache.invalidateSiblingPrefetch(path, h)
		// The open-time size/rev no longer describe this file: the image
		// grew (or changed) through this descriptor. Later reads must not
		// admit whole-file cache entries under the stale identity — a
		// 4096-byte read then returned only the open-time prefix forever.
		h.mu.Lock()
		h.grown = true
		h.identityGen++
		h.mu.Unlock()
	}
	return err
}

// Sync implements fs.Handle.
func (h *cachingHandle) Sync(ctx context.Context) error {
	err := h.inner.Sync(ctx)
	if err == nil {
		// Only a SUCCESSFUL commit boundary releases the dirty ownership:
		// a rejected upload leaves the bytes uncommitted and their cache
		// bypass must stay in force.
		h.cache.clearDirty(h.identity(), h)
	}
	return err
}

// Flush implements fs.Handle.
func (h *cachingHandle) Flush(ctx context.Context) error {
	err := h.inner.Flush(ctx)
	if err == nil {
		h.cache.clearDirty(h.identity(), h)
	}
	return err
}

// Release implements fs.Handle.
func (h *cachingHandle) Release(ctx context.Context) error {
	h.cache.open.Delete(h)
	// The prefetcher runs on its own background context: dropping the
	// handle without closing it left requests running for a closed file
	// (and a later global sweep could no longer find them).
	if h.prefetch != nil {
		h.prefetch.Close()
	}
	if h.isSidecar {
		h.cache.sidecars.remove(h.identity())
	}
	// The descriptor's close is a durability boundary for its writes (the
	// below plane commits them): stop forcing sibling reads around the
	// stable tiers.
	h.cache.clearDirty(h.identity(), h)
	return h.inner.Release(ctx)
}

// identity snapshots the handle's path under its mutex: rename/survivor
// retargeting rewrites it concurrently, and a bare read both races and
// can split one cache operation across two identities.
func (h *cachingHandle) identity() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.path
}

// SetPath retargets the handle (and its prefetcher) onto the renamed path.
func (h *cachingHandle) SetPath(p string) {
	h.mu.Lock()
	old := h.path
	h.path = p
	h.identityGen++
	h.mu.Unlock()
	if old != p {
		// The dirty mark is ownership-based and keyed by path: leaving it
		// on the old name bypassed caches for a later same-name file
		// forever, while the writer's own boundary now cleared the new
		// name (a no-op) and never the old one.
		h.cache.moveDirty(old, p, h)
	}
	if h.prefetch != nil {
		// The prefetcher addresses the REMOTE namespace: a retarget that
		// handed it the view path mixed a same-named file outside the
		// mount subtree into the stream.
		h.prefetch.SetPath(h.cache.remotePathOf(p))
	}
}

package cache

import (
	"container/list"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/fusecomp/membudget"
)

const (
	defaultReadCacheMaxSize = 128 << 20        // 128MB
	defaultReadCacheTTL     = 30 * time.Second // 30s
	readCacheNoExpiryTTL    = -1
	// Positive kernel caches stay bounded for generic mounts. The coding-agent
	// profile uses a shorter attribute TTL while leaving entry caching unchanged.
	// defaultSmallFileThreshold is the local fallback used when no server
	// value has been negotiated yet. The authoritative value is fetched from
	// //status on the dat9 client and propagated through FS.smallFileMax.
	// defaultReadCacheMaxFileSize is deliberately larger than the inline
	// fallback so medium files do not miss userspace cache solely because
	// their server inline threshold is lower. The aggregate cache cap still
	// bounds memory use. This threshold also gates the whole-file
	// single-request read path in Read: files at or below it are fetched
	// with one GET instead of block-split range reads + prefetch, so it is
	// sized to cover common small-object workloads (e.g. 2MiB media chunks)
	// without turning the cache into an unbounded object store.
	defaultReadCacheMaxFileSize = 4 << 20
)

// ReadCache is a thread-safe LRU + TTL read cache for small and medium files.
// It only caches files whose size does not exceed the per-cache maxFile limit.
// Entries are evicted when the total cached size exceeds maxSize or when
// their TTL expires. A negative TTL disables time-based expiry.
type ReadCache struct {
	mu      sync.Mutex
	items   map[string]*cacheEntry
	order   *list.List // front = most recently used
	size    int64      // current total bytes cached
	maxSize int64
	maxFile int64
	ttl     time.Duration
	// budget, when set, is charged for the bytes this cache retains. A short
	// budget degrades the cache (entries are not retained); it never fails
	// the read that was being cached.
	budget *membudget.Budget
}

// cacheEntry holds a single cached file's data and metadata.
type cacheEntry struct {
	path     string
	data     []byte
	revision int64 // dat9 file revision for invalidation
	expires  time.Time
	elem     *list.Element // position in LRU list
}

// NewReadCache creates a ReadCache with the given capacity and TTL.
// If maxSize <= 0, defaultReadCacheMaxSize (128MB) is used.
// If ttl == 0, defaultReadCacheTTL (30s) is used.
// If ttl < 0, entries do not expire by time.
func NewReadCache(maxSize int64, ttl time.Duration) *ReadCache {
	return NewReadCacheWithMaxFileSize(maxSize, ttl, 0)
}

// NewReadCacheWithMaxFileSize creates a ReadCache with an explicit per-file
// admission limit. If maxFileSize <= 0, defaultReadCacheMaxFileSize is used.
func NewReadCacheWithMaxFileSize(maxSize int64, ttl time.Duration, maxFileSize int64) *ReadCache {
	if maxSize <= 0 {
		maxSize = defaultReadCacheMaxSize
	}
	if ttl == 0 {
		ttl = defaultReadCacheTTL
	}
	if maxFileSize <= 0 {
		maxFileSize = defaultReadCacheMaxFileSize
	}
	return &ReadCache{
		items:   make(map[string]*cacheEntry),
		order:   list.New(),
		maxSize: maxSize,
		maxFile: maxFileSize,
		ttl:     ttl,
	}
}

// SetMemoryBudget wires the process-wide memory budget this cache charges its
// retained payloads to. A nil budget disables the charge (the local maxSize
// LRU still applies).
func (rc *ReadCache) SetMemoryBudget(b *membudget.Budget) {
	if rc == nil {
		return
	}
	rc.mu.Lock()
	rc.budget = b
	rc.mu.Unlock()
	if b != nil {
		// Cached read images are the cheapest bytes to give back when a
		// write buffer needs room.
		b.RegisterReclaimer(membudget.ClassReadCache, rc.ReclaimBytes)
	}
}

// ReclaimBytes evicts LRU entries until at least target bytes are freed and
// reports how many bytes it actually freed. It is the budget's reclaimer hook
// for this cache.
func (rc *ReadCache) ReclaimBytes(target int64) int64 {
	if rc == nil || target <= 0 {
		return 0
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	var freed int64
	for freed < target {
		tail := rc.order.Back()
		if tail == nil {
			break
		}
		entry, ok := tail.Value.(*cacheEntry)
		if !ok {
			break
		}
		freed += int64(len(entry.data))
		rc.evict(entry)
	}
	return freed
}

// releaseLocked returns the budget charge of bytes this cache no longer
// retains (a replaced entry). The caller must hold rc.mu.
func (rc *ReadCache) releaseLocked(n int64) {
	if rc.budget == nil || n <= 0 {
		return
	}
	rc.budget.Release(membudget.ClassReadCache, n)
}

// Size reports the bytes currently retained.
func (rc *ReadCache) Size() int64 {
	if rc == nil {
		return 0
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.size
}

// MaxFileSize returns the largest payload admitted into this cache.
func (rc *ReadCache) MaxFileSize() int64 {
	if rc == nil || rc.maxFile <= 0 {
		return defaultReadCacheMaxFileSize
	}
	return rc.maxFile
}

// Get returns cached data for the given path if the entry exists, has not
// expired, and its revision matches currentRevision. If currentRevision <= 0
// the revision check is skipped. On a hit the entry is promoted to the front
// of the LRU list. On a miss or stale entry, the entry is removed and
// (nil, false) is returned.
func (rc *ReadCache) Get(path string, currentRevision int64) ([]byte, bool) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	entry, ok := rc.items[path]
	if !ok {
		return nil, false
	}

	// Check TTL expiration. A negative TTL means no time-based expiry.
	if rc.ttl > 0 && time.Now().After(entry.expires) {
		rc.evict(entry)
		return nil, false
	}

	// Check revision if caller supplied a positive revision.
	if currentRevision > 0 && currentRevision != entry.revision {
		rc.evict(entry)
		return nil, false
	}

	// Cache hit — promote to front of LRU.
	rc.order.MoveToFront(entry.elem)

	// Return the cached data directly. The caller (FUSE Read) treats this
	// as read-only and copies it into the kernel response buffer via
	// gofuse.ReadResultData, so the cached data is never mutated. If the
	// entry is evicted after this returns, the slice remains valid because
	// the returned slice still keeps the backing array reachable for the GC.
	return entry.data, true
}

// GetPending returns the path's unversioned pending seed (revision 0):
// this mount's newest staged image, planted at the write plane's flush
// boundary. It outranks any revision a handle pinned.
func (rc *ReadCache) GetPending(path string) ([]byte, bool) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	entry, ok := rc.items[path]
	if !ok || entry.revision != 0 {
		return nil, false
	}
	if rc.ttl > 0 && time.Now().After(entry.expires) {
		rc.evict(entry)
		return nil, false
	}
	rc.order.MoveToFront(entry.elem)
	return entry.data, true
}

// Put stores data in the cache for the given path and revision. Only files
// whose size does not exceed the cache's per-file limit are cached. The cache
// limit is intentionally a static memory-pressure cap rather than a mirror
// of the server's inline_threshold; raising the latter should not silently
// expand FUSE's per-mount RAM footprint. If an entry for the path already
// exists it is updated in place. After insertion, LRU eviction runs until
// the total cached size is within maxSize.
func (rc *ReadCache) Put(path string, data []byte, revision int64) {
	rc.put(path, data, revision, true)
}

// PutOwned stores data in the cache without cloning it.
// Callers must pass a fresh slice that will not be mutated after this call.
func (rc *ReadCache) PutOwned(path string, data []byte, revision int64) {
	rc.put(path, data, revision, false)
}

// PutPendingOwned stores the path's unversioned pending seed, bypassing
// the whole-file admission cap.
func (rc *ReadCache) PutPendingOwned(path string, data []byte) {
	rc.putUncapped(path, data, 0, false)
}

// Rename moves a cached image to the new path (a rename does not change
// content or revision); the old name's entry dies with the name.
func (rc *ReadCache) Rename(oldPath, newPath string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if entry, ok := rc.items[oldPath]; ok {
		delete(rc.items, oldPath)
		entry.path = newPath
		rc.items[newPath] = entry
	}
}

// Invalidate removes a specific path from the cache.
func (rc *ReadCache) Invalidate(path string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if entry, ok := rc.items[path]; ok {
		rc.evict(entry)
	}
}

// InvalidatePrefix removes all entries whose path starts with prefix.
// This is useful for directory-level invalidation (e.g. after a rename or
// delete of an entire subtree).
func (rc *ReadCache) InvalidatePrefix(prefix string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	for p, entry := range rc.items {
		if strings.HasPrefix(p, prefix) {
			rc.evict(entry)
		}
	}
}

// InvalidateAll removes all entries from the cache.
func (rc *ReadCache) InvalidateAll() {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.items = make(map[string]*cacheEntry)
	rc.order.Init()
	if rc.budget != nil {
		rc.budget.Release(membudget.ClassReadCache, rc.size)
	}
	rc.size = 0
}

func (rc *ReadCache) put(path string, data []byte, revision int64, clone bool) {
	if int64(len(data)) > rc.MaxFileSize() {
		return
	}
	rc.putUncapped(path, data, revision, clone)
}

// putUncapped stores without the whole-file admission cap: the write
// plane's pending seed is authoritative bytes, not a heuristic admission
// (its memory bound is the staging's, which already caps the image).
func (rc *ReadCache) putUncapped(path string, data []byte, revision int64, clone bool) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	dataLen := int64(len(data))
	// Charge the full payload first: a cache insert is allowed to degrade
	// (the entry is simply not retained), never to fail. chargeLocked may
	// evict this cache's own LRU tail, so the entry is looked up again below
	// and a surviving replaced entry gives its old bytes back then.
	if !rc.chargeLocked(dataLen) {
		return
	}

	stored := data
	if clone {
		stored = make([]byte, dataLen)
		copy(stored, data)
	}

	now := time.Now()

	if existing, ok := rc.items[path]; ok {
		// Update existing entry.
		oldLen := int64(len(existing.data))
		rc.size -= oldLen
		rc.releaseLocked(oldLen)
		existing.data = stored
		existing.revision = revision
		existing.expires = now.Add(rc.ttl)
		rc.size += dataLen
		rc.order.MoveToFront(existing.elem)
	} else {
		// New entry.
		entry := &cacheEntry{
			path:     path,
			data:     stored,
			revision: revision,
			expires:  now.Add(rc.ttl),
		}
		entry.elem = rc.order.PushFront(entry)
		rc.items[path] = entry
		rc.size += dataLen
	}

	// Evict LRU entries until we are within budget.
	for rc.size > rc.maxSize && rc.order.Len() > 0 {
		tail := rc.order.Back()
		if tail == nil {
			break
		}
		rc.evict(tail.Value.(*cacheEntry))
	}
}

// evict removes an entry from the items map, the LRU list, and decrements
// the total cached size. The caller must hold rc.mu.
func (rc *ReadCache) evict(entry *cacheEntry) {
	delete(rc.items, entry.path)
	rc.order.Remove(entry.elem)
	size := int64(len(entry.data))
	rc.size -= size
	if rc.budget != nil {
		rc.budget.Release(membudget.ClassReadCache, size)
	}
}

// chargeLocked reserves n bytes of the process memory budget, evicting this
// cache's own LRU tail while the budget is short. It reports whether the
// payload may be retained. The caller must hold rc.mu; the budget call is
// Charge (never Acquire) so no reclaimer can re-enter this lock.
func (rc *ReadCache) chargeLocked(n int64) bool {
	if rc.budget == nil || n <= 0 {
		return true
	}
	for {
		if rc.budget.Charge(membudget.ClassReadCache, n) == nil {
			return true
		}
		tail := rc.order.Back()
		if tail == nil {
			return false
		}
		rc.evict(tail.Value.(*cacheEntry))
	}
}

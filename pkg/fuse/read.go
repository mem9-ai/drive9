package fuse

import (
	"container/list"
	"strings"
	"sync"
	"time"
)

const (
	defaultReadCacheMaxSize = 128 << 20        // 128MB
	defaultReadCacheTTL     = 30 * time.Second // 30s
	readCacheNoExpiryTTL    = -1
	// defaultPositiveKernelCacheTTL keeps positive path metadata hot across
	// repeated scans while SSE/self-invalidation still handles namespace changes.
	defaultPositiveKernelCacheTTL = 60 * time.Second
	// defaultSmallFileThreshold is the local fallback used when no server
	// value has been negotiated yet. The authoritative value is fetched from
	// /v1/status on the dat9 client and propagated through FS.smallFileMax.
	defaultSmallFileThreshold = 50_000
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

// cacheEntry holds a single cached file's data and metadata.
type cacheEntry struct {
	path     string
	data     []byte
	revision int64 // dat9 file revision for invalidation
	expires  time.Time
	elem     *list.Element // position in LRU list
}

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

func (rc *ReadCache) put(path string, data []byte, revision int64, clone bool) {
	if int64(len(data)) > rc.MaxFileSize() {
		return
	}

	rc.mu.Lock()
	defer rc.mu.Unlock()

	dataLen := int64(len(data))
	stored := data
	if clone {
		stored = make([]byte, dataLen)
		copy(stored, data)
	}

	now := time.Now()

	if existing, ok := rc.items[path]; ok {
		// Update existing entry.
		rc.size -= int64(len(existing.data))
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
	rc.size = 0
}

// evict removes an entry from the items map, the LRU list, and decrements
// the total cached size. The caller must hold rc.mu.
func (rc *ReadCache) evict(entry *cacheEntry) {
	delete(rc.items, entry.path)
	rc.order.Remove(entry.elem)
	rc.size -= int64(len(entry.data))
}

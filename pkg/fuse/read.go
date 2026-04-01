package fuse

import (
	"container/list"
	"strings"
	"sync"
	"time"
)

const (
	defaultReadCacheMaxSize = 128 << 20          // 128MB
	defaultReadCacheTTL     = 30 * time.Second   // 30s
	smallFileThreshold      = 50 << 10           // 50KB - matches client.DefaultSmallFileThreshold
)

// cacheEntry holds a single cached file's data and metadata.
type cacheEntry struct {
	path     string
	data     []byte
	revision int64         // dat9 file revision for invalidation
	expires  time.Time
	elem     *list.Element // position in LRU list
}

// ReadCache is a thread-safe LRU + TTL read cache for small files.
// It only caches files whose size does not exceed smallFileThreshold (1MB).
// Entries are evicted when the total cached size exceeds maxSize or when
// their TTL expires.
type ReadCache struct {
	mu      sync.Mutex
	items   map[string]*cacheEntry
	order   *list.List // front = most recently used
	size    int64      // current total bytes cached
	maxSize int64
	ttl     time.Duration
}

// NewReadCache creates a ReadCache with the given capacity and TTL.
// If maxSize <= 0, defaultReadCacheMaxSize (128MB) is used.
// If ttl <= 0, defaultReadCacheTTL (30s) is used.
func NewReadCache(maxSize int64, ttl time.Duration) *ReadCache {
	if maxSize <= 0 {
		maxSize = defaultReadCacheMaxSize
	}
	if ttl <= 0 {
		ttl = defaultReadCacheTTL
	}
	return &ReadCache{
		items:   make(map[string]*cacheEntry),
		order:   list.New(),
		maxSize: maxSize,
		ttl:     ttl,
	}
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

	// Check TTL expiration.
	if time.Now().After(entry.expires) {
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

	// Return a copy so the caller cannot mutate cached data.
	out := make([]byte, len(entry.data))
	copy(out, entry.data)
	return out, true
}

// Put stores data in the cache for the given path and revision. Only files
// whose size does not exceed smallFileThreshold are cached. If an entry for
// the path already exists it is updated in place. After insertion, LRU
// eviction runs until the total cached size is within maxSize.
func (rc *ReadCache) Put(path string, data []byte, revision int64) {
	if len(data) > smallFileThreshold {
		return
	}

	rc.mu.Lock()
	defer rc.mu.Unlock()

	dataLen := int64(len(data))

	// Make a defensive copy of the data.
	stored := make([]byte, dataLen)
	copy(stored, data)

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

// evict removes an entry from the items map, the LRU list, and decrements
// the total cached size. The caller must hold rc.mu.
func (rc *ReadCache) evict(entry *cacheEntry) {
	delete(rc.items, entry.path)
	rc.order.Remove(entry.elem)
	rc.size -= int64(len(entry.data))
}

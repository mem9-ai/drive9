package fuse

import (
	"sync"
	"time"
)

const (
	defaultDirCacheTTL = 5 * time.Second
)

// CachedFileInfo matches the client.FileInfo shape but avoids importing client.
type CachedFileInfo struct {
	Name  string
	Size  int64
	IsDir bool
	Mtime time.Time
}

type dirCacheEntry struct {
	items   []CachedFileInfo
	expires time.Time
}

// DirCache is a thread-safe, TTL-based cache for directory listings.
type DirCache struct {
	mu      sync.Mutex
	entries map[string]*dirCacheEntry // keyed by directory path
	ttl     time.Duration
}

// NewDirCache creates a new DirCache with the given TTL.
// If ttl <= 0, defaultDirCacheTTL is used.
func NewDirCache(ttl time.Duration) *DirCache {
	if ttl <= 0 {
		ttl = defaultDirCacheTTL
	}
	return &DirCache{
		entries: make(map[string]*dirCacheEntry),
		ttl:     ttl,
	}
}

// Get returns cached directory entries if they exist and have not expired.
// If the entry has expired, it is deleted and (nil, false) is returned.
func (dc *DirCache) Get(path string) ([]CachedFileInfo, bool) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	entry, ok := dc.entries[path]
	if !ok {
		return nil, false
	}

	if time.Now().After(entry.expires) {
		delete(dc.entries, path)
		return nil, false
	}

	return entry.items, true
}

// Put stores directory entries in the cache with an expiration of now + ttl.
func (dc *DirCache) Put(path string, items []CachedFileInfo) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	dc.entries[path] = &dirCacheEntry{
		items:   items,
		expires: time.Now().Add(dc.ttl),
	}
}

// Invalidate removes a specific directory entry from the cache.
func (dc *DirCache) Invalidate(path string) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	delete(dc.entries, path)
}

// InvalidateAll clears all cache entries.
func (dc *DirCache) InvalidateAll() {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	dc.entries = make(map[string]*dirCacheEntry)
}

package fuse

import (
	"path"
	"strings"
	"sync"
	"time"
)

const (
	defaultDirCacheTTL              = 10 * time.Second
	defaultNamespaceCacheMaxEntries = 2000
)

// CachedFileInfo matches the client.FileInfo shape but avoids importing client.
type CachedFileInfo struct {
	Name       string
	Size       int64
	IsDir      bool
	Mtime      time.Time
	Revision   int64
	Mode       uint32 // permission bits
	HasMode    bool   // true when Mode is explicitly known (including 0)
	Uid        uint32
	Gid        uint32
	HasUID     bool
	HasGID     bool
	ResourceID string
	Nlink      uint32
}

type namespaceLookupKind uint8

const (
	namespaceLookupNone namespaceLookupKind = iota
	namespaceLookupPositive
	namespaceLookupNegative
	namespaceLookupCompleteMiss
	namespaceLookupSessionMiss
	namespaceLookupPartialMiss
)

type namespaceLookupResult struct {
	kind namespaceLookupKind
	item CachedFileInfo
}

type dirCacheEntry struct {
	items           map[string]CachedFileInfo
	order           []string
	expires         time.Time // positive entries and cached ReadDir listing TTL
	complete        bool
	completeExpires time.Time // safe ENOENT-on-miss TTL for a complete listing
	sessionExpires  time.Time // safe ENOENT-on-miss TTL for a session-created dir
	negatives       map[string]time.Time
}

// DirCache is a thread-safe, TTL-based namespace cache.
//
// It stores positive entries, explicit negative lookups, complete directory
// listings, and mount-local session-created directories. Misses are only safe
// to answer locally when they come from a complete listing, an explicit
// negative marker, or a session-created directory. Partial parents still miss
// through to remote stat.
type DirCache struct {
	mu          sync.Mutex
	entries     map[string]*dirCacheEntry // keyed by directory path
	ttl         time.Duration
	negativeTTL time.Duration
	maxEntries  int
}

// NewDirCache creates a new DirCache with the given TTL.
// If ttl <= 0, defaultDirCacheTTL is used.
func NewDirCache(ttl time.Duration) *DirCache {
	return NewNamespaceCache(ttl, ttl, defaultNamespaceCacheMaxEntries)
}

// NewNamespaceCache creates a namespace-aware directory cache.
func NewNamespaceCache(ttl, negativeTTL time.Duration, maxEntries int) *DirCache {
	if ttl <= 0 {
		ttl = defaultDirCacheTTL
	}
	if negativeTTL <= 0 || negativeTTL > ttl {
		negativeTTL = ttl
	}
	if maxEntries <= 0 {
		maxEntries = defaultNamespaceCacheMaxEntries
	}
	return &DirCache{
		entries:     make(map[string]*dirCacheEntry),
		ttl:         ttl,
		negativeTTL: negativeTTL,
		maxEntries:  maxEntries,
	}
}

// Get returns cached directory entries if they exist and have not expired.
// If the entry has expired, it is deleted and (nil, false) is returned.
func (dc *DirCache) Get(dirPath string) ([]CachedFileInfo, bool) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	now := time.Now()
	entry, ok := dc.getEntryLocked(dirPath, now)
	if !ok || !entry.completeListValid(now) {
		return nil, false
	}

	items := make([]CachedFileInfo, 0, len(entry.order))
	for _, name := range entry.order {
		item, ok := entry.items[name]
		if ok {
			items = append(items, item)
		}
	}
	return items, true
}

// Put stores directory entries in the cache with an expiration of now + ttl.
func (dc *DirCache) Put(dirPath string, items []CachedFileInfo) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	now := time.Now()
	oldEntry := dc.entries[dirPath]
	if oldEntry != nil {
		oldEntry.prune(now)
	}
	entry := newDirCacheEntry()
	entry.expires = now.Add(dc.ttl)
	limit := len(items)
	if limit > dc.maxEntries {
		limit = dc.maxEntries
	}
	for i := 0; i < limit; i++ {
		item := items[i]
		if oldEntry != nil {
			if oldItem, ok := oldEntry.items[item.Name]; ok {
				item = mergeCachedOwner(item, oldItem)
			}
		}
		entry.upsert(item, dc.maxEntries)
	}
	if len(items) <= dc.maxEntries {
		entry.complete = true
		entry.completeExpires = now.Add(dc.negativeTTL)
	}
	dc.entries[dirPath] = entry
}

// Lookup returns the namespace-cache state for parent/name.
func (dc *DirCache) Lookup(parentPath, name string) namespaceLookupResult {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	now := time.Now()
	entry, ok := dc.getEntryLocked(parentPath, now)
	if !ok {
		return namespaceLookupResult{}
	}
	if item, ok := entry.items[name]; ok && entry.positiveValid(now) {
		return namespaceLookupResult{kind: namespaceLookupPositive, item: item}
	}
	if entry.negativeValid(name, now) {
		return namespaceLookupResult{kind: namespaceLookupNegative}
	}
	if entry.sessionValid(now) {
		return namespaceLookupResult{kind: namespaceLookupSessionMiss}
	}
	if entry.completeValid(now) {
		return namespaceLookupResult{kind: namespaceLookupCompleteMiss}
	}
	if entry.hasState() {
		return namespaceLookupResult{kind: namespaceLookupPartialMiss}
	}
	return namespaceLookupResult{}
}

// Upsert records or refreshes a known child entry for a parent.
func (dc *DirCache) Upsert(parentPath string, item CachedFileInfo) {
	if item.Name == "" {
		return
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()

	now := time.Now()
	entry := dc.ensureEntryLocked(parentPath)
	entry.expires = now.Add(dc.ttl)
	entry.upsert(item, dc.maxEntries)
	delete(entry.negatives, item.Name)
}

// Remove deletes a known child entry from a parent without implying the parent
// namespace is complete.
func (dc *DirCache) Remove(parentPath, name string) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	entry, ok := dc.entries[parentPath]
	if !ok {
		return
	}
	entry.remove(name)
	if !entry.hasState() {
		delete(dc.entries, parentPath)
	}
}

func (dc *DirCache) HasPositiveEntries(dirPath string) bool {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	now := time.Now()
	entry, ok := dc.getEntryLocked(dirPath, now)
	if !ok || !entry.positiveValid(now) {
		return false
	}
	return len(entry.items) > 0
}

// MarkNegative records a short-lived ENOENT marker for parent/name.
func (dc *DirCache) MarkNegative(parentPath, name string) {
	if name == "" || dc.negativeTTL <= 0 {
		return
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()

	entry := dc.ensureEntryLocked(parentPath)
	entry.remove(name)
	if entry.negatives == nil {
		entry.negatives = make(map[string]time.Time)
	}
	entry.negatives[name] = time.Now().Add(dc.negativeTTL)
}

// MarkSessionCreatedDir records a directory created by this mount as having a
// locally managed empty namespace for a short TTL.
func (dc *DirCache) MarkSessionCreatedDir(dirPath string) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	now := time.Now()
	entry := dc.ensureEntryLocked(dirPath)
	entry.expires = now.Add(dc.ttl)
	entry.complete = true
	entry.completeExpires = now.Add(dc.negativeTTL)
	entry.sessionExpires = now.Add(dc.negativeTTL)
	entry.negatives = make(map[string]time.Time)
}

// InvalidatePrefix removes cached namespace state for dirPath and descendants.
func (dc *DirCache) InvalidatePrefix(dirPath string) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	if dirPath == "/" {
		dc.entries = make(map[string]*dirCacheEntry)
		return
	}
	prefix := dirPath
	prefix += "/"
	for p := range dc.entries {
		if p == dirPath || strings.HasPrefix(p, prefix) {
			delete(dc.entries, p)
		}
	}
}

// Invalidate removes a specific directory entry from the cache.
func (dc *DirCache) Invalidate(dirPath string) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	delete(dc.entries, dirPath)
}

// InvalidateAll clears all cache entries.
func (dc *DirCache) InvalidateAll() {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	dc.entries = make(map[string]*dirCacheEntry)
}

func newDirCacheEntry() *dirCacheEntry {
	return &dirCacheEntry{
		items:     make(map[string]CachedFileInfo),
		negatives: make(map[string]time.Time),
	}
}

func (dc *DirCache) ensureEntryLocked(dirPath string) *dirCacheEntry {
	entry, ok := dc.entries[dirPath]
	if !ok {
		entry = newDirCacheEntry()
		dc.entries[dirPath] = entry
	}
	if entry.items == nil {
		entry.items = make(map[string]CachedFileInfo)
	}
	if entry.negatives == nil {
		entry.negatives = make(map[string]time.Time)
	}
	return entry
}

func (dc *DirCache) getEntryLocked(dirPath string, now time.Time) (*dirCacheEntry, bool) {
	entry, ok := dc.entries[dirPath]
	if !ok {
		return nil, false
	}
	entry.prune(now)
	if !entry.hasState() {
		delete(dc.entries, dirPath)
		return nil, false
	}
	return entry, true
}

func (e *dirCacheEntry) upsert(item CachedFileInfo, maxEntries int) {
	if item.Name == "" {
		return
	}
	if e.items == nil {
		e.items = make(map[string]CachedFileInfo)
	}
	if existing, ok := e.items[item.Name]; ok {
		item = mergeCachedOwner(item, existing)
	}
	if _, exists := e.items[item.Name]; !exists {
		if maxEntries > 0 && len(e.items) >= maxEntries {
			if len(e.order) == 0 {
				return
			}
			evict := e.order[0]
			e.order = e.order[1:]
			delete(e.items, evict)
			delete(e.negatives, evict)
			e.complete = false
			e.completeExpires = time.Time{}
			e.sessionExpires = time.Time{}
		}
		e.order = append(e.order, item.Name)
	}
	e.items[item.Name] = item
}

func mergeCachedOwner(item, existing CachedFileInfo) CachedFileInfo {
	if !item.HasUID && existing.HasUID {
		item.Uid = existing.Uid
		item.HasUID = true
	}
	if !item.HasGID && existing.HasGID {
		item.Gid = existing.Gid
		item.HasGID = true
	}
	return item
}

func (e *dirCacheEntry) remove(name string) {
	delete(e.items, name)
	delete(e.negatives, name)
	for i, existing := range e.order {
		if existing == name {
			e.order = append(e.order[:i], e.order[i+1:]...)
			return
		}
	}
}

func (e *dirCacheEntry) prune(now time.Time) {
	if !e.expires.IsZero() && now.After(e.expires) {
		e.items = make(map[string]CachedFileInfo)
		e.order = nil
		e.expires = time.Time{}
		e.complete = false
	}
	if !e.completeExpires.IsZero() && now.After(e.completeExpires) {
		e.completeExpires = time.Time{}
	}
	if !e.sessionExpires.IsZero() && now.After(e.sessionExpires) {
		e.sessionExpires = time.Time{}
	}
	for name, expires := range e.negatives {
		if now.After(expires) {
			delete(e.negatives, name)
		}
	}
}

func (e *dirCacheEntry) positiveValid(now time.Time) bool {
	return e.expires.IsZero() || !now.After(e.expires)
}

func (e *dirCacheEntry) completeListValid(now time.Time) bool {
	return e.complete && e.positiveValid(now)
}

func (e *dirCacheEntry) completeValid(now time.Time) bool {
	return e.complete && !e.completeExpires.IsZero() && !now.After(e.completeExpires)
}

func (e *dirCacheEntry) sessionValid(now time.Time) bool {
	return !e.sessionExpires.IsZero() && !now.After(e.sessionExpires)
}

func (e *dirCacheEntry) negativeValid(name string, now time.Time) bool {
	expires, ok := e.negatives[name]
	return ok && !now.After(expires)
}

func (e *dirCacheEntry) hasState() bool {
	return len(e.items) > 0 || len(e.negatives) > 0 || e.complete || !e.completeExpires.IsZero() || !e.sessionExpires.IsZero()
}

func cacheParentName(p string) (string, string) {
	dir := path.Dir(p)
	if dir == "." {
		dir = "/"
	}
	return dir, path.Base(p)
}

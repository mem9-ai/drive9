package fuse

import (
	"path"
	"strings"
	"sync"
	"time"
)

const (
	defaultDirCacheTTL              = 10 * time.Second
	defaultNamespaceCacheMaxEntries = 200000

	// escalateMissThreshold/escalateMissWindow detect negative-lookup storms:
	// once this many remote ENOENT stats hit the same directory within the
	// window, the caller should issue one listing so the remaining misses are
	// answered locally instead of one remote stat per name.
	escalateMissThreshold = 3
	escalateMissWindow    = time.Second
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
	ExtentIno  uint64 // JuiceFS inode for content_layout=extent files
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

	// Negative-lookup storm tracking; see RecordRemoteNegative.
	remoteMisses      int
	missWindowStart   time.Time
	escalateNotBefore time.Time

	// localSeq records the namespace sequence at which each child was last
	// recorded locally (Upsert). An installed listing can only speak for the
	// state at its own request time, so a child recorded after that request
	// was issued must survive the install even when the response omits it.
	//
	// removedSeq is the mirror image: the sequence at which a child was
	// removed locally. A listing issued before the removal still contains the
	// name, and installing that response must not resurrect it.
	//
	// Both maps only need to cover entries that can still race an in-flight
	// listing, so installs drop the ones older than their own snapshot and
	// prune drops the maps altogether once seqExpires lapses.
	localSeq   map[string]uint64
	removedSeq map[string]uint64
	seqExpires time.Time
}

// ListingSnapshot marks the local namespace state at the moment a directory
// listing request is issued. Pass it to PutListing when the response arrives:
// children recorded (or removed) after this point cannot be represented by
// that response and are merged in or filtered out instead of being taken
// verbatim.
//
// The zero value means "no baseline" and installs the listing as-is.
type ListingSnapshot struct {
	dir   string
	seq   uint64
	valid bool
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
	// seq is a monotonic local-namespace counter. Every local namespace
	// mutation (Upsert/Remove) takes the next value, which is what lets a
	// listing install distinguish "the response already covers this" from
	// "this happened after the request was issued".
	seq uint64
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
	if !ok || !entry.listingValid(now) {
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

// BeginListing captures the local namespace baseline for a directory listing
// request. Call it immediately before issuing the request and hand the result
// to PutListing when the response arrives, so children recorded while the
// request was in flight are not dropped by the install.
func (dc *DirCache) BeginListing(dirPath string) ListingSnapshot {
	if dc == nil || dirPath == "" {
		return ListingSnapshot{}
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()
	return ListingSnapshot{dir: dirPath, seq: dc.seq, valid: true}
}

// Put stores directory entries in the cache with an expiration of now + ttl.
//
// Prefer BeginListing + PutListing for listing responses: this variant has no
// baseline, so it installs the items verbatim and can drop children recorded
// while the response was in flight.
func (dc *DirCache) Put(dirPath string, items []CachedFileInfo) {
	dc.PutListing(dirPath, items, ListingSnapshot{})
}

// PutListing installs a directory listing response that was issued at the
// namespace baseline in snapshot.
//
// Children recorded locally after that baseline are not present in the
// response (the request predates them) and are carried over instead of being
// dropped; children removed locally after the baseline are filtered out, so a
// stale response cannot resurrect a name the mount already deleted. Everything
// else follows the response verbatim, so remote deletions still take effect.
func (dc *DirCache) PutListing(dirPath string, items []CachedFileInfo, snapshot ListingSnapshot) {
	if dc == nil || dirPath == "" {
		return
	}
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
	hasBaseline := oldEntry != nil && snapshot.valid && snapshot.dir == dirPath
	if hasBaseline {
		// Both record kinds are replayed so the newest local state wins over a
		// response that predates it. Removals run first: noteLocal/noteRemoved
		// keep the two maps mutually exclusive per name, so a name that was
		// removed and then recreated carries only localSeq and is rewritten by
		// the loop below.
		for name, seq := range oldEntry.removedSeq {
			if seq <= snapshot.seq {
				continue
			}
			entry.remove(name)
			entry.noteRemoved(name, seq, now, dc.ttl)
		}
		for name, seq := range oldEntry.localSeq {
			if seq <= snapshot.seq {
				continue
			}
			item, ok := oldEntry.items[name]
			if !ok {
				continue
			}
			// The response either predates this child (it is absent, and
			// dropping it would hide an already-acknowledged file until the
			// listing expires) or carries its pre-mutation entry (a
			// remove-then-recreate). Both lose to the newer local record;
			// upsert keeps any owner/handle info the response contributed.
			entry.upsert(item, dc.maxEntries)
			entry.noteLocal(name, seq, now, dc.ttl)
		}
	}
	if len(items) <= dc.maxEntries {
		entry.complete = true
		entry.completeExpires = now.Add(dc.ttl)
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
	delete(entry.removedSeq, item.Name)
	entry.noteLocal(item.Name, dc.nextSeqLocked(), now, dc.ttl)
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
	entry.noteRemoved(name, dc.nextSeqLocked(), time.Now(), dc.ttl)
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

// HasPositiveEntriesExceptTombstones returns true if the directory has any
// positive cache entries whose child path is not in the tombstones set.
// tombstones is a map of full child paths (dirPath + "/" + name) that have
// been recently deleted. This prevents stale SSE-repopulated dir cache
// entries from causing rmdir to return ENOTEMPTY for recently-unlinked files.
func (dc *DirCache) HasPositiveEntriesExceptTombstones(dirPath string, tombstones map[string]struct{}) bool {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	now := time.Now()
	entry, ok := dc.getEntryLocked(dirPath, now)
	if !ok || !entry.positiveValid(now) {
		return false
	}
	for name := range entry.items {
		childP := dirPath + "/" + name
		if dirPath == "/" {
			childP = "/" + name
		}
		if _, isTombstoned := tombstones[childP]; isTombstoned {
			continue
		}
		return true
	}
	return false
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
// locally managed empty namespace for the directory TTL. Local mutations flow
// through Upsert/Remove and foreign writes invalidate via SSE, so the marker
// can safely outlive the negative TTL used for single-name ENOENT markers.
func (dc *DirCache) MarkSessionCreatedDir(dirPath string) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	now := time.Now()
	entry := dc.ensureEntryLocked(dirPath)
	entry.expires = now.Add(dc.ttl)
	entry.complete = true
	entry.completeExpires = now.Add(dc.ttl)
	entry.sessionExpires = now.Add(dc.ttl)
	entry.negatives = make(map[string]time.Time)
}

// RecordRemoteNegative notes that a remote stat for a child of dirPath
// returned ENOENT. It reports true when the caller should escalate to a
// single directory listing: escalateMissThreshold misses accumulated within
// escalateMissWindow and no escalation cooldown is active.
func (dc *DirCache) RecordRemoteNegative(dirPath string) bool {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	now := time.Now()
	entry := dc.ensureEntryLocked(dirPath)
	if !entry.escalateNotBefore.IsZero() && now.Before(entry.escalateNotBefore) {
		return false
	}
	if entry.missWindowStart.IsZero() || now.Sub(entry.missWindowStart) > escalateMissWindow {
		entry.missWindowStart = now
		entry.remoteMisses = 1
		return false
	}
	entry.remoteMisses++
	if entry.remoteMisses < escalateMissThreshold {
		return false
	}
	entry.remoteMisses = 0
	entry.missWindowStart = time.Time{}
	// Suppress duplicate escalations from concurrent lookups while the
	// caller's listing is in flight. Concurrent misses arriving during that
	// RTT still fall through to individual remote stats; that tail cost is
	// accepted to keep this path free of cross-request coordination.
	entry.escalateNotBefore = now.Add(escalateMissWindow)
	return true
}

// DeferEscalation applies an escalation cooldown of one full cache TTL. Used
// after a listing failed or was too large to answer misses locally, so a
// sustained probe storm cannot turn into repeated listings. This deliberately
// overwrites the short post-escalation cooldown set by RecordRemoteNegative:
// that one only bridges the in-flight listing, while this one says "listing
// this directory does not pay off, stop trying for a while".
func (dc *DirCache) DeferEscalation(dirPath string) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	entry := dc.ensureEntryLocked(dirPath)
	entry.escalateNotBefore = time.Now().Add(dc.ttl)
	entry.remoteMisses = 0
	entry.missWindowStart = time.Time{}
}

// CanAnswerMisses reports whether dirPath currently has namespace state that
// answers child misses locally (a valid complete listing or session marker).
func (dc *DirCache) CanAnswerMisses(dirPath string) bool {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	now := time.Now()
	entry, ok := dc.getEntryLocked(dirPath, now)
	if !ok {
		return false
	}
	return entry.sessionValid(now) || entry.completeValid(now)
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
		items:      make(map[string]CachedFileInfo),
		negatives:  make(map[string]time.Time),
		localSeq:   make(map[string]uint64),
		removedSeq: make(map[string]uint64),
	}
}

// nextSeqLocked returns the next local namespace sequence. Caller holds dc.mu.
func (dc *DirCache) nextSeqLocked() uint64 {
	dc.seq++
	return dc.seq
}

// noteLocal records that name was recorded locally at seq. now is used to
// bound how long the record is kept: it only has to outlive a listing request
// that was already in flight when the child was recorded.
func (e *dirCacheEntry) noteLocal(name string, seq uint64, now time.Time, ttl time.Duration) {
	if seq == 0 {
		return
	}
	if e.localSeq == nil {
		e.localSeq = make(map[string]uint64)
	}
	e.localSeq[name] = seq
	delete(e.removedSeq, name)
	e.seqExpires = now.Add(ttl)
}

// noteRemoved records that name was removed locally at seq.
func (e *dirCacheEntry) noteRemoved(name string, seq uint64, now time.Time, ttl time.Duration) {
	if seq == 0 {
		return
	}
	if e.removedSeq == nil {
		e.removedSeq = make(map[string]uint64)
	}
	e.removedSeq[name] = seq
	delete(e.localSeq, name)
	e.seqExpires = now.Add(ttl)
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
			delete(e.localSeq, evict)
			delete(e.removedSeq, evict)
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
	if item.ExtentIno == 0 {
		item.ExtentIno = existing.ExtentIno
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
		// The carried-over children and removals were only held to protect a
		// listing request that was already in flight; once the entry itself
		// expires there is nothing left for an install to merge against.
		e.localSeq = make(map[string]uint64)
		e.removedSeq = make(map[string]uint64)
		e.seqExpires = time.Time{}
	}
	if !e.seqExpires.IsZero() && now.After(e.seqExpires) {
		// Same bound for the anti-resurrection records when the entry is kept
		// alive by other state (negatives, completeness, session marker).
		e.localSeq = nil
		e.removedSeq = nil
		e.seqExpires = time.Time{}
	}
	if !e.completeExpires.IsZero() && now.After(e.completeExpires) {
		e.completeExpires = time.Time{}
	}
	if !e.sessionExpires.IsZero() && now.After(e.sessionExpires) {
		e.sessionExpires = time.Time{}
	}
	if !e.missWindowStart.IsZero() && now.Sub(e.missWindowStart) > escalateMissWindow {
		e.missWindowStart = time.Time{}
		e.remoteMisses = 0
	}
	if !e.escalateNotBefore.IsZero() && now.After(e.escalateNotBefore) {
		e.escalateNotBefore = time.Time{}
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

// listingValid reports whether the entry can still answer a readdir as a
// complete listing.
//
// This is deliberately keyed on the listing's own expiry (completeExpires),
// not on e.expires: Upsert refreshes e.expires for every locally recorded
// child, so gating on it lets an unrelated commit stream keep a stale listing
// authoritative indefinitely instead of letting it lapse after one TTL.
func (e *dirCacheEntry) listingValid(now time.Time) bool {
	return e.complete && e.completeValid(now)
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
	return len(e.items) > 0 || len(e.negatives) > 0 || e.complete ||
		!e.completeExpires.IsZero() || !e.sessionExpires.IsZero() ||
		!e.missWindowStart.IsZero() || !e.escalateNotBefore.IsZero() ||
		len(e.removedSeq) > 0
}

func cacheParentName(p string) (string, string) {
	dir := path.Dir(p)
	if dir == "." {
		dir = "/"
	}
	return dir, path.Base(p)
}

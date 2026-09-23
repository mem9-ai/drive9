package fuse

import (
	"path"
	"sort"
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

	// gen is this directory's part of the global fencing clock: the value
	// DirCache.gen held when the directory last changed (an install, an
	// authoritative local write, or a local removal). A read that began at an
	// older value is superseded by whatever changed since.
	gen uint64

	// installGen is the request generation of the newest listing install this
	// entry accepted. A response taken before it is stale for every name that
	// install could have carried, so its items are not applied.
	installGen uint64

	// localGen stamps each name this mount wrote or removed with the clock
	// value at that moment. A listing response describes the directory as of
	// when its request was taken, so a response item carrying a stamp newer
	// than the request cannot speak for that name: it is this mount's own,
	// newer state. A name with a stamp and no items entry was removed locally
	// and must not be reinstated by a response.
	//
	// The stamps need no expiry of their own: one entry per name is bounded by
	// the directory's size, and a stale stamp only ever means "this mount
	// changed the name after that older request", which stays true.
	localGen map[string]uint64
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
	// gen is the global fencing clock for remote reads. It only increases;
	// each entry takes a fresh value whenever its directory changes in a way
	// that can invalidate an in-flight read, so a generation a token holds can
	// never recur.
	gen uint64
	// retired remembers the fencing value of directories whose entry was
	// dropped. Without it, retiring an entry would erase the very fact an
	// in-flight read needs: that its directory changed while the read was
	// outstanding. A tombstone is superseded (and dropped) as soon as a fresh
	// entry for the same directory takes a larger value, and it lapses after
	// retiredTTL so a long-running mount cannot accumulate one per path it has
	// ever invalidated.
	retired map[string]retiredDir
}

// retainedTTL bounds how long a retirement is remembered. It only has to cover
// reads that were already in flight when the directory was retired; past that a
// late response can no longer republish anything, because the entry it would
// have installed into is long gone.
const retiredTTL = 2 * time.Minute

// retiredDir is a directory's retirement tombstone: the fencing value in force
// when its entry was dropped, and when that happened.
type retiredDir struct {
	gen uint64
	at  time.Time
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
		retired:     make(map[string]retiredDir),
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

// Put stores directory entries in the cache with an expiration of now + ttl.
func (dc *DirCache) Put(dirPath string, items []CachedFileInfo) {
	dc.PutListing(dirPath, items, dc.Generation(dirPath))
}

// PutListing merges a directory listing response into the cache, given the
// fencing value the request was issued at, and returns the view that request
// must serve.
//
// # Why the merge is additive
//
// A response describes the directory as of the moment its request was taken.
// Between that moment and the install, this mount may have committed new
// children, and a child committed after the request was issued is simply
// absent from the response — not deleted. Since a settled commit has already
// left the pending overlay, nothing downstream can restore such a name, so an
// install that trusted the response's omissions would hide an acknowledged
// file from readdir (issue #966). Names therefore enter the served set through
// upsert and leave it only through an explicit local removal or a whole-entry
// retirement (invalidation, expiry), which gives one structural invariant:
//
//	A name this mount has committed cannot disappear from readdir because of
//	a racing listing.
//
// Remote deletions are not lost by this: they are the directory cache's
// ordinary bounded staleness, covered by the SSE invalidation the server emits
// for structural operations (a delete is a ResetEvent) and, as the fallback,
// by this entry's TTL. That is the contract docs/specs/cache-invalidation.md
// assigns to this cache; prompt removal on install was an extra the response
// cannot justify, because it cannot distinguish a deleted name from one
// committed during its own round trip.
//
// Two per-name rules keep a response from speaking for state it predates, both
// answered by the stamps described on dirCacheEntry.localGen:
//
//   - A name this mount changed after requestGen keeps its local state; the
//     response item does not overwrite it (nor contribute to the view).
//   - A name this mount removed after requestGen stays removed.
func (dc *DirCache) PutListing(dirPath string, items []CachedFileInfo, requestGen uint64) []CachedFileInfo {
	if dc == nil || dirPath == "" {
		return items
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()

	now := time.Now()
	// A response taken before the directory was retired describes state the
	// retirement removed (an SSE reset discards the whole directory). It may
	// answer the request that issued it, but republishing it into the cache
	// would undo the invalidation until the entry lapsed on its own.
	if retired, ok := dc.retiredAtLocked(dirPath, now); ok && requestGen < retired {
		return items
	}
	if old, ok := dc.entries[dirPath]; ok && old != nil {
		old.prune(now)
	}
	entry := dc.ensureEntryLocked(dirPath)
	entry.expires = now.Add(dc.ttl)

	// A lapsed listing cannot serve as the additive base. Its names are as old
	// as its expiry, and an unrelated local write refreshes the entry's own TTL
	// while the listing's has already gone, so an additive merge would replay a
	// remotely deleted name indefinitely — and renew it on every install. Once
	// the listing has lapsed the response is the newest word on which of those
	// names still exist, so the base is reset and only names this mount changed
	// after the request was taken are carried over: the ones the response
	// cannot know about. This is also the escape hatch for a remote deletion
	// that no invalidation reported (see the trade-off in the doc comment).
	if !(entry.complete && !entry.completeExpires.IsZero()) {
		keptItems := make(map[string]CachedFileInfo, len(entry.items))
		keptOrder := make([]string, 0, len(entry.items))
		for _, name := range entry.order {
			item, live := entry.items[name]
			if !live {
				continue
			}
			if stamp := entry.localGen[name]; stamp > requestGen {
				keptItems[name] = item
				keptOrder = append(keptOrder, name)
			}
		}
		entry.items = keptItems
		entry.order = keptOrder
	}

	// The cache retains a bounded prefix of the response (the pre-existing
	// large-directory guard: an over-cap listing answers positive lookups from
	// its prefix and reports the rest as partial misses), but callers receive
	// every response item. Truncating the reply would hide names past the cap
	// from this readdir and make the lookup fallback cache a false ENOENT.
	limit := len(items)
	if limit > dc.maxEntries {
		limit = dc.maxEntries
	}
	// A response taken before another install cannot overwrite what that
	// install published: listings overlap, so the older request's response may
	// describe each name at an older revision than the cache already has. Its
	// cache effect is limited to names it is still the newest word on, and the
	// caller is served the accepted state either way.
	staleInstall := entry.installGen > requestGen

	served := make(map[string]CachedFileInfo, len(items)+len(entry.items))
	complete := len(items) <= dc.maxEntries
	for i := 0; i < len(items); i++ {
		item := items[i]
		if stamp, changed := entry.localGen[item.Name]; changed && stamp > requestGen {
			// This mount wrote or removed the name after the request was
			// taken; the response cannot speak for it.
			if local, live := entry.items[item.Name]; live {
				served[item.Name] = local
			}
			continue
		}
		if staleInstall {
			// Keep whatever the newer install published for this name.
			if current, live := entry.items[item.Name]; live {
				served[item.Name] = current
			}
			continue
		}
		if i < limit {
			if entry.upsert(item, dc.maxEntries) {
				// A name was displaced to make room, so the cache no longer
				// holds the whole listing.
				complete = false
			}
		}
		served[item.Name] = item
	}
	for name, item := range entry.items {
		if _, fromResponse := served[name]; !fromResponse {
			served[name] = item
		}
	}
	// A listing is only complete when the cache holds all of it: a response
	// larger than the cap is served in full but cached in part, and a complete
	// miss for a name the cache never held is a false ENOENT.
	if complete {
		entry.complete = true
		entry.completeExpires = now.Add(dc.ttl)
	}
	entry.gen = dc.nextGenerationLocked()
	if !staleInstall {
		// This install is now the newest word on the directory's names.
		entry.installGen = requestGen
	}

	// Emit in the cache's order so the reply matches what a later readdir would
	// serve, then append the rest deterministically.
	out := make([]CachedFileInfo, 0, len(served))
	for _, name := range entry.order {
		item, ok := served[name]
		if !ok {
			continue
		}
		out = append(out, item)
		delete(served, name)
	}
	if len(served) > 0 {
		names := make([]string, 0, len(served))
		for name := range served {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			out = append(out, served[name])
		}
	}
	return out
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

// ObservationToken marks the cache state at the moment a remote read is
// issued. Hand it to Observe when the read completes so a response that is
// merely late cannot republish state a newer event superseded.
//
// The fence is the directory's generation (see dirCacheEntry.gen), not the
// revision: a revision only orders content inside one resource identity and is
// not advanced by every visible metadata change, so ranking a read against a
// listing by revision is wrong in both directions — it ranks a recreated
// lower-revision object below the deleted one it replaced, and it rejects a
// same-revision observation that carries newer metadata.
//
// The zero value carries no ordering information, so Observe discards it.
type ObservationToken struct {
	dir   string
	gen   uint64
	valid bool
}

// BeginObservation captures the token for a remote read of one child under
// dirPath. Call it immediately before issuing the request.
func (dc *DirCache) BeginObservation(dirPath string) ObservationToken {
	if dc == nil || dirPath == "" {
		return ObservationToken{}
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()
	return ObservationToken{dir: dirPath, gen: dc.generationLocked(dirPath), valid: true}
}

// Observe records a child entry learned from a remote read (a stat/HEAD
// refresh, for example), paired with the token for that read.
//
// Unlike Upsert this is not an authoritative local mutation: completion order
// is not a version order, so an observation may not resurrect a name a local
// removal took away, and it may not overwrite what a newer event published.
// Within those limits it does refresh metadata, which is what carries a change
// the revision cannot express, such as a remote chmod.
//
// The token decides whether the observation can still be applied. It can only
// when nothing has changed the directory since the read began, which the
// generation answers in one comparison:
//
//   - entry present, generation unchanged   -> apply
//   - entry present, generation changed     -> discard (a newer event won)
//   - entry retired since (absent, was cached) -> discard
//   - entry absent and none was cached      -> apply: no event has touched this
//     directory, so the read is the freshest information there is. A later
//     creation always takes a fresh generation, so this cannot race one.
func (dc *DirCache) Observe(parentPath string, item CachedFileInfo, token ObservationToken) {
	if item.Name == "" || dc == nil {
		return
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()

	if !token.valid || token.dir != parentPath {
		return
	}
	// The directory must still be at the value the read began against. A
	// retirement advances that value too, so a read outstanding across an
	// invalidation is discarded rather than allowed to recreate the directory
	// from stale knowledge.
	if dc.generationLocked(parentPath) != token.gen {
		// The read still proves the name exists, which is all an ENOENT marker
		// denies.
		if entry, ok := dc.entries[parentPath]; ok && entry != nil {
			delete(entry.negatives, item.Name)
		}
		return
	}
	entry := dc.ensureEntryLocked(parentPath)
	entry.expires = time.Now().Add(dc.ttl)
	entry.upsert(item, dc.maxEntries)
	delete(entry.negatives, item.Name)
	// An observation is still this mount's knowledge of the name, stamped now
	// so a response taken before it cannot overwrite it.
	stamp := dc.nextGenerationLocked()
	entry.gen = stamp
	entry.localGen[item.Name] = stamp
}

// Upsert records or refreshes a known child entry for a parent.
//
// This is the authoritative entry point: it means *this mount* established the
// child's current state (a commit settled, a namespace object was created or
// renamed, a local overlay resolved). It adds or refreshes the name and
// advances the directory's generation, so any remote read in flight for this
// directory can no longer overwrite what it published.
//
// For a child learned from a plain remote read, use Observe instead: a read is
// a point-in-time observation, not a mutation.
func (dc *DirCache) Upsert(parentPath string, item CachedFileInfo) {
	dc.upsertInternal(parentPath, item)
}

// upsertInternal stores a child entry and advances the fencing generation.
func (dc *DirCache) upsertInternal(parentPath string, item CachedFileInfo) {
	if item.Name == "" {
		return
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()

	entry := dc.ensureEntryLocked(parentPath)
	entry.expires = time.Now().Add(dc.ttl)
	entry.upsert(item, dc.maxEntries)
	delete(entry.negatives, item.Name)
	stamp := dc.nextGenerationLocked()
	entry.gen = stamp
	entry.localGen[item.Name] = stamp
}

// Remove deletes a known child entry from a parent without implying the parent
// namespace is complete.
//
// The entry is created when absent even though there is nothing to delete: the
// generation this advances is what stops a remote read that began before the
// removal from republishing the removed name.
func (dc *DirCache) Remove(parentPath, name string) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	entry := dc.ensureEntryLocked(parentPath)
	entry.remove(name)
	stamp := dc.nextGenerationLocked()
	entry.gen = stamp
	entry.localGen[name] = stamp
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
		// Retire every entry rather than replacing the map: entries with
		// listings or observations still in flight have to keep their
		// reconciliation state (see invalidateEntryLocked).
		for p := range dc.entries {
			dc.invalidateEntryLocked(p)
		}
		return
	}
	prefix := dirPath
	prefix += "/"
	for p := range dc.entries {
		if p == dirPath || strings.HasPrefix(p, prefix) {
			dc.invalidateEntryLocked(p)
		}
	}
}

// Invalidate removes a specific directory entry from the cache.
func (dc *DirCache) Invalidate(dirPath string) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	dc.invalidateEntryLocked(dirPath)
}

// InvalidateAll clears all cache entries.
func (dc *DirCache) InvalidateAll() {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	// Retire each entry rather than replacing the map: entries with listings
	// still in flight have to keep their reconciliation state.
	for dirPath := range dc.entries {
		dc.invalidateEntryLocked(dirPath)
	}
}

// invalidateEntryLocked drops a directory's cached namespace state.
//
// An entry with listings still in flight is kept rather than deleted: those
// requests hold a flight registered on it and will replay the reconciliation
// records it carries. Deleting the entry would strand both — the flights would
// Only what the cache serves is dropped: an entry that is retired simply goes
// away, and a later read for it starts from a fresh generation (see
// ensureEntryLocked), so nothing that was in flight for the old entry can
// republish into the new one. Caller holds dc.mu.
func (dc *DirCache) invalidateEntryLocked(dirPath string) {
	// Retiring an entry must advance the directory's fencing value, not erase
	// it: a read that began against the retired entry has to be able to tell
	// that the directory changed while it was outstanding.
	delete(dc.entries, dirPath)
	if dc.retired == nil {
		dc.retired = make(map[string]retiredDir)
	}
	dc.retired[dirPath] = retiredDir{gen: dc.nextGenerationLocked(), at: time.Now()}
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
		// A created entry takes a fresh generation. The clock only increases, so
		// a recreated entry can never reuse the value a retired entry held, and
		// the fresh value supersedes any retirement tombstone: nothing that was
		// in flight for the retired entry can match it.
		entry.gen = dc.nextGenerationLocked()
		delete(dc.retired, dirPath)
		dc.entries[dirPath] = entry
	}
	if entry.items == nil {
		entry.items = make(map[string]CachedFileInfo)
	}
	if entry.negatives == nil {
		entry.negatives = make(map[string]time.Time)
	}
	if entry.localGen == nil {
		entry.localGen = make(map[string]uint64)
	}
	return entry
}

// generationLocked is the directory's effective fencing value: the live
// entry's, or the value it was retired at. Caller holds dc.mu.
func (dc *DirCache) generationLocked(dirPath string) uint64 {
	return dc.generationAtLocked(dirPath, time.Now())
}

// generationAtLocked is generationLocked against an explicit clock, so callers
// that already hold a timestamp do not pay for a second one. An expired
// tombstone is dropped and no longer participates. Caller holds dc.mu.
func (dc *DirCache) generationAtLocked(dirPath string, now time.Time) uint64 {
	gen := uint64(0)
	if tomb, ok := dc.retired[dirPath]; ok {
		if now.Sub(tomb.at) > retiredTTL {
			delete(dc.retired, dirPath)
		} else {
			gen = tomb.gen
		}
	}
	if entry, ok := dc.entries[dirPath]; ok && entry != nil && entry.gen > gen {
		gen = entry.gen
	}
	return gen
}

// retiredAtLocked reports the value a directory was retired at, if that
// retirement is still in force. Caller holds dc.mu.
func (dc *DirCache) retiredAtLocked(dirPath string, now time.Time) (uint64, bool) {
	tomb, ok := dc.retired[dirPath]
	if !ok || now.Sub(tomb.at) > retiredTTL {
		return 0, false
	}
	return tomb.gen, true
}

// servedOrder returns mapping keys in a deterministic order, so a cache rebuild
// produces a stable readdir order regardless of Go's map iteration.
func servedOrder(served map[string]CachedFileInfo) []string {
	names := make([]string, 0, len(served))
	for name := range served {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// nextGenerationLocked advances the fencing clock. Caller holds dc.mu.
func (dc *DirCache) nextGenerationLocked() uint64 {
	dc.gen++
	return dc.gen
}

// Generation hands a caller about to issue a remote read a value identifying
// that request. Hand it back on install (PutListing) or completion (Observe) so
// the cache can tell whether anything has overtaken it.
//
// The value comes from the clock and advances it, so every call is unique and
// later calls sort after earlier ones. That uniqueness is what orders two
// overlapping requests against each other: reporting the directory's
// last-event value instead would give two requests issued between the same pair
// of events the same value, and an older response could then be mistaken for
// the newer one.
func (dc *DirCache) Generation(dirPath string) uint64 {
	if dc == nil || dirPath == "" {
		return 0
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()
	return dc.nextGenerationLocked()
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

// upsert stores a child entry, reporting whether the entry had to displace
// another name to make room (the caller uses that to avoid claiming the
// listing is complete).
//
// At the cap the eviction scans for a name with no local stamp: a name this
// mount committed is exactly what the listing race protection exists to keep
// visible, so it must not be the one dropped to make room for a name the
// server can simply list again. If every name is locally owned, the incoming
// item is not cached at all.
func (e *dirCacheEntry) upsert(item CachedFileInfo, maxEntries int) (evicted bool) {
	if item.Name == "" {
		return false
	}
	if e.items == nil {
		e.items = make(map[string]CachedFileInfo)
	}
	if existing, ok := e.items[item.Name]; ok {
		item = mergeCachedOwner(item, existing)
	}
	if _, exists := e.items[item.Name]; !exists {
		if maxEntries > 0 && len(e.items) >= maxEntries {
			victim := -1
			for i, name := range e.order {
				if _, local := e.localGen[name]; !local {
					victim = i
					break
				}
			}
			if victim < 0 {
				// Every cached name is locally owned; keep them and leave the
				// incoming name uncached.
				return true
			}
			evict := e.order[victim]
			e.order = append(e.order[:victim], e.order[victim+1:]...)
			delete(e.items, evict)
			delete(e.negatives, evict)
			e.complete = false
			e.completeExpires = time.Time{}
			e.sessionExpires = time.Time{}
			evicted = true
		}
		e.order = append(e.order, item.Name)
	}
	e.items[item.Name] = item
	return evicted
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
		// localGen is deliberately kept: a stamp only ever means "this mount
		// changed the name at that point", which stays true, and dropping it
		// would let a response taken before the change reinstate a removal.
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

// completeValid reports whether the installed listing is still within its own
// TTL and may therefore answer misses as a complete directory.
func (e *dirCacheEntry) completeValid(now time.Time) bool {
	return e.complete && !e.completeExpires.IsZero() && !now.After(e.completeExpires)
}

// sessionValid reports whether a session-created-directory marker is still
// active, which lets misses be answered locally for a directory this mount
// created and has been managing.
func (e *dirCacheEntry) sessionValid(now time.Time) bool {
	return !e.sessionExpires.IsZero() && !now.After(e.sessionExpires)
}

// negativeValid reports whether name still has an unexpired explicit
// ENOENT marker.
func (e *dirCacheEntry) negativeValid(name string, now time.Time) bool {
	expires, ok := e.negatives[name]
	return ok && !now.After(expires)
}

// hasState reports whether the entry still holds anything worth keeping. An
// entry that answers false is dropped from the cache on the next lookup.
func (e *dirCacheEntry) hasState() bool {
	return len(e.items) > 0 || len(e.negatives) > 0 || e.complete ||
		!e.completeExpires.IsZero() || !e.sessionExpires.IsZero() ||
		!e.missWindowStart.IsZero() || !e.escalateNotBefore.IsZero() ||
		// Session markers keep an entry alive so a mount-created directory can
		// answer misses; see MarkSessionCreatedDir.
		!e.sessionExpires.IsZero() ||
		// A removal leaves only its stamp behind, and that stamp is what keeps
		// a response predating the removal from reinstating the name.
		len(e.localGen) > 0
}

func cacheParentName(p string) (string, string) {
	dir := path.Dir(p)
	if dir == "." {
		dir = "/"
	}
	return dir, path.Base(p)
}

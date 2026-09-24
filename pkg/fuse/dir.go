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

	// listingExpires is the deadline of the newest installed listing's
	// response-derived state, complete or not. Unlike expires it is never
	// refreshed by a local mutation, so an unrelated commit stream cannot keep a
	// response's names alive past one TTL; and unlike completeExpires the
	// install sets it whether or not the whole listing fits in the cache, so a
	// directory too large to cache in full is bounded here too. A past deadline
	// is meaningful — it marks the response-derived state as lapsed — so prune
	// leaves it alone.
	listingExpires time.Time

	// fromListing holds the names whose currently cached value came from a
	// listing response, and is always a subset of items. It is what makes an
	// otherwise stale omission evidence: for such a name the response that
	// supplied it is the only thing vouching for it, so a newer response
	// omitting it, or this listing's own deadline passing, retires it. A name
	// this mount wrote or observed never enters the set — that state stays
	// authoritative even while the server has not caught up with it, and
	// dropping it is the readdir invisibility this cache exists to prevent
	// (issue #966).
	//
	// The map is allocated only when a response actually installs a name, so a
	// directory this mount only ever writes into carries no extra allocation.
	fromListing map[string]struct{}

	// gen is this directory's part of the global fencing clock: the value
	// DirCache.gen held when the directory last changed (an install, an
	// authoritative local write, or a local removal). A read that began at an
	// older value is superseded by whatever changed since.
	gen uint64

	// installSeq is the request sequence of the newest listing install this
	// entry accepted. A response whose request predates it is stale for every
	// name that install could have carried, so its items are not applied.
	installSeq uint64

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
	// inFlight counts uncompleted remote reads per directory. It is the
	// lifetime of every piece of reconciliation state below: a per-name stamp or
	// a retirement watermark only matters to a request that was already issued
	// when it was written, so once no request is outstanding for a directory
	// nothing can still observe them and they are released. That bound is the
	// mount's concurrency, so none of this grows with the number of directories
	// a mount has ever touched.
	inFlight map[string]int
	// retired is the retirement watermark for directories that have a request
	// in flight, recording that the directory changed out from under it. It
	// exists only for those directories: an invalidation with nothing
	// outstanding has no reader to fence, so it records nothing.
	retired map[string]uint64
}

// requestRef is shared by a request token and the cache so a token can be
// released more than once (an install plus a deferred release, say) without
// double-counting.
type requestRef struct {
	released bool
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
		inFlight:    make(map[string]int),
		retired:     make(map[string]uint64),
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
	// A caller with no request of its own still installs under a registered
	// request, so the token's lifetime rules apply uniformly.
	request := dc.BeginRequest(dirPath)
	dc.PutListing(dirPath, items, request)
	dc.EndRequest(request)
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
// upsert and leave it only through an explicit local removal, a whole-entry
// retirement (invalidation, expiry), or a name whose only evidence — a
// response (see dirCacheEntry.fromListing) — has since been superseded, which
// gives one structural invariant:
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
func (dc *DirCache) PutListing(dirPath string, items []CachedFileInfo, request RequestToken) []CachedFileInfo {
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
	if retired := dc.retired[dirPath]; request.seq < retired {
		// The request is settled (its response is served) even though nothing is
		// installed, so it releases like any other completion.
		dc.releaseRequestLocked(request)
		return items
	}
	if old, ok := dc.entries[dirPath]; ok && old != nil {
		old.prune(now)
	}
	entry := dc.ensureEntryLocked(dirPath)
	entry.expires = now.Add(dc.ttl)

	// A response taken before another install cannot overwrite what that
	// install published: listings overlap, so the older request's response may
	// describe each name at an older revision than the cache already has. Its
	// cache effect is limited to names it is still the newest word on, and the
	// caller is served the accepted state either way. A stale install also
	// neither renews nor retires anything: both belong to the newer install.
	staleInstall := entry.installSeq > request.seq

	// The cache retains a bounded prefix of the response (the pre-existing
	// large-directory guard: an over-cap listing answers positive lookups from
	// its prefix and reports the rest as partial misses), but callers receive
	// every response item. Truncating the reply would hide names past the cap
	// from this readdir and make the lookup fallback cache a false ENOENT.
	limit := len(items)
	if limit > dc.maxEntries {
		limit = dc.maxEntries
	}

	// Names the cache held before this install. They are served to this request
	// even if the merge has to evict one for room: the reply must not omit a
	// name the cache was holding, and the pending overlay cannot restore a
	// committed child the way it restores an in-flight one.
	prior := make(map[string]CachedFileInfo, len(entry.items))
	for name, item := range entry.items {
		prior[name] = item
	}
	served := make(map[string]CachedFileInfo, len(items)+len(prior))
	complete := len(items) <= dc.maxEntries
	for i := 0; i < len(items); i++ {
		item := items[i]
		if stamp, changed := entry.localGen[item.Name]; changed && stamp > request.event {
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
			// The name's cached value is now the response's, so only a newer
			// response (or this listing's deadline) can vouch for it again.
			entry.markFromListing(item.Name)
		}
		served[item.Name] = item
	}
	for name, item := range prior {
		if _, already := served[name]; !already {
			served[name] = item
		}
	}
	for name, item := range entry.items {
		if _, already := served[name]; !already {
			served[name] = item
		}
	}
	// A listing is only complete when the cache holds all of it: a response
	// larger than the cap is served in full but cached in part, and a complete
	// miss for a name the cache never held is a false ENOENT. Clearing is
	// explicit rather than merely skipping the assignment below, because the
	// entry may hold completeness from an earlier, smaller listing.
	if complete {
		entry.complete = true
		entry.completeExpires = now.Add(dc.ttl)
	} else {
		entry.complete = false
		entry.completeExpires = time.Time{}
	}
	entry.gen = dc.nextGenerationLocked()
	if !staleInstall {
		// This install is now the newest word on the directory's names, and
		// what it published lapses one TTL from now.
		entry.installSeq = request.seq
		entry.listingExpires = now.Add(dc.ttl)
	}
	// Releasing here is what retires this request's stamps once it was the last
	// one outstanding.
	dc.releaseRequestLocked(request)

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
type RequestToken struct {
	dir string
	// seq is unique per request and increasing, so two requests can be ordered
	// against each other even when nothing else happened between them.
	seq uint64
	// event is the directory's fencing value when the request was issued. An
	// observation applies only while it is unchanged: unlike seq this is
	// advanced by events (installs, local writes, removals, retirements) and
	// not by other requests merely starting, so concurrent reads of one
	// directory do not supersede each other.
	event uint64
	ref   *requestRef
	valid bool
}

// BeginRequest registers a remote read of dirPath and returns the token that
// identifies it. Call it immediately before issuing the request and release the
// token with EndRequest when the request settles (installing a listing releases
// it implicitly).
//
// Registration is what bounds the cache's reconciliation state: the per-name
// stamps and the directory's retirement watermark are only meaningful to a read
// that was already outstanding when they were written, so the last completing
// request for a directory releases them. Nothing accumulates per path a mount
// has merely invalidated or probed.
func (dc *DirCache) BeginRequest(dirPath string) RequestToken {
	if dc == nil || dirPath == "" {
		return RequestToken{}
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()
	if dc.inFlight == nil {
		dc.inFlight = make(map[string]int)
	}
	dc.inFlight[dirPath]++
	return RequestToken{
		dir:   dirPath,
		seq:   dc.nextGenerationLocked(),
		event: dc.generationLocked(dirPath),
		ref:   &requestRef{},
		valid: true,
	}
}

// EndRequest releases a token whose request produced no install. Safe to call
// more than once and after an install, which releases the same token.
func (dc *DirCache) EndRequest(token RequestToken) {
	if dc == nil || !token.valid {
		return
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()
	dc.releaseRequestLocked(token)
}

// releaseRequestLocked retires one outstanding read and, when it was the last
// for that directory, reclaims the reconciliation state only it could observe.
// Caller holds dc.mu.
func (dc *DirCache) releaseRequestLocked(token RequestToken) {
	if !token.valid || token.ref == nil || token.ref.released {
		return
	}
	token.ref.released = true
	if dc.inFlight != nil {
		if n := dc.inFlight[token.dir]; n > 1 {
			dc.inFlight[token.dir] = n - 1
			return
		}
		delete(dc.inFlight, token.dir)
	}
	// Nothing is outstanding for this directory, so no response can still be
	// waiting to consult its stamps or watermark.
	delete(dc.retired, token.dir)
	if entry, ok := dc.entries[token.dir]; ok && entry != nil {
		entry.reclaimReconciliationLocked()
	}
}

// noteLocalLocked records that this mount changed name in dirPath, so a response
// from a request that was already in flight for that directory cannot speak for
// it, and advances the directory's fencing value either way. Caller holds dc.mu.
//
// The stamp is written only while such a request exists. Its only reader is one
// of those requests: with none outstanding there is nothing to defend the name
// from, because the next request to start is newer than this change by
// construction. That conditional is what keeps the map bounded by concurrency
// rather than by every name the directory has ever held — and it matters most at
// the hard cap, where names the cache refuses to store would otherwise leave a
// stamp each (a 1000-name burst at cap 4 left 1000 of them).
func (dc *DirCache) noteLocalLocked(entry *dirCacheEntry, dirPath, name string) {
	stamp := dc.nextGenerationLocked()
	entry.gen = stamp
	if dc.inFlight[dirPath] > 0 {
		entry.localGen[name] = stamp
		return
	}
	// Nothing can consult a stamp for this directory, so any earlier ones are
	// unreadable too: reclaiming here is what gives a mutation with no request
	// in flight a reclaim trigger at all.
	entry.reclaimReconciliationLocked()
}

// reclaimReconciliationLocked drops per-name state that only an outstanding
// request could have needed. Caller holds dc.mu.
func (e *dirCacheEntry) reclaimReconciliationLocked() {
	if e == nil {
		return
	}
	e.localGen = make(map[string]uint64)
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
func (dc *DirCache) Observe(parentPath string, item CachedFileInfo, token RequestToken) {
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
	if dc.generationLocked(parentPath) != token.event {
		// The read still proves the name exists, which is all an ENOENT marker
		// denies.
		if entry, ok := dc.entries[parentPath]; ok && entry != nil {
			delete(entry.negatives, item.Name)
		}
		dc.releaseRequestLocked(token)
		return
	}
	entry := dc.ensureEntryLocked(parentPath)
	entry.expires = time.Now().Add(dc.ttl)
	entry.upsert(item, dc.maxEntries)
	delete(entry.negatives, item.Name)
	// An observation is this mount's knowledge of the name, so it owns the value
	// just like a local write does: a listing deadline passing may not retire it.
	delete(entry.fromListing, item.Name)
	// An observation is still this mount's knowledge of the name, so a response
	// taken before it must not overwrite it. Both the stamp and this request are
	// released together, so neither outlives the readers that could consult it.
	dc.noteLocalLocked(entry, parentPath, item.Name)
	dc.releaseRequestLocked(token)
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
	// The name is this mount's own now: a later listing omitting it (a commit
	// the server has not listed yet) or a listing deadline passing must not
	// retire state the mount established itself.
	delete(entry.fromListing, item.Name)
	dc.noteLocalLocked(entry, parentPath, item.Name)
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
	dc.noteLocalLocked(entry, parentPath, name)
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
	// A retirement only has to fence reads that were already outstanding. With
	// none, there is nothing to remember: recording one per invalidated path
	// would grow with the mount's history rather than its concurrency.
	if dc.inFlight != nil && dc.inFlight[dirPath] > 0 {
		dc.retireLocked(dirPath)
	}
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
		// A created entry takes a fresh generation from the global clock, which
		// only increases, so it is necessarily ahead of any retirement watermark
		// already recorded for this directory: a response taken before that
		// retirement stays older than the entry it would install into.
		//
		// The watermark is deliberately NOT cleared here. Clearing it was an
		// earlier version of this fix, and it reopened the very window it was
		// meant to close, because any local write recreates the entry and the
		// next pre-retirement response is then installed as though the
		// retirement had never happened.
		entry.gen = dc.nextGenerationLocked()
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
// entry's, or the watermark it was retired at. Caller holds dc.mu.
func (dc *DirCache) generationLocked(dirPath string) uint64 {
	gen := dc.retired[dirPath]
	if entry, ok := dc.entries[dirPath]; ok && entry != nil && entry.gen > gen {
		gen = entry.gen
	}
	return gen
}

// retireLocked records that a directory changed out from under any read in
// flight, and returns the watermark. Caller holds dc.mu.
func (dc *DirCache) retireLocked(dirPath string) uint64 {
	gen := dc.nextGenerationLocked()
	if dc.retired == nil {
		dc.retired = make(map[string]uint64)
	}
	dc.retired[dirPath] = gen
	return gen
}

// nextGenerationLocked advances the fencing clock. Caller holds dc.mu.
func (dc *DirCache) nextGenerationLocked() uint64 {
	dc.gen++
	return dc.gen
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
// Eviction is FIFO and maxEntries is a hard bound even when the incoming name
// is one this mount just committed: a directory larger than the cap cannot be
// held in full, so a committed name evicted here is protected by the revoked
// complete/session markers — misses go back to the server instead of being
// answered from a cache that no longer holds every name — not by growing
// items past the caller-configured limit.
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
			if len(e.order) == 0 {
				return true
			}
			evict := e.order[0]
			e.order = e.order[1:]
			delete(e.items, evict)
			delete(e.negatives, evict)
			delete(e.fromListing, evict)
			evicted = true
			// The entry cannot hold the whole directory any more, so it must
			// stop speaking for the names it could not keep: a surviving
			// complete or session marker would answer a lookup for one of them
			// as "absent", which is a false ENOENT for a name the server has.
			// Revoking it sends such a lookup to the server instead.
			//
			// maxEntries is a hard bound even for authoritative writes: growing
			// past a caller-configured limit is not this cache's decision, and
			// the served view below keeps the evicted name visible to the
			// request that is being answered.
			e.complete = false
			e.completeExpires = time.Time{}
			e.sessionExpires = time.Time{}
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
	delete(e.fromListing, name)
	for i, existing := range e.order {
		if existing == name {
			e.order = append(e.order[:i], e.order[i+1:]...)
			return
		}
	}
}

// markFromListing records that name's cached value is the word of a listing
// response, so that a lapsed listing gives it up. Only a name the cache holds
// can carry provenance. Caller holds dc.mu.
func (e *dirCacheEntry) markFromListing(name string) {
	if _, live := e.items[name]; !live {
		return
	}
	if e.fromListing == nil {
		e.fromListing = make(map[string]struct{})
	}
	e.fromListing[name] = struct{}{}
}

func (e *dirCacheEntry) prune(now time.Time) {
	if !e.expires.IsZero() && now.After(e.expires) {
		e.items = make(map[string]CachedFileInfo)
		e.order = nil
		e.expires = time.Time{}
		e.complete = false
		// Every name went with the items, so there is nothing left for
		// provenance to single out.
		e.fromListing = nil
		// localGen is deliberately kept: a stamp only ever means "this mount
		// changed the name at that point", which stays true, and dropping it
		// would let a response taken before the change reinstate a removal.
	}
	// A listing deadline that has gone retires the names only it vouched for.
	// They are the response's word, so once that word is stale the next response
	// is a newer word on which of them still exist, and a deletion it reflects
	// has to take effect — otherwise a name the server no longer has survives as
	// long as any unrelated local write keeps refreshing the entry's own TTL.
	// Names this mount wrote itself are not in the set and are untouched: the
	// server may not have caught up with them yet.
	//
	// This is keyed on the listing's own deadline rather than on the complete
	// marker, because a response larger than the cache cap never sets that
	// marker and its cached prefix would otherwise never lapse.
	if !e.listingExpires.IsZero() && now.After(e.listingExpires) {
		dropped := false
		for name := range e.fromListing {
			e.remove(name)
			dropped = true
		}
		e.fromListing = nil
		if dropped {
			// Names just left, so the cache can no longer claim to hold the
			// whole directory: a miss for one of them must go to the server
			// instead of being answered from a set that dropped it.
			e.complete = false
			e.completeExpires = time.Time{}
		}
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

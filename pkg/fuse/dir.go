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

	// Reconciliation state, kept across listing installs.
	//
	// localSeq records the namespace sequence at which each child was last
	// recorded by an authoritative local mutation (Upsert). An installed
	// listing can only speak for the state at its own request time, so a child
	// recorded after that request was issued must survive the install even
	// when the response omits it. localItem holds that child's entry, kept
	// independently of the served items so the payload outlives an install
	// that legitimately drops it.
	//
	// removedSeq is the mirror image: the sequence at which a child was
	// removed locally. A listing issued before the removal still contains the
	// name, and installing that response must not resurrect it.
	//
	// outstanding counts in-flight listing requests per snapshot sequence. A
	// record only matters to a listing issued before it, so it is retained
	// until every outstanding snapshot postdates it. This is deliberately not
	// a time bound: a listing request has no client-side deadline, so no TTL
	// can promise to outlive one.
	localSeq    map[string]uint64
	localItem   map[string]CachedFileInfo
	removedSeq  map[string]uint64
	outstanding map[uint64]int

	// installSeq is dc.installs' value at this directory's last listing
	// install. It is the ordering primitive for remote observations: an
	// observation only installs while no listing has been installed since the
	// observation began.
	installSeq uint64
}

// listingFlight is the release handle for one in-flight listing request. The
// caller's snapshot and the cache share it, so releasing it twice (an install
// plus a deferred EndListing, say) is a no-op rather than a double decrement.
type listingFlight struct {
	dir      string
	seq      uint64
	released bool
}

// ListingSnapshot marks the local namespace state at the moment a directory
// listing request is issued. Pass it to PutListing when the response arrives:
// children recorded (or removed) after this point cannot be represented by
// that response and are merged in or filtered out instead of being taken
// verbatim. If the response will never be installed, release the snapshot
// with EndListing so the records it still needs can be reclaimed.
//
// The zero value means "no baseline" and installs the listing as-is.
type ListingSnapshot struct {
	dir    string
	seq    uint64
	flight *listingFlight
	valid  bool
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
	// installs counts listing installs. An observation captured against an
	// older value is superseded by whatever the install published, so it must
	// not be applied on top of it.
	installs uint64
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
//
// If the request never produces an install (it failed, or the caller bailed
// out), call EndListing with the returned snapshot so the reconciliation
// records it holds can be reclaimed.
func (dc *DirCache) BeginListing(dirPath string) ListingSnapshot {
	if dc == nil || dirPath == "" {
		return ListingSnapshot{}
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()
	flight := &listingFlight{dir: dirPath, seq: dc.seq}
	entry := dc.ensureEntryLocked(dirPath)
	if entry.outstanding == nil {
		entry.outstanding = make(map[uint64]int)
	}
	entry.outstanding[flight.seq]++
	return ListingSnapshot{dir: dirPath, seq: flight.seq, flight: flight, valid: true}
}

// EndListing releases an in-flight listing snapshot that will not be
// installed. Installing through PutListing releases it automatically.
func (dc *DirCache) EndListing(snapshot ListingSnapshot) {
	if dc == nil || !snapshot.valid || snapshot.flight == nil {
		return
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()
	dc.releaseListingLocked(snapshot.flight)
}

// releaseListingLocked retires one in-flight snapshot and reclaims records no
// outstanding listing can still observe. Caller holds dc.mu.
func (dc *DirCache) releaseListingLocked(flight *listingFlight) {
	if flight == nil || flight.released {
		return
	}
	flight.released = true
	entry, ok := dc.entries[flight.dir]
	if !ok {
		return
	}
	if entry.outstanding != nil {
		if n := entry.outstanding[flight.seq]; n > 1 {
			entry.outstanding[flight.seq] = n - 1
		} else {
			delete(entry.outstanding, flight.seq)
		}
	}
	entry.reclaimRecordsLocked()
}

// reclaimRecordsLocked drops reconciliation records that no outstanding
// listing can still observe. A record is dead once every in-flight snapshot
// postdates it (or none remain). Caller holds dc.mu.
func (e *dirCacheEntry) reclaimRecordsLocked() {
	if e == nil {
		return
	}
	if len(e.outstanding) == 0 {
		e.localSeq = nil
		e.localItem = nil
		e.removedSeq = nil
		return
	}
	var oldest uint64
	first := true
	for seq := range e.outstanding {
		if first || seq < oldest {
			oldest = seq
			first = false
		}
	}
	for name, seq := range e.localSeq {
		if seq <= oldest {
			delete(e.localSeq, name)
			delete(e.localItem, name)
		}
	}
	for name, seq := range e.removedSeq {
		if seq <= oldest {
			delete(e.removedSeq, name)
		}
	}
	if len(e.localSeq) == 0 {
		e.localItem = nil
	}
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
// namespace baseline in snapshot, and returns the reconciled view callers
// must serve from together with whether that view is authoritative.
//
// Children recorded by an authoritative local mutation after that baseline are
// not present in the response (the request predates them) and are carried over
// instead of being dropped; children removed locally after the baseline are
// filtered out, so a stale response cannot resurrect a name the mount already
// deleted. Everything else follows the response verbatim, so remote deletions
// still take effect.
//
// The returned slice is the response plus those amendments, so it is the view
// this request must serve. The cache is only what a *later* readdir will see;
// a caller that serves the raw response still hides a child that committed
// during its RTT, and a committed child is already gone from the pending
// overlay, so nothing else can restore it.
//
// installed reports whether the install actually ran; it is false only when
// the call was a no-op because there is no cache to install into, in which
// case the caller keeps the raw response. Callers must branch on it rather
// than on the length of the view: an install that legitimately filtered every
// response entry yields an empty view, and treating that as "nothing to say"
// would restore the stale response the reconciliation just corrected.
func (dc *DirCache) PutListing(dirPath string, items []CachedFileInfo, snapshot ListingSnapshot) (view []CachedFileInfo, installed bool) {
	if dc == nil || dirPath == "" {
		return items, false
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
	// The cache keeps a bounded prefix of the response (the pre-existing
	// large-directory guard: an over-cap listing answers positive lookups from
	// its prefix and reports the rest as partial misses), but what this install
	// publishes to its caller is every response item. Truncating the reply
	// would hide names past the cap from this readdir, make the Lookup fallback
	// miss them and cache a false ENOENT, and make
	// remoteDirectoryHasChildren judge a populated directory empty.
	limit := len(items)
	if limit > dc.maxEntries {
		limit = dc.maxEntries
	}
	served := make(map[string]CachedFileInfo, len(items))
	for i := 0; i < len(items); i++ {
		item := items[i]
		if oldEntry != nil {
			if oldItem, ok := oldEntry.items[item.Name]; ok {
				item = mergeCachedOwner(item, oldItem)
			}
		}
		if i < limit {
			entry.upsert(item, dc.maxEntries)
		}
		served[item.Name] = item
	}
	// Completeness is decided before the replay below: a carried-over child that
	// overflows the entry evicts and clears it, and a listing must not claim to
	// be complete after that point, or the next miss is answered with a wrong
	// ENOENT instead of reaching the server.
	if len(items) <= dc.maxEntries {
		entry.complete = true
		entry.completeExpires = now.Add(dc.ttl)
	}
	if oldEntry != nil {
		// Records and in-flight snapshots are carried over in full. Listing
		// requests overlap: an install whose snapshot is OLDER can land after
		// one whose snapshot is NEWER, and publishing only the records newer
		// than the current snapshot would already have discarded the ones that
		// install still needs. The replay below is what applies the snapshot
		// filter, and reclaimRecordsLocked (driven by the carried-over
		// outstanding set) is what eventually drops the records.
		entry.localSeq = cloneSeqTimes(oldEntry.localSeq)
		entry.removedSeq = cloneSeqTimes(oldEntry.removedSeq)
		entry.localItem = cloneCachedItems(oldEntry.localItem)
		entry.outstanding = cloneSeqCounts(oldEntry.outstanding)
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
			delete(served, name)
		}
		for name, seq := range oldEntry.localSeq {
			if seq <= snapshot.seq {
				continue
			}
			// The response either predates this child (it is absent, and
			// dropping it would hide an already-acknowledged file until the
			// listing expires) or carries its pre-mutation entry (a
			// remove-then-recreate). Both lose to the newer local record;
			// upsert keeps any owner/handle info the response contributed.
			//
			// The child's own copy is kept separately from the served items so
			// a later install that legitimately drops the name (a newer
			// snapshot) still leaves the bytes replayable for an older
			// snapshot still in flight.
			item, ok := oldEntry.localItem[name]
			if !ok {
				if item, ok = oldEntry.items[name]; !ok {
					continue
				}
			}
			entry.upsert(item, dc.maxEntries)
			served[name] = item
		}
	}
	dc.installs++
	entry.installSeq = dc.installs
	dc.entries[dirPath] = entry
	// Release this request's flight and reclaim what it no longer needs.
	dc.releaseListingLocked(snapshot.flight)

	if len(served) == 0 {
		return nil, true
	}
	// Emit in the cache's order so the reply matches what the next readdir
	// would serve, then append any amended child the cap pushed out of it.
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
	return out, true
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
//
// This is the authoritative entry point: it means *this mount* established the
// child's current state (a commit settled, a namespace object was created or
// renamed, a local overlay resolved). Such a record outranks a listing
// response that was issued before it, because the server side of that response
// cannot have observed the mutation yet.
//
// For a child learned from a plain remote read, use Observe instead: a read is
// a point-in-time observation, not a mutation, so it must not displace the
// whole-directory view a listing install just fetched.
func (dc *DirCache) Upsert(parentPath string, item CachedFileInfo) {
	dc.upsertInternal(parentPath, item, true)
}

// ObservationToken marks the cache state at the moment a remote read is
// issued. Hand it to Observe when the read completes so a response that is
// merely late cannot overwrite a listing installed while it was in flight.
//
// The primitive is the install sequence, not the revision. A revision only
// orders content within one resource identity, and not every visible metadata
// change advances it (a remote chmod leaves the revision untouched), so trying
// to order a stat against a listing by revision is wrong in both directions:
// it ranks a recreated lower-revision object below the deleted one it
// replaced, and it rejects a same-revision observation that carries newer
// metadata.
//
// The zero value carries no ordering information, so Observe discards it: it
// can never install, only clear an ENOENT marker for the name it observed.
type ObservationToken struct {
	dir        string
	installSeq uint64
	valid      bool
}

// BeginObservation captures the token for a remote read of one child under
// dirPath. Call it immediately before issuing the request; the directory's
// install sequence is what the completing response is checked against.
func (dc *DirCache) BeginObservation(dirPath string) ObservationToken {
	if dc == nil || dirPath == "" {
		return ObservationToken{}
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()
	// An uncached directory has no install sequence yet, so the baseline is the
	// current global counter — the same value ensureEntryLocked seeds into an
	// entry it creates. Reading 0 here would collide with a fresh entry's
	// initial 0 and let a read that predates a listing match the entry that
	// listing created (then later recreated after a drop).
	seq := dc.installs
	if entry, ok := dc.entries[dirPath]; ok && entry != nil {
		seq = entry.installSeq
	}
	return ObservationToken{dir: dirPath, installSeq: seq, valid: true}
}

// Observe records a child entry learned from a remote read (a stat/HEAD
// refresh, for example), paired with the token for that read.
//
// Unlike Upsert this is not an authoritative local mutation: completion order
// is not a version order, so an observation may not be replayed over a listing
// response and may not resurrect a name a listing omitted. Within those limits
// it does refresh metadata, which is what carries a change the revision cannot
// express, such as a remote chmod.
//
// The token decides whether the observation can still be applied: if a listing
// was installed after the token was taken, that listing is the newer
// whole-directory view — for the names it carries and for the names it omits —
// so the response is discarded rather than allowed to fill perceived gaps in
// it. A read issued after the last install matches the current sequence and is
// applied, which is how a metadata change the revision cannot express (a remote
// chmod, say) still reaches the cache.
//
// Either way the observation proves the name exists now, so an unexpired
// ENOENT marker for it is cleared even when the entry itself is not replaced.
func (dc *DirCache) Observe(parentPath string, item CachedFileInfo, token ObservationToken) {
	dc.upsertObserved(parentPath, item, token)
}

// upsertObserved applies an observation under the token's authority.
func (dc *DirCache) upsertObserved(parentPath string, item CachedFileInfo, token ObservationToken) {
	if item.Name == "" {
		return
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()

	entry := dc.ensureEntryLocked(parentPath)
	// The token must still describe the current install state. A listing
	// installed while this read was in flight is the newer whole-directory
	// view, covering names the read carries and names it omits alike, so a
	// response that began before that install is discarded wholesale rather
	// than allowed to fill perceived gaps in it.
	if !token.valid || token.dir != parentPath || entry.installSeq != token.installSeq {
		delete(entry.negatives, item.Name)
		return
	}
	// A local authoritative record for this name outranks any observation: it
	// reflects this mount's own mutation, which the server side of that
	// observation cannot have observed yet.
	if _, local := entry.localSeq[item.Name]; local {
		delete(entry.negatives, item.Name)
		return
	}
	entry.expires = time.Now().Add(dc.ttl)
	entry.upsert(item, dc.maxEntries)
	delete(entry.negatives, item.Name)
}

// upsertInternal stores a child entry. authoritative selects whether the write
// is recorded as a local mutation that a later listing install must replay.
func (dc *DirCache) upsertInternal(parentPath string, item CachedFileInfo, authoritative bool) {
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
	if authoritative {
		entry.noteLocal(item.Name, item, dc.nextSeqLocked())
	}
}

// Remove deletes a known child entry from a parent without implying the parent
// namespace is complete.
//
// The removal is recorded even when the parent has no cached state yet, so a
// list request already in flight cannot resurrect the name: the interleaving
// "cold parent -> BeginListing -> successful Unlink -> stale response" is
// exactly when the parent's cache holds nothing to correct.
func (dc *DirCache) Remove(parentPath, name string) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	entry := dc.ensureEntryLocked(parentPath)
	entry.remove(name)
	entry.noteRemoved(name, dc.nextSeqLocked())
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
// later release onto whatever entry replaced it (and, because BeginListing does
// not consume dc.seq, an older flight can even decrement a newer flight's
// count), and a probe landing before the install would reclaim records the
// install still needs. What invalidation must drop is the served namespace,
// which is what this clears. Caller holds dc.mu.
func (dc *DirCache) invalidateEntryLocked(dirPath string) {
	entry, ok := dc.entries[dirPath]
	if !ok {
		return
	}
	if len(entry.outstanding) == 0 {
		delete(dc.entries, dirPath)
		return
	}
	entry.clearServedState()
}

// clearServedState drops everything the entry publishes while keeping the
// reconciliation state (records, flights, install sequence) that in-flight
// listings depend on.
func (e *dirCacheEntry) clearServedState() {
	e.items = make(map[string]CachedFileInfo)
	e.order = nil
	e.expires = time.Time{}
	e.complete = false
	e.completeExpires = time.Time{}
	e.sessionExpires = time.Time{}
	e.negatives = make(map[string]time.Time)
	e.missWindowStart = time.Time{}
	e.remoteMisses = 0
	e.escalateNotBefore = time.Time{}
}

func newDirCacheEntry() *dirCacheEntry {
	return &dirCacheEntry{
		items:      make(map[string]CachedFileInfo),
		negatives:  make(map[string]time.Time),
		localSeq:   make(map[string]uint64),
		localItem:  make(map[string]CachedFileInfo),
		removedSeq: make(map[string]uint64),
	}
}

// cloneSeqTimes copies a sequence map so an install cannot alias the entry it
// has just replaced.
func cloneSeqTimes(src map[string]uint64) map[string]uint64 {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]uint64, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// cloneCachedItems copies a child-entry map so an install cannot alias the
// entry it has just replaced.
func cloneCachedItems(src map[string]CachedFileInfo) map[string]CachedFileInfo {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]CachedFileInfo, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// cloneSeqCounts copies the outstanding-snapshot counter map so an install
// cannot alias the entry it has just replaced.
func cloneSeqCounts(src map[uint64]int) map[uint64]int {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[uint64]int, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// nextSeqLocked returns the next local namespace sequence. Caller holds dc.mu.
func (dc *DirCache) nextSeqLocked() uint64 {
	dc.seq++
	return dc.seq
}

// noteLocal records that name was recorded by an authoritative local mutation
// at seq, keeping its entry so a listing install issued before this record can
// still replay it. Retention is bounded by the outstanding in-flight listing
// set, not by time: see reclaimRecordsLocked.
func (e *dirCacheEntry) noteLocal(name string, item CachedFileInfo, seq uint64) {
	if seq == 0 {
		return
	}
	if e.localSeq == nil {
		e.localSeq = make(map[string]uint64)
	}
	if e.localItem == nil {
		e.localItem = make(map[string]CachedFileInfo)
	}
	e.localSeq[name] = seq
	e.localItem[name] = item
	delete(e.removedSeq, name)
}

// noteRemoved records that name was removed locally at seq.
func (e *dirCacheEntry) noteRemoved(name string, seq uint64) {
	if seq == 0 {
		return
	}
	if e.removedSeq == nil {
		e.removedSeq = make(map[string]uint64)
	}
	e.removedSeq[name] = seq
	delete(e.localSeq, name)
	delete(e.localItem, name)
}

func (dc *DirCache) ensureEntryLocked(dirPath string) *dirCacheEntry {
	entry, ok := dc.entries[dirPath]
	if !ok {
		entry = newDirCacheEntry()
		// Seed the install baseline from the global counter rather than leaving
		// zero. The counter never decreases, so an entry recreated after a
		// listing install inherits a value at or above that install, and a
		// token captured before it can no longer match (see BeginObservation).
		entry.installSeq = dc.installs
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
	// Records are only useful to a listing that was already in flight when
	// they were written. This is the probe path, so reclaim on the in-flight
	// set rather than on a timer: a listing request has no client-side
	// deadline, so no TTL could bound this safely. Install never reaches here
	// (PutListing reads dc.entries directly), which keeps its replay from
	// racing its own reclamation.
	entry.reclaimRecordsLocked()
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
			delete(e.localItem, evict)
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
		// The served items are stale, but the reconciliation records are not
		// cleared here: a listing issued before them is still in flight and
		// still needs them. reclaimRecordsLocked decides that, on the
		// in-flight set rather than on time.
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
		// An in-flight listing snapshot must keep the entry alive: a cache
		// lookup landing between BeginListing and the install would otherwise
		// drop the entry and take the snapshot (and the records its install
		// still needs) with it.
		len(e.outstanding) > 0 ||
		// Records outlive the served items: an install that drops a
		// locally-recorded child still needs them to replay for an older
		// snapshot that is still in flight.
		len(e.localSeq) > 0 || len(e.removedSeq) > 0
}

func cacheParentName(p string) (string, string) {
	dir := path.Dir(p)
	if dir == "." {
		dir = "/"
	}
	return dir, path.Base(p)
}

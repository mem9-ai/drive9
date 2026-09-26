package fuse

import (
	"container/list"
	"path"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	defaultDirCacheTTL              = 10 * time.Second
	defaultCodingAgentDirCacheTTL   = 30 * time.Second
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

	// freshUntil is the deadline of the evidence holding this value up, and is
	// what makes the cache's validity per name instead of per directory:
	//
	//	zero -> this mount's own state (a settled commit, a directory it
	//	        created). It does not age out, because a response omitting it
	//	        cannot be told apart from one taken before the commit landed —
	//	        see the note on PutListing. Invalidation is what retires it.
	//	> 0  -> a remote read: the listing that carried the name, or the stat
	//	        that observed it. Only newer evidence about this name rewrites
	//	        it, so no other name's refresh, listing, or local write can
	//	        extend it.
	//
	// Only DirCache sets this field; a caller-built value leaves it zero and
	// takes on the deadline of whatever read carries it into the cache.
	freshUntil time.Time
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
	kind                namespaceLookupKind
	item                CachedFileInfo
	dirGeneration       uint64
	mutationGeneration  uint64
	namespaceGeneration uint64
}

// dirCacheEntry is one directory's cached namespace.
//
// # How long a cached name stays valid
//
// Each name carries its own deadline, in CachedFileInfo.freshUntil, and only
// evidence about that name can rewrite it:
//
//	a listing that carries the name   -> the listing's deadline (now + ttl)
//	a stat/HEAD observation of it     -> that observation's deadline
//	a local commit of it (Upsert)     -> no deadline: this mount's own state
//
// Nothing is shared between names, so no bulk refresh can extend a name that
// the newest evidence about it no longer covers: a directory listing that omits
// a name neither renews nor removes it (removal is not evidence the response
// can justify — see PutListing), and an unrelated local write cannot keep it
// alive. A name is valid exactly as long as the newest read or write that
// spoke about *it* says so, which is the one rule this entry is built around.
//
// The three entry-level fields below are not per-name lifetimes and are
// deliberately kept apart from them: complete/completeExpires are the
// directory's miss authority (a property of the listing as a whole, not of any
// one name), and sessionExpires is a mount-created directory's.
type dirCacheEntry struct {
	items           map[string]CachedFileInfo
	order           []string
	complete        bool
	completeExpires time.Time // safe ENOENT-on-miss TTL for a complete listing
	sessionExpires  time.Time // safe ENOENT-on-miss TTL for a session-created dir
	negatives       map[string]time.Time

	// Negative-lookup storm tracking; see RecordRemoteNegative.
	remoteMisses      int
	missWindowStart   time.Time
	escalateNotBefore time.Time

	// freshnessHint is the earliest instant at which some cached name may have
	// outlived its deadline, and it exists so prune stays O(1) on the read path:
	// before that instant there is nothing to scan for, and after it one scan
	// both drops what expired and recomputes the hint exactly.
	//
	// It is maintained conservatively — upsert can only lower it, and a refresh
	// that moves a name to a later deadline leaves it early — so it is never
	// later than the true minimum. Being early costs one scan that finds
	// nothing; being late would keep a name past its evidence, which is the bug
	// this whole mechanism exists to prevent. Zero means no cached name carries
	// a deadline, so there is nothing to expire.
	freshnessHint time.Time

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
	// now is the clock this cache reads, held in a field so tests can advance
	// time explicitly instead of sleeping and hoping a deadline passed while the
	// scheduler looked elsewhere. Production always leaves it as time.Now.
	now func() time.Time
	// gen is the global fencing clock for remote reads. It only increases;
	// each entry takes a fresh value whenever its directory changes in a way
	// that can invalidate an in-flight read, so a generation a token holds can
	// never recur.
	gen uint64
	// namespaceGen advances only when the whole namespace view loses trust.
	// Directory-scoped mutations use mutationGenerations below so an unrelated
	// write does not invalidate every directory's cached shape evidence.
	namespaceGen uint64
	// mutationGenerations is a bounded LRU of directory-scoped namespace
	// identities. Reads allocate but do not advance an identity; authoritative
	// mutations advance only the affected directory (or prefix). Reallocating
	// an evicted identity always takes a newer value, so eviction is safely
	// conservative rather than allowing an old marker to match again.
	mutationGenerations map[string]directoryMutationState
	mutationOrder       *list.List
	mutationClock       uint64
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
	// installFloor is the newest listing install recorded for a directory, held
	// outside the entry because it has to outlive the entry itself. A newer
	// install can be the last state an entry holds, and once every deadline on
	// it passes an ordinary read drops the entry — while the listing request
	// that began before that install is still outstanding and still has to learn
	// it lost. Rebuilding the entry from scratch would start installSeq at 0 and
	// let the superseded response republish a namespace a newer listing already
	// replaced. Like retired, it lives exactly as long as some request could
	// consult it.
	installFloor map[string]uint64
}

// requestRef is shared by a request token and the cache so a token can be
// released more than once (an install plus a deferred release, say) without
// double-counting.
type requestRef struct {
	released bool
}

type directoryMutationState struct {
	generation uint64
	element    *list.Element
}

type listingInstallReceipt struct {
	accepted            []CachedFileInfo
	childCount          int
	dirGeneration       uint64
	mutationGeneration  uint64
	namespaceGeneration uint64
	complete            bool
	installed           bool
}

type batchObservationReceipt struct {
	accepted            []CachedFileInfo
	mutationGeneration  uint64
	namespaceGeneration uint64
}

type directoryPrefetchSnapshot struct {
	parentPath          string
	targetPath          string
	name                string
	resourceID          string
	mutationGeneration  uint64
	namespaceGeneration uint64
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
		entries:             make(map[string]*dirCacheEntry),
		mutationGenerations: make(map[string]directoryMutationState),
		mutationOrder:       list.New(),
		inFlight:            make(map[string]int),
		retired:             make(map[string]uint64),
		installFloor:        make(map[string]uint64),
		ttl:                 ttl,
		negativeTTL:         negativeTTL,
		maxEntries:          maxEntries,
		now:                 time.Now,
	}
}

// Get returns the cached listing of dirPath when the cache can still serve it
// as a whole directory, which requires the newest listing's miss-authority
// deadline to be running. Names whose own evidence ran out are already gone
// (getEntryLocked prunes), so what this returns is one coherent listing or
// nothing at all.
func (dc *DirCache) Get(dirPath string) ([]CachedFileInfo, bool) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	now := dc.now()
	entry, ok := dc.getEntryLocked(dirPath, now)
	if !ok || !entry.completeValid(now) {
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

func (dc *DirCache) directoryPrefetchSnapshot(dirPath string) (directoryPrefetchSnapshot, bool) {
	if dc == nil || dirPath == "" || dirPath == "/" {
		return directoryPrefetchSnapshot{}, false
	}
	parentPath, name := cacheParentName(dirPath)
	dc.mu.Lock()
	defer dc.mu.Unlock()
	return dc.directoryPrefetchSnapshotLocked(parentPath, dirPath, name, dc.now())
}

func (dc *DirCache) directoryPrefetchCandidates(currentPath string, limit int) []directoryPrefetchSnapshot {
	if dc == nil || currentPath == "" || currentPath == "/" || limit <= 0 {
		return nil
	}
	parentPath, currentName := cacheParentName(currentPath)
	dc.mu.Lock()
	defer dc.mu.Unlock()

	now := dc.now()
	parent, ok := dc.getEntryLocked(parentPath, now)
	if !ok || !parent.completeValid(now) {
		return nil
	}
	currentIndex := -1
	for i, name := range parent.order {
		if name == currentName {
			if item, exists := parent.items[name]; exists && item.IsDir {
				currentIndex = i
			}
			break
		}
	}
	if currentIndex < 0 {
		return nil
	}

	candidates := make([]directoryPrefetchSnapshot, 0, limit)
	for _, name := range parent.order[currentIndex+1:] {
		if len(candidates) >= limit {
			break
		}
		item, exists := parent.items[name]
		if !exists || !item.IsDir || item.ResourceID == "" {
			continue
		}
		targetPath := dirEntryChildPath(parentPath, name)
		if target, exists := dc.getEntryLocked(targetPath, now); exists && target.completeValid(now) {
			continue
		}
		candidates = append(candidates, directoryPrefetchSnapshot{
			parentPath:          parentPath,
			targetPath:          targetPath,
			name:                name,
			resourceID:          item.ResourceID,
			mutationGeneration:  dc.mutationGenerationLocked(targetPath),
			namespaceGeneration: dc.namespaceGen,
		})
	}
	return candidates
}

func (dc *DirCache) directoryPrefetchSnapshotLocked(parentPath, targetPath, name string, now time.Time) (directoryPrefetchSnapshot, bool) {
	parent, ok := dc.getEntryLocked(parentPath, now)
	if !ok || !parent.completeValid(now) {
		return directoryPrefetchSnapshot{}, false
	}
	item, ok := parent.items[name]
	if !ok || !item.IsDir || item.ResourceID == "" {
		return directoryPrefetchSnapshot{}, false
	}
	return directoryPrefetchSnapshot{
		parentPath:          parentPath,
		targetPath:          targetPath,
		name:                name,
		resourceID:          item.ResourceID,
		mutationGeneration:  dc.mutationGenerationLocked(targetPath),
		namespaceGeneration: dc.namespaceGen,
	}, true
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
// retirement (invalidation), or their own evidence running out (see
// dirCacheEntry), which gives one structural invariant:
//
//	A name this mount has committed cannot disappear from readdir because of
//	a racing listing.
//
// Remote deletions are not lost by this: every name a response supplies carries
// that response's deadline, and nothing else can renew it, so a name the server
// stops listing is gone one TTL after the last response that carried it. Only
// names this mount established itself have no deadline, and for those the
// fallback is the SSE invalidation the server emits for structural operations
// (a delete is a ResetEvent) — a TTL could not be the fallback there without
// reintroducing #966, because a response taken before a commit omits the name
// for reasons of timing rather than deletion. That is the contract
// docs/specs/cache-invalidation.md assigns to this cache; prompt removal on
// install was an extra the response cannot justify, because it cannot
// distinguish a deleted name from one committed during its own round trip.
//
// Two per-name rules keep a response from speaking for state it predates, both
// answered by the stamps described on dirCacheEntry.localGen:
//
//   - A name this mount changed after requestGen keeps its local state; the
//     response item does not overwrite it (nor contribute to the view).
//   - A name this mount removed after requestGen stays removed.
func (dc *DirCache) PutListing(dirPath string, items []CachedFileInfo, request RequestToken) []CachedFileInfo {
	out, _ := dc.putListing(dirPath, items, request)
	return out
}

func (dc *DirCache) putListing(dirPath string, items []CachedFileInfo, request RequestToken) ([]CachedFileInfo, listingInstallReceipt) {
	if dc == nil || dirPath == "" {
		return items, listingInstallReceipt{}
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()
	return dc.putListingLocked(dirPath, items, request)
}

func (dc *DirCache) putListingIfSnapshot(items []CachedFileInfo, request RequestToken, snapshot directoryPrefetchSnapshot) ([]CachedFileInfo, listingInstallReceipt, bool) {
	if dc == nil || snapshot.targetPath == "" {
		return items, listingInstallReceipt{}, false
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()
	if !dc.directoryPrefetchSnapshotCurrentLocked(snapshot, dc.now()) {
		dc.releaseRequestLocked(request)
		return items, listingInstallReceipt{}, false
	}
	view, receipt := dc.putListingLocked(snapshot.targetPath, items, request)
	return view, receipt, receipt.installed
}

func (dc *DirCache) directoryPrefetchSnapshotCurrentLocked(snapshot directoryPrefetchSnapshot, now time.Time) bool {
	if dc.namespaceGen != snapshot.namespaceGeneration || dc.mutationGenerationLocked(snapshot.targetPath) != snapshot.mutationGeneration {
		return false
	}
	parent, ok := dc.getEntryLocked(snapshot.parentPath, now)
	if !ok || !parent.completeValid(now) {
		return false
	}
	item, ok := parent.items[snapshot.name]
	return ok && item.IsDir && item.ResourceID == snapshot.resourceID
}

func (dc *DirCache) directoryPrefetchCachedListing(snapshot directoryPrefetchSnapshot) ([]CachedFileInfo, bool) {
	if dc == nil || snapshot.targetPath == "" {
		return nil, false
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()

	now := dc.now()
	if !dc.directoryPrefetchSnapshotCurrentLocked(snapshot, now) {
		return nil, false
	}
	entry, ok := dc.getEntryLocked(snapshot.targetPath, now)
	if !ok || !entry.completeValid(now) {
		return nil, false
	}
	items := make([]CachedFileInfo, 0, len(entry.order))
	for _, name := range entry.order {
		if item, exists := entry.items[name]; exists {
			items = append(items, item)
		}
	}
	return items, true
}

func (dc *DirCache) putListingLocked(dirPath string, items []CachedFileInfo, request RequestToken) ([]CachedFileInfo, listingInstallReceipt) {

	now := dc.now()
	// A response taken before the directory was retired describes state the
	// retirement removed (an SSE reset discards the whole directory). It may
	// answer the request that issued it, but republishing it into the cache
	// would undo the invalidation until the entry lapsed on its own.
	if retired := dc.retired[dirPath]; request.seq < retired {
		// The request is settled (its response is served) even though nothing is
		// installed, so it releases like any other completion.
		dc.releaseRequestLocked(request)
		return items, listingInstallReceipt{}
	}
	if old, ok := dc.entries[dirPath]; ok && old != nil {
		old.prune(now)
	}
	entry := dc.ensureEntryLocked(dirPath)

	// A response taken before another install cannot overwrite what that
	// install published: listings overlap, so the older request's response may
	// describe each name at an older revision than the cache already has. Its
	// cache effect is limited to names it is still the newest word on, and the
	// caller is served the accepted state either way. A stale install also
	// neither renews nor retires anything: both belong to the newer install.
	//
	// The comparison reads the newest install from both places it can live. The
	// entry holds it while the entry exists; the cache-level floor holds it
	// across the entry's death, because the request that has to learn it lost
	// may outlive every deadline on the entry. Taking the entry's value first
	// and never the smaller of the two is what makes the fence monotone while
	// requests overlap.
	newestInstall := entry.installSeq
	if floor := dc.installFloor[dirPath]; floor > newestInstall {
		newestInstall = floor
	}
	staleInstall := newestInstall > request.seq

	// The cache retains a bounded prefix of the response (the pre-existing
	// large-directory guard: an over-cap listing answers positive lookups from
	// its prefix and reports the rest as partial misses), but callers receive
	// every response item. Truncating the reply would hide names past the cap
	// from this readdir and make the lookup fallback cache a false ENOENT.
	limit := len(items)
	if limit > dc.maxEntries {
		limit = dc.maxEntries
	}
	// Every item this response carries stands on this response as its evidence,
	// so it takes this deadline — and no other. A name the response does not
	// carry is absent here: its own deadline, from the last read or write that
	// did carry it, is untouched, which is what keeps a directory that is
	// listed often from renewing a name the server stopped listing.
	deadline := now.Add(dc.ttl)

	// Names the cache held before this install. They are served to this request
	// even if the merge has to evict one for room: the reply must not omit a
	// name the cache was holding, and the pending overlay cannot restore a
	// committed child the way it restores an in-flight one.
	prior := make(map[string]CachedFileInfo, len(entry.items))
	for name, item := range entry.items {
		prior[name] = item
	}
	served := make(map[string]CachedFileInfo, len(items)+len(prior))
	acceptedNames := make(map[string]struct{}, len(items))
	complete := len(items) <= dc.maxEntries
	for i := 0; i < len(items); i++ {
		// The response is this name's evidence, so its deadline is the
		// response's — never one inherited from whatever the caller's struct
		// was before.
		item := items[i]
		item.freshUntil = deadline
		if stamp, changed := entry.localGen[item.Name]; changed && stamp > request.event {
			// This mount wrote or removed the name after the request was
			// taken; the response cannot speak for it.
			if local, live := entry.items[item.Name]; live {
				served[item.Name] = local
			}
			continue
		}
		if staleInstall {
			// Keep whatever the newer install published for this name: this
			// response is the older word, so it may neither change the cache nor
			// answer for a directory state a newer listing superseded.
			//
			// A name only this response carries is therefore absent from the
			// reply even though the server listed it (#967 review, Codex). That
			// is the same trade the bug this PR fixes is about: the response
			// describes the directory as of its own request, so serving names
			// from it after a newer listing has spoken would republish a
			// namespace that listing replaced. The mount's readdir is a
			// point-in-time answer either way, and the next readdir — no longer
			// overlapping — lists the directory in full.
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
			acceptedNames[item.Name] = struct{}{}
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
	//
	// Miss authority belongs to the newest install alone, together with the
	// deadline it runs on. A stale response neither restores it (it is the older
	// word, and the newer listing may have revoked it precisely because it could
	// not be cached whole) nor revokes it (the newer one knows whether it fits).
	// Touching it here is how a losing response turned names the winner had
	// proven to exist, but could not hold, into false ENOENTs.
	if !staleInstall {
		if complete {
			entry.complete = true
			entry.completeExpires = deadline
		} else {
			entry.complete = false
			entry.completeExpires = time.Time{}
		}
		// This install is now the newest word on the directory's names. The
		// floor records it too, and only while another request can still
		// consult it: with none outstanding, no response remains that could
		// need to be recognized as stale, and a later request is newer than
		// this install by construction.
		entry.installSeq = request.seq
		if dc.inFlight[dirPath] > 0 {
			dc.installFloor[dirPath] = request.seq
		}
	}
	entry.gen = dc.nextGenerationLocked()
	receipt := listingInstallReceipt{}
	if !staleInstall {
		receipt = listingInstallReceipt{
			childCount:          len(entry.items),
			dirGeneration:       entry.gen,
			mutationGeneration:  dc.mutationGenerationLocked(dirPath),
			namespaceGeneration: dc.namespaceGen,
			complete:            entry.complete,
			installed:           true,
		}
		for _, name := range entry.order {
			if _, accepted := acceptedNames[name]; !accepted {
				continue
			}
			if item, ok := entry.items[name]; ok {
				receipt.accepted = append(receipt.accepted, item)
			}
		}
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
	return out, receipt
}

// Lookup returns the namespace-cache state for parent/name.
func (dc *DirCache) Lookup(parentPath, name string) namespaceLookupResult {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	now := dc.now()
	entry, ok := dc.getEntryLocked(parentPath, now)
	result := namespaceLookupResult{
		dirGeneration:       dc.generationLocked(parentPath),
		mutationGeneration:  dc.mutationGenerationLocked(parentPath),
		namespaceGeneration: dc.namespaceGen,
	}
	if !ok {
		return result
	}
	// A name still in items is within its own deadline: getEntryLocked pruned
	// the entry, and prune drops exactly the names whose evidence ran out. That
	// is the one place the rule lives, so this cannot drift from it.
	if item, ok := entry.items[name]; ok {
		result.kind = namespaceLookupPositive
		result.item = item
		return result
	}
	if entry.negativeValid(name, now) {
		result.kind = namespaceLookupNegative
		return result
	}
	if entry.sessionValid(now) {
		result.kind = namespaceLookupSessionMiss
		return result
	}
	if entry.completeValid(now) {
		result.kind = namespaceLookupCompleteMiss
		return result
	}
	if entry.hasState() {
		result.kind = namespaceLookupPartialMiss
		return result
	}
	return result
}

func (dc *DirCache) lookupSnapshotCurrent(parentPath string, result namespaceLookupResult) bool {
	if dc == nil {
		return false
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()
	return dc.generationLocked(parentPath) == result.dirGeneration &&
		dc.mutationGenerationLocked(parentPath) == result.mutationGeneration &&
		dc.namespaceGen == result.namespaceGeneration
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
	// install is the newest listing installation observed when the request
	// started. Batch observations may merge around per-path HEAD results, but
	// not around a listing installed while the batch was in flight: that
	// listing may carry newer metadata even when its request started earlier.
	install uint64
	ref     *requestRef
	valid   bool
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
		dir:     dirPath,
		seq:     dc.nextGenerationLocked(),
		event:   dc.generationLocked(dirPath),
		install: dc.latestInstallLocked(dirPath),
		ref:     &requestRef{},
		valid:   true,
	}
}

func (dc *DirCache) latestInstallLocked(dirPath string) uint64 {
	latest := dc.installFloor[dirPath]
	if entry := dc.entries[dirPath]; entry != nil && entry.installSeq > latest {
		latest = entry.installSeq
	}
	return latest
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
	// waiting to consult its stamps, watermark, or install floor.
	delete(dc.retired, token.dir)
	delete(dc.installFloor, token.dir)
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
	// An observation is a remote read at a point in time, not a local mutation:
	// it carries the same kind of deadline a listing does, and it is newer
	// evidence than any listing that predates it — so it outlives that listing's
	// deadline, but not its own. Promoting it to mount-authoritative state would
	// let a remote chmod or delete that SSE did not report live past every TTL
	// the directory cache has.
	observed := item
	if observed.freshUntil.IsZero() {
		observed.freshUntil = dc.now().Add(dc.ttl)
	}
	entry.upsert(observed, dc.maxEntries)
	delete(entry.negatives, item.Name)
	// The observation must also win over a response that predates it, which is
	// what the stamp records. Both it and this request are released together, so
	// neither outlives the readers that could consult it.
	dc.noteLocalLocked(entry, parentPath, item.Name)
	dc.releaseRequestLocked(token)
}

func (dc *DirCache) observeBatch(parentPath string, items []CachedFileInfo, token RequestToken, mutationGeneration, namespaceGeneration uint64) batchObservationReceipt {
	if dc == nil || len(items) == 0 {
		return batchObservationReceipt{}
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()

	if !token.valid || token.dir != parentPath || dc.namespaceGen != namespaceGeneration || dc.mutationGenerationLocked(parentPath) != mutationGeneration {
		dc.releaseRequestLocked(token)
		return batchObservationReceipt{}
	}
	if retired := dc.retired[parentPath]; token.seq < retired {
		dc.releaseRequestLocked(token)
		return batchObservationReceipt{}
	}
	if dc.latestInstallLocked(parentPath) != token.install {
		dc.releaseRequestLocked(token)
		return batchObservationReceipt{}
	}
	entry := dc.ensureEntryLocked(parentPath)
	deadline := dc.now().Add(dc.ttl)
	accepted := make([]CachedFileInfo, 0, len(items))
	for _, item := range items {
		if item.Name == "" {
			continue
		}
		if stamp, changed := entry.localGen[item.Name]; changed && stamp > token.event {
			continue
		}
		item.freshUntil = deadline
		entry.upsert(item, dc.maxEntries)
		delete(entry.negatives, item.Name)
		accepted = append(accepted, item)
	}
	if len(accepted) == 0 {
		dc.releaseRequestLocked(token)
		return batchObservationReceipt{}
	}
	stamp := dc.nextGenerationLocked()
	entry.gen = stamp
	if dc.inFlight[parentPath] > 0 {
		for _, item := range accepted {
			entry.localGen[item.Name] = stamp
		}
	} else {
		entry.reclaimReconciliationLocked()
	}
	dc.releaseRequestLocked(token)
	return batchObservationReceipt{
		accepted:            accepted,
		mutationGeneration:  mutationGeneration,
		namespaceGeneration: namespaceGeneration,
	}
}

func (dc *DirCache) generation(dirPath string) uint64 {
	if dc == nil {
		return 0
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()
	return dc.generationLocked(dirPath)
}

func (dc *DirCache) mutationGeneration(dirPath string) uint64 {
	if dc == nil {
		return 0
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()
	return dc.mutationGenerationLocked(dirPath)
}

func (dc *DirCache) namespaceGeneration() uint64 {
	if dc == nil {
		return 0
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()
	return dc.namespaceGen
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
	// A local write is this mount's own state, so it carries no deadline: only
	// invalidation retires it. Overwriting whatever evidence the name had before
	// is the point — a settled commit is newer than, and outranks, any read that
	// carried the name earlier.
	item.freshUntil = time.Time{}
	entry.upsert(item, dc.maxEntries)
	delete(entry.negatives, item.Name)
	dc.bumpMutationGenerationLocked(parentPath)
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
	dc.bumpMutationGenerationLocked(parentPath)
	dc.noteLocalLocked(entry, parentPath, name)
	if !entry.hasState() {
		delete(dc.entries, parentPath)
	}
}

func (dc *DirCache) HasPositiveEntries(dirPath string) bool {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	entry, ok := dc.getEntryLocked(dirPath, dc.now())
	if !ok {
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

	entry, ok := dc.getEntryLocked(dirPath, dc.now())
	if !ok {
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
	entry.negatives[name] = dc.now().Add(dc.negativeTTL)
}

// MarkSessionCreatedDir records a directory created by this mount as having a
// locally managed empty namespace for the directory TTL. Local mutations flow
// through Upsert/Remove and foreign writes invalidate via SSE, so the marker
// can safely outlive the negative TTL used for single-name ENOENT markers.
func (dc *DirCache) MarkSessionCreatedDir(dirPath string) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	now := dc.now()
	entry := dc.ensureEntryLocked(dirPath)
	entry.complete = true
	entry.completeExpires = now.Add(dc.ttl)
	entry.sessionExpires = now.Add(dc.ttl)
	entry.negatives = make(map[string]time.Time)
	dc.bumpMutationGenerationLocked(dirPath)
}

// RecordRemoteNegative notes that a remote stat for a child of dirPath
// returned ENOENT. It reports true when the caller should escalate to a
// single directory listing: escalateMissThreshold misses accumulated within
// escalateMissWindow and no escalation cooldown is active.
func (dc *DirCache) RecordRemoteNegative(dirPath string) bool {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	now := dc.now()
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
	entry.escalateNotBefore = dc.now().Add(dc.ttl)
	entry.remoteMisses = 0
	entry.missWindowStart = time.Time{}
}

// CanAnswerMisses reports whether dirPath currently has namespace state that
// answers child misses locally (a valid complete listing or session marker).
func (dc *DirCache) CanAnswerMisses(dirPath string) bool {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	now := dc.now()
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
		dc.namespaceGen++
		dc.mutationGenerations = make(map[string]directoryMutationState)
		dc.mutationOrder.Init()
		// Retire every entry rather than replacing the map: entries with
		// listings or observations still in flight have to keep their
		// reconciliation state (see invalidateEntryLocked).
		for p := range dc.entries {
			dc.invalidateEntryLocked(p)
		}
		return
	}
	dc.bumpMutationPrefixLocked(dirPath)
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

	dc.bumpMutationGenerationLocked(dirPath)
	dc.invalidateEntryLocked(dirPath)
}

// InvalidateAll clears all cache entries.
func (dc *DirCache) InvalidateAll() {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	dc.namespaceGen++
	dc.mutationGenerations = make(map[string]directoryMutationState)
	dc.mutationOrder.Init()

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

func (dc *DirCache) mutationGenerationLocked(dirPath string) uint64 {
	if state, ok := dc.mutationGenerations[dirPath]; ok {
		dc.mutationOrder.MoveToBack(state.element)
		return state.generation
	}
	return dc.bumpMutationGenerationLocked(dirPath)
}

func (dc *DirCache) bumpMutationGenerationLocked(dirPath string) uint64 {
	state, exists := dc.mutationGenerations[dirPath]
	if !exists && len(dc.mutationGenerations) >= dc.maxEntries {
		oldest := dc.mutationOrder.Front()
		delete(dc.mutationGenerations, oldest.Value.(string))
		dc.mutationOrder.Remove(oldest)
	}
	dc.mutationClock++
	if exists {
		dc.mutationOrder.MoveToBack(state.element)
	} else {
		state.element = dc.mutationOrder.PushBack(dirPath)
	}
	state.generation = dc.mutationClock
	dc.mutationGenerations[dirPath] = directoryMutationState{
		generation: state.generation,
		element:    state.element,
	}
	return dc.mutationClock
}

func (dc *DirCache) bumpMutationPrefixLocked(dirPath string) {
	prefix := dirPath + "/"
	found := false
	for candidate, state := range dc.mutationGenerations {
		if candidate != dirPath && !strings.HasPrefix(candidate, prefix) {
			continue
		}
		dc.mutationClock++
		state.generation = dc.mutationClock
		dc.mutationOrder.MoveToBack(state.element)
		dc.mutationGenerations[candidate] = state
		found = found || candidate == dirPath
	}
	if !found {
		dc.bumpMutationGenerationLocked(dirPath)
	}
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
	// Keep the expiry guard no later than the earliest deadline being stored;
	// see freshnessHint. Names with no deadline never wake it.
	if !item.freshUntil.IsZero() && (e.freshnessHint.IsZero() || item.freshUntil.Before(e.freshnessHint)) {
		e.freshnessHint = item.freshUntil
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
	for i, existing := range e.order {
		if existing == name {
			e.order = append(e.order[:i], e.order[i+1:]...)
			return
		}
	}
}

// prune drops everything whose own deadline has passed. It is the one place a
// name's freshness is enforced: every read path resolves an entry through
// getEntryLocked, which prunes before answering, and the next install prunes
// before it merges.
//
// Dropping a name is not a claim about the directory, so it is the entry's miss
// authority that has to give way: a lookup for a name the cache no longer holds
// must reach the server rather than be answered "absent" from a set that simply
// let it expire. That is the same rule eviction follows.
//
// The sweep rewrites the ordering in one pass rather than deleting name by name,
// because a listing gives every name it carries the same deadline: expiring one
// expires the whole directory, and a per-name removal rescans the ordering for
// each — quadratic at the 200k default cap, and it runs under the cache mutex.
func (e *dirCacheEntry) prune(now time.Time) {
	if !e.freshnessHint.IsZero() && !now.Before(e.freshnessHint) {
		hint := time.Time{}
		dropped := false
		for name, item := range e.items {
			if item.freshUntil.IsZero() {
				continue
			}
			if now.After(item.freshUntil) {
				// Deleting from a map while ranging it is well defined: an
				// entry deleted before the range reaches it is not produced.
				delete(e.items, name)
				delete(e.negatives, name)
				dropped = true
				continue
			}
			if hint.IsZero() || item.freshUntil.Before(hint) {
				hint = item.freshUntil
			}
		}
		if dropped {
			// Rebuild the ordering in one pass instead of removing name by
			// name: a listing gives every name it carries the same deadline, so
			// one expiry empties the whole directory, and a per-name removal
			// rescans (and shifts) the ordering for each of them — quadratic at
			// the 200k default cap, while holding the cache mutex.
			kept := e.order[:0]
			for _, name := range e.order {
				if _, live := e.items[name]; live {
					kept = append(kept, name)
				}
			}
			// Drop the references past the new length so a swept directory does
			// not keep the expired names alive through the backing array.
			clear(e.order[len(kept):])
			e.order = kept
			e.complete = false
			e.completeExpires = time.Time{}
			e.sessionExpires = time.Time{}
		}
		e.freshnessHint = hint
	}

	if !e.completeExpires.IsZero() && now.After(e.completeExpires) {
		e.completeExpires = time.Time{}
		// The flag means "the newest listing is still authoritative", which it
		// cannot be without a running deadline; leaving it set would also keep
		// an otherwise empty entry alive in the cache.
		e.complete = false
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
//
// Names are pruned before this is consulted, so a non-empty items map is live
// state. The map is also allocated on the write path, which is why the length
// test — not nilness — is the one that counts.
func (e *dirCacheEntry) hasState() bool {
	return len(e.items) > 0 || len(e.negatives) > 0 || e.complete ||
		!e.completeExpires.IsZero() || !e.sessionExpires.IsZero() ||
		!e.missWindowStart.IsZero() || !e.escalateNotBefore.IsZero() ||
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

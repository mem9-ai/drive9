package fuse

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This file covers issue #966: a directory listing that lands while a commit
// settles must not hide the committed child. The mechanism under test is the
// invariant that a listing install is additive — a name this mount committed
// cannot disappear from readdir because of a racing listing — plus the per-name
// deadlines and stamps that keep each response from speaking for state it
// predates.

// cachedNames reports what the cache holds for dirPath, after the same prune
// every read path performs. Pruning here is what makes these helpers observe
// the cache a caller sees rather than the raw fields: a name past its own
// deadline is not part of the answer, whether or not some read has swept it yet.
func cachedNames(t *testing.T, dc *DirCache, dirPath string) []string {
	t.Helper()
	dc.mu.Lock()
	defer dc.mu.Unlock()
	entry := dc.entries[dirPath]
	if entry == nil {
		return nil
	}
	entry.prune(dc.now())
	names := make([]string, 0, len(entry.items))
	for _, name := range entry.order {
		if _, ok := entry.items[name]; ok {
			names = append(names, name)
		}
	}
	return names
}

func viewNames(view []CachedFileInfo) []string {
	names := make([]string, 0, len(view))
	for _, item := range view {
		names = append(names, item.Name)
	}
	return names
}

func containsName(names []string, want string) bool {
	for _, name := range names {
		if name == want {
			return true
		}
	}
	return false
}

func assertCachedChild(t *testing.T, dc *DirCache, dirPath, name string, check func(CachedFileInfo)) {
	t.Helper()
	dc.mu.Lock()
	entry := dc.entries[dirPath]
	var item CachedFileInfo
	var ok bool
	if entry != nil {
		entry.prune(dc.now())
		item, ok = entry.items[name]
	}
	dc.mu.Unlock()
	if entry == nil {
		t.Fatalf("expected an entry for %s", dirPath)
	}
	if !ok {
		t.Fatalf("child %s missing: names=%v", name, cachedNames(t, dc, dirPath))
	}
	check(item)
}

// listInstall mirrors production: register the request, then install the
// response it produced.
func listInstall(dc *DirCache, dirPath string, items []CachedFileInfo) []CachedFileInfo {
	request := dc.BeginRequest(dirPath)
	return dc.PutListing(dirPath, items, request)
}

// testClock drives a DirCache deterministically, so expiry is decided by an
// explicit advance instead of by sleeping and hoping the scheduler cooperated.
// Codex flagged the sleep-based windows on #967 as a flakiness source.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.Now()}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward, never backward: deadlines are compared with
// After, so only monotonic movement is meaningful.
func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// newControlledCache builds a cache of the given geometry on a test clock.
func newControlledCache(ttl, negativeTTL time.Duration, maxEntries int) (*DirCache, *testClock) {
	dc := NewNamespaceCache(ttl, negativeTTL, maxEntries)
	clock := newTestClock()
	dc.now = clock.Now
	return dc, clock
}

// --- The core invariant: an install never removes a committed name ---------

// The reported bug, at the cache layer: a child committed during the listing's
// round trip must survive the install and reach the caller that fetched it.
func TestDirCacheInstallKeepsChildCommittedDuringRTT(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "a.dat"}})

	// The listing request is issued (its generation captured), then a commit
	// for b.dat settles during its RTT.
	request := dc.BeginRequest("/d")
	dc.Upsert("/d", CachedFileInfo{Name: "b.dat"})

	view := dc.PutListing("/d", []CachedFileInfo{{Name: "a.dat"}}, request)
	if !containsName(viewNames(view), "b.dat") {
		t.Fatalf("caller served a view without the committed child: view=%v", viewNames(view))
	}
	if !containsName(cachedNames(t, dc, "/d"), "b.dat") {
		t.Fatalf("cache lost the committed child: names=%v", cachedNames(t, dc, "/d"))
	}
}

// The invariant under many concurrent commits and repeated installs.
func TestDirCacheInstallNeverDropsCommittedNames(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "base.dat"}})

	const committed = 64
	committedNames := make([]string, 0, committed)
	for i := range committed {
		name := fmt.Sprintf("u_%03d.dat", i)
		committedNames = append(committedNames, name)
		request := dc.BeginRequest("/d")
		dc.Upsert("/d", CachedFileInfo{Name: name})
		// Every commit races an unrelated response taken before it.
		dc.PutListing("/d", []CachedFileInfo{{Name: "base.dat"}, {Name: "other.dat"}}, request)
	}

	names := cachedNames(t, dc, "/d")
	for _, want := range committedNames {
		if !containsName(names, want) {
			t.Fatalf("committed child %s lost by a racing install: %v", want, names)
		}
	}
}

// A local removal is authoritative: a response taken before it must not
// reinstate the name, and the view must not carry it either.
func TestDirCacheInstallDoesNotResurrectRemovedChild(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "gone.dat"}, {Name: "keep.dat"}})

	request := dc.BeginRequest("/d")
	dc.Remove("/d", "gone.dat")
	view := dc.PutListing("/d", []CachedFileInfo{{Name: "gone.dat"}, {Name: "keep.dat"}}, request)

	if containsName(viewNames(view), "gone.dat") {
		t.Fatalf("view resurrected a removed child: %v", viewNames(view))
	}
	if names := cachedNames(t, dc, "/d"); containsName(names, "gone.dat") {
		t.Fatalf("cache resurrected a removed child: %v", names)
	}
	if !containsName(cachedNames(t, dc, "/d"), "keep.dat") {
		t.Fatalf("unrelated child lost: %v", cachedNames(t, dc, "/d"))
	}
}

// A removal that has no cached listing yet must also be protected: the stamp
// is what carries the removal forward until an entry exists.
func TestDirCacheInstallDoesNotResurrectRemovalOnColdParent(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	request := dc.BeginRequest("/d")
	dc.Remove("/d", "gone.dat")

	view := dc.PutListing("/d", []CachedFileInfo{{Name: "gone.dat"}}, request)
	if containsName(viewNames(view), "gone.dat") {
		t.Fatalf("view resurrected a removal recorded before any cache entry: %v", viewNames(view))
	}
}

// The view carries the merged metadata for a name a local mutation moved
// during the RTT, not the response's older struct for that name.
func TestDirCacheInstallViewCarriesUpdatedSameName(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "f.dat", Size: 10, Revision: 1}})

	request := dc.BeginRequest("/d")
	dc.Upsert("/d", CachedFileInfo{Name: "f.dat", Size: 20, Revision: 2})
	view := dc.PutListing("/d", []CachedFileInfo{{Name: "f.dat", Size: 10, Revision: 1}}, request)

	for _, item := range view {
		if item.Name == "f.dat" {
			if item.Size != 20 || item.Revision != 2 {
				t.Fatalf("view carried the response's older metadata: size=%d rev=%d, want 20/2", item.Size, item.Revision)
			}
			return
		}
	}
	t.Fatalf("child missing from the view: %v", viewNames(view))
}

// A response that legitimately omits the last child (everything was removed
// locally during the RTT) yields an empty authoritative view.
func TestDirCacheInstallViewCanBeEmpty(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "only.dat"}})

	request := dc.BeginRequest("/d")
	dc.Remove("/d", "only.dat")
	view := dc.PutListing("/d", []CachedFileInfo{{Name: "only.dat"}}, request)

	if len(view) != 0 {
		t.Fatalf("view carried a removed name: %v", viewNames(view))
	}
}

// Documented trade-off: because installs are additive, a name another actor
// deleted remotely leaves the cache through invalidation or expiry, not through
// the next listing. Pinning it here keeps the behaviour explicit.
func TestDirCacheRemoteDeleteLeavesOnExpiryNotOnInstall(t *testing.T) {
	dc, clock := newControlledCache(60*time.Millisecond, 60*time.Millisecond, defaultNamespaceCacheMaxEntries)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "gone.dat"}, {Name: "keep.dat"}})

	// A later response omitting gone.dat does not remove it.
	listInstall(dc, "/d", []CachedFileInfo{{Name: "keep.dat"}})
	if names := cachedNames(t, dc, "/d"); !containsName(names, "gone.dat") {
		t.Fatalf("fixture: expected the name to still be cached, names=%v", names)
	}

	// Expiry retires the name, the documented fallback when a structural
	// operation's invalidation is not what removed it.
	clock.Advance(200 * time.Millisecond)
	if names := cachedNames(t, dc, "/d"); containsName(names, "gone.dat") {
		t.Fatalf("expired entry still holds the remotely deleted name: %v", names)
	}
}

// A response larger than the cache cap is served in full even though the cache
// retains only its bounded prefix.
func TestDirCacheInstallViewIsNotCappedByMaxEntries(t *testing.T) {
	const cap = 4
	dc := NewNamespaceCache(10*time.Second, 10*time.Second, cap)

	response := make([]CachedFileInfo, 0, cap*3)
	for i := range cap * 3 {
		response = append(response, CachedFileInfo{Name: fmt.Sprintf("f_%02d.dat", i), Revision: 1})
	}
	view := listInstall(dc, "/d", response)
	if len(view) != len(response) {
		t.Fatalf("view truncated to the cache cap: got %d, want %d", len(view), len(response))
	}
	if got := len(dc.entries["/d"].items); got > cap {
		t.Fatalf("cache exceeded maxEntries: %d > %d", got, cap)
	}
	if _, ok := dc.Get("/d"); ok {
		t.Fatal("a truncated listing must not be served as complete")
	}
}

// --- The observation fence -------------------------------------------------

// A read that completes with nothing having touched the directory publishes.
func TestDirCacheObservationAppliesWhenNothingChanged(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "f.dat", Size: 5, Revision: 1}})

	token := dc.BeginRequest("/d")
	dc.Observe("/d", CachedFileInfo{Name: "f.dat", Size: 8, Revision: 1}, token)

	assertCachedChild(t, dc, "/d", "f.dat", func(item CachedFileInfo) {
		if item.Size != 8 {
			t.Fatalf("observation not applied: size=%d, want 8", item.Size)
		}
	})
}

// A remote chmod advances no revision (Store.Chmod writes only mode), so a
// same-revision observation must still be able to update metadata.
func TestDirCacheObservationAppliesSameRevisionMetadataChange(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "f.dat", Size: 50, Revision: 5, Mode: 0o644, HasMode: true}})

	token := dc.BeginRequest("/d")
	dc.Observe("/d", CachedFileInfo{Name: "f.dat", Size: 50, Revision: 5, Mode: 0o600, HasMode: true}, token)

	assertCachedChild(t, dc, "/d", "f.dat", func(item CachedFileInfo) {
		if !item.HasMode || item.Mode != 0o600 {
			t.Fatalf("same-revision metadata change rejected: mode=%#o, want 0600", item.Mode)
		}
	})
}

// A read outlived by a listing install must be discarded: the install is the
// newer whole-directory view for the names it carries and the names it omits.
func TestDirCacheObservationSupersededByInstallIsDiscarded(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "keep.dat"}})

	token := dc.BeginRequest("/d")
	listInstall(dc, "/d", []CachedFileInfo{{Name: "keep.dat"}})

	dc.Observe("/d", CachedFileInfo{Name: "late.dat", Size: 9}, token)
	dc.Observe("/d", CachedFileInfo{Name: "keep.dat", Size: 99}, token)

	names := cachedNames(t, dc, "/d")
	if containsName(names, "late.dat") {
		t.Fatalf("superseded read published a child: %v", names)
	}
	assertCachedChild(t, dc, "/d", "keep.dat", func(item CachedFileInfo) {
		if item.Size == 99 {
			t.Fatal("superseded read overwrote a published entry")
		}
	})
}

// A local commit after the read began is authoritative: the read cannot undo
// what this mount just committed.
func TestDirCacheObservationSupersededByLocalUpsertIsDiscarded(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "base.dat"}})

	token := dc.BeginRequest("/d")
	dc.Upsert("/d", CachedFileInfo{Name: "f.dat", Size: 20, Revision: 2})
	dc.Observe("/d", CachedFileInfo{Name: "f.dat", Size: 10, Revision: 1}, token)

	assertCachedChild(t, dc, "/d", "f.dat", func(item CachedFileInfo) {
		if item.Size != 20 || item.Revision != 2 {
			t.Fatalf("observation overwrote a local commit: size=%d rev=%d, want 20/2", item.Size, item.Revision)
		}
	})
}

// A local removal after the read began is equally authoritative: the older
// positive read must not resurrect the name.
func TestDirCacheObservationSupersededByLocalRemoveIsDiscarded(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "f.dat", Size: 10, Revision: 10, ResourceID: "res-A"}})

	token := dc.BeginRequest("/d")
	dc.Remove("/d", "f.dat")
	dc.Observe("/d", CachedFileInfo{Name: "f.dat", Size: 10, Revision: 10, ResourceID: "res-A"}, token)

	if names := cachedNames(t, dc, "/d"); containsName(names, "f.dat") {
		t.Fatalf("observation resurrected a locally removed child: %v", names)
	}
}

// Delete + recreate: the replacement restarts at a low revision while the
// deleted object carried a high one. A read of the deleted object that outlives
// the recreation must not restore its identity.
func TestDirCacheObservationDoesNotRestoreDeletedObjectIdentity(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "f.dat", Size: 10, Revision: 10, ResourceID: "res-A"}})

	token := dc.BeginRequest("/d")
	listInstall(dc, "/d", []CachedFileInfo{{Name: "f.dat", Size: 1, Revision: 1, ResourceID: "res-B"}})
	dc.Observe("/d", CachedFileInfo{Name: "f.dat", Size: 10, Revision: 10, ResourceID: "res-A"}, token)

	assertCachedChild(t, dc, "/d", "f.dat", func(item CachedFileInfo) {
		if item.ResourceID != "res-B" || item.Revision != 1 {
			t.Fatalf("late read restored the deleted object: res=%q rev=%d", item.ResourceID, item.Revision)
		}
	})
}

// ABA across a drop and recreation: the entry is retired while a read is in
// flight, then recreated by the late read itself. The recreated entry must not
// let that read install.
func TestDirCacheObservationDoesNotSurviveEntryRetirement(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "f.dat", Size: 1, Revision: 1, ResourceID: "res-B"}})

	token := dc.BeginRequest("/d")
	dc.Invalidate("/d")
	if _, ok := dc.entries["/d"]; ok {
		t.Fatal("fixture bug: the entry must be retired")
	}
	dc.Observe("/d", CachedFileInfo{Name: "f.dat", Size: 10, Revision: 10, ResourceID: "res-A"}, token)

	if got := dc.Lookup("/d", "f.dat"); got.kind == namespaceLookupPositive {
		t.Fatalf("retired-entry read republished the deleted identity: resourceID=%q rev=%d",
			got.item.ResourceID, got.item.Revision)
	}
}

// The same, through the uncached path.
func TestDirCacheUncachedObservationTokenDoesNotMatchLaterEntry(t *testing.T) {
	dc := NewDirCache(10 * time.Second)

	token := dc.BeginRequest("/d")
	listInstall(dc, "/d", []CachedFileInfo{{Name: "f.dat", Size: 1, Revision: 1, ResourceID: "res-B"}})
	dc.Invalidate("/d")
	dc.Observe("/d", CachedFileInfo{Name: "f.dat", Size: 10, Revision: 10, ResourceID: "res-A"}, token)

	if got := dc.Lookup("/d", "f.dat"); got.kind == namespaceLookupPositive {
		t.Fatalf("uncached-token read republished a deleted identity: resourceID=%q", got.item.ResourceID)
	}
}

// A read issued while uncached, with nothing else touching the directory, must
// still publish: there is no newer information to defer to.
func TestDirCacheUncachedObservationAppliesWhenNothingIntervenes(t *testing.T) {
	dc := NewDirCache(10 * time.Second)

	token := dc.BeginRequest("/d")
	dc.Observe("/d", CachedFileInfo{Name: "f.dat", Size: 7, Revision: 1}, token)

	if got := dc.Lookup("/d", "f.dat"); got.kind != namespaceLookupPositive || got.item.Size != 7 {
		t.Fatalf("uncached read with no competing event was dropped: kind=%v item=%+v", got.kind, got.item)
	}
}

// A zero token carries no ordering information and is discarded entirely.
func TestDirCacheZeroObservationTokenIsDiscarded(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "keep.dat"}})

	dc.Observe("/d", CachedFileInfo{Name: "late.dat", Size: 9}, RequestToken{})

	if names := cachedNames(t, dc, "/d"); containsName(names, "late.dat") {
		t.Fatalf("zero token published a child: %v", names)
	}
}

// A discarded observation still clears a stale ENOENT marker: it proves the
// name exists, which is what the marker denies.
func TestDirCacheDiscardedObservationClearsNegativeMarker(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "keep.dat"}})
	dc.MarkNegative("/d", "f.dat")

	token := dc.BeginRequest("/d")
	listInstall(dc, "/d", []CachedFileInfo{{Name: "keep.dat"}})
	dc.Observe("/d", CachedFileInfo{Name: "f.dat", Size: 3, Revision: 1}, token)

	if got := dc.Lookup("/d", "f.dat"); got.kind == namespaceLookupNegative {
		t.Fatal("observation that proves existence did not clear the ENOENT marker")
	}
}

// --- Invalidation ----------------------------------------------------------

// Invalidation drops what the cache serves, and a directory that is read again
// starts clean.
func TestDirCacheInvalidateDropsServedState(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "a.dat"}})

	dc.Invalidate("/d")
	if _, ok := dc.Get("/d"); ok {
		t.Fatal("invalidated directory still serves a listing")
	}
	if got := dc.Lookup("/d", "a.dat"); got.kind != namespaceLookupNone {
		t.Fatalf("invalidated child still resolves: kind=%v", got.kind)
	}

	listInstall(dc, "/d", []CachedFileInfo{{Name: "b.dat"}})
	dc.InvalidatePrefix("/")
	if _, ok := dc.Get("/d"); ok {
		t.Fatal("InvalidatePrefix(\"/\") left a servable listing")
	}
}

// --- Cross-layer: the view the callers actually serve ----------------------

// A child committed during the LIST RTT must reach the request's caller, which
// is what the first implementation got wrong: the cache was reconciled but the
// caller was still handed the raw response.
func TestReconciledFileInfosProjectsTheMergedView(t *testing.T) {
	// A child committed during the LIST RTT must reach the caller.
	merged := []CachedFileInfo{{Name: "base.dat", Size: 5, Revision: 4}, {Name: "new.dat", Size: 7, Revision: 9}}
	got := reconciledFileInfos(merged)
	names := make([]string, 0, len(got))
	for _, item := range got {
		names = append(names, item.Name)
	}
	if !containsName(names, "new.dat") {
		t.Fatalf("committed child missing from the projection: %v", names)
	}

	// Same-name metadata comes from the merged item, not a response struct.
	updated := []CachedFileInfo{{Name: "base.dat", Size: 50, Revision: 6}}
	got = reconciledFileInfos(updated)
	if got[0].Size != 50 || got[0].Revision != 6 {
		t.Fatalf("projection carried stale metadata: %+v", got[0])
	}

	// An empty merged view is a real answer: the projection must not fall back
	// to the raw response, which would reinstate a locally removed name.
	if got := reconciledFileInfos(nil); len(got) != 0 {
		t.Fatalf("empty merged view produced entries: %v", got)
	}
}

// --- The Rmdir pre-check ---------------------------------------------------

// remoteDirectoryHasChildren answers a question about the remote directory, so
// it must decide from the response: a name the additive cache still holds must
// not keep a remotely empty directory looking occupied.
func TestRemoteDirectoryHasChildrenUsesTheResponseNotTheCache(t *testing.T) {
	var listCount atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list") == "" {
			http.NotFound(w, r)
			return
		}
		listCount.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"entries":[]}`))
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	fs.dirCache.Upsert("/gone", CachedFileInfo{Name: "stale.dat", Size: 1, Revision: 1})

	hasChildren, _, err := fs.remoteDirectoryHasChildren(context.Background(), "/gone")
	if err != nil {
		t.Fatalf("remoteDirectoryHasChildren: %v", err)
	}
	if hasChildren {
		t.Fatal("a name only the additive cache holds made an empty remote directory look occupied")
	}
	if got := listCount.Load(); got != 1 {
		t.Fatalf("list calls = %d, want 1", got)
	}
}

// A failed LIST must not leave the directory in a state that blocks the next
// probe.
func TestRemoteDirectoryHasChildrenReleasesStateOnListError(t *testing.T) {
	var listCount atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list") == "" {
			http.NotFound(w, r)
			return
		}
		listCount.Add(1)
		http.Error(w, "list failed", http.StatusInternalServerError)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	for range 3 {
		if _, _, err := fs.remoteDirectoryHasChildren(context.Background(), "/err"); err == nil {
			t.Fatal("expected the list failure to surface")
		}
	}
	if got := listCount.Load(); got != 3 {
		t.Fatalf("list calls = %d, want 3", got)
	}
}

// --- Interleaving matrices -------------------------------------------------
//
// The rounds of review this change went through each found a different cell of
// the same state/event matrix, because the earlier implementations had several
// independent pieces of bookkeeping (per-name records, in-flight counters,
// snapshot sequences, an install clock) whose interactions had to be reasoned
// about case by case. These two matrices pin the whole space instead, so a
// future change to the mechanism has to keep every cell correct rather than
// just the ones a hand-written scenario happened to cover.

// Every ordering of "what happened before the read" x "what happened during the
// read" must leave the read applied exactly when nothing happened after it.
func TestDirCacheObservationFenceMatrix(t *testing.T) {
	type event struct {
		name string
		run  func(dc *DirCache)
	}
	events := []event{
		{"nothing", func(dc *DirCache) {}},
		{"upsert", func(dc *DirCache) { dc.Upsert("/d", CachedFileInfo{Name: "after.dat", Revision: 5}) }},
		{"remove", func(dc *DirCache) { dc.Remove("/d", "f.dat") }},
		{"install", func(dc *DirCache) {
			listInstall(dc, "/d", []CachedFileInfo{{Name: "f.dat", Revision: 3}})
		}},
		{"invalidate", func(dc *DirCache) { dc.Invalidate("/d") }},
	}
	for _, pre := range events {
		for _, post := range events {
			t.Run(pre.name+"_then_"+post.name, func(t *testing.T) {
				dc := NewDirCache(10 * time.Second)
				listInstall(dc, "/d", []CachedFileInfo{{Name: "seed.dat", Revision: 1}})
				pre.run(dc)

				token := dc.BeginRequest("/d")
				post.run(dc)

				// A late read of an object that existed before the directory
				// changed; it may only install when nothing intervened.
				dc.Observe("/d", CachedFileInfo{Name: "f.dat", Size: 10, Revision: 10, ResourceID: "res-A"}, token)

				got := dc.Lookup("/d", "f.dat")
				applied := got.kind == namespaceLookupPositive && got.item.Revision == 10
				if want := post.name == "nothing"; applied != want {
					t.Fatalf("pre=%s post=%s: read applied=%v, want %v (kind=%v)",
						pre.name, post.name, applied, want, got.kind)
				}
			})
		}
	}
}

// Every three-event sequence over the mutation/install/invalidation alphabet
// must preserve both halves of the install invariant: a committed name never
// disappears, and a locally removed name is never reinstated.
func TestDirCacheInstallInvariantMatrix(t *testing.T) {
	type event struct {
		name string
		run  func(dc *DirCache, pending *[]RequestToken)
	}
	events := []event{
		{"install", func(dc *DirCache, pending *[]RequestToken) {
			// Install every response still outstanding, oldest first.
			for _, g := range *pending {
				dc.PutListing("/d", []CachedFileInfo{{Name: "resp.dat"}}, g)
			}
		}},
		{"commit", func(dc *DirCache, pending *[]RequestToken) {
			dc.Upsert("/d", CachedFileInfo{Name: "committed.dat", Revision: 2})
		}},
		{"remove", func(dc *DirCache, pending *[]RequestToken) {
			dc.Remove("/d", "victim.dat")
		}},
		{"newRequest", func(dc *DirCache, pending *[]RequestToken) {
			*pending = append(*pending, dc.BeginRequest("/d"))
		}},
		{"invalidate", func(dc *DirCache, pending *[]RequestToken) { dc.Invalidate("/d") }},
	}
	seq := make([]int, 3)
	for i := range events {
		for j := range events {
			for k := range events {
				seq[0], seq[1], seq[2] = i, j, k
				dc := NewDirCache(10 * time.Second)
				listInstall(dc, "/d", []CachedFileInfo{{Name: "victim.dat"}, {Name: "resp.dat"}})
				pending := []RequestToken{}
				removed, committed, invalidated := false, false, false
				for _, idx := range seq {
					switch events[idx].name {
					case "remove":
						removed = true
					case "commit":
						committed = true
					case "invalidate":
						invalidated = true
					}
					events[idx].run(dc, &pending)
				}
				if invalidated {
					// Retirement may drop anything; the guarantee resumes on
					// the next mutation.
					continue
				}
				names := cachedNames(t, dc, "/d")
				if removed && containsName(names, "victim.dat") {
					t.Fatalf("sequence %s%d%d: removed name reinstated: %v", events[seq[0]].name, seq[1], seq[2], names)
				}
				if committed && !containsName(names, "committed.dat") {
					t.Fatalf("sequence %s%d%d: committed name dropped: %v", events[seq[0]].name, seq[1], seq[2], names)
				}
			}
		}
	}
}

// --- Third review round on #966 -------------------------------------------

// A response taken before the directory was retired must not republish what the
// retirement discarded: it may answer its own request, but installing it would
// undo the invalidation until the entry lapsed on its own.
func TestDirCacheListingIsNotInstalledAcrossRetirement(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "seed.dat"}})

	// A listing request is issued (its generation captured)...
	request := dc.BeginRequest("/d")
	// ...then an SSE reset retires the directory...
	dc.Invalidate("/d")
	// ...and the stale response arrives afterwards.
	view := dc.PutListing("/d", []CachedFileInfo{{Name: "resurrected.dat"}}, request)

	// The request still gets its own answer.
	if !containsName(viewNames(view), "resurrected.dat") {
		t.Fatalf("the issuing request must still receive its response: %v", viewNames(view))
	}
	// But nothing was republished into the cache.
	if _, ok := dc.entries["/d"]; ok {
		t.Fatalf("a pre-retirement response was installed after the retirement: %v", cachedNames(t, dc, "/d"))
	}
	if got := dc.Lookup("/d", "resurrected.dat"); got.kind != namespaceLookupNone {
		t.Fatalf("retired directory resolved a name from a pre-retirement response: kind=%v", got.kind)
	}
}

// A listing issued after a retirement installs normally: the refusal is about
// ordering, not about the directory being poisoned.
func TestDirCacheListingAfterRetirementInstalls(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "seed.dat"}})
	dc.Invalidate("/d")

	request := dc.BeginRequest("/d")
	dc.PutListing("/d", []CachedFileInfo{{Name: "fresh.dat"}}, request)

	if names := cachedNames(t, dc, "/d"); !containsName(names, "fresh.dat") {
		t.Fatalf("a listing issued after the retirement was refused: %v", names)
	}
}

// At the cache cap, a name this mount committed must not be the one displaced to
// cache a server name: the committed child stays, and the entry stops claiming
// it can answer misses for whatever it could not hold.
func TestDirCacheCapKeepsCommittedChildVisibleWithoutHoldingIt(t *testing.T) {
	const cap = 2
	dc := NewNamespaceCache(10*time.Second, 10*time.Second, cap)
	// The committed name is cached FIRST, so it sits at the head of the
	// eviction order. A policy that simply drops order[0] takes exactly the
	// name the race protection exists to keep.
	dc.Upsert("/d", CachedFileInfo{Name: "committed.dat", Revision: 2})
	listInstall(dc, "/d", []CachedFileInfo{{Name: "server.dat"}})

	view := listInstall(dc, "/d", []CachedFileInfo{{Name: "server.dat"}, {Name: "other.dat"}})

	// The request being answered must not be handed a reply that hides the
	// committed child, even though the cap forced it out of the cache.
	if !containsName(viewNames(view), "committed.dat") {
		t.Fatalf("the served view dropped the committed child: %v", viewNames(view))
	}
	// maxEntries stays a hard bound, so the cache itself holds only the cap.
	if got := len(dc.entries["/d"].items); got > cap {
		t.Fatalf("cache grew past maxEntries: %d > %d", got, cap)
	}
	// And the entry must not answer the evicted name as absent: without miss
	// authority the lookup reaches the server and finds it.
	if dc.CanAnswerMisses("/d") {
		t.Fatalf("an entry that could not hold the directory still answers misses: %v", cachedNames(t, dc, "/d"))
	}
	if got := dc.Lookup("/d", "committed.dat"); got.kind == namespaceLookupCompleteMiss || got.kind == namespaceLookupSessionMiss {
		t.Fatalf("evicted committed child answered as absent: kind=%v", got.kind)
	}
}

// When merging evicts or the cap truncates, the listing must not claim to be
// complete: a complete-listing miss is a false ENOENT for a name that exists.
func TestDirCacheInstallIsNotCompleteWhenTheCacheCouldNotHoldIt(t *testing.T) {
	const cap = 2
	dc := NewNamespaceCache(10*time.Second, 10*time.Second, cap)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "server.dat"}})

	// Force an eviction by caching a locally owned name first, at the head of
	// the eviction order.
	dc.Upsert("/d", CachedFileInfo{Name: "committed.dat", Revision: 2})
	listInstall(dc, "/d", []CachedFileInfo{{Name: "server.dat"}})
	listInstall(dc, "/d", []CachedFileInfo{{Name: "server.dat"}, {Name: "other.dat"}})

	if dc.CanAnswerMisses("/d") {
		t.Fatalf("listing claims completeness though the cache could not hold it: %v", cachedNames(t, dc, "/d"))
	}
}

// A retirement watermark must hold until the directory moves on, not until some
// wall-clock window lapses: a list request has no client-side deadline, so a
// response arriving after such a window would be reinstated as though the
// retirement had never happened.
func TestDirCacheRetirementWatermarkOutlivesAnyRead(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "f.dat", Size: 1, Revision: 1, ResourceID: "res-B"}})

	request := dc.BeginRequest("/d")
	dc.Invalidate("/d")
	// A local write afterwards must not discard the watermark: the older
	// response is still older than the retirement, for the names it carries.
	dc.Upsert("/d", CachedFileInfo{Name: "other.dat", Revision: 9})
	dc.PutListing("/d", []CachedFileInfo{{Name: "f.dat", Size: 1, Revision: 1, ResourceID: "res-B"}}, request)

	if names := cachedNames(t, dc, "/d"); containsName(names, "f.dat") {
		t.Fatalf("a pre-retirement response was reinstated after a local write: %v", names)
	}
	if got := dc.Lookup("/d", "f.dat"); got.kind == namespaceLookupPositive {
		t.Fatalf("retired identity resolved after a pre-retirement response: res=%q", got.item.ResourceID)
	}
}

// A listing issued after a retirement installs normally, and the fresh entry
// continues to fence responses older than it.
func TestDirCacheListingAfterRetirementStillFencesOlder(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "seed.dat"}})

	older := dc.BeginRequest("/d")
	dc.Invalidate("/d")
	listInstall(dc, "/d", []CachedFileInfo{{Name: "fresh.dat"}})
	dc.PutListing("/d", []CachedFileInfo{{Name: "resurrected.dat"}}, older)

	if names := cachedNames(t, dc, "/d"); containsName(names, "resurrected.dat") {
		t.Fatalf("pre-retirement response installed over a post-retirement one: %v", names)
	}
}

// An oversized install after a complete listing must clear completeness: the
// entry may still hold a valid-looking complete flag from the smaller listing,
// and a complete miss for a name the cache could not hold is a false ENOENT.
func TestDirCacheOversizedInstallClearsStaleCompleteness(t *testing.T) {
	const cap = 2
	dc := NewNamespaceCache(10*time.Second, 10*time.Second, cap)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "a.dat"}, {Name: "b.dat"}})
	if !dc.CanAnswerMisses("/d") {
		t.Fatal("fixture: a complete listing should answer misses")
	}

	listInstall(dc, "/d", []CachedFileInfo{{Name: "a.dat"}, {Name: "b.dat"}, {Name: "c.dat"}})

	if dc.CanAnswerMisses("/d") {
		t.Fatalf("oversized install kept stale completeness: %v", cachedNames(t, dc, "/d"))
	}
	if got := dc.Lookup("/d", "c.dat"); got.kind == namespaceLookupCompleteMiss || got.kind == namespaceLookupSessionMiss {
		t.Fatalf("a name the cache could not hold was answered as absent: kind=%v", got.kind)
	}
}

// When every cached name is locally owned the cap has nothing it may displace,
// so a further authoritative write is left uncached (maxEntries stays a hard
// bound) and the entry loses its miss authority, so a lookup for that name
// reaches the server instead of being answered "absent" from a cache that
// simply could not hold it.
func TestDirCacheAllLocalSaturationRevokesMissAuthority(t *testing.T) {
	const cap = 2
	dc := NewNamespaceCache(10*time.Second, 10*time.Second, cap)
	dc.MarkSessionCreatedDir("/d")

	for _, name := range []string{"a.dat", "b.dat", "c.dat"} {
		dc.Upsert("/d", CachedFileInfo{Name: name, Revision: 1})
	}

	if got := len(dc.entries["/d"].items); got > cap {
		t.Fatalf("authoritative writes grew the cache past maxEntries: %d > %d", got, cap)
	}
	if dc.CanAnswerMisses("/d") {
		t.Fatal("an over-cap entry still claims to answer misses")
	}
	got := dc.Lookup("/d", "c.dat")
	if got.kind == namespaceLookupCompleteMiss || got.kind == namespaceLookupSessionMiss {
		t.Fatalf("a name the cache could not hold was answered as absent: kind=%v", got.kind)
	}
}

// An older concurrent response must not downgrade what a newer install already
// published. Listings overlap, so response order is not request order.
func TestDirCacheOlderInstallDoesNotDowngradeNewer(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "f.dat", Size: 10, Revision: 1}})

	// Request A is issued, then request B, and B's response lands first.
	older := dc.BeginRequest("/d")
	listInstall(dc, "/d", []CachedFileInfo{{Name: "f.dat", Size: 99, Revision: 9}})

	// A's response now arrives carrying the older metadata.
	view := dc.PutListing("/d", []CachedFileInfo{{Name: "f.dat", Size: 10, Revision: 1}}, older)

	assertCachedChild(t, dc, "/d", "f.dat", func(item CachedFileInfo) {
		if item.Size != 99 || item.Revision != 9 {
			t.Fatalf("older install downgraded the cache: size=%d rev=%d, want 99/9", item.Size, item.Revision)
		}
	})
	for _, item := range view {
		if item.Name == "f.dat" && (item.Size != 99 || item.Revision != 9) {
			t.Fatalf("older install served downgraded metadata: size=%d rev=%d", item.Size, item.Revision)
		}
	}
}

// A lapsed listing cannot be the additive base. An unrelated local write keeps
// the entry alive past the listing's own expiry, so an additive merge would
// replay a remotely deleted name indefinitely and renew it on every install.
func TestDirCacheExpiredListingDoesNotRetainRemoteDeletions(t *testing.T) {
	dc, clock := newControlledCache(80*time.Millisecond, 80*time.Millisecond, defaultNamespaceCacheMaxEntries)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "gone.dat"}, {Name: "keep.dat"}})

	// The listing lapses, then an unrelated local write refreshes the entry.
	clock.Advance(150 * time.Millisecond)
	dc.Upsert("/d", CachedFileInfo{Name: "other.dat", Revision: 2})

	// A fresh listing omits the remotely deleted name.
	view := listInstall(dc, "/d", []CachedFileInfo{{Name: "keep.dat"}, {Name: "other.dat"}})

	if names := cachedNames(t, dc, "/d"); containsName(names, "gone.dat") {
		t.Fatalf("lapsed listing retained a remotely deleted name: %v", names)
	}
	if containsName(viewNames(view), "gone.dat") {
		t.Fatalf("lapsed listing served a remotely deleted name: %v", viewNames(view))
	}
	// Names this mount changed after the request are still carried over.
	if !containsName(cachedNames(t, dc, "/d"), "other.dat") {
		t.Fatalf("local commit lost by the rebuild: %v", cachedNames(t, dc, "/d"))
	}
}

// Tenth review round: the same lapse rule has to cover an *incomplete* listing.
// An over-cap response never sets the complete marker, so nothing but the
// listing's own expiry can retire the prefix it cached — and an unrelated local
// write refreshing the entry's TTL must not extend that prefix's life either.
func TestDirCacheExpiredPartialListingDropsUnrelistedName(t *testing.T) {
	const cap = 3
	dc, clock := newControlledCache(80*time.Millisecond, 80*time.Millisecond, cap)

	// Over-cap: cached as the bounded prefix, and not complete.
	listInstall(dc, "/d", []CachedFileInfo{
		{Name: "keep1.dat"}, {Name: "gone.dat"}, {Name: "keep2.dat"}, {Name: "uncached.dat"},
	})
	if _, ok := dc.Get("/d"); ok {
		t.Fatal("fixture: an over-cap listing must not be served as complete")
	}

	// The listing lapses; an unrelated local write refreshes the entry's TTL and
	// evicts keep1.dat (FIFO) but leaves gone.dat in place.
	clock.Advance(150 * time.Millisecond)
	dc.Upsert("/d", CachedFileInfo{Name: "local.dat", Revision: 2})

	view := listInstall(dc, "/d", []CachedFileInfo{{Name: "keep2.dat"}, {Name: "local.dat"}})

	if names := cachedNames(t, dc, "/d"); containsName(names, "gone.dat") {
		t.Fatalf("lapsed partial listing retained a remotely deleted name: %v", names)
	}
	if containsName(viewNames(view), "gone.dat") {
		t.Fatalf("lapsed partial listing served a remotely deleted name: %v", viewNames(view))
	}
	served := viewNames(view)
	if !containsName(served, "local.dat") || !containsName(served, "keep2.dat") {
		t.Fatalf("view lost names the cache must keep: %v", served)
	}
	// A lookup for the lapsed name must not answer from the cache either.
	if got := dc.Lookup("/d", "gone.dat"); got.kind == namespaceLookupPositive {
		t.Fatalf("lapsed partial listing answered a positive lookup: %v", got.kind)
	}
}

// A name the response re-lists gets a fresh deadline: a live directory whose
// every listing carries it must not lose it to the first listing's expiry.
func TestDirCacheReListedNameOutlivesItsFirstDeadline(t *testing.T) {
	dc, clock := newControlledCache(80*time.Millisecond, 80*time.Millisecond, defaultNamespaceCacheMaxEntries)

	listInstall(dc, "/d", []CachedFileInfo{{Name: "live.dat"}})
	clock.Advance(50 * time.Millisecond)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "live.dat"}})
	// Past the first install's deadline, inside the second's.
	clock.Advance(50 * time.Millisecond)

	if names := cachedNames(t, dc, "/d"); !containsName(names, "live.dat") {
		t.Fatalf("re-listed name retired by its first deadline: %v", names)
	}
	if got := dc.Lookup("/d", "live.dat"); got.kind != namespaceLookupPositive {
		t.Fatalf("lookup of a re-listed name = %v, want positive", got.kind)
	}
}

// A listing lapse retires what a *response* supplied, never what this mount
// wrote: the entry's own TTL is what says the cache is still in use, and a
// commit settled here is invisible to the server's listing for as long as it
// has not landed, so rebuilding around the response alone would hide it
// (the bug #966 reports).
func TestDirCacheListingLapseKeepsLocalNames(t *testing.T) {
	dc, clock := newControlledCache(80*time.Millisecond, 80*time.Millisecond, defaultNamespaceCacheMaxEntries)

	listInstall(dc, "/d", []CachedFileInfo{{Name: "remote.dat"}})
	// Past the listing's deadline, a local write extends the entry's own TTL
	// without touching the listing's.
	clock.Advance(120 * time.Millisecond)
	dc.Upsert("/d", CachedFileInfo{Name: "acked.dat", Revision: 3})

	// The response cannot carry acked.dat: no LIST has run since the commit.
	view := listInstall(dc, "/d", []CachedFileInfo{{Name: "remote2.dat"}})

	for _, names := range [][]string{cachedNames(t, dc, "/d"), viewNames(view)} {
		if !containsName(names, "acked.dat") {
			t.Fatalf("listing lapse hid a locally committed name: %v", names)
		}
	}
	if names := cachedNames(t, dc, "/d"); containsName(names, "remote.dat") {
		t.Fatalf("lapsed listing retained a remotely deleted name: %v", names)
	}
}

// The same for a name the mount did not create but did write: a listing first
// supplied it, the mount then committed new content over it, so the cached
// value is the commit's and a listing deadline passing must not retire it.
// The response that lands after the lapse omits the name — the stale-listing
// window the mount already guards elsewhere (see markPathDeleted) — and the
// served view still has to carry what the mount committed.
func TestDirCacheListingLapseKeepsLocallyRewrittenName(t *testing.T) {
	dc, clock := newControlledCache(80*time.Millisecond, 80*time.Millisecond, defaultNamespaceCacheMaxEntries)

	listInstall(dc, "/d", []CachedFileInfo{{Name: "f.dat", Size: 1, Revision: 1}, {Name: "gone.dat"}})
	clock.Advance(120 * time.Millisecond)
	// No request in flight, so the write carries no stamp of its own: only its
	// provenance can keep it across the lapse.
	dc.Upsert("/d", CachedFileInfo{Name: "f.dat", Size: 2, Revision: 2})

	view := listInstall(dc, "/d", []CachedFileInfo{{Name: "other.dat"}})

	assertCachedChild(t, dc, "/d", "f.dat", func(item CachedFileInfo) {
		if item.Size != 2 || item.Revision != 2 {
			t.Fatalf("lapse dropped a locally rewritten name's value: size=%d rev=%d", item.Size, item.Revision)
		}
	})
	if !containsName(viewNames(view), "f.dat") {
		t.Fatalf("lapse hid a locally rewritten name from readdir: %v", viewNames(view))
	}
	if names := cachedNames(t, dc, "/d"); containsName(names, "gone.dat") {
		t.Fatalf("lapse retained a response name the new response omits: %v", names)
	}
}

// A stat observation is newer evidence than the listing it replaces, so it must
// outlive that listing's deadline too: a chmod a remote actor made is carried by
// an observation (the revision does not advance), and dropping it would revert
// the mode until the next listing reinstates it.
func TestDirCacheListingLapseKeepsObservedName(t *testing.T) {
	dc, clock := newControlledCache(80*time.Millisecond, 80*time.Millisecond, defaultNamespaceCacheMaxEntries)

	listInstall(dc, "/d", []CachedFileInfo{{Name: "f.dat", Mode: 0o644, HasMode: true, Revision: 1}})
	clock.Advance(120 * time.Millisecond)

	token := dc.BeginRequest("/d")
	dc.Observe("/d", CachedFileInfo{Name: "f.dat", Mode: 0o600, HasMode: true, Revision: 1}, token)

	view := listInstall(dc, "/d", []CachedFileInfo{{Name: "other.dat"}})

	assertCachedChild(t, dc, "/d", "f.dat", func(item CachedFileInfo) {
		if !item.HasMode || item.Mode != 0o600 {
			t.Fatalf("lapse dropped an observed mode: hasMode=%t mode=%o", item.HasMode, item.Mode)
		}
	})
	if !containsName(viewNames(view), "f.dat") {
		t.Fatalf("lapse hid an observed name from readdir: %v", viewNames(view))
	}
}

// --- Eleventh review round on #966: who renews a name, and until when -------

// A second listing must not renew a name it does not carry. The deadline
// belongs to the name, so the response that stopped listing it says nothing
// about it — and a directory listed more often than the TTL must still let a
// remotely deleted name go: otherwise continuous readdir keeps it alive
// forever, which is unbounded staleness the spec does not permit.
func TestDirCacheListingDoesNotRenewAnOmittedName(t *testing.T) {
	dc, clock := newControlledCache(80*time.Millisecond, 80*time.Millisecond, defaultNamespaceCacheMaxEntries)

	listInstall(dc, "/d", []CachedFileInfo{{Name: "gone.dat"}, {Name: "live.dat"}})

	// A second listing 50ms later: inside the first deadline, and it carries
	// only live.dat, so gone.dat keeps the deadline the *first* response gave it.
	clock.Advance(50 * time.Millisecond)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "live.dat"}})

	// Past the first response's deadline (t0+80ms), inside the second's.
	clock.Advance(50 * time.Millisecond)

	if names := cachedNames(t, dc, "/d"); containsName(names, "gone.dat") {
		t.Fatalf("a listing renewed a name it does not carry: %v", names)
	}
	if got := dc.Lookup("/d", "gone.dat"); got.kind == namespaceLookupPositive {
		t.Fatal("an omitted name still answered a positive lookup past its own deadline")
	}
	// The name the second response did carry stays.
	if got := dc.Lookup("/d", "live.dat"); got.kind != namespaceLookupPositive {
		t.Fatalf("a re-listed name = %v, want positive", got.kind)
	}
}

// A stat observation is a remote read, so it expires on its own clock: it must
// not turn into mount-authoritative state. Promoting it left a remote chmod or
// delete that SSE did not report able to outlive every TTL the cache has.
func TestDirCacheObservationExpiresOnItsOwnDeadline(t *testing.T) {
	dc, clock := newControlledCache(60*time.Millisecond, 60*time.Millisecond, defaultNamespaceCacheMaxEntries)

	request := dc.BeginRequest("/d")
	dc.Observe("/d", CachedFileInfo{Name: "observed.dat", Revision: 1}, request)

	// Past the observation's deadline, an unrelated local write refreshes
	// nothing but its own name.
	clock.Advance(100 * time.Millisecond)
	dc.Upsert("/d", CachedFileInfo{Name: "local.dat", Revision: 2})

	view := listInstall(dc, "/d", []CachedFileInfo{{Name: "local.dat"}})

	if names := cachedNames(t, dc, "/d"); containsName(names, "observed.dat") {
		t.Fatalf("an expired observation survived an unrelated local write: %v", names)
	}
	if containsName(viewNames(view), "observed.dat") {
		t.Fatalf("view served an expired observation: %v", viewNames(view))
	}
	if got := dc.Lookup("/d", "observed.dat"); got.kind == namespaceLookupPositive {
		t.Fatal("an expired observation still answered a positive lookup")
	}
}

// A local commit, by contrast, carries no deadline: it is this mount's own
// state, and neither a listing omitting it nor the passage of time may retire
// it — that omission is exactly the #966 invisibility. Only invalidation says
// otherwise, because only invalidation reflects the server's word.
func TestDirCacheLocalNameOutlivesEveryRemoteDeadline(t *testing.T) {
	dc, clock := newControlledCache(60*time.Millisecond, 60*time.Millisecond, defaultNamespaceCacheMaxEntries)

	dc.Upsert("/d", CachedFileInfo{Name: "acked.dat", Revision: 3})

	// Many listings that never carry the committed name, spread over several
	// TTLs: a commit whose upload is still in flight is invisible to each.
	for range 4 {
		clock.Advance(100 * time.Millisecond)
		view := listInstall(dc, "/d", []CachedFileInfo{{Name: "later.dat"}})
		if !containsName(viewNames(view), "acked.dat") {
			t.Fatalf("a listing hid a locally committed name: %v", viewNames(view))
		}
	}

	if got := dc.Lookup("/d", "acked.dat"); got.kind != namespaceLookupPositive {
		t.Fatalf("lookup of a locally committed name = %v, want positive", got.kind)
	}
	if names := cachedNames(t, dc, "/d"); !containsName(names, "acked.dat") {
		t.Fatalf("a locally committed name aged out: %v", names)
	}

	// Invalidation is what retires it.
	dc.Invalidate("/d")
	if got := dc.Lookup("/d", "acked.dat"); got.kind == namespaceLookupPositive {
		t.Fatal("invalidation left a locally committed name positive")
	}
}

// The two expiries an entry has are independent: a name's own deadline expiring
// drops that name without claiming anything about the directory, so a second
// name whose own evidence is still running must survive it — and the entry must
// not keep answering misses for the dropped one.
func TestDirCachePerNameDeadlinesAreIndependent(t *testing.T) {
	dc, clock := newControlledCache(80*time.Millisecond, 80*time.Millisecond, defaultNamespaceCacheMaxEntries)

	// observed.dat is seen at t0, keep.dat is listed at t0+50ms.
	request := dc.BeginRequest("/d")
	dc.Observe("/d", CachedFileInfo{Name: "observed.dat", Revision: 1}, request)
	clock.Advance(50 * time.Millisecond)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "keep.dat"}})

	// Past the observation's deadline, inside the listing's.
	clock.Advance(50 * time.Millisecond)

	if names := cachedNames(t, dc, "/d"); containsName(names, "observed.dat") {
		t.Fatalf("an expired observation outlived its deadline while a listing was fresh: %v", names)
	}
	if got := dc.Lookup("/d", "keep.dat"); got.kind != namespaceLookupPositive {
		t.Fatalf("a fresh name = %v, want positive", got.kind)
	}
	// The dropped name must fall through to the server, not be answered absent:
	// the cache pruned it because its evidence ran out, which is not a statement
	// that the server no longer has it.
	if got := dc.Lookup("/d", "observed.dat"); got.kind != namespaceLookupPartialMiss {
		t.Fatalf("lookup of an expired name = %v, want partial miss (remote fallback)", got.kind)
	}
}

// The expiry guard must stay O(1) on the read path, which means an entry has to
// know the earliest deadline it is holding: too late and a name outlives its
// evidence, too early only costs a scan. This pins the hint's two ends — it
// tracks the earliest live deadline, and it goes back to zero once nothing with
// a deadline is left.
func TestDirCacheFreshnessHintTracksTheEarliestDeadline(t *testing.T) {
	dc := NewNamespaceCache(10*time.Second, 10*time.Second, defaultNamespaceCacheMaxEntries)

	// A locally committed name has no deadline, so it must not wake the guard.
	dc.Upsert("/d", CachedFileInfo{Name: "local.dat"})
	entry := dc.entries["/d"]
	if !entry.freshnessHint.IsZero() {
		t.Fatalf("a deadline-free write armed the expiry guard: %v", entry.freshnessHint)
	}

	listInstall(dc, "/d", []CachedFileInfo{{Name: "remote.dat"}})
	listDeadline := entry.freshnessHint
	if listDeadline.IsZero() {
		t.Fatal("an installed listing did not arm the expiry guard")
	}

	// An observation is a later deadline, so it must not push the guard out.
	request := dc.BeginRequest("/d")
	dc.Observe("/d", CachedFileInfo{Name: "observed.dat", Revision: 1}, request)
	if entry.freshnessHint.After(listDeadline) {
		t.Fatalf("a later deadline moved the guard past the earliest one: %v > %v", entry.freshnessHint, listDeadline)
	}

	// Expire everything: the guard must be disarmed, not left pointing at a
	// deadline that is already behind us, or every read would scan forever.
	dc.mu.Lock()
	for name, item := range entry.items {
		if item.freshUntil.IsZero() {
			continue
		}
		item.freshUntil = time.Now().Add(-time.Second)
		entry.items[name] = item
	}
	entry.freshnessHint = time.Now().Add(-time.Second)
	dc.mu.Unlock()
	if names := cachedNames(t, dc, "/d"); containsName(names, "remote.dat") {
		t.Fatalf("expired names survived a prune: %v", names)
	}
	if !entry.freshnessHint.IsZero() {
		t.Fatalf("the expiry guard stayed armed with nothing left to expire: %v", entry.freshnessHint)
	}
	// The locally committed name has no deadline and must still be there.
	if !containsName(cachedNames(t, dc, "/d"), "local.dat") {
		t.Fatalf("a deadline-free name was dropped: %v", cachedNames(t, dc, "/d"))
	}
}

// --- Thirteenth review round on #966: an install fence that outlives its entry --

// The install fence must be written where it survives its own entry. A newer
// install can be the last state the entry holds, and if every deadline on it
// then passes, an ordinary read drops the whole entry — including the sequence
// that would have told the still-outstanding older request it lost. That request
// finds a fresh entry with installSeq 0, so it republishes a namespace the newer
// listing had already superseded, for a full TTL. LIST RTT has no upper bound
// below the cache TTL, so this is not a timing accident that a shorter window
// avoids.
func TestDirCacheStaleInstallFenceOutlivesItsEntry(t *testing.T) {
	dc, clock := newControlledCache(time.Minute, time.Minute, defaultNamespaceCacheMaxEntries)

	// A is issued first, B second, B installs first.
	older := dc.BeginRequest("/d")
	listInstall(dc, "/d", []CachedFileInfo{{Name: "live.dat"}})

	// Every deadline on the entry passes, and an ordinary read drops it.
	clock.Advance(2 * time.Minute)
	if _, ok := dc.Get("/d"); ok {
		t.Fatal("fixture: the entry should have expired")
	}

	// A finally returns, carrying a name B's listing had already removed.
	view := dc.PutListing("/d", []CachedFileInfo{{Name: "deleted.dat"}}, older)

	if containsName(viewNames(view), "deleted.dat") {
		t.Fatalf("a superseded response served its pre-B namespace: %v", viewNames(view))
	}
	if got := dc.Lookup("/d", "deleted.dat"); got.kind == namespaceLookupPositive {
		t.Fatal("a superseded response republished a name the newer listing had removed")
	}
	// Nothing of A's response may be cached, and A must not be able to answer
	// misses on B's behalf now that the fence is intact.
	if names := cachedNames(t, dc, "/d"); containsName(names, "deleted.dat") {
		t.Fatalf("cache republished a superseded name: %v", names)
	}
	if dc.CanAnswerMisses("/d") {
		t.Fatal("a superseded response granted miss authority after its entry expired")
	}
}

// The same fence, without the entry expiring: nothing about the guard may be
// load-bearing on the entry still being alive.
func TestDirCacheStaleInstallFenceWhileEntryLives(t *testing.T) {
	dc, _ := newControlledCache(time.Minute, time.Minute, defaultNamespaceCacheMaxEntries)

	older := dc.BeginRequest("/d")
	listInstall(dc, "/d", []CachedFileInfo{{Name: "live.dat"}})

	view := dc.PutListing("/d", []CachedFileInfo{{Name: "deleted.dat"}}, older)

	if containsName(viewNames(view), "deleted.dat") {
		t.Fatalf("a superseded response served its pre-B namespace: %v", viewNames(view))
	}
	if names := cachedNames(t, dc, "/d"); containsName(names, "deleted.dat") {
		t.Fatalf("cache republished a superseded name: %v", names)
	}
}

// Once every request that could consult the fence has settled, it goes away:
// the guard is concurrency-bounded, not history-bounded.
func TestDirCacheInstallFenceIsReleasedWithItsLastRequest(t *testing.T) {
	dc, _ := newControlledCache(time.Minute, time.Minute, defaultNamespaceCacheMaxEntries)

	older := dc.BeginRequest("/d")
	listInstall(dc, "/d", []CachedFileInfo{{Name: "live.dat"}})

	if got := len(dc.installFloor); got != 1 {
		t.Fatalf("expected the fence to be recorded while a request is outstanding, got %d", got)
	}

	dc.EndRequest(older)

	if got := len(dc.installFloor); got != 0 {
		t.Fatalf("fence records outlived the requests that could consult them: %d", got)
	}
}

// A name this mount deleted after the request must not come back through the
// reply of the response that predates the removal.
func TestDirCacheStaleInstallStillHidesLocallyRemovedNames(t *testing.T) {
	const cap = 2
	dc, _ := newControlledCache(time.Minute, time.Minute, cap)

	older := dc.BeginRequest("/d")
	dc.Remove("/d", "gone.dat")

	var over []CachedFileInfo
	for i := range cap * 3 {
		over = append(over, CachedFileInfo{Name: fmt.Sprintf("new_%02d.dat", i), Revision: 1})
	}
	listInstall(dc, "/d", over)

	view := dc.PutListing("/d", []CachedFileInfo{{Name: "gone.dat"}}, older)

	if containsName(viewNames(view), "gone.dat") {
		t.Fatalf("stale response served a locally removed name: %v", viewNames(view))
	}
}

// --- Twelfth review round on #966: bulk expiry and stale completeness -------

// A response that lost the race must not speak for the directory's
// completeness. The newer install owns that authority, and an older response
// restoring it turns names the newer listing proved to exist — but could not
// cache, being past the cap — into false ENOENT.
func TestDirCacheStaleInstallDoesNotRestoreCompleteness(t *testing.T) {
	const cap = 2
	dc := NewNamespaceCache(10*time.Second, 10*time.Second, cap)

	// Request A is issued first, request B second.
	older := dc.BeginRequest("/d")

	// B lands first, over-cap: the cache holds only a prefix of it and must
	// therefore not answer misses.
	over := make([]CachedFileInfo, 0, cap*3)
	for i := range cap * 3 {
		over = append(over, CachedFileInfo{Name: fmt.Sprintf("f_%02d.dat", i), Revision: 1})
	}
	listInstall(dc, "/d", over)

	// A lands late carrying one name. It is stale for every cache effect.
	dc.PutListing("/d", []CachedFileInfo{{Name: "f_00.dat", Revision: 1}}, older)

	if dc.CanAnswerMisses("/d") {
		t.Fatal("a stale install restored the complete authority the newer listing revoked")
	}
	// A name the newer listing carried but the cache could not hold must reach
	// the server, not be answered absent.
	if got := dc.Lookup("/d", "f_05.dat"); got.kind != namespaceLookupPartialMiss {
		t.Fatalf("lookup of an uncached name = %v, want partial miss (remote fallback)", got.kind)
	}
}

// The mirror: a stale install must not *revoke* completeness either. The newer
// listing fit in the cache, so it may answer misses, and the older over-cap
// response arriving afterwards may not take that away.
func TestDirCacheStaleInstallDoesNotRevokeCompleteness(t *testing.T) {
	const cap = 4
	dc := NewNamespaceCache(10*time.Second, 10*time.Second, cap)

	older := dc.BeginRequest("/d")
	listInstall(dc, "/d", []CachedFileInfo{{Name: "a.dat"}, {Name: "b.dat"}})

	over := make([]CachedFileInfo, 0, cap*2)
	for i := range cap * 2 {
		over = append(over, CachedFileInfo{Name: fmt.Sprintf("f_%02d.dat", i), Revision: 1})
	}
	dc.PutListing("/d", over, older)

	if !dc.CanAnswerMisses("/d") {
		t.Fatal("a stale install revoked the complete authority the newer listing established")
	}
}

// A listing gives every name it carries the same deadline, so the first read
// after that deadline expires a whole directory in one sweep. That sweep must
// stay linear in the directory size: a per-name removal rescans (and shifts) the
// ordering for each name expired, which is quadratic and runs while holding the
// cache mutex, blocking every other directory operation on the mount.
//
// The bound is absolute rather than a timing ratio because the two shapes are
// three orders of magnitude apart at this size: linear sweeps 100k names in
// tens of milliseconds, the quadratic form needs seconds.
func TestDirCacheBulkExpiryIsLinear(t *testing.T) {
	const n = 100000
	dc, clock := newControlledCache(time.Minute, time.Minute, n)

	response := make([]CachedFileInfo, 0, n)
	for i := range n {
		response = append(response, CachedFileInfo{Name: fmt.Sprintf("f_%07d.dat", i), Revision: 1})
	}
	request := dc.BeginRequest("/d")
	dc.PutListing("/d", response, request)
	clock.Advance(2 * time.Minute)

	start := time.Now()
	names := cachedNames(t, dc, "/d")
	elapsed := time.Since(start)

	if len(names) != 0 {
		t.Fatalf("expired directory kept %d names", len(names))
	}
	if elapsed > 2*time.Second {
		t.Fatalf("expiring %d names took %v: the sweep is not linear", n, elapsed)
	}
}

// BenchmarkDirCacheBulkExpiry measures the same sweep, so a change of shape
// shows up as a changed slope rather than a threshold crossing.
func BenchmarkDirCacheBulkExpiry(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			for b.Loop() {
				dc, clock := newControlledCache(time.Minute, time.Minute, n)
				response := make([]CachedFileInfo, 0, n)
				for i := range n {
					response = append(response, CachedFileInfo{Name: fmt.Sprintf("f_%07d.dat", i)})
				}
				request := dc.BeginRequest("/d")
				dc.PutListing("/d", response, request)
				clock.Advance(2 * time.Minute)
				// The sweep under test.
				dc.Get("/d")
			}
		})
	}
}

// --- Eighth review round on #966: bounded resource lifetime -----------------

// The reconciliation state must not accumulate per path a mount has merely
// invalidated. An invalidation with no request outstanding has no reader to
// fence, so it must not leave a permanent record, and a burst of distinct
// directories must leave the map exactly as it found it.
func TestDirCacheNoStateAccumulatesForAbsentPaths(t *testing.T) {
	dc := NewDirCache(10 * time.Second)

	// None of these paths is cached or has anything in flight.
	for i := range 10000 {
		dc.Invalidate(fmt.Sprintf("/never-cached/%05d", i))
	}

	if got := len(dc.retired); got != 0 {
		t.Fatalf("invalidating %d absent paths retained %d retirement records", 10000, got)
	}
	if got := len(dc.inFlight); got != 0 {
		t.Fatalf("in-flight registry grew to %d entries", got)
	}
}

// A retirement only has to fence requests that were already outstanding, so it
// is released with the last one: a long-running mount keeps no history.
func TestDirCacheRetirementReleasedWithLastRequest(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "f.dat", Size: 1, Revision: 1}})

	// Two overlapping requests, then the directory is retired.
	first := dc.BeginRequest("/d")
	second := dc.BeginRequest("/d")
	dc.Invalidate("/d")
	if got := len(dc.retired); got != 1 {
		t.Fatalf("fixture: expected the retirement to be recorded while reads are outstanding, got %d", got)
	}

	// The first completing request releases its own registration only.
	dc.EndRequest(first)
	if got := len(dc.retired); got != 1 {
		t.Fatalf("retirement released while a request was still outstanding (%d reads left)", len(dc.inFlight))
	}

	// The last one releases it: nothing can still consult it.
	dc.EndRequest(second)
	if got := len(dc.retired); got != 0 {
		t.Fatalf("retirement retained after the last request completed: %d records", got)
	}
	if got := len(dc.inFlight); got != 0 {
		t.Fatalf("in-flight registry not emptied: %d entries", got)
	}
}

// Per-name stamps are only readable by the requests that were outstanding when
// they were written; they are released with the last of them.
func TestDirCacheNameStampsReleasedWithLastRequest(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "seed.dat"}})

	// A request is outstanding, then a commit lands: the stamp matters only to
	// that request.
	request := dc.BeginRequest("/d")
	dc.Upsert("/d", CachedFileInfo{Name: "committed.dat", Revision: 2})
	if got := len(dc.entries["/d"].localGen); got == 0 {
		t.Fatal("fixture: expected the commit to stamp the name")
	}

	dc.EndRequest(request)

	if got := len(dc.entries["/d"].localGen); got != 0 {
		t.Fatalf("name stamps retained after the last request: %d", got)
	}
	// The committed name itself stays: released reconciliation state must not
	// take the cache entry with it.
	if !containsName(cachedNames(t, dc, "/d"), "committed.dat") {
		t.Fatalf("releasing stamps dropped cached state: %v", cachedNames(t, dc, "/d"))
	}
}

// maxEntries is a hard bound: repeated authoritative writes to a directory that
// is already at the cap must not grow it, and the entry must stop answering
// misses rather than report an uncached name as absent.
func TestDirCacheAuthoritativeWritesRespectMaxEntries(t *testing.T) {
	const cap = 4
	dc := NewNamespaceCache(10*time.Second, 10*time.Second, cap)
	dc.MarkSessionCreatedDir("/d")

	for i := range 100 {
		dc.Upsert("/d", CachedFileInfo{Name: fmt.Sprintf("f_%03d.dat", i), Revision: 1})
	}

	entry := dc.entries["/d"]
	if len(entry.items) > cap {
		t.Fatalf("authoritative writes grew the cache past maxEntries: %d > %d", len(entry.items), cap)
	}
	// Whatever it could not hold must not be answered as absent: without miss
	// authority the lookup falls through to the server.
	if dc.CanAnswerMisses("/d") {
		t.Fatalf("an entry that could not hold the directory still answers misses: %v", cachedNames(t, dc, "/d"))
	}
	got := dc.Lookup("/d", "f_099.dat")
	if got.kind == namespaceLookupCompleteMiss || got.kind == namespaceLookupSessionMiss {
		t.Fatalf("uncached authoritative name answered as absent: kind=%v", got.kind)
	}
}

// Every request registration must be released on every exit path, including the
// retry loop and the early returns that never reach an install. A registration
// that outlives its request would pin the directory's reconciliation state for
// the mount's lifetime, which is the leak this lifetime scheme exists to avoid.
func TestRemotePathsReleaseEveryRegistration(t *testing.T) {
	var listCalls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list") != "" {
			listCalls.Add(1)
			http.Error(w, "list failed", http.StatusInternalServerError)
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)

	// Both list paths, repeatedly, all failing: the retry loop starts a new
	// registration per attempt, so a missing release shows up as a count.
	for range 5 {
		_, _, _ = fs.remoteDirectoryHasChildren(context.Background(), "/leak")
		_, _, _ = fs.lookupListWithRetry(nil, "/leak")
	}

	if got := len(fs.dirCache.inFlight); got != 0 {
		t.Fatalf("in-flight registrations leaked across failing requests: %d", got)
	}
	if got := len(fs.dirCache.retired); got != 0 {
		t.Fatalf("retirements accumulated for directories with no reader: %d", got)
	}
	if listCalls.Load() == 0 {
		t.Fatal("fixture: expected the list attempts to reach the server")
	}
}

// --- Ninth review round on #966: reclaim triggers and retry release ---------

// A mutation with no request in flight has no stamp trigger, because its only
// reader is a request that was already outstanding. Stamps must therefore be
// written only while one exists — otherwise a directory keeps one per name it
// has ever held, which at the hard cap is every name the cache refused to store.
func TestDirCacheNoStampsAccumulateWithoutRequests(t *testing.T) {
	const cap = 4
	dc := NewNamespaceCache(10*time.Second, 10*time.Second, cap)

	for i := range 1000 {
		dc.Upsert("/d", CachedFileInfo{Name: fmt.Sprintf("f_%04d.dat", i), Revision: 1})
	}

	entry := dc.entries["/d"]
	if len(entry.items) > cap {
		t.Fatalf("cache grew past maxEntries: %d > %d", len(entry.items), cap)
	}
	if got := len(entry.localGen); got != 0 {
		t.Fatalf("stamps accumulated with no request to consult them: %d (items=%d)", got, len(entry.items))
	}
}

// The same, for removals: a directory that only ever sees unlinks must not
// leave a stamp per removed name.
func TestDirCacheNoRemovalStampsAccumulateWithoutRequests(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "seed.dat"}})

	for i := range 100 {
		dc.Remove("/d", fmt.Sprintf("gone_%04d.dat", i))
	}

	if got := len(dc.entries["/d"].localGen); got != 0 {
		t.Fatalf("removal stamps accumulated with no request in flight: %d", got)
	}
}

// A stamp still has to exist while a request can consult it: a response taken
// before the change must still be fenced.
func TestDirCacheStampsWrittenWhileRequestOutstanding(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "seed.dat"}})

	request := dc.BeginRequest("/d")
	dc.Upsert("/d", CachedFileInfo{Name: "committed.dat", Revision: 2})
	if got := len(dc.entries["/d"].localGen); got == 0 {
		t.Fatal("no stamp written while a request was outstanding")
	}

	// The older response must not be able to speak for the committed name.
	dc.PutListing("/d", []CachedFileInfo{{Name: "other.dat"}}, request)
	if !containsName(cachedNames(t, dc, "/d"), "committed.dat") {
		t.Fatalf("committed child lost: %v", cachedNames(t, dc, "/d"))
	}
}

// Each retry attempt is a request of its own, and every exit from the loop must
// release it: the non-transient error return and the mount-view generation
// mismatch return both leave the loop directly.
func TestLookupListRetryReleasesTokenOnEveryExit(t *testing.T) {
	cases := []struct {
		name string
		// respond answers list attempts after the first (transient) failure.
		// It runs on the server side before the response is written, so the
		// mismatch case advances the mount view generation the way a
		// concurrent reset would and the install hits the EAGAIN exit.
		respond func(fs *Dat9FS, w http.ResponseWriter)
	}{
		{
			name: "non-transient error",
			respond: func(_ *Dat9FS, w http.ResponseWriter) {
				http.Error(w, "denied", http.StatusForbidden)
			},
		},
		{
			name: "success then generation mismatch",
			respond: func(fs *Dat9FS, w http.ResponseWriter) {
				fs.mountViewGeneration.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"entries":[{"name":"a.dat","size":1,"isDir":false}]}`))
			},
		},
		{
			name: "transient throughout",
			respond: func(_ *Dat9FS, w http.ResponseWriter) {
				http.Error(w, "transient", http.StatusInternalServerError)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			attempt := 0
			var fs *Dat9FS
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("list") == "" {
					http.NotFound(w, r)
					return
				}
				attempt++
				if attempt == 1 {
					http.Error(w, "transient", http.StatusInternalServerError)
					return
				}
				tc.respond(fs, w)
			}))
			defer ts.Close()

			opts := &MountOptions{}
			opts.setDefaults()
			fs = NewDat9FS(newTestClient(ts.URL), opts)

			_, _, _ = fs.lookupListWithRetry(nil, "/leak")

			if got := len(fs.dirCache.inFlight); got != 0 {
				t.Fatalf("retry left %d registration(s) behind", got)
			}
			if got := len(fs.dirCache.retired); got != 0 {
				t.Fatalf("retry left %d retirement(s) behind", got)
			}
		})
	}
}

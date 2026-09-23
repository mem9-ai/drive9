package fuse

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// This file covers issue #966: a directory listing that lands while a commit
// settles must not hide the committed child. The mechanism under test is the
// invariant that a listing install is additive — a name this mount committed
// cannot disappear from readdir because of a racing listing — plus the
// per-name stamps that keep a response from speaking for state it predates.

func cachedNames(t *testing.T, dc *DirCache, dirPath string) []string {
	t.Helper()
	entry := dc.entries[dirPath]
	if entry == nil {
		return nil
	}
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
	entry := dc.entries[dirPath]
	if entry == nil {
		t.Fatalf("expected an entry for %s", dirPath)
	}
	item, ok := entry.items[name]
	if !ok {
		t.Fatalf("child %s missing: names=%v", name, cachedNames(t, dc, dirPath))
	}
	check(item)
}

// listInstall mirrors production: capture the fencing value, then install the
// response that request produced.
func listInstall(dc *DirCache, dirPath string, items []CachedFileInfo) []CachedFileInfo {
	return dc.PutListing(dirPath, items, dc.Generation(dirPath))
}

// --- The core invariant: an install never removes a committed name ---------

// The reported bug, at the cache layer: a child committed during the listing's
// round trip must survive the install and reach the caller that fetched it.
func TestDirCacheInstallKeepsChildCommittedDuringRTT(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "a.dat"}})

	// The listing request is issued (its generation captured), then a commit
	// for b.dat settles during its RTT.
	requestGen := dc.Generation("/d")
	dc.Upsert("/d", CachedFileInfo{Name: "b.dat"})

	view := dc.PutListing("/d", []CachedFileInfo{{Name: "a.dat"}}, requestGen)
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
		requestGen := dc.Generation("/d")
		dc.Upsert("/d", CachedFileInfo{Name: name})
		// Every commit races an unrelated response taken before it.
		dc.PutListing("/d", []CachedFileInfo{{Name: "base.dat"}, {Name: "other.dat"}}, requestGen)
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

	requestGen := dc.Generation("/d")
	dc.Remove("/d", "gone.dat")
	view := dc.PutListing("/d", []CachedFileInfo{{Name: "gone.dat"}, {Name: "keep.dat"}}, requestGen)

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
	requestGen := dc.Generation("/d")
	dc.Remove("/d", "gone.dat")

	view := dc.PutListing("/d", []CachedFileInfo{{Name: "gone.dat"}}, requestGen)
	if containsName(viewNames(view), "gone.dat") {
		t.Fatalf("view resurrected a removal recorded before any cache entry: %v", viewNames(view))
	}
}

// The view carries the merged metadata for a name a local mutation moved
// during the RTT, not the response's older struct for that name.
func TestDirCacheInstallViewCarriesUpdatedSameName(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "f.dat", Size: 10, Revision: 1}})

	requestGen := dc.Generation("/d")
	dc.Upsert("/d", CachedFileInfo{Name: "f.dat", Size: 20, Revision: 2})
	view := dc.PutListing("/d", []CachedFileInfo{{Name: "f.dat", Size: 10, Revision: 1}}, requestGen)

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

	requestGen := dc.Generation("/d")
	dc.Remove("/d", "only.dat")
	view := dc.PutListing("/d", []CachedFileInfo{{Name: "only.dat"}}, requestGen)

	if len(view) != 0 {
		t.Fatalf("view carried a removed name: %v", viewNames(view))
	}
}

// Documented trade-off: because installs are additive, a name another actor
// deleted remotely leaves the cache through invalidation or expiry, not through
// the next listing. Pinning it here keeps the behaviour explicit.
func TestDirCacheRemoteDeleteLeavesOnExpiryNotOnInstall(t *testing.T) {
	dc := NewNamespaceCache(60*time.Millisecond, 60*time.Millisecond, defaultNamespaceCacheMaxEntries)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "gone.dat"}, {Name: "keep.dat"}})

	// A later response omitting gone.dat does not remove it.
	listInstall(dc, "/d", []CachedFileInfo{{Name: "keep.dat"}})
	if names := cachedNames(t, dc, "/d"); !containsName(names, "gone.dat") {
		t.Fatalf("fixture: expected the name to still be cached, names=%v", names)
	}

	// Expiry retires the whole entry, the documented fallback when a structural
	// operation's invalidation is not what removed it.
	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		dc.Get("/d")
		time.Sleep(10 * time.Millisecond)
	}
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

	token := dc.BeginObservation("/d")
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

	token := dc.BeginObservation("/d")
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

	token := dc.BeginObservation("/d")
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

	token := dc.BeginObservation("/d")
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

	token := dc.BeginObservation("/d")
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

	token := dc.BeginObservation("/d")
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

	token := dc.BeginObservation("/d")
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

	token := dc.BeginObservation("/d")
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

	token := dc.BeginObservation("/d")
	dc.Observe("/d", CachedFileInfo{Name: "f.dat", Size: 7, Revision: 1}, token)

	if got := dc.Lookup("/d", "f.dat"); got.kind != namespaceLookupPositive || got.item.Size != 7 {
		t.Fatalf("uncached read with no competing event was dropped: kind=%v item=%+v", got.kind, got.item)
	}
}

// A zero token carries no ordering information and is discarded entirely.
func TestDirCacheZeroObservationTokenIsDiscarded(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "keep.dat"}})

	dc.Observe("/d", CachedFileInfo{Name: "late.dat", Size: 9}, ObservationToken{})

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

	token := dc.BeginObservation("/d")
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
			dc.PutListing("/d", []CachedFileInfo{{Name: "f.dat", Revision: 3}}, dc.Generation("/d"))
		}},
		{"invalidate", func(dc *DirCache) { dc.Invalidate("/d") }},
	}
	for _, pre := range events {
		for _, post := range events {
			t.Run(pre.name+"_then_"+post.name, func(t *testing.T) {
				dc := NewDirCache(10 * time.Second)
				listInstall(dc, "/d", []CachedFileInfo{{Name: "seed.dat", Revision: 1}})
				pre.run(dc)

				token := dc.BeginObservation("/d")
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
		run  func(dc *DirCache, pending *[]uint64)
	}
	events := []event{
		{"install", func(dc *DirCache, pending *[]uint64) {
			// Install every response still outstanding, oldest first.
			for _, g := range *pending {
				dc.PutListing("/d", []CachedFileInfo{{Name: "resp.dat"}}, g)
			}
		}},
		{"commit", func(dc *DirCache, pending *[]uint64) {
			dc.Upsert("/d", CachedFileInfo{Name: "committed.dat", Revision: 2})
		}},
		{"remove", func(dc *DirCache, pending *[]uint64) {
			dc.Remove("/d", "victim.dat")
		}},
		{"newRequest", func(dc *DirCache, pending *[]uint64) {
			*pending = append(*pending, dc.Generation("/d"))
		}},
		{"invalidate", func(dc *DirCache, pending *[]uint64) { dc.Invalidate("/d") }},
	}
	seq := make([]int, 3)
	for i := range events {
		for j := range events {
			for k := range events {
				seq[0], seq[1], seq[2] = i, j, k
				dc := NewDirCache(10 * time.Second)
				listInstall(dc, "/d", []CachedFileInfo{{Name: "victim.dat"}, {Name: "resp.dat"}})
				pending := []uint64{}
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
	requestGen := dc.Generation("/d")
	// ...then an SSE reset retires the directory...
	dc.Invalidate("/d")
	// ...and the stale response arrives afterwards.
	view := dc.PutListing("/d", []CachedFileInfo{{Name: "resurrected.dat"}}, requestGen)

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

	requestGen := dc.Generation("/d")
	dc.PutListing("/d", []CachedFileInfo{{Name: "fresh.dat"}}, requestGen)

	if names := cachedNames(t, dc, "/d"); !containsName(names, "fresh.dat") {
		t.Fatalf("a listing issued after the retirement was refused: %v", names)
	}
}

// At the cache cap, a name this mount committed must not be the one displaced
// to make room for a name the server can simply list again. (Review round 3.)
func TestDirCacheEvictionNeverDropsLocallyCommittedName(t *testing.T) {
	const cap = 2
	// The committed name is cached FIRST, so it sits at the head of the
	// eviction order. A policy that simply drops order[0] takes exactly the
	// name the race protection exists to keep.
	dc := NewNamespaceCache(10*time.Second, 10*time.Second, cap)
	dc.Upsert("/d", CachedFileInfo{Name: "committed.dat", Revision: 2})
	dc.PutListing("/d", []CachedFileInfo{{Name: "server.dat"}}, 0)

	view := listInstall(dc, "/d", []CachedFileInfo{{Name: "server.dat"}, {Name: "other.dat"}})

	if !containsName(cachedNames(t, dc, "/d"), "committed.dat") {
		t.Fatalf("a committed child was evicted to cache a server name: %v", cachedNames(t, dc, "/d"))
	}
	if !containsName(viewNames(view), "committed.dat") {
		t.Fatalf("the view dropped the committed child: %v", viewNames(view))
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
	dc.PutListing("/d", []CachedFileInfo{{Name: "server.dat"}}, 0)
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

	requestGen := dc.Generation("/d")
	dc.Invalidate("/d")
	// A local write afterwards must not discard the watermark: the older
	// response is still older than the retirement, for the names it carries.
	dc.Upsert("/d", CachedFileInfo{Name: "other.dat", Revision: 9})
	dc.PutListing("/d", []CachedFileInfo{{Name: "f.dat", Size: 1, Revision: 1, ResourceID: "res-B"}}, requestGen)

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

	older := dc.Generation("/d")
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
// but an authoritative local write must still be admitted: hiding a child this
// mount just committed is worse than exceeding the cache bound. The entry also
// loses its miss authority, since it no longer holds the whole directory.
func TestDirCacheAllLocalSaturationKeepsCommittedChild(t *testing.T) {
	const cap = 2
	dc := NewNamespaceCache(10*time.Second, 10*time.Second, cap)
	dc.MarkSessionCreatedDir("/d")

	for _, name := range []string{"a.dat", "b.dat", "c.dat"} {
		dc.Upsert("/d", CachedFileInfo{Name: name, Revision: 1})
	}

	if !containsName(cachedNames(t, dc, "/d"), "c.dat") {
		t.Fatalf("committed child missing from an all-local saturated entry: %v", cachedNames(t, dc, "/d"))
	}
	if got := dc.Lookup("/d", "c.dat"); got.kind != namespaceLookupPositive {
		t.Fatalf("committed child resolved as absent: kind=%v", got.kind)
	}
	if dc.CanAnswerMisses("/d") {
		t.Fatal("an over-cap entry still claims to answer misses")
	}
}

// An older concurrent response must not downgrade what a newer install already
// published. Listings overlap, so response order is not request order.
func TestDirCacheOlderInstallDoesNotDowngradeNewer(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "f.dat", Size: 10, Revision: 1}})

	// Request A is issued, then request B, and B's response lands first.
	older := dc.Generation("/d")
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
	dc := NewNamespaceCache(80*time.Millisecond, 80*time.Millisecond, defaultNamespaceCacheMaxEntries)
	listInstall(dc, "/d", []CachedFileInfo{{Name: "gone.dat"}, {Name: "keep.dat"}})

	// The listing lapses, then an unrelated local write refreshes the entry.
	time.Sleep(150 * time.Millisecond)
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

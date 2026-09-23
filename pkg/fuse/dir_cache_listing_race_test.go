package fuse

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

func dirCacheNames(t *testing.T, dc *DirCache, dirPath string) []string {
	t.Helper()
	items, ok := dc.Get(dirPath)
	if !ok {
		return nil
	}
	names := make([]string, 0, len(items))
	for _, item := range items {
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

// A directory listing response is installed after its RTT. Any child recorded
// locally (Upsert) while that request was in flight must survive the install:
// the response cannot know about it, but the local commit already
// acknowledged it, so dropping it makes an already-durable file invisible to
// readdir until the listing expires.
func TestDirCacheListingInstallKeepsConcurrentUpsert(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	// Pre-existing cached state, installed by its own completed listing.
	dc.PutListing("/d", []CachedFileInfo{{Name: "a.dat"}}, dc.BeginListing("/d"))

	// A listing request is issued; its response will say {a.dat}.
	snapshot := dc.BeginListing("/d")

	// A commit for b.dat settles while that request is in flight.
	dc.Upsert("/d", CachedFileInfo{Name: "b.dat"})

	// The stale response arrives and is installed.
	view, installed := dc.PutListing("/d", []CachedFileInfo{{Name: "a.dat"}}, snapshot)
	if !installed {
		t.Fatal("install did not run for a live snapshot")
	}

	// The requesting caller must be served the reconciled view...
	outNames := make([]string, 0, len(view))
	for _, item := range view {
		outNames = append(outNames, item.Name)
	}
	if !containsName(outNames, "b.dat") {
		t.Fatalf("caller served the raw response and lost the committed child: view=%v", outNames)
	}
	// ... and the cache must agree for the next readdir.
	names := dirCacheNames(t, dc, "/d")
	if !containsName(names, "a.dat") {
		t.Fatalf("listing entry lost after install: names=%v", names)
	}
	if !containsName(names, "b.dat") {
		t.Fatalf("concurrently committed child dropped by listing install: names=%v", names)
	}
}

// A child removed from the remote namespace between the snapshot and the
// install must not be resurrected by the merge: the listing is authoritative
// for names it could know about.
func TestDirCacheListingInstallDropsRemotelyDeletedChild(t *testing.T) {
	dc := NewDirCache(10 * time.Second)

	dc.PutListing("/d", []CachedFileInfo{{Name: "a.dat"}, {Name: "gone.dat"}}, ListingSnapshot{})
	snapshot := dc.BeginListing("/d")

	// The remote file is deleted by another actor while the request is in
	// flight; no local Upsert happens for it.
	dc.PutListing("/d", []CachedFileInfo{{Name: "a.dat"}}, snapshot)

	names := dirCacheNames(t, dc, "/d")
	if containsName(names, "gone.dat") {
		t.Fatalf("remotely deleted child resurrected by listing install: names=%v", names)
	}
	if !containsName(names, "a.dat") {
		t.Fatalf("surviving child lost: names=%v", names)
	}
}

// Listing validity must run on the listing's own clock. Unrelated Upserts
// refresh the positive-entry TTL, and using that same field to gate cached
// listings lets one stale response stay authoritative for as long as the
// directory keeps receiving commits.
func TestDirCacheUpsertDoesNotExtendListingValidity(t *testing.T) {
	dc := NewNamespaceCache(200*time.Millisecond, 200*time.Millisecond, defaultNamespaceCacheMaxEntries)

	snapshot := dc.BeginListing("/d")
	dc.PutListing("/d", []CachedFileInfo{{Name: "a.dat"}}, snapshot)

	// Keep the directory "busy": unrelated commits land continuously for
	// longer than the listing TTL.
	deadline := time.Now().Add(600 * time.Millisecond)
	for time.Now().Before(deadline) {
		dc.Upsert("/d", CachedFileInfo{Name: "busy.dat"})
		time.Sleep(20 * time.Millisecond)
	}

	// The listing is long past its own TTL and must not be served as a valid
	// listing snapshot anymore; the caller has to re-fetch.
	if _, ok := dc.Get("/d"); ok {
		t.Fatalf("stale listing still valid after %v under continuous Upserts; names=%v",
			600*time.Millisecond, dirCacheNames(t, dc, "/d"))
	}
}

// Upsert must still refresh the visibility of the child it records.
func TestDirCacheUpsertVisibleImmediately(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	snapshot := dc.BeginListing("/d")
	dc.PutListing("/d", []CachedFileInfo{{Name: "a.dat"}}, snapshot)
	dc.Upsert("/d", CachedFileInfo{Name: "b.dat", Size: 7})

	items, ok := dc.Get("/d")
	if !ok {
		t.Fatal("expected cached listing")
	}
	for _, item := range items {
		if item.Name == "b.dat" {
			if item.Size != 7 {
				t.Fatalf("b.dat size = %d, want 7", item.Size)
			}
			return
		}
	}
	t.Fatalf("upserted child missing: names=%v", dirCacheNames(t, dc, "/d"))
}

// Many concurrent installs and upserts must not lose committed names: the
// reported workload has uploads settling throughout every listing RTT.
func TestDirCacheListingInstallUnderConcurrentUpserts(t *testing.T) {
	dc := NewDirCache(10 * time.Second)

	base := []CachedFileInfo{{Name: "base.dat"}}
	dc.PutListing("/d", base, dc.BeginListing("/d"))

	// One listing request is in flight while many commits settle.
	snapshot := dc.BeginListing("/d")
	const committed = 64
	for i := range committed {
		dc.Upsert("/d", CachedFileInfo{Name: fmt.Sprintf("u_%03d.dat", i)})
	}
	dc.PutListing("/d", base, snapshot)

	names := dirCacheNames(t, dc, "/d")
	for i := range committed {
		want := fmt.Sprintf("u_%03d.dat", i)
		if !containsName(names, want) {
			t.Fatalf("committed child %s dropped by listing install (have %d names)", want, len(names))
		}
	}
}

// The two races mirror each other: a listing issued before a local removal
// still contains the removed name and must not resurrect it, while a child
// recorded after the listing was issued must survive it. Both can be in
// flight at the same time, in either order.
func TestDirCacheListingInstallRespectsConcurrentRemoval(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	dc.PutListing("/d", []CachedFileInfo{{Name: "keep.dat"}, {Name: "gone.dat"}}, ListingSnapshot{})

	// A listing request is issued while gone.dat is still known locally.
	snapshot := dc.BeginListing("/d")
	// ... and the file is unlinked before the response lands.
	dc.Remove("/d", "gone.dat")

	// The response still carries gone.dat.
	dc.PutListing("/d", []CachedFileInfo{{Name: "keep.dat"}, {Name: "gone.dat"}}, snapshot)

	names := dirCacheNames(t, dc, "/d")
	if containsName(names, "gone.dat") {
		t.Fatalf("removed child resurrected by stale listing install: names=%v", names)
	}
	if !containsName(names, "keep.dat") {
		t.Fatalf("unrelated child lost: names=%v", names)
	}
}

// A child removed and later recreated (unlink + create of the same name) must
// follow the newest local state, not the older removal record.
func TestDirCacheListingInstallAfterRemoveThenRecreate(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	dc.PutListing("/d", []CachedFileInfo{{Name: "f.dat"}}, ListingSnapshot{})

	snapshot := dc.BeginListing("/d")
	dc.Remove("/d", "f.dat")
	dc.Upsert("/d", CachedFileInfo{Name: "f.dat", Size: 42})

	dc.PutListing("/d", []CachedFileInfo{{Name: "f.dat"}}, snapshot)

	items, ok := dc.Get("/d")
	if !ok {
		t.Fatal("expected cached listing")
	}
	for _, item := range items {
		if item.Name == "f.dat" {
			if item.Size != 42 {
				t.Fatalf("recreated f.dat size = %d, want the newer 42", item.Size)
			}
			return
		}
	}
	t.Fatalf("recreated child missing: names=%v", dirCacheNames(t, dc, "/d"))
}

// Listing requests overlap, so an install whose snapshot is OLDER can land
// after one whose snapshot is NEWER. Both must see the records their own
// snapshot predates, even when the newer install already settled them.
func TestDirCacheOlderListingInstallLandsAfterNewer(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	dc.PutListing("/d", []CachedFileInfo{{Name: "base.dat"}}, ListingSnapshot{})

	// Request A is issued, then a commit and a removal land, then request B is
	// issued. Both mutations therefore sit strictly between A and B, which is
	// the window where a newer install must not discard the records that an
	// older install still needs.
	snapshotA := dc.BeginListing("/d")
	dc.Upsert("/d", CachedFileInfo{Name: "committed.dat", Size: 11})
	dc.Remove("/d", "base.dat")
	snapshotB := dc.BeginListing("/d")

	// B lands first. Its response was taken after the mutations, so it
	// already reflects them and needs no replay.
	dc.PutListing("/d", []CachedFileInfo{{Name: "committed.dat"}}, snapshotB)
	names := dirCacheNames(t, dc, "/d")
	if !containsName(names, "committed.dat") || containsName(names, "base.dat") {
		t.Fatalf("newer install wrong: names=%v", names)
	}

	// A lands second. Its response predates both mutations, so it must still
	// carry the committed child over and keep the removal applied.
	dc.PutListing("/d", []CachedFileInfo{{Name: "base.dat"}}, snapshotA)
	names = dirCacheNames(t, dc, "/d")
	if !containsName(names, "committed.dat") {
		t.Fatalf("older install dropped the committed child: names=%v", names)
	}
	if containsName(names, "base.dat") {
		t.Fatalf("older install resurrected the removed child: names=%v", names)
	}
}

// The mirror case: the newer request lands second and must reach the same
// conclusion for the mutations it predates.
func TestDirCacheNewerListingInstallLandsAfterOlder(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	dc.PutListing("/d", []CachedFileInfo{{Name: "base.dat"}}, ListingSnapshot{})

	snapshotA := dc.BeginListing("/d")
	dc.Upsert("/d", CachedFileInfo{Name: "committed.dat", Size: 11})
	dc.Remove("/d", "base.dat")
	snapshotB := dc.BeginListing("/d")

	// The older request lands first and replays both mutations.
	dc.PutListing("/d", []CachedFileInfo{{Name: "base.dat"}}, snapshotA)
	names := dirCacheNames(t, dc, "/d")
	if !containsName(names, "committed.dat") || containsName(names, "base.dat") {
		t.Fatalf("older install wrong: names=%v", names)
	}

	// The newer response already reflects them and must stay consistent.
	dc.PutListing("/d", []CachedFileInfo{{Name: "committed.dat"}}, snapshotB)
	names = dirCacheNames(t, dc, "/d")
	if !containsName(names, "committed.dat") {
		t.Fatalf("newer install lost the committed child: names=%v", names)
	}
	if containsName(names, "base.dat") {
		t.Fatalf("newer install resurrected the removed child: names=%v", names)
	}
}

// A request issued BEFORE a commit must not drop it even when a NEWER request
// (issued after the commit) has already installed and settled its records.
func TestDirCacheOlderListingInstallSurvivesNewerResponseThatKnowsTheChild(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	dc.PutListing("/d", []CachedFileInfo{{Name: "base.dat"}}, ListingSnapshot{})

	snapshotOld := dc.BeginListing("/d")
	dc.Upsert("/d", CachedFileInfo{Name: "committed.dat", Size: 11})
	snapshotNew := dc.BeginListing("/d")

	// The newer response knows the child, so the install needs no replay and
	// must not be taken as licence to drop the older request's record.
	dc.PutListing("/d", []CachedFileInfo{{Name: "base.dat"}, {Name: "committed.dat"}}, snapshotNew)

	// The older response predates the commit: without the record the child
	// would vanish from readdir.
	dc.PutListing("/d", []CachedFileInfo{{Name: "base.dat"}}, snapshotOld)
	if names := dirCacheNames(t, dc, "/d"); !containsName(names, "committed.dat") {
		t.Fatalf("older install dropped the committed child: names=%v", names)
	}
}

// A carried-over child that overflows the entry during replay evicts other
// names and clears completeness. The install must not then claim the listing
// is complete, or a subsequent miss would be answered with a wrong ENOENT
// instead of reaching the server.
func TestDirCacheListingInstallDoesNotClaimCompletenessAfterEviction(t *testing.T) {
	dc := NewNamespaceCache(10*time.Second, 10*time.Second, 4)

	// Fill the entry to its limit.
	dc.PutListing("/d", []CachedFileInfo{{Name: "a.dat"}, {Name: "b.dat"}, {Name: "c.dat"}, {Name: "d.dat"}}, ListingSnapshot{})

	snapshot := dc.BeginListing("/d")
	// Commit a new child while the listing is in flight: replay will need a
	// slot and evict "a.dat" at the cap.
	dc.Upsert("/d", CachedFileInfo{Name: "e.dat"})

	dc.PutListing("/d", []CachedFileInfo{{Name: "a.dat"}, {Name: "b.dat"}, {Name: "c.dat"}, {Name: "d.dat"}}, snapshot)

	// The evicted name must not be answerable as a "complete listing" miss.
	if dc.CanAnswerMisses("/d") {
		t.Fatalf("listing claims completeness after replay eviction; names=%v", dirCacheNames(t, dc, "/d"))
	}
}

// --- Regressions for the review findings on #966 -------------------------

// The request that loses the race must not be served the raw response.
// Serving the unmerged items hides a child that committed during the LIST's
// RTT, and because a committed child is already gone from the pending overlay,
// nothing later in that request can restore it.
func TestDirCacheListingInstallReturnsReconciledView(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	dc.PutListing("/d", []CachedFileInfo{{Name: "base.dat"}}, dc.BeginListing("/d"))

	// The listing request is issued, then a commit settles during its RTT.
	snapshot := dc.BeginListing("/d")
	dc.Upsert("/d", CachedFileInfo{Name: "committed.dat", Size: 7})

	// The response predates the commit, so it does not contain the child.
	view, installed := dc.PutListing("/d", []CachedFileInfo{{Name: "base.dat"}}, snapshot)
	if !installed {
		t.Fatal("install did not run for a live snapshot")
	}

	outNames := make([]string, 0, len(view))
	for _, item := range view {
		outNames = append(outNames, item.Name)
	}
	if !containsName(outNames, "committed.dat") {
		t.Fatalf("caller was served the raw response and lost the committed child: view=%v", outNames)
	}
	if !containsName(outNames, "base.dat") {
		t.Fatalf("response child missing from the reconciled view: view=%v", outNames)
	}
	for _, item := range view {
		if item.Name == "committed.dat" && item.Size != 7 {
			t.Fatalf("reconciled child lost its metadata: size=%d, want 7", item.Size)
		}
	}
}

// A removal that happens while the parent has no cached state must still be
// recorded, or the in-flight response resurrects the deleted name for the
// listing TTL. "Parent entry absent -> unlink -> stale response" is exactly
// the interleaving where the cache holds nothing to correct from.
func TestDirCacheListingInstallAfterColdParentRemoval(t *testing.T) {
	dc := NewDirCache(10 * time.Second)

	// The parent has no served state at all: the listing request is the first
	// thing to touch it, so there is nothing cached to correct from.
	snapshot := dc.BeginListing("/d")
	if entry := dc.entries["/d"]; entry == nil || len(entry.items) != 0 {
		t.Fatal("fixture bug: the parent must hold no served state")
	}

	// A file is unlinked while the request is still in flight.
	dc.Remove("/d", "gone.dat")

	// The response was taken before the unlink and still carries the name.
	view, installed := dc.PutListing("/d", []CachedFileInfo{{Name: "gone.dat"}}, snapshot)
	if !installed {
		t.Fatal("install did not run for a live snapshot")
	}

	outNames := make([]string, 0, len(view))
	for _, item := range view {
		outNames = append(outNames, item.Name)
	}
	if containsName(outNames, "gone.dat") {
		t.Fatalf("removed child resurrected in the served view: view=%v", outNames)
	}
	if names := dirCacheNames(t, dc, "/d"); containsName(names, "gone.dat") {
		t.Fatalf("removed child resurrected in the cache: names=%v", names)
	}
}

// A record must survive as long as the listing that needs it is outstanding,
// even past the directory TTL: a listing request has no client-side deadline,
// so lifetime cannot be tied to a timer. The record is released by the install
// (or EndListing), not by expiry.
//
// The probes below matter: entry pruning only runs when the cache is touched,
// so a test that merely sleeps exercises nothing. Real traffic keeps probing
// the directory while the slow listing is in flight.
func TestDirCacheRecordsOutliveDirTTLWhileListingInFlight(t *testing.T) {
	dc := NewNamespaceCache(50*time.Millisecond, 50*time.Millisecond, defaultNamespaceCacheMaxEntries)

	dc.PutListing("/d", []CachedFileInfo{{Name: "base.dat"}}, dc.BeginListing("/d"))

	// A slow listing request is issued, then a commit lands during its RTT.
	snapshot := dc.BeginListing("/d")
	dc.Upsert("/d", CachedFileInfo{Name: "committed.dat", Size: 3})

	// Outlast several directory TTLs, probing the cache the way concurrent
	// traffic does. Each probe prunes the entry.
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		dc.Get("/d")
		dc.Lookup("/d", "committed.dat")
		time.Sleep(20 * time.Millisecond)
	}

	view, installed := dc.PutListing("/d", []CachedFileInfo{{Name: "base.dat"}}, snapshot)
	if !installed {
		t.Fatal("install did not run for a live snapshot")
	}

	outNames := make([]string, 0, len(view))
	for _, item := range view {
		outNames = append(outNames, item.Name)
	}
	if !containsName(outNames, "committed.dat") {
		t.Fatalf("record expired on the cache TTL while its listing was still in flight: view=%v", outNames)
	}
}

// Records must not leak once nothing can observe them: with no listing in
// flight, the next install (or an explicit EndListing) reclaims them.
func TestDirCacheRecordsReclaimedWithoutInFlightListing(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	dc.PutListing("/d", []CachedFileInfo{{Name: "base.dat"}}, dc.BeginListing("/d"))

	dc.Upsert("/d", CachedFileInfo{Name: "committed.dat"})
	snapshot := dc.BeginListing("/d")
	dc.PutListing("/d", []CachedFileInfo{{Name: "base.dat"}}, snapshot)

	// The install consumed the only in-flight snapshot, so nothing can still
	// observe the record.
	entry := dc.entries["/d"]
	if len(entry.localSeq) != 0 || len(entry.removedSeq) != 0 || len(entry.outstanding) != 0 {
		t.Fatalf("records not reclaimed: localSeq=%v removedSeq=%v outstanding=%v",
			entry.localSeq, entry.removedSeq, entry.outstanding)
	}

	// A failed listing must also release its snapshot.
	abandoned := dc.BeginListing("/d")
	dc.Upsert("/d", CachedFileInfo{Name: "later.dat"})
	dc.EndListing(abandoned)
	if got := len(dc.entries["/d"].outstanding); got != 0 {
		t.Fatalf("EndListing left %d outstanding snapshots", got)
	}
}

// An observation from a plain remote read must not displace the whole-directory
// view a listing install just fetched: request completion order is not a
// version order, so a stat that simply finished later may describe an older
// state than the listing does. The token is what tells them apart.
func TestDirCacheObserveDoesNotDisplaceListingResponse(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	dc.PutListing("/d", []CachedFileInfo{{Name: "f.dat", Size: 100, Revision: 5}}, dc.BeginListing("/d"))

	// The stat request is issued first...
	token := dc.BeginObservation("/d")
	// ...then a newer listing installs for the same name...
	dc.PutListing("/d", []CachedFileInfo{{Name: "f.dat", Size: 100, Revision: 5}}, dc.BeginListing("/d"))
	// ...and only now does the older stat complete.
	dc.Observe("/d", CachedFileInfo{Name: "f.dat", Size: 0, Revision: 0}, token)

	assertDirCacheChild(t, dc, "/d", "f.dat", func(item CachedFileInfo) {
		if item.Size != 100 || item.Revision != 5 {
			t.Fatalf("late observation displaced the listing response: size=%d rev=%d, want 100/5", item.Size, item.Revision)
		}
	})
}

// An observation must not resurrect a name the listing response omitted: the
// response is the newer whole-directory view, and it is the one that decides
// which names exist.
func TestDirCacheObserveDoesNotResurrectOmittedChild(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	dc.PutListing("/d", []CachedFileInfo{{Name: "keep.dat"}}, dc.BeginListing("/d"))

	// The stat is issued first, the listing omitting deleted.dat installs
	// while it is in flight, then the stat completes.
	token := dc.BeginObservation("/d")
	view, installed := dc.PutListing("/d", []CachedFileInfo{{Name: "keep.dat"}}, dc.BeginListing("/d"))
	if !installed {
		t.Fatal("install did not run for a live snapshot")
	}
	dc.Observe("/d", CachedFileInfo{Name: "deleted.dat", Size: 4}, token)

	outNames := make([]string, 0, len(view))
	for _, item := range view {
		outNames = append(outNames, item.Name)
	}
	if containsName(outNames, "deleted.dat") {
		t.Fatalf("observation resurrected a name the listing omitted: view=%v", outNames)
	}
	if names := dirCacheNames(t, dc, "/d"); containsName(names, "deleted.dat") {
		t.Fatalf("observation resurrected a name the listing omitted: names=%v", names)
	}
}

// A stat that began BEFORE a listing install carries no authority over what
// that install published, even when its revision is numerically higher. This
// is the delete+recreate case: the recreated object restarts at a low revision
// while the deleted one had a high revision, so ranking by revision would let
// the stale stat drag the cache back to the deleted object's identity.
func TestDirCacheLateObservationDoesNotRestoreDeletedObjectIdentity(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	dc.PutListing("/d", []CachedFileInfo{{Name: "f.dat", Size: 10, Revision: 10, ResourceID: "res-A"}}, dc.BeginListing("/d"))

	// The old object is unlinked and recreated; a listing installs the new one.
	token := dc.BeginObservation("/d")
	dc.PutListing("/d", []CachedFileInfo{{Name: "f.dat", Size: 1, Revision: 1, ResourceID: "res-B"}}, dc.BeginListing("/d"))

	// The stat of the deleted object completes late, carrying its higher
	// numeric revision and its old identity.
	dc.Observe("/d", CachedFileInfo{Name: "f.dat", Size: 10, Revision: 10, ResourceID: "res-A"}, token)

	assertDirCacheChild(t, dc, "/d", "f.dat", func(item CachedFileInfo) {
		if item.ResourceID != "res-B" || item.Revision != 1 || item.Size != 1 {
			t.Fatalf("late stat restored the deleted object: res=%q rev=%d size=%d, want res-B/1/1",
				item.ResourceID, item.Revision, item.Size)
		}
	})
}

// A stat issued AFTER the listing must still be able to refresh metadata the
// revision cannot express: a remote chmod changes mode without advancing the
// revision, so an equal-revision observation carries newer metadata and has to
// be applied. Ranking by revision alone would reject it and serve the old mode.
func TestDirCacheObservationAppliesSameRevisionMetadataChange(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	dc.PutListing("/d", []CachedFileInfo{{Name: "f.dat", Size: 50, Revision: 5, Mode: 0o644, HasMode: true}}, dc.BeginListing("/d"))

	// The stat is issued after that install, so its token matches the current
	// install sequence and it outranks nothing newer.
	token := dc.BeginObservation("/d")
	dc.Observe("/d", CachedFileInfo{Name: "f.dat", Size: 50, Revision: 5, Mode: 0o600, HasMode: true}, token)

	assertDirCacheChild(t, dc, "/d", "f.dat", func(item CachedFileInfo) {
		if !item.HasMode || item.Mode != 0o600 {
			t.Fatalf("same-revision metadata change rejected: mode=%#o has=%t, want 0600", item.Mode, item.HasMode)
		}
	})
}

// An observation whose token no longer matches the install state is discarded
// entirely: the listing installed meanwhile is the newer view, and it decides
// the directory's contents whether or not it carries this name.
func TestDirCacheStaleTokenObservationIsDiscarded(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	dc.PutListing("/d", []CachedFileInfo{{Name: "keep.dat"}}, dc.BeginListing("/d"))

	// A stat is issued, then a newer listing installs before it completes.
	token := dc.BeginObservation("/d")
	dc.PutListing("/d", []CachedFileInfo{{Name: "keep.dat"}}, dc.BeginListing("/d"))

	// The stat completes late and must not publish anything, not even a name
	// the current listing happens not to carry.
	dc.Observe("/d", CachedFileInfo{Name: "late.dat", Size: 9}, token)
	if names := dirCacheNames(t, dc, "/d"); containsName(names, "late.dat") {
		t.Fatalf("stale-token observation published a child: names=%v", names)
	}
	// It also must not overwrite an entry the newer listing carries.
	dc.Observe("/d", CachedFileInfo{Name: "keep.dat", Size: 99}, token)
	assertDirCacheChild(t, dc, "/d", "keep.dat", func(item CachedFileInfo) {
		if item.Size == 99 {
			t.Fatal("stale-token observation overwrote a published entry")
		}
	})
}

// A read issued after the last install matches the current sequence, so it is
// applied: that is how a change the revision cannot express still lands.
func TestDirCacheCurrentTokenObservationApplies(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	dc.PutListing("/d", []CachedFileInfo{{Name: "f.dat", Size: 5, Revision: 1}}, dc.BeginListing("/d"))

	token := dc.BeginObservation("/d")
	dc.Observe("/d", CachedFileInfo{Name: "f.dat", Size: 8, Revision: 1}, token)

	assertDirCacheChild(t, dc, "/d", "f.dat", func(item CachedFileInfo) {
		if item.Size != 8 {
			t.Fatalf("current-token observation was rejected: size=%d, want 8", item.Size)
		}
	})
}

// A local authoritative record always outranks an observation, even one whose
// token is current: the server side of that observation cannot have seen this
// mount's own mutation yet.
func TestDirCacheObservationDoesNotOverwriteLocalMutation(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	dc.PutListing("/d", []CachedFileInfo{{Name: "base.dat"}}, dc.BeginListing("/d"))

	// This mount settles a write of its own for f.dat.
	dc.Upsert("/d", CachedFileInfo{Name: "f.dat", Size: 20, Revision: 2})

	// A stat of the same name completes afterwards, describing the state
	// before this mount's write.
	token := dc.BeginObservation("/d")
	dc.Observe("/d", CachedFileInfo{Name: "f.dat", Size: 10, Revision: 1}, token)

	assertDirCacheChild(t, dc, "/d", "f.dat", func(item CachedFileInfo) {
		if item.Size != 20 || item.Revision != 2 {
			t.Fatalf("observation overwrote a local mutation: size=%d rev=%d, want 20/2", item.Size, item.Revision)
		}
	})
}

// An observation still proves the name exists, so it clears a stale negative
// marker even when it cannot replace the published entry.
func TestDirCacheObservationClearsNegativeMarker(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	dc.PutListing("/d", []CachedFileInfo{}, dc.BeginListing("/d"))
	dc.MarkNegative("/d", "f.dat")

	// The stat is issued after that install, so its token is current.
	token := dc.BeginObservation("/d")
	dc.Observe("/d", CachedFileInfo{Name: "f.dat", Size: 3, Revision: 1}, token)

	if got := dc.Lookup("/d", "f.dat"); got.kind != namespaceLookupPositive {
		t.Fatalf("observation did not publish the child: kind=%v", got.kind)
	}
}

// A discarded observation still clears a stale ENOENT marker for the name it
// proves exists: the marker is about existence, not about which view wins.
func TestDirCacheDiscardedObservationClearsNegativeMarker(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	dc.PutListing("/d", []CachedFileInfo{}, dc.BeginListing("/d"))
	dc.MarkNegative("/d", "f.dat")

	// The stat is issued, then a listing installs before it completes.
	token := dc.BeginObservation("/d")
	dc.PutListing("/d", []CachedFileInfo{}, dc.BeginListing("/d"))

	dc.Observe("/d", CachedFileInfo{Name: "f.dat", Size: 3, Revision: 1}, token)

	if got := dc.Lookup("/d", "f.dat"); got.kind == namespaceLookupNegative {
		t.Fatal("stale ENOENT marker survived an observation that proves existence")
	}
}

// assertDirCacheChild runs check against the cached child, failing if the
// listing or the child is missing.
func assertDirCacheChild(t *testing.T, dc *DirCache, dirPath, name string, check func(CachedFileInfo)) {
	t.Helper()
	items, ok := dc.Get(dirPath)
	if !ok {
		t.Fatalf("expected a cached listing for %s", dirPath)
	}
	for _, item := range items {
		if item.Name == name {
			check(item)
			return
		}
	}
	t.Fatalf("child %s missing: names=%v", name, dirCacheNames(t, dc, dirPath))
}

// A cache lookup landing between BeginListing and the install must not drop
// the entry: the in-flight snapshot and the records its install still needs
// live on that entry.
func TestDirCacheListingStateSurvivesConcurrentCacheLookup(t *testing.T) {
	dc := NewDirCache(10 * time.Second)

	// The parent starts cold and a listing request is issued.
	snapshot := dc.BeginListing("/d")

	// Concurrent cache traffic for the same directory: a readdir cache probe
	// and a child lookup, both of which walk getEntryLocked.
	if _, ok := dc.Get("/d"); ok {
		t.Fatal("fixture bug: the parent must start cold")
	}
	dc.Lookup("/d", "anything.dat")

	// A file is unlinked while the request is in flight, then the stale
	// response (taken before the unlink) arrives.
	dc.Remove("/d", "gone.dat")
	view, installed := dc.PutListing("/d", []CachedFileInfo{{Name: "gone.dat"}}, snapshot)
	if !installed {
		t.Fatal("install did not run for a live snapshot")
	}

	outNames := make([]string, 0, len(view))
	for _, item := range view {
		outNames = append(outNames, item.Name)
	}
	if containsName(outNames, "gone.dat") {
		t.Fatalf("removed child resurrected after concurrent cache lookups: view=%v", outNames)
	}
}

// An install that legitimately filtered every response entry must be served as
// an empty listing, not silently replaced by the stale response. N1: the
// projection used to fall back to `original` whenever the reconciled view was
// empty, so a lookup after "every child was removed during the RTT" resurrected
// a deleted name, and remoteDirectoryHasChildren reported the directory non-empty.
func TestReconciledFileInfosKeepsEmptyAuthoritativeView(t *testing.T) {
	original := []client.FileInfo{{Name: "gone.dat", Size: 10, Revision: 1}}
	if got := reconciledFileInfos(original, nil, true); len(got) != 0 {
		t.Fatalf("empty authoritative view was replaced by the stale response: %v", got)
	}
	// A no-op install (no cache) keeps the raw response.
	if got := reconciledFileInfos(original, nil, false); len(got) != 1 {
		t.Fatalf("non-install must keep the response: %v", got)
	}
}

// N1 on the production path: a child the local mutation removed during the LIST
// RTT must not come back through the lookup fallback's projection.
func TestDirCacheEmptyReconciledViewHasNoChildren(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	response := []CachedFileInfo{{Name: "only.dat", Size: 5, Revision: 1}}
	dc.PutListing("/d", response, dc.BeginListing("/d"))

	// The request is issued, then the only child is deleted locally.
	snapshot := dc.BeginListing("/d")
	dc.Remove("/d", "only.dat")
	view, installed := dc.PutListing("/d", response, snapshot)
	if !installed {
		t.Fatal("expected a live install")
	}
	if len(view) != 0 {
		t.Fatalf("removed child survived the reconciled view: %v", view)
	}
	if items, ok := dc.Get("/d"); ok && len(items) != 0 {
		t.Fatalf("cache still lists the removed child: %v", items)
	}
}

// N2: when a local mutation moves the SAME name to a newer revision/size during
// the LIST RTT, the requesting consumer must get the reconciled metadata, not
// the response's older struct for that name.
func TestReconciledFileInfosProjectsUpdatedSameName(t *testing.T) {
	original := []client.FileInfo{{Name: "f.dat", Size: 10, Revision: 1}}
	view := []CachedFileInfo{{Name: "f.dat", Size: 20, Revision: 2}}
	got := reconciledFileInfos(original, view, true)
	if len(got) != 1 {
		t.Fatalf("want 1 entry, got %v", got)
	}
	if got[0].Revision != 2 || got[0].Size != 20 {
		t.Fatalf("served the response's stale struct for a reconciled name: rev=%d size=%d, want 2/20",
			got[0].Revision, got[0].Size)
	}
}

// N2 on the production path: an authoritative Upsert landing mid-RTT must move
// the name for the consumer that is being answered right now.
func TestDirCacheReconciledViewCarriesUpdatedSameName(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	dc.PutListing("/d", []CachedFileInfo{{Name: "f.dat", Size: 10, Revision: 1}}, dc.BeginListing("/d"))

	snapshot := dc.BeginListing("/d")
	dc.Upsert("/d", CachedFileInfo{Name: "f.dat", Size: 20, Revision: 2})
	view, installed := dc.PutListing("/d", []CachedFileInfo{{Name: "f.dat", Size: 10, Revision: 1}}, snapshot)
	if !installed {
		t.Fatal("expected a live install")
	}
	found := false
	for _, item := range view {
		if item.Name == "f.dat" {
			found = true
			if item.Size != 20 || item.Revision != 2 {
				t.Fatalf("view carries the response's older metadata: size=%d rev=%d, want 20/2", item.Size, item.Revision)
			}
		}
	}
	if !found {
		t.Fatalf("child missing from the view: %v", view)
	}
}

// N4: a failed LIST must release its snapshot, or the directory keeps an
// outstanding flight forever and its records can never be reclaimed. Repeated
// failed probes must not accumulate state.
func TestDirCacheFailedListingReleasesSnapshot(t *testing.T) {
	dc := NewDirCache(10 * time.Second)

	// Take a snapshot and abandon it the way a failed ListCtx does.
	for range 3 {
		dc.EndListing(dc.BeginListing("/d"))
		if flights := outstandingFlights(dc.entries["/d"]); flights != 0 {
			t.Fatalf("abandoned listing left %d outstanding flights", flights)
		}
	}

	// A record written while one flight was outstanding must be reclaimable
	// once that flight is released: otherwise it pins state forever.
	snapshot := dc.BeginListing("/d")
	dc.Upsert("/d", CachedFileInfo{Name: "f.dat", Revision: 1})
	dc.EndListing(snapshot)
	entry := dc.entries["/d"]
	// The authoritative record survives (it is not superseded), but the flight
	// must be gone so later reclamation can proceed.
	if flights := outstandingFlights(entry); flights != 0 {
		t.Fatalf("failed LIST left the snapshot outstanding: %v", entry.outstanding)
	}
}

// N4 on the production path: when the LIST inside remoteDirectoryHasChildren
// fails, the listing snapshot must be released. An unreleased flight keeps the
// directory's reconciliation records from ever being reclaimed, so repeated
// failed probes accumulate state without bound.
func TestRemoteDirectoryHasChildrenReleasesSnapshotOnListError(t *testing.T) {
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

	for range 3 {
		if _, _, err := fs.remoteDirectoryHasChildren(context.Background(), "/leak"); err == nil {
			t.Fatal("expected the list failure to surface")
		}
	}
	if got := listCalls.Load(); got != 3 {
		t.Fatalf("list calls = %d, want 3", got)
	}
	entry := fs.dirCache.entries["/leak"]
	if flights := outstandingFlights(entry); flights != 0 {
		t.Fatalf("failed LISTs left %d outstanding flights; records can never be reclaimed", flights)
	}
}

// outstandingFlights totals the in-flight listing snapshots an entry is
// tracking, counting multiplicity: concurrent listings can share one snapshot
// sequence.
func outstandingFlights(entry *dirCacheEntry) int {
	if entry == nil {
		return 0
	}
	total := 0
	for _, n := range entry.outstanding {
		total += n
	}
	return total
}

// The view handed to a caller is not capped by the cache's entry limit: the cap
// bounds what the cache retains, but a truncated reply would hide names past
// the cap from this readdir, make the Lookup fallback miss them (and cache a
// false ENOENT), and make remoteDirectoryHasChildren judge a populated
// directory empty.
func TestDirCacheListingViewIsNotCappedByMaxEntries(t *testing.T) {
	const cap = 4
	dc := NewNamespaceCache(10*time.Second, 10*time.Second, cap)

	response := make([]CachedFileInfo, 0, cap*3)
	for i := range cap * 3 {
		response = append(response, CachedFileInfo{Name: fmt.Sprintf("f_%02d.dat", i), Revision: 1})
	}
	view, installed := dc.PutListing("/d", response, dc.BeginListing("/d"))
	if !installed {
		t.Fatal("expected a live install")
	}
	if len(view) != len(response) {
		t.Fatalf("view truncated to the cache cap: got %d entries, want %d", len(view), len(response))
	}
	for _, want := range response {
		if !containsName(func() []string {
			names := make([]string, 0, len(view))
			for _, item := range view {
				names = append(names, item.Name)
			}
			return names
		}(), want.Name) {
			t.Fatalf("child %s missing from the view", want.Name)
		}
	}
	// The cache itself still honours the cap. Read it through the entry: a
	// listing the cache had to truncate cannot answer misses, so Get saying no
	// is expected there and is not what this asserts.
	entry := dc.entries["/d"]
	if entry == nil {
		t.Fatal("expected a cached entry")
	}
	if len(entry.items) > cap {
		t.Fatalf("cache exceeded maxEntries: %d > %d", len(entry.items), cap)
	}
}

// Invalidation must not strand a listing that is still in flight: the entry
// carries both the flight and the reconciliation records its install will
// replay. Deleting it let a probe reclaim those records first, so a child
// committed during the request vanished from the installed view.
func TestDirCacheInvalidateKeepsInFlightReconciliation(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	dc.PutListing("/d", []CachedFileInfo{{Name: "keep.dat"}}, dc.BeginListing("/d"))

	snapshot := dc.BeginListing("/d")
	// An SSE reset (or any invalidation) lands while the request is in flight.
	dc.Invalidate("/d")
	// A local commit lands too, then a probe touches the directory.
	dc.Upsert("/d", CachedFileInfo{Name: "committed.dat", Size: 3})
	dc.Lookup("/d", "committed.dat")
	dc.Get("/d")

	view, installed := dc.PutListing("/d", []CachedFileInfo{{Name: "keep.dat"}}, snapshot)
	if !installed {
		t.Fatal("expected a live install")
	}
	names := make([]string, 0, len(view))
	for _, item := range view {
		names = append(names, item.Name)
	}
	if !containsName(names, "committed.dat") {
		t.Fatalf("invalidation dropped a record an in-flight install needed: view=%v", names)
	}
}

// Overlapping listings whose snapshots share a sequence must not release each
// other's flight: an older response installing first used to decrement the
// newer flight's count, reclaiming the records the newer install still needed.
func TestDirCacheSequencesSharedAcrossFlightsReleaseIndependently(t *testing.T) {
	dc := NewDirCache(10 * time.Second)
	dc.PutListing("/d", []CachedFileInfo{{Name: "keep.dat"}}, dc.BeginListing("/d"))

	// Both requests are issued before any mutation, so they share a sequence.
	first := dc.BeginListing("/d")
	dc.Invalidate("/d")
	second := dc.BeginListing("/d")
	dc.Upsert("/d", CachedFileInfo{Name: "committed.dat", Size: 3})

	// The first response installs first.
	dc.PutListing("/d", []CachedFileInfo{{Name: "keep.dat"}}, first)
	// The second response lands afterwards and predates the same mutation.
	view, _ := dc.PutListing("/d", []CachedFileInfo{{Name: "keep.dat"}}, second)

	names := make([]string, 0, len(view))
	for _, item := range view {
		names = append(names, item.Name)
	}
	if !containsName(names, "committed.dat") {
		t.Fatalf("shared-sequence release reclaimed a record still in use: view=%v", names)
	}
}

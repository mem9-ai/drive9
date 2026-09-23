package fuse

import (
	"fmt"
	"testing"
	"time"
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

	// The listing snapshot the caller is about to install, taken before the
	// concurrent commit landed.
	snapshot := dc.BeginListing("/d")
	dc.PutListing("/d", []CachedFileInfo{{Name: "a.dat"}}, snapshot)

	// A commit for b.dat settles while the listing request is in flight.
	dc.Upsert("/d", CachedFileInfo{Name: "b.dat"})

	// The stale response arrives and is installed.
	dc.PutListing("/d", []CachedFileInfo{{Name: "a.dat"}}, snapshot)

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

	snapshot := dc.BeginListing("/d")
	base := []CachedFileInfo{{Name: "base.dat"}}
	dc.PutListing("/d", base, snapshot)

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

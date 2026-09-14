package fuse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestReadDirCacheHitDoesNotFenceInFlightRemoteListing(t *testing.T) {
	for _, identity := range []string{"known", "legacy"} {
		for _, commit := range []bool{false, true} {
			name := identity + "/read-only"
			if commit {
				name = identity + "/local-commit"
			}
			t.Run(name, func(t *testing.T) {
				entered, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				rid, oldRev, newRev := "resource-1", int64(7), int64(8)
				if identity == "legacy" {
					rid, oldRev, newRev = "", 0, 0
				}
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != http.MethodGet || r.URL.Query().Get("list") != "1" {
						t.Errorf("unexpected request: %s %s", r.Method, r.URL)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					close(entered)
					<-release
					_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{map[string]any{
						"name": "file.dat", "size": 128, "revision": newRev, "resource_id": rid, "mode": 0o640, "hasMode": true,
					}}})
				}))
				t.Cleanup(ts.Close)
				t.Cleanup(unblock)
				opts := &MountOptions{}
				opts.setDefaults()
				fs := NewDat9FS(newTestClient(ts.URL), opts)
				ino := fs.inodes.LookupWithIdentity("/file.dat", rid, 1, false, 4096, time.Now())
				fs.inodes.UpdateRevision(ino, oldRev)
				fs.inodes.UpdateMode(ino, 0o600)
				live, _ := fs.inodes.GetEntry(ino)
				fs.dirCache.Put("/", []CachedFileInfo{cachedInfoFromEntry("file.dat", live)})
				before := fs.inodes.AttrVersion()
				done := make(chan error, 1)
				go func() {
					_, _, err := fs.remoteDirectoryHasChildren(context.Background(), "/")
					done <- err
				}()
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("listing did not reach the response gate")
				}
				if got := readDirPlusAttrs(t, fs, 1, "/")["file.dat"].Size; got != 4096 {
					t.Fatalf("cache-hit size = %d, want 4096", got)
				}
				if after := fs.inodes.AttrVersion(); after != before {
					t.Errorf("cache read advanced mutation version: %d -> %d", before, after)
				}
				wantSize, wantMode := uint64(128), uint32(0o640)
				if commit {
					commitRev := newRev + 1
					if identity == "legacy" {
						commitRev = 0
					}
					fs.onCommitQueueSuccess(&CommitEntry{Path: "/file.dat", Inode: ino, Size: 8192}, commitRev)
					if fs.inodes.AttrVersion() <= before {
						t.Fatal("local commit did not advance the mutation version")
					}
					wantSize, wantMode = 8192, 0o600
				}
				unblock()
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("listing did not complete")
				}
				attr := readDirPlusAttrs(t, fs, 1, "/")["file.dat"]
				if attr.Size != wantSize || attr.Mode&0o777 != wantMode {
					t.Fatalf("published attributes = %+v, want size=%d mode=%o", attr, wantSize, wantMode)
				}
			})
		}
	}
}

func TestDirectoryObservationsDoNotAdvanceMutationVersion(t *testing.T) {
	for _, scenario := range []string{"cold", "existing", "alias", "replacement", "unrelated-mutation"} {
		t.Run(scenario, func(t *testing.T) {
			m := NewInodeToPath()
			var wantMutation uint64
			if scenario != "cold" {
				path, rid := "/file.dat", "resource-1"
				switch scenario {
				case "alias":
					path = "/alias.dat"
				case "replacement":
					rid = "old-resource"
				}
				m.LookupWithIdentity(path, rid, 1, false, 4096, time.Now())
				if scenario != "replacement" {
					wantMutation = m.AttrVersion()
				}
				if scenario == "unrelated-mutation" {
					m.Lookup("/other.dat", false, 0, time.Now())
				}
			}
			before := m.AttrVersion()
			entry := m.EnsureDirEntry("/file.dat", CachedFileInfo{
				ResourceID: "resource-1", Size: 128, Mode: 0o640, HasMode: true, observedVersion: before,
			}, false)
			if entry.Size != 128 || entry.ResourceID != "resource-1" || entry.Mode != 0o640 {
				t.Fatalf("observation was not installed: %+v", entry)
			}
			if m.AttrVersion() != before || entry.attrVersion != wantMutation {
				t.Fatalf("observation manufactured a mutation: global=%d inode=%d want=%d/%d", m.AttrVersion(), entry.attrVersion, before, wantMutation)
			}
		})
	}
}

func TestDirectoryObservationRetainsReadVersionInCache(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Query().Get("list") != "1" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{map[string]any{
			"name": "file.dat", "size": 128, "revision": 2, "resource_id": "resource-1",
		}}})
	}))
	defer ts.Close()
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(ts.URL), opts)
	ino := fs.inodes.LookupWithIdentity("/file.dat", "resource-1", 1, false, 4096, time.Now())
	fs.inodes.UpdateRevision(ino, 1)
	fs.inodes.Lookup("/other.dat", false, 0, time.Now())
	before := fs.inodes.AttrVersion()
	// A partial cache forces listDir to fetch remotely, then merge with this
	// older value tagged after the unrelated inode advanced the global clock.
	fs.cacheFileForPath("/file.dat", 4096, time.Now(), 1)
	if size := readDirPlusAttrs(t, fs, 1, "/")["file.dat"].Size; size != 128 {
		t.Fatalf("remote size = %d, want 128", size)
	}
	item := fs.dirCache.Lookup("/", "file.dat").item
	if item.Size != 128 || item.observedVersion != before || fs.inodes.AttrVersion() != before {
		t.Fatalf("remote observation lost its provenance: item=%+v clock=%d want=%d", item, fs.inodes.AttrVersion(), before)
	}
}

func TestRecoveredPendingInodeSurvivesCommitCleanup(t *testing.T) {
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://localhost"), opts)
	var err error
	fs.pendingIndex, err = NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gen, err := fs.pendingIndex.PutWithBaseRev("/file.dat", 4096, PendingNew, 0)
	if err != nil {
		t.Fatal(err)
	}
	fs.dirCache.Put("/", []CachedFileInfo{{Name: "file.dat", Size: 0}})
	if size := readDirPlusAttrs(t, fs, 1, "/")["file.dat"].Size; size != 4096 {
		t.Fatalf("recovered pending size = %d, want 4096", size)
	}
	// A recovery entry may have been queued before its inode was discovered.
	// An already captured directory snapshot must remain fenced after cleanup.
	stale, _ := fs.dirCache.Get("/")
	fs.onCommitQueueSuccess(&CommitEntry{Path: "/file.dat", Size: 4096}, 0)
	fs.pendingIndex.RemoveIfGeneration("/file.dat", gen)
	entries := fs.cachedToDirEntries("/", stale)
	if entries[0].Size != 4096 {
		t.Fatalf("cleanup allowed stale listing to overwrite recovered pending size: %+v", entries[0])
	}
}

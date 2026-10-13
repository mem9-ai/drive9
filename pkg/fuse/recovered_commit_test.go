package fuse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRecoveredCommitFencesInFlightListing(t *testing.T) {
	for _, state := range []struct{ discovered, cache bool }{{true, true}, {false, true}, {false, false}} {
		for _, revision := range []int64{0, 2} {
			t.Run(fmt.Sprintf("discovered=%v/cache=%v/revision=%d", state.discovered, state.cache, revision), func(t *testing.T) {
				data := bytes.Repeat([]byte("r"), 4096)
				uploadStarted, listStarted := make(chan struct{}), make(chan struct{})
				allowUpload, allowList := make(chan struct{}), make(chan struct{})
				var uploadOnce, listOnce sync.Once
				releaseUpload := func() { uploadOnce.Do(func() { close(allowUpload) }) }
				releaseList := func() { listOnce.Do(func() { close(allowList) }) }
				var committed atomic.Bool
				var lists atomic.Int32
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch {
					case r.Method == http.MethodPut && r.URL.Path == "/v1/fs/dir/file.dat":
						body, err := io.ReadAll(r.Body)
						if err != nil || !bytes.Equal(body, data) {
							t.Errorf("recovered payload mismatch: %v, size=%d", err, len(body))
						}
						close(uploadStarted)
						<-allowUpload
						committed.Store(true)
						_ = json.NewEncoder(w).Encode(map[string]any{"revision": 2})
					case r.Method == http.MethodGet && r.URL.Query().Get("list") == "1":
						size, rev := 0, 1
						if committed.Load() {
							size, rev = len(data), 2
						}
						if lists.Add(1) == 1 {
							close(listStarted)
							<-allowList
						}
						_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{map[string]any{
							"name": "file.dat", "size": size, "revision": rev, "resource_id": "resource-1", "mode": 0o644, "hasMode": true, "nlink": 1,
						}}})
					default:
						t.Errorf("unexpected request: %s %s", r.Method, r.URL)
						w.WriteHeader(http.StatusBadRequest)
					}
				}))
				t.Cleanup(ts.Close)
				opts := &MountOptions{SyncMode: SyncInteractive}
				opts.setDefaults()
				fs := NewDat9FS(newTestClient(ts.URL), opts)
				fs.client.SetSmallFileThresholdForTests(1 << 20)
				var err error
				fs.shadowStore, err = NewShadowStoreWithQuota(t.TempDir(), 0, 0)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(fs.shadowStore.Close)
				fs.pendingIndex, err = NewPendingIndex(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				if err := fs.shadowStore.WriteFull("/dir/file.dat", data, 1); err != nil {
					t.Fatal(err)
				}
				if _, err := fs.pendingIndex.PutWithBaseRev("/dir/file.dat", int64(len(data)), PendingOverwrite, 1); err != nil {
					t.Fatal(err)
				}
				queue := NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 8)
				queue.PathLock = fs.lockRemoteCommitPath
				queue.DurableWatermark = fs.latestCommittedRevision
				queue.OnUploaded = fs.onCommitQueueUploaded
				queue.OnCleanup = fs.onCommitQueueCleanup
				queue.OnSuccess = func(entry *CommitEntry, _ int64) {
					if !entry.recovered || entry.Inode != 0 || entry.PendingIndexGen == 0 {
						t.Errorf("not an inode-less recovery entry: %+v", entry)
					}
					// Exercise both versions of the success-callback contract.
					fs.onCommitQueueSuccess(entry, revision)
					if entry.Inode != 0 {
						t.Error("publication changed the queue entry's immutable inode binding")
					}
				}
				fs.commitQueue = queue
				t.Cleanup(func() { releaseList(); releaseUpload(); queue.DrainAll() })
				queue.RecoverPending()
				select {
				case <-uploadStarted:
				case <-time.After(5 * time.Second):
					t.Fatal("recovery upload did not reach gate")
				}
				dirIno := fs.inodes.Lookup("/dir", true, 0, time.Now())
				info := CachedFileInfo{Name: "file.dat", ResourceID: "resource-1", Size: 0, Revision: 1, Mode: 0o644, HasMode: true, Nlink: 1}
				if state.discovered {
					fs.dirCache.Put("/dir", []CachedFileInfo{info})
					if size := readDirPlusAttrs(t, fs, dirIno, "/dir")["file.dat"].Size; size != 4096 {
						t.Fatalf("pending discovery size=%d", size)
					}
				}
				// Force remote enumeration while retaining any known metadata.
				fs.dirCache.Invalidate("/dir")
				if state.cache {
					fs.dirCache.Upsert("/dir", info)
				}
				type result struct {
					entries []DirEntry
					err     error
				}
				done := make(chan result, 1)
				go func() { entries, err := fs.listDir(context.Background(), "/dir"); done <- result{entries, err} }()
				select {
				case <-listStarted:
				case <-time.After(5 * time.Second):
					t.Fatal("remote listing did not reach gate")
				}
				beforeCommit := fs.inodes.AttrVersion()
				releaseUpload()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := queue.WaitIdle(ctx); err != nil {
					t.Fatal(err)
				}
				if fs.pendingIndex.Generation("/dir/file.dat") != 0 {
					t.Fatal("recovery cleanup did not remove pending metadata")
				}
				if fs.inodes.AttrVersion() <= beforeCommit {
					t.Error("recovery completion did not advance mutation fence")
				}
				releaseList()
				var listed result
				select {
				case listed = <-done:
				case <-ctx.Done():
					t.Fatal("stale listing did not complete")
				}
				if listed.err != nil {
					t.Fatal(listed.err)
				}
				dh := &DirHandle{Ino: dirIno, Path: "/dir", Entries: listed.entries}
				attr := readDirPlusHandleAttrs(t, fs, dh)["file.dat"]
				if attr.Size != 4096 {
					t.Errorf("READDIRPLUS published size %d after recovery cleanup", attr.Size)
				}
				if cached := fs.dirCache.Lookup("/dir", "file.dat").item; cached.Size != 4096 {
					t.Errorf("stale listing poisoned cache: %+v", cached)
				} else if revision == 0 && cached.Revision != 0 {
					t.Errorf("unknown committed revision labeled with old base: %+v", cached)
				}
				fs.dirCache.Invalidate("/dir")
				fresh := readDirPlusAttrs(t, fs, dirIno, "/dir")["file.dat"]
				live, _ := fs.inodes.GetEntry(fresh.NodeId)
				if fresh.Size != 4096 || live.ResourceID != "resource-1" || live.Revision != 2 {
					t.Errorf("fresh committed metadata not visible: %+v", live)
				}
			})
		}
	}
}

func TestRecoveredCommitRejectsStaleOwnership(t *testing.T) {
	for _, ownership := range []string{"new-generation", "missing-generation", "removed", "no-index"} {
		t.Run(ownership, func(t *testing.T) {
			opts := &MountOptions{}
			opts.setDefaults()
			fs := NewDat9FS(newTestClient("http://localhost"), opts)
			index, err := NewPendingIndex(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			fs.pendingIndex = index
			gen, err := index.PutWithBaseRev("/file.dat", 4096, PendingNew, 0)
			if err != nil {
				t.Fatal(err)
			}
			entry := &CommitEntry{Path: "/file.dat", Size: 4096, Kind: PendingNew, PendingIndexGen: gen, recovered: true}
			switch ownership {
			case "new-generation":
				if _, err := index.PutWithBaseRev("/file.dat", 128, PendingNew, 0); err != nil {
					t.Fatal(err)
				}
			case "missing-generation":
				entry.PendingIndexGen = 0
			case "removed":
				index.RemoveIfGeneration("/file.dat", gen)
			case "no-index":
				fs.pendingIndex = nil
			}
			ino := fs.inodes.LookupWithIdentity("/file.dat", "new-incarnation", 1, false, 128, time.Now())
			fs.inodes.UpdateRevision(ino, 9)
			before, _ := fs.inodes.GetEntry(ino)
			fs.cacheEntryForPath("/file.dat", before)
			cached := fs.dirCache.Lookup("/", "file.dat").item
			fs.recordCommittedRevision("/file.dat", 9)
			fs.readCache.Put("/file.dat", []byte("new content"), 9)
			pendingBefore, _ := index.GetMeta("/file.dat")
			fs.onCommitQueueSuccess(entry, 2)
			after, _ := fs.inodes.GetEntry(ino)
			pendingAfter, _ := index.GetMeta("/file.dat")
			if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(pendingBefore, pendingAfter) {
				t.Fatalf("stale recovery touched inode or pending state: before=%+v after=%+v", before, after)
			}
			if got := fs.dirCache.Lookup("/", "file.dat").item; !reflect.DeepEqual(got, cached) {
				t.Errorf("stale recovery changed directory cache: %+v", got)
			}
			if data, ok := fs.readCache.Get("/file.dat", 9); !ok || string(data) != "new content" {
				t.Error("stale recovery invalidated the new incarnation's read cache")
			}
			if fs.latestCommittedRevision("/file.dat") != 9 || entry.Inode != 0 {
				t.Error("stale recovery changed revision watermark or queue binding")
			}
		})
	}
}

func TestRecoveredCommitSelectsAuthoritativeSize(t *testing.T) {
	for _, dirty := range []int64{-1, 0, 8192} {
		t.Run(fmt.Sprint("dirty=", dirty), func(t *testing.T) {
			opts := &MountOptions{}
			opts.setDefaults()
			fs := NewDat9FS(newTestClient("http://localhost"), opts)
			var err error
			fs.pendingIndex, err = NewPendingIndex(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			// Recovery may correct stale metadata size from the actual shadow.
			gen, err := fs.pendingIndex.PutWithBaseRev("/file.dat", 512, PendingOverwrite, 1)
			if err != nil {
				t.Fatal(err)
			}
			ino := fs.inodes.LookupWithIdentity("/file.dat", "resource-1", 2, false, 512, time.Now())
			fs.inodes.UpdateOwner(ino, 123, 456, true, true)
			fs.inodes.UpdateMode(ino, 0o640)
			wantSize := int64(4096)
			if dirty >= 0 {
				fs.markDirtySize(ino, dirty)
				wantSize = dirty
			}
			before := fs.inodes.AttrVersion()
			entry := &CommitEntry{Path: "/file.dat", Size: 4096, Kind: PendingOverwrite, PendingIndexGen: gen, recovered: true}
			fs.onCommitQueueSuccess(entry, 2)
			live, _ := fs.inodes.GetEntry(ino)
			if live.Size != wantSize || live.Revision != 2 || live.attrVersion <= before || live.Nlookup != 1 {
				t.Errorf("wrong committed inode state: %+v", live)
			}
			if live.ResourceID != "resource-1" || live.Nlink != 2 || live.Uid != 123 || live.Gid != 456 || live.Mode != 0o640 {
				t.Errorf("publication lost existing identity/owner/mode: %+v", live)
			}
			if got := fs.dirCache.Lookup("/", "file.dat").item; got.Size != wantSize || got.observedVersion != live.attrVersion {
				t.Errorf("cache publication does not match live state: %+v", got)
			}
			if entry.Inode != 0 || fs.pendingIndex.Generation("/file.dat") != gen {
				t.Error("publication changed queue binding or consumed pending ownership")
			}
		})
	}
}

func TestRecoveredCreateDoesNotAdoptStaleCachedHardlink(t *testing.T) {
	for _, discovered := range []bool{false, true} {
		t.Run(fmt.Sprint("discovered=", discovered), func(t *testing.T) {
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
			alias := fs.inodes.LookupWithIdentity("/alias.dat", "old-resource", 1, false, 128, time.Now())
			before, _ := fs.inodes.GetEntry(alias)
			fs.dirCache.Upsert("/", cachedInfoFromEntry("file.dat", before))
			if discovered {
				fs.cachedToDirEntries("/", []CachedFileInfo{cachedInfoFromEntry("file.dat", before)})
			}
			fs.onCommitQueueSuccess(&CommitEntry{Path: "/file.dat", Size: 4096, Kind: PendingNew, PendingIndexGen: gen, recovered: true}, 1)
			ino, ok := fs.inodes.GetInode("/file.dat")
			after, _ := fs.inodes.GetEntry(alias)
			if !ok || ino == alias || !reflect.DeepEqual(before, after) {
				t.Fatalf("recovered create changed old hardlink: ino=%d alias=%d before=%+v after=%+v", ino, alias, before, after)
			}
			live, _ := fs.inodes.GetEntry(ino)
			if live.Size != 4096 || live.ResourceID != "" {
				t.Fatalf("create inherited stale resource metadata: %+v", live)
			}

		})
	}
}

package fuse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestOtherRemoteListingsTrackObservationVersion(t *testing.T) {
	for _, route := range []string{"lookup", "lookup-retry", "rmdir"} {
		for _, concurrent := range []bool{false, true} {
			name := route + "/fresh"
			if concurrent {
				name = route + "/concurrent-local-update"
			}
			t.Run(name, func(t *testing.T) {
				var fs *Dat9FS
				var ino uint64
				var calls atomic.Int32
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != http.MethodGet || r.URL.Query().Get("list") != "1" {
						t.Errorf("unexpected request: %s %s", r.Method, r.URL)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					if calls.Add(1) == 1 && route == "lookup-retry" {
						// The retry must capture a new version, not reuse the first.
						fs.inodes.UpdateSize(ino, 8192)
						fs.cacheFileForPath("/dir/file.dat", 8192, time.Now(), 7)
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
					if concurrent {
						fs.onCommitQueueSuccess(&CommitEntry{Path: "/dir/file.dat", Inode: ino, Size: 16384}, 9)
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{map[string]any{
						"name": "file.dat", "size": 128, "revision": 8, "resource_id": "file-1", "mode": 0o640, "hasMode": true,
					}}})
				}))
				defer ts.Close()
				opts := &MountOptions{LookupRetryCount: 1, LookupRetryTimeout: time.Second}
				opts.setDefaults()
				fs = NewDat9FS(newTestClient(ts.URL), opts)
				dirIno := fs.inodes.Lookup("/dir", true, 0, time.Now())
				ino = fs.inodes.LookupWithIdentity("/dir/file.dat", "file-1", 1, false, 4096, time.Now())
				fs.inodes.UpdateRevision(ino, 7)
				fs.inodes.UpdateMode(ino, 0o600)
				fs.cacheFileForPath("/dir/file.dat", 4096, time.Now(), 7)
				var err error
				if route == "rmdir" {
					_, _, err = fs.remoteDirectoryHasChildren(context.Background(), "/dir")
				} else {
					_, _, err = fs.lookupListWithRetry(nil, "/dir")
				}
				if err != nil {
					t.Fatal(err)
				}
				wantSize, wantRev := int64(128), int64(8)
				if concurrent {
					wantSize, wantRev = 16384, 9
				}
				cached := fs.dirCache.Lookup("/dir", "file.dat").item
				if cached.Size != wantSize || cached.Revision != wantRev {
					t.Fatalf("cache = %+v, want size=%d revision=%d", cached, wantSize, wantRev)
				}
				attr := readDirPlusAttrs(t, fs, dirIno, "/dir")["file.dat"]
				if attr.Size != uint64(wantSize) {
					t.Fatalf("READDIRPLUS size = %d, want %d", attr.Size, wantSize)
				}
				if !concurrent && attr.Mode&0o777 != 0o640 {
					t.Fatalf("READDIRPLUS mode = %o, want 0640", attr.Mode)
				}
			})
		}
	}
}

func TestReadDirPlusFreshResourceReplacement(t *testing.T) {
	for _, kind := range []string{"new-file", "directory", "unknown-identity", "same-resource-stale-revision"} {
		t.Run(kind, func(t *testing.T) {
			isDir := kind == "directory"
			rid, rev, size, mode := "new-resource", int64(1), int64(128), uint32(0o640)
			if isDir {
				rid, rev, size, mode = "new-directory", 0, 0, 0o755
			} else if kind == "unknown-identity" {
				rid = ""
			} else if kind == "same-resource-stale-revision" {
				rid = "old-resource"
			}
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Query().Get("list") != "1" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{map[string]any{
					"name": "file.dat", "size": size, "isDir": isDir, "revision": rev,
					"resource_id": rid, "mode": mode, "hasMode": true, "nlink": 1,
				}}})
			}))
			defer ts.Close()
			opts := &MountOptions{}
			opts.setDefaults()
			fs := NewDat9FS(newTestClient(ts.URL), opts)
			oldID := "old-resource"
			if kind == "unknown-identity" {
				oldID = ""
			}
			oldIno := fs.inodes.LookupWithIdentity("/file.dat", oldID, 2, false, 4096, time.Now())
			if oldID != "" {
				fs.inodes.LookupWithIdentity("/alias.dat", oldID, 2, false, 4096, time.Now())
			}
			fs.inodes.UpdateRevision(oldIno, 10)
			fs.inodes.UpdateMode(oldIno, 0o600)
			wantSize, wantMode, wantRev := size, mode, rev
			if kind == "same-resource-stale-revision" {
				wantSize, wantMode, wantRev = 4096, 0o600, 10
			}
			for range 2 {
				fs.dirCache.Invalidate("/")
				attr := readDirPlusAttrs(t, fs, 1, "/")["file.dat"]
				wantType := uint32(syscall.S_IFREG)
				if isDir {
					wantType = syscall.S_IFDIR
				}
				if attr.Size != uint64(wantSize) || attr.Mode&syscall.S_IFMT != wantType || attr.Mode&0o777 != wantMode {
					t.Errorf("replacement attributes = %+v, want size=%d mode=%o", attr, wantSize, wantType|wantMode)
				}
				entry, _ := fs.inodes.GetEntry(attr.NodeId)
				if entry.Revision != wantRev {
					t.Errorf("revision = %d, want %d", entry.Revision, wantRev)
				}
				if kind == "new-file" && entry.ResourceID != rid {
					t.Errorf("resource identity = %q, want %q", entry.ResourceID, rid)
				}
				if kind == "new-file" || isDir {
					if attr.NodeId == oldIno {
						t.Error("replacement reused the old resource's inode")
					}
					alias, _ := fs.inodes.GetEntry(oldIno)
					if alias.ResourceID != "old-resource" || alias.IsDir || alias.Size != 4096 || alias.Revision != 10 || alias.Mode != 0o600 {
						t.Errorf("replacement rewrote the old hardlink: %+v", alias)
					}
				}
			}
		})
	}
}

func TestDirEntryReplacementPreservesAuthoritativeLocalState(t *testing.T) {
	for _, protection := range []string{"pending", "concurrent-mutation"} {
		t.Run(protection, func(t *testing.T) {
			inodes := NewInodeToPath()
			ino := inodes.LookupWithIdentity("/file.dat", "old-resource", 1, false, 4096, time.Now())
			item := CachedFileInfo{ResourceID: "new-resource", Size: 128, Revision: 1, observedVersion: inodes.AttrVersion()}
			if protection == "concurrent-mutation" {
				inodes.UpdateSize(ino, 4096)
			}
			entry := inodes.EnsureDirEntry("/file.dat", item, protection == "pending")
			if entry.Ino != ino || entry.ResourceID != "old-resource" || entry.Size != 4096 {
				t.Fatalf("remote replacement discarded authoritative local state: %+v", entry)
			}
		})
	}
}

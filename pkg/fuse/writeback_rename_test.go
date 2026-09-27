package fuse

import (
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestOwnedWriteBackRename(t *testing.T) {
	for _, mode := range []string{"upload_after_migration", "upload_during_migration", "prepare_failure", "shadow_failure", "cache_failure"} {
		t.Run(mode, func(t *testing.T) {
			const oldP, newP = "/olddir/file.txt", "/newdir/file.txt"
			var fs *Dat9FS
			var puts, posts, deletes atomic.Int32
			var uploaded string
			var oldPendingDir, oldShadowDir string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodHead:
					w.Header().Set("Content-Length", "0")
					w.Header().Set("X-Dat9-IsDir", "true")
					if strings.HasSuffix(r.URL.Path, "file.txt") {
						w.Header().Set("X-Dat9-IsDir", "false")
					}
				case http.MethodPost:
					posts.Add(1)
					if mode == "cache_failure" {
						if err := os.Mkdir(fs.writeBack.datFile(newP), 0o755); err != nil {
							t.Error(err)
						}
					}
					if mode == "prepare_failure" {
						fs.pendingIndex.dir = filepath.Join(t.TempDir(), "missing", "index")
					}
					if mode == "shadow_failure" {
						fs.shadowStore.dir = filepath.Join(t.TempDir(), "missing", "shadow")
					}
				case http.MethodDelete:
					deletes.Add(1)
				case http.MethodPut:
					body, _ := io.ReadAll(r.Body)
					uploaded = string(body)
					puts.Add(1)
					w.Header().Set("X-Dat9-Revision", "1")
				}
			}))
			defer server.Close()
			opts := &MountOptions{}
			opts.setDefaults()
			fs = NewDat9FS(newTestClient(server.URL), opts)
			fs.client.SetSmallFileThresholdForTests(1 << 20)
			var err error
			fs.writeBack, err = NewWriteBackCache(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			fs.pendingIndex, err = NewPendingIndex(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			fs.shadowStore, err = NewShadowStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer fs.shadowStore.Close()
			oldPendingDir, oldShadowDir = fs.pendingIndex.dir, fs.shadowStore.dir
			if err := fs.shadowStore.WriteFull(oldP, []byte("payload"), 0); err != nil {
				t.Fatal(err)
			}
			pg, err := fs.pendingIndex.PutWithBaseRev(oldP, 7, PendingNew, 0)
			if err != nil {
				t.Fatal(err)
			}
			owned := fs.snapshotStagingGens(oldP)
			if _, _, err := fs.writeBack.putHandleSnapshot(oldP, []byte("payload"), 7, PendingNew, 0, 0, false, "snapshot", "", true, true, nil, 0, 1, owned, nil); err != nil {
				t.Fatal(err)
			}
			u := &WriteBackUploader{client: fs.client, cache: fs.writeBack, remoteRoot: "/", inflight: make(map[string]*pathState), uploadCh: make(chan string, 8)}
			var sampled StagingGens
			u.SnapshotStagingGens = func(p string) StagingGens { sampled = fs.snapshotStagingGens(p); return sampled }
			u.OnDataCommitted = fs.onWriteBackDataCommitted
			u.OnSuccess = fs.onWriteBackUploadSuccess
			fs.uploader = u
			if mode == "upload_during_migration" {
				u.stopped.Store(true)
			}
			fs.inodes.Lookup("/olddir", true, 0, time.Time{})
			fs.inodes.Lookup("/newdir", true, 0, time.Time{})
			fs.inodes.Lookup(oldP, false, 7, time.Time{})
			st := fs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "olddir", "newdir")
			fs.pendingIndex.dir, fs.shadowStore.dir = oldPendingDir, oldShadowDir
			cacheMeta, cacheOK := fs.writeBack.GetMeta(newP)
			oldGen, newGen := fs.pendingIndex.Generation(oldP), fs.pendingIndex.Generation(newP)
			t.Logf("AFTER_RENAME mode=%s status=%v remote_renames=%d puts=%d cache_new=%t old_pending=%d new_pending=%d original_pending=%d shadow_old=%t shadow_new=%t", mode, st, posts.Load(), puts.Load(), cacheOK, oldGen, newGen, pg, fs.shadowStore.Has(oldP), fs.shadowStore.Has(newP))
			if posts.Load() != 1 {
				t.Fatalf("remote rename count=%d", posts.Load())
			}
			if mode == "prepare_failure" || mode == "shadow_failure" || mode == "cache_failure" {
				if st != gofuse.EIO {
					t.Fatalf("migration failure returned %v", st)
				}
			} else if st != gofuse.OK {
				t.Fatal(st)
			}
			switch mode {
			case "upload_after_migration":
				if !cacheOK || cacheMeta.ownedStagingGens.PendingIndexGen != newGen || newGen == pg || newGen == 0 {
					t.Fatal("expected stale cache ownership after rekey")
				}
				u.uploadOne(newP)
				t.Logf("AFTER_UPLOAD sampled_pending=%d bound_pending=%d remaining_pending=%d shadow=%t puts=%d payload=%q", sampled.PendingIndexGen, cacheMeta.ownedStagingGens.PendingIndexGen, fs.pendingIndex.Generation(newP), fs.shadowStore.Has(newP), puts.Load(), uploaded)
				if puts.Load() != 1 || fs.pendingIndex.Generation(newP) != 0 || fs.shadowStore.Has(newP) {
					t.Fatal("expected successful upload with exact staging cleanup")
				}
				parent, _ := fs.inodes.GetInode("/newdir")
				if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: parent}, "file.txt"); st != gofuse.OK || deletes.Load() != 1 {
					t.Fatalf("uploaded PendingNew unlink=%v deletes=%d", st, deletes.Load())
				}
			case "upload_during_migration":
				t.Logf("EARLY_UPLOAD sampled_pending=%d sampled_shadow=%d remaining_pending=%d payload=%q", sampled.PendingIndexGen, sampled.ShadowGen, newGen, uploaded)
				if puts.Load() != 1 || sampled.PendingIndexGen == 0 || newGen != 0 || fs.shadowStore.Has(newP) {
					t.Fatal("Submit consumed intermediate migration state")
				}

			case "cache_failure":
				retained, ok := fs.writeBack.GetMeta(oldP)
				if !ok || retained.Kind != PendingConflict || !fs.shadowStore.Has(oldP) || oldGen != pg {
					t.Fatal("failed cache rename did not preserve and quarantine source")
				}
				u.uploadOne(oldP)
				if puts.Load() != 0 {
					t.Fatal("old queued upload bypassed conflict")
				}
			default:
				if !cacheOK || oldGen != pg || newGen != 0 || !fs.shadowStore.Has(oldP) || fs.shadowStore.Has(newP) {
					t.Fatal("unexpected partial migration state")
				}
				if cacheMeta.Kind != PendingConflict {
					t.Fatal("failed migration not quarantined")
				}
				// A path token queued before failure must not bypass the conflict marker.
				u.uploadOne(newP)
				if puts.Load() != 0 {
					t.Fatal("queued upload consumed failed migration")
				}
				if _, err := os.Stat(filepath.Join(oldPendingDir, hashPath(oldP)+".meta")); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

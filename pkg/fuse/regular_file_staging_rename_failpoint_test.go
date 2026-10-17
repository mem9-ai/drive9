//go:build failpoint

package fuse

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pingcap/failpoint"
)

// This isolates exact-path store migration from the real Release handoff matrix.
// Stage through the real helpers, then release the controlled publisher fence.
func TestRegularFileRenameStagingAndFailure(t *testing.T) {
	for _, mode := range []string{"cache", "pending_only", "prepare_failure", "remote_failure", "reverse", "target_replacement", "hardlink_alias", "uncommitted_fence", "orphan_pending", "prefix_isolation", "mismatch_proof", "stale_view"} {
		t.Run(mode, func(t *testing.T) {
			oldP, newP := "/old", "/new"
			if mode == "reverse" {
				oldP, newP = "/z", "/a"
			}
			var mu sync.Mutex
			remotePath, data, rev := oldP, "hello world", int64(1)
			posts, oldPuts := 0, 0
			var fs *Dat9FS
			fs = newOwnedRenameFenceFS(t, func(w http.ResponseWriter, r *http.Request) {
				p := strings.TrimPrefix(r.URL.Path, "/v1/fs")
				mu.Lock()
				defer mu.Unlock()
				switch r.Method {
				case http.MethodHead:
					if p == "/" {
						w.Header().Set("X-Dat9-IsDir", "true")
						w.Header().Set("Content-Length", "0")
						return
					}
					if mode == "target_replacement" && p == newP && remotePath == oldP {
						w.Header().Set("Content-Length", "6")
						w.Header().Set("X-Dat9-Revision", "7")
						w.Header().Set("X-Dat9-Resource-ID", "target")
						return
					}
					if p != remotePath && !(mode == "hardlink_alias" && p == "/alias") {
						http.NotFound(w, r)
						return
					}
					w.Header().Set("Content-Length", fmt.Sprint(len(data)))
					w.Header().Set("X-Dat9-Revision", fmt.Sprint(rev))
					w.Header().Set("X-Dat9-Resource-ID", "source")
				case http.MethodGet:
					if mode == "stale_view" && rev == 2 {
						fs.mountViewGeneration.Add(1)
					}
					if r.URL.Query().Has("list") {
						_, _ = io.WriteString(w, `{"entries":[]}`)
						return
					}
					if mode == "target_replacement" && p == newP && remotePath == oldP {
						_, _ = io.WriteString(w, "target")
						return
					}
					if p != remotePath && !(mode == "hardlink_alias" && p == "/alias") {
						http.NotFound(w, r)
						return
					}
					_, _ = io.WriteString(w, data)
				case http.MethodPost:
					posts++
					if mode == "remote_failure" {
						w.WriteHeader(http.StatusForbidden)
						return
					}
					if p != newP || r.Header.Get("X-Dat9-Rename-Source") != oldP {
						t.Error("wrong rename")
						w.WriteHeader(400)
						return
					}
					remotePath = newP
				case http.MethodPut:
					if p == oldP && remotePath == newP {
						oldPuts++
					}
					if p != remotePath || r.Header.Get("X-Dat9-Expected-Revision") != fmt.Sprint(rev) {
						w.WriteHeader(http.StatusConflict)
						return
					}
					b, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
						w.WriteHeader(500)
						return
					}
					data = string(b)
					rev++
					_, _ = fmt.Fprintf(w, `{"revision":%d}`, rev)
				default:
					t.Errorf("unexpected %s %s", r.Method, r.URL.String())
					w.WriteHeader(400)
				}
			}, false)
			cacheHas := func(p string) bool { _, ok := fs.writeBack.GetMeta(p); return ok }
			ino := fs.inodes.Lookup(oldP, false, 11, time.Now())
			fs.inodes.UpdateRevision(ino, 1)
			var opened gofuse.OpenOut
			if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR)}, &opened); st != gofuse.OK {
				t.Fatal(st)
			}
			if mode == "hardlink_alias" && !fs.inodes.AddAlias(ino, "/alias", "source", 2, false, 11, time.Now()) {
				t.Fatal("alias")
			}
			reviewFtruncate(t, fs, ino, opened.Fh, 5)
			fh, _ := fs.fileHandles.Get(opened.Fh)
			fs.commitQueue = NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 8)
			fs.commitQueue.PathLock = fs.lockRemoteCommitPath
			fs.commitQueue.OnUploaded = fs.onCommitQueueUploaded
			fs.commitQueue.OnSuccess = fs.onCommitQueueSuccess
			fs.commitQueue.OnCleanup = fs.onCommitQueueCleanup
			defer func() { fh.Lock(); fs.releaseHandleRemoteCommitPathLocked(fh); fh.Unlock(); fs.commitQueue.DrainAll() }()
			enableOwnedRenameContextHook(t, fs, func(_ *Dat9FS, _, chosen context.Context) context.Context {
				short, stop := context.WithTimeout(chosen, 150*time.Millisecond)
				t.Cleanup(stop)
				return short
			})
			staged := false
			stage := func() {
				fh.Lock()
				defer fh.Unlock()
				if err := fs.fenceFtruncateLocked(fh); err != nil {
					t.Fatal(err)
				}
				if err := fs.stageShadowLocked(fh, true); err != nil {
					t.Fatal(err)
				}
				if mode != "pending_only" && mode != "uncommitted_fence" && mode != "orphan_pending" && mode != "mismatch_proof" && mode != "stale_view" {
					if err := fs.snapshotWriteBackLocked(fh, true); err != nil {
						t.Fatal(err)
					}
				}
				if mode != "uncommitted_fence" && mode != "mismatch_proof" && mode != "stale_view" {
					fs.releaseHandleRemoteCommitPathLocked(fh)
				}
			}
			if mode == "uncommitted_fence" || mode == "orphan_pending" || mode == "mismatch_proof" || mode == "stale_view" {
				stage()
				staged = true
				if mode == "mismatch_proof" || mode == "stale_view" {
					mu.Lock()
					data = "hello"
					if mode == "mismatch_proof" {
						data = "other"
					}
					rev = 2
					mu.Unlock()
					fs.recordCommittedRevision(oldP, 2)
				}
				if mode == "orphan_pending" {
					fs.deleteFileHandle(opened.Fh, fh)
				}
			}
			enableQueueRenameHook(t, fs, func(_ *Dat9FS, phase string) {
				if phase == "after_capture" && !staged {
					stage()
					staged = true
				}
			})
			if mode == "prepare_failure" {
				point := "github.com/mem9-ai/drive9/pkg/fuse/ownedWriteBackRenameRekeyed"
				if err := failpoint.EnableCall(point, func(observed *Dat9FS, p, q string) {
					if observed == fs && p == oldP {
						fs.pendingIndex.dir = filepath.Join(t.TempDir(), "missing")
					}
				}); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = failpoint.Disable(point) })
			}
			var targetIno, targetFH uint64
			if mode == "target_replacement" {
				targetIno = fs.inodes.Lookup(newP, false, 6, time.Now())
				fs.inodes.UpdateRevision(targetIno, 7)
				var target gofuse.OpenOut
				if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: targetIno}, Flags: uint32(syscall.O_RDONLY)}, &target); st != gofuse.OK {
					t.Fatal(st)
				}
				targetFH = target.Fh
			}

			var st gofuse.Status
			if mode == "prefix_isolation" {
				other := oldP + "-other"
				if err := fs.writeBack.Put(other, []byte("X"), 1, PendingNew); err != nil {
					t.Fatal(err)
				}
				lock := fs.writeBack.acquirePathLock(other)
				var once sync.Once
				release := func() { once.Do(func() { fs.writeBack.releasePathLock(other, lock) }) }
				defer release()
				// Release the unrelated cache lock before remote rename. The
				// separately registered capture hook still stages the source.
				point := "github.com/mem9-ai/drive9/pkg/fuse/ownedDirectoryRenamePhase"
				if err := failpoint.EnableCall(point, func(observed *Dat9FS, phase string) {
					if observed == fs && phase == "before_remote" {
						release()
					}
				}); err != nil {
					t.Fatal(err)
				}
				done := make(chan gofuse.Status, 1)
				go func() {
					done <- fs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, strings.TrimPrefix(oldP, "/"), strings.TrimPrefix(newP, "/"))
				}()
				select {
				case st = <-done:
				case <-time.After(time.Second):
					release()
					st = <-done
					t.Error("exact source capture waited on a prefix-colliding sibling cache")
				}
				if !cacheHas(other) {
					t.Error("unrelated sibling cache changed")
				}
			} else {
				st = fs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, strings.TrimPrefix(oldP, "/"), strings.TrimPrefix(newP, "/"))
			}
			mu.Lock()
			count, current := posts, remotePath
			mu.Unlock()
			if !staged {
				t.Error("did not reach exact-file capture")
			}
			if mode == "uncommitted_fence" || mode == "orphan_pending" || mode == "pending_only" || mode == "mismatch_proof" || mode == "stale_view" {
				if st != gofuse.EAGAIN || count != 0 || current != oldP {
					t.Errorf("unresolved-owner admission status=%v renames=%d path=%s", st, count, current)
				}
				if b, err := fs.shadowStore.ReadAll(oldP); err != nil || string(b) != "hello" {
					t.Errorf("owner bytes=%q/%v", b, err)
				}
				if m, ok := fs.pendingIndex.GetMeta(oldP); !ok || m.Kind == PendingConflict {
					t.Error("admission altered exact source staging")
				}

				if mode == "pending_only" || mode == "orphan_pending" {
					if mode == "pending_only" {
						fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: opened.Fh})
						fs.commitQueue.DrainAll()
					} else {
						if count := fs.commitQueue.RecoverPendingSync(context.Background()); count != 1 {
							t.Errorf("normal recovery committed %d images", count)
						}
					}
					retry := fs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, strings.TrimPrefix(oldP, "/"), strings.TrimPrefix(newP, "/"))
					mu.Lock()
					final, where, renames := data, remotePath, posts
					mu.Unlock()
					if retry != gofuse.OK || final != "hello" || where != newP || renames != 1 {
						t.Errorf("normal commit then rename=%v data=%q path=%s renames=%d", retry, final, where, renames)
					}
					t.Logf("DEFERRED mode=%s first=%v retry=%v remote=%q", mode, st, retry, final)
				}
				return
			}
			if mode == "remote_failure" {
				if st == gofuse.OK || current != oldP || !cacheHas(oldP) || !fs.pendingIndex.HasPending(oldP) || !fs.shadowStore.Has(oldP) {
					t.Errorf("remote error consumed source: status=%v path=%s", st, current)
				}
				return
			}
			if mode == "prepare_failure" {
				if st != gofuse.EIO || count != 1 || current != newP {
					t.Errorf("migration failure status=%v renames=%d", st, count)
				}
				if m, ok := fs.pendingIndex.GetMeta(oldP); !ok || m.Kind != PendingConflict {
					t.Error("retained source pending generation not quarantined")
				}
				if m, ok := fs.writeBack.GetMeta(newP); !ok || m.Kind != PendingConflict {
					t.Error("moved cache not quarantined")
				}
				if b, err := fs.shadowStore.ReadAll(oldP); err != nil || string(b) != "hello" {
					t.Errorf("quarantine lost source bytes=%q/%v", b, err)
				}
				return
			}
			if st != gofuse.OK || count != 1 {
				t.Errorf("rename=%v count=%d", st, count)
			}
			if cacheHas(oldP) || fs.pendingIndex.HasPending(oldP) || fs.shadowStore.Has(oldP) {
				t.Error("exact source staging left at old path")
			}
			if b, err := fs.shadowStore.ReadAll(newP); err != nil || string(b) != "hello" {
				t.Errorf("new source staging=%q/%v", b, err)
			}
			if mode != "pending_only" {
				select {
				case p := <-fs.uploader.uploadCh:
					if p != newP {
						t.Errorf("submit=%s", p)
					}
					if err := fs.uploader.UploadSync(context.Background(), p); err != nil {
						t.Fatal(err)
					}
				default:
					t.Error("migrated cache not submitted")
				}
			}
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: opened.Fh})
			fs.commitQueue.DrainAll()
			mu.Lock()
			final, wrong := data, oldPuts
			mu.Unlock()
			if final != "hello" || wrong != 0 {
				t.Errorf("accepted bytes=%q old puts=%d", final, wrong)
			}
			if mode == "target_replacement" {
				if b, st, err := readDat9FSTestRange(fs, targetIno, targetFH, 0, 6); err != nil || st != gofuse.OK || string(b) != "target" {
					t.Errorf("replaced reader=%q/%v/%v", b, st, err)
				}
			}
			if mode == "hardlink_alias" {
				if alias, ok := fs.inodes.GetInode("/alias"); !ok || alias != ino {
					t.Error("surviving alias identity changed")
				}
			}
			t.Logf("EXACT mode=%s rename=%v remote=%q old_puts=%d", mode, st, final, wrong)
		})
	}
}

//go:build failpoint

package fuse

import (
	"context"
	"fmt"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pingcap/failpoint"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func awaitOwnedRenameStep(t *testing.T, c <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout: %s", label)
	}
}
func stageOwnedRenameChild(t *testing.T, fs *Dat9FS, p string) {
	t.Helper()
	if err := fs.shadowStore.WriteFull(p, []byte("payload"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.pendingIndex.PutWithBaseRev(p, 7, PendingNew, 0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := fs.writeBack.putHandleSnapshot(p, []byte("payload"), 7, PendingNew, 0, 0, false, "snapshot", "", true, true, nil, 0, 1, fs.snapshotStagingGens(p), nil); err != nil {
		t.Fatal(err)
	}
	fs.inodes.Lookup(p, false, 7, time.Time{})
}
func newOwnedRenameFenceFS(t *testing.T, handler http.HandlerFunc, shared bool) *Dat9FS {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	opts := &MountOptions{}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(server.URL), opts)
	fs.client.SetSmallFileThresholdForTests(1 << 20)
	var err error
	dir := t.TempDir()
	fs.writeBack, err = NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !shared {
		dir = t.TempDir()
	}
	fs.pendingIndex, err = NewPendingIndex(dir)
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore, err = NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fs.shadowStore.Close)
	u := &WriteBackUploader{client: fs.client, cache: fs.writeBack, remoteRoot: "/", inflight: make(map[string]*pathState), uploadCh: make(chan string, 8)}
	u.SnapshotStagingGens = fs.snapshotStagingGens
	u.OnDataCommitted = fs.onWriteBackDataCommitted
	u.OnSuccess = fs.onWriteBackUploadSuccess
	fs.uploader = u
	return fs
}
func TestOwnedRenameCommitFenceOrdering(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		for _, shared := range []bool{false, true} {
			for _, mode := range []string{"upload_first", "rename_failure", "remote_failure", "rename_success"} {
				t.Run(fmt.Sprintf("%s/reverse=%t/shared=%t", mode, reverse, shared), func(t *testing.T) {
					oldDir, newDir := "/adir", "/zdir"
					if reverse {
						oldDir, newDir = newDir, oldDir
					}
					oldP, newP := oldDir+"/file.txt", newDir+"/file.txt"
					putStarted, putResume := make(chan struct{}), make(chan struct{})
					renameStarted, renameResume := make(chan struct{}), make(chan struct{})
					captured, recoverySelected := make(chan struct{}), make(chan struct{})
					var putOnce, renameOnce, capturedOnce, selectedOnce sync.Once
					releasePut := func() { putOnce.Do(func() { close(putResume) }) }
					releaseRename := func() { renameOnce.Do(func() { close(renameResume) }) }
					var oldPuts, newPuts, posts atomic.Int32
					var putCompleted atomic.Bool
					var fs *Dat9FS
					fs = newOwnedRenameFenceFS(t, func(w http.ResponseWriter, r *http.Request) {
						switch r.Method {
						case http.MethodHead:
							w.Header().Set("Content-Length", "0")
							w.Header().Set("X-Dat9-IsDir", "true")
							if strings.HasSuffix(r.URL.Path, "file.txt") {
								w.Header().Set("X-Dat9-IsDir", "false")
							}
						case http.MethodGet:
							if !strings.HasSuffix(strings.TrimSuffix(r.URL.Path, "/"), newDir) {
								t.Errorf("unexpected GET=%s", r.URL.String())
							}
							_, _ = io.WriteString(w, `{"entries":[]}`)
						case http.MethodPut:
							body, _ := io.ReadAll(r.Body)
							if string(body) != "payload" {
								t.Errorf("payload=%q", body)
							}
							if strings.HasSuffix(r.URL.Path, oldP) {
								oldPuts.Add(1)
								if mode == "upload_first" {
									close(putStarted)
									<-putResume
								}
								putCompleted.Store(true)
							} else if strings.HasSuffix(r.URL.Path, newP) {
								newPuts.Add(1)
							} else {
								t.Errorf("unexpected PUT=%s", r.URL.Path)
							}
							w.Header().Set("X-Dat9-Revision", "1")
							_, _ = io.WriteString(w, `{"revision":1}`)
						case http.MethodPost:
							posts.Add(1)
							// The remote rename must execute inside both source and destination
							// commit exclusions, not just after a best-effort WaitPrefix call.
							for _, p := range []string{oldP, newP} {
								fs.remoteCommitMu.Lock()
								lock := fs.remoteCommitLocks[p]
								fs.remoteCommitMu.Unlock()
								if lock == nil {
									t.Errorf("remote rename has no lock for %s", p)
								} else if lock.TryLock() {
									lock.Unlock()
									t.Errorf("remote rename ran outside exclusion for %s", p)
								}
							}
							if mode == "upload_first" && !putCompleted.Load() {
								t.Error("remote rename overtook prior recovery PUT")
							}
							if mode == "rename_failure" || mode == "upload_first" {
								if err := os.Mkdir(fs.writeBack.datFile(newP), 0o755); err != nil {
									t.Error(err)
								}
							}
							close(renameStarted)
							<-renameResume
							if mode == "remote_failure" {
								http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
							}
						default:
							t.Errorf("unexpected method %s", r.Method)
						}
					}, shared)
					t.Cleanup(func() { releasePut(); releaseRename() })
					fs.uploader.stopped.Store(true) // Submit must occur after uploader exclusion is released.
					fs.inodes.Lookup(oldDir, true, 0, time.Time{})
					fs.inodes.Lookup(newDir, true, 0, time.Time{})
					stageOwnedRenameChild(t, fs, oldP)
					cq := NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 8)
					cq.DrainAll()
					fs.commitQueue = cq
					cq.DurableWatermark = fs.latestCommittedRevision
					cq.IsSuperseded = fs.commitEntrySuperseded
					cq.PathLock = func(p string) func() {
						selectedOnce.Do(func() { close(recoverySelected) })
						return fs.lockRemoteCommitPath(p)
					}
					enableOwnedRenameFenceHook(t, fs, func(observed *Dat9FS, phase string) {
						if observed == fs && phase == "captured" {
							capturedOnce.Do(func() { close(captured) })
						}
					})
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					recoveryDone := make(chan int, 1)
					renameDone := make(chan gofuse.Status, 1)
					startRecovery := func() { go func() { recoveryDone <- cq.RecoverPendingSync(ctx) }() }
					startRename := func() {
						go func() {
							renameDone <- fs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, strings.TrimPrefix(oldDir, "/"), strings.TrimPrefix(newDir, "/"))
						}()
					}
					if mode == "upload_first" {
						startRecovery()
						awaitOwnedRenameStep(t, putStarted, "recovery PUT started")
						startRename()
						awaitOwnedRenameStep(t, captured, "rename reached pre-remote fence")
						releasePut()
						awaitOwnedRenameStep(t, renameStarted, "remote rename after PUT")
						releaseRename()
					} else {
						startRename()
						awaitOwnedRenameStep(t, renameStarted, "remote rename holds fence")
						startRecovery()
						awaitOwnedRenameStep(t, recoverySelected, "recovery selected old metadata")
						if oldPuts.Load() != 0 {
							t.Error("recovery bypassed held namespace fence")
						}
						releaseRename()
					}
					var st gofuse.Status
					select {
					case st = <-renameDone:
					case <-ctx.Done():
						t.Fatal("rename deadlock")
					}
					var committed int
					select {
					case committed = <-recoveryDone:
					case <-ctx.Done():
						t.Fatal("recovery deadlock")
					}
					want := gofuse.EIO
					if mode == "rename_success" {
						want = gofuse.OK
					}
					if mode == "remote_failure" {
						want = gofuse.EACCES
					}
					if st != want {
						t.Errorf("rename=%v want=%v", st, want)
					}
					if posts.Load() != 1 {
						t.Errorf("POSTs=%d", posts.Load())
					}
					switch mode {
					case "upload_first", "remote_failure":
						if oldPuts.Load() != 1 || committed != 1 {
							t.Errorf("valid recovery old PUT=%d committed=%d", oldPuts.Load(), committed)
						}
					case "rename_failure":
						if oldPuts.Load() != 0 || committed != 0 {
							t.Errorf("quarantine bypass old PUT=%d committed=%d", oldPuts.Load(), committed)
						}
						if meta, ok := fs.pendingIndex.GetMeta(oldP); !ok || meta.Kind != PendingConflict {
							t.Errorf("pending=%+v/%t", meta, ok)
						}
						if data, err := fs.shadowStore.ReadAll(oldP); err != nil || string(data) != "payload" {
							t.Errorf("retained=%q/%v", data, err)
						}
					case "rename_success":
						if oldPuts.Load() != 0 || committed != 0 || newPuts.Load() != 1 {
							t.Errorf("old=%d new=%d recovered=%d", oldPuts.Load(), newPuts.Load(), committed)
						}
					}
					for _, p := range []string{oldP, newP} {
						fs.remoteCommitMu.Lock()
						lock := fs.remoteCommitLocks[p]
						fs.remoteCommitMu.Unlock()
						if lock == nil || !lock.TryLock() {
							t.Errorf("commit lock not released: %s", p)
						} else {
							lock.Unlock()
						}
					}
					t.Logf("ORDER mode=%s old_put=%d new_put=%d rename=%d status=%v committed=%d", mode, oldPuts.Load(), newPuts.Load(), posts.Load(), st, committed)
				})
			}
		}
	}
}
func TestOwnedRenameCaptureChangeRetriesBeforeRemote(t *testing.T) {
	for _, changed := range []string{"new_path", "new_generation"} {
		t.Run(changed, func(t *testing.T) {
			var posts atomic.Int32
			fs := newOwnedRenameFenceFS(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					posts.Add(1)
				}
				w.Header().Set("Content-Length", "0")
				w.Header().Set("X-Dat9-IsDir", "true")
			}, false)
			fs.inodes.Lookup("/old", true, 0, time.Time{})
			fs.inodes.Lookup("/new", true, 0, time.Time{})
			stageOwnedRenameChild(t, fs, "/old/file.txt")
			captures := 0
			enableOwnedRenameFenceHook(t, fs, func(observed *Dat9FS, phase string) {
				if observed != fs || phase != "captured" {
					return
				}
				captures++
				if captures > 1 {
					return
				}
				p := "/old/file.txt"
				if changed == "new_path" {
					p = "/old/later.txt"
				}
				stageOwnedRenameChild(t, fs, p)
			})
			st := fs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "old", "new")
			if st != gofuse.OK || posts.Load() != 1 || captures != 2 {
				t.Fatalf("changed capture rename=%v POSTs=%d captures=%d", st, posts.Load(), captures)
			}
			if data, err := fs.shadowStore.ReadAll("/new/file.txt"); err != nil || string(data) != "payload" {
				t.Fatalf("source lost: %q/%v", data, err)
			}
			t.Logf("CAPTURE change=%s status=%v POSTs=%d", changed, st, posts.Load())
		})
	}
}

func enableOwnedRenameFenceHook(t *testing.T, fs *Dat9FS, hook func(*Dat9FS, string)) {
	t.Helper()
	for point, phase := range map[string]string{"ownedRenamePathsCaptured": "captured", "ownedRenamePathsLocked": "locked"} {
		name := "github.com/mem9-ai/drive9/pkg/fuse/" + point
		if err := failpoint.EnableCall(name, func(observed *Dat9FS) {
			if observed == fs {
				hook(observed, phase)
			}
		}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = failpoint.Disable(name) })
	}
}

func TestOwnedRenameMultipleChildrenAndLatePublication(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint(reverse), func(t *testing.T) {
			oldDir, newDir := "/a", "/z"
			if reverse {
				oldDir, newDir = newDir, oldDir
			}
			var fs *Dat9FS
			var uploads atomic.Int32
			fs = newOwnedRenameFenceFS(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodHead:
					w.Header().Set("Content-Length", "0")
					w.Header().Set("X-Dat9-IsDir", "true")
				case http.MethodGet:
					_, _ = io.WriteString(w, `{"entries":[]}`)
				case http.MethodPost:
					for _, name := range []string{"one", "two"} {
						for _, dir := range []string{oldDir, newDir} {
							p := dir + "/" + name
							fs.remoteCommitMu.Lock()
							lock := fs.remoteCommitLocks[p]
							fs.remoteCommitMu.Unlock()
							if lock == nil {
								t.Errorf("missing lock %s", p)
							} else if lock.TryLock() {
								lock.Unlock()
								t.Errorf("unlocked path %s", p)
							}
						}
					}
				case http.MethodPut:
					if !strings.HasPrefix(r.URL.Path, "/v1/fs"+newDir+"/") {
						t.Errorf("old-path upload: %s", r.URL.Path)
					}
					uploads.Add(1)
					_, _ = io.WriteString(w, `{"revision":1}`)
				}
			}, true)
			fs.uploader.stopped.Store(true)
			fs.inodes.Lookup(oldDir, true, 0, time.Time{})
			fs.inodes.Lookup(newDir, true, 0, time.Time{})
			for _, name := range []string{"one", "two"} {
				stageOwnedRenameChild(t, fs, oldDir+"/"+name)
			}
			enableOwnedRenameFenceHook(t, fs, func(_ *Dat9FS, phase string) {
				if phase == "locked" {
					stageOwnedRenameChild(t, fs, oldDir+"/late")
				}
			})
			point := "github.com/mem9-ai/drive9/pkg/fuse/ownedWriteBackRenameRekeyed"
			var sawLate atomic.Bool
			if err := failpoint.EnableCall(point, func(observed *Dat9FS, oldPath, newPath string) {
				if observed != fs || oldPath != oldDir+"/late" {
					return
				}
				fs.uploader.inflightMu.Lock()
				source, target := fs.uploader.inflight[oldPath], fs.uploader.inflight[newPath]
				fs.uploader.inflightMu.Unlock()
				if source == nil || target == nil {
					t.Error("late entry borrowed exclusions it did not hold")
				}
				sawLate.Store(true)
			}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = failpoint.Disable(point) })
			st := fs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, strings.TrimPrefix(oldDir, "/"), strings.TrimPrefix(newDir, "/"))
			if st != gofuse.OK || uploads.Load() != 3 || !sawLate.Load() {
				t.Fatalf("rename=%v uploads=%d late=%t", st, uploads.Load(), sawLate.Load())
			}
			if len(fs.pendingIndex.ListPendingPaths()) != 0 {
				t.Fatal("successful migration left pending data")
			}
		})
	}
}

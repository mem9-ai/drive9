//go:build failpoint

package fuse

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pingcap/failpoint"
)

func TestQueueOnlyFtruncateRenameInternalRetry(t *testing.T) {
	for _, tc := range []struct {
		name            string
		flush           bool
		handoff, worker string
	}{
		{"drained_before_wait", true, "before_rename", "immediate"},
		{"handoff_after_wait_upload_after_remote", true, "after_wait", "after_remote"},
		{"handoff_after_wait_worker_after_rename", true, "after_wait", "after_rename"},
		{"live_fd_handoff_after_capture", false, "after_capture", "after_rename"},
		{"live_fd_release_after_fence", false, "before_remote", "after_rename"},
		{"live_fd_release_after_queue_check", false, "after_queue_check", "after_rename"},
		{"queue_deadline", true, "after_wait", "after_rename"},
		{"cancel_legacy", true, "after_wait", "after_rename"},
		{"cancel_default", true, "after_wait", "after_rename"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const oldP, newP = "/old/file.bin", "/new/file.bin"
			var mu sync.Mutex
			remotePath, remoteData, rev := oldP, "hello world", int64(1)
			renamed, oldPuts, newPuts, posts := false, 0, 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				p := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/fs"), "/")
				if p == "" {
					p = "/"
				}
				mu.Lock()
				defer mu.Unlock()
				switch r.Method {
				case http.MethodHead:
					if p == "/" || p == "/old" && !renamed || p == "/new" && renamed {
						w.Header().Set("X-Dat9-IsDir", "true")
						w.Header().Set("Content-Length", "0")
						return
					}
					if p != remotePath {
						http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
						return
					}
					w.Header().Set("X-Dat9-IsDir", "false")
					w.Header().Set("Content-Length", strconv.Itoa(len(remoteData)))
					w.Header().Set("X-Dat9-Revision", strconv.FormatInt(rev, 10))
					w.Header().Set("X-Dat9-Resource-ID", "same-existing-file")
				case http.MethodGet:
					if r.URL.Query().Has("list") {
						_, _ = io.WriteString(w, `{"entries":[]}`)
						return
					}
					if p != remotePath {
						http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
						return
					}
					_, _ = io.WriteString(w, remoteData)
				case http.MethodPost:
					if p != "/new" || r.Header.Get("X-Dat9-Rename-Source") != "/old" {
						t.Errorf("unexpected rename %s source=%q", r.URL.String(), r.Header.Get("X-Dat9-Rename-Source"))
						w.WriteHeader(400)
						return
					}
					posts++
					renamed = true
					remotePath = newP
				case http.MethodPut:
					if renamed && p == oldP {
						oldPuts++
					}
					if p == newP {
						newPuts++
					}
					// A strict CAS mock rejects an old-path write after namespace rename.
					// Thus this test does not rely on the server recreating missing parents.
					if p != remotePath || r.Header.Get("X-Dat9-Expected-Revision") != strconv.FormatInt(rev, 10) {
						http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
						return
					}
					data, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
						w.WriteHeader(500)
						return
					}
					remoteData = string(data)
					rev++
					_, _ = fmt.Fprintf(w, `{"revision":%d}`, rev)
				default:
					t.Errorf("unexpected %s %s", r.Method, r.URL.String())
					w.WriteHeader(400)
				}
			}))
			defer server.Close()
			opts := &MountOptions{SyncMode: SyncStrict, WritePolicy: WritePolicyWriteBack, TrustLocalEvents: true}
			if tc.name == "cancel_legacy" {
				opts.LegacyInterruptibleMutations = true
			}
			opts.setDefaults()
			fs := NewDat9FS(newTestClient(server.URL), opts)
			fs.client.SetSmallFileThresholdForTests(1 << 20)
			var err error
			fs.shadowStore, err = NewShadowStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer fs.shadowStore.Close()
			fs.pendingIndex, err = NewPendingIndex(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			fs.writeBack, err = NewWriteBackCache(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			fs.uploader = &WriteBackUploader{client: fs.client, cache: fs.writeBack, remoteRoot: "/", uploadCh: make(chan string, 8)}
			fs.commitQueue = NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 8)
			fs.commitQueue.OnUploaded = fs.onCommitQueueUploaded
			fs.commitQueue.OnSuccess = fs.onCommitQueueSuccess
			fs.commitQueue.OnCleanup = fs.onCommitQueueCleanup
			fs.commitQueue.DurableWatermark = fs.latestCommittedRevision
			fs.commitQueue.IsSuperseded = fs.commitEntrySuperseded
			workerSelected, workerResume := make(chan struct{}), make(chan struct{})
			var oldWorkerAdmissions atomic.Int32
			var selectedOnce, resumeOnce sync.Once
			resume := func() { resumeOnce.Do(func() { close(workerResume) }) }
			fs.commitQueue.PathLock = func(p string) func() {
				if p == oldP {
					oldWorkerAdmissions.Add(1)
				}
				selectedOnce.Do(func() { close(workerSelected) })
				<-workerResume
				return fs.lockRemoteCommitPath(p)
			}
			defer func() { resume(); fs.commitQueue.DrainAll() }()
			fs.inodes.Lookup("/old", true, 0, time.Now())
			ino := fs.inodes.Lookup(oldP, false, 11, time.Now())
			fs.inodes.UpdateRevision(ino, 1)
			var open gofuse.OpenOut
			if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR)}, &open); st != gofuse.OK {
				t.Fatal(st)
			}
			reviewFtruncate(t, fs, ino, open.Fh, 5)
			if tc.flush {
				if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: open.Fh}); st != gofuse.OK {
					t.Fatal(st)
				}
				if _, ok := fs.writeBack.GetMeta(oldP); !ok {
					t.Fatal("Flush did not create writeback control snapshot")
				}
			}
			handoff := func() {
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: open.Fh})
				select {
				case <-workerSelected:
				case <-time.After(5 * time.Second):
					t.Fatal("Release did not enqueue actual worker")
				}
				if _, ok := fs.writeBack.GetMeta(oldP); ok {
					t.Fatal("handoff retained writeBack snapshot")
				}
				if !fs.pendingIndex.HasPending(oldP) || !fs.shadowStore.Has(oldP) || !fs.commitQueue.HasPath(oldP) {
					t.Fatal("handoff missing queue/pending/shadow ownership")
				}
				t.Logf("HANDOFF cache=false pending=true shadow=true queue=true")
			}
			if tc.handoff == "before_rename" {
				handoff()
				resume()
				fs.commitQueue.WaitPath(oldP)
			}
			releasedAfterFence := make(chan struct{})
			asyncRelease := false
			busyBefore, busyAfter := make(chan struct{}), make(chan struct{})
			var busyBeforeOnce, busyAfterOnce sync.Once
			var fenceReleased atomic.Bool
			fired := false
			requestCancel := make(chan struct{})
			var requestCancelOnce sync.Once
			var requestCtx, selectedCtx context.Context
			ctxCalls := 0
			assertNoOldPublication := func(phase string) {
				t.Helper()
				fs.commitQueue.mu.Lock()
				queued := fs.commitQueue.hasPendingPrefixLocked("/old/")
				fs.commitQueue.mu.Unlock()
				if queued || fs.pendingIndex.HasPending(oldP) || fs.shadowStore.Has(oldP) || oldWorkerAdmissions.Load() != 0 {
					t.Errorf("%s: Release published old-path staging or queue work", phase)
				}
				select {
				case <-releasedAfterFence:
					t.Errorf("%s: Release completed before handle retarget", phase)
				default:
				}
				t.Logf("PUBLICATION phase=%s old_queue=%t old_worker_admissions=%d", phase, queued, oldWorkerAdmissions.Load())
			}
			contextHook := func(observed *Dat9FS, parent, chosen context.Context) context.Context {
				if observed != fs {
					return chosen
				}
				ctxCalls++
				requestCtx = parent
				selectedCtx = chosen
				if tc.name == "queue_deadline" {
					short, stop := context.WithTimeout(chosen, 80*time.Millisecond)
					t.Cleanup(stop)
					selectedCtx = short
				}
				return selectedCtx
			}
			enableOwnedRenameContextHook(t, fs, contextHook)
			enableQueueRenameHook(t, fs, func(observed *Dat9FS, phase string) {
				if observed != fs {
					return
				}
				if phase == "retry_released" {
					assertRetryExclusionsReleased(t, fs, oldP, newP, "")
					t.Log("INTERNAL_RETRY all exclusions released before queue drain")
					if tc.name == "queue_deadline" {
						return
					}
					if tc.name == "cancel_legacy" || tc.name == "cancel_default" {
						requestCancelOnce.Do(func() { close(requestCancel) })
						select {
						case <-requestCtx.Done():
						case <-time.After(time.Second):
							t.Fatal("request cancel was not observed")
						}
						if tc.name == "cancel_legacy" {
							return
						}
						if selectedCtx.Err() != nil {
							t.Error("default interrupt-safe policy changed")
						}
					}
					resume()
					return
				}
				if phase == "release_busy" {
					busyBeforeOnce.Do(func() { close(busyBefore) })
					if fenceReleased.Load() {
						busyAfterOnce.Do(func() { close(busyAfter) })
					}
					return
				}
				if phase == "after_fence_release" && asyncRelease {
					if linked, exists := fs.inodes.GetInode(oldP); exists && linked == ino {
						t.Error("commit fence released before inode namespace publication")
					}
					fenceReleased.Store(true)
					select {
					case <-busyAfter:
					case <-releasedAfterFence:
						t.Error("Release published before local handle retargeting: inode mapping was not protected")
					case <-time.After(5 * time.Second):
						t.Fatal("Release did not retry at fence-release boundary")
					}
					if tc.handoff == "after_queue_check" {
						assertNoOldPublication(phase)
					}
					return
				}
				if !fired && phase == tc.handoff {
					fired = true
					if tc.handoff == "before_remote" || tc.handoff == "after_queue_check" {
						if tc.handoff == "after_queue_check" {
							mu.Lock()
							renameCalls := posts
							mu.Unlock()
							if renameCalls != 0 {
								t.Fatal("queue admission hook ran after remote rename")
							}
							assertNoOldPublication("before_release_attempt")
						}
						fs.remoteCommitMu.Lock()
						lock := fs.remoteCommitLocks[oldP]
						fs.remoteCommitMu.Unlock()
						free := lock == nil || lock.TryLock()
						if lock != nil && free {
							lock.Unlock()
						}
						if free {
							handoff()
						} else {
							asyncRelease = true
							go func() {
								fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: open.Fh})
								close(releasedAfterFence)
							}()
							select {
							case <-busyBefore:
							case <-releasedAfterFence:
								t.Fatal("Release bypassed held child fence")
							case <-time.After(5 * time.Second):
								t.Fatal("Release did not attempt held child fence")
							}
							if tc.handoff == "after_queue_check" {
								assertNoOldPublication("after_release_attempt")
							}
						}
					} else {
						handoff()
					}
				}
				if tc.handoff == "after_queue_check" && (phase == "before_remote" || phase == "after_remote") {
					assertNoOldPublication(phase)
				}
				if phase == "before_remote" {
					fs.remoteCommitMu.Lock()
					lock := fs.remoteCommitLocks[oldP]
					fs.remoteCommitMu.Unlock()
					fenced := lock != nil && !lock.TryLock()
					if lock != nil && !fenced {
						lock.Unlock()
					}
					t.Logf("PRE_REMOTE child_fenced=%t writeback=%d pending=%d", fenced, len(fs.writeBack.ListByPrefix("/old/")), len(fs.pendingIndex.ListByPrefix("/old/")))
				}
				if phase == "after_remote" && tc.worker == "after_remote" {
					// Do not wait inside Rename for a worker blocked by its child fence.
					// Without that fence, force the old upload before local migration.
					fs.remoteCommitMu.Lock()
					lock := fs.remoteCommitLocks[oldP]
					fs.remoteCommitMu.Unlock()
					free := lock == nil || lock.TryLock()
					if lock != nil && free {
						lock.Unlock()
					}
					resume()
					if free {
						fs.commitQueue.WaitPath(oldP)
					}
				}
			})
			st := fs.Rename(requestCancel, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "old", "new")

			if ctxCalls != 1 {
				t.Errorf("operation context renewed %d times", ctxCalls)
			}
			if tc.name == "queue_deadline" || tc.name == "cancel_legacy" {
				mu.Lock()
				beforeRPC := posts
				mu.Unlock()
				if st != gofuse.EAGAIN || beforeRPC != 0 {
					t.Errorf("aborted retry status=%v remote rename=%d", st, beforeRPC)
				}
				if !fs.commitQueue.HasPath(oldP) || !fs.pendingIndex.HasPending(oldP) {
					t.Error("aborted rename canceled or consumed accepted queue data")
				}
				assertRetryExclusionsReleased(t, fs, oldP, newP, "")
				resume()
				fs.commitQueue.DrainAll()
				mu.Lock()
				data, n := remoteData, posts
				mu.Unlock()
				if data != "hello" || n != 0 {
					t.Errorf("accepted upload could not finish after rename abort: data=%q renames=%d", data, n)
				}
				t.Logf("ABORT mode=%s status=%v remote_rename=%d retained_commit=%q", tc.name, st, n, data)
				return
			}

			if asyncRelease {
				select {
				case <-releasedAfterFence:
				case <-time.After(5 * time.Second):
					t.Fatal("Release did not progress after local namespace publication")
				}
			}
			resume()
			fs.commitQueue.DrainAll()
			mu.Lock()
			got, countOld, countNew, countRename := remoteData, oldPuts, newPuts, posts
			mu.Unlock()
			pending, hasPending := fs.pendingIndex.GetMeta(newP)
			bytes, readErr := fs.shadowStore.ReadAll(newP)
			t.Logf("RESULT status=%v remote=%q old_path_puts=%d new_path_puts=%d rename=%d new_pending=%t pending_meta=%+v retained=%q/%v queued_new=%t", st, got, countOld, countNew, countRename, hasPending, pending, bytes, readErr, fs.commitQueue.HasPath(newP))
			if st != gofuse.OK {
				t.Errorf("rename=%v", st)
			}
			if countRename != 1 {
				t.Errorf("remote rename calls=%d want 1", countRename)
			}
			if got != "hello" {
				t.Errorf("acknowledged truncate did not land at renamed path: got %q want hello", got)
			}
			if countOld != 0 {
				t.Errorf("%d old-path uploads after remote rename", countOld)
			}
			if tc.handoff == "after_queue_check" && (!fired || !asyncRelease || countNew != 1 || oldWorkerAdmissions.Load() != 0) {
				t.Errorf("queue boundary not preserved: fired=%t blocked=%t new_puts=%d old_worker_admissions=%d", fired, asyncRelease, countNew, oldWorkerAdmissions.Load())
			}
		})
	}
}

func enableQueueRenameHook(t *testing.T, fs *Dat9FS, hook func(*Dat9FS, string)) {
	t.Helper()
	point := "github.com/mem9-ai/drive9/pkg/fuse/ownedDirectoryRenamePhase"
	if err := failpoint.EnableCall(point, func(observed *Dat9FS, phase string) {
		if observed == fs {
			hook(observed, phase)
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = failpoint.Disable(point) })
	capture := "github.com/mem9-ai/drive9/pkg/fuse/ownedRenamePathsCaptured"
	if err := failpoint.EnableCall(capture, func(observed *Dat9FS) {
		if observed == fs {
			hook(observed, "after_capture")
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = failpoint.Disable(capture) })
	admission := "github.com/mem9-ai/drive9/pkg/fuse/ownedRenamePathsLocked"
	if err := failpoint.EnableCall(admission, func(observed *Dat9FS) {
		if observed == fs {
			hook(observed, "after_queue_check")
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = failpoint.Disable(admission) })
	busy := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateCommitFenceBusy"
	if err := failpoint.EnableCall(busy, func(observed *Dat9FS, _ *FileHandle) {
		if observed == fs {
			hook(observed, "release_busy")
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = failpoint.Disable(busy) })
}
func enableOwnedRenameContextHook(t *testing.T, fs *Dat9FS, hook func(*Dat9FS, context.Context, context.Context) context.Context) {
	t.Helper()
	point := "github.com/mem9-ai/drive9/pkg/fuse/ownedRenameRetryContext"
	if err := failpoint.EnableCall(point, func(observed *Dat9FS, parent context.Context, chosen *context.Context) {
		if observed == fs {
			*chosen = hook(observed, parent, *chosen)
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = failpoint.Disable(point) })
}

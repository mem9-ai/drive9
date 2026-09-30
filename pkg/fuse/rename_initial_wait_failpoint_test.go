//go:build failpoint

package fuse

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pingcap/failpoint"
)

func newRenameInitialWaitFS(t *testing.T, directory bool) (*Dat9FS, *atomic.Int32) {
	t.Helper()
	posts := &atomic.Int32{}
	fs := newOwnedRenameFenceFS(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			if r.URL.Path == "/v1/fs/new" {
				http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
				return
			}
			w.Header().Set("X-Dat9-IsDir", fmt.Sprint(directory))
			w.Header().Set("Content-Length", "0")
		case http.MethodGet:
			_, _ = io.WriteString(w, `{"entries":[]}`)
		case http.MethodPost:
			posts.Add(1)
		case http.MethodPut:
			_, _ = io.Copy(io.Discard, r.Body)
			_, _ = io.WriteString(w, `{"revision":2}`)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.String())
		}
	}, false)
	fs.inodes.Lookup("/old", directory, 0, time.Now())
	return fs, posts
}

// The marker has no worker: it models each admitted queue representation
// whose existing worker is stalled. Remove it only after checking timeout
// preservation, or deliberately to test forward progress.
func holdRenameQueueEntry(fs *Dat9FS, path, representation string) (*CommitEntry, func()) {
	cq := &CommitQueue{
		inFlight:     make(map[string]*CommitEntry),
		queuedByPath: make(map[string]map[*CommitEntry]struct{}),
		immediate:    make(map[*CommitEntry]struct{}),
	}
	entry := &CommitEntry{Path: path, Inode: 3, MutationSeq: 1}
	switch representation {
	case "queued":
		cq.queue = []*CommitEntry{entry}
		cq.queuedByPath[path] = map[*CommitEntry]struct{}{entry: {}}
	case "inflight":
		cq.inFlight[path] = entry
	case "immediate":
		cq.immediate[entry] = struct{}{}
	}
	fs.commitQueue = cq
	var once sync.Once
	return entry, func() {
		once.Do(func() {
			cq.mu.Lock()
			cq.queue = nil
			delete(cq.queuedByPath, path)
			delete(cq.inFlight, path)
			delete(cq.immediate, entry)
			cq.mu.Unlock()
		})
	}
}

func TestRenameInitialWaitDeadline(t *testing.T) {
	for _, kind := range []string{"queued", "inflight", "immediate", "uploader"} {
		for _, path := range []string{"/old", "/new", "/old/child"} {
			if kind == "uploader" && path == "/old/child" {
				continue // Descendant upload fences have their own existing tests.
			}
			for _, policy := range []string{"deadline", "legacy_cancel", "default_cancel", "gvisor_cancel"} {
				t.Run(fmt.Sprintf("%s%s/%s", kind, path, policy), func(t *testing.T) {
					fs, posts := newRenameInitialWaitFS(t, path == "/old/child")
					fs.opts.LegacyInterruptibleMutations = policy == "legacy_cancel"
					fs.opts.GVisorCompat = policy == "gvisor_cancel"
					var entry *CommitEntry
					var unblock func()
					if kind == "uploader" {
						release := fs.uploader.acquirePath(path)
						var once sync.Once
						unblock = func() { once.Do(release) }
					} else {
						entry, unblock = holdRenameQueueEntry(fs, path, kind)
					}
					defer unblock()
					// Accepted bytes must survive an aborted wait, independently of
					// the synthetic occupancy marker used to control scheduling.
					dataPath := path
					if err := fs.shadowStore.WriteFull(dataPath, []byte("hello"), 0); err != nil {
						t.Fatal(err)
					}
					if _, err := fs.pendingIndex.PutWithBaseRev(dataPath, 5, PendingOverwrite, 1); err != nil {
						t.Fatal(err)
					}
					selected := make(chan context.Context, 1)
					var contextCalls atomic.Int32
					enableOwnedRenameContextHook(t, fs, func(_ *Dat9FS, parent, chosen context.Context) context.Context {
						contextCalls.Add(1)
						short, stop := context.WithTimeout(chosen, 100*time.Millisecond)
						t.Cleanup(stop)
						selected <- short
						return short
					})
					busy := make(chan struct{}, 1)
					enableQueueRenameHook(t, fs, func(_ *Dat9FS, phase string) {
						if phase == "initial_wait_busy" {
							select {
							case busy <- struct{}{}:
							default:
							}
						}
					})
					cancel := make(chan struct{})
					done := make(chan gofuse.Status, 1)
					go func() {
						done <- fs.Rename(cancel, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "old", "new")
					}()
					var operation context.Context
					select {
					case operation = <-selected:
					case status := <-done:
						t.Fatalf("Rename exited before wait: status=%v; fixture did not reach admission", status)
					case <-time.After(400 * time.Millisecond):
						t.Error("initial wait bypassed operation context selection")
						close(cancel)
						unblock()
						select {
						case <-done:
						case <-time.After(3 * time.Second):
							t.Fatal("Rename failed to exit after test released occupancy")
						}
						return
					}
					awaitOwnedRenameStep(t, busy, "initial wait observes existing occupancy")
					if policy != "deadline" {
						close(cancel)
					}
					var status gofuse.Status
					select {
					case status = <-done:
					case <-time.After(time.Second):
						unblock()
						<-done
						t.Fatal("initial wait ignored its operation deadline")
					}
					if status != gofuse.EAGAIN || posts.Load() != 0 || contextCalls.Load() != 1 {
						t.Errorf("status=%v renames=%d contexts=%d", status, posts.Load(), contextCalls.Load())
					}
					wantErr := context.DeadlineExceeded
					if policy == "legacy_cancel" {
						wantErr = context.Canceled
					}
					if operation.Err() != wantErr {
						t.Errorf("operation error=%v want %v", operation.Err(), wantErr)
					}
					if entry != nil {
						fs.commitQueue.mu.Lock()
						canceled := entry.canceled
						fs.commitQueue.mu.Unlock()
						if canceled || !fs.commitQueue.HasPath(path) {
							t.Error("Rename canceled or removed accepted queue entry")
						}
					} else {
						fs.uploader.inflightMu.Lock()
						occupied := fs.uploader.inflight[path] != nil
						fs.uploader.inflightMu.Unlock()
						if !occupied {
							t.Error("Rename removed another uploader's occupancy")
						}
					}
					if data, err := fs.shadowStore.ReadAll(dataPath); err != nil || string(data) != "hello" || !fs.pendingIndex.HasPending(dataPath) {
						t.Errorf("wait discarded staged data: %q/%v", data, err)
					}
				})
			}
		}
	}
}

func TestRenameFlushReacquisitionDeadline(t *testing.T) {
	for _, path := range []string{"/old", "/new"} {
		for _, mode := range []string{"deadline", "legacy_cancel", "recovers"} {
			t.Run(path+"/"+mode, func(t *testing.T) {
				fs, posts := newRenameInitialWaitFS(t, false)
				fs.opts.LegacyInterruptibleMutations = mode == "legacy_cancel"
				if err := fs.writeBack.PutWithBaseRev("/old", []byte("hello"), 5, PendingOverwrite, 1); err != nil {
					t.Fatal(err)
				}
				var unblock func()
				var operation context.Context
				enableOwnedRenameContextHook(t, fs, func(_ *Dat9FS, _, chosen context.Context) context.Context {
					budget := 100 * time.Millisecond
					if mode == "recovers" {
						budget = time.Second
					}
					short, stop := context.WithTimeout(chosen, budget)
					t.Cleanup(stop)
					operation = short
					return short
				})
				acquired := make(chan struct{})
				enableQueueRenameHook(t, fs, func(_ *Dat9FS, phase string) {
					if phase == "before_pending_flush" {
						release := fs.uploader.acquirePath(path)
						var once sync.Once
						unblock = func() { once.Do(release) }
						close(acquired)
					}
				})
				cancel := make(chan struct{})
				point := "github.com/mem9-ai/drive9/pkg/fuse/renameUploadSyncBusy"
				var busyOnce sync.Once
				var observedBusy atomic.Bool
				if err := failpoint.EnableCall(point, func(u *WriteBackUploader, p string) {
					if u != fs.uploader || p != path {
						return
					}
					observedBusy.Store(true)
					busyOnce.Do(func() {
						if mode == "recovers" {
							unblock()
						}
						if mode == "legacy_cancel" {
							close(cancel)
						}
					})
				}); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = failpoint.Disable(point) })
				done := make(chan gofuse.Status, 1)
				go func() {
					done <- fs.Rename(cancel, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "old", "new")
				}()
				awaitOwnedRenameStep(t, acquired, "competing uploader acquired after drain")
				defer unblock()
				select {
				case status := <-done:
					if !observedBusy.Load() {
						t.Error("did not exercise failed uploader acquisition")
					}
					if mode == "recovers" {
						if status != gofuse.OK || posts.Load() != 1 {
							t.Errorf("recovery status=%v renames=%d", status, posts.Load())
						}
						if _, ok := fs.writeBack.GetMeta("/old"); ok {
							t.Error("successful synchronous flush retained source cache")
						}
						assertRetryExclusionsReleased(t, fs, "/old", "/new", "")
					} else {
						expected := context.DeadlineExceeded
						if mode == "legacy_cancel" {
							expected = context.Canceled
						}
						if status != gofuse.EAGAIN || operation == nil || operation.Err() != expected || posts.Load() != 0 {
							t.Errorf("reacquisition status=%v context=%v renames=%d", status, operation, posts.Load())
						}
						if path == "/old" {
							if meta, ok := fs.writeBack.GetMeta("/old"); !ok || meta.Size != 5 {
								t.Error("failed uploader admission discarded source cache")
							}
						}
						assertRetryExclusionsReleased(t, fs, "/old", "/new", "uploader:"+path)
					}
				case <-time.After(2 * time.Second):
					t.Error("synchronous uploader reacquisition ignored request deadline")
					unblock()
					select {
					case <-done:
					case <-time.After(3 * time.Second):
						t.Fatal("Rename did not finish after releasing competing uploader")
					}
				}
			})
		}
	}
}

func TestRenameInitialWaitProgressAndBudget(t *testing.T) {
	for _, mode := range []string{"default_cancel_recovers", "gvisor_cancel_recovers", "legacy_recovers", "shared_budget", "expired", "delayed_old", "delayed_new", "delayed_child"} {
		t.Run(mode, func(t *testing.T) {
			fs, posts := newRenameInitialWaitFS(t, true)
			fs.opts.LegacyInterruptibleMutations = mode == "legacy_recovers"
			fs.opts.GVisorCompat = mode == "gvisor_cancel_recovers"
			path := "/old/child"
			if mode == "delayed_old" {
				path = "/old"
			} else if mode == "delayed_new" {
				path = "/new"
			}
			entry, unblock := holdRenameQueueEntry(fs, path, "queued")
			defer unblock()
			if mode == "delayed_old" || mode == "delayed_new" || mode == "delayed_child" {
				cq := fs.commitQueue
				cq.workCh = make(chan *CommitEntry, 2)
				timer := time.NewTimer(time.Hour)
				defer timer.Stop()
				cq.delayed = map[*CommitEntry]*time.Timer{entry: timer}
			}
			cancel := make(chan struct{})
			var operation, parent context.Context
			var calls, uploadContexts int
			enableOwnedRenameContextHook(t, fs, func(_ *Dat9FS, request, chosen context.Context) context.Context {
				calls++
				parent = request
				budget := 400 * time.Millisecond
				if mode == "expired" {
					budget = -time.Second
				}
				short, stop := context.WithTimeout(chosen, budget)
				t.Cleanup(stop)
				operation = short
				return short
			})
			point := "github.com/mem9-ai/drive9/pkg/fuse/renameUploadSyncContext"
			if err := failpoint.EnableCall(point, func(u *WriteBackUploader, ctx context.Context, _ string) {
				if u != fs.uploader {
					return
				}
				uploadContexts++
				if ctx != operation {
					t.Error("drain-to-flush transition replaced the operation context")
				}
			}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = failpoint.Disable(point) })
			var first sync.Once
			var releaseUploader func()
			defer func() {
				if releaseUploader != nil {
					releaseUploader()
				}
			}()
			enableQueueRenameHook(t, fs, func(_ *Dat9FS, phase string) {
				if phase == "initial_wait_busy" {
					first.Do(func() {
						if mode == "shared_budget" {
							// Consume a known portion of this actual operation's budget.
							// Scheduling is gated by the hook, not a guessed worker sleep.
							deadline, _ := operation.Deadline()
							timer := time.NewTimer(time.Until(deadline.Add(-150 * time.Millisecond)))
							<-timer.C
						}
						if mode == "default_cancel_recovers" || mode == "gvisor_cancel_recovers" {
							close(cancel)
							awaitOwnedRenameStep(t, parent.Done(), "request cancellation")
							if operation.Err() != nil {
								t.Error("interrupt-safe operation inherited FUSE cancellation")
							}
						}
						if fs.commitQueue.workCh != nil {
							select {
							case got := <-fs.commitQueue.workCh:
								if got != entry || !entry.dispatched || len(fs.commitQueue.delayed) != 0 {
									t.Error("initial wait did not activate the exact delayed entry")
								}
							default:
								t.Error("initial wait left delayed work undispatched")
							}
						}
						unblock()
					})
				}
				if phase == "before_pending_flush" && mode == "shared_budget" {
					releaseUploader = fs.uploader.acquirePath("/old")
				}
			})
			status := fs.Rename(cancel, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "old", "new")
			if calls != 1 {
				t.Errorf("context selected %d times", calls)
			}
			if mode == "shared_budget" || mode == "expired" {
				if status != gofuse.EAGAIN || operation.Err() != context.DeadlineExceeded || posts.Load() != 0 {
					t.Errorf("budget status=%v err=%v renames=%d", status, operation.Err(), posts.Load())
				}
			} else if status != gofuse.OK || posts.Load() != 1 || uploadContexts != 2 {
				t.Errorf("progress status=%v renames=%d flushes=%d", status, posts.Load(), uploadContexts)
			}
			if mode == "shared_budget" && uploadContexts != 1 {
				t.Error("shared budget never exercised synchronous uploader contention")
			}
		})
	}
}

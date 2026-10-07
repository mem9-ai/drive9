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
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestRegularFileRenameRejectsStaleEmptyFallback(t *testing.T) {
	for _, scenario := range []string{"plain_empty", "flush_after_capture_uncommitted", "flush_after_capture_landed_open", "release_after_wait_drained", "release_before_preflight", "zero_release_after_wait", "view_changed", "identity_changed", "binding_removed"} {
		t.Run(scenario, func(t *testing.T) {
			const oldP, newP = "/old", "/new"
			var mu sync.Mutex
			remotePath, remoteData, revision := oldP, "hello world", int64(1)
			if scenario == "plain_empty" || strings.HasSuffix(scenario, "changed") || scenario == "binding_removed" {
				remoteData = ""
			}
			creates, deletes, renames := 0, 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				p := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/fs"), "/")
				if p == "" {
					p = "/"
				}
				mu.Lock()
				defer mu.Unlock()
				switch r.Method {
				case http.MethodHead:
					if p == "/" {
						w.Header().Set("X-Dat9-IsDir", "true")
						w.Header().Set("Content-Length", "0")
						return
					}
					if p != remotePath {
						http.NotFound(w, r)
						return
					}
					w.Header().Set("X-Dat9-IsDir", "false")
					w.Header().Set("Content-Length", strconv.Itoa(len(remoteData)))
					w.Header().Set("X-Dat9-Revision", strconv.FormatInt(revision, 10))
					w.Header().Set("X-Dat9-Resource-ID", "source")
				case http.MethodGet:
					if r.URL.Query().Has("list") {
						_, _ = io.WriteString(w, `{"entries":[]}`)
						return
					}
					if p != remotePath {
						http.NotFound(w, r)
						return
					}
					_, _ = io.WriteString(w, remoteData)
				case http.MethodPut:
					if p != remotePath || r.Header.Get("X-Dat9-Expected-Revision") != strconv.FormatInt(revision, 10) {
						http.Error(w, `{"error":"revision conflict"}`, http.StatusConflict)
						return
					}
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
						w.WriteHeader(500)
						return
					}
					remoteData = string(body)
					revision++
					_, _ = fmt.Fprintf(w, `{"revision":%d}`, revision)
				case http.MethodPost:
					if p != newP {
						t.Errorf("unexpected POST %s", r.URL.String())
						w.WriteHeader(500)
						return
					}
					if r.URL.Query().Has("rename") {
						if r.Header.Get("X-Dat9-Rename-Source") != oldP {
							t.Error("wrong rename source")
						}
						renames++
						http.NotFound(w, r)
						return
					}
					if r.URL.Query().Has("create") {
						creates++
						remotePath, remoteData, revision = newP, "", int64(3)
						_, _ = fmt.Fprintf(w, `{"revision":%d}`, revision)
						return
					}
					t.Errorf("unexpected POST %s", r.URL.String())
					w.WriteHeader(500)
				case http.MethodDelete:
					if p != oldP || creates == 0 {
						t.Error("unexpected source delete")
					}
					deletes++
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Errorf("unexpected %s %s", r.Method, r.URL.String())
					w.WriteHeader(500)
				}
			}))
			defer server.Close()
			opts := &MountOptions{SyncMode: SyncStrict, WritePolicy: WritePolicyWriteBack, TrustLocalEvents: true}
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
			fs.uploader = &WriteBackUploader{client: fs.client, cache: fs.writeBack, remoteRoot: "/", inflight: make(map[string]*pathState), uploadCh: make(chan string, 16)}
			fs.uploader.SnapshotStagingGens = fs.snapshotStagingGens
			fs.uploader.OnDataCommitted = fs.onWriteBackDataCommitted
			fs.uploader.OnSuccess = fs.onWriteBackUploadSuccess
			fs.commitQueue = NewCommitQueue(fs.client, fs.shadowStore, fs.pendingIndex, nil, 1, 8)
			fs.commitQueue.PathLock = fs.lockRemoteCommitPath
			fs.commitQueue.OnUploaded = fs.onCommitQueueUploaded
			fs.commitQueue.OnSuccess = fs.onCommitQueueSuccess
			fs.commitQueue.OnCleanup = fs.onCommitQueueCleanup
			fs.commitQueue.DurableWatermark = fs.latestCommittedRevision
			fs.commitQueue.IsSuperseded = fs.commitEntrySuperseded
			defer fs.commitQueue.DrainAll()
			initialSize := int64(11)
			if scenario == "plain_empty" || strings.HasSuffix(scenario, "changed") || scenario == "binding_removed" {
				initialSize = 0
			}
			ino := fs.inodes.Lookup(oldP, false, initialSize, time.Now())
			fs.inodes.UpdateMode(ino, uint32(syscall.S_IFREG)|0o644)
			fs.inodes.UpdateRevision(ino, 1)
			var opened gofuse.OpenOut
			if scenario != "plain_empty" && !strings.HasSuffix(scenario, "changed") && scenario != "binding_removed" {
				if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR)}, &opened); st != gofuse.OK {
					t.Fatal(st)
				}
				reviewFtruncate(t, fs, ino, opened.Fh, 0)
			}
			wrote, released, drained := false, false, false
			flushFenceHeld, flushCacheOwned := false, false
			write := func() {
				if wrote {
					t.Fatal("duplicate child write")
				}
				if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: opened.Fh}, []byte("hello")); st != gofuse.OK {
					t.Fatalf("child Write=%v", st)
				}
				wrote = true
			}
			flush := func() {
				if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: opened.Fh}); st != gofuse.OK {
					t.Fatalf("Flush=%v", st)
				}
				fh, ok := fs.fileHandles.Get(opened.Fh)
				if !ok {
					t.Fatal("Flush removed fd")
				}
				fh.Lock()
				flushFenceHeld = fh.ftruncateFence && fh.RemoteCommitUnlock != nil
				fh.Unlock()
				if m, ok := fs.writeBack.GetMeta(oldP); ok {
					flushCacheOwned = m.ownedStagingKnown
				}
				if !flushFenceHeld || !flushCacheOwned {
					t.Fatal("healthy Flush did not publish fenced owned cache")
				}
			}
			release := func() {
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: opened.Fh})
				released = true
				fs.commitQueue.WaitPath(oldP)
				drained = true
				if _, ok := fs.fileHandles.Get(opened.Fh); ok {
					t.Fatal("Release retained fd")
				}
				mu.Lock()
				bytes := remoteData
				mu.Unlock()
				if scenario != "zero_release_after_wait" && bytes != "hello" {
					t.Fatalf("accepted child did not commit on normal Release: %q", bytes)
				}
			}
			if scenario == "release_before_preflight" {
				write()
				flush()
				release()
			}
			enableOwnedRenameContextHook(t, fs, func(_ *Dat9FS, _, chosen context.Context) context.Context {
				bounded, cancel := context.WithTimeout(chosen, 350*time.Millisecond)
				t.Cleanup(cancel)
				return bounded
			})
			preflightEntry, exists := fs.inodes.GetEntry(ino)
			if !exists {
				t.Fatal("source inode missing before Rename")
			}
			decisionSeen := false
			currentSize, currentRev := int64(-1), int64(-1)
			remoteBefore := ""
			preflightSize, preflightRev := preflightEntry.Size, preflightEntry.Revision
			fired := false
			enableQueueRenameHook(t, fs, func(_ *Dat9FS, phase string) {
				if phase == "before_remote" {
					decisionSeen = true
					if current, ok := fs.inodes.GetEntry(ino); ok {
						currentSize, currentRev = current.Size, current.Revision
					}
					mu.Lock()
					remoteBefore = remoteData
					mu.Unlock()
				}
				if fired {
					return
				}
				if strings.HasPrefix(scenario, "flush_after_capture") && phase == "after_capture" {
					fired = true
					write()
					flush()
					if scenario == "flush_after_capture_landed_open" {
						// Run the real uploader while the healthy fd retains its fence;
						// callbacks and Rename perform any retirement themselves.
						if err := fs.uploader.UploadSync(context.Background(), oldP); err != nil {
							t.Fatal(err)
						}
					}
				}
				if scenario == "release_after_wait_drained" && phase == "after_wait" {
					fired = true
					write()
					flush()
					release()
				}

				if scenario == "zero_release_after_wait" && phase == "after_wait" {
					fired = true
					flush()
					release()
				}
				if phase == "after_wait" {
					switch scenario {
					case "view_changed":
						fired = true
						fs.mountViewGeneration.Add(1)
					case "identity_changed":
						fired = true
						fs.inodes.EnsureInodeWithIdentity(oldP, "replacement", 1, false, 0, time.Now())
					case "binding_removed":
						fired = true
						fs.inodes.RemoveLink(oldP)
					}
				}
			})
			st := fs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "old", "new")
			mu.Lock()
			where, bytes, createCount, deleteCount, renameCount := remotePath, remoteData, creates, deletes, renames
			mu.Unlock()
			preserved := scenario == "plain_empty" || bytes == "hello"
			if scenario == "flush_after_capture_uncommitted" {
				if st != gofuse.EAGAIN || decisionSeen || createCount != 0 || deleteCount != 0 || renameCount != 0 {
					t.Fatalf("uncommitted image bypassed healthy fence: %v/%t/%d/%d/%d", st, decisionSeen, createCount, deleteCount, renameCount)
				}
				fh, ok := fs.fileHandles.Get(opened.Fh)
				if !ok {
					t.Fatal("Rename abort removed fd")
				}
				fh.Lock()
				retained := fh.DirtySeq != 0 && string(fh.Dirty.Bytes()) == "hello" && fh.RemoteCommitUnlock != nil
				fh.Unlock()
				if !retained {
					t.Fatal("Rename abort did not preserve healthy owner/image")
				}
				preserved = true
			}
			if scenario == "plain_empty" && (st != gofuse.OK || where != newP || createCount != 1 || deleteCount != 1) {
				t.Fatal("ordinary empty fallback compatibility failed")
			}
			if scenario == "release_before_preflight" && (st != gofuse.ENOENT || where != oldP || !preserved || createCount != 0 || deleteCount != 0) {
				t.Fatalf("fresh nonempty preflight control failed: status=%v, want %v", st, gofuse.ENOENT)
			}
			if scenario == "flush_after_capture_landed_open" || scenario == "release_after_wait_drained" {
				if !fired || !decisionSeen || remoteBefore != "hello" || preflightSize != 0 || currentSize != 5 {
					t.Fatalf("healthy stale-preflight boundary not reached: fired=%t decision=%t remote=%q frozen=%d current=%d", fired, decisionSeen, remoteBefore, preflightSize, currentSize)
				}
			}
			if scenario == "flush_after_capture_landed_open" || scenario == "release_after_wait_drained" {
				if st != gofuse.ENOENT || !preserved || where != oldP || createCount != 0 || deleteCount != 0 {
					t.Fatalf("Rename failed to preserve accepted child on stale preflight: status=%v, want %v", st, gofuse.ENOENT)
				}
			}
			if scenario == "zero_release_after_wait" {
				if !fired || !released || !drained || preflightSize != 0 || currentSize != 0 || currentRev <= preflightRev {
					t.Fatal("same-size committed revision transition missing")
				}
				if st != gofuse.ENOENT || createCount != 0 || deleteCount != 0 {
					t.Fatalf("Rename ignored same-size revision advance: status=%v, want %v", st, gofuse.ENOENT)
				}
			}
			if strings.HasSuffix(scenario, "changed") || scenario == "binding_removed" {
				if !fired || !decisionSeen {
					t.Fatal("identity/view control not reached")
				}
				if st != gofuse.ENOENT || createCount != 0 || deleteCount != 0 {
					t.Fatalf("Rename accepted changed identity/view/binding: status=%v, want %v", st, gofuse.ENOENT)
				}
			}
			if opened.Fh != 0 && !released {
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: opened.Fh})
				fs.commitQueue.DrainAll()
				mu.Lock()
				final := remoteData
				mu.Unlock()
				if final != "hello" {
					t.Fatalf("normal Release after Rename did not preserve child: %q", final)
				}
			}
			t.Logf("case=%s status=%v frozen=%d/%d current=%d/%d create=%d delete=%d final_path=%s", scenario, st, preflightSize, preflightRev, currentSize, currentRev, createCount, deleteCount, where)

		})
	}
}

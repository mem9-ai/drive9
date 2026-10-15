package fuse

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// Actual CQ-full Release must use its existing causal commit/retirement path.
// The old raw legacy branch and the old forced-overtaking probe are not used.
func TestR6LiveAppendCQFullFallbackOrdersAndRetiresLinkedSnapshots(t *testing.T) {
	for _, interactive := range []bool{false, true} {
		for _, scenario := range []string{"a_claims_first", "b_lands_first", "empty_rid_b_lands_first"} {
			t.Run(fmt.Sprintf("%s/interactive=%t", scenario, interactive), func(t *testing.T) {
				var fs *Dat9FS
				var ino uint64
				var server *casFileServer
				var ids []uint64
				resourceID := "shared-hardlink-resource"
				if scenario == "empty_rid_b_lands_first" {
					var sourceID, peerID uint64
					fs, ino, server, sourceID, peerID, _ = issue1023LinkWithoutConfirmationRID(t)
					ids = []uint64{sourceID, peerID}
					resourceID = "confirmed-link-shared-resource"
				} else {
					fs, ino, server, ids = r6LinkedHandlerFixture(t, false, nil, nil)
				}
				if interactive {
					fs.syncMode = SyncInteractive
				}
				a, _ := fs.fileHandles.Get(ids[0])
				b, _ := fs.fileHandles.Get(ids[1])
				queueEntered, queueRelease := make(chan struct{}), make(chan struct{})
				aEntered, aRelease := make(chan struct{}), make(chan struct{})
				releaseQueue := sync.OnceFunc(func() { close(queueRelease) })
				releaseA := sync.OnceFunc(func() { close(aRelease) })
				var queueOnce, aOnce sync.Once
				var aPuts, bPuts atomic.Int32
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/v1/fs/r6-queue-occupied" || r.URL.Path == "/v1/fs/r6-queue-pending" {
						if r.Method == http.MethodPut && r.URL.Path == "/v1/fs/r6-queue-occupied" {
							queueOnce.Do(func() { close(queueEntered) })
							<-queueRelease
						}
						_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "revision": 2})
						return
					}
					if r.URL.Path != "/v1/fs"+a.Path && r.URL.Path != "/v1/fs"+b.Path {
						http.NotFound(w, r)
						return
					}
					if r.Method == http.MethodPut && r.URL.Path == "/v1/fs"+a.Path {
						aPuts.Add(1)
						if scenario == "a_claims_first" {
							aOnce.Do(func() { close(aEntered) })
							<-aRelease
						}
					}
					if r.Method == http.MethodPut && r.URL.Path == "/v1/fs"+b.Path {
						bPuts.Add(1)
					}
					w.Header().Set("X-Dat9-Resource-ID", resourceID)
					w.Header().Set("X-Dat9-Nlink", "2")
					clone := r.Clone(r.Context())
					urlCopy := *r.URL
					urlCopy.Path = "/v1/fs" + server.path
					clone.URL = &urlCopy
					server.serveHTTP(w, clone)
				}))
				t.Cleanup(ts.Close)
				fs.client = newTestClient(ts.URL)
				fs.client.SetSmallFileThresholdForTests(1 << 20)
				fs.commitQueue.client = fs.client
				cache, err := NewWriteBackCache(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				uploader := NewWriteBackUploader(fs.client, cache, 1)
				fs.SetWriteBack(cache, uploader)
				uploader.SnapshotStagingGens = fs.snapshotStagingGens
				uploader.OnSuccess = fs.onWriteBackUploadSuccess
				t.Cleanup(uploader.DrainAll)
				t.Cleanup(func() { releaseA(); releaseQueue() })
				saturateQueue := func() {
					fs.commitQueue.mu.Lock()
					fs.commitQueue.maxPending = 2
					fs.commitQueue.mu.Unlock()
					entry := &CommitEntry{Path: "/r6-queue-occupied", BaseRev: 1, Size: 1, Kind: PendingOverwrite}
					entry.bindPayload([]byte("q"))
					if err := fs.commitQueue.Enqueue(entry); err != nil {
						t.Fatal(err)
					}
					select {
					case <-queueEntered:
					case <-time.After(3 * time.Second):
						t.Fatal("actual CQ worker did not reach its capacity gate")
					}
					entry = &CommitEntry{Path: "/r6-queue-pending", BaseRev: 1, Size: 1, Kind: PendingOverwrite}
					entry.bindPayload([]byte("q"))
					if err := fs.commitQueue.Enqueue(entry); err != nil {
						t.Fatal(err)
					}
				}
				if n, status := pr939Append(fs, ino, ids[0], "A"); n != 1 || status != gofuse.OK {
					t.Fatalf("A Write=%d/%v", n, status)
				}
				if status := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]}); status != gofuse.OK {
					t.Fatalf("A live Flush=%v", status)
				}
				if n, status := pr939Append(fs, ino, ids[1], "B"); n != 1 || status != gofuse.OK {
					t.Fatalf("B acknowledged live-owner Write=%d/%v", n, status)
				}
				if got := pr939HandleBytes(b); got != "baseAB" {
					t.Fatalf("B acknowledged image=%q", got)
				}
				if scenario != "a_claims_first" {
					if status := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]}); status != gofuse.OK {
						t.Fatalf("B first Fsync=%v", status)
					}
					waitCommitQueueIdle(t, fs.commitQueue)
					if _, body, _ := server.snapshot(); string(body) != "baseAB" {
						t.Fatal("B did not land the complete acknowledged descendant before A Release")
					}
					saturateQueue()
					if pending, _ := fs.commitQueue.PendingStats(); pending != 2 {
						t.Fatal("A Release did not enter the intentionally full CQ fixture")
					}
					fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]})
					if aPuts.Load() != 0 {
						t.Error("already-contained A snapshot was uploaded instead of validated retirement")
					}
				} else {
					saturateQueue()
					aStopped, bStopped := make(chan struct{}), make(chan struct{})
					bResult := make(chan gofuse.Status, 1)
					t.Cleanup(func() {
						releaseA()
						for _, stopped := range []chan struct{}{aStopped, bStopped} {
							select {
							case <-stopped:
							case <-time.After(3 * time.Second):
								t.Error("CQ fallback goroutine did not exit")
							}
						}
					})
					go func() {
						defer close(aStopped)
						fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]})
					}()
					select {
					case <-aEntered:
					case <-time.After(3 * time.Second):
						t.Fatal("synchronous append fallback did not reach its actual PUT")
					}
					fs.commitQueue.mu.Lock()
					claimed := false
					for active := range fs.commitQueue.immediate {
						claimed = claimed || active.Path == a.Path && active.Inode == ino
					}
					fs.commitQueue.mu.Unlock()
					if !claimed || uploader.hasPath(a.Path) {
						t.Fatal("CQ-full append did not use the existing immediate mutation claim")
					}
					var queuedB *CommitEntry
					if interactive {
						// A already claimed its synchronous fallback; now permit the
						// normal interactive enqueue without another capacity failure.
						fs.commitQueue.mu.Lock()
						fs.commitQueue.maxPending = 16
						fs.commitQueue.mu.Unlock()
					}
					go func() {
						defer close(bStopped)
						bResult <- fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]})
					}()
					if interactive {
						select {
						case status := <-bResult:
							if status != gofuse.OK {
								t.Fatalf("interactive B local Fsync=%v", status)
							}
						case <-time.After(3 * time.Second):
							t.Fatal("interactive B Fsync did not stage/enqueue")
						}
						fs.commitQueue.mu.Lock()
						for entry := range fs.commitQueue.queuedByPath[b.Path] {
							if entry.Inode == ino {
								queuedB = entry
							}
						}
						fs.commitQueue.mu.Unlock()
						if queuedB == nil {
							t.Fatal("interactive B did not enter actual CQ")
						}
						releaseQueue()
					}
					// Observe the actual immediate/worker ordering boundary, never
					// infer order from a scheduling delay.
					trace := make([]byte, 1<<20)
					deadline := time.Now().Add(3 * time.Second)
					waiting := false
					for !waiting && time.Now().Before(deadline) {
						n := runtime.Stack(trace, true)
						for _, goroutine := range strings.Split(string(trace[:n]), "\n\n") {
							if !interactive && strings.Contains(goroutine, fmt.Sprintf("(*Dat9FS).Fsync(%p", fs)) && strings.Contains(goroutine, "(*CommitQueue).beginImmediateMutationCommit(") {
								waiting = true
							}
							if interactive && strings.Contains(goroutine, fmt.Sprintf("(*CommitQueue).beginInFlight(%p", fs.commitQueue)) && strings.Contains(goroutine, fmt.Sprintf("%p", queuedB)) {
								waiting = true
							}
						}
						runtime.Gosched()
					}
					if !waiting || bPuts.Load() != 0 {
						t.Fatal("B remote commit did not wait on actual earlier same-inode claim")
					}
					releaseA()
					if !interactive {
						select {
						case status := <-bResult:
							if status != gofuse.OK {
								t.Errorf("ordered B Fsync=%v", status)
							}
						case <-time.After(3 * time.Second):
							t.Fatal("B Fsync did not complete after earlier A fallback")
						}
					}
					select {
					case <-aStopped:
					case <-time.After(3 * time.Second):
						t.Fatal("A Release did not complete after fallback")
					}
					if interactive {
						waitCommitQueueIdle(t, fs.commitQueue)
					}
				}
				// First verify A retired under the required full-queue condition.
				// Then remove unrelated artificial pressure before testing C's
				// healthy progress; interactive Fsync retains its defined limit.
				if meta, present := cache.GetMeta(a.Path); present && meta.Kind != PendingChmod {
					t.Error("A content was not retired before releasing dummy pressure")
				}
				if meta, present := fs.pendingIndex.GetMeta(a.Path); present && meta.Kind != PendingChmod {
					t.Error("A shadow metadata was not retired before releasing dummy pressure")
				}
				releaseQueue()
				waitCommitQueueIdle(t, fs.commitQueue)
				if n, status := pr939Append(fs, ino, ids[1], "C"); n != 1 || status != gofuse.OK {
					t.Errorf("C Write after fallback/retirement=%d/%v", n, status)
				} else if status := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]}); status != gofuse.OK {
					t.Errorf("C Fsync=%v", status)
				}
				releaseQueue()
				uploader.DrainAll()
				fs.commitQueue.DrainAll()
				if _, body, _ := server.snapshot(); string(body) != "baseABC" {
					t.Errorf("complete records after drain=%q, want baseABC", body)
				}
				if meta, present := cache.GetMeta(a.Path); present && meta.Kind != PendingChmod {
					t.Error("obsolete A content remains in WB after drain")
				}
				if meta, present := fs.pendingIndex.GetMeta(a.Path); present && meta.Kind != PendingChmod {
					t.Error("obsolete A content remains in pending-index after drain")
				}
			})
		}
	}
}

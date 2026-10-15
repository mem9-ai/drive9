package fuse

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestIssue1023AppendStagingFailurePreservesErrno(t *testing.T) {
	for _, operation := range []string{"flush", "interactive-fsync"} {
		for _, spill := range []bool{false, true} {
			for _, method := range []string{http.MethodHead, http.MethodGet} {
				for _, failure := range []struct {
					name string
					http int
					want gofuse.Status
				}{
					{"forbidden", http.StatusForbidden, gofuse.EACCES},
					{"missing", http.StatusNotFound, gofuse.ENOENT},
					{"precondition", http.StatusPreconditionFailed, gofuse.Status(syscall.ESTALE)},
					{"unavailable", http.StatusServiceUnavailable, gofuse.EAGAIN},
					{"disconnect", 0, gofuse.EIO},
				} {
					t.Run(fmt.Sprintf("%s/spill=%t/%s/%s", operation, spill, method, failure.name), func(t *testing.T) {
						var fs *Dat9FS
						var ino uint64
						var server *casFileServer
						var target *FileHandle
						var targetID, sourceID uint64
						if spill {
							var ids []uint64
							fs, server, _, ino, ids, target = issue1023CreateShadowAppenders(t, false)
							targetID, sourceID = ids[0], ids[1]
						} else {
							fs, ino, server, _ = reviewR2FS(t, "/staging-errno.txt", false)
							target, targetID = pr939Handle(t, fs, ino, "/staging-errno.txt", "base", false)
							_, sourceID = pr939Handle(t, fs, ino, "/staging-errno.txt", "base", false)
						}
						cache, err := NewWriteBackCache(t.TempDir())
						if err != nil {
							t.Fatal(err)
						}
						fs.writeBack = cache
						var fail atomic.Bool
						var failedReads, mutations atomic.Int32
						proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							if fail.Load() && r.Method == method {
								failedReads.Add(1)
								if failure.http == 0 {
									conn, _, err := w.(http.Hijacker).Hijack()
									if err != nil {
										t.Error(err)
										return
									}
									_ = conn.Close()
									return
								}
								w.WriteHeader(failure.http)
								return
							}
							if fail.Load() && r.Method != http.MethodHead && r.Method != http.MethodGet {
								mutations.Add(1)
							}
							server.serveHTTP(w, r)
						}))
						t.Cleanup(proxy.Close)
						fs.client = newTestClient(proxy.URL)
						fs.client.SetSmallFileThresholdForTests(1 << 20)
						fs.commitQueue.client = fs.client
						for _, write := range []struct {
							id   uint64
							data string
						}{{targetID, "A"}, {sourceID, "B"}, {sourceID, "C"}} {
							issue1023ShadowAppend(t, fs, ino, write.id, write.data)
						}
						if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: sourceID}); st != gofuse.OK {
							t.Fatal(st)
						}
						waitCommitQueueIdle(t, fs.commitQueue)
						fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: sourceID})
						waitCommitQueueIdle(t, fs.commitQueue)
						target.Lock()
						if target.ShadowSpill != spill || !target.Dirty.HasDirtyParts() ||
							fs.latestCommittedRevision(target.Path) <= target.BaseRev {
							target.Unlock()
							t.Fatal("live dirty ancestor staging premise lost")
						}
						before := string(target.Dirty.Bytes())
						buffer, version := target.Dirty, target.Dirty.contentVersion
						seq, base, size := target.DirtySeq, target.BaseRev, target.OrigSize
						gens := fs.captureHandleStagingGensLocked(target)
						target.Unlock()
						revision, body, puts := server.snapshot()
						path := target.Path
						invoke := func() gofuse.Status {
							if operation == "flush" {
								return fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: targetID})
							}
							return fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: targetID})
						}
						if operation == "interactive-fsync" {
							fs.syncMode = SyncInteractive
						}
						fail.Store(true)
						status := invoke()
						if status != failure.want {
							t.Errorf("staging failure status=%v want=%v", status, failure.want)
						}
						if failedReads.Load() == 0 {
							t.Fatal("fault did not reach the real remote verification read")
						}
						if mutations.Load() != 0 {
							t.Errorf("verification failure fell through to %d remote mutations", mutations.Load())
						}
						target.Lock()
						if target.Dirty != buffer || buffer.contentVersion != version ||
							target.DirtySeq != seq || target.BaseRev != base || target.OrigSize != size ||
							string(target.Dirty.Bytes()) != before || !target.Dirty.HasDirtyParts() ||
							fs.captureHandleStagingGensLocked(target) != gens {
							t.Error("verification failure changed dirty content, baseline or staging ownership")
						}
						target.Unlock()
						if _, ok := fs.writeBack.GetMeta(path); ok {
							t.Error("verification failure published a writeback fallback")
						}
						if rev, after, attempts := server.snapshot(); rev != revision ||
							string(after) != string(body) || len(attempts) != len(puts) {
							t.Error("verification failure changed remote data or attempted an upload")
						}
						if unlock, ok := fs.tryLockRemoteCommitPath(path); !ok {
							t.Fatal("failed verification leaked its path fence")
						} else {
							unlock()
						}
						fail.Store(false)
						if st := invoke(); st != gofuse.OK {
							t.Fatalf("retry with verification restored: %v", st)
						}
						if got := pr939HandleBytes(target); got != string(body) {
							t.Fatalf("retry adopted %q want complete landed image %q", got, body)
						}
						fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: targetID})
						waitCommitQueueIdle(t, fs.commitQueue)
						if _, ok := fs.pendingIndex.GetMeta(path); ok || fs.shadowStore.Has(path) {
							t.Error("successful retry left pending shadow staging")
						}
						if _, ok := fs.writeBack.GetMeta(path); ok {
							t.Error("successful retry left cached staging")
						}
						if conflicts, _, _ := fs.pendingIndex.ConflictSummary(); conflicts != 0 {
							t.Errorf("retry conflicts=%d", conflicts)
						}
					})
				}
			}
		}
	}
}

func TestIssue1023AppendStagingLocallyAdvancedProofIsRetryable(t *testing.T) {
	for _, operation := range []string{"flush", "interactive-fsync"} {
		t.Run(operation, func(t *testing.T) {
			const path = "/staging-advanced-proof.txt"
			fs, ino, server, _ := reviewR2FS(t, path, false)
			cache, err := NewWriteBackCache(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			fs.writeBack = cache
			target, targetID := pr939Handle(t, fs, ino, path, "base", false)
			_, sourceID := pr939Handle(t, fs, ino, path, "base", false)
			for _, write := range []struct {
				id   uint64
				data string
			}{{targetID, "A"}, {sourceID, "B"}, {sourceID, "C"}} {
				issue1023ShadowAppend(t, fs, ino, write.id, write.data)
			}
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: sourceID}); st != gofuse.OK {
				t.Fatal(st)
			}
			waitCommitQueueIdle(t, fs.commitQueue)
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: sourceID})
			waitCommitQueueIdle(t, fs.commitQueue)
			before := pr939HandleBytes(target)
			target.Lock()
			buffer, sequence, base := target.Dirty, target.DirtySeq, target.BaseRev
			gens := fs.captureHandleStagingGensLocked(target)
			target.Unlock()
			var advanced atomic.Bool
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet && advanced.CompareAndSwap(false, true) {
					// Model a successful local successor publication between the
					// real HEAD and GET. This tests consumer retry classification,
					// while concurrent producer coverage remains in the other tests.
					proof := fs.commitQueue.landedCommit(path)
					server.mu.Lock()
					server.revision++
					server.body = append(append([]byte(nil), server.body...), 'D')
					revision, data := server.revision, append([]byte(nil), server.body...)
					server.mu.Unlock()
					fs.recordCommittedRevisionWithSize(path, revision, int64(len(data)))
					fs.commitQueue.rememberLanded(path, revision, int64(len(data)), payloadChecksum(data),
						generateMountID(), snapshotAncestors(proof.snapshotID, proof.ancestors)...)
				}
				server.serveHTTP(w, r)
			}))
			t.Cleanup(proxy.Close)
			fs.client = newTestClient(proxy.URL)
			fs.commitQueue.client = fs.client
			if operation == "interactive-fsync" {
				fs.syncMode = SyncInteractive
			}
			invoke := func() gofuse.Status {
				if operation == "flush" {
					return fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: targetID})
				}
				return fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: targetID})
			}
			if st := invoke(); st != gofuse.EAGAIN {
				t.Errorf("locally advanced proof status=%v want=EAGAIN", st)
			}
			if !advanced.Load() {
				t.Fatal("verification GET did not advance the local proof")
			}
			target.Lock()
			preserved := target.Dirty == buffer && target.DirtySeq == sequence && target.BaseRev == base &&
				string(target.Dirty.Bytes()) == before && fs.captureHandleStagingGensLocked(target) == gens
			target.Unlock()
			if !preserved {
				t.Error("proof advancement changed the dirty ancestor")
			}
			if _, ok := fs.writeBack.GetMeta(path); ok {
				t.Error("proof advancement published a fallback")
			}
			if _, data, _ := server.snapshot(); string(data) != "baseABCD" {
				t.Fatalf("local successor lost: %q", data)
			}
			if st := invoke(); st != gofuse.OK {
				t.Fatalf("retry after local proof advancement=%v", st)
			}
			if got := pr939HandleBytes(target); got != "baseABCD" {
				t.Fatalf("retry adopted incomplete image %q", got)
			}
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: targetID})
			waitCommitQueueIdle(t, fs.commitQueue)
			if _, ok := fs.pendingIndex.GetMeta(path); ok || fs.shadowStore.Has(path) {
				t.Error("proof retry left pending staging")
			}
		})
	}
}

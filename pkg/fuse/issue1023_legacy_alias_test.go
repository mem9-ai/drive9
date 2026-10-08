package fuse

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// Use real Link/Open; unknownIdentity starts with the normal identity-less inode
// path, rather than deleting a ResourceID after a successful confirmed Link.
func r6LinkedHandlerFixture(t *testing.T, unknownIdentity bool, queuePutEntered, queuePutRelease chan struct{}) (*Dat9FS, uint64, *casFileServer, []uint64) {
	t.Helper()
	return r6LinkedHandlerFixtureWithSourceFlags(t, unknownIdentity, queuePutEntered, queuePutRelease, uint32(syscall.O_RDWR|syscall.O_APPEND))
}

func r6LinkedHandlerFixtureWithSourceFlags(t *testing.T, unknownIdentity bool, queuePutEntered, queuePutRelease chan struct{}, sourceFlags uint32) (*Dat9FS, uint64, *casFileServer, []uint64) {
	t.Helper()
	const original = "/hardlink-original"
	const alias = "/hardlink-alias"
	fs, ino, server, closeOld := reviewR2FS(t, original, false)
	cleanupKernelCacheBypassAfterTest(t, fs)
	closeOld()
	var linked, failConfirm atomic.Bool
	var confirms atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Query().Get("hardlink") == "1" {
			if r.URL.Path != "/v1/fs"+alias || r.Header.Get("X-Dat9-Hardlink-Source") != original {
				t.Error("unexpected Link request")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			linked.Store(true)
			failConfirm.Store(unknownIdentity)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
			return
		}
		if r.URL.Path == "/v1/fs/r6-queue-occupied" || r.URL.Path == "/v1/fs/r6-queue-pending" {
			w.Header().Set("X-Dat9-Revision", "2")
			if r.Method == http.MethodPut && r.URL.Path == "/v1/fs/r6-queue-occupied" {
				close(queuePutEntered)
				<-queuePutRelease
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "revision": 2})
			return
		}
		if r.URL.Path != "/v1/fs"+original && (r.URL.Path != "/v1/fs"+alias || !linked.Load()) {
			http.NotFound(w, r)
			return
		}
		if r.Method == http.MethodHead && r.URL.Path == "/v1/fs"+alias && failConfirm.Load() {
			confirms.Add(1)
			w.WriteHeader(statusClientClosedRequest)
			return
		}
		if !unknownIdentity {
			w.Header().Set("X-Dat9-Resource-ID", "shared-hardlink-resource")
		}
		w.Header().Set("X-Dat9-Mode", "420")
		if linked.Load() {
			w.Header().Set("X-Dat9-Nlink", "2")
		} else {
			w.Header().Set("X-Dat9-Nlink", "1")
		}
		clone := r.Clone(r.Context())
		urlCopy := *r.URL
		urlCopy.Path = "/v1/fs" + original
		clone.URL = &urlCopy
		server.serveHTTP(w, clone)
	}))
	t.Cleanup(ts.Close)
	fs.client = newTestClient(ts.URL)
	fs.client.SetSmallFileThresholdForTests(1 << 20)
	fs.commitQueue.client = fs.client
	if !unknownIdentity {
		fs.inodes.SetIdentity(ino, "shared-hardlink-resource", 1)
	}
	fs.inodes.UpdateMode(ino, 0o644)
	open := func(pid uint32, flags uint32) uint64 {
		var out gofuse.OpenOut
		if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino, Caller: gofuse.Caller{Pid: pid}}, Flags: flags}, &out); st != gofuse.OK {
			t.Fatalf("Open=%v", st)
		}
		return out.Fh
	}
	a := open(101, sourceFlags)
	if entry, _ := fs.inodes.GetEntry(ino); unknownIdentity && entry.ResourceID != "" {
		t.Fatal("unknown identity premise missing before Link")
	}
	var out gofuse.EntryOut
	if st := fs.Link(nil, &gofuse.LinkIn{InHeader: gofuse.InHeader{NodeId: 1}, Oldnodeid: ino}, "hardlink-alias", &out); st != gofuse.OK {
		t.Fatalf("committed Link=%v", st)
	}
	failConfirm.Store(false)
	if !linked.Load() || out.NodeId != ino || out.Nlink != 2 {
		t.Fatal("real Link did not establish the shared inode")
	}
	if unknownIdentity {
		entry, _ := fs.inodes.GetEntry(ino)
		if confirms.Load() == 0 || entry.ResourceID != "" || len(entry.Paths) != 2 {
			t.Fatal("failed advisory Stat/empty cached identity premise missing")
		}
	}
	b := open(202, uint32(syscall.O_RDWR|syscall.O_APPEND))
	ids := []uint64{a, b}
	first, _ := fs.fileHandles.Get(a)
	second, _ := fs.fileHandles.Get(b)
	if first.Path != original || second.Path != alias {
		t.Fatal("retained source and alias handles did not come from Link/Open")
	}
	t.Cleanup(func() {
		for _, id := range ids {
			if fh, ok := fs.fileHandles.Get(id); ok {
				fh.Lock()
				fs.releaseHandleRemoteCommitPathLocked(fh)
				fh.Unlock()
			}
		}
	})
	return fs, ino, server, ids
}

func TestR6LegacyAliasPendingUsesRealRelease(t *testing.T) {
	for _, variant := range []string{"no_cq_no_shadow", "cache_removed_before_callback"} {
		t.Run(variant, func(t *testing.T) {
			queueEntered, queueRelease := make(chan struct{}), make(chan struct{})
			releaseQueue := sync.OnceFunc(func() { close(queueRelease) })
			fs, ino, server, ids := r6LinkedHandlerFixtureWithSourceFlags(t, false, queueEntered, queueRelease, uint32(syscall.O_RDWR))
			cache, err := NewWriteBackCache(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			uploader := NewWriteBackUploader(fs.client, cache, 1)
			fs.SetWriteBack(cache, uploader)
			uploader.SnapshotStagingGens = fs.snapshotStagingGens
			gapEntered, gapRelease := make(chan struct{}), make(chan struct{})
			releaseGap := sync.OnceFunc(func() { close(gapRelease) })
			uploader.OnSuccess = func(meta WriteBackMeta, revision int64, gens StagingGens) {
				if variant == "cache_removed_before_callback" {
					close(gapEntered)
					<-gapRelease
				}
				fs.onWriteBackUploadSuccess(meta, revision, gens)
			}
			t.Cleanup(uploader.DrainAll)
			server.firstPutStarted = make(chan struct{})
			server.releaseFirstPut = make(chan struct{})
			releasePut := sync.OnceFunc(func() { close(server.releaseFirstPut) })
			t.Cleanup(func() { releasePut(); releaseGap(); releaseQueue() })
			fs.commitQueue.DrainAll()
			fs.commitQueue, fs.shadowStore, fs.pendingIndex = nil, nil, nil

			// A is a real non-append producer whose EOF pwrite remains on the
			// legacy uploader; B remains O_APPEND and retains the same assertions.
			if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0], Offset: 4}, []byte("A")); n != 1 || st != gofuse.OK {
				t.Fatalf("A Write=%d/%v", n, st)
			}
			if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]}); st != gofuse.OK {
				t.Fatalf("A Flush=%v", st)
			}
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]})
			if _, present := fs.fileHandles.Get(ids[0]); present || len(fs.openHandles.SnapshotInode(ino)) != 1 {
				t.Fatal("A did not leave the live handle registry after actual Release")
			}
			select {
			case <-server.firstPutStarted:
			case <-time.After(2 * time.Second):
				t.Fatal("actual Release did not submit a legacy uploader PUT")
			}
			if fs.commitQueue != nil && fs.commitQueue.HasPath("/hardlink-original") {
				t.Fatal("A unexpectedly entered CQ instead of actual backpressure fallback")
			}
			if variant == "cache_removed_before_callback" {
				releasePut()
				select {
				case <-gapEntered:
				case <-time.After(2 * time.Second):
					t.Fatal("legacy uploader did not reach its actual post-remove callback")
				}
				if _, present := cache.GetMeta("/hardlink-original"); present {
					t.Fatal("writeback meta was not removed before OnSuccess")
				}
			}
			b, _ := fs.fileHandles.Get(ids[1])
			b.Lock()
			pending := fs.appendCommitPending(b)
			b.Unlock()
			if !pending {
				t.Error("released alias legacy upload is absent from append admission")
			}
			type result struct {
				n  uint32
				st gofuse.Status
			}
			done := make(chan result, 1)
			go func() { n, st := pr939Append(fs, ino, ids[1], "B"); done <- result{n, st} }()
			releasePut()
			releaseGap()
			select {
			case got := <-done:
				if got.n != 1 || got.st != gofuse.OK {
					t.Errorf("B Write=%d/%v, want 1/OK after legacy completion", got.n, got.st)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("B append did not resume after legacy completion")
			}
			if got := pr939HandleBytes(b); got != "baseAB" {
				t.Errorf("acknowledged alias image=%q, want baseAB", got)
			}
			releaseQueue()
			if err := uploader.WaitIdle(t.Context()); err != nil {
				t.Fatal(err)
			}
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]}); st != gofuse.OK {
				t.Errorf("B Fsync=%v", st)
			}
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]})
			uploader.DrainAll()
			if fs.commitQueue != nil {
				waitCommitQueueIdle(t, fs.commitQueue)
			}
			if _, body, _ := server.snapshot(); string(body) != "baseAB" {
				t.Errorf("remote after drain=%q, want baseAB", body)
			}
		})
	}
}

func TestR6LinkAdvisoryStatFailureNeverAcknowledgesMissingRecord(t *testing.T) {
	fs, ino, server, ids := r6LinkedHandlerFixture(t, true, nil, nil)
	if n, st := pr939Append(fs, ino, ids[0], "A"); n != 1 || st != gofuse.OK {
		t.Fatalf("A Write=%d/%v", n, st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]}); st != gofuse.OK {
		t.Fatal(st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]})
	waitCommitQueueIdle(t, fs.commitQueue)
	if _, present := fs.fileHandles.Get(ids[0]); present {
		t.Fatal("A still registered")
	}
	if _, body, _ := server.snapshot(); string(body) != "baseA" {
		t.Fatalf("successful A commit=%q", body)
	}
	b, _ := fs.fileHandles.Get(ids[1])
	n, st := pr939Append(fs, ino, ids[1], "B")
	if n == 0 && st == gofuse.EAGAIN {
		// Explicit fail-closed is acceptable while the server supplies no
		// stable ResourceID. A's successful record must remain remotely intact.
		if _, body, _ := server.snapshot(); string(body) != "baseA" {
			t.Error("rejected B changed successful A record")
		}
		return
	}
	if n != 1 || st != gofuse.OK {
		t.Fatalf("B outcome=%d/%v, want full OK or explicit zero/EAGAIN", n, st)
	}
	if got := pr939HandleBytes(b); got != "baseAB" {
		t.Errorf("unsafe successful B image=%q, want baseAB", got)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]}); st != gofuse.OK {
		t.Error(st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]})
	fs.commitQueue.DrainAll()
	if _, body, _ := server.snapshot(); string(body) != "baseAB" {
		t.Errorf("successful records after drain=%q", body)
	}
}

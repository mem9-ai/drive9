package fuse

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// A Flush owns baseA. B has already ACKed baseAB and enters generic SDK PUT.
// A Release then enters its own expected-1 SDK PUT; B wins, A receives a real
// 409. Release has no returned durability status, so progress and empty drain
// are the independent assertions.
func TestR6NoCQReleaseLosingToGenericFsyncRetiresVerifiedAncestor(t *testing.T) {
	fs, ino, server, ids := r6LinkedHandlerFixture(t, false, nil, nil)
	fs.commitQueue.DrainAll()
	fs.commitQueue, fs.shadowStore, fs.pendingIndex = nil, nil, nil
	a, _ := fs.fileHandles.Get(ids[0])
	b, _ := fs.fileHandles.Get(ids[1])
	paths := []string{a.Path, b.Path}
	entered := []chan struct{}{make(chan struct{}), make(chan struct{})}
	gates := []chan struct{}{make(chan struct{}), make(chan struct{})}
	var enters, releases [2]sync.Once
	var attempts [2]atomic.Int32
	var conflicts atomic.Int32
	unblock := func(i int) { releases[i].Do(func() { close(gates[i]) }) }
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		which := -1
		for i, path := range paths {
			if r.URL.Path == "/v1/fs"+path {
				which = i
			}
		}
		if which < 0 {
			http.NotFound(w, r)
			return
		}
		if r.Method == http.MethodPut && attempts[which].Add(1) == 1 {
			if got := r.Header.Get("X-Dat9-Expected-Revision"); got != "1" {
				t.Errorf("first SDK PUT path=%s expected=%q, want1", r.URL.Path, got)
			}
			enters[which].Do(func() { close(entered[which]) })
			<-gates[which]
		}
		clone := r.Clone(r.Context())
		urlCopy := *r.URL
		urlCopy.Path = "/v1/fs" + server.path
		clone.URL = &urlCopy
		recorder := httptest.NewRecorder()
		recorder.Header().Set("X-Dat9-Resource-ID", "shared-hardlink-resource")
		recorder.Header().Set("X-Dat9-Nlink", "2")
		server.serveHTTP(recorder, clone)
		if recorder.Code == http.StatusConflict {
			conflicts.Add(1)
		}
		for key, values := range recorder.Header() {
			w.Header()[key] = append([]string(nil), values...)
		}
		w.WriteHeader(recorder.Code)
		_, _ = w.Write(recorder.Body.Bytes())
	}))
	t.Cleanup(ts.Close)
	fs.client = newTestClient(ts.URL)
	fs.client.SetSmallFileThresholdForTests(1 << 20)
	cache, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	uploader := NewWriteBackUploader(fs.client, cache, 1)
	fs.SetWriteBack(cache, uploader)
	uploader.OnSuccess = fs.onWriteBackUploadSuccess
	uploader.SnapshotStagingGens = fs.snapshotStagingGens
	t.Cleanup(uploader.DrainAll)
	if n, st := pr939Append(fs, ino, ids[0], "A"); n != 1 || st != gofuse.OK {
		t.Fatal(n, st)
	}
	if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]}); st != gofuse.OK {
		t.Fatal(st)
	}
	if meta, ok := cache.GetMeta(a.Path); !ok || meta.SnapshotID == "" || !meta.lineageTrusted {
		t.Fatal("actual trusted owned A WB snapshot missing")
	}
	if n, st := pr939Append(fs, ino, ids[1], "B"); n != 1 || st != gofuse.OK {
		t.Fatal(n, st)
	}
	if got := pr939HandleBytes(b); got != "baseAB" {
		t.Fatalf("acknowledged B=%q", got)
	}
	bDone, bResult := make(chan struct{}), make(chan gofuse.Status, 1)
	aDone := make(chan struct{})
	var aStarted atomic.Bool
	t.Cleanup(func() {
		unblock(0)
		unblock(1)
		select {
		case <-bDone:
		case <-time.After(3 * time.Second):
			t.Error("B Fsync did not exit")
		}
		if aStarted.Load() {
			select {
			case <-aDone:
			case <-time.After(3 * time.Second):
				t.Error("A Release did not exit")
			}
		}
	})
	go func() {
		defer close(bDone)
		bResult <- fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]})
	}()
	select {
	case <-entered[1]:
	case <-time.After(3 * time.Second):
		t.Fatal("B generic SDK did not enter before A Release")
	}
	aStarted.Store(true)
	go func() {
		defer close(aDone)
		fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]})
	}()
	select {
	case <-entered[0]:
	case <-time.After(3 * time.Second):
		t.Fatal("A actual Release did not reach expected-1 SDK PUT")
	}
	unblock(1)
	select {
	case st := <-bResult:
		if st != gofuse.OK {
			t.Fatalf("winning B Fsync=%v", st)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("B did not publish before A CAS")
	}
	if revision, bytes, _ := server.snapshot(); revision != 2 || string(bytes) != "baseAB" {
		t.Fatalf("B actual winner=%d/%q", revision, bytes)
	}
	unblock(0)
	select {
	case <-aDone:
	case <-time.After(3 * time.Second):
		t.Fatal("A Release did not finish after SDK409")
	}
	if err := uploader.WaitIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
	if conflicts.Load() == 0 {
		t.Fatal("A expected-1 SDK PUT did not encounter actual409")
	}
	if _, present := fs.fileHandles.Get(ids[0]); present {
		t.Error("A Release did not remove its actual handle")
	}
	if meta, present := cache.GetMeta(a.Path); present {
		t.Errorf("losing already-landed ancestor remains cached: %+v", meta)
	}
	if _, bytes, _ := server.snapshot(); string(bytes) != "baseAB" {
		t.Errorf("ancestor overwrote acknowledged B: %q", bytes)
	}
	n, st := pr939Append(fs, ino, ids[1], "C")
	if n != 1 || st != gofuse.OK {
		t.Errorf("next independent append did not progress after Release/drain: %d/%v", n, st)
	} else {
		if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]}); st != gofuse.OK {
			t.Errorf("C Fsync=%v", st)
		}
		if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]}); st != gofuse.OK {
			t.Errorf("B close Flush=%v", st)
		}
		fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]})
	}
	uploader.DrainAll()
	if _, bytes, _ := server.snapshot(); string(bytes) != "baseABC" {
		t.Errorf("all acknowledged/progress records after drain=%q, want baseABC", bytes)
	}
	for _, path := range paths {
		if _, present := cache.GetMeta(path); present || fs.hasPendingLocalState(path) {
			t.Errorf("WB/shadow pending after drain: %s", path)
		}
	}
}

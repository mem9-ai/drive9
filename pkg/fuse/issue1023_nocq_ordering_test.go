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

// A has participated in the append image copied by B. Its trusted Release now
// keeps the dirty handle through synchronous flush. Gate the real A Release
// and subsequent B Fsync SDK requests, land A first, then require B's actual409
// retry to preserve B and the next C record. No raw-uploader hook is expected.
func TestR6NoCQOrderedLegacyParentPreservesAcknowledgedDescendant(t *testing.T) {
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
	if n, st := pr939Append(fs, ino, ids[1], "B"); n != 1 || st != gofuse.OK {
		t.Fatal(n, st)
	}
	if got := pr939HandleBytes(b); got != "baseAB" {
		t.Fatalf("acknowledged B=%q", got)
	}
	a.Lock()
	participated := a.appendSnapshot && a.StagedSnapshotID != ""
	a.Unlock()
	if !participated {
		t.Fatal("actual copied A image was not marked as append participant")
	}
	aDone, bDone := make(chan struct{}), make(chan struct{})
	bResult := make(chan gofuse.Status, 1)
	var bStarted atomic.Bool
	t.Cleanup(func() {
		unblock(0)
		unblock(1)
		select {
		case <-aDone:
		case <-time.After(3 * time.Second):
			t.Error("A Release did not exit")
		}
		if bStarted.Load() {
			select {
			case <-bDone:
			case <-time.After(3 * time.Second):
				t.Error("B Fsync did not exit")
			}
		}
	})
	go func() {
		defer close(aDone)
		fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]})
	}()
	select {
	case <-entered[0]:
	case <-time.After(3 * time.Second):
		t.Fatal("A Release did not enter actual SDK")
	}
	bStarted.Store(true)
	go func() {
		defer close(bDone)
		bResult <- fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]})
	}()
	select {
	case <-entered[1]:
	case <-time.After(3 * time.Second):
		t.Fatal("B subsequent Fsync did not enter its actual SDK request")
	}
	unblock(0)
	select {
	case <-aDone:
	case <-time.After(3 * time.Second):
		t.Fatal("A Release did not complete after its successful SDK")
	}
	if err := uploader.WaitIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
	if revision, body, _ := server.snapshot(); revision != 2 || string(body) != "baseA" {
		t.Fatalf("actual released parent=%d/%q, want2/baseA", revision, body)
	}
	unblock(1)
	select {
	case st := <-bResult:
		if st != gofuse.OK {
			t.Errorf("already-ACKed B Fsync after SDK409=%v, wantOK", st)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("B Fsync did not finish its verified parent retry")
	}
	if conflicts.Load() == 0 {
		t.Fatal("B's initial expected1 SDK did not receive actual409")
	}
	if _, body, _ := server.snapshot(); string(body) != "baseAB" {
		t.Errorf("B record after Fsync=%q", body)
	}
	if _, present := fs.fileHandles.Get(ids[0]); present {
		t.Error("A Release retained its actual handle")
	}
	n, st := pr939Append(fs, ino, ids[1], "C")
	if n != 1 || st != gofuse.OK {
		t.Errorf("C append did not progress: %d/%v", n, st)
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
	if _, body, _ := server.snapshot(); string(body) != "baseABC" {
		t.Errorf("all records after drain=%q, wantbaseABC", body)
	}
	for _, path := range paths {
		if _, present := cache.GetMeta(path); present || fs.hasPendingLocalState(path) {
			t.Errorf("WB/shadow pending after successful durability/drain: %s", path)
		}
	}
}

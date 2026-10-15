package fuse

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// Both immutable first PUTs reach the SDK/server boundary at revision 1.
// The second request really receives 409, after the first Fsync has published
// its successful tuple. Both durability calls must still succeed causally.
func TestR6NoCQConcurrentLinkedFsyncCausalConflict(t *testing.T) {
	for _, first := range []int{0, 1} {
		t.Run(fmt.Sprintf("first=%d", first), func(t *testing.T) {
			fs, ino, server, ids := r6LinkedHandlerFixture(t, false, nil, nil)
			fs.commitQueue.DrainAll()
			fs.commitQueue, fs.shadowStore, fs.pendingIndex = nil, nil, nil
			handles := make([]*FileHandle, 2)
			for i, id := range ids {
				handles[i], _ = fs.fileHandles.Get(id)
			}
			entered := []chan struct{}{make(chan struct{}), make(chan struct{})}
			gates := []chan struct{}{make(chan struct{}), make(chan struct{})}
			var enters, releases [2]sync.Once
			var attempts [2]atomic.Int32
			var conflicts atomic.Int32
			unblock := func(i int) { releases[i].Do(func() { close(gates[i]) }) }
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				which := -1
				for i, handle := range handles {
					if r.URL.Path == "/v1/fs"+handle.Path {
						which = i
					}
				}
				if which < 0 {
					http.NotFound(w, r)
					return
				}
				if r.Method == http.MethodPut && attempts[which].Add(1) == 1 {
					if got := r.Header.Get("X-Dat9-Expected-Revision"); got != "1" {
						t.Errorf("first SDK PUT path=%s expected revision=%q, want 1", r.URL.Path, got)
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
			if n, status := pr939Append(fs, ino, ids[0], "A"); n != 1 || status != gofuse.OK {
				t.Fatalf("A Write=%d/%v", n, status)
			}
			if n, status := pr939Append(fs, ino, ids[1], "B"); n != 1 || status != gofuse.OK {
				t.Fatalf("B copied/acknowledged Write=%d/%v", n, status)
			}
			if got := pr939HandleBytes(handles[1]); got != "baseAB" {
				t.Fatalf("B's acknowledged descendant=%q", got)
			}
			results := []chan gofuse.Status{make(chan gofuse.Status, 1), make(chan gofuse.Status, 1)}
			stopped := []chan struct{}{make(chan struct{}), make(chan struct{})}
			t.Cleanup(func() {
				unblock(0)
				unblock(1)
				for _, done := range stopped {
					select {
					case <-done:
					case <-time.After(3 * time.Second):
						t.Error("concurrent Fsync did not exit before cleanup")
					}
				}
			})
			for i, id := range ids {
				go func() {
					defer close(stopped[i])
					results[i] <- fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id})
				}()
			}
			for _, ready := range entered {
				select {
				case <-ready:
				case <-time.After(3 * time.Second):
					t.Fatal("both initial SDK PUTs did not reach the channel-controlled boundary")
				}
			}
			unblock(first)
			select {
			case status := <-results[first]:
				if status != gofuse.OK {
					t.Errorf("first durability call=%v, want OK", status)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("first durability call did not publish before second CAS")
			}
			wantFirst := "baseA"
			if first == 1 {
				wantFirst = "baseAB"
			}
			if revision, body, _ := server.snapshot(); revision != 2 || string(body) != wantFirst {
				t.Fatalf("first landed revision/body=%d/%q, want 2/%q", revision, body, wantFirst)
			}
			second := 1 - first
			unblock(second)
			select {
			case status := <-results[second]:
				if status != gofuse.OK {
					t.Errorf("second durability call after actual CAS conflict=%v, want OK", status)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("second durability call did not finish after CAS conflict")
			}
			if conflicts.Load() == 0 {
				t.Fatal("the second immutable revision-1 SDK PUT did not encounter actual HTTP 409")
			}
			for i, id := range ids {
				handles[i].Lock()
				clean := handles[i].DirtySeq == 0 && !handles[i].Dirty.HasDirtyParts()
				handles[i].Unlock()
				if !clean {
					t.Errorf("successful Fsync fh=%d left acknowledged dirty staging", id)
				}
				if status := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id}); status != gofuse.OK {
					t.Errorf("close Flush fh=%d status=%v", id, status)
				}
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id})
			}
			if _, body, _ := server.snapshot(); string(body) != "baseAB" {
				t.Errorf("remote acknowledged records after both Fsync/close=%q, want baseAB", body)
			}
			for _, handle := range handles {
				if fs.hasPendingLocalState(handle.Path) {
					t.Error("content staging remains after successful no-CQ durability/close")
				}
			}
			if fs.uploader != nil {
				fs.uploader.DrainAll()
			}
		})
	}
}

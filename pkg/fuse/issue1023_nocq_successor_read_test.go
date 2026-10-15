package fuse

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// The GET boundary advances this same mount's real verified descendant after
// the consumer HEAD observed its parent. Both acknowledged records are in the
// successor; the waiting peer's Fsync must retry/consume it, not return EIO.
func TestR6NoCQLegacyRebaseRetriesOwnPublishedSuccessorRead(t *testing.T) {
	fs, ino, server, ids := r6LinkedHandlerFixture(t, false, nil, nil)
	fs.commitQueue.DrainAll()
	fs.commitQueue, fs.shadowStore, fs.pendingIndex = nil, nil, nil
	a, _ := fs.fileHandles.Get(ids[0])
	b, _ := fs.fileHandles.Get(ids[1])
	if n, status := pr939Append(fs, ino, ids[0], "A"); n != 1 || status != gofuse.OK {
		t.Fatal(n, status)
	}
	if n, status := pr939Append(fs, ino, ids[1], "B"); n != 1 || status != gofuse.OK {
		t.Fatal(n, status)
	}
	if status := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]}); status != gofuse.OK {
		t.Fatal(status)
	}
	parent := fs.landedAppendCommit(b.Path)
	if revision, body, _ := server.snapshot(); revision != 2 || string(body) != "baseA" || parent.rev != 2 || parent.snapshotID == "" {
		t.Fatal("actual parent revision 2/proof was not prepared")
	}
	// A refreshes from B's acknowledged live image before adding C. Its
	// eventual snapshot therefore includes B's prepared identity and parent.
	if n, status := pr939Append(fs, ino, ids[0], "C"); n != 1 || status != gofuse.OK {
		t.Fatal(n, status)
	}
	if got := pr939HandleBytes(a); got != "baseABC" {
		t.Fatalf("actual successor preparation=%q, want baseABC", got)
	}
	var armed, advanced atomic.Bool
	var nestedStatus atomic.Uint32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/fs"+a.Path && r.URL.Path != "/v1/fs"+b.Path {
			http.NotFound(w, r)
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/v1/fs"+b.Path && armed.Load() && advanced.CompareAndSwap(false, true) {
			// This actual Fsync uses A's different path fence. B holds its
			// own mutex; publication TryLock must skip it and leave the
			// proof advance for the consumer to detect after this GET.
			status := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]})
			nestedStatus.Store(uint32(status))
			if status != gofuse.OK {
				t.Errorf("actual successor Fsync in GET=%v", status)
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
		}
		w.Header().Set("X-Dat9-Resource-ID", "shared-hardlink-resource")
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
	armed.Store(true)
	status := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]})
	armed.Store(false)
	if !advanced.Load() || gofuse.Status(nestedStatus.Load()) != gofuse.OK {
		t.Fatal("actual HEAD2/GET3 owned-successor window was not exercised")
	}
	if revision, body, _ := server.snapshot(); revision != 3 || string(body) != "baseABC" {
		t.Fatalf("actual successor revision/body=%d/%q, want 3/baseABC", revision, body)
	}
	if status != gofuse.OK {
		t.Errorf("B Fsync after locally proven successor advanced=%v, want OK", status)
	}
	if n, status := pr939Append(fs, ino, ids[1], "D"); n != 1 || status != gofuse.OK {
		t.Errorf("B subsequent Write=%d/%v, want full OK", n, status)
	} else if status := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]}); status != gofuse.OK {
		t.Errorf("B subsequent Fsync=%v", status)
	}
	if _, body, _ := server.snapshot(); string(body) != "baseABCD" {
		t.Errorf("complete acknowledged records=%q, want baseABCD", body)
	}
}

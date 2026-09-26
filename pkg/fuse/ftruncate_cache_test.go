package fuse

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func newCommittedFtruncateCacheFS(t *testing.T) (*Dat9FS, uint64, uint64, *atomic.Int64) {
	t.Helper()
	remote := []byte("hello world")
	revision := int64(1)
	var mu sync.Mutex
	reads := new(atomic.Int64)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", strconv.Itoa(len(remote)))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(revision, 10))
		case http.MethodGet:
			reads.Add(1)
			_, _ = w.Write(remote)
		case http.MethodPut:
			data, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			remote, revision = data, revision+1
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(revision, 10))
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"status":"ok","revision":`+strconv.FormatInt(revision, 10)+`}`)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	opts := &MountOptions{SyncMode: SyncStrict, WritePolicy: WritePolicyWriteBack, TrustLocalEvents: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(server.URL), opts)
	fs.client.SetSmallFileThresholdForTests(1 << 20)
	ino := fs.inodes.Lookup("/file.bin", false, int64(len(remote)), time.Now())
	fs.inodes.UpdateRevision(ino, 1)
	a := addFtruncateTestHandle(t, fs, ino, "/file.bin", remote)
	c := addFtruncateTestHandle(t, fs, ino, "/file.bin", nil)
	reviewFtruncate(t, fs, ino, a, 5)
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a}); st != gofuse.OK {
		t.Fatalf("commit truncate = %v", st)
	}
	if !fs.openHandles.HasVisibleTruncate(ino) {
		t.Fatal("committed truncate must retain its marker while A is open")
	}
	if cached, ok := fs.readCache.Get("/file.bin", 2); !ok || string(cached) != "hello" {
		t.Fatalf("committed cache = %q/%t, want hello", cached, ok)
	}
	reads.Store(0)
	return fs, ino, c, reads
}

func TestReadWriteBackFtruncateCommittedCache(t *testing.T) {
	for _, tc := range []struct {
		name   string
		offset int64
		want   string
	}{
		{name: "full-short-read", want: "hello"},
		{name: "suffix", offset: 2, want: "llo"},
		{name: "eof", offset: 5},
		{name: "past-eof", offset: 9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, ino, reader, reads := newCommittedFtruncateCacheFS(t)
			for i := 0; i < 20; i++ {
				got, st, err := readDat9FSTestRange(fs, ino, reader, tc.offset, 64)
				if err != nil || st != gofuse.OK || !bytes.Equal(got, []byte(tc.want)) {
					t.Fatalf("read = %q/%v/%v, want %q/OK", got, st, err, tc.want)
				}
			}
			if got := reads.Load(); got != 0 {
				t.Fatalf("remote GETs = %d, want 0", got)
			}
		})
	}
}

func TestReadWriteBackFtruncateCommittedCacheRejectsUnprovenState(t *testing.T) {
	for _, name := range []string{"stale-revision", "wrong-size", "missing-cache", "unknown-revision", "observed-new-revision", "unverified", "newer-dirty", "pending-index"} {
		t.Run(name, func(t *testing.T) {
			fs, ino, reader, reads := newCommittedFtruncateCacheFS(t)
			wantStatus, wantGETs := gofuse.OK, int64(1)
			switch name {
			case "stale-revision":
				fs.readCache.Put("/file.bin", []byte("OLD!!"), 1)
			case "wrong-size":
				fs.readCache.Put("/file.bin", []byte("hello world"), 2)
			case "missing-cache":
				fs.readCache.Invalidate("/file.bin")
			case "unknown-revision":
				fs.recordCommittedMutation(ino, fs.ftruncateCommittedSeq(ino), 0, 5)
			case "observed-new-revision":
				fs.inodes.UpdateRevision(ino, 3)
			case "unverified":
				fs.markStatCacheUnverified()
			case "newer-dirty":
				fs.markDirtySize(ino, 5)
				wantStatus, wantGETs = gofuse.Status(syscall.EAGAIN), 0
			case "pending-index":
				pending, err := NewPendingIndex(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				fs.pendingIndex = pending
				if _, err := pending.PutWithBaseRev("/file.bin", 5, PendingOverwrite, 2); err != nil {
					t.Fatal(err)
				}
				wantStatus, wantGETs = gofuse.Status(syscall.EAGAIN), 0
			}
			got, st, err := readDat9FSTestRange(fs, ino, reader, 0, 64)
			if err != nil || st != wantStatus || st == gofuse.OK && string(got) != "hello" {
				t.Fatalf("read = %q/%v/%v, want status %v", got, st, err, wantStatus)
			}
			if got := reads.Load(); got != wantGETs {
				t.Fatalf("remote GETs = %d, want %d", got, wantGETs)
			}
		})
	}
}

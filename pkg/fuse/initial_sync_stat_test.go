package fuse

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

type initialSyncStatFixture struct {
	t          *testing.T
	fs         *Dat9FS
	watcher    *SSEWatcher
	headCalls  atomic.Int32
	beforeHead func(int32, http.ResponseWriter, *http.Request) bool
}

func newInitialSyncStatFixture(t *testing.T) *initialSyncStatFixture {
	t.Helper()
	fixture := &initialSyncStatFixture{t: t}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead || r.URL.Path != "/v1/fs/file.txt" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		call := fixture.headCalls.Add(1)
		if fixture.beforeHead != nil && fixture.beforeHead(call, w, r) {
			return
		}
		writeInitialSyncStatResponse(w, call)
	}))
	opts := &MountOptions{LookupRetryCount: -1}
	opts.setDefaults()
	fixture.fs = NewDat9FS(newTestClient(server.URL), opts)
	fixture.watcher = &SSEWatcher{fs: fixture.fs}
	t.Cleanup(func() {
		fixture.fs.directoryPrefetch.shutdown()
		server.Close()
	})
	return fixture
}

func writeInitialSyncStatResponse(w http.ResponseWriter, call int32) {
	size := int64(11)
	revision := int64(11)
	mtime := int64(11)
	mode := uint32(0o640)
	resourceID := "old-resource"
	if call > 1 {
		size = 22
		revision = 22
		mtime = 22
		mode = 0o600
		resourceID = "current-resource"
	}
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.Header().Set("X-Dat9-IsDir", "false")
	w.Header().Set("X-Dat9-Revision", strconv.FormatInt(revision, 10))
	w.Header().Set("X-Dat9-Mtime", strconv.FormatInt(mtime, 10))
	w.Header().Set("X-Dat9-Mode", strconv.FormatUint(uint64(mode), 10))
	w.Header().Set("X-Dat9-Resource-ID", resourceID)
	w.Header().Set("X-Dat9-Nlink", "1")
	w.WriteHeader(http.StatusOK)
}

func (f *initialSyncStatFixture) initialReset() {
	f.watcher.handleEvent(nil, &client.ResetEvent{Reason: "initial_sync"})
}

func (f *initialSyncStatFixture) seedFile() uint64 {
	f.t.Helper()
	return f.fs.inodes.LookupWithIdentity("/file.txt", "seed-resource", 1, false, 1, time.Unix(1, 0))
}

func (f *initialSyncStatFixture) run(operation string, cancel <-chan struct{}) (gofuse.Status, uint64, gofuse.Attr) {
	f.t.Helper()
	switch operation {
	case "Lookup":
		var out gofuse.EntryOut
		status := f.fs.Lookup(cancel, &gofuse.InHeader{NodeId: 1}, "file.txt", &out)
		return status, out.NodeId, out.Attr
	case "GetAttr":
		ino := f.seedFile()
		var out gofuse.AttrOut
		status := f.fs.GetAttr(cancel, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out)
		return status, ino, out.Attr
	default:
		f.t.Fatalf("unknown operation %q", operation)
		return gofuse.EIO, 0, gofuse.Attr{}
	}
}

func assertInitialSyncStatCurrent(t *testing.T, fs *Dat9FS, ino uint64, attr gofuse.Attr) {
	t.Helper()
	if attr.Size != 22 || attr.Mtime != 22 || attr.Mode&0o777 != 0o600 {
		t.Fatalf("FUSE attrs = %+v, want only current-generation stat", attr)
	}
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		t.Fatalf("inode %d was not published", ino)
	}
	if entry.Size != 22 || entry.Revision != 22 || entry.ResourceID != "current-resource" ||
		!entry.HasMode || entry.Mode != 0o600 || !entry.Mtime.Equal(time.Unix(22, 0)) {
		t.Fatalf("inode metadata = %+v, want only current-generation stat", entry)
	}
	if entry.Nlookup != 1 {
		t.Fatalf("inode lookup references = %d, want 1", entry.Nlookup)
	}
	if snapshots := fs.inodes.Snapshot(); len(snapshots) != 2 {
		t.Fatalf("inode snapshot count = %d, want root plus one file", len(snapshots))
	}
}

func TestInitialSyncStatRetriesBeforePublishingMetadata(t *testing.T) {
	for _, operation := range []string{"Lookup", "GetAttr"} {
		t.Run(operation, func(t *testing.T) {
			fixture := newInitialSyncStatFixture(t)
			originalGeneration := fixture.fs.mountViewGeneration.Load()
			fixture.beforeHead = func(call int32, _ http.ResponseWriter, _ *http.Request) bool {
				if call == 1 {
					fixture.initialReset()
				}
				return false
			}

			status, ino, attr := fixture.run(operation, nil)
			if status != gofuse.OK {
				t.Fatalf("%s status = %v, want OK", operation, status)
			}
			if got := fixture.headCalls.Load(); got != 2 {
				t.Fatalf("HEAD calls = %d, want exactly two", got)
			}
			if got := fixture.fs.initialSyncResetGeneration.Load(); got != originalGeneration+1 || got != fixture.fs.mountViewGeneration.Load() {
				t.Fatalf("tagged generation = %d, original = %d", got, originalGeneration)
			}
			assertInitialSyncStatCurrent(t, fixture.fs, ino, attr)
		})
	}
}

func TestInitialSyncStatExcludedViewChanges(t *testing.T) {
	for _, operation := range []string{"Lookup", "GetAttr"} {
		for _, reset := range []string{"runtime", "multiple initial", "during retry"} {
			t.Run(operation+"/"+reset, func(t *testing.T) {
				fixture := newInitialSyncStatFixture(t)
				fixture.beforeHead = func(call int32, _ http.ResponseWriter, _ *http.Request) bool {
					switch reset {
					case "runtime":
						fixture.fs.resetMountView()
					case "multiple initial":
						fixture.initialReset()
						fixture.initialReset()
					case "during retry":
						fixture.initialReset()
					}
					return false
				}

				status, ino, attr := fixture.run(operation, nil)
				if status != gofuse.Status(syscall.EAGAIN) {
					t.Fatalf("%s status = %v, want EAGAIN", operation, status)
				}
				wantCalls := int32(1)
				if reset == "during retry" {
					wantCalls = 2
				}
				if got := fixture.headCalls.Load(); got != wantCalls {
					t.Fatalf("HEAD calls = %d, want %d", got, wantCalls)
				}
				if attr != (gofuse.Attr{}) {
					t.Fatalf("%s published attrs on excluded reset: %+v", operation, attr)
				}
				if operation == "Lookup" {
					if ino != 0 {
						t.Fatalf("Lookup published inode %d on excluded reset", ino)
					}
					if _, ok := fixture.fs.inodes.GetInode("/file.txt"); ok {
						t.Fatal("Lookup registered an inode on excluded reset")
					}
					return
				}
				entry, ok := fixture.fs.inodes.GetEntry(ino)
				if !ok || entry.Size != 1 || entry.Revision != 0 || entry.ResourceID != "seed-resource" || entry.Nlookup != 1 {
					t.Fatalf("GetAttr changed inode on excluded reset: %+v, found=%t", entry, ok)
				}
			})
		}
	}
}

func TestInitialSyncStatDoesNotRetryRemoteErrors(t *testing.T) {
	for _, operation := range []string{"Lookup", "GetAttr"} {
		for _, statusCode := range []int{http.StatusUnauthorized, http.StatusServiceUnavailable} {
			t.Run(operation+"/"+http.StatusText(statusCode), func(t *testing.T) {
				fixture := newInitialSyncStatFixture(t)
				fixture.beforeHead = func(_ int32, w http.ResponseWriter, _ *http.Request) bool {
					fixture.initialReset()
					http.Error(w, http.StatusText(statusCode), statusCode)
					return true
				}

				status, _, _ := fixture.run(operation, nil)
				want := gofuse.EACCES
				if statusCode == http.StatusServiceUnavailable {
					want = gofuse.Status(syscall.EAGAIN)
				}
				if status != want || fixture.headCalls.Load() != 1 {
					t.Fatalf("status = %v, HEAD calls = %d; want %v without generation reload", status, fixture.headCalls.Load(), want)
				}
			})
		}
	}
}

func TestInitialSyncStatDoesNotRetryCancellation(t *testing.T) {
	for _, operation := range []string{"Lookup", "GetAttr"} {
		t.Run(operation, func(t *testing.T) {
			fixture := newInitialSyncStatFixture(t)
			started := make(chan struct{})
			fixture.beforeHead = func(_ int32, _ http.ResponseWriter, r *http.Request) bool {
				fixture.initialReset()
				close(started)
				<-r.Context().Done()
				return true
			}
			cancel := make(chan struct{})
			done := make(chan gofuse.Status, 1)
			go func() {
				status, _, _ := fixture.run(operation, cancel)
				done <- status
			}()
			select {
			case <-started:
				close(cancel)
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for HEAD request")
			}
			select {
			case status := <-done:
				if status != gofuse.Status(syscall.EAGAIN) || fixture.headCalls.Load() != 1 {
					t.Fatalf("status = %v, HEAD calls = %d; want EAGAIN without generation reload", status, fixture.headCalls.Load())
				}
			case <-time.After(2 * time.Second):
				t.Fatal("stat cancellation did not finish")
			}
		})
	}
}

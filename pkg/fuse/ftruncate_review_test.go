package fuse

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func reviewFtruncate(t *testing.T, fs *Dat9FS, ino, fh, size uint64) {
	t.Helper()
	if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
		InHeader: gofuse.InHeader{NodeId: ino},
		Valid:    gofuse.FATTR_FH | gofuse.FATTR_SIZE,
		Fh:       fh,
		Size:     size,
	}}, &gofuse.AttrOut{}); st != gofuse.OK {
		t.Fatalf("ftruncate to %d = %v", size, st)
	}
}

func TestReadWriteBackFtruncateSurvivesNewerWriteRollback(t *testing.T) {
	remote := []byte("hello world")
	fs, ino, remoteReads := newFtruncateVisibilityFS(t, remote)
	a := addFtruncateTestHandle(t, fs, ino, "/file.bin", remote)
	b := addFtruncateTestHandle(t, fs, ino, "/file.bin", remote)
	c := addFtruncateTestHandle(t, fs, ino, "/file.bin", nil)
	reviewFtruncate(t, fs, ino, a, 5)
	bHandle, _ := fs.fileHandles.Get(b)
	bHandle.WritePolicy = WritePolicyWriteSync
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("HELLO WORLD")); st == gofuse.OK {
		t.Fatal("write-sync unexpectedly succeeded against rejecting server")
	}
	got, st, err := readDat9FSTestRange(fs, ino, c, 0, len(remote))
	if err != nil || st != gofuse.OK || string(got) != "hello" {
		t.Fatalf("read after rollback = %q/%v/%v, want hello/OK", got, st, err)
	}
	var out gofuse.AttrOut
	if st := fs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &out); st != gofuse.OK || out.Size != 5 {
		t.Fatalf("size after rollback = %d/%v, want 5/OK", out.Size, st)
	}
	if remoteReads.Load() != 0 {
		t.Fatalf("old remote GETs = %d, want 0", remoteReads.Load())
	}
}

func TestReadWriteBackFtruncateSurvivesStackedRollback(t *testing.T) {
	remote := []byte("hello world")
	fs, ino, remoteReads := newFtruncateVisibilityFS(t, remote)
	a := addFtruncateTestHandle(t, fs, ino, "/file.bin", remote)
	bID := addFtruncateTestHandle(t, fs, ino, "/file.bin", remote)
	cID := addFtruncateTestHandle(t, fs, ino, "/file.bin", remote)
	reader := addFtruncateTestHandle(t, fs, ino, "/file.bin", nil)
	reviewFtruncate(t, fs, ino, a, 5)
	b, _ := fs.fileHandles.Get(bID)
	c, _ := fs.fileHandles.Get(cID)
	b.Lock()
	bSnapshot := b.Dirty.snapshot()
	b.DirtySeq = fs.markDirtySize(ino, b.Dirty.Size())
	b.Unlock()
	c.Lock()
	cSnapshot := c.Dirty.snapshot()
	c.DirtySeq = fs.markDirtySize(ino, c.Dirty.Size())
	c.Unlock()
	b.Lock()
	fs.restoreFailedWriteSyncLocked(b, bSnapshot, 0, nil)
	b.Unlock()
	c.Lock()
	fs.restoreFailedWriteSyncLocked(c, cSnapshot, 0, nil)
	c.Unlock()
	got, st, err := readDat9FSTestRange(fs, ino, reader, 0, len(remote))
	if err != nil || st != gofuse.OK || string(got) != "hello" {
		t.Fatalf("read after stacked rollback = %q/%v/%v, want hello/OK", got, st, err)
	}
	if remoteReads.Load() != 0 {
		t.Fatalf("old remote GETs = %d, want 0", remoteReads.Load())
	}
}

func TestReadWriteBackFtruncateNewerWriterOwnRange(t *testing.T) {
	for _, tc := range []struct {
		name string
		data []byte
		read int
		want string
	}{
		{name: "full", data: []byte("HELLO WORLD"), read: 11, want: "HELLO WORLD"},
		{name: "partial", data: []byte("X"), read: 1, want: "X"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			remote := []byte("hello world")
			fs, ino, remoteReads := newFtruncateVisibilityFS(t, remote)
			a := addFtruncateTestHandle(t, fs, ino, "/file.bin", remote)
			b := addFtruncateTestHandle(t, fs, ino, "/file.bin", remote)
			c := addFtruncateTestHandle(t, fs, ino, "/file.bin", nil)
			reviewFtruncate(t, fs, ino, a, 5)
			if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, tc.data); st != gofuse.OK || n != uint32(len(tc.data)) {
				t.Fatalf("newer write = %d/%v", n, st)
			}
			got, st, err := readDat9FSTestRange(fs, ino, b, 0, tc.read)
			if err != nil || st != gofuse.OK || string(got) != tc.want {
				t.Fatalf("own read = %q/%v/%v, want %q/OK", got, st, err, tc.want)
			}
			if _, st, err := readDat9FSTestRange(fs, ino, c, 0, len(remote)); err != nil || st != gofuse.Status(syscall.EAGAIN) {
				t.Fatalf("other reader = %v/%v, want EAGAIN", st, err)
			}
			if tc.name == "partial" {
				if _, st, err := readDat9FSTestRange(fs, ino, b, 0, len(remote)); err != nil || st != gofuse.Status(syscall.EAGAIN) {
					t.Fatalf("own unproven tail = %v/%v, want EAGAIN", st, err)
				}
			}
			if remoteReads.Load() != 0 {
				t.Fatalf("old remote GETs = %d, want 0", remoteReads.Load())
			}
		})
	}
}

func TestReadWriteBackFtruncateManyOwnRanges(t *testing.T) {
	remote := []byte("hello world")
	fs, ino, _ := newFtruncateVisibilityFS(t, remote)
	a := addFtruncateTestHandle(t, fs, ino, "/file.bin", remote)
	b := addFtruncateTestHandle(t, fs, ino, "/file.bin", remote)
	reviewFtruncate(t, fs, ino, a, 0)
	for i := 0; i < 1025; i++ {
		if n, st := fs.Write(nil, &gofuse.WriteIn{
			InHeader: gofuse.InHeader{NodeId: ino}, Fh: b, Offset: uint64(i * 2),
		}, []byte("X")); st != gofuse.OK || n != 1 {
			t.Fatalf("write %d = %d/%v, want 1/OK", i, n, st)
		}
	}
	got, st, err := readDat9FSTestRange(fs, ino, b, 0, 1)
	if err != nil || st != gofuse.OK || string(got) != "X" {
		t.Fatalf("first own range after 1025 writes = %q/%v/%v, want X/OK", got, st, err)
	}
}

func TestReadWriteBackFtruncateRepeatedOwnRangeStaysBounded(t *testing.T) {
	remote := []byte("hello world")
	fs, ino, _ := newFtruncateVisibilityFS(t, remote)
	a := addFtruncateTestHandle(t, fs, ino, "/file.bin", remote)
	b := addFtruncateTestHandle(t, fs, ino, "/file.bin", remote)
	reviewFtruncate(t, fs, ino, a, 0)
	for i := 0; i < 1025; i++ {
		if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("X")); st != gofuse.OK || n != 1 {
			t.Fatalf("repeat write %d = %d/%v, want 1/OK", i, n, st)
		}
	}
	handle, _ := fs.fileHandles.Get(b)
	handle.Lock()
	rangeCount := len(handle.ftruncateWrites)
	handle.Unlock()
	if rangeCount != 1 {
		t.Fatalf("coverage ranges = %d, want 1", rangeCount)
	}
	got, st, err := readDat9FSTestRange(fs, ino, b, 0, 1)
	if err != nil || st != gofuse.OK || string(got) != "X" {
		t.Fatalf("repeated own range = %q/%v/%v, want X/OK", got, st, err)
	}
}

func TestReadWriteBackFtruncateNewerCommitSupersedesA(t *testing.T) {
	remote := []byte("hello world")
	var mu sync.Mutex
	revision := int64(1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", strconv.Itoa(len(remote)))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(revision, 10))
		case http.MethodGet:
			_, _ = w.Write(remote)
		case http.MethodPut:
			data, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			remote, revision = data, revision+1
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(revision, 10))
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	opts := &MountOptions{SyncMode: SyncStrict, WritePolicy: WritePolicyWriteBack, TrustLocalEvents: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(server.URL), opts)
	fs.smallFileMax.Store(1 << 20)
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(shadow.Close)
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore, fs.pendingIndex = shadow, pending
	if err := shadow.WriteFull("/file.bin", remote, 1); err != nil {
		t.Fatal(err)
	}
	ino := fs.inodes.Lookup("/file.bin", false, int64(len(remote)), time.Now())
	fs.inodes.UpdateRevision(ino, 1)
	if !fs.inodes.AddAlias(ino, "/alias.bin", "", 2, false, int64(len(remote)), time.Now()) {
		t.Fatal("add hardlink alias")
	}
	a := addFtruncateTestHandle(t, fs, ino, "/file.bin", remote)
	b := addFtruncateTestHandle(t, fs, ino, "/alias.bin", remote)
	c := addFtruncateTestHandle(t, fs, ino, "/file.bin", nil)
	aHandle, _ := fs.fileHandles.Get(a)
	aHandle.ShadowReady = true
	aHandle.ShadowStageGen = shadow.ActiveGeneration("/file.bin")
	reviewFtruncate(t, fs, ino, a, 5)
	cHandle, _ := fs.fileHandles.Get(c)
	cHandle.ShadowGen = shadow.Pin("/file.bin")
	cHandle.ShadowPinned = true
	t.Cleanup(func() { shadow.Unpin(cHandle.ShadowGen) })
	bHandle, _ := fs.fileHandles.Get(b)
	bHandle.WritePolicy = WritePolicyWriteSync
	want := []byte("HELLO WORLD")
	if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, want); st != gofuse.OK || n != uint32(len(want)) {
		t.Fatalf("newer write-sync = %d/%v, want %d/OK", n, st, len(want))
	}
	got, st, err := readDat9FSTestRange(fs, ino, c, 0, len(want))
	if err != nil || st != gofuse.OK || !bytes.Equal(got, want) {
		t.Fatalf("read after newer commit = %q/%v/%v, want %q/OK", got, st, err, want)
	}
	fs.commitQueue = &CommitQueue{
		maxPending:   8,
		client:       fs.client,
		shadows:      shadow,
		index:        pending,
		queuedByPath: make(map[string]map[*CommitEntry]struct{}),
		inFlight:     make(map[string]*CommitEntry),
		workCh:       make(chan *CommitEntry, 9),
	}
	if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
		InHeader: gofuse.InHeader{NodeId: ino}, Valid: gofuse.FATTR_SIZE, Size: 0,
	}}, &gofuse.AttrOut{}); st != gofuse.OK {
		t.Fatalf("path truncate after newer commit = %v, want OK", st)
	}
	got, st, err = readDat9FSTestRange(fs, ino, c, 0, len(want))
	if err != nil || st != gofuse.OK || len(got) != 0 {
		t.Fatalf("read after path zero truncate = %x/%v/%v, want EOF/OK", got, st, err)
	}
}

func TestReadWriteBackFtruncateRechecksAfterLazyLoad(t *testing.T) {
	for _, tc := range []struct {
		name           string
		commitRecorded bool
	}{
		{name: "commit-recorded", commitRecorded: true},
		{name: "put-visible-before-callback"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			remote := []byte("Hi")
			revision := int64(1)
			var mu sync.Mutex
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				switch r.Method {
				case http.MethodHead:
					w.Header().Set("Content-Length", strconv.Itoa(len(remote)))
					w.Header().Set("X-Dat9-IsDir", "false")
					w.Header().Set("X-Dat9-Revision", strconv.FormatInt(revision, 10))
				case http.MethodGet:
					_, _ = w.Write(remote)
				default:
					w.WriteHeader(http.StatusMethodNotAllowed)
				}
			}))
			t.Cleanup(server.Close)
			opts := &MountOptions{SyncMode: SyncInteractive, WritePolicy: WritePolicyWriteBack, TrustLocalEvents: true}
			opts.setDefaults()
			fs := NewDat9FS(newTestClient(server.URL), opts)
			ino := fs.inodes.Lookup("/file.bin", false, 2, time.Now())
			fs.inodes.UpdateRevision(ino, 1)
			a := &FileHandle{
				Ino: ino, Path: "/file.bin", Flags: uint32(syscall.O_RDWR),
				WritePolicy: WritePolicyWriteBack, BaseRev: 1, OrigSize: 2,
				Dirty: fs.newWriteBuffer("/file.bin", 1024, 2),
			}
			a.Dirty.totalSize = 2
			a.Dirty.remoteSize = 2
			aID := fs.allocateFileHandle(a)
			cID := addFtruncateTestHandle(t, fs, ino, "/file.bin", nil)
			reviewFtruncate(t, fs, ino, aID, 4)
			loads := 0
			a.Dirty.LoadPart = func(partNum int) ([]byte, error) {
				if partNum != 1 {
					t.Fatalf("unexpected lazy part %d", partNum)
				}
				loads++
				mu.Lock()
				remote, revision = []byte("BYYY"), 2
				mu.Unlock()
				if tc.commitRecorded {
					seq := fs.markDirtySize(ino, 4)
					fs.recordCommittedMutation(ino, seq, 2, 4)
				}
				return []byte("BY"), nil
			}
			got, st, err := readDat9FSTestRange(fs, ino, cID, 0, 4)
			if tc.commitRecorded {
				if err != nil || st != gofuse.OK || string(got) != "BYYY" {
					t.Fatalf("read after concurrent commit = %q/%v/%v, want BYYY/OK", got, st, err)
				}
				return
			}
			if err != nil || st != gofuse.Status(syscall.EAGAIN) {
				t.Fatalf("read before commit callback = %q/%v/%v, want EAGAIN", got, st, err)
			}
			_, st, err = readDat9FSTestRange(fs, ino, cID, 0, 4)
			if err != nil || st != gofuse.Status(syscall.EAGAIN) || loads != 2 {
				t.Fatalf("retry after revision change = %v/%v, loads=%d, want EAGAIN and fresh load", st, err, loads)
			}
		})
	}
}

func TestReadWriteBackFtruncateLegacyUploadRecordsBeforeChmod(t *testing.T) {
	remote := []byte("hello world")
	var mu sync.Mutex
	var cache *WriteBackCache
	replaceMetaOnGet := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", strconv.Itoa(len(remote)))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
		case http.MethodGet:
			if replaceMetaOnGet {
				replaceMetaOnGet = false
				if _, _, err := cache.putHandleSnapshot("/file.bin", []byte("NEWER DATA!"), 11, PendingOverwrite, 2, 0o644, true, "", "", false, true, nil, 0, 0); err != nil {
					t.Errorf("stage newer data during GET: %v", err)
				}
			}
			_, _ = w.Write(remote)
		case http.MethodPut:
			data, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			remote = data
			w.Header().Set("X-Dat9-Revision", "2")
		case http.MethodPost:
			w.WriteHeader(http.StatusForbidden) // Data landed; chmod did not.
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
	a := addFtruncateTestHandle(t, fs, ino, "/file.bin", remote)
	c := addFtruncateTestHandle(t, fs, ino, "/file.bin", nil)
	reviewFtruncate(t, fs, ino, a, 5)
	newerSeq := fs.markDirtySize(ino, int64(len(remote)))
	var err error
	cache, err = NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.writeBack = cache
	if _, _, err := cache.putHandleSnapshot("/file.bin", []byte("HELLO WORLD"), int64(len(remote)), PendingOverwrite, 1, 0o644, true, "", "", false, true, nil, ino, newerSeq); err != nil {
		t.Fatal(err)
	}
	uploader := &WriteBackUploader{client: fs.client, cache: cache, remoteRoot: "/"}
	uploader.OnDataCommitted = fs.onWriteBackDataCommitted
	successCalled := false
	uploader.OnSuccess = func(WriteBackMeta, int64, StagingGens) { successCalled = true }
	if _, err := uploader.UploadSyncWithRevision(context.Background(), "/file.bin"); err == nil {
		t.Fatal("chmod failure unexpectedly succeeded")
	}
	if got := fs.ftruncateCommittedSeq(ino); got != newerSeq || successCalled {
		t.Fatalf("data-commit watermark = %d, OnSuccess = %t, want %d/false", got, successCalled, newerSeq)
	}
	meta, ok := cache.GetMeta("/file.bin")
	if !ok || meta.Kind != PendingChmod {
		t.Fatalf("cached meta = %+v/%t, want PendingChmod", meta, ok)
	}
	got, st, err := readDat9FSTestRange(fs, ino, c, 0, len(remote))
	if err != nil || st != gofuse.OK || string(got) != "HELLO WORLD" {
		t.Fatalf("read after data commit/chmod failure = %q/%v/%v, want HELLO WORLD/OK", got, st, err)
	}
	mu.Lock()
	replaceMetaOnGet = true
	mu.Unlock()
	_, st, err = readDat9FSTestRange(fs, ino, c, 0, len(remote))
	if err != nil || st != gofuse.Status(syscall.EAGAIN) {
		t.Fatalf("read with a newer staged generation = %v/%v, want EAGAIN", st, err)
	}
}

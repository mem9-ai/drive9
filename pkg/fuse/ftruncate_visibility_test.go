package fuse

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func newFtruncateVisibilityFS(t *testing.T, remote []byte) (*Dat9FS, uint64, *atomic.Int64) {
	t.Helper()
	remoteReads := new(atomic.Int64)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", strconv.Itoa(len(remote)))
			w.Header().Set("X-Dat9-IsDir", "false")
			w.Header().Set("X-Dat9-Revision", "1")
		case http.MethodGet:
			remoteReads.Add(1)
			_, _ = w.Write(remote)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	opts := &MountOptions{SyncMode: SyncInteractive, WritePolicy: WritePolicyWriteBack, TrustLocalEvents: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(server.URL), opts)
	ino := fs.inodes.Lookup("/file.bin", false, int64(len(remote)), time.Now())
	fs.inodes.UpdateRevision(ino, 1)
	return fs, ino, remoteReads
}

func addFtruncateTestHandle(t *testing.T, fs *Dat9FS, ino uint64, path string, content []byte) uint64 {
	t.Helper()
	fh := &FileHandle{Ino: ino, Path: path, WritePolicy: WritePolicyWriteBack}
	if content != nil {
		fh.Dirty = fs.newWriteBuffer(path, maxPreloadSize, 0)
		if _, err := fh.Dirty.Write(0, content); err != nil {
			t.Fatal(err)
		}
		fh.Dirty.ClearDirty()
		fh.BaseRev = 1
		fh.OrigSize = int64(len(content))
	}
	return fs.allocateFileHandle(fh)
}

func TestReadWriteBackFtruncateVisibleBeforeClose(t *testing.T) {
	for _, tc := range []struct {
		name   string
		remote []byte
		size   uint64
		want   []byte
		warm   bool
	}{
		{name: "shrink-cold", remote: []byte("hello world"), size: 5, want: []byte("hello")},
		{name: "shrink-warm", remote: []byte("hello world"), size: 5, want: []byte("hello"), warm: true},
		{name: "grow-cold", remote: []byte("Hi"), size: 4, want: []byte{'H', 'i', 0, 0}},
		{name: "grow-warm", remote: []byte("Hi"), size: 4, want: []byte{'H', 'i', 0, 0}, warm: true},
		{name: "zero", remote: []byte("hello world"), size: 0, want: nil, warm: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, ino, remoteReads := newFtruncateVisibilityFS(t, tc.remote)
			writerID := addFtruncateTestHandle(t, fs, ino, "/file.bin", tc.remote)
			readerID := addFtruncateTestHandle(t, fs, ino, "/file.bin", nil)
			writableReaderID := addFtruncateTestHandle(t, fs, ino, "/file.bin", tc.remote)
			if tc.warm {
				fs.readCache.Put("/file.bin", tc.remote, 1)
			}
			if st := fs.SetAttr(nil, &gofuse.SetAttrIn{
				SetAttrInCommon: gofuse.SetAttrInCommon{
					InHeader: gofuse.InHeader{NodeId: ino},
					Valid:    gofuse.FATTR_FH | gofuse.FATTR_SIZE,
					Fh:       writerID,
					Size:     tc.size,
				},
			}, &gofuse.AttrOut{}); st != gofuse.OK {
				t.Fatalf("ftruncate status = %v", st)
			}
			for _, id := range []uint64{readerID, writableReaderID, addFtruncateTestHandle(t, fs, ino, "/file.bin", nil)} {
				got, st, err := readDat9FSTestRange(fs, ino, id, 0, len(tc.remote)+4)
				if err != nil || st != gofuse.OK || !bytes.Equal(got, tc.want) {
					t.Fatalf("read before writer close = %x/%v/%v, want %x", got, st, err, tc.want)
				}
			}
			if remoteReads.Load() != 0 {
				t.Fatalf("remote GETs after ftruncate = %d, want 0", remoteReads.Load())
			}
		})
	}
}

func TestReadWriteBackFtruncateShrinkThenRegrowBeforeClose(t *testing.T) {
	remote := []byte("hello world")
	fs, ino, remoteReads := newFtruncateVisibilityFS(t, remote)
	writerID := addFtruncateTestHandle(t, fs, ino, "/file.bin", remote)
	readerID := addFtruncateTestHandle(t, fs, ino, "/file.bin", nil)
	for _, size := range []uint64{5, 8} {
		if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: ino},
			Valid:    gofuse.FATTR_FH | gofuse.FATTR_SIZE,
			Fh:       writerID,
			Size:     size,
		}}, &gofuse.AttrOut{}); st != gofuse.OK {
			t.Fatalf("ftruncate to %d status = %v", size, st)
		}
	}
	want := append([]byte("hello"), 0, 0, 0)
	got, st, err := readDat9FSTestRange(fs, ino, readerID, 0, len(remote))
	if err != nil || st != gofuse.OK || !bytes.Equal(got, want) {
		t.Fatalf("shrink-regrow read = %x/%v/%v, want %x", got, st, err, want)
	}
	if remoteReads.Load() != 0 {
		t.Fatalf("old remote GETs = %d, want 0", remoteReads.Load())
	}
}

func TestReadWriteBackFtruncateUsesCurrentShadowGeneration(t *testing.T) {
	remote := []byte("Hi")
	fs, ino, remoteReads := newFtruncateVisibilityFS(t, remote)
	shadow, err := NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(shadow.Close)
	pending, err := NewPendingIndex(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs.shadowStore = shadow
	fs.pendingIndex = pending
	if err := shadow.WriteFull("/file.bin", remote, 1); err != nil {
		t.Fatal(err)
	}
	writerID := addFtruncateTestHandle(t, fs, ino, "/file.bin", remote)
	writer, _ := fs.fileHandles.Get(writerID)
	writer.ShadowReady = true
	writer.ShadowStageGen = shadow.ActiveGeneration("/file.bin")
	readerID := addFtruncateTestHandle(t, fs, ino, "/file.bin", nil)
	if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
		InHeader: gofuse.InHeader{NodeId: ino},
		Valid:    gofuse.FATTR_FH | gofuse.FATTR_SIZE,
		Fh:       writerID,
		Size:     4,
	}}, &gofuse.AttrOut{}); st != gofuse.OK {
		t.Fatalf("ftruncate status = %v", st)
	}
	want := []byte{'H', 'i', 0, 0}
	got, st, err := readDat9FSTestRange(fs, ino, readerID, 0, 8)
	if err != nil || st != gofuse.OK || !bytes.Equal(got, want) {
		t.Fatalf("shadow read = %x/%v/%v, want %x", got, st, err, want)
	}
	if remoteReads.Load() != 0 {
		t.Fatalf("old remote GETs = %d, want 0", remoteReads.Load())
	}
}

func TestWriteBufferLazyShrinkRegrowZeroesDiscardedRemoteParts(t *testing.T) {
	const partSize = int64(16)
	remote := []byte("ABCDEFGHIJKLMNOPabcdefghijklmnop")
	wb := NewWriteBuffer("/file.bin", 64, partSize)
	wb.totalSize = int64(len(remote))
	wb.remoteSize = int64(len(remote))
	wb.LoadPart = func(partNum int) ([]byte, error) {
		start := (partNum - 1) * int(partSize)
		return bytes.Clone(remote[start : start+int(partSize)]), nil
	}
	if err := wb.Truncate(10); err != nil {
		t.Fatal(err)
	}
	if err := wb.Truncate(24); err != nil {
		t.Fatal(err)
	}
	for p := 0; p < 2; p++ {
		if err := wb.EnsureLoaded(p); err != nil {
			t.Fatal(err)
		}
	}
	got := make([]byte, 24)
	if n := wb.ReadAt(0, got); n != len(got) {
		t.Fatalf("read %d bytes, want %d", n, len(got))
	}
	want := append([]byte("ABCDEFGHIJ"), make([]byte, 14)...)
	if !bytes.Equal(got, want) {
		t.Fatalf("shrink-regrow bytes = %x, want %x", got, want)
	}
}

func TestReadWriteBackFtruncateDoesNotLeakToReplacementInode(t *testing.T) {
	remote := []byte("new-data")
	fs, oldIno, remoteReads := newFtruncateVisibilityFS(t, remote)
	writerID := addFtruncateTestHandle(t, fs, oldIno, "/file.bin", []byte("old-data"))
	if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
		InHeader: gofuse.InHeader{NodeId: oldIno},
		Valid:    gofuse.FATTR_FH | gofuse.FATTR_SIZE,
		Fh:       writerID,
		Size:     3,
	}}, &gofuse.AttrOut{}); st != gofuse.OK {
		t.Fatalf("old inode ftruncate status = %v", st)
	}
	fs.inodes.RemoveLinkPreserve("/file.bin")
	newIno := fs.inodes.Lookup("/file.bin", false, int64(len(remote)), time.Now())
	if newIno == oldIno {
		t.Fatal("replacement reused the old inode")
	}
	fs.inodes.UpdateRevision(newIno, 2)
	readerID := addFtruncateTestHandle(t, fs, newIno, "/file.bin", nil)
	got, st, err := readDat9FSTestRange(fs, newIno, readerID, 0, len(remote))
	if err != nil || st != gofuse.OK || !bytes.Equal(got, remote) {
		t.Fatalf("replacement read = %q/%v/%v, want %q", got, st, err, remote)
	}
	if remoteReads.Load() == 0 {
		t.Fatal("replacement read did not reach the new remote object")
	}
}

func TestReadWriteBackFtruncateBusyWriterDoesNotReadOldRemote(t *testing.T) {
	remote := []byte("hello world")
	fs, ino, remoteReads := newFtruncateVisibilityFS(t, remote)
	writerID := addFtruncateTestHandle(t, fs, ino, "/file.bin", remote)
	readerID := addFtruncateTestHandle(t, fs, ino, "/file.bin", nil)
	if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
		InHeader: gofuse.InHeader{NodeId: ino},
		Valid:    gofuse.FATTR_FH | gofuse.FATTR_SIZE,
		Fh:       writerID,
		Size:     5,
	}}, &gofuse.AttrOut{}); st != gofuse.OK {
		t.Fatalf("ftruncate status = %v", st)
	}
	writer, _ := fs.fileHandles.Get(writerID)
	writer.Lock()
	defer writer.Unlock()
	_, st, err := readDat9FSTestRange(fs, ino, readerID, 0, len(remote))
	if err != nil {
		t.Fatal(err)
	}
	if st != gofuse.Status(syscall.EAGAIN) {
		t.Fatalf("read with busy writer status = %v, want EAGAIN", st)
	}
	if remoteReads.Load() != 0 {
		t.Fatalf("old remote GETs = %d, want 0", remoteReads.Load())
	}
}

func TestReadWriteBackFtruncateDoesNotUseOlderTruncateAfterNewerWriter(t *testing.T) {
	remote := []byte("hello world")
	fs, ino, remoteReads := newFtruncateVisibilityFS(t, remote)
	writerID := addFtruncateTestHandle(t, fs, ino, "/file.bin", remote)
	readerID := addFtruncateTestHandle(t, fs, ino, "/file.bin", nil)
	if st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
		InHeader: gofuse.InHeader{NodeId: ino},
		Valid:    gofuse.FATTR_FH | gofuse.FATTR_SIZE,
		Fh:       writerID,
		Size:     5,
	}}, &gofuse.AttrOut{}); st != gofuse.OK {
		t.Fatalf("ftruncate status = %v", st)
	}
	newerID := addFtruncateTestHandle(t, fs, ino, "/file.bin", remote)
	newer, _ := fs.fileHandles.Get(newerID)
	newer.Lock()
	newer.DirtySeq = fs.markDirtySize(ino, newer.Dirty.Size())
	newer.Unlock()
	_, st, err := readDat9FSTestRange(fs, ino, readerID, 0, len(remote))
	if err != nil {
		t.Fatal(err)
	}
	if st != gofuse.Status(syscall.EAGAIN) {
		t.Fatalf("read with unrelated newer writer status = %v, want EAGAIN", st)
	}
	if remoteReads.Load() != 0 {
		t.Fatalf("old remote GETs = %d, want 0", remoteReads.Load())
	}
}

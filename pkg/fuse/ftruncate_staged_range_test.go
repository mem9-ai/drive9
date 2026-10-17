package fuse

import (
	"bytes"
	"net/http"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// Stage through production helpers, then detach the source to isolate staged
// selection. The Linux kernel regression separately covers the Release handoff.
func newStagedFtruncateRangeFS(t *testing.T, size int, readonly bool) (*Dat9FS, uint64, uint64, []byte) {
	t.Helper()
	fs, ino, writerID, readerID, _ := newFtruncateCommitFS(t)
	if readonly {
		readerID = openFtruncateObserver(t, fs, ino)
	}
	reviewFtruncate(t, fs, ino, writerID, 0)
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i*37 + i/251)
	}
	if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: writerID}, data); st != gofuse.OK || n != uint32(len(data)) {
		t.Fatalf("write=%d/%v", n, st)
	}
	writer, _ := fs.fileHandles.Get(writerID)
	writer.Lock()
	err := fs.stageShadowLocked(writer, true)
	fs.releaseHandleRemoteCommitPathLocked(writer)
	writer.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	fs.deleteFileHandle(writerID, writer)
	return fs, ino, readerID, data
}

func TestFtruncateStagedRangeReads(t *testing.T) {
	for _, readonly := range []bool{false, true} {
		name := "writable"
		if readonly {
			name = "readonly"
		}
		t.Run(name, func(t *testing.T) {
			fs, ino, reader, data := newStagedFtruncateRangeFS(t, 9<<20, readonly)
			var requests atomic.Int32
			interceptFtruncateHTTP(t, fs, func(_ http.ResponseWriter, _ *http.Request) bool { requests.Add(1); return false })
			defer func() {
				if requests.Load() != 0 {
					t.Errorf("staged reads made %d remote requests", requests.Load())
				}
			}()
			for _, tc := range []struct {
				name   string
				offset int64
				size   int
			}{
				{"first", 0, 512 << 10}, {"middle", 12345, 64 << 10}, {"large-request", 65537, (1 << 20) + 1},
				{"cross-eof", int64(len(data) - 17), 512 << 10}, {"at-eof", int64(len(data)), 64}, {"past-eof", int64(len(data) + 3), 64},
			} {
				t.Run(tc.name, func(t *testing.T) {
					got, st, err := readDat9FSTestRange(fs, ino, reader, tc.offset, tc.size)
					start := min(tc.offset, int64(len(data)))
					end := min(start+int64(tc.size), int64(len(data)))
					if err != nil || st != gofuse.OK || !bytes.Equal(got, data[start:end]) {
						t.Fatalf("read at %d len=%d status=%v err=%v want len=%d", tc.offset, len(got), st, err, end-start)
					}
				})
			}
		})
	}
}

func TestFtruncateStagedRangeAllocation(t *testing.T) {
	fs, ino, reader, _ := newStagedFtruncateRangeFS(t, 9<<20, true)
	if _, st, err := readDat9FSTestRange(fs, ino, reader, 0, 64<<10); err != nil || st != gofuse.OK {
		t.Fatalf("warm read=%v/%v", st, err)
	}
	fdsBefore, fdErr := os.ReadDir("/proc/self/fd")
	start := time.Now()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < 16; i++ {
		got, st, err := readDat9FSTestRange(fs, ino, reader, int64(i*65536), 64<<10)
		if err != nil || st != gofuse.OK || len(got) != 64<<10 {
			t.Fatalf("read=%d/%v/%v", len(got), st, err)
		}
	}
	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("16 reads of 64KiB from 9MiB file: allocated=%d elapsed=%s", allocated, time.Since(start))
	if fdErr == nil {
		fdsAfter, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatal(err)
		}
		if len(fdsAfter) > len(fdsBefore)+1 {
			t.Fatalf("fd count grew from %d to %d", len(fdsBefore), len(fdsAfter))
		}
		t.Logf("fd count before=%d after=%d", len(fdsBefore), len(fdsAfter))
	}
	if allocated > 8<<20 {
		t.Fatalf("range reads allocated %d bytes; expected request-bounded allocation", allocated)
	}
}

func TestFtruncateStagedRangeRejectsInvalidShadow(t *testing.T) {
	for _, mode := range []string{"generation", "metadata-size", "truncated-file", "missing-file"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, reader, _ := newStagedFtruncateRangeFS(t, 9<<20, true)
			switch mode {
			case "generation":
				if err := fs.shadowStore.WriteFull("/file.bin", []byte("new"), 1); err != nil {
					t.Fatal(err)
				}
			case "metadata-size":
				fs.pendingIndex.mu.Lock()
				fs.pendingIndex.items["/file.bin"].Size--
				fs.pendingIndex.mu.Unlock()
			case "truncated-file":
				if err := os.Truncate(fs.shadowStore.shadowPath("/file.bin"), 1); err != nil {
					t.Fatal(err)
				}
			case "missing-file":
				if err := os.Remove(fs.shadowStore.shadowPath("/file.bin")); err != nil {
					t.Fatal(err)
				}
			}
			for _, off := range []int64{0, 9 << 20} {
				got, st, err := readDat9FSTestRange(fs, ino, reader, off, 64)
				if err != nil || st != gofuse.EAGAIN || len(got) != 0 {
					t.Fatalf("offset=%d read=%d/%v/%v want EAGAIN without bytes", off, len(got), st, err)
				}
			}
			unlock, ok := fs.tryLockRemoteCommitPath("/file.bin")
			if !ok {
				t.Fatal("failure leaked alias fence")
			}
			unlock()
			lock := fs.shadowStore.acquirePathLock("/file.bin")
			fs.shadowStore.releasePathLock("/file.bin", lock)
		})
	}
}

func TestFtruncateStagedRangeSelectsNewestAlias(t *testing.T) {
	for _, newSize := range []int{5, (1 << 20) + 1} {
		fs, ino, reader, _ := newStagedFtruncateRangeFS(t, 9<<20, true)
		if !fs.inodes.AddAlias(ino, "/newer.bin", "", 2, false, int64(newSize), time.Now()) {
			t.Fatal("add alias")
		}
		old, _ := fs.pendingIndex.GetMeta("/file.bin")
		next := bytes.Repeat([]byte("n"), newSize)
		if err := fs.shadowStore.WriteFull("/newer.bin", next, 1); err != nil {
			t.Fatal(err)
		}
		if _, err := fs.pendingIndex.PutWithBaseRev("/newer.bin", int64(newSize), PendingOverwrite, 1); err != nil {
			t.Fatal(err)
		}
		// Build a newer descendant fixture; selection must be correct regardless
		// of map enumeration order.
		// Kernel tests cover publication through actual file operations.
		fs.pendingIndex.mu.Lock()
		meta := fs.pendingIndex.items["/newer.bin"]
		meta.Inode, meta.MutationSeq = ino, old.MutationSeq+1
		meta.SnapshotID, meta.ParentSnapshotID = "newer-staged", old.SnapshotID
		meta.liveAncestors = append([]string{old.SnapshotID}, processLocalMetaAncestors(old)...)
		meta.lineageTrusted, meta.ownedStagingKnown = true, true
		meta.ownedStagingGens = StagingGens{PendingIndexGen: meta.Generation, ShadowGen: fs.shadowStore.ActiveGeneration("/newer.bin")}
		fs.pendingIndex.mu.Unlock()
		for i := 0; i < 32; i++ {
			got, st, err := readDat9FSTestRange(fs, ino, reader, 2, 32)
			if err != nil || st != gofuse.OK || !bytes.Equal(got, next[2:min(34, len(next))]) {
				t.Fatalf("new size=%d read=%q/%v/%v", newSize, got, st, err)
			}
		}
	}
}

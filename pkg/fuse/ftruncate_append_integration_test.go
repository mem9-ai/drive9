package fuse

import (
	"bytes"
	"fmt"
	"strings"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestFtruncateAppendRetainsInheritedPrefix(t *testing.T) {
	for _, parent := range []string{"pending", "fsync", "nonfinal_flush"} {
		t.Run(parent, func(t *testing.T) {
			fs, ino, a, unused, remote := newFtruncateCommitFS(t)
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: unused})
			var opened gofuse.OpenOut
			if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR | syscall.O_APPEND)}, &opened); st != gofuse.OK {
				t.Fatal(st)
			}
			observer := openFtruncateObserver(t, fs, ino)
			reviewFtruncate(t, fs, ino, a, 5)
			switch parent {
			case "fsync":
				hardlinkSync(t, fs, ino, a)
			case "nonfinal_flush":
				if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a}); st != gofuse.OK {
					t.Fatal(st)
				}
			}
			if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: opened.Fh}, []byte("!")); st != gofuse.OK || n != 1 {
				t.Fatalf("append after truncate=%d/%v", n, st)
			}
			for _, id := range []uint64{opened.Fh, observer} {
				readHardlinkWant(t, fs, ino, id, "hello!")
			}
			hardlinkSync(t, fs, ino, opened.Fh)
			if got := remote(); got != "hello!" {
				t.Fatalf("committed append=%q, want hello!", got)
			}
			for _, id := range []uint64{a, opened.Fh, observer} {
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id})
			}
			fs.commitQueue.WaitPath("/file.bin")
			if got := remote(); got != "hello!" {
				t.Fatalf("parent close overwrote accepted append=%q", got)
			}
			if meta, ok := fs.pendingIndex.GetMeta("/file.bin"); ok && meta.Kind == PendingConflict {
				t.Fatalf("accepted append retained conflict: %+v", meta)
			}
		})
	}
}

func TestFtruncateOversizedAppendPreservesStateOnRefusal(t *testing.T) {
	for _, tc := range []struct {
		size   uint64
		prefix string
	}{{0, ""}, {2, "ba"}, {8, "base\x00\x00\x00\x00"}} {
		t.Run(fmt.Sprint(tc.size), func(t *testing.T) {
			fs, ino, server, ids := r6LinkedHandlerFixture(t, false, nil, nil)
			fs.commitQueue.DrainAll()
			fs.commitQueue, fs.shadowStore, fs.pendingIndex = nil, nil, nil
			fs.client.SetSmallFileThresholdForTests(32 << 20)
			cache, err := NewWriteBackCache(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			uploader := NewWriteBackUploader(fs.client, cache, 2)
			fs.SetWriteBack(cache, uploader)
			uploader.OnSuccess, uploader.SnapshotStagingGens = fs.onWriteBackUploadSuccess, fs.snapshotStagingGens
			t.Cleanup(uploader.DrainAll)
			a, _ := fs.fileHandles.Get(ids[0])
			b, _ := fs.fileHandles.Get(ids[1])
			reviewFtruncate(t, fs, ino, ids[0], tc.size)
			if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]}); st != gofuse.OK {
				t.Fatal(st)
			}
			record := strings.Repeat("B", maxLandedPayloadBytes-len(tc.prefix)+1)
			issue1023RejectOversizedAppend(t, fs, ino, ids[1], record, a, b)
			if t.Failed() {
				return
			}
			hardlinkSync(t, fs, ino, ids[0])
			if _, body, _ := server.snapshot(); string(body) != tc.prefix {
				t.Fatalf("committed truncate=%q, want %q", body, tc.prefix)
			}
			if n, st := pr939Append(fs, ino, ids[1], record); int(n) != len(record) || st != gofuse.OK {
				t.Fatalf("append retry after source retirement=%d/%v", n, st)
			}
			hardlinkSync(t, fs, ino, ids[1])
			want := []byte(tc.prefix + record)
			for _, id := range ids {
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id})
			}
			uploader.DrainAll()
			if _, body, _ := server.snapshot(); !bytes.Equal(body, want) {
				t.Fatalf("final bytes=%d, want %d", len(body), len(want))
			}
			for _, path := range []string{a.Path, b.Path} {
				if _, pending := cache.GetMeta(path); pending {
					t.Fatalf("drain retained pending data at %s", path)
				}
			}
		})
	}
}

func TestFtruncateCleanAliasReboundKeepsLineage(t *testing.T) {
	for _, tc := range []struct {
		size uint64
		want string
	}{{0, "J"}, {5, "Jello"}, {14, "Jello world\x00\x00\x00"}} {
		for _, parent := range []string{"pending", "committed"} {
			t.Run(fmt.Sprintf("%d/%s", tc.size, parent), func(t *testing.T) {
				fs, ino, a, b, remote := newHardlinkFS(t)
				observer := openFtruncateObserver(t, fs, ino)
				checkRead := func(id uint64) {
					t.Helper()
					got, st, err := readDat9FSTestRange(fs, ino, id, 0, len(tc.want)+1)
					if err != nil || st != gofuse.OK || string(got) != tc.want {
						t.Fatalf("handle %d read=%q/%v/%v, want %q", id, got, st, err, tc.want)
					}
				}
				reviewFtruncate(t, fs, ino, a, tc.size)
				if parent == "committed" {
					hardlinkSync(t, fs, ino, a)
				}
				hardlinkWrite(t, fs, ino, b, "H")
				hardlinkSync(t, fs, ino, b)
				if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a}, []byte("J")); n != 1 || st != gofuse.OK {
					t.Fatalf("write after alias commit=%d/%v", n, st)
				}
				for _, id := range []uint64{a, b, observer} {
					checkRead(id)
				}
				hardlinkSync(t, fs, ino, a)
				for _, id := range []uint64{a, b, observer} {
					checkRead(id)
					fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id})
				}
				for _, path := range []string{"/file.bin", "/alias.bin"} {
					fs.commitQueue.WaitPath(path)
				}
				if got := remote(); got != tc.want {
					t.Fatalf("final accepted bytes=%q, want %q", got, tc.want)
				}
			})
		}
	}
}

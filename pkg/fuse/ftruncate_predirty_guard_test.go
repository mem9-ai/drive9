package fuse

import (
	"bytes"
	"fmt"
	"net/http"
	"sync/atomic"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestFtruncateRejectsIndependentDirtySibling(t *testing.T) {
	for _, callerDirty := range []bool{false, true} {
		for _, tc := range []struct {
			name string
			size uint64
			want string
		}{
			{"shrink", 5, "Hello"},
			{"grow", 14, "Hello world\x00\x00\x00"},
			{"zero", 0, ""},
		} {
			t.Run(fmt.Sprintf("%s/caller-dirty=%t", tc.name, callerDirty), func(t *testing.T) {
				var puts atomic.Int64
				fs, ino, a, b, remote := newFtruncateCommitFS(t, func(http.ResponseWriter, *http.Request) bool {
					puts.Add(1)
					return false
				})
				if callerDirty {
					if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a, Offset: 2}, []byte("X")); n != 1 || st != gofuse.OK {
						t.Fatalf("caller write = %d/%v, want 1/OK", n, st)
					}
				}
				if n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("H")); n != 1 || st != gofuse.OK {
					t.Fatalf("sibling write = %d/%v, want 1/OK", n, st)
				}
				caller, _ := fs.fileHandles.Get(a)
				sibling, _ := fs.fileHandles.Get(b)
				callerBytes, siblingBytes := caller.Dirty.Bytes(), sibling.Dirty.Bytes()
				callerSeq, siblingSeq := caller.DirtySeq, sibling.DirtySeq
				callerVersion, siblingVersion := caller.Dirty.contentVersion, sibling.Dirty.contentVersion

				st := fs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
					InHeader: gofuse.InHeader{NodeId: ino},
					Valid:    gofuse.FATTR_FH | gofuse.FATTR_SIZE,
					Fh:       a,
					Size:     tc.size,
				}}, &gofuse.AttrOut{})
				if st != gofuse.EAGAIN {
					t.Fatalf("truncate with independent dirty sibling = %v, want EAGAIN", st)
				}
				for _, fh := range []*FileHandle{caller, sibling} {
					if !fh.TryLock() {
						t.Fatal("rejected truncate retained a handle lock")
					}
					fh.Unlock()
					if fh.RemoteCommitUnlock != nil || fh.pendingFtruncate.Load() != nil || fh.Dirty.visibleTruncate {
						t.Fatal("rejected truncate retained a fence or published inheritance")
					}
				}
				if !bytes.Equal(caller.Dirty.Bytes(), callerBytes) || !bytes.Equal(sibling.Dirty.Bytes(), siblingBytes) ||
					caller.Dirty.Size() != 11 || sibling.Dirty.Size() != 11 ||
					caller.DirtySeq != callerSeq || sibling.DirtySeq != siblingSeq ||
					caller.Dirty.contentVersion != callerVersion || sibling.Dirty.contentVersion != siblingVersion {
					t.Fatal("rejected truncate changed accepted bytes, size or mutation generation")
				}
				entry, ok := fs.inodes.GetEntry(ino)
				if !ok || entry.Size != 11 || entry.Revision != 1 || fs.openHandles.HasVisibleTruncate(ino) {
					t.Fatalf("rejected truncate changed inode attributes or visibility: %+v", entry)
				}
				unlock, locked := fs.tryLockRemoteCommitPath("/file.bin")
				if !locked {
					t.Fatal("rejected truncate left the path fence locked")
				}
				unlock()
				if puts.Load() != 0 || remote() != "hello world" {
					t.Fatal("rejected truncate published remote data")
				}
				if got, st, err := readDat9FSTestRange(fs, ino, b, 0, 11); err != nil || st != gofuse.OK || string(got) != "Hello world" {
					t.Fatalf("accepted sibling write = %q/%v/%v, want Hello world/OK", got, st, err)
				}
				if callerDirty {
					// Composition of independent dirty writers belongs to the
					// separate shared-content refactor. This guard preserves both.
					return
				}
				if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}); st != gofuse.OK {
					t.Fatalf("sibling fsync after refusal = %v, want OK", st)
				}
				if got := remote(); got != "Hello world" {
					t.Fatalf("sibling commit after refusal = %q, want Hello world", got)
				}

				// Reopen on the committed baseline. This does not assert that an
				// existing stale handle has acquired full ext4 coherence.
				var fresh gofuse.OpenOut
				if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR)}, &fresh); st != gofuse.OK {
					t.Fatalf("open after sibling commit = %v, want OK", st)
				}
				reviewFtruncate(t, fs, ino, fresh.Fh, tc.size)
				if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: fresh.Fh}); st != gofuse.OK {
					t.Fatalf("fresh truncate fsync = %v, want OK", st)
				}
				if got := remote(); got != tc.want {
					t.Fatalf("fresh truncate commit = %q, want %q", got, tc.want)
				}
			})
		}
	}
}

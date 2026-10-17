package fuse

import (
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestFtruncateNonfinalFlushKeepsSiblingsReadable(t *testing.T) {
	for _, alias := range []bool{false, true} {
		name := "same_path"
		if alias {
			name = "hardlink"
		}
		t.Run(name, func(t *testing.T) {
			fs, ino, a, b, remote := newFtruncateCommitFS(t)
			if alias {
				fs, ino, a, b, remote = newHardlinkFS(t)
			}
			ro := openFtruncateObserver(t, fs, ino)
			reviewFtruncate(t, fs, ino, a, 5)
			for _, id := range []uint64{b, ro} {
				readHardlinkWant(t, fs, ino, id, "hello")
			}
			if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a}); st != gofuse.OK {
				t.Fatal(st)
			}
			parent, _ := fs.fileHandles.Get(a)
			parent.Lock()
			held := parent.RemoteCommitUnlock != nil
			parent.Unlock()
			if held {
				t.Error("nonfinal Flush retained the remote-commit fence")
			}
			for _, id := range []uint64{b, ro} {
				data, st, err := readDat9FSTestRange(fs, ino, id, 0, 5)
				if err != nil || st != gofuse.OK || string(data) != "hello" {
					t.Errorf("read after nonfinal Flush fh=%d = %q/%v/%v", id, data, st, err)
				}
			}
			if remote() != "hello world" {
				t.Error("writeback Flush uploaded before final close")
			}
			if t.Failed() {
				return
			}
			var opened gofuse.OpenOut
			if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR)}, &opened); st != gofuse.OK {
				t.Fatalf("new writable open after nonfinal Flush=%v", st)
			}
			readHardlinkWant(t, fs, ino, opened.Fh, "hello")
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: opened.Fh})
			hardlinkWrite(t, fs, ino, b, "H")
			hardlinkSync(t, fs, ino, b)
			for _, id := range []uint64{a, b} {
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id})
			}
			entry, _ := fs.inodes.GetEntry(ino)
			for p := range entry.Paths {
				fs.commitQueue.WaitPath(p)
				if meta, ok := fs.pendingIndex.GetMeta(p); ok && meta.Kind == PendingConflict {
					t.Fatalf("later close retained conflict at %s: %+v", p, meta)
				}
			}
			if remote() != "Hello" {
				t.Fatalf("later accepted write lost: %q", remote())
			}
		})
	}
}

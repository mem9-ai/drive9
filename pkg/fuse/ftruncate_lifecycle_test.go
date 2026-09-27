package fuse

import (
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestFtruncateInheritanceRetargetsRename(t *testing.T) {
	for _, directory := range []bool{false, true} {
		name := "file"
		if directory {
			name = "directory"
		}
		t.Run(name, func(t *testing.T) {
			fs, ino, a, b, remote := newFtruncateCommitFS(t)
			oldPath, newPath := "/file.bin", "/renamed.bin"
			input := &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}
			if directory {
				fs.inodes.Lookup("/dir", true, 0, time.Now())
				fs.finishLocalRename(input, "/file.bin", "/dir/file.bin")
				oldPath, newPath = "/dir", "/moved"
			}
			reviewFtruncate(t, fs, ino, a, 5)
			parent, _ := fs.fileHandles.Get(a)
			oldEvent := parent.pendingFtruncate.Load()
			oldEventPath := oldEvent.path
			fs.finishLocalRename(input, oldPath, newPath)
			if oldEvent.path != oldEventPath {
				t.Fatal("rename mutated a shared immutable event")
			}
			for _, id := range []uint64{a, b} {
				fh, _ := fs.fileHandles.Get(id)
				if event := fh.pendingFtruncate.Load(); event.path != fh.Path || event.id != oldEvent.id {
					t.Fatalf("event not retargeted: %+v path=%s", event, fh.Path)
				}
			}
			if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}, []byte("H")); st != gofuse.OK {
				t.Fatal(st)
			}
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b}); st != gofuse.OK {
				t.Fatal(st)
			}
			if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a}); st != gofuse.OK {
				t.Fatal(st)
			}
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
			if got := remote(); got != "Hello" {
				t.Fatalf("remote=%q", got)
			}
		})
	}
}

func TestFtruncateReleasedSourceRead(t *testing.T) {
	for _, tc := range []struct {
		name   string
		failed bool
		child  bool
	}{
		{"queued-root", false, false}, {"failed-root", true, false},
		{"queued-child", false, true}, {"failed-child", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, ino, a, b, _ := newFtruncateCommitFS(t)
			// Warm the sibling's original clean buffer, which used to bypass shadow.
			if _, st, err := readDat9FSTestRange(fs, ino, b, 0, 11); err != nil || st != gofuse.OK {
				t.Fatalf("warm read: %v/%v", st, err)
			}
			reviewFtruncate(t, fs, ino, a, 5)
			want := "hello"
			if tc.child {
				if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a}, []byte("H")); st != gofuse.OK {
					t.Fatal(st)
				}
				want = "Hello"
			}
			gate := make(chan struct{})
			var once sync.Once
			t.Cleanup(func() { once.Do(func() { close(gate) }) })
			if tc.failed {
				fs.commitQueue.DrainAll()
			} else {
				fs.commitQueue.PathLock = func(path string) func() { <-gate; return fs.lockRemoteCommitPath(path) }
			}
			fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
			if fs.openHandles.HasVisibleTruncate(ino) {
				t.Fatal("test requires the source marker to be gone")
			}
			before, _ := fs.pendingIndex.GetMeta("/file.bin")
			got, st, err := readDat9FSTestRange(fs, ino, b, 0, 11)
			if err != nil || st != gofuse.OK || string(got) != want {
				t.Fatalf("source closed: got=%q status=%v err=%v want=%q", got, st, err, want)
			}
			after, _ := fs.pendingIndex.GetMeta("/file.bin")
			if before == nil || after == nil || before.Generation != after.Generation || before.Kind != after.Kind {
				t.Fatal("read changed staging ownership")
			}
			if !tc.failed {
				once.Do(func() { close(gate) })
				fs.commitQueue.WaitPath("/file.bin")
				got, st, err = readDat9FSTestRange(fs, ino, b, 0, 11)
				if err != nil || st != gofuse.OK || string(got) != want {
					t.Fatalf("after commit: %q/%v/%v", got, st, err)
				}
			}
		})
	}
}

func TestFtruncateReleasedReadDoesNotHideLaterLiveWrite(t *testing.T) {
	fs, ino, a, b, _ := newFtruncateCommitFS(t)
	var c gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDWR)}, &c); st != gofuse.OK {
		t.Fatal(st)
	}
	reviewFtruncate(t, fs, ino, a, 5)
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
	fs.commitQueue.WaitPath("/file.bin")
	if _, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: c.Fh}, []byte("H")); st != gofuse.OK {
		t.Fatal(st)
	}
	if got, st, _ := readDat9FSTestRange(fs, ino, b, 0, 5); st != gofuse.EAGAIN {
		t.Fatalf("hid newer live write with %q/%v", got, st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: c.Fh}); st != gofuse.OK {
		t.Fatal(st)
	}
	if got, st, err := readDat9FSTestRange(fs, ino, b, 0, 5); err != nil || st != gofuse.OK || string(got) != "Hello" {
		t.Fatalf("after successor commit %q/%v/%v", got, st, err)
	}
}

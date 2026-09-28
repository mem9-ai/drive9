//go:build failpoint

package fuse

import (
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pingcap/failpoint"
)

func TestFtruncateObserverRegistrationPublication(t *testing.T) {
	fs, ino, a, b, _ := newFtruncateCommitFS(t)
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: b})
	if err := fs.shadowStore.WriteFull("/file.bin", []byte("hello world"), 1); err != nil {
		t.Fatal(err)
	}
	point := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateReaderRegistered"
	if err := failpoint.EnableCall(point, func(observed *Dat9FS, reader *FileHandle) {
		if observed != fs {
			return
		}
		if reader.ShadowPinned {
			t.Error("pin acquired before registration")
		}
		fs.shadowStore.Remove("/file.bin")
		reviewFtruncate(t, fs, ino, a, 5)
		hardlinkSync(t, fs, ino, a)
		fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: a})
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = failpoint.Disable(point) })
	ro := openFtruncateObserver(t, fs, ino)
	_ = failpoint.Disable(point)
	readHardlinkWant(t, fs, ino, ro, "hello")
}

func TestFtruncateObserverSelectionRevalidation(t *testing.T) {
	for _, mode := range []string{"event", "view", "path", "stage", "commit", "zero-event", "zero-view"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, a, _, _ := newFtruncateCommitFS(t)
			ro := openFtruncateObserver(t, fs, ino)
			reviewFtruncate(t, fs, ino, a, 5)
			hardlinkSync(t, fs, ino, a)
			if mode == "zero-event" || mode == "zero-view" {
				reviewFtruncate(t, fs, ino, a, 0)
			}
			reader, _ := fs.fileHandles.Get(ro)
			point := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateAliasReadSelected"
			if err := failpoint.EnableCall(point, func(observed *Dat9FS, fh *FileHandle, _ string) {
				if observed != fs || fh != reader {
					return
				}
				switch mode {
				case "event", "zero-event":
					e := *reader.pendingFtruncate.Load()
					e.seq++
					reader.pendingFtruncate.Store(&e)
				case "view", "zero-view":
					fs.mountViewGeneration.Add(1)
				case "path":
					fs.inodes.RemoveLinkPreserve("/file.bin")
					fs.inodes.Lookup("/file.bin", false, 3, time.Now())
				case "stage":
					_, _ = fs.pendingIndex.PutWithBaseRev("/file.bin", 3, PendingOverwrite, 2)
				case "commit":
					state := stateOf(fs, ino)
					fs.recordCommittedMutation(ino, state.committedSeq+1, state.committedRevision+1, 3)
				}
			}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = failpoint.Disable(point) })
			got, st, err := readDat9FSTestRange(fs, ino, ro, 0, 11)
			_ = failpoint.Disable(point)
			if err != nil || st != gofuse.EAGAIN || len(got) != 0 {
				t.Fatalf("changed %s returned %q/%v/%v", mode, got, st, err)
			}
		})
	}
}

// Mount-view reset inspects Prefetch without taking the handle mutex. It must
// never observe a published reader whose prefetcher is still being initialized.
func TestFtruncateObserverPrefetchPublishedBeforeRegistration(t *testing.T) {
	fs, ino, _, _, _ := newFtruncateCommitFS(t)
	fs.inodes.UpdateSize(ino, fs.readCache.MaxFileSize()+1)
	point := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateReaderRegistered"
	called := false
	if err := failpoint.EnableCall(point, func(observed *Dat9FS, reader *FileHandle) {
		if observed != fs {
			return
		}
		called = true
		if reader.Prefetch == nil {
			t.Error("published reader has no initialized prefetcher")
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = failpoint.Disable(point) })
	ro := openFtruncateObserver(t, fs, ino)
	if !called {
		t.Fatal("registration boundary not reached")
	}
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ro})
}

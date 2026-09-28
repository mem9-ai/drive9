//go:build failpoint

package fuse

import (
	"strings"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pingcap/failpoint"
)

func TestReviewFixResetRevalidates(t *testing.T) {
	for _, mode := range []string{"event", "view", "pending", "identity"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, a, _, _ := newFtruncateCommitFS(t)
			interceptFtruncateHTTP(t, fs, nil)
			ro := openFtruncateObserver(t, fs, ino)
			reviewFtruncate(t, fs, ino, a, 5)
			hardlinkSync(t, fs, ino, a)
			fs.resetMountView()
			reader, _ := fs.fileHandles.Get(ro)
			old := reader.pendingFtruncate.Load()
			point := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateResetVerified"
			if err := failpoint.EnableCall(point, func(observed *Dat9FS, fh *FileHandle) {
				if observed != fs || fh != reader {
					return
				}
				switch mode {
				case "event":
					e := *old
					e.seq++
					reader.pendingFtruncate.Store(&e)
				case "view":
					fs.mountViewGeneration.Add(1)
				case "pending":
					_, _ = fs.pendingIndex.PutWithBaseRev("/file.bin", 5, PendingOverwrite, 2)
				case "identity":
					fs.inodes.SetIdentity(ino, "replacement", 1)
				}
			}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = failpoint.Disable(point) })
			got, st, err := readDat9FSTestRange(fs, ino, ro, 0, 5)
			_ = failpoint.Disable(point)
			if err != nil || st != gofuse.EAGAIN || len(got) != 0 {
				t.Fatalf("changed %s recovered stale view: %q/%v/%v", mode, got, st, err)
			}
			if reader.pendingFtruncate.Load() == nil {
				t.Fatal("discarded a changed/unverified event")
			}
		})
	}
}

func TestReviewFixLargeRangeRevalidates(t *testing.T) {
	for _, mode := range []string{"view", "event", "staging"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, a, b, _ := newFtruncateCommitFS(t)
			ro := openFtruncateObserver(t, fs, ino)
			reviewFtruncate(t, fs, ino, a, 5)
			hardlinkWrite(t, fs, ino, b, strings.Repeat("x", int(maxLandedPayloadBytes)+1))
			reader, _ := fs.fileHandles.Get(ro)
			point := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateAliasReadSelected"
			if err := failpoint.EnableCall(point, func(observed *Dat9FS, fh *FileHandle, origin string) {
				if observed != fs || fh != reader {
					return
				}
				if origin != "live" {
					t.Errorf("origin=%s", origin)
				}
				switch mode {
				case "view":
					fs.mountViewGeneration.Add(1)
				case "event":
					e := *reader.pendingFtruncate.Load()
					e.seq++
					reader.pendingFtruncate.Store(&e)
				case "staging":
					_, _ = fs.pendingIndex.PutWithBaseRev("/file.bin", 7, PendingOverwrite, 1)
				}
			}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = failpoint.Disable(point) })
			got, st, err := readDat9FSTestRange(fs, ino, ro, 0, 16)
			_ = failpoint.Disable(point)
			if err != nil || st != gofuse.Status(syscall.EAGAIN) || len(got) != 0 {
				t.Fatalf("invalidated range=%q/%v/%v", got, st, err)
			}
			child, _ := fs.fileHandles.Get(b)
			if !child.TryLock() {
				t.Fatal("range failure leaked source lock")
			}
			child.Unlock()
		})
	}
}

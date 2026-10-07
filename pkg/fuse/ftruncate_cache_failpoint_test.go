//go:build failpoint

package fuse

import (
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pingcap/failpoint"
)

func TestReadWriteBackFtruncateCommittedCacheRevalidates(t *testing.T) {
	for _, name := range []string{"completed-newer-commit", "newer-dirty", "new-staging", "path-replaced", "mount-view-reset", "unverified"} {
		t.Run(name, func(t *testing.T) {
			fs, ino, reader, reads := newCommittedFtruncateCacheFS(t)
			pending, err := NewPendingIndex(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			fs.pendingIndex = pending
			called := false
			point := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateCommittedCacheBeforeValidate"
			if err := failpoint.EnableCall(point, func(observed *Dat9FS, observedIno uint64, path string) {
				if observed != fs || observedIno != ino || path != "/file.bin" {
					return
				}
				called = true
				switch name {
				case "completed-newer-commit":
					seq := fs.markDirtySize(ino, 5)
					fs.recordCommittedMutation(ino, seq, 3, 5)
					fs.clearDirtySize(ino, seq)
					fs.inodes.UpdateRevision(ino, 3)
					fs.readCache.Put(path, []byte("NEW!!"), 3)
				case "newer-dirty":
					fs.markDirtySize(ino, 5)
				case "new-staging":
					if _, err := pending.PutWithBaseRev(path, 5, PendingOverwrite, 2); err != nil {
						t.Fatal(err)
					}
				case "path-replaced":
					fs.inodes.RemoveLink(path)
					fs.inodes.Lookup(path, false, 5, time.Now())
				case "mount-view-reset":
					fs.mountViewGeneration.Add(1)
				case "unverified":
					fs.markStatCacheUnverified()
				}
			}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = failpoint.Disable(point) })
			got, st, err := readDat9FSTestRange(fs, ino, reader, 0, 64)
			if !called || err != nil || st != gofuse.Status(syscall.EAGAIN) {
				t.Fatalf("changed cache read = %q/%v/%v, injected=%t; want EAGAIN", got, st, err, called)
			}
			if reads.Load() != 0 {
				t.Fatal("invalidated cache hit fell through to a different read source")
			}
		})
	}
}

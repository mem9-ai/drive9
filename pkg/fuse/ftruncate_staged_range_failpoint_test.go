//go:build failpoint

package fuse

import (
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pingcap/failpoint"
)

func TestFtruncateStagedRangeRevalidates(t *testing.T) {
	for _, mode := range []string{"shadow-generation", "metadata-generation", "shadow-removed", "path-replaced", "view", "event", "commit"} {
		t.Run(mode, func(t *testing.T) {
			fs, ino, readerID, _ := newStagedFtruncateRangeFS(t, 9<<20, true)
			reader, _ := fs.fileHandles.Get(readerID)
			reached := false
			point := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateAliasReadSelected"
			if err := failpoint.EnableCall(point, func(observed *Dat9FS, fh *FileHandle, origin string) {
				if observed != fs || fh != reader {
					return
				}
				reached = true
				if origin != "staged" {
					t.Errorf("origin=%s want staged", origin)
				}
				switch mode {
				case "shadow-generation":
					if err := fs.shadowStore.WriteFull("/file.bin", []byte("replacement"), 1); err != nil {
						t.Error(err)
					}
				case "metadata-generation":
					if _, err := fs.pendingIndex.PutWithBaseRev("/file.bin", 9<<20, PendingOverwrite, 1); err != nil {
						t.Error(err)
					}
				case "shadow-removed":
					fs.shadowStore.Remove("/file.bin")
				case "path-replaced":
					fs.inodes.RemoveLinkPreserve("/file.bin")
					fs.inodes.Lookup("/file.bin", false, 11, time.Now())
				case "view":
					fs.mountViewGeneration.Add(1)
				case "event":
					event := *reader.pendingFtruncate.Load()
					event.seq++
					reader.pendingFtruncate.Store(&event)
				case "commit":
					fs.recordCommittedMutation(ino, 1000, 2, 9<<20)
				}
			}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = failpoint.Disable(point) })
			got, st, err := readDat9FSTestRange(fs, ino, readerID, 12345, 64<<10)
			_ = failpoint.Disable(point)
			if !reached || err != nil || st != gofuse.EAGAIN || len(got) != 0 {
				t.Fatalf("reached=%t read=%d/%v/%v want EAGAIN without bytes", reached, len(got), st, err)
			}
			unlock, ok := fs.tryLockRemoteCommitPath("/file.bin")
			if !ok {
				t.Fatal("read failure leaked alias fence")
			}
			unlock()
			// This also verifies the local shadow path lock was released before the
			// invalidation callback and remains available after an error.
			if err := fs.shadowStore.WriteFull("/file.bin", []byte("retry"), 2); err != nil {
				t.Fatal(err)
			}
		})
	}
}

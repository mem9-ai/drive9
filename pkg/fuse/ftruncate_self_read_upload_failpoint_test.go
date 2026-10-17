//go:build failpoint

package fuse

import (
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pingcap/failpoint"
)

func TestFtruncateStreamCommitPublishesBeforeRelock(t *testing.T) {
	reached := false
	runFtruncateSelfReadMemoryUploadCommit(t, func(fs *Dat9FS, ino, id uint64) {
		h, _ := fs.fileHandles.Get(id)
		point := "github.com/mem9-ai/drive9/pkg/fuse/ftruncateStreamCommitBeforeRelock"
		if err := failpoint.EnableCall(point, func(observed *Dat9FS, handle *FileHandle) {
			if observed != fs || handle != h || reached {
				return
			}
			reached = true
			// The upload has released its path fence and has not reacquired
			// fh.mu. A successful nested Write represents a legal concurrent
			// request at that boundary and must remain readable.
			n, st := fs.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: id}, []byte("X"))
			if n == 1 && st == gofuse.OK {
				got, readStatus, err := readDat9FSTestRange(fs, ino, id, 0, 5)
				if err != nil || readStatus != gofuse.OK || string(got) != "Xello" {
					t.Errorf("accepted boundary write self-read = %q/%v/%v, want Xello/OK", got, readStatus, err)
				}
			} else {
				// Write contention/errno availability is tracked separately. The
				// helper retries after Fsync and verifies the exact final image.
				if n != 0 || st == gofuse.OK {
					t.Errorf("boundary write = %d/%v, want success or no accepted bytes", n, st)
				}
				t.Logf("boundary writer was not admitted: %v", st)
			}
		}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = failpoint.Disable(point) })
	}, "retained")
	if !reached {
		t.Fatal("stream commit relock boundary was not reached")
	}
}

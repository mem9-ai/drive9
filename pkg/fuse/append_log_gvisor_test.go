package fuse

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestAppendLogGVisorSupersededEntryPoints(t *testing.T) {
	for _, entryPoint := range []string{"Flush", "Fsync", "Release", "flushHandle"} {
		t.Run(entryPoint, func(t *testing.T) {
			var remoteCalls atomic.Int32
			fs, fh, closeServer := newAppendLogEngineFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
				remoteCalls.Add(1)
				http.Error(w, "superseded mutation must not reach remote storage", http.StatusInternalServerError)
			})
			defer closeServer()
			fs.opts.GVisorCompat = true
			fh.Ino = fs.inodes.Lookup(fh.Path, false, fh.OrigSize, time.Now())
			fs.inodes.UpdateRevision(fh.Ino, fh.BaseRev)
			fh.DirtySeq = fs.markDirtySize(fh.Ino, fh.Dirty.Size())
			handleID := fs.allocateFileHandle(fh)
			defer fs.fileHandles.Delete(handleID)

			// A newer same-inode commit must suppress the stale append snapshot
			// before either the append or full-rewrite transport is selected.
			const committedSize = int64(11)
			newerSeq := fs.markDirtySize(fh.Ino, committedSize)
			fs.recordCommittedMutation(fh.Ino, newerSeq, 6, committedSize)

			status := gofuse.OK
			switch entryPoint {
			case "Flush":
				status = fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: fh.Ino}, Fh: handleID})
			case "Fsync":
				status = fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: fh.Ino}, Fh: handleID})
			case "Release":
				fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: fh.Ino}, Fh: handleID})
			case "flushHandle":
				fh.Lock()
				status = fs.flushHandle(context.Background(), fh)
				fh.Unlock()
			}
			if status != gofuse.OK || remoteCalls.Load() != 0 {
				t.Fatalf("status/remote calls = %v/%d, want OK/0", status, remoteCalls.Load())
			}
			if fh.DirtySeq != 0 || fh.Dirty.HasDirtyParts() {
				t.Fatalf("superseded handle remains dirty: seq=%d dirty=%t", fh.DirtySeq, fh.Dirty.HasDirtyParts())
			}
			if fh.BaseRev != 6 || fh.OrigSize != committedSize || fh.Dirty.Size() != committedSize {
				t.Fatalf("baseline revision/original size/buffer size = %d/%d/%d, want 6/11/11", fh.BaseRev, fh.OrigSize, fh.Dirty.Size())
			}
		})
	}
}

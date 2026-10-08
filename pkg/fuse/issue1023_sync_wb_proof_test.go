package fuse

import (
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestR6NoCQSyncWriteBackPublishesActualSnapshot(t *testing.T) {
	fs, ino, server, ids := r6LinkedHandlerFixture(t, false, nil, nil)
	fs.commitQueue.DrainAll()
	fs.commitQueue, fs.shadowStore, fs.pendingIndex = nil, nil, nil
	cache, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	uploader := NewWriteBackUploader(fs.client, cache, 1)
	fs.SetWriteBack(cache, uploader)
	t.Cleanup(uploader.DrainAll)
	if n, st := pr939Append(fs, ino, ids[0], "A"); n != 1 || st != gofuse.OK {
		t.Fatal(n, st)
	}
	if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]}); st != gofuse.OK {
		t.Fatal(st)
	}
	if n, st := pr939Append(fs, ino, ids[1], "B"); n != 1 || st != gofuse.OK {
		t.Fatal(n, st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]}); st != gofuse.OK {
		t.Fatal(st)
	}
	if _, body, _ := server.snapshot(); string(body) != "baseA" {
		t.Fatalf("actual synchronous WB upload=%q", body)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]}); st != gofuse.OK {
		t.Errorf("already-ACKed child Fsync after actual WB parent=%v", st)
	}
	if _, body, _ := server.snapshot(); string(body) != "baseAB" {
		t.Errorf("complete acknowledged records=%q, want baseAB", body)
	}
}

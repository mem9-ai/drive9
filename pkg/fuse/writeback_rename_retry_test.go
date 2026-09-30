package fuse

import (
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestOwnedRenameCoverageDoesNotClaimLegacyCache(t *testing.T) {
	fs, ino, fhID, _, _ := newFtruncateCommitFS(t)
	const oldPath = "/old/file.bin"
	fs.finishLocalRename(&gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "/file.bin", oldPath)
	reviewFtruncate(t, fs, ino, fhID, 5)
	if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: fhID}); st != gofuse.OK {
		t.Fatal(st)
	}
	fh, _ := fs.fileHandles.Get(fhID)
	fh.Lock()
	fs.releaseHandleRemoteCommitPathLocked(fh)
	fh.Unlock()
	// Legacy cache metadata has no process-local ownership proof. The live
	// participant still requires a fence, without acquiring cleanup authority.
	fs.writeBack.mu.Lock()
	fs.writeBack.metas[oldPath].ownedStagingKnown = false
	generation := fs.writeBack.metas[oldPath].Generation
	fs.writeBack.mu.Unlock()
	covered, release, err := fs.tryLockOwnedRenamePaths("/old", "/new")
	defer release()
	if err != nil {
		t.Fatalf("stable live/legacy coverage was rejected: %v", err)
	}
	if _, ok := covered[oldPath]; !ok {
		t.Fatal("live participant omitted from capture")
	}
	if meta, ok := fs.writeBack.GetMeta(oldPath); !ok || meta.Generation != generation || meta.ownedStagingKnown {
		t.Fatalf("capture changed cache ownership: %+v/%t", meta, ok)
	}
}

func TestUploaderTryAcquirePreservesAnotherOwner(t *testing.T) {
	u := &WriteBackUploader{}
	release, ok := u.tryAcquirePath("/file")
	if !ok {
		t.Fatal("free path was not acquired")
	}
	owner := u.inflight["/file"]
	if other, ok := u.tryAcquirePath("/file"); ok || other != nil {
		t.Fatal("busy path was acquired twice")
	}
	replacement := &pathState{done: make(chan struct{})}
	u.inflight["/file"] = replacement
	release()
	if u.inflight["/file"] != replacement {
		t.Fatal("release removed another owner")
	}
	select {
	case <-owner.done:
	default:
		t.Fatal("original owner did not finish")
	}
	select {
	case <-replacement.done:
		t.Fatal("release completed another owner")
	default:
	}
}

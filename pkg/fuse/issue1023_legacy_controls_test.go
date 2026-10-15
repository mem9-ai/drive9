package fuse

import (
	"slices"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestR6LiveFlushStagingDoesNotWaitForFutureRelease(t *testing.T) {
	fs, ino, _, ids := r6LinkedHandlerFixture(t, false, nil, nil)
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
	// No Release and no Submit: this actual Flush producer still owns its
	// generation and must permit both its own write and a linked peer's write.
	for _, step := range []struct {
		id         uint64
		data, want string
	}{{ids[0], "C", "baseAC"}, {ids[1], "B", "baseACB"}} {
		if n, st := pr939Append(fs, ino, step.id, step.data); n != 1 || st != gofuse.OK {
			t.Fatalf("live staged append=%d/%v", n, st)
		}
		fh, _ := fs.fileHandles.Get(step.id)
		if got := pr939HandleBytes(fh); got != step.want {
			t.Errorf("image=%q, want=%q", got, step.want)
		}
	}
}

func TestR6CQNilUntrustedDirtyKnownNewerFencePreservesImage(t *testing.T) {
	fs, ino, server, ids := r6LinkedHandlerFixture(t, false, nil, nil)
	fs.commitQueue.DrainAll()
	fs.commitQueue = nil
	if n, st := pr939Append(fs, ino, ids[0], "A"); n != 1 || st != gofuse.OK {
		t.Fatal(n, st)
	}
	if n, st := pr939Append(fs, ino, ids[1], "B"); n != 1 || st != gofuse.OK {
		t.Fatal(n, st)
	}
	peer, _ := fs.fileHandles.Get(ids[1])
	if got := pr939HandleBytes(peer); got != "baseAB" {
		t.Fatal(got)
	}
	// Actual generic Fsync, with no CQ proof store, commits A and publishes
	// the newer linked watermark. B's existing successful record stays local.
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]}); st != gofuse.OK {
		t.Fatal(st)
	}
	if _, body, _ := server.snapshot(); string(body) != "baseA" {
		t.Fatalf("source commit=%q", body)
	}
	peer.Lock()
	// Recovery cannot reconstruct the live SID relation merely from bytes.
	peer.LineageTrusted = false
	buffer, seq, version, base := peer.Dirty, peer.DirtySeq, peer.Dirty.contentVersion, peer.BaseRev
	contentID, stagedID, stagedSeq := peer.ContentSnapshotID, peer.StagedSnapshotID, peer.StagedSnapshotSeq
	ancestors := append([]string(nil), peer.contentAncestors...)
	before := string(peer.Dirty.Bytes())
	peer.Unlock()
	if fs.latestCommittedRevision(peer.Path) <= base {
		t.Fatal("actual source commit did not publish newer alias fence")
	}
	if n, st := pr939Append(fs, ino, ids[1], "C"); n != 0 || st != gofuse.EAGAIN {
		t.Fatalf("unproved dirty append=%d/%v", n, st)
	}
	peer.Lock()
	preserved := peer.Dirty == buffer && peer.DirtySeq == seq && peer.Dirty.contentVersion == version && peer.BaseRev == base &&
		peer.ContentSnapshotID == contentID && peer.StagedSnapshotID == stagedID && peer.StagedSnapshotSeq == stagedSeq &&
		slices.Equal(peer.contentAncestors, ancestors) && string(peer.Dirty.Bytes()) == before
	peer.Unlock()
	if !preserved {
		t.Error("zero-ACK failure changed retained dirty image/identity/base")
	}
}

func TestR6OlderLegacyCallbackDoesNotReplacePublishedSuccessor(t *testing.T) {
	fs, ino, server, ids := r6LinkedHandlerFixture(t, false, nil, nil)
	if n, st := pr939Append(fs, ino, ids[0], "A"); n != 1 || st != gofuse.OK {
		t.Fatal(n, st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]}); st != gofuse.OK {
		t.Fatal(st)
	}
	first, _ := fs.fileHandles.Get(ids[0])
	oldProof := fs.commitQueue.landedCommit(first.Path)
	oldMeta := WriteBackMeta{Path: first.Path, Size: 5, Kind: PendingOverwrite, BaseRev: 1, SnapshotID: oldProof.snapshotID, lineageTrusted: true, liveAncestors: oldProof.ancestors, liveChecksum: payloadChecksum([]byte("baseA"))}
	if n, st := pr939Append(fs, ino, ids[1], "B"); n != 1 || st != gofuse.OK {
		t.Fatal(n, st)
	}
	if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]}); st != gofuse.OK {
		t.Fatal(st)
	}
	revision, body, _ := server.snapshot()
	if string(body) != "baseAB" || revision <= oldProof.rev {
		t.Fatal("actual successor did not land")
	}
	paths := fs.appendReadPaths(ino)
	proofs := make(map[string]pathCommitLandmark)
	for _, path := range paths {
		proofs[path] = fs.commitQueue.landedCommit(path)
	}
	// Delayed success must clean only its captured generations, never publish
	// the already-superseded bytes/identity after a real successful successor.
	fs.onWriteBackUploadSuccess(oldMeta, oldProof.rev, StagingGens{})
	for _, path := range paths {
		got := fs.commitQueue.landedCommit(path)
		want := proofs[path]
		if fs.latestCommittedRevision(path) != revision || got.rev != want.rev || got.snapshotID != want.snapshotID || got.checksum != want.checksum {
			t.Errorf("old callback replaced %s successor: %+v", path, got)
		}
	}
}

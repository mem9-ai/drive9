package fuse

import (
	"errors"
	"os"
	"reflect"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

type r6LegacyRebaseState struct {
	buffer                                *WriteBuffer
	seq, version, wbSeq, wbGen, stagedSeq uint64
	base, orig, prefixRev, prefixEnd      int64
	isNew, zeroBase                       bool
	contentID, stagedID, parentID, bytes  string
	ancestors, stagedAncestors            []string
}

func r6CaptureLegacyRebaseState(fh *FileHandle) r6LegacyRebaseState {
	return r6LegacyRebaseState{
		buffer: fh.Dirty, seq: fh.DirtySeq, version: fh.Dirty.contentVersion,
		wbSeq: fh.WriteBackSeq, wbGen: fh.WriteBackGen, stagedSeq: fh.StagedSnapshotSeq,
		base: fh.BaseRev, orig: fh.OrigSize, prefixRev: fh.Dirty.prefixRevision, prefixEnd: fh.Dirty.prefixEnd,
		isNew: fh.IsNew, zeroBase: fh.ZeroBase, contentID: fh.ContentSnapshotID,
		stagedID: fh.StagedSnapshotID, parentID: fh.StagedParentSnapshotID,
		bytes: string(fh.Dirty.Bytes()), ancestors: append([]string(nil), fh.contentAncestors...),
		stagedAncestors: append([]string(nil), fh.stagedAncestors...),
	}
}

// This tests only the consumer against real handler bytes and a captured,
// actually uploaded SID. Publication and the whole workload have separate tests.
func TestR6LegacyParentRebaseConsumerControls(t *testing.T) {
	for _, variant := range []string{"owned_cache", "base0_creator_shape", "same_bytes_unrelated_sid", "untrusted", "recovered_cache", "nonprefix", "metadata_write_failure"} {
		t.Run(variant, func(t *testing.T) {
			fs, ino, server, ids := r6LinkedHandlerFixture(t, false, nil, nil)
			fs.commitQueue.DrainAll()
			fs.commitQueue, fs.shadowStore, fs.pendingIndex = nil, nil, nil
			if n, st := pr939Append(fs, ino, ids[0], "A"); n != 1 || st != gofuse.OK {
				t.Fatal(n, st)
			}
			if n, st := pr939Append(fs, ino, ids[1], "B"); n != 1 || st != gofuse.OK {
				t.Fatal(n, st)
			}
			source, _ := fs.fileHandles.Get(ids[0])
			peer, _ := fs.fileHandles.Get(ids[1])
			source.Lock()
			sid, _ := ensureStagedSnapshotLineageLocked(source)
			ancestors := append([]string(nil), source.stagedAncestors...)
			source.Unlock()
			var cache *WriteBackCache
			if variant == "owned_cache" || variant == "recovered_cache" || variant == "metadata_write_failure" {
				var err error
				cache, err = NewWriteBackCache(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				uploader := NewWriteBackUploader(fs.client, cache, 1)
				fs.SetWriteBack(cache, uploader)
				t.Cleanup(uploader.DrainAll)
				if st := fs.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[1]}); st != gofuse.OK {
					t.Fatal(st)
				}
			}
			if st := fs.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: ids[0]}); st != gofuse.OK {
				t.Fatal(st)
			}
			revision, remote, _ := server.snapshot()
			if string(remote) != "baseA" {
				t.Fatalf("actual parent=%q", remote)
			}
			proof := pathCommitLandmark{rev: revision, size: int64(len(remote)), checksum: payloadChecksum(remote), snapshotID: sid, ancestors: ancestors}
			fs.recordCommittedAppendSnapshot(peer.Path, proof)
			peer.Lock()
			if variant == "base0_creator_shape" {
				// Exercise the existing PendingNew authorization shape; the
				// separate real creator workload must still pass independently.
				peer.IsNew, peer.ZeroBase, peer.BaseRev, peer.OrigSize = true, true, 0, 0
			}
			if variant == "same_bytes_unrelated_sid" {
				if err := peer.Dirty.Truncate(int64(len(remote))); err != nil {
					peer.Unlock()
					t.Fatal(err)
				}
				if _, err := peer.Dirty.Write(0, remote); err != nil {
					peer.Unlock()
					t.Fatal(err)
				}
				peer.DirtySeq = fs.markDirtySize(ino, peer.Dirty.Size())
				peer.ContentSnapshotID, peer.contentAncestors = generateMountID(), nil
				clearStagedSnapshotLineageLocked(peer)
			}
			if variant == "untrusted" {
				peer.LineageTrusted = false
			}
			if variant == "nonprefix" {
				if _, err := peer.Dirty.Write(0, []byte("x")); err != nil {
					peer.Unlock()
					t.Fatal(err)
				}
			}
			var oldMeta *WriteBackMeta
			var oldDisk []byte
			if cache != nil {
				var ok bool
				oldMeta, ok = cache.GetMeta(peer.Path)
				if !ok {
					peer.Unlock()
					t.Fatal("owned cache disappeared")
				}
				var err error
				oldDisk, err = os.ReadFile(cache.datFile(peer.Path))
				if err != nil {
					peer.Unlock()
					t.Fatal(err)
				}
			}
			if variant == "recovered_cache" {
				recovered, err := NewWriteBackCache(cache.dir)
				if err != nil {
					peer.Unlock()
					t.Fatal(err)
				}
				meta, ok := recovered.GetMeta(peer.Path)
				if !ok || meta.lineageTrusted || meta.SnapshotID != "" {
					peer.Unlock()
					t.Fatal("disk recovery retained live identity")
				}
				fs.writeBack = recovered
			}
			if variant == "metadata_write_failure" {
				// Force atomic metadata rename to fail while leaving .dat intact.
				if err := os.Remove(cache.metaFile(peer.Path)); err != nil {
					peer.Unlock()
					t.Fatal(err)
				}
				if err := os.Mkdir(cache.metaFile(peer.Path), 0o755); err != nil {
					peer.Unlock()
					t.Fatal(err)
				}
			}
			before := r6CaptureLegacyRebaseState(peer)
			unlock := fs.takeHandleRemoteCommitPathLocked(peer)
			err := fs.rebaseLegacyAppendOntoLandedParentLocked(t.Context(), peer, proof)
			unlock()
			after := r6CaptureLegacyRebaseState(peer)
			peer.Unlock()
			success := variant == "owned_cache" || variant == "base0_creator_shape"
			if !success {
				if err == nil || (variant != "metadata_write_failure" && !errors.Is(err, syscall.EAGAIN)) {
					t.Fatalf("unsafe rebase err=%v", err)
				}
				if !reflect.DeepEqual(before, after) {
					t.Errorf("failed rebase changed pending handle: before=%+v after=%+v", before, after)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if after.base != revision || after.orig != int64(len(remote)) || after.isNew || after.zeroBase ||
					after.buffer != before.buffer || after.seq != before.seq || after.bytes != before.bytes ||
					after.contentID != before.contentID || after.stagedID != before.stagedID ||
					!reflect.DeepEqual(after.ancestors, before.ancestors) || !reflect.DeepEqual(after.stagedAncestors, before.stagedAncestors) ||
					after.prefixRev != revision || after.prefixEnd != int64(len(remote)) || after.wbSeq != before.wbSeq {
					t.Errorf("rebase did not preserve descendant while adopting verified baseline: before=%+v after=%+v", before, after)
				}
			}
			if cache != nil {
				disk, err := os.ReadFile(cache.datFile(peer.Path))
				if err != nil || string(disk) != string(oldDisk) {
					t.Fatalf("metadata rebase changed durable data: data=%q err=%v", disk, err)
				}
				meta, ok := cache.GetMeta(peer.Path)
				if !ok {
					t.Fatal("metadata lost")
				}
				if variant == "owned_cache" {
					if meta.Generation == oldMeta.Generation || after.wbGen != meta.Generation || meta.BaseRev != revision ||
						meta.Kind != PendingOverwrite || meta.SnapshotID != oldMeta.SnapshotID || meta.Size != oldMeta.Size {
						t.Errorf("owned metadata not rebased exactly: before=%+v after=%+v", oldMeta, meta)
					}
					if cache.RemoveIfGeneration(peer.Path, oldMeta.Generation) {
						t.Error("old upload removed rebased cache")
					}
				} else if meta.Generation != oldMeta.Generation || meta.BaseRev != oldMeta.BaseRev {
					t.Error("failed rebase changed original cache metadata")
				}
			}
		})
	}
}

package fuse

import (
	"bytes"
	"context"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

// LayerCacheIdentity identifies the remote content retained in a clean cache.
// Sequences are scoped to a Layer; ancestor and child sequences can overlap.
type LayerCacheIdentity struct {
	LayerID  string
	EntrySeq int64
}

func layerCacheIdentity(entry *client.FSLayerEntry, fallbackLayer string) LayerCacheIdentity {
	if entry == nil {
		return LayerCacheIdentity{}
	}
	id := entry.LayerID
	if id == "" {
		id = fallbackLayer
	}
	return LayerCacheIdentity{LayerID: id, EntrySeq: entry.EntrySeq}
}

func (fs *Dat9FS) upsertLayerFile(ctx context.Context, localPath string, data []byte, expectedRevision int64, mode uint32, hasMode bool) (LayerCacheIdentity, error) {
	if fs.isLayerAbandoned() {
		return LayerCacheIdentity{}, errLayerRolledBack
	}
	// The base write API uses -1 for an unconditional write. Layer entries
	// use zero for no base claim; a negative value fails commit preflight.
	expectedRevision = max(expectedRevision, 0)
	var entry *client.FSLayerEntry
	var err error
	if int64(len(data)) > maxInlineLayerEntryBytes {
		start := fs.perfStart()
		entry, err = fs.client.UploadFSLayerFile(ctx, fs.layerRef(), fs.remotePath(localPath), bytes.NewReader(data), int64(len(data)), expectedRevision, mode, hasMode)
		fs.perfRecordRemote(perfRemoteMutation, start, err, uint64(len(data)))
	} else {
		req := client.FSLayerEntryRequest{
			Path: fs.remotePath(localPath), Op: "upsert", Kind: "file",
			BaseRevision: expectedRevision, Content: data, SizeBytes: int64(len(data)),
		}
		if hasMode {
			req.Mode = mode & 0o777
		}
		entry, err = fs.upsertLayerEntry(ctx, req, uint64(len(data)))
	}
	if err != nil {
		return LayerCacheIdentity{}, err
	}
	if hasMode {
		fs.markLayerFileMode(localPath, mode)
	} else {
		fs.markLayerFile(localPath)
	}
	return layerCacheIdentity(entry, fs.layerRef()), nil
}

// flushLayerHandleLocked uploads and publishes under a proven path lock.
// Caller holds fh.mu; Release may drop it while waiting for another transfer.
func (fs *Dat9FS) flushLayerHandleLocked(ctx context.Context, fh *FileHandle) gofuse.Status {
	size := fh.Dirty.Size()
	unlockRemoteCommit, err := fs.lockLayerCommitPathLocked(fh)
	if err != nil {
		return httpToFuseStatus(err)
	}
	defer unlockRemoteCommit()
	if st := fs.layerHandleMutationStatusLocked(fh); st != gofuse.OK {
		return st
	}
	if fh.releasing {
		// Waiting for another transfer must not consume our upload budget.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout(size))
		defer cancel()
	}
	if fh.Unlinked {
		fs.cancelUnlinkedRemotePublishLocked(fh)
		return gofuse.OK
	}
	if fh.Dirty == nil || !fh.Dirty.HasDirtyParts() {
		return gofuse.OK
	}
	size = fh.Dirty.Size()
	if fs.discardSupersededMutationLocked(fh) {
		fs.removeHandleOwnedStagingLocked(fh)
		return gofuse.OK
	}
	// Freeze the upsert base BEFORE any lazy fetch. The layer branch
	// deliberately reads the un-adopted BaseRev (expectedRevisionForHandle,
	// not the Locked variant) and holds fh.mu for the whole branch, so a
	// sibling commit mid-fetch cannot advance the base — the layer upsert
	// always goes out with base M and the server-side CAS is the backstop
	// against splicing older dirty bytes into a newer remote. Keep this
	// capture ahead of materializeFullForUploadLocked.
	expectedRevision := expectedRevisionForHandle(fh)
	if fh.ShadowSpill || (fh.Streamer != nil && fh.Streamer.Started()) || !fs.materializeFullForUploadLocked(fh) {
		if fh.ShadowSpill {
			if err := fs.commitLayerShadowLocked(ctx, fh, true, true); err != nil {
				safeLogPrintf("layer shadowspill flush failed for %s: %v", fh.Path, err)
				return httpToFuseStatus(err)
			}
			return gofuse.OK
		}
		safeLogPrintf("layer flush cannot materialize full file for %s", fh.Path)
		return gofuse.EIO
	}
	data := fh.Dirty.bytesView()
	mode, hasMode := fs.modeForPendingHandle(fh)
	identity, err := fs.upsertLayerFile(ctx, fh.Path, data, expectedRevision, mode, hasMode)
	if err != nil {
		safeLogPrintf("layer flush failed for %s: %v", fh.Path, err)
		return httpToFuseStatus(err)
	}
	// write-sync handles need no staging before upload, but Layer reads
	// still need a local overlay afterward: the file may not exist in base.
	if fs.shadowStore != nil {
		if err := fs.shadowStore.WriteFull(fh.Path, data, expectedRevision); err != nil {
			return httpToFuseStatus(err)
		}
		fh.ShadowReady = true
		fh.ShadowStageGen = fs.shadowStore.ActiveGeneration(fh.Path)
	}
	fs.recordCommittedMutation(fh.Ino, fh.DirtySeq, 0, size)
	if fh.Dirty != nil {
		fh.Dirty.ClearDirty()
	}
	fs.clearDirtySize(fh.Ino, fh.DirtySeq)
	fh.DirtySeq = 0
	clearReadTargetForLockedHandle(fh)
	if len(data) <= int(fs.readCache.MaxFileSize()) {
		fs.readCache.Put(fh.Path, data, 0)
	}
	fs.inodes.UpdateSize(fh.Ino, size)
	if hasMode {
		fs.inodes.UpdateMode(fh.Ino, mode&0o777)
		fs.clearPendingModeForInodeGeneration(fh.Ino, fh, mode&0o777, fh.PendingModeGen)
		clearPendingModeLocked(fh)
	}
	fs.cacheFileForPath(fh.Path, size, time.Now(), 0)
	if fs.pendingIndex != nil {
		if gen, putErr := fs.pendingIndex.PutLayerCache(fh.Path, size, fh.BaseRev, mode, hasMode, identity); putErr != nil {
			safeLogPrintf("layer flush pending index update failed for %s: %v", fh.Path, putErr)
		} else {
			fh.PendingIndexGen = gen
		}
	}
	return gofuse.OK
}

// lockLayerCommitPathLocked returns a proven path fence with fh.mu held.
// Drop fh.mu while waiting: the transfer owning the path may need it to finish.
func (fs *Dat9FS) lockLayerCommitPathLocked(fh *FileHandle) (func(), error) {
	// An inherited best-effort handle lock may be a no-op after a timeout.
	fs.releaseHandleRemoteCommitPathLocked(fh)
	for {
		path := fh.Path
		wait := fs.opts.RemoteCommitWaitTimeout
		unbounded := fh.releasing || wait <= 0
		fh.Unlock()
		var unlock func()
		locked := true
		if unbounded {
			// Release has no error reply: retain ownership until the lock is free.
			unlock = fs.lockRemoteCommitPath(path)
		} else {
			unlock, locked = fs.lockRemoteCommitPathTimeout(path, wait)
		}
		fh.Lock()
		if !locked {
			return nil, syscall.EAGAIN
		}
		if path == fh.Path {
			return unlock, nil
		}
		unlock()
	}
}

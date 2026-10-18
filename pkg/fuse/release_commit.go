package fuse

import (
	"context"
	"sync/atomic"
	"syscall"
)

// A completion for one selected upload image, published before dropping mu.
// It finishes only this handle's own recorded commit; never transferable authority
// for a different image or handle, and never serialized into recovery metadata.
type releaseCommitReceipt struct {
	buffer                  *WriteBuffer
	version, seq, ino, view uint64
	path, resource          string
	size, expected          int64
	gens                    StagingGens
	revision                atomic.Int64
}

func (fs *Dat9FS) captureReleaseCommitLocked(fh *FileHandle, expected int64, state []*handleLockState) *releaseCommitReceipt {
	if len(state) == 0 || !state[0].participating || fh.Dirty == nil || fh.DirtySeq == 0 {
		return nil
	}
	entry, ok := fs.inodes.GetEntry(fh.Ino)
	if !ok {
		return nil
	}
	r := &releaseCommitReceipt{buffer: fh.Dirty, version: fh.Dirty.contentVersion, seq: fh.DirtySeq,
		ino: fh.Ino, view: fs.mountViewGeneration.Load(), path: fh.Path, resource: entry.ResourceID, size: fh.Dirty.Size(), expected: expected,
		gens: fs.captureHandleStagingGensLocked(fh)}
	fh.releaseCommit = r
	return r
}

func (r *releaseCommitReceipt) complete(revision int64) {
	if r != nil {
		// This acknowledges the successful conditional upload, not a fresh
		// remote-read baseline. Missing CAS/revision information cannot certify it.
		r.revision.Store(committedRevisionForExpectedRevision(r.expected, revision))
	}
}

func (fs *Dat9FS) consumeReleaseCommitLocked(ctx context.Context, fh *FileHandle) (bool, error) {
	if fh == nil {
		return false, nil
	}
	r := fh.releaseCommit
	if r == nil || r.revision.Load() <= 0 {
		return false, nil
	}
	if fh.Dirty != r.buffer || fh.Dirty.contentVersion != r.version ||
		(fh.DirtySeq != r.seq && (fh.DirtySeq != 0 || fh.Dirty.HasDirtyParts())) || fh.Dirty.Size() != r.size {
		fh.releaseCommit = nil // A successor or restored image owns its own completion.
		return false, nil
	}
	entry, ok := fs.inodes.GetEntry(fh.Ino)
	if !ok || fh.Unlinked || fh.Ino != r.ino || !fs.ftruncateAliasLinked(fh) ||
		(r.resource != "" && entry.ResourceID != r.resource) || (r.resource == "" && fh.Path != r.path) {
		return false, nil
	}
	hadFence := fh.ftruncateFence
	if err := fs.fenceFtruncateLocked(fh); err != nil {
		return true, err
	}
	if !hadFence {
		defer fs.releaseHandleRemoteCommitPathLocked(fh)
	}
	view, epoch := fs.mountViewGeneration.Load(), fh.readInvalidationEpoch.Load()
	paths := fs.ftruncateAliasPaths(fh.Ino)
	changedView := r.view != view
	if changedView {
		// Completion survives cache invalidation, but an old view cannot
		// authorize finalization against a replaced or advanced remote image.
		if r.resource == "" {
			return true, syscall.EAGAIN
		}
		stat, err := fs.client.StatCtx(ctx, fs.remotePath(fh.Path))
		if err != nil {
			return true, err
		}
		if err := ctx.Err(); err != nil {
			return true, err
		}
		if stat == nil || stat.IsDir || stat.ResourceID != r.resource || stat.Revision != r.revision.Load() || stat.Size != r.size {
			return true, syscall.EAGAIN
		}
	}
	if !fs.mountViewMu.TryRLock() {
		return true, errFtruncateBusy
	}
	defer fs.mountViewMu.RUnlock()
	entry, ok = fs.inodes.GetEntry(fh.Ino)
	if !ok || !fs.ftruncateAliasesValid(fh.Ino, paths, view) ||
		(r.resource != "" && entry.ResourceID != r.resource) ||
		(changedView && epoch != fh.readInvalidationEpoch.Load()) {
		return true, syscall.EAGAIN
	}
	fs.updateHandleRevisionMetadataLocked(fh, r.revision.Load(), r.size)
	if fs.captureHandleStagingGensLocked(fh) != r.gens {
		// Do not clean newer staging. The exact same image may retry using
		// its own completed CAS baseline, but must preserve the newer owner.
		return false, nil
	}
	if fh.HasPendingMode {
		return true, nil // Keep the source intact until its metadata finishes too.
	}
	fs.clearDirtySize(fh.Ino, fh.DirtySeq)
	fh.Dirty.ClearDirty()
	fh.DirtySeq, fh.WriteBackSeq = 0, 0
	fs.removeHandleOwnedStagingLocked(fh)
	clearHandleShadowClaimLocked(fh)
	fh.releaseCommit = nil
	fs.markHandleRemoteCommittedLocked(fh, r.revision.Load())
	if fh.Streamer != nil {
		fh.Streamer.ResetForNextWrite(expectedRevisionForHandle(fh))
	}
	clearReadTargetForLockedHandle(fh)
	return true, nil
}

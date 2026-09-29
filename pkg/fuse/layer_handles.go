package fuse

import (
	"errors"
	"fmt"
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/client"
)

var errLayerReplayBusy = errors.New("layer refresh handle busy")

// lockLayerReplayHandles is called under the path publication fence. Never
// wait for fh.mu here: a writer holding it may itself be waiting for that fence.
// A busy handle postpones replay without advancing the watcher's cursor.
func (fs *Dat9FS) lockLayerReplayHandles(path string) ([]*FileHandle, func(), error) {
	var handles []*FileHandle
	unlock := func() {
		for _, fh := range handles {
			fh.Unlock()
		}
	}
	for _, fh := range fs.openHandles.SnapshotPath(path) {
		if !fh.TryLock() {
			unlock()
			return nil, nil, fmt.Errorf("%w: %s", errLayerReplayBusy, path)
		}
		if fh.Path != path || fh.Unlinked || isLocalFileHandle(fh) || fh.Layer == PathLayerGitWorkspace {
			fh.Unlock()
			continue
		}
		handles = append(handles, fh)
	}
	return handles, unlock, nil
}

// preserveLayerReplayConflict retains the newest local mutation durably before
// rejecting publication over an observed external tip. The Layer API compares
// base revisions, not Layer tips, so retrying that upload would lose an update.
// Caller holds the path fence and all affected handle locks.
func (fs *Dat9FS) preserveLayerReplayConflict(path string, tip client.FSLayerEntry, handles []*FileHandle, pending *PendingIndex) (bool, error) {
	meta, exists := pending.GetMeta(path)
	if exists && meta.Kind == PendingConflict {
		return true, nil
	}
	identity := layerCacheIdentity(&tip, fs.layerRef())
	unchanged := exists && meta.LayerID == identity.LayerID && meta.LayerEntrySeq > 0 &&
		(meta.LayerEntrySeq == identity.EntrySeq || (meta.LayerID == fs.layerRef() && meta.LayerEntrySeq > identity.EntrySeq))
	var newest *FileHandle
	for _, fh := range handles {
		if fh.Dirty != nil && (fh.Dirty.HasDirtyParts() || fh.HasPendingMode) && (newest == nil || fh.DirtySeq > newest.DirtySeq) {
			newest = fh
		}
	}
	if newest != nil && !unchanged {
		if !newest.ShadowSpill && !fs.materializeFullForUploadLocked(newest) {
			return true, fmt.Errorf("preserve layer conflict %s: cannot materialize local content", path)
		}
		if err := fs.stageShadowLocked(newest, true); err != nil {
			return true, err
		}
		return true, pending.MarkConflict(path)
	}
	if exists && !meta.LayerClean {
		// No prior identity means the local snapshot had no Layer tip.
		// An observed tip is still a divergence, even after the writer closed.
		if !unchanged {
			return true, pending.MarkConflict(path)
		}
		return true, nil
	}
	// An unchanged replay must not rebase a live dirty buffer to its clean cache.
	return newest != nil, nil
}

func (fs *Dat9FS) rebaseLayerReplayHandles(path string, handles []*FileHandle) error {
	if len(handles) == 0 {
		return nil
	}
	meta, ok := fs.pendingIndex.GetMeta(path)
	if !ok || !meta.LayerClean || meta.Kind == PendingConflict {
		return nil
	}
	for _, fh := range handles {
		clearReadTargetForLockedHandle(fh)
		if fh.Dirty == nil {
			continue
		}
		if err := fs.loadWritableHandleFromShadowLocked(fh, meta); err != nil {
			return err
		}
		fh.ShadowSpill = false
		fh.DirtySeq, fh.ShadowStageSeq, fh.WriteBackSeq = 0, 0, 0
		fh.ShadowCommitReady, fh.ShadowCommitSeq = false, 0
		clearStagedSnapshotLineageLocked(fh)
		fs.inodes.UpdateSize(fh.Ino, meta.Size)
	}
	return nil
}

func (fs *Dat9FS) layerHandleMutationStatusLocked(fh *FileHandle) gofuse.Status {
	if !fs.layerEnabled() || fh.Unlinked {
		return gofuse.OK
	}
	if fs.pendingIndex != nil {
		if meta, ok := fs.pendingIndex.GetMeta(fh.Path); ok && meta.Kind == PendingConflict {
			return gofuse.Status(syscall.EAGAIN)
		}
	}
	if fs.isLayerWhiteout(fh.Path) {
		return gofuse.ENOENT
	}
	return gofuse.OK
}

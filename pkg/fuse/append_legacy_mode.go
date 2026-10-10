package fuse

import (
	"context"
	"slices"
	"syscall"
)

// claimLandedAppendPendingModeLocked retains only the mode metadata for this
// live landed image. WBSeq describes content; a clean handle can still own a
// fresh PendingChmod generation. Caller holds fh.mu.
func (fs *Dat9FS) claimLandedAppendPendingModeLocked(fh *FileHandle, snapshotID string) *WriteBackMeta {
	if fs.commitQueue != nil || fs.layerEnabled() || fs.writeBack == nil || fh.WriteBackGen == 0 ||
		!fh.LineageTrusted || !fh.HasPendingMode || fh.PendingModeGen == 0 || snapshotID == "" {
		return nil
	}
	meta, ok := fs.writeBack.GetMeta(fh.Path)
	if !ok || meta.Kind != PendingChmod || meta.Generation == 0 || !meta.lineageTrusted ||
		!meta.HasMode || meta.Mode&posixPermissionModeMask != fh.PendingMode&posixPermissionModeMask ||
		(meta.SnapshotID != snapshotID && !slices.Contains(fh.contentAncestors, meta.SnapshotID)) {
		return nil
	}
	fh.WriteBackGen = meta.Generation
	return meta
}

// finishLandedAppendPendingModeLocked completes only the mode obligation of a
// clean no-CQ append image, retaining newer mode/cache generations. The
// existing mode helper owns its generation recheck and handle unlock window.
func (fs *Dat9FS) finishLandedAppendPendingModeLocked(ctx context.Context, fh *FileHandle) error {
	if fs.commitQueue != nil || fs.layerEnabled() || fh.Dirty == nil || fh.Dirty.HasDirtyParts() ||
		!fh.LineageTrusted || fh.ContentSnapshotID == "" || !fh.HasPendingMode {
		return nil
	}
	meta := fs.claimLandedAppendPendingModeLocked(fh, fh.ContentSnapshotID)
	if meta == nil {
		if fs.writeBack != nil && fh.WriteBackGen != 0 {
			if pending, ok := fs.writeBack.GetMeta(fh.Path); ok && pending.Kind == PendingChmod {
				return syscall.EAGAIN
			}
		}
		return fs.applyPendingModeForHandleLocked(ctx, fh)
	}
	path, ino, generation := fh.Path, fh.Ino, meta.Generation
	if err := fs.applyPendingModeForHandleLocked(ctx, fh); err != nil {
		return err
	}
	// A concurrent SetAttr keeps HasPendingMode with a newer mode generation;
	// its cache must remain available for the next durability call.
	if fh.Path == path && fh.Ino == ino && !fh.Unlinked && !fh.HasPendingMode {
		fs.writeBack.RemoveIfGeneration(path, generation)
		if fh.WriteBackGen == generation {
			fh.WriteBackGen = 0
		}
	}
	return nil
}

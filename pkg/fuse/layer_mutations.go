package fuse

import (
	"context"
	"fmt"
	"io"
	"syscall"

	"github.com/mem9-ai/drive9/pkg/client"
)

func (fs *Dat9FS) upsertLayerEntry(ctx context.Context, req client.FSLayerEntryRequest, bytes uint64) (*client.FSLayerEntry, error) {
	if fs.isLayerAbandoned() {
		return nil, errLayerRolledBack
	}
	layerRef := fs.layerRef()
	if layerRef == "" {
		return nil, fmt.Errorf("fs layer is not configured")
	}
	start := fs.perfStart()
	entry, err := fs.client.UpsertFSLayerEntry(ctx, layerRef, req)
	fs.perfRecordRemote(perfRemoteMutation, start, err, bytes)
	return entry, err
}

func (fs *Dat9FS) upsertLayerMkdir(ctx context.Context, localPath string, mode uint32) error {
	if _, err := fs.upsertLayerEntry(ctx, client.FSLayerEntryRequest{
		Path: fs.remotePath(localPath),
		Op:   "mkdir",
		Kind: "dir",
		Mode: mode & 0o777,
	}, 0); err != nil {
		return err
	}
	fs.markLayerDir(localPath, mode)
	return nil
}

func (fs *Dat9FS) upsertLayerWhiteout(ctx context.Context, localPath string, kind deleteKind) error {
	entryKind := "file"
	if kind == deleteKindDir {
		entryKind = "dir"
	}
	if _, err := fs.upsertLayerEntry(ctx, client.FSLayerEntryRequest{
		Path: fs.remotePath(localPath),
		Op:   "whiteout",
		Kind: entryKind,
	}, 0); err != nil {
		return err
	}
	fs.markLayerWhiteout(localPath)
	return nil
}

func (fs *Dat9FS) upsertLayerChmod(ctx context.Context, localPath string, mode uint32) error {
	// Chmod can publish the retained shadow even with no live writer. It
	// cannot resolve an observed conflict by overwriting the remote tip.
	if fs.pendingIndex != nil {
		if meta, ok := fs.pendingIndex.GetMeta(localPath); ok && meta.Kind == PendingConflict {
			return syscall.EAGAIN
		}
	}
	if fs.shadowStore != nil {
		baseRev := int64(0)
		hadPending := false
		var pendingGen uint64
		if fs.pendingIndex != nil {
			if meta, ok := fs.pendingIndex.GetMeta(localPath); ok {
				baseRev = meta.BaseRev
				hadPending = true
				pendingGen = meta.Generation
			}
		}
		data, err := fs.shadowStore.ReadAll(localPath)
		if testHookAfterLayerChmodShadowRead != nil {
			testHookAfterLayerChmodShadowRead(localPath)
		}
		if err == nil {
			if !hadPending && fs.client != nil {
				stat, err := fs.client.StatCtx(ctx, fs.remotePath(localPath))
				if err == nil && stat != nil && !stat.IsDir {
					baseRev = stat.Revision
				} else if err != nil && !isNotFoundErr(err) {
					return err
				}
			}
			identity, err := fs.upsertLayerFile(ctx, localPath, data, baseRev, mode, true)
			if err != nil {
				return err
			}
			if fs.pendingIndex != nil {
				if !hadPending {
					if _, err := fs.pendingIndex.putLayerCacheIfAbsent(localPath, int64(len(data)), baseRev, mode, true, identity); err != nil {
						return fmt.Errorf("put pending mode for layer chmod %s: %w", localPath, err)
					}
					return nil
				}
				if err := fs.pendingIndex.MarkLayerCommittedIfGeneration(localPath, pendingGen, baseRev, mode, true, identity); err != nil {
					return fmt.Errorf("update pending mode for layer chmod %s: %w", localPath, err)
				}
			}
			return nil
		}
	}
	kind := fs.layerEntryKind(ctx, localPath)
	if kind == "file" && fs.client != nil {
		entry, err := fs.client.GetFSLayerEntry(ctx, fs.layerRef(), fs.remotePath(localPath))
		if err == nil && entry != nil && entry.Op == "upsert" && entry.Kind == "file" && entry.StorageRef == "" && entry.StorageType != "s3" {
			baseRev := entry.BaseRevision
			identity, err := fs.upsertLayerFile(ctx, localPath, entry.Content, baseRev, mode, true)
			if err != nil {
				return err
			}
			if fs.pendingIndex != nil {
				if _, err := fs.pendingIndex.putLayerCacheIfAbsent(localPath, int64(len(entry.Content)), baseRev, mode, true, identity); err != nil {
					return fmt.Errorf("put pending mode for layer chmod %s: %w", localPath, err)
				}
			}
			return nil
		}
		if err != nil && !isNotFoundErr(err) {
			return err
		}
	}
	if _, err := fs.upsertLayerEntry(ctx, client.FSLayerEntryRequest{
		Path: fs.remotePath(localPath),
		Op:   "chmod",
		Kind: kind,
		Mode: mode & 0o777,
	}, 0); err != nil {
		return err
	}
	switch kind {
	case "file":
		fs.markLayerFileMode(localPath, mode)
	case "dir":
		fs.markLayerDir(localPath, mode)
	case "symlink":
		if target, existingMode, ok := fs.layerSymlink(localPath); ok {
			nextMode := (existingMode &^ uint32(0o777)) | (mode & 0o777)
			if nextMode&uint32(syscall.S_IFMT) == 0 {
				nextMode |= uint32(syscall.S_IFLNK)
			}
			fs.markLayerSymlink(localPath, target, nextMode)
		}
	}
	return nil
}

func (fs *Dat9FS) layerEntryKind(ctx context.Context, localPath string) string {
	if _, ok := fs.layerDirMode(localPath); ok {
		return "dir"
	}
	if _, _, ok := fs.layerSymlink(localPath); ok {
		return "symlink"
	}
	if fs != nil && fs.inodes != nil {
		if ino, ok := fs.inodes.GetInode(localPath); ok {
			if entry, ok := fs.inodes.GetEntry(ino); ok {
				if entry.IsDir {
					return "dir"
				}
				if entryIsSymlink(entry) {
					return "symlink"
				}
			}
		}
	}
	if fs != nil && fs.client != nil {
		if stat, err := fs.client.StatCtx(ctx, fs.remotePath(localPath)); err == nil && stat != nil {
			if stat.IsDir {
				return "dir"
			}
			if stat.HasMode && isSymlinkMode(stat.Mode) {
				return "symlink"
			}
		}
	}
	return "file"
}

func (fs *Dat9FS) upsertLayerSymlink(ctx context.Context, localPath string, target string) error {
	if _, err := fs.upsertLayerEntry(ctx, client.FSLayerEntryRequest{
		Path:        fs.remotePath(localPath),
		Op:          "symlink",
		Kind:        "symlink",
		ContentText: target,
		SizeBytes:   int64(len(target)),
		Mode:        symlinkMode(),
	}, uint64(len(target))); err != nil {
		return err
	}
	fs.markLayerSymlink(localPath, target, symlinkMode())
	return nil
}

func (fs *Dat9FS) upsertLayerRename(ctx context.Context, oldLocalPath, newLocalPath string) error {
	oldRemote := fs.remotePath(oldLocalPath)
	newRemote := fs.remotePath(newLocalPath)
	if target, mode, ok := fs.layerSymlink(oldLocalPath); ok {
		if mode == 0 {
			mode = symlinkMode()
		}
		if _, err := fs.upsertLayerEntry(ctx, client.FSLayerEntryRequest{
			Path:        newRemote,
			Op:          "symlink",
			Kind:        "symlink",
			ContentText: target,
			SizeBytes:   int64(len(target)),
			Mode:        mode,
		}, uint64(len(target))); err != nil {
			return err
		}
		fs.markLayerSymlink(newLocalPath, target, mode)
		if err := fs.upsertLayerWhiteout(ctx, oldLocalPath, deleteKindFile); err != nil {
			return err
		}
		return nil
	}
	var (
		data    []byte
		mode    uint32
		hasMode bool
	)
	if fs.pendingIndex != nil {
		if meta, ok := fs.pendingIndex.GetMeta(oldLocalPath); ok {
			mode = meta.Mode
			hasMode = meta.HasMode
			if fs.shadowStore != nil && fs.shadowStore.Has(oldLocalPath) {
				var err error
				data, err = fs.shadowStore.ReadAll(oldLocalPath)
				if err != nil {
					return err
				}
			} else {
				entry, err := fs.client.GetFSLayerEntry(ctx, fs.layerRef(), oldRemote)
				if err != nil && !isNotFoundErr(err) {
					return err
				}
				if err == nil {
					if entry.StorageRef != "" || entry.StorageType == "s3" {
						reader, err := fs.client.ReadFSLayerFileStream(ctx, fs.layerRef(), oldRemote, layerEntryFetchMaxSeq(entry, false, 0))
						if err != nil {
							return err
						}
						data, err = io.ReadAll(reader)
						closeErr := reader.Close()
						if err != nil {
							return err
						}
						if closeErr != nil {
							return closeErr
						}
					} else {
						// Empty inline content is still authoritative Layer data.
						data = append([]byte{}, entry.Content...)
					}
					if entry.Mode != 0 {
						mode = entry.Mode
						hasMode = true
					}
				}
			}
		}
	}
	if data == nil {
		sourceStat, err := fs.client.StatCtx(ctx, oldRemote)
		if err != nil {
			return err
		}
		if sourceStat.IsDir {
			if targetStat, err := fs.client.StatCtx(ctx, newRemote); err == nil {
				if targetStat.IsDir {
					return fmt.Errorf("rename target %s is a directory", newRemote)
				}
				return fmt.Errorf("rename target %s exists", newRemote)
			} else if !isNotFoundErr(err) {
				return err
			}
			if _, err := fs.upsertLayerEntry(ctx, client.FSLayerEntryRequest{
				Path:        oldRemote,
				Op:          "rename",
				Kind:        "dir",
				ContentText: newRemote,
				Mode:        sourceStat.Mode & 0o777,
			}, 0); err != nil {
				return err
			}
			fs.markLayerWhiteout(oldLocalPath)
			fs.markLayerDir(newLocalPath, sourceStat.Mode&0o777)
			return nil
		}
		data, err = fs.client.ReadCtx(ctx, oldRemote)
		if err != nil {
			return err
		}
		mode = sourceStat.Mode
		hasMode = sourceStat.HasMode
	}
	targetBaseRev := int64(0)
	if targetStat, err := fs.client.StatCtx(ctx, newRemote); err == nil {
		if targetStat.IsDir {
			return fmt.Errorf("rename target %s is a directory", newRemote)
		}
		targetBaseRev = targetStat.Revision
	} else if !isNotFoundErr(err) {
		return err
	}
	identity, err := fs.upsertLayerFile(ctx, newLocalPath, data, targetBaseRev, mode, hasMode)
	if err != nil {
		return err
	}
	if fs.shadowStore != nil {
		if err := fs.shadowStore.WriteFull(newLocalPath, data, targetBaseRev); err != nil {
			return err
		}
	}
	if fs.pendingIndex != nil {
		if _, err := fs.pendingIndex.PutLayerCache(newLocalPath, int64(len(data)), targetBaseRev, mode, hasMode, identity); err != nil {
			return err
		}
	}
	if err := fs.upsertLayerWhiteout(ctx, oldLocalPath, deleteKindFile); err != nil {
		return err
	}
	return nil
}

func (fs *Dat9FS) commitLayerShadowLocked(ctx context.Context, fh *FileHandle, shadowSpill, pathLocked bool) error {
	if fs.commitQueue == nil {
		return fmt.Errorf("missing commit queue")
	}
	if fs.shadowStore == nil || !fs.shadowStore.Has(fh.Path) {
		return fmt.Errorf("missing shadow for %s", fh.Path)
	}
	gvisorCompat := fs.gvisorCompatibilityEnabled()
	if !pathLocked {
		unlock, err := fs.lockLayerCommitPathLocked(fh)
		if err != nil {
			return err
		}
		defer unlock()
	}
	if fh.Unlinked {
		fs.cancelUnlinkedRemotePublishLocked(fh)
		return nil
	}
	if st := fs.layerHandleMutationStatusLocked(fh); st != 0 {
		return syscall.Errno(st)
	}
	if fs.discardSupersededMutationLocked(fh) {
		fs.removeHandleOwnedStagingLocked(fh)
		return nil
	}
	// A duplicate descriptor can append after an earlier Flush staged metadata.
	// Publish the current size and shadow source under the upload fence before
	// binding the generation that onCommitSuccess will acknowledge as clean.
	if fs.pendingIndex != nil {
		if err := fs.stageShadowLocked(fh, true); err != nil {
			return err
		}
	}
	handlePath := fh.Path
	handleIno := fh.Ino
	mutationSeq := fh.DirtySeq
	size := int64(0)
	if fh.Dirty != nil {
		size = fh.Dirty.Size()
	}
	mode, hasMode := fs.modeForPendingHandle(fh)
	expectedRevision := fs.expectedRevisionForHandleLocked(fh)
	payloadBaseRev := fh.BaseRev
	entry := &CommitEntry{
		Path:        fh.Path,
		Inode:       fh.Ino,
		MutationSeq: fh.DirtySeq,
		BaseRev:     expectedRevision,
		Size:        size,
		Kind:        fs.pendingKindForHandle(fh),
		ShadowSpill: shadowSpill,
		Mode:        mode,
		HasMode:     hasMode,
	}
	fs.bindCommitEntryToHandleLocked(entry, fh, payloadBaseRev)
	fh.Unlock()
	err := fs.commitQueue.commitNowPathLocked(ctx, entry)
	fh.Lock()
	if err != nil {
		return err
	}
	if gvisorCompat && (fh.Unlinked || fh.Path != handlePath || fh.DirtySeq != mutationSeq) {
		return nil
	}
	if hasMode {
		fs.clearPendingModeForInodeGeneration(fh.Ino, fh, mode&0o777, fh.PendingModeGen)
		clearPendingModeLocked(fh)
	}
	if fh.Dirty != nil {
		fh.Dirty.ClearDirty()
		if gvisorCompat {
			fs.clearDirtySize(handleIno, mutationSeq)
		} else {
			fs.clearDirtySize(fh.Ino, fh.DirtySeq)
		}
		fh.DirtySeq = 0
	}
	fh.WriteBackSeq = 0
	return nil
}

func (fs *Dat9FS) upsertLayerHardlink(ctx context.Context, srcP, dstP string, mode uint32, hasMode bool) ([]byte, error) {
	if fs == nil || !fs.layerEnabled() {
		return nil, fmt.Errorf("fs layer is not configured")
	}
	if _, err := fs.client.StatCtx(ctx, fs.remotePath(dstP)); err == nil {
		return nil, fmt.Errorf("hardlink target exists: %s", dstP)
	} else if !isNotFoundErr(err) {
		return nil, err
	}
	var (
		data []byte
		err  error
	)
	if fs.shadowStore != nil && fs.shadowStore.Has(srcP) {
		data, err = fs.shadowStore.ReadAll(srcP)
	} else {
		data, err = fs.client.ReadCtx(ctx, fs.remotePath(srcP))
	}
	if err != nil {
		return nil, err
	}
	if !hasMode {
		mode = 0o644
	}
	identity, err := fs.upsertLayerFile(ctx, dstP, data, 0, mode, true)
	if err != nil {
		return nil, err
	}
	if fs.shadowStore != nil {
		if err := fs.shadowStore.WriteFull(dstP, data, 0); err != nil {
			return nil, err
		}
	}
	if fs.pendingIndex != nil {
		if _, err := fs.pendingIndex.PutLayerCache(dstP, int64(len(data)), 0, mode, true, identity); err != nil {
			return nil, err
		}
	}
	return data, nil
}

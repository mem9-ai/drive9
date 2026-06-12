package fuse

import (
	"context"
	"strings"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

const stickyPermissionBit uint32 = 0o1000

type renamePathInfo struct {
	path    string
	exists  bool
	entry   *InodeEntry
	mode    uint32
	hasMode bool
	isDir   bool
	special bool
}

func (i renamePathInfo) owner(fs *Dat9FS) gofuse.Owner {
	if i.entry != nil {
		return fs.entryOwner(i.entry)
	}
	return gofuse.Owner{Uid: fs.uid, Gid: fs.gid}
}

func (i renamePathInfo) modeOrDefault() uint32 {
	if i.hasMode {
		return i.mode
	}
	if i.isDir {
		return uint32(syscall.S_IFDIR) | 0o755
	}
	return uint32(syscall.S_IFREG) | defaultRegularFileMode
}

func (fs *Dat9FS) renamePreflight(ctx context.Context, input *gofuse.RenameIn, oldP, newP string) (renamePathInfo, renamePathInfo, gofuse.Status) {
	oldInfo, err := fs.renamePathInfo(ctx, oldP)
	if err != nil {
		return oldInfo, renamePathInfo{}, httpToFuseStatus(err)
	}
	if !oldInfo.exists {
		return oldInfo, renamePathInfo{}, gofuse.ENOENT
	}

	if oldInfo.isDir && strings.HasPrefix(newP, oldP+"/") {
		return oldInfo, renamePathInfo{}, gofuse.Status(syscall.EINVAL)
	}

	oldParent, ok := fs.inodes.GetPath(input.NodeId)
	if !ok {
		oldParent = parentDir(oldP)
	}
	newParent := oldParent
	if input.Newdir != input.NodeId {
		if p, ok := fs.inodes.GetPath(input.Newdir); ok {
			newParent = p
		} else {
			newParent = parentDir(newP)
		}
	}

	oldParentInfo, err := fs.renamePathInfo(ctx, oldParent)
	if err != nil {
		return oldInfo, renamePathInfo{}, httpToFuseStatus(err)
	}
	if !oldParentInfo.exists || !oldParentInfo.isDir {
		return oldInfo, renamePathInfo{}, gofuse.ENOENT
	}
	newParentInfo := oldParentInfo
	if newParent != oldParent {
		newParentInfo, err = fs.renamePathInfo(ctx, newParent)
		if err != nil {
			return oldInfo, renamePathInfo{}, httpToFuseStatus(err)
		}
		if !newParentInfo.exists || !newParentInfo.isDir {
			return oldInfo, renamePathInfo{}, gofuse.ENOENT
		}
	}

	caller := input.Owner
	if st := fs.renameCheckParentAccess(caller, oldParentInfo); st != gofuse.OK {
		return oldInfo, renamePathInfo{}, st
	}
	if newParent != oldParent {
		if st := fs.renameCheckParentAccess(caller, newParentInfo); st != gofuse.OK {
			return oldInfo, renamePathInfo{}, st
		}
	}
	if st := fs.renameCheckSticky(caller, oldParentInfo, oldInfo); st != gofuse.OK {
		return oldInfo, renamePathInfo{}, st
	}

	newInfo, err := fs.renamePathInfo(ctx, newP)
	if err != nil {
		return oldInfo, newInfo, httpToFuseStatus(err)
	}

	if newInfo.exists {
		switch {
		case oldInfo.isDir && !newInfo.isDir:
			return oldInfo, newInfo, gofuse.Status(syscall.ENOTDIR)
		case !oldInfo.isDir && newInfo.isDir:
			return oldInfo, newInfo, gofuse.Status(syscall.EISDIR)
		case oldInfo.isDir && newInfo.isDir:
			empty, st := fs.renameDirEmpty(ctx, newP, newInfo)
			if st != gofuse.OK {
				return oldInfo, newInfo, st
			}
			if !empty {
				return oldInfo, newInfo, gofuse.Status(syscall.ENOTEMPTY)
			}
		}
		if st := fs.renameCheckSticky(caller, newParentInfo, newInfo); st != gofuse.OK {
			return oldInfo, newInfo, st
		}
	}

	return oldInfo, newInfo, gofuse.OK
}

func (fs *Dat9FS) renameCheckParentAccess(caller gofuse.Owner, parent renamePathInfo) gofuse.Status {
	if caller.Uid == 0 {
		return gofuse.OK
	}
	if !hasPOSIXAccess(caller, parent.owner(fs), parent.modeOrDefault(), gofuse.W_OK|gofuse.X_OK, nil) {
		return gofuse.EACCES
	}
	return gofuse.OK
}

func (fs *Dat9FS) renameCheckSticky(caller gofuse.Owner, parent, victim renamePathInfo) gofuse.Status {
	if caller.Uid == 0 || parent.modeOrDefault()&stickyPermissionBit == 0 {
		return gofuse.OK
	}
	if caller.Uid == parent.owner(fs).Uid || caller.Uid == victim.owner(fs).Uid {
		return gofuse.OK
	}
	return gofuse.EPERM
}

func (fs *Dat9FS) renamePathInfo(ctx context.Context, p string) (renamePathInfo, error) {
	info := renamePathInfo{path: p}
	if entry, ok := fs.specialNodeEntry(p); ok {
		return renameInfoFromEntry(p, entry, true), nil
	}
	if ino, ok := fs.inodes.GetInode(p); ok {
		if entry, ok := fs.inodes.GetEntry(ino); ok {
			return renameInfoFromEntry(p, entry, false), nil
		}
	}
	if fs.pendingIndex != nil {
		if meta, ok := fs.pendingIndex.GetMeta(p); ok {
			mtime := meta.Mtime
			if mtime.IsZero() {
				mtime = time.Now()
			}
			ino := fs.inodes.EnsureInode(p, false, meta.Size, mtime)
			if meta.HasMode {
				fs.inodes.UpdateMode(ino, meta.Mode)
			}
			if entry, ok := fs.inodes.GetEntry(ino); ok {
				return renameInfoFromEntry(p, entry, false), nil
			}
		}
	} else if fs.writeBack != nil {
		if meta, ok := fs.writeBack.GetMeta(p); ok {
			mtime := meta.Mtime
			if mtime.IsZero() {
				mtime = time.Now()
			}
			ino := fs.inodes.EnsureInode(p, false, meta.Size, mtime)
			if meta.HasMode {
				fs.inodes.UpdateMode(ino, meta.Mode)
			}
			if entry, ok := fs.inodes.GetEntry(ino); ok {
				return renameInfoFromEntry(p, entry, false), nil
			}
		}
	}
	if entry, ok := fs.openHandleEntry(p); ok {
		return renameInfoFromEntry(p, entry, false), nil
	}

	statStart := fs.perfStart()
	stat, err := fs.client.StatCtx(ctx, fs.remotePath(p))
	fs.perfRecordRemote(perfRemoteStat, statStart, err, 0)
	if err != nil {
		if isNotFoundErr(err) {
			return info, nil
		}
		return info, err
	}
	mtime := stat.Mtime
	if mtime.IsZero() {
		mtime = time.Now()
	}
	ino := fs.inodes.EnsureInodeWithIdentity(p, stat.ResourceID, stat.Nlink, stat.IsDir, stat.Size, mtime)
	if stat.Revision > 0 {
		fs.inodes.UpdateRevision(ino, stat.Revision)
	}
	if stat.HasMode {
		fs.inodes.UpdateMode(ino, stat.Mode)
	}
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok {
		return info, syscall.EIO
	}
	return renameInfoFromEntry(p, entry, false), nil
}

func renameInfoFromEntry(p string, entry *InodeEntry, special bool) renamePathInfo {
	info := renamePathInfo{
		path:    p,
		exists:  true,
		entry:   entry,
		mode:    entry.Mode,
		hasMode: entry.HasMode,
		isDir:   entry.IsDir,
		special: special || entryIsMetadataOnlySpecial(entry),
	}
	if info.hasMode && info.mode&fileKindModeMask == uint32(syscall.S_IFDIR) {
		info.isDir = true
	}
	return info
}

func (fs *Dat9FS) renameDirEmpty(ctx context.Context, p string, info renamePathInfo) (bool, gofuse.Status) {
	if !info.exists || !info.isDir {
		return false, gofuse.Status(syscall.ENOTDIR)
	}
	if fs.dirCache != nil {
		if entries := fs.specialNodeDirEntries(p, map[string]struct{}{}); len(entries) > 0 {
			return false, gofuse.OK
		}
	}
	prefix := p + "/"
	for _, entry := range fs.inodes.Snapshot() {
		for childPath := range entry.Paths {
			if strings.HasPrefix(childPath, prefix) {
				return false, gofuse.OK
			}
		}
	}
	listStart := fs.perfStart()
	items, err := fs.client.ListCtx(ctx, fs.remotePath(p))
	fs.perfRecordRemote(perfRemoteList, listStart, err, 0)
	if err != nil {
		if isNotFoundErr(err) {
			return true, gofuse.OK
		}
		// Some tests and older servers do not expose a usable list response for
		// this preflight. Defer to the backend rename, which still enforces
		// non-empty directory replacement on the authoritative namespace.
		return true, gofuse.OK
	}
	return len(items) == 0, gofuse.OK
}

func (fs *Dat9FS) removeRenameSpecialTarget(ctx context.Context, info renamePathInfo) gofuse.Status {
	if !info.exists || !info.special {
		return gofuse.OK
	}
	fs.removeSpecialNode(info.path)
	fs.inodes.RemoveLink(info.path)
	parentPath, name := cacheParentName(info.path)
	fs.dirCache.Remove(parentPath, name)
	fs.cacheNegativePath(info.path)
	return gofuse.OK
}

func (fs *Dat9FS) renameMetadataOnlySpecial(ctx context.Context, input *gofuse.RenameIn, oldP, newP string, newInfo renamePathInfo) gofuse.Status {
	if newInfo.exists && !newInfo.special {
		if newInfo.isDir {
			return gofuse.Status(syscall.EISDIR)
		}
		if err := fs.deleteRemoteFileWithInterruptRecovery(ctx, newP); err != nil && !isNotFoundErr(err) {
			return httpToFuseStatus(err)
		}
		fs.inodes.RemoveLink(newP)
	}
	if st := fs.removeRenameSpecialTarget(ctx, newInfo); st != gofuse.OK {
		return st
	}
	if !fs.renameSpecialNode(oldP, newP) {
		return gofuse.ENOENT
	}
	fs.finishLocalRename(input, oldP, newP)
	return gofuse.OK
}

func (fs *Dat9FS) prepareRenameDirReplacement(ctx context.Context, oldInfo, newInfo renamePathInfo) gofuse.Status {
	if !oldInfo.isDir || !newInfo.exists || !newInfo.isDir || newInfo.special {
		return gofuse.OK
	}
	if err := fs.deleteRemoteDirWithInterruptRecovery(ctx, newInfo.path); err != nil {
		if isNotFoundErr(err) {
			return gofuse.OK
		}
		if isConflictErr(err) {
			return gofuse.Status(syscall.ENOTEMPTY)
		}
		return httpToFuseStatus(err)
	}
	fs.inodes.Remove(newInfo.path)
	parentPath, name := cacheParentName(newInfo.path)
	fs.adjustDirectoryLinkCount(parentPath, -1)
	fs.dirCache.Remove(parentPath, name)
	fs.cacheNegativePath(newInfo.path)
	fs.dirCache.InvalidatePrefix(newInfo.path)
	fs.readCache.InvalidatePrefix(newInfo.path + "/")
	fs.invalidateDiskReadCachePrefix(newInfo.path + "/")
	return gofuse.OK
}

func (fs *Dat9FS) renameRemoteFileToMissingTargetFallback(ctx context.Context, input *gofuse.RenameIn, oldInfo, newInfo renamePathInfo, renameErr error) (bool, gofuse.Status) {
	if !isNotFoundErr(renameErr) || !oldInfo.exists || oldInfo.isDir || oldInfo.special || newInfo.exists {
		return false, gofuse.OK
	}
	if oldInfo.entry == nil || !entryIsRegularFile(oldInfo.entry) || oldInfo.entry.Size != 0 {
		return false, gofuse.OK
	}
	if oldInfo.entry.Nlink > 1 || len(oldInfo.entry.Paths) > 1 {
		return false, gofuse.OK
	}

	createStart := fs.perfStart()
	committedRev, createErr := fs.client.CreateFileCtx(ctx, fs.remotePath(newInfo.path))
	if isCreateActionUnsupportedErr(createErr) {
		committedRev, createErr = fs.client.WriteCtxConditionalWithRevision(ctx, fs.remotePath(newInfo.path), nil, 0)
	}
	fs.perfRecordRemote(perfRemoteMutation, createStart, createErr, 0)
	if createErr != nil {
		return false, gofuse.OK
	}

	if err := fs.deleteRemoteFileWithInterruptRecovery(ctx, oldInfo.path); err != nil && !isNotFoundErr(err) {
		return true, httpToFuseStatus(err)
	}

	if committedRev > 0 && oldInfo.entry.Ino > 0 {
		fs.inodes.UpdateRevision(oldInfo.entry.Ino, committedRev)
		fs.readCache.Put(newInfo.path, nil, committedRev)
	}
	fs.finishLocalRename(input, oldInfo.path, newInfo.path)
	return true, gofuse.OK
}

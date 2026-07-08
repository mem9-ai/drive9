package fuse

import (
	"context"
	"path"
	"sort"
	"strings"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

const fileKindModeMask uint32 = syscall.S_IFMT

func metadataNodeMode(mode uint32) uint32 {
	return mode & (fileKindModeMask | posixPermissionModeMask)
}

func metadataOnlySpecialMode(mode uint32) bool {
	switch mode & fileKindModeMask {
	case syscall.S_IFIFO, syscall.S_IFCHR, syscall.S_IFBLK, syscall.S_IFSOCK:
		return true
	default:
		return false
	}
}

func entryIsMetadataOnlySpecial(entry *InodeEntry) bool {
	return entry != nil && !entry.IsDir && entry.HasMode && metadataOnlySpecialMode(entry.Mode)
}

func (fs *Dat9FS) specialNodeEntry(p string) (*InodeEntry, bool) {
	if fs == nil {
		return nil, false
	}
	fs.specialMu.RLock()
	ino, ok := fs.specialByPath[p]
	fs.specialMu.RUnlock()
	if !ok {
		return nil, false
	}
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok || !entryIsMetadataOnlySpecial(entry) {
		fs.removeSpecialNode(p)
		return nil, false
	}
	return entry, true
}

func (fs *Dat9FS) addSpecialNode(p string, ino uint64) {
	fs.specialMu.Lock()
	defer fs.specialMu.Unlock()

	fs.specialByPath[p] = ino
}

func (fs *Dat9FS) removeSpecialNode(p string) {
	fs.specialMu.Lock()
	defer fs.specialMu.Unlock()

	delete(fs.specialByPath, p)
}

func (fs *Dat9FS) renameSpecialNode(oldP, newP string) bool {
	fs.specialMu.Lock()
	defer fs.specialMu.Unlock()

	ino, ok := fs.specialByPath[oldP]
	if !ok {
		return false
	}
	delete(fs.specialByPath, oldP)
	delete(fs.specialByPath, newP)
	fs.specialByPath[newP] = ino
	return true
}

func (fs *Dat9FS) renameSpecialNodeSubtree(oldP, newP string) {
	if fs == nil || oldP == newP {
		return
	}

	fs.specialMu.Lock()
	defer fs.specialMu.Unlock()

	oldPrefix := strings.TrimSuffix(oldP, "/") + "/"
	updates := make(map[string]uint64)
	for p, ino := range fs.specialByPath {
		if p == oldP {
			updates[newP] = ino
			delete(fs.specialByPath, p)
			continue
		}
		if strings.HasPrefix(p, oldPrefix) {
			updates[newP+p[len(oldP):]] = ino
			delete(fs.specialByPath, p)
		}
	}
	for p := range updates {
		delete(fs.specialByPath, p)
	}
	for p, ino := range updates {
		fs.specialByPath[p] = ino
	}
}

func (fs *Dat9FS) linkMetadataOnlySpecial(input *gofuse.LinkIn, srcEntry *InodeEntry, dstP, name string, out *gofuse.EntryOut) gofuse.Status {
	if _, ok := fs.specialNodeEntry(dstP); ok {
		return gofuse.Status(syscall.EEXIST)
	}
	if fs.openHandles.Has(0, dstP) || fs.hasPendingLocalState(dstP) || fs.hasQueuedCommit(dstP) {
		return gofuse.Status(syscall.EEXIST)
	}
	ctx, cancel := context.WithTimeout(context.Background(), namespaceMutationRetryTimeout)
	exists, err := fs.pendingRenameTargetExists(ctx, dstP)
	cancel()
	if err != nil {
		return httpToFuseStatus(err)
	}
	if exists {
		return gofuse.Status(syscall.EEXIST)
	}

	nlink := srcEntry.Nlink + 1
	if nlink < 2 {
		nlink = 2
	}
	if !fs.inodes.AddAliasIfAbsent(input.Oldnodeid, dstP, "", nlink, false, srcEntry.Size, time.Now()) {
		return gofuse.Status(syscall.EEXIST)
	}
	fs.inodes.UpdateCtime(input.Oldnodeid, time.Now())
	fs.addSpecialNode(dstP, input.Oldnodeid)

	entry, ok := fs.inodes.GetEntry(input.Oldnodeid)
	if !ok {
		return gofuse.EIO
	}
	parentPath, _ := fs.inodes.GetPath(input.NodeId)
	fs.dirCache.Upsert(parentPath, cachedInfoFromEntry(name, entry))
	fs.touchDirectoryChangeTime(parentPath, time.Now())
	for aliasPath := range entry.Paths {
		fs.cacheEntryForPath(aliasPath, entry)
	}
	fs.fillEntryOut(entry, out)
	return gofuse.OK
}

func (fs *Dat9FS) specialNodeDirEntries(dirPath string, existing map[string]struct{}) []DirEntry {
	if fs == nil {
		return nil
	}

	fs.specialMu.RLock()
	paths := make([]string, 0, len(fs.specialByPath))
	for p := range fs.specialByPath {
		if parentDir(p) == dirPath {
			paths = append(paths, p)
		}
	}
	fs.specialMu.RUnlock()
	sort.Strings(paths)

	entries := make([]DirEntry, 0, len(paths))
	for _, p := range paths {
		name := path.Base(p)
		if _, ok := existing[name]; ok {
			continue
		}
		entry, ok := fs.specialNodeEntry(p)
		if !ok {
			continue
		}
		mtime := entry.Mtime
		if mtime.IsZero() {
			mtime = time.Now()
		}
		entries = append(entries, DirEntry{
			Name:        name,
			Ino:         entry.Ino,
			Mode:        dirEntryMode(false, entry.HasMode, entry.Mode),
			Size:        entry.Size,
			Mtime:       mtime,
			AttrMode:    entry.Mode,
			HasMode:     entry.HasMode,
			IsDir:       false,
			Nlink:       entry.Nlink,
			HasMetadata: true,
		})
		existing[name] = struct{}{}
	}
	return entries
}

func (fs *Dat9FS) setMetadataOnlySpecialAttr(input *gofuse.SetAttrIn, entry *InodeEntry, out *gofuse.AttrOut) gofuse.Status {
	if input.Valid&gofuse.FATTR_SIZE != 0 {
		return gofuse.Status(syscall.EINVAL)
	}

	metadataChanged := false
	if mtime, ok := input.GetMTime(); ok {
		entry.Mtime = mtime
		fs.inodes.UpdateMtime(input.NodeId, mtime)
		metadataChanged = true
	}
	if atime, ok := input.GetATime(); ok {
		entry.Atime = atime
		fs.inodes.UpdateAtime(input.NodeId, atime)
		metadataChanged = true
	}

	ownerUID, hasUID, ownerGID, hasGID := resolveSetAttrOwner(input)
	if hasUID || hasGID {
		if st := fs.checkSetAttrOwnerForCaller(input, entry, ownerUID, hasUID, ownerGID, hasGID); st != gofuse.OK {
			return st
		}
		if hasUID {
			entry.Uid = ownerUID
			entry.HasUID = true
		}
		if hasGID {
			entry.Gid = ownerGID
			entry.HasGID = true
		}
		fs.inodes.UpdateOwner(input.NodeId, ownerUID, ownerGID, hasUID, hasGID)
		metadataChanged = true
	}

	if input.Valid&gofuse.FATTR_MODE != 0 {
		perm, st := fs.setAttrModeForCaller(input, entry)
		if st != gofuse.OK {
			return st
		}
		mode := (entry.Mode & fileKindModeMask) | perm
		entry.Mode = mode
		entry.HasMode = true
		fs.inodes.UpdateMode(input.NodeId, mode)
		metadataChanged = true
	}

	if metadataChanged {
		ctime := time.Now()
		entry.Ctime = ctime
		fs.inodes.UpdateCtime(input.NodeId, ctime)
		for aliasPath := range entry.Paths {
			fs.cacheEntryForPath(aliasPath, entry)
		}
	}
	fs.fillAttr(entry, &out.Attr)
	out.SetTimeout(fs.opts.AttrTTL)
	return gofuse.OK
}
